/**
 * The exporter's local watermark: one SQLite file (ids, revisions, counts and
 * object keys; never content). Losing it is safe: a fresh run re-reads Neon,
 * finds every unchanged chunk and manifest already stored and writes nothing,
 * but it then re-reads everything, so keep it.
 *
 * - `groups`: every `(org, session)` the last sweeps saw, with its Neon
 *   revision and activity, and the sweep id that last saw it.
 * - `exported`: what this exporter last wrote for a group: the revision it
 *   read before reading the content, and the manifest keys it owns.
 */
import { DatabaseSync } from "node:sqlite";

export interface ExportedRow {
  org_id: string;
  session_id: string;
  revision: number;
  exported_at: number;
  keys: string[];
  error: string | null;
  attempts: number;
}

export interface WorkItem {
  org_id: string;
  session_id: string;
  revision: number;
  kind: "changed" | "recent" | "history" | "retry" | "reverify";
  events: number;
}

export interface Backlog {
  groups: number;
  exported: number;
  pending: number;
  pendingEvents: number;
  recentPending: number;
  recentPendingEvents: number;
  changed: number;
  errored: number;
  pendingDeletes: number;
}

export class SyncState {
  readonly db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    // Shared by the exporter and the erasure job (two processes): WAL plus a busy wait.
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 10000;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS groups (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL,
        revision INTEGER NOT NULL DEFAULT 0, last_ms INTEGER, events INTEGER NOT NULL DEFAULT 0,
        rev_sweep INTEGER NOT NULL DEFAULT 0, act_sweep INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS exported (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL,
        revision INTEGER NOT NULL, exported_at INTEGER NOT NULL, keys TEXT NOT NULL,
        error TEXT, attempts INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0,
        failed_revision INTEGER,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS erasures (
        key TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind IN ('object', 'prefix')),
        org_id TEXT NOT NULL, reason TEXT NOT NULL, enqueued_at INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0, error TEXT);
      CREATE TABLE IF NOT EXISTS pending_keys (org_id TEXT NOT NULL, session_id TEXT NOT NULL, key TEXT NOT NULL,
        PRIMARY KEY (org_id, session_id, key));
      CREATE TABLE IF NOT EXISTS leases (prefix TEXT PRIMARY KEY, holder TEXT NOT NULL, at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS erasure_seen (org_id TEXT NOT NULL, session_id TEXT NOT NULL, sweep INTEGER NOT NULL,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS truncations (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL, manifest_key TEXT NOT NULL,
        strings INTEGER NOT NULL, recorded_at INTEGER NOT NULL,
        PRIMARY KEY (org_id, session_id, manifest_key));
    `);
    // Added after the first production start: migrate in place.
    const cols = this.db.prepare("PRAGMA table_info(truncations)").all() as { name: string }[];
    if (!cols.some((c) => c.name === "max_chars"))
      try {
        this.db.exec("ALTER TABLE truncations ADD COLUMN max_chars INTEGER NOT NULL DEFAULT 0");
      } catch (error) {
        // The other process (exporter or erasure job) added it first.
        if (!/duplicate column/i.test(String((error as Error).message))) throw error;
      }
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS groups_last ON groups (last_ms DESC);
    `);
  }

  #tx(fn: () => void) {
    // IMMEDIATE: two processes share this file; a deferred read-then-write could
    // fail with SQLITE_BUSY_SNAPSHOT, which busy_timeout does not retry.
    this.db.exec("BEGIN IMMEDIATE");
    try {
      fn();
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  meta(k: string): string | undefined {
    return (this.db.prepare("SELECT v FROM meta WHERE k = ?").get(k) as { v: string } | undefined)?.v;
  }

  setMeta(k: string, v: string) {
    this.db.prepare("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v").run(k, v);
  }

  /** Record a sweep page of `session_revisions`. */
  seeRevisions(sweep: number, rows: { org_id: string; session_id: string; revision: string }[]) {
    const q = this.db.prepare(
      `INSERT INTO groups (org_id, session_id, revision, rev_sweep) VALUES (?, ?, ?, ?)
       ON CONFLICT (org_id, session_id) DO UPDATE SET revision = excluded.revision, rev_sweep = excluded.rev_sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, Number(r.revision), sweep);
    });
  }

  /** Record a sweep page of rollup activity. */
  seeActivity(sweep: number, rows: { org_id: string; session_id: string; last_ms: string; events: string }[], catalog = false) {
    // A catalog row only marks the group seen (and fills last_ms when the rollups gave none);
    // it never overwrites the rollups' activity and event count.
    const q = this.db.prepare(
      catalog
        ? `INSERT INTO groups (org_id, session_id, last_ms, events, act_sweep) VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (org_id, session_id) DO UPDATE SET
             last_ms = CASE WHEN groups.act_sweep = excluded.act_sweep THEN max(coalesce(groups.last_ms, 0), excluded.last_ms) ELSE excluded.last_ms END,
             events = CASE WHEN groups.act_sweep = excluded.act_sweep THEN groups.events ELSE excluded.events END,
             act_sweep = excluded.act_sweep`
        : `INSERT INTO groups (org_id, session_id, last_ms, events, act_sweep) VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (org_id, session_id) DO UPDATE SET last_ms = excluded.last_ms, events = excluded.events,
             act_sweep = excluded.act_sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, Number(r.last_ms), Number(r.events), sweep);
    });
  }

  /**
   * After complete sweeps of both sources: a group neither saw is gone from
   * Neon. Its row is dropped; its exported keys stay until the delete runs.
   */
  dropUnseen(revSweep: number, actSweep: number): number {
    return this.db
      .prepare("DELETE FROM groups WHERE rev_sweep < ? AND act_sweep < ?")
      .run(revSweep, actSweep).changes as number;
  }

  exported(org: string, session: string): ExportedRow | undefined {
    const row = this.db.prepare("SELECT * FROM exported WHERE org_id = ? AND session_id = ?").get(org, session) as
      | (Omit<ExportedRow, "keys"> & { keys: string })
      | undefined;
    return row ? { ...row, keys: JSON.parse(row.keys) as string[] } : undefined;
  }

  /** A key the exporter is about to write under, recorded before the write survives a crash. */
  recordPendingKey(org: string, session: string, key: string) {
    this.db.prepare("INSERT OR IGNORE INTO pending_keys (org_id, session_id, key) VALUES (?, ?, ?)").run(org, session, key);
  }

  /** On start: fold keys of runs that died mid-group into their group's watermark (as failed, to retry). */
  recoverPendingKeys(at = Date.now()): number {
    const rows = this.db.prepare("SELECT org_id, session_id, key FROM pending_keys").all() as { org_id: string; session_id: string; key: string }[];
    const groups = new Map<string, { org: string; session: string; keys: string[] }>();
    for (const r of rows) {
      const k = JSON.stringify([r.org_id, r.session_id]);
      const g = groups.get(k) ?? { org: r.org_id, session: r.session_id, keys: [] };
      g.keys.push(r.key);
      groups.set(k, g);
    }
    for (const g of groups.values()) this.markFailed(g.org, g.session, "interrupted", g.keys, -1, at);
    return groups.size;
  }

  markExported(org: string, session: string, revision: number, keys: string[], at = Date.now()) {
    this.db.prepare("DELETE FROM pending_keys WHERE org_id = ? AND session_id = ?").run(org, session);
    if (keys.length === 0 && !this.db.prepare("SELECT 1 FROM groups WHERE org_id = ? AND session_id = ?").get(org, session)) {
      this.db.prepare("DELETE FROM exported WHERE org_id = ? AND session_id = ?").run(org, session);
      return;
    }
    this.db
      .prepare(
        `INSERT INTO exported (org_id, session_id, revision, exported_at, keys, error, attempts, next_try)
         VALUES (?, ?, ?, ?, ?, NULL, 0, 0)
         ON CONFLICT (org_id, session_id) DO UPDATE SET revision = excluded.revision,
           exported_at = excluded.exported_at, keys = excluded.keys, error = NULL, attempts = 0, next_try = 0,
           failed_revision = NULL`,
      )
      .run(org, session, revision, at, JSON.stringify([...keys].sort()));
  }

  /**
   * A failed export keeps its old keys plus every key it may have written
   * before failing (so a later erasure still finds them), and is retried with
   * a backoff of at most 4 h, or at once when Neon's revision moves again.
   */
  markFailed(org: string, session: string, code: string, keys: string[], revision: number, at = Date.now()) {
    const prior = this.exported(org, session);
    // Waiting for the other process's lease, or an interrupted run, is not the group's failure.
    const transientWait = code === "prefix_busy" || code === "interrupted";
    const attempts = (prior?.attempts ?? 0) + (transientWait ? 0 : 1);
    const nextTry = at + (transientWait ? 2 * 60 * 1000 : Math.min(4, 2 ** Math.max(0, attempts - 1)) * 60 * 60 * 1000);
    const all = JSON.stringify([...new Set([...(prior?.keys ?? []), ...keys])].sort());
    this.db
      .prepare(
        `INSERT INTO exported (org_id, session_id, revision, exported_at, keys, error, attempts, next_try, failed_revision)
         VALUES (?, ?, -1, 0, ?, ?, ?, ?, ?)
         ON CONFLICT (org_id, session_id) DO UPDATE SET keys = excluded.keys, error = excluded.error,
           attempts = excluded.attempts, next_try = excluded.next_try, failed_revision = excluded.failed_revision`,
      )
      .run(org, session, all, code, attempts, nextTry, revision);
    this.db.prepare("DELETE FROM pending_keys WHERE org_id = ? AND session_id = ?").run(org, session);
  }

  /**
   * The truncation ledger: every exported manifest that holds a string cut to
   * the scrub bound, so it can be re-exported in full once the sealer takes
   * large strings. Replaced on each export of the group; T0 waits on it being empty.
   */
  recordTruncations(org: string, session: string, byKey: { key: string; strings: number; maxChars?: number }[], at = Date.now()) {
    this.#tx(() => {
      this.db.prepare("DELETE FROM truncations WHERE org_id = ? AND session_id = ?").run(org, session);
      const q = this.db.prepare(
        "INSERT INTO truncations (org_id, session_id, manifest_key, strings, recorded_at, max_chars) VALUES (?, ?, ?, ?, ?, ?)",
      );
      for (const t of byKey) if (t.strings > 0) q.run(org, session, t.key, t.strings, at, t.maxChars ?? 0);
    });
  }

  // ---------------------------------------------------------------- per-session leases

  /**
   * The exporter and the erasure job never work on one session prefix at the
   * same time: otherwise the eraser could delete a chunk the exporter has just
   * decided to reuse. A lease older than `ttlMs` (a crashed holder) is taken over.
   */
  acquireLease(prefix: string, holder: string, ttlMs = 2 * 60 * 60 * 1000, now = Date.now()): boolean {
    let ok = false;
    this.#tx(() => {
      const row = this.db.prepare("SELECT holder, at FROM leases WHERE prefix = ?").get(prefix) as
        | { holder: string; at: number }
        | undefined;
      if (row && row.holder !== holder && now - Number(row.at) < ttlMs) return;
      this.db
        .prepare("INSERT INTO leases (prefix, holder, at) VALUES (?, ?, ?) ON CONFLICT (prefix) DO UPDATE SET holder = excluded.holder, at = excluded.at")
        .run(prefix, holder, now);
      ok = true;
    });
    return ok;
  }

  /** Extend a lease this holder still has; false if it was taken over. */
  renewLease(prefix: string, holder: string, now = Date.now()): boolean {
    return Number(this.db.prepare("UPDATE leases SET at = ? WHERE prefix = ? AND holder = ?").run(now, prefix, holder).changes) === 1;
  }

  /** Leases of a holder kind other than `current` (a previous run of the same single process). */
  releaseStaleLeases(kind: string, current: string): number {
    return Number(this.db.prepare("DELETE FROM leases WHERE holder LIKE ? AND holder <> ?").run(`${kind}%`, current).changes);
  }

  releaseLeases(holder: string, prefixes: Iterable<string>) {
    const q = this.db.prepare("DELETE FROM leases WHERE prefix = ? AND holder = ?");
    this.#tx(() => {
      for (const p of prefixes) q.run(p, holder);
    });
  }

  /** Erasures the exporter has just made unnecessary: objects it references again, prefixes it writes again. */
  cancelErasures(keys: Iterable<string>) {
    const q = this.db.prepare("DELETE FROM erasures WHERE key = ?");
    this.#tx(() => {
      for (const k of keys) q.run(k);
    });
  }

  // ---------------------------------------------------------------- erasure queue

  /** Queue objects (or whole session prefixes) for the erasure job. Idempotent per key. */
  enqueueErasure(items: { key: string; kind: "object" | "prefix"; org_id: string; reason: string }[], at = Date.now()) {
    const q = this.db.prepare(
      `INSERT INTO erasures (key, kind, org_id, reason, enqueued_at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (key) DO UPDATE SET next_try = 0,
         kind = CASE WHEN excluded.kind = 'prefix' THEN 'prefix' ELSE erasures.kind END`,
    );
    this.#tx(() => {
      for (const i of items) q.run(i.key, i.kind, i.org_id, i.reason, at);
    });
  }

  nextErasures(now: number, limit: number): { key: string; kind: "object" | "prefix"; org_id: string; reason: string; enqueued_at: number }[] {
    // Prefixes first (whole sessions), then the oldest objects.
    return this.db
      .prepare(
        `SELECT key, kind, org_id, reason, enqueued_at FROM erasures WHERE next_try <= ?
          ORDER BY kind = 'prefix' DESC, enqueued_at LIMIT ?`,
      )
      .all(now, limit) as unknown as { key: string; kind: "object" | "prefix"; org_id: string; reason: string; enqueued_at: number }[];
  }

  erasureQueued(key: string, enqueuedAt: number): boolean {
    return !!this.db.prepare("SELECT 1 FROM erasures WHERE key = ? AND enqueued_at = ?").get(key, enqueuedAt);
  }

  erasureDone(key: string, enqueuedAt: number) {
    // Only if not re-queued meanwhile.
    this.db.prepare("DELETE FROM erasures WHERE key = ? AND enqueued_at = ?").run(key, enqueuedAt);
  }

  erasureFailed(key: string, code: string, at = Date.now()) {
    this.db
      .prepare("UPDATE erasures SET attempts = attempts + 1, error = ?, next_try = ? + min(3600000, 60000 * (1 << min(attempts, 6))) WHERE key = ?")
      .run(code, at, key);
  }

  erasureBacklog(now = Date.now()): { queued: number; oldestAgeH: number | null; failing: number } {
    const r = this.db.prepare("SELECT count(*) AS n, min(enqueued_at) AS o, count(error) AS f FROM erasures").get() as {
      n: number;
      o: number | null;
      f: number;
    };
    return { queued: Number(r.n), oldestAgeH: r.o === null ? null : +((now - Number(r.o)) / 3_600_000).toFixed(2), failing: Number(r.f) };
  }

  /** The erasure job's own rollup sweep: every group Neon still has activity for. */
  seeForErasure(sweep: number, rows: { org_id: string; session_id: string }[]) {
    const q = this.db.prepare(
      `INSERT INTO erasure_seen (org_id, session_id, sweep) VALUES (?, ?, ?)
       ON CONFLICT (org_id, session_id) DO UPDATE SET sweep = excluded.sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, sweep);
    });
  }

  /**
   * After a complete erasure sweep: exported groups Neon no longer has (the
   * exporter's units come only from rollups, so no rollup means nothing to keep).
   */
  vanishedExports(sweep: number, sweepStartedAt: number, prune = true): { org_id: string; session_id: string; keys: string[] }[] {
    // Only exports older than the sweep: a group created behind the sweep's cursor and
    // exported meanwhile is not missing from Neon, only from this sweep.
    const rows = this.db
      .prepare(
        `SELECT e.org_id, e.session_id, e.keys FROM exported e
          LEFT JOIN erasure_seen s ON s.org_id = e.org_id AND s.session_id = e.session_id AND s.sweep = ?
          WHERE s.org_id IS NULL AND e.keys <> '[]' AND e.exported_at < ?
            AND NOT EXISTS (SELECT 1 FROM pending_keys p WHERE p.org_id = e.org_id AND p.session_id = e.session_id)`,
      )
      .all(sweep, sweepStartedAt) as { org_id: string; session_id: string; keys: string }[];
    if (prune) this.db.prepare("DELETE FROM erasure_seen WHERE sweep < ?").run(sweep);
    return rows.map((r) => ({ ...r, keys: JSON.parse(r.keys) as string[] }));
  }

  /** A group whose session prefixes are queued for erasure forgets them. */
  forgetKeys(org: string, session: string, keys: string[]) {
    this.#tx(() => {
      const prior = this.exported(org, session);
      if (!prior) return;
      const left = prior.keys.filter((k) => !keys.includes(k));
      this.db.prepare("UPDATE exported SET keys = ? WHERE org_id = ? AND session_id = ?").run(JSON.stringify(left), org, session);
    });
  }

  /** Exported groups stuck in error while they still own keys (an SLA risk the queue age does not show). */
  stuckWithKeys(): number {
    return Number((this.db.prepare("SELECT count(*) AS n FROM exported WHERE error IS NOT NULL AND keys <> '[]' AND attempts >= 6").get() as { n: number }).n);
  }

  /**
   * After the sealer takes long strings (scrub rule set 2): make every group in
   * the truncation ledger look changed, so it is re-read and stored whole. The
   * ledger rows clear as each group re-exports without a cut.
   */
  forceReexportTruncated(): number {
    let n = 0;
    this.#tx(() => {
      // Healthy groups: look changed. Failing ones (even past the attempt cap):
      // retry now, so no ledgered group is left holding T0 shut.
      n =
        Number(
          this.db
            .prepare(
              `UPDATE exported SET revision = -2, exported_at = 0
                WHERE error IS NULL AND (org_id, session_id) IN (SELECT DISTINCT org_id, session_id FROM truncations)`,
            )
            .run().changes,
        ) +
        Number(
          this.db
            .prepare(
              `UPDATE exported SET failed_revision = -2, attempts = 0, next_try = 0
                WHERE error IS NOT NULL AND (org_id, session_id) IN (SELECT DISTINCT org_id, session_id FROM truncations)`,
            )
            .run().changes,
        );
    });
    return n;
  }

  truncationTotals(): { sessions: number; manifests: number; strings: number; maxChars?: number } {
    const r = this.db
      .prepare(
        "SELECT count(DISTINCT org_id || '/' || session_id) AS s, count(*) AS m, coalesce(sum(strings), 0) AS n, coalesce(max(max_chars), 0) AS x FROM truncations",
      )
      .get() as { s: number; m: number; n: number; x: number };
    return { sessions: Number(r.s), manifests: Number(r.m), strings: Number(r.n), ...(Number(r.x) ? { maxChars: Number(r.x) } : {}) };
  }

  /**
   * A deletion fence (tombstone or reset) changed without bumping the
   * session's revision: make the exported group look changed, past its debounce.
   */
  forceChanged(groups: { org_id: string; session_id: string }[]): number {
    const ok = this.db.prepare(
      `UPDATE exported SET revision = -2, exported_at = 0
        WHERE org_id = ? AND session_id = ? AND error IS NULL AND keys <> '[]' AND revision <> -2`,
    );
    // A failed group past its attempt cap is revived (once per cap); others
    // keep their backoff, which already retries them.
    const failed = this.db.prepare(
      `UPDATE exported SET failed_revision = -2, attempts = 0, next_try = 0
        WHERE org_id = ? AND session_id = ? AND error IS NOT NULL AND keys <> '[]'
          AND attempts >= 6`,
    );
    let n = 0;
    this.#tx(() => {
      for (const g of groups)
        n += Number(ok.run(g.org_id, g.session_id).changes) + Number(failed.run(g.org_id, g.session_id).changes);
    });
    return n;
  }

  /**
   * The next groups to export, in priority order:
   * (A group gone from Neon is the erasure job's, not the exporter's.)
   *  1. changed since their export and past their debounce (bigger sessions wait longer, at most 24 h);
   *  2. never exported, active in the last `recentMs`, newest first;
   *  3. never exported, the rest of history, newest first;
   *  4. failed ones due for a retry, or whose revision moved since;
   *  5. when nothing else waits, exports older than `reverifyMs`, oldest first.
   */
  nextWork(now: number, recentMs: number, limit: number, reverifyMs = 0): WorkItem[] {
    const out: WorkItem[] = [];
    const take = (sql: string, kind: WorkItem["kind"], ...params: (number | string)[]) => {
      if (out.length >= limit) return;
      const rows = this.db.prepare(sql).all(...params, limit - out.length) as Omit<WorkItem, "kind">[];
      for (const r of rows) out.push({ ...r, kind });
    };
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
        WHERE e.error IS NULL AND g.revision <> e.revision
          AND e.exported_at + min(86400000, 1800000 + g.events * 600) <= ?
        ORDER BY g.last_ms DESC NULLS FIRST LIMIT ?`,
      "changed",
      now,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g
        LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND (g.last_ms IS NULL OR g.last_ms >= ?)
        ORDER BY g.last_ms DESC NULLS LAST LIMIT ?`,
      "recent",
      now - recentMs,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g
        LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND g.last_ms < ? ORDER BY g.last_ms DESC LIMIT ?`,
      "history",
      now - recentMs,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
        WHERE e.error IS NOT NULL
          AND ((e.next_try <= ? AND e.attempts < 6) OR g.revision <> coalesce(e.failed_revision, g.revision))
        ORDER BY e.next_try LIMIT ?`,
      "retry",
      now,
    );
    // Last: a rolling re-read of old exports, for changes Neon records without a
    // revision bump (delivery records that never project to an event). Only when
    // nothing else is waiting; a re-read of an unchanged session writes nothing.
    if (out.length === 0 && reverifyMs > 0)
      take(
        `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
          WHERE e.error IS NULL AND e.exported_at < ? ORDER BY e.exported_at LIMIT ?`,
        "reverify",
        now - reverifyMs,
      );
    // A group can match two queries only across them (e.g. never twice); dedupe defensively.
    const seen = new Set<string>();
    return out.filter((w) => {
      const k = `${w.org_id}\u0000${w.session_id}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  backlog(now: number, recentMs: number): Backlog {
    const one = (sql: string, ...p: number[]) =>
      Object.values(this.db.prepare(sql).get(...p) as Record<string, number>).map((v) => Number(v ?? 0));
    const [groups, exported] = one(
      `SELECT (SELECT count(*) FROM groups), (SELECT count(*) FROM exported WHERE error IS NULL)`,
    );
    const [pending, pendingEvents] = one(
      `SELECT count(*), sum(g.events) FROM groups g LEFT JOIN exported e USING (org_id, session_id) WHERE e.org_id IS NULL`,
    );
    const [recentPending, recentPendingEvents] = one(
      `SELECT count(*), sum(g.events) FROM groups g LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND (g.last_ms IS NULL OR g.last_ms >= ?)`,
      now - recentMs,
    );
    const [changed] = one(
      `SELECT count(*) FROM groups g JOIN exported e USING (org_id, session_id) WHERE e.error IS NULL AND g.revision <> e.revision`,
    );
    const [errored] = one(`SELECT count(*) FROM exported WHERE error IS NOT NULL`);
    const [pendingDeletes] = one(
      `SELECT count(*) FROM exported e LEFT JOIN groups g USING (org_id, session_id) WHERE g.org_id IS NULL AND e.keys <> '[]'`,
    );
    return {
      groups: groups!,
      exported: exported!,
      pending: pending!,
      pendingEvents: pendingEvents!,
      recentPending: recentPending!,
      recentPendingEvents: recentPendingEvents!,
      changed: changed!,
      errored: errored!,
      pendingDeletes: pendingDeletes!,
    };
  }

  errorCounts(): Record<string, number> {
    const rows = this.db.prepare("SELECT error, count(*) AS n FROM exported WHERE error IS NOT NULL GROUP BY error").all() as {
      error: string;
      n: number;
    }[];
    return Object.fromEntries(rows.map((r) => [r.error, r.n]));
  }

  close() {
    this.db.close();
  }
}
