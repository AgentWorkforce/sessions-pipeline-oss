/**
 * `neon-s3-erasure`: the only process that deletes from the transcript
 * bucket, and the only one holding the eraser key. The exporter
 * (`../neon-s3-sync`) is write-only.
 *
 * Within the 48 h erasure SLA it removes, by version id (permanently):
 *  - whole session prefixes of groups Neon no longer has: its own sweep of the
 *    rollups (the exporter's units come only from rollups, so a group with no
 *    rollup has nothing left to keep), every 10 min;
 *  - whole session prefixes of groups a deletion fence covers
 *    (`deleted_upload_sessions`, `session_resets`), every 10 min;
 *  - what the exporter queued: objects a rewritten manifest no longer
 *    references, and session prefixes it no longer produces.
 *
 * It reads Neon (read-only) and the shared watermark, never object content.
 * A session prefix is leased while it is erased, so the exporter never reuses
 * a chunk the eraser is deleting.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { orgPrefix } from "../neon-s3-sync/export-session.ts";
import { activityPage, catalogPage, fencedGroups, fencedLivePage, groupExistsInNeon, lockWaiters, type LiveFencedGroup } from "../neon-s3-sync/neon-reader.ts";
import { liveObjectPrefix, livePointerPrefix, liveSessionsIn, liveStagedPrefix, liveTargets } from "./live.ts";

/** Clock-skew margin between this host and S3 `LastModified`. */
export const LIVE_SKEW_MS = 2 * 60 * 1000;
/**
 * A staged batch (session ingest) holds several sessions, and its fan-out may
 * still be retrying or waiting for a DLQ redrive (runbooks/session-ingest-rollout.md:
 * redrive within 24 h). When a session of its (workspace, user, origin) is
 * tombstoned, its staged objects older than this are erased; younger ones on a
 * later sweep. The session's own manifests and chunks are erased at once, as before.
 */
export const STAGED_ERASE_AFTER_MS = 48 * 60 * 60 * 1000;

const DOT_SEGMENT = /(^|\/)\.{1,2}(\/|$)/;
import type { SyncState } from "../neon-s3-sync/state.ts";
import { errorCode, type Log } from "../neon-s3-sync/sync.ts";
import type { Eraser } from "./eraser.ts";

export interface ErasureConfig {
  keyRoot: string;
  sweepEveryMs: number;
  idleSleepMs: number;
  progressEveryMs: number;
  /** Logged as a breach when the oldest queued erasure is older than this. */
  slaWarnMs: number;
  /**
   * Whether the existence guard also counts legacy-only evidence (conversation
   * turns, links, outcomes). `S3X_ERASER_RECOGNIZES_LEGACY`; enable before the
   * exporter's `S3X_INCLUDE_LEGACY_SOURCES`.
   */
  recognizeLegacy: boolean;
}

export const DEFAULT_ERASURE_CONFIG: ErasureConfig = {
  keyRoot: "96bulls",
  sweepEveryMs: 10 * 60 * 1000,
  idleSleepMs: 30 * 1000,
  progressEveryMs: 60 * 1000,
  slaWarnMs: 24 * 60 * 60 * 1000,
  recognizeLegacy: false,
};

const SESSION_PREFIX = /^[^/]+\/[^/]+\/v1\/ws=[^/]+\/m=[^/]+\/h=[^/]+\/d=\d{4}\/\d{2}\/\d{2}\/s=[^/]+\/$/;
const LEASE_HOLDER = `eraser:${process.pid}`;

/** The session prefix a queued key belongs to, if the key has this layout under the org's prefix. */
export function sessionPrefixOf(keyRoot: string, orgId: string, key: string): string | undefined {
  const root = `${orgPrefix(keyRoot, orgId)}/v1/`;
  if (!key.startsWith(root) || DOT_SEGMENT.test(key)) return undefined;
  const m = /^(.*?\/s=[^/]+\/)/.exec(key);
  return m && SESSION_PREFIX.test(m[1]!) ? m[1] : undefined;
}

export class ErasureJob {
  readonly counters = { swept: 0, prefixesErased: 0, objectsErased: 0, versionsDeleted: 0, failed: 0, refused: 0, deferred: 0, fenceSweepFailed: 0, liveVersionsDeleted: 0, liveFenceFailed: 0 };
  #lastSweep = 0;
  #lastProgress = 0;

  constructor(
    readonly db: Queryable,
    readonly state: SyncState,
    readonly eraser: Eraser,
    readonly log: Log,
    readonly cfg: ErasureConfig = DEFAULT_ERASURE_CONFIG,
    readonly now: () => number = Date.now,
    readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
    readonly stopped: () => boolean = () => false,
  ) {}

  /** Queue the session prefixes of `keys` (manifest keys) and drop them from the group's watermark. */
  #queueGroup(org: string, session: string, keys: string[], reason: string) {
    this.state.enqueueErasure(
      keys.map((k) => ({ key: k.slice(0, -"manifest.json".length), kind: "prefix" as const, org_id: org, reason })),
      this.now(),
    );
    this.state.forgetKeys(org, session, keys);
  }

  /** With `dryRun`, reports what it would erase and changes nothing. */
  async sweep(opts: { dryRun?: boolean } = {}) {
    const startedAt = this.now();
    const waiters = await lockWaiters(this.db);
    if (waiters.nonAdvisory > 3 || waiters.total > 25) {
      this.log({ message: "sweep deferred: lock waiters", lock_waiters: waiters.total });
      return;
    }
    // Live (srv_) erasure first and on its own: it reads only tombstones, so
    // a failure or an early return in the rollup and catalog walks below
    // must not hold it back.
    const live = await this.#eraseLiveFenced(opts.dryRun ?? false).catch((error) => {
      this.counters.liveFenceFailed += 1;
      this.log({ message: "live fence sweep failed", error_code: errorCode(error) });
      return { groups: 0, versions: 0 };
    });
    const id = Number(this.state.meta("erasure_sweep_done") ?? 0) + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let seen = 0;
    for (;;) {
      const rows = await activityPage(this.db, after);
      // A dry run marks under a sweep id no real sweep uses (and the next real sweep clears).
      this.state.seeForErasure(opts.dryRun ? -id : id, rows);
      seen += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    // Catalog sessions count as present too: the exporter exports them.
    let cat: Parameters<typeof catalogPage>[1] = null;
    for (;;) {
      if (this.stopped()) break;
      const rows = await catalogPage(this.db, cat);
      this.state.seeForErasure(opts.dryRun ? -id : id, rows);
      seen += rows.length;
      if (rows.length === 0) break;
      cat = rows.at(-1)!;
    }
    if (this.stopped()) return;
    // A sweep that saw nothing at all is a broken read, not an empty Neon: never erase on it.
    if (seen === 0) {
      this.log({ message: "sweep saw no rollups; nothing erased" });
      return;
    }
    if (!opts.dryRun) this.state.setMeta("erasure_sweep_done", String(id));
    let gone = 0;
    for (const g of this.state.vanishedExports(opts.dryRun ? -id : id, startedAt, !opts.dryRun)) {
      // Confirm on Neon right before erasing: no rollup and no catalog row now
      // (and, with recognizeLegacy, no legacy turn, link or outcome row).
      if (await groupExistsInNeon(this.db, g.org_id, g.session_id, this.cfg.recognizeLegacy)) continue;
      gone += 1;
      if (!opts.dryRun) this.#queueGroup(g.org_id, g.session_id, g.keys, "gone_from_neon");
    }
    let fenced = 0;
    try {
      for (const g of await fencedGroups(this.db)) {
        const row = this.state.exported(g.org_id, g.session_id);
        if (row?.keys.length) {
          if (!opts.dryRun) this.#queueGroup(g.org_id, g.session_id, row.keys, "fenced");
          fenced += 1;
        }
      }
    } catch (error) {
      this.counters.fenceSweepFailed += 1;
      this.log({ message: "fence sweep failed", error_code: errorCode(error) });
    }
    this.counters.swept += 1;
    if (!opts.dryRun) this.state.setMeta("erasure_sweep_at", String(this.now()));
    this.log({ message: opts.dryRun ? "erasure sweep (dry run)" : "erasure sweep", sweep: id, count: seen, gone, fenced, live_fenced: live.groups, live_versions: live.versions });
  }

  /**
   * Tombstoned sessions written by relayhistory-api's live writer (live.ts),
   * erased directly on every sweep, not queued: the exporter never writes
   * or leases `m=srv_` prefixes.
   *
   * Cost: tombstones are paged by primary key (no cap) and grouped by
   * (workspace, user, origin); each group's objects and pointers are listed
   * once (two list calls), and only tombstones with a directory or a pointer
   * left are visited. A tombstone with nothing live (most, and every fully
   * erased one) costs a share of those two calls.
   *
   * Re-enable race: a page can be minutes old by the time a row is reached,
   * and a re-enabled session may already be re-uploading. Each tombstone is
   * re-read right before its erasure, and only versions last modified before
   * that read (less LIVE_SKEW_MS for clock skew) are erased. Anything newer is
   * kept, and so is the pointer while anything is kept, so the next sweep
   * finds the session again.
   */
  async #eraseLiveFenced(dryRun: boolean): Promise<{ groups: number; versions: number }> {
    let groups = 0;
    let versions = 0;
    let after: LiveFencedGroup | null = null;
    for (;;) {
      if (this.stopped()) break;
      let rows: LiveFencedGroup[];
      try {
        rows = await fencedLivePage(this.db, after);
      } catch (error) {
        this.counters.liveFenceFailed += 1;
        this.log({ message: "live fence sweep failed", error_code: errorCode(error) });
        break;
      }
      if (rows.length === 0) break;
      after = rows.at(-1)!;
      // Per (workspace, user, origin), listed once per page: the session
      // directories that hold any object, and the pointers. A tombstone is
      // visited while either remains, so neither a directory left by a
      // re-upload nor an orphaned pointer is ever skipped for good.
      const listed = new Map<string, { dirs: Set<string>; pointers: Set<string>; staged: string[] } | null>();
      for (const g of rows) {
        if (this.stopped()) break;
        let t;
        let objects: string;
        let pointerRoot: string;
        try {
          t = liveTargets(this.cfg.keyRoot, g);
          objects = liveObjectPrefix(this.cfg.keyRoot, g);
          pointerRoot = livePointerPrefix(this.cfg.keyRoot, g);
        } catch {
          // An id that is not one safe path segment was never written either.
          this.counters.refused += 1;
          continue;
        }
        // A failed listing is remembered for the page (null), so the group's
        // other tombstones wait for the next sweep instead of each retrying it.
        if (listed.has(objects) && listed.get(objects) === null) continue;
        let seen = listed.get(objects);
        try {
          if (!seen) {
            seen = {
              dirs: liveSessionsIn(objects, await this.eraser.listKeys(objects)),
              pointers: new Set(await this.eraser.listKeys(pointerRoot)),
              // The session ingest's staged batches: a tombstone is revisited while any remain.
              staged: (await this.eraser.listKeys(liveStagedPrefix(this.cfg.keyRoot, g))).filter((k) => k.endsWith(".staged.ndjson.zst")),
            };
            listed.set(objects, seen);
          }
        } catch (error) {
          listed.set(objects, null);
          this.counters.liveFenceFailed += 1;
          this.log({ message: "live listing failed", error_code: errorCode(error) });
          continue;
        }
        const dirKey = `${t.base.slice(objects.length).replace(/\/$/, "")}/${t.sessionSegment}`;
        if (!seen.dirs.has(dirKey) && !seen.pointers.has(t.pointerKey) && seen.staged.length === 0) continue;
        groups += 1;
        if (dryRun) continue;
        try {
          const checkedAt = this.now();
          const { rows: still } = await this.db.query(
            `SELECT 1 FROM sessions.deleted_upload_sessions
              WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3 AND origin_id = $4 AND source = $5 AND session_id = $6`,
            [g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id],
          );
          if (still.length === 0) continue; // re-enabled since the page was read
          const before = new Date(checkedAt - LIVE_SKEW_MS);
          // The session ingest's staged copies of this (workspace, user, origin)'s batches,
          // once past the fan-out redrive window. Shared by the group's tombstones: once a page.
          const stagedBefore = new Date(checkedAt - STAGED_ERASE_AFTER_MS);
          const stagedLeft: string[] = [];
          for (const key of seen.staged) {
            const st = await this.eraser.eraseObjectBefore(key, stagedBefore);
            versions += st.deleted;
            this.counters.liveVersionsDeleted += st.deleted;
            if (st.kept > 0) stagedLeft.push(key);
          }
          seen.staged = stagedLeft;
          const dir = await this.eraser.eraseSessionAnyDate(t.base, t.sessionSegment, before);
          versions += dir.deleted;
          this.counters.liveVersionsDeleted += dir.deleted;
          if (dir.kept > 0) continue; // the next sweep comes back for the rest
          const ptr = await this.eraser.eraseObjectBefore(t.pointerKey, before);
          versions += ptr.deleted;
          this.counters.liveVersionsDeleted += ptr.deleted;
        } catch (error) {
          this.counters.liveFenceFailed += 1;
          this.log({ message: "live erasure failed", error_code: errorCode(error) });
        }
      }
    }
    return { groups, versions };
  }

  /** Erase what is queued and due. Returns how many items it erased or failed (not deferred). */
  async drain(limit = 100): Promise<number> {
    const items = this.state.nextErasures(this.now(), limit);
    let handled = 0;
    for (const item of items) {
      if (this.stopped()) break;
      const prefix =
        item.kind === "prefix"
          ? SESSION_PREFIX.test(item.key) && item.key.startsWith(`${orgPrefix(this.cfg.keyRoot, item.org_id)}/v1/`)
            ? item.key
            : undefined
          : sessionPrefixOf(this.cfg.keyRoot, item.org_id, item.key);
      try {
        if (!prefix) {
          // Never delete outside a session prefix of the item's own org.
          this.counters.refused += 1;
          this.state.erasureFailed(item.key, "erasure_refused_shape", this.now());
          handled += 1;
          continue;
        }
        if (!this.state.acquireLease(prefix, LEASE_HOLDER, undefined, this.now())) {
          this.counters.deferred += 1;
          continue;
        }
      } catch (error) {
        this.log({ message: "erasure state busy", error_code: errorCode(error) });
        continue;
      }
      try {
        // The exporter may have cancelled it before we took the lease.
        if (!this.state.erasureQueued(item.key, item.enqueued_at)) continue;
        const n = item.kind === "prefix" ? await this.eraser.erasePrefix(item.key) : await this.eraser.eraseObject(item.key);
        this.counters.versionsDeleted += n;
        if (item.kind === "prefix") this.counters.prefixesErased += 1;
        else this.counters.objectsErased += 1;
        this.state.erasureDone(item.key, item.enqueued_at);
        handled += 1;
      } catch (error) {
        this.counters.failed += 1;
        this.state.erasureFailed(item.key, errorCode(error), this.now());
        this.log({ message: "erasure failed", error_code: errorCode(error), kind: item.kind });
        handled += 1;
      } finally {
        try {
          this.state.releaseLeases(LEASE_HOLDER, [prefix]);
        } catch {}
      }
    }
    return handled;
  }

  progress(force = false) {
    const now = this.now();
    if (!force && now - this.#lastProgress < this.cfg.progressEveryMs) return;
    this.#lastProgress = now;
    const queue = this.state.erasureBacklog(now);
    const lastSweep = Number(this.state.meta("erasure_sweep_at") ?? 0);
    const sweepAgeH = lastSweep ? +((now - lastSweep) / 3_600_000).toFixed(2) : null;
    const stuck = this.state.stuckWithKeys();
    const breach =
      (queue.oldestAgeH !== null && queue.oldestAgeH * 3_600_000 > this.cfg.slaWarnMs) ||
      (sweepAgeH !== null && sweepAgeH > 6) ||
      stuck > 0;
    this.log({ message: breach ? "erasure SLA at risk" : "progress", ...this.counters, queue, sweep_age_h: sweepAgeH, stuck_with_keys: stuck });
  }

  async run(opts: { once?: boolean } = {}) {
    this.state.releaseStaleLeases("eraser:", LEASE_HOLDER);
    while (!this.stopped()) {
      if (this.now() - this.#lastSweep >= this.cfg.sweepEveryMs) {
        this.#lastSweep = this.now();
        await this.sweep();
      }
      const handled = await this.drain();
      this.progress();
      if (handled === 0) {
        if (opts.once) break;
        await this.sleep(this.cfg.idleSleepMs);
      }
    }
    this.progress(true);
  }
}
