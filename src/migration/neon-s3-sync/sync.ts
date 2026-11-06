/**
 * The continuous loop: sweep Neon for changes, pick the next groups by
 * priority (erasure, changed, last 14 days newest first, then history newest
 * first), export them, and log progress as counts only.
 */
import { withLegacySource, includeLegacyUnits, legacyActivityPage } from "./legacy-source.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { erasableByExporter, exportUnit, type ExportTarget } from "./export-session.ts";
import {
  activityPage,
  catalogPage,
  fencedGroups,
  lockWaiters,
  neonUnitSource,
  revisionsPage,
  sessionUnits,
  type PageHook,
  type UnitSource,
} from "./neon-reader.ts";
import type { SyncState, WorkItem } from "./state.ts";
import type { Throttle } from "./throttle.ts";

export interface LockGuard {
  maxNonAdvisory: number;
  maxTotal: number;
  pauseMs: number;
  checkEveryMs: number;
}

export interface SyncConfig {
  /** Explicit dormant opt-in; includes legacy evidence and completeness records. */
  includeLegacySources?: boolean;
  recentMs: number;
  revSweepEveryMs: number;
  actSweepEveryMs: number;
  progressEveryMs: number;
  idleSleepMs: number;
  /** Re-read exports older than this when idle (0 = never). */
  reverifyMs: number;
  lockGuard: LockGuard;
}

export const DEFAULT_SYNC_CONFIG: SyncConfig = {
  recentMs: 14 * 24 * 60 * 60 * 1000,
  revSweepEveryMs: 2 * 60 * 1000,
  actSweepEveryMs: 30 * 60 * 1000,
  progressEveryMs: 60 * 1000,
  idleSleepMs: 30 * 1000,
  reverifyMs: 14 * 24 * 60 * 60 * 1000,
  lockGuard: { maxNonAdvisory: 3, maxTotal: 25, pauseMs: 2 * 60 * 1000, checkEveryMs: 30 * 1000 },
};

export type Log = (fields: Record<string, unknown>) => void;

const LEASE_HOLDER = `exporter:${process.pid}`;

/** Error code only: a Postgres or store error message can quote data. */
export function errorCode(error: unknown): string {
  const e = error as { code?: unknown; name?: unknown } | null;
  if (e && typeof e.code === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(e.code)) return e.code;
  if (e && typeof e.name === "string" && /^[A-Za-z0-9_]{1,64}$/.test(e.name)) return e.name;
  return "unknown";
}

export interface Counters {
  rows: number;
  bytes: number;
  groups: number;
  groupsFailed: number;
  manifestsWritten: number;
  manifestsUnchanged: number;
  chunksSealed: number;
  chunksReused: number;
  erasuresQueued: number;
  rawBytes: number;
  storedBytesWritten: number;
  eventsDone: number;
  lockPauses: number;
  fenced: number;
  truncatedStrings: number;
}

export class Syncer {
  readonly counters: Counters = {
    rows: 0,
    bytes: 0,
    groups: 0,
    groupsFailed: 0,
    manifestsWritten: 0,
    manifestsUnchanged: 0,
    chunksSealed: 0,
    chunksReused: 0,
    erasuresQueued: 0,
    rawBytes: 0,
    storedBytesWritten: 0,
    eventsDone: 0,
    lockPauses: 0,
    fenced: 0,
    truncatedStrings: 0,
  };
  readonly started: number;
  #lastLockCheck = 0;
  #lastProgress = 0;
  readonly source: UnitSource;
  readonly hook: PageHook;
  /** Extra read lanes (own connections) that export groups in parallel. */
  readonly lanes: { db: Queryable; source: UnitSource }[];

  constructor(
    readonly db: Queryable,
    readonly state: SyncState,
    readonly target: ExportTarget,
    readonly throttle: Throttle,
    readonly log: Log,
    readonly cfg: SyncConfig = DEFAULT_SYNC_CONFIG,
    readonly stopped: () => boolean = () => false,
    sourceFor: (db: Queryable, hook: PageHook) => UnitSource = neonUnitSource,
    extraDbs: Queryable[] = [],
  ) {
    this.started = throttle.now();
    this.hook = async (rows, bytes) => {
      // Stop promptly: a large session would otherwise hold shutdown for many
      // minutes. Its keys are already recorded and it retries in 2 min.
      if (this.stopped()) throw Object.assign(new Error("stopping"), { code: "interrupted" });
      this.counters.rows += rows;
      this.counters.bytes += bytes;
      await this.throttle.take(rows, bytes);
      await this.guardLocks();
    };
    const makeSource = (d: Queryable) => {
      const source = sourceFor(d, this.hook);
      return this.cfg.includeLegacySources ? withLegacySource(d, source, this.hook) : source;
    };
    this.source = makeSource(db);
    this.lanes = [{ db, source: this.source }, ...extraDbs.map((d) => ({ db: d, source: makeSource(d) }))];
  }

  /** Pause while Neon shows lock waiters beyond the guard (another backfill, a convoy). */
  #pause: Promise<void> | undefined;

  async guardLocks(force = false) {
    // Every lane waits out one pause together.
    if (this.#pause) return this.#pause;
    const g = this.cfg.lockGuard;
    const now = this.throttle.now();
    if (!force && now - this.#lastLockCheck < g.checkEveryMs) return;
    this.#lastLockCheck = now;
    this.#pause = this.#waitForLocks(g).finally(() => (this.#pause = undefined));
    return this.#pause;
  }

  async #waitForLocks(g: LockGuard) {
    for (;;) {
      const w = await lockWaiters(this.db);
      if (w.nonAdvisory <= g.maxNonAdvisory && w.total <= g.maxTotal) return;
      this.counters.lockPauses += 1;
      this.log({ message: "paused: lock waiters", lock_waiters: w.total, non_advisory: w.nonAdvisory, pause_ms: g.pauseMs });
      if (this.stopped()) return;
      await this.throttle.sleep(g.pauseMs);
    }
  }

  #sweepId(k: string) {
    return Number(this.state.meta(k) ?? 0);
  }

  async sweepRevisions() {
    const id = this.#sweepId("rev_sweep_done") + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let n = 0;
    for (;;) {
      const rows = await revisionsPage(this.db, after);
      // A sweep page is a narrow index-only read: weigh it lightly against the row budget.
      await this.hook(Math.ceil(rows.length / 50), 0);
      this.state.seeRevisions(id, rows);
      n += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    if (this.stopped()) return;
    // Fences change without a revision bump: re-export (erase) what they now cover.
    // A failure here must not stop the sweep: change-driven work also checks the fence itself.
    try {
      const forced = this.state.forceChanged(await fencedGroups(this.db));
      if (forced) this.log({ message: "fenced after export", count: forced });
    } catch (error) {
      this.log({ message: "fence sweep failed", error_code: errorCode(error) });
    }
    this.state.setMeta("rev_sweep_done", String(id));
    this.state.setMeta("rev_sweep_at", String(this.throttle.now()));
    this.#dropUnseen();
    this.log({ message: "revision sweep", sweep: id, count: n });
  }

  async sweepActivity() {
    const id = this.#sweepId("act_sweep_done") + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let n = 0;
    for (;;) {
      const rows = await activityPage(this.db, after);
      await this.hook(Math.ceil(rows.length / 50), 0);
      this.state.seeActivity(id, rows);
      n += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    // Sessions with delivery records but no rollup (no projected events yet).
    let cat: Parameters<typeof catalogPage>[1] = null;
    for (;;) {
      if (this.stopped()) break;
      const rows = await catalogPage(this.db, cat);
      await this.hook(Math.ceil(rows.length / 50), 0);
      this.state.seeActivity(id, rows, true);
      n += rows.length;
      if (rows.length === 0) break;
      cat = rows.at(-1)!;
    }
    if (this.cfg.includeLegacySources) {
      let legacy: Parameters<typeof legacyActivityPage>[1] = null;
      for (;;) {
        if (this.stopped()) break;
        const rows = await legacyActivityPage(this.db, legacy);
        await this.hook(Math.ceil(rows.length / 50), 0);
        this.state.seeActivity(id, rows, true);
        // Legacy updates do not universally bump session_revisions; force a reread.
        this.state.forceChanged(rows);
        n += rows.length;
        if (!rows.length) break;
        legacy = rows.at(-1)!;
      }
    }
    if (this.stopped()) return;
    this.state.setMeta("act_sweep_done", String(id));
    this.state.setMeta("act_sweep_at", String(this.throttle.now()));
    this.#dropUnseen();
    this.log({ message: "activity sweep", sweep: id, count: n });
  }

  #dropUnseen() {
    const r = this.#sweepId("rev_sweep_done");
    const a = this.#sweepId("act_sweep_done");
    if (r > 0 && a > 0) {
      const dropped = this.state.dropUnseen(r, a);
      if (dropped) this.log({ message: "gone from neon", count: dropped });
    }
  }

  async sweepsIfDue() {
    const now = this.throttle.now();
    if (now - Number(this.state.meta("act_sweep_at") ?? 0) >= this.cfg.actSweepEveryMs) await this.sweepActivity();
    if (now - Number(this.state.meta("rev_sweep_at") ?? 0) >= this.cfg.revSweepEveryMs) await this.sweepRevisions();
  }

  /** Export (or erase) one `(org, session)` group. Never throws. */
  async processGroup(work: WorkItem, lane = this.lanes[0]!): Promise<boolean> {
    const prior = this.state.exported(work.org_id, work.session_id);
    const t0 = this.throttle.now();
    // Every key this run may write under, recorded (and leased) before the write.
    const touched = new Set<string>();
    const leased: string[] = [];
    const referenced = new Set<string>();
    try {
      const keys: string[] = [];
      const truncations: { key: string; strings: number; maxChars: number }[] = [];
      const erase: { key: string; kind: "object" | "prefix"; org_id: string; reason: string }[] = [];
      const currentUnits = await sessionUnits(lane.db, work.org_id, work.session_id);
      const units = this.cfg.includeLegacySources
        ? await includeLegacyUnits(lane.db, work.org_id, work.session_id, currentUnits)
        : currentUnits;
      await this.hook(units.length, 0);
      for (const unit of units) {
        const target = {
          ...this.target,
          renew: (k: string) => this.state.renewLease(k.slice(0, -"manifest.json".length), LEASE_HOLDER),
        };
        const r = await exportUnit(target, unit, lane.source, {
          // Work driven by a change must see a fence written with that change.
          freshFence: work.kind !== "recent" && work.kind !== "history",
          onLease: (k) => {
            const prefix = k.slice(0, -"manifest.json".length);
            if (!this.state.acquireLease(prefix, LEASE_HOLDER))
              throw Object.assign(new Error("the erasure job holds this session"), { code: "prefix_busy" });
            leased.push(prefix);
          },
          onKey: (k) => {
            touched.add(k);
            this.state.recordPendingKey(work.org_id, work.session_id, k);
          },
        });
        if (r.fenced) this.counters.fenced += 1;
        for (const m of r.manifests) {
          keys.push(m.manifestKey);
          truncations.push({ key: m.manifestKey, strings: m.truncatedStrings, maxChars: m.truncatedMaxChars });
          if (m.written) this.counters.manifestsWritten += 1;
          else this.counters.manifestsUnchanged += 1;
          this.counters.chunksSealed += m.chunksSealed;
          this.counters.chunksReused += m.chunksReused;
          this.counters.rawBytes += m.rawBytes;
          this.counters.truncatedStrings += m.truncatedStrings;
          if (m.chunksSealed) this.counters.storedBytesWritten += m.storedBytes;
          for (const k of m.referenced) referenced.add(k);
          for (const o of m.orphans) erase.push({ key: o, kind: "object", org_id: work.org_id, reason: "unreferenced" });
        }
      }
      // Session prefixes this group no longer produces (fenced, emptied, moved
      // to another date or machine): the whole prefix goes to the erasure job,
      // unless the manifest there is no longer this exporter's.
      const keep = new Set(keys);
      for (const key of new Set([...(prior?.keys ?? []), ...touched])) {
        if (keep.has(key)) continue;
        if (await erasableByExporter(this.target, work.org_id, key, work.session_id))
          erase.push({ key: key.slice(0, -"manifest.json".length), kind: "prefix", org_id: work.org_id, reason: "stale_prefix" });
      }
      this.state.enqueueErasure(erase);
      // Under the lease: nothing this group now references, or writes under, stays queued.
      this.state.cancelErasures([...referenced, ...keys.map((k) => k.slice(0, -"manifest.json".length))]);
      this.counters.erasuresQueued += erase.length;
      this.state.markExported(work.org_id, work.session_id, work.revision, keys);
      this.state.recordTruncations(work.org_id, work.session_id, truncations);
      this.counters.groups += 1;
      this.counters.eventsDone += work.events;
      if (work.events >= 20_000)
        this.log({ message: "large group done", kind: work.kind, count: work.events, duration_ms: this.throttle.now() - t0 });
      return true;
    } catch (error) {
      const code = errorCode(error);
      // A credential failure is not the group's: its pending keys stay recorded
      // (recovered at the next start) and it keeps its attempts.
      if (code !== "unauthorized") this.state.markFailed(work.org_id, work.session_id, code, [...touched], work.revision);
      this.counters.groupsFailed += 1;
      const aws = /<Code>([A-Za-z0-9]{1,64})<\/Code>/.exec(String((error as { message?: unknown })?.message ?? ""))?.[1];
      this.log({ message: "group failed", error_code: code, kind: work.kind, ...(aws ? { aws_code: aws } : {}) });
      // A credential or permission failure is not this group's: stop rather than fail every group.
      if (code === "unauthorized") throw error;
      if (isConnectionError(error)) throw error;
      return false;
    } finally {
      this.state.releaseLeases(LEASE_HOLDER, leased);
    }
  }

  progress(force = false) {
    const now = this.throttle.now();
    if (!force && now - this.#lastProgress < this.cfg.progressEveryMs) return;
    this.#lastProgress = now;
    const b = this.state.backlog(now, this.cfg.recentMs);
    const secs = Math.max(1, (now - this.started) / 1000);
    const eventsPerSec = this.counters.eventsDone / secs;
    const eta = (events: number) => (eventsPerSec > 0 ? +(events / eventsPerSec / 3600).toFixed(1) : null);
    const rates = this.throttle.rates(now);
    this.log({
      message: "progress",
      slow_window: rates.slow,
      rows_per_sec_cap: rates.rowsPerSec,
      rows_per_sec: +(this.counters.rows / secs).toFixed(1),
      mb_per_sec_read: +(this.counters.bytes / secs / 1e6).toFixed(2),
      events_per_sec: +eventsPerSec.toFixed(1),
      ...this.counters,
      backlog: b,
      eta_recent_h: eta(b.recentPendingEvents),
      eta_all_h: eta(b.pendingEvents),
      errors: this.state.errorCounts(),
      erasure_queue: this.state.erasureBacklog(now),
      // Outstanding truncations (ledger in the state file); T0 waits on zero.
      truncation_ledger: this.state.truncationTotals(),
    });
  }

  /** Run until stopped (or, with `once`, until there is no work left). */
  /**
   * Run until stopped (or, with `once`, until there is no work left). Lanes
   * pull from one shared queue that is refilled as it drains, so a large
   * session holds only its own lane, never the others (no batch barrier).
   * Sweeps run on their own loop beside the lanes.
   */
  async run(opts: { once?: boolean; maxGroups?: number } = {}) {
    // Only one exporter runs per state file (lock): leases of an earlier one are stale.
    const stale = this.state.releaseStaleLeases("exporter:", LEASE_HOLDER);
    if (stale) this.log({ message: "released stale exporter leases", count: stale });
    const recovered = this.state.recoverPendingKeys();
    if (recovered) this.log({ message: "recovered interrupted groups", count: recovered });
    if (!this.state.meta("act_sweep_done")) await this.sweepActivity();
    if (!this.state.meta("rev_sweep_done")) await this.sweepRevisions();
    const keyOf = (w: WorkItem) => `${w.org_id}\u0000${w.session_id}`;
    const inFlight = new Set<string>();
    let queue: WorkItem[] = [];
    let done = 0;
    let fatal: unknown;
    let finished = false;
    const halt = () => this.stopped() || fatal !== undefined || finished;
    const refill = () => {
      if (queue.length >= this.lanes.length) return;
      const queued = new Set(queue.map(keyOf));
      const more = this.state
        .nextWork(this.throttle.now(), this.cfg.recentMs, 4 * this.lanes.length + inFlight.size, this.cfg.reverifyMs)
        .filter((w) => !inFlight.has(keyOf(w)) && !queued.has(keyOf(w)));
      queue = [...queue, ...more];
    };
    const sweeps = (async () => {
      while (!halt()) {
        try {
          await this.sweepsIfDue();
        } catch (error) {
          if (isConnectionError(error)) fatal ??= error;
          else this.log({ message: "sweep failed", error_code: errorCode(error) });
        }
        await this.throttle.sleep(5_000);
      }
    })();
    // allSettled: a fatal error in one lane lets the others finish their group
    // (and record it) before the process exits.
    await Promise.allSettled(
      this.lanes.map(async (lane) => {
        while (!halt()) {
          refill();
          const item = queue.shift();
          if (!item) {
            if (opts.once && inFlight.size === 0) {
              finished = true;
              return;
            }
            this.progress();
            await this.throttle.sleep(inFlight.size ? 2_000 : this.cfg.idleSleepMs);
            continue;
          }
          inFlight.add(keyOf(item));
          try {
            await this.processGroup(item, lane);
          } catch (error) {
            fatal ??= error;
            return;
          } finally {
            inFlight.delete(keyOf(item));
          }
          done += 1;
          this.progress();
          if (opts.maxGroups !== undefined && done >= opts.maxGroups) finished = true;
        }
      }),
    );
    finished = true;
    await sweeps;
    this.progress(true);
    if (fatal !== undefined) throw fatal;
  }
}

export function isConnectionError(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  const message = String((error as { message?: unknown } | null)?.message ?? "");
  return (
    code === "ECONNRESET" ||
    code === "EPIPE" ||
    code === "57P01" ||
    code === "08006" ||
    code === "08003" ||
    /Connection terminated|Client has encountered a connection error/.test(message)
  );
}
