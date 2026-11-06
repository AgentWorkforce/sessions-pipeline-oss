/**
 * Read-rate limiter for Neon: a token bucket on rows and on payload bytes,
 * whose rate drops by `slowFactor` inside configured "slow windows" (the
 * hackathon hours), plus a pause while Neon has lock waiters.
 */
export interface SlowWindow {
  start: number;
  end: number;
}

/** `2026-10-31T15:00Z/2026-11-01T00:00Z,...` */
export function parseWindows(spec: string | undefined): SlowWindow[] {
  if (!spec?.trim()) return [];
  return spec.split(",").map((part) => {
    const [a, b] = part.trim().split("/");
    const start = Date.parse(a ?? "");
    const end = Date.parse(b ?? "");
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start)
      throw new Error(`invalid slow window ${JSON.stringify(part)}`);
    return { start, end };
  });
}

export interface ThrottleConfig {
  rowsPerSec: number;
  bytesPerSec: number;
  slowWindows: SlowWindow[];
  slowFactor: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

export class Throttle {
  #rowDebt = 0;
  #byteDebt = 0;
  #last: number;
  readonly cfg: ThrottleConfig;
  readonly now: () => number;
  readonly sleep: (ms: number) => Promise<void>;

  constructor(cfg: ThrottleConfig) {
    if (!(cfg.rowsPerSec > 0) || !(cfg.bytesPerSec > 0)) throw new Error("rates must be positive");
    if (!(cfg.slowFactor > 0 && cfg.slowFactor <= 1)) throw new Error("slow factor must be in (0, 1]");
    this.cfg = cfg;
    this.now = cfg.now ?? Date.now;
    this.sleep = cfg.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.#last = this.now();
  }

  /** 1, or `slowFactor` inside a slow window. */
  factor(at = this.now()): number {
    return this.cfg.slowWindows.some((w) => at >= w.start && at < w.end) ? this.cfg.slowFactor : 1;
  }

  rates(at = this.now()) {
    const f = this.factor(at);
    return { rowsPerSec: this.cfg.rowsPerSec * f, bytesPerSec: this.cfg.bytesPerSec * f, slow: f < 1 };
  }

  /** Account for what was just read, and wait until it fits the current rate. */
  async take(rows: number, bytes: number) {
    const t = this.now();
    const { rowsPerSec, bytesPerSec } = this.rates(t);
    const elapsed = Math.max(0, (t - this.#last) / 1000);
    this.#last = t;
    this.#rowDebt = Math.max(0, this.#rowDebt - elapsed * rowsPerSec) + rows;
    this.#byteDebt = Math.max(0, this.#byteDebt - elapsed * bytesPerSec) + bytes;
    // Allow one second of burst; wait out anything beyond it.
    const wait = Math.max(this.#rowDebt / rowsPerSec, this.#byteDebt / bytesPerSec) - 1;
    if (wait > 0) await this.sleep(Math.ceil(wait * 1000));
  }
}
