import { describe, expect, it } from "vitest";
import { getEventListeners } from "node:events";
import { runtimeConformance, type Observed } from "../src/runtime/conformance.js";
import {
  createWorkerRuntime,
  settle,
  type PipelineWork,
  type WorkItem,
  type WorkOutcome,
  type WorkSource,
} from "../src/runtime/index.js";

/** In-memory stand-in for the HoopDb pipeline_jobs queue. */
function memorySource(keys: readonly string[]) {
  const queue: WorkItem[] = keys.map((key, i) => ({ type: "manifest-landed", id: `job-${i}`, key, attempt: 1 }));
  const settled = new Map<string, WorkOutcome>();
  const source: WorkSource = {
    async claim(max) {
      return queue.splice(0, max);
    },
    async settle(item, outcome) {
      settled.set(item.id, outcome);
    },
  };
  return { source, settled, queue };
}

const observe = (o: WorkOutcome): Observed => (o.status === "done" ? "acked" : o.status === "retry" ? "redeliver" : "dead");

runtimeConformance("workers (in-memory queue)", async () => ({
  adapter: createWorkerRuntime({ source: memorySource([]).source }),
  async deliverManifests(work, keys) {
    const m = memorySource(keys);
    const binding = createWorkerRuntime({ source: m.source, batchSize: 10, now: () => 0 }).bind({ ...work, schedules: [] });
    await binding.tick();
    const out = new Map<string, Observed>();
    keys.forEach((key, i) => out.set(key, observe(m.settled.get(`job-${i}`)!)));
    return out;
  },
  async fireSchedule(work, job) {
    if (!work.schedules.some((s) => s.job === job)) throw new Error("undeclared schedule");
    const only: PipelineWork = { ...work, schedules: work.schedules.filter((s) => s.job === job) };
    await createWorkerRuntime({ source: memorySource([]).source, now: () => 0 }).bind(only).tick();
  },
}));

describe("worker runtime specifics", () => {
  it("shares overlapping ticks and settles started items before beginning the next", async () => {
    const order: string[] = [];
    let release!: () => void;
    let entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const handling = new Promise<void>(resolve => { entered = resolve; });
    type Item = { id: string; attempt: number; payload: number };
    let claims = 0;
    const source: WorkSource<Item> = {
      async claim() { claims++; return [{ id: "one", attempt: 0, payload: 1 }, { id: "two", attempt: 0, payload: 2 }]; },
      async start(item) { order.push(`start:${item.id}`); return { ...item, attempt: 1 }; },
      async settle(item) { order.push(`settle:${item.id}`); },
    };
    const binding = createWorkerRuntime({ source }).bind({ schedules: [], async handle(items) {
      const item = items[0]!;
      order.push(`handle:${item.id}`);
      if (item.id === "one") { entered(); await held; }
      return new Map([[item.id, { status: "done" as const }]]);
    } });
    const first = binding.tick();
    await handling;
    const second = binding.tick();
    expect(claims).toBe(1);
    release();
    expect(await Promise.all([first, second])).toEqual([2, 2]);
    expect(order).toEqual(["start:one", "handle:one", "settle:one", "start:two", "handle:two", "settle:two"]);
  });

  it("fires each schedule once per window and again in the next window", async () => {
    let t = 0;
    const fired: string[] = [];
    const work: PipelineWork = {
      schedules: [{ job: "reconcile", everySeconds: 300 }],
      async handle(items) {
        for (const i of items) if (i.type === "schedule") fired.push(`${i.job}@${i.firedAtMs}`);
        return new Map(items.map((i) => [i.id, { status: "done" } as WorkOutcome]));
      },
    };
    const b = createWorkerRuntime({ source: memorySource([]).source, now: () => t }).bind(work);
    await b.tick();
    t = 299_999;
    await b.tick();
    t = 300_000;
    await b.tick();
    expect(fired).toEqual(["reconcile@0", "reconcile@300000"]);
  });

  it("rejects malformed schedules at bind time", () => {
    const rt = createWorkerRuntime({ source: memorySource([]).source });
    const handle = async () => new Map();
    expect(() => rt.bind({ handle, schedules: [{ job: "Bad Name", everySeconds: 1 }] })).toThrow();
    expect(() => rt.bind({ handle, schedules: [{ job: "a", everySeconds: 0 }] })).toThrow();
    expect(() => rt.bind({ handle, schedules: [{ job: "a", everySeconds: 1 }, { job: "a", everySeconds: 2 }] })).toThrow();
  });

  it("run() stops when the signal aborts", async () => {
    const controller = new AbortController();
    const b = createWorkerRuntime({ source: memorySource([]).source, idleMs: 1 }).bind({ handle: async () => new Map(), schedules: [] });
    const running = b.run(controller.signal);
    controller.abort();
    await expect(running).resolves.toBeUndefined();
  });

  it("cleans up idle listeners and handles an abort while claiming work", async () => {
    const controller = new AbortController();
    let claims = 0;
    let idleListeners = -1;
    const source: WorkSource = {
      async claim() {
        if (++claims === 4) {
          idleListeners = getEventListeners(controller.signal, "abort").length;
          controller.abort();
        }
        return [];
      },
      async settle() {},
    };
    const binding = createWorkerRuntime({ source, idleMs: 1 }).bind({
      schedules: [], handle: async () => new Map(),
    });
    await binding.run(controller.signal);
    expect(idleListeners).toBe(0);
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  it("settle() turns a bogus outcome into a retry", async () => {
    const item: WorkItem = { type: "manifest-landed", id: "x", key: "k", attempt: 1 };
    const out = await settle(
      { schedules: [], handle: async () => new Map([["x", { status: "maybe" } as unknown as WorkOutcome]]) },
      [item],
      { runtime: "test" },
    );
    expect(out.get("x")).toMatchObject({ status: "retry", code: "no_outcome" });
  });

  it.each([
    { status: "retry", delaySeconds: NaN, code: "busy" },
    { status: "retry", delaySeconds: Infinity, code: "busy" },
    { status: "retry", delaySeconds: -1, code: "busy" },
    { status: "retry", delaySeconds: 1 },
    { status: "dead" },
  ])("retries malformed outcomes with a safe finite delay: %j", async (invalid) => {
    const item: WorkItem = { type: "manifest-landed", id: "x", key: "k", attempt: 1 };
    const out = await settle({
      schedules: [], handle: async () => new Map([[item.id, invalid as WorkOutcome]]),
    }, [item], { runtime: "test" });
    expect(out.get(item.id)).toEqual({ status: "retry", delaySeconds: 30, code: "no_outcome" });
  });
});
