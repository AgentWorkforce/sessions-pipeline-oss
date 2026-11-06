import { describe, expect, test } from "bun:test";
import { runWorkerLoop, workerTick } from "../worker.ts";
import { createWorkerRuntime, type WorkSource } from "@rhc/runtime/workers.ts";

describe("stage worker orchestration", () => {
  for (const stages of [["stage"], []]) {
    test(`shutdown during readiness validation blocks ${stages.length ? "stage claims" : "side ticks"}`, async () => {
      const controller = new AbortController();
      let release!: () => void;
      let entered!: () => void;
      const held = new Promise<void>(resolve => { release = resolve; });
      const checking = new Promise<void>(resolve => { entered = resolve; });
      const seen: string[] = [];
      const running = workerTick({ signal: controller.signal, stages,
        gate: async () => { entered(); await held; return true; },
        poll: async stage => { seen.push(stage); return 1; },
        tick: async () => { seen.push("side"); },
      });
      await checking;
      controller.abort();
      release();
      expect(await running).toEqual({ claimed: 0, gated: true });
      expect(seen).toEqual([]);
    });
  }

  test("shutdown drains a started delivery before cleanup and skips later stages and side ticks", async () => {
    const controller = new AbortController();
    const order: string[] = [];
    let release!: () => void;
    let entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const handling = new Promise<void>(resolve => { entered = resolve; });
    let finished = false;
    type Item = { id: string; attempt: number };
    const source: WorkSource<Item> = {
      async claim() { order.push("claim"); return [{ id: "job", attempt: 0 }]; },
      async start(item) { order.push("start"); return { ...item, attempt: 1 }; },
      async settle() { order.push("settle"); },
    };
    const binding = createWorkerRuntime({ source }).bind({ schedules: [], async handle(items) {
      order.push("handle"); entered(); await held;
      return new Map(items.map(item => [item.id, { status: "done" as const }]));
    } });
    const running = runWorkerLoop({ signal: controller.signal, idleMs: 60_000, tick: async () => {
      const result = await workerTick({ stages: ["first", "second"],
        gate: async () => !controller.signal.aborted,
        poll: async stage => { order.push(stage); return binding.tick(controller.signal); },
        tick: async () => { order.push("side"); },
      });
      return result.claimed;
    } }).then(() => { finished = true; order.push("cleanup"); });
    await handling;
    controller.abort();
    await Promise.resolve();
    expect(finished).toBe(false);
    release();
    await running;
    expect(order).toEqual(["first", "claim", "start", "handle", "settle", "cleanup"]);
  });

  test("shutdown interrupts an idle wait and prevents another cycle", async () => {
    const controller = new AbortController();
    let ticks = 0;
    const timer = setTimeout(() => controller.abort(), 10);
    try {
      await runWorkerLoop({ signal: controller.signal, idleMs: 60_000, tick: async () => { ticks++; return 0; } });
      expect(ticks).toBe(1);
    } finally { clearTimeout(timer); }
  }, 1_000);

  test("side scheduling stays behind the live gate after stage bindings", async () => {
    const controller = new AbortController();
    const seen: string[] = [];
    let cycles = 0;
    await runWorkerLoop({ signal: controller.signal, idleMs: 0, tick: async () => {
      const result = await workerTick({ stages: ["a", "b"], gate: async () => true,
        poll: async stage => { seen.push(stage); return 0; },
        tick: async () => { seen.push("side"); if (++cycles === 2) controller.abort(); },
      });
      return result.claimed;
    } });
    expect(seen).toEqual(["a", "b", "side", "a", "b", "side"]);
  });
});
