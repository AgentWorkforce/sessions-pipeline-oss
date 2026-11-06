/**
 * Shared PipelineRuntimeAdapter conformance suite. A harness drives the
 * adapter through its platform's real trigger SHAPES (an SQS event, a
 * Pub/Sub push request, a queue claim, an actor call) with synthetic data,
 * and reports what the platform would observe for each delivery:
 *
 *   acked      the platform forgets it
 *   redeliver  the platform delivers it again later
 *   dead       parked for an operator (DLQ, dead queue row, actor dead set)
 *
 * Every runtime must map the engine's outcomes the same way.
 */
import { describe, expect, it } from "vitest";
import type { PipelineRuntimeAdapter, PipelineWork, WorkItem, WorkOutcome } from "./contracts.js";

export type Observed = "acked" | "redeliver" | "dead";

export interface RuntimeHarness {
  readonly adapter: PipelineRuntimeAdapter;
  /**
   * Deliver landed manifests (by object key) through the platform shape.
   * Returns the platform's view per key.
   */
  deliverManifests(work: PipelineWork, keys: readonly string[]): Promise<Map<string, Observed>>;
  /** Fire one named schedule through the platform shape. */
  fireSchedule(work: PipelineWork, job: string): Promise<void>;
}

function recordingWork(decide: (item: WorkItem) => WorkOutcome | "throw" | "omit") {
  const seen: WorkItem[] = [];
  const work: PipelineWork = {
    schedules: [
      { job: "reconcile", everySeconds: 300 },
      { job: "brief", everySeconds: 60 },
    ],
    async handle(items) {
      seen.push(...items);
      const out = new Map<string, WorkOutcome>();
      for (const item of items) {
        const d = decide(item);
        if (d === "throw") throw new Error("synthetic handler failure");
        if (d !== "omit") out.set(item.id, d);
      }
      return out;
    },
  };
  return { work, seen };
}

const keyOf = (item: WorkItem) => (item.type === "manifest-landed" ? item.key : `schedule:${item.job}`);

export function runtimeConformance(name: string, harness: () => Promise<RuntimeHarness>): void {
  describe(`PipelineRuntimeAdapter conformance: ${name}`, () => {
    const k1 = "96bulls/org-a/ws/claude/s1/m=machine/manifest.json";
    const k2 = "96bulls/org-a/ws/claude/s2/m=machine/manifest.json";
    const k3 = "96bulls/org-a/ws/claude/s3/m=machine/manifest.json";

    it("declares coherent capabilities", async () => {
      const h = await harness();
      const caps = h.adapter.capabilities();
      expect(caps.kind).toBe(h.adapter.kind);
      expect(caps.maxBatch).toBeGreaterThanOrEqual(1);
      expect(["poll", "push"]).toContain(caps.trigger);
    });

    it("hands each landed manifest key to the engine unchanged and acks done work", async () => {
      const h = await harness();
      const { work, seen } = recordingWork(() => ({ status: "done" }));
      const observed = await h.deliverManifests(work, [k1, k2]);
      expect([...observed.values()]).toEqual(["acked", "acked"]);
      expect(seen.filter((i) => i.type === "manifest-landed").map(keyOf).sort()).toEqual([k1, k2]);
      for (const item of seen) expect(item.attempt).toBeGreaterThanOrEqual(1);
    });

    it("redelivers only the items that asked to retry, or were left unanswered", async () => {
      const h = await harness();
      const { work } = recordingWork((item) =>
        keyOf(item) === k2
          ? { status: "retry", delaySeconds: 5, code: "scope_busy" }
          : keyOf(item) === k3
            ? "omit"
            : { status: "done" },
      );
      const observed = await h.deliverManifests(work, [k1, k2, k3]);
      expect(observed.get(k1)).toBe("acked");
      expect(observed.get(k2)).toBe("redeliver");
      expect(observed.get(k3)).toBe("redeliver");
    });

    it("redelivers everything when the handler throws", async () => {
      const h = await harness();
      const { work } = recordingWork(() => "throw");
      const observed = await h.deliverManifests(work, [k1, k2]);
      expect([...observed.values()]).toEqual(["redeliver", "redeliver"]);
    });

    it("never acks a dead item as done: it is parked or left for the platform DLQ", async () => {
      const h = await harness();
      const { work } = recordingWork((item) => (keyOf(item) === k1 ? { status: "dead", code: "invalid_manifest" } : { status: "done" }));
      const observed = await h.deliverManifests(work, [k1, k2]);
      expect(["dead", "redeliver"]).toContain(observed.get(k1));
      expect(observed.get(k2)).toBe("acked");
    });

    it("fires a named schedule as a schedule work item", async () => {
      const h = await harness();
      const { work, seen } = recordingWork(() => ({ status: "done" }));
      await h.fireSchedule(work, "reconcile");
      const fired = seen.filter((i) => i.type === "schedule");
      expect(fired.length).toBeGreaterThanOrEqual(1);
      expect(fired.map((i) => (i.type === "schedule" ? i.job : ""))).toContain("reconcile");
    });

    it("refuses a schedule the work does not declare", async () => {
      const h = await harness();
      const { work } = recordingWork(() => ({ status: "done" }));
      await expect(h.fireSchedule(work, "not-a-job")).rejects.toThrow();
    });
  });
}
