import { describe, expect, it, vi } from "vitest";
import {
  DestinationError,
  DestinationRegistry,
  type DestinationAdapter,
} from "../src/destinations/index.js";

function adapter(id: string): DestinationAdapter {
  return { id, kind: "test", acceptance: "stored", write: vi.fn() };
}

describe("destination registry", () => {
  it("plans storage before independent index destinations without doing I/O", () => {
    const registry = new DestinationRegistry();
    const instances = [adapter("storage"), adapter("directory"), adapter("search")];
    for (const instance of instances) registry.register(instance);
    const plan = registry.plan([
      { destinationId: "directory", after: ["storage"] },
      { destinationId: "storage" },
      { destinationId: "search", after: ["storage"] },
    ]);
    expect(plan.map((layer) => layer.map((item) => item.id)))
      .toEqual([["storage"], ["directory", "search"]]);
    for (const instance of instances) expect(instance.write).not.toHaveBeenCalled();
    expect(Object.isFrozen(plan)).toBe(true);
    expect(Object.isFrozen(plan[0])).toBe(true);
  });

  it("rejects duplicate registration without replacing the original", () => {
    const registry = new DestinationRegistry();
    registry.register(adapter("storage"));
    const original = registry.get("storage");
    expect(() => registry.register(adapter("storage"))).toThrow(DestinationError);
    expect(registry.get("storage")).toBe(original);
  });

  it("captures instance metadata and preserves a method's receiver", async () => {
    const registry = new DestinationRegistry();
    const instance = {
      id: "storage", kind: "test", acceptance: "stored" as const,
      write: vi.fn(async function (this: { marker: string }) { return this.marker; }),
      marker: "bound",
    };
    registry.register(instance as unknown as DestinationAdapter);
    instance.id = "renamed";
    instance.kind = "changed";
    expect(registry.get("storage").id).toBe("storage");
    expect(registry.get("storage").kind).toBe("test");
    expect(await registry.get("storage").write({} as never)).toBe("bound");
  });

  it.each([
    [],
    [{ destinationId: "unknown" }],
    [{ destinationId: "storage" }, { destinationId: "storage" }],
    [{ destinationId: "storage", after: ["storage"] }],
    [{ destinationId: "storage", after: ["search"] }],
    [{ destinationId: "storage", after: ["search", "search"] }, { destinationId: "search" }],
    [{ destinationId: "storage", after: ["search"] }, { destinationId: "search", after: ["storage"] }],
  ].map((route) => ({ route })))("rejects a malformed/incomplete route without dispatching: $route", ({ route }) => {
    const registry = new DestinationRegistry();
    const storage = adapter("storage");
    registry.register(storage);
    registry.register(adapter("search"));
    expect(() => registry.plan(route)).toThrow(DestinationError);
    expect(storage.write).not.toHaveBeenCalled();
  });

  it("does not retain planner state after a cycle is rejected", () => {
    const registry = new DestinationRegistry();
    registry.register(adapter("storage"));
    registry.register(adapter("search"));
    expect(() => registry.plan([
      { destinationId: "storage", after: ["search"] },
      { destinationId: "search", after: ["storage"] },
    ])).toThrow(DestinationError);
    expect(registry.plan([{ destinationId: "search" }])[0]?.[0]?.id).toBe("search");
  });

  it("rejects invalid identities and oversized registries", () => {
    const registry = new DestinationRegistry();
    expect(() => registry.register(adapter("../private"))).toThrow(DestinationError);
    for (let i = 0; i < 64; i++) registry.register(adapter(`destination_${i}`));
    expect(() => registry.register(adapter("overflow"))).toThrow(DestinationError);
    expect(() => registry.get("missing")).toThrow(DestinationError);
  });
});
