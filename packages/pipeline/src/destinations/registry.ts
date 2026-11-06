import {
  assertDestinationLabel,
  DestinationError,
  type DestinationAdapter,
} from "./contracts.js";

export interface DestinationRouteEntry {
  readonly destinationId: string;
  /** These destinations must acknowledge before this destination is attempted. */
  readonly after?: readonly string[];
}

const MAX_DESTINATIONS = 64;

/** Explicit registration only: no environment reads, plugin discovery or I/O. */
export class DestinationRegistry {
  readonly #adapters = new Map<string, DestinationAdapter>();

  register(adapter: DestinationAdapter): void {
    assertDestinationLabel(adapter.id);
    assertDestinationLabel(adapter.kind);
    if (
      this.#adapters.has(adapter.id) ||
      this.#adapters.size >= MAX_DESTINATIONS ||
      !["stored", "indexed"].includes(adapter.acceptance) ||
      typeof adapter.write !== "function"
    ) {
      throw new DestinationError("invalid_argument");
    }
    // Snapshot metadata and bind the implementation: a later caller mutation
    // cannot change which registered instance a route resolves to.
    this.#adapters.set(adapter.id, Object.freeze({
      id: adapter.id,
      kind: adapter.kind,
      acceptance: adapter.acceptance,
      write: adapter.write.bind(adapter),
    }));
  }

  get(id: string): DestinationAdapter {
    const adapter = this.#adapters.get(id);
    if (!adapter) throw new DestinationError("invalid_argument");
    return adapter;
  }

  /**
   * Validate the entire route, then return deterministic dependency layers.
   * Planning never calls an adapter. The host owns durable per-destination
   * progress and must not dispatch a later layer before its dependencies ACK.
   */
  plan(route: readonly DestinationRouteEntry[]): readonly (readonly DestinationAdapter[])[] {
    if (route.length === 0 || route.length > MAX_DESTINATIONS) {
      throw new DestinationError("invalid_argument");
    }
    const pending = new Map<string, Set<string>>();
    for (const entry of route) {
      assertDestinationLabel(entry.destinationId);
      this.get(entry.destinationId);
      const after = entry.after ?? [];
      if (pending.has(entry.destinationId) || new Set(after).size !== after.length) {
        throw new DestinationError("invalid_argument");
      }
      pending.set(entry.destinationId, new Set(after));
    }
    for (const [id, dependencies] of pending) {
      for (const dependency of dependencies) {
        if (dependency === id || !pending.has(dependency)) {
          throw new DestinationError("invalid_argument");
        }
      }
    }
    const layers: (readonly DestinationAdapter[])[] = [];
    const done = new Set<string>();
    while (pending.size) {
      const ready = [...pending]
        .filter(([, after]) => [...after].every((id) => done.has(id)))
        .map(([id]) => id);
      if (ready.length === 0) throw new DestinationError("invalid_argument");
      layers.push(Object.freeze(ready.map((id) => this.get(id))));
      for (const id of ready) {
        pending.delete(id);
        done.add(id);
      }
    }
    return Object.freeze(layers);
  }
}
