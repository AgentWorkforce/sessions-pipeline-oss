/**
 * Explicit adapter registration and env selection for the two pluggable
 * seams: the transcript search index and the pipeline runtime.
 *
 * Same rules as the destination registry (../destinations/registry.ts): no
 * plugin discovery, no I/O, no credential reads. The OSS engine registers its
 * defaults (postgres search, worker-pod runtime); a composition explicitly
 * registers any additional implementations before calling `create`.
 * Provider implementation ownership is not selected by this registry.
 *
 * Creation happens once at boot and fails closed: an unknown or unregistered
 * name is a configuration error, never a silent fallback. `select` only
 * validates a name against the entry point's declared supported choices.
 */

export const SEARCH_INDEX_BACKENDS = ["postgres", "clickhouse", "opensearch"] as const;
export type SearchIndexBackendName = (typeof SEARCH_INDEX_BACKENDS)[number];

export const PIPELINE_RUNTIMES = ["workers", "lambda", "cloudrun", "terse"] as const;
export type PipelineRuntimeName = (typeof PIPELINE_RUNTIMES)[number];

/** Defaults: what a self-hosted install runs today (pg_trgm search, worker pods). */
export const DEFAULT_SEARCH_INDEX_BACKEND: SearchIndexBackendName = "postgres";
export const DEFAULT_PIPELINE_RUNTIME: PipelineRuntimeName = "workers";

export class AdapterConfigError extends Error {
  constructor(
    readonly variable: string,
    readonly reason: "unknown" | "not_registered" | "not_allowed",
    readonly allowed: readonly string[],
  ) {
    super(
      `${variable}: ${reason === "unknown" ? "unknown value" : reason === "not_registered" ? "adapter not registered in this build" : "not allowed for this entry point"}; allowed: ${allowed.join(", ") || "(none)"}`,
    );
    this.name = "AdapterConfigError";
  }
}

export interface AdapterFactory<Config, Adapter> {
  readonly name: string;
  create(config: Config): Adapter;
}

export class AdapterRegistry<Name extends string, Config, Adapter> {
  readonly #factories = new Map<string, AdapterFactory<Config, Adapter>>();

  constructor(
    readonly variable: string,
    readonly known: readonly Name[],
    readonly defaultName: Name,
  ) {}

  register(factory: AdapterFactory<Config, Adapter>): void {
    if (!(this.known as readonly string[]).includes(factory.name)) {
      throw new AdapterConfigError(this.variable, "unknown", this.known);
    }
    if (this.#factories.has(factory.name) || typeof factory.create !== "function") {
      throw new Error(`${this.variable}: ${factory.name} is already registered`);
    }
    this.#factories.set(factory.name, Object.freeze({ name: factory.name, create: factory.create.bind(factory) }));
  }

  registered(): readonly Name[] {
    return this.known.filter((n) => this.#factories.has(n));
  }

  /**
   * Resolve the configured name. `allowed` narrows what an entry point can
   * run (the Lambda entry only runs the lambda runtime, for example).
   */
  select(env: Readonly<Record<string, string | undefined>>, allowed: readonly Name[] = this.known): Name {
    const raw = env[this.variable]?.trim();
    const name = (raw === undefined || raw === "" ? this.defaultName : raw) as Name;
    if (!(this.known as readonly string[]).includes(name)) {
      throw new AdapterConfigError(this.variable, "unknown", this.known);
    }
    if (!allowed.includes(name)) throw new AdapterConfigError(this.variable, "not_allowed", allowed);
    return name;
  }

  /** select + require registration + create. */
  create(env: Readonly<Record<string, string | undefined>>, config: Config, allowed?: readonly Name[]): Adapter {
    const name = this.select(env, allowed);
    const factory = this.#factories.get(name);
    if (!factory) throw new AdapterConfigError(this.variable, "not_registered", this.registered());
    return factory.create(config);
  }
}

/**
 * Boot-time validation for entry points that do not yet dispatch through the
 * adapters: refuses unknown names and names this entry point cannot run, and
 * returns the resolved names. With neither variable set it returns the
 * entry point's defaults, so current installs see no change.
 */
export function validateAdapterEnv(
  env: Readonly<Record<string, string | undefined>>,
  entry: {
    readonly search?: readonly SearchIndexBackendName[];
    readonly searchDefault?: SearchIndexBackendName;
    readonly runtime: readonly PipelineRuntimeName[];
    readonly runtimeDefault?: PipelineRuntimeName;
  },
): { search: SearchIndexBackendName; runtime: PipelineRuntimeName } {
  const search = new AdapterRegistry<SearchIndexBackendName, unknown, unknown>(
    "SEARCH_INDEX_BACKEND",
    SEARCH_INDEX_BACKENDS,
    entry.searchDefault ?? DEFAULT_SEARCH_INDEX_BACKEND,
  );
  const runtime = new AdapterRegistry<PipelineRuntimeName, unknown, unknown>(
    "PIPELINE_RUNTIME",
    PIPELINE_RUNTIMES,
    entry.runtimeDefault ?? DEFAULT_PIPELINE_RUNTIME,
  );
  return {
    search: search.select(env, entry.search ?? [entry.searchDefault ?? DEFAULT_SEARCH_INDEX_BACKEND]),
    runtime: runtime.select(env, entry.runtime),
  };
}
