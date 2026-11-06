/**
 * Registries pre-loaded with the OSS defaults. A composition adds its own
 * factories to the same registries before selecting. Provider implementation
 * placement and customer distribution are separate reviewed changes.
 */
import type { PipelineRuntimeAdapter } from "../runtime/contracts.js";
import { createWorkerRuntime, type WorkSource } from "../runtime/workers.js";
import type { SearchIndexAdapter } from "../search-index/contracts.js";
import { createPostgresSearchIndex, type SqlClient } from "../search-index/postgres.js";
import {
  AdapterRegistry,
  DEFAULT_PIPELINE_RUNTIME,
  DEFAULT_SEARCH_INDEX_BACKEND,
  PIPELINE_RUNTIMES,
  SEARCH_INDEX_BACKENDS,
  type PipelineRuntimeName,
  type SearchIndexBackendName,
} from "./registry.js";

/** What a search factory receives. Secrets arrive resolved (file-mounted), never via env. */
export interface SearchFactoryInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  /** The HoopDb (or a dedicated) Postgres for the postgres backend. */
  readonly sql?: SqlClient;
  readonly secrets?: Readonly<Record<string, string | undefined>>;
  /** Composition-specific clients (ClickHouse client, AWS credential provider, fetch). */
  readonly extras?: Readonly<Record<string, unknown>>;
}

export interface RuntimeFactoryInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly source?: WorkSource;
  readonly extras?: Readonly<Record<string, unknown>>;
}

export type SearchIndexRegistry = AdapterRegistry<SearchIndexBackendName, SearchFactoryInput, SearchIndexAdapter>;
export type PipelineRuntimeRegistry = AdapterRegistry<PipelineRuntimeName, RuntimeFactoryInput, PipelineRuntimeAdapter>;

export function createSearchIndexRegistry(): SearchIndexRegistry {
  const registry: SearchIndexRegistry = new AdapterRegistry("SEARCH_INDEX_BACKEND", SEARCH_INDEX_BACKENDS, DEFAULT_SEARCH_INDEX_BACKEND);
  registry.register({
    name: "postgres",
    create(input) {
      if (!input.sql) throw new Error("SEARCH_INDEX_BACKEND=postgres needs a Postgres client");
      return createPostgresSearchIndex({
        client: input.sql,
        ...(input.env.SEARCH_INDEX_POSTGRES_SCHEMA ? { schema: input.env.SEARCH_INDEX_POSTGRES_SCHEMA } : {}),
      });
    },
  });
  return registry;
}

export function createPipelineRuntimeRegistry(): PipelineRuntimeRegistry {
  const registry: PipelineRuntimeRegistry = new AdapterRegistry("PIPELINE_RUNTIME", PIPELINE_RUNTIMES, DEFAULT_PIPELINE_RUNTIME);
  registry.register({
    name: "workers",
    create(input) {
      if (!input.source) throw new Error("PIPELINE_RUNTIME=workers needs a work source");
      return createWorkerRuntime({ source: input.source });
    },
  });
  return registry;
}
