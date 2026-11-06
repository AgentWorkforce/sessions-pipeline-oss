# Search index and pipeline runtime adapters

Scope: portable contracts, Postgres search, and worker-runtime integration with
the existing Postgres stage queue. Worker entry points bind each stage to the
portable runtime while retaining readiness gates, trace context and side ticks.
Search service routing and additional provider bindings remain separate work.

Two seams are pluggable:

- **`SearchIndexAdapter`** decides where the derived transcript search index
  lives: Postgres `pg_trgm`, ClickHouse, OpenSearch, or Elasticsearch.
- **`PipelineRuntimeAdapter`** decides how stage work is triggered and run:
  worker pods, Lambda + SQS, Cloud Run + Pub/Sub, or Terse durable actors.

Both follow the destination registry pattern (#207,
`docs/destination-adapters.md`). Adapters are registered explicitly. There is
no plugin discovery, and registration and selection do no I/O. Selection fails
closed at boot.

## Included implementations

| Part | Path |
| --- | --- |
| Search contract, validation and query reference semantics | `packages/pipeline/src/search-index/contracts.ts` |
| Backend-independent scope reconcile | `packages/pipeline/src/search-index/reconcile.ts` |
| Postgres search using pg_trgm | `packages/pipeline/src/search-index/postgres.ts` |
| Shared search conformance | `packages/pipeline/src/search-index/conformance.ts` |
| Runtime contract and shared failure handling | `packages/pipeline/src/runtime/contracts.ts` |
| Worker poll loop and in-process schedules | `packages/pipeline/src/runtime/workers.ts` |
| Shared runtime conformance | `packages/pipeline/src/runtime/conformance.ts` |
| Explicit registration and boot selection | `packages/pipeline/src/adapters/` |

The default registries contain only Postgres search and the worker runtime.
Recognizing a provider name does not install that implementation. Creating an
unregistered adapter fails. Each entry point declares the implementations it
can run; future compositions must explicitly register and allow any additions.

OpenSearch, ClickHouse, Lambda, Cloud Run and Terse implementation ownership,
distribution and service integration are separate follow-ups. This foundation
does not select their repository placement or advertise them as Helm profiles.

## SearchIndexAdapter

```ts
interface SearchIndexAdapter {
  id; backend; capabilities(): SearchIndexCapabilities;
  ensureSchema();                                   // explicit operator step, idempotent
  upsert(docs);                                     // version-guarded, <= 5000 per call
  deleteEvents(tenant, positions);                  // exact stale rows (reconcile)
  eraseScope({ orgId, workspaceId, source, sessionIds });  // erasure, any ts
  scanFingerprints(scope, { fromMs, limit });       // reconcile scan; truncated => never delete
  query({ tenant, sessionId?, source?, kind?, projectId?, q?, sinceMs?, untilMs?,
          order, cursor?, limit, maxContentChars? }); // -> { hits, next? }
  health();
}
```

Every adapter must reproduce these semantics. The conformance suite checks
each one against the reference oracle (`matchesQuery`):

- **Rows:** one row per event, keyed by tenant (`orgId`, `workspaceId`) plus
  `(tsMs, sessionId, source, machineId, kind, eventId)`. If an event's ts or
  workspace moves, it becomes a new row, and reconcile deletes the old one.
- **Search (`q`):** a case-insensitive *literal* substring of the first 4096
  code points of `content`, or of `taskTitle`. `%`, `_`, `*`, `?` and `\` have
  no wildcard meaning. Text and the query use JavaScript `toLowerCase()` as
  specified by `matchesQuery`, including its Unicode behavior; a backend's
  native case matching must reproduce that result.
- **Order and cursor:** page order is code-point (byte) order over the
  position tuple, ascending or descending. A cursor is the last position
  served. A page can come back short and still carry `next`; the ClickHouse
  read path behaves the same way.
- **Versions:** an upsert never replaces a row with a greater `version`.
  ReplacingMergeTree, `ON CONFLICT ... WHERE version <=` and OpenSearch
  `external_gte` all enforce this.
- **Erasure:** `eraseScope` returns only once no row of the scope is
  readable (`synchronousErasure`).
- **Results:** a hit has the read API's row shape. The index-only
  `taskRefId` and `deliveryRecordId` columns are stored but not returned.
- **Errors:** errors carry bounded codes only. They never include content,
  credentials or backend error bodies.

Tenant ids must come from verified auth. The adapter filters on them; it does
not authenticate them.

### Postgres implementation

Postgres uses pg_trgm GIN indexes on normalized content-prefix and title columns.
Normalization applies the same lowercase operation as the query reference to the
first 4096 code points of content and to the complete title. Literal `LIKE` under
`COLLATE "C"` avoids database-locale differences for Unicode text.
The derived index has its own schema (`search_index`, configurable); transcript
text never goes in the metadata directory, `session_directory` (decision 0002).
Key columns use `COLLATE "C"` to preserve cursor order. `ensureSchema` requires
pg_trgm to be installed already and performs DDL only when explicitly called.

This adapter does not replace RelayHistory's existing transcript search path.
Selecting which service owns self-host transcript queries, migrating any data,
and routing requests through an adapter require a separate reviewed change.

## PipelineRuntimeAdapter

```ts
type WorkItem = { type: "manifest-landed", id, key, bucket?, sequencer?, etag?, version?, deleted?, attempt }
              | { type: "schedule", id, job, firedAtMs, attempt };
type WorkOutcome = done | retry(delaySeconds, code) | dead(code);
interface PipelineWork { handle(items, ctx): Promise<Map<id, WorkOutcome>>; schedules: { job, everySeconds }[] }
interface PipelineRuntimeAdapter<Binding> { kind; capabilities(); bind(work): Binding }
```

The engine supplies the work, an idempotent batch handler. The runtime
translates its platform's triggers into work items, and translates outcomes
into the platform's ack, retry and dead-letter mechanics. Every runtime calls
`settle()`, so the failure handling is identical on every platform: a handler
that throws retries the whole batch, and a missing or bogus outcome is retried.
Delivery is at-least-once everywhere.

The included `workers` runtime claims work from a caller-provided queue,
settles each outcome independently, and fires schedules once per wall-clock
window. The caller supplies the actual queue and idempotent work handler.
Several replicas may run only when queue claims are leased and the work
handler serializes each scope. Schedule handlers must be safe to run on more
than one replica.

`WorkSource.settle` acknowledges completed items, releases retries with their
delay, and parks dead items. The worker binding has a `tick()` that runs one
iteration for callers that own their loop and a `run(signal)` loop for standalone use. Idle
waits release their abort listeners and stop promptly when signaled.

The contract can represent platform queues, push delivery and durable actors,
but it does not implement those bindings. In particular, the Sessions Terse
runtime remains a stub in a separate draft; existing Relaycast/Relayfile actor
profiles do not establish a working Sessions actor runtime.

### Postgres stage delivery

`postgresStageConsumer` bridges `sessions.pipeline_jobs` to a worker-runtime
source and work handler. It carries the existing `StageMessage` in memory and
reuses the existing stage handler, retry policy, event recorder and quarantine
store. The queue schema and serialized message format stay the same.

Claiming a batch leases rows without spending delivery attempts. Immediately
before handling one row, the source renews the waiting rows and counts that
delivery's attempt. Each acknowledgment or retry finishes before the next row
starts. Every state change is fenced by the original lease token; a late
consumer cannot acknowledge another consumer's claim. Settlement counters count
only successful fenced writes.

The generic runtime supports deployment-specific item types alongside schedules.
Sources with a `start` hook use serial delivery and settlement; other sources
retain batch handling. Concurrent `tick()` calls share one active iteration.
The compatibility `pollPostgresStage` entry point also uses this runtime.

On SIGTERM/SIGINT, workers stop starting new deliveries, finish the active
delivery or side tick, and drain health requests before closing database and
telemetry resources. An idle wait wakes immediately. Unstarted leased rows keep
their attempt counts and become available when the lease expires; a new worker
uses the same recovery path after a crash or forced termination. Allow enough
termination grace for the slowest handler; handlers remain idempotent because
delivery is at least once.

Existing per-stage readiness checks and side-tick scheduling remain in the outer
worker loop. A closed gate blocks later stage claims and the side tick. Periodic
components retain their existing timing checks.

## Configuration

| Variable | Recognized names | Current entry-point behavior |
| --- | --- | --- |
| `SEARCH_INDEX_BACKEND` | `postgres`, `clickhouse`, `opensearch` | workers allow `postgres`; the existing Lambda entry allows `clickhouse` |
| `PIPELINE_RUNTIME` | `workers`, `lambda`, `cloudrun`, `terse` | workers allow `workers`; the existing Lambda entry allows `lambda` |
| `SEARCH_INDEX_POSTGRES_SCHEMA` | SQL identifier | `search_index` unless explicitly configured; validated when constructing the adapter |

Empty or unset selection variables use each entry point's existing behavior.
Unknown names and recognized but unsupported choices fail at boot. Workers use
the `workers` adapter over their existing stage queues; the Lambda service keeps
its existing implementation. A future entry point must explicitly declare any
broader choices.

The existing worker settings keep their defaults: `WORKER_BATCH_SIZE=10`,
`WORKER_IDLE_MS=1000`, and `WORKER_LEASE_SECONDS=300`. Keep the lease longer than
the slowest delivery. No new deployment setting is required for this wiring.

Secrets arrive through the caller's existing file-mounted secret/client
mechanism. Registry construction, registration and name selection do not read
credentials or perform I/O.

## Future provider implementations and distribution

The [OSS/cloud boundary](https://github.com/AgentWorkforce/build-plans/blob/52920781bb3c99ca0211b025df1b5d922cb2ebb3/proposals/2026-11-04-oss-cloud-split.md)
requires Helm to consume versioned OSS artifacts and lists an OSS
durable-actors/Terse profile. Reusable self-host OpenSearch and a real Terse
runtime must fit that boundary or have an explicitly reviewed distribution
exception before a customer profile can use them. Their placement decision
remains separate from this foundation. Hosted deployment/account policy,
managed credentials and private service composition belong in the hosted layer.

Draft provider implementations currently use a private engine checkout during
development. That is not a versioned consumer artifact or an approved customer
image distribution path. No new Helm selection values, image pins, Terraform
resources, migrations or service routing are provided here.

## Validation

Run the package checks with the repository's pinned toolchain:

```sh
bun run --filter @96bulls/pipeline typecheck
bun run --filter @96bulls/pipeline test
bun test src/shared src/session-pipeline
```

The shared search suite exercises PGlite with pg_trgm and can run against a
disposable PostgreSQL database using `SEARCH_CONFORMANCE_PG_URL`. CI runs that
suite on PostgreSQL 15, 16 and 17 before the role-hardening proofs. The runtime
suite checks delivery outcomes, scheduling and overlapping ticks in memory. Stage
queue integration tests use the real queue schema in PGlite to exercise delayed
retry, dead letters, replay, stale leases, shutdown and restart recovery. Worker
orchestration tests cover draining, idle cancellation and side-tick ordering.

These checks cover adapter semantics and existing entry-point compatibility.
Search routing, a deployed provider binding and customer image distribution
need their own integration validation before those features are available.
