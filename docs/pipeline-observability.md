# Session pipeline observability: traces, stage events, errors, DLQs

Status: **inactive**. Code and tests only. No queue is bound, no table is created,
and no deploy config changes. This is workstream W9 of the sessions pipeline
redesign (harness logs → local `history.db` → delivery coordinator → object storage
→ storage event → optional normalize → `sessions` pointer index).

Code: `packages/relayhistory/src/lib/pipeline/`, `src/routes/pipeline.ts`.
Tests: `test/pipeline-observability.test.ts` (fakes only: in-memory sink, in-memory
dead letters, a local queue simulator in `test/support/pipeline-queues.ts`).

## Trace ids

- One `trace_id` per session, a UUIDv7 minted on the machine when the session is
  first seen (`uuidv7()`; the Rust client should mint the same format).
- It travels as `X-Relay-Trace-Id` (dashed UUID) or W3C `traceparent`. The
  traceparent's 32-hex trace id is the same UUID without dashes, so an OTEL
  collector sees the same trace. A malformed header is ignored, not echoed.
- The app middleware puts it on `c.var.traceId`, echoes it in the response
  header, and adds `trace_id` to the request log line. Every pipeline log line
  carries `trace_id`, `workspace_id`, `machine_id`, `session_id`, `harness`,
  `stage`.
- Server stages take tenant identity from resolved auth or from the canonical
  object key (`ws=/m=/h=/d=/s=`, parsed by `objectKeyIdentity`), never from a body.

## Stages and `pipeline_events`

Stages: `ingest`, `deliver.put` and `fetch` (spans), and the seven queued stages
of the registry (below). Statuses: `started`, `ok`, `error`, `retrying`,
`dead_lettered`, `replayed`, `skipped`. Migration `20261025000600` renamed the
first queued stages (`storage.event` → `object-landed`, `index.upsert` →
`session-indexed`, `transform` → `file-normalize`) and added the rest.

| stage                                                                  | recorded by                                                                                                                                          |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ingest`                                                               | client, via `POST /v1/pipeline/events` (local harness → history.db)                                                                                  |
| `deliver.put`                                                          | client for object storage; server for the legacy RelayHistory destination (`/ingest`, `/delivery/batches`) with `attrs.destination = "relayhistory"` |
| `object-landed`, `session-indexed`                                     | server queue consumer                                                                                                                                |
| `file-normalize`                                                       | server queue consumer (`skipped` when not enabled)                                                                                                   |
| `summary-short`, `summary-long`, `status-classify`, `file-maintenance` | server queue consumer (registered, not implemented yet)                                                                                              |
| `fetch`                                                                | client, via `POST /v1/pipeline/events`                                                                                                               |

Clients cannot report server stages, and client `attrs` keep only numbers,
booleans and identifier-shaped strings. Identity keys (`trace_id`, `org_id`,
`workspace_id`, `machine_id`, `stage`, …) are dropped from attrs and can never
override the recorder's own log fields. A batch is validated in full before
anything is written, so a 400 leaves no partial trace. A client `at` more than
30 days old or more than 5 minutes ahead is replaced with server time, which
keeps rows inside the retention window. Each event's idempotency discriminator
is `attrs.destination` plus the `chunk`, `seq` or `offset` it describes.

Traces are scoped by **org and workspace** from resolved auth, because
workspace ids are not unique across orgs. A token with no workspace claim uses
`default`, the same workspace `applyIngest` stores its uploads under. Every
`StageMessage` must carry `ctx.orgId`. Trace reads page through every event
(1000 per page, ordered by `(at, idempotency_key)`) up to 20,000. Past that, the
response sets `truncated: true` and its `answer` says the view is partial.

The writer is the `PipelineEventSink` interface. Implementations:
`InMemoryPipelineEventSink` (tests), `logOnlyPipelineEventSink` (the default:
log lines only, trace lookup answers 503 `not_configured`), and
`postgrestPipelineEventSink`, which works with any PostgREST, whether that's
Supabase's REST API or one next to a self-hosted Postgres. Recording never throws into a stage.

### Tables (owned by the schema workstream)

Both tables are defined in `supabase/migrations/20261025000000_sessions_index.sql`
(PR #171), and this PR is stacked on it:

- `sessions.pipeline_events`
  - Columns: `trace_id`, `stage`, `status`, `attempt`, `error` jsonb `{code, name, message?, retryable}`, `attrs`, `at`, `duration_ms`, `org_id`, `workspace_id`, `machine_id`, `harness`, `session_id`, `idempotency_key`.
  - Range-partitioned by month on `at`, with a default partition and **90-day retention**.
  - CHECKs on `stage` and `status`.
  - Index `(trace_id, at)`, plus a partial index on failures.
- `sessions.pipeline_dead_letters`
  - One row per `<stage>:<org>:<workspace>:<workKey>` (`deadLetterId`); `state` is `dead`, `replaying` or `resolved`.

Both tables have RLS on and no policy, so only `service_role` can use them. The
Worker connects through Supabase REST with the service key (accepted for
server-side code) and enforces the workspace filter on every read in code. The
`sessions` schema must be exposed in the Supabase API settings.

The idempotency key has no unique index. On a partitioned table, a unique key
must include `at`, and a redelivered transition gets a new `at`. Inserts are
plain appends, and `summarizeTrace` collapses repeated `idempotency_key`s into
one event (the first recording stands).

Retention: the minute cron calls `POST /rpc/pipeline_events_maintain` once a
day at 03:17 UTC (`maintainPipelineEvents`). This does nothing until
`PIPELINE_EVENTS_URL` is set. `maintain` is `SECURITY DEFINER` and callable by
`service_role` only, so it can create partitions over REST (tested).

`test/pipeline-supabase-schema.test.ts` applies the real migration to PGlite and
runs the PostgREST writers against it through a small PostgREST stand-in.

## Per-stage queues and DLQs

`lib/pipeline/registry.ts` (`STAGE_REGISTRY`) lists the queued stages, each with
its own queue and DLQ, its Worker binding and its attempt budget:

```
object-landed ─┬→ session-indexed ─┬→ summary-short
               │                   ├→ summary-long
               │                   └→ status-classify
               └→ file-normalize  ──→ file-maintenance
each relayhistory-pipeline-<stage> → <name>-dlq
```

Implemented now: `object-landed` and `session-indexed`, and only the
`object-landed` fan-out is wired. The other five are registered stubs: their
queues and bindings exist, and a message sent to one is quarantined as
`stage_not_configured` until its handler ships and replay re-sends it.
`pipelineQueueDeps(env, { handlers })` can supply a handler for any stage.

`object-landed` enqueues the pointer first and the transform second. Pointer
indexing gets its own queue hop on purpose, so a slow index never holds up
storage events. A transform
that throws, times out, is dead-lettered, or can't be enqueued never delays or
undoes pointer indexing (tested). If the transform can't be enqueued, it's
quarantined as a dead letter so replay can recover it. Only if quarantine also
fails does the storage event retry, and re-sending the pointer is a harmless
upsert. A deleted manifest (`payload.deleted`) goes to `session-indexed` only, as
a pointer with `deleted: true`; chunk notifications are `skipped`.

### `PipelineQueue` backends

Producers send through `PipelineQueue` (`lib/pipeline/queue.ts`), which routes by
`message.stage`:

- `cloudflarePipelineQueue(stageBindings(env))`: the Worker default. The platform
  retries and moves exhausted messages to `<queue>-dlq`.
- `postgresPipelineQueue(sql)` + `pollPostgresStage({ sql, stage, deps })`: for a
  deployment without Cloudflare Queues. One table, `sessions.pipeline_jobs`
  (migration `20261025000600`). A poll claims visible jobs with
  `FOR UPDATE SKIP LOCKED` under a lease (default 300 s), so any number of
  consumers can share a stage; ack deletes the job and retry sets its backoff,
  both only under the claim's `lease_id`. A consumer that dies leaves its jobs to
  be claimed again when the lease runs out; a job that has used every attempt
  that way is reclaimed straight into the DLQ path, so a message that kills its
  consumer still reaches quarantine. A poll settles each job (ack or retry)
  before starting the next and renews the lease on the rest of its batch. After `maxAttempts` (registry) the
  job is flagged `dlq` and its next delivery goes through the DLQ path into
  `pipeline_dead_letters`; a failing DLQ delivery is retried, never dropped.
  `postgresDeadLetterStore(sql)` is the dead letter store without PostgREST.
  `sql` is anything with `query(text, params) → { rows }` (`pg`, Neon, PGlite).
- `MemoryPipelineQueue`: tests.

Both real backends hand deliveries to the same `handleStageQueue`, so retries,
quarantine, events and replay are identical. Queue messages carry ids and object
keys, never session content.

Consumer semantics follow `relayhistory-session-events`: one message at a time,
per-message ack or retry with backoff (30 s doubling, capped at 15 min). A
non-retryable `PipelineError` is quarantined right away instead of using up its
retries. Malformed messages are acked and logged. A message for a stage this
deployment hasn't wired (no handler) is quarantined right away, not retried.
Queue retries are finite, and the dead letter store holds it until the stage
exists and replay re-sends it. The DLQ
consumer quarantines into `DeadLetterStore` and acks. If it can't quarantine,
it retries.

Config to add **when activating**. It isn't applied here: `deploy-cloudflare.mjs`
and the local `wrangler.jsonc` are unchanged.

```jsonc
// producers
// one per STAGE_REGISTRY entry, e.g.
{ "binding": "PIPELINE_OBJECT_LANDED", "queue": "relayhistory-pipeline-object-landed${suffix}" },
{ "binding": "PIPELINE_SESSION_INDEXED", "queue": "relayhistory-pipeline-session-indexed${suffix}" },
{ "binding": "PIPELINE_FILE_NORMALIZE", "queue": "relayhistory-pipeline-file-normalize${suffix}" },
// consumers, per stage
{ "queue": "relayhistory-pipeline-<stage>${suffix}", "max_batch_size": 1, "max_batch_timeout": 1,
  "max_retries": <maxAttempts - 1>, "dead_letter_queue": "relayhistory-pipeline-<stage>${suffix}-dlq" },
{ "queue": "relayhistory-pipeline-<stage>${suffix}-dlq", "max_batch_size": 10, "max_retries": 3 }
```

`api-worker.ts` already sends any `relayhistory-pipeline-*` batch to
`handleStageQueue`. A self-hosted install can call the same handler from any
queue runner (SQS, Pub/Sub, NATS) with the `{ queue, messages[] }` shape.

## Replay

`replayDeadLetters` is a **dry run by default**. With `dryRun: false` it claims
each letter (compare-and-set `dead → replaying`) before sending, so concurrent
replays send it once. It keeps the original `workKey`, so idempotent handlers
apply the work at most once more. It releases the claim if the send fails. A
replayed message that succeeds resolves its letter. One that dies again goes
back to `dead` with `dead_count + 1`. `force` re-sends letters still `replaying`. Without `force`, a claim older
than one hour (`REPLAY_LEASE_MS`) is treated as abandoned and taken over. If a
claim call fails (it may have committed unseen), replay releases its own claim.

Operator route, gated by a shared `PIPELINE_OPS_SECRET` (503 when unset). That's
enough for the MVP; a scoped admin token will replace it later.

- `GET  /v1/internal/pipeline/dead-letters?stage=&traceId=&limit=`
- `POST /v1/internal/pipeline/dead-letters/replay` with `{stage?, traceId?, workspaceId?, limit?, dryRun?: true, force?: false}`

CLI: `node scripts/pipeline-dlq-replay.mjs --url … --secret-file ./secret [--stage file-normalize] [--list] [--execute]`.
The secret file must be mode 0600.

## "Did my upload land?"

`GET /v1/pipeline/traces/:traceId` (scope `rth:read`, the caller's workspace only)
returns every event for the trace, a per-stage summary, a per-destination
`deliver.put` summary, and a verdict:

- `indexed` means landed: the raw object is stored and its pointer is indexed. Transform state is reported alongside and doesn't change this.
- `stored` means the storage event was seen and the pointer is pending.
- `uploaded` means the client put finished and there's no storage event yet.
- `failed` means a required stage was dead-lettered or failed permanently.
- `in_progress` means events exist but none of the above applies yet.
- `not_found` (HTTP 404) means no events exist for this trace in this workspace.

`answer` is one sentence an agent can repeat as-is. Only the object-storage
destination decides the verdict. The legacy RelayHistory destination appears
under `destinations.relayhistory`.

## Error tracking

A failure is reduced to `{code, name, message?, retryable}` before any reporter
sees it. The message is included only for `PipelineError`s this service creates.
Database, provider and storage error text is never forwarded. Set
`PIPELINE_ERROR_TRACKER` to choose the reporter:

- `otlp` (default; vendor-neutral, good for self-hosted): `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`. Records carry the trace id. With no endpoint set it logs to the console only.
- `sentry` (optional): `SENTRY_DSN`, sent to the envelope API with no SDK dependency.
- `console`
- `none`

Enterprise code can pass any `ErrorReporter`. DSNs and headers are runtime
secrets. None is committed here.

## Keyed telemetry ids

Machine and session ids never leave the platform raw. Everything that goes to
external telemetry carries them as keyed tokens: Worker log lines (and their
PostHog export), OTLP and Sentry error reports. A token is
`h1_` + the first 128 bits of `HMAC-SHA256(PIPELINE_TELEMETRY_ID_KEY, "<machine|session>:<id>")`
(`lib/pipeline/telemetry-ids.ts`).

- **One session stays traceable to its customer.** `trace_id` (one UUIDv7 per
  session), `org_id` and `workspace_id` stay raw everywhere. The token is
  deterministic, so one machine or session is the same value in every line and
  every tool.
- **Inside log fields too**: every field the recorder's logger emits (its own
  lines and a handler's `recorder.log`), at any depth (nested objects and
  arrays included), has object-key `m=`/`s=` segments and the trace's own raw
  machine and session ids replaced by their tokens, whatever the field is
  called. Ids of 8+ characters are replaced wherever they occur; shorter ones
  where they stand alone. A value that is both ids takes the session token.
- **Raw ids only in service-role storage**: `pipeline_events`, `pipeline_jobs`,
  `pipeline_dead_letters` and the queue messages, beside the same `trace_id`.
- **Operator lookup** (ops secret):
  - `POST /v1/internal/pipeline/telemetry-ids` with `{machineIds?, sessionIds?}`
    returns `{machines: [token…], sessions: [token…]}`, in request order, so
    the raw ids are never echoed back. An id that fails `isSafeIdentifier`
    (secret-shaped, or not 1–256 identifier characters), or a non-string
    entry, gets a 400 that names only its position.
  - `GET /v1/internal/pipeline/traces/:traceId?orgId=&workspaceId=` takes the raw
    trace and tenant fields from a hashed line and returns that trace's events,
    raw ids included.
- **The key** is a Worker secret of at least 32 characters, for example
  `openssl rand -hex 32 | wrangler secret put PIPELINE_TELEMETRY_ID_KEY`. Keep it
  in the secret store, never in the repo, a config file, a log or a PR. A
  shorter value counts as unset. With no key the ids are left out of telemetry
  entirely (fail closed), and the telemetry-ids route answers 503.
- **Rotation** changes every token. While searching across a rotation, compute
  tokens with both keys. A token can't be reversed without the key, and the key
  only maps known ids to tokens; the raw id comes from service-role storage.
- **Activation**: like `PIPELINE_OPS_SECRET`, the key isn't wired into
  `deploy-cloudflare.mjs` yet. Add it to the deploy's secret list when the
  pipeline is activated.

## Env

`PIPELINE_EVENTS_URL`, `PIPELINE_EVENTS_TOKEN` (secret), `PIPELINE_EVENTS_SCHEMA`
(default `sessions`), `PIPELINE_ERROR_TRACKER`, `OTEL_EXPORTER_OTLP_*`,
`SENTRY_DSN` (secret), `PIPELINE_OPS_SECRET` (secret),
`PIPELINE_TELEMETRY_ID_KEY` (secret, 32+ chars),
`PIPELINE_FILE_NORMALIZE_ENABLED`, plus the three queue producer bindings above.
