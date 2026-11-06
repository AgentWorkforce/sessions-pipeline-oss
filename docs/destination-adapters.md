# Destination adapters

The OSS engine defines destination contracts in
`packages/pipeline/src/destinations/index.ts`. Operators explicitly register
instances and choose their routing dependencies. Hosted S3, Supabase and
ClickHouse composition belongs to `sessions-pipeline-cloud`; equivalent
operator-defined adapters use the same engine contract.

This additive module is not imported by an ingest service, Worker, migration,
stack or environment loader. Existing delivery and pipeline behavior is
unchanged. It is a contract for a later reviewed integration, not a new public
HTTP endpoint or a replacement for the existing durable delivery coordinator.

## Contract and ownership

A `DestinationDelivery` contains a versioned delivery identity, verified org
and workspace, session ID, media type and already-scrubbed bytes. The host must
authenticate/authorize the caller and scrub before constructing it. The API is
for trusted application code; tenant fields alone do not confer authorization.
It does not replace the existing v1 manifest, object layout or `ai-hist`
record/revision export protocol. A receiver maps those formats into deliveries.

An adapter binds each delivery ID to its tenant, metadata and bytes. Identical
replays return a receipt; conflicting reuse must fail. Receipts distinguish
`stored` from `indexed`: acceptance by S3 says nothing about Supabase/ClickHouse
visibility. Each destination has its own progress; a later destination must
not erase the original storage receipt or force a successful write to repeat.

The registry validates a route completely before returning dependency layers.
For example, `directory` and `search` can both depend on `storage`. Unknown
instances, duplicate IDs, missing dependencies and cycles fail closed.
Registration/planning performs no network or disk I/O and never discovers
plugins or reads credentials. Adapters are trusted code, not sandboxed plugins.

The registry does not execute a route or own retries. The integrating host must
persist independent receipts/checkpoints, retry with the same delivery identity,
gate downstream work on real acceptance, and report partial progress. Use the
existing durable coordinator/queue interfaces rather than inventing a second
scheduler here. Protocol mapping, erasure, query/federation and self-hosted MCP
remain explicit follow-ups; an unsupported capability must not be implied.

## Simple OSS default

`createLocalDestination({ id: "local", root: "/private/session-store" })` from
`packages/pipeline/src/destinations/local.ts` is the suggested basic destination.
Construction is inert; only an explicit write creates the directory. It reuses
the existing Node filesystem object-store adapter with immutable conditional
creates. The record stores exact payload bytes as base64 with tenant/session
metadata and a SHA-256 digest. Its key is a hash of the tenant and delivery ID,
so identifiers are never interpreted as local paths.

The operator controls an absolute private directory and its filesystem
permissions. This simple adapter requires POSIX filesystem permissions. It
creates a new root with mode `0700` and refuses an existing root with any group
or other permission bits, or a symlink root; it never chmods existing data.
Keep it outside web roots and shared/untrusted writer directories.
It is a single-process, single-host store with local filesystem acceptance,
not a distributed durability or power-loss guarantee. It offers no query index,
replication, HA, automatic retention, encryption at rest or managed capacity.
The caller must manage disk space, permissions, backups and volume encryption.
The default payload limit is 1 MiB; the configured maximum is 16 MiB.
No cloud account, Supabase or ClickHouse is required.

Writes may reject cancellation before starting. Once a storage PUT begins, an
abort cannot undo it; callers use the actual receipt or replay an uncertain
outcome. Backend failures produce bounded error codes without paths, payloads
or backend error text. A `stored` receipt never claims search readiness.

Production-scale destinations, managed queues, tenancy policy and Agent Relay's
operational composition remain separate implementations. Self-hosted operators
can supply their own destinations without a product-specific routing protocol.

## Verification

Run from `packages/pipeline` after the root frozen install. Like the existing
pipeline object-layout facade, the local adapter reuses object-store source;
generated `object-store/dist` output is not required:

```sh
bun run typecheck
bun run test test/destination-registry.test.ts test/destination-local.test.ts
```

Tests exercise dependency ordering without dispatch, invalid routes, replay after
recreating an adapter, conflicting reuse, tenant separation, payload mutation,
concurrent writes, cancellation, size limits and disk failures. They use only
synthetic bytes and temporary local directories.
