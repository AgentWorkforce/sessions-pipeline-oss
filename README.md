> This source export is superseded. The canonical engine repository is
> [AgentWorkforce/sessions-pipeline](https://github.com/AgentWorkforce/sessions-pipeline),
> where public-release preparation is in progress. No supported image release
> is issued from this export.

# Agent Relay Sessions Pipeline

Portable services for ingesting, scrubbing, storing, indexing and processing AI coding session history. Licensed under Apache-2.0.

This source distribution contains the service runtime, Postgres migrations and role bootstrap SQL, pipeline contracts and worker runtime, and generic object-store adapters. Hosted account configuration, production operations records and deployment automation are maintained separately.

## Development

Use Bun 1.4.2 and Node 24. The workspace packages are built from this checkout.

```sh
bun install --frozen-lockfile
bun run build
bun run typecheck
bun test --timeout 30000
bun run --filter @96bulls/pipeline test
bun run --filter @relayhistory/object-store test
```

Some integration tests require disposable external services. CI supplies ClickHouse 26.8.34.13; PostgreSQL tests use PGlite unless a test explicitly requests a real server. Skipped integration tests do not establish backend compatibility.

## Service images

```sh
docker build -f docker/services.Dockerfile -t sessions-pipeline:local .
```

The image runs as an unprivileged user. Set `SERVICE` to select a process:

| Value | Process |
| --- | --- |
| `hoopdb-migrate` | Schema migration runner |
| `locker-init` | Object-store initialization |
| `alleyoop-ingest` | Ingest API |
| `alleyoop-courier` | Upload courier |
| `hoopdb-indexer` | Session indexer |
| `referee` | Session classification |
| `bob-costas` | Summary enrichment |
| `session-brief` | Brief worker and its queue lifecycle |

Database connections, authentication, encryption keys and object-store bindings must be supplied by the operator. Role bootstrap SQL is under `supabase/bootstrap`; see [role provisioning](runbooks/session-directory-search-worker-bootstrap.md). Inspect and approve migrations before applying them to a real database.

Telemetry has no default remote collector. PostHog log export requires both an explicit host and API key. Configure an OTLP endpoint explicitly if trace export is wanted.

`docker/large-read-worker.Dockerfile` builds a separate, default-off Node worker. Shipping its source does not activate its runtime or schema profile.

## Contracts and limits

- [Object-store adapters](packages/object-store/README.md) include filesystem, S3-compatible, GCS and Azure implementations. An adapter's existence does not establish a full installation on that provider.
- [Destination contracts](docs/destination-adapters.md) define delivery and acknowledgment boundaries.
- [Search and runtime contracts](docs/search-and-runtime-adapters.md) include Postgres/worker defaults. Alternate provider implementations and Terse are not activated by this release.
- The local capture SDK and CLI remain in the public [RelayHistory project](https://github.com/AgentWorkforce/relayhistory). They are not bundled here.

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md) and [NOTICE](NOTICE).

Experimental hosted publication/GC and owner-operation tools retain example target pins. They are not customer deployment profiles. The service image entrypoint does not select those runtimes.
