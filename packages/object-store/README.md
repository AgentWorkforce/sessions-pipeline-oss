# @relayhistory/object-store

Read side of RelayHistory's raw session object storage, for the History
Worker and Node services: the TypeScript mirror of the Rust
`relayhistory-object-store` crate
([AgentWorkforce/relayhistory#542](https://github.com/AgentWorkforce/relayhistory/pull/542)).

- **`ObjectStore`**: `head`, ranged `get`, cursor `list`, `presignGet`,
  plus single-request `put` with create / compare-and-swap semantics
  (multipart is not mirrored).
- **Adapters**:
  - `createS3Store`: S3, R2 and MinIO
  - `createGcsStore`: GCS XML API, with JSON API listing
  - `createAzureBlobStore`: Shared Key, SAS or Entra bearer auth
  - `createFsStore` from `@relayhistory/object-store/fs`, Node only
- **Layout**: v1 key builders and parsers (`manifestKey`, `rawChunkKey`,
  `parseSessionKey`, ...), identical to the Rust `layout.rs`.
- **Manifest v1**: `parseManifest` validates the schema and every invariant in
  `plugins/object-store/rust/docs/manifest-v1.md`, including the
  `content_sha256` hash chain and the key/session match. `pointerFields`
  returns the values for the Supabase `sessions` pointer row, and `chunkKeys`
  lists each chunk's object key.

The main entry uses only `fetch` and WebCrypto, so it runs in Cloudflare
Workers and in Node 20 and later. `test/workers-bundle.test.ts` bundles it with
no Node built-ins available. Runtime dependency: `zod`.

## Contract, shared with the Rust crate

| Behaviour                                                         | Result                                                                                                                                             |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `put(..., { ifNoneMatch: true })`, identical bytes already stored | `{ created: false }`                                                                                                                               |
| `put(..., { ifNoneMatch: true })`, different bytes stored         | `ObjectStoreError` with code `already_exists`                                                                                                      |
| `put(..., { ifMatch: version })`, lost race or object missing     | `precondition_failed`                                                                                                                              |
| Every `put`                                                       | Stores `content-sha256` metadata. A caller `contentSha256` is checked before upload.                                                               |
| `trace-id` metadata                                               | Written as `x-amz-meta-trace-id`, `x-goog-meta-trace-id` or `x-ms-meta-trace_id`; always read back as `trace-id`                                   |
| `err.retryable`                                                   | True only for network errors, 408, 429 and 5xx                                                                                                     |
| Credentials                                                       | Static, or a function returning expiring scoped credentials. The result is cached and refreshed 5 minutes before `expiresAt`, and never persisted. |

Objects written by the Rust adapters can be read here, and the reverse. This
includes metadata, listing and idempotent re-create. It was checked against
MinIO, Azurite, fake-gcs-server and a shared fs directory: the fs ETags use
`object_store`'s format, so either side can compare-and-swap.

## Tests

```sh
npm test -w @relayhistory/object-store               # unit tests + fs conformance
eval "$(packages/object-store/scripts/emulators.sh up)"
npm test -w @relayhistory/object-store               # + MinIO, fake-gcs-server, Azurite
packages/object-store/scripts/emulators.sh down
```

| Backend | Emulator                           | Cases                                                                                                                                                                   |
| ------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| fs      | n/a                                | 10/10                                                                                                                                                                   |
| s3      | MinIO                              | 10/10                                                                                                                                                                   |
| azure   | Azurite                            | 10/10                                                                                                                                                                   |
| gcs     | fake-gcs-server (`tustvold/` fork) | 6/10. Its 4 compare-and-swap and create cases are skipped because the emulator ignores generation preconditions. The Rust suite and `object_store` skip the same cases. |

## Limits

- `createFsStore` serializes compare-and-swap within one process only. Reads
  are always safe.
- GCS `presignGet` needs a `signingKey`. Azure `presignGet` needs `sharedKey`
  auth. Otherwise it fails with `unsupported`.
- S3 and Azure list cursors are the backend's continuation tokens. GCS and fs
  cursors are the last key returned. Treat every cursor as opaque.
