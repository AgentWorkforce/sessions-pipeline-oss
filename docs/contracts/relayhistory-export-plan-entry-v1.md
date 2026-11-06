# Contract: RelayHistory export plan, plan entry and upload identity, v1

Status: **binding proposal, code only** (helm-chart-chief contract decision, E3). It is shared by:

- **relayauth #110 with #111**, the issuer, pinned at **#111 @ `2adf414a7767d8cc3e268c6a4bb790d27447e9e4`** (SOC2 code/control pass). #111 stores the plan, derives `meta.upload` and signs it. Its `deriveMigrationGrantPlan` and `migrationUploadIdentity` at that head reproduce these vectors' `plan_digest` and every upload identity byte for byte (checked by running them against this file). Its verifier also accepts exactly these nine identity fields, as the ingests do.
- **The #40 courier**: `derivePlanContract`, the mint request's `upload`.
- **Both ingests**, which recompute the identity from the actual request:
  - relayhistory-cloud #228;
  - this repo's `src/alleyoop/manifests.ts` plus `packages/pipeline`.

Test vectors: [`relayhistory-export-plan-entry-v1.vectors.json`](relayhistory-export-plan-entry-v1.vectors.json). Every implementation must reproduce every value. They were cross-checked three ways:

- the reference implementation, `packages/pipeline/src/lib/pipeline/migration-assertions.ts`: `canonicalJson`, `planEntrySha256`, `exportPlanDigest`, `uploadIdentityOf`, `derivePlanEntry`, `migrationSessionMatches`, `migrationUploadMatches` and `parseUploadIdentity`. It is byte-identical in relayhistory-cloud and tested against these vectors by `packages/pipeline/test/migration-assertion-contract.test.ts`;
- the courier's `derivePlanContract`, now on #55 at `e6af180a757dd7a41b3e46977b0b1542383608d9`. Re-checked by running it against this file: `plan_digest`, `entry_sha256`, `content_sha256` and the complete upload identities, **including `plan_digest`**, match byte for byte. #55's fixture copy is the same JSON. The earlier #40 commit `2f17febc` matched the entry, content and plan digests, but its `UploadIdentity` did not yet carry `plan_digest` (added from `19d5c799`). The vectors file's `note` still names `2f17febc`, so that every copy of the file stays byte-identical;
- an independent Python implementation.

## Canonical form

- **Ids:** `org_id`, `workspace_id` and `source_user_id` carry no `relay_` prefix. Signed claims (`org`, `wks`, `sponsorId`) are normalized by stripping one leading `relay_` before any comparison. An upload identity whose ids are not canonical is a malformed token, and both "alias" vectors must be refused.
- **JSON:**
  - object keys are sorted with `Array.prototype.sort` (UTF-16 code unit order; every key here is ASCII);
  - `undefined` members are dropped, and there is no whitespace;
  - every other value is written as `JSON.stringify` writes it.

## Plan entry: one manifest upload

| field | meaning |
|---|---|
| `grant_id` | the migration grant |
| `key` | the v1 manifest key without any deployment prefix: `v1/ws=…/m=…/h=…/d=YYYY/MM/DD/s=…/manifest.json` (the work key's object) |
| `org_id`, `workspace_id`, `source_user_id` | the grant pair, in canonical form |
| `session` | `{machine_id, harness, session_id, started_at}`; `started_at` is the manifest's `session.started_at`, and its date must be the key's `d=` |
| `state` | `open` or `closed` (`raw.state`) |
| `chunks` | `[{seq, epoch, start_offset, bytes, sha256, records}]`, where `sha256` is the SHA-256 of the chunk's **source** bytes and `records` defaults to 0 |
| `content_sha256` | the manifest v1 `content_sha256` hash chain over `chunks` |
| `raw_bytes` | the sum of `chunks[].bytes` |

- `entry_sha256` = lowercase hex `SHA-256(canonical(entry))`.
- `plan_digest`, the grant's `planDigest`, = lowercase hex `SHA-256(canonical({v: 1, grant_id, exported_at, manifests: entries sorted by key}))`.
- This is the hash of the canonical **stored** plan.
- Two rules for the issuer:
  - It stores the plan when it creates the grant, and it checks `plan_digest` against the stored plan.
  - It mints an assertion only for an entry of that plan whose `(source_user_id, workspace_id)` is the requested pair.

## Upload identity: signed in `meta.upload`

The issuer derives this from the stored plan; it is never taken from client input:

```
{ grant_id, plan_digest, key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at},
  content_sha256, entry_sha256 }
```

It binds:

- the grant and its plan;
- the org and workspace;
- the source user;
- the machine and session key;
- the manifest (work) key;
- the content and source-chunk digest, through `content_sha256` and `entry_sha256`.

## Mint request (courier to issuer)

- The courier sends `upload: {key}` on `POST /v1/tokens/migration-assertion`: the manifest key only, with `grantId`, `sourceUserId`, `workspaceId` and `nonce` as today.
- The issuer looks the key up in the grant's stored plan. It refuses when there is no such entry, or when the entry's `(source_user_id, workspace_id)` is not the requested pair. Otherwise it derives the whole upload identity above from that entry and signs it as `meta.upload`.
- The courier never supplies the other identity fields.

## Work key (changed)

- The pipeline work key of a migration commit is **`upload.key + "@" + upload.entry_sha256`**: the signed manifest key and the signed entry digest. Both come from `meta.upload`.
- It is **not** the storage version. It used to be `<manifest key>@<object version>`; that changed.
- The server still reads the manifest back from storage. That copy must pass the same identity and provenance checks before anything is enqueued, but its version is not an identity input.
- A posted `etag`, `version` or `previousVersion` never feeds the work key.
- A retry of the same planned upload, even under a fresh assertion, has the same work key.
- **The courier:** the commit response's `workKey` equals `upload.key + "@" + upload.entry_sha256`, exactly. The vectors list it under `work_keys`.
- **The consumer:** the ingest also sets `payload.migration = {key, entry_sha256}` on the `object-landed` job. `objectLandedWorkKey` (in `stages.ts`) then expects the signed work key, and accepts it only when `migration.key` is exactly this object's key without its deployment prefix and the digest is 64 lowercase hex.
  - **Live jobs** carry no `migration` field and keep `manifestWorkKey`: key@version.
  - **No fallback:** a malformed `migration` field is refused (`invalid_work_key`), never treated as live.
  - **Why this is safe:** only the ingest writes queue jobs, so a client cannot set the field.
  - **Tested end to end:** route, enqueue, consume `object-landed`, then `session-indexed`, for both kinds of job.

## What the ingest recomputes (never an echoed field)

| request | recomputed from the request | must equal |
|---|---|---|
| any | normalized signed `org`, `wks`, `sponsorId`; signed `meta.grantId`, `meta.planDigest` | `upload.org_id`, `workspace_id`, `source_user_id`, `grant_id`, `plan_digest` |
| `POST /v1/pipeline/chunks` | key, machine, harness, session id and date of the request's session | `upload.key`, `upload.session.*`, and the date of `started_at` |
| `GET /v1/pipeline/manifests/status` | the same, from `?key=` | the same |
| `POST /v1/pipeline/manifests` | the plan entry derived from the key and the validated manifest (session, state, chunk list, content recomputed over the chunks) | `upload.session`, `content_sha256`, `entry_sha256` |

Any difference is **403 `migration_upload_forbidden`**. A fresh machine key, a fresh session key, other content, or another user's upload cannot match.

## What the ingest checks, and what it does not

- The ingest authorizes **only from the signed assertion**:
  - signature, issuer, origin-bound audience, class, scope, lifetime and 5 s skew;
  - signed `wks` and `sponsorId` equal to the grant pair `meta.workspaceId` and `meta.sourceUserId`;
  - the grant and plan listed in `RELAYHISTORY_MIGRATION_GRANTS`;
  - the signed `meta.upload` identity recomputed from the actual request;
  - single use.
- It does **not** look up live workspace membership.
- Whether the source user belongs to the workspace is the **grant operator's responsibility when the grant is created**. RelayAuth #111 then mints only for the exact `(source_user_id, workspace_id)` pairs stored in that grant's plan.
- A membership change after the grant is created is not seen by the ingest. Revoking the grant (or removing it from `RELAYHISTORY_MIGRATION_GRANTS`) is the control.

## Chunk requests carry their plan entry

- A migration chunk request carries the canonical plan entry in front of the chunk bytes.
- The request body is `canonical(entry)` immediately followed by the chunk's source bytes. Header `X-Relay-Migration-Plan-Entry-Length` gives the entry's byte length, at most 1 MiB.
- Before storing anything, the ingest checks:
  - `SHA-256(entry bytes) == upload.entry_sha256`, and the entry bytes are exactly canonical;
  - the entry's key, tenancy and session are the request's and the signed ones;
  - its `content_sha256` matches its chunk list;
  - this chunk's `(seq, epoch, start_offset, bytes, SHA-256 of the actual bytes)` is one of the entry's chunks. So `seq` is always below the entry's chunk count.
- Otherwise: 403 `migration_upload_forbidden`.

## Manifest provenance and the work key

- **Provenance:** the chunk list, digests, session, state, key, grant, plan and tenancy are bound by `entry_sha256` and the signed tenancy. A manifest with an extra or missing chunk cannot match.
- **Required source fields:** the manifest must carry:
  - `extensions["client.relayhistory_export"]` with `grant_id`, `plan_digest` and `source_user_id`;
  - `extensions["96bulls"].user_id`.

  Each must be present and equal to the signed grant, plan digest and canonical source user. Missing or different is 403 `migration_upload_forbidden`.
- **Work key:** see "Work key (changed)" above. A deployment that cannot read storage refuses migration commits (503 `migration_storage_read_required`).

## Audit

- Every committed upload appends one row to `sessions.migration_ingest_audit`: grant, plan digest, entry digest, work key, org, workspace, user, result.
- The row is written in the same transaction as the replay row and the pipeline job.
- It holds no token, signature, nonce, `jti` or chunk data.
- It is append-only (a trigger refuses UPDATE and DELETE). Retention: the export's lifetime plus 400 days, removed only by a reviewed owner migration.

## Clocks

- The ingest allows 5 s of skew.
- **Issuer more than 5 s ahead:** every assertion is refused as not yet valid (401 `invalid_claims`, terminal).
- **Issuer more than 5 s behind:** assertions expire early (401 `migration_assertion_expired`; re-mint).
- Clock-offset monitoring on the issuer, ingest and courier hosts is required (see `docs/relayhistory-t0-runbook.md`).

## Other rules

- **Chunks:**
  - The ledger allows one chunk per `(jti, seq)`. An identical repeat is idempotent; any other chunk at that seq is 409 `migration_chunk_conflict`.
  - There are at most 256 chunks per assertion (403 `migration_chunk_limit`). The route accepts up to 16 MiB per chunk (the courier seals at 4 MiB), so this is up to 4 GiB, far more than a courier can send in an assertion's 65 seconds. Mint again for the rest of the session.
- **Expiry:** an assertion lives at most 60 s, with 5 s of skew.
  - The token's shape is validated first.
  - A well-formed token with `exp + 5 <= now` gets **401 `migration_assertion_expired`**: re-mint.
  - Every other 401 is terminal: `invalid_token` (including a bad signature), and `invalid_claims` (including a malformed expired token, or one issued more than 5 s in the future).
- **Replay:** 409 `migration_assertion_replayed` means mint a fresh assertion; an identical retry is idempotent. 503 `migration_ledger_unavailable` fails closed.
- **Replay retention:**
  - There is no cron. Each ledger insert deletes at most 100 replay rows whose `expires_at` (`exp + 5 s`) passed more than 5 minutes ago.
  - An expired token is refused before the ledger is read, so a purged token is never accepted again.
  - Nonce uniqueness holds only within validity plus margin. After a purge, a **new**, valid token may carry the same nonce and is accepted.

## Known limit

- v1 entries cover a session's whole chunk list. A straddling session (O4: a `backfill` epoch on a live manifest) needs an entry over the backfill epoch only.
- Until then such a session does not match, and it is refused.
