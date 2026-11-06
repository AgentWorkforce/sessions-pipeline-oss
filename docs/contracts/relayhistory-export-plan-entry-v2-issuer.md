# Contract: relayauth issuer and ingest changes for plan entry v2 (backfill epochs)

Status: **draft for contract review (amended after the #86 HOLD).** The relayauth #111 implementation follows this document once it passes contract review. Contract v1 and its vectors are unchanged.

- Data contract: `relayhistory-export-plan-entry-v2.md` (96BullSessions #79).
- Vectors for this document: [`relayhistory-export-plan-entry-v2-issuer.vectors.json`](relayhistory-export-plan-entry-v2-issuer.vectors.json).

Terms used below:

- **MAX** is `Number.MAX_SAFE_INTEGER` = 2^53 − 1.
- **Canonical JSON** is v1's: keys sorted, no whitespace, `JSON.stringify` values.
- **Canonical id** means one leading `relay_` is stripped, and the result must match `^[A-Za-z0-9._:-]{1,255}$`.

## 1. Plan input and the legacy conditional (executable)

The grant-creation request carries `plan: {exported_at, manifests, backfills?}`. The issuer runs the following, in order:

```
input = plan
if "backfills" not in input  OR  input.backfills is an empty array:
    # Legacy (v1) path: byte for byte what #111 does today.
    stored = {v: 1, grant_id, exported_at, manifests: normalize_v1(input.manifests)}   # no "backfills" key
    digest = SHA-256(canonical(stored))
    pairs  = sorted unique (source_user_id, workspace_id) of stored.manifests
    limits, serialization, identities and verification are v1's, unchanged
elif input.backfills is a non-empty array:
    stored = {v: 2, grant_id, exported_at,
              manifests: normalize_v1(input.manifests),     # may be []
              backfills: normalize_v2(input.backfills)}     # 1..1000 entries
    digest = SHA-256(canonical(stored))
    pairs  = sorted unique pairs over manifests AND backfills
else:
    400 invalid_plan            # backfills present but not an array, or null
```

Three cases follow from this:

- **Absent `backfills`, or `backfills: []`:** the issuer stores the exact v1 object, with no `backfills` key and no `v: 2`. Its digest equals today's.
- **A non-empty list:** the issuer stores the v2 object, sorted by key. This is the only case that uses `v: 2`.
- **The vectors:** `legacy.absent`, `legacy.empty` and `v2_plan` cover the three cases.

The issuer must reject the following with 400 `invalid_plan`:

- an entry count above 1000 in total across `manifests` and `backfills`;
- a duplicate key across the two lists;
- a `backfills` that is not an array;
- any v2 entry failing §2.

## 2. v2 entry input (`normalize_v2`)

The input has exactly these keys:

```
{ v: 2, kind: "backfill", key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at}, format: "ndjson",
  chunks: [{start_offset, bytes, sha256, records}] }
```

### Rules

Any violation gives 400 `invalid_plan`.

- **Ids:**
  - `org_id`, `workspace_id` and `source_user_id` are canonical, and `org_id` equals the grant's org.
  - The session strings are 1 to 256 characters.
  - `started_at` is RFC 3339, normalized as in v1.
- **Key:** the v1 manifest key shape, without any prefix. Its `ws=`, `m=`, `h=`, `s=` and `d=` segments equal `workspace_id`, `machine_id`, `harness`, `session_id` and `started_at`'s UTC date.
- **Format:** `format` is `"ndjson"`.
- **Chunk count:** 1 to 4096 chunks per entry, and at most 50,000 chunks per plan across both lists.
- **Chunks:** each chunk has exactly `{start_offset, bytes, sha256, records}`. Every number is a JSON integer, never a float, a string or an exponent:
  - `start_offset`: 0 ≤ `start_offset` ≤ MAX, and contiguous: the first is 0 and each next one is the previous `start_offset + bytes`.
  - `bytes`: 1 ≤ `bytes` ≤ MAX, with `start_offset + bytes` ≤ MAX. The issuer computes this as an exact integer check and rejects on overflow.
  - `records`: **mandatory**, 0 ≤ `records` ≤ MAX. There is no default.
  - `sha256`: 64 lowercase hex.
- **Issuer-derived fields:** the issuer adds `grant_id`, `backfill_sha256` and `raw_bytes`.
  - `backfill_sha256` is the manifest v1 content hash chain over the chunks with every `epoch` set to 0.
  - `raw_bytes` is the sum of `bytes`, at most MAX; this equals the last chunk's `start_offset + bytes`.
  - **A client-supplied `grant_id`, `backfill_sha256` or `raw_bytes` is rejected.**

### The stored v2 entry

```
{ v: 2, kind: "backfill", grant_id, key, org_id, workspace_id, source_user_id,
  session, format: "ndjson", chunks, backfill_sha256, raw_bytes }
```

- **`entry_sha256`** = `SHA-256(canonical(stored_entry))`. The vector is `backfill_entry_sha256`.

## 3. The two v2 token classes

The two classes share nothing that could let one stand in for the other: not a token class, audience, scope, meta key or replay namespace.

|                                | v2 commit assertion                 | v2 base-read capability                         |
| ------------------------------ | ----------------------------------- | ----------------------------------------------- |
| mint `upload` selector         | `{key, base}`                       | `{key, kind: "backfill_base_read"}`             |
| `sub`                          | `agent_relayhistory_migration`      | `agent_relayhistory_migration_base_read`        |
| `meta.tokenClass`              | `migration_assertion`               | `migration_base_read`                           |
| `scopes`                       | exactly `["rth:sync"]`              | exactly `["rth:migration_base_read"]`           |
| `aud`                          | exactly `[<origin>]`                | exactly `[<origin>/v1/pipeline/manifests/base]` |
| TTL (`exp − iat`)              | ≤ 60 s                              | ≤ 30 s                                          |
| meta identity key              | `meta.upload` (§5)                  | `meta.read` (§4); `meta.upload` must be absent  |
| `jti` prefix                   | `tok_…` (as v1)                     | `mbr_…`                                         |
| replay namespace at the ingest | `sessions.migration_assertion_uses` | `sessions.migration_base_read_uses`             |

### Rules for both classes

- **The rest of the claims are v1's:** `org`, `wks`, `sponsorId`, `sponsorChain`, `token_type: "access"`, `iat`, `exp`, a nonce in meta, and `grantId`, `planDigest`, `sourceUserId`, `workspaceId`, `actorId` and `actorOrgId` in meta.
- **Mint checks:**
  - The key is looked up in the grant's stored plan, in `backfills`. A missing key gets 403 `migration_upload_not_allowed`.
  - The entry's pair must equal the request's canonical pair, else 403 `migration_pair_not_allowed`.
  - The live checks and revocation rules are v1's.
- **Tenancy and identity come only from the stored entry and the grant.** A request carrying any other `upload` key gets 400 `invalid_upload_selector`, and so does a request carrying both `kind` and `base`.
- **There is no listing or search endpoint.**

## 4. The base-read capability: `meta.read`

```
{ v: 2, kind: "backfill_base_read", grant_id, plan_digest, key, org_id, workspace_id,
  source_user_id, session: {machine_id, harness, session_id, started_at}, entry_sha256 }
```

- Every field is derived from the stored entry and the grant. The vector is `base_read_meta_read`.

### The ingest's `GET /v1/pipeline/manifests/base`

- **Activation:** the route is **default-off** (§9). Requests must be **exactly** `GET` on `/v1/pipeline/manifests/base`, with **exactly one** query parameter, `key`. This is the signed unprefixed key, URL-encoded.
- **Verification:**
  - The verifier checks the class, audience, scope, TTL and 5 s skew.
  - The signed pair must equal the grant pair, `meta.read.*` must equal the signed tenancy and the grant/plan claims, and the `key` parameter must equal `meta.read.key`.
  - The deployment prefix is applied by the server's one prefix function (§7).
- **Atomic validate, consume and audit:**
  1. Read a **snapshot** of the stored manifest object at the server-prefixed key: its bytes and version from one storage read.
  2. Validate the snapshot:
     - it must exist (else 404, and nothing is consumed);
     - it parses;
     - its session equals `meta.read.session`;
     - the owner does not start with `migration:`;
     - there is no `backfill` epoch;
     - the format is `ndjson`.

     A snapshot that fails any check gets 409 `migration_base_unusable`, and nothing is consumed.

  3. In **one database transaction**:
     - take the grant advisory lock;
     - delete expired rows (bounded batch);
     - check grant consistency;
     - insert the capability's `jti` and `nonce` into `sessions.migration_base_read_uses`, insert-if-absent on both;
     - append the audit row (§8).

     **Exactly one** concurrent request with a given `jti` or nonce wins. A loser gets 409 `migration_assertion_replayed`.

  4. **Only after the commit** does the route respond: 200 `{base: {revision, version, owner, writer, chunk_count, epoch, content_sha256}}`, computed from the snapshot of step 1. On a transaction or audit failure it returns 503 `migration_ledger_unavailable` with no base in the body.

- **The base read is single use.** If its response is lost, the courier mints a fresh capability and reads again. The old one is spent, and the read changes nothing.

## 5. The v2 commit identity: `meta.upload`

```
{ v: 2, kind: "backfill", grant_id, plan_digest, key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at}, backfill_sha256, entry_sha256,
  base: {revision, version, owner, writer, chunk_count, epoch, content_sha256} }
```

- **Origin of `base`:** the courier copies it verbatim from the base read.
- **Issuer checks on `base`:** the shape only, as below, else 400 `invalid_upload_selector`. The issuer cannot vouch for the base; the ingest compares it exactly with storage.

### Bounds on `base`

| field             | bound                                                                                         |
| ----------------- | --------------------------------------------------------------------------------------------- |
| `revision`        | 1 ≤ `revision` ≤ MAX − 1, so that `revision + 1` stays safe                                   |
| `chunk_count`     | 0 ≤ `chunk_count` ≤ MAX − 4096, so that every `chunk_count + i` stays safe                    |
| `epoch`           | 0 ≤ `epoch` ≤ 4294967294; the manifest chain writes epochs as uint32, so `epoch + 1` must fit |
| `version`         | a string of 1 to 512 characters (see below)                                                   |
| `owner`, `writer` | strings of 1 to 512 characters; `owner` must not start with `migration:`                      |
| `content_sha256`  | 64 lowercase hex                                                                              |

- **What `version` holds:** the object store's **version id** when the store reports one (an S3 `VersionId`, a GCS `generation`). Otherwise it is the **ETag exactly as the store returns it, quotes included**. The ingest computes it with the same rule (`version ?? eTag`).

### The commit's compare-and-swap

The commit body's `previousVersion` (`{eTag?, version?}`) must name the stored object's current version. That means both of these hold:

- `sameVersion(stored.version, previousVersion)`;
- `version ?? eTag` of the stored object equals `base.version`.

The storage write is conditional on that version (`ifMatch`).

## 6. The two-way route matrix (ingest)

Routes are matched **exactly** on method and path:

- The method is case-sensitive, so `get` is not `GET`.
- Exactly one trailing slash is removed before matching, for all routes alike, and no other normalization is done. `/base//`, `/base/x` and `/base;x` do not match.
- The query string never selects the route.

| token \ route                     | `POST /v1/pipeline/chunks` | `POST /v1/pipeline/manifests` | `GET /v1/pipeline/manifests/status` | `GET /v1/pipeline/manifests/base` | `/v1/cli/login` | any other route |
| --------------------------------- | -------------------------- | ----------------------------- | ----------------------------------- | --------------------------------- | --------------- | --------------- |
| v1 upload assertion               | its own checks             | its own checks                | its own checks                      | **403**                           | **403**         | **403**         |
| v2 commit assertion               | its own checks             | its own checks                | its own checks                      | **403**                           | **403**         | **403**         |
| v2 base-read capability           | **403**                    | **403**                       | **403**                             | its own checks (§4)               | **403**         | **403**         |
| live courier / ordinary assertion | as today                   | as today                      | as today                            | **403**                           | as today        | as today        |
| generic RelayAuth bearer          | as today                   | as today                      | as today                            | **403**                           | as today        | as today        |

- **The other methods** (`HEAD`, `POST`, `PUT` and `DELETE`) on `/base` get 403 for every token. So do `HEAD` and `POST` on `/status`.
- **Base-read checks at the route:**
  - a request to `/base` with **any query parameter other than exactly one `key`** gets 400;
  - a base-read capability whose `meta.read.key` is not the requested key gets 403 `migration_upload_forbidden`.
- **Refusal codes:**
  - 403 `migration_assertion_route` when the route is wrong;
  - 403 `migration_token_class_forbidden` when the class is right but the route is not its own;
  - 401 `invalid_claims` when the shape is malformed.

## 7. Prefix and key identity

There is one server function, `storageKey(env, auth.orgId, signedUnprefixedKey)`:

```
base = PIPELINE_KEY_PREFIX with leading and trailing "/" stripped
if PIPELINE_ORG_FROM_PREFIX == "true":  prefix = base ? base + "/" + org : org
else:                                   prefix = base
storageKey = prefix ? prefix + "/" + key : key
```

- **Where it is used:** the base read, the chunk route (for the chunk's session prefix), the commit and status, all with the same signed unprefixed key.
- **A client-supplied prefix** is accepted only when it equals `prefix` exactly. Anything else gets 403.
- **`prefixAllowed` is exact:** with org-derived prefixes the prefix must be exactly `<base>/<org>`, or `<org>` when no base is set (#69).

## 8. Failure and edge semantics; audit; retention; minimization

| event                                                                   | outcome                                                                                                                                                                                                                                 |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| base changes between the base read and the commit (a live write landed) | the stored manifest is no longer the signed base: 403 `migration_upload_forbidden`, or 409 `revision_conflict` from the CAS; nothing consumed; the courier reads the base again (a fresh capability) and mints a fresh commit assertion |
| base-read replay (same `jti` or nonce)                                  | 409 `migration_assertion_replayed`; no base returned                                                                                                                                                                                    |
| concurrent base reads with the same capability                          | exactly one 200; the others 409                                                                                                                                                                                                         |
| ledger or audit failure on a base read                                  | 503 `migration_ledger_unavailable`; nothing consumed; no base returned                                                                                                                                                                  |
| lost base-read response                                                 | the capability is spent; mint a fresh one; reading is side-effect-free otherwise                                                                                                                                                        |
| commit race (two commits on the same base)                              | the storage CAS lets at most one land; the other gets 409 `revision_conflict`, or 403 once the base moved                                                                                                                               |
| lost commit response                                                    | an identical retry with a fresh commit assertion on the **same** base returns 202 with the same work key `key@entry_sha256@r<revision+1>` (the identical-object branch); a rebuilt append on a newer base is new work with a new `@r`   |
| ledger or audit failure on a commit                                     | 503; the replay row, job and audit row all roll back together                                                                                                                                                                           |

**Audit.** Both are append-only and hold no token, signature, nonce, `jti`, chunk bytes or manifest content.

- **Issuer issuance audit (relayauth):** one record per mint attempt with:
  - `grant_id`, `upload_kind` (`v1` / `backfill_base_read` / `backfill`), `SHA-256(key)` and `entry_sha256`;
  - `base.revision` for a commit identity;
  - the result (`issued` or the refusal code).
- **Ingest audit:** `sessions.migration_ingest_audit` (an existing table) with the result `base_read` added. A row records `grant_id`, `plan_digest`, `entry_sha256`, the org, workspace and user, and the result. For base reads, `work_key` is `sha256(key) + "@base_read@r" + base.revision`, so the key's machine and session ids are not stored. Commits keep their work key.
- **Retention:** the export's lifetime plus 400 days, removed only by a reviewed owner migration. Replay rows (both namespaces) follow the existing delete-on-insert retention: a bounded batch, a 5 minute margin past `exp + 5 s`.
- **Metadata minimization:** a base-read response carries exactly the 7 fields. It never returns the manifest, chunk list, extensions or any other session's data.

## 9. Deploy and activation

| gate                                              | default | effect                                                                                            |
| ------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------- |
| ingest `RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS` | off     | v1 and v2 commit assertions                                                                       |
| ingest deployment flag `migrationBackfill`        | off     | v2 commit assertions on chunks, commit and status; off gives 403 `migration_backfill_unsupported` |
| ingest `RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ`  | **off** | the `/base` route and the base-read capability; off gives 404 for `/base`                         |
| relayauth migration-base-read issuance            | **off** | the `{key, kind: "backfill_base_read"}` selector; off gives 403 `migration_base_read_disabled`    |

- **Activation order:**
  1. Deploy all code with everything off.
  2. Enable relayauth issuance for one grant.
  3. Enable the ingest base read.
  4. Enable `migrationBackfill`.

  Each step needs Khaliq's explicit go. The relayhistory-cloud Worker never enables `migrationBackfill` or the base read.

## 10. Test plan

Each item below needs at least one test in the named repo.

- **Compatibility:**
  - relayauth: absent and `[]` backfills store the exact v1 bytes and digest (`legacy` vectors);
  - every existing v1 vector and test still passes unchanged;
  - ingests: v1 behaviour unchanged.
- **Vectors:** relayauth and both ingests reproduce this file's vectors and the v2 data vectors.
- **Tenant and pair isolation:** refusals at mint and at the ingest for:
  - another org, user or workspace;
  - another key in the same plan;
  - another grant's key;
  - a capability for org A used under org B's prefix.
- **Tamper:** any changed `meta.read`, `meta.upload` or `base` field is refused, and so is an extra key. Other tamper cases:
  - a client-supplied `backfill_sha256` or `raw_bytes` at mint;
  - a chunk with no `records`;
  - non-contiguous offsets.
- **Bounds:** MAX, MAX + 1, floats, strings and negative values for every number in §2 and §5, plus overflow of `start_offset + bytes` and of `revision + 1` / `chunk_count + i` / `epoch + 1`.
- **Prefix:** the base read, chunks, commit and status under a static prefix, under `<base>/<org>`, and with a bare org; wrong, extra and absent client prefixes.
- **Token-class confusion:**
  - a base-read capability presented as an upload assertion, and the reverse;
  - a `meta.read` put inside `meta.upload`;
  - the base-read audience or scope on an upload token, and the upload audience on a base-read token.
- **Route matrix:** every cell of §6, plus method case, trailing-slash, path and query variants.
- **Atomic failures:**
  - the audit insert fails on a base read: 503, nothing consumed, no base;
  - a crash before COMMIT on a commit: the replay row, job and audit roll back.
- **Races:**
  - concurrent base reads with one capability: exactly one 200;
  - a live write between the base read and the commit: the commit is refused, then succeeds after a re-read;
  - two commits on one base: one lands;
  - a lost commit response: an identical retry returns the same work key.
