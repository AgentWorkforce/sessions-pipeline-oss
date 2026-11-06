# Dormant canonical session projection reader

The existing Cloudflare read Worker gains `GET /v1/canonical/sessions/:id`.
It reads S3 canonical records through the same reader and projection as the S3
search writer. It does not read Neon or an obsolete Neon-fed read-store table.
This is a provider protocol for History adapters, not a declaration that every
public session API has migrated.

The endpoint is disabled unless `CANONICAL_READ_ENABLED=true`. It needs:

- `CANONICAL_SUPABASE_URL` and `CANONICAL_SUPABASE_API_KEY` (the REST gateway key,
  not service-role/owner credentials).
- Existing `PIPELINE_RAW_S3_BUCKET`, region/endpoint and read credentials; the
  raw store may be S3 or R2. Use a scoped read-only object policy.
- Explicit `PIPELINE_KEY_PREFIX` / `PIPELINE_ORG_FROM_PREFIX` matching the admitted
  pointer/object deployment. No client-supplied bucket or key is accepted.

No configuration, credential, grant or resource is applied by this change. Use
the reviewed deployment/configuration procedure and its inverse. Existing
ClickHouse read paths are unchanged. Canonical paths route before CH/Hyperdrive
configuration checks because they do not need those services.

The caller sends an existing short-lived `session_directory_search_worker`
JWT, minted/exchanged by History's current directory transport. The Worker's
initial decode is untrusted: it may only select the claim-bound pointer RPC.
PostgREST must successfully verify that SAME bearer through
`session_directory_pointers(workspace,harness,session_id)` before any S3 read.
The RPC pins role/issuer/audience/lifetime and authenticated tenant. Every returned
pointer is independently checked against org/workspace/source/session, provider,
bucket, raw format and revision. The Worker never accepts a client object URL.

The canonical-identity helper validates original IDs against their actual encoded
object-layout IDs. The pointer's `content_sha256` is the manifest's raw chunk hash
chain, not a digest of manifest JSON. Manifest parsing verifies that chain, while
the shared reader verifies stored/plaintext chunk digests and folds highest
canonical record revisions/tombstones with the authoritative projection. Source
sets are reread after hydration; drift refuses the read. No incomplete/partial
projection is returned as a complete session.

Query parameters are `workspace`, `source`, `table`, `limit`, `order`, and opaque
`cursor`, `view` and `maxContent`. A source is required. Tables are the six existing projection tables plus the three explicitly scoped legacy evidence tables described in canonical-legacy-source.md.
Convergence rows sort by exact timestamp/event identity in either direction.
Cursors bind tenant/session/source, source generation, table, order, view, content limit and offset.
A changed generation requires restarting pagination; it never silently continues
against another revision or another backend. Each response attests its workspace
and carries the exact identity/generation, projected rows and next cursor.

Bounds are explicit: whole read 8 seconds, metadata/reply page 2 MiB, manifest
1 MiB, complete raw scope 16 MiB and 100,000 records. Raw projected rows above
the response bound refuse the request; otherwise a byte-short page has a cursor.
The explicit `view=replay` on convergence events removes only duplicate raw
payload, retaining every flat projected field plus the native message ID. Its
optional positive `maxContent` preserves the public replay truncation semantics
by retaining one extra character for the History mapper. A first replay row may
exceed the normal page target up to 64 MiB, preserving the existing first-event
exception; subsequent rows continue on a cursor. Tool arrays, task fields and
usage counters are not truncated. Workerd fixtures exercise a greater-than-2MiB
first event. The total source bounds still require capacity proof.
The existing pointer RPC has LIMIT64 without a sentinel, so the reader refuses
64 pointers rather than treating a potentially truncated set as complete. These
limits need production coverage/latency proof before activation; oversized
sessions are a migration blocker, not an excuse to fall back to Neon.

Error bodies expose only generic code/message/correlation id; the
`X-Session-Read-Reason` header carries a bounded reason. Upstream error bodies are
cancelled. Metadata oversize and abort cancel the successful stream too. No
prompt, raw record, token or object-store error message is logged.

The new canonical tests use synthetic source objects and independently reject
foreign tenants, pointer digest drift, generation races, forged JWTs before S3,
RPC truncation and oversized bodies. A separate offline workerd suite proves
real fzstd decoding and canonical projection in the Worker without CH/PG access.

Public replay/catalog/timing/output/thread adapters and source coverage remain
separate gates. Current canonical exports do not contain legacy session_links,
session_outcomes or conversation_turns tables. Do not fabricate empty outcomes,
links or legacy messages when implementing those public contracts.
