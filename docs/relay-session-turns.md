# Relay session turns off Neon (the last live Neon writer)

`POST /v1/sessions/:id/turns` (relayhistory-cloud `routes/turns.ts`, called by the agent-relay
`@agent-relay/session` SessionClient) is the last route that still INSERTs into Neon
(`sessions.accept_conversation_turns` → `sessions.conversation_turns`). This change lets
relayhistory send the same turns to this ingest instead, as `conversation_turn` records in the
session's S3 stream, through the normal buffered path.

## Pieces
| Repo | What |
|---|---|
| sessions-pipeline `src/session-ingest/turns.ts` | The record shape and `parseTurnBatch` (strict; tenant-bound; record id derived from the row key). |
| sessions-pipeline `src/session-ingest/app.ts` | `POST /v1/turns/batches`: same signed binding identity, `rth:sync`, custody (relayhistory envelope, shadow, or frozen-supabase), claim, buffer stage and receipt as `/v1/delivery/batches`. `conversation_turn` is not a public `DeliveryKind`: the public route still answers 422 `unsupported_evidence` for it. |
| relayhistory-cloud `lib/turns-forward.ts` | `TURNS_WRITE_PATH` = unset/`neon` (default, today) \| `dual` \| `forward`; `TURNS_FORWARD_ORGS` canary allowlist. |

## Record shape (v1)
- `origin_id` `relayhistory:conversation_turns:v1` (one synthetic origin; Neon delivery has never seen it, so it has no legacy receipt or S3 marker).
- `kind` `conversation_turn`, `operation` `upsert`, `source` = the turn's resolved Neon source, `session_id` = the path id.
- `record_id` = `"turn:" + sha256hex(JSON.stringify([workspace_id, source, session_id, turn_index]))`, with `workspace_id` = the token's workspace or `default` (Neon's key). Pinned vector: `("default","claude","s-1",0)` → `turn:561b1d69…13ebb` in both repos. One record per Neon row key, so a re-POST of an index replaces it.
- `revision` = relayhistory's strictly increasing microsecond clock; a later write wins (the projection fold and the ledger's stale filter).
- `payload` = the Neon row: `id, org_id, workspace_id, source, session_id, turn_index, session_owner, role, content, actor_name, actor_role, metadata_json, ts, tagged`. No `ts_ms`: a Luhn-valid 13-digit number would be nulled by the delivery card scrub.
- Text (`content`, `session_owner`, `actor_name`) travels as the client sent it and is scrubbed once here by the same scrubber Neon's path uses (`packages/pipeline/src/lib/scrub.ts` is byte-identical to relayhistory's `lib/scrub.ts`). `source` and `metadata_json` are Neon's scrubbed values (the source is resolved from them). Known, deliberate differences from Neon: metadata keys `raw`, `raw_json`, `raw_path`, `raw_locator` are dropped (the delivery redaction barrier); a single turn too large for a 1.4 MB batch travels pre-scrubbed and is re-bounded, so text past the 262,144-character scrub limit carries a different truncation note.
- The projection ignores the kind (no transcript events, no catalog rows: `unprojected`).

## Modes (relayhistory)
| Mode | Neon | Ingest | Response | Reads |
|---|---|---|---|---|
| `neon` (default) | write | none | Neon's | Neon |
| `dual` | write, authoritative | background (`waitUntil`) forward of what Neon holds after the write committed (read back by key: commit order, Neon's ids; fenced or erased keys never copied); a re-read after the ack logs `erased_during_forward` | Neon's, unchanged | Neon |
| `forward` | none; reads Neon's erasure fence and drops what `accept_conversation_turns` would | required; one batch per ≤500 rows | 200 only after every batch's durable receipt (batch id, every revision id), else 503 `turns_unavailable`; refuses (`turn_fence_unavailable`) without a readable fence | Neon (stale!) |

Under relayhistory custody (`DELIVERY_CUSTODY_SOURCE_MODE` unset), the forward still READS Neon (custody witness, cutover state), like a delivery batch. Only `frozen-supabase` takes Neon off it entirely.

## Activation gates
`dual` can be turned on now (it changes no answer). `forward` must not be turned on until all of these are true:
1. **Read-your-writes.** The SessionClient reads its write back from `GET /v1/sessions/:id/turns` immediately and retries into the next index when it is missing. That GET reads Neon today, so in `forward` mode every `writeTurn` fails (test: "forward: documents the activation gate"). Needs the read side (turns from S3 in the read Worker) AND a read path that sees buffered, not-yet-published stages, or a SessionClient change that tolerates eventual consistency.
2. **Erasure fence off Neon.** Neon drops turns for sessions in `conversation_turn_fences` (any member's erasure). relayhistory's `forward` reads that fence from Neon today, and refuses under frozen-supabase custody. The ingest's own deletion fences are per origin, so the synthetic origin is not covered. Removing the Neon read needs the fences copied to Supabase plus a cross-origin session fence check on this route (a Supabase function; migration slot 20261105030000 is reserved for it).
3. **Custody.** `forward` with relayhistory custody still reads Neon; flip `frozen-supabase` first (docs/p1-frozen-source-custody.md).
4. **Erasure during a forward.** A `turns forward` line with `erased_during_forward: true` names a session whose copy reached the buffered stage after its erasure committed. Purge those copies before publication is enabled.
5. **Publication.** Records reach canonical images and directory pointers only when the publication drain runs. A turn-only relay session (the SessionClient's random UUID) then gets a `session_index` row and would appear in the directory list (Neon never listed them). The list must exclude turn-only images before publication is enabled for orgs using turns.
6. **Backfill.** Historical rows come from the legacy export (`legacy_conversation_turn`, PR 2). Read-side merge rule: per (workspace, source, session, turn_index) a live `conversation_turn` wins over a legacy row. Once `dual` is on, never go back to `neon` without re-running the export for the affected sessions.

## Rollback
Unset `TURNS_WRITE_PATH` (live in seconds). Records already forwarded stay in the buffered stage and are inert until publication; Neon stays complete in `dual`.
