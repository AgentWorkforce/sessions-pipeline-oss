/**
 * EXTRACTED (not verbatim) from AgentWorkforce/relayhistory-cloud
 * packages/relayhistory/src/lib/delivery.ts at the commit in SOURCE.json:
 * lines 37-538 (request validation, normalization, digests, servicePayload),
 * 700-731 (deliveryAccount, requireDeliveryAccount, PreparedDelivery) and
 * 740-794 (prepareDelivery), byte for byte. Only this header and the imports below differ: the Neon half of
 * that file (commitDelivery, listDelivery, the conflict decoders) is not here,
 * and `recordDeliveryCardColumnNulled` is a local counter instead of
 * relayhistory's request-stats store. `AuthContext` is the subset
 * prepareDelivery reads. Re-sync with scripts/vendor-relayhistory-delivery.ts;
 * the unit test checks every line against SOURCE.json's digests.
 */
import { sessionImage } from "./session-image.ts";
import { exceedsScrubBound, scrubNumber, scrubText } from "@rhc/lib/scrub.ts";
import {
  DELIVERY_KINDS,
  DELIVERY_MAPPING_VERSION,
  DeliveryError,
  MAX_DELIVERY_RECORDS,
  MAX_DELIVERY_RECORD_BYTES,
  RETIRED_DELIVERY_KINDS,
  retiredDeliveryRecord,
  type DeliveryKind,
  type StoredDeliveryReceipt,
  type HistoryExportBatch,
  type HistoryExportRecord,
} from "./delivery-contracts.ts";

export interface AuthContext {
  orgId: string;
  workspaceId?: string | null;
}
let cardColumnsNulled = 0;
function recordDeliveryCardColumnNulled(): void {
  cardColumnsNulled += 1;
}
/** Card-number columns nulled since the last call (counted on the request log line). */
export function takeCardColumnsNulled(): number {
  const n = cardColumnsNulled;
  cardColumnsNulled = 0;
  return n;
}
// ---- relayhistory delivery.ts lines 37-538 ----
const encoder = new TextEncoder();
function invalid(message: string): never {
  throw new DeliveryError("invalid_delivery", 400, message);
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
/**
 * An unpaired UTF-16 surrogate, which PostgreSQL refuses to convert to `jsonb`.
 *
 * JSON carries these as `\ud800` escapes, so the bounded UTF-8 decode never sees one and
 * `JSON.parse` hands back a string no `::jsonb` cast will accept. Left unchecked the
 * failure surfaces on the database call and is reported as a 503, so a client retries a
 * batch that can never be accepted. These are rejected as invalid input instead.
 */
const LONE_SURROGATE =
  /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
function wellFormed(value: string): boolean {
  return !LONE_SURROGATE.test(value);
}
function identifier(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    encoder.encode(value).length <= 512 &&
    !/[\x00-\x1f\x7f]/.test(value) &&
    wellFormed(value)
  );
}
function positive(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}
const BABYSITTER_RETENTION_CLASS = "standard_30d";
const MAX_BABYSITTER_RETENTION_MS = 31 * 24 * 60 * 60 * 1000;
const CONTROL_PLANE_REF = /^[A-Za-z0-9][A-Za-z0-9._:/#@-]{0,199}$/;
const CONTROL_PLANE_KIND = /^[a-z][a-z0-9_-]{0,31}$/;
const SHA_1 = /^[a-f0-9]{40}$/;
const SHA_256 = /^[a-f0-9]{64}$/;
const GITHUB_REF = /^[A-Za-z0-9_.-]{1,100}\/[A-Za-z0-9_.-]{1,100}#[1-9][0-9]*$/;
const GITHUB_REF_PARTS =
  /^([A-Za-z0-9_.-]{1,100})\/([A-Za-z0-9_.-]{1,100})#([1-9][0-9]*)$/;
const GITHUB_ISSUE_URL =
  /^https:\/\/github\.com\/([A-Za-z0-9_.-]{1,100})\/([A-Za-z0-9_.-]{1,100})\/issues\/([1-9][0-9]*)$/;
const GITHUB_PR_URL =
  /^https:\/\/github\.com\/([A-Za-z0-9_.-]{1,100})\/([A-Za-z0-9_.-]{1,100})\/pull\/([1-9][0-9]*)$/;
function exact(value: Record<string, unknown>, fields: string[]) {
  if (Object.keys(value).some((key) => !fields.includes(key)))
    invalid("Unknown delivery envelope field");
}
function inspect(value: unknown, depth = 0): void {
  if (depth > 24) invalid("Delivery payload nesting exceeds 24 levels");
  if (typeof value === "number" && !Number.isFinite(value))
    invalid("Delivery numbers must be finite");
  if (typeof value === "string" && value.includes("\0"))
    invalid("Delivery strings cannot contain null characters");
  if (Array.isArray(value)) for (const item of value) inspect(item, depth + 1);
  else if (object(value))
    for (const [key, item] of Object.entries(value)) {
      inspect(key, depth + 1);
      inspect(item, depth + 1);
    }
}
function optional(value: unknown, predicate: (input: unknown) => boolean) {
  return value === undefined || value === null || predicate(value);
}
function controlPlaneRef(value: unknown): boolean {
  return typeof value === "string" && CONTROL_PLANE_REF.test(value);
}
function exactString(expected: string) {
  return (value: unknown) => value === expected;
}
function oneOf(...expected: string[]) {
  return (value: unknown) =>
    typeof value === "string" && expected.includes(value);
}
function natural(value: unknown): boolean {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function matches(pattern: RegExp) {
  return (value: unknown) => typeof value === "string" && pattern.test(value);
}
function sameGithubTarget(
  ref: unknown,
  url: unknown,
  urlPattern: RegExp,
): boolean {
  if (typeof ref !== "string" || typeof url !== "string") return false;
  const refParts = GITHUB_REF_PARTS.exec(ref);
  const urlParts = urlPattern.exec(url);
  return Boolean(
    refParts &&
    urlParts &&
    refParts[1].toLowerCase() === urlParts[1].toLowerCase() &&
    refParts[2].toLowerCase() === urlParts[2].toLowerCase() &&
    refParts[3] === urlParts[3],
  );
}
type BabysitterKind = "session_lineage" | "turn_receipt";
const BABYSITTER_RETENTION = {
  retention_class: exactString(BABYSITTER_RETENTION_CLASS),
  expires_at_ms: positive,
} satisfies Record<string, (value: unknown) => boolean>;
/**
 * These two evidence kinds are control-plane joins, not transcript storage.
 * Every retained string therefore has field-specific reference syntax; generic
 * secret scrubbing is defense in depth, never the boundary that keeps prose out.
 * Their payloads retain exactly these fields, because an undeclared field would
 * carry a string no reference syntax constrains.
 */
const BABYSITTER_FIELDS: Record<
  BabysitterKind,
  Record<string, (value: unknown) => boolean>
> = {
  session_lineage: {
    id: controlPlaneRef,
    lineage_id: controlPlaneRef,
    source_session_source: (value) =>
      optional(value, matches(CONTROL_PLANE_KIND)),
    source_session_id: (value) => optional(value, controlPlaneRef),
    source_transcript_id: (value) => optional(value, controlPlaneRef),
    source_prompt_ref: (value) => optional(value, controlPlaneRef),
    source_prompt_digest: (value) => optional(value, matches(SHA_256)),
    issue_provider: (value) => optional(value, exactString("github")),
    issue_ref: (value) => optional(value, matches(GITHUB_REF)),
    issue_source_ref: (value) => optional(value, controlPlaneRef),
    issue_url: (value) => optional(value, matches(GITHUB_ISSUE_URL)),
    garden_run_id: (value) => optional(value, controlPlaneRef),
    coding_session_harness: matches(CONTROL_PLANE_KIND),
    coding_session_id: controlPlaneRef,
    coding_transcript_id: (value) => optional(value, controlPlaneRef),
    pr_provider: exactString("github"),
    pr_ref: matches(GITHUB_REF),
    pr_url: matches(GITHUB_PR_URL),
    created_at_ms: natural,
    updated_at_ms: natural,
    ...BABYSITTER_RETENTION,
  },
  turn_receipt: {
    id: controlPlaneRef,
    lineage_id: controlPlaneRef,
    delivery_id: controlPlaneRef,
    status: oneOf("queued", "delivered", "completed", "failed"),
    head_sha: matches(SHA_1),
    sequence: positive,
    queued_at_ms: natural,
    delivered_at_ms: natural,
    completed_at_ms: natural,
    coding_session_harness: matches(CONTROL_PLANE_KIND),
    coding_session_id: controlPlaneRef,
    transcript_id: (value) => optional(value, controlPlaneRef),
    ...BABYSITTER_RETENTION,
  },
};
function babysitter(kind: unknown): kind is BabysitterKind {
  return kind === "session_lineage" || kind === "turn_receipt";
}
function assertBabysitterEvidence(
  kind: BabysitterKind,
  payload: Record<string, unknown>,
) {
  const validators = BABYSITTER_FIELDS[kind];
  for (const [field, validate] of Object.entries(validators))
    if (!validate(payload[field]))
      invalid(`Invalid ${kind} ${field} reference`);
  if (kind === "session_lineage") {
    const issueValues = [
      payload.issue_provider,
      payload.issue_ref,
      payload.issue_url,
    ];
    if (
      issueValues.some((value) => value !== undefined && value !== null) &&
      (issueValues.some((value) => value === undefined || value === null) ||
        !sameGithubTarget(
          payload.issue_ref,
          payload.issue_url,
          GITHUB_ISSUE_URL,
        ))
    )
      invalid("Issue reference and URL must identify the same GitHub issue");
    if (!sameGithubTarget(payload.pr_ref, payload.pr_url, GITHUB_PR_URL))
      invalid(
        "PR reference and URL must identify the same GitHub pull request",
      );
  }
}
function assertDeliveryRequest(
  value: unknown,
): asserts value is { protocolVersion: 1; batch: HistoryExportBatch } {
  if (!object(value)) invalid("Delivery request must be an object");
  exact(value, ["protocolVersion", "batch"]);
  if (value.protocolVersion !== 1)
    throw new DeliveryError(
      "unsupported_protocol",
      422,
      "Delivery protocol version is unsupported",
    );
  const batch = value.batch;
  if (!object(batch)) invalid("Delivery batch must be an object");
  exact(batch, [
    "schema_version",
    "origin_id",
    "batch_id",
    "job_id",
    "generation",
    "destination_id",
    "instance_id",
    "account_id",
    "mapping_version",
    "records",
  ]);
  if (batch.schema_version !== 1)
    throw new DeliveryError(
      "unsupported_schema",
      422,
      "Delivery schema version is unsupported",
    );
  for (const key of [
    "origin_id",
    "batch_id",
    "job_id",
    "destination_id",
    "instance_id",
    "account_id",
    "mapping_version",
  ])
    if (!identifier(batch[key]))
      invalid(
        "Delivery identifiers must be 1..512 UTF-8 bytes without control characters",
      );
  if (batch.mapping_version !== DELIVERY_MAPPING_VERSION)
    throw new DeliveryError(
      "unsupported_mapping",
      422,
      "Delivery mapping version is unsupported",
    );
  if (!positive(batch.generation))
    invalid("Delivery generation must be a positive safe integer");
  if (
    !Array.isArray(batch.records) ||
    !batch.records.length ||
    batch.records.length > MAX_DELIVERY_RECORDS
  )
    invalid(`Delivery batch must contain 1..${MAX_DELIVERY_RECORDS} records`);
  const revisions = new Set<string>();
  for (const record of batch.records) {
    if (!object(record)) invalid("Delivery record must be an object");
    exact(record, [
      "schema_version",
      "origin_id",
      "record_id",
      "revision_id",
      "revision",
      "kind",
      "source",
      "session_id",
      "operation",
      "payload",
    ]);
    if (record.schema_version !== 1)
      throw new DeliveryError(
        "unsupported_schema",
        422,
        "Delivery schema version is unsupported",
      );
    if (
      !DELIVERY_KINDS.includes(record.kind as DeliveryKind) &&
      !retiredDeliveryRecord(record)
    )
      throw new DeliveryError(
        "unsupported_evidence",
        422,
        "Delivery evidence kind is unsupported",
      );
    if (record.origin_id !== batch.origin_id || !positive(record.revision))
      invalid(
        "Record origin must match batch and revision must be a positive safe integer",
      );
    for (const key of ["record_id", "revision_id", "source"])
      if (!identifier(record[key]))
        invalid("Invalid delivery record identifier");
    if (
      record.session_id !== null &&
      record.session_id !== "" &&
      !identifier(record.session_id)
    )
      invalid("Invalid session identifier");
    if (revisions.has(record.revision_id as string))
      invalid("Duplicate revision identity in batch");
    revisions.add(record.revision_id as string);
    if (
      !(record.operation === "delete" && record.payload === null) &&
      !(record.operation === "upsert" && object(record.payload))
    )
      invalid("Record must be an object upsert or null tombstone");
    if (record.operation === "upsert" && babysitter(record.kind)) {
      const retainedUntil = record.payload?.expires_at_ms;
      if (
        record.payload?.retention_class !== BABYSITTER_RETENTION_CLASS ||
        !positive(retainedUntil) ||
        retainedUntil > Date.now() + MAX_BABYSITTER_RETENTION_MS
      )
        invalid(
          "Babysitter evidence requires the bounded standard_30d retention policy",
        );
      assertBabysitterEvidence(record.kind, record.payload);
    }
    inspect(record.payload);
  }
}
export function parseDeliveryRequest(value: unknown): HistoryExportBatch {
  assertDeliveryRequest(value);
  return normalizeDeliveryBatch(value.batch);
}
/**
 * The local change feed reports a NULL session as `''` (`COALESCE(session, '')` in
 * ai-hist `change_feed.rs`, and history tombstones store `''`). Both spellings mean
 * "no session", so `''` is stored as NULL rather than rejected: rejecting it 400'd the
 * whole batch, and a failed batch never advances the cursor, so the machine was wedged.
 *
 * Normalization happens BEFORE any digest is taken, so batch and revision digests are
 * over the normalized record. That keeps replays idempotent whichever spelling a client
 * sends (a client that later switches `''` to `null` for the same revision is the same
 * content, not a 409), and changes no previously stored digest: a record with `''` was
 * always rejected, so none was ever digested. Applying it again is a no-op.
 */
export function normalizeDeliveryBatch(
  batch: HistoryExportBatch,
): HistoryExportBatch {
  if (!batch.records.some((record) => record.session_id === "")) return batch;
  return {
    ...batch,
    records: batch.records.map((record) =>
      record.session_id === "" ? { ...record, session_id: null } : record,
    ),
  };
}
/** Sorted object keys; array order and JSON values remain significant. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value))
    return "[" + value.map(canonicalJson).join(",") + "]";
  if (object(value))
    return (
      "{" +
      Object.keys(value)
        .sort()
        .map((key) => JSON.stringify(key) + ":" + canonicalJson(value[key]))
        .join(",") +
      "}"
    );
  return JSON.stringify(value);
}
export async function digest(value: unknown): Promise<string> {
  return [
    ...new Uint8Array(
      await crypto.subtle.digest(
        "SHA-256",
        encoder.encode(canonicalJson(value)),
      ),
    ),
  ]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}
/**
 * Excluded wherever they appear, at the top level or inside structured evidence.
 * `raw`/`raw_json` are whole provider blobs; `raw_path`/`raw_locator` locate the
 * source transcript on the uploading machine, which the service can never read
 * and which would publish that machine's filesystem layout. Every other field
 * the client sends is retained.
 */
const RAW_FIELDS = new Set(["raw", "raw_json", "raw_path", "raw_locator"]);
/** Local columns with this suffix hold JSON text and are retained as structured evidence. */
const JSON_SUFFIX = "_json";
/** Field names are the local table's column names, never free text. */
const COLUMN = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/;
/** Records whether any string scrubbed while building one payload was truncated. */
interface ScrubState {
  truncated: boolean;
}
function scrubTracked(value: string, state: ScrubState): string {
  if (exceedsScrubBound(value)) state.truncated = true;
  return scrubText(value);
}
function scrubStructured(value: unknown, state: ScrubState): unknown {
  if (typeof value === "string") return scrubTracked(value, state);
  if (typeof value === "number") return scrubNumber(value);
  if (Array.isArray(value))
    return value.map((item) => scrubStructured(item, state));
  if (object(value)) {
    const entries = Object.entries(value).filter(
      ([key]) => !RAW_FIELDS.has(key),
    );
    const keys = entries.map(([key]) => scrubTracked(key, state));
    if (new Set(keys).size !== keys.length)
      invalid("Structured evidence keys collide after scrubbing");
    return Object.fromEntries(
      entries.map(([, item], index) => [
        keys[index],
        scrubStructured(item, state),
      ]),
    );
  }
  return value;
}
/**
 * Checked on the scrubbed text because that is what is stored, but what it rejects is
 * always the client's own malformed input: `boundForScrub` keeps surrogate pairs whole,
 * so a well-formed string cannot be turned into an invalid one here. Structured evidence
 * needs no check — `canonicalJson` re-serializes it to a JSON string, which escapes a
 * lone surrogate back to ASCII.
 */
function retainedText(value: string, state: ScrubState): string {
  const text = scrubTracked(value, state);
  if (!wellFormed(text))
    invalid("Delivery strings must be well-formed Unicode");
  return text;
}
function structuredEvidence(raw: unknown, state: ScrubState): string | null {
  if (raw === null) return null;
  if (typeof raw !== "string")
    invalid("Structured evidence must retain its JSON string representation");
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    invalid("Structured evidence contains invalid JSON");
  }
  inspect(parsed);
  return canonicalJson(scrubStructured(parsed, state));
}
/**
 * The service representation of one record: every column the client sent under
 * its local name, minus `RAW_FIELDS`. A payload is one flat local row, so each
 * name is a column identifier and each value a JSON scalar. `*_json` columns are
 * parsed, scrubbed and re-serialized as canonical JSON strings; every other string
 * is scrubbed text. Babysitter kinds keep only their declared reference fields.
 *
 * Identifier columns (`event_uid`, `tool_use_id`, `message_id`, `request_id`,
 * `provider_message_id`, `parent_id`, `agent_id`, `subagent_session_id`, commit SHAs…)
 * are deliberately scrubbed like any other string. The server owns secret/PII scrubbing
 * and a column name is client-controlled, so an exemption keyed on name would be a
 * bypass for anything a client labels an id. It also buys nothing: the local ids are
 * opaque (`<uuid>:<n>`, `toolu_…`, hex), which no rule matches, and the rules that
 * could touch one are deterministic, so a scrubbed id still replays and joins
 * identically. The one rule that did corrupt real values — the email rule eating
 * `git@host:path` remotes — is fixed in the pattern itself.
 *
 * When any retained string exceeds the scrub bound it is cut and the truncation is
 * announced in band. `payload_truncated` (the local `session_events` column) is then
 * set to 1 so readers can tell from the column, not only the text, that it is partial.
 * It is set on `session_event` rows and on any row that already carries the column;
 * other kinds have no such column and do not gain one.
 */
export function servicePayload(
  record: HistoryExportRecord,
): Record<string, unknown> | null {
  if (record.payload === null) return null;
  const declared = babysitter(record.kind)
    ? Object.keys(BABYSITTER_FIELDS[record.kind])
    : Object.keys(record.payload);
  const retained: [string, unknown][] = [];
  const state: ScrubState = { truncated: false };
  for (const key of declared) {
    const value = record.payload[key];
    if (value === undefined || RAW_FIELDS.has(key)) continue;
    if (!COLUMN.test(key))
      invalid("Delivery payload field names must be local column identifiers");
    // Raster bytes are not prose: text scrubbing corrupts base64. Exempt only
    // a bounded, validated embedded image block, never arbitrary tagged text.
    if (
      key === "text" &&
      record.kind === "session_event" &&
      record.payload.raw_kind === "input_image" &&
      sessionImage(value)
    )
      retained.push([key, value]);
    else if (key.endsWith(JSON_SUFFIX))
      retained.push([key, structuredEvidence(value, state)]);
    else if (typeof value === "string")
      retained.push([key, retainedText(value, state)]);
    else if (typeof value === "number") {
      // A card number in a numeric column becomes null, not the `[REDACTED]` string:
      // projections cast these columns to numeric, and a string would fail the cast.
      // Each one is counted on the request log line, so the drop is never silent.
      const card = scrubNumber(value) !== value;
      if (card) recordDeliveryCardColumnNulled();
      retained.push([key, card ? null : value]);
    } else if (value === null || typeof value === "boolean")
      retained.push([key, value]);
    else invalid("Delivery payload fields must be JSON scalars");
  }
  // Own properties even for a name like `__proto__`.
  const output = Object.fromEntries(retained);
  if (
    state.truncated &&
    (record.kind === "session_event" ||
      Object.hasOwn(output, "payload_truncated"))
  )
    output.payload_truncated = 1;
  return output;
}
// ---- relayhistory delivery.ts lines 700-731 ----
/** A client assertion only; tenancy still comes exclusively from authentication. */
export async function deliveryAccount(auth: AuthContext): Promise<string> {
  return "relayhistory:" + (await digest([auth.orgId, auth.workspaceId ?? ""]));
}
export async function requireDeliveryAccount(
  auth: AuthContext,
  expected: unknown,
): Promise<void> {
  if (expected !== (await deliveryAccount(auth)))
    throw new DeliveryError(
      "delivery_account_mismatch",
      403,
      "Delivery account does not match authenticated tenant",
    );
}
/** One retained record as `accept_delivery_batch` takes it: scrubbed payload, digest of the original. */
export type PreparedDeliveryRecord = HistoryExportRecord & { digest: string };

/**
 * Everything `accept_delivery_batch` needs, computed without the database: the
 * write path (lib/write-path) stores these same records in object storage and
 * replays them into Neon later, so both copies are byte-for-byte the same input.
 */
export interface PreparedDelivery {
  originId: string;
  batchId: string;
  batchDigest: string;
  receipt: StoredDeliveryReceipt;
  /** Retired kinds already dropped; in batch order. */
  records: PreparedDeliveryRecord[];
}

// ---- relayhistory delivery.ts lines 740-794 ----
export async function prepareDelivery(
  auth: AuthContext,
  batch: HistoryExportBatch,
): Promise<PreparedDelivery> {
  await requireDeliveryAccount(auth, batch.account_id);
  // Idempotent; covers callers that did not come through parseDeliveryRequest.
  batch = normalizeDeliveryBatch(batch);
  const batchDigest = await digest({ protocolVersion: 1, batch });
  // `limits` is merged at response time below and never stored, so the
  // persisted receipt stays byte-for-byte replayable across limit changes.
  const receipt: StoredDeliveryReceipt = {
    protocolVersion: 1,
    receiptId:
      "rhr_" +
      (await digest([
        "relayhistory-receipt-v1",
        auth.orgId,
        auth.workspaceId ?? "",
        batch.origin_id,
        batch.batch_id,
        batchDigest,
      ])),
    batchId: batch.batch_id,
    acceptedRevisionIds: batch.records.map((record) => record.revision_id),
    unsupportedRevisionIds: [],
    acceptanceLevel: "durable",
  };
  // Retired evidence (see RETIRED_DELIVERY_KINDS) is acknowledged in the receipt
  // and covered by the batch digest, so a replay stays exact, but never stored.
  const records = await Promise.all(
    batch.records
      .filter((record) => !retiredDeliveryRecord(record))
      .map(async (record) => {
        const retained = { ...record, payload: servicePayload(record) };
        if (
          encoder.encode(JSON.stringify(retained)).length >
          MAX_DELIVERY_RECORD_BYTES
        )
          throw new DeliveryError(
            "delivery_too_large",
            413,
            "Transformed delivery record exceeds the readback byte limit",
          );
        return { ...retained, digest: await digest(record) };
      }),
  );
  return {
    originId: batch.origin_id,
    batchId: batch.batch_id,
    batchDigest,
    receipt,
    records,
  };
}

