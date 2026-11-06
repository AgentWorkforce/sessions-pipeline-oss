// GENERATED from relayhistory-cloud packages/relayhistory/src/lib/search-feature-journal.ts
// Source commit: d482b688a4d52028fdc3613b8f1e7990eb31572a; SHA256: b43043e3b2898b5e44b317ed7e95d3382db49db65438e9976fe6f80b68f0bd11
// Regenerate/check: bun scripts/sync-search-feature-source.ts --source <RHC-worktree> [--check]
/** Readonly legacy feature source API. No connection, token, model or writes. */
export const FEATURE_SOURCE_PAGE_BYTES = 16 * 1024 * 1024;
export type FeatureKind = "event" | "brief";
export interface FeatureReader {
  query<T>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}
export interface FeatureReference {
  kind: FeatureKind;
  key: Record<string, string | null>;
  revision: string;
  sourceHash: string;
  deleted: boolean;
}
export interface FeatureSnapshot {
  reference: FeatureReference;
  row: Record<string, unknown> | null;
}
export interface FeatureJournalPage {
  changes: FeatureReference[];
  nextRevision: string;
  observedRevision: string;
}
export class FeatureJournalError extends Error {
  constructor(
    readonly reason: string,
    readonly code?: string,
  ) {
    super("Semantic feature source is unavailable");
    this.name = "FeatureJournalError";
  }
}
async function sourceQuery<T>(
  db: FeatureReader,
  query: string,
  params?: unknown[],
): Promise<{ rows: T[] }> {
  try {
    return await db.query<T>(query, params);
  } catch (error) {
    const raw = (error as { code?: unknown })?.code;
    throw new FeatureJournalError(
      "query_failed",
      typeof raw === "string" && /^[0-9A-Z]{5}$/.test(raw) ? raw : undefined,
    );
  }
}
const fail = (r: string): never => {
  throw new FeatureJournalError(r);
};
const decimal = (v: unknown): v is string =>
  typeof v === "string" &&
  /^(0|[1-9][0-9]{0,18})$/.test(v) &&
  BigInt(v) <= 9223372036854775807n;
function kind(k: FeatureKind) {
  if (k !== "event" && k !== "brief") return fail("kind_invalid");
}
function limit(n: number) {
  if (!Number.isInteger(n) || n < 1 || n > 200) return fail("batch_invalid");
}
function reference(v: any): FeatureReference {
  if (
    !v ||
    typeof v !== "object" ||
    !v.key ||
    typeof v.key !== "object" ||
    Array.isArray(v.key) ||
    !decimal(v.revision) ||
    typeof v.sourceHash !== "string" ||
    !/^[0-9a-f]{64}$/.test(v.sourceHash) ||
    typeof v.deleted !== "boolean"
  )
    return fail("reference_invalid");
  kind(v.kind);
  const fields =
    v.kind === "event"
      ? [
          "orgId",
          "workspaceId",
          "source",
          "sessionId",
          "machineId",
          "kind",
          "eventId",
          "ts",
        ]
      : ["orgId", "workspaceId", "source", "sessionId"];
  if (
    Object.keys(v.key).length !== fields.length ||
    fields.some(
      (k) =>
        !(k in v.key) ||
        !(
          typeof v.key[k] === "string" ||
          (k === "workspaceId" && v.key[k] === null)
        ),
    )
  )
    return fail("key_invalid");
  return {
    ...v,
    key: Object.fromEntries(
      Object.entries(v.key).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
    ),
  };
}
export function featureVersion(revision: string): string {
  if (!decimal(revision)) return fail("revision_invalid");
  return (2n * BigInt(revision) + 1n).toString();
}
export async function readFeatureJournalPage(
  db: FeatureReader,
  k: FeatureKind,
  afterRevision: string,
  batch = 100,
): Promise<FeatureJournalPage> {
  kind(k);
  limit(batch);
  if (!decimal(afterRevision)) return fail("revision_invalid");
  const result = (
    await sourceQuery<{ p: any }>(
      db,
      `WITH observed AS MATERIALIZED(SELECT revision FROM sessions.search_feature_clock WHERE singleton),
 selected AS MATERIALIZED(SELECT c.* FROM sessions.search_feature_changes c,observed o WHERE c.feature_type=$1 AND c.revision>$2::bigint AND c.revision<=o.revision ORDER BY c.revision LIMIT $3)
 , packed AS MATERIALIZED(SELECT coalesce(jsonb_agg(jsonb_build_object('kind',s.feature_type,'key',s.feature_key,'revision',s.revision::text,'sourceHash',s.source_hash,'deleted',s.deleted) ORDER BY s.revision),'[]'::jsonb) data FROM selected s)
 SELECT jsonb_build_object('observedRevision',o.revision::text,'too_large',octet_length(p.data::text)>${FEATURE_SOURCE_PAGE_BYTES},'rows',CASE WHEN octet_length(p.data::text)>${FEATURE_SOURCE_PAGE_BYTES} THEN '[]'::jsonb ELSE p.data END) p FROM observed o,packed p`,
      [k, afterRevision, batch + 1],
    )
  ).rows[0]?.p;
  if (result?.too_large === true) return fail("page_bytes");
  if (
    !result ||
    !decimal(result.observedRevision) ||
    BigInt(result.observedRevision) < BigInt(afterRevision) ||
    !Array.isArray(result.rows) ||
    result.rows.length > batch + 1
  )
    return fail("page_invalid");
  const all = result.rows.map(reference),
    changes = all.slice(0, batch);
  let previous = BigInt(afterRevision);
  for (const r of all) {
    if (
      r.kind !== k ||
      BigInt(r.revision) <= previous ||
      BigInt(r.revision) > BigInt(result.observedRevision)
    )
      return fail("page_invalid");
    previous = BigInt(r.revision);
  }
  return {
    changes,
    observedRevision: result.observedRevision,
    nextRevision:
      all.length > batch ? changes.at(-1)!.revision : result.observedRevision,
  };
}
const payloadHashSql =
  "encode(sha256(convert_to(coalesce(payload,jsonb_build_array('deleted',kind,key))::text,'UTF8')),'hex')";
export async function readFeatureSnapshots(
  db: FeatureReader,
  refs: FeatureReference[],
): Promise<FeatureSnapshot[]> {
  if (refs.length > 200) return fail("batch_invalid");
  if (!refs.length) return [];
  const wanted = refs.map(reference),
    keys = new Set<string>();
  for (const r of wanted) {
    const k = JSON.stringify([r.kind, r.key]);
    if (keys.has(k)) return fail("duplicate");
    keys.add(k);
  }
  const packet = (
    await sourceQuery<any>(
      db,
      `WITH wanted AS MATERIALIZED(SELECT x.kind,x.key,x.ordinal FROM jsonb_to_recordset($1::jsonb) AS x(kind text,key jsonb,ordinal integer)),
 current AS MATERIALIZED(SELECT w.*,h.revision,h.source_hash,h.deleted,sessions.search_feature_payload(w.kind,w.key) payload
 FROM wanted w LEFT JOIN sessions.search_feature_heads h ON h.feature_type=w.kind AND h.feature_key=w.key)
 , encoded AS MATERIALIZED(SELECT kind,key,ordinal,revision::text revision,source_hash,deleted,payload,${payloadHashSql} actual_hash FROM current),
 packed AS MATERIALIZED(SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY ordinal),'[]'::jsonb) data FROM encoded e)
 SELECT jsonb_build_object('too_large',octet_length(data::text)>${FEATURE_SOURCE_PAGE_BYTES},'rows',CASE WHEN octet_length(data::text)>${FEATURE_SOURCE_PAGE_BYTES} THEN '[]'::jsonb ELSE data END) page FROM packed`,
      [
        JSON.stringify(
          wanted.map((r, ordinal) => ({ kind: r.kind, key: r.key, ordinal })),
        ),
      ],
    )
  ).rows[0]?.page;
  if (packet?.too_large === true) return fail("page_bytes");
  if (!packet || !Array.isArray(packet.rows)) return fail("snapshot_invalid");
  const rows: any[] = packet.rows;
  if (rows.length !== wanted.length) return fail("snapshot_missing");
  return rows.map((r, n) => {
    const original = wanted[n]!;
    if (
      r.ordinal !== n ||
      r.kind !== original.kind ||
      JSON.stringify(
        Object.fromEntries(
          Object.entries(r.key).sort(([a], [b]) =>
            a < b ? -1 : a > b ? 1 : 0,
          ),
        ),
      ) !== JSON.stringify(original.key)
    )
      return fail("snapshot_invalid");
    if (r.revision === null) {
      if (original.revision !== "0") return fail("snapshot_missing");
      r.revision = "0";
      r.source_hash = r.actual_hash;
      r.deleted = r.payload === null;
    }
    if (r.source_hash !== r.actual_hash || r.deleted !== (r.payload === null))
      return fail("source_changed");
    const ref = reference({
      kind: r.kind,
      key: r.key,
      revision: r.revision,
      sourceHash: r.source_hash,
      deleted: r.deleted,
    });
    if (
      r.payload !== null &&
      (typeof r.payload !== "object" || Array.isArray(r.payload))
    )
      return fail("snapshot_invalid");
    return { reference: ref, row: r.payload };
  });
}
const eventColumns = [
    "org_id",
    "machine_id",
    "source",
    "session_id",
    "kind",
    "event_id",
  ] as const,
  briefColumns = ["org_id", "session_id"] as const;
export async function readFeatureBaselinePage(
  db: FeatureReader,
  k: FeatureKind,
  cursor: string | null,
  batch = 100,
): Promise<{ rows: FeatureSnapshot[]; nextCursor: string | null }> {
  kind(k);
  limit(batch);
  const columns = k === "event" ? eventColumns : briefColumns;
  let after: string[] | null = null;
  if (cursor !== null) {
    try {
      const v = JSON.parse(cursor);
      if (
        v.kind !== k ||
        !Array.isArray(v.after) ||
        v.after.length !== columns.length ||
        v.after.some((s: unknown) => typeof s !== "string")
      )
        return fail("cursor_invalid");
      after = v.after;
    } catch {
      return fail("cursor_invalid");
    }
  }
  const table = k === "event" ? "convergence_events" : "session_brief_search",
    keyFn =
      k === "event" ? "search_feature_event_key" : "search_feature_brief_key",
    payloadFn =
      k === "event"
        ? "search_feature_event_payload"
        : "search_feature_brief_payload";
  const tuple = columns.map((c) => `s.${c}`).join(","),
    params: unknown[] = [batch + 1];
  const where = after
    ? `AND (${tuple})>(${after
        .map((v, n) => {
          params.push(v);
          return `$${n + 2}::text`;
        })
        .join(",")})`
    : "";
  const packet = (
    await sourceQuery<any>(
      db,
      `WITH source AS MATERIALIZED(SELECT sessions.${keyFn}(s) key,sessions.${payloadFn}(s) payload,${columns.map((c) => `s.${c}`).join(",")} FROM sessions.${table} s WHERE true ${where} ORDER BY ${tuple} LIMIT $1)
 ,encoded AS MATERIALIZED(SELECT s.*,encode(sha256(convert_to(coalesce(payload,jsonb_build_array('deleted','${k}',key))::text,'UTF8')),'hex') actual_hash FROM source s),
 packed AS MATERIALIZED(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY ${tuple}),'[]'::jsonb) data FROM encoded s)
 SELECT jsonb_build_object('too_large',octet_length(data::text)>${FEATURE_SOURCE_PAGE_BYTES},'rows',CASE WHEN octet_length(data::text)>${FEATURE_SOURCE_PAGE_BYTES} THEN '[]'::jsonb ELSE data END) page FROM packed`,
      params,
    )
  ).rows[0]?.page;
  if (packet?.too_large === true) return fail("page_bytes");
  if (!packet || !Array.isArray(packet.rows)) return fail("baseline_invalid");
  const rows: any[] = packet.rows;
  if (rows.length > batch + 1) return fail("baseline_invalid");
  const selected = rows.slice(0, batch);
  const refs = selected.map((r) =>
    reference({
      kind: k,
      key: r.key,
      revision: "0",
      sourceHash: r.actual_hash,
      deleted: r.payload === null,
    }),
  );
  const snapshots = await readFeatureSnapshots(db, refs);
  const last = selected.at(-1),
    nextCursor =
      rows.length > batch && last
        ? JSON.stringify({ kind: k, after: columns.map((c) => last[c]) })
        : null;
  return { rows: snapshots, nextCursor };
}

/** Generated expected metadata comes from committed0076 SQL, never source rows. */
export interface FeatureSourceAuthority {
  version: 1;
  repository: string;
  apiSha256: string;
  migrationSha256: string;
  catalogSha256: string;
  identitySha256: string;
  catalog: {
    relations: ReadonlyArray<{
      identity: string;
      kind: string;
      rls: boolean;
      forceRls: boolean;
    }>;
    database: {
      datcollate: string;
      datctype: string;
      datlocprovider: string;
      datlocale: string;
    };
    columns: ReadonlyArray<{
      table: string;
      column: string;
      collation: string;
      provider: string;
      schema: string;
      version: string | null;
    }>;
    functions: ReadonlyArray<{
      identity: string;
      bodyHash: string;
      language: string;
      securityDefiner: boolean;
      volatility: string;
      config: readonly string[];
    }>;
    triggers: ReadonlyArray<{
      table: string;
      name: string;
      functionIdentity: string;
      enabled: string;
      type: number;
      internal: boolean;
      when: boolean;
      args: number;
      columns: string;
      deferrable: boolean;
      initiallyDeferred: boolean;
      constraint: boolean;
    }>;
    headsView: string;
  };
}
const stableMetadata = (v: unknown): string =>
  JSON.stringify(v, (_key, item) =>
    item && typeof item === "object" && !Array.isArray(item)
      ? Object.fromEntries(
          Object.entries(item).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
        )
      : item,
  );
const normalizedHeadsView = (s: string) =>
  s
    .toLowerCase()
    .replace(/"|\s/g, "")
    .replace(/sessions\./g, "")
    .replace(/search_feature_changes\./g, "");
/** Attest the installed ledger, payload/queue/refresh definitions and triggers,
 * in ONE read snapshot. Compiled constants alone never prove live SQL identity. */
export async function verifyFeatureSourceAuthority(
  db: FeatureReader,
  expected: FeatureSourceAuthority,
): Promise<{ identitySha256: string; observedRevision: string }> {
  const hex = (s: unknown) => typeof s === "string" && /^[a-f0-9]{64}$/.test(s);
  if (
    !expected ||
    expected.version !== 1 ||
    expected.repository !== "AgentWorkforce/relayhistory-cloud" ||
    ![
      expected.apiSha256,
      expected.migrationSha256,
      expected.catalogSha256,
      expected.identitySha256,
    ].every(hex) ||
    expected.catalog?.functions?.length !== 11 ||
    expected.catalog?.triggers?.length !== 3 ||
    expected.catalog?.columns?.length !== 16 ||
    expected.catalog?.relations?.length !== 7
  )
    return fail("authority_invalid");
  const catalogBytes = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(stableMetadata(expected.catalog)),
  );
  const catalogHash = Array.from(new Uint8Array(catalogBytes), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
  if (catalogHash !== expected.catalogSha256) return fail("authority_invalid");
  const identityBytes = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(
      JSON.stringify({
        apiSha256: expected.apiSha256,
        migrationSha256: expected.migrationSha256,
        catalogSha256: expected.catalogSha256,
      }),
    ),
  );
  const identity = Array.from(new Uint8Array(identityBytes), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
  if (identity !== expected.identitySha256) return fail("authority_invalid");
  const fns = [...expected.catalog.functions].sort((a, b) =>
      a.identity < b.identity ? -1 : a.identity > b.identity ? 1 : 0,
    ),
    triggers = [...expected.catalog.triggers].sort((a, b) =>
      a.table + "/" + a.name < b.table + "/" + b.name ? -1 : 1,
    );
  const columns = [...expected.catalog.columns].sort((a, b) =>
    a.table + "/" + a.column < b.table + "/" + b.column ? -1 : 1,
  );
  const relations = [...expected.catalog.relations].sort((a, b) =>
    a.identity < b.identity ? -1 : 1,
  );
  const report = (
    await sourceQuery<any>(
      db,
      `WITH f AS MATERIALIZED(SELECT * FROM jsonb_to_recordset($1::jsonb) x(identity text)),
 t AS MATERIALIZED(SELECT * FROM jsonb_to_recordset($2::jsonb) x("table" text,name text,"functionIdentity" text)),
 cdef AS MATERIALIZED(SELECT * FROM jsonb_to_recordset($3::jsonb) x("table" text,"column" text)),
 rdef AS MATERIALIZED(SELECT * FROM jsonb_to_recordset($4::jsonb) x(identity text))
 SELECT (SELECT checksum FROM sessions.__migrations WHERE name='0076_search_feature_journal.sql') checksum,
 (SELECT revision::text FROM sessions.search_feature_clock WHERE singleton) observed_revision,
 (SELECT jsonb_agg(jsonb_build_object('identity',f.identity,'bodyHash',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),'language',l.lanname,'securityDefiner',p.prosecdef,'volatility',p.provolatile::text,'config',coalesce(to_jsonb(p.proconfig),'[]'::jsonb)) ORDER BY f.identity)
 FROM f LEFT JOIN pg_catalog.pg_proc p ON p.oid=to_regprocedure(f.identity) LEFT JOIN pg_catalog.pg_language l ON l.oid=p.prolang) functions,
 (SELECT jsonb_agg(jsonb_build_object('table',t."table",'name',t.name,'functionIdentity',CASE WHEN g.tgfoid=to_regprocedure(t."functionIdentity") THEN t."functionIdentity" ELSE NULL END,'enabled',g.tgenabled::text,'type',g.tgtype::integer,'internal',g.tgisinternal,'when',g.tgqual IS NOT NULL,'args',g.tgnargs::integer,'columns',g.tgattr::text,'deferrable',g.tgdeferrable,'initiallyDeferred',g.tginitdeferred,'constraint',g.tgconstraint<>0) ORDER BY t."table",t.name)
 FROM t LEFT JOIN pg_catalog.pg_trigger g ON g.tgrelid=to_regclass(t."table") AND g.tgname=t.name) triggers,
 (SELECT jsonb_build_object('datcollate',d.datcollate,'datctype',d.datctype,'datlocprovider',d.datlocprovider::text,'datlocale',coalesce(to_jsonb(d)->>'datlocale','')) FROM pg_catalog.pg_database d WHERE datname=current_database()) database,
 (SELECT jsonb_agg(jsonb_build_object('table',e."table",'column',e."column",'collation',c.collname,'provider',c.collprovider::text,'schema',n.nspname,'version',c.collversion) ORDER BY e."table",e."column") FROM cdef e LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=to_regclass(e."table") AND a.attname=e."column" AND a.attnum>0 AND NOT a.attisdropped LEFT JOIN pg_catalog.pg_collation c ON c.oid=a.attcollation LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace) columns,
 (SELECT jsonb_agg(jsonb_build_object('identity',e.identity,'kind',c.relkind::text,'rls',c.relrowsecurity,'forceRls',c.relforcerowsecurity) ORDER BY e.identity) FROM rdef e LEFT JOIN pg_catalog.pg_class c ON c.oid=to_regclass(e.identity)) relations,
 pg_catalog.pg_get_viewdef(to_regclass('sessions.search_feature_heads'),false) heads_view`,
      [
        JSON.stringify(fns.map((f) => ({ identity: f.identity }))),
        JSON.stringify(
          triggers.map((t) => ({
            table: t.table,
            name: t.name,
            functionIdentity: t.functionIdentity,
          })),
        ),
        JSON.stringify(
          columns.map((c) => ({ table: c.table, column: c.column })),
        ),
        JSON.stringify(relations.map((r) => ({ identity: r.identity }))),
      ],
    )
  ).rows;
  if (report.length !== 1) return fail("authority_mismatch");
  const r = report[0];
  if (stableMetadata(r.database) !== stableMetadata(expected.catalog.database))
    return fail("authority_collation_mismatch");
  if (stableMetadata(r.relations) !== stableMetadata(relations))
    return fail("authority_visibility_mismatch");
  if (stableMetadata(r.columns) !== stableMetadata(columns))
    return fail("authority_collation_mismatch");
  if (
    r.checksum !== expected.migrationSha256 ||
    !decimal(r.observed_revision) ||
    stableMetadata(r.functions) !== stableMetadata(fns) ||
    stableMetadata(r.triggers) !== stableMetadata(triggers) ||
    typeof r.heads_view !== "string" ||
    normalizedHeadsView(r.heads_view) !==
      normalizedHeadsView(expected.catalog.headsView)
  )
    return fail("authority_mismatch");
  return { identitySha256: identity, observedRevision: r.observed_revision };
}
