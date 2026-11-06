/**
 * P1 control-mirror repair: re-seed ONE Supabase mirror head (sessions.ingest_source_control_heads)
 * from Neon's sessions.delivery_cutover_control_heads.
 *
 * Needed when a selection/reset committed on Neon between `frozen-source-copy --copy` seeding the
 * mirror and relayhistory turning DELIVERY_CUSTODY_SOURCE_MODE=mirror on: that user's next mirrored
 * begin fails closed (version/digest mismatch) and --seal reports the head.
 *
 * Neon is read in its own READ ONLY transaction. The mirror row is replaced in one owner transaction
 * holding the row lock, and only when:
 *   - both heads exist, are for the same epoch, and neither is pending;
 *   - Neon's version is strictly newer than the mirror's.
 * Equal version+digest is "already" (no write). Anything else refuses. Never writes Neon.
 */
export interface Queryable { query<T = any>(sql: string, params?: unknown[]): Promise<{ rows: T[] }> }
export interface HeadKey { org: string; workspace: string; user: string }
export interface RepairResult { key: HeadKey; outcome: "repaired" | "already"; dryRun: boolean; before: { version: string; digest: string }; after: { version: string; digest: string } }
const refuse = (code: string): never => { throw Object.assign(new Error("control head repair: " + code), { code }); };
const COLS = "org_id,workspace_id,user_id,epoch,version::text AS version,digest,kind,payload,pending,before_privacy,privacy_version::text AS privacy_version";
const KEY = "WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3";

export async function repairControlHead(neon: Queryable, supa: Queryable, key: HeadKey, opts: { dryRun?: boolean } = {}): Promise<RepairResult> {
  if (![key.org, key.workspace, key.user].every((v) => typeof v === "string") || !key.org || !key.user) refuse("invalid_key");
  const params = [key.org, key.workspace, key.user];
  await neon.query("BEGIN READ ONLY");
  let n: any;
  try { n = (await neon.query(`SELECT ${COLS} FROM sessions.delivery_cutover_control_heads ${KEY}`, params)).rows[0]; }
  finally { await neon.query("ROLLBACK"); }
  if (!n) refuse("neon_head_missing");
  if (n.pending) refuse("neon_head_pending");

  await supa.query("BEGIN");
  try {
    const m = (await supa.query<any>(`SELECT ${COLS} FROM sessions.ingest_source_control_heads ${KEY} FOR UPDATE`, params)).rows[0];
    if (!m) refuse("mirror_head_missing_use_copy");
    const before = { version: m.version as string, digest: m.digest as string }, after = { version: n.version as string, digest: n.digest as string };
    if (m.pending) refuse("mirror_head_pending");
    if (m.epoch !== n.epoch) refuse("epoch_mismatch");
    if (m.version === n.version && m.digest === n.digest) { await supa.query("ROLLBACK"); return { key, outcome: "already", dryRun: !!opts.dryRun, before, after }; }
    if (!(BigInt(n.version) > BigInt(m.version))) refuse("neon_not_newer");
    if (opts.dryRun) { await supa.query("ROLLBACK"); return { key, outcome: "repaired", dryRun: true, before, after }; }
    const r = await supa.query(`UPDATE sessions.ingest_source_control_heads SET epoch=$4,version=$5::bigint,digest=$6,kind=$7,payload=$8::jsonb,pending=false,
        before_privacy=$9::jsonb,privacy_version=$10::bigint,updated_at=clock_timestamp() ${KEY} AND version=$11::bigint AND digest=$12 AND NOT pending RETURNING version::text AS version,digest`,
      [...params, n.epoch, n.version, n.digest, n.kind, JSON.stringify(n.payload), n.before_privacy === null ? null : JSON.stringify(n.before_privacy), n.privacy_version, m.version, m.digest]);
    if (r.rows.length !== 1 || r.rows[0].version !== n.version || r.rows[0].digest !== n.digest) refuse("postcondition");
    await supa.query("COMMIT");
    return { key, outcome: "repaired", dryRun: false, before, after };
  } catch (e) { await supa.query("ROLLBACK").catch(() => {}); throw e; }
}
