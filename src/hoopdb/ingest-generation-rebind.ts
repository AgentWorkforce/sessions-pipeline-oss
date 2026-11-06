/**
 * Rebind the live ingest authority to a new schema generation in ONE owner transaction.
 *
 * A new sessions migration changes generation(). The ingest Worker, the buffered GC and the target's
 * guards all require the request generation == sessions.ingest_lazy_policy.generation ==
 * sessions.ingest_buffer_admission.schema_generation == the readiness PASS generation. After an
 * owner-apply moves readiness to the new generation, this moves the policy (through the reviewed
 * owner function ingest_lazy_policy_set, identity unchanged) and every admission row together.
 *
 * Idempotent: rerunning after success changes nothing. It refuses unless readiness PASS is for `to`,
 * the policy is at `from` (or already `to`), and every admission row is at `from` or `to`.
 */
import type { MigrationClient } from "./migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";

const HEX64 = /^[0-9a-f]{64}$/;
export interface RebindResult { from: string; to: string; policy: "rebound" | "already"; admissionsRebound: number; admissionsTotal: number; dryRun: boolean }
const refuse = (code: string): never => { throw Object.assign(new Error("ingest generation rebind: " + code), { code }); };

export async function rebindIngestGeneration(client: MigrationClient, opts: { from: string; to: string; dryRun?: boolean }): Promise<RebindResult> {
  const { from, to } = opts;
  if (!HEX64.test(from) || !HEX64.test(to) || from === to) refuse("invalid_generations");
  await client.query("BEGIN");
  try {
    // Serialize with the canonical migrator (it holds the session-level form of this lock).
    await client.query("SELECT pg_advisory_xact_lock(961996, 1)");
    const marker = (await client.query<{ generation: string; status: string }>(
      `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1 FOR SHARE`, [ROLE_VALIDATION_NAME])).rows[0];
    if (!marker || marker.generation !== to || marker.status !== "pass") refuse("readiness_not_pass_for_target");
    const pol = (await client.query<any>(`SELECT generation, source_identity_sha256, control_epoch, privacy_baseline_sha256,
        retention_contract_sha256, source_proof_sha256, valid_until FROM sessions.ingest_lazy_policy WHERE singleton FOR UPDATE`)).rows[0];
    if (!pol) refuse("policy_missing");
    if (pol.generation !== from && pol.generation !== to) refuse("policy_generation_unexpected");
    const counts = (await client.query<{ at_from: string; at_to: string; total: string }>(`SELECT count(*) FILTER (WHERE schema_generation=$1)::text AS at_from,
        count(*) FILTER (WHERE schema_generation=$2)::text AS at_to, count(*)::text AS total FROM sessions.ingest_buffer_admission`, [from, to])).rows[0]!;
    if (Number(counts.at_from) + Number(counts.at_to) !== Number(counts.total)) refuse("admission_generation_unexpected");
    let policy: RebindResult["policy"] = "already";
    if (pol.generation === from) {
      const validUntil = pol.valid_until instanceof Date ? pol.valid_until.toISOString() : new Date(pol.valid_until).toISOString();
      await client.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({
        baselineMode: "lazy-current", generation: to, sourceIdentitySha256: pol.source_identity_sha256, controlEpoch: pol.control_epoch,
        privacyBaselineSha256: pol.privacy_baseline_sha256, retentionContractSha256: pol.retention_contract_sha256,
        sourceProofSha256: pol.source_proof_sha256, validUntil, issuer: "relayhistory-cloud" })]);
      policy = "rebound";
    }
    const moved = await client.query<{ n: string }>(
      `WITH u AS (UPDATE sessions.ingest_buffer_admission SET schema_generation=$2 WHERE schema_generation=$1 RETURNING 1) SELECT count(*)::text AS n FROM u`, [from, to]);
    const after = (await client.query<{ g: string }>(`SELECT (SELECT generation FROM sessions.ingest_lazy_policy WHERE singleton) AS g`)).rows[0]!;
    const left = (await client.query<{ n: string }>(`SELECT count(*)::text AS n FROM sessions.ingest_buffer_admission WHERE schema_generation<>$1`, [to])).rows[0]!;
    if (after.g !== to || left.n !== "0") refuse("postcondition_failed");
    await client.query(opts.dryRun ? "ROLLBACK" : "COMMIT");
    return { from, to, policy, admissionsRebound: Number(moved.rows[0]!.n), admissionsTotal: Number(counts.total), dryRun: !!opts.dryRun };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
