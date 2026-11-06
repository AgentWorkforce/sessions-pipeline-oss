/** Retired Neon-record/session-boolean seeding fails closed. Only fence import remains here.
 * Per-record S3 byte evidence and execution live in seed-proof.ts / seed-s3.ts. */
export interface SeedSql {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

export interface SeedResult {
  pages: number;
  read: number;
  upserted: number;
  /** Legacy API shape, retained only for a fail-closed old caller. */
  skipped: number;
}

/** Legacy callback type. It is never accepted as evidence. */
export type Exported = (sessionId: string) => boolean;

/** Retired unsafe API. A session/exporter boolean can never attest S3 rows. */
export async function seedOrg(..._args: unknown[]): Promise<SeedResult> {
  throw Object.assign(new Error("S3 per-record proof required"), {code:"seed_proof_required"});
}

/** Actual RR Neon fences imported exactly once per user, before any destination activity. */
export async function seedFences(
  neon: { readOnly<T>(work: (sql: SeedSql) => Promise<T>): Promise<T> },
  supabase: SeedSql,
  orgId: string,
  knownUsers: Array<{workspace_id:string;user_id:string}> = [],
): Promise<{ deletions: number; resets: number }> {
  const [deletions,resets] = await neon.readOnly(async sql => [
    (await sql.query("SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions WHERE org_id=$1 LIMIT 200001",[orgId])).rows,
    (await sql.query("SELECT org_id,workspace_id,user_id,request_id,deleted::text AS deleted FROM sessions.session_resets WHERE org_id=$1 LIMIT 200001",[orgId])).rows,
  ]);
  if(deletions.length>200000||resets.length>200000)
    throw Object.assign(new Error("fence metadata bound"),{code:"seed_fence_bound"});
  const users=new Map<string,{org_id:string;workspace_id:string;user_id:string;deletions:Record<string,unknown>[];resets:Record<string,unknown>[]}>();
  const group=(ws:string,user:string)=>{
    const key=JSON.stringify([ws,user]);
    let value=users.get(key);
    if(!value){value={org_id:orgId,workspace_id:ws,user_id:user,deletions:[],resets:[]};users.set(key,value)}
    return value;
  };
  for(const u of knownUsers)group(u.workspace_id,u.user_id);
  for(const raw of deletions){
    const r=raw as Record<string,string>;
    group(r.workspace_id!,r.user_id!).deletions.push({origin_id:r.origin_id,source:r.source,session_id:r.session_id});
  }
  for(const raw of resets){
    const r=raw as Record<string,string>;
    group(r.workspace_id!,r.user_id!).resets.push({request_id:r.request_id,deleted:r.deleted});
  }
  const result={deletions:0,resets:0};
  for(const p of users.values()){
    if(p.deletions.length+p.resets.length>5000||Buffer.byteLength(JSON.stringify(p))>16*1024*1024)
      throw Object.assign(new Error("per-user fence metadata bound"),{code:"seed_fence_bound"});
    const out=(await supabase.query<{r:{accepted:boolean;deletions:number;resets:number}}>(
      "SELECT sessions.ingest_seed_fence_baseline($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]?.r;
    if(out?.accepted!==true)throw Object.assign(new Error("fence baseline refused"),{code:"seed_fence_baseline"});
    result.deletions+=out.deletions;result.resets+=out.resets;
  }
  return result;
}

/** Retired unsafe helper, retained only to fail old callers closed. */
export function exportedFromState(..._args: unknown[]): Exported {
  throw Object.assign(new Error("Exporter session state is not S3 proof"), {code:"seed_proof_required"});
}
