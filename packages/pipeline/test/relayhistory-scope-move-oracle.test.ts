/** Reference oracle for legal native History attachment/movement and per-copy
 * privacy. It runs the vendored source SQL with live delivery triggers; it does
 * not claim the background publisher implements cross-scope movement yet.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";

import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
const MIGRATIONS = fileURLToPath(
  new URL("../../../vendor/relayhistory-cloud/migrations/", import.meta.url)
    .href,
);
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");

const WORKSPACE = "workspace-1";
const SESSION = "session-1";
let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { pg_trgm } });
  await db.exec(`SET search_path = pg_catalog, public;
    CREATE SCHEMA IF NOT EXISTS sessions;
    CREATE TABLE IF NOT EXISTS sessions.__migrations (
      name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
  for (const file of readdirSync(MIGRATIONS)
    .filter((name) => name.endsWith(".sql"))
    .sort())
    await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
  // Production's 0029 rollout: each stage's indexes, then its live trigger.
  const { rows } = await db.query<{ index_name: string; definition: string }>(
    "SELECT index_name, definition FROM sessions.delivery_rollout_indexes()",
  );
  for (const index of rows)
    await db.exec(`CREATE INDEX ${index.index_name} ${index.definition}`);
  await db.exec(`SET session_replication_role = replica;
    UPDATE sessions.upload_cleanup_rollout SET phase = 5 WHERE singleton;
    SET session_replication_role = origin;`);
  await db.exec(`SELECT sessions.activate_delivery_catalog();
    SELECT sessions.activate_delivery_projection_v2();
    SET search_path = public`);
}, 120_000);
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec(`SET session_replication_role = replica;
    TRUNCATE sessions.deleted_upload_sessions,sessions.session_resets,sessions.uploaded_support_identities,sessions.delivery_records, sessions.convergence_events, sessions.session_catalog,
      sessions.session_catalog_agents, sessions.session_relationships, sessions.session_markers,
      sessions.session_commit_links, sessions.session_rollups CASCADE;
    SET session_replication_role = origin;`);
});


async function put(origin:string,session:string|null,revision:number,prompt:string){
 await db.query(`INSERT INTO sessions.delivery_records
 (org_id,workspace_id,user_id,origin_id,record_id,revision_id,revision,digest,kind,source,session_id,operation,payload)
 VALUES ('org','workspace-1','u',$1,'native-history',$1||':'||$3::text,$3::bigint,'digest','history','claude',$2,'upsert',jsonb_build_object('prompt',$4::text,'timestamp_ms',1793000000000))
 ON CONFLICT(org_id,workspace_id,origin_id,record_id) DO UPDATE SET session_id=excluded.session_id,revision=excluded.revision,revision_id=excluded.revision_id,payload=excluded.payload`,[origin,session,revision,prompt]);
}
async function current(){return(await db.query<{session_id:string;content:string}>(`SELECT session_id,content FROM sessions.convergence_events WHERE delivery_record_id='native-history' ORDER BY session_id`)).rows;}
async function disable(origin:string,session:string){await db.query(`SELECT sessions.set_session_uploads('org','workspace-1','u',$1,jsonb_build_array(jsonb_build_object('source','claude','session_id',$2::text)),false)`,[origin,session]);}
it('History null attachment and later session move retract the native winner globally',async()=>{
 await put('a',null,1,'unattached');expect(await current()).toEqual([]);
 await put('a','session-A',2,'attached');expect(await current()).toEqual([{session_id:'session-A',content:'attached'}]);
 await put('b','session-A',1,'other retained copy');
 await put('a','session-B',3,'moved');
 expect(await current()).toEqual([{session_id:'session-B',content:'moved'}]);
});
it('privacy removes selected copy then restores independently retained cross-origin winner',async()=>{
 await put('a','session-A',1,'remaining copy');await put('b','session-B',1,'current winner');
 expect(await current()).toEqual([{session_id:'session-B',content:'current winner'}]);
 await disable('b','session-B');
 expect(await current()).toEqual([{session_id:'session-A',content:'remaining copy'}]);
 await disable('a','session-A');expect(await current()).toEqual([]);
});
it('removing a losing copy cannot move a surviving newer History winner backwards',async()=>{
 await put('a','session-A',1,'old');await put('b','session-A',1,'other');await put('a','session-B',2,'moved');
 await disable('b','session-A');expect(await current()).toEqual([{session_id:'session-B',content:'moved'}]);
});
