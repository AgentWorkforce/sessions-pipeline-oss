// Synthetic transport only: no provider CLI or network is permitted.
import pg from "pg";
import { createRequire, syncBuiltinESMExports } from "node:module";
const cp = createRequire(import.meta.url)("node:child_process");
let logins=0, currentPassword="", neoCreated=0, targetCreated=0, targetClosed=0;
let closedWhileWorkersActive=false;
const clients: FakeClient[] = [], records: string[] = [];
cp.execFileSync = (exe: string) => {
  if(exe!=="/synthetic-supabase-refuse-network") throw Error("unexpected_process_refused");
  currentPassword="synthetic-cli-password-"+(++logins);
  return ['PGHOST="db.abcdefghijklmnopqrst.supabase.co"','PGPORT="5432"','PGUSER="cli_login_postgres"','PGDATABASE="postgres"',"PGPASSWORD="+JSON.stringify(currentPassword)].join("\n");
};
syncBuiltinESMExports();
globalThis.fetch = async () => { throw Error("network_refused_in_copy_fixture"); };
class FakeClient {
  target: boolean; neoIndex=0; ended=false; cfg: any;
  constructor(cfg: any) {
    this.cfg=cfg;this.target=!!cfg.host;
    if(this.target) targetCreated++;else this.neoIndex=++neoCreated;
    clients.push(this);
  }
  async connect() {
    await new Promise(r=>setTimeout(r,this.target?5:1));
    if(process.env.FROZEN_COPY_TEST_CONNECT===(this.target?"target":this.neoIndex===2?"source":"none")) throw Object.assign(Error("synthetic_connect_failure"),{code:"08006"});
    if(this.target && this.cfg.password!==currentPassword) throw Object.assign(Error("synthetic_login_rotated"),{code:"28P01"});
  }
  async query(sql: string,args: any[]=[]) {
    if(this.ended) throw Error("query_after_end");
    if(/^(BEGIN|ROLLBACK|COMMIT|SET)/.test(sql))return {rows:[]};
    if(!this.target) {
      if(sql.includes("delivery_cutover_status()"))return {rows:[{s:{phase:"active"}}]};
      if(sql.includes("FROM sessions.delivery_origins"))return {rows:[{org_id:"org-a"},{org_id:"org-b"}]};
      if(sql.includes("FROM sessions.deleted_upload_sessions")||sql.includes("FROM sessions.session_resets")||sql.includes("FROM sessions.delivery_cutover_control_heads")||sql.includes("FROM sessions.delivery_receipts"))return {rows:[]};
      if(sql.includes("FROM sessions.delivery_records r")) {
        await new Promise(r=>setTimeout(r,5));
        return {rows:args[1]!==""?[]:[{org_id:args[0],workspace_id:"ws",origin_id:"origin",record_id:args[0],revision_id:"rev",revision:1,digest:"digest",kind:"kind",source:"source",session_id:"session",operation:"upsert",user_id:"user",projection_accepted_seq:null,catalog_accepted_seq:null}]};
      }
    }else{
      if(sql.includes("SELECT 1 FROM sessions.ingest_frozen_source_org_seals"))return {rows:[]};
      if(sql.includes("INSERT INTO sessions.ingest_frozen_source_records")) {
        const org=args[0][0];
        if(process.env.FROZEN_COPY_TEST_FAIL===org) {
          if(process.env.FROZEN_COPY_TEST_UNDEFINED==="1") throw undefined;
          throw Object.assign(Error("synthetic_target_failure"),{code:"XX000"});
        }
        records.push(org);return {rows:[]};
      }
    }
    throw Error("unexpected_sql_refused");
  }
  async end() {
    if(this.target) {
      targetClosed++;
      closedWhileWorkersActive ||= clients.some(c=>!c.target&&c.neoIndex>1&&!c.ended);
    }
    this.ended=true;
  }
}
pg.Client=FakeClient as any;
process.on("beforeExit",()=>console.log(JSON.stringify({syntheticCopyProof:{logins,targetCreated,targetClosed,closedWhileWorkersActive,records:records.sort(),allNeoClosed:clients.filter(c=>!c.target).every(c=>c.ended)}})));
