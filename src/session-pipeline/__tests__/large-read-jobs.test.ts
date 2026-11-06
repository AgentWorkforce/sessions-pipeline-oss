import {afterEach,expect,test} from "bun:test";
import {replayDatabase,addReplayPointer,REPLAY_FIXTURE_IDENTITY as i} from "./replay-fixture.ts";
import {projectSessionDirectory} from "../../hoopdb/session-directory-projector.ts";
import {nodeReadBudget} from "../../session-read-api/node-read-profile.ts";
const open:Awaited<ReturnType<typeof replayDatabase>>[]=[];
afterEach(async()=>{for(const x of open.splice(0))await x.pg.close()});
const scope=[i.orgId,i.workspaceId,i.source,i.sessionId],n1="11111111-1111-4111-8111-111111111111",n2="22222222-2222-4222-8222-222222222222";
async function fresh(){const x=await replayDatabase();open.push(x);await addReplayPointer(x.db,{key:"96bulls/org_replay/v1/ws=ws_replay/m=m/h=claude/d=2026-11-01/s=physical/manifest.json",digest:"a".repeat(64),physicalSession:"physical",physicalMachine:"m"});await projectSessionDirectory(x.db,{orgId:i.orgId,workspaceId:i.workspaceId,harness:i.source,sessionId:"physical"});return x}
const enqueue=async(x:any)=>(await x.db.query("SELECT sessions.enqueue_session_large_read($1,$2,$3,$4) r",scope)).rows[0].r;
const claim=async(x:any,nonce=n1)=>(await x.db.query("SELECT sessions.claim_session_large_read($1::uuid,120) r",[nonce])).rows[0].r;
async function pub(x:any,nonce=n1){const c=(await x.db.query("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",scope)).rows[0].c,p=c.snapshot.pointers[0],generation="c".repeat(64),hash="b".repeat(64),prefix=p.manifest_key.slice(0,-"manifest.json".length)+"normalized/read-projection-v1/"+p.content_sha256+"/"+generation+"/";return(await x.db.query("SELECT sessions.publish_session_large_read($1,$2,$3,$4,$5::bigint,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::integer,$14::uuid) ok",[...scope,c.source.epoch,c.source.fingerprint,JSON.stringify(c.snapshot),generation,"d".repeat(64),prefix,prefix+hash+".json",hash,100,nonce])).rows[0].ok}
test("duplicates collapse, one lease owns publication, and success requires actual source CAS",async()=>{const x=await fresh();expect((await enqueue(x)).enqueued).toBe(true);await enqueue(x);expect((await x.pg.query("SELECT count(*) n FROM sessions.session_large_read_jobs")).rows[0].n).toBe(1);expect((await claim(x)).leaseNonce).toBe(n1);expect(await claim(x,n2)).toBeNull();expect(await pub(x,n2)).toBe(false);expect(await pub(x,n1)).toBe(true);expect((await x.pg.query("SELECT state FROM sessions.session_large_read_jobs")).rows[0].state).toBe("complete");expect(await claim(x,n2)).toBeNull()});
test("expired lease is reclaimed and the old nonce can neither heartbeat nor publish",async()=>{const x=await fresh();await enqueue(x);await claim(x);await x.pg.exec("UPDATE sessions.session_large_read_jobs SET lease_expires_at=now()-interval '1 second'");expect(await pub(x,n1)).toBe(false);expect((await claim(x,n2)).leaseNonce).toBe(n2);expect((await x.db.query("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",[...scope,n1])).rows[0].ok).toBe(false);expect(await pub(x,n1)).toBe(false);expect(await pub(x,n2)).toBe(true)});
test("owner ABA supersedes an old job and a new source epoch gets a fresh lease",async()=>{const x=await fresh();await enqueue(x);const old=await claim(x);await x.pg.exec("UPDATE sessions.session_index SET user_id='other';UPDATE sessions.session_index SET user_id='user_replay'");expect(await pub(x)).toBe(false);const next=await enqueue(x);expect(next.source.epoch).not.toBe(old.source.epoch);expect((await claim(x,n2)).leaseNonce).toBe(n2);expect(await pub(x,n2)).toBe(true)});
test("directory DELETE prevents enqueue, heartbeat and publication with surviving pointers",async()=>{const x=await fresh();await enqueue(x);await claim(x);await x.pg.exec("DELETE FROM sessions.session_directory");expect((await enqueue(x)).enqueued).toBe(false);expect(await pub(x)).toBe(false);expect((await x.db.query("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",[...scope,n1])).rows[0].ok).toBe(false)});
test("routing handles oversized and unknown metadata durably before acknowledging",async()=>{const x=await fresh();expect((await x.db.query("SELECT sessions.route_session_large_read($1,$2,$3,$4) r",scope)).rows[0].r.delegated).toBe(true);await x.pg.exec("UPDATE sessions.session_index SET raw_bytes=10,event_count=2");expect((await x.db.query("SELECT sessions.route_session_large_read($1,$2,$3,$4) r",scope)).rows[0].r.delegated).toBe(false);await x.pg.exec("UPDATE sessions.session_index SET raw_bytes=67108865");expect((await x.db.query("SELECT sessions.route_session_large_read($1,$2,$3,$4) r",scope)).rows[0].r.delegated).toBe(true);expect((await x.pg.query("SELECT state FROM sessions.session_large_read_jobs")).rows[0].state).toBe("queued")});
test("public search worker cannot read the ledger or execute backend handoff",async()=>{const x=await fresh();await x.pg.exec("SET ROLE session_directory_search_worker");await expect(x.pg.query("SELECT * FROM sessions.session_large_read_jobs")).rejects.toMatchObject({code:"42501"});await expect(enqueue(x)).rejects.toMatchObject({code:"42501"});await x.pg.exec("RESET ROLE")});
test("retries keep only bounded error codes, cap poison attempts, and reject stale nonce",async()=>{const x=await fresh();await enqueue(x);await claim(x);await expect(x.db.query("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...scope,n1,"private error body"])).rejects.toMatchObject({code:"22023"});expect((await x.db.query("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...scope,n2,"failed"])).rows[0].ok).toBe(false);await x.pg.exec("UPDATE sessions.session_large_read_jobs SET attempts=5");expect((await x.db.query("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...scope,n1,"failed"])).rows[0].ok).toBe(true);expect((await x.pg.query("SELECT state FROM sessions.session_large_read_jobs")).rows[0].state).toBe("failed");expect(await claim(x,n2)).toBeNull()});
test("larger Node profile is explicit and bounded while standard admission stays unchanged",()=>{expect(nodeReadBudget().sourceBytes).toBe(256*1024**2);expect(nodeReadBudget().projectedRows).toBe(500000);expect(nodeReadBudget("large").sourceBytes).toBe(2*1024**3);expect(nodeReadBudget("large").artifactBytes).toBe(16*1024**3);expect(()=>nodeReadBudget("unbounded" as any)).toThrow("node_read_profile_invalid")});

// Hold a real transaction open across wall-clock expiry. now() alone cannot
// distinguish these cases because PostgreSQL freezes it at transaction start.
test.each(["heartbeat","publish","retry"] as const)("held transaction cannot %s an actually expired lease",async(operation)=>{
 const x=await fresh();await enqueue(x);await claim(x);await x.pg.exec("BEGIN");
 try{
  await x.pg.exec("UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '50 milliseconds'");
  await new Promise(resolve=>setTimeout(resolve,80));
  expect((await x.pg.query<{expired:boolean}>("SELECT lease_expires_at<clock_timestamp() expired FROM sessions.session_large_read_jobs")).rows[0]!.expired).toBe(true);
  const accepted=operation==="publish"?await pub(x):operation==="heartbeat"?(await x.db.query<{ok:boolean}>("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",[...scope,n1])).rows[0]!.ok:(await x.db.query<{ok:boolean}>("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...scope,n1,"failed"])).rows[0]!.ok;
  expect(accepted).toBe(false);
 }finally{await x.pg.exec("ROLLBACK")}
});
test.each(["queued","running"] as const)("held transaction claims a %s job when wall time reaches its due boundary",async(state)=>{
 const x=await fresh();await enqueue(x);if(state==="running")await claim(x);await x.pg.exec("BEGIN");
 try{
  await x.pg.exec(state==="queued"?"UPDATE sessions.session_large_read_jobs SET available_at=clock_timestamp()+interval '50 milliseconds'":"UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '50 milliseconds'");
  await new Promise(resolve=>setTimeout(resolve,80));
  expect((await claim(x,n2))?.leaseNonce).toBe(n2);
 }finally{await x.pg.exec("ROLLBACK")}
});
test("claim and heartbeat extend from invocation wall time inside a held transaction",async()=>{
 const x=await fresh();await enqueue(x);await x.pg.exec("BEGIN");
 try{
  await new Promise(resolve=>setTimeout(resolve,80));
  const invoked=(await x.pg.query<{at:string}>("SELECT clock_timestamp()::text at")).rows[0]!.at;
  expect((await claim(x)).leaseNonce).toBe(n1);
  expect((await x.pg.query<{fresh:boolean}>("SELECT lease_expires_at >= $1::timestamptz+interval '120 seconds' fresh FROM sessions.session_large_read_jobs",[invoked])).rows[0]!.fresh).toBe(true);
  const beatAt=(await x.pg.query<{at:string}>("SELECT clock_timestamp()::text at")).rows[0]!.at;
  expect((await x.db.query<{ok:boolean}>("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",[...scope,n1])).rows[0]!.ok).toBe(true);
  expect((await x.pg.query<{fresh:boolean}>("SELECT lease_expires_at >= $1::timestamptz+interval '120 seconds' fresh FROM sessions.session_large_read_jobs",[beatAt])).rows[0]!.fresh).toBe(true);
  const retryAt=(await x.pg.query<{at:string}>("SELECT clock_timestamp()::text at")).rows[0]!.at;
  expect((await x.db.query<{ok:boolean}>("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...scope,n1,"failed"])).rows[0]!.ok).toBe(true);
  expect((await x.pg.query<{fresh:boolean}>("SELECT available_at >= $1::timestamptz+interval '30 seconds' fresh FROM sessions.session_large_read_jobs",[retryAt])).rows[0]!.fresh).toBe(true);
 }finally{await x.pg.exec("ROLLBACK")}
});

test("heartbeat cannot renew after source capture stalls across lease expiry",async()=>{
 const x=await fresh();await enqueue(x);await claim(x);
 await x.pg.exec(`ALTER FUNCTION sessions.capture_session_replay_pages(text,text,text,text) RENAME TO capture_session_replay_pages_unstalled;
 CREATE FUNCTION sessions.capture_session_replay_pages(p_org text,p_workspace text,p_harness text,p_session text) RETURNS jsonb LANGUAGE plpgsql STABLE AS $stall$
 BEGIN PERFORM pg_sleep(0.15); RETURN sessions.capture_session_replay_pages_unstalled(p_org,p_workspace,p_harness,p_session); END $stall$;`);
 await x.pg.exec("UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '100 milliseconds'");
 expect((await x.db.query<{ok:boolean}>("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",[...scope,n1])).rows[0]!.ok).toBe(false);
 expect((await x.pg.query<{expired:boolean}>("SELECT lease_expires_at<clock_timestamp() expired FROM sessions.session_large_read_jobs")).rows[0]!.expired).toBe(true);
});
test("publication rolls back provisional artifacts when its nested write stalls across expiry",async()=>{
 const x=await fresh();await enqueue(x);await claim(x);
 await x.pg.exec(`CREATE FUNCTION sessions.stall_large_read_publication() RETURNS trigger LANGUAGE plpgsql AS $stall$ BEGIN PERFORM pg_sleep(0.15); RETURN NEW; END $stall$;
 CREATE TRIGGER stall_large_read_publication BEFORE UPDATE ON sessions.session_replay_publications FOR EACH ROW EXECUTE FUNCTION sessions.stall_large_read_publication();`);
 await x.pg.exec("UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '100 milliseconds'");
 expect(await pub(x)).toBe(false);
 expect((await x.pg.query<{index_key:string|null}>("SELECT index_key FROM sessions.session_replay_publications")).rows[0]!.index_key).toBeNull();
 expect((await x.pg.query<{state:string}>("SELECT state FROM sessions.session_large_read_jobs")).rows[0]!.state).toBe("running");
});

test('durable discovery reaches a129th cohort, rotates, and coalesces scans',async()=>{
 const x=await fresh();
 for(let n=0;n<129;n++){
  const id='tail-'+String(n).padStart(3,'0');
  await addReplayPointer(x.db,{key:'96bulls/org_replay/v1/ws=ws_replay/m=m/h=claude/d=2026-11-01/s='+id+'/manifest.json',digest:'a'.repeat(64),physicalSession:id,physicalMachine:'m',canonicalSession:id});
  await projectSessionDirectory(x.db,{orgId:i.orgId,workspaceId:i.workspaceId,harness:i.source,sessionId:id});
 }
 const discover=async()=>(await x.db.query<{r:{scanned:number;changed:number;cycleComplete:boolean}}>('SELECT sessions.discover_session_large_reads() r')).rows[0]!.r;
 expect(await discover()).toMatchObject({scanned:128,changed:128,cycleComplete:false});
 expect(await discover()).toMatchObject({scanned:2,changed:2,cycleComplete:true});
 expect((await x.pg.query<{n:number}>('SELECT count(*)::int n FROM sessions.session_large_read_jobs')).rows[0]!.n).toBe(130);
 expect(await discover()).toMatchObject({scanned:128,changed:0,cycleComplete:false});
 await x.pg.exec('SET ROLE session_directory_search_worker');
 await expect(discover()).rejects.toMatchObject({code:'42501'});
 await expect(x.pg.query('SELECT * FROM sessions.session_large_read_scan')).rejects.toMatchObject({code:'42501'});
 await x.pg.exec('RESET ROLE');
});
