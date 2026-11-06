import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ManifestV1 } from "../../shared/vendor.ts";
import { objectStoreManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { chooseRelayhistoryRevision } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { canonicalManifestIdentity } from "../../shared/manifest-identity.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { writeBatchObjects } from "../writer.ts";
import { SeedProofIndex, type SeedPointer, type ProofDeps } from "../seed-proof.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const cleanup:Array<()=>void>=[];
afterEach(()=>cleanup.splice(0).forEach(f=>f()));
const sha=(s:string)=>createHash("sha256").update(s).digest("hex");
async function fixture() {
 const fs=scratchStore();cleanup.push(fs.done);
 const dir=mkdtempSync(join(tmpdir(),"ingest-seed-proof-"));cleanup.push(()=>rmSync(dir,{recursive:true,force:true}));
 const deps:ProofDeps={store:fs.store,bucket:"fixture",provider:"s3",
  reader:objectStoreManifestReader(fs.store,{provider:"s3",bucket:"fixture"}),
  prefixFor:org=>"96bulls/"+org,
  decompress:(raw,max)=>new Uint8Array(zstdDecompressSync(raw,{maxOutputLength:max}))};
 const write=async(revision:number,text:string,operation:"upsert"|"delete"="upsert")=>{
  const p=await prepareDelivery(TENANT,await batch([record({record_id:"record1",revision,operation,
    revision_id:"original-revision-"+revision,payload:operation==="delete"?null:{text,ts_ms:1761955200000}})],{batch_id:"batch-"+revision+"-"+text}));
  return writeBatchObjects({store:fs.store,keyRoot:"96bulls",sleep:async()=>{}},{
    orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:TENANT.userId,originId:p.originId,batchDigest:p.batchDigest,records:p.records});
 };
 const pointer=async():Promise<SeedPointer>=>{
  const listed=await fs.store.list(objectKey("96bulls/org-a/v1"));
  const key=String(listed.items.find(x=>String(x.key).endsWith("manifest.json"))!.key);
  const manifest=JSON.parse(new TextDecoder().decode(await readAll((await fs.store.getObject(objectKey(key))).body))) as ManifestV1;
  const canonical=canonicalManifestIdentity(manifest);
  return {org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,harness:manifest.session.harness,
    session_id:canonical.sessionId,machine_id:canonical.machineId,bucket:"fixture",storage_provider:"s3",
    manifest_key:key,revision:String(manifest.revision),content_sha256:manifest.raw.content_sha256,raw_format:"ndjson",
    capture:{pointer_id:sha(key+manifest.raw.content_sha256),scope_sha256:sha("scope"),reset_epoch:"0",fence_versions:{}}};
 };
 return {...fs,dir,deps,write,pointer};
}
test("verified S3 revision bound retains no invented original ID/digest or payload",async()=>{
 const f=await fixture();await f.write(7,"private-payload-marker-should-never-persist");
 const p=await f.pointer(),path=join(f.dir,"proof.sqlite"),index=new SeedProofIndex(path,TENANT.orgId,"a".repeat(40));
 try{
  await index.addPointer(p,f.deps);expect(index.seal([p]).records).toBe(1);
  const b=[...index.batches()][0]!;
  expect(b.records[0]!.revision).toBe("7");
  expect(b.records[0]).not.toHaveProperty("revision_id");
  expect(b.records[0]).not.toHaveProperty("digest");
  expect(b.records[0]).not.toHaveProperty("payload");
  expect(b.records[0]!.semantic_sha256).toMatch(/^[a-f0-9]{64}$/);
 }finally{index.close()}
 expect(readFileSync(path).includes(Buffer.from("private-payload-marker-should-never-persist"))).toBe(false);
});
test("highest revision and tombstone are proof seats, not filtered away",async()=>{
 const f=await fixture();await f.write(7,"original");await f.write(9,"deleted","delete");
 const p=await f.pointer(),index=new SeedProofIndex(join(f.dir,"proof.sqlite"),TENANT.orgId,"a".repeat(40));
 try{await index.addPointer(p,f.deps);index.seal([p]);expect([...index.batches()][0]!.records[0]).toMatchObject({revision:"9",operation:"delete"})}
 finally{index.close()}
});
test("equal revision changed bytes fails the authoritative fold before sealing",async()=>{
 const f=await fixture();await f.write(7,"original");await f.write(7,"changed");
 const p=await f.pointer(),index=new SeedProofIndex(join(f.dir,"proof.sqlite"),TENANT.orgId,"a".repeat(40));
 try{await expect(index.addPointer(p,f.deps)).rejects.toMatchObject({code:"relayhistory_revision_conflict"});
  expect(()=>index.seal([p])).toThrow("seed_index_not_building")}
 finally{index.close()}
});
test("legacy live ownership requires every origin's cryptographic physical machine",async()=>{
 const f=await fixture();const written=await f.write(7,"original");
 const m=written.manifests[0]!;
 delete m.manifest.extensions!["96bulls"];
 m.manifest.writer.plugin_version="relayhistory-api/legacy";
 m.manifest.writer.delivery_instance_id="relayhistory-api";
 await f.store.put(objectKey(m.key),new TextEncoder().encode(JSON.stringify(m.manifest)));
 const p=await f.pointer(),index=new SeedProofIndex(join(f.dir,"good.sqlite"),TENANT.orgId,"a".repeat(40));
 try{await index.addPointer(p,f.deps);expect(index.seal([p]).records).toBe(1)}finally{index.close()}
 const bad=new SeedProofIndex(join(f.dir,"bad.sqlite"),TENANT.orgId,"a".repeat(40));
 try{await expect(bad.addPointer({...p,user_id:"other-user"},f.deps)).rejects.toMatchObject({code:"seed_manifest_owner"})}finally{bad.close()}
});
test("foreign namespace, owner and corrupted chunk cannot produce a seed proof",async()=>{
 const f=await fixture();const w=await f.write(7,"original");const p=await f.pointer();
 for(const delta of [{org_id:"other-org"},{user_id:"other-user"},{bucket:"foreign"},{session_id:"foreign"}]){
  const index=new SeedProofIndex(join(f.dir,sha(JSON.stringify(delta))+".sqlite"),TENANT.orgId,"a".repeat(40));
  try{await expect(index.addPointer({...p,...delta},f.deps)).rejects.toThrow()}finally{index.close()}
 }
 const chunk=w.parts[0]!.chunkKey;const bytes=await readAll((await f.store.getObject(objectKey(chunk))).body);bytes[0]^=1;
 await f.store.put(objectKey(chunk),bytes);
 const index=new SeedProofIndex(join(f.dir,"corrupt.sqlite"),TENANT.orgId,"a".repeat(40));
 try{await expect(index.addPointer(p,f.deps)).rejects.toMatchObject({code:"chunk_digest_mismatch"})}finally{index.close()}
});
test("source-generation change and incomplete pointer inventory refuse seal",async()=>{
 const f=await fixture();await f.write(7,"original");const p=await f.pointer();
 const index=new SeedProofIndex(join(f.dir,"proof.sqlite"),TENANT.orgId,"a".repeat(40));
 try{
  await index.addPointer(p,f.deps);
  expect(()=>index.seal([])).toThrow("seed_source_changed");
  expect(()=>index.seal([{...p,capture:{...p.capture,pointer_id:sha("changed")}}])).toThrow("seed_source_changed");
  expect(index.seal([p]).records).toBe(1);
 }finally{index.close()}
});
test("crash resume re-reads bytes and never adopts a cached presence boolean",async()=>{
 const f=await fixture();const w=await f.write(7,"original");const p=await f.pointer(),path=join(f.dir,"proof.sqlite");
 let index=new SeedProofIndex(path,TENANT.orgId,"a".repeat(40));
 await index.addPointer(p,f.deps);index.seal([p]);index.close();
 await f.store.delete(objectKey(w.parts[0]!.chunkKey));
 index=new SeedProofIndex(path,TENANT.orgId,"a".repeat(40));
 try{index.restartVerification();await expect(index.addPointer(p,f.deps)).rejects.toMatchObject({code:"chunk_missing"})}finally{index.close()}
});
test("checkpoint acknowledgement is idempotent and source/index identity cannot be adopted",async()=>{
 const f=await fixture();await f.write(7,"original");const p=await f.pointer(),path=join(f.dir,"proof.sqlite");
 const index=new SeedProofIndex(path,TENANT.orgId,"a".repeat(40));
 try{await index.addPointer(p,f.deps);index.seal([p]);const b=[...index.batches()][0]!;
  index.acknowledged(b.batch_sha256,b.records.length);expect([...index.batches()]).toHaveLength(0);
 }finally{index.close()}
 expect(()=>new SeedProofIndex(path,TENANT.orgId,"b".repeat(40))).toThrow("seed_index_identity");
});
test("shared evidence fold preserves acceptance conflicts, earlier received copy and int64 revisions",()=>{
 const a={revision:"9223372036854775806",received_at:"2026-01-01T00:00:00Z",accepted_seq:"5",content:"same"};
 const b={...a,received_at:"2026-01-02T00:00:00Z"};
 expect(chooseRelayhistoryRevision(a,b,(a,b)=>a.content===b.content)).toEqual(a);
 expect(chooseRelayhistoryRevision(a,{...b,revision:"9223372036854775807"},()=>false).revision).toBe("9223372036854775807");
 expect(()=>chooseRelayhistoryRevision(a,{...b,accepted_seq:"6"},()=>true)).toThrow();
});
