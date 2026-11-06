/**
 * Per-record S3 evidence for offline ingest bootstrap. SQLite stores only IDs,
 * revision seats and hashes; transcript payloads are verified a chunk at a time.
 */
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import type { ManifestReader } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import { computeContentSha256, parseSessionKey } from "../../packages/object-store/src/index.ts";
import { chooseRelayhistoryRevision, relayhistoryRevisionContent, relayhistoryExportProvenance, type RelayhistoryRevisionSeat } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { streamManifestRecords } from "../session-read-api/canonical-projection.ts";
import { canonicalManifestIdentity, manifestUserId } from "../shared/manifest-identity.ts";
import { DELIVERY_KINDS, retiredDeliveryRecord } from "./vendor/delivery/delivery-contracts.ts";
import { LIVE_OWNER, liveMachineId, NO_SESSION_HARNESS, NO_SESSION_ID } from "./writer.ts";

export const SEED_PROOF_PROTOCOL = 1;
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
export class SeedProofError extends Error {
  constructor(readonly code: string) { super(code); }
}
const fail = (code: string): never => { throw new SeedProofError(code); };
export interface SeedPointer {
  org_id: string; workspace_id: string; user_id: string; harness: string;
  session_id: string; machine_id: string; bucket: string; storage_provider: string;
  manifest_key: string; revision: string; content_sha256: string; raw_format: string;
  /** Captured server metadata including row-version and deletion/reset epochs. */
  capture: Record<string, unknown>;
}
export interface SeedEvidence extends RelayhistoryRevisionSeat {
  org_id: string; workspace_id: string; user_id: string; source: string;
  session_id: string | null; origin_id: string; record_id: string;
  kind: string; operation: "upsert" | "delete"; semantic_sha256: string;
  pointer_id: string;
}
export interface ProofDeps {
  store: ObjectStore; reader: ManifestReader; bucket: string; provider: string;
  prefixFor(orgId: string): string;
  decompress(stored: Uint8Array, maxBytes: number): Uint8Array;
}
const identity = (p: SeedPointer) => JSON.stringify([
  p.org_id,p.workspace_id,p.user_id,p.harness,p.session_id,p.machine_id,p.bucket,
  p.storage_provider,p.manifest_key,p.revision,p.content_sha256,p.capture,
]);
const recordKey = (r: SeedEvidence) => JSON.stringify([r.org_id,r.workspace_id,r.origin_id,r.record_id]);
const namespace = (r: SeedEvidence) => JSON.stringify([r.user_id,r.source,r.session_id,r.kind]);
export class SeedProofIndex {
  private db: DatabaseSync;
  private getRecord: ReturnType<DatabaseSync["prepare"]>;
  private putRecord: ReturnType<DatabaseSync["prepare"]>;
  constructor(readonly path: string, readonly orgId: string, readonly sourceHead: string) {
    if (!orgId || !/^[a-f0-9]{40}$/.test(sourceHead)) fail("seed_index_identity");
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      PRAGMA cache_size=-65536; PRAGMA temp_store=FILE; PRAGMA max_page_count=8388608;
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS pointers (
        id TEXT PRIMARY KEY,metadata TEXT NOT NULL,state TEXT NOT NULL,records INTEGER NOT NULL DEFAULT 0,unseeded_records INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS records (
        key TEXT PRIMARY KEY,namespace TEXT NOT NULL,evidence TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS progress (
        batch_sha TEXT PRIMARY KEY,committed_rows INTEGER NOT NULL); `);
    this.getRecord=this.db.prepare("SELECT namespace,evidence FROM records WHERE key=?");
    this.putRecord=this.db.prepare("INSERT INTO records VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET evidence=excluded.evidence");
    const expected = JSON.stringify({protocol:SEED_PROOF_PROTOCOL,orgId,sourceHead});
    const prior = this.db.prepare("SELECT value FROM meta WHERE key='identity'").get() as {value:string}|undefined;
    if (prior && prior.value !== expected) fail("seed_index_identity");
    this.db.prepare("INSERT OR IGNORE INTO meta VALUES('identity',?)").run(expected);
    this.db.prepare("INSERT OR IGNORE INTO meta VALUES('state','building')").run();
  }
  /** Crash resume re-verifies every S3 byte; no cached presence boolean is trusted. */
  restartVerification() {
    if(this.state()==="failed")fail("seed_index_failed");
    this.setState("building");
    this.db.exec("UPDATE pointers SET state='building'");
  }
  close() { this.db.close(); }
  private state() { return (this.db.prepare("SELECT value FROM meta WHERE key='state'").get() as {value:string}).value; }
  private setState(state:string) { this.db.prepare("UPDATE meta SET value=? WHERE key='state'").run(state); }
  private add(evidence:SeedEvidence) {
    const key = recordKey(evidence), ns = namespace(evidence);
    const old = this.getRecord.get(key) as {namespace:string;evidence:string}|undefined;
    if (old && old.namespace !== ns) fail("seed_record_namespace_conflict");
    const previous = old ? JSON.parse(old.evidence) as SeedEvidence : undefined;
    const winner = chooseRelayhistoryRevision(previous,evidence,(a,b)=>a.semantic_sha256===b.semantic_sha256);
    this.putRecord.run(key,ns,JSON.stringify(winner));
  }
  async addPointer(pointer:SeedPointer,deps:ProofDeps) {
    if (this.state() !== "building") fail("seed_index_not_building");
    if (pointer.org_id !== this.orgId || pointer.bucket !== deps.bucket ||
        pointer.storage_provider !== deps.provider || !["ndjson","jsonl","sqlite-rows"].includes(pointer.raw_format) ||
        !/^[a-f0-9]{64}$/.test(pointer.content_sha256) ||
        !/^[1-9][0-9]*$/.test(pointer.revision)) fail("seed_pointer_identity");
    const id=String(pointer.capture.pointer_id ?? "");
    if(!/^[a-f0-9]{64}$/.test(id)) fail("seed_pointer_capture");
    const old=this.db.prepare("SELECT metadata,state FROM pointers WHERE id=?").get(id) as {metadata:string;state:string}|undefined;

    if (old && old.metadata !== JSON.stringify(pointer)) fail("seed_pointer_identity");
    this.db.prepare("INSERT OR IGNORE INTO pointers(id,metadata,state) VALUES(?,?,'building')")
      .run(id,JSON.stringify(pointer));
    let transaction=false;
    try {
      const read=await deps.reader.read(pointer.manifest_key);
      if (!read) fail("seed_manifest_missing");
      const manifest=read!.manifest, canonical=canonicalManifestIdentity(manifest);
      const at=parseSessionKey(pointer.manifest_key);
      if (!at || at.prefix!==deps.prefixFor(pointer.org_id) ||
          !((at.prefix??"").endsWith("/"+pointer.org_id) || at.prefix===pointer.org_id) ||
          at.rest!=="manifest.json" || at.workspaceId!==pointer.workspace_id ||
          at.machineId!==manifest.session.machine_id || at.sessionId!==manifest.session.session_id ||
          at.harness!==manifest.session.harness ||
          manifest.session.workspace_id!==pointer.workspace_id || manifest.session.harness!==pointer.harness ||
          canonical.sessionId!==pointer.session_id || canonical.machineId!==pointer.machine_id ||
          manifest.raw.format!==pointer.raw_format || String(manifest.revision)!==pointer.revision ||
          manifest.raw.content_sha256!==pointer.content_sha256 ||
          await computeContentSha256(manifest.raw.chunks)!==pointer.content_sha256)
        fail("seed_manifest_identity");
      let exported=false;
      if (manifest.extensions?.["client.relayhistory_export"]!==undefined) {
        relayhistoryExportProvenance(manifest); exported=true;
      }
      if (manifest.raw.format!=="ndjson" || (!exported && manifest.raw.owner!==LIVE_OWNER)) {
        this.db.prepare("UPDATE pointers SET state='non_delivery',records=0 WHERE id=?").run(id);
        return;
      }
      const owner=manifestUserId(manifest);
      // Old live manifests lack the admitted extension. Their user is provable only
      // through the existing writer's exact physical machine derivation, for EVERY row.
      const legacyOwner=owner===undefined && manifest.raw.owner===LIVE_OWNER &&
        manifest.writer.delivery_instance_id==="relayhistory-api";
      if (owner!==pointer.user_id && !legacyOwner) fail("seed_manifest_owner");
      let count=0,unseeded=0;
      this.db.exec("BEGIN"); transaction=true;
      for await (const record of streamManifestRecords(manifest,deps.store,deps.prefixFor(pointer.org_id),deps.decompress)) {
        if (legacyOwner && liveMachineId(pointer.workspace_id,record.origin_id,pointer.user_id)!==manifest.session.machine_id)
          fail("seed_manifest_owner");
        count++;
        if (!(DELIVERY_KINDS as readonly string[]).includes(record.kind)) {
          if(record.kind==="convergence_event" || retiredDeliveryRecord(record)) {unseeded++;continue;}
          fail("seed_record_kind_unsupported");
        }
        const noSession=pointer.harness===NO_SESSION_HARNESS && pointer.session_id===NO_SESSION_ID;
        this.add({
          org_id:pointer.org_id,workspace_id:pointer.workspace_id,user_id:pointer.user_id,
          source:pointer.harness,session_id:noSession?null:pointer.session_id,
          origin_id:record.origin_id,record_id:record.record_id,kind:record.kind,
          revision:record.revision,operation:record.operation,received_at:record.received_at,
          ...(record.accepted_seq===undefined?{}:{accepted_seq:record.accepted_seq}),
          semantic_sha256:hash(relayhistoryRevisionContent(record)),pointer_id:id,
        });
        if (count%5000===0) { this.db.exec("COMMIT;BEGIN"); }
      }
      const declared=manifest.raw.chunks.reduce((n,c)=>n+(c.records??NaN),0);
      if (!Number.isSafeInteger(declared) || declared!==count) fail("seed_record_count");
      this.db.prepare("UPDATE pointers SET state='complete',records=?,unseeded_records=? WHERE id=?").run(count,unseeded,id);
      this.db.exec("COMMIT");transaction=false;
    } catch(error) {
      if(transaction) this.db.exec("ROLLBACK");
      this.db.prepare("UPDATE pointers SET state='failed' WHERE id=?").run(id);
      this.setState("failed");
      throw error;
    }
  }
  /** A complete second capture is mandatory; a session boolean is never evidence. */
  seal(currentPointers:SeedPointer[]) {
    if(this.state()!=="building")fail("seed_index_not_building");
    const actual=(this.db.prepare("SELECT id,state FROM pointers ORDER BY id").all() as {id:string;state:string}[]);
    const expected=currentPointers.map(p=>String(p.capture.pointer_id??"")).sort();
    if(new Set(expected).size!==expected.length || actual.some(p=>p.state!=="complete" && p.state!=="non_delivery") ||
       JSON.stringify(actual.map(p=>p.id))!==JSON.stringify(expected))fail("seed_source_changed");
    const snapshot=hash(JSON.stringify([SEED_PROOF_PROTOCOL,this.orgId,this.sourceHead,expected]));
    this.db.prepare("INSERT INTO meta VALUES('snapshot',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(snapshot);
    this.setState("sealed");
    return {snapshot_sha256:snapshot,pointers:actual.length,
      non_delivery_pointers:actual.filter(p=>p.state==="non_delivery").length,
      unseeded_records:(this.db.prepare("SELECT coalesce(sum(unseeded_records),0) AS n FROM pointers").get() as {n:number}).n,
      records:(this.db.prepare("SELECT count(*) AS n FROM records").get() as {n:number}).n};
  }
  *batches(limit=1000) {
    if(this.state()!=="sealed" || !Number.isSafeInteger(limit) || limit<1 || limit>1000)fail("seed_index_not_sealed");
    const snapshot=(this.db.prepare("SELECT value FROM meta WHERE key='snapshot'").get() as {value:string}).value;
    let after="";
    for(;;) {
      const rows=this.db.prepare("SELECT key,evidence FROM records WHERE key>? ORDER BY key LIMIT ?").all(after,limit) as {key:string;evidence:string}[];
      if(!rows.length)break;
      const records=rows.map(r=>JSON.parse(r.evidence) as SeedEvidence);
      const pointerIds=[...new Set(records.map(r=>r.pointer_id))].sort();
      const pointers=pointerIds.map(id=>JSON.parse((this.db.prepare("SELECT metadata FROM pointers WHERE id=? AND state='complete'").get(id) as {metadata:string}).metadata) as SeedPointer);
      const batch={protocol:SEED_PROOF_PROTOCOL,snapshot_sha256:snapshot,source_head:this.sourceHead,pointer_ids:pointerIds,pointers,records};
      const batch_sha256=hash(JSON.stringify(batch));
      if(!this.db.prepare("SELECT 1 FROM progress WHERE batch_sha=?").get(batch_sha256))
        yield { ...batch,batch_sha256 };
      after=rows.at(-1)!.key;
    }
  }
  acknowledged(batchSha:string,rows:number) {
    if(!/^[a-f0-9]{64}$/.test(batchSha) || !Number.isSafeInteger(rows) || rows<0)fail("seed_ack");
    this.db.prepare("INSERT OR IGNORE INTO progress VALUES(?,?)").run(batchSha,rows);
  }
}
