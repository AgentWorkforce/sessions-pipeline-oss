
import {describe,test,expect} from "bun:test";
import worker,{ingestSettings,type IngestWorkerEnv} from "../worker.ts";
const full:IngestWorkerEnv={HYPERDRIVE:{connectionString:"postgres://synthetic-invalid-unused"},INGEST_FANOUT:{async send(){throw Error("queue_unused");},async sendBatch(){throw Error("queue_unused");}},S3_ACCESS_KEY_ID:"synthetic",S3_SECRET_ACCESS_KEY:"synthetic",INGEST_SIGNING_KEY:"synthetic",INGEST_S3_BUCKET:"synthetic",INGEST_KMS_KEY_ARN:"synthetic"};
describe("buffer-only runtime gate",()=>{
 test("default canonical unchanged; explicit buffered requires inline0 and replicaoff",async()=>{
  expect((await ingestSettings(full))?.acceptanceMode).toBe("canonical");
  expect((await ingestSettings(full))?.inlineMaxSessions).toBe(40);
  await expect(ingestSettings({...full,INGEST_INLINE_MAX_SESSIONS:"0"})).rejects.toThrow();
  await expect(ingestSettings({...full,INGEST_ACCEPTANCE_MODE:"buffered"})).rejects.toThrow();
  const s=await ingestSettings({...full,INGEST_ACCEPTANCE_MODE:"buffered",INGEST_INLINE_MAX_SESSIONS:"0",INGEST_REPLICA_MODE:"off",INGEST_EXPECT_GENERATION:"a".repeat(64)});
  expect(s?.acceptanceMode).toBe("buffered");expect(s?.inlineMaxSessions).toBe(0);expect(s?.replicaMode).toBe("off");
  await expect(ingestSettings({...full,INGEST_ACCEPTANCE_MODE:"buffered",INGEST_INLINE_MAX_SESSIONS:"0",INGEST_REPLICA_MODE:"required"})).rejects.toThrow();
  await expect(ingestSettings({...full,INGEST_ACCEPTANCE_MODE:"caller-chosen"})).rejects.toThrow();
 });
 test("accidentally unpaused queue retries without DB connection, S3 or ACK",async()=>{
  let connectionReads=0,retries=0,acks=0;
  const env={...full,INGEST_ACCEPTANCE_MODE:"buffered",INGEST_INLINE_MAX_SESSIONS:"0",INGEST_EXPECT_GENERATION:"a".repeat(64),
   HYPERDRIVE:{get connectionString(){connectionReads++;throw Error("db_unused");}}};
  await worker.queue({messages:[{body:{v:1,kind:"ingest-fanout",key:"never-read",from:0,to:1},ack(){acks++;},retry(o){expect(o?.delaySeconds).toBe(300);retries++;}}]},env);
  expect(connectionReads).toBe(0);expect(retries).toBe(1);expect(acks).toBe(0);
 });
});

