import{test,expect}from'bun:test';
import{publicationEnabled,runCanonicalPublicationRuntime,publicationRuntimeConfig,scopedPublicationStore}from'../publication-runtime.ts';
import type{ObjectStore}from'../../../packages/object-store/src/index.ts';
test('publish and erasure have separate default-off gates; disabled does no config/credential/DB/store work',async()=>{
 let calls=0;const fake:any={assets:()=>{calls++;throw Error('unexpected_assets')}};
 expect(await runCanonicalPublicationRuntime({},'publish',fake)).toEqual({enabled:false});expect(await runCanonicalPublicationRuntime({},'gc',fake)).toEqual({enabled:false});expect(calls).toBe(0);
 expect(publicationEnabled({INGEST_PUBLICATION_GC_ENABLED:'true'},'gc')).toBe(true);expect(publicationEnabled({INGEST_PUBLICATION_GC_ENABLED:'true'},'publish')).toBe(false);
 expect(()=>publicationEnabled({INGEST_PUBLISH_ENABLED:'1'},'publish')).toThrow('publication_enabled_invalid');
});
test('runtime refuses wrong compute/account/region/source generation inputs before any factories',()=>{
 expect(()=>publicationRuntimeConfig({AWS_REGION:'us-west-2'})).toThrow('publication_target_required');
 const good={AWS_REGION:'us-east-1',HOOPDB_DB_MODE:'supabase',INGEST_PUBLISH_BUCKET:'owned-bucket',INGEST_PUBLISH_EXPECT_GENERATION:'a'.repeat(64),INGEST_PUBLISH_SECRET_ARN:'arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls/test/canonical-publisher-abcdef',INGEST_PUBLISH_KMS_KEY_ARN:'arn:aws:kms:us-east-1:111122223333:key/12345678-1234-1234-1234-123456789abc',INGEST_PUBLISH_INSTANCE_PROFILE_ARN:'arn:aws:iam::111122223333:instance-profile/owned-profile',INGEST_PUBLISH_ROLE_NAME:'owned-role'};
 expect(publicationRuntimeConfig(good).candidateLimit).toBe(1);expect(publicationRuntimeConfig(good).budgetMs).toBe(1200000);
 expect(()=>publicationRuntimeConfig({...good,INGEST_PUBLISH_EXPECT_GENERATION:'bad'})).toThrow('publication_target_invalid');
 expect(()=>publicationRuntimeConfig({...good,INGEST_PUBLISH_BUDGET_MS:'3600001'})).toThrow('publication_budget_invalid');
});
test('runtime may read legacy bases but only PUT generation-qualified keys; arbitrary erase/list/presign refused',async()=>{
 let calls=0;const raw=new Proxy({backend:'s3'},{get(t,k){if(k==='backend')return t.backend;return async()=>{calls++;return{}}}})as unknown as ObjectStore;
 const store=scopedPublicationStore(raw),base='96bulls/org/v1/ws=w/m=srv_x/h=claude/d=2026/11/01/s=s/manifest.json',gen='96bulls/org/upload-generations/g=12345678-1234-1234-1234-123456789abc/v1/ws=w/m=srv_x/h=claude/d=2026/11/01/s=s/publications/abcdefab-1234-1234-1234-123456789abc/manifest.json';
 await store.get(base as never);await store.put(gen as never,new Uint8Array([1]));expect(calls).toBe(2);
 for(const run of[()=>store.put(base as never,new Uint8Array([1])),()=>store.delete(gen as never),()=>store.list('96bulls' as never),()=>store.presignGet(gen as never,10)])await expect(run()).rejects.toThrow('publication_');expect(calls).toBe(2);
});
