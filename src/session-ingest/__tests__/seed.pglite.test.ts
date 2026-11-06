/** Source-blind APIs are retired; actual SQL/FS seeding is in seed-proof.pglite.test.ts. */
import { expect,test } from "bun:test";
import { seedOrg,exportedFromState } from "../seed.ts";
test("session/exporter booleans never authorize a record seed",async()=>{
 const sql={query:async()=>{throw new Error("must not reach SQL")}};
 for(const allowed of [true,false]){
  await expect(seedOrg(null,sql,"org",()=>allowed)).rejects.toMatchObject({code:"seed_proof_required"});
 }
 expect(()=>exportedFromState([{session_id:"s",group_revision:5,exported_revision:5,error:null,failed_revision:null}])).toThrow();
});
