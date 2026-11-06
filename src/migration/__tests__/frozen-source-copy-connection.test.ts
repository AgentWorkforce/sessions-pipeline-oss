import {test,expect} from "bun:test";
import {spawnSync} from "node:child_process";
import {fileURLToPath} from "node:url";
const script=fileURLToPath(new URL("../../../scripts/frozen-source-copy.ts",import.meta.url));
const preload=fileURLToPath(new URL("./fixtures/frozen-copy-preload.ts",import.meta.url));
function run(fail?:string, undefinedFailure=false, connectFailure?:string) {
 const result=spawnSync(process.execPath,["--preload",preload,script,"--copy","--concurrency","2"],{
  input:JSON.stringify({neonOwnerUrl:"postgresql://neondb_owner:synthetic-not-authorized@ep-example-branch-00000000.synthetic.invalid/neondb"})+"\n",
  encoding:"utf8",timeout:10000,env:{PATH:process.env.PATH,SUPABASE_CLI:"/synthetic-supabase-refuse-network",...(fail?{FROZEN_COPY_TEST_FAIL:fail}:{}),...(undefinedFailure?{FROZEN_COPY_TEST_UNDEFINED:"1"}:{}),...(connectFailure?{FROZEN_COPY_TEST_CONNECT:connectFailure}:{})},
 });
 const proof=result.stdout.split("\n").filter(x=>x.startsWith("{")).map(x=>JSON.parse(x)).find(x=>x.syntheticCopyProof)?.syntheticCopyProof;
 return {result,proof};
}
test("two source readers reuse one authenticated owner writer without rotating CLI passwords",()=>{
 const {result,proof}=run();
 expect(result.status).toBe(0);
 expect(proof).toEqual({logins:1,targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,records:["org-a","org-b"],allNeoClosed:true});
 expect(result.stdout).not.toContain("synthetic-cli-password");
 expect(result.stderr).toBe("");
});
test("copy error drains both source readers before closing the one owner writer",()=>{
 const {result,proof}=run("org-a");
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("synthetic_target_failure");
 expect(proof.logins).toBe(1);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
 expect(result.stdout).not.toContain("synthetic-cli-password");
});

test("an undefined worker rejection still fails closed and drains the shared writer",()=>{
 const {result,proof}=run("org-a",true);
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("copy_worker_failed");
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});

for (const side of ["source","target"]) test(`a failed ${side} connection closes every created client`,()=>{
 const {result,proof}=run(undefined,false,side);
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("synthetic_connect_failure");
 expect(proof.logins).toBe(1);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});
