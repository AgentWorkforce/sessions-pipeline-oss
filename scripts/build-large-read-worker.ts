/** Build the default-off large canonical Node worker bundle (docker/large-read-worker.Dockerfile).
 * Uses the Lambda's derived-schema plugin so the shipped bundle embeds its schema/verifier assets
 * and never reads supabase/ at load time. Usage: bun scripts/build-large-read-worker.ts <out.mjs> */
import {build,NODE_CONDITION_NAMES} from "alchemy/Bundle/Bundle";
import * as Effect from "effect/Effect";
import {readFile} from "node:fs/promises";
import {resolve,dirname} from "node:path";
import {fileURLToPath} from "node:url";
import {derivedSchemaBuildPlugin} from "../stacks/aws/derived-schema-build.ts";
const root=resolve(dirname(fileURLToPath(import.meta.url)),".."),out=process.argv[2];
if(!out||!out.endsWith(".mjs"))throw Error("usage: build-large-read-worker.ts <out.mjs>");
await Effect.runPromise(build({input:resolve(root,"src/session-pipeline/large-read-main.ts"),cwd:root,platform:"node",tsconfig:resolve(root,"tsconfig.json"),resolve:{conditionNames:[...NODE_CONDITION_NAMES]},plugins:[derivedSchemaBuildPlugin()]},
 {format:"esm",file:resolve(out),codeSplitting:false,sourcemap:false}));
const bundle=await readFile(resolve(out),"utf8");
// The image has no supabase/ tree: a load-time path into it would exit ENOENT before the flag check.
// Embedded verifier SQL may mention supabase/bootstrap in comments; a relative module path may not.
if(/\.\.\/supabase\//.test(bundle)||bundle.includes("//#region src/shared/db.ts"))throw Error("large worker bundle reads supabase/ at runtime");
console.log(JSON.stringify({built:true,bytes:Buffer.byteLength(bundle)}));
