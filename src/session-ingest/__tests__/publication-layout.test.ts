
import {describe,test,expect} from "bun:test";
import {readFileSync} from "node:fs";
import {join} from "node:path";
import {parseManifest,chunkKeys,manifestKey,parseSessionKey,publicationManifestKey,uploadGenerationPrefix,parseUploadGenerationPrefix,isManifestRest,isPublicationManifestRest} from "../../../packages/object-store/src/index.ts";
import {prefixAllowed,prefixOrgResolver,buildObjectLandedMessage} from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
const generation="12345678-1234-1234-1234-123456789abc",publication="abcdefab-1234-1234-1234-123456789abc";
const fixture=JSON.parse(readFileSync(join(import.meta.dir,"../../../packages/object-store/test/fixtures/rust-manifest-v1-scrubbed.json"),"utf8")).manifest;
const loc={prefix:"96bulls/org-a",workspaceId:fixture.session.workspace_id,machineId:fixture.session.machine_id,harness:fixture.session.harness,startDate:fixture.session.started_at.slice(0,10),sessionId:fixture.session.session_id};
describe("immutable upload publication layout",()=>{
 test("legacy and qualified image manifest share canonical format and chunk layout; scrub epoch remains unchanged",async()=>{
  const legacy=await parseManifest(fixture,manifestKey(loc));
  const at={...loc,prefix:uploadGenerationPrefix(loc.prefix,generation)};
  const key=publicationManifestKey(at,publication);
  const image=await parseManifest(fixture,key);
  expect(image.raw.epoch).toBe(legacy.raw.epoch);
  expect(parseSessionKey(key)?.rest).toBe("publications/"+publication+"/manifest.json");
  const raw=chunkKeys(image,{prefix:at.prefix});
  expect(raw.length).toBeGreaterThan(0);
  expect(raw.every(x=>String(x.key).startsWith(at.prefix+"/v1/")&&!String(x.key).includes("/publications/"))).toBe(true);
  expect(raw.map(x=>x.entry)).toEqual(chunkKeys(legacy,{prefix:loc.prefix}).map(x=>x.entry));
  expect(buildObjectLandedMessage({landed:{key,etag:"synthetic-etag"},orgId:"org-a",manifest:image}).status).toBe("enqueue");
 });
 test("generation prefix stays bound to the actual org, not the last UUID segment",async()=>{
  const prefix=uploadGenerationPrefix("96bulls/org-a",generation);
  expect(await prefixOrgResolver.orgFor({prefix} as never)).toBe("org-a");
  expect(prefixAllowed({PIPELINE_ORG_FROM_PREFIX:"true",PIPELINE_KEY_PREFIX:"96bulls"},prefix,"org-a")).toBe(true);
  expect(prefixAllowed({PIPELINE_ORG_FROM_PREFIX:"true",PIPELINE_KEY_PREFIX:"96bulls"},prefix,"org-b")).toBe(false);
  expect(prefixAllowed({PIPELINE_ORG_FROM_PREFIX:"true",PIPELINE_KEY_PREFIX:"other"},prefix,"org-a")).toBe(false);
 });
 test("malformed/nested generations and unqualified immutable manifests refuse",async()=>{
  expect(parseUploadGenerationPrefix("96bulls/org-a/upload-generations/g=oops")).toBeUndefined();
  expect(parseUploadGenerationPrefix(uploadGenerationPrefix("96bulls/org-a",generation)+"/upload-generations/g="+publication)).toBeUndefined();
  expect(()=>uploadGenerationPrefix("96bulls/../org-a",generation)).toThrow();
  expect(isManifestRest("manifest.json")).toBe(true);
  expect(isPublicationManifestRest("publications/"+publication+"/manifest.json")).toBe(true);
  expect(isManifestRest("publications/../manifest.json")).toBe(false);
  const unqualified=String(manifestKey(loc)).replace("manifest.json","publications/"+publication+"/manifest.json");
  await expect(parseManifest(fixture,unqualified)).rejects.toThrow();
  expect(buildObjectLandedMessage({landed:{key:unqualified},orgId:"org-a",manifest:fixture}).status).toBe("ignored");
 });
});
