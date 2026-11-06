import { expect, test } from "bun:test";
import { createS3Eraser } from "../neon-s3-erasure/eraser.ts";
const key = "96bulls/test/buffered-batches/v1/b=unit.staged.ndjson.zst";
function fixture(rows: { id: string; latest?: boolean; marker?: boolean; key?: string }[], truncated = false) {
  const deleted: string[] = []; let reads = 0;
  const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    if (init?.method === "DELETE") { deleted.push(url.searchParams.get("versionId")!); return new Response(null, { status: 204 }); }
    reads++;
    const entries = rows.filter(r => !deleted.includes(r.id)).map(r => `<${r.marker ? "DeleteMarker" : "Version"}><Key>${r.key ?? key}</Key><VersionId>${r.id}</VersionId><IsLatest>${r.latest ?? false}</IsLatest></${r.marker ? "DeleteMarker" : "Version"}>`).join("");
    return new Response(`<ListVersionsResult>${entries}<IsTruncated>${truncated}</IsTruncated></ListVersionsResult>`);
  }) as typeof fetch;
  const eraser = createS3Eraser({ bucket: "unit-bucket", region: "us-east-1", credentials: { accessKeyId: "unit-access", secretAccessKey: "unit-secret" }, fetch: fetcher });
  return { eraser, deleted, reads: () => reads };
}
test("permanently removes prior payload/marker versions while retaining the current redaction barrier", async () => {
  const f = fixture([{ id: "payload" }, { id: "marker", marker: true }, { id: "barrier", latest: true }, { id: "foreign", latest: true, key: key + "-sibling" }]);
  expect(await f.eraser.eraseObjectExcept(key, "barrier")).toEqual({ deleted: 2, kept: 1 });
  expect(f.deleted.sort()).toEqual(["marker", "payload"]);
});
for (const [name, rows] of [
  ["newer version", [{ id: "barrier" }, { id: "newer", latest: true }]],
  ["delete marker", [{ id: "barrier", latest: true, marker: true }]],
  ["missing barrier", [{ id: "payload", latest: true }]],
  ["duplicate latest", [{ id: "barrier", latest: true }, { id: "payload", latest: true }]],
] as const) test(`refuses ${name} before any permanent deletion`, async () => {
  const f = fixture([...rows]);
  await expect(f.eraser.eraseObjectExcept(key, "barrier")).rejects.toMatchObject({ code: "erasure_barrier_not_current" });
  expect(f.deleted).toEqual([]);
});
test("requires a concrete preserved version before reading or deleting", async () => {
  const f = fixture([]); await expect(f.eraser.eraseObjectExcept(key, "")).rejects.toThrow();
  expect(f.reads()).toBe(0); expect(f.deleted).toEqual([]);
});
test("refuses truncated inventory without a next version cursor before deleting", async () => {
  const f = fixture([{ id: "barrier", latest: true }, { id: "payload" }], true);
  await expect(f.eraser.eraseObjectExcept(key, "barrier")).rejects.toMatchObject({ code: "erasure_inventory" });
  expect(f.deleted).toEqual([]);
});

for(const [name,xml] of [
 ["missing version ID", "<ListVersionsResult><Version><Key>"+key+"</Key><IsLatest>false</IsLatest></Version><Version><Key>"+key+"</Key><VersionId>barrier</VersionId><IsLatest>true</IsLatest></Version><IsTruncated>false</IsTruncated></ListVersionsResult>"],
 ["invalid latest flag", "<ListVersionsResult><Version><Key>"+key+"</Key><VersionId>old</VersionId><IsLatest>unknown</IsLatest></Version><Version><Key>"+key+"</Key><VersionId>barrier</VersionId><IsLatest>true</IsLatest></Version><IsTruncated>false</IsTruncated></ListVersionsResult>"],
 ["truncated XML", "<ListVersionsResult><Version><Key>"+key+"</Key><VersionId>barrier</VersionId><IsLatest>true</IsLatest></Version><IsTruncated>false</IsTruncated><Version>"],
] as const) test("refuses unproven inventory: "+name+" before deleting",async()=>{
 let deletes=0;
 const eraser=createS3Eraser({bucket:"unit-bucket",region:"us-east-1",credentials:{accessKeyId:"unit-access",secretAccessKey:"unit-secret"},fetch:(async(_u,init)=>{if(init?.method==="DELETE")deletes++;return new Response(xml)}) as typeof fetch});
 await expect(eraser.eraseObjectExcept(key,"barrier")).rejects.toMatchObject({code:"erasure_inventory"});
 expect(deletes).toBe(0);
});
