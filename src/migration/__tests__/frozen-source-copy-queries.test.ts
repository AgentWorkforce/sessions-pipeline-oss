import { beforeAll, afterAll, test, expect } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { SUPA_PAGE, SUPA_RECEIPTS, SUPA_CONTROL_HEADS } from "../frozen-source-copy-queries.ts";

const db = new PGlite();
const keys = ["a", "A", "a-b", "a_b", "á", "z", "Z", "!x", "_a"];
beforeAll(async () => {
  await db.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.ingest_frozen_source_records(
      org_id text COLLATE "und-x-icu",workspace_id text COLLATE "und-x-icu",
      origin_id text COLLATE "und-x-icu",record_id text COLLATE "und-x-icu",
      revision_id text,revision bigint,digest text,kind text,source text,
      session_id text,operation text,user_id text,
      projection_accepted_seq bigint,catalog_accepted_seq bigint,
      PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
    CREATE TABLE sessions.ingest_frozen_source_receipts(
      org_id text COLLATE "und-x-icu",workspace_id text COLLATE "und-x-icu",
      origin_id text COLLATE "und-x-icu",batch_id text COLLATE "und-x-icu",
      digest text,receipt jsonb,PRIMARY KEY(org_id,workspace_id,origin_id,batch_id));
    CREATE TABLE sessions.ingest_source_control_heads(
      org_id text,workspace_id text,user_id text,version bigint,digest text,pending boolean); `);
  for (const key of keys) {
    await db.query("INSERT INTO sessions.ingest_frozen_source_records VALUES('org','ws','origin',$1,'rev',1,'digest','kind','source','session','upsert','user',1,2)", [key]);
    await db.query("INSERT INTO sessions.ingest_frozen_source_receipts VALUES('org','ws','origin',$1,'digest',$2::jsonb)", [key, JSON.stringify({ receipt: key })]);
  }
  await db.exec("INSERT INTO sessions.ingest_frozen_source_records(org_id,workspace_id,origin_id,record_id) VALUES('foreign','ws','origin','foreign'); INSERT INTO sessions.ingest_frozen_source_receipts VALUES('foreign','ws','origin','foreign','digest','{}')");
  await db.exec("INSERT INTO sessions.ingest_source_control_heads VALUES('org','ws','z',2,'digest-z',false),('org','ws','a',1,'digest-a',true)");
}, 15000);
afterAll(async () => { await db.close(); });

for (const [name, query, id] of [
  ["record", SUPA_PAGE, "record_id"],
  ["receipt", SUPA_RECEIPTS, "batch_id"],
] as const) {
  test.each([1, 2, 3])(`frozen ${name} keyset paging loses no mixed-case/punctuation/Unicode keys at page size %i`, async (size) => {
    let last = ["", "", ""];
    const seen: string[] = [];
    for (let page = 0; page <= keys.length; page++) {
      const { rows } = await db.query<Record<string, string>>(query, ["org", ...last, size]);
      if (!rows.length) break;
      for (const row of rows) seen.push(row[id]!);
      const tail = rows.at(-1)!;
      last = [tail.workspace_id!, tail.origin_id!, tail[id]!];
      if (rows.length < size) break;
    }
    expect(seen.length).toBe(keys.length);
    expect([...new Set(seen)].sort()).toEqual([...keys].sort());
  });
}
test("frozen sealer reads actual control-head SQL without collating positional integer constants", async () => {
  const { rows } = await db.query<{user_id: string;v: string;pending: boolean}>(SUPA_CONTROL_HEADS);
  expect(rows.map(r => [r.user_id, r.v, r.pending])).toEqual([["a", "1", true], ["z", "2", false]]);
});


for (const args of [["--seal", "--org", "subset"], ["--seal", "--org", ""], ["--seal", "--org"]]) {
  test("partial completeness seal refuses before reading credentials: " + JSON.stringify(args), () => {
    const script = fileURLToPath(new URL("../../../scripts/frozen-source-copy.ts", import.meta.url));
    const result = spawnSync(process.execPath, [script, ...args], {
      input: "not-json-not-a-credential", encoding: "utf8", env: { PATH: process.env.PATH },
    });
    expect(result.status).toBe(64);
    expect(result.stderr).toContain("--seal requires the complete source");
    expect(result.stdout).toBe("");
  });
}
