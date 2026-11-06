import { afterEach, expect, test } from "bun:test";
import { readFile, writeFile } from "node:fs/promises";
import { refusalBackoffMs } from "../courier.ts";
import { courierWorld } from "./courier-harness.ts";

const worlds: Awaited<ReturnType<typeof courierWorld>>[] = [];
afterEach(async () => {
  await Promise.all(worlds.splice(0).map((w) => w.close()));
});

test("backoff after a permanent refusal: 1 h, doubling, at most 24 h", () => {
  expect(refusalBackoffMs(1)).toBe(3_600_000);
  expect(refusalBackoffMs(2)).toBe(7_200_000);
  expect(refusalBackoffMs(9)).toBe(24 * 3_600_000);
});

test("a chunk the ingest refuses permanently (400) is not re-sent every pass; it retries after its backoff and clears on success", async () => {
  const w = await courierWorld({ format: "ndjson" });
  worlds.push(w);
  await w.write("hello");
  w.hooks.chunkResponse = () => Response.json({ error: { code: "bad_request" } }, { status: 400 });
  await w.run();
  expect(w.counts.chunkPosts).toBe(1);
  const state = () => readFile(w.statePath, "utf8").then((t) => JSON.parse(t) as { sessions: Record<string, { refused?: { code: string; attempts: number; at: string } }> });
  const [entry] = Object.values((await state()).sessions);
  expect(entry!.refused).toMatchObject({ code: "bad_request", attempts: 1 });
  // Next passes, within the backoff: no request reaches the ingest.
  await w.run();
  await w.run();
  expect(w.counts.chunkPosts).toBe(1);
  // A 5xx is transient: never recorded as a refusal (checked after the backoff passes).
  const s = await state();
  for (const v of Object.values(s.sessions)) v.refused!.at = new Date(Date.now() - 2 * 3_600_000).toISOString();
  await writeFile(w.statePath, JSON.stringify(s));
  w.hooks.chunkResponse = undefined;
  await w.run();
  expect(w.counts.chunkPosts).toBe(2);
  const [after] = Object.values((await state()).sessions);
  expect(after!.refused).toBeUndefined();
});
