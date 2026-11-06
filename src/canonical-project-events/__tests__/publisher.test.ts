import { expect, it } from "bun:test";
import { publishCanonicalProjectEvents } from "../publisher.ts";
import { cloudProjectEventMessage, type ProjectEventClaim } from "../protocol.ts";
import { canonicalProjectEventStore } from "../store.ts";
const scope = { orgId: "10000000-0000-4000-8000-000000000001", workspaceId: "20000000-0000-4000-8000-000000000002" };
const schemaGeneration = "a".repeat(64);
const claim = (generation = "9007199254740993"): ProjectEventClaim => ({
  event: { eventId: "11111111-1111-1111-1111-111111111111", kind: "session.revised", generation,
    schemaGeneration, identity: { ...scope, source: "claude", sessionId: "session:one", userId: null },
    authorityId: "22222222-2222-2222-2222-222222222222", sourceEpoch: "3", sourceFingerprint: "b".repeat(64), publicationKind: "publish" },
  leaseToken: "33333333-3333-3333-3333-333333333333", leaseVersion: "1",
});
it("emits exactly Cloud's ID-only ABI and preserves bigint generation", async () => {
  const sent: unknown[] = [], completed: ProjectEventClaim[] = [];
  const result = await publishCanonicalProjectEvents({ schemaGeneration, scope,
    store: { claim: async () => [claim()], sent: async c => { completed.push(c); return true; }, release: async () => { throw new Error("unexpected release"); } },
    queue: { send: async body => { sent.push(body); } } });
  expect(sent).toEqual([{ kind: "session.revised", eventId: claim().event.eventId, generation: "9007199254740993" }]);
  expect(completed).toHaveLength(1); expect(result).toEqual({ claimed: 1, sent: 1, superseded: 0 });
  expect(Object.keys(cloudProjectEventMessage(claim().event)).sort()).toEqual(["eventId", "generation", "kind"]);
});
it("refuses malformed, foreign, wrong-generation and deferred-brief batches before sending anything", async () => {
  for (const mutate of [
    (c: ProjectEventClaim) => { c.event.identity.orgId = "99999999-9999-9999-9999-999999999999"; },
    (c: ProjectEventClaim) => { c.event.identity.workspaceId = "99999999-9999-9999-9999-999999999999"; },
    (c: ProjectEventClaim) => { c.event.schemaGeneration = "c".repeat(64); },
    (c: ProjectEventClaim) => { c.event.generation = "9223372036854775808"; },
    (c: ProjectEventClaim) => { c.event.kind = "brief.completed" as never; },
    (c: ProjectEventClaim) => { (c.event as unknown as Record<string, unknown>).body = "must not cross transport"; },
  ]) {
    let sends = 0; const bad = claim(); mutate(bad);
    await expect(publishCanonicalProjectEvents({ schemaGeneration, scope,
      store: { claim: async () => [claim(), bad], sent: async () => true, release: async () => true },
      queue: { send: async () => { sends++; } } })).rejects.toMatchObject({ code: "canonical_project_event_invalid" });
    expect(sends).toBe(0);
  }
});
it("releases only failed/unsent claims after send failure and never claims delivery as admission", async () => {
  const items = [claim("1"), claim("2"), claim("3")], released: string[] = [], marked: string[] = [];
  let calls = 0;
  await expect(publishCanonicalProjectEvents({ schemaGeneration, scope,
    store: { claim: async () => items, sent: async c => { marked.push(c.event.generation); return true; }, release: async c => { released.push(c.event.generation); return true; } },
    queue: { send: async () => { if (++calls === 2) throw new Error("fixture transport outage"); } } }))
    .rejects.toMatchObject({ code: "canonical_project_send_failed" });
  expect(marked).toEqual(["1"]); expect(released).toEqual(["2", "3"]);
});
it("lost delivery confirmation is safely retryable; stale sent CAS does not overwrite a new generation", async () => {
  let releases = 0;
  const store = { claim: async () => [claim()], sent: async () => false, release: async () => { releases++; return true; } };
  expect(await publishCanonicalProjectEvents({ schemaGeneration, scope, store, queue: { send: async () => {} } }))
    .toEqual({ claimed: 1, sent: 0, superseded: 1 });
  expect(releases).toBe(0);
  await expect(publishCanonicalProjectEvents({ schemaGeneration, scope,
    store: { ...store, sent: async () => { throw new Error("lost DB reply"); } }, queue: { send: async () => {} } }))
    .rejects.toMatchObject({ code: "canonical_project_send_failed" });
  expect(releases).toBe(1);
});
it("RPC store pins its exact scope/schema and carries nonce/version/generation on all mutations", async () => {
  const requests: { sql: string; input: Record<string, unknown> }[] = [];
  const sql = { query: async (text: string, params: unknown[]) => {
    requests.push({ sql: text, input: JSON.parse(String(params[0])) });
    return { rows: [{ result: text.includes("_claim_") ? [claim()] : { ok: true } }] };
  } } as never;
  const store = canonicalProjectEventStore(sql, schemaGeneration, scope);
  const list = await store.claim({ schemaGeneration, scope, limit: 5 }); await store.sent(list[0]!); await store.release(list[0]!);
  expect(requests).toHaveLength(3);
  for (const request of requests) { expect(request.input.scope).toEqual(scope); expect(request.input.schemaGeneration).toBe(schemaGeneration); }
  expect(requests[1]!.input).toMatchObject({ generation: "9007199254740993", leaseToken: claim().leaseToken, leaseVersion: "1" });
  await expect(store.claim({ schemaGeneration, scope: { ...scope, workspaceId: "99999999-9999-9999-9999-999999999999" }, limit: 5 }))
    .rejects.toThrow("scope_invalid");
  expect(requests).toHaveLength(3);
});
