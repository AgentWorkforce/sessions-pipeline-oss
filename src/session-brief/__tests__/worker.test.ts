/**
 * The session-brief stage end to end over PGlite (every migration), an fs Locker, a fake
 * manifest reader and a fake model: wakeup debounce, admission limits, input building,
 * model call, scrub, the brief row, the directory projection and the read RPC.
 */
import { afterEach, describe, expect, setDefaultTimeout, spyOn, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import type { ManifestV1, ObjectStore } from "../../shared/vendor.ts";
import { hoopDbSessionIndex } from "../../hoopdb/index.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { writeShortSummary } from "../../hoopdb/session-directory-summary.ts";
import { briefConfig } from "../config.ts";
import { SessionAnalysisProviderError } from "../engine/transport.ts";
import { SUMMARY_INSTRUCTIONS } from "../engine/summary-prompt.ts";
import { armWakeup, scrubBrief, sweepUnbriefed } from "../store.ts";
import { briefTick } from "../worker.ts";
import {
  FakeModel,
  KEY,
  briefRpc,
  database,
  dueNow,
  hoopDb,
  mapReader,
  pointer,
  recordStream,
  tempStore,
  testConfig,
  transcript,
} from "./harness.ts";


// Each test builds a PGlite database with the full migration plan (a few seconds).
setDefaultTimeout(60_000);

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

const PLANTED = "PLANTED-TRANSCRIPT-MARKER-7f3a";
const user = (text: string, extra: Record<string, unknown> = {}) => ({
  type: "user",
  timestamp: "2026-10-29T10:00:01.000Z",
  message: { role: "user", content: text },
  ...extra,
});
const assistant = (text: string, ts = "2026-10-29T10:00:02.000Z") => ({
  type: "assistant",
  timestamp: ts,
  message: { role: "assistant", model: "claude-x", content: [{ type: "text", text }, { type: "tool_use", name: "Bash", input: {} }] },
});
const toolResult = {
  type: "user",
  timestamp: "2026-10-29T10:00:03.000Z",
  message: { role: "user", content: [{ type: "tool_result", content: "TOOL-OUTPUT-SHOULD-NOT-REACH-MODEL" }] },
};

interface Setup {
  pg: PGlite;
  store: ObjectStore;
  manifests: Map<string, ManifestV1>;
  model: FakeModel;
  tick: (overrides?: Partial<ReturnType<typeof testConfig>>) => Promise<Record<string, number> | null>;
  upload: (lines: unknown[], opts?: { revision?: number; machine?: string; scrubbed?: boolean }) => Promise<void>;
}

async function setup(opts: { transport?: boolean } = {}): Promise<Setup> {
  const pg = await database();
  open.push(pg);
  const store = tempStore();
  const manifests = new Map<string, ManifestV1>();
  const model = new FakeModel();
  const upload: Setup["upload"] = async (lines, o = {}) => {
    const machine = o.machine ?? "m-1";
    const revision = o.revision ?? 1;
    const manifestKey = `manifest/${KEY.sessionId}/${machine}/r${revision}`;
    manifests.set(manifestKey, await transcript(store, lines, { machine, revision, scrubbed: o.scrubbed }));
    await pointer(pg, { machine, revision, manifestKey });
    await armWakeup(hoopDb(pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 });
  };
  const tick: Setup["tick"] = async (overrides = {}) =>
    briefTick({
      reader: mapReader(manifests),
      store,
      transport: opts.transport === false ? null : model,
      config: testConfig(overrides),
    })(hoopDb(pg), new Date());
  return { pg, store, manifests, model, tick, upload };
}

const brief = async (pg: PGlite) =>
  (await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_briefs")).rows[0];
const directory = async (pg: PGlite) =>
  (
    await pg.query<Record<string, unknown>>(
      "SELECT title, short_summary, short_summary_source, search_text FROM sessions.session_directory",
    )
  ).rows[0]!;
const wakeups = async (pg: PGlite) =>
  (await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_brief_wakeups")).rows;

describe("debounce (relayhistory 0048: ten minutes idle, sixty minutes at most)", () => {
  test("a wakeup is due ten minutes after the newest evidence and is not claimed before", async () => {
    const s = await setup();
    await s.upload([user("fix the build"), assistant("done")]);
    const [w] = await wakeups(s.pg);
    const delay = (
      await s.pg.query<{ s: number }>("SELECT extract(epoch FROM due_at - evidence_at)::int AS s FROM sessions.session_brief_wakeups")
    ).rows[0]!.s;
    expect(delay).toBe(600);
    expect(w!.admission).toBe("background");
    expect(await s.tick()).toEqual({});
    expect(s.model.requests).toHaveLength(0);
  });

  test("new evidence pushes it back, but never past sixty minutes from the first", async () => {
    const s = await setup();
    await s.upload([user("one")]);
    await s.pg.exec("UPDATE sessions.session_brief_wakeups SET pending_since = now() - interval '55 minutes'");
    await armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 });
    const { rows } = await s.pg.query<{ left: number; generation: string }>(
      "SELECT extract(epoch FROM due_at - now())::int AS left, generation::text AS generation FROM sessions.session_brief_wakeups",
    );
    // Capped at pending_since + 60 min, i.e. about five minutes from now, not ten.
    expect(rows[0]!.left).toBeGreaterThan(290);
    expect(rows[0]!.left).toBeLessThanOrEqual(300);
    expect(rows[0]!.generation).toBe("2");
  });

  test("the HoopDb indexer arms the wakeup on session-indexed only when briefs are on", async () => {
    for (const brief of [null, { idleSeconds: 600, maxWaitSeconds: 3600 }]) {
      const pg = await database();
      open.push(pg);
      const store = tempStore();
      const manifest = await transcript(store, [user("hello")], { key: { ...KEY, harness: "codex" } });
      const reader = { bucket: "b", read: async () => ({ manifest, etag: "e1" }) } as never;
      const queue = { backend: "memory", send: async () => {}, sendBatch: async () => {} } as never;
      await hoopDbSessionIndex({ db: hoopDb(pg), reader, queue, current: () => undefined, brief }).upsertRawPointer(
        { traceId: manifest.trace_id, workspaceId: KEY.workspaceId, manifestKey: "v1/k/manifest.json", scrubbed: true } as never,
        { traceId: manifest.trace_id, orgId: KEY.orgId, workspaceId: KEY.workspaceId } as never,
      );
      expect((await wakeups(pg)).length, String(brief)).toBe(brief ? 1 : 0);
    }
  });

  test("the env default is off", () => {
    expect(briefConfig({}).enabled).toBe(false);
    expect(briefConfig({ SESSION_BRIEF_ENABLED: "1" }).enabled).toBe(false);
    expect(briefConfig({ SESSION_BRIEF_ENABLED: "true" }).enabled).toBe(true);
  });
});

describe("a due wakeup is briefed by the model", () => {
  test("ready: the brief row, the directory title and short summary, and the RPC", async () => {
    const s = await setup();
    await s.upload([
      user(`please fix the flaky queue test ${PLANTED}`),
      assistant("I'll look at the lease renewal."),
      toolResult,
      user("subagent prompt", { isSidechain: true }),
      assistant("Fixed: the ack now waits for the renewal.", "2026-10-29T10:00:04.000Z"),
    ]);
    await dueNow(s.pg);
    // src/shared/log.ts writes JSON lines straight to stdout/stderr.
    const logs: string[] = [];
    const capture = (chunk: unknown) => {
      logs.push(String(chunk));
      return true;
    };
    const out = spyOn(process.stdout, "write").mockImplementation(capture as never);
    const err = spyOn(process.stderr, "write").mockImplementation(capture as never);
    let counts;
    try {
      counts = await s.tick();
    } finally {
      out.mockRestore();
      err.mockRestore();
    }
    expect(counts).toEqual({ generated: 1 });

    // Exactly relayhistory's summary request.
    expect(s.model.requests).toHaveLength(1);
    const req = s.model.requests[0]!;
    expect(req).toMatchObject({
      model: "gpt-5.6-luna",
      instructions: SUMMARY_INSTRUCTIONS,
      schemaName: "session_summary",
      timeoutMs: 20_000,
      store: false,
      maxOutputTokens: 256,
      reasoningEffort: "none",
    });
    const input = JSON.parse(req.input);
    expect(input.source).toBe("claude");
    expect(input.sessionId).toBe("s-1");
    expect(input.messages.map((m: { role: string }) => m.role)).toEqual(["user", "assistant", "assistant"]);
    expect(req.input).toContain(PLANTED);
    expect(req.input).not.toContain("TOOL-OUTPUT-SHOULD-NOT-REACH-MODEL");
    expect(req.input).not.toContain("subagent prompt");

    const row = (await brief(s.pg))!;
    expect(row).toMatchObject({
      status: "ready",
      title: s.model.reply.title,
      description: s.model.reply.description,
      state: "completed",
      model: "gpt-5.6-luna",
      prompt_version: "session-summary-v3",
      engine_version: "session-summary-v3:96bulls-v1",
      scrub_ruleset: "relayhistory-scrub",
      message_count: 3,
      shortened_messages: 0,
      error: null,
    });
    expect(Number(row.input_tokens)).toBe(1234);
    expect(row.brief_source_revisions).toEqual({ "m-1": 1 });
    expect(await wakeups(s.pg)).toEqual([]);

    const dir = await directory(s.pg);
    expect(dir.title).toBe(s.model.reply.title);
    expect(dir.short_summary).toBe(s.model.reply.description);
    expect(dir.short_summary_source).toBe("session-brief/gpt-5.6-luna");
    expect(String(dir.search_text)).toContain(s.model.reply.title);

    expect((await briefRpc(s.pg))[0]).toMatchObject({
      status: "ready",
      title: s.model.reply.title,
      description: s.model.reply.description,
      state: "completed",
    });

    // Ids, counts and codes only: no transcript text, model output or credential in logs.
    expect(logs.length).toBeGreaterThan(0);
    for (const line of logs) {
      expect(line).not.toContain(PLANTED);
      expect(line).not.toContain(s.model.reply.title);
      expect(line).not.toContain("lease renewal");
    }
  });

  test("canonical directory ids still sweep, title and summarize through the physical pointer", async () => {
    const s = await setup();
    await s.upload([user("brief the canonical session"), assistant("done")]);
    await s.pg.exec(
      `UPDATE sessions.session_index
          SET canonical_session_id = 'kimi:s-1', canonical_machine_id = 'machine:m-1';
       DELETE FROM sessions.session_directory;
       DELETE FROM sessions.session_brief_wakeups;`,
    );
    await projectSessionDirectory(hoopDb(s.pg), KEY);
    expect(await sweepUnbriefed(hoopDb(s.pg), 10)).toBe(1);
    expect((await wakeups(s.pg)).map((w) => w.session_id)).toEqual(["s-1"]);
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ generated: 1 });
    expect(
      (
        await s.pg.query<Record<string, unknown>>(
          "SELECT session_id, title, short_summary FROM sessions.session_directory",
        )
      ).rows,
    ).toEqual([
      {
        session_id: "kimi:s-1",
        title: s.model.reply.title,
        short_summary: s.model.reply.description,
      },
    ]);
  });

  test("a RelayHistory record stream (the Neon export shape) briefs from its projected events", async () => {
    const s = await setup();
    const rec = (id: string, ts: number, kind: string, payload: Record<string, unknown>) => ({
      kind,
      origin_id: "origin",
      record_id: id,
      revision: "1",
      operation: "upsert",
      received_at: "2026-10-30T12:00:00.000Z",
      payload: { ts_ms: ts, ...payload },
    });
    const manifestKey = "manifest/s-1/m-1/stream";
    s.manifests.set(
      manifestKey,
      await recordStream(s.store, [
        rec("r1", 1_000, "history", { prompt: "migrate the briefs to supabase" }),
        rec("r2", 2_000, "session_event", { role: "assistant", text: "Added the session_briefs table." }),
        rec("r3", 3_000, "tool_call", { name: "Bash", target: "bun test" }),
      ]),
    );
    await pointer(s.pg, { manifestKey });
    await armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
    expect(await s.tick()).toEqual({ generated: 1 });
    const input = JSON.parse(s.model.requests[0]!.input);
    expect(input.messages).toEqual([
      { role: "user", text: "migrate the briefs to supabase", truncated: false },
      { role: "assistant", text: "Added the session_briefs table.", truncated: false },
    ]);
  });

  test("an unchanged input on a new revision reuses the brief without a model call", async () => {
    const s = await setup();
    const lines = [user("fix the build"), assistant("done")];
    await s.upload(lines);
    await dueNow(s.pg);
    await s.tick();
    await s.pg.exec("UPDATE sessions.session_briefs SET attempted_at = now() - interval '10 minutes'");
    await s.upload(lines, { revision: 2 });
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ reused: 1 });
    expect(s.model.requests).toHaveLength(1);
    expect((await brief(s.pg))!.brief_source_revisions).toEqual({ "m-1": 2 });
    expect((await briefRpc(s.pg))[0]).toMatchObject({ status: "ready" });
  });

  test("a v2 brief (no state) is regenerated for an unchanged input instead of reused", async () => {
    const s = await setup();
    const lines = [user("fix the build"), assistant("done")];
    await s.upload(lines);
    await dueNow(s.pg);
    await s.tick();
    await s.pg.exec(`UPDATE sessions.session_briefs
                        SET state = NULL, prompt_version = 'session-summary-v2',
                            engine_version = 'session-summary-v2:96bulls-v1',
                            attempted_at = now() - interval '10 minutes'`);
    await s.upload(lines, { revision: 2 });
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ generated: 1 });
    expect(s.model.requests).toHaveLength(2);
    expect(await brief(s.pg)).toMatchObject({ state: "completed", prompt_version: "session-summary-v3" });
    expect((await briefRpc(s.pg))[0]).toMatchObject({ status: "ready", state: "completed" });
  });

  test("a brief's state must be one of the summary states", () => {
    const text = { title: "Fix the build", description: "Fixed it. Tests pass." };
    expect(scrubBrief({ ...text, state: "blocked" }).state).toBe("blocked");
    expect(() => scrubBrief({ ...text, state: "done" as never })).toThrow("state_invalid");
  });

  test("new content re-briefs, and the projector keeps the brief title over the pointer title", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    await s.tick();
    // A new revision is projected before the re-brief: the brief title must survive it.
    await s.pg.exec("UPDATE sessions.session_briefs SET attempted_at = now() - interval '10 minutes'");
    await s.upload([user("fix the build"), assistant("and add a test")], { revision: 2 });
    expect((await directory(s.pg)).title).toBe(s.model.reply.title);
    s.model.reply = { title: "Fix the build and add a test", description: "Fixed the build. Added a regression test for it.", state: "completed" };
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ generated: 1 });
    expect((await directory(s.pg)).title).toBe("Fix the build and add a test");
  });

  test("an extractive summary never replaces the model brief's short summary", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    await s.tick();
    const later = new Date(Date.now() + 60_000);
    const res = await writeShortSummary(hoopDb(s.pg), { ...KEY, summary: "claude: extractive", source: "bob-costas/extractive/v1", generatedAt: later });
    expect(res.written).toBe(false);
    expect((await directory(s.pg)).short_summary).toBe(s.model.reply.description);
  });
});

describe("what is never sent to the model", () => {
  test("an unscrubbed manifest quarantines the session: no read, no model call, no brief text", async () => {
    const s = await setup();
    await s.upload([user("secret stuff")], { scrubbed: false });
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ quarantined: 1 });
    expect(s.model.requests).toHaveLength(0);
    expect(await brief(s.pg)).toMatchObject({ status: "quarantined", title: null, error: "session_quarantined" });
    expect((await briefRpc(s.pg))[0]).toMatchObject({ status: "failed", error: "session_quarantined" });
    expect((await directory(s.pg)).title).toBe("pointer title");
  });

  test("a session with no user or assistant text is empty", async () => {
    const s = await setup();
    await s.upload([toolResult]);
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ empty: 1 });
    expect(s.model.requests).toHaveLength(0);
  });

  test("with no model configured nothing is claimed", async () => {
    const s = await setup({ transport: false });
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    expect(await s.tick()).toBeNull();
    const [w] = await wakeups(s.pg);
    expect(w).toMatchObject({ attempts: 0, lease_id: null });
  });

  test("briefs off: the tick does nothing", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    expect(await s.tick({ enabled: false })).toBeNull();
    expect(s.model.requests).toHaveLength(0);
  });
});

describe("limits and failures", () => {
  test("throttle: a second attempt within two minutes waits, without spending an attempt", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    await s.tick();
    await s.upload([user("fix the build"), assistant("more")], { revision: 2 });
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ throttled: 1 });
    expect(s.model.requests).toHaveLength(1);
    const [w] = await wakeups(s.pg);
    expect(w).toMatchObject({ attempts: 0, lease_id: null });
  });

  test("the org's daily pool of distinct sessions, then rate_limited until the next UTC day", async () => {
    const s = await setup();
    await s.pg.query(
      `INSERT INTO sessions.session_brief_attempts (org_id, workspace_id, harness, session_id, day, attempts)
       VALUES ('org-a', 'ws-a', 'claude', 'other', (now() AT TIME ZONE 'UTC')::date, 1)`,
    );
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    expect(await s.tick({ backgroundDailyBudget: 1 })).toEqual({ rate_limited: 1 });
    expect(s.model.requests).toHaveLength(0);
    expect(await brief(s.pg)).toMatchObject({ status: "rate_limited", error: "session_brief_rate_limited" });
    const { rows } = await s.pg.query<{ tomorrow: boolean }>(
      `SELECT due_at = (date_trunc('day', now() AT TIME ZONE 'UTC') + interval '1 day') AT TIME ZONE 'UTC' AS tomorrow
         FROM sessions.session_brief_wakeups`,
    );
    expect(rows[0]!.tomorrow).toBe(true);
    // Another org's pool is separate.
    expect((await briefRpc(s.pg))[0]).toMatchObject({ status: "rate_limited" });
  });

  test("the global daily cap counts every org's attempts and refuses until the next UTC day", async () => {
    const s = await setup();
    await s.pg.query(
      `INSERT INTO sessions.session_brief_attempts (org_id, workspace_id, harness, session_id, day, attempts)
       VALUES ('org-elsewhere', 'ws-z', 'codex', 'z-1', (now() AT TIME ZONE 'UTC')::date, 3)`,
    );
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    expect(await s.tick({ globalDailyCap: 3 })).toEqual({ rate_limited: 1 });
    expect(s.model.requests).toHaveLength(0);
    expect(await brief(s.pg)).toMatchObject({ status: "rate_limited", error: "session_brief_global_capped" });
    // Under the ceiling, the same session is briefed.
    await dueNow(s.pg);
    expect(await s.tick({ globalDailyCap: 4, throttleSeconds: 0 })).not.toHaveProperty("rate_limited");
    expect(s.model.requests).toHaveLength(1);
  });

  test("the env default is a 20,000-attempt global ceiling, and 0 refuses every call", () => {
    expect(briefConfig({}).globalDailyCap).toBe(20_000);
    expect(briefConfig({ SESSION_BRIEF_GLOBAL_DAILY_CAP: "0" }).globalDailyCap).toBe(0);
    expect(briefConfig({ SESSION_BRIEF_GLOBAL_DAILY_CAP: "abc" }).globalDailyCap).toBe(20_000);
  });

  test("the per-session daily cap", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await s.pg.query(
      `INSERT INTO sessions.session_brief_attempts (org_id, workspace_id, harness, session_id, day, attempts)
       VALUES ('org-a', 'ws-a', 'claude', 's-1', (now() AT TIME ZONE 'UTC')::date, 30)`,
    );
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ rate_limited: 1 });
    expect(await brief(s.pg)).toMatchObject({ error: "session_brief_capped" });
  });

  test("a retryable provider failure is retried once, then failed; the previous brief is kept", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    await s.tick();
    await s.pg.exec("UPDATE sessions.session_briefs SET attempted_at = now() - interval '10 minutes'");
    await s.upload([user("fix the build"), assistant("more")], { revision: 2 });
    s.model.next.push(
      new SessionAnalysisProviderError("model_rate_limited", true),
      new SessionAnalysisProviderError("model_unavailable", true),
    );
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ retrying: 1 });
    const [w] = await wakeups(s.pg);
    expect(w).toMatchObject({ attempts: 1, last_error: "model_rate_limited", lease_id: null });
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ failed: 1 });
    expect(await wakeups(s.pg)).toEqual([]);
    const row = (await brief(s.pg))!;
    expect(row).toMatchObject({ status: "failed", error: "model_unavailable", title: "Fix the flaky queue test" });
    expect((await briefRpc(s.pg))[0]).toMatchObject({ status: "failed", stale: true, title: "Fix the flaky queue test" });
  });

  test("invalid model output fails at once with a controlled code", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    s.model.next.push({ outputText: JSON.stringify({ title: "x".repeat(91), description: "d" }), inputTokens: 1, outputTokens: 1, providerRequestId: null });
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ failed: 1 });
    expect(await brief(s.pg)).toMatchObject({ status: "failed", error: "model_output_invalid", title: null });
  });

  test("a scrubbed-away credential in the model output is stored scrubbed", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    const token = ["ghp", "x".repeat(36)].join("_");
    s.model.reply = { title: "Rotate the token", description: `Rotated ${token} for CI.`, state: "completed" };
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ generated: 1 });
    const row = (await brief(s.pg))!;
    expect(String(row.description)).not.toContain(token);
    expect(String((await directory(s.pg)).short_summary)).not.toContain(token);
  });

  test("a wakeup re-armed during the run survives it, and the session is briefed again", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    const model = s.model;
    const original = model.complete.bind(model);
    model.complete = async (r) => {
      await armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 });
      return original(r);
    };
    expect(await s.tick()).toEqual({ generated: 1 });
    const [w] = await wakeups(s.pg);
    expect(Number(w!.generation)).toBe(2);
    expect(w).toMatchObject({ lease_id: null });
  });

  test("deleting the session's pointer clears the brief and the directory title falls back", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await s.upload([user("other machine")], { machine: "m-2" });
    await dueNow(s.pg);
    await s.tick();
    expect((await directory(s.pg)).title).toBe(s.model.reply.title);
    await s.pg.exec("UPDATE sessions.session_index SET deleted_at = now() WHERE machine_id = 'm-2'");
    await projectSessionDirectory(hoopDb(s.pg), KEY);
    expect(await brief(s.pg)).toBeUndefined();
    expect((await directory(s.pg)).title).toBe("pointer title");
  });
});

describe("review fixes (#127, Devin 2026-11-02)", () => {
  const modelHook = (s: Setup, during: () => Promise<void>) => {
    const real = s.model.complete.bind(s.model);
    s.model.complete = async (request) => {
      await during();
      return real(request);
    };
  };

  test("a machine erased during generation never returns in the brief (stale, nothing written)", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await s.upload([user("other machine content")], { machine: "m-2" });
    await dueNow(s.pg);
    modelHook(s, () => s.pg.exec("UPDATE sessions.session_index SET deleted_at = now() WHERE machine_id = 'm-2'").then(() => undefined));
    expect(await s.tick()).toEqual({ stale: 1 });
    expect(s.model.requests).toHaveLength(1);
    const b = await brief(s.pg);
    expect(b?.title ?? null).toBeNull();
    expect((await directory(s.pg)).short_summary).not.toBe(s.model.reply.description);
  });

  test("an older input with fewer machines at the same maximum revision does not overwrite", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    // m-2 joins at the same revision while the m-1-only input is with the model.
    modelHook(s, async () => {
      await pointer(s.pg, { machine: "m-2", revision: 1, manifestKey: `manifest/${KEY.sessionId}/m-2/r1` });
    });
    expect(await s.tick()).toEqual({ stale: 1 });
    expect((await brief(s.pg))?.title ?? null).toBeNull();
  });

  test("a quarantine removes the brief's own description from the directory and search", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ generated: 1 });
    expect((await directory(s.pg)).short_summary).toBe(s.model.reply.description);
    await s.upload([user("secret stuff")], { revision: 2, scrubbed: false });
    await dueNow(s.pg);
    expect(await s.tick({ throttleSeconds: 0 })).toEqual({ quarantined: 1 });
    const d = await directory(s.pg);
    expect(d.short_summary).toBeNull();
    expect(String(d.search_text)).not.toContain("lease renewal");
  });

  test("a run re-armed by new evidence starts a fresh debounce window, not the stale first one", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    // The session has been pending for two hours: the old window's cap is long past.
    await s.pg.exec("UPDATE sessions.session_brief_wakeups SET pending_since = now() - interval '2 hours'");
    modelHook(s, () => armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 }));
    expect(await s.tick()).toEqual({ generated: 1 });
    const { rows } = await s.pg.query<{ fresh: boolean; future: boolean }>(
      `SELECT pending_since > now() - interval '1 minute' AS fresh,
              due_at > now() + interval '500 seconds' AS future
         FROM sessions.session_brief_wakeups`,
    );
    expect(rows[0]).toEqual({ fresh: true, future: true });
  });

  test("a reused or empty outcome spends no admission: only a model call is counted", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    expect(await s.tick()).toEqual({ generated: 1 });
    // Same text on a new revision: reused without a model call.
    await s.upload([user("fix the build")], { revision: 2 });
    await dueNow(s.pg);
    expect(await s.tick({ throttleSeconds: 0 })).toEqual({ reused: 1 });
    const { rows } = await s.pg.query<{ n: number }>(
      "SELECT coalesce(sum(attempts), 0)::int AS n FROM sessions.session_brief_attempts",
    );
    expect(rows[0]!.n).toBe(1);
    expect(s.model.requests).toHaveLength(1);
  });

  test("a wakeup cleanup failure after the brief committed leaves the brief ready", async () => {
    const s = await setup();
    await s.upload([user("fix the build")]);
    await dueNow(s.pg);
    const real = hoopDb(s.pg);
    const failing = {
      ...real,
      query: async (text: string, params?: unknown[]) => {
        if (/DELETE FROM sessions\.session_brief_wakeups\s+WHERE[\s\S]*generation = \$6::bigint/.test(text)) throw new Error("transient");
        return real.query(text, params as never);
      },
    } as typeof real;
    const counts = await briefTick({
      reader: mapReader(s.manifests),
      store: s.store,
      transport: s.model,
      config: testConfig(),
    })(failing, new Date());
    expect(counts).toEqual({ generated: 1 });
    expect(await brief(s.pg)).toMatchObject({ status: "ready", title: s.model.reply.title });
  });
});
