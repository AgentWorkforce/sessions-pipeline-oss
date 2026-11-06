/**
 * The PostgREST writers against the real Supabase `sessions` schema
 * (`supabase/migrations`, owned by the schema workstream), applied to local
 * PGlite. A small PostgREST stand-in turns the
 * writers' REST calls into SQL run as `service_role`, so a column, type, CHECK
 * or grant the code disagrees with fails here. No hosted Supabase is involved.
 */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import {
  deadLetterId,
  sessionIndexedHandler,
  maintainPipelineEvents,
  PIPELINE_STATUSES,
  PipelineRecorder,
  postgrestDeadLetterStore,
  postgrestPipelineEventSink,
  replayDeadLetters,
  objectLandedHandler,
  summarizeTrace,
  fileNormalizeHandler,
  uuidv7,
  type RawPointer,
  type StageQueueDeps,
} from "../src/lib/pipeline/index.js";
import { FakePipelineQueues } from "./support/pipeline-queues.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const WORKSPACE = "ws_synthetic";
const ORG = "org_synthetic";
const SCOPE = { orgId: ORG, workspaceId: WORKSPACE };
const MACHINE = "machine-synthetic";
const SESSION = "sess-synthetic-1";
const MANIFEST_KEY = `tenant-a/v1/ws=${WORKSPACE}/m=${MACHINE}/h=claude/d=2026/10/25/s=${SESSION}/manifest.json`;

let db: PGlite;

const IDENT = /^[a-z_]+$/;
function ident(name: string) {
  if (!IDENT.test(name)) throw new Error(`bad identifier ${name}`);
  return name;
}

/** Just enough PostgREST for the two stores: eq/neq/in filters, order, limit, upsert, PATCH. */
async function postgrest(url: string, init: RequestInit = {}) {
  const parsed = new URL(url);
  const table = `sessions.${ident(parsed.pathname.split("/").pop()!)}`;
  const params: unknown[] = [];
  const where: string[] = [];
  let order = "";
  let limit = "";
  let onConflict = "";
  for (const [key, value] of parsed.searchParams) {
    if (key === "order") {
      order = ` ORDER BY ${value
        .split(",")
        .map((part) => {
          const [column, direction] = part.split(".");
          return `${ident(column!)} ${direction === "desc" ? "DESC" : "ASC"}`;
        })
        .join(", ")}`;
    } else if (key === "limit") limit = ` LIMIT ${Number(value)}`;
    else if (key === "offset") limit += ` OFFSET ${Number(value)}`;
    else if (key === "on_conflict") onConflict = ident(value);
    else {
      const [op, ...rest] = value.split(".");
      const operand = rest.join(".");
      if (op === "in") {
        params.push(operand.slice(1, -1).split(","));
        where.push(`${ident(key)}::text = ANY($${params.length}::text[])`);
      } else {
        params.push(operand);
        if (op === "is" && operand === "null") {
          params.pop();
          where.push(`${ident(key)} IS NULL`);
        } else if (op === "lt")
          where.push(`${ident(key)} < $${params.length}::timestamptz`);
        else
          where.push(
            `${ident(key)}::text ${op === "neq" ? "<>" : "="} $${params.length}::text`,
          );
      }
    }
  }
  const filter = where.length ? ` WHERE ${where.join(" AND ")}` : "";
  const prefer = new Headers(init.headers).get("Prefer") ?? "";
  const returning = prefer.includes("return=representation")
    ? ` RETURNING to_jsonb(${table.split(".")[1]}.*) AS row`
    : "";
  let sql: string;
  if (!init.method || init.method === "GET") {
    sql = `SELECT to_jsonb(t.*) AS row FROM ${table} t${filter}${order}${limit}`;
  } else {
    const body = JSON.parse(String(init.body));
    const rows = Array.isArray(body) ? body : [body];
    const columns = Object.keys(rows[0]).map(ident);
    params.push(JSON.stringify(init.method === "PATCH" ? rows[0] : rows));
    const json = `$${params.length}::jsonb`;
    if (init.method === "PATCH") {
      sql = `UPDATE ${table} SET (${columns.join(",")}) = (SELECT ${columns.join(",")} FROM jsonb_populate_record(NULL::${table}, ${json}))${filter}${returning}`;
    } else {
      const upsert =
        onConflict && prefer.includes("merge-duplicates")
          ? ` ON CONFLICT (${onConflict}) DO UPDATE SET ${columns.map((c) => `${c}=EXCLUDED.${c}`).join(",")}`
          : "";
      sql = `INSERT INTO ${table} (${columns.join(",")}) SELECT ${columns.join(",")} FROM jsonb_populate_recordset(NULL::${table}, ${json})${upsert}${returning}`;
    }
  }
  try {
    const result = await db.query<{ row: unknown }>(sql, params);
    return returning || !init.method || init.method === "GET"
      ? Response.json(result.rows.map((r) => r.row))
      : new Response(null, { status: 201 });
  } catch (error) {
    return Response.json(
      { message: (error as Error).message },
      { status: 400 },
    );
  }
}

const fetcher = postgrest as unknown as typeof fetch;
const options = { url: "http://postgrest.local", token: "synthetic", fetcher };

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_directory_search_worker.sql"),
      "utf8",
    ),
  );
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_token_verifier.sql"),
      "utf8",
    ),
  );
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_ingest_roles.sql"),
      "utf8",
    ),
  );
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
  await db.exec("SET ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  vi.spyOn(console, "info").mockImplementation(() => {});
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
  await db.exec(
    "TRUNCATE sessions.pipeline_events; TRUNCATE sessions.pipeline_dead_letters",
  );
});

describe("pipeline_events on the Supabase schema", () => {
  it("stores every status the recorder emits and reads the trace back", async () => {
    const sink = postgrestPipelineEventSink(options);
    const traceId = uuidv7();
    const recorder = new PipelineRecorder(
      {
        traceId,
        orgId: ORG,
        workspaceId: WORKSPACE,
        machineId: MACHINE,
        harness: "claude",
        sessionId: SESSION,
      },
      { sink },
    );
    for (const [index, status] of PIPELINE_STATUSES.entries())
      await recorder.record("file-normalize", status, {
        attempt: index + 1,
        errorCode: "transform_failed",
        attrs: { duration_ms: 7 },
        now: new Date(Date.UTC(2026, 9, 25, 12, 0, index)),
      });
    // A write failure is swallowed by the recorder, so assert the rows landed.
    const rows = await db.query<Record<string, unknown>>(
      "SELECT * FROM sessions.pipeline_events ORDER BY at",
    );
    expect(rows.rows).toHaveLength(PIPELINE_STATUSES.length);
    expect(rows.rows[2]).toMatchObject({
      org_id: "org_synthetic",
      machine_id: MACHINE,
      harness: "claude",
      duration_ms: 7,
      attempt: 3,
      error: { code: "transform_failed", name: "undefined", retryable: true },
    });
    const { events } = await sink.byTrace(SCOPE, traceId);
    expect(events.map((event) => event.status)).toEqual([...PIPELINE_STATUSES]);
    expect(
      (await sink.byTrace({ orgId: ORG, workspaceId: "ws_other" }, traceId))
        .events,
    ).toEqual([]);
    expect(
      (
        await sink.byTrace(
          { orgId: "org_other", workspaceId: WORKSPACE },
          traceId,
        )
      ).events,
    ).toEqual([]);
  });

  it("rejects stages and statuses outside the contract", async () => {
    await expect(
      db.query(
        "INSERT INTO sessions.pipeline_events (trace_id, stage, status) VALUES ($1, 'upload', 'ok')",
        [uuidv7()],
      ),
    ).rejects.toThrow(/pipeline_events_stage_check/);
    await expect(
      db.query(
        "INSERT INTO sessions.pipeline_events (trace_id, stage, status) VALUES ($1, 'fetch', 'retry')",
        [uuidv7()],
      ),
    ).rejects.toThrow(/pipeline_events_status_check/);
  });

  it("keeps the new columns and checks on partitions created later", async () => {
    await db.query(
      "SELECT sessions.pipeline_events_maintain(p_months_ahead => 4)",
    );
    const later = new Date();
    // Mid-month first: from the 29th-31st, adding months can roll into the
    // month after (Oct 29 + 4 months = "Feb 29" = Mar 1), past the partitions.
    later.setUTCDate(15);
    later.setUTCMonth(later.getUTCMonth() + 4);
    const sink = postgrestPipelineEventSink(options);
    const traceId = uuidv7();
    await new PipelineRecorder(
      { traceId, orgId: ORG, workspaceId: WORKSPACE, machineId: MACHINE },
      { sink },
    ).record("fetch", "ok", { now: later });
    const [row] = (
      await db.query<{ partition: string; machine_id: string }>(
        "SELECT tableoid::regclass::text AS partition, machine_id FROM sessions.pipeline_events WHERE trace_id = $1",
        [traceId],
      )
    ).rows;
    expect(row!.partition).not.toBe("sessions.pipeline_events_default");
    expect(row!.machine_id).toBe(MACHINE);
  });
});

describe("dead letters on the Supabase schema", () => {
  it("quarantines, replays idempotently and resolves against the real table", async () => {
    const sink = postgrestPipelineEventSink(options);
    const deadLetters = postgrestDeadLetterStore(options);
    const queues = new FakePipelineQueues();
    const pointers = new Map<string, RawPointer>();
    let healthy = false;
    let applied = 0;
    const deps: StageQueueDeps = {
      sink,
      deadLetters,
      handlers: {
        "object-landed": objectLandedHandler({
          senders: { send: (stage, message) => queues.send(stage, message) },
          quarantine: (message, at) => deadLetters.quarantine(message, at),
          transformEnabled: true,
        }),
        "session-indexed": sessionIndexedHandler({
          upsertRawPointer: async (pointer) => {
            pointers.set(pointer.manifestKey, pointer);
          },
        }),
        "file-normalize": fileNormalizeHandler({
          normalize: async () => {
            if (!healthy) throw new Error("provider down");
            applied += 1;
            return {};
          },
        }),
      },
    };
    const traceId = uuidv7();
    await queues.send("object-landed", {
      v: 1,
      stage: "object-landed",
      ctx: { traceId, workspaceId: WORKSPACE, orgId: ORG },
      workKey: `${MANIFEST_KEY}@v1`,
      payload: { key: MANIFEST_KEY, kind: "manifest", version: "v1" },
    });
    await queues.drain(deps);
    expect(pointers.size).toBe(1);
    const [letter] = (
      await db.query<Record<string, unknown>>(
        "SELECT * FROM sessions.pipeline_dead_letters",
      )
    ).rows;
    expect(letter).toMatchObject({
      stage: "file-normalize",
      state: "dead",
      org_id: "org_synthetic",
      dead_count: 1,
    });

    queues.sent.length = 0;
    const replay = () =>
      replayDeadLetters(
        {
          deadLetters,
          sink,
          send: (stage, message) => queues.send(stage, message),
        },
        { dryRun: false },
      );
    const results = await Promise.all([replay(), replay(), replay()]);
    expect(results.flatMap((result) => result.sent)).toHaveLength(1);
    expect(queues.sent).toHaveLength(1);

    healthy = true;
    await queues.drain(deps);
    expect(applied).toBe(1);
    expect((await replay()).sent).toEqual([]);
    const resolved = (
      await db.query<Record<string, unknown>>(
        "SELECT state, replay_count, resolved_at FROM sessions.pipeline_dead_letters",
      )
    ).rows[0]!;
    expect(resolved.state).toBe("resolved");
    expect(resolved.replay_count).toBe(1);
    expect(resolved.resolved_at).not.toBeNull();

    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.verdict).toBe("indexed");
    expect(summary.stages["file-normalize"].status).toBe("ok");
  });

  it("never undoes a replay claim that lands while a death is quarantined", async () => {
    const message = {
      v: 1 as const,
      stage: "session-indexed" as const,
      ctx: { traceId: uuidv7(), orgId: ORG, workspaceId: WORKSPACE },
      workKey: "raced",
      payload: {},
    };
    const id = deadLetterId(message.stage, message.ctx, message.workKey);
    await postgrestDeadLetterStore(options).quarantine(message, new Date());
    const replayId = uuidv7();
    let raced = false;
    const store = postgrestDeadLetterStore({
      ...options,
      fetcher: (async (url: string, init?: RequestInit) => {
        const response = await postgrest(url, init);
        // Between quarantine's read and its write, a replay claims the letter.
        if (!raced && (!init?.method || init.method === "GET")) {
          raced = true;
          await db.query(
            `UPDATE sessions.pipeline_dead_letters
                SET state = 'replaying', replay_count = replay_count + 1,
                    last_replay_id = $2, last_replay_at = now()
              WHERE id = $1`,
            [id, replayId],
          );
        }
        return response;
      }) as unknown as typeof fetch,
    });
    const letter = await store.quarantine(message, new Date());
    expect(raced).toBe(true);
    expect(letter).toMatchObject({
      state: "dead",
      deadCount: 2,
      replayCount: 1,
      lastReplayId: replayId,
    });
  });

  it("fills the session on a later death over REST too", async () => {
    const store = postgrestDeadLetterStore(options);
    const bare = {
      v: 1 as const,
      stage: "session-indexed" as const,
      ctx: { traceId: uuidv7(), orgId: ORG, workspaceId: WORKSPACE },
      workKey: "late-rest",
      payload: {},
    };
    await store.quarantine(bare, new Date());
    await store.quarantine(
      { ...bare, ctx: { ...bare.ctx, sessionId: SESSION, machineId: MACHINE } },
      new Date(),
    );
    await store.quarantine(bare, new Date());
    expect(
      (
        await db.query(
          "SELECT session_id, machine_id FROM sessions.pipeline_dead_letters WHERE id = $1",
          [deadLetterId(bare.stage, bare.ctx, bare.workKey)],
        )
      ).rows,
    ).toEqual([{ session_id: SESSION, machine_id: MACHINE }]);
  });

  it("treats a replaying letter with no claim time as expired", async () => {
    const store = postgrestDeadLetterStore(options);
    const message = {
      v: 1 as const,
      stage: "file-normalize" as const,
      ctx: { traceId: uuidv7(), orgId: ORG, workspaceId: WORKSPACE },
      workKey: "undated",
      payload: {},
    };
    await store.quarantine(message, new Date());
    await db.query(
      "UPDATE sessions.pipeline_dead_letters SET state = 'replaying', last_replay_at = NULL WHERE id = $1",
      [deadLetterId("file-normalize", message.ctx, "undated")],
    );
    const eligible = await store.list({
      replayable: { leaseCutoff: new Date().toISOString() },
    });
    expect(eligible.map((letter) => letter.id)).toEqual([
      `file-normalize:${ORG}:${WORKSPACE}:undated`,
    ]);
  });

  it("bounds every store request with a deadline", async () => {
    const signals: (AbortSignal | null | undefined)[] = [];
    const store = postgrestDeadLetterStore({
      ...options,
      fetcher: (async (url: string, init?: RequestInit) => {
        signals.push(init?.signal);
        return postgrest(url, init);
      }) as unknown as typeof fetch,
    });
    await store.resolve("file-normalize:missing", new Date());
    await store.list({});
    expect(signals.length).toBeGreaterThan(0);
    expect(signals.every((signal) => signal instanceof AbortSignal)).toBe(true);
  });

  it("is invisible to tenant roles", async () => {
    await db.exec("RESET ROLE; SET ROLE authenticated");
    try {
      await expect(
        db.query("SELECT 1 FROM sessions.pipeline_dead_letters"),
      ).rejects.toThrow(/permission denied/);
    } finally {
      await db.exec("RESET ROLE; SET ROLE service_role");
    }
  });
});

describe("pipeline_events maintenance", () => {
  it("calls the maintain RPC once a day, only when configured", async () => {
    const calls: string[] = [];
    const fetcher = (async (url: string, init?: RequestInit) => {
      calls.push(url);
      expect(new Headers(init?.headers).get("Content-Profile")).toBe(
        "sessions",
      );
      // As REST runs it: service_role, which can create partitions only because
      // the function is SECURITY DEFINER.
      await db.query(
        "SELECT sessions.pipeline_events_maintain(p_months_ahead => 6)",
      );
      return new Response(null, { status: 204 });
    }) as unknown as typeof fetch;
    const env = {
      PIPELINE_EVENTS_URL: "http://postgrest.local/",
      PIPELINE_EVENTS_TOKEN: "synthetic",
    };
    const at = (h: number, m: number) => new Date(Date.UTC(2026, 9, 25, h, m));
    expect(await maintainPipelineEvents({}, at(3, 17), fetcher)).toBe(
      "skipped",
    );
    expect(await maintainPipelineEvents(env, at(3, 16), fetcher)).toBe(
      "skipped",
    );
    expect(await maintainPipelineEvents(env, at(3, 17), fetcher)).toBe("ok");
    expect(calls).toEqual([
      "http://postgrest.local/rpc/pipeline_events_maintain",
    ]);
    const failing = (async () => {
      throw new Error("down");
    }) as unknown as typeof fetch;
    expect(await maintainPipelineEvents(env, at(3, 17), failing)).toBe(
      "failed",
    );
  });
});
