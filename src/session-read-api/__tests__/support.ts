/**
 * Test support: a real ClickHouse (READ_API_TEST_CLICKHOUSE_URL, e.g. the CI
 * service or `docker run -p 127.0.0.1:18123:8123 clickhouse/clickhouse-server:26.8`)
 * with the production `transcript_events` DDL, and an in-memory Liveness.
 */
import { createClickHouseClient, type ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { schemaStatements } from "../../session-pipeline/vendor/search-index/schema.ts";
import { keyOf, type Liveness, type SessionKey } from "../page.ts";

export const CH_URL = process.env.READ_API_TEST_CLICKHOUSE_URL;
export const CH_USER = process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default";
export const CH_PASSWORD = process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "";

/** A fresh database with the production schema. */
export async function freshClickHouse(): Promise<ClickHouseClient & { drop(): Promise<void> }> {
  if (!CH_URL) throw new Error("READ_API_TEST_CLICKHOUSE_URL is not set");
  const database = `read_api_test_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;
  const admin = createClickHouseClient({ url: CH_URL, user: CH_USER, password: CH_PASSWORD, database: "default" });
  await admin.command(`CREATE DATABASE ${database}`);
  const ch = createClickHouseClient({ url: CH_URL, user: CH_USER, password: CH_PASSWORD, database });
  for (const statement of schemaStatements()) await ch.command(statement);
  return Object.assign(ch, { drop: () => admin.command(`DROP DATABASE IF EXISTS ${database}`) });
}

export interface RowInput {
  org?: string;
  ws?: string;
  session?: string;
  source?: string;
  machine?: string;
  kind?: string;
  id: string;
  ts: string;
  content?: string;
  title?: string;
  project?: string;
  version?: number;
  user?: string;
}

export function row(r: RowInput) {
  const content = r.content ?? `content of ${r.id}`;
  return {
    org_id: r.org ?? "org-a",
    workspace_id: r.ws ?? "ws-a",
    ts: r.ts,
    session_id: r.session ?? "s-1",
    source: r.source ?? "codex",
    machine_id: r.machine ?? "m-1",
    kind: r.kind ?? "message",
    event_id: r.id,
    type: "assistant",
    user_id: r.user ?? "u-1",
    actor_name: "",
    actor_role: "assistant",
    subagent_id: "",
    project_id: r.project ?? "",
    task_title: r.title ?? "",
    task_ref: "",
    task_ref_id: "",
    tags: [],
    files_touched: [],
    tool_name: "",
    tool_status: "",
    delivery_record_id: "",
    content,
    content_bytes: new TextEncoder().encode(content).length,
    ingested_at: "2026-11-01 00:00:00.000",
    fingerprint: "f",
    version: r.version ?? 1,
  };
}

/**
 * In-memory pointer state of one workspace: erased sessions and created_at per
 * (source, session). A key in `createdAt` that is not erased is a live pointer.
 */
export function memoryLiveness(init: { erased?: SessionKey[]; createdAt?: Record<string, number> } = {}) {
  const erased = new Set((init.erased ?? []).map(keyOf));
  const createdAt = new Map(Object.entries(init.createdAt ?? {}));
  const calls: { erased: SessionKey[][]; livePointerSessions: string[][] } = { erased: [], livePointerSessions: [] };
  const liveness: Liveness = {
    async livePointerSessions(_org, _ws, sessionIds) {
      calls.livePointerSessions.push(sessionIds);
      const live = new Set([...createdAt.keys()].filter((k) => !erased.has(k)).map((k) => k.split("\u0000")[1]!));
      return new Set(sessionIds.filter((id) => live.has(id)));
    },
    async erased(_org, _ws, keys) {
      calls.erased.push(keys);
      return new Set(keys.map(keyOf).filter((k) => erased.has(k)));
    },
    async sessionStart(_org, _ws, sessionId, source) {
      const matches = [...createdAt.entries()].filter(([k]) => {
        const [s, id] = k.split("\u0000");
        return id === sessionId && (source === undefined || s === source);
      });
      const live = matches.filter(([k]) => !erased.has(k));
      const erasedHere = [...erased].filter((k) => {
        const [s, id] = k.split("\u0000");
        return id === sessionId && (source === undefined || s === source);
      });
      return {
        pointers: matches.length + erasedHere.filter((k) => !createdAt.has(k)).length,
        live: live.length,
        createdAtMs: live.length ? Math.min(...live.map(([, v]) => v)) : null,
      };
    },
  };
  return { liveness, calls, erase: (k: SessionKey) => erased.add(keyOf(k)) };
}

export const at = (k: SessionKey) => keyOf(k);
