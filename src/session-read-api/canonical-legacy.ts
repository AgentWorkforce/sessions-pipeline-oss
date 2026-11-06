import { comparePublicTurnRows } from "../shared/legacy-turn-source.ts";
/** Legacy public evidence preserved explicitly in the canonical raw stream. */
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import type { CanonicalIdentity } from "./canonical.ts";
export const LEGACY_SOURCE_TABLES = [
  "conversation_turns",
  "session_links",
  "session_outcomes",
] as const;
const kinds = new Map<string, string>([
  ["legacy_conversation_turn", "conversation_turns"],
  ["legacy_session_link", "session_links"],
  ["legacy_session_outcome", "session_outcomes"],
]);
function fail(): never {
  throw new Error("canonical legacy evidence invalid");
}
const clock = (v: unknown, nullable = false) =>
  (nullable && v === null) ||
  (typeof v === "string" && Number.isFinite(Date.parse(v)));
const integer = (v: unknown) =>
  typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
export function canonicalLegacy(
  records: RelayhistoryStreamRecord[],
  identity: CanonicalIdentity,
) {
  let coverage = false;
  const rows: Array<{ table: string; row: Record<string, unknown> }> = [];
  for (const record of records) {
    const table = kinds.get(record.kind);
    if (!table && record.kind !== "legacy_scope") continue;
    const p = record.payload;
    if (
      !p ||
      p.org_id !== identity.orgId ||
      p.workspace_id !== identity.workspaceId ||
      p.source !== identity.source ||
      p.session_id !== identity.sessionId
    )
      fail();
    if (record.kind === "legacy_scope") {
      if (
        p.version !== 1 ||
        record.origin_id !== "legacy:scope" ||
        record.record_id !== "legacy:scope:coverage" ||
        !Array.isArray(p.tables) ||
        JSON.stringify([...p.tables].sort()) !==
          JSON.stringify([...LEGACY_SOURCE_TABLES].sort())
      )
        fail();
      coverage = true;
      continue;
    }
    if (record.origin_id !== "legacy:" + table) fail();
    if (table === "conversation_turns") {
      if (
        typeof p.id !== "string" ||
        "legacy:" + table + ":" + p.id !== record.record_id ||
        (p.legacy_stored_source !== undefined &&
          typeof p.legacy_stored_source !== "string") ||
        !integer(p.turn_index) ||
        Number(p.turn_index) > 2147483647 ||
        !["user", "assistant", "system"].includes(String(p.role)) ||
        !["session_owner", "content", "actor_name", "actor_role"].every(
          (k) => typeof p[k] === "string",
        ) ||
        !clock(p.ts)
      )
        fail();
    } else if (table === "session_links") {
      if (
        typeof p.id !== "string" ||
        "legacy:" + table + ":" + p.id !== record.record_id ||
        !/^[1-9][0-9]{0,18}$/u.test(p.id) ||
        BigInt(p.id) > 9223372036854775807n ||
        !["link_kind", "link_ref"].every(
          (k) => typeof p[k] === "string" && p[k],
        ) ||
        !clock(p.link_ts, true)
      )
        fail();
    } else if (table === "session_outcomes") {
      if (
        typeof p.commit_sha !== "string" ||
        "legacy:" + table + ":" + p.commit_sha !== record.record_id ||
        !p.commit_sha ||
        typeof p.reverted !== "boolean" ||
        !clock(p.shipped_at, true) ||
        !clock(p.created_at)
      )
        fail();
    }
    rows.push({ table: table!, row: p });
  }
  const orderedTurns = rows
    .filter((r) => r.table === "conversation_turns")
    .sort((a, b) => comparePublicTurnRows(a.row, b.row));
  let next = 0;
  return {
    rows: rows.map((r) =>
      r.table === "conversation_turns" ? orderedTurns[next++]! : r,
    ),
    coverage: coverage
      ? { version: 1 as const, tables: [...LEGACY_SOURCE_TABLES] }
      : null,
  };
}
