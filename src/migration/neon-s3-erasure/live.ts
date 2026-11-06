/**
 * relayhistory-api's live writer (`WRITE_PATH`, relayhistory-cloud #241)
 * stores delivered sessions under its own machine id, so the exporter's
 * watermark never lists them:
 *
 *   {root}/{org}/v1/ws={ws}/m=srv_{sha256(ws \x1f origin \x1f user)[0:32]}/h={source}/d=…/s={session}/…
 *   {root}/{org}/live-sessions/v1/ws={ws}/m=srv_…/h={source}/s={session}.json   (pointer)
 *
 * Path components use the same map as the exporter (`component`) and the
 * Worker. The `d=` comes from the session's first event and the eraser has no
 * GetObject, so the session directory is found by listing under `h=` and
 * matching `s=`.
 */
import { createHash } from "node:crypto";
import { component, orgPrefix } from "../neon-s3-sync/export-session.ts";
import type { LiveFencedGroup } from "../neon-s3-sync/neon-reader.ts";

/** relayhistory-cloud `liveMachineId`: per user, so one user's erasure never reaches another's copy. */
export function liveMachineId(workspaceId: string, originId: string, userId: string): string {
  return `srv_${createHash("sha256").update(`${workspaceId}\u001f${originId}\u001f${userId}`).digest("hex").slice(0, 32)}`;
}

/** Where one (workspace, user, origin)'s pointers live: one list call covers all its sessions. */
export function livePointerPrefix(keyRoot: string, g: Pick<LiveFencedGroup, "org_id" | "workspace_id" | "origin_id" | "user_id">): string {
  return `${orgPrefix(keyRoot, g.org_id)}/live-sessions/v1/ws=${component(g.workspace_id)}/m=${liveMachineId(g.workspace_id, g.origin_id, g.user_id)}/`;
}

/** The (workspace, user, origin)'s object root: one list call shows every session with live objects. */
export function liveObjectPrefix(keyRoot: string, g: Pick<LiveFencedGroup, "org_id" | "workspace_id" | "origin_id" | "user_id">): string {
  return `${orgPrefix(keyRoot, g.org_id)}/v1/ws=${component(g.workspace_id)}/m=${liveMachineId(g.workspace_id, g.origin_id, g.user_id)}/`;
}

/**
 * The session ingest's staged batches for one (workspace, user, origin)
 * (96BullSessions src/session-ingest/staging.ts): one object per batch wider
 * than a Worker invocation, holding every session of that batch.
 */
export function liveStagedPrefix(keyRoot: string, g: Pick<LiveFencedGroup, "org_id" | "workspace_id" | "origin_id" | "user_id">): string {
  return `${orgPrefix(keyRoot, g.org_id)}/live-batches/v1/ws=${component(g.workspace_id)}/m=${liveMachineId(g.workspace_id, g.origin_id, g.user_id)}/`;
}

/** `h=…/s=…` of every session directory under `prefix` with at least one object version. */
export function liveSessionsIn(prefix: string, keys: readonly string[]): Set<string> {
  const out = new Set<string>();
  for (const key of keys) {
    if (!key.startsWith(prefix)) continue;
    const m = /^(h=[^/]+)\/d=\d{4}\/\d{2}\/\d{2}\/(s=[^/]+)\//.exec(key.slice(prefix.length));
    if (m) out.add(`${m[1]}/${m[2]}`);
  }
  return out;
}

export interface LiveTargets {
  /** List base, ends in `/`: `{root}/{org}/v1/ws=/m=srv_/h=/`. */
  base: string;
  /** `s={session}`. */
  sessionSegment: string;
  pointerKey: string;
}

export function liveTargets(keyRoot: string, g: LiveFencedGroup): LiveTargets {
  const org = orgPrefix(keyRoot, g.org_id);
  const ws = component(g.workspace_id);
  const m = liveMachineId(g.workspace_id, g.origin_id, g.user_id);
  const h = component(g.source);
  const s = component(g.session_id);
  return {
    base: `${org}/v1/ws=${ws}/m=${m}/h=${h}/`,
    sessionSegment: `s=${s}`,
    pointerKey: `${org}/live-sessions/v1/ws=${ws}/m=${m}/h=${h}/s=${s}.json`,
  };
}
