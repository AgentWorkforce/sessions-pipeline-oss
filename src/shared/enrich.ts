/**
 * Section 12 propagation: every StageMessage carries, inside `ctx`, the
 * session uuid, tenant id and a W3C traceparent whose trace id is the pipeline
 * trace id. The vendored builders don't know these fields; this adds them once,
 * at the point a message is sent, by reading the manifest's `extensions`.
 */
import { manifestKey as buildManifestKey, parseSessionKey, type ManifestReader, type ManifestV1, type StageMessage } from "./vendor.ts";
import { isUuid, traceparentFor } from "./ids.ts";
import { currentTraceparent } from "./telemetry.ts";

export const EXT = "96bulls";

export interface SessionExt {
  session_uuid: string;
  session_uuid_source: "minted" | "reused" | "derived";
  tenant_id?: string;
  user_id?: string;
}

export function sessionExt(manifest: ManifestV1): SessionExt | undefined {
  const ext = (manifest.extensions as Record<string, unknown> | undefined)?.[EXT] as Partial<SessionExt> | undefined;
  if (!ext || !isUuid(ext.session_uuid)) return undefined;
  const source = ext.session_uuid_source;
  return {
    session_uuid: ext.session_uuid.toLowerCase(),
    session_uuid_source: source === "reused" || source === "derived" ? source : "minted",
    ...(typeof ext.tenant_id === "string" ? { tenant_id: ext.tenant_id } : {}),
    ...(typeof ext.user_id === "string" ? { user_id: ext.user_id } : {}),
  };
}

export type EnrichedCtx = StageMessage["ctx"] & {
  session_uuid?: string;
  session_uuid_source?: string;
  tenant_id: string;
  traceparent: string;
  /** The authenticated user who caused this work, when known (ingest requests). */
  user_id?: string;
};

import { AsyncLocalStorage } from "node:async_hooks";
/** Set by the ingest server for the duration of an authenticated request. */
export const requestIdentity = new AsyncLocalStorage<{ userId?: string }>();

/** The manifest key for any payload that names an object in the session's prefix. */
export function manifestKeyOf(message: StageMessage): string | undefined {
  const p = message.payload as { key?: unknown; manifestKey?: unknown };
  const key = typeof p?.manifestKey === "string" ? p.manifestKey : typeof p?.key === "string" ? p.key : undefined;
  if (!key) return undefined;
  const loc = parseSessionKey(key);
  if (!loc) return undefined;
  // Immutable managed snapshots and their derived children share one publication.
  const publication=/^publications\/([a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12})\//.exec(loc.rest??'');
  if(publication)return key.slice(0,key.length-loc.rest.length)+publication[0]+'manifest.json';
  return buildManifestKey(loc);
}

/**
 * Returns a copy of `message` whose ctx carries session_uuid, tenant_id and a
 * traceparent that continues the active span when there is one. Reads the
 * manifest when the message does not already carry a session uuid.
 */
export async function enrichMessage(message: StageMessage, reader?: ManifestReader): Promise<StageMessage> {
  const ctx = message.ctx as EnrichedCtx;
  let ext: SessionExt | undefined;
  if (!ctx.session_uuid && reader) {
    const key = manifestKeyOf(message);
    if (key) {
      try {
        const read = await reader.read(key);
        if (read) ext = sessionExt(read.manifest);
      } catch {
        // Unreadable manifest: the stage itself will report it; ids stay absent.
      }
    }
  }
  const active = currentTraceparent();
  const traceparent =
    active && active.slice(3, 35) === message.ctx.traceId.replace(/-/g, "").toLowerCase()
      ? active
      : traceparentFor(message.ctx.traceId);
  return {
    ...message,
    ctx: {
      ...ctx,
      ...(ext ? { session_uuid: ext.session_uuid, session_uuid_source: ext.session_uuid_source } : {}),
      // Tenant is the authenticated org, never a manifest claim.
      tenant_id: message.ctx.orgId,
      ...(ctx.user_id ? { user_id: ctx.user_id } : requestIdentity.getStore()?.userId ? { user_id: requestIdentity.getStore()!.userId } : {}),
      traceparent,
    } as EnrichedCtx,
  };
}

export const ctxOf = (message: StageMessage) => message.ctx as EnrichedCtx;
