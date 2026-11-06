// Parity fixture extracted from the former hosted Worker, without hosted configuration.
// Authentication remains the caller's responsibility; this helper does not verify JWT signatures.
import {
  canonicalArtifactPage,
  type ArtifactReadDeps,
} from "../../canonical-artifact-reader.ts";
import { compareArtifactEventRows } from "../../canonical-artifact.ts";
import { objectKey } from "../../../../packages/object-store/src/index.ts";
import { decompress } from "fzstd";
import { rawStoreFromEnv } from "../../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import type { Bindings } from "../../../../packages/pipeline/src/env.ts";
import { logLine } from "../../../shared/log.ts";
import {
  canonicalSnapshot,
  CanonicalReadError,
  type CanonicalIdentity,
  type CanonicalPointer,
  type CanonicalDeps,
} from "../../canonical.ts";

export type CanonicalEnv = Pick<
  Bindings,
  | "PIPELINE_RAW_S3_BUCKET"
  | "PIPELINE_RAW_S3_ENDPOINT"
  | "PIPELINE_RAW_S3_REGION"
  | "PIPELINE_RAW_S3_ACCESS_KEY_ID"
  | "PIPELINE_RAW_S3_SECRET_ACCESS_KEY"
  | "PIPELINE_RAW_S3_SSE"
  | "PIPELINE_RAW_PROVIDER"
  | "PIPELINE_ORG_FROM_PREFIX"
  | "PIPELINE_KEY_PREFIX"
> & {
  CANONICAL_READ_ENABLED?: string;
  CANONICAL_READ_MODE?: "raw" | "pages" | string;
  CANONICAL_SUPABASE_URL?: string;
  CANONICAL_SUPABASE_API_KEY?: string;
};
const MAX_BODY = 2 * 1024 * 1024;
const tables = new Set([
  "convergence_events",
  "session_catalog",
  "session_catalog_agents",
  "session_relationships",
  "session_markers",
  "session_commit_links",
  "conversation_turns",
  "session_links",
  "session_outcomes",
  "session_timing",
  "session_directory_event_values",
]);
async function boundedJson(
  response: Response,
  signal: AbortSignal,
): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) throw new CanonicalReadError("metadata_parse");
  let onAbort!: () => void;
  const abort = new Promise<never>((_, reject) => {
    onAbort = () => {
      reject(new CanonicalReadError("deadline"));
      void reader.cancel().catch(() => {});
    };
  });
  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) onAbort();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const chunk = await Promise.race([reader.read(), abort]);
      if (chunk.done) break;
      length += chunk.value.byteLength;
      if (length > MAX_BODY) throw new CanonicalReadError("metadata_bound");
      chunks.push(chunk.value);
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const c of chunks) {
      bytes.set(c, offset);
      offset += c.byteLength;
    }
    return JSON.parse(
      new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes),
    );
  } finally {
    signal.removeEventListener("abort", onAbort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
function tokenTenant(
  token: string,
  workspace: string,
): { orgId: string; workspaceId: string } {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) throw 0;
    const value = JSON.parse(
      atob(parts[1]!.replace(/-/g, "+").replace(/_/g, "/")),
    );
    const org = value.app_metadata?.org_id,
      workspaces = value.app_metadata?.workspace_ids;
    if (
      value.role !== "session_directory_search_worker" ||
      typeof org !== "string" ||
      !org ||
      org.length > 255 ||
      !Array.isArray(workspaces) ||
      workspaces.length !== 1 ||
      workspaces[0] !== workspace
    )
      throw 0;
    // Untrusted until PostgREST successfully verifies this SAME bearer with the
    // claim-bound RPC. Never use this value for an S3 operation before that call.
    return { orgId: org, workspaceId: workspace };
  } catch {
    throw new CanonicalReadError("authorization", 401);
  }
}
/** PostgREST resolves RPCs by argument NAME, so the body keys must match each SQL
 * signature exactly. request_session_analysis_cache(p_workspace,p_harness,p_session)
 * (20261105023000_session_analysis_publications.sql) is the only claim RPC without the
 * _id suffix; every other one here takes (p_workspace_id,p_harness,p_session_id). */
export function canonicalRpcBody(
  rpc: string,
  identity: Pick<CanonicalIdentity, "workspaceId" | "source" | "sessionId">,
): Record<string, string> {
  return rpc === "request_session_analysis_cache"
    ? {
        p_workspace: identity.workspaceId,
        p_harness: identity.source,
        p_session: identity.sessionId,
      }
    : {
        p_workspace_id: identity.workspaceId,
        p_harness: identity.source,
        p_session_id: identity.sessionId,
      };
}
function canonicalMetadataReader(
  env: CanonicalEnv,
  token: string,
  signal: AbortSignal,
  fetcher: typeof fetch = fetch,
  rpc = "session_directory_pointers",
) {
  const base = env.CANONICAL_SUPABASE_URL,
    apiKey = env.CANONICAL_SUPABASE_API_KEY;
  if (!base || !apiKey) throw new CanonicalReadError("not_configured");
  const url = new URL(base);
  if (url.protocol !== "https:" && url.hostname !== "127.0.0.1")
    throw new CanonicalReadError("not_configured");
  const root = url.pathname.replace(/\/+$/u, "");
  url.pathname =
    (root.endsWith("/rest/v1") ? root : root + "/rest/v1") + "/rpc/" + rpc;
  url.search = "";
  url.hash = "";
  return async (identity: CanonicalIdentity): Promise<unknown> => {
    const response = await fetcher(url, {
      method: "POST",
      redirect: "error",
      headers: {
        apikey: apiKey,
        authorization: `Bearer ${token}`,
        "content-profile": "sessions",
        "content-type": "application/json",
      },
      body: JSON.stringify(canonicalRpcBody(rpc, identity)),
      signal,
    });
    if (!response.ok) {
      void response.body?.cancel().catch(() => {});
      throw new CanonicalReadError(
        "metadata_unavailable",
        response.status === 401 || response.status === 403 ? 401 : 503,
      );
    }
    return boundedJson(response, signal);
  };
}
export { tokenTenant as canonicalTokenTenant };
export function canonicalClaimReader(
  env: CanonicalEnv,
  token: string,
  signal: AbortSignal,
  rpc: "session_directory_analysis_cache" | "request_session_analysis_cache",
  fetcher: typeof fetch = fetch,
) {
  return canonicalMetadataReader(env, token, signal, fetcher, rpc);
}
export function canonicalPointerReader(
  env: CanonicalEnv,
  token: string,
  signal: AbortSignal,
  fetcher: typeof fetch = fetch,
) {
  const read = canonicalMetadataReader(env, token, signal, fetcher);
  return async (identity: CanonicalIdentity): Promise<CanonicalPointer[]> => {
    const value = await read(identity);
    if (!Array.isArray(value)) throw new CanonicalReadError("metadata_parse");
    return value as CanonicalPointer[];
  };
}
export function canonicalPublicationReader(
  env: CanonicalEnv,
  token: string,
  signal: AbortSignal,
  fetcher: typeof fetch = fetch,
) {
  return canonicalMetadataReader(
    env,
    token,
    signal,
    fetcher,
    "session_directory_replay_pages",
  );
}
export async function canonicalHandle(
  request: Request,
  env: CanonicalEnv,
  fetcher: typeof fetch = fetch,
  override?: CanonicalDeps,
  artifactOverride?: ArtifactReadDeps,
): Promise<Response> {
  const id = crypto.randomUUID();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  try {
    if (env.CANONICAL_READ_ENABLED !== "true")
      throw new CanonicalReadError("not_configured");
    const url = new URL(request.url),
      match = /^\/v1\/canonical\/sessions\/([^/]+)$/u.exec(url.pathname);
    if (request.method !== "GET" || !match)
      throw new CanonicalReadError("route", 400);
    for (const key of url.searchParams.keys())
      if (
        ![
          "workspace",
          "source",
          "table",
          "limit",
          "cursor",
          "order",
          "view",
          "maxContent",
          "generation",
          "since",
          "until",
        ].includes(key) ||
        url.searchParams.getAll(key).length !== 1
      )
        throw new CanonicalReadError("parameter", 400);
    const workspace = url.searchParams.get("workspace"),
      source = url.searchParams.get("source"),
      sessionId = decodeURIComponent(match[1]!),
      table = url.searchParams.get("table") ?? "convergence_events",
      limit = Number(url.searchParams.get("limit") ?? 200);
    if (
      !workspace ||
      !source ||
      !/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(source) ||
      !sessionId ||
      [...sessionId].length > 255 ||
      !tables.has(table) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 1000
    )
      throw new CanonicalReadError("parameter", 400);
    const bearer = /^Bearer\s+(\S+)$/iu.exec(
      request.headers.get("authorization") ?? "",
    )?.[1];
    if (!bearer) throw new CanonicalReadError("authorization", 401);
    const identity = { ...tokenTenant(bearer, workspace), source, sessionId };
    const signal = AbortSignal.any([request.signal, controller.signal]);
    const boundedFetch: typeof fetch = (input, init) =>
      fetcher(input, {
        ...init,
        signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal,
      });
    const expectedGeneration = url.searchParams.get("generation");
    if (
      expectedGeneration !== null &&
      !/^[0-9a-f]{64}$/u.test(expectedGeneration)
    )
      throw new CanonicalReadError("parameter", 400);
    const view = url.searchParams.get("view") ?? "raw";
    const rawMax = url.searchParams.get("maxContent"),
      maxContent = rawMax === null ? undefined : Number(rawMax);
    if (
      (view !== "replay" && maxContent !== undefined) ||
      !["raw", "replay", "bounds", "legacy"].includes(view) ||
      (["replay", "bounds"].includes(view) && table !== "convergence_events") ||
      (view === "legacy" &&
        !["conversation_turns", "session_links", "session_outcomes"].includes(
          table,
        )) ||
      (maxContent !== undefined &&
        (!Number.isFinite(maxContent) ||
          maxContent <= 0 ||
          Math.trunc(maxContent) !== maxContent))
    )
      throw new CanonicalReadError("parameter", 400);
    const order = url.searchParams.get("order") ?? "asc";
    if (order !== "asc" && order !== "desc")
      throw new CanonicalReadError("parameter", 400);
    const mode = env.CANONICAL_READ_MODE ?? "raw";
    // Raw mode has no timing projection: keep refusing session_timing (and
    // windows) there; only the pages artifact serves them.
    if (
      mode !== "pages" &&
      (url.searchParams.has("since") ||
        url.searchParams.has("until") ||
        table === "session_timing")
    )
      throw new CanonicalReadError("parameter", 400);
    if (mode !== "raw" && mode !== "pages")
      throw new CanonicalReadError("not_configured");
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new CanonicalReadError("deadline"));
      }, 8000);
    });
    if (mode === "pages") {
      let artifactDeps = artifactOverride;
      if (!artifactDeps) {
        const raw = rawStoreFromEnv(env, boundedFetch);
        if (!raw) throw new CanonicalReadError("not_configured");
        artifactDeps = {
          signal,
          publication: canonicalPublicationReader(
            env,
            bearer,
            signal,
            boundedFetch,
          ),
          read: async (key) => (await raw.store.getObject(objectKey(key))).body,
        };
      }
      const page = await Promise.race([
        canonicalArtifactPage({ ...artifactDeps, signal }, identity, {
          table,
          view: view as "raw" | "replay" | "bounds" | "legacy",
          limit,
          order,
          cursor: url.searchParams.get("cursor"),
          maxContent,
          generation: expectedGeneration,
          since: url.searchParams.get("since") ?? undefined,
          until: url.searchParams.get("until") ?? undefined,
        }),
        deadline,
      ]);
      return Response.json(
        {
          orgId: identity.orgId,
          workspaceId: workspace,
          source,
          sessionId,
          ...page,
          correlationId: id,
        },
        {
          headers: {
            "X-Relayhistory-Workspace-Id": workspace,
            "Cache-Control": "private, no-store",
          },
        },
      );
    }
    let deps = override;
    if (!deps) {
      const raw = rawStoreFromEnv(env, boundedFetch);
      if (!raw) throw new CanonicalReadError("not_configured");
      deps = {
        ...raw,
        prefix: env,
        pointers: canonicalPointerReader(env, bearer, signal, boundedFetch),
        decompress: (bytes, max) => {
          const output = decompress(bytes, new Uint8Array(max));
          if (output.byteLength > max)
            throw new CanonicalReadError("chunk_bound");
          return output;
        },
      };
    }
    const snapshot = await Promise.race([
      // Display values only for the table that returns them; every other table reads as before.
      canonicalSnapshot(deps, identity, { directoryValues: table === "session_directory_event_values" }),
      deadline,
    ]);
    // Requested display values are optional on the raw path: their refusal omits those rows only.
    if ("displayOmitted" in snapshot && snapshot.displayOmitted && table === "session_directory_event_values")
      console.error(logLine("warn", "canonical display values omitted", { service: "96bulls-session-read-worker", stage: "canonical-read", org_id: identity.orgId, workspace_id: identity.workspaceId, harness: identity.source, session_id: identity.sessionId, reason: snapshot.displayOmitted }));
    if (
      expectedGeneration !== null &&
      expectedGeneration !== snapshot.generation
    )
      throw new CanonicalReadError("source_generation_changed");
    let offset = 0;
    const cursor = url.searchParams.get("cursor");
    if (cursor) {
      try {
        const c = JSON.parse(atob(cursor));
        if (
          c.generation !== snapshot.generation ||
          c.table !== table ||
          c.order !== order ||
          c.view !== view ||
          c.maxContent !== (maxContent ?? null) ||
          !Number.isSafeInteger(c.offset) ||
          c.offset < 0
        )
          throw 0;
        offset = c.offset;
      } catch {
        throw new CanonicalReadError("cursor", 400);
      }
    }
    const all = snapshot.rows
      .filter((row) => row.table === table)
      .map((item) => {
        if (view === "bounds")
          return {
            ...item,
            row: Object.fromEntries(
              [
                "org_id",
                "workspace_id",
                "source",
                "session_id",
                "event_id",
                "machine_id",
                "kind",
                "ts",
              ].map((k) => [k, item.row[k]]),
            ),
          };
        if (view !== "replay") return item;
        const { record, ...row } = item.row;
        const payload =
          record && typeof record === "object"
            ? (record as { payload?: unknown }).payload
            : undefined;
        const message =
          payload && typeof payload === "object"
            ? (payload as { message_id?: unknown }).message_id
            : undefined;
        // Replay alone needs native grouping, not the duplicate raw payload. Keep
        // every flat projected field, including task/counter/tool arrays intact.
        return {
          ...item,
          row: {
            ...row,
            ...(typeof row.content === "string" &&
            maxContent !== undefined &&
            row.content.length > maxContent
              ? { content: row.content.slice(0, maxContent + 1) }
              : {}),
            record: {
              payload: {
                message_id: typeof message === "string" ? message : null,
              },
            },
          },
        };
      });
    if (table === "convergence_events")
      all.sort(
        (a, b) =>
          (order === "asc" ? 1 : -1) * compareArtifactEventRows(a.row, b.row),
      );
    if (offset > all.length) throw new CanonicalReadError("cursor", 400);
    const rows = [];
    let bytes = 0;
    for (const row of all.slice(offset, offset + limit)) {
      const size = new TextEncoder().encode(JSON.stringify(row)).byteLength;
      if (bytes + size > MAX_BODY - 4096 && rows.length) break;
      if (
        !rows.length &&
        size >
          (["replay", "legacy"].includes(view)
            ? 64 * 1024 * 1024
            : MAX_BODY - 4096)
      )
        throw new CanonicalReadError("row_bound");
      rows.push(row);
      bytes += size;
    }
    const next = offset + rows.length;
    return Response.json(
      {
        orgId: identity.orgId,
        workspaceId: workspace,
        source,
        sessionId,
        generation: snapshot.generation,
        exists: snapshot.exists,
        legacyCoverage: snapshot.legacyCoverage,
        rows,
        nextCursor:
          next < all.length
            ? btoa(
                JSON.stringify({
                  generation: snapshot.generation,
                  table,
                  order,
                  view,
                  maxContent: maxContent ?? null,
                  offset: next,
                }),
              )
            : null,
        correlationId: id,
      },
      {
        headers: {
          "X-Relayhistory-Workspace-Id": workspace,
          "Cache-Control": "private, no-store",
        },
      },
    );
  } catch (error) {
    const e =
      error instanceof CanonicalReadError
        ? error
        : new CanonicalReadError("canonical_unavailable");
    return Response.json(
      {
        // 501 matches the read API's unsupported_by_backend body: the caller
        // falls back instead of retrying.
        error:
          e.status === 501
            ? {
                code: "unsupported_by_backend",
                message: `not served by this backend: ${e.reason}`,
                reason: e.reason,
              }
            : {
                code:
                  e.status === 400
                    ? "bad_request"
                    : e.status === 401
                      ? "unauthorized"
                      : "session_read_unavailable",
                message: e.message,
              },
        correlationId: id,
      },
      {
        status: e.status,
        headers: {
          "X-Session-Read-Reason": e.reason,
          "Cache-Control": "private, no-store",
        },
      },
    );
  } finally {
    if (timer) clearTimeout(timer);
    controller.abort();
  }
}
