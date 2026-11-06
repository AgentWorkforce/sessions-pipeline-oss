/**
 * Stand-in for relayhistory-cloud packages/relayhistory/src/app.ts.
 *
 * Upstream `createApp()` is the whole relayhistory Hono app. This one carries only
 * what integration/relayhistory-cloud/packages/relayhistory/src/app.ts.patch
 * (PR #210) adds, so test/pipeline-observability.test.ts can drive it:
 *   - the id middleware, including the trace id read from `X-Relay-Trace-Id` /
 *     `traceparent` and echoed back on the response;
 *   - one request log line with `trace_id` (the observability.ts.patch hunk), in
 *     place of upstream middleware/observability.ts and its request stats;
 *   - `/health`;
 *   - `pipelineOpsRoutes` under /v1 outside user auth, and `pipelineRoutes` under
 *     /v1 behind `auth`.
 * Upstream's `auth` is `requireAuth` (token verification against the database).
 * Here it is injectable; the default only checks that a host middleware already
 * set `auth` on the context and answers 401 otherwise.
 */
import { Hono, type MiddlewareHandler } from "hono";
import { matchedRoutes } from "hono/route";
import type { AppContext, AppEnv } from "./env.js";
import { createLogger, loggerEnvFromBindings } from "./lib/logger.js";
import { TRACE_ID_HEADER, traceIdFromHeaders } from "./lib/pipeline/trace.js";
import { pipelineOpsRoutes, pipelineRoutes } from "./routes/pipeline.js";

export interface CreateAppOptions {
  /** Authenticates `/v1` user routes. Default: 401 unless `auth` is already set. */
  auth?: MiddlewareHandler<AppEnv>;
}

export const requireHostAuth: MiddlewareHandler<AppEnv> = async (c, next) => {
  if (!c.get("auth")) {
    return c.json(
      {
        error: {
          code: "missing_authorization",
          message: "missing Authorization header",
        },
        correlationId: c.get("correlationId") ?? "",
      },
      401,
    );
  }
  await next();
};

export function createApp(options: CreateAppOptions = {}): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    const correlationId = c.req.header("X-Correlation-Id")?.trim() || requestId;

    // Only a well-formed session pipeline trace id is kept; anything else is ignored.
    const traceId = traceIdFromHeaders((name) => c.req.header(name));

    c.set("requestId", requestId);
    c.set("correlationId", correlationId);
    if (traceId) c.set("traceId", traceId);

    await next();

    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", correlationId);
    if (traceId) c.header(TRACE_ID_HEADER, traceId);
  });
  app.use("*", requestLog);

  app.get("/health", (c) => c.json({ ok: true, service: "pipeline" }));

  const v1Routes = new Hono<AppEnv>();
  v1Routes.use("*", options.auth ?? requireHostAuth);
  v1Routes.route("/", pipelineRoutes);

  app.route("/v1", pipelineOpsRoutes);
  app.route("/v1", v1Routes);

  return app;
}

/** The request line, reduced to the fields this package can fill. */
const requestLog: MiddlewareHandler<AppEnv> = async (c, next) => {
  const started = Date.now();
  await next();
  const status = c.res.status;
  const fields = {
    request_id: c.get("requestId"),
    correlation_id: c.get("correlationId"),
    ...(c.get("traceId") ? { trace_id: c.get("traceId") } : {}),
    method: c.req.method,
    route: matchedRoute(c),
    status_code: status,
    duration_ms: Date.now() - started,
  };
  // `c.env` is undefined when a test drives the router with no bindings.
  const logger = createLogger(loggerEnvFromBindings(c.env ?? {}), "request");
  if (status >= 500) logger.error("request failed", fields);
  else if (status >= 400) logger.warn("request rejected", fields);
  else logger.info("request completed", fields);
};

/** The route pattern Hono matched (upstream middleware/observability.ts). */
function matchedRoute(c: AppContext): string {
  const routes = matchedRoutes(c);
  for (let index = routes.length - 1; index >= 0; index -= 1) {
    const path = routes[index]?.path;
    if (path && path !== "*" && !path.endsWith("/*")) return path;
  }
  return "unmatched";
}
