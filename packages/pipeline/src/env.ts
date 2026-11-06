/**
 * Stand-in for relayhistory-cloud packages/relayhistory/src/env.ts.
 *
 * The pipeline framework (PR #210) imports `Bindings`, `AppEnv`, `AppContext` and
 * `AuthContext` from "../env.js". Upstream that file declares every binding of the
 * whole relayhistory Worker; this copy keeps only what the framework reads: the
 * logger's ENVIRONMENT/POSTHOG_* fields, and the PR #210 additions from
 * integration/relayhistory-cloud/packages/relayhistory/src/env.ts.patch verbatim
 * (event store, error tracker, ops secret, telemetry id key, the seven per-stage
 * `Queue<StageMessage>` bindings), and the PR #211 storage-adapter additions
 * (PIPELINE_RAW_*, PIPELINE_ORG_*, PIPELINE_KEY_PREFIX, PIPELINE_RECONCILE_*)
 * verbatim from relayhistory-cloud `git diff f544f6d d9f2ba2 -- env.ts`.
 * `Variables` and `AuthContext` keep the upstream field names; upstream-only
 * auth extras (historyOrigin, sessionId, analyticsIdentity) are left out
 * because nothing here reads them.
 */
import type { Context } from "hono";

// RelayAuth defaults, as upstream env.ts declares them (needed by auth/relayauth.ts).
export const DEFAULT_RELAYAUTH_JWKS_URL =
  "https://relayauth.dev/.well-known/jwks.json";
export const DEFAULT_RELAYAUTH_ISSUER = "https://relayauth.dev";
export const DEFAULT_RELAYAUTH_AUDIENCE = "relayhistory";

export interface AuthContext {
  userId: string;
  orgId: string;
  workspaceId?: string;
  tokenSubject: string;
  scopes: string[];
  claims: Record<string, unknown>;
  /** As upstream env.ts: set when an assertion was accepted for this History's own origin. */
  historyOrigin?: string;
  sessionId?: string;
  /** As upstream env.ts: set only by verified auth boundaries (auth/relayauth.ts). */
  analyticsIdentity?: import("./auth/analytics-identity.js").AnalyticsIdentity;
  /**
   * As upstream env.ts (relayhistory-cloud feat/migration-assertion-ingest):
   * set only for a RelayAuth migration assertion (relayauth#110).
   */
  migration?: import("./lib/pipeline/migration-assertions.js").MigrationAssertionClaims;
}

export interface Bindings {
  // RelayAuth JWT verification (auth/relayauth.ts), as upstream env.ts declares them.
  RELAYAUTH_JWKS_URL?: string;
  RELAYAUTH_ISSUER?: string;
  RELAYAUTH_AUDIENCE?: string;
  RELAYHISTORY_ORIGIN?: string;
  RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS?: string;
  /** As upstream env.ts: the dedicated, default-off migration-assertion switch. */
  RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS?: string;
  /** The v2 pre-mint base read's own default-off activation gate. */
  RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ?: string;
  /** As upstream env.ts: accepted grants, comma-separated `grantId=planDigest`. */
  RELAYHISTORY_MIGRATION_GRANTS?: string;
  ALLOW_INTERIM_RELAYAUTH_BEARER?: string;
  ENVIRONMENT?: string;
  // Request logs leave the Worker only when the key is set (see lib/logger.ts).
  POSTHOG_API_KEY?: string;
  POSTHOG_HOST?: string;
  // Session pipeline observability (see lib/pipeline and docs/pipeline-observability.md).
  // `PIPELINE_EVENTS_URL` is any PostgREST base URL exposing `pipeline_events` and
  // `pipeline_dead_letters`; with no URL/token, stage events are log lines only.
  PIPELINE_EVENTS_URL?: string;
  PIPELINE_EVENTS_TOKEN?: string;
  PIPELINE_EVENTS_SCHEMA?: string;
  /** `otlp` (default; console only until an endpoint is set) | `sentry` | `console` | `none`. */
  PIPELINE_ERROR_TRACKER?: string;
  OTEL_EXPORTER_OTLP_ENDPOINT?: string;
  OTEL_EXPORTER_OTLP_HEADERS?: string;
  SENTRY_DSN?: string;
  /** Bearer for the operator-only dead letter replay route. Unset: route answers 503. */
  PIPELINE_OPS_SECRET?: string;
  /**
   * Secret (32+ chars) keying the HMAC that replaces machine and session ids in
   * pipeline telemetry (lib/pipeline/telemetry-ids.ts). Unset: those ids are
   * left out of telemetry. Never committed; set with `wrangler secret put`.
   */
  PIPELINE_TELEMETRY_ID_KEY?: string;
  PIPELINE_FILE_NORMALIZE_ENABLED?: string;
  PIPELINE_OBJECT_LANDED?: Queue<import("./lib/pipeline/dlq.js").StageMessage>;
  PIPELINE_SESSION_INDEXED?: Queue<
    import("./lib/pipeline/dlq.js").StageMessage
  >;
  PIPELINE_FILE_NORMALIZE?: Queue<import("./lib/pipeline/dlq.js").StageMessage>;
  PIPELINE_SUMMARY_SHORT?: Queue<import("./lib/pipeline/dlq.js").StageMessage>;
  PIPELINE_SUMMARY_LONG?: Queue<import("./lib/pipeline/dlq.js").StageMessage>;
  PIPELINE_STATUS_CLASSIFY?: Queue<
    import("./lib/pipeline/dlq.js").StageMessage
  >;
  PIPELINE_BACKFILL_ACTIVATION?: Queue<
    import("./lib/pipeline/dlq.js").StageMessage
  >;
  PIPELINE_FILE_MAINTENANCE?: Queue<
    import("./lib/pipeline/dlq.js").StageMessage
  >;
  // Storage-notification adapter (lib/pipeline/adapter.ts); inert until set.
  // Unset uses the S3 API (MinIO or R2). PIPELINE_RAW_PROVIDER=gcs uses the
  // GCS adapter and a metadata-server token, never HMAC keys. Credentials are
  // runtime secrets and are never logged.
  PIPELINE_RAW_PROVIDER?: string;
  /** Override for tests. Defaults to the GKE metadata server. Metadata host or loopback only. */
  PIPELINE_RAW_GCS_TOKEN_URI?: string;
  PIPELINE_RAW_S3_BUCKET?: string;
  PIPELINE_RAW_S3_ENDPOINT?: string;
  PIPELINE_RAW_S3_REGION?: string;
  PIPELINE_RAW_S3_ACCESS_KEY_ID?: string;
  PIPELINE_RAW_S3_SECRET_ACCESS_KEY?: string;
  /** `AES256` | `aws:kms`: server-side encryption for stores that need it requested (MinIO + KMS). */
  PIPELINE_RAW_S3_SSE?: string;
  /** Owner of every raw object (single tenant). */
  PIPELINE_ORG_ID?: string;
  /** "true": the org is the last segment of the key's deployment prefix. */
  PIPELINE_ORG_FROM_PREFIX?: string;
  /** Deployment prefix raw keys sit under, when the org is not taken from it. */
  PIPELINE_KEY_PREFIX?: string;
  /** "true" runs the manifest reconcile job from the minute cron. */
  PIPELINE_RECONCILE_ENABLED?: string;
  /** Reconcile look-back window in minutes (default 15). */
  PIPELINE_RECONCILE_LOOKBACK_MINUTES?: string;
}

export interface Variables {
  requestId: string;
  correlationId: string;
  /** Session pipeline trace id from `X-Relay-Trace-Id` / `traceparent`, when well formed. */
  traceId?: string;
  auth?: AuthContext;
}

export interface AppEnv {
  Bindings: Bindings;
  Variables: Variables;
}

export type AppContext = Context<AppEnv>;
