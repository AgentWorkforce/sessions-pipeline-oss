// Copied verbatim from AgentWorkforce/relayhistory-cloud packages/relayhistory/src/auth/analytics-identity.ts at 6f6d568849c483be8e3a2a5aaeba3da1d4b47de9 (96BullSessions stand-in; see MOVE_NOTES.md).
import type { AuthContext } from "../env.js";

/** Minimal server-owned attribution retained through opaque-token rotation. No PII. */
export interface AnalyticsIdentity {
  version: 2;
  userId: string;
  organizationId: string;
  workspaceId: string;
  actorKind: "human" | "agent" | "service";
  actorId: string;
  source: "cloud_whoami" | "relayauth_assertion" | "cloud_service_binding";
}

function id(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 256 &&
    value === value.trim() &&
    !/[\u0000-\u001f\u007f]/.test(value)
  );
}

/** Project persisted data through a closed vocabulary, including legacy/null rows. */
export function analyticsIdentity(
  value: unknown,
): AnalyticsIdentity | undefined {
  if (!value || typeof value !== "object") return undefined;
  const v = value as Record<string, unknown>;
  if (
    v.version !== 2 ||
    !id(v.userId) ||
    !id(v.organizationId) ||
    !id(v.workspaceId) ||
    !id(v.actorId) ||
    !["human", "agent", "service"].includes(String(v.actorKind)) ||
    !["cloud_whoami", "relayauth_assertion", "cloud_service_binding"].includes(
      String(v.source),
    )
  )
    return undefined;
  return {
    version: 2,
    userId: v.userId,
    organizationId: v.organizationId,
    workspaceId: v.workspaceId,
    actorKind: v.actorKind as AnalyticsIdentity["actorKind"],
    actorId: v.actorId,
    source: v.source as AnalyticsIdentity["source"],
  };
}

/** Only call after the JWT's signature, issuer, audience and time checks succeed. */
export function analyticsFromVerifiedAssertion(
  auth: AuthContext,
): AnalyticsIdentity | undefined {
  const claims = auth.claims;
  const audience = claims.aud;
  // `historyOrigin` is set only when the verifier accepted the assertion
  // because its audience is this History's own origin.
  if (
    !auth.historyOrigin &&
    !(
      audience === "relayhistory" ||
      (Array.isArray(audience) && audience.includes("relayhistory"))
    )
  )
    return undefined;
  const metadata = claims.meta;
  if (!metadata || typeof metadata !== "object") return undefined;
  const m = metadata as Record<string, unknown>;
  // RelayAuth's dedicated API-key assertion endpoint binds these exact Cloud
  // IDs. Ordinary agent metadata and generic sponsored tokens do not qualify.
  if (
    m.tokenClass !== "relayhistory_assertion" ||
    auth.tokenSubject !== "agent_relayhistory_assertion"
  )
    return undefined;
  const canonical = analyticsIdentity({
    version: 2,
    userId: claims.sponsorId,
    organizationId: claims.org,
    workspaceId: claims.wks,
    actorKind: "agent",
    actorId: `relayauth:${auth.tokenSubject}`,
    source: "relayauth_assertion",
  });
  const local = (value: string) =>
    value.startsWith("relay_") ? value.slice(6) : value;
  return canonical &&
    local(canonical.userId) === auth.userId &&
    local(canonical.organizationId) === auth.orgId &&
    local(canonical.workspaceId) === auth.workspaceId
    ? canonical
    : undefined;
}
