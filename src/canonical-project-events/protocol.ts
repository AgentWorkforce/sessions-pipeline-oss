/** F10 metadata-only authority contract. Queue bodies retain Cloud's current ABI.
 * A queue message is never tenant, completion, or publication authority itself.
 */
export interface ProjectEventScope { orgId: string; workspaceId: string }
export interface CanonicalProjectEvent {
  eventId: string;
  kind: "session.revised";
  generation: string;
  schemaGeneration: string;
  identity: ProjectEventScope & { source: string; sessionId: string; userId: string | null };
  authorityId: string;
  sourceEpoch: string;
  sourceFingerprint: string;
  publicationKind: "publish" | "retire";
}
export interface ProjectEventClaim {
  event: CanonicalProjectEvent;
  leaseToken: string;
  leaseVersion: string;
}
export type CloudProjectEventMessage = { kind: "session.revised"; eventId: string; generation: string };

export class ProjectEventProtocolError extends Error {
  readonly code = "canonical_project_event_invalid";
  constructor() { super("Canonical project event authority contract refused"); }
}
const fail = (): never => { throw new ProjectEventProtocolError(); };
const uuid = (v: unknown): v is string => typeof v === "string" && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(v);
const sha = (v: unknown): v is string => typeof v === "string" && /^[0-9a-f]{64}$/.test(v);
export function positiveBigint(v: unknown): v is string {
  return typeof v === "string" && /^[1-9][0-9]{0,18}$/.test(v) && BigInt(v) <= 9223372036854775807n;
}
const object = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
function keys(v: unknown, expected: readonly string[]): asserts v is Record<string, unknown> {
  if (!object(v) || Object.keys(v).length !== expected.length || expected.some(k => !Object.hasOwn(v, k))) fail();
}
export function assertProjectEventScope(v: unknown): asserts v is ProjectEventScope {
  keys(v, ["orgId", "workspaceId"]);
  if (!uuid(v.orgId) || !uuid(v.workspaceId)) fail();
}
export function assertProjectEventClaim(value: unknown, schemaGeneration: string, scope: ProjectEventScope): asserts value is ProjectEventClaim {
  assertProjectEventScope(scope);
  if (!sha(schemaGeneration)) fail();
  keys(value, ["event", "leaseToken", "leaseVersion"]);
  if (!uuid(value.leaseToken) || !positiveBigint(value.leaseVersion)) fail();
  const e = value.event;
  keys(e, ["eventId", "kind", "generation", "schemaGeneration", "identity", "authorityId", "sourceEpoch", "sourceFingerprint", "publicationKind"]);
  keys(e.identity, ["orgId", "workspaceId", "source", "sessionId", "userId"]);
  const id = e.identity;
  if (!uuid(e.eventId) || e.kind !== "session.revised" ||
      !positiveBigint(e.generation) || e.schemaGeneration !== schemaGeneration ||
      !uuid(e.authorityId) || !positiveBigint(e.sourceEpoch) || !sha(e.sourceFingerprint) || !["publish", "retire"].includes(String(e.publicationKind)) ||
      id.orgId !== scope.orgId || id.workspaceId !== scope.workspaceId ||
      !(id.userId === null || uuid(id.userId)) ||
      typeof id.source !== "string" || id.source.length < 1 || id.source.length > 128 ||
      typeof id.sessionId !== "string" || id.sessionId.length < 1 || id.sessionId.length > 512 ||
      /[\u0000-\u001f\u007f]/.test(id.source + id.sessionId)) fail();
}
/** Explicitly select only the ID fields. Never spread durable metadata into transport. */
export function cloudProjectEventMessage(event: CanonicalProjectEvent): CloudProjectEventMessage {
  return { kind: "session.revised", eventId: event.eventId, generation: event.generation };
}
