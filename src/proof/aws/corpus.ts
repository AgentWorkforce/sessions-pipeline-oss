/**
 * Synthetic planted sessions for the proof: UUIDv7 ids, at least two tenants,
 * and three kinds of planted text per session.
 *
 *   summaryTerm       in the first user turn, so it lands in the scrubbed
 *                     short_summary and is searchable on V1;
 *   tenantCanary      the same, shared by every session of one tenant; another
 *                     tenant's token must never see it;
 *   transcriptCanary  only in later turns, so it lives in the S3 transcript and
 *                     never in short_summary. V1 search must not find it, and
 *                     no log or trace may carry it.
 *
 * The first user turn also carries a shape-only fake credential: Technical Foul
 * must scrub it before storage, and it must never appear in short_summary.
 */
import { digestRecords, extractiveSummarizer } from "../../bob-costas/summarizer.ts";
import { uuidv7 } from "../../shared/ids.ts";
import { scrubText } from "../../shared/vendor.ts";
export interface Tenant { orgId: string; workspaceId: string; }

/** Shape-only, matches the scrub rule for AWS key ids; not a credential. Same value as concourse/seed.ts. */
export const FAKE_AWS_KEY_ID = "AKIAIOSFODNN7EXAMPLE";

export interface PlantedSession {
  tenant: Tenant;
  machineId: string;
  harness: "claude";
  /** The harness session id (UUIDv7 so the run is time-ordered and unique). */
  sessionId: string;
  sessionUuid: string;
  traceId: string;
  startedAt: string;
  summaryTerm: string;
  tenantCanary: string;
  transcriptCanary: string;
  /** JSONL records per manifest revision; revision 2 appends to revision 1. */
  revisions: string[][];
  /** Object key prefix the ingest derives for this tenant (`<base>/<org>`, PIPELINE_ORG_FROM_PREFIX), or none. */
  keyPrefix?: string;
}

export interface CorpusOptions {
  runId: string;
  sessions: number;
  tenants: readonly Tenant[];
  now?: number;
  /** With a base, keys are `<base>/<org>/v1/...` as the proof stage's ingest writes them. */
  keyPrefixBase?: string;
}

/** Terms are lowercase ASCII with no separators the query normalizer would collapse. */
const token = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, "");

export function runTag(runId: string): string {
  const t = token(runId).slice(-10);
  if (t.length < 6) throw new Error("runId must contain at least 6 alphanumerics");
  return t;
}

export function plantCorpus(o: CorpusOptions): PlantedSession[] {
  if (!Number.isInteger(o.sessions) || o.sessions < 2) throw new RangeError("plant at least 2 sessions");
  if (o.tenants.length < 2) throw new RangeError("tenant isolation needs at least 2 tenants");
  const keys = new Set(o.tenants.map((t) => `${t.orgId}/${t.workspaceId}`));
  if (keys.size !== o.tenants.length) throw new RangeError("tenants must be distinct");
  if (new Set(o.tenants.map((t) => t.orgId)).size !== o.tenants.length) throw new RangeError("each tenant needs its own org");
  const tag = runTag(o.runId);
  const base = o.now ?? Date.now();
  const out: PlantedSession[] = [];
  for (let i = 0; i < o.sessions; i++) {
    const ti = i % o.tenants.length;
    const tenant = o.tenants[ti]!;
    const at = base - 60_000 + i;
    const sessionId = uuidv7(at);
    const p: PlantedSession = {
      tenant,
      machineId: `proof-${tag}-m${ti}`,
      harness: "claude",
      sessionId,
      sessionUuid: uuidv7(at),
      traceId: uuidv7(at),
      startedAt: new Date(at).toISOString(),
      summaryTerm: `zq${tag}t${ti}s${i}sum`,
      tenantCanary: `zq${tag}t${ti}tenantcanary`,
      transcriptCanary: `zq${tag}t${ti}s${i}transcriptonly`,
      revisions: [],
      ...(o.keyPrefixBase ? { keyPrefix: `${o.keyPrefixBase}/${tenant.orgId}` } : {}),
    };
    const first = transcript(p, at, 1);
    p.revisions = [first, [...first, ...transcript(p, at, 2)]];
    out.push(p);
  }
  return out;
}

function transcript(p: PlantedSession, at: number, revision: 1 | 2): string[] {
  const t = (s: number) => new Date(at + s * 1000).toISOString();
  const cwd = "/workspace/proof";
  const base = { isSidechain: false, cwd, sessionId: p.sessionId, version: "2.1.96" };
  if (revision === 1)
    return [
      { ...base, parentUuid: null, type: "user", uuid: "u1", timestamp: t(0), message: { role: "user", content: `Proof session ${p.summaryTerm} for ${p.tenantCanary}. AWS_ACCESS_KEY_ID=${FAKE_AWS_KEY_ID}` } },
      { ...base, parentUuid: "u1", type: "assistant", uuid: "a1", timestamp: t(2), message: { role: "assistant", model: "proof-model", content: [{ type: "text", text: `Noted ${p.transcriptCanary}.` }] } },
      { ...base, parentUuid: "a1", type: "user", uuid: "u2", timestamp: t(3), message: { role: "user", content: `Second turn mentions ${p.transcriptCanary} only in the transcript.` } },
    ].map((r) => JSON.stringify(r));
  return [
    { ...base, parentUuid: "u2", type: "assistant", uuid: "a2", timestamp: t(10), message: { role: "assistant", model: "proof-model", content: [{ type: "text", text: `Revision two ${p.transcriptCanary}.` }] } },
    { ...base, parentUuid: "a2", type: "user", uuid: "u3", timestamp: t(11), message: { role: "user", content: `Third turn ${p.transcriptCanary}.` } },
  ].map((r) => JSON.stringify(r));
}

/**
 * The short_summary the extractive summarizer (BOB_COSTAS_MODEL=off, the
 * self-host default) produces for a revision. The proof compares the directory
 * against this exactly when the deployment runs the extractive summarizer.
 */
export async function expectedShortSummary(p: PlantedSession, revision: 1 | 2): Promise<string> {
  // The ingest scrubs chunks before storage (Technical Foul); Bob Costas reads the scrubbed records.
  const lines = p.revisions[revision - 1]!.map((l) => scrubText(l));
  return (await extractiveSummarizer.summarize("short", digestRecords(p.harness, lines))).text;
}

export const allCanaries = (c: readonly PlantedSession[]) => [
  ...new Set(c.flatMap((p) => [p.summaryTerm, p.tenantCanary, p.transcriptCanary])),
];

/** What must never appear in a log or a trace: transcript text, the fake key, and summary content. */
export const telemetryForbidden = (c: readonly PlantedSession[]) => [...allCanaries(c), FAKE_AWS_KEY_ID];
