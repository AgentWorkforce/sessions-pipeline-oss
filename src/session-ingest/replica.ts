/**
 * The optional Neon replica, for the transition only (design section 3).
 *
 * Neon still serves every read route the read API (#135) does not, so while
 * those reads depend on it, each batch the ingest accepts is also queued for
 * Neon as relayhistory's own `NeonReplicaMessage` (relayhistory-cloud
 * packages/relayhistory/src/lib/write-path/neon-replica.ts): batch identity,
 * receipt, and per record where its line sits plus the revision id and digest
 * a line does not hold. No transcript content: the consumer reads the chunks
 * back from S3. The ingest Worker sends it to relayhistory's own
 * `relayhistory-neon-replica` Cloudflare Queue (a producer binding in the same
 * account), and relayhistory's existing consumer applies it
 * (`accept_delivery_batch`, idempotent by batch digest), so Neon's locks are
 * paid by that consumer, never by this request.
 *
 * Off the request path in every sense that matters: the request waits for one
 * queue send, never for Neon. With the replica `required` (the default while
 * Neon serves reads) a failed send is a 503 and the client's retry re-sends;
 * `best_effort` logs and answers 200. Dropping the replica is unbinding
 * NEON_REPLICA from the ingest Worker.
 *
 * A message over the queue budget carries its per-record list in a create-only
 * descriptor beside the batch marker, as relayhistory does. The descriptor
 * holds revision ids, which are client ids; it is written only when the
 * scrubber leaves every id unchanged, so nothing at rest in S3 escapes the
 * scrubber. Otherwise (ids the scrubber would rewrite, in a batch too large
 * for one message: rare) the replica is skipped and counted; S3 holds the
 * batch, and Neon converges when the read routes move off it.
 */
import { createHash } from "node:crypto";
import { scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";
import type { PreparedDelivery } from "./vendor/delivery/prepare.ts";
import type { LedgerTenant } from "./ledger.ts";
import type { WriteResult } from "./writer.ts";
import { layoutComponent } from "../shared/layout-component.ts";

/** Cloudflare Queues refuse messages over 128 KB, and the consumer re-queues there. */
export const MAX_REPLICA_MESSAGE_BYTES = 120_000;

export interface NeonReplicaMessage {
  v: 1;
  kind: "neon-replica";
  auth: { orgId: string; workspaceId: string; userId: string };
  originId: string;
  batchId: string;
  batchDigest: string;
  receipt: PreparedDelivery["receipt"];
  parts: { key: string; source: string; sessionId: string; lines: number; sha256: string }[];
  records: [number, number, string, string, string, number][];
  fence: { resets: number };
  enqueuedAt: string;
  descriptor?: { key: string; sha256: string };
  /**
   * A staged batch (staging.ts): `parts` is empty and each record's [part,
   * line] is [group, line] in this staged object, which the consumer reads
   * instead of chunks. Its lines are scrubbed, like every chunk.
   */
  staged?: { key: string; groups: number };
  /** The writer: record ids in stored lines may be the scrubbed form of the message's. */
  writer?: "96bulls-session-ingest";
}

export type ReplicaMode = "off" | "required" | "best_effort";

export interface ReplicaSink {
  mode: ReplicaMode;
  send(message: NeonReplicaMessage): Promise<void>;
  /** Create-only put of a descriptor body (S3, beside the batch marker). */
  putDescriptor(key: string, body: Uint8Array): Promise<void>;
}

const encoder = new TextEncoder();
const sha256Hex = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
const bytes = (message: NeonReplicaMessage) => encoder.encode(JSON.stringify(message)).length;

/** relayhistory's `batchMarkerKey`, so the descriptor lands where its consumer expects one. */
export function batchMarkerKey(keyRoot: string, tenant: LedgerTenant, originId: string, batchId: string): string {
  const ws = layoutComponent(tenant.workspaceId);
  return `${keyRoot}/${tenant.orgId}/live-batches/v1/ws=${ws}/o=${sha256Hex(originId).slice(0, 32)}/b=${sha256Hex(batchId).slice(0, 32)}.json`;
}

export function replicaMessage(tenant: LedgerTenant, prepared: PreparedDelivery, written: WriteResult, resets: number, now: Date, recordsIndex: number[]): NeonReplicaMessage {
  return {
    v: 1,
    kind: "neon-replica",
    auth: { orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId },
    originId: prepared.originId,
    batchId: prepared.batchId,
    batchDigest: prepared.batchDigest,
    receipt: prepared.receipt,
    parts: written.parts.map((p) => ({ key: p.chunkKey, source: p.source, sessionId: p.sessionId, lines: p.lines, sha256: p.sha256 })),
    // `recordsIndex[i]` is the prepared record written at position i (deleted sessions are not written).
    records: recordsIndex.map((r, i) => {
      const record = prepared.records[r]!;
      const at = written.placed[i]!;
      return [at.part, at.line, record.revision_id, record.digest, record.record_id, record.revision];
    }),
    fence: { resets },
    enqueuedAt: now.toISOString(),
    writer: "96bulls-session-ingest",
  };
}

/** The replica of a staged batch: placements are [group, line] in the staged object. */
export function stagedReplicaMessage(
  tenant: LedgerTenant,
  prepared: PreparedDelivery,
  staged: { key: string; groups: number; index: number[][] },
  resets: number,
  now: Date,
  recordsIndex: number[],
): NeonReplicaMessage {
  const records: NeonReplicaMessage["records"] = [];
  staged.index.forEach((lines, group) =>
    lines.forEach((keepIndex, line) => {
      const record = prepared.records[recordsIndex[keepIndex]!]!;
      records.push([group, line, record.revision_id, record.digest, record.record_id, record.revision]);
    }),
  );
  return {
    v: 1,
    kind: "neon-replica",
    auth: { orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId },
    originId: prepared.originId,
    batchId: prepared.batchId,
    batchDigest: prepared.batchDigest,
    receipt: prepared.receipt,
    parts: [],
    records,
    fence: { resets },
    enqueuedAt: now.toISOString(),
    staged: { key: staged.key, groups: staged.groups },
    writer: "96bulls-session-ingest",
  };
}

/** Whether every id a descriptor would hold at rest is one the scrubber leaves unchanged. */
export function idsScrubIdentity(prepared: PreparedDelivery): boolean {
  const line = encoder.encode(JSON.stringify([prepared.batchId, prepared.originId, ...prepared.receipt.acceptedRevisionIds, ...prepared.records.map((r) => r.record_id)]) + "\n");
  const scrubbed = scrubRecords(line).scrubbed;
  return scrubbed.byteLength === line.byteLength && scrubbed.every((b, i) => b === line[i]);
}

export type ReplicaOutcome = "off" | "sent" | "sent_descriptor" | "skipped_ids_not_identity" | "failed";

/** Queue the replica message. Throws only in `required` mode. */
export async function sendReplica(sink: ReplicaSink | undefined, message: NeonReplicaMessage, prepared: PreparedDelivery, markerKey: string): Promise<ReplicaOutcome> {
  if (!sink || sink.mode === "off") return "off";
  try {
    if (bytes(message) <= MAX_REPLICA_MESSAGE_BYTES) {
      await sink.send(message);
      return "sent";
    }
    if (!idsScrubIdentity(prepared)) return "skipped_ids_not_identity";
    const body = encoder.encode(JSON.stringify({ v: 1, parts: message.parts, records: message.records, receipt: message.receipt }));
    const key = markerKey.replace(/\.json$/, ".replica.json");
    await sink.putDescriptor(key, body);
    const pointer: NeonReplicaMessage = {
      ...message,
      parts: [],
      records: [],
      receipt: { ...message.receipt, acceptedRevisionIds: [], unsupportedRevisionIds: [] },
      descriptor: { key, sha256: sha256Hex(body) },
    };
    await sink.send(pointer);
    return "sent_descriptor";
  } catch (error) {
    if (sink.mode === "required") throw error;
    return "failed";
  }
}
