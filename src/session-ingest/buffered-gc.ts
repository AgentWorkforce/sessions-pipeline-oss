/** Buffered-byte erasure: preserve acknowledged groups, retire before cleanup,
 * retain a current redaction object, and permanently remove only prior versions.
 * All object writes have a registered SQL intent. No canonical publication.
 */
import { createHash } from 'node:crypto';
import { zstdDecompressSync } from 'node:zlib';
import { objectKey, isObjectStoreError, type ObjectStore } from '../shared/vendor.ts';
import type { PreservedVersionEraser } from '../migration/neon-s3-erasure/eraser.ts';
import { bufferedOrdinalSelection, type BufferedGcTicket, type BufferedStageSpec } from './buffered-protocol.ts';
import { BUFFER_STAGE_MAX_BYTES, readBufferedStage, prepareBufferedReplacement, putBufferedStage } from './staging.ts';
const sha = (b: Uint8Array) => createHash('sha256').update(b).digest('hex');
const uuid = (s: unknown): s is string => typeof s === 'string' && /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(s);
const obj = (v: unknown): v is Record<string, any> => v !== null && typeof v === 'object' && !Array.isArray(v);
export class BufferedGcError extends Error {
  constructor(readonly code: string) { super('buffer gc refused: ' + code); }
}
function no(code: string): never { throw new BufferedGcError(code); }
export function assertBufferedSpec(v: unknown): asserts v is BufferedStageSpec {
  if (!obj(v) || typeof v.key !== 'string' || !/^96bulls\/[A-Za-z0-9._-]{1,128}\/buffered-batches\/v1\/ws=[^/]+\/m=srv_[0-9a-f]{32}\/b=[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{32}\.staged\.ndjson\.zst$/.test(v.key) || v.key.split('/').some((s: string) => s === '.' || s === '..') || !/^[0-9a-f]{64}$/.test(v.sha256) || !Number.isSafeInteger(v.bytes) || v.bytes < 1 || v.bytes > BUFFER_STAGE_MAX_BYTES || !Array.isArray(v.groupOrdinals) || !v.groupOrdinals.length || v.groupOrdinals.length > 500) no('stage_spec');
  const seen = new Set<number>();
  for (const group of v.groupOrdinals) {
    if (!Array.isArray(group) || !group.length) no('stage_ordinals');
    for (const index of group) {
      if (!Number.isInteger(index) || index < 0 || index > 999 || seen.has(index)) no('stage_ordinals');
      seen.add(index);
    }
  }
  if (seen.size > 500) no('stage_ordinals');
}
export function assertBufferedTicket(v: unknown): asserts v is BufferedGcTicket {
  assertBufferedSpec(v);
  const t = v as unknown as BufferedGcTicket;
  if (!uuid(t.stageToken) || !uuid(t.claimToken) || !uuid(t.leaseToken) || !/^\d+$/.test(t.rowVersion) || !/^\d+$/.test(t.resetEpoch) || !['preparing', 'acknowledged'].includes(t.receiptState) || !['plan', 'cleanup'].includes(t.phase) || !['live', 'retired', 'redacted'].includes(t.cleanupState) || !Array.isArray(t.keepGroupIndexes)) no('ticket');
  try { bufferedOrdinalSelection(t.groupOrdinals, t.keepOrdinals, t.keepGroupIndexes); }
  catch { no('ticket_keep'); }
  const recordCount = t.groupOrdinals.reduce((n, group) => n + group.length, 0);
  if (t.phase === 'plan' && (t.receiptState !== 'acknowledged' || t.keepOrdinals.length === recordCount)) no('ticket_plan');
  if (t.phase === 'cleanup' && t.keepOrdinals.length !== 0) no('ticket_cleanup');
  if (t.replacement !== undefined) { assertBufferedSpec(t.replacement); if (!uuid(t.replacement.stageToken)) no('ticket_replacement'); }
}
export interface BufferedGcSql { query(sql: string, params: unknown[]): Promise<{ rows: Record<string, unknown>[] }> }
export interface BufferedGcLedger {
  claim(limit: number): Promise<BufferedGcTicket[]>;
  prepare(ticket: BufferedGcTicket, spec: BufferedStageSpec): Promise<string>;
  swap(ticket: BufferedGcTicket, replacementStageToken: string | null): Promise<boolean>;
  finish(ticket: BufferedGcTicket, barrierVersionId: string): Promise<boolean>;
}
export function sqlBufferedGcLedger(db: BufferedGcSql,expectedGeneration:string): BufferedGcLedger {
  if(!/^[a-f0-9]{64}$/.test(expectedGeneration))no("generation_required");
  const call = async (name: string, p: unknown): Promise<Record<string, any>> => {
    let value: unknown;
    try { value = (await db.query(`SELECT sessions.${name}($1::jsonb) AS result`, [JSON.stringify(p)])).rows[0]?.result; }
    catch (e) { const code = (e as { code?: unknown })?.code; return no(typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code) ? 'sql_' + code : 'sql_failed'); }
    if (!obj(value)) no('sql_result');
    return value;
  };
  const key = (t: BufferedGcTicket) => ({ stageToken: t.stageToken, leaseToken: t.leaseToken });
  return {
    async claim(limit) {
      if (!Number.isInteger(limit) || limit < 1 || limit > 100) no('claim_limit');
      const r = await call('ingest_buffer_gc_claim', { limit,generation:expectedGeneration });
      if (!Array.isArray(r.tickets) || r.tickets.length > limit) no('sql_tickets');
      r.tickets.forEach(assertBufferedTicket);
      return r.tickets;
    },
    async prepare(t, spec) {
      assertBufferedSpec(spec);
      const r = await call('ingest_buffer_gc_prepare', { ...key(t), ...spec });
      if (!uuid(r.replacementStageToken) || r.key !== spec.key || r.sha256 !== spec.sha256 || r.bytes !== spec.bytes || JSON.stringify(r.groupOrdinals) !== JSON.stringify(spec.groupOrdinals)) no('sql_replacement');
      if (t.replacement && r.replacementStageToken !== t.replacement.stageToken) no('sql_replacement');
      return r.replacementStageToken;
    },
    async swap(t, replacementStageToken) {
      const r = await call('ingest_buffer_gc_swap', { ...key(t), replacementStageToken });
      if (typeof r.swapped !== 'boolean') no('sql_swap'); return r.swapped;
    },
    async finish(t, barrierVersionId) {
      const r = await call('ingest_buffer_gc_finish', { ...key(t), barrierVersionId });
      if (typeof r.finished !== 'boolean') no('sql_finish'); return r.finished;
    },
  };
}
async function boundedBody(body: ReadableStream<Uint8Array>, max: number): Promise<Uint8Array> {
  const reader = body.getReader(), parts: Uint8Array[] = []; let size = 0;
  try { for (;;) { const r = await reader.read(); if (r.done) break; size += r.value.byteLength; if (size > max) no('object_bound'); parts.push(r.value); } }
  finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  const all = new Uint8Array(size); let at = 0; for (const p of parts) { all.set(p, at); at += p.byteLength; } return all;
}
/** All workers converge on one exact, persistent non-delete object version. */
export async function ensureBufferedBarrier(store: ObjectStore, t: BufferedGcTicket): Promise<string> {
  assertBufferedTicket(t);
  if (t.phase !== 'cleanup' || !['retired', 'redacted'].includes(t.cleanupState)) no('barrier_before_retirement');
  const bytes = new TextEncoder().encode(JSON.stringify({ v: 1, kind: 'buffered-redaction', stage_token: t.stageToken, claim_token: t.claimToken, original_sha256: t.sha256 }));
  const expected = sha(bytes);
  for (let n = 0; n < 8; n++) {
    let current, deniedRead: unknown;
    try { current = await store.getObject(objectKey(t.key)); }
    catch (e) {
      // S3 without broad ListBucket may report 403 for a missing key. A
      // conditional create can prove absence without overwriting unreadable data.
      if (isObjectStoreError(e, 'unauthorized') && e.status === 403) deniedRead = e;
      else if (!isObjectStoreError(e, 'not_found')) throw e;
    }
    if (current) {
      const body = await boundedBody(current.body, Math.max(t.bytes, bytes.byteLength));
      const digest = sha(body);
      if (body.byteLength === bytes.byteLength && digest === expected) {
        const version = current.meta.version.version;
        if (typeof version !== 'string' || !version || version === 'null') no('barrier_version_unavailable');
        return version;
      }
      if (body.byteLength !== t.bytes || digest !== t.sha256) no('barrier_unexpected_content');
    }
    try { await store.put(objectKey(t.key), bytes, current ? { ifMatch: current.meta.version, contentType: 'application/json' } : { ifNoneMatch: true, contentType: 'application/json' }); }
    catch (e) {
      if (!isObjectStoreError(e, 'already_exists') && !isObjectStoreError(e, 'precondition_failed')) throw e;
      if (deniedRead) throw deniedRead; // Existing unreadable object remains untouched.
    }
  }
  return no('barrier_contended');
}
export interface BufferedGcDeps { ledger: BufferedGcLedger; store: ObjectStore; eraser: PreservedVersionEraser; keyRoot: '96bulls' }
export async function collectBufferedTicket(t: BufferedGcTicket, deps: BufferedGcDeps): Promise<'finished' | 'deferred'> {
  assertBufferedTicket(t);
  let cleanup = t;
  if (t.phase === 'plan') {
    let replacement: string | null = null;
    if (t.keepGroupIndexes.length) {
      const decoded = await readBufferedStage(deps.store, t, b => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })));
      const prepared = prepareBufferedReplacement(deps.keyRoot, t, decoded);
      assertBufferedSpec(prepared.spec);
      replacement = await deps.ledger.prepare(t, prepared.spec); // Intent BEFORE object IO.
      await putBufferedStage(deps.store, prepared);
    }
    if (!await deps.ledger.swap(t, replacement)) return 'deferred'; // Current pointer must move BEFORE old cleanup.
    cleanup = { ...t, phase: 'cleanup', cleanupState: 'retired', keepOrdinals: [], keepGroupIndexes: [], replacement: undefined };
  }
  const version = await ensureBufferedBarrier(deps.store, cleanup);
  await deps.eraser.eraseObjectExcept(t.key, version);
  return await deps.ledger.finish(t, version) ? 'finished' : 'deferred';
}
export async function bufferedGcTick(deps: BufferedGcDeps, limit = 10) {
  const tickets = await deps.ledger.claim(limit); let finished = 0, deferred = 0;
  for (const t of tickets) {
    try { if (await collectBufferedTicket(t, deps) === 'finished') finished++; else deferred++; }
    catch (e) { if (e instanceof BufferedGcError && ['sql_40001', 'sql_55P03', 'sql_57014'].includes(e.code)) deferred++; else throw e; }
  }
  return { examined: tickets.length, finished, deferred };
}
