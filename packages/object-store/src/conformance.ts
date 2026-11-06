/**
 * The shared conformance suite every TS `ObjectStore` must pass. The cases
 * mirror the Rust crate's `conformance` module except multipart, which the TS
 * read side does not implement.
 */
import { readAll } from "./core.js";
import { sha256Hex } from "./crypto.js";
import { isObjectStoreError } from "./errors.js";
import { joinKey, type ObjectKey } from "./key.js";
import {
  MAX_PRESIGN_TTL_SECONDS,
  META_CONTENT_SHA256,
  META_TRACE_ID,
  type ByteRange,
  type ObjectStore,
} from "./types.js";

export const CASES = [
  "put_get_head_metadata",
  "not_found",
  "create_is_idempotent_for_same_content",
  "create_conflicts_on_different_content",
  "if_match_compare_and_swap",
  "if_match_concurrent_single_winner",
  "content_sha256_verified_before_upload",
  "range_get",
  "list_pagination",
  "presign_get",
  "delete_is_idempotent",
] as const;
export type CaseName = (typeof CASES)[number];

export interface SuiteOptions {
  /** Unique per run. */
  prefix: ObjectKey;
  /** The page size the store was built with. */
  pageSize: number;
  /** Fetch a presigned URL's body (HTTP for clouds, file read for fs). */
  fetchPresigned: (url: URL) => Promise<Uint8Array>;
  /** Documented emulator gaps: reported as skipped, never passed. */
  skip?: Partial<Record<CaseName, string>>;
}

export type CaseOutcome =
  | { name: CaseName; status: "passed" }
  | { name: CaseName; status: "skipped"; reason: string }
  | { name: CaseName; status: "failed"; error: string };

class CaseFailure extends Error {}

function ensure(cond: unknown, message: string): asserts cond {
  if (!cond) throw new CaseFailure(message);
}

async function expectCode(
  promise: Promise<unknown>,
  code: string,
  what: string,
) {
  try {
    await promise;
  } catch (err) {
    ensure(
      isObjectStoreError(err) && err.code === code,
      `${what}: expected ${code}, got ${String(err)}`,
    );
    return;
  }
  throw new CaseFailure(`${what}: expected ${code}, but it succeeded`);
}

function random(len: number): Uint8Array {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i += 65536)
    crypto.getRandomValues(out.subarray(i, Math.min(len, i + 65536)));
  return out;
}

const text = (s: string) => new TextEncoder().encode(s);
const equal = (a: Uint8Array, b: Uint8Array) =>
  a.length === b.length && a.every((v, i) => v === b[i]);

type Case = (
  store: ObjectStore,
  base: ObjectKey,
  opts: SuiteOptions,
) => Promise<void>;

const cases: Record<CaseName, Case> = {
  async delete_is_idempotent(store, base) {
    const key = joinKey(base, "delete/object.bin");
    await store.put(key, text("gone soon"), {
      ifNoneMatch: true,
      metadata: { "trace-id": "t" },
    });
    ensure((await store.head(key)) !== undefined, "object not stored");
    await store.delete(key);
    ensure((await store.head(key)) === undefined, "object survived delete");
    await store.delete(key);
    await store.put(key, text("different"), { ifNoneMatch: true });
    ensure(
      (await store.head(key))?.metadata["trace-id"] === undefined,
      "delete left the old object's metadata behind",
    );
  },
  async put_get_head_metadata(store, base) {
    const k = joinKey(base, "chunk.jsonl.zst");
    const body = random(512);
    const trace = "0192f0c4-7f00-7000-8000-00000000abcd";
    const ack = await store.put(k, body, {
      ifNoneMatch: true,
      metadata: { [META_TRACE_ID]: trace },
    });
    ensure(
      ack.created && ack.size === body.length,
      `ack ${JSON.stringify(ack)}`,
    );
    ensure(ack.contentSha256 === (await sha256Hex(body)), "ack digest");
    const meta = await store.head(k);
    ensure(meta, "head returned undefined after put");
    ensure(meta.size === body.length, `head size ${meta.size}`);
    ensure(
      meta.metadata[META_TRACE_ID] === trace,
      `trace-id metadata: ${JSON.stringify(meta.metadata)}`,
    );
    ensure(
      meta.metadata[META_CONTENT_SHA256] === ack.contentSha256,
      "content-sha256 metadata",
    );
    ensure(equal(await readAll(await store.get(k)), body), "body round trip");
  },

  async not_found(store, base) {
    const k = joinKey(base, "missing");
    ensure(
      (await store.head(k)) === undefined,
      "head of missing must be undefined",
    );
    await expectCode(store.get(k), "not_found", "get missing");
  },

  async create_is_idempotent_for_same_content(store, base) {
    const k = joinKey(base, "0000000001-0-abcdef12.jsonl.zst");
    const body = random(4096);
    const first = await store.put(k, body, { ifNoneMatch: true });
    const again = await store.put(k, body, { ifNoneMatch: true });
    ensure(first.created && !again.created, "created flags");
    ensure(first.contentSha256 === again.contentSha256, "digest drift");
  },

  async create_conflicts_on_different_content(store, base) {
    const k = joinKey(base, "object");
    await store.put(k, random(64), { ifNoneMatch: true });
    // Same length, different bytes.
    await expectCode(
      store.put(k, random(64), { ifNoneMatch: true }),
      "already_exists",
      "conflicting create",
    );
  },

  async if_match_compare_and_swap(store, base) {
    const k = joinKey(base, "manifest.json");
    const v1 = await store.put(k, text('{"v":1}'), { ifNoneMatch: true });
    // The protocol's read: body and version from the same response.
    const current = await store.getObject(k);
    await readAll(current.body);
    ensure(
      current.meta.version.eTag || current.meta.version.version,
      "getObject returned no version",
    );
    ensure(
      current.meta.metadata[META_CONTENT_SHA256] === v1.contentSha256,
      `getObject metadata ${JSON.stringify(current.meta.metadata)}`,
    );
    const v2 = await store.put(k, text('{"v":2}'), {
      ifMatch: current.meta.version,
    });
    await expectCode(
      store.put(k, text('{"v":3}'), { ifMatch: v1.version }),
      "precondition_failed",
      "stale CAS",
    );
    await store.put(k, text('{"v":3}'), { ifMatch: v2.version });
    ensure(
      equal(await readAll(await store.get(k)), text('{"v":3}')),
      "final body",
    );
    await expectCode(
      store.put(joinKey(base, "absent.json"), text("{}"), {
        ifMatch: v1.version,
      }),
      "precondition_failed",
      "CAS on missing object",
    );
  },

  async if_match_concurrent_single_winner(store, base) {
    const k = joinKey(base, "manifest.json");
    const v1 = await store.put(k, text('{"v":1}'), { ifNoneMatch: true });
    const results = await Promise.allSettled(
      Array.from({ length: 8 }, (_, i) =>
        store.put(k, text(`{"writer":${i}}`), { ifMatch: v1.version }),
      ),
    );
    const winners = results.filter((r) => r.status === "fulfilled").length;
    const unexpected = results.filter(
      (r) =>
        r.status === "rejected" &&
        !isObjectStoreError(r.reason, "precondition_failed"),
    );
    ensure(
      unexpected.length === 0,
      `unexpected errors: ${unexpected.map((r) => String((r as PromiseRejectedResult).reason))}`,
    );
    ensure(winners === 1, `${winners} of 8 racing CAS writes succeeded`);
  },

  async content_sha256_verified_before_upload(store, base) {
    const k = joinKey(base, "object");
    await expectCode(
      store.put(k, text("payload"), {
        ifNoneMatch: true,
        contentSha256: await sha256Hex("other"),
      }),
      "content_mismatch",
      "mismatched digest",
    );
    ensure(
      (await store.head(k)) === undefined,
      "mismatched body must not be written",
    );
    await store.put(k, text("payload"), {
      ifNoneMatch: true,
      contentSha256: (await sha256Hex("payload")).toUpperCase(),
    });
  },

  async range_get(store, base) {
    const k = joinKey(base, "object");
    const body = random(10_000);
    await store.put(k, body, { ifNoneMatch: true });
    const ranges: ByteRange[] = [
      { start: 0, end: 1 },
      { start: 10, end: 20 },
      { start: 9000, end: 10000 },
      { start: 4096, end: 8192 },
    ];
    for (const r of ranges) {
      const got = await readAll(await store.get(k, r));
      ensure(
        equal(got, body.subarray(r.start, r.end)),
        `range [${r.start},${r.end}) returned ${got.length} bytes`,
      );
    }
    try {
      await store.get(k, { start: 5, end: 5 });
      throw new CaseFailure("empty range accepted");
    } catch (err) {
      ensure(
        isObjectStoreError(err, "invalid_argument"),
        `empty range: ${String(err)}`,
      );
    }
  },

  async list_pagination(store, base, opts) {
    const dir = joinKey(base, "raw");
    const count = opts.pageSize * 2 + 1;
    const expected: string[] = [];
    for (let seq = 0; seq < count; seq++) {
      const k = joinKey(
        dir,
        `${String(seq).padStart(10, "0")}-0-00000000.jsonl.zst`,
      );
      await store.put(k, random(16), { ifNoneMatch: true });
      expected.push(k);
    }
    // Shares the string prefix `raw` but not the `raw/` segment.
    await store.put(joinKey(base, "raw-sibling"), random(1), {
      ifNoneMatch: true,
    });
    const seen: string[] = [];
    let cursor: string | undefined;
    let pages = 0;
    do {
      const page = await store.list(dir, cursor);
      pages++;
      ensure(
        page.items.length <= opts.pageSize,
        `page of ${page.items.length} > ${opts.pageSize}`,
      );
      seen.push(...page.items.map((m) => m.key));
      cursor = page.nextCursor;
      ensure(pages <= count + 1, "pagination does not terminate");
    } while (cursor);
    ensure(
      JSON.stringify(seen) === JSON.stringify(expected),
      `listed ${seen.join(",")}`,
    );
    ensure(
      pages >= 3,
      `expected >= 3 pages at page size ${opts.pageSize}, got ${pages}`,
    );
  },

  async presign_get(store, base, opts) {
    const k = joinKey(base, "object.json");
    const body = text('{"presigned":true}');
    await store.put(k, body, { ifNoneMatch: true });
    const url = await store.presignGet(k, 300);
    ensure(
      equal(await opts.fetchPresigned(url), body),
      "presigned body mismatch",
    );
    await expectCode(store.presignGet(k, 0), "invalid_argument", "zero ttl");
    await expectCode(
      store.presignGet(k, MAX_PRESIGN_TTL_SECONDS + 1),
      "invalid_argument",
      "ttl > max",
    );
  },
};

/** Run every case; returns per-case outcomes (never throws for a case failure). */
export async function runSuite(
  store: ObjectStore,
  opts: SuiteOptions,
): Promise<CaseOutcome[]> {
  const out: CaseOutcome[] = [];
  for (const name of CASES) {
    const reason = opts.skip?.[name];
    if (reason) {
      out.push({ name, status: "skipped", reason });
      continue;
    }
    try {
      await cases[name](store, joinKey(opts.prefix, name), opts);
      out.push({ name, status: "passed" });
    } catch (err) {
      out.push({
        name,
        status: "failed",
        error: err instanceof Error ? `${err.message}` : String(err),
      });
    }
  }
  return out;
}
