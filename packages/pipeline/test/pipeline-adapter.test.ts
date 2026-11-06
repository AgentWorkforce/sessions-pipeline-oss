import { Hono } from "hono";
import { mkdtemp, rm } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { zstdDecompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { computeContentSha256 } from "../../object-store/src/manifest.js";
import { signRequest } from "../../object-store/src/sigv4.js";
import type { AppEnv, AuthContext, Bindings } from "../src/env.js";
import { scrubJson } from "../src/lib/scrub.js";
import {
  buildObjectDeletedMessage,
  buildObjectLandedMessage,
  handleObjectEventQueue,
  isObjectEventsDlq,
  normalizeEtag,
  objectStoreManifestReader,
  orgResolverFromEnv,
  prefixAllowed,
  prefixOrgResolver,
  r2EventSource,
  rawStoreFromEnv,
  staticOrgResolver,
  type ManifestReader,
} from "../src/lib/pipeline/adapter.js";
import {
  PipelineError,
  manifestWorkKey,
  type StageMessage,
} from "../src/lib/pipeline/index.js";
import {
  manifestKey,
  objectKey,
  parseManifest,
  readAll,
  type ChunkEntry,
  type ObjectStore,
  type SessionLocation,
} from "../src/lib/pipeline/object-layout.js";
import {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  scrubRecords,
  scrubStringToken,
  sealRawChunk,
} from "../src/lib/pipeline/raw-chunks.js";
import {
  reconcileListPrefix,
  reconcileManifests,
  runManifestReconcile,
} from "../src/lib/pipeline/reconcile.js";
import {
  createPipelineRoutes,
  type PipelineRouteDeps,
} from "../src/routes/pipeline.js";

/** Rust-produced v1 manifests with their keys (the object-store package's interop fixtures). */
function fixture(name: "rust-manifest-v1" | "rust-manifest-v1-scrubbed") {
  const path = fileURLToPath(
    new URL(`../../object-store/test/fixtures/${name}.json`, import.meta.url)
      .href,
  );
  return JSON.parse(readFileSync(path, "utf8")) as {
    key: string;
    manifest: Record<string, unknown>;
  };
}
const UNSCRUBBED = fixture("rust-manifest-v1");
const SCRUBBED = fixture("rust-manifest-v1-scrubbed");
const ORG = "org-synthetic";
// Synthetic secrets only: shaped like real ones so the scrub rules fire.
const GH_TOKEN = "ghp_123456789012345678901234567890123456";
const EMAIL = "dev@example.com";

let logs: string[] = [];
beforeEach(() => {
  logs = [];
  for (const level of ["info", "warn", "error", "log"] as const)
    vi.spyOn(console, level).mockImplementation((...args) => {
      logs.push(args.map(String).join(" "));
    });
});
afterEach(() => vi.restoreAllMocks());

const manifestOf = (f: { key: string; manifest: unknown }) =>
  parseManifest(f.manifest, f.key);

/** A reader over an in-memory "bucket" keyed by object key. */
function memoryReader(
  objects: Map<string, { manifest: unknown; etag?: string }>,
  bucket = "raw-sessions",
): ManifestReader {
  return {
    provider: "r2",
    bucket,
    async read(key) {
      const object = objects.get(key);
      if (!object) return null;
      return {
        manifest: await parseManifest(object.manifest, key),
        ...(object.etag ? { etag: normalizeEtag(object.etag)! } : {}),
      };
    },
  };
}

function queue() {
  const sent: StageMessage[] = [];
  return {
    sent,
    send: async (m: StageMessage) => void sent.push(structuredClone(m)),
  };
}

function deliver(
  deps: Parameters<typeof handleObjectEventQueue>[1],
  body: unknown,
  queueName = "relayhistory-object-events",
) {
  let outcome: string | undefined;
  return handleObjectEventQueue(
    {
      queue: queueName,
      messages: [
        {
          body,
          attempts: 1,
          ack: () => void (outcome ??= "ack"),
          retry: (o?: { delaySeconds?: number }) =>
            void (outcome ??= `retry:${o?.delaySeconds}`),
        },
      ],
    },
    deps,
  ).then(() => outcome);
}

const r2Event = (key: string, action = "PutObject", eTag = '"abc123"') => ({
  account: "acct",
  action,
  bucket: "raw-sessions",
  object: { key, size: 1234, eTag },
  eventTime: "2026-10-27T07:00:00Z",
});

describe("R2 notifications", () => {
  it("normalizes puts and deletes and ignores everything else", () => {
    expect(r2EventSource.normalize(r2Event(SCRUBBED.key))).toEqual({
      provider: "r2",
      key: SCRUBBED.key,
      bucket: "raw-sessions",
      etag: "abc123",
      size: 1234,
      eventTime: "2026-10-27T07:00:00Z",
    });
    expect(
      r2EventSource.normalize(r2Event(SCRUBBED.key, "LifecycleDeletion")),
    ).toMatchObject({ key: SCRUBBED.key, deleted: true });
    expect(
      r2EventSource.normalize(r2Event(SCRUBBED.key, "SomethingElse")),
    ).toBeNull();
    expect(r2EventSource.normalize(null)).toBeNull();
    expect(r2EventSource.normalize({ action: "PutObject" })).toBeNull();
  });

  it("treats quoted, weak and bare ETags as one", () => {
    expect(normalizeEtag('"abc"')).toBe("abc");
    expect(normalizeEtag('W/"abc"')).toBe("abc");
    expect(normalizeEtag("abc")).toBe("abc");
    expect(normalizeEtag('""')).toBeUndefined();
  });
});

describe("org attribution and the deployment prefix", () => {
  it("resolves the org statically, from the prefix, or not at all", async () => {
    expect(await staticOrgResolver(ORG).orgFor({ workspaceId: "ws_1" })).toBe(
      ORG,
    );
    expect(
      await prefixOrgResolver.orgFor({ workspaceId: "w", prefix: "a/org-x" }),
    ).toBe("org-x");
    expect(orgResolverFromEnv({})).toBeUndefined();
    expect(orgResolverFromEnv({ PIPELINE_ORG_FROM_PREFIX: "true" })?.kind).toBe(
      "prefix",
    );
  });

  it("allows only the caller's prefix", () => {
    expect(prefixAllowed({}, undefined, ORG)).toBe(true);
    expect(prefixAllowed({}, "other", ORG)).toBe(false);
    expect(prefixAllowed({ PIPELINE_KEY_PREFIX: "/raw/" }, "raw", ORG)).toBe(
      true,
    );
    // Org-derived prefixes: exactly `<base>/<caller org>`, or `<caller org>`
    // with no base; never another base, an extra segment or a bare org when a
    // base is configured.
    const orgFromPrefix = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "raw" };
    expect(prefixAllowed(orgFromPrefix, `raw/${ORG}`, ORG)).toBe(true);
    for (const prefix of ["raw/org-b", `other/${ORG}`, `raw/extra/${ORG}`, ORG, "raw", undefined])
      expect(prefixAllowed(orgFromPrefix, prefix, ORG)).toBe(false);
    expect(
      prefixAllowed({ PIPELINE_ORG_FROM_PREFIX: "true" }, ORG, ORG),
    ).toBe(true);
    expect(
      prefixAllowed({ PIPELINE_ORG_FROM_PREFIX: "true" }, `raw/${ORG}`, ORG),
    ).toBe(false);
  });
});

describe("buildObjectLandedMessage", () => {
  it("builds one message from the manifest and the identity read", async () => {
    const manifest = await manifestOf(SCRUBBED);
    const built = buildObjectLandedMessage({
      landed: { key: SCRUBBED.key, provider: "r2", bucket: "raw", etag: "x" },
      orgId: ORG,
      manifest,
      read: { etag: '"stored"' },
    });
    expect(built.status).toBe("enqueue");
    if (built.status !== "enqueue") return;
    const { message } = built;
    expect(message.stage).toBe("object-landed");
    expect(message.ctx).toEqual({
      traceId: manifest.trace_id.toLowerCase(),
      orgId: ORG,
      workspaceId: "ws_1",
      machineId: "mach-1",
      sessionId: "s-scrub-interop",
      harness: "claude",
    });
    // The ETag actually read wins over the notification's.
    expect(message.payload).toEqual({
      key: SCRUBBED.key,
      kind: "manifest",
      provider: "r2",
      bucket: "raw",
      etag: "stored",
      revision: manifest.revision,
      scrubbed: true,
    });
    expect(message.workKey).toBe(manifestWorkKey(message.payload));
    expect(message.workKey).toBe(`${SCRUBBED.key}@stored`);
  });

  it("marks an unscrubbed manifest so its pointer is quarantined", async () => {
    const built = buildObjectLandedMessage({
      landed: { key: UNSCRUBBED.key, etag: "e" },
      orgId: ORG,
      manifest: await manifestOf(UNSCRUBBED),
    });
    expect(built.status === "enqueue" && built.message.payload.scrubbed).toBe(
      false,
    );
  });

  it("ignores chunks and foreign keys, rejects contradictions", async () => {
    const manifest = await manifestOf(SCRUBBED);
    const chunk = SCRUBBED.key.replace("manifest.json", "raw/x.jsonl.zst");
    expect(
      buildObjectLandedMessage({
        landed: { key: chunk },
        orgId: ORG,
        manifest,
      }),
    ).toEqual({ status: "ignored", reason: "not_a_manifest" });
    expect(
      buildObjectLandedMessage({
        landed: { key: "elsewhere/file" },
        orgId: ORG,
        manifest,
      }),
    ).toEqual({ status: "ignored", reason: "not_a_session_key" });
    const other = SCRUBBED.key.replace("s=s-scrub-interop", "s=another");
    expect(() =>
      buildObjectLandedMessage({
        landed: { key: other, etag: "e" },
        orgId: ORG,
        manifest,
      }),
    ).toThrow(PipelineError);
    // No ETag or version: every revision would share one work key.
    expect(() =>
      buildObjectLandedMessage({
        landed: { key: SCRUBBED.key },
        orgId: ORG,
        manifest,
      }),
    ).toThrow(/ETag or object version/);
  });

  it("gives every delete its own work key and a fresh trace", () => {
    const built = buildObjectDeletedMessage({
      landed: {
        key: SCRUBBED.key,
        provider: "r2",
        bucket: "raw",
        eventTime: "2026-10-27T07:00:00Z",
      },
      orgId: ORG,
    });
    expect(built.status).toBe("enqueue");
    if (built.status !== "enqueue") return;
    expect(built.message.payload).toEqual({
      key: SCRUBBED.key,
      kind: "manifest",
      deleted: true,
      provider: "r2",
      bucket: "raw",
      version: "deleted:2026-10-27T07:00:00Z",
    });
    expect(built.message.workKey).toBe(manifestWorkKey(built.message.payload));
    expect(built.message.ctx.traceId).toMatch(/^[0-9a-f-]{36}$/);
    expect(built.message.revision).toBeUndefined();
  });
});

describe("object-event queue consumer", () => {
  const objects = () =>
    new Map([
      [SCRUBBED.key, { manifest: SCRUBBED.manifest, etag: '"stored-etag"' }],
    ]);
  const deps = (q = queue(), reader = memoryReader(objects())) => ({
    q,
    deps: {
      source: r2EventSource,
      reader,
      orgs: staticOrgResolver(ORG),
      allowPrefix: (prefix: string | undefined, orgId: string) =>
        prefixAllowed({}, prefix, orgId),
      send: q.send,
    },
  });

  it("enqueues a put with the ETag read from storage", async () => {
    const { q, deps: d } = deps();
    expect(await deliver(d, r2Event(SCRUBBED.key))).toBe("ack");
    expect(q.sent).toHaveLength(1);
    expect(q.sent[0]!.workKey).toBe(`${SCRUBBED.key}@stored-etag`);
    expect(q.sent[0]!.payload).toMatchObject({
      provider: "r2",
      bucket: "raw-sessions",
      scrubbed: true,
    });
  });

  it("forwards a manifest delete without reading storage", async () => {
    const { q, deps: d } = deps(queue(), memoryReader(new Map()));
    expect(await deliver(d, r2Event(SCRUBBED.key, "DeleteObject"))).toBe("ack");
    expect(q.sent[0]!.payload).toMatchObject({ deleted: true });
  });

  it("acks chunks, other buckets and keys outside the prefix", async () => {
    const { q, deps: d } = deps();
    const chunk = SCRUBBED.key.replace("manifest.json", "raw/a.jsonl.zst");
    expect(await deliver(d, r2Event(chunk))).toBe("ack");
    expect(
      await deliver(d, { ...r2Event(SCRUBBED.key), bucket: "another-bucket" }),
    ).toBe("ack");
    expect(await deliver(d, r2Event(`elsewhere/${SCRUBBED.key}`))).toBe("ack");
    expect(q.sent).toHaveLength(0);
  });

  it("retries an unreadable or unattributable manifest, acks an invalid one", async () => {
    const { deps: d } = deps(queue(), memoryReader(new Map()));
    expect(await deliver(d, r2Event(SCRUBBED.key))).toBe("retry:30");
    expect(
      await deliver(
        { ...d, orgs: { kind: "none", orgFor: async () => null } },
        r2Event(SCRUBBED.key),
      ),
    ).toBe("retry:300");
    expect(
      await deliver({ ...d, reader: undefined }, r2Event(SCRUBBED.key)),
    ).toBe("retry:300");
    const broken = new Map([
      [SCRUBBED.key, { manifest: { schema_v: 1 }, etag: "e" }],
    ]);
    expect(
      await deliver(
        { ...d, reader: memoryReader(broken) },
        r2Event(SCRUBBED.key),
      ),
    ).toBe("ack");
  });

  it("retries when the stage queue send fails", async () => {
    const { deps: d } = deps();
    const failing = {
      ...d,
      send: async () => {
        throw new Error("queue down");
      },
    };
    expect(await deliver(failing, r2Event(SCRUBBED.key))).toBe("retry:60");
  });

  it("never consumes its own DLQ", async () => {
    const { q, deps: d } = deps();
    expect(isObjectEventsDlq("relayhistory-object-events-dlq")).toBe(true);
    expect(
      await deliver(d, r2Event(SCRUBBED.key), "relayhistory-object-events-dlq"),
    ).toBe("retry:43200");
    expect(q.sent).toHaveLength(0);
  });
});

// --- Raw store: the real fs driver, synthetic sessions only ---------------

let root: string;
let store: ObjectStore;
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "rh-adapter-"));
  store = createFsStore({ root });
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

const LOC: SessionLocation = {
  workspaceId: "ws_1",
  machineId: "mach-1",
  harness: "claude",
  sessionId: "s-synthetic-1",
  startDate: "2026-10-25",
};

/** A synthetic source range with secrets in a value, a key and a plain line. */
const SOURCE = new TextEncoder().encode(
  [
    JSON.stringify({ type: "user", text: `use ${GH_TOKEN} please` }),
    JSON.stringify({ [EMAIL]: "contact", n: 1 }),
    `plain line with ${EMAIL}`,
    "",
  ].join("\n"),
);

/** A valid v1 manifest for sealed chunks, as a client would write it. */
async function manifestFor(
  loc: SessionLocation,
  entries: ChunkEntry[],
  scrub = true,
) {
  const at = "2026-10-25T10:00:00.000Z";
  const next = entries.reduce((n, e) => e.start_offset + e.bytes, 0);
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
    revision: entries.length + 1,
    created_at: at,
    updated_at: at,
    session: {
      workspace_id: loc.workspaceId,
      machine_id: loc.machineId,
      harness: loc.harness,
      session_id: loc.sessionId,
      started_at: at,
    },
    writer: { delivery_instance_id: "dest_test", plugin_version: "0.0.0" },
    raw: {
      owner: "dest_test",
      state: "open",
      updated_at: at,
      format: "jsonl",
      compression: "zstd",
      chunk_count: entries.length,
      raw_bytes: entries.reduce((n, e) => n + e.bytes, 0),
      stored_bytes: entries.reduce((n, e) => n + e.stored_bytes, 0),
      content_sha256: await computeContentSha256(entries),
      epoch: 0,
      epochs: [
        {
          epoch: 0,
          opened_at: at,
          reason: "initial",
          first_seq: 0,
          opened_by: "dest_test",
        },
      ],
      next_offset: next,
      chunks: entries,
      ...(scrub
        ? { scrub: { ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION } }
        : {}),
    },
  };
}

async function putManifest(loc: SessionLocation, manifest: unknown) {
  const key = manifestKey(loc);
  await store.put(key, new TextEncoder().encode(JSON.stringify(manifest)));
  return String(key);
}

describe("scrubbing raw chunks before object storage", () => {
  it("scrubs every record, key and plain line", () => {
    const { scrubbed, records } = scrubRecords(SOURCE);
    const text = new TextDecoder().decode(scrubbed);
    expect(records).toBe(3);
    expect(text).not.toContain(GH_TOKEN);
    expect(text).not.toContain(EMAIL);
    expect(text.split("\n")).toHaveLength(4);
    expect(JSON.parse(text.split("\n")[0]!)).toEqual({
      type: "user",
      text: "use [REDACTED] please",
    });
  });

  it("matches a multi-line private key in plain text whole", () => {
    const body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjSYNTHETICKEYBODY";
    const pem = [
      "-----BEGIN " + "PRIVATE KEY-----",
      body,
      body,
      "-----END " + "PRIVATE KEY-----",
    ].join("\n");
    const source = `{"type":"user"}\n${pem}\n{"type":"done"}\n`;
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(source)).scrubbed,
    );
    expect(out).not.toContain(body);
    expect(out).not.toContain("PRIVATE KEY");
    expect(out.startsWith('{"type":"user"}\n')).toBe(true);
    expect(out.endsWith('{"type":"done"}\n')).toBe(true);
  });

  it("changes only scrubbed strings: big integers and __proto__ keys survive", () => {
    const line =
      '{"eventId":9007199254740993,"__proto__":{"m":"hi"},"n": 1.50e3,"card":4111111111111111,"t":"' +
      GH_TOKEN +
      '"}';
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${line}\n`)).scrubbed,
    );
    expect(out).toBe(
      '{"eventId":9007199254740993,"__proto__":{"m":"hi"},"n": 1.50e3,"card":"[REDACTED]","t":"[REDACTED]"}\n',
    );
  });

  it("keeps both values when two keys scrub to the same name, like scrubJson", () => {
    const line = JSON.stringify({
      "a@example.com": 1,
      "b@example.com": { [GH_TOKEN]: 2, x: [{ "c@example.com": 3 }] },
      "[REDACTED]": 4,
    });
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${line}\n`)).scrubbed,
    );
    expect(JSON.parse(out)).toEqual(
      scrubJson(JSON.parse(line) as Record<string, unknown>),
    );
    expect(JSON.parse(out)).toEqual({
      "[REDACTED]": 1,
      "[REDACTED] (2)": { "[REDACTED]": 2, x: [{ "[REDACTED]": 3 }] },
      "[REDACTED] (3)": 4,
    });
    // A key the source itself repeated stays exactly as the source had it.
    const dup = '{"k":1,"k":2}\n';
    expect(
      new TextDecoder().decode(
        scrubRecords(new TextEncoder().encode(dup)).scrubbed,
      ),
    ).toBe(dup);
  });

  it("finds a secret behind a JSON escape, on a string line or inside a record", () => {
    const escaped = GH_TOKEN.replace("p", "\\u0070");
    const source = `"${escaped}"\n{"t":"${escaped}"}\n`;
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(source)).scrubbed,
    );
    expect(out).toBe('"[REDACTED]"\n{"t":"[REDACTED]"}\n');
    expect(out).not.toContain(GH_TOKEN.slice(4));
  });

  it("keeps every line of a long plain-text run, and a key across the segment limit", () => {
    const line = "plain log line 0123456789 abcdefghijklmnopqrstuvwxyz";
    const lines: string[] = [];
    // Well past MAX_SCRUB_CHARS (256 KiB) of short lines.
    for (let size = 0; size < 300_000; size += lines.at(-1)!.length + 1)
      lines.push(`${line} ${lines.length}`);
    const body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjSYNTHETICKEYBODY";
    const pem = [
      "-----BEGIN " + "PRIVATE KEY-----",
      ...Array.from({ length: 200 }, () => body),
      "-----END " + "PRIVATE KEY-----",
    ];
    // The key starts just before the 256 KiB boundary and ends after it.
    let at = 0;
    for (let size = 0; size < 262_144 - 2_000; at += 1)
      size += lines[at]!.length + 1;
    const all = [...lines.slice(0, at), ...pem, ...lines.slice(at)];
    const source = `${all.join("\n")}\n`;
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(source)).scrubbed,
    );
    expect(out).not.toContain("characters over the");
    expect(out).not.toContain(body);
    for (const kept of [lines[0]!, lines[at - 1]!, lines[at]!, lines.at(-1)!])
      expect(out).toContain(kept);
    expect(out.split("\n").length).toBe(lines.length + 2);
  });

  it("redacts a whole string when only its raw, escaped text matches a rule", () => {
    // A stand-in rule that only the escaped form can match.
    const rule = (text: string) => text.replace(/\\u0041/g, "[REDACTED]");
    expect(scrubStringToken('"x\\u0041y"', "xAy", rule)).toBe("[REDACTED]");
    // The decoded value matched: its scrubbed form is used.
    expect(scrubStringToken('"xAy"', "xAy", (t) => t.replace("A", "[R]"))).toBe(
      "x[R]y",
    );
    // Neither form matched: the value is unchanged.
    expect(scrubStringToken('"xBy"', "xBy", rule)).toBe("xBy");
  });

  it("never cuts a segment between a Bearer prefix and its token", () => {
    const token = "abcdefghijklmnopqrstuvwxyz0123456789";
    const filler = "plain log line abcdefghijklmnopqrstuvwxyz";
    const lines: string[] = [];
    let size = 0;
    // Fill to just under MAX_SCRUB_CHARS, so the token line forces a cut.
    while (size < 262_144 - 50) {
      lines.push(filler);
      size += filler.length + 1;
    }
    lines.push("Authorization: Bearer", token, "after");
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${lines.join("\n")}\n`)).scrubbed,
    );
    expect(out).not.toContain(token);
    expect(out).not.toContain("characters over the");
    expect(out.endsWith("after\n")).toBe(true);
  });

  it("finds a safe cut past a run of chained Bearer lines, and stores every line", () => {
    const token = "abcdefghijklmnopqrstuvwxyz0123456789";
    const filler = "plain log line abcdefghijklmnopqrstuvwxyz";
    const lines: string[] = [];
    let size = 0;
    while (size < 262_144 - 50) {
      lines.push(filler);
      size += filler.length + 1;
    }
    // Devin's case: the last line ends in Bearer, then five lines that each
    // start with a token and end in Bearer, then an ordinary line.
    lines[lines.length - 1] += " Bearer";
    for (let n = 0; n < 5; n += 1) lines.push(`${token}${n} Bearer`);
    lines.push(token, "after");
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${lines.join("\n")}\n`)).scrubbed,
    );
    expect(out).not.toContain(token);
    expect(out).not.toContain("characters over the");
    // Every filler line is stored; each Bearer match folds its newline into
    // "Bearer [REDACTED]", so only the token lines merge.
    const fillers = lines.filter((l) => l.startsWith(filler)).length;
    expect(out.split(filler).length - 1).toBe(fillers);
    expect(out.endsWith("after\n")).toBe(true);
  });

  it("refuses a chunk it cannot segment without breaking a match", () => {
    const filler = "plain log line abcdefghijklmnopqrstuvwxyz";
    const lines: string[] = [];
    let size = 0;
    while (size < 262_144 - 70_000) {
      lines.push(filler);
      size += filler.length + 1;
    }
    // 70 KB of lines that each end in Bearer and start with a token: every
    // boundary in the last 64 KiB splits a match.
    lines[lines.length - 1] += " Bearer";
    for (let n = 0; size < 262_144 + 1_000; n += 1) {
      const chained = `abcdefghijklmnopqrstuvwxyz${String(n).padStart(10, "0")} Bearer`;
      lines.push(chained);
      size += chained.length + 1;
    }
    expect(() =>
      scrubRecords(new TextEncoder().encode(`${lines.join("\n")}\n`)),
    ).toThrow(/cannot be split/);
  });

  it("stores a line or string past the scrub limit whole (version 2), and refuses one it cannot segment", () => {
    const long = "word ".repeat(70_000); // 350 KB, one line
    const asLine = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${long}\n`)).scrubbed,
    );
    expect(asLine).toBe(`${long}\n`);
    const asRecord = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${JSON.stringify({ text: long })}\n`))
        .scrubbed,
    );
    expect(asRecord).toBe(`${JSON.stringify({ text: long })}\n`);
    // One unbroken run past the limit has no safe cut: refused, never truncated.
    const run = "w".repeat(300_000);
    expect(() =>
      scrubRecords(new TextEncoder().encode(`${JSON.stringify({ text: run })}\n`)),
    ).toThrow(/scrub limit/);
    // Just under the limit is stored whole.
    const fits = "word ".repeat(50_000);
    const out = new TextDecoder().decode(
      scrubRecords(new TextEncoder().encode(`${fits}\n`)).scrubbed,
    );
    expect(out).toBe(`${fits}\n`);
  });

  it("refuses bytes the scrubber cannot read", () => {
    expect(() => scrubRecords(new Uint8Array([0xff, 0xfe, 0x0a]))).toThrow(
      /UTF-8/,
    );
  });

  it("stores only scrubbed bytes, keyed and described like a client seal", async () => {
    const sealed = await sealRawChunk(store, {
      location: LOC,
      format: "jsonl",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
    });
    expect(sealed.created).toBe(true);
    expect(sealed.scrub).toEqual({ ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION });
    const object = await store.getObject(objectKey(sealed.key));
    const compressed = await readAll(object.body);
    const stored = zstdDecompressSync(compressed);
    const text = new TextDecoder().decode(stored);
    // Nothing unscrubbed reached storage, compressed or not.
    expect(text).not.toContain(GH_TOKEN);
    expect(text).not.toContain(EMAIL);
    expect(Buffer.from(compressed).toString("latin1")).not.toContain(GH_TOKEN);
    // Source fields describe the source range; stored fields what was stored.
    expect(sealed.entry.bytes).toBe(SOURCE.byteLength);
    expect(sealed.entry.stored_raw_bytes).toBe(stored.byteLength);
    expect(sealed.entry.stored_bytes).toBe(compressed.byteLength);
    expect(sealed.key).toContain(`/raw/0000000000-0-${sealed.entry.sha256_8}`);
    expect(object.meta.metadata["scrub-ruleset"]).toBe(SCRUB_RULESET);
    // The manifest built from it validates, scrub marker and all.
    const manifest = await manifestFor(LOC, [sealed.entry]);
    await expect(
      parseManifest(manifest, manifestKey(LOC)),
    ).resolves.toBeTruthy();
  });

  it("is idempotent, and keeps an existing object over a new write", async () => {
    const input = {
      location: LOC,
      format: "jsonl" as const,
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
    };
    const first = await sealRawChunk(store, input);
    const again = await sealRawChunk(store, input);
    expect(again.created).toBe(false);
    expect(again.entry).toEqual(first.entry);
  });

  it("refuses to vouch for a stored object with no scrub metadata", async () => {
    const first = await sealRawChunk(store, {
      location: LOC,
      format: "jsonl",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
    });
    // Another writer put unscrubbed bytes at the same key.
    await rm(join(root, first.key));
    await rm(join(root, `${first.key}.meta`), { force: true });
    await store.put(objectKey(first.key), new Uint8Array([1, 2, 3]));
    await expect(
      sealRawChunk(store, {
        location: LOC,
        format: "jsonl",
        seq: 0,
        epoch: 0,
        startOffset: 0,
        source: SOURCE,
      }),
    ).rejects.toThrow(/no scrub metadata/);
  });

  it("refuses identical bytes stored without scrub metadata", async () => {
    const input = {
      location: LOC,
      format: "jsonl" as const,
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
    };
    // The same sealed bytes, stored at the same key by a writer that set no
    // scrub metadata.
    const elsewhere = await mkdtemp(join(tmpdir(), "rh-adapter-b-"));
    try {
      const other = createFsStore({ root: elsewhere });
      const sealed = await sealRawChunk(other, input);
      const bytes = await readAll(
        (await other.getObject(objectKey(sealed.key))).body,
      );
      await store.put(objectKey(sealed.key), bytes);
    } finally {
      await rm(elsewhere, { recursive: true, force: true });
    }
    await expect(sealRawChunk(store, input)).rejects.toThrow(
      /no scrub metadata/,
    );
  });
});

describe("raw store configuration", () => {
  const base = {
    PIPELINE_RAW_S3_BUCKET: "b",
    PIPELINE_RAW_S3_ACCESS_KEY_ID: "k",
    PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s",
  };

  it("fails closed on an unknown encryption mode, logging the setting not its value", () => {
    expect(rawStoreFromEnv({ ...base, PIPELINE_RAW_S3_SSE: "aws-kms" })).toBe(
      undefined,
    );
    expect(logs.join("\n")).toContain("PIPELINE_RAW_S3_SSE");
    expect(logs.join("\n")).not.toContain("aws-kms");
    expect(logs.join("\n")).not.toContain('"s"');
  });

  it("requests the configured encryption on every put", async () => {
    const seen: Headers[] = [];
    const raw = rawStoreFromEnv(
      {
        ...base,
        PIPELINE_RAW_S3_ENDPOINT: "http://localhost:1",
        PIPELINE_RAW_S3_SSE: "AES256",
      },
      (async (_url: unknown, init?: RequestInit) => {
        seen.push(new Headers(init?.headers));
        return new Response(null, { status: 200, headers: { etag: '"e"' } });
      }) as typeof fetch,
    )!;
    await raw.store.put(objectKey("a/b.json"), new Uint8Array([1]));
    expect(seen[0]!.get("x-amz-server-side-encryption")).toBe("AES256");
    expect(rawStoreFromEnv(base)?.provider).toBe("s3");
    expect(rawStoreFromEnv({ ...base, PIPELINE_RAW_PROVIDER: "s3" })?.provider).toBe("s3");
    expect(rawStoreFromEnv({ ...base, PIPELINE_RAW_PROVIDER: "r2" })?.provider).toBe("r2");
  });

  it("does not relabel a non-S3 provider as S3", () => {
    const env = {
      PIPELINE_RAW_S3_BUCKET: "b",
      PIPELINE_RAW_S3_ACCESS_KEY_ID: "AKIA_BOUNDARY_KEY",
      PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "SECRET_BOUNDARY_VALUE",
      PIPELINE_RAW_PROVIDER: "gcs",
    };
    expect(rawStoreFromEnv(env)).toBeUndefined();
    expect(rawStoreFromEnv({ ...env, PIPELINE_RAW_PROVIDER: "azure" })).toBeUndefined();
    expect(logs.join("\n")).toContain("PIPELINE_RAW_PROVIDER");
    expect(logs.join("\n")).toContain("gcs without HMAC keys");
    expect(logs.join("\n")).toContain("s3 | r2 | gcs");
    expect(logs.join("\n")).not.toContain("AKIA_BOUNDARY_KEY");
    expect(logs.join("\n")).not.toContain("SECRET_BOUNDARY_VALUE");
  });

  it("does not point the S3 client at the GCS API", () => {
    expect(
      rawStoreFromEnv({
        ...base,
        PIPELINE_RAW_S3_ENDPOINT: "https://storage.googleapis.com",
        PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "SUPER_HMAC_VALUE",
      }),
    ).toBeUndefined();
    expect(
      rawStoreFromEnv({
        ...base,
        PIPELINE_RAW_S3_ENDPOINT: "https://storage.googleapis.com./",
        PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "SUPER_HMAC_VALUE",
      }),
    ).toBeUndefined();
    expect(logs.join("\n")).toContain("PIPELINE_RAW_S3_ENDPOINT");
    expect(logs.join("\n")).not.toContain("SUPER_HMAC_VALUE");
  });

  it("selects a GCS store when no HMAC key is set", () => {
    const raw = rawStoreFromEnv({
      PIPELINE_RAW_S3_BUCKET: "proof-bucket",
      PIPELINE_RAW_PROVIDER: "gcs",
    });
    expect(raw?.provider).toBe("gcs");
    expect(raw?.bucket).toBe("proof-bucket");
    expect(raw?.store.backend).toBe("gcs");
    expect(
      rawStoreFromEnv({
        PIPELINE_RAW_S3_BUCKET: "proof-bucket",
        PIPELINE_RAW_PROVIDER: "gcs",
        PIPELINE_RAW_S3_ENDPOINT: "https://storage.googleapis.com",
      })?.provider,
    ).toBe("gcs");
    expect(
      rawStoreFromEnv({
        PIPELINE_RAW_S3_BUCKET: "proof-bucket",
        PIPELINE_RAW_PROVIDER: "gcs",
        PIPELINE_RAW_S3_ENDPOINT: "http://127.0.0.1:4443/storage/v1",
      })?.provider,
    ).toBe("gcs");
    expect(
      rawStoreFromEnv({
        PIPELINE_RAW_S3_BUCKET: "proof-bucket",
        PIPELINE_RAW_PROVIDER: "gcs",
        PIPELINE_RAW_S3_ENDPOINT: "https://attacker.example/storage/v1",
      }),
    ).toBeUndefined();
    expect(
      rawStoreFromEnv({
        PIPELINE_RAW_S3_BUCKET: "proof-bucket",
        PIPELINE_RAW_PROVIDER: "gcs",
        PIPELINE_RAW_GCS_TOKEN_URI: "http://evil.example/token?secret=SUPERSECRET",
      }),
    ).toBeUndefined();
    expect(logs.join("\n")).not.toContain("SUPERSECRET");
    expect(logs.join("\n")).not.toContain("evil.example");
    expect(logs.join("\n")).not.toContain("attacker.example");
  });
});

describe("manifest reads are size-capped on the bytes read", () => {
  it("refuses an oversized manifest even when no length is declared", async () => {
    const big = new TextEncoder().encode("x".repeat(4096));
    const fake = {
      getObject: async () => ({
        meta: { size: 0, version: { eTag: '"e"' }, metadata: {} },
        body: new Blob([big]).stream(),
      }),
    } as unknown as ObjectStore;
    const reader = objectStoreManifestReader(fake, {
      provider: "s3",
      maxBytes: 1024,
    });
    await expect(reader.read(SCRUBBED.key)).rejects.toThrow(/larger/);
  });
});

describe("manifest reconcile (stores with no event source)", () => {
  it("enqueues recent manifests once each, skipping chunks and invalid ones", async () => {
    const sealed = await sealRawChunk(store, {
      location: LOC,
      format: "jsonl",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
    });
    const key = await putManifest(LOC, await manifestFor(LOC, [sealed.entry]));
    const brokenLoc = { ...LOC, sessionId: "s-broken" };
    await putManifest(brokenLoc, { schema_v: 1 });
    const q = queue();
    const options = {
      store,
      reader: objectStoreManifestReader(store, {
        provider: "fs" as const,
        bucket: "local",
      }),
      orgs: staticOrgResolver(ORG),
      send: q.send,
      listPrefix: "v1",
      modifiedSince: new Date(Date.now() - 60_000),
      maxPages: 5,
    };
    const result = await reconcileManifests(options);
    expect(result).toMatchObject({
      listed: 2,
      enqueued: 1,
      skipped: 1,
      failed: 0,
    });
    expect(q.sent[0]!.payload).toMatchObject({
      key,
      provider: "fs",
      bucket: "local",
      scrubbed: true,
    });
    // A second run re-sends the same unit of work, not a new one.
    await reconcileManifests(options);
    expect(q.sent[1]!.workKey).toBe(q.sent[0]!.workKey);
    // Nothing older than the window is re-sent.
    const later = await reconcileManifests({
      ...options,
      modifiedSince: new Date(Date.now() + 60_000),
    });
    expect(later.enqueued).toBe(0);
    // No content reached a log line.
    expect(logs.join("\n")).not.toContain(GH_TOKEN);
  });

  it("lists where this deployment's manifests live", () => {
    expect(reconcileListPrefix({})).toBe("v1");
    expect(reconcileListPrefix({ PIPELINE_KEY_PREFIX: "/raw/" })).toBe(
      "raw/v1",
    );
    expect(
      reconcileListPrefix({
        PIPELINE_ORG_FROM_PREFIX: "true",
        PIPELINE_KEY_PREFIX: "raw",
      }),
    ).toBe("raw");
    expect(
      reconcileListPrefix({ PIPELINE_ORG_FROM_PREFIX: "true" }),
    ).toBeUndefined();
  });

  it("stays inert unless enabled and configured", async () => {
    const now = new Date("2026-10-27T07:05:00Z");
    expect(await runManifestReconcile({} as Bindings, now)).toBe("skipped");
    expect(
      await runManifestReconcile(
        { PIPELINE_RECONCILE_ENABLED: "true" } as Bindings,
        now,
      ),
    ).toBe("skipped");
    expect(
      await runManifestReconcile(
        { PIPELINE_RECONCILE_ENABLED: "true" } as Bindings,
        new Date("2026-10-27T07:06:00Z"),
      ),
    ).toBe("skipped");
  });
});

// --- Routes ---------------------------------------------------------------

const AUTH: AuthContext = {
  orgId: ORG,
  workspaceId: "ws_1",
  scopes: ["rth:sync"],
} as unknown as AuthContext;

function app(deps: Partial<PipelineRouteDeps>, env: Partial<Bindings>) {
  const sent: StageMessage[] = [];
  const routes = createPipelineRoutes({
    sink: () => ({ record: async () => {} }) as never,
    deadLetters: () => ({}) as never,
    send: async (_env, _stage, message) => void sent.push(message),
    ...deps,
  });
  const hono = new Hono<AppEnv>();
  hono.use("*", async (c, next) => {
    c.set("auth" as never, AUTH as never);
    c.set("correlationId", "corr");
    await next();
  });
  hono.route("/v1", routes);
  return {
    sent,
    request: (path: string, init: RequestInit) =>
      hono.request(path, init, { PIPELINE_ORG_ID: ORG, ...env } as Bindings),
  };
}

describe("POST /v1/pipeline/manifests", () => {
  const post = (a: ReturnType<typeof app>, body: unknown) =>
    a.request("/v1/pipeline/manifests", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

  it("reads the manifest from storage and enqueues it", async () => {
    const reader = memoryReader(
      new Map([[SCRUBBED.key, { manifest: SCRUBBED.manifest, etag: '"s"' }]]),
    );
    const a = app({ manifests: () => reader }, {});
    const res = await post(a, { key: SCRUBBED.key, etag: "client-etag" });
    expect(res.status).toBe(202);
    expect(await res.json()).toMatchObject({
      workKey: `${SCRUBBED.key}@s`,
      scrubbed: true,
    });
    expect(a.sent[0]!.payload).toMatchObject({
      provider: "r2",
      bucket: "raw-sessions",
    });
  });

  it("validates a posted copy when no store is configured", async () => {
    const a = app({ manifests: () => undefined }, {});
    const res = await post(a, {
      key: SCRUBBED.key,
      etag: "e1",
      manifest: SCRUBBED.manifest,
    });
    expect(res.status).toBe(202);
    const bad = await post(a, {
      key: SCRUBBED.key,
      etag: "e1",
      manifest: { ...SCRUBBED.manifest, revision: "x", secret: GH_TOKEN },
    });
    expect(bad.status).toBe(400);
    expect(await bad.text()).not.toContain(GH_TOKEN);
  });

  it("refuses keys outside the caller's workspace or org, fails closed unconfigured", async () => {
    const a = app({ manifests: () => undefined }, {});
    const other = SCRUBBED.key.replace("ws=ws_1", "ws=ws_2");
    expect((await post(a, { key: other })).status).toBe(403);
    expect((await post(a, { key: `p/${SCRUBBED.key}` })).status).toBe(403);
    expect((await post(a, { key: "nope" })).status).toBe(400);
    const foreign = app(
      { manifests: () => undefined },
      { PIPELINE_ORG_ID: "org-other" },
    );
    expect((await post(foreign, { key: SCRUBBED.key })).status).toBe(403);
    const unconfigured = app(
      { manifests: () => undefined },
      { PIPELINE_ORG_ID: "" },
    );
    expect((await post(unconfigured, { key: SCRUBBED.key })).status).toBe(503);
  });

  it("answers 404 for a manifest not in storage", async () => {
    const a = app({ manifests: () => memoryReader(new Map()) }, {});
    expect((await post(a, { key: SCRUBBED.key })).status).toBe(404);
  });
});

describe("POST /v1/pipeline/chunks", () => {
  const query =
    "machine=mach-1&harness=claude&session=s-synthetic-1&date=2026-10-25&seq=0&epoch=0&offset=0&format=jsonl";

  it("scrubs the chunk before it reaches storage and returns its entry", async () => {
    const a = app(
      { rawStore: () => ({ store, provider: "s3", bucket: "b" }) },
      {},
    );
    const res = await a.request(`/v1/pipeline/chunks?${query}`, {
      method: "POST",
      body: SOURCE,
    });
    expect(res.status).toBe(201);
    const body = (await res.json()) as {
      key: string;
      entry: ChunkEntry;
      scrub: unknown;
    };
    expect(body.key.startsWith("v1/ws=ws_1/m=mach-1/")).toBe(true);
    expect(body.scrub).toEqual({ ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION });
    const stored = zstdDecompressSync(
      await readAll((await store.getObject(objectKey(body.key))).body),
    );
    expect(new TextDecoder().decode(stored)).not.toContain(GH_TOKEN);
    expect(JSON.stringify(body)).not.toContain(GH_TOKEN);
    expect(logs.join("\n")).not.toContain(GH_TOKEN);
  });

  it("refuses another org's or an unconfigured owner, storing nothing", async () => {
    const raw = () => ({ store, provider: "s3" as const, bucket: "b" });
    const foreign = app({ rawStore: raw }, { PIPELINE_ORG_ID: "org-other" });
    const res = await foreign.request(`/v1/pipeline/chunks?${query}`, {
      method: "POST",
      body: SOURCE,
    });
    expect(res.status).toBe(403);
    const unconfigured = app({ rawStore: raw }, { PIPELINE_ORG_ID: "" });
    expect(
      (
        await unconfigured.request(`/v1/pipeline/chunks?${query}`, {
          method: "POST",
          body: SOURCE,
        })
      ).status,
    ).toBe(503);
    expect((await store.list(objectKey("v1"))).items).toHaveLength(0);
  });

  it("caps the body while reading, with no declared length", async () => {
    const a = app(
      { rawStore: () => ({ store, provider: "s3", bucket: "b" }) },
      {},
    );
    const chunk = new Uint8Array(1024 * 1024).fill(0x61);
    let sent = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        // 64 MiB in 1 MiB pieces, far past the 16 MiB cap.
        if (sent++ < 64) controller.enqueue(chunk);
        else controller.close();
      },
    });
    const res = await a.request(`/v1/pipeline/chunks?${query}`, {
      method: "POST",
      body,
      duplex: "half",
    } as RequestInit);
    expect(res.status).toBe(413);
    // Reading stopped at the cap (plus the stream's read-ahead).
    expect(sent).toBeLessThan(20);
  });

  it("rejects bad input and answers 503 with no raw store", async () => {
    const a = app(
      { rawStore: () => ({ store, provider: "s3", bucket: "b" }) },
      {},
    );
    expect(
      (
        await a.request("/v1/pipeline/chunks?format=jsonl", {
          method: "POST",
          body: SOURCE,
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await a.request(
          `/v1/pipeline/chunks?${query.replace("mach-1", "..")}`,
          {
            method: "POST",
            body: SOURCE,
          },
        )
      ).status,
    ).toBe(400);
    // A missing or empty position is rejected, never read as 0.
    for (const missing of ["seq", "epoch", "offset"]) {
      const partial = query
        .split("&")
        .map((p) => (p.startsWith(`${missing}=`) ? `${missing}=` : p))
        .join("&");
      for (const q of [
        partial,
        query.replace(new RegExp(`&?${missing}=[^&]*`), ""),
      ])
        expect(
          (
            await a.request(`/v1/pipeline/chunks?${q}`, {
              method: "POST",
              body: SOURCE,
            })
          ).status,
          `${missing} in ${q}`,
        ).toBe(400);
    }
    const none = app({ rawStore: () => undefined }, {});
    expect(
      (
        await none.request(`/v1/pipeline/chunks?${query}`, {
          method: "POST",
          body: SOURCE,
        })
      ).status,
    ).toBe(503);
  });
});

// --- MinIO, when the object-store emulators are up -------------------------
// eval "$(packages/object-store/scripts/emulators.sh up)"

const s3Endpoint = process.env.RH_OBJSTORE_S3_ENDPOINT;
describe.skipIf(!s3Endpoint)("MinIO end to end", () => {
  it("seals scrubbed chunks, reconciles the manifest, and reads it back", async () => {
    const bucket = `rh-adapter-${Date.now()}`;
    const credentials = {
      accessKeyId: process.env.RH_OBJSTORE_S3_ACCESS_KEY_ID!,
      secretAccessKey: process.env.RH_OBJSTORE_S3_SECRET_ACCESS_KEY!,
    };
    const url = new URL(`${s3Endpoint}/${bucket}/`);
    const headers = new Headers();
    await signRequest({
      method: "PUT",
      url,
      headers,
      payloadSha256:
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      region: "us-east-1",
      credentials,
    });
    expect([200, 409]).toContain(
      (await fetch(url, { method: "PUT", headers })).status,
    );
    const raw = rawStoreFromEnv({
      PIPELINE_RAW_S3_BUCKET: bucket,
      PIPELINE_RAW_S3_ENDPOINT: s3Endpoint,
      PIPELINE_RAW_S3_ACCESS_KEY_ID: credentials.accessKeyId,
      PIPELINE_RAW_S3_SECRET_ACCESS_KEY: credentials.secretAccessKey,
    })!;
    const sealed = await sealRawChunk(raw.store, {
      location: LOC,
      format: "jsonl",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
    });
    const stored = zstdDecompressSync(
      await readAll((await raw.store.getObject(objectKey(sealed.key))).body),
    );
    expect(new TextDecoder().decode(stored)).not.toContain(GH_TOKEN);
    const manifest = await manifestFor(LOC, [sealed.entry]);
    await raw.store.put(
      manifestKey(LOC),
      new TextEncoder().encode(JSON.stringify(manifest)),
    );
    const q = queue();
    const result = await reconcileManifests({
      store: raw.store,
      reader: objectStoreManifestReader(raw.store, {
        provider: raw.provider,
        bucket,
      }),
      orgs: staticOrgResolver(ORG),
      send: q.send,
      listPrefix: "v1",
      modifiedSince: new Date(Date.now() - 600_000),
    });
    expect(result.enqueued).toBe(1);
    expect(q.sent[0]!.payload).toMatchObject({
      provider: "s3",
      bucket,
      scrubbed: true,
    });
    expect(q.sent[0]!.workKey).toMatch(/@[0-9a-f]{32}$/);
  });
});
