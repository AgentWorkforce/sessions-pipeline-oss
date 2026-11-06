import { describe, expect, test } from "bun:test";
import { hoopDbSessionIndex } from "../index.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { ManifestReader, ManifestV1, PipelineQueue, RawPointer, TraceContext } from "../../shared/vendor.ts";

const traceId = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const sessionUuid = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19";
const manifestKey = "v1/ws=ws-test/m=machine-test/h=codex/d=2026/10/29/s=session-test/manifest.json";

function scrubbedManifest(): ManifestV1 {
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: traceId,
    revision: 1,
    created_at: "2026-10-29T07:00:00.000Z",
    updated_at: "2026-10-29T07:00:01.000Z",
    session: {
      workspace_id: "ws-test",
      machine_id: "machine-test",
      harness: "codex",
      session_id: "session-test",
      started_at: "2026-10-29T07:00:00.000Z",
    },
    writer: { delivery_instance_id: "machine-test", plugin_version: "test" },
    raw: {
      owner: "machine-test",
      state: "open",
      updated_at: "2026-10-29T07:00:01.000Z",
      format: "jsonl",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: 12,
      stored_bytes: 10,
      content_sha256: "1".repeat(64),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: "2026-10-29T07:00:00.000Z", reason: "initial", first_seq: 0, opened_by: "machine-test" }],
      next_offset: 12,
      chunks: [{
        seq: 0,
        epoch: 0,
        start_offset: 0,
        bytes: 12,
        stored_bytes: 10,
        sha256: "2".repeat(64),
        sha256_8: "22222222",
        stored_sha256: "3".repeat(64),
        compression: "zstd",
      }],
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: {
      "96bulls": { session_uuid: sessionUuid, session_uuid_source: "minted" },
    },
  };
}

describe("HoopDb session index", () => {
  test("copies the manifest scrub verdict into the pointer upsert", async () => {
    const calls: Array<{ text: string; params?: unknown[] }> = [];
    const db = {
      async query<R>(text: string, params?: unknown[]) {
        calls.push({ text, params });
        if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
        return { rows: [] as R[] };
      },
    } as unknown as HoopDb;
    const manifest = scrubbedManifest();
    const reader: ManifestReader = {
      provider: "s3",
      bucket: "locker",
      async read() { return { manifest, etag: "etag-1" }; },
    };
    const queue: PipelineQueue = {
      backend: "memory",
      async send() {},
      async sendBatch() {},
    };
    const pointer: RawPointer = {
      traceId,
      workspaceId: "ws-test",
      machineId: "machine-test",
      harness: "codex",
      sessionId: "session-test",
      manifestKey,
      provider: "s3",
      bucket: "locker",
    };
    const ctx: TraceContext = {
      traceId,
      orgId: "org-test",
      workspaceId: "ws-test",
      machineId: "machine-test",
      harness: "codex",
      sessionId: "session-test",
    };

    await hoopDbSessionIndex({ db, reader, queue, current: () => undefined }).upsertRawPointer(pointer, ctx);

    const upsert = calls.find((call) => call.text.includes("upsert_session_index"));
    const rows = JSON.parse(String(upsert?.params?.[0])) as Array<{ scrubbed?: boolean }>;
    expect(rows[0]?.scrubbed).toBe(true);
  });

  test("repairs equal-revision identity before projecting the directory", async () => {
    const calls: Array<{ text: string; params?: unknown[] }> = [];
    const db = {
      async query<R>(text: string, params?: unknown[]) {
        calls.push({ text, params });
        if (text.includes("SELECT session_uuid::text"))
          return {
            rows: [{ session_uuid: sessionUuid, user_id: "unknown", fanout_revision: "1" }] as R[],
          };
        if (text.includes("upsert_session_index"))
          return { rows: [{ applied: false, revision: "1" }] as R[] };
        if (text.includes("WITH live AS"))
          return { rows: [{ projected: true }] as R[] };
        return { rows: [] as R[] };
      },
    } as unknown as HoopDb;
    const manifest = scrubbedManifest();
    manifest.session.session_id = "kimi_session";
    manifest.session.machine_id = "machine_encoded";
    manifest.extensions = {
      ...manifest.extensions,
      "96bulls": {
        session_uuid: sessionUuid,
        session_uuid_source: "minted",
        user_id: "user-real",
        canonical_session_id: "kimi:session",
        canonical_machine_id: "machine:encoded",
      },
    };
    const reader: ManifestReader = {
      provider: "s3",
      bucket: "locker",
      async read() {
        return { manifest, etag: "etag-1" };
      },
    };
    const queue: PipelineQueue = {
      backend: "memory",
      async send() {},
      async sendBatch() {},
    };
    await hoopDbSessionIndex({ db, reader, queue, current: () => undefined })
      .upsertRawPointer(
        {
          traceId,
          workspaceId: "ws-test",
          machineId: "machine_encoded",
          harness: "codex",
          sessionId: "kimi_session",
          manifestKey,
          provider: "s3",
          bucket: "locker",
        },
        {
          traceId,
          orgId: "org-test",
          workspaceId: "ws-test",
          machineId: "machine_encoded",
          harness: "codex",
          sessionId: "kimi_session",
        },
      );

    const extAt = calls.findIndex((call) =>
      call.text.includes("INSERT INTO sessions.session_index_ext"),
    );
    const repairAt = calls.findIndex((call) =>
      call.text.includes("canonical_session_id = $8"),
    );
    const projectionAt = calls.findIndex((call) =>
      call.text.includes("WITH live AS"),
    );
    expect(extAt).toBeGreaterThan(-1);
    expect(repairAt).toBeGreaterThan(extAt);
    expect(projectionAt).toBeGreaterThan(repairAt);
    expect(calls[extAt]!.params?.[8]).toBe("user-real");
    expect(calls[repairAt]!.params?.slice(6)).toEqual([
      "user-real",
      "kimi:session",
      "machine:encoded",
    ]);
  });
});
