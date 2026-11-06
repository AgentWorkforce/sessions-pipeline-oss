import {
  chmod,
  mkdir,
  mkdtemp,
  readdir,
  rm,
  symlink,
  unlink,
  utimes,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MAX_USER_METADATA_BYTES } from "../src/core.js";
import { FsTransport, createFsStore } from "../src/fs.js";
import { objectKey, readAll, sha256HexForTest } from "./helpers.js";

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(
    dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })),
  );
});

async function store(opts: { pageSize?: number } = {}) {
  const root = await mkdtemp(join(tmpdir(), "rh-objstore-fs-"));
  dirs.push(root);
  return { root, store: createFsStore({ root, ...opts }) };
}

const bytes = (s: string) => new TextEncoder().encode(s);

describe("fs sidecars", () => {
  it("repairs a lost sidecar on an idempotent retry", async () => {
    const { root, store: s } = await store();
    const key = objectKey("s/raw/0000000000-0-aaaaaaaa.jsonl.zst");
    await s.put(key, bytes("chunk"), {
      ifNoneMatch: true,
      metadata: { "trace-id": "t-1" },
    });
    await unlink(
      join(root, ".rh/meta/s/raw/0000000000-0-aaaaaaaa.jsonl.zst.json"),
    );
    expect((await s.head(key))?.metadata).toEqual({});
    const ack = await s.put(key, bytes("chunk"), {
      ifNoneMatch: true,
      metadata: { "trace-id": "t-1" },
    });
    expect(ack.created).toBe(false);
    expect((await s.head(key))?.metadata).toMatchObject({
      "trace-id": "t-1",
      "content-sha256": ack.contentSha256,
    });
  });

  it("a repair verified against an old version never overwrites a newer sidecar", async () => {
    const { root, store: s } = await store();
    const key = objectKey("s/manifest.json");
    const v1 = await s.put(key, bytes('{"v":1}'), { ifNoneMatch: true });
    const v2 = await s.put(key, bytes('{"v":2}'), {
      ifMatch: v1.version,
      metadata: { "trace-id": "t-2" },
    });
    // A repair verified against v1 arrives after v2 committed.
    await new FsTransport(root).repairMetadata(
      key,
      v1.version,
      {},
      "application/json",
    );
    expect((await s.head(key))?.metadata).toMatchObject({
      "trace-id": "t-2",
      "content-sha256": v2.contentSha256,
    });
  });

  it("never trusts a sidecar written for another version", async () => {
    const { root, store: s } = await store();
    const key = objectKey("s/manifest.json");
    await s.put(key, bytes("old!"), { ifNoneMatch: true });
    // Crash after replacing the body but before the sidecar (same length).
    // The rewrite happens later than the put; set its mtime explicitly, since
    // filesystems with coarse timestamps (some CI runners) can give both writes
    // the same mtime, and so the same inode-mtime-size ETag.
    const body = join(root, "s/manifest.json");
    await writeFile(body, "new!");
    const later = new Date(Date.now() + 60_000);
    await utimes(body, later, later);
    expect((await s.head(key))?.metadata).toEqual({});
    await expect(
      s.put(key, bytes("old!"), { ifNoneMatch: true }),
    ).rejects.toMatchObject({
      code: "already_exists",
    });
  });
});

describe("metadata limit", () => {
  it("leaves room for the adapter's digest", async () => {
    const { store: s } = await store();
    const fits = { k: "v".repeat(MAX_USER_METADATA_BYTES - 1) };
    await expect(
      s.put(objectKey("a/b"), bytes("x"), { metadata: fits }),
    ).resolves.toBeTruthy();
    const over = { k: "v".repeat(MAX_USER_METADATA_BYTES) };
    await expect(
      s.put(objectKey("a/c"), bytes("x"), { metadata: over }),
    ).rejects.toMatchObject({
      code: "invalid_argument",
    });
  });
});

describe("fs safety", () => {
  it("serializes creates with overwrites so a sidecar never describes another body", async () => {
    const { store: s } = await store();
    for (let i = 0; i < 40; i++) {
      const key = objectKey(`race/${i}`);
      await Promise.allSettled([
        s.put(key, bytes(`create-${i}`), { ifNoneMatch: true }),
        s.put(key, bytes(`overwrite-${i}`)),
      ]);
      const meta = await s.head(key);
      const body = await readAll(await s.get(key));
      const digest = meta?.metadata["content-sha256"];
      if (digest !== undefined)
        expect(digest).toBe(await sha256HexForTest(body));
    }
  });

  it("shares the write lock between instances on the same root", async () => {
    const root = await mkdtemp(join(tmpdir(), "rh-objstore-fs-"));
    dirs.push(root);
    const a = createFsStore({ root });
    const b = createFsStore({ root });
    const key = objectKey("m/manifest.json");
    const v1 = await a.put(key, bytes("1"), { ifNoneMatch: true });
    const results = await Promise.allSettled([
      a.put(key, bytes("a"), { ifMatch: v1.version }),
      b.put(key, bytes("b"), { ifMatch: v1.version }),
    ]);
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
  });

  it("refuses to follow a symlink out of the root", async () => {
    const { root, store: s } = await store();
    const outside = await mkdtemp(join(tmpdir(), "rh-objstore-outside-"));
    dirs.push(outside);
    await writeFile(join(outside, "secret"), "host file");
    await symlink(outside, join(root, "escape"));
    await expect(s.get(objectKey("escape/secret"))).rejects.toMatchObject({
      code: "invalid_key",
    });
    await expect(
      s.put(objectKey("escape/new"), bytes("x")),
    ).rejects.toMatchObject({
      code: "invalid_key",
    });
    expect(await readdir(outside)).toEqual(["secret"]);
  });

  it("does not leave staging files behind when a write fails", async () => {
    const { root, store: s } = await store();
    // A directory where the object should go makes the rename fail.
    await mkdir(join(root, "a", "b"), { recursive: true });
    await expect(s.put(objectKey("a/b"), bytes("x"))).rejects.toBeTruthy();
    expect(
      (await readdir(join(root, "a"))).filter((n) => n.includes("#")),
    ).toEqual([]);
  });
});

describe("fs listing and errors", () => {
  it("pages in key order without missing or repeating keys", async () => {
    const { store: s } = await store({ pageSize: 2 });
    const keys = ["p/a-c", "p/a/b", "p/a/z", "p/b", "p/a0", "p/c/d/e"];
    for (const k of keys)
      await s.put(objectKey(k), bytes(k), { ifNoneMatch: true });
    const seen: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await s.list(objectKey("p"), cursor);
      expect(page.items.length).toBeLessThanOrEqual(2);
      seen.push(...page.items.map((m) => m.key));
      cursor = page.nextCursor;
    } while (cursor);
    expect(seen).toEqual([...keys].sort());
  });

  it.skipIf(process.getuid?.() === 0)(
    "reports unreadable files as ObjectStoreError",
    async () => {
      const { root, store: s } = await store();
      const key = objectKey("locked/x");
      await s.put(key, bytes("x"));
      await chmod(join(root, "locked", "x"), 0o000);
      await expect(s.get(key)).rejects.toMatchObject({ code: "unauthorized" });
      await chmod(join(root, "locked", "x"), 0o600);
    },
  );

  it("rejects metadata values with edge spaces", async () => {
    const { store: s } = await store();
    await expect(
      s.put(objectKey("m/x"), bytes("x"), { metadata: { label: " draft " } }),
    ).rejects.toMatchObject({ code: "invalid_argument" });
  });
});
