import { createHash } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createLocalDestination } from "../src/destinations/local.js";
import type { DestinationDelivery } from "../src/destinations/index.js";

const roots: string[] = [];
async function temporaryRoot() {
  const root = await mkdtemp(join(tmpdir(), "sessions-destinations-"));
  roots.push(root);
  return root;
}
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

function delivery(overrides: Partial<DestinationDelivery> = {}): DestinationDelivery {
  return {
    schemaVersion: 1,
    deliveryId: "delivery-1",
    tenant: { orgId: "org-test", workspaceId: "workspace-test" },
    sessionId: "session-test",
    mediaType: "application/json",
    payload: new TextEncoder().encode('{"text":"synthetic session"}'),
    ...overrides,
  };
}

describe("local filesystem destination", () => {
  it("does no I/O until explicitly written, then persists exact bytes and a stored receipt", async () => {
    const root = join(await temporaryRoot(), "new-store");
    const destination = createLocalDestination({ id: "local", root });
    await expect(stat(root)).rejects.toMatchObject({ code: "ENOENT" });
    const input = delivery({ payload: Uint8Array.from([0, 1, 127, 255]) });
    const receipt = await destination.write(input);
    expect((await stat(root)).mode & 0o077).toBe(0);
    expect(receipt).toEqual({
      destinationId: "local", deliveryId: "delivery-1", acceptance: "stored", duplicate: false,
      contentSha256: createHash("sha256").update(input.payload).digest("hex"),
    });
    const objects = await readdir(join(root, "destinations/v1"));
    expect(objects).toHaveLength(1);
    const stored = JSON.parse(await readFile(join(root, "destinations/v1", objects[0]!), "utf8"));
    expect(Buffer.from(stored.payloadBase64, "base64")).toEqual(Buffer.from(input.payload));
    expect(stored.tenant).toEqual(input.tenant);
    expect(stored.contentSha256).toBe(receipt.contentSha256);
  });

  it("recovers a lost response through identical replay after recreating the adapter", async () => {
    const root = await temporaryRoot();
    const input = delivery();
    const first = await createLocalDestination({ id: "local", root }).write(input);
    const replay = await createLocalDestination({ id: "local", root }).write(input);
    expect(replay).toEqual({ ...first, duplicate: true });
  });

  it.each([
    { payload: new TextEncoder().encode("changed") },
    { sessionId: "another-session" },
    { mediaType: "text/plain" },
  ])("refuses conflicting bytes or metadata for one delivery identity: %j", async (change) => {
    const destination = createLocalDestination({ id: "local", root: await temporaryRoot() });
    await destination.write(delivery());
    await expect(destination.write(delivery(change))).rejects.toMatchObject({ code: "conflict", retryable: false });
    expect((await destination.write(delivery())).duplicate).toBe(true);
  });

  it("keeps identical delivery IDs isolated by both organization and workspace", async () => {
    const root = await temporaryRoot();
    const destination = createLocalDestination({ id: "local", root });
    for (const tenant of [
      { orgId: "one", workspaceId: "one" },
      { orgId: "two", workspaceId: "one" },
      { orgId: "one", workspaceId: "two" },
    ]) expect((await destination.write(delivery({ tenant }))).duplicate).toBe(false);
    expect(await readdir(join(root, "destinations/v1"))).toHaveLength(3);
  });

  it("hashes delivery identities instead of treating them as filesystem paths", async () => {
    const root = await temporaryRoot();
    const destination = createLocalDestination({ id: "local", root });
    await destination.write(delivery({ deliveryId: "../../outside", tenant: { orgId: "../tenant", workspaceId: "/etc" } }));
    expect(await readdir(join(root, "destinations/v1"))).toEqual([expect.stringMatching(/^[a-f0-9]{64}\.json$/)]);
  });

  it("takes a snapshot before awaiting writes", async () => {
    const root = await temporaryRoot();
    const destination = createLocalDestination({ id: "local", root });
    const input = delivery();
    const expectedHash = createHash("sha256").update(input.payload).digest("hex");
    const pending = destination.write(input);
    input.payload.fill(0);
    (input.tenant as { orgId: string }).orgId = "changed";
    expect((await pending).contentSha256).toBe(expectedHash);
    expect((await destination.write(delivery())).duplicate).toBe(true);
  });

  it("serializes concurrent conflicting creates and never overwrites the winner", async () => {
    const root = await temporaryRoot();
    const one = createLocalDestination({ id: "one", root });
    const two = createLocalDestination({ id: "two", root });
    const results = await Promise.allSettled([
      one.write(delivery()), two.write(delivery({ payload: new Uint8Array([99]) })),
    ]);
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(results.find((r) => r.status === "rejected")).toMatchObject({ reason: { code: "conflict" } });
  });

  it("rejects cancellation and oversized input before creating storage", async () => {
    const root = join(await temporaryRoot(), "not-created");
    const destination = createLocalDestination({ id: "local", root, maxPayloadBytes: 4 });
    await expect(destination.write(delivery())).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(destination.write(delivery({ payload: new Uint8Array([1]) }), { signal: AbortSignal.abort() }))
      .rejects.toMatchObject({ code: "aborted" });
    await expect(stat(root)).rejects.toMatchObject({ code: "ENOENT" });
    expect(() => createLocalDestination({ id: "local", root: "relative/path" })).toThrow();
  });

  it("never acknowledges a failed write or exposes backend paths in errors", async () => {
    const root = join(await temporaryRoot(), "not-a-directory");
    await writeFile(root, "occupied");
    const destination = createLocalDestination({ id: "local", root });
    await expect(destination.write(delivery())).rejects.toMatchObject({
      code: "storage_unavailable", message: "destination: storage_unavailable",
    });
  });

  it("refuses a permissive existing root without changing its permissions", async () => {
    const root = await temporaryRoot();
    await chmod(root, 0o755);
    const destination = createLocalDestination({ id: "local", root });
    await expect(destination.write(delivery())).rejects.toMatchObject({ code: "storage_unavailable" });
    expect((await stat(root)).mode & 0o777).toBe(0o755);
    expect(await readdir(root)).toEqual([]);
  });

  it("refuses a symlink store root without writing into its target", async () => {
    const base = await temporaryRoot();
    const target = join(base, "target");
    await mkdir(target, { mode: 0o700 });
    const root = join(base, "linked-store");
    await symlink(target, root);
    await expect(createLocalDestination({ id: "local", root }).write(delivery()))
      .rejects.toMatchObject({ code: "storage_unavailable" });
    expect(await readdir(target)).toEqual([]);
  });
});
