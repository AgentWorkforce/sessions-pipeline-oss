import { describe, expect, it, vi } from "vitest";
import { cachedProvider } from "../src/core.js";
import { errorForStatus } from "../src/errors.js";

describe("error classification", () => {
  it("maps statuses to contract codes", () => {
    expect(errorForStatus("s3", "k", 404, "").code).toBe("not_found");
    expect(errorForStatus("s3", "k", 412, "").code).toBe("precondition_failed");
    expect(errorForStatus("s3", "k", 403, "").code).toBe("unauthorized");
    for (const s of [408, 429, 500, 503])
      expect(errorForStatus("s3", "k", s, "").retryable).toBe(true);
    expect(errorForStatus("s3", "k", 400, "bad").retryable).toBe(false);
  });
});

describe("cachedProvider", () => {
  it("caches until the refresh window, then re-vends", async () => {
    let n = 0;
    const vend = vi.fn(async () => ({
      token: `t${n++}`,
      expiresAt: new Date(Date.now() + 60 * 60 * 1000),
    }));
    const get = cachedProvider(vend);
    expect((await get()).token).toBe("t0");
    expect((await get()).token).toBe("t0");
    expect(vend).toHaveBeenCalledTimes(1);

    const soon = vi.fn(async () => ({
      token: `s${n++}`,
      expiresAt: new Date(Date.now() + 2 * 60 * 1000),
    }));
    const getSoon = cachedProvider(soon);
    await getSoon();
    await getSoon();
    expect(soon).toHaveBeenCalledTimes(2);
  });

  it("shares one in-flight vend between concurrent callers", async () => {
    const vend = vi.fn(async () => ({
      token: "x",
      expiresAt: new Date(Date.now() + 3_600_000),
    }));
    const get = cachedProvider(vend);
    await Promise.all([get(), get(), get()]);
    expect(vend).toHaveBeenCalledTimes(1);
  });
});

describe("presign ttl", () => {
  it("rejects sub-second ttls that would sign an already-expired URL", async () => {
    const { mkdtemp } = await import("node:fs/promises");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const { createFsStore } = await import("../src/fs.js");
    const s = createFsStore({ root: await mkdtemp(join(tmpdir(), "rh-ttl-")) });
    await expect(s.presignGet("a" as never, 0.5)).rejects.toMatchObject({
      code: "invalid_argument",
    });
    await expect(s.presignGet("a" as never, 1)).resolves.toBeTruthy();
  });
});
