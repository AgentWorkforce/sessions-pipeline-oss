import { describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { keyringFromEnv, parseMasterKeys } from "../keyring.ts";

describe("summary keyring config", () => {
  it("parses id:hex entries, first is current", () => {
    const k = parseMasterKeys(`k2:${"b".repeat(64)}, k1:${"a".repeat(64)}`);
    expect(k.map((e) => e.id)).toEqual(["k2", "k1"]);
    expect(k[0]!.bytes.length).toBe(32);
  });
  it("rejects bad ids, bad key material and duplicates without echoing the key", () => {
    const secret = "z".repeat(64);
    expect(() => parseMasterKeys(`k1:${secret}`)).toThrow(/64 lowercase hex/);
    try { parseMasterKeys(`k1:${secret}`); } catch (e) { expect(String(e)).not.toContain(secret); }
    expect(() => parseMasterKeys(`K!:${"a".repeat(64)}`)).toThrow(/invalid id/);
    expect(() => parseMasterKeys(`k1:${"a".repeat(64)},k1:${"b".repeat(64)}`)).toThrow(/repeats/);
  });
  it("reads SUMMARY_MASTER_KEYS_FILE (a mounted tmpfs file or Secret volume)", async () => {
    const f = join(mkdtempSync(join(tmpdir(), "kr-")), "keys");
    writeFileSync(f, `local-1:${"c".repeat(64)}\n`, { mode: 0o600 });
    expect((await (await keyringFromEnv({ SUMMARY_MASTER_KEYS_FILE: f })).currentKey({ orgId: "o", workspaceId: "w" })).keyId).toBe("hkdf1:local-1");
  });
  it("refuses to run without SUMMARY_MASTER_KEYS", async () => {
    await expect(keyringFromEnv({})).rejects.toThrow(/SUMMARY_MASTER_KEYS is required/);
  });
  it("derives different keys per workspace (ciphertext from one cannot open in another)", async () => {
    const kr = await keyringFromEnv({ SUMMARY_MASTER_KEYS: `k1:${"a".repeat(64)}` });
    const a = await kr.currentKey({ orgId: "o", workspaceId: "w1" });
    const b = await kr.currentKey({ orgId: "o", workspaceId: "w2" });
    const iv = new Uint8Array(12);
    const ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, a.key, new TextEncoder().encode("x"));
    await expect(crypto.subtle.decrypt({ name: "AES-GCM", iv }, b.key, ct)).rejects.toBeDefined();
  });
});
