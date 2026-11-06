/**
 * The workspace keyring for summary objects (upstream leaves key custody to the
 * deployment and ships only `unconfiguredKeyring`). Self-hostable: no cloud KMS
 * is required.
 *
 * Each workspace's AES-256-GCM data key is derived with HKDF-SHA256 from a
 * deployment master key, so no per-workspace key is stored anywhere:
 *
 *   key = HKDF-SHA256(ikm = master, salt = "96bulls/summary-keyring/v1",
 *                     info = "<orgId>\n<workspaceId>")
 *
 * The key id written into each envelope is `hkdf1:<masterId>`, so a reader
 * knows which master to derive from. Rotation: put a new master first in
 * SUMMARY_MASTER_KEYS and keep the old one after it until every summary has
 * been rewritten; removing a master crypto-shreds what it wrote.
 *
 * SUMMARY_MASTER_KEYS = "<id>:<64 hex chars>[,<id>:<64 hex chars>...]", the first
 * one current (or SUMMARY_MASTER_KEYS_FILE, a path holding the same value). It comes from a Secret (1Password), never from code or values.
 * Keys are imported non-extractable and are never logged.
 */
import { readFileSync } from "node:fs";
import { PipelineError } from "./vendor.ts";
import type { WorkspaceIds, WorkspaceKeyring } from "../bob-costas/summary-objects.ts";

export const KEY_ID_PREFIX = "hkdf1:";
const SALT = new TextEncoder().encode("96bulls/summary-keyring/v1");
const MASTER_ID = /^[a-z0-9][a-z0-9-]{0,31}$/;

interface Master {
  id: string;
  ikm: CryptoKey;
}

export function parseMasterKeys(raw: string): Array<{ id: string; bytes: Uint8Array }> {
  const entries = raw.split(",").map((s) => s.trim()).filter(Boolean);
  if (entries.length === 0) throw new Error("96bulls: SUMMARY_MASTER_KEYS is empty");
  const seen = new Set<string>();
  return entries.map((entry, i) => {
    const sep = entry.indexOf(":");
    const id = sep > 0 ? entry.slice(0, sep) : "";
    const hex = sep > 0 ? entry.slice(sep + 1) : "";
    // Never echo the entry: it holds key material.
    if (!MASTER_ID.test(id)) throw new Error(`96bulls: SUMMARY_MASTER_KEYS entry ${i + 1} has an invalid id (want [a-z0-9-], up to 32)`);
    if (!/^[0-9a-f]{64}$/.test(hex)) throw new Error(`96bulls: SUMMARY_MASTER_KEYS entry ${i + 1} (${id}) must be 64 lowercase hex characters (32 bytes)`);
    if (seen.has(id)) throw new Error(`96bulls: SUMMARY_MASTER_KEYS repeats id ${id}`);
    seen.add(id);
    return { id, bytes: Uint8Array.from(hex.match(/../g)!, (b) => parseInt(b, 16)) };
  });
}

export async function masterKeyring(keys: Array<{ id: string; bytes: Uint8Array }>): Promise<WorkspaceKeyring> {
  const masters: Master[] = [];
  for (const k of keys)
    masters.push({ id: k.id, ikm: await crypto.subtle.importKey("raw", k.bytes as Uint8Array<ArrayBuffer>, "HKDF", false, ["deriveKey"]) });
  const byId = new Map(masters.map((m) => [m.id, m]));
  const cache = new Map<string, Promise<CryptoKey>>();
  const derive = (m: Master, ids: WorkspaceIds): Promise<CryptoKey> => {
    const cacheKey = `${m.id}\n${ids.orgId}\n${ids.workspaceId}`;
    let p = cache.get(cacheKey);
    if (!p) {
      p = crypto.subtle.deriveKey(
        { name: "HKDF", hash: "SHA-256", salt: SALT, info: new TextEncoder().encode(`${ids.orgId}\n${ids.workspaceId}`) },
        m.ikm,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"],
      );
      cache.set(cacheKey, p);
      p.catch(() => cache.delete(cacheKey));
    }
    return p;
  };
  const current = masters[0]!;
  return {
    async currentKey(ids) {
      return { keyId: `${KEY_ID_PREFIX}${current.id}`, key: await derive(current, ids) };
    },
    async keyById(ids, keyId) {
      const m = keyId.startsWith(KEY_ID_PREFIX) ? byId.get(keyId.slice(KEY_ID_PREFIX.length)) : undefined;
      if (!m) throw new PipelineError("keyring_unknown_key", "the summary was written under a key this deployment does not hold", false);
      return derive(m, ids);
    },
  };
}

/** The keyring from SUMMARY_MASTER_KEYS. Bob Costas refuses to start without one, so no summary is ever stored unencrypted. */
export async function keyringFromEnv(env: Record<string, string | undefined> = process.env): Promise<WorkspaceKeyring> {
  // A mounted file (local stack: tmpfs; Kubernetes: a Secret volume) or the env var.
  const file = env.SUMMARY_MASTER_KEYS_FILE?.trim();
  const raw = (file ? readFileSync(file, "utf8") : env.SUMMARY_MASTER_KEYS)?.trim();
  if (!raw) throw new Error("96bulls: SUMMARY_MASTER_KEYS is required: summaries are stored encrypted in the Locker (see docs/aws-stage.md)");
  return masterKeyring(parseMasterKeys(raw));
}
