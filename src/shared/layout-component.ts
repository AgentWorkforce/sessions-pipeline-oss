/**
 * A source id (session, workspace, machine, harness) as a v1 layout key
 * component: `[A-Za-z0-9._-]{1,128}`, not starting with `.`. One map for every
 * writer (the courier and the Neon export), so a live upload and an export of
 * the same session land on the same key.
 *
 * Ids over 128 chars keep 119 chars plus `-` and 8 hex of sha256(id): a plain
 * cut made ids differing only past char 128 collide (38 production sessions,
 * `pi:<64hex>:…:agent-N`). Ids that fit are unchanged.
 */
import { createHash } from "node:crypto";

export function layoutComponent(id: string): string {
  const safe = id.replace(/[^A-Za-z0-9._-]/g, "_").replace(/^\./, "_");
  if (safe.length <= 128) return safe;
  return `${safe.slice(0, 119)}-${createHash("sha256").update(id).digest("hex").slice(0, 8)}`;
}
