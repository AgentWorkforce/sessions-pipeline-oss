/** SQL readers shared by every lane (function signatures and GRANT EXECUTE statements). */
const TYPE_ALIASES: Record<string, string> = {
  timestamptz: "timestamp with time zone",
  "timestamptz[]": "timestamp with time zone[]",
  int: "integer",
};

/** CREATE [OR REPLACE] FUNCTION sessions.x(args) ... AS $tag$: signature -> SECURITY DEFINER. */
export function functionDefinitions(sql: string): Map<string, boolean> {
  const defs = new Map<string, boolean>();
  for (const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)) {
    const types = m[2]!.trim()
      ? m[2]!.split(",").map((p) => {
          const t = p.trim().replace(/\s+DEFAULT\s+[\s\S]*$/i, "").split(/\s+/).slice(1).join(" ");
          return TYPE_ALIASES[t] ?? t;
        }).join(",")
      : "";
    const sig = `${m[1]}(${types})`;
    if (defs.has(sig)) throw Error(`function defined twice in one lane source: ${sig}`);
    defs.set(sig, /SECURITY DEFINER/i.test(m[3]!));
  }
  return defs;
}

/** GRANT EXECUTE ON FUNCTION a(..),b(..) TO role; for the given roles, in statement order. */
export function functionGrants(sql: string, roles: readonly string[]): Array<[string, string]> {
  const out: Array<[string, string]> = [];
  if (!roles.length) return out;
  const alt = roles.map((r) => r.replace(/[^a-z0-9_]/gi, "")).join("|");
  const re = new RegExp(`GRANT EXECUTE ON FUNCTION([\\s\\S]*?)TO\\s+(${alt})\\s*;`, "gi");
  for (const m of sql.matchAll(re))
    for (const raw of m[1]!.replace(/\s+/g, "").split(/,(?=sessions\.)/))
      out.push([raw.replace(/timestamptz/g, "timestamp with time zone"), m[2]!]);
  return out;
}

/** Every role any GRANT EXECUTE ON FUNCTION statement in this text grants to. */
export function functionGrantees(sql: string): Set<string> {
  const roles = new Set<string>();
  for (const m of sql.matchAll(/GRANT EXECUTE ON FUNCTION[\s\S]*?\bTO\s+([^;]+);/gi))
    for (const r of m[1]!.split(",")) roles.add(r.trim().replace(/^"|"$/g, ""));
  return roles;
}
