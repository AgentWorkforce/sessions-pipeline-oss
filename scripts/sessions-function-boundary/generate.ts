/**
 * Pure composition of every function-boundary lane into the shared artifacts. No IO besides the
 * injected reader; the same inputs always give byte-identical outputs, whatever the lane order.
 */
import { createHash } from "node:crypto";
import { functionDefinitions, functionGrantees, functionGrants } from "./sql.ts";
import type { LaneManifest, VerifierTarget } from "./types.ts";

export const ACL_PATH = "src/hoopdb/sessions-function-acl.json";
export const WRAPPER_PATH = "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql";
export const GUARDS_PATH = "src/hoopdb/migrate-guards.ts";

export type Reader = (path: string) => Promise<string | undefined>;

export interface LaneSummary { functions: number; replaces: number; grants: number }
export interface Generated {
  outputs: Record<string, string>;
  summary: { lanes: Record<string, LaneSummary>; boundarySHA: string };
}

export class BoundaryError extends Error {}
const fail = (msg: string): never => { throw new BoundaryError(msg); };
const byCodeUnit = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const sameSet = (a: readonly string[], b: readonly string[]) =>
  a.length === b.length && [...a].sort(byCodeUnit).join("\n") === [...b].sort(byCodeUnit).join("\n");

/** Every function has exactly one owning lane; prefixes may not overlap another lane's claims. */
export function checkOwnership(lanes: readonly LaneManifest[]): Map<string, string> {
  const owner = new Map<string, string>();
  const names = new Set<string>();
  for (const l of lanes) {
    if (names.has(l.lane)) fail(`duplicate lane name: ${l.lane}`);
    names.add(l.lane);
    if (l.migration === null && (Object.keys(l.functions).length || Object.keys(l.replaces).length || l.prefixes.length || l.extraOutputs))
      fail(`empty slot ${l.lane} must not claim functions, prefixes or outputs before its migration is set`);
    for (const sig of Object.keys(l.functions)) {
      const prev = owner.get(sig);
      if (prev) fail(`function ownership conflict: ${sig} is claimed by lanes ${prev} and ${l.lane}`);
      owner.set(sig, l.lane);
    }
  }
  for (const l of lanes)
    for (const p of l.prefixes) {
      for (const other of lanes) {
        if (other === l) continue;
        for (const q of other.prefixes)
          if (p.startsWith(q) || q.startsWith(p)) fail(`function ownership conflict: prefixes ${p} (${l.lane}) and ${q} (${other.lane}) overlap`);
        for (const sig of Object.keys(other.functions))
          if (sig.startsWith(p)) fail(`function ownership conflict: ${sig} (${other.lane}) falls under prefix ${p} of lane ${l.lane}`);
      }
    }
  return owner;
}

/** The lane's SQL must define exactly its manifest (owned + replaced), with the same SECDEF and grants. */
async function checkLaneSource(l: LaneManifest, read: Reader) {
  if (l.migration === null) return { sql: "", defined: [] as string[] };
  const sql = (await read(l.migration)) ?? fail(`${l.lane}: migration ${l.migration} is missing`);
  const defs = functionDefinitions(sql);
  const expected = new Map<string, boolean>();
  for (const [sig, f] of Object.entries(l.functions)) expected.set(sig, f.secdef);
  for (const [sig, f] of Object.entries(l.replaces)) {
    if (expected.has(sig)) fail(`${l.lane}: ${sig} is both owned and replaced`);
    expected.set(sig, f.secdef);
  }
  for (const sig of defs.keys()) if (!expected.has(sig)) fail(`manifest drift (${l.lane}): ${l.migration} defines ${sig}, which the manifest does not list`);
  for (const [sig, secdef] of expected) {
    if (!defs.has(sig)) fail(`manifest drift (${l.lane}): the manifest lists ${sig}, which ${l.migration} does not define`);
    if (defs.get(sig) !== secdef) fail(`manifest drift (${l.lane}): ${sig} SECURITY DEFINER is ${defs.get(sig)} in SQL, ${secdef} in the manifest`);
  }
  for (const sig of Object.keys(l.functions))
    if (!l.prefixes.some((p) => sig.startsWith(p))) fail(`${l.lane}: owned ${sig} is outside the lane's prefixes`);
  // Every grantee of the lane migration must be a declared one: an unlisted role (anon, authenticated,
  // a new worker role) is never silently left out of the reviewed manifest.
  const own = l.grantSources.find((s) => s.path === l.migration);
  for (const role of functionGrantees(sql))
    if (!own?.roles.includes(role)) fail(`manifest drift (${l.lane}): ${l.migration} grants EXECUTE to ${role}, which the lane's grant sources do not declare`);
  const granted = new Map<string, string[]>();
  for (const src of l.grantSources) {
    const text = await read(src.path);
    if (text === undefined) { if (src.optional) continue; fail(`${l.lane}: grant source ${src.path} is missing`); }
    for (const [sig, role] of functionGrants(text!, src.roles)) {
      if (!defs.has(sig)) { if (src.path === l.migration) fail(`${l.lane}: grant outside the lane's SQL authority: ${sig}`); continue; }
      if (sig in l.replaces) fail(`${l.lane}: ${sig} is replaced, not owned; its grants belong to its owner`);
      granted.set(sig, [...(granted.get(sig) ?? []), role]);
    }
  }
  for (const [sig, f] of Object.entries(l.functions))
    if (!sameSet(f.grants, granted.get(sig) ?? [])) fail(`manifest drift (${l.lane}): ${sig} grants are [${(granted.get(sig) ?? []).join(",")}] in SQL, [${f.grants.join(",")}] in the manifest`);
  return { sql, defined: [...defs.keys()] };
}

const listBlock = (text: string, re: RegExp, what: string) => text.match(re) ?? fail(`${what} source missing`);
const quotedItems = (block: string, q: string) => [...block.matchAll(new RegExp(`${q}([^${q}]+)${q}`, "g"))].map((m) => m[1]!);

export async function generate(read: Reader, lanes: readonly LaneManifest[], verifiers: readonly VerifierTarget[]): Promise<Generated> {
  checkOwnership(lanes);
  const sources = new Map<string, { sql: string; defined: string[] }>();
  for (const l of lanes) sources.set(l.lane, await checkLaneSource(l, read));

  const ownedOrPrefixed = (sig: string) => lanes.some((l) => sig in l.functions || l.prefixes.some((p) => sig.startsWith(p)));
  const laneDefined = (sig: string) => lanes.some((l) => sig in l.functions || sig in l.replaces);

  // ACL manifest: lane-owned entries are regenerated; every other entry (hand-maintained or owned
  // by an older single-purpose generator) is kept as is.
  const acl = JSON.parse((await read(ACL_PATH)) ?? fail(`${ACL_PATH} missing`)) as { functions: Record<string, string[]> };
  for (const key of Object.keys(acl.functions)) if (ownedOrPrefixed(key)) delete acl.functions[key];
  for (const l of lanes) {
    for (const [sig, f] of Object.entries(l.functions)) acl.functions[sig] = ["<owner>:EXECUTE", ...f.grants.map((r) => r + ":EXECUTE")];
    for (const sig of Object.keys(l.replaces)) if (!acl.functions[sig]) fail(`${l.lane}: replaced function ${sig} has no original ACL entry`);
  }
  acl.functions = Object.fromEntries(Object.entries(acl.functions).sort(([a], [b]) => byCodeUnit(a, b)));
  const runtimeNew = lanes.flatMap((l) => Object.entries(l.functions).filter(([, f]) => f.grants.includes("service_role")).map(([s]) => s));

  let wrapper = (await read(WRAPPER_PATH)) ?? fail(`${WRAPPER_PATH} missing`);
  wrapper = wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,
    () => "v_manifest jsonb := $manifest$" + JSON.stringify(acl.functions) + "$manifest$::jsonb;");
  const secdefBlock = listBlock(wrapper, /v_secdef text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/, "boundary SECDEF");
  const secdefs = quotedItems(secdefBlock[1]!, "'").filter((k) => !laneDefined(k));
  for (const l of lanes) {
    for (const [sig, f] of Object.entries(l.functions)) if (f.secdef) secdefs.push(sig);
    for (const [sig, f] of Object.entries(l.replaces)) if (f.secdef) secdefs.push(sig);
  }
  wrapper = wrapper.replace(secdefBlock[0], "v_secdef text[] := ARRAY[\n" + [...new Set(secdefs)].sort().map((k) => "    '" + k + "'").join(",\n") + "\n  ]::text[];");
  const runtimeBlock = listBlock(wrapper, /v_runtime_fns text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/, "boundary runtime manifest");
  const runtime = quotedItems(runtimeBlock[1]!, "'").filter((k) => !ownedOrPrefixed(k));
  wrapper = wrapper.replace(runtimeBlock[0], "v_runtime_fns text[] := ARRAY[\n" + [...new Set([...runtime, ...runtimeNew])].sort().map((k) => "    '" + k + "'").join(",\n") + "\n  ]::text[];");
  const body = wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1] ?? fail("boundary body missing");
  const boundarySHA = createHash("sha256").update(body).digest("hex");

  let guards = (await read(GUARDS_PATH)) ?? fail(`${GUARDS_PATH} missing`);
  const guardRuntime = listBlock(guards, /export const RUNTIME_APPLICATION_FUNCTIONS = \[([\s\S]*?)\] as const;/, "runtime application");
  const guardSigs = quotedItems(guardRuntime[1]!, '"').filter((k) => !ownedOrPrefixed(k));
  guards = guards.replace(guardRuntime[0], "export const RUNTIME_APPLICATION_FUNCTIONS = [\n" + [...new Set([...guardSigs, ...runtimeNew])].sort().map((k) => "  " + JSON.stringify(k)).join(",\n") + ",\n] as const;");
  guards = guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/, "$1" + JSON.stringify(boundarySHA));

  const outputs: Record<string, string> = {
    [ACL_PATH]: JSON.stringify(acl, null, 2) + "\n",
    [WRAPPER_PATH]: wrapper,
    [GUARDS_PATH]: guards,
  };
  for (const v of verifiers) {
    if (v.path in outputs) fail(`verifier ${v.path} is configured twice`);
    const source = v.template ?? v.path;
    const text = (await read(source)) ?? fail(`verifier ${source} missing`);
    if (!v.pattern.test(text)) fail(`verifier ${source}: approved-function array missing`);
    const sigs = Object.entries(acl.functions).filter(([, g]) => g.includes(v.role + ":EXECUTE")).map(([k]) => k).sort();
    const rendered = text.replace(v.pattern, () => v.render(sigs));
    outputs[v.path] = v.transform ? v.transform(rendered) : rendered;
  }
  for (const l of lanes) {
    if (!l.extraOutputs) continue;
    const src = sources.get(l.lane)!;
    const extra = await l.extraOutputs({ read: async (p) => (await read(p)) ?? fail(`${l.lane}: ${p} missing`), sql: src.sql, defined: src.defined, lane: l });
    for (const [p, text] of Object.entries(extra)) {
      if (p in outputs) fail(`${l.lane}: output ${p} is already generated by another lane or the shared boundary`);
      outputs[p] = text;
    }
  }
  const lanesSummary: Record<string, LaneSummary> = {};
  for (const l of [...lanes].sort((a, b) => byCodeUnit(a.lane, b.lane)))
    lanesSummary[l.lane] = {
      functions: Object.keys(l.functions).length,
      replaces: Object.keys(l.replaces).length,
      grants: Object.values(l.functions).reduce((n, f) => n + f.grants.length, 0),
    };
  return { outputs, summary: { lanes: lanesSummary, boundarySHA } };
}
