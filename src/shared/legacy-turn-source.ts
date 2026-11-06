/** Scoped public conversation-turn attribution. Mirrors RHC selectedSessionTurns,
 * not the overloaded stored source column. No I/O, query execution or owner access.
 * SQL fragments accept only trusted aliases/positional bind parameters. */
function ref(alias: string) {
  if (!/^[a-z_][a-z0-9_]{0,62}$/u.test(alias))
    throw new Error("turn source SQL alias invalid");
  return '"' + alias + '"';
}
function columns(alias: string) {
  if (alias.length > 32) throw new Error("turn source SQL alias too long");
  return ref(alias);
}
export function publicTurnSourceSql(alias: string): string {
  const t = columns(alias),
    e = ref(alias + "_owner_event");
  return `COALESCE(NULLIF(${t}.metadata->>'nativeCli',''),NULLIF(${t}.metadata->>'source',''),
 CASE WHEN ${t}.source<>'' AND ${t}.source<>${t}.session_owner THEN ${t}.source END,
 CASE WHEN EXISTS(SELECT 1 FROM sessions.convergence_events ${e}
  WHERE ${e}.org_id=${t}.org_id AND ${e}.workspace_id=${t}.workspace_id
   AND ${e}.session_id=${t}.session_id AND ${e}.source=${t}.session_owner)
 THEN ${t}.session_owner END)`;
}
/** Discovery may resolve an untagged turn from exactly one event source in the
 * same tenant/workspace/session. NULL is an explicit unbound coverage state. */
export function resolvedPublicTurnSourceSql(alias: string): string {
  const t = columns(alias),
    e = ref(alias + "_fallback_event");
  return `COALESCE(${publicTurnSourceSql(alias)},(SELECT min(${e}.source)
 FROM sessions.convergence_events ${e} WHERE ${e}.org_id=${t}.org_id
 AND ${e}.workspace_id=${t}.workspace_id AND ${e}.session_id=${t}.session_id
 HAVING count(DISTINCT ${e}.source)=1))`;
}
/** Exact current source-selected public replay rule, including the unambiguous
 * fallback. Callers separately enforce org/workspace/session and valid provider. */
export function selectedPublicTurnPredicateSql(
  alias: string,
  sourceParameter: string,
): string {
  if (!/^\$[1-9][0-9]*$/u.test(sourceParameter))
    throw new Error("turn source SQL parameter invalid");
  const t = columns(alias),
    e = ref(alias + "_other_event"),
    attributed = publicTurnSourceSql(alias);
  return `(${attributed}=${sourceParameter} OR (${attributed} IS NULL AND NOT EXISTS(
 SELECT 1 FROM sessions.convergence_events ${e} WHERE ${e}.org_id=${t}.org_id
 AND ${e}.workspace_id=${t}.workspace_id AND ${e}.session_id=${t}.session_id
 AND ${e}.source<>${sourceParameter})))`;
}

const orderEncoder = new TextEncoder();
function textOrder(a: unknown, b: unknown) {
  const x = orderEncoder.encode(String(a ?? "")),
    y = orderEncoder.encode(String(b ?? ""));
  for (let i = 0; i < Math.min(x.length, y.length); i++)
    if (x[i] !== y[i]) return x[i]! - y[i]!;
  return x.length - y.length;
}
/** The public turn tuple uses the persisted source label, not the attributed
 * harness. Exporters retain it privately when canonicalizing row.source. */
export function comparePublicTurnRows(
  a: Record<string, unknown>,
  b: Record<string, unknown>,
): number {
  const key = (r: Record<string, unknown>) =>
    r.legacy_stored_source ?? r.source;
  const id =
    /^[0-9]+$/u.test(String(a.id)) && /^[0-9]+$/u.test(String(b.id))
      ? BigInt(String(a.id)) < BigInt(String(b.id))
        ? -1
        : BigInt(String(a.id)) > BigInt(String(b.id))
          ? 1
          : 0
      : textOrder(a.id, b.id);
  return (
    Number(a.turn_index) - Number(b.turn_index) ||
    textOrder(a.workspace_id, b.workspace_id) ||
    textOrder(key(a), key(b)) ||
    id
  );
}
