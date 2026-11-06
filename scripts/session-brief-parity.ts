/**
 * Session brief parity: Neon (relayhistory's session_analysis_jobs kind 'brief') against
 * Supabase (sessions.session_briefs). Prints counts and similarity only, never a title,
 * a description or a session id.
 *
 *   bun scripts/session-brief-parity.ts <neon.psv> <supabase.psv>
 *
 * Each input is `org_id | session_id | title | description`, one row per session, as
 * `psql -XAt -F' | '` prints it (e.g. the read-only ~/.agent-tmp/neon-inv/q/ro.sh, run
 * with umask 077 into a scratch file that is deleted after). Rows are joined on
 * (org_id, session_id); a session with several workspaces/harnesses on either side is
 * compared once per key and counted as `ambiguous`.
 *
 * Similarity is token Jaccard over lowercase word tokens; exact = equal after trimming.
 * Model output is not deterministic, so parity is judged on the distribution (median and
 * share above 0.5), not on exact equality.
 */
import { readFileSync } from "node:fs";

type Row = { title: string; description: string };

export function parse(text: string): { rows: Map<string, Row>; ambiguous: number } {
  const rows = new Map<string, Row>();
  const seen = new Map<string, number>();
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    const parts = line.split(" | ");
    if (parts.length < 4) continue;
    const [org, session, title, ...rest] = parts;
    const key = `${org}\u0000${session}`;
    seen.set(key, (seen.get(key) ?? 0) + 1);
    rows.set(key, { title: title!.trim(), description: rest.join(" | ").trim() });
  }
  let ambiguous = 0;
  for (const [key, n] of seen)
    if (n > 1) {
      ambiguous += 1;
      rows.delete(key);
    }
  return { rows, ambiguous };
}

const tokens = (s: string) => new Set(s.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? []);

export function jaccard(a: string, b: string): number {
  const x = tokens(a);
  const y = tokens(b);
  if (!x.size && !y.size) return 1;
  let inter = 0;
  for (const t of x) if (y.has(t)) inter += 1;
  return inter / (x.size + y.size - inter);
}

const quantile = (v: number[], q: number) => {
  if (!v.length) return null;
  const s = [...v].sort((a, b) => a - b);
  return Number(s[Math.min(s.length - 1, Math.floor(q * s.length))]!.toFixed(3));
};

export function compare(neon: Map<string, Row>, supabase: Map<string, Row>) {
  const title: number[] = [];
  const description: number[] = [];
  let exactTitle = 0;
  for (const [key, n] of neon) {
    const s = supabase.get(key);
    if (!s) continue;
    title.push(jaccard(n.title, s.title));
    description.push(jaccard(n.description, s.description));
    if (n.title === s.title) exactTitle += 1;
  }
  const share = (v: number[], min: number) =>
    v.length ? Number((v.filter((x) => x >= min).length / v.length).toFixed(3)) : null;
  return {
    neon: neon.size,
    supabase: supabase.size,
    both: title.length,
    neon_only: [...neon.keys()].filter((k) => !supabase.has(k)).length,
    supabase_only: [...supabase.keys()].filter((k) => !neon.has(k)).length,
    title_exact: exactTitle,
    title_jaccard: { p25: quantile(title, 0.25), median: quantile(title, 0.5), p75: quantile(title, 0.75), share_ge_0_5: share(title, 0.5) },
    description_jaccard: {
      p25: quantile(description, 0.25),
      median: quantile(description, 0.5),
      p75: quantile(description, 0.75),
      share_ge_0_5: share(description, 0.5),
    },
  };
}

if (import.meta.main) {
  const [neonPath, supabasePath] = process.argv.slice(2);
  if (!neonPath || !supabasePath) {
    console.error("usage: bun scripts/session-brief-parity.ts <neon.psv> <supabase.psv>");
    process.exit(2);
  }
  const neon = parse(readFileSync(neonPath, "utf8"));
  const supabase = parse(readFileSync(supabasePath, "utf8"));
  console.log(
    JSON.stringify(
      { ...compare(neon.rows, supabase.rows), ambiguous: { neon: neon.ambiguous, supabase: supabase.ambiguous } },
      null,
      2,
    ),
  );
}
