/**
 * Write guard for the migration tools that change BULLS_TARGET_URL
 * (migration:copy, migration:rebuild). Without --execute they run read-only.
 * With it, the target must positively identify as the expected database:
 *
 *   --expect-supabase-ref=<ref>   the target URL must be that Supabase project,
 *                                 by direct host db.<ref>.supabase.co or by
 *                                 pooler user <role>.<ref>;
 *   --expect-target-host=<host>   only for local rehearsals: the exact host,
 *                                 never a Neon or Supabase domain.
 *
 * The source (when there is one) may not be a Supabase URL or the target's
 * host, so swapped URLs can never write to Neon.
 */
export type WriteMode = "dry-run" | "execute";

const REF = /^[a-z0-9]{20}$/;

export function supabaseProjectRef(value: string): string | null {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  const direct = /^db\.([a-z0-9]{20})\.supabase\.co$/.exec(url.hostname);
  if (direct) return direct[1]!;
  if (!/^[a-z0-9-]+\.pooler\.supabase\.com$/.test(url.hostname)) return null;
  const pooled = /^[a-z_][a-z0-9_]*\.([a-z0-9]{20})$/.exec(decodeURIComponent(url.username));
  return pooled ? pooled[1]! : null;
}

const hostOf = (value: string) => {
  try {
    return new URL(value).hostname;
  } catch {
    return null;
  }
};
const managedHost = (host: string) =>
  /(^|\.)neon\.tech$/.test(host) || /(^|\.)supabase\.(co|com)$/.test(host);

export interface GuardInput {
  targetUrl: string;
  sourceUrl?: string;
  execute: boolean;
  expectSupabaseRef?: string;
  expectTargetHost?: string;
}

/** Throws, without echoing URLs, unless the run is a dry run or a positively identified target. */
export function writeMode(input: GuardInput): WriteMode {
  if (!input.execute) return "dry-run";
  const targetHost = hostOf(input.targetUrl);
  if (!targetHost) throw new Error("BULLS_TARGET_URL is not a URL");
  if (input.sourceUrl !== undefined) {
    if (supabaseProjectRef(input.sourceUrl) !== null)
      throw new Error("the source is a Supabase URL: refusing to write (swapped URLs?)");
    if (hostOf(input.sourceUrl) === targetHost)
      throw new Error("source and target are the same host: refusing to write");
  }
  if (input.expectSupabaseRef !== undefined) {
    if (!REF.test(input.expectSupabaseRef))
      throw new Error("--expect-supabase-ref is not a Supabase project ref");
    if (supabaseProjectRef(input.targetUrl) !== input.expectSupabaseRef)
      throw new Error("the target is not the expected Supabase project: refusing to write");
    return "execute";
  }
  if (input.expectTargetHost !== undefined) {
    if (managedHost(targetHost))
      throw new Error("--expect-target-host is for local rehearsals only; use --expect-supabase-ref");
    if (targetHost !== input.expectTargetHost)
      throw new Error("the target is not the expected host: refusing to write");
    return "execute";
  }
  throw new Error("--execute needs --expect-supabase-ref (or --expect-target-host for a rehearsal)");
}

/** Parse the guard flags from argv. */
export function guardFromArgv(argv: string[], targetUrl: string, sourceUrl?: string): WriteMode {
  const flag = (name: string) =>
    argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
  return writeMode({
    targetUrl,
    ...(sourceUrl === undefined ? {} : { sourceUrl }),
    execute: argv.includes("--execute"),
    ...(flag("expect-supabase-ref") === undefined
      ? {}
      : { expectSupabaseRef: flag("expect-supabase-ref")! }),
    ...(flag("expect-target-host") === undefined
      ? {}
      : { expectTargetHost: flag("expect-target-host")! }),
  });
}
