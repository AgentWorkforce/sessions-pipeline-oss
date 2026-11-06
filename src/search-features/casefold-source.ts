/** Readonly source case mapping for a background publication configuration.
 * No model/key/client/environment access; no feature base-table reads.
 * Refuses contextual or multi-scalar folds instead of guessing a runtime locale. */
import { createHash } from "node:crypto";
import type { FeatureReader } from "./source.ts";
import { BriefFeatureError } from "./brief-publisher.ts";
export interface LegacyCasefoldProfile {
  sourceHash: string;
  source: Record<string, string>;
  entries: [string, string][];
}
export async function readLegacyCasefoldProfile(
  db: FeatureReader,
): Promise<LegacyCasefoldProfile> {
  try {
    const metadata = async () => {
      const r = (
        await db.query<Record<string, string>>(
          "SELECT datcollate,datctype,datlocprovider,coalesce(to_jsonb(d)->>'datlocale','') datlocale FROM pg_catalog.pg_database d WHERE datname=current_database()",
        )
      ).rows;
      if (r.length !== 1) throw new BriefFeatureError("casefold_profile");
      return r[0]!;
    };
    const source = await metadata(),
      entries: [string, string][] = [];
    if (
      !["datcollate", "datctype", "datlocprovider"].every(
        (k) => typeof source[k] === "string" && source[k],
      )
    )
      throw new BriefFeatureError("casefold_profile");
    if (typeof source.datlocale !== "string")
      throw new BriefFeatureError("casefold_profile");
    if (
      !(
        (source.datlocprovider === "c" && source.datlocale === "") ||
        (source.datlocprovider === "b" &&
          source.datcollate === "C.UTF-8" &&
          source.datctype === "C.UTF-8" &&
          source.datlocale === "C.UTF-8")
      )
    )
      throw new BriefFeatureError("casefold_contextual");
    for (let start = 1; start <= 0x10ffff; start += 8192) {
      const chars: string[] = [];
      for (let n = start; n < Math.min(start + 8192, 0x110000); n++)
        if (n < 0xd800 || n > 0xdfff) chars.push(String.fromCodePoint(n));
      const result = (
        await db.query<{ folded: string }>("SELECT lower($1::text) folded", [
          chars.join(""),
        ])
      ).rows;
      if (result.length !== 1 || typeof result[0]!.folded !== "string")
        throw new BriefFeatureError("casefold_profile");
      const lower = Array.from(result[0]!.folded);
      if (lower.length !== chars.length)
        throw new BriefFeatureError("casefold_nonsimple");
      for (let n = 0; n < chars.length; n++)
        if (chars[n] !== lower[n]) entries.push([chars[n]!, lower[n]!]);
    }
    if (JSON.stringify(await metadata()) !== JSON.stringify(source))
      throw new BriefFeatureError("casefold_changed");
    const sourceHash = createHash("sha256")
      .update(JSON.stringify({ source, entries }))
      .digest("hex");
    return { sourceHash, source, entries };
  } catch (e) {
    if (e instanceof BriefFeatureError) throw e;
    throw new BriefFeatureError("casefold_source_unavailable");
  }
}
export function foldLegacyQuery(
  profile: LegacyCasefoldProfile,
  text: string,
): string {
  const map = new Map(profile.entries);
  return Array.from(text, (c) => map.get(c) ?? c).join("");
}
