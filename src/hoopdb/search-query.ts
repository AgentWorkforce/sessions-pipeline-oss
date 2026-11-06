/**
 * One normalization rule for session search queries, shared by the TypeScript
 * caller and the SQL RPC (sessions.session_directory_search_query in
 * supabase/migrations/20261030020000_session_directory_short_summary.sql).
 *
 *   1. NFC.
 *   2. Remove every format character (General_Category Cf: ZWSP, ZWNJ, ZWJ, BOM,
 *      bidi controls, soft hyphen, ...).
 *   3. Replace each run of separators and controls (Z, Cc: NBSP, U+2000-U+200A,
 *      U+3000, tabs, newlines, ...) with one ASCII space, and trim.
 *   4. NFC again (removing a format character can expose a composable pair).
 *   5. Meaningful characters are code points that are neither that space nor a
 *      combining mark (M). At least 3 are required; the normalized query may hold
 *      at most 200 code points.
 *
 * The ranges are pinned data generated from Unicode 17.0 (ICU 78.1, bun 1.4.2) with
 * `/^[\p{Z}\p{Cc}]$/u`, `/^\p{Cf}$/u` and `/^\p{M}$/u` over every code point.
 * They are data, not engine lookups, so TypeScript and PostgreSQL apply the same
 * table; the migration embeds `pgClass(...)` of each, and a test pins that.
 */

export type CodePointRange = readonly [number, number];

export const SEARCH_QUERY_UNICODE_VERSION = "17.0";
export const SEARCH_QUERY_MIN_CHARS = 3;
export const SEARCH_QUERY_MAX_CHARS = 200;

/** Z (separators) plus Cc (controls). */
export const SEPARATOR_RANGES: readonly CodePointRange[] = [
  [0x0000, 0x0020],
  [0x007f, 0x00a0],
  [0x1680, 0x1680],
  [0x2000, 0x200a],
  [0x2028, 0x2029],
  [0x202f, 0x202f],
  [0x205f, 0x205f],
  [0x3000, 0x3000],
];

/** Cf (format characters). */
export const FORMAT_RANGES: readonly CodePointRange[] = [
  [0x00ad, 0x00ad],
  [0x0600, 0x0605],
  [0x061c, 0x061c],
  [0x06dd, 0x06dd],
  [0x070f, 0x070f],
  [0x0890, 0x0891],
  [0x08e2, 0x08e2],
  [0x180e, 0x180e],
  [0x200b, 0x200f],
  [0x202a, 0x202e],
  [0x2060, 0x2064],
  [0x2066, 0x206f],
  [0xfeff, 0xfeff],
  [0xfff9, 0xfffb],
  [0x110bd, 0x110bd],
  [0x110cd, 0x110cd],
  [0x13430, 0x1343f],
  [0x1bca0, 0x1bca3],
  [0x1d173, 0x1d17a],
  [0xe0001, 0xe0001],
  [0xe0020, 0xe007f],
];

/** M (combining marks: Mn, Mc, Me). */
export const MARK_RANGES: readonly CodePointRange[] = [
  [0x0300, 0x036f],
  [0x0483, 0x0489],
  [0x0591, 0x05bd],
  [0x05bf, 0x05bf],
  [0x05c1, 0x05c2],
  [0x05c4, 0x05c5],
  [0x05c7, 0x05c7],
  [0x0610, 0x061a],
  [0x064b, 0x065f],
  [0x0670, 0x0670],
  [0x06d6, 0x06dc],
  [0x06df, 0x06e4],
  [0x06e7, 0x06e8],
  [0x06ea, 0x06ed],
  [0x0711, 0x0711],
  [0x0730, 0x074a],
  [0x07a6, 0x07b0],
  [0x07eb, 0x07f3],
  [0x07fd, 0x07fd],
  [0x0816, 0x0819],
  [0x081b, 0x0823],
  [0x0825, 0x0827],
  [0x0829, 0x082d],
  [0x0859, 0x085b],
  [0x0897, 0x089f],
  [0x08ca, 0x08e1],
  [0x08e3, 0x0903],
  [0x093a, 0x093c],
  [0x093e, 0x094f],
  [0x0951, 0x0957],
  [0x0962, 0x0963],
  [0x0981, 0x0983],
  [0x09bc, 0x09bc],
  [0x09be, 0x09c4],
  [0x09c7, 0x09c8],
  [0x09cb, 0x09cd],
  [0x09d7, 0x09d7],
  [0x09e2, 0x09e3],
  [0x09fe, 0x09fe],
  [0x0a01, 0x0a03],
  [0x0a3c, 0x0a3c],
  [0x0a3e, 0x0a42],
  [0x0a47, 0x0a48],
  [0x0a4b, 0x0a4d],
  [0x0a51, 0x0a51],
  [0x0a70, 0x0a71],
  [0x0a75, 0x0a75],
  [0x0a81, 0x0a83],
  [0x0abc, 0x0abc],
  [0x0abe, 0x0ac5],
  [0x0ac7, 0x0ac9],
  [0x0acb, 0x0acd],
  [0x0ae2, 0x0ae3],
  [0x0afa, 0x0aff],
  [0x0b01, 0x0b03],
  [0x0b3c, 0x0b3c],
  [0x0b3e, 0x0b44],
  [0x0b47, 0x0b48],
  [0x0b4b, 0x0b4d],
  [0x0b55, 0x0b57],
  [0x0b62, 0x0b63],
  [0x0b82, 0x0b82],
  [0x0bbe, 0x0bc2],
  [0x0bc6, 0x0bc8],
  [0x0bca, 0x0bcd],
  [0x0bd7, 0x0bd7],
  [0x0c00, 0x0c04],
  [0x0c3c, 0x0c3c],
  [0x0c3e, 0x0c44],
  [0x0c46, 0x0c48],
  [0x0c4a, 0x0c4d],
  [0x0c55, 0x0c56],
  [0x0c62, 0x0c63],
  [0x0c81, 0x0c83],
  [0x0cbc, 0x0cbc],
  [0x0cbe, 0x0cc4],
  [0x0cc6, 0x0cc8],
  [0x0cca, 0x0ccd],
  [0x0cd5, 0x0cd6],
  [0x0ce2, 0x0ce3],
  [0x0cf3, 0x0cf3],
  [0x0d00, 0x0d03],
  [0x0d3b, 0x0d3c],
  [0x0d3e, 0x0d44],
  [0x0d46, 0x0d48],
  [0x0d4a, 0x0d4d],
  [0x0d57, 0x0d57],
  [0x0d62, 0x0d63],
  [0x0d81, 0x0d83],
  [0x0dca, 0x0dca],
  [0x0dcf, 0x0dd4],
  [0x0dd6, 0x0dd6],
  [0x0dd8, 0x0ddf],
  [0x0df2, 0x0df3],
  [0x0e31, 0x0e31],
  [0x0e34, 0x0e3a],
  [0x0e47, 0x0e4e],
  [0x0eb1, 0x0eb1],
  [0x0eb4, 0x0ebc],
  [0x0ec8, 0x0ece],
  [0x0f18, 0x0f19],
  [0x0f35, 0x0f35],
  [0x0f37, 0x0f37],
  [0x0f39, 0x0f39],
  [0x0f3e, 0x0f3f],
  [0x0f71, 0x0f84],
  [0x0f86, 0x0f87],
  [0x0f8d, 0x0f97],
  [0x0f99, 0x0fbc],
  [0x0fc6, 0x0fc6],
  [0x102b, 0x103e],
  [0x1056, 0x1059],
  [0x105e, 0x1060],
  [0x1062, 0x1064],
  [0x1067, 0x106d],
  [0x1071, 0x1074],
  [0x1082, 0x108d],
  [0x108f, 0x108f],
  [0x109a, 0x109d],
  [0x135d, 0x135f],
  [0x1712, 0x1715],
  [0x1732, 0x1734],
  [0x1752, 0x1753],
  [0x1772, 0x1773],
  [0x17b4, 0x17d3],
  [0x17dd, 0x17dd],
  [0x180b, 0x180d],
  [0x180f, 0x180f],
  [0x1885, 0x1886],
  [0x18a9, 0x18a9],
  [0x1920, 0x192b],
  [0x1930, 0x193b],
  [0x1a17, 0x1a1b],
  [0x1a55, 0x1a5e],
  [0x1a60, 0x1a7c],
  [0x1a7f, 0x1a7f],
  [0x1ab0, 0x1add],
  [0x1ae0, 0x1aeb],
  [0x1b00, 0x1b04],
  [0x1b34, 0x1b44],
  [0x1b6b, 0x1b73],
  [0x1b80, 0x1b82],
  [0x1ba1, 0x1bad],
  [0x1be6, 0x1bf3],
  [0x1c24, 0x1c37],
  [0x1cd0, 0x1cd2],
  [0x1cd4, 0x1ce8],
  [0x1ced, 0x1ced],
  [0x1cf4, 0x1cf4],
  [0x1cf7, 0x1cf9],
  [0x1dc0, 0x1dff],
  [0x20d0, 0x20f0],
  [0x2cef, 0x2cf1],
  [0x2d7f, 0x2d7f],
  [0x2de0, 0x2dff],
  [0x302a, 0x302f],
  [0x3099, 0x309a],
  [0xa66f, 0xa672],
  [0xa674, 0xa67d],
  [0xa69e, 0xa69f],
  [0xa6f0, 0xa6f1],
  [0xa802, 0xa802],
  [0xa806, 0xa806],
  [0xa80b, 0xa80b],
  [0xa823, 0xa827],
  [0xa82c, 0xa82c],
  [0xa880, 0xa881],
  [0xa8b4, 0xa8c5],
  [0xa8e0, 0xa8f1],
  [0xa8ff, 0xa8ff],
  [0xa926, 0xa92d],
  [0xa947, 0xa953],
  [0xa980, 0xa983],
  [0xa9b3, 0xa9c0],
  [0xa9e5, 0xa9e5],
  [0xaa29, 0xaa36],
  [0xaa43, 0xaa43],
  [0xaa4c, 0xaa4d],
  [0xaa7b, 0xaa7d],
  [0xaab0, 0xaab0],
  [0xaab2, 0xaab4],
  [0xaab7, 0xaab8],
  [0xaabe, 0xaabf],
  [0xaac1, 0xaac1],
  [0xaaeb, 0xaaef],
  [0xaaf5, 0xaaf6],
  [0xabe3, 0xabea],
  [0xabec, 0xabed],
  [0xfb1e, 0xfb1e],
  [0xfe00, 0xfe0f],
  [0xfe20, 0xfe2f],
  [0x101fd, 0x101fd],
  [0x102e0, 0x102e0],
  [0x10376, 0x1037a],
  [0x10a01, 0x10a03],
  [0x10a05, 0x10a06],
  [0x10a0c, 0x10a0f],
  [0x10a38, 0x10a3a],
  [0x10a3f, 0x10a3f],
  [0x10ae5, 0x10ae6],
  [0x10d24, 0x10d27],
  [0x10d69, 0x10d6d],
  [0x10eab, 0x10eac],
  [0x10efa, 0x10eff],
  [0x10f46, 0x10f50],
  [0x10f82, 0x10f85],
  [0x11000, 0x11002],
  [0x11038, 0x11046],
  [0x11070, 0x11070],
  [0x11073, 0x11074],
  [0x1107f, 0x11082],
  [0x110b0, 0x110ba],
  [0x110c2, 0x110c2],
  [0x11100, 0x11102],
  [0x11127, 0x11134],
  [0x11145, 0x11146],
  [0x11173, 0x11173],
  [0x11180, 0x11182],
  [0x111b3, 0x111c0],
  [0x111c9, 0x111cc],
  [0x111ce, 0x111cf],
  [0x1122c, 0x11237],
  [0x1123e, 0x1123e],
  [0x11241, 0x11241],
  [0x112df, 0x112ea],
  [0x11300, 0x11303],
  [0x1133b, 0x1133c],
  [0x1133e, 0x11344],
  [0x11347, 0x11348],
  [0x1134b, 0x1134d],
  [0x11357, 0x11357],
  [0x11362, 0x11363],
  [0x11366, 0x1136c],
  [0x11370, 0x11374],
  [0x113b8, 0x113c0],
  [0x113c2, 0x113c2],
  [0x113c5, 0x113c5],
  [0x113c7, 0x113ca],
  [0x113cc, 0x113d0],
  [0x113d2, 0x113d2],
  [0x113e1, 0x113e2],
  [0x11435, 0x11446],
  [0x1145e, 0x1145e],
  [0x114b0, 0x114c3],
  [0x115af, 0x115b5],
  [0x115b8, 0x115c0],
  [0x115dc, 0x115dd],
  [0x11630, 0x11640],
  [0x116ab, 0x116b7],
  [0x1171d, 0x1172b],
  [0x1182c, 0x1183a],
  [0x11930, 0x11935],
  [0x11937, 0x11938],
  [0x1193b, 0x1193e],
  [0x11940, 0x11940],
  [0x11942, 0x11943],
  [0x119d1, 0x119d7],
  [0x119da, 0x119e0],
  [0x119e4, 0x119e4],
  [0x11a01, 0x11a0a],
  [0x11a33, 0x11a39],
  [0x11a3b, 0x11a3e],
  [0x11a47, 0x11a47],
  [0x11a51, 0x11a5b],
  [0x11a8a, 0x11a99],
  [0x11b60, 0x11b67],
  [0x11c2f, 0x11c36],
  [0x11c38, 0x11c3f],
  [0x11c92, 0x11ca7],
  [0x11ca9, 0x11cb6],
  [0x11d31, 0x11d36],
  [0x11d3a, 0x11d3a],
  [0x11d3c, 0x11d3d],
  [0x11d3f, 0x11d45],
  [0x11d47, 0x11d47],
  [0x11d8a, 0x11d8e],
  [0x11d90, 0x11d91],
  [0x11d93, 0x11d97],
  [0x11ef3, 0x11ef6],
  [0x11f00, 0x11f01],
  [0x11f03, 0x11f03],
  [0x11f34, 0x11f3a],
  [0x11f3e, 0x11f42],
  [0x11f5a, 0x11f5a],
  [0x13440, 0x13440],
  [0x13447, 0x13455],
  [0x1611e, 0x1612f],
  [0x16af0, 0x16af4],
  [0x16b30, 0x16b36],
  [0x16f4f, 0x16f4f],
  [0x16f51, 0x16f87],
  [0x16f8f, 0x16f92],
  [0x16fe4, 0x16fe4],
  [0x16ff0, 0x16ff1],
  [0x1bc9d, 0x1bc9e],
  [0x1cf00, 0x1cf2d],
  [0x1cf30, 0x1cf46],
  [0x1d165, 0x1d169],
  [0x1d16d, 0x1d172],
  [0x1d17b, 0x1d182],
  [0x1d185, 0x1d18b],
  [0x1d1aa, 0x1d1ad],
  [0x1d242, 0x1d244],
  [0x1da00, 0x1da36],
  [0x1da3b, 0x1da6c],
  [0x1da75, 0x1da75],
  [0x1da84, 0x1da84],
  [0x1da9b, 0x1da9f],
  [0x1daa1, 0x1daaf],
  [0x1e000, 0x1e006],
  [0x1e008, 0x1e018],
  [0x1e01b, 0x1e021],
  [0x1e023, 0x1e024],
  [0x1e026, 0x1e02a],
  [0x1e08f, 0x1e08f],
  [0x1e130, 0x1e136],
  [0x1e2ae, 0x1e2ae],
  [0x1e2ec, 0x1e2ef],
  [0x1e4ec, 0x1e4ef],
  [0x1e5ee, 0x1e5ef],
  [0x1e6e3, 0x1e6e3],
  [0x1e6e6, 0x1e6e6],
  [0x1e6ee, 0x1e6ef],
  [0x1e6f5, 0x1e6f5],
  [0x1e8d0, 0x1e8d6],
  [0x1e944, 0x1e94a],
  [0xe0100, 0xe01ef],
];

const jsEscape = (cp: number) => `\\u{${cp.toString(16)}}`;
const pgEscape = (cp: number) =>
  cp > 0xffff
    ? `\\U${cp.toString(16).toUpperCase().padStart(8, "0")}`
    : `\\u${cp.toString(16).toUpperCase().padStart(4, "0")}`;

/** Bracket-expression body for a JavaScript `u` RegExp. */
export function jsClass(ranges: readonly CodePointRange[]): string {
  return ranges
    .map(([a, b]) => (a === b ? jsEscape(a) : `${jsEscape(a)}-${jsEscape(b)}`))
    .join("");
}

/**
 * Bracket-expression body for a PostgreSQL ARE. NUL cannot occur in PostgreSQL
 * text, so a range starting at U+0000 starts at U+0001 there.
 */
export function pgClass(ranges: readonly CodePointRange[]): string {
  return ranges
    .map(([a, b]) => [Math.max(a, 1), b] as const)
    .map(([a, b]) => (a === b ? pgEscape(a) : `${pgEscape(a)}-${pgEscape(b)}`))
    .join("");
}

const FORMAT = new RegExp(`[${jsClass(FORMAT_RANGES)}]`, "gu");
const SEPARATORS = new RegExp(`[${jsClass(SEPARATOR_RANGES)}]+`, "gu");
const MARK = new RegExp(`^[${jsClass(MARK_RANGES)}]$`, "u");

export type SearchQuery =
  | { ok: true; query: string; meaningfulChars: number }
  | { ok: false; reason: "too_short" | "too_long" };

/** The normalized query, or why it is refused (the RPC raises 22023 for both). */
export function normalizeSearchQuery(raw: string): SearchQuery {
  const query = raw
    .normalize("NFC")
    .replace(FORMAT, "")
    .replace(SEPARATORS, " ")
    .replace(/^ +| +$/g, "")
    .normalize("NFC");
  const points = Array.from(query);
  const meaningfulChars = points.filter(
    (c) => c !== " " && !MARK.test(c),
  ).length;
  if (meaningfulChars < SEARCH_QUERY_MIN_CHARS)
    return { ok: false, reason: "too_short" };
  if (points.length > SEARCH_QUERY_MAX_CHARS)
    return { ok: false, reason: "too_long" };
  return { ok: true, query, meaningfulChars };
}

/** Count of meaningful characters under the same rule (0 for blank-looking text). */
export function meaningfulChars(raw: string): number {
  const result = normalizeSearchQuery(raw);
  if (result.ok) return result.meaningfulChars;
  const query = raw
    .normalize("NFC")
    .replace(FORMAT, "")
    .replace(SEPARATORS, " ");
  return Array.from(query).filter((c) => c !== " " && !MARK.test(c)).length;
}

const TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?(?:\s*(Z|[+-]\d{2}(?::?\d{2})?))?$/i;

/**
 * The one timestamp parser for the API, independent of the host time zone:
 * `YYYY-MM-DD[THH:MM[:SS[.ffffff]]][Z|±HH[:MM]]`, `T` or a space between date and
 * time. A value with no offset is UTC (the PostgreSQL session's zone), never local
 * time. Returns `YYYY-MM-DDTHH:MM:SS.ffffffZ`: fixed width, microseconds kept (a
 * JavaScript Date would drop them, which would break keyset cursors), or null when
 * the value is not a valid timestamp (including more than 6 fractional digits).
 */
export function canonicalTimestamp(value: string): string | null {
  const m = TIMESTAMP.exec(value.trim());
  if (!m) return null;
  const [, y, mo, d, h = "00", mi = "00", sec = "00", frac = "", offset = "Z"] =
    m;
  const year = Number(y),
    month = Number(mo),
    day = Number(d);
  const hour = Number(h),
    minute = Number(mi),
    second = Number(sec);
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  )
    return null;
  // Reject impossible dates (2026-02-30): Date.UTC normalizes them, so round-trip.
  const probe = new Date(Date.UTC(year, month - 1, day));
  if (
    probe.getUTCFullYear() !== year ||
    probe.getUTCMonth() !== month - 1 ||
    probe.getUTCDate() !== day
  )
    return null;
  let offsetMinutes = 0;
  if (offset.toUpperCase() !== "Z") {
    const sign = offset[0] === "-" ? -1 : 1;
    const digits = offset.slice(1).replace(":", "");
    const oh = Number(digits.slice(0, 2)),
      om = Number(digits.slice(2) || "0");
    if (oh > 15 || om > 59) return null;
    offsetMinutes = sign * (oh * 60 + om);
  }
  const ms =
    Date.UTC(year, month - 1, day, hour, minute, second) -
    offsetMinutes * 60_000;
  if (!Number.isFinite(ms)) return null;
  const at = new Date(ms).toISOString(); // whole seconds, so ".000Z"
  return `${at.slice(0, 19)}.${frac.padEnd(6, "0")}Z`;
}

/** The RelayHistory read surface's filters, in the RPC's terms. */
export interface SessionSearchRequest {
  /** Omit for an organization-wide page over the claimed workspaces. */
  workspaceId?: string;
  q?: string;
  /** The read API's `source`; the directory's `harness`. */
  source?: string;
  projectId?: string;
  projectContains?: string;
  noProject?: boolean;
  agent?: string;
  agentMatch?: "exact" | "prefix";
  state?: "active" | "done" | "abandoned" | "unknown";
  userId?: string;
  machineId?: string;
  since?: string;
  until?: string;
  sessionId?: string;
  cursor?: {
    lastActivityAt: string;
    sessionId: string;
    harness: string;
    workspaceId?: string;
  };
  limit?: number;
}

export type SearchRpcRefusal = {
  ok: false;
  code: "22023" | "0A000";
  reason: string;
};

/**
 * The PostgREST request the caller (RelayHistory #226) must send, or the typed refusal
 * the RPC itself would raise. It applies the same rules locally, so an invalid request
 * never reaches the database; the RPC re-checks everything.
 */
export function searchRpcRequest(request: SessionSearchRequest):
  | {
      ok: true;
      path: "rpc/search_session_directory";
      headers: { "content-profile": "sessions" };
      body: Record<string, unknown>;
    }
  | SearchRpcRefusal {
  const refuse = (
    code: "22023" | "0A000",
    reason: string,
  ): SearchRpcRefusal => ({ ok: false, code, reason });
  const body: Record<string, unknown> = {};
  if (request.workspaceId !== undefined)
    body.p_workspace_id = request.workspaceId;
  const limit = request.limit ?? 50;
  if (!Number.isInteger(limit) || limit < 1 || limit > 50)
    return refuse("22023", "limit must be 1 to 50");
  body.p_limit = limit;
  if (request.q !== undefined) {
    const q = normalizeSearchQuery(request.q);
    if (!q.ok)
      return refuse("22023", "query needs 3 to 200 meaningful characters");
    body.p_query = q.query;
  }
  if (request.projectContains !== undefined) {
    const p = normalizeSearchQuery(request.projectContains);
    if (!p.ok)
      return refuse(
        "22023",
        "project_contains needs 3 to 200 meaningful characters",
      );
    body.p_project_contains = p.query;
  }
  const agentMatch = request.agentMatch ?? "exact";
  if (request.agent !== undefined) {
    // Same as the RPC's lower(btrim(p_agent)): btrim strips spaces only, not tabs.
    const agent = request.agent.replace(/^ +| +$/g, "").toLowerCase();
    if (agent === "" || Array.from(agent).length > 200)
      return refuse("22023", "agent must be 1 to 200 characters");
    if (agentMatch === "prefix" && !normalizeSearchQuery(request.agent).ok)
      return refuse(
        "22023",
        "agent prefix needs at least 3 meaningful characters",
      );
    body.p_agent = request.agent;
    body.p_agent_match = agentMatch;
  }
  if (request.source !== undefined) {
    if (!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(request.source))
      return refuse("22023", "harness is invalid");
    body.p_harness = request.source;
  }
  if (request.state !== undefined) {
    if (!["active", "done", "abandoned", "unknown"].includes(request.state))
      return refuse("22023", "state is invalid");
    body.p_state = request.state;
  }
  // One canonical parser for every timestamp (canonicalTimestamp): an offset-less
  // value is UTC, as in the UTC PostgreSQL session, never the host's time zone, and
  // the RPC receives fixed-width microsecond UTC values. An unparseable value is a
  // typed 22023 instead of PostgREST's 22007 cast failure.
  const since =
    request.since === undefined ? undefined : canonicalTimestamp(request.since);
  const until =
    request.until === undefined ? undefined : canonicalTimestamp(request.until);
  if (since === null) return refuse("22023", "since is not a timestamp");
  if (until === null) return refuse("22023", "until is not a timestamp");
  if (since !== undefined) body.p_since = since;
  if (until !== undefined) body.p_until = until;
  // Same fixed-width UTC format, so string order is time order, to the microsecond.
  if (since !== undefined && until !== undefined && since > until)
    return refuse("22023", "since is after until");
  if (
    request.noProject &&
    (request.projectId !== undefined || request.projectContains !== undefined)
  )
    return refuse(
      "22023",
      "no_project contradicts project_id or project_contains",
    );
  if (request.projectId !== undefined) body.p_project_id = request.projectId;
  if (request.noProject) body.p_no_project = true;
  if (request.userId !== undefined) body.p_user_id = request.userId;
  if (request.machineId !== undefined) body.p_machine_id = request.machineId;
  if (request.sessionId !== undefined) body.p_session_id = request.sessionId;
  if (request.cursor) {
    const c = request.cursor;
    if (
      request.workspaceId === undefined
        ? c.workspaceId === undefined
        : c.workspaceId !== undefined && c.workspaceId !== request.workspaceId
    )
      return refuse("22023", "cursor is incomplete or for another scope");
    // As for since/until: the same canonical parser, microseconds kept.
    const cursorAt = canonicalTimestamp(c.lastActivityAt);
    if (cursorAt === null)
      return refuse("22023", "cursor lastActivityAt is not a timestamp");
    body.p_cursor_last_activity_at = cursorAt;
    body.p_cursor_session_id = c.sessionId;
    body.p_cursor_harness = c.harness;
    if (c.workspaceId !== undefined) body.p_cursor_workspace_id = c.workspaceId;
  }
  if (
    request.workspaceId === undefined &&
    (request.agent !== undefined ||
      request.projectId !== undefined ||
      request.projectContains !== undefined ||
      request.noProject ||
      request.userId !== undefined ||
      request.machineId !== undefined)
  )
    return refuse(
      "0A000",
      "agent, project, no_project, user and machine filters need a workspace",
    );
  return {
    ok: true,
    path: "rpc/search_session_directory",
    headers: { "content-profile": "sessions" },
    body,
  };
}
