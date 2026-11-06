/**
 * The server's raw-chunk writer: scrubs a source range of PII and secrets
 * before it reaches object storage, then compresses and stores it at its v1
 * key. It mirrors the client's `scrub::seal::put_raw_chunk` (manifest spec,
 * "Scrubbing"), so a chunk the server seals has the same shape as one a
 * client sealed. Its `raw.scrub.version` says which rules ran: from rule set 2
 * the server also stores a string past the scrub bound, scrubbed in segments,
 * where rule set 1 (and a client still on it) refuses it.
 *
 *   - `start_offset`, `bytes` and `sha256` describe the **source** range, so
 *     the key, resume, epochs and the hash chain do not depend on scrubbing;
 *   - `stored_raw_bytes` and `raw_sha256` describe the scrubbed records that
 *     were stored, and `stored_bytes`/`stored_sha256` the compressed object.
 *
 * The unscrubbed source is never written anywhere, and neither it nor the
 * scrubbed records are ever logged or put in an error message.
 */
import { sha256Hex as digestHex } from "../../../../object-store/src/crypto.js";
import {
  MAX_SCRUB_CHARS,
  exceedsScrubBound,
  isCardNumber,
  scrubText,
} from "../scrub.js";
import { PipelineError } from "./errors.js";
import {
  isObjectStoreError,
  rawChunkKey,
  readAll,
  type ChunkEntry,
  type ObjectKey,
  type ObjectStore,
  type RawFormat,
  type SessionLocation,
} from "./object-layout.js";

/** `raw.scrub.ruleset` for chunks scrubbed with `lib/scrub.ts`. */
export const SCRUB_RULESET = "relayhistory-scrub";
/**
 * `raw.scrub.version` for the current `lib/scrub.ts` rules. Bump it with any
 * rule change so readers can tell which rules a stored chunk went through;
 * existing objects are never rewritten.
 *
 * - 1: strings and non-record runs up to `MAX_SCRUB_CHARS`; longer ones refused.
 * - 2: the same rules; a longer string or run is scrubbed whole, in segments
 *   cut only where no rule can match across the cut (`scrubLong`). Output for
 *   anything within the bound is byte-identical to version 1.
 */
export const SCRUB_RULESET_VERSION = 2;

/** Object metadata the client's `put_raw_chunk` writes; read back on a lost create race. */
export const META_RAW_SHA256 = "raw-sha256";
export const META_RAW_BYTES = "raw-bytes";
export const META_SCRUB_RULESET = "scrub-ruleset";
export const META_SCRUB_VERSION = "scrub-version";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

// <scrub-segmenter> (folded into the scrub rule set digest test: from here to the end marker)
/**
 * Scrub one source range record by record. A line that is a JSON object,
 * array or string is a record: every string token in it (values and keys) is scrubbed
 * with `scrubText` in place, and an integer that is a card number becomes
 * `"[REDACTED]"`; everything else (number lexemes, whitespace, key order,
 * duplicate and `__proto__` keys) is kept byte for byte, so nothing but the
 * scrubbed strings changes. Consecutive lines that are not records are
 * scrubbed together as one text, so a multi-line secret (a PEM private key)
 * is matched whole; a long run is cut into segments under `MAX_SCRUB_CHARS`
 * at line boundaries (never inside an open private-key block or between two
 * digit groups), so `scrubText` never truncates it. Line endings are kept;
 * `records` counts complete lines.
 */
export function scrubRecords(source: Uint8Array): {
  scrubbed: Uint8Array;
  records: number;
} {
  let text: string;
  try {
    text = decoder.decode(source);
  } catch {
    // Not UTF-8: refuse rather than store bytes the scrubber cannot read.
    throw new PipelineError(
      "chunk_not_utf8",
      "raw chunk is not valid UTF-8",
      false,
    );
  }
  const parts = text.split(/(\r?\n)/);
  let records = 0;
  let out = "";
  // A segment of a run of non-record lines, scrubbed as one text.
  let run = "";
  let keyOpen = false;
  const flush = () => {
    if (run) out += scrubBounded(run);
    run = "";
  };
  /** This line and at least CUT_CONTEXT_CHARS of what follows it. */
  const lookaheadAt = (i: number) => {
    let la = "";
    for (let j = i; j < parts.length && la.length < CUT_CONTEXT_CHARS + (parts[i]?.length ?? 0); j += 2)
      la += (parts[j] ?? "") + (parts[j + 1] ?? "");
    return la;
  };
  /**
   * Whether the run may end here (the boundary before line `i`) without
   * splitting a match. Only a credential cue (a keyword or `:`/`=`) right
   * before the run's trailing whitespace can reach across a line into the
   * next one (digit groups are checked too, cheaply); without one, no scrub
   * runs, so mixed text-and-JSON logs cost what they did in rule set 1.
   */
  const boundarySafe = (i: number) => {
    if (!run) return true;
    let start = run.length;
    while (start > 0 && /\s/.test(run[start - 1]!)) start--;
    const cue = CREDENTIAL_KEYWORD.test(run.slice(Math.max(0, start - 64), start));
    const digits = /[\d)]/.test(run[start - 1] ?? "") && /^\s*[\d(]/.test(parts[i] ?? "");
    if (!cue && !digits) return true;
    // Bounded per chunk: past the cap a cued boundary is treated as unsafe
    // (a leading JSON string after it is redacted; a key line stays in the run).
    if (boundaryChecks >= MAX_BOUNDARY_CHECKS) return false;
    boundaryChecks += 1;
    return cutIsSafe(run + lookaheadAt(i), run.length, { checks: 0, candidates: 0 }).ok;
  };
  let boundaryChecks = 0;
  for (let i = 0; i < parts.length; i += 2) {
    const line = parts[i]!;
    const ending = parts[i + 1] ?? "";
    if (ending) records += 1;
    // Inside an open private-key block a record line (two writers interleaving
    // on one stream) stays in the run, so the key is redacted whole, as
    // whole-text scrubbing would; flushing here would store the rest of it.
    if (!keyOpen && isRecord(line)) {
      // The run's text may end in a keyword whose value is this record
      // (`DB_PASSWORD=` then `"<secret>"`). The record is scrubbed once, as a
      // record, and kept intact; if the boundary splits a match, the value the
      // text rule would take, a leading JSON string, is redacted in place. (A
      // record that opens with `{` or `[` offers the rule only the bracket.)
      const safe = boundarySafe(i);
      flush();
      const scrubbed = scrubRecordLine(line);
      out += (safe ? scrubbed : redactLeadingString(scrubbed)) + ending;
      continue;
    }
    const next = line + ending;
    // A segment ends before a line that opens a private key (the key then
    // starts its own segment and is matched whole). Before a line that would
    // take it past the limit, it ends at the latest safe line boundary (see
    // `safeCut`), carrying the lines after that cut into the next segment.
    if (run && !keyOpen && privateKeyOpenAfter(line, false) && boundarySafe(i)) flush();
    else if (run && !keyOpen && run.length + next.length > MAX_SCRUB_CHARS) {
      // At least CUT_CONTEXT_CHARS of what follows, for the context check.
      let lookahead = next;
      for (let j = i + 2; j < parts.length && lookahead.length < CUT_CONTEXT_CHARS + next.length; j += 2)
        lookahead += (parts[j] ?? "") + (parts[j + 1] ?? "");
      const cut = safeCut(run, lookahead.slice(0, next.length + CUT_CONTEXT_CHARS));
      // Every boundary in 64 KiB splits a match: refuse the chunk rather than
      // let `scrubText` truncate the segment and drop lines.
      if (!cut)
        throw new PipelineError(
          "chunk_unsegmentable",
          "raw chunk text cannot be split for scrubbing without breaking a match",
          false,
        );
      const carry = run.slice(cut);
      run = run.slice(0, cut);
      flush();
      run = carry;
    }
    // Inside an open key block a record line joins the run record-scrubbed, not raw (K-2):
    // raw JSON escapes hide tokens from the text rules (`\nghp_...` has no word boundary),
    // and the record that closes the block would otherwise carry what follows its footer
    // into the run as unscrubbed JSON. Record scrubbing leaves a lone footer string intact,
    // so the block still pairs and is redacted whole.
    const piece = keyOpen && isRecord(line) ? scrubRecordLine(line) : line;
    run += piece + ending;
    keyOpen = privateKeyOpenAfter(piece, keyOpen);
  }
  flush();
  return { scrubbed: encoder.encode(out), records };
}

/**
 * Longest whitespace run a segment may be cut inside. Rules whose match spans
 * a separator (`Bearer <token>`, `password: value`) allow whitespace of any
 * length, so a cut inside a longer run cannot be proven safe within a bounded
 * context: such a cut is refused, never guessed.
 */
const MAX_CUT_GAP = 1024;
/** Most candidate cuts a segment search fully checks before refusing (bounds CPU). */
const MAX_CUT_CHECKS = 48;
/** Most context-checked record or key-line boundaries per chunk (bounds CPU on cue-heavy logs). */
const MAX_BOUNDARY_CHECKS = 256;
/** Most candidates of any kind (cheap refusals included) a segment search looks at. */
const MAX_CUT_CANDIDATES = 4096;
/**
 * A credential keyword (and optional `:`/`=`) at the end of the text before a
 * whitespace run: a cut there would separate the keyword from its value.
 */
const CREDENTIAL_KEYWORD =
  // Not `\b`: in `DB_PASSWORD` there is no word boundary between `_` and `P`.
  /(?:(?<![A-Za-z0-9])(?:bearer|basic|token|password|passwd|pwd|secret|api[-_]?key|access[-_]?key|secret[-_]?key|private[-_]?key|client[-_]?secret|authorization|auth|credentials?|key)["']?\s*[:=]?|[:=])["']?$/i;

/**
 * Whether the text may be cut at `cut` (the segments either side are then
 * scrubbed separately). The cut sits inside a whitespace run `[start, end)`.
 * It must hold that:
 *  - the run is at most `MAX_CUT_GAP` long;
 *  - no credential keyword or `:`/`=` ends right before the run;
 *  - a digit or `)` before and a digit or `(` after does not straddle it (cards, phone numbers);
 *  - `CUT_CONTEXT_CHARS` beyond each end of the run scrubs the same together and apart.
 * `budget.checks` counts the full checks (three `scrubText` calls each).
 */
interface CutBudget {
  checks: number;
  candidates: number;
}

/**
 * Characters the cut search has walked and candidates it has looked at, process
 * wide. Tests read it to bound the segmenter's work deterministically (time is
 * load-sensitive); nothing else reads it.
 */
export const segmenterWork = { walked: 0, candidates: 0 };

/**
 * Where a value would start after a keyword that ends right before a
 * whitespace run ending at `end`: past the run, an optional `:`/`=`, more
 * whitespace and an optional quote. -1 when a stretch of whitespace there is
 * longer than MAX_CUT_GAP (a value could hide past any bounded window).
 */
function valueStart(text: string, end: number): number {
  let i = end;
  const skip = () => {
    const from = i;
    while (i < text.length && /\s/.test(text[i]!)) {
      i++;
      segmenterWork.walked++;
      if (i - from > MAX_CUT_GAP) return false;
    }
    return true;
  };
  if (!skip()) return -1;
  if (text[i] === ":" || text[i] === "=") i++;
  if (!skip()) return -1;
  if (text[i] === '"' || text[i] === "'" || text[i] === "`") i++;
  return i;
}

/**
 * `{ ok }` plus `runStart`, the start of the whitespace run around `cut`, so a
 * caller can skip a refused run in one step (each run is walked once).
 */
function cutIsSafe(text: string, cut: number, budget: CutBudget): { ok: boolean; runStart: number } {
  budget.candidates += 1;
  segmenterWork.candidates++;
  let start = cut;
  while (start > 0 && /\s/.test(text[start - 1]!)) {
    start--;
    segmenterWork.walked++;
  }
  // Forward walk bounded: a run past MAX_CUT_GAP is refused whatever its length.
  let end = cut;
  while (end < text.length && end - start <= MAX_CUT_GAP && /\s/.test(text[end]!)) {
    end++;
    segmenterWork.walked++;
  }
  const no = { ok: false, runStart: start };
  if (end - start > MAX_CUT_GAP) return no;
  if (/[\d)]/.test(text[start - 1] ?? "") && /[\d(]/.test(text[end] ?? "")) return no;
  // A credential keyword (or `:`/`=`) right before the run does not refuse the
  // cut; it widens the context check so it reaches past where the keyword's
  // value would start. Only an unbounded gap there refuses.
  let reach = end;
  if (CREDENTIAL_KEYWORD.test(text.slice(Math.max(0, start - 64), start))) {
    const v = valueStart(text, end);
    if (v < 0) return no;
    reach = v;
    // The value must be in view: if the text ends first, the caller has not
    // given enough lookahead to prove anything.
    if (reach + 1 > text.length) return no;
  }
  budget.checks += 1;
  const before = text.slice(Math.max(0, start - CUT_CONTEXT_CHARS), cut);
  const after = text.slice(cut, Math.min(text.length, reach + CUT_CONTEXT_CHARS));
  return { ok: scrubText(before + after) === scrubText(before) + scrubText(after), runStart: start };
}

const withinBudget = (b: CutBudget) => b.checks < MAX_CUT_CHECKS && b.candidates < MAX_CUT_CANDIDATES;

/** How far back `safeCut` looks for a boundary, and how much text it scrubs. */
const CUT_SEARCH_CHARS = 65_536;
const CUT_CONTEXT_CHARS = 4096;

/**
 * Where a segment may end so that `next` starts the following one: the latest
 * line boundary in the last 64 KiB of `run` that no rule matches across, or 0
 * when there is none. Rules
 * that span a newline do so with a short prefix before the secret (`Bearer`,
 * `key =`, a digit group), so the text around a boundary is scrubbed together
 * and compared with scrubbing each side alone; any difference means a match
 * crosses it.
 */
function safeCut(run: string, lookahead: string): number {
  // `lookahead` is the line that would overflow plus at least CUT_CONTEXT_CHARS
  // of what follows it, so a secret on a later line is in the context check.
  const text = run + lookahead;
  const budget: CutBudget = { checks: 0, candidates: 0 };
  const floor = Math.max(1, run.length - CUT_SEARCH_CHARS);
  const keys = keyBlockSpans(run);
  for (let cut = run.length; cut >= floor && withinBudget(budget); ) {
    // Never inside a key block, even one that closed before the overflowing line
    // (the context check cannot see a header more than CUT_CONTEXT_CHARS back):
    // go to the line boundary at or before its header.
    const inside = keyBlockAround(keys, cut);
    if (inside) {
      cut = run.lastIndexOf("\n", inside[0] - 1) + 1;
      if (cut === 0) break;
      continue;
    }
    const v = cutIsSafe(text, cut, budget);
    if (v.ok) return cut;
    // The previous line boundary before this whitespace run: just after the
    // newline ending the line before it (0, below the floor, when there is none).
    cut = run.lastIndexOf("\n", Math.min(cut, v.runStart) - 2) + 1;
  }
  return 0;
}

// Fresh objects per use: a shared global regex keeps `lastIndex` between
// calls (and `matchAll` clones it), so one request's scan could make a later
// one miss a key header and cut inside the key.
const PEM_BEGIN_SRC = "-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----";
const PEM_END_SRC = "-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----";
const pemBegin = () => new RegExp(PEM_BEGIN_SRC, "g");
const pemEnd = () => new RegExp(PEM_END_SRC, "g");

/**
 * Whether a private-key block is still open after `line`, pairing header and footer
 * in order exactly as `keyBlockSpans` and the PEM rule do: a footer only counts after
 * the header's own match. (Taking the last marker instead disagreed on a header that
 * shares its dashes with a footer, `-----BEGIN … KEY-----END … KEY-----`: the rule sees
 * that block open, so it must be open here too.)
 */
function privateKeyOpenAfter(line: string, open: boolean) {
  const PEM_BEGIN = pemBegin();
  const PEM_END = pemEnd();
  let at = 0;
  for (;;) {
    const re = open ? PEM_END : PEM_BEGIN;
    re.lastIndex = at;
    const m = re.exec(line);
    if (!m) return open;
    at = m.index + m[0].length;
    open = !open;
  }
}

/**
 * Every private-key block in `text` as [start, end): from its header to just past
 * its footer, or to the end of `text` for a block left open. A cut strictly inside
 * one splits the key across segments, and each segment alone holds a header without
 * its footer (or the reverse), which no rule redacts: the key lines after the cut
 * would be stored in clear. The cut searches never stop inside one.
 */
function keyBlockSpans(text: string): Array<[number, number]> {
  const spans: Array<[number, number]> = [];
  const PEM_BEGIN = pemBegin();
  const PEM_END = pemEnd();
  let at = 0;
  for (;;) {
    PEM_BEGIN.lastIndex = at;
    const begin = PEM_BEGIN.exec(text);
    if (!begin) break;
    PEM_END.lastIndex = begin.index + begin[0].length;
    const end = PEM_END.exec(text);
    if (!end) {
      spans.push([begin.index, text.length]);
      break;
    }
    spans.push([begin.index, end.index + end[0].length]);
    at = end.index + end[0].length;
  }
  return spans;
}

/** The key block `cut` falls strictly inside, if any (spans are sorted and disjoint: binary search). */
function keyBlockAround(spans: Array<[number, number]>, cut: number): [number, number] | undefined {
  let lo = 0;
  let hi = spans.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const [start, end] = spans[mid]!;
    if (cut <= start) hi = mid - 1;
    else if (cut >= end) lo = mid + 1;
    else return spans[mid];
  }
  return undefined;
}

/** A record line whose first token is a JSON string, with that string redacted (still valid JSON). */
function redactLeadingString(record: string): string {
  const lead = record.length - record.trimStart().length;
  if (record[lead] !== '"') return record;
  let j = lead + 1;
  while (j < record.length && record[j] !== '"') j += record[j] === "\\" ? 2 : 1;
  return `${record.slice(0, lead)}${JSON.stringify(REDACTED)}${record.slice(j + 1)}`;
}

/**
 * A line that is a JSON object, array or string. A string line is a record
 * too: scrubbing it as plain text would miss a secret behind a `\\u` escape.
 */
function isRecord(line: string) {
  const first = line.trimStart()[0];
  if (first !== "{" && first !== "[" && first !== '"') return false;
  try {
    JSON.parse(line);
    return true;
  } catch {
    return false;
  }
}

/**
 * The scrubbed value of one JSON string token. A rule can match the raw
 * literal (escapes included) but not the decoded value; re-serializing that
 * value would still store what the rule matched, so the whole string is
 * redacted instead. Unchanged means neither form matched any rule.
 */
export function scrubStringToken(
  literal: string,
  value: string,
  scrub: (text: string) => string = scrubBounded,
): string {
  const scrubbed = scrub(value);
  if (scrubbed !== value) return scrubbed;
  return scrub(literal) === literal ? value : REDACTED;
}

const REDACTED = "[REDACTED]";

/**
 * `scrubText` for stored chunks. `scrubText` itself truncates past its bound,
 * which would store less than the source range reports, so a longer text is
 * scrubbed whole with `scrubLong` instead (version 2; version 1 refused it).
 */
function scrubBounded(text: string): string {
  return scrubLong(text);
}

/**
 * Where a long text may be split so that scrubbing the two sides separately
 * equals scrubbing them together: the latest index at or below
 * `MAX_SCRUB_CHARS` (and no more than `CUT_SEARCH_CHARS` before it) that
 *  - follows whitespace (no rule but PEM matches across a non-whitespace boundary
 *    within one token, so a token is never split),
 *  - does not sit between two digit groups (cards, phone numbers),
 *  - is not inside a private-key block whose footer comes later, and
 *  - leaves `CUT_CONTEXT_CHARS` on each side scrubbing identically together and
 *    apart, which also covers rules whose match spans a separator before the
 *    secret (`Bearer <token>`, `key = value`).
 * 0 when there is none.
 */
function longCut(text: string): number {
  let top = MAX_SCRUB_CHARS;
  // A key block open at the bound: cut before its header, or not at all.
  let open = -1;
  {
    const PEM_BEGIN = pemBegin();
    const PEM_END = pemEnd();
    let at = 0;
    for (;;) {
      PEM_BEGIN.lastIndex = at;
      const begin = PEM_BEGIN.exec(text);
      if (!begin || begin.index >= top) break;
      PEM_END.lastIndex = begin.index + begin[0].length;
      const end = PEM_END.exec(text);
      if (!end) {
        open = begin.index;
        break;
      }
      // A block straddling the bound is open at the bound too.
      if (end.index + end[0].length > top) {
        open = begin.index;
        break;
      }
      at = end.index + end[0].length;
    }
  }
  if (open !== -1) top = open;
  const floor = Math.max(1, MAX_SCRUB_CHARS - CUT_SEARCH_CHARS);
  const budget: CutBudget = { checks: 0, candidates: 0 };
  const keys = keyBlockSpans(text.slice(0, top));
  for (let cut = top; cut >= floor && withinBudget(budget); cut--) {
    // Not inside a key block that closed below the bound either.
    const inside = keyBlockAround(keys, cut);
    if (inside) {
      cut = inside[0] + 1; // the loop's cut-- lands on the header's start
      continue;
    }
    // Only at the end of a whitespace run (after whitespace, before text):
    // one candidate per run, so a run is checked once, not once per char.
    if (!/\s/.test(text[cut - 1] ?? "") || /\s/.test(text[cut] ?? "")) continue;
    const v = cutIsSafe(text, cut, budget);
    if (v.ok) return cut;
    cut = Math.min(cut, v.runStart);
  }
  return 0;
}
// </scrub-segmenter>

/**
 * Scrub a text of any length, whole: segments of at most `MAX_SCRUB_CHARS`
 * split at `longCut`, each scrubbed with `scrubText`. Nothing is truncated. A
 * text with no safe cut (one unbroken run of more than the bound, such as a
 * base64 dump with no whitespace) is refused, never stored partly scrubbed.
 */
export function scrubLong(text: string): string {
  if (!exceedsScrubBound(text)) return scrubText(text);
  let out = "";
  let rest = text;
  while (exceedsScrubBound(rest)) {
    const cut = longCut(rest);
    if (!cut)
      throw new PipelineError(
        "chunk_string_too_long",
        `raw chunk holds a line or string longer than the ${MAX_SCRUB_CHARS}-character scrub limit with no safe place to segment it`,
        false,
      );
    out += scrubText(rest.slice(0, cut));
    rest = rest.slice(cut);
  }
  return out + scrubText(rest);
}

/** Whether `scrubLong` can take this text (it has safe cuts all the way). */
export function canScrubLong(text: string): boolean {
  let rest = text;
  while (exceedsScrubBound(rest)) {
    const cut = longCut(rest);
    if (!cut) return false;
    rest = rest.slice(cut);
  }
  return true;
}

/** Key names seen in one JSON object while its record is rewritten. */
interface ObjectKeys {
  /** Names written so far (after scrubbing and any suffix). */
  emitted: Set<string>;
  /** Source key names, so a duplicate the source already had stays as it was. */
  source: Set<string>;
  /** Next suffix to try per scrubbed name, so many collisions stay linear. */
  nextSuffix: Map<string, number>;
  expectKey: boolean;
}

/**
 * Rewrite the string and number tokens of one valid JSON line. The line was
 * already parsed, so tokens can be found without a full grammar: a string runs
 * to its first unescaped quote, a number is a maximal run of number chars.
 *
 * Keys follow `scrubJson`: a key whose name collides with one already written
 * in the same object gets a ` (2)`, ` (3)`… suffix, so two keys that scrub to
 * the same name both stay readable. A key the source itself repeated is kept
 * as the source had it.
 */
function scrubRecordLine(line: string): string {
  // One entry per open container; null for an array.
  const stack: (ObjectKeys | null)[] = [];
  let out = "";
  let i = 0;
  while (i < line.length) {
    const ch = line[i]!;
    const top = stack[stack.length - 1];
    if (ch === '"') {
      let j = i + 1;
      while (line[j] !== '"') j += line[j] === "\\" ? 2 : 1;
      const literal = line.slice(i, j + 1);
      const value = JSON.parse(literal) as string;
      const scrubbed = scrubStringToken(literal, value);
      let name = scrubbed;
      if (top?.expectKey) {
        const repeated = top.source.has(value);
        top.source.add(value);
        if (!repeated && top.emitted.has(name)) {
          const base = name;
          let n = top.nextSuffix.get(base) ?? 2;
          while (top.emitted.has(`${base} (${n})`)) n += 1;
          name = `${base} (${n})`;
          top.nextSuffix.set(base, n + 1);
        }
        top.emitted.add(name);
        top.expectKey = false;
      }
      out += name === value ? literal : JSON.stringify(name);
      i = j + 1;
    } else if (ch === "-" || (ch >= "0" && ch <= "9")) {
      let j = i + 1;
      while (j < line.length && /[0-9eE+.-]/.test(line[j]!)) j += 1;
      const lexeme = line.slice(i, j);
      // Checked on the digits, so a card number past 2^53 is still caught.
      out +=
        /^\d+$/.test(lexeme) && isCardNumber(lexeme)
          ? JSON.stringify(REDACTED)
          : lexeme;
      i = j;
    } else {
      if (ch === "{")
        stack.push({
          emitted: new Set(),
          source: new Set(),
          nextSuffix: new Map(),
          expectKey: true,
        });
      else if (ch === "[") stack.push(null);
      else if (ch === "}" || ch === "]") stack.pop();
      else if (ch === "," && top) top.expectKey = true;
      out += ch;
      i += 1;
    }
  }
  return out;
}

// Ported from relayhistory-cloud #258 (df0eafe): one zstd frame workerd can always write.
type ZstdCompress = (input: Uint8Array) => Uint8Array;

const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd];
/** RFC 8878 caps a block at 128 KiB. */
const MAX_RAW_BLOCK = 128 * 1024;

/**
 * One standard zstd frame holding `input` in Raw (uncompressed) blocks (RFC
 * 8878 §3.1.1): single segment, 4-byte content size, no checksum. Every zstd
 * decoder reads it as one ordinary frame, so readers that decode only the
 * first frame of a stream (Node's and workerd's `zstdDecompressSync` do, for
 * some sizes) still get every byte. Used when compression cannot be trusted.
 */
export function rawZstdFrame(input: Uint8Array): Uint8Array {
  if (input.byteLength > 0xffff_ffff)
    throw new PipelineError("chunk_too_large", "chunk exceeds 4 GiB", false);
  const blocks = Math.max(1, Math.ceil(input.byteLength / MAX_RAW_BLOCK));
  const out = new Uint8Array(4 + 1 + 4 + blocks * 3 + input.byteLength);
  out.set(ZSTD_MAGIC, 0);
  // Frame_Header_Descriptor: FCS flag 2 (4 bytes), Single_Segment 1.
  out[4] = 0xa0;
  new DataView(out.buffer).setUint32(5, input.byteLength, true);
  let o = 9;
  for (let b = 0; b < blocks; b++) {
    const start = b * MAX_RAW_BLOCK;
    const size = Math.min(MAX_RAW_BLOCK, input.byteLength - start);
    const last = b === blocks - 1 ? 1 : 0;
    // Block_Header: Last_Block (bit 0), Block_Type Raw = 0 (bits 1-2), Block_Size (bits 3-23).
    const header = last | (size << 3);
    out[o] = header & 0xff;
    out[o + 1] = (header >>> 8) & 0xff;
    out[o + 2] = (header >>> 16) & 0xff;
    o += 3;
    out.set(input.subarray(start, start + size), o);
    o += size;
  }
  return out;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
  return true;
}

/** workerd's `zstdCompressSync` fails past about 40 KiB of output (1.20260617). */
const WORKERD =
  typeof navigator !== "undefined" &&
  navigator.userAgent === "Cloudflare-Workers";
const WORKERD_MAX_COMPRESSED = 36 * 1024;
const SAMPLE_BYTES = 64 * 1024;

let zstd: Promise<ZstdCompress | null> | undefined;
/**
 * A zstd encoder that always produces one frame that decodes to `input`.
 *
 * `node:zlib`'s `zstdCompressSync` is tried first. workerd 1.20260617 throws
 * `Error("Unexpected end of file")` from it once the compressed output passes
 * about 40 KiB, and its streaming compressor fails at irregular sizes, so the
 * output is verified by decompressing it. On any error or mismatch the input
 * is stored in Raw blocks (`rawZstdFrame`): larger, never wrong. Splitting
 * into several compressed frames was rejected: Node's and workerd's
 * `zstdDecompressSync` silently return only the first frame for some inputs.
 *
 * Feature-detected, like the request-body decoder: without `node:zlib` zstd
 * every chunk is stored raw.
 */
function zstdEncoder(): Promise<ZstdCompress | null> {
  zstd ??= import("node:zlib").then(
    (zlib) => {
      const compress =
        typeof zlib.zstdCompressSync === "function"
          ? zlib.zstdCompressSync
          : null;
      const decompress =
        typeof zlib.zstdDecompressSync === "function"
          ? zlib.zstdDecompressSync
          : null;
      return (input: Uint8Array) => {
        if (compress && decompress) {
          // In workerd a chunk whose compressed output would pass the
          // compressor's limit ends up Raw anyway: predict it from the first
          // SAMPLE_BYTES and skip the doomed full compress and verify.
          if (WORKERD && input.byteLength > SAMPLE_BYTES) {
            try {
              const sample = compress(input.subarray(0, SAMPLE_BYTES));
              const projected =
                (sample.byteLength / SAMPLE_BYTES) * input.byteLength;
              if (projected > WORKERD_MAX_COMPRESSED)
                return rawZstdFrame(input);
            } catch {
              return rawZstdFrame(input);
            }
          }
          try {
            const packed = new Uint8Array(compress(input));
            const back = decompress(packed, {
              maxOutputLength: Math.max(1, input.byteLength),
            });
            if (sameBytes(new Uint8Array(back), input)) return packed;
          } catch {
            // Fall through to Raw blocks.
          }
        }
        return rawZstdFrame(input);
      };
    },
    () => (input: Uint8Array) => rawZstdFrame(input),
  );
  return zstd;
}

export async function rawChunkSealingAvailable() {
  return (await zstdEncoder()) !== null;
}

export interface SealRawChunk {
  location: SessionLocation;
  format: RawFormat;
  seq: number;
  epoch: number;
  startOffset: number;
  /** The unscrubbed source range. Scrubbed here; never stored or logged as is. */
  source: Uint8Array;
  traceId?: string;
}

/** The chunk-entry fields that describe the source range, known before the put. */
interface SourceFields {
  seq: number;
  epoch: number;
  start_offset: number;
  bytes: number;
  sha256: string;
  sha256_8: string;
  compression: "zstd";
  records: number;
}

export interface SealedChunk {
  key: string;
  entry: ChunkEntry;
  /** False when an identical chunk was already stored, or another writer won the create. */
  created: boolean;
  /** `raw.scrub` to record on the manifest for this chunk. */
  scrub: { ruleset: string; version: number };
}

/**
 * Scrub, compress and store one source range at its v1 raw key. Create-only:
 * if the key already holds different bytes (a re-delivery after a rule-set
 * bump, or a concurrent writer), the stored object is kept and its entry is
 * built from the stored object's metadata and bytes, never from this write.
 */
export async function sealRawChunk(
  store: ObjectStore,
  input: SealRawChunk,
): Promise<SealedChunk> {
  if (input.source.byteLength === 0)
    throw new PipelineError("chunk_empty", "raw chunk is empty", false);
  const compress = await zstdEncoder();
  if (!compress)
    throw new PipelineError(
      "zstd_unavailable",
      "this runtime cannot zstd-compress raw chunks",
      false,
    );
  const sourceSha = await digestHex(input.source);
  const key = rawChunkKey(
    input.location,
    input.seq,
    input.startOffset,
    sourceSha,
    input.format,
  );
  const { scrubbed, records } = scrubRecords(input.source);
  const stored = compress(scrubbed);
  const base: SourceFields = {
    seq: input.seq,
    epoch: input.epoch,
    start_offset: input.startOffset,
    bytes: input.source.byteLength,
    sha256: sourceSha,
    sha256_8: sourceSha.slice(0, 8),
    compression: "zstd" as const,
    records,
  };
  const scrub = { ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION };
  const rawSha = await digestHex(scrubbed);
  try {
    const ack = await store.put(key, stored, {
      ifNoneMatch: true,
      metadata: {
        [META_RAW_SHA256]: rawSha,
        [META_RAW_BYTES]: String(scrubbed.byteLength),
        [META_SCRUB_RULESET]: SCRUB_RULESET,
        [META_SCRUB_VERSION]: String(SCRUB_RULESET_VERSION),
        ...(input.traceId ? { "trace-id": input.traceId } : {}),
      },
    });
    // Identical bytes already stored: they may have been stored by another
    // writer without scrub metadata, so describe the object from its own
    // metadata (and refuse it without), exactly as for different bytes.
    if (!ack.created) return keepStored(store, key, base);
    return {
      key,
      created: true,
      scrub,
      entry: {
        ...base,
        stored_bytes: ack.size,
        stored_sha256: ack.contentSha256,
        stored_raw_bytes: scrubbed.byteLength,
        raw_sha256: rawSha,
      },
    };
  } catch (error) {
    if (!isObjectStoreError(error, "already_exists")) throw error;
    return keepStored(store, key, base);
  }
}

/** The entry for the object already at `key`, from its own metadata and bytes. */
async function keepStored(
  store: ObjectStore,
  key: ObjectKey,
  base: SourceFields,
): Promise<SealedChunk> {
  const object = await store.getObject(key);
  const bytes = await readAll(object.body);
  const meta = object.meta.metadata;
  const rawSha = meta[META_RAW_SHA256];
  const rawBytes = Number(meta[META_RAW_BYTES]);
  const version = Number(meta[META_SCRUB_VERSION]);
  // An object with no scrub metadata cannot vouch for itself: never record a
  // scrub marker for it.
  if (
    !rawSha ||
    !Number.isSafeInteger(rawBytes) ||
    rawBytes < 1 ||
    !meta[META_SCRUB_RULESET] ||
    !Number.isSafeInteger(version) ||
    version < 1
  )
    throw new PipelineError(
      "chunk_stored_unscrubbed",
      "the stored chunk at this key carries no scrub metadata",
      false,
    );
  return {
    key,
    created: false,
    scrub: { ruleset: meta[META_SCRUB_RULESET]!, version },
    entry: {
      ...base,
      stored_bytes: bytes.byteLength,
      stored_sha256: await digestHex(bytes),
      stored_raw_bytes: rawBytes,
      raw_sha256: rawSha,
    },
  };
}
