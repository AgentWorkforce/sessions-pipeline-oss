import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { MAX_SCRUB_CHARS, scrubText } from "../src/lib/scrub.js";
import {
  SCRUB_RULESET_VERSION,
  canScrubLong,
  segmenterWork,
  scrubLong,
  scrubRecords,
  sealRawChunk,
} from "../src/lib/pipeline/raw-chunks.js";
import { readAll } from "../src/lib/pipeline/object-layout.js";

const TOKEN = "Zq8vR2kLm9Xp4Tn7Wc3Ys6Hd"; // 24 chars: matches only after "Bearer "
const AWS = "AKIAIOSFODNN7EXAMPLE";
const words = (chars: number) => "lorem ipsum dolor sit amet ".repeat(Math.ceil(chars / 27)).slice(0, chars);

/** A naive segmenter (cut at the last whitespace under the bound): what scrubLong must not be. */
function naiveScrub(text: string): string {
  let out = "";
  let rest = text;
  while (rest.length > MAX_SCRUB_CHARS) {
    let cut = MAX_SCRUB_CHARS;
    while (!/\s/.test(rest[cut - 1]!)) cut--;
    out += scrubText(rest.slice(0, cut));
    rest = rest.slice(cut);
  }
  return out + scrubText(rest);
}

describe("scrubLong (scrub rule set version 2)", () => {
  it("is version 2", () => {
    expect(SCRUB_RULESET_VERSION).toBe(2);
  });

  it("keeps a long text whole and scrubs secrets anywhere in it, including at the bound", () => {
    const text = `${words(MAX_SCRUB_CHARS - 10)} ${AWS} ${words(300_000)} ${AWS} end`;
    const out = scrubLong(text);
    expect(out).not.toContain(AWS);
    expect(out).not.toContain("truncated");
    expect(out.endsWith(" end")).toBe(true);
    // Within the bound, output equals the version-1 rules exactly.
    const short = `${words(1000)} ${AWS}`;
    expect(scrubLong(short)).toBe(scrubText(short));
  });

  it("never splits `Bearer <token>`: a naive segmenter leaks the token, scrubLong does not", () => {
    // The last whitespace before the bound is the one right after "Bearer".
    // One earlier safe cut, 1000 chars before the bound; otherwise no whitespace.
    const dashed = `${words(MAX_SCRUB_CHARS - "Bearer ".length - 1).replace(/ /g, "-")}-`;
    const prefix = `${dashed.slice(0, MAX_SCRUB_CHARS - 1000)} ${dashed.slice(MAX_SCRUB_CHARS - 999)}`;
    const text = `${prefix}Bearer ${TOKEN}${"-".repeat(20)} ${words(50_000)}`;
    expect(text.lastIndexOf(" ", MAX_SCRUB_CHARS - 1)).toBe(MAX_SCRUB_CHARS - 1);
    expect(scrubText(`${text.slice(MAX_SCRUB_CHARS - 100, MAX_SCRUB_CHARS + 100)}`)).not.toContain(TOKEN); // the rule matches here
    expect(naiveScrub(text)).toContain(TOKEN);
    expect(scrubLong(text)).not.toContain(TOKEN);
    // Without that earlier cut there is no safe place at all: refused, not leaked.
    expect(() => scrubLong(text.replace(" ", "-"))).toThrow(/scrub limit/);
  });

  it("never cuts inside a private key block that straddles the bound", () => {
    const key = `-----BEGIN PRIVATE KEY-----\n${"MIIEvQIBADANBgkqhkiG9w0BAQEFAASC ".repeat(200)}\n-----END PRIVATE KEY-----`;
    const text = `${words(MAX_SCRUB_CHARS - 3000)} ${key} ${words(10_000)}`;
    const out = scrubLong(text);
    expect(out).not.toContain("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC");
  });

  it("refuses an unbroken run with no safe cut rather than storing it partly scrubbed", () => {
    const blob = "A".repeat(MAX_SCRUB_CHARS + 10);
    expect(canScrubLong(blob)).toBe(false);
    expect(() => scrubLong(blob)).toThrow(/scrub limit/);
  });
});

describe("sealRawChunk with a record over the old bound (revert-fails)", () => {
  const dirs: string[] = [];
  afterEach(async () => {
    await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
  });

  it("stores the whole string, scrubbed, under scrub version 2", async () => {
    const dir = await mkdtemp(join(tmpdir(), "long-strings-"));
    dirs.push(dir);
    const store = createFsStore({ root: dir });
    const value = `${words(400_000)} ${AWS} tail`;
    const source = new TextEncoder().encode(`${JSON.stringify({ kind: "tool_call", payload: { output: value } })}\n`);
    // Version 1 refused this chunk with chunk_string_too_long.
    const sealed = await sealRawChunk(store, {
      location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "h", sessionId: "s", startDate: "2026-10-31" },
      format: "ndjson",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source,
    });
    expect(sealed.scrub).toEqual({ ruleset: "relayhistory-scrub", version: 2 });
    const stored = new TextDecoder().decode(zstdDecompressSync(await readAll(await store.get(sealed.key as never))));
    const out = (JSON.parse(stored) as { payload: { output: string } }).payload.output;
    expect(out).not.toContain(AWS);
    expect(out.endsWith(" tail")).toBe(true);
    expect(out.length).toBeGreaterThan(400_000);
    expect(scrubRecords(source).records).toBe(1);
  });
});

// ---------------------------------------------------------------- review of a26aaf5

const filler = (n: number) => "lorem ipsum dolor sit amet ".repeat(Math.ceil(n / 27)).slice(0, n);

describe("a gap between a credential keyword and its value is never cut", () => {
  const tok = "Zq8vR2kLm9Xp4Tn7Wc3Ys6Hd";
  it("Bearer <5000 spaces> token: redacted or refused, never stored", () => {
    const s = `Bearer${" ".repeat(5000)}${tok}`;
    expect(scrubText(s)).not.toContain(tok);
    const text = `${filler(MAX_SCRUB_CHARS - 2500)}${s} ${filler(10_000)}`;
    let out = "";
    try {
      out = scrubLong(text);
    } catch {
      return; // refusal is safe
    }
    expect(out).not.toContain(tok);
  });

  it("password: <5000 newlines> value", () => {
    const s = `password:${"\n".repeat(5000)}hunter2hunter2`;
    const text = `${filler(MAX_SCRUB_CHARS - 2500)}${s} ${filler(10_000)}`;
    let out = "";
    try {
      out = scrubLong(text);
    } catch {
      return;
    }
    expect(out).not.toContain("hunter2hunter2");
  });

  it("the same through a JSON record (live ingest path)", () => {
    const text = `${filler(MAX_SCRUB_CHARS - 2500)}Bearer${" ".repeat(5000)}${tok} ${filler(10_000)}`;
    let out = "";
    try {
      out = new TextDecoder().decode(scrubRecords(new TextEncoder().encode(`${JSON.stringify({ output: text })}\n`)).scrubbed);
    } catch {
      return;
    }
    expect(out).not.toContain(tok);
  });

  it("a short gap right at the bound is not cut between the keyword and its value", () => {
    // Cuts fall between pairs (after a value), never between a keyword and its value.
    const pairs = "password: hunter2hunter2 ".repeat(Math.ceil((MAX_SCRUB_CHARS + 5000) / 25));
    expect(scrubText("password: hunter2hunter2")).not.toContain("hunter2hunter2");
    expect(scrubLong(pairs)).not.toContain("hunter2hunter2");

    // With ordinary text before it, the cut falls before the keyword and the value is redacted.
    const text = `${filler(MAX_SCRUB_CHARS - 40)} password: ${" ".repeat(200)}hunter2hunter2 ${filler(5000)}`;
    expect(scrubLong(text)).not.toContain("hunter2hunter2");
  });
});

describe("bounded CPU before a refusal", () => {
  it("a 263 KB adversarial string is refused in well under a second (was ~53 s)", () => {
    const bad = "key: ".repeat(Math.ceil((MAX_SCRUB_CHARS + 1000) / 5));
    const t0 = performance.now();
    expect(() => scrubLong(bad)).toThrow(/scrub limit/);
    const t1 = performance.now();
    expect(() => scrubRecords(new TextEncoder().encode(`${JSON.stringify({ o: bad })}\n`))).toThrow(/scrub limit/);
    const t2 = performance.now();
    expect(t1 - t0).toBeLessThan(3000);
    expect(t2 - t1).toBeLessThan(6000);
  });

  it("a 10.8 MB benign text still scrubs whole", () => {
    const words = "lorem ipsum dolor sit amet ".repeat(400_000);
    expect(scrubLong(words)).toBe(words);
  });
});

describe("private key blocks across the bound", () => {
  it("a 20 KB PGP private key block straddling the bound stays redacted", () => {
    const key = `-----BEGIN PGP PRIVATE KEY BLOCK-----\n${"lQdGBGVsZWN0cm9uaWNzZWNyZXRrZXltYXRlcmlhbA ".repeat(500)}\n-----END PGP PRIVATE KEY BLOCK-----`;
    const text = `${filler(MAX_SCRUB_CHARS - 10_000)} ${key} ${filler(10_000)}`;
    expect(scrubLong(text)).not.toContain("lQdGBGVsZWN0cm9uaWNzZWNyZXRrZXltYXRlcmlhbA");
  });
});

describe("a whitespace run longer than the context window", () => {
  it("is never cut at its end when the keyword is out of the window (Bearer + 5000 spaces ending below the bound)", () => {
    const tok = "Zq8vR2kLm9Xp4Tn7Wc3Ys6Hd";
    const gap = 5000;
    const prefix = `${filler(MAX_SCRUB_CHARS - 11 - gap - "Bearer".length)} `;
    const text = `${prefix}Bearer${" ".repeat(gap)}${tok} ${filler(20_000)}`;
    // The rule redacts it in one piece.
    expect(scrubText(`x Bearer${" ".repeat(gap)}${tok} y`)).not.toContain(tok);
    let out = "";
    try {
      out = scrubLong(text);
    } catch {
      return;
    }
    expect(out).not.toContain(tok);
  });
});

// ---------------------------------------------------------------- review of 986826b

const enc = new TextEncoder();
const dec = new TextDecoder();
const logLines = (n: number) => "a log line of ordinary text here\n".repeat(Math.ceil(n / 33));

describe("no regex state shared between calls (C1)", () => {
  it("a private key in a line run is redacted before and after an unrelated long string mentioned a key header", () => {
    const MAT = "MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEePzNm0tRgeLezV6ffAt0gun";
    const pem = `-----BEGIN RSA PRIVATE KEY-----\n${`${MAT}\n`.repeat(320)}-----END RSA PRIVATE KEY-----\n`;
    const b = `${logLines(MAX_SCRUB_CHARS - 10_000)}${pem}${logLines(20_000)}`;
    const runB = () => {
      try {
        return dec.decode(scrubRecords(enc.encode(b)).scrubbed).includes(MAT) ? "LEAK" : "redacted";
      } catch {
        return "refused";
      }
    };
    expect(runB()).not.toBe("LEAK");
    const a = `${filler(MAX_SCRUB_CHARS + 50_000)} -----BEGIN EC PRIVATE KEY----- x -----END EC PRIVATE KEY----- ${filler(1000)}`;
    scrubRecords(enc.encode(`${JSON.stringify({ o: a })}\n`));
    scrubLong(a);
    expect(runB()).not.toBe("LEAK");
    expect(runB()).not.toBe("LEAK");
  });
});

describe("bounded work on the line path (H2)", () => {
  it("blank-line runs cost linear work, counted (not timed)", () => {
    const work = () => segmenterWork.walked + segmenterWork.candidates;
    for (const text of [`x\n${"\n".repeat(30_000)}y\n`, " \n".repeat(150_000), `x\n${"\n".repeat(300_000)}y\n`]) {
      const before = work();
      try {
        scrubRecords(enc.encode(text));
      } catch {
        // refusing is acceptable; quadratic work is not
      }
      // Linear: a few walks of the input at most (the quadratic form was ~n^2 / 2).
      expect(work() - before).toBeLessThan(10 * text.length);
    }
  }, 120_000);
});

describe("prefixed credential names (H3)", () => {
  for (const name of ["DB_PASSWORD", "AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "STRIPE_API_KEY"]) {
    it(`${name}\n=\n<secret> in one long line is never split from its value (the reported shape)`, () => {
      const secret = "Zq8vR2kLm9Xp4Tn7Wc3Ys6HdQQ";
      const s = `${name}\n=\n${secret}`;
      expect(scrubText(s)).not.toContain(secret);
      const leaks: number[] = [];
      for (let d = -40; d <= 40; d++) {
        const text = `${filler(MAX_SCRUB_CHARS - name.length + d)} ${s} ${filler(20_000)}\n`;
        let out: string;
        try {
          out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
        } catch {
          continue;
        }
        if (out.includes(secret)) leaks.push(d);
      }
      expect(leaks).toEqual([]);
    }, 120_000);
  }

  it("the keyword guard alone: `DB_PASSWORD` then `=` and a whitespace gap past the context window", () => {
    // Without recognising DB_PASSWORD (\b fails after `_`), the cut after its
    // line looks safe: the value is more than 4 KiB away, outside the window.
    const secret = "Zq8vR2kLm9Xp4Tn7Wc3Ys6HdQQ";
    const s = `DB_PASSWORD\n=${" ".repeat(1000)}\n${" ".repeat(1000)}\n${" ".repeat(1000)}\n${" ".repeat(1000)}\n${" ".repeat(900)}${secret}`;
    expect(scrubText(`x ${s} y`)).not.toContain(secret);
    const leaks: number[] = [];
    for (let d = -20; d <= 20; d++) {
      const text = `${logLines(MAX_SCRUB_CHARS - 12 + d).slice(0, MAX_SCRUB_CHARS - 12 + d)}${s}\n${logLines(20_000)}`;
      let out: string;
      try {
        out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
      } catch {
        continue;
      }
      if (out.includes(secret)) leaks.push(d);
    }
    expect(leaks).toEqual([]);
  }, 120_000);
});

describe("line-path flushes before a record or key line are context-checked (M-3)", () => {
  const secret = "Zq8vR2kLm9Xp4Tn7Wc3Ys6HdQQ";
  const key = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEeP\n-----END RSA PRIVATE KEY-----";
  const cases: [string, string][] = [
    ["DB_PASSWORD= then a JSON string line", `log\nDB_PASSWORD=\n"${secret}"\n`],
    ["api_key: then a JSON string line", `log\napi_key:\n"${secret}"\n`],
    ["Authorization: Bearer then a key line", `log\nAuthorization: Bearer\n${secret} ${key}\n`],
    ["password: then a key line", `log\npassword:\n${secret} ${key}\n`],
  ];
  for (const [label, text] of cases)
    it(label, () => {
      expect(scrubText(text)).not.toContain(secret);
      expect(dec.decode(scrubRecords(enc.encode(text)).scrubbed)).not.toContain(secret);
    });
  it("an ordinary record line after ordinary text is still scrubbed as its own record", () => {
    const text = `just a log line\n${JSON.stringify({ a: "hello" })}\n`;
    expect(dec.decode(scrubRecords(enc.encode(text)).scrubbed)).toBe(text);
  });
});

describe("availability: lines ending in =, : or a keyword are stored, not refused (M-2)", () => {
  // (Lines like `next page token:` are different: the text rules themselves read
  // the next line's first word as that token's value, so every boundary there
  // splits a match and refusing is correct.)
  for (const [label, line] of [
    ["set=", (n: number) => `debug flag value ${n} set=\n`],
    ["key", (n: number) => `row ${n} looked up by primary key\n`],
  ] as const)
    it(`400 KiB of "${label}" lines`, () => {
      let text = "";
      for (let n = 0; text.length < 400 * 1024; n++) text += line(n);
      const out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
      expect(out.length).toBeGreaterThan(300 * 1024);
    }, 120_000);
});

// ---------------------------------------------------------------- review #3 of aff76c5

describe("record boundaries: bounded work and intact records", () => {
  it("mixed text and JSON logs run no boundary scrub without a credential cue (counted)", () => {
    let text = "";
    for (let n = 0; text.length < 1024 * 1024; n++)
      text += `2026-11-01 worker ${n} handled request in 12ms\n${JSON.stringify({ n, msg: "ok", path: `/v1/x/${n}` })}\n`;
    const before = segmenterWork.candidates;
    const out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
    // Rule set 1 ran none; a check per record line here would be ~20,000.
    expect(segmenterWork.candidates - before).toBeLessThan(50);
    expect(out).toBe(text);
  }, 120_000);

  it("a log where every line is a cue before a JSON record costs a bounded number of checks per chunk", () => {
    let text = "";
    for (let n = 0; text.length < 1024 * 1024; n++) text += `config token:\n${JSON.stringify({ n, msg: "ok" })}\n`;
    const before = segmenterWork.candidates;
    const out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
    expect(segmenterWork.candidates - before).toBeLessThanOrEqual(256);
    // Objects stay intact past the cap (the cap only ever redacts a leading JSON string).
    expect(out).toBe(text);
  }, 120_000);

  for (const cue of ["password:", "token:", "key ="]) {
    it(`a JSON object or array after \`${cue}\` stays byte-identical`, () => {
      for (const record of [JSON.stringify({ a: 1, b: "x" }), JSON.stringify([1, "two"])]) {
        const text = `log line\nconfig ${cue}\n${record}\nafter\n`;
        const out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
        expect(out.split("\n")[2]).toBe(record);
        expect(out.endsWith("after\n")).toBe(true);
      }
    });
  }

  it("a JSON string that is the value of a keyword on the line before is redacted and stays valid JSON", () => {
    const secret = "Zq8vR2kLm9Xp4Tn7Wc3Ys6HdQQ";
    const out = dec.decode(scrubRecords(enc.encode(`log\nDB_PASSWORD=\n"${secret}"\nnext\n`)).scrubbed);
    expect(out).not.toContain(secret);
    expect(JSON.parse(out.split("\n")[2]!)).toBe("[REDACTED]");
    expect(out.endsWith("next\n")).toBe(true);
  });
});

// ---------------------------------------------------------------- review #4 of 3b4b60b (H-B)

describe("a JSON line inside an open private-key block does not split the key (H-B)", () => {
  const b64a = "MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEeP";
  const b64b = "Xq7Lm2Rt9Vw4Ys8Pz3Kd6Hn1Bc5Gf0Jj2Aa9Ee4Ii7O";
  const cases: [string, string][] = [
    [
      "RSA key with a JSON log line between body lines",
      `-----BEGIN RSA PRIVATE KEY-----\n${b64a}\n{"log":"x"}\n${b64b}\n-----END RSA PRIVATE KEY-----\n`,
    ],
    [
      "EC key header mid-line, a JSON line right after it",
      `key follows -----BEGIN EC PRIVATE KEY-----\n{"a":1}\n${b64b}\n-----END EC PRIVATE KEY-----\n`,
    ],
  ];
  for (const [label, text] of cases)
    it(label, () => {
      // Whole-text scrubbing redacts the block; the chunk scrubber must too.
      expect(scrubText(text)).not.toContain(b64b);
      const { scrubbed, records } = scrubRecords(enc.encode(text));
      const out = dec.decode(scrubbed);
      expect(out).not.toContain(b64a);
      expect(out).not.toContain(b64b);
      expect(out).not.toContain("-----END");
      expect(records).toBe(text.split("\n").length - 1);
    });
});

// ------------------------------------- independent review of ea3d6cd (K-1), bulls-lead-claude

describe("a cut never lands inside a private-key block that closed before the overflow (K-1)", () => {
  const body = (i: number) => `MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEeP${String(i).padStart(4, "0")}`;
  const build = (gapLines: number) => {
    const tail: string[] = [];
    // The cue makes the header boundary unsafe, so the run is not flushed before the key.
    tail.push("Here is the private key:");
    tail.push("-----BEGIN RSA PRIVATE KEY-----");
    for (let i = 0; i < 300; i++) tail.push(body(i));
    // Whitespace runs over MAX_CUT_GAP inside the block push the search back into it.
    for (let i = 300; i < 300 + gapLines; i++) tail.push(body(i) + " ".repeat(1100));
    tail.push("-----END RSA PRIVATE KEY----- 1");
    // Digit-straddled lines: every later boundary is unsafe.
    for (let i = 0; i < 1500; i++) tail.push(`${i},${i * 7},${i * 13}`);
    const tailText = tail.join("\n") + "\n";
    // Filler so the whole text just passes the bound, overflowing inside the numeric lines.
    const filler = "lorem ipsum dolor sit amet ".repeat(40) + "\n";
    const need = Math.ceil((MAX_SCRUB_CHARS + 4_096 - tailText.length) / filler.length);
    return filler.repeat(Math.max(0, need)) + tailText;
  };
  for (const gaps of [40, 1])
    it(`no key line survives (${gaps} gap line${gaps === 1 ? "" : "s"})`, () => {
      const text = build(gaps);
      expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
      expect(scrubText(text)).not.toContain(body(0));
      // Either the chunk is refused (no safe cut within the search window: never stored
      // partly scrubbed) or it is stored with the whole key redacted. Never a key line in clear.
      let out: string;
      try {
        out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
      } catch (error) {
        expect((error as { code?: string }).code).toBe("chunk_unsegmentable");
        return;
      }
      for (let i = 0; i < 300 + gaps; i++) expect(out).not.toContain(body(i));
      expect(out).not.toContain("-----END RSA PRIVATE KEY");
    });
});

// ------------------------- independent re-review of 7f0035a (K-2 and shared dashes)

describe("a record inside an open key block joins the run record-scrubbed (K-2)", () => {
  it("a token after the footer in the closing record is redacted, as on main", () => {
    const tok = "ghp_AbcdEFGHijklMNOPqrstUVWX12";
    const rec = JSON.stringify({ t: "-----END RSA PRIVATE KEY-----", out: "first line\n" + tok + " more" });
    const out = dec.decode(scrubRecords(enc.encode(`-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n${rec}\n`)).scrubbed);
    expect(out).not.toContain(tok);
    expect(out).not.toContain("MIIabc");
  });
});

describe("a header that shares its dashes with a footer leaves the block open, as the rule does", () => {
  const body = "MIIEow" + "Q".repeat(58) + "5";
  it("a record line after it does not flush the key body out unredacted", () => {
    const text = `x\n-----BEGIN RSA PRIVATE KEY-----END RSA PRIVATE KEY-----\n{"a":1}\n${body}\n`;
    expect(scrubText(text)).not.toContain(body);
    const out = dec.decode(scrubRecords(enc.encode(text)).scrubbed);
    expect(out).not.toContain(body);
  });
});
