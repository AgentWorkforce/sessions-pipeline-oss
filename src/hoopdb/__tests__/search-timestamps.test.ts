/**
 * The API's one timestamp parser (canonicalTimestamp) and searchRpcRequest's use of
 * it. Expectations are fixed UTC constants, so this file must pass identically under
 * any host time zone; search-timestamps-tz-matrix.test.ts runs it under TZ=UTC and
 * TZ=America/Los_Angeles. PGlite's session is UTC, like the production PostgreSQL
 * session, and the parser must agree with it.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { canonicalTimestamp, searchRpcRequest } from "../search-query.ts";

const CASES: Array<[string, string | null]> = [
  ["2026-09-10T12:00:00", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10 12:00:00", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10T12:00", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10", "2026-09-10T00:00:00.000000Z"],
  ["2026-09-10T12:00:00Z", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10T12:00:00.000Z", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10T12:00:00.123456", "2026-09-10T12:00:00.123456Z"],
  ["2026-09-10T12:00:00.000001Z", "2026-09-10T12:00:00.000001Z"],
  ["2026-09-10T14:00:00+02:00", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10T07:00:00-05:00", "2026-09-10T12:00:00.000000Z"],
  ["2026-09-10T17:30:00+0530", "2026-09-10T12:00:00.000000Z"],
  ["2026-12-31T23:30:00-01:00", "2027-01-01T00:30:00.000000Z"],
  // A US DST transition day: offset-less is still UTC, not Los Angeles time.
  ["2026-11-01T01:30:00", "2026-11-01T01:30:00.000000Z"],
  ["2026-03-08T02:30:00", "2026-03-08T02:30:00.000000Z"],
  ["not-a-date", null],
  ["2026-02-30", null],
  ["2026-13-01T00:00:00Z", null],
  ["2026-09-10T24:00:00Z", null],
  ["2026-09-10T12:00:00.1234567Z", null],
  ["", null],
];

describe(`canonical timestamps (TZ=${process.env.TZ ?? "unset"})`, () => {
  test("the host time zone took effect (so the matrix really varies it)", () => {
    const offset = new Date("2026-10-31T12:00:00Z").getTimezoneOffset();
    if (process.env.TZ === "America/Los_Angeles") expect(offset).toBe(420);
    if (process.env.TZ === "UTC") expect(offset).toBe(0);
  });

  test("offset-less is UTC, offsets convert, microseconds are kept, invalid is null", () => {
    for (const [input, expected] of CASES)
      expect(canonicalTimestamp(input), input).toBe(expected);
  });

  test("searchRpcRequest sends canonical UTC for since, until and the cursor", () => {
    const built = searchRpcRequest({
      workspaceId: "ws-a",
      since: "2026-09-10T05:00:00-07:00",
      until: "2026-09-10 12:00:00.5",
      cursor: {
        lastActivityAt: "2026-09-10T12:00:00.123456",
        sessionId: "s-1",
        harness: "codex",
      },
    });
    expect(built.ok).toBe(true);
    if (!built.ok) return;
    expect(built.body.p_since).toBe("2026-09-10T12:00:00.000000Z");
    expect(built.body.p_until).toBe("2026-09-10T12:00:00.500000Z");
    expect(built.body.p_cursor_last_activity_at).toBe(
      "2026-09-10T12:00:00.123456Z",
    );
  });

  test("since > until is decided at microsecond precision, independent of the host zone", () => {
    const refused = searchRpcRequest({
      workspaceId: "ws-a",
      since: "2026-09-10T12:00:00.000002",
      until: "2026-09-10T12:00:00.000001",
    });
    expect(!refused.ok && refused.code).toBe("22023");
    const equal = searchRpcRequest({
      workspaceId: "ws-a",
      since: "2026-09-10T12:00:00",
      until: "2026-09-10T05:00:00-07:00",
    });
    expect(equal.ok).toBe(true);
    for (const field of ["since", "until"] as const) {
      const bad = searchRpcRequest({
        workspaceId: "ws-a",
        [field]: "2026-02-30",
      });
      expect(!bad.ok && bad.code, field).toBe("22023");
    }
  });
});

describe("agreement with the UTC PostgreSQL session", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = new PGlite();
    // The production PostgreSQL session is UTC whatever the client host's zone.
    await db.exec("SET TIME ZONE 'UTC'");
  });
  afterAll(async () => {
    await db?.close();
  });

  test("PostgreSQL reads every valid input and its canonical form as the same instant", async () => {
    const offset = (
      await db.query<{ o: number }>(
        "SELECT EXTRACT(TIMEZONE FROM now())::int AS o",
      )
    ).rows[0]!.o;
    expect(offset).toBe(0);
    for (const [input, expected] of CASES) {
      if (expected === null) continue;
      const r = (
        await db.query<{ eq: boolean; text: string }>(
          `SELECT $1::timestamptz = $2::timestamptz AS eq,
                to_char($2::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS text`,
          [input, expected],
        )
      ).rows[0]!;
      expect(r.eq, input).toBe(true);
      expect(r.text, input).toBe(expected);
    }
  });
});
