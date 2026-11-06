import { describe, expect, test } from "bun:test";
import { parseCliLogin, ownerPlanInputs, withOwnerPlanClient } from "../../../scripts/schema-release-owner-operator-plan.ts";
const good = ["export PGHOST='db.abcdefghijklmnopqrst.supabase.co'", "export PGPORT='5432'", "export PGUSER='cli_login_postgres'", "export PGPASSWORD='fake-unit-only-credential-value'", "export PGDATABASE='postgres'"].join("\n");
describe("existing approved project owner plan", () => {
  test("reads CLI credential in memory without interpreting unrelated shell commands", () => { const p = parseCliLogin(good + "\nrm -rf /never-run-this-unit-fixture"); expect(p.password).toBe("fake-unit-only-credential-value"); });
  for (const [from, to] of [["abcdefghijklmnopqrst", "anotherproject"], ["5432", "6543"], ["cli_login_postgres", "postgres"], ["PGDATABASE='postgres'", "PGDATABASE='other'"], ["fake-unit-only-credential-value", "[REDACTED]"], ["fake-unit-only-credential-value", "x"]]) {
    test(`refuses altered credential target ${from}`, () => expect(() => parseCliLogin(good.replace(from, to))).toThrow());
  }
  test("rejects duplicate fields instead of accepting a last-value override", () => expect(() => parseCliLogin(good + "\nexport PGUSER='postgres'")).toThrow());
  test("does not execute unquoted command substitution", () => expect(() => parseCliLogin(good.replace("PGUSER='cli_login_postgres'", "PGUSER=$(touch /never-unit-fixture)"))).toThrow());
  test("refuses oversized CLI output", () => expect(() => parseCliLogin(good + "x".repeat(65536))).toThrow());
  test("refuses missing credential fields", () => expect(() => parseCliLogin(good.split("\n").filter(s => !s.startsWith("export PGPASSWORD=")).join("\n"))).toThrow());
  const env = { OPERATOR_MODE: "plan", OPERATOR_SOURCE_SHA: "a".repeat(40), OPERATOR_GENERATION: "b".repeat(64) };
  test("accepts a pinned read-only plan invocation", () => expect(ownerPlanInputs(env, ["node", "script"])).toEqual({ sha: "a".repeat(40), digest: "b".repeat(64) }));
  for (const change of [{ OPERATOR_MODE: "apply" }, { OPERATOR_MODE: undefined }, { OPERATOR_SOURCE_SHA: "main" }, { OPERATOR_GENERATION: "latest" }]) test(`refuses unsafe mode or unpinned source ${JSON.stringify(change)}`, () => expect(() => ownerPlanInputs({ ...env, ...change }, ["node", "script"])).toThrow());
  test("rejects an apply argument even in plan mode", () => expect(() => ownerPlanInputs(env, ["node", "script", "apply"])).toThrow());
});

describe("operator connection cleanup without network", () => {
  for (const failure of ["connect", "begin", "work", "rollback", "none"] as const) {
    test(`closes when ${failure} fails; rolls back only a begun transaction`, async () => {
      const calls: string[] = [];
      const client = {
        async connect() { calls.push("connect"); if (failure === "connect") throw new Error("connect"); },
        async query(sql: string) {
          const kind = sql.startsWith("BEGIN") ? "begin" : "rollback";
          calls.push(kind);
          if (failure === kind) throw new Error(kind);
          return {};
        },
        async end() { calls.push("end"); },
      };
      const result = withOwnerPlanClient(client, async () => { calls.push("work"); if (failure === "work") throw new Error("work"); return 42; });
      if (["connect", "begin", "work"].includes(failure)) await expect(result).rejects.toThrow(failure);
      else expect(await result).toBe(42);
      expect(calls).toEqual(failure === "connect" ? ["connect", "end"] : failure === "begin" ? ["connect", "begin", "end"] : ["connect", "begin", "work", "rollback", "end"]);
    });
  }
});
