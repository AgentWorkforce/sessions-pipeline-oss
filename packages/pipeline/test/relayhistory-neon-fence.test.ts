/**
 * The Neon write fence for the T0 hold (`src/migration/relayhistory-neon-fence.ts`):
 * scoped to `sessions`, it must stop direct DML and SECURITY DEFINER writers
 * for the runtime role, keep reads working, and restore exactly.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterEach, beforeEach, expect, it } from "vitest";
import { planFence } from "../../../src/migration/relayhistory-neon-fence.js";

let db: PGlite;
beforeEach(async () => {
  db = new PGlite();
  await db.exec(`CREATE ROLE app NOLOGIN;
    CREATE SCHEMA sessions; GRANT USAGE ON SCHEMA sessions TO app;
    CREATE SCHEMA cloud; GRANT USAGE, CREATE ON SCHEMA cloud TO app;
    CREATE TABLE cloud.public_rows (id int); GRANT INSERT, SELECT ON cloud.public_rows TO app;
    CREATE TABLE sessions.events (id int PRIMARY KEY, body text);
    GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.events TO app;
    INSERT INTO sessions.events VALUES (1, 'pre-T0');
    -- A writer the app calls; runs as its owner, like accept_delivery_batch.
    CREATE FUNCTION sessions.accept(p int) RETURNS void LANGUAGE sql SECURITY DEFINER
      AS $$ INSERT INTO sessions.events VALUES (p, 'via function') $$;
    -- Default ACL (NULL): EXECUTE to PUBLIC.
    CREATE FUNCTION sessions.ack(p int) RETURNS void LANGUAGE sql SECURITY DEFINER
      AS $$ UPDATE sessions.events SET body = 'acked' WHERE id = p $$;
    CREATE FUNCTION sessions.count_events() RETURNS bigint LANGUAGE sql STABLE
      AS $$ SELECT count(*) FROM sessions.events $$;
    REVOKE ALL ON FUNCTION sessions.accept(int) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION sessions.accept(int) TO app;`);
});
afterEach(async () => {
  await db.close();
});

const asApp = async (sql: string) => {
  await db.exec("SET ROLE app");
  try {
    return await db.query(sql);
  } finally {
    await db.exec("RESET ROLE");
  }
};
const exec = (statements: string[]) => db.exec(statements.join("\n"));

it("stops every sessions write for the role, keeps reads and other schemas, and restores", async () => {
  const plan = await planFence(db, ["app"]);
  expect(plan).toMatchObject({ tables: 1, volatileFunctions: 2, readableFunctions: 1 });

  await exec(plan.fence);
  for (const write of [
    "INSERT INTO sessions.events VALUES (2, 'x')",
    "UPDATE sessions.events SET body = 'x'",
    "DELETE FROM sessions.events",
    "SELECT sessions.accept(3)",
    "SELECT sessions.ack(1)",
  ])
    await expect(asApp(write), write).rejects.toThrow(/permission denied/);
  expect((await asApp("SELECT body FROM sessions.events")).rows).toEqual([{ body: "pre-T0" }]);
  expect((await asApp("SELECT sessions.count_events() AS n")).rows).toEqual([{ n: 1 }]);
  // Cloud's own schema is untouched.
  await asApp("INSERT INTO cloud.public_rows VALUES (1)");

  await exec(plan.restore);
  await asApp("INSERT INTO sessions.events VALUES (2, 'after restore')");
  await asApp("SELECT sessions.accept(3)");
  await asApp("SELECT sessions.ack(1)");
  expect((await db.query("SELECT id, body FROM sessions.events ORDER BY id")).rows).toEqual([
    { id: 1, body: "acked" },
    { id: 2, body: "after restore" },
    { id: 3, body: "via function" },
  ]);
});

it("rejects a malformed role name", async () => {
  await expect(planFence(db, ["app; DROP TABLE x"])).rejects.toThrow("bad role name");
  await expect(planFence(db, [])).rejects.toThrow("at least one role");
});
