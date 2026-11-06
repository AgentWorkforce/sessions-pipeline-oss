/** Source/container contract. The Lambda build plugin replaces this module with
 * a literal capture of the same migration generation and verifier files. */
import { readFile } from "node:fs/promises";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { ROLE_VERIFY_SQL_PATH, TOKEN_VERIFIER_VERIFY_SQL_PATH, INGEST_SEED_VERIFY_SQL_PATH, INGEST_ROLE_VERIFY_SQL_PATH, type ExpectedSchema, type SchemaVerifierSql } from "../shared/db.ts";

export interface DerivedSchemaAssets {
  expected: ExpectedSchema;
  verifiers: SchemaVerifierSql;
}

export async function derivedSchemaAssets(): Promise<DerivedSchemaAssets> {
  const expected = await expectedSchema();
  const [searchWorker, tokenVerifier,ingestSeeder,ingest] = await Promise.all([
    readFile(ROLE_VERIFY_SQL_PATH, "utf8"), readFile(TOKEN_VERIFIER_VERIFY_SQL_PATH, "utf8"),readFile(INGEST_SEED_VERIFY_SQL_PATH,"utf8"),readFile(INGEST_ROLE_VERIFY_SQL_PATH,"utf8"),
  ]);
  return { expected, verifiers: { searchWorker, tokenVerifier,ingestSeeder,ingest } };
}
