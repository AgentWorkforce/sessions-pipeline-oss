/** Embed nonsecret schema-gate assets in the existing Lambda build. No runtime
 * role checks are changed, and no migration or production query is executed. */
import { fileURLToPath } from "node:url";
import { resolve, join } from "node:path";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";
import { ROLE_VERIFY_SQL_PATH, TOKEN_VERIFIER_VERIFY_SQL_PATH,INGEST_SEED_VERIFY_SQL_PATH,INGEST_ROLE_VERIFY_SQL_PATH } from "../../src/shared/db.ts";

const MODULE = fileURLToPath(new URL("../../src/session-pipeline/derived-schema.ts", import.meta.url));
const ROOT = fileURLToPath(new URL("../..", import.meta.url));

export async function packagedDerivedSchemaSource(onInput?: (path: string) => void): Promise<string> {
  const first = await derivedSchemaAssets();
  const final = await derivedSchemaAssets();
  // Reject a source tree edited while its schema and verifier inputs are captured.
  // dbMode is a runtime setting and is deliberately not taken from the build shell.
  delete first.expected.dbMode;
  delete final.expected.dbMode;
  if (JSON.stringify(first) !== JSON.stringify(final))
    throw new Error("derived schema sources changed during the build");
  if (!first.expected.names.length || !/^[0-9a-f]{64}$/.test(first.expected.generation) ||
      !first.verifiers.searchWorker.trim() || !first.verifiers.tokenVerifier.trim()||!first.verifiers.ingestSeeder.trim()||!first.verifiers.ingest.trim())
    throw new Error("derived schema build assets are incomplete");
  // SQL is a build input even though it is absent from the runtime module graph.
  for (const path of [ROLE_VERIFY_SQL_PATH, TOKEN_VERIFIER_VERIFY_SQL_PATH,INGEST_SEED_VERIFY_SQL_PATH,INGEST_ROLE_VERIFY_SQL_PATH,
    ...first.expected.names.map(name => {
      const [kind, file] = name.split(":");
      if (!file || !["upstream", "96bulls"].includes(kind!)) throw new Error("unknown schema input");
      return join(ROOT, kind === "upstream" ? "supabase/migrations" : "src/hoopdb/migrations", file);
    })]) onInput?.(path);
  return `const assets = ${JSON.stringify(first)};
Object.freeze(assets.expected.names); Object.freeze(assets.expected);
Object.freeze(assets.verifiers); Object.freeze(assets);
export async function derivedSchemaAssets() {
  const dbMode = process.env.HOOPDB_DB_MODE;
  return { expected: { ...assets.expected,
    ...(["local", "selfhosted", "cloudsql", "supabase"].includes(dbMode ?? "") ? { dbMode } : {}) },
    verifiers: assets.verifiers };
}
`;
}

export function derivedSchemaBuildPlugin() {
  return {
    name: "96bulls-derived-schema-assets",
    async load(this: { addWatchFile(path: string): void }, id: string) {
      if (resolve(id.split("?")[0]!) !== resolve(MODULE)) return null;
      return { code: await packagedDerivedSchemaSource(path => this.addWatchFile(path)), map: null };
    },
  };
}
