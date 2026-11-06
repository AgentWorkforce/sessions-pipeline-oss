/** Separate cron publisher: the ingest HTTP/queue Worker keeps its private,
 * no-cron deployment contract. No uploads, S3 credentials or fetch handler. */
import pg from "pg";
import { canonicalProjectPublisherSettings, type CanonicalProjectPublisherEnv } from "./settings.ts";
import { canonicalProjectEventStore } from "./store.ts";
import { publishCanonicalProjectEvents } from "./publisher.ts";

export default {
  async scheduled(_event: unknown, env: CanonicalProjectPublisherEnv): Promise<void> {
    const settings = canonicalProjectPublisherSettings(env);
    if (!settings) return;
    const client = new pg.Client({ connectionString: settings.connectionString });
    client.on("error", () => {});
    try {
      await client.connect();
      const store = canonicalProjectEventStore({ query: async <T>(text: string, args?: unknown[]) => ({ rows: (await client.query(text, args)).rows as T[] }) }, settings.schemaGeneration, settings.scope);
      const result = await publishCanonicalProjectEvents({ store, queue: settings.queue, schemaGeneration: settings.schemaGeneration, scope: settings.scope });
      console.log(JSON.stringify({ area: "canonical-project-events", event: "published", ...result }));
    } finally { await client.end().catch(() => {}); }
  },
};
