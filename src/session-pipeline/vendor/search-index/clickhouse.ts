/**
 * Minimal ClickHouse HTTP client over `fetch`, shared by the Worker tail and the
 * Node backfill. No dependency: the official web client pulls in more than the
 * three calls ingest needs (command, JSON select, JSONEachRow insert).
 *
 * Nothing here logs. Errors carry only the ClickHouse error code and name: a
 * ClickHouse error body can quote the offending input row, which is transcript
 * content, so the body text never leaves this module.
 */

export interface ClickHouseConfig {
  /** https://host:8443 (Cloud) or http://127.0.0.1:8123 (local). */
  url: string;
  user: string;
  password: string;
  database: string;
}

export class ClickHouseError extends Error {
  readonly code: string | null;
  readonly status: number;
  constructor(status: number, code: string | null, name: string | null) {
    super(
      `ClickHouse request failed (status ${status}${code ? `, code ${code}` : ""}${name ? `, ${name}` : ""})`,
    );
    this.name = "ClickHouseError";
    this.status = status;
    this.code = code;
  }
}

export type QueryParams = Record<string, string | number | boolean | unknown[]>;

export interface ClickHouseClient {
  readonly database: string;
  command(
    query: string,
    options?: RequestOptions & { params?: QueryParams },
  ): Promise<void>;
  select<T>(
    query: string,
    options?: RequestOptions & { params?: QueryParams },
  ): Promise<T[]>;
  insert(
    table: string,
    rows: readonly object[],
    options?: RequestOptions,
  ): Promise<void>;
}

export interface RequestOptions {
  settings?: Record<string, string | number>;
  timeoutMs?: number;
  /**
   * Insert only: server-side buffered insert (async_insert=1) that still
   * waits for the flush (wait_for_async_insert=1), so a returned call means
   * the rows are durable. Used by the minute tail's many small inserts; the
   * backfill keeps large synchronous inserts.
   */
  asyncInsert?: boolean;
}

const DEFAULT_TIMEOUT_MS = 30_000;

/** Encode one query parameter in ClickHouse's text format (arrays/tuples nested). */
export function encodeParam(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(encodeNested).join(",")}]`;
  if (typeof value === "boolean") return value ? "1" : "0";
  // Top-level values are parsed in ClickHouse's escaped (TSV) format.
  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

/** A ClickHouse string literal with every quote and backslash escaped. */
export function quoteString(value: string): string {
  return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

function encodeNested(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(encodeNested).join(",")}]`;
  if (value && typeof value === "object" && "tuple" in value) {
    const items = (value as { tuple: unknown[] }).tuple;
    return `(${items.map(encodeNested).join(",")})`;
  }
  if (typeof value === "number") return String(value);
  if (typeof value === "boolean") return value ? "1" : "0";
  return quoteString(String(value));
}

/**
 * A ClickHouse array literal for inlining into the query body. Used for large
 * key lists, which would overflow a URL as query parameters. Every string is
 * quoted and escaped, so no input can end the literal.
 */
export function arrayLiteral(values: unknown[]): string {
  return `[${values.map(encodeNested).join(",")}]`;
}

/** Marks a value as a ClickHouse Tuple inside an Array parameter. */
export function tuple(...items: unknown[]): { tuple: unknown[] } {
  return { tuple: items };
}

function parseError(status: number, body: string): ClickHouseError {
  const code = body.match(/Code:\s*(\d+)/)?.[1] ?? null;
  // The error name sits just before the trailing "(version ...)".
  const name =
    body.match(/\(([A-Z][A-Z0-9_]{2,})\)\s*\(version [^)]*\)\s*$/)?.[1] ?? null;
  return new ClickHouseError(status, code, name);
}

async function gzip(text: string): Promise<Uint8Array> {
  const stream = new Blob([text])
    .stream()
    .pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export function createClickHouseClient(
  config: ClickHouseConfig,
  fetcher: typeof fetch = fetch,
): ClickHouseClient {
  const base = config.url.replace(/\/+$/, "");
  // Credentials and transcript rows only go over TLS, except to loopback
  // (local tests and the local container).
  const parsed = new URL(base);
  const loopback = ["127.0.0.1", "localhost", "[::1]"].includes(
    parsed.hostname,
  );
  if (parsed.protocol !== "https:" && !loopback) {
    throw new Error("ClickHouse URL must use https");
  }
  const headers = {
    "X-ClickHouse-User": config.user,
    "X-ClickHouse-Key": config.password,
  };

  async function request(
    search: URLSearchParams,
    body: BodyInit,
    extraHeaders: Record<string, string>,
    options: RequestOptions,
  ): Promise<Response> {
    search.set("database", config.database);
    for (const [key, value] of Object.entries(options.settings ?? {})) {
      search.set(key, String(value));
    }
    const response = await fetcher(`${base}/?${search.toString()}`, {
      method: "POST",
      headers: { ...headers, ...extraHeaders },
      body,
      signal: AbortSignal.timeout(options.timeoutMs ?? DEFAULT_TIMEOUT_MS),
    });
    if (!response.ok) {
      throw parseError(response.status, await response.text());
    }
    return response;
  }

  function withParams(params: QueryParams | undefined): URLSearchParams {
    const search = new URLSearchParams();
    for (const [key, value] of Object.entries(params ?? {})) {
      search.set(`param_${key}`, encodeParam(value));
    }
    return search;
  }

  return {
    database: config.database,
    async command(query, options = {}) {
      const search = withParams(options.params);
      search.set("wait_end_of_query", "1");
      const response = await request(search, query, {}, options);
      await response.arrayBuffer();
    },
    async select<T>(
      query: string,
      options: RequestOptions & { params?: QueryParams } = {},
    ) {
      const search = withParams(options.params);
      search.set("default_format", "JSONEachRow");
      search.set("output_format_json_quote_64bit_integers", "0");
      const response = await request(search, query, {}, options);
      const text = await response.text();
      const rows: T[] = [];
      for (const line of text.split("\n")) {
        if (line) rows.push(JSON.parse(line) as T);
      }
      return rows;
    },
    async insert(table, rows, options = {}) {
      if (rows.length === 0) return;
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)) {
        throw new Error("invalid table name");
      }
      const search = new URLSearchParams();
      search.set("query", `INSERT INTO ${table} FORMAT JSONEachRow`);
      // Acknowledged inserts: a returned call means the rows are committed
      // (synchronously, or async with wait_for_async_insert=1).
      search.set("async_insert", options.asyncInsert ? "1" : "0");
      if (options.asyncInsert) {
        search.set("wait_for_async_insert", "1");
        // Bound the flush wait: the tail inserts one session at a time.
        search.set("async_insert_busy_timeout_max_ms", "200");
      }
      search.set("wait_end_of_query", "1");
      search.set("date_time_input_format", "best_effort");
      let text = "";
      for (const row of rows) text += `${JSON.stringify(row)}\n`;
      const body = await gzip(text);
      const response = await request(
        search,
        body,
        { "Content-Encoding": "gzip" },
        { timeoutMs: 300_000, ...options },
      );
      await response.arrayBuffer();
    },
  };
}
