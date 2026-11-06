// The DOM `BodyInit` name, for code shared with the Worker (the vendored
// ClickHouse client in src/session-pipeline/vendor/search-index/clickhouse.ts).
// Node's fetch types define RequestInit but not this alias.
type BodyInit = NonNullable<RequestInit["body"]>;
