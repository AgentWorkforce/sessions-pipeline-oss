/**
 * Error codes shared by every adapter, mirroring the Rust crate's `Error`
 * (`plugins/object-store/rust/src/error.rs` in relayhistory). Callers branch on
 * `code`, never on message text.
 */
export type ObjectStoreErrorCode =
  | "not_found"
  /** A create (`ifNoneMatch`) found an object with *different* content. */
  | "already_exists"
  /** A compare-and-swap (`ifMatch`) lost a race, or the object is missing. */
  | "precondition_failed"
  /** The body did not hash to the caller's `contentSha256`. */
  | "content_mismatch"
  | "invalid_key"
  | "invalid_argument"
  | "unsupported"
  | "unauthorized"
  /** Network failure, 408, 429 or 5xx: safe to retry with backoff. */
  | "transient"
  | "backend";

export class ObjectStoreError extends Error {
  readonly code: ObjectStoreErrorCode;
  readonly backend: string;
  readonly status: number | undefined;

  constructor(
    code: ObjectStoreErrorCode,
    backend: string,
    message: string,
    options: { status?: number; cause?: unknown } = {},
  ) {
    super(`${backend}: ${message}`, { cause: options.cause });
    this.name = "ObjectStoreError";
    this.code = code;
    this.backend = backend;
    this.status = options.status;
  }

  get retryable(): boolean {
    return this.code === "transient";
  }
}

export function isObjectStoreError(
  err: unknown,
  code?: ObjectStoreErrorCode,
): err is ObjectStoreError {
  return (
    err instanceof ObjectStoreError && (code === undefined || err.code === code)
  );
}

/** Map an HTTP status that is not otherwise handled. Never includes credentials. */
export function errorForStatus(
  backend: string,
  key: string,
  status: number,
  body: string,
): ObjectStoreError {
  const detail = `${key}: HTTP ${status}${body ? `: ${body.slice(0, 300)}` : ""}`;
  if (status === 404)
    return new ObjectStoreError("not_found", backend, detail, { status });
  if (status === 412)
    return new ObjectStoreError("precondition_failed", backend, detail, {
      status,
    });
  if (status === 401 || status === 403)
    return new ObjectStoreError("unauthorized", backend, detail, { status });
  if (status === 408 || status === 429 || status >= 500)
    return new ObjectStoreError("transient", backend, detail, { status });
  return new ObjectStoreError("backend", backend, detail, { status });
}
