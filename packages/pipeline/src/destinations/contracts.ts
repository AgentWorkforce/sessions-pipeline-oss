/** Portable destination contracts. Authentication and scrubbing precede this API. */
export interface DestinationTenant {
  readonly orgId: string;
  readonly workspaceId: string;
}

/** An immutable delivery identity within one authenticated tenant. */
export interface DestinationDelivery {
  readonly schemaVersion: 1;
  readonly deliveryId: string;
  readonly tenant: DestinationTenant;
  readonly sessionId: string;
  readonly mediaType: string;
  /** Already scrubbed bytes; this API neither authenticates nor scrubs input. */
  readonly payload: Uint8Array;
}

export type DestinationAcceptance = "stored" | "indexed";

export interface DestinationReceipt {
  readonly destinationId: string;
  readonly deliveryId: string;
  /** SHA-256 of the delivered payload bytes, lowercase hexadecimal. */
  readonly contentSha256: string;
  /** Storage acceptance does not establish search/index visibility. */
  readonly acceptance: DestinationAcceptance;
  readonly duplicate: boolean;
}

export interface DestinationWriteContext {
  readonly signal?: AbortSignal;
}

/**
 * Trusted operator-supplied code, not a sandbox. Implementations must bind the
 * delivery ID to tenant + metadata + bytes, reject conflicting reuse, and
 * resolve only after their stated acceptance level has been reached.
 * Retry queues, checkpoints, credentials and routing policy belong to the host.
 */
export interface DestinationAdapter {
  readonly id: string;
  readonly kind: string;
  readonly acceptance: DestinationAcceptance;
  write(
    delivery: DestinationDelivery,
    context?: DestinationWriteContext,
  ): Promise<DestinationReceipt>;
}

export type DestinationErrorCode =
  | "invalid_argument"
  | "conflict"
  | "aborted"
  | "storage_unavailable";

/** Messages never contain payloads, paths, credentials or backend error text. */
export class DestinationError extends Error {
  constructor(
    readonly code: DestinationErrorCode,
    readonly retryable = false,
  ) {
    super(`destination: ${code}`);
    this.name = "DestinationError";
  }
}

export function assertDestinationLabel(value: unknown): asserts value is string {
  if (typeof value !== "string" || !/^[a-z][a-z0-9_-]{0,63}$/.test(value)) {
    throw new DestinationError("invalid_argument");
  }
}
