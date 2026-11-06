/** A deliberately small, single-process filesystem destination for self-host. */
import { createHash } from "node:crypto";
import { lstat, mkdir } from "node:fs/promises";
import { isAbsolute } from "node:path";
// As with lib/pipeline/object-layout.ts, this repository typechecks/tests the
// pipeline before building object-store/dist. Reuse its source implementation.
import { createFsStore } from "../../../object-store/src/fs.js";
import { objectKey, ObjectStoreError } from "../../../object-store/src/index.js";
import {
  assertDestinationLabel,
  DestinationError,
  type DestinationAdapter,
  type DestinationDelivery,
  type DestinationWriteContext,
} from "./contracts.js";

const DEFAULT_MAX_PAYLOAD_BYTES = 1024 * 1024;
const MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;
const MAX_LABEL_LENGTH = 200;

export interface LocalDestinationOptions {
  readonly id: string;
  /** Operator-controlled, private, absolute directory; never a client field. */
  readonly root: string;
  readonly maxPayloadBytes?: number;
}

function validText(value: unknown, max = MAX_LABEL_LENGTH): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= max &&
    value.trim() === value && !/[\u0000-\u001f\u007f]/.test(value);
}

function encodeDelivery(delivery: DestinationDelivery, maxBytes: number) {
  if (
    delivery.schemaVersion !== 1 ||
    !validText(delivery.deliveryId) ||
    !validText(delivery.tenant?.orgId) ||
    !validText(delivery.tenant?.workspaceId) ||
    !validText(delivery.sessionId) ||
    !validText(delivery.mediaType) ||
    !(delivery.payload instanceof Uint8Array) ||
    delivery.payload.byteLength > maxBytes
  ) {
    throw new DestinationError("invalid_argument");
  }
  // Copy/encode before the first await: callers cannot change bytes or tenant
  // metadata while a filesystem operation is in progress.
  const payload = Buffer.from(delivery.payload);
  const contentSha256 = createHash("sha256").update(payload).digest("hex");
  const identity = JSON.stringify([
    delivery.tenant.orgId, delivery.tenant.workspaceId, delivery.deliveryId,
  ]);
  const key = objectKey(`destinations/v1/${createHash("sha256").update(identity).digest("hex")}.json`);
  const body = Buffer.from(JSON.stringify({
    schemaVersion: 1,
    deliveryId: delivery.deliveryId,
    tenant: { orgId: delivery.tenant.orgId, workspaceId: delivery.tenant.workspaceId },
    sessionId: delivery.sessionId,
    mediaType: delivery.mediaType,
    contentSha256,
    payloadBase64: payload.toString("base64"),
  }));
  return { key, body, contentSha256, deliveryId: delivery.deliveryId };
}

/**
 * Construction is inert. First write creates the directory via the existing
 * object-store fs adapter. Immutable creates are replayable, never overwrites.
 * This stores receipts/bytes only: no query index, background job or retention.
 */
export function createLocalDestination(options: LocalDestinationOptions): DestinationAdapter {
  assertDestinationLabel(options.id);
  if (typeof options.root !== "string" || !isAbsolute(options.root)) {
    throw new DestinationError("invalid_argument");
  }
  const maxBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_PAYLOAD_BYTES) {
    throw new DestinationError("invalid_argument");
  }
  const { id, root } = options;
  let store: ReturnType<typeof createFsStore> | undefined;
  return Object.freeze({
    id,
    kind: "local-filesystem",
    acceptance: "stored",
    async write(delivery: DestinationDelivery, context?: DestinationWriteContext) {
      if (context?.signal?.aborted) throw new DestinationError("aborted");
      const encoded = encodeDelivery(delivery, maxBytes);
      try {
        if (!store) {
          // Preserve pre-existing permissions; refuse a shared/symlink root
          // rather than storing session content in a permissive directory.
          await mkdir(root, { recursive: true, mode: 0o700 });
          const directory = await lstat(root);
          if (!directory.isDirectory() || (directory.mode & 0o077) !== 0) {
            throw new DestinationError("storage_unavailable");
          }
          store = createFsStore({ root });
        }
        const ack = await store.put(encoded.key, encoded.body, { ifNoneMatch: true });
        // A cancellation after PUT begins cannot undo storage. Return its real
        // outcome; a lost response may be safely retried with the same identity.
        return Object.freeze({
          destinationId: id,
          deliveryId: encoded.deliveryId,
          contentSha256: encoded.contentSha256,
          acceptance: "stored" as const,
          duplicate: !ack.created,
        });
      } catch (error) {
        if (error instanceof DestinationError) throw error;
        if (error instanceof ObjectStoreError && error.code === "already_exists") {
          throw new DestinationError("conflict");
        }
        throw new DestinationError(
          "storage_unavailable",
          error instanceof ObjectStoreError && error.retryable,
        );
      }
    },
  });
}
