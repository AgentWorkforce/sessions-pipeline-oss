/**
 * Local-directory store for Node (tests, air-gapped hosts). On-disk layout is
 * the same as the Rust `FsStore`: objects at `{root}/{key}`, metadata sidecars
 * at `{root}/.rh/meta/{key}.json`, ETags in `object_store`'s
 * `"{inode:x}-{mtime_us:x}-{size:x}"` form, so either implementation can read
 * (and compare-and-swap) a directory the other wrote.
 *
 * Import from `@relayhistory/object-store/fs`; it needs `node:fs` and is not
 * available in Workers.
 */
import { randomUUID } from "node:crypto";
import { constants as fsConstants, mkdirSync, realpathSync } from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pathToFileURL } from "node:url";
import { BaseStore, type PutMode, type Transport } from "./core.js";
import { ObjectStoreError } from "./errors.js";
import { isObjectKey, type ObjectKey } from "./key.js";
import type {
  ByteRange,
  ByteStream,
  GetObject,
  Metadata,
  ObjectMeta,
  ObjectVersion,
} from "./types.js";

export interface FsConfig {
  root: string;
  pageSize?: number;
}

const BACKEND = "fs";
const RESERVED = ".rh";

/** @internal Exported for tests; use {@link createFsStore}. */
/**
 * Write locks shared by every store instance in this process that points at
 * the same (real) root directory, so two `createFsStore({ root })` calls
 * cannot both win a compare-and-swap.
 */
const LOCKS = new Map<string, Promise<void>>();

export class FsTransport implements Transport {
  readonly backend = BACKEND;
  readonly #root: string;

  constructor(root: string) {
    mkdirSync(root, { recursive: true });
    this.#root = realpathSync(path.resolve(root));
  }

  /**
   * Absolute path for `key` under the root, refusing to traverse a symbolic
   * link: a link inside the store could otherwise point a valid key at any
   * file on the host.
   *
   * Threat model: this rejects links that exist when the operation starts
   * (and reads open the final component with `O_NOFOLLOW`). A concurrent
   * writer inside the root could still swap an intermediate directory for a
   * link between this check and the open; Node has no `openat`, so the root
   * must not be shared with untrusted writers.
   */
  async #within(...segments: string[]): Promise<string> {
    let current = this.#root;
    for (const segment of segments) {
      current = path.join(current, segment);
      try {
        if ((await fsp.lstat(current)).isSymbolicLink()) {
          throw new ObjectStoreError(
            "invalid_key",
            BACKEND,
            `${path.relative(this.#root, current)} is a symbolic link; the store does not follow links`,
          );
        }
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") break;
        throw err;
      }
    }
    return path.join(this.#root, ...segments);
  }

  #path(key: string): Promise<string> {
    return this.#within(...key.split("/"));
  }

  #sidecar(key: string): Promise<string> {
    return this.#within(RESERVED, "meta", ...`${key}.json`.split("/"));
  }

  async #stat(key: string) {
    try {
      const st = await fsp.stat(await this.#path(key), { bigint: true });
      return st.isFile() ? st : undefined;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw err;
    }
  }

  async #writeAtomic(
    target: string,
    body: Uint8Array,
    exclusive: boolean,
  ): Promise<void> {
    await fsp.mkdir(path.dirname(target), { recursive: true });
    const staging = `${target}#${randomUUID()}`;
    const handle = await fsp.open(staging, "wx");
    try {
      await handle.writeFile(body);
      await handle.sync();
    } catch (err) {
      await handle.close().catch(() => {});
      await fsp.rm(staging, { force: true });
      throw err;
    }
    await handle.close();
    try {
      if (exclusive) {
        await fsp.link(staging, target);
        await fsp.unlink(staging);
      } else {
        await fsp.rename(staging, target);
      }
    } catch (err) {
      await fsp.rm(staging, { force: true });
      if ((err as NodeJS.ErrnoException).code === "EEXIST") {
        throw new ObjectStoreError(
          "already_exists",
          BACKEND,
          `${target} exists`,
        );
      }
      throw err;
    }
  }

  /**
   * Serializes every write to this root within the process. Cross-process
   * exclusion (the Rust store's `flock`) is not available from Node without a
   * native module, so mixing a Node writer with another writer on one
   * directory is unsupported; reads are always safe.
   */
  async #exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const previous = LOCKS.get(this.#root) ?? Promise.resolve();
    let release!: () => void;
    const mine = new Promise<void>((r) => (release = r));
    LOCKS.set(this.#root, mine);
    await previous;
    try {
      return await fn();
    } finally {
      release();
      if (LOCKS.get(this.#root) === mine) LOCKS.delete(this.#root);
    }
  }

  async put(
    key: ObjectKey,
    body: Uint8Array,
    mode: PutMode,
    metadata: Metadata,
    contentType: string,
  ): Promise<ObjectVersion> {
    // Every write, including a create, is serialized: a create's stat and
    // sidecar write must not interleave with an overwrite of the same key,
    // or the sidecar could bind the create's digest to the other body.
    const write = async (): Promise<ObjectVersion> => {
      if (mode.kind === "update") {
        const current = await this.#stat(key);
        if (!current || eTag(current) !== mode.version.eTag) {
          throw new ObjectStoreError(
            "precondition_failed",
            BACKEND,
            `${key}: version changed`,
          );
        }
      }
      await this.#writeAtomic(
        await this.#path(key),
        body,
        mode.kind === "create",
      );
      const st = await this.#stat(key);
      const version = st ? { eTag: eTag(st) } : {};
      await this.#writeSidecar(key, version, metadata, contentType);
      return version;
    };
    return this.#exclusive(write);
  }

  /**
   * The sidecar records the ETag of the object version it describes (same
   * format as the Rust `FsStore`); a sidecar for another version is stale and
   * ignored, so a crash between the two writes can never pair a body with
   * another body's digest.
   */
  async #writeSidecar(
    key: string,
    version: ObjectVersion,
    metadata: Metadata,
    contentType: string,
  ): Promise<void> {
    const sidecar = new TextEncoder().encode(
      JSON.stringify({
        content_type: contentType,
        metadata,
        e_tag: version.eTag ?? null,
      }),
    );
    await this.#writeAtomic(await this.#sidecar(key), sidecar, false);
  }

  async #readMetadata(key: string, currentETag: string): Promise<Metadata> {
    try {
      const sidecar = JSON.parse(
        await fsp.readFile(await this.#sidecar(key), "utf8"),
      ) as {
        metadata?: Metadata;
        e_tag?: string | null;
      };
      return sidecar.e_tag === currentETag ? (sidecar.metadata ?? {}) : {};
    } catch {
      // Missing or torn sidecar: no metadata.
      return {};
    }
  }

  /**
   * Rewrite a lost sidecar, under the write lock and only if the object is
   * still the version that was verified, so a repair can never stamp an old
   * ETag over a newer write's sidecar.
   */
  async repairMetadata(
    key: ObjectKey,
    version: ObjectVersion,
    metadata: Metadata,
    contentType: string,
  ): Promise<void> {
    await this.#exclusive(async () => {
      const st = await this.#stat(key);
      if (!st || eTag(st) !== version.eTag) return;
      await this.#writeSidecar(key, version, metadata, contentType);
    });
  }

  async delete(key: ObjectKey): Promise<void> {
    await this.#exclusive(async () => {
      await fsp.rm(await this.#path(key), { force: true });
      await fsp.rm(await this.#sidecar(key), { force: true });
    });
  }

  async head(key: ObjectKey): Promise<ObjectMeta | undefined> {
    const st = await this.#stat(key);
    if (!st) return undefined;
    return meta(key, st, await this.#readMetadata(key, eTag(st)));
  }

  async getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject> {
    // Open first, then stat the open handle: the body and the version
    // describe the same file even if it is replaced concurrently.
    let handle;
    try {
      // O_NOFOLLOW refuses a link swapped into the final component after
      // the `#within` check (POSIX; ignored where unsupported).
      handle = await fsp.open(
        await this.#path(key),
        fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0),
      );
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        throw new ObjectStoreError("not_found", BACKEND, `${key} not found`, {
          status: 404,
        });
      }
      throw err;
    }
    const st = await handle.stat({ bigint: true });
    if (range && range.start >= Number(st.size)) {
      await handle.close();
      throw new ObjectStoreError(
        "invalid_argument",
        BACKEND,
        `${key}: range past end of object`,
      );
    }
    const stream = handle.createReadStream(
      range ? { start: range.start, end: range.end - 1 } : {},
    );
    return {
      meta: meta(key, st, await this.#readMetadata(key, eTag(st))),
      body: Readable.toWeb(stream) as unknown as ByteStream,
    };
  }

  /**
   * Walks the subtree in key order and stops after `pageSize + 1` keys, so
   * each page costs O(page + depth) directory reads, not a full walk.
   * Directories sort as `name/` so the walk order equals lexicographic key
   * order (`a-c` < `a/b`); subtrees entirely at or before the cursor are
   * skipped without being read.
   */
  async list(prefix: string, cursor: string | undefined, pageSize: number) {
    const base = prefix.replace(/\/$/, "");
    const dir = await this.#path(base);
    const keys: string[] = [];
    const want = pageSize + 1;
    const walk = async (abs: string, rel: string): Promise<void> => {
      let entries;
      try {
        entries = await fsp.readdir(abs, { withFileTypes: true });
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
        throw fsError(err, rel);
      }
      const ordered = entries
        .filter((e) => e.isDirectory() || (e.isFile() && !e.name.includes("#")))
        .map((e) => ({
          e,
          sortKey: e.isDirectory() ? `${rel}/${e.name}/` : `${rel}/${e.name}`,
        }))
        .sort((a, b) =>
          a.sortKey < b.sortKey ? -1 : a.sortKey > b.sortKey ? 1 : 0,
        );
      for (const { e, sortKey } of ordered) {
        if (keys.length >= want) return;
        if (e.isDirectory()) {
          // Every key under `sortKey` is <= cursor iff the prefix itself is
          // before the cursor and the cursor does not lie inside it.
          if (
            cursor !== undefined &&
            sortKey < cursor &&
            !cursor.startsWith(sortKey)
          )
            continue;
          await walk(path.join(abs, e.name), sortKey.slice(0, -1));
        } else if (cursor === undefined || sortKey > cursor) {
          keys.push(sortKey);
        }
      }
    };
    await walk(dir, base);
    const page = keys.slice(0, pageSize);
    const items: ObjectMeta[] = [];
    for (const k of page.filter(isObjectKey)) {
      const st = await this.#stat(k);
      if (st) items.push(meta(k, st, {}));
    }
    const last = page.at(-1);
    return keys.length > pageSize && last
      ? { items, nextCursor: last }
      : { items };
  }

  /** A `file://` URL; the TTL cannot be enforced on a local path. */
  async presignGet(key: ObjectKey): Promise<URL> {
    return pathToFileURL(await this.#path(key));
  }
}

/** `object_store::local` ETag: `"{inode:x}-{mtime_micros:x}-{size:x}"`. */
function eTag(st: { ino: bigint; size: bigint; mtimeNs: bigint }): string {
  return `"${st.ino.toString(16)}-${(st.mtimeNs / 1000n).toString(16)}-${st.size.toString(16)}"`;
}

function meta(
  key: string,
  st: { ino: bigint; size: bigint; mtimeNs: bigint },
  metadata: Metadata,
): ObjectMeta {
  return {
    key: key as ObjectKey,
    size: Number(st.size),
    lastModified: new Date(Number(st.mtimeNs / 1_000_000n)),
    version: { eTag: eTag(st) },
    metadata,
  };
}

/**
 * Map a Node filesystem error to the contract's codes, so callers always get
 * an `ObjectStoreError` with a `code` and `retryable` (EACCES on an
 * unreadable file, ENOSPC on a full disk, ...).
 */
export function fsError(err: unknown, key: string): ObjectStoreError {
  if (err instanceof ObjectStoreError) return err;
  const code = (err as NodeJS.ErrnoException | undefined)?.code;
  const detail = `${key}: ${code ?? "error"}`;
  switch (code) {
    case "ENOENT":
      return new ObjectStoreError("not_found", BACKEND, detail, {
        status: 404,
        cause: err,
      });
    case "EACCES":
    case "EPERM":
    case "EROFS":
      return new ObjectStoreError("unauthorized", BACKEND, detail, {
        cause: err,
      });
    case "ENOSPC":
    case "EMFILE":
    case "ENFILE":
    case "EBUSY":
    case "EAGAIN":
    case "EINTR":
    case "EIO":
      return new ObjectStoreError("transient", BACKEND, detail, { cause: err });
    case "ELOOP":
      return new ObjectStoreError(
        "invalid_key",
        BACKEND,
        `${key}: is a symbolic link; the store does not follow links`,
        { cause: err },
      );
    default:
      return new ObjectStoreError("backend", BACKEND, detail, { cause: err });
  }
}

/** Every transport call surfaces disk errors as `ObjectStoreError`s. */
function withFsErrors(t: FsTransport): Transport {
  const wrap =
    <A extends unknown[], R>(fn: (...args: A) => Promise<R>) =>
    async (...args: A): Promise<R> => {
      try {
        return await fn(...args);
      } catch (err) {
        throw fsError(err, String(args[0] ?? ""));
      }
    };
  return {
    backend: t.backend,
    put: wrap(t.put.bind(t)),
    head: wrap(t.head.bind(t)),
    delete: wrap(t.delete.bind(t)),
    getObject: wrap(t.getObject.bind(t)),
    list: wrap(t.list.bind(t)),
    presignGet: wrap(t.presignGet.bind(t)),
    repairMetadata: wrap(t.repairMetadata.bind(t)),
  };
}

export function createFsStore(config: FsConfig): BaseStore {
  return new BaseStore(
    withFsErrors(new FsTransport(config.root)),
    config.pageSize,
  );
}
