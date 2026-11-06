/**
 * Runs the shared conformance suite against every adapter. The fs store
 * always runs; the cloud adapters run against local emulators when their
 * variables are set (`eval "$(scripts/emulators.sh up)"`), the same variables
 * the Rust crate's suite uses. `RH_OBJSTORE_REQUIRE_EMULATORS=1` turns a
 * missing one into a failure.
 */
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { runSuite, type SuiteOptions } from "../src/conformance.js";
import { fromBase64, hmacSha256, toBase64 } from "../src/crypto.js";
import { createFsStore } from "../src/fs.js";
import { signRequest } from "../src/sigv4.js";
import {
  createAzureBlobStore,
  createGcsStore,
  createS3Store,
  objectKey,
  type ObjectStore,
} from "./helpers.js";

const PAGE_SIZE = 3;
const BUCKET = "rh-conformance";
// Azurite's published, well-known development account (not a secret).
const AZURITE_ACCOUNT = "devstoreaccount1";
const AZURITE_KEY =
  "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

const env = (name: string) => process.env[name] || undefined;
const required = env("RH_OBJSTORE_REQUIRE_EMULATORS") !== undefined;

function emulator(name: string): string | undefined {
  const value = env(name);
  if (!value && required)
    throw new Error(
      `${name} is required when RH_OBJSTORE_REQUIRE_EMULATORS is set`,
    );
  return value;
}

const runPrefix = (backend: string) =>
  objectKey(
    `conformance-ts/${backend}/${Date.now()}${Math.random().toString(36).slice(2, 8)}`,
  );

async function httpFetch(url: URL): Promise<Uint8Array> {
  const res = await fetch(url);
  const body = new Uint8Array(await res.arrayBuffer());
  if (!res.ok)
    throw new Error(
      `HTTP ${res.status}: ${new TextDecoder().decode(body).slice(0, 300)}`,
    );
  return body;
}

async function assertConforms(
  backend: string,
  store: ObjectStore,
  fetchPresigned: SuiteOptions["fetchPresigned"],
  skip: SuiteOptions["skip"] = {},
) {
  const outcomes = await runSuite(store, {
    prefix: runPrefix(backend),
    pageSize: PAGE_SIZE,
    fetchPresigned,
    skip,
  });
  for (const o of outcomes) {
    const detail =
      o.status === "skipped"
        ? ` (${o.reason})`
        : o.status === "failed"
          ? `: ${o.error}`
          : "";
    console.log(`[${backend}] ${o.status.padEnd(7)} ${o.name}${detail}`);
  }
  expect(outcomes.filter((o) => o.status === "failed")).toEqual([]);
}

describe("conformance", () => {
  const dirs: string[] = [];
  afterAll(async () => {
    await Promise.all(dirs.map((d) => rm(d, { recursive: true, force: true })));
  });

  it("fs", async () => {
    const root = await mkdtemp(join(tmpdir(), "rh-objstore-"));
    dirs.push(root);
    const store = createFsStore({ root, pageSize: PAGE_SIZE });
    await assertConforms(
      "fs",
      store,
      async (url) => new Uint8Array(await readFile(fileURLToPath(url))),
    );
  });

  const s3Endpoint = emulator("RH_OBJSTORE_S3_ENDPOINT");
  it.skipIf(!s3Endpoint)("s3 (MinIO)", async () => {
    const credentials = {
      accessKeyId: env("RH_OBJSTORE_S3_ACCESS_KEY_ID")!,
      secretAccessKey: env("RH_OBJSTORE_S3_SECRET_ACCESS_KEY")!,
    };
    // CreateBucket: PUT /{bucket}/.
    const url = new URL(`${s3Endpoint}/${BUCKET}/`);
    const headers = new Headers();
    await signRequest({
      method: "PUT",
      url,
      headers,
      payloadSha256:
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      region: "us-east-1",
      credentials,
    });
    const created = await fetch(url, { method: "PUT", headers });
    expect([200, 409]).toContain(created.status);
    const store = createS3Store({
      bucket: BUCKET,
      region: "us-east-1",
      endpoint: s3Endpoint!,
      credentials,
      pageSize: PAGE_SIZE,
    });
    await assertConforms("s3", store, httpFetch);
  });

  const gcsBase = emulator("RH_OBJSTORE_GCS_BASE_URL");
  it.skipIf(!gcsBase)("gcs (fake-gcs-server)", async () => {
    const created = await fetch(`${gcsBase}/storage/v1/b?project=test`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: BUCKET }),
    });
    expect([200, 409]).toContain(created.status);
    const keyFile = env("RH_OBJSTORE_GCS_SERVICE_ACCOUNT_KEY_FILE");
    const sa = keyFile
      ? (JSON.parse(await readFile(keyFile, "utf8")) as {
          client_email: string;
          private_key: string;
        })
      : undefined;
    const noPreconditions =
      "fake-gcs-server ignores x-goog-if-generation-match (same gap the Rust suite and object_store skip)";
    const store = createGcsStore({
      bucket: BUCKET,
      baseUrl: gcsBase!,
      token: { accessToken: "fake-gcs-server-ignores-auth" },
      ...(sa
        ? {
            signingKey: {
              clientEmail: sa.client_email,
              privateKey: sa.private_key,
            },
          }
        : {}),
      pageSize: PAGE_SIZE,
    });
    await assertConforms("gcs", store, httpFetch, {
      create_is_idempotent_for_same_content: noPreconditions,
      create_conflicts_on_different_content: noPreconditions,
      if_match_compare_and_swap: noPreconditions,
      if_match_concurrent_single_winner: noPreconditions,
      ...(sa
        ? {}
        : {
            presign_get:
              "no RH_OBJSTORE_GCS_SERVICE_ACCOUNT_KEY_FILE to sign with",
          }),
    });
  });

  const azureEndpoint = emulator("RH_OBJSTORE_AZURITE_ENDPOINT");
  it.skipIf(!azureEndpoint)("azure (Azurite)", async () => {
    await createAzureContainer(azureEndpoint!);
    const store = createAzureBlobStore({
      account: AZURITE_ACCOUNT,
      container: BUCKET,
      endpoint: azureEndpoint!,
      auth: { kind: "sharedKey", accountKey: AZURITE_KEY },
      pageSize: PAGE_SIZE,
    });
    await assertConforms("azure", store, httpFetch);
  });
});

/** Create Container with Shared Key; provisioning is not part of the adapters. */
async function createAzureContainer(endpoint: string): Promise<void> {
  const url = new URL(
    `${endpoint.replace(/\/+$/, "")}/${BUCKET}?restype=container`,
  );
  const date = new Date().toUTCString();
  const version = "2021-12-02";
  const stringToSign = `PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:${date}\nx-ms-version:${version}\n/${AZURITE_ACCOUNT}${url.pathname}\nrestype:container`;
  const sig = toBase64(await hmacSha256(fromBase64(AZURITE_KEY), stringToSign));
  const res = await fetch(url, {
    method: "PUT",
    headers: {
      "x-ms-date": date,
      "x-ms-version": version,
      authorization: `SharedKey ${AZURITE_ACCOUNT}:${sig}`,
    },
  });
  expect([201, 409], await res.text()).toContain(res.status);
}
