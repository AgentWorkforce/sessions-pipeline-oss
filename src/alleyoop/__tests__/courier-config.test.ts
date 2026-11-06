import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { configFromEnv, runCourierOnce } from "../courier.ts";

const savedEnv = { ...process.env };
const tempDirs: string[] = [];

beforeEach(() => {
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, savedEnv, {
    CONCOURSE_DB: "/does/not/exist/history.db",
    TENANT_ORG_ID: "org-proof",
    WORKSPACE_ID: "workspace-proof",
    USER_ID: "user-proof",
    MACHINE_ID: "machine-current",
    INGEST_URL: "https://history.example.test",
    RELAYAUTH_URL: "https://auth.example.test",
    RELAYAUTH_HISTORY_ORIGIN: "https://history.example.test",
    RELAYAUTH_API_KEY_COMMAND_JSON: '["false"]',
    COURIER_START_MODE: "all",
  });
  delete process.env.CLOUD_URL;
  delete process.env.JWT_PRIVATE_KEY_PEM;
  delete process.env.ALLEYOOP_TOKEN;
  delete process.env.ALLOW_IN_CLUSTER_HTTP;
});

afterEach(async () => {
  for (const key of Object.keys(process.env))
    if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true })));
});

describe("RelayAuth courier machine binding", () => {
  test("a matching RelayAuth configuration is machine-bound", () => {
    expect(configFromEnv()).toMatchObject({
      machineId: "machine-current",
      machineBound: true,
    });
  });

  test("a persisted mismatch fails before mint or upload", async () => {
    const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-config-"));
    tempDirs.push(dir);
    const statePath = join(dir, "courier-state.json");
    await writeFile(
      statePath,
      JSON.stringify({ machine_id: "machine-old", sessions: {} }),
    );
    process.env.COURIER_STATE = statePath;

    const cfg = configFromEnv();
    let tokenReads = 0;
    cfg.token = {
      async get() {
        tokenReads += 1;
        throw new Error("token must not be read");
      },
      invalidate() {
        return true;
      },
    };

    await expect(runCourierOnce(cfg)).rejects.toMatchObject({
      code: "machine_id_mismatch",
      retryable: false,
    });
    expect(tokenReads).toBe(0);
  });
});

describe("courier destinations (ALLOW_IN_CLUSTER_HTTP)", () => {
  const inCluster = {
    INGEST_URL: "http://alleyoop-ingest.sessions.svc:8080",
    RELAYAUTH_URL: "http://relayauth.relayauth.svc:8787",
    RELAYAUTH_HISTORY_ORIGIN: "http://alleyoop-ingest.sessions.svc:8080",
  };

  test("flag off: an in-cluster plain-http RelayAuth URL is refused", () => {
    Object.assign(process.env, { RELAYAUTH_URL: inCluster.RELAYAUTH_URL });
    expect(() => configFromEnv()).toThrow(
      /RELAYAUTH_URL must use https \(plain http is loopback-only; set ALLOW_IN_CLUSTER_HTTP=true/,
    );
  });

  test("flag on: in-cluster RelayAuth, ingest and origin are accepted", () => {
    Object.assign(process.env, inCluster, { ALLOW_IN_CLUSTER_HTTP: "true" });
    expect(configFromEnv()).toMatchObject({
      ingestUrl: "http://alleyoop-ingest.sessions.svc:8080",
      machineBound: true,
    });
    process.env.RELAYAUTH_URL = "http://relayauth.relayauth.svc.cluster.local:8787";
    expect(() => configFromEnv()).not.toThrow();
  });

  test("flag on: public, IP-literal and credentialed http stay refused", () => {
    process.env.ALLOW_IN_CLUSTER_HTTP = "true";
    for (const url of [
      "http://relayauth.example.com",
      "http://10.0.0.1:8787",
      "http://evil.svc.example.com",
      "http://svc",
    ]) {
      process.env.RELAYAUTH_URL = url;
      expect(() => configFromEnv()).toThrow(/RELAYAUTH_URL must use https/);
    }
    process.env.RELAYAUTH_URL = "http://user:pass@relayauth.relayauth.svc";
    expect(() => configFromEnv()).toThrow(
      /RELAYAUTH_URL must not contain credentials/,
    );
  });

  test("a durable credential never goes to a plain-http public ingest", () => {
    process.env.INGEST_URL = "http://history.example.test";
    expect(() => configFromEnv()).toThrow(/INGEST_URL must use https/);
    process.env.ALLOW_IN_CLUSTER_HTTP = "true";
    expect(() => configFromEnv()).toThrow(/INGEST_URL must use https/);
  });

  test("the history origin is transport-checked and stays an exact bare origin", () => {
    Object.assign(process.env, inCluster, {
      RELAYAUTH_URL: "https://auth.example.test",
    });
    expect(() => configFromEnv()).toThrow(/must use https/);
    process.env.ALLOW_IN_CLUSTER_HTTP = "true";
    expect(() => configFromEnv()).not.toThrow();
    // The flag never relaxes the exact-origin audience rule.
    process.env.RELAYAUTH_HISTORY_ORIGIN = "http://alleyoop-ingest.sessions.svc:8080/";
    expect(() => configFromEnv()).toThrow(/RELAYAUTH_HISTORY_ORIGIN must be an exact bare origin/);
    process.env.RELAYAUTH_HISTORY_ORIGIN = "http://history.example.test";
    expect(() => configFromEnv()).toThrow(/RELAYAUTH_HISTORY_ORIGIN must use https/);
  });

  test("Cloud sessions use the same rule for CLOUD_URL", () => {
    Object.assign(process.env, {
      CLOUD_URL: "http://cloud.cloud.svc:3000",
      CLOUD_TOKEN_COMMAND_JSON: '["false"]',
    });
    expect(() => configFromEnv()).toThrow(/CLOUD_URL must use https/);
    process.env.ALLOW_IN_CLUSTER_HTTP = "true";
    expect(() => configFromEnv()).not.toThrow();
    process.env.CLOUD_URL = "http://cloud.example.com";
    expect(() => configFromEnv()).toThrow(/CLOUD_URL must use https/);
  });

  test("local-only modes keep Docker-network http ingest URLs", () => {
    delete process.env.RELAYAUTH_URL;
    Object.assign(process.env, {
      INGEST_URL: "http://96bulls-ingest:8080",
      COURIER_ALLOW_FIXED_TOKEN: "1",
      ALLEYOOP_TOKEN: "local-test-token",
    });
    expect(configFromEnv().ingestUrl).toBe("http://96bulls-ingest:8080");
    process.env.INGEST_URL = "http://u:p@96bulls-ingest:8080";
    expect(() => configFromEnv()).toThrow(/INGEST_URL must not contain credentials/);
  });

  test("an invalid flag value fails startup", () => {
    process.env.ALLOW_IN_CLUSTER_HTTP = "yes";
    expect(() => configFromEnv()).toThrow(/ALLOW_IN_CLUSTER_HTTP must be true or false/);
  });
});
