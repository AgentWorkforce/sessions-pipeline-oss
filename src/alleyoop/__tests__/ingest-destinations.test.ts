import { afterEach, describe, expect, test } from "bun:test";
import { assertIngestDestinations, buildIngestApp } from "../ingest.ts";

const savedEnv = { ...process.env };
afterEach(() => {
  for (const key of Object.keys(process.env))
    if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
});

describe("ingest destinations (ALLOW_IN_CLUSTER_HTTP)", () => {
  test("unset values keep the local defaults", () => {
    expect(() => assertIngestDestinations({})).not.toThrow();
  });

  test("hosted https and loopback values pass with the flag off", () => {
    expect(() =>
      assertIngestDestinations({
        RELAYAUTH_JWKS_URL: "https://relayauth.example.com/.well-known/jwks.json",
        RELAYHISTORY_ORIGIN: "https://history.example.com",
      }),
    ).not.toThrow();
    expect(() =>
      assertIngestDestinations({
        RELAYAUTH_JWKS_URL: "http://127.0.0.1:8080/.well-known/jwks.json",
      }),
    ).not.toThrow();
  });

  test("an in-cluster JWKS needs the flag", () => {
    const env = {
      RELAYAUTH_JWKS_URL: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
      RELAYHISTORY_ORIGIN: "http://alleyoop-ingest.sessions.svc.cluster.local:8080",
    };
    expect(() => assertIngestDestinations(env)).toThrow(
      /RELAYAUTH_JWKS_URL must use https/,
    );
    expect(() =>
      assertIngestDestinations({ ...env, ALLOW_IN_CLUSTER_HTTP: "true" }),
    ).not.toThrow();
  });

  test("the flag never admits a public plain-http JWKS (key substitution)", () => {
    for (const url of [
      "http://relayauth.example.com/.well-known/jwks.json",
      "http://10.0.0.1/.well-known/jwks.json",
      "http://evil.svc.example.com/.well-known/jwks.json",
    ])
      expect(() =>
        assertIngestDestinations({
          RELAYAUTH_JWKS_URL: url,
          ALLOW_IN_CLUSTER_HTTP: "true",
        }),
      ).toThrow(/RELAYAUTH_JWKS_URL must use https/);
  });

  test("RELAYHISTORY_ORIGIN stays an exact bare origin and is transport-checked", () => {
    const on = { ALLOW_IN_CLUSTER_HTTP: "true" };
    expect(() =>
      assertIngestDestinations({ ...on, RELAYHISTORY_ORIGIN: "http://history.example.com" }),
    ).toThrow(/RELAYHISTORY_ORIGIN must use https/);
    expect(() =>
      assertIngestDestinations({ ...on, RELAYHISTORY_ORIGIN: "https://history.example.com/v1" }),
    ).toThrow(/bare origin/);
    expect(() =>
      assertIngestDestinations({ ...on, RELAYHISTORY_ORIGIN: "https://u:p@history.example.com" }),
    ).toThrow(/must not contain credentials/);
  });

  test("buildIngestApp refuses a bad destination before touching the database", async () => {
    Object.assign(process.env, {
      LOCKER_BUCKET: "raw",
      TENANT_ORG_ID: "org",
      RELAYAUTH_JWKS_URL: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
    });
    delete process.env.ALLOW_IN_CLUSTER_HTTP;
    delete process.env.HOOPDB_URL;
    await expect(buildIngestApp()).rejects.toThrow(
      /RELAYAUTH_JWKS_URL must use https/,
    );
  });
});
