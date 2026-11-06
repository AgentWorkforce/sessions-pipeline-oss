import { describe, expect, test } from "bun:test";
import { readApiAuthConfig } from "../lambda.ts";

describe("read API auth destinations", () => {
  test("unset JWKS keeps the vendored default; https passes", () => {
    expect(readApiAuthConfig({ READ_API_ORIGIN: "https://read.example.com" })).toEqual({
      origin: "https://read.example.com",
      jwksUrl: undefined,
      issuer: undefined,
    });
    expect(
      readApiAuthConfig({ RELAYAUTH_JWKS_URL: "https://relayauth.example.com/.well-known/jwks.json" }).jwksUrl,
    ).toBe("https://relayauth.example.com/.well-known/jwks.json");
  });

  test("plain-http JWKS needs loopback or the in-cluster flag with a Service name", () => {
    const jwks = "http://relayauth.relayauth.svc:8787/.well-known/jwks.json";
    expect(() => readApiAuthConfig({ RELAYAUTH_JWKS_URL: jwks })).toThrow(/RELAYAUTH_JWKS_URL must use https/);
    expect(readApiAuthConfig({ RELAYAUTH_JWKS_URL: jwks, ALLOW_IN_CLUSTER_HTTP: "true" }).jwksUrl).toBe(jwks);
    expect(() =>
      readApiAuthConfig({ RELAYAUTH_JWKS_URL: "http://relayauth.example.com/jwks", ALLOW_IN_CLUSTER_HTTP: "true" }),
    ).toThrow(/RELAYAUTH_JWKS_URL must use https/);
  });
});
