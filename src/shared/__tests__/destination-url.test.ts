import { describe, expect, test } from "bun:test";
import {
  assertDestinationUrl,
  destinationPolicy,
  inClusterHttpAllowed,
  isAllowedDestination,
  isKubernetesServiceHost,
} from "../destination-url.ts";

const off = { allowInClusterHttp: false };
const on = { allowInClusterHttp: true };

describe("ALLOW_IN_CLUSTER_HTTP flag", () => {
  test("off unless exactly true", () => {
    expect(inClusterHttpAllowed({})).toBe(false);
    expect(inClusterHttpAllowed({ ALLOW_IN_CLUSTER_HTTP: "" })).toBe(false);
    expect(inClusterHttpAllowed({ ALLOW_IN_CLUSTER_HTTP: "false" })).toBe(false);
    expect(inClusterHttpAllowed({ ALLOW_IN_CLUSTER_HTTP: "true" })).toBe(true);
    expect(inClusterHttpAllowed({ ALLOW_IN_CLUSTER_HTTP: " TRUE " })).toBe(true);
    expect(destinationPolicy({ ALLOW_IN_CLUSTER_HTTP: "true" })).toEqual(on);
  });

  test("a typo is a startup error, not a silent default", () => {
    for (const v of ["1", "yes", "on", "ture"])
      expect(() => inClusterHttpAllowed({ ALLOW_IN_CLUSTER_HTTP: v })).toThrow(
        /ALLOW_IN_CLUSTER_HTTP must be true or false/,
      );
  });
});

describe("Kubernetes Service host names", () => {
  test("only <svc>.<ns>.svc and <svc>.<ns>.svc.cluster.local", () => {
    expect(isKubernetesServiceHost("relayauth.relayauth.svc")).toBe(true);
    expect(isKubernetesServiceHost("x.ns.svc.cluster.local")).toBe(true);
    for (const host of [
      "svc",
      "ns.svc",
      "a.b.c.svc",
      "pod.x.ns.svc.cluster.local",
      "x.ns.svc.cluster.example",
      "evil.svc.example.com",
      "relayauth.example.com",
      "10.0.0.1",
      "[::1]",
      "-bad.ns.svc",
      "x..svc",
      "relayauth.relayauth.svc.",
      "",
    ])
      expect(isKubernetesServiceHost(host)).toBe(false);
  });
});

describe("assertDestinationUrl", () => {
  test("https is always allowed; loopback http is always allowed", () => {
    for (const policy of [off, on]) {
      expect(isAllowedDestination("https://relayauth.example.com", policy)).toBe(true);
      expect(isAllowedDestination("https://agentrelay.com/cloud", policy)).toBe(true);
      expect(isAllowedDestination("http://127.0.0.1:8787", policy)).toBe(true);
      expect(isAllowedDestination("http://localhost:8080", policy)).toBe(true);
      expect(isAllowedDestination("http://[::1]:8080", policy)).toBe(true);
    }
  });

  test("flag off: in-cluster plain http is refused with a hint", () => {
    expect(() =>
      assertDestinationUrl("RELAYAUTH_URL", "http://relayauth.relayauth.svc:8787", off),
    ).toThrow(/RELAYAUTH_URL must use https \(plain http is loopback-only; set ALLOW_IN_CLUSTER_HTTP=true/);
    expect(isAllowedDestination("http://x.ns.svc.cluster.local", off)).toBe(false);
  });

  test("flag on: Kubernetes Service URLs are allowed", () => {
    expect(isAllowedDestination("http://relayauth.relayauth.svc:8787", on)).toBe(true);
    expect(isAllowedDestination("http://x.ns.svc.cluster.local", on)).toBe(true);
    expect(isAllowedDestination("http://RelayAuth.RelayAuth.SVC:8787/", on)).toBe(true);
    expect(
      isAllowedDestination("http://relayauth.relayauth.svc:8787/.well-known/jwks.json", on),
    ).toBe(true);
  });

  test("flag on: everything else still needs https", () => {
    for (const url of [
      "http://relayauth.example.com",
      "http://10.0.0.1",
      "http://10.0.0.1:8787",
      "http://[fd00::1]:8787",
      "http://evil.svc.example.com",
      "http://svc",
      "http://relayauth",
      "http://relayauth.relayauth",
      "http://a.b.c.svc",
      "http://relayauth.relayauth.svc.",
      "http://localhost.example.com",
      "http://127.0.0.1.nip.io",
      "http://x.ns.svc.cluster.local.evil.com",
    ])
      expect(() => assertDestinationUrl("RELAYAUTH_URL", url, on)).toThrow(
        /RELAYAUTH_URL must use https/,
      );
  });

  test("credentials, query and fragment are refused whatever the host", () => {
    for (const policy of [off, on])
      for (const url of [
        "http://user:pass@relayauth.relayauth.svc",
        "http://user@relayauth.relayauth.svc",
        "https://user:pass@relayauth.example.com",
        "http://relayauth.relayauth.svc/?x=1",
        "https://relayauth.example.com/#frag",
        "http://user:pass@127.0.0.1:8787",
      ])
        expect(() => assertDestinationUrl("RELAYAUTH_URL", url, policy)).toThrow(
          /must not contain credentials, query or fragment/,
        );
  });

  test("other schemes and junk are refused; the value is never echoed", () => {
    for (const url of ["ftp://relayauth.relayauth.svc", "file:///etc/passwd", "relayauth.svc", "not a url secret-ish"])
      expect(() => assertDestinationUrl("CLOUD_URL", url, on)).toThrow(
        /CLOUD_URL must be an absolute http\(s\) URL/,
      );
    let message = "";
    try {
      assertDestinationUrl("CLOUD_URL", "http://user:hunter2@host.example", off);
    } catch (error) {
      message = String(error);
    }
    expect(message).toContain("CLOUD_URL");
    expect(message).not.toContain("hunter2");
  });

  test("allowPath:false demands a bare origin", () => {
    expect(isAllowedDestination("https://history.example", off, { allowPath: false })).toBe(true);
    expect(() =>
      assertDestinationUrl("RELAYHISTORY_ORIGIN", "https://history.example/v1", off, { allowPath: false }),
    ).toThrow(/bare origin/);
  });

  test("allowAnyHttpHost is a shape check only", () => {
    expect(isAllowedDestination("http://96bulls-ingest:8080", off, { allowAnyHttpHost: true })).toBe(true);
    expect(isAllowedDestination("http://u:p@96bulls-ingest:8080", off, { allowAnyHttpHost: true })).toBe(false);
  });
});
