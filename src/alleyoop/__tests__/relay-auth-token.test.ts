import { describe, expect, test } from "bun:test";
import {
  CloudSessionAssertionProvider,
  RelayAuthAssertionProvider,
  secretCommand,
} from "../relay-auth-token.ts";

describe("RelayAuth assertion provider", () => {
  test("mints an exact-origin assertion and caches it only until the refresh window", async () => {
    let now = Date.parse("2026-10-29T12:00:00Z");
    const requests: Array<{ url: string; init: RequestInit }> = [];
    let keyReads = 0;
    const provider = new RelayAuthAssertionProvider({
      baseUrl: "https://relayauth.example",
      historyOrigin: "https://sessions.example.invalid",
      orgId: "org_kind",
      workspaceId: "ws_kind",
      machineId: "machine-kind",
      sponsorId: "user_kind",
      apiKey: async () => {
        keyReads += 1;
        return "rak_test_only_not_a_real_key";
      },
      now: () => now,
      fetcher: async (url, init) => {
        requests.push({ url: String(url), init: init! });
        const sequence = requests.length;
        return Response.json(
          {
            accessToken: `assertion-${sequence}`,
            accessTokenExpiresAt: new Date(now + 60_000).toISOString(),
            tokenType: "Bearer",
          },
          { status: 201 },
        );
      },
    });

    expect(await provider.get()).toBe("assertion-1");
    expect(await provider.get()).toBe("assertion-1");
    expect(keyReads).toBe(1);
    expect(requests).toHaveLength(1);
    expect(requests[0]!.url).toBe(
      "https://relayauth.example/v1/tokens/relayhistory-assertion",
    );
    expect(new Headers(requests[0]!.init.headers).get("x-api-key")).toBe(
      "rak_test_only_not_a_real_key",
    );
    expect(JSON.parse(String(requests[0]!.init.body))).toEqual({
      orgId: "org_kind",
      workspaceId: "ws_kind",
      machineId: "machine-kind",
      sponsorId: "user_kind",
      scopes: ["rth:sync", "rth:read"],
      expiresIn: 60,
      historyOrigin: "https://sessions.example.invalid",
    });

    now += 46_000;
    expect(await provider.get()).toBe("assertion-2");
    expect(keyReads).toBe(2);
  });

  test("coalesces concurrent mints and invalidates after a 401", async () => {
    let mints = 0;
    const provider = new RelayAuthAssertionProvider({
      baseUrl: "https://relayauth.example/",
      historyOrigin: "https://history.example",
      orgId: "org",
      workspaceId: "workspace",
      machineId: "machine-a",
      sponsorId: "user",
      apiKey: async () => "rak_test_only_not_a_real_key",
      fetcher: async () => {
        mints += 1;
        await Promise.resolve();
        return Response.json(
          {
            accessToken: `assertion-${mints}`,
            accessTokenExpiresAt: new Date(Date.now() + 60_000).toISOString(),
            tokenType: "Bearer",
          },
          { status: 201 },
        );
      },
    });

    expect(await Promise.all([provider.get(), provider.get()])).toEqual([
      "assertion-1",
      "assertion-1",
    ]);
    expect(mints).toBe(1);
    expect(provider.invalidate()).toBe(true);
    expect(await provider.get()).toBe("assertion-2");
  });

  test("does not expose a refusal body or the API key", async () => {
    const provider = new RelayAuthAssertionProvider({
      baseUrl: "https://relayauth.example",
      historyOrigin: "https://history.example",
      orgId: "org",
      workspaceId: "workspace",
      machineId: "machine-a",
      sponsorId: "user",
      apiKey: async () => "rak_super_secret",
      fetcher: async () =>
        new Response("server leaked rak_super_secret", { status: 403 }),
    });

    await expect(provider.get()).rejects.toMatchObject({
      code: "relayauth_http_403",
      retryable: false,
    });
    await expect(provider.get()).rejects.not.toThrow(
      /rak_super_secret|server leaked/,
    );
  });

  test("refuses to send the mint key over remote plain HTTP", () => {
    expect(
      () =>
        new RelayAuthAssertionProvider({
          baseUrl: "http://relayauth.example",
          historyOrigin: "https://history.example",
          orgId: "org",
          workspaceId: "workspace",
          machineId: "machine-a",
          sponsorId: "user",
          apiKey: async () => "rak_test_only_not_a_real_key",
        }),
    ).toThrow(/must use https/);
    expect(
      () =>
        new RelayAuthAssertionProvider({
          baseUrl: "http://127.0.0.1:8787",
          historyOrigin: "https://history.example",
          orgId: "org",
          workspaceId: "workspace",
          machineId: "machine-a",
          sponsorId: "user",
          apiKey: async () => "rak_test_only_not_a_real_key",
        }),
    ).not.toThrow();
  });

  test("refuses a non-canonical machine id before reading the mint key", async () => {
    let keyReads = 0;
    for (const machineId of [
      "",
      ".hidden",
      "has space",
      "relay_reserved",
      "é",
      "x".repeat(129),
    ]) {
      expect(
        () =>
          new RelayAuthAssertionProvider({
            baseUrl: "https://relayauth.example",
            historyOrigin: "https://history.example",
            orgId: "org",
            workspaceId: "workspace",
            machineId,
            sponsorId: "user",
            apiKey: async () => {
              keyReads += 1;
              return "rak_test_only_not_a_real_key";
            },
          }),
      ).toThrow(/MACHINE_ID/);
    }
    expect(keyReads).toBe(0);
  });
});

describe("RelayAuth API key command", () => {
  test("uses a JSON argv array without shell parsing", async () => {
    const read = secretCommand(
      JSON.stringify([
        process.execPath,
        "-e",
        "process.stdout.write('rak_from_command\\n')",
      ]),
    );
    expect(await read()).toBe("rak_from_command");
  });

  test("rejects shell strings and secret-shaped multiline output", async () => {
    expect(() => secretCommand("op read secret")).toThrow(/JSON argv array/);
    const read = secretCommand(
      JSON.stringify([
        process.execPath,
        "-e",
        "process.stdout.write('one two')",
      ]),
    );
    await expect(read()).rejects.toMatchObject({
      code: "relayauth_api_key_source_failed",
    });
  });
});

describe("CloudSessionAssertionProvider", () => {
  const session = (token: string, expires = "2030-01-01T00:00:00.000Z") =>
    Response.json({
      accessToken: token,
      accessTokenExpiresAt: expires,
      tokenType: "Bearer",
    });
  const provider = (fetcher: typeof fetch, machineId = "m-0123abcd") =>
    new CloudSessionAssertionProvider({
      baseUrl: "https://cloud.example/cloud/",
      workspaceId: "ws/1",
      machineId,
      cloudToken: async () => "cloud-secret",
      fetcher,
      now: () => Date.parse("2029-01-01T00:00:00.000Z"),
    });

  test("asks Cloud for a sync session bound to the machine, and caches it", async () => {
    const calls: { url: string; init: RequestInit }[] = [];
    const p = provider((async (url: string, init: RequestInit) => {
      calls.push({ url, init });
      return session(`token-${calls.length}`);
    }) as typeof fetch);
    expect(await p.get()).toBe("token-1");
    expect(await p.get()).toBe("token-1");
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe(
      "https://cloud.example/cloud/api/v1/workspaces/ws%2F1/relayhistory/session",
    );
    expect(JSON.parse(String(calls[0]!.init.body))).toEqual({
      mode: "sync",
      label: "courier",
      machine_id: "m-0123abcd",
    });
    expect(new Headers(calls[0]!.init.headers).get("authorization")).toBe(
      "Bearer cloud-secret",
    );
    expect(calls[0]!.init.redirect).toBe("error");
    expect(p.invalidate()).toBe(true);
    expect(await p.get()).toBe("token-2");
  });

  test("a machine the caller does not own is not retried; an outage is", async () => {
    const refused = provider((async () =>
      Response.json(
        { code: "machine_not_authorized" },
        { status: 403 },
      )) as unknown as typeof fetch);
    await expect(refused.get()).rejects.toMatchObject({
      code: "machine_not_authorized",
      retryable: false,
    });
    const down = provider((async () =>
      Response.json(
        { code: "machine_authority_unavailable" },
        { status: 503 },
      )) as unknown as typeof fetch);
    await expect(down.get()).rejects.toMatchObject({
      code: "machine_authority_unavailable",
      retryable: true,
    });
  });

  test("refuses an invalid session and unsafe configuration", async () => {
    await expect(
      provider((async () =>
        Response.json({ accessToken: "t" })) as unknown as typeof fetch).get(),
    ).rejects.toMatchObject({ code: "cloud_invalid_response" });
    expect(() => provider(fetch, "")).toThrow(/MACHINE_ID/);
    expect(
      () =>
        new CloudSessionAssertionProvider({
          baseUrl: "http://cloud.example",
          workspaceId: "w",
          machineId: "m",
          cloudToken: async () => "",
        }),
    ).toThrow(/https/);
    expect(
      () =>
        new CloudSessionAssertionProvider({
          baseUrl: "https://u:p@cloud.example",
          workspaceId: "w",
          machineId: "m",
          cloudToken: async () => "",
        }),
    ).toThrow(/credentials/);
  });
});

describe("in-cluster plain HTTP (ALLOW_IN_CLUSTER_HTTP)", () => {
  const base = {
    historyOrigin: "https://history.example",
    orgId: "org",
    workspaceId: "workspace",
    machineId: "machine-a",
    sponsorId: "user",
    apiKey: async () => "rak_test_only_not_a_real_key",
  };

  test("a Service URL needs the explicit policy; the default refuses it", () => {
    const baseUrl = "http://relayauth.relayauth.svc:8787";
    expect(() => new RelayAuthAssertionProvider({ ...base, baseUrl })).toThrow(
      /RELAYAUTH_URL must use https/,
    );
    expect(
      () =>
        new RelayAuthAssertionProvider({
          ...base,
          baseUrl,
          destinationPolicy: { allowInClusterHttp: true },
        }),
    ).not.toThrow();
  });

  test("the mint goes to the in-cluster URL and binds the configured origin unchanged", async () => {
    const seen: Array<{ url: string; body: { historyOrigin?: string } }> = [];
    const provider = new RelayAuthAssertionProvider({
      ...base,
      baseUrl: "http://relayauth.relayauth.svc:8787/",
      destinationPolicy: { allowInClusterHttp: true },
      fetcher: async (url, init) => {
        seen.push({ url: String(url), body: JSON.parse(String(init!.body)) });
        return Response.json(
          {
            accessToken: "assertion",
            accessTokenExpiresAt: new Date(Date.now() + 60_000).toISOString(),
            tokenType: "Bearer",
          },
          { status: 201 },
        );
      },
    });
    expect(await provider.get()).toBe("assertion");
    expect(seen).toEqual([
      {
        url: "http://relayauth.relayauth.svc:8787/v1/tokens/relayhistory-assertion",
        body: expect.objectContaining({ historyOrigin: "https://history.example" }),
      },
    ]);
  });

  test("the policy does not relax public hosts, IPs or the origin rule", () => {
    const policy = { allowInClusterHttp: true };
    for (const baseUrl of [
      "http://relayauth.example.com",
      "http://10.0.0.1",
      "http://evil.svc.example.com",
      "http://svc",
    ])
      expect(
        () => new RelayAuthAssertionProvider({ ...base, baseUrl, destinationPolicy: policy }),
      ).toThrow(/must use https/);
    expect(
      () =>
        new RelayAuthAssertionProvider({
          ...base,
          baseUrl: "http://user:pass@relayauth.relayauth.svc",
          destinationPolicy: policy,
        }),
    ).toThrow(/must not contain credentials/);
    expect(
      () =>
        new RelayAuthAssertionProvider({
          ...base,
          baseUrl: "http://relayauth.relayauth.svc:8787",
          historyOrigin: "http://history.example",
          destinationPolicy: policy,
        }),
    ).toThrow(/RELAYAUTH_HISTORY_ORIGIN must use https/);
    expect(
      () =>
        new RelayAuthAssertionProvider({
          ...base,
          baseUrl: "http://relayauth.relayauth.svc:8787",
          historyOrigin: "https://history.example/",
          destinationPolicy: policy,
        }),
    ).toThrow(/RELAYAUTH_HISTORY_ORIGIN must be an exact bare origin/);
  });

  test("CLOUD_URL follows the same rule", () => {
    const cloud = {
      workspaceId: "ws",
      machineId: "machine-a",
      cloudToken: async () => "cloud-test-only",
    };
    expect(
      () => new CloudSessionAssertionProvider({ ...cloud, baseUrl: "http://cloud.cloud.svc:3000" }),
    ).toThrow(/CLOUD_URL must use https/);
    expect(
      () =>
        new CloudSessionAssertionProvider({
          ...cloud,
          baseUrl: "http://cloud.cloud.svc:3000",
          destinationPolicy: { allowInClusterHttp: true },
        }),
    ).not.toThrow();
    expect(
      () =>
        new CloudSessionAssertionProvider({
          ...cloud,
          baseUrl: "http://cloud.example.com",
          destinationPolicy: { allowInClusterHttp: true },
        }),
    ).toThrow(/CLOUD_URL must use https/);
  });
});
