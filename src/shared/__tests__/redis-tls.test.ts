import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import { X509Certificate } from "node:crypto";
import { describe, expect, test } from "bun:test";
import { redisTlsPing, type RedisTlsConnector } from "../redis-tls.ts";

const ca = readFileSync(new URL("../../../certs/supabase-prod-ca-2021.crt", import.meta.url), "utf8");
const valid = {
  REDIS_HOST: "10.80.7.195",
  REDIS_PORT: "6378",
  REDIS_AUTH_ENABLED: "false",
  REDIS_TRANSIT_ENCRYPTION: "SERVER_AUTHENTICATION",
  REDIS_TLS_CA_PEM: ca,
};

function connector(response: string, authorized = true) {
  let options: Record<string, unknown> | undefined;
  const connect: RedisTlsConnector = (next, secure) => {
    options = next as Record<string, unknown>;
    const socket = new EventEmitter() as EventEmitter & {
      authorized: boolean;
      setTimeout(ms: number): typeof socket;
      write(data: string): boolean;
      destroy(): void;
    };
    socket.authorized = authorized;
    socket.setTimeout = () => socket;
    socket.write = (data) => {
      expect(data).toBe("*1\r\n$4\r\nPING\r\n");
      queueMicrotask(() => socket.emit("data", Buffer.from(response)));
      return true;
    };
    socket.destroy = () => {};
    queueMicrotask(secure);
    return socket as never;
  };
  return { connect, options: () => options };
}

describe("Redis TLS readiness", () => {
  test("requires TLS, the service CA, and passwordless AUTH settings", async () => {
    for (const env of [
      { ...valid, REDIS_TLS_CA_PEM: undefined },
      { ...valid, REDIS_AUTH_ENABLED: "true" },
      { ...valid, REDIS_TRANSIT_ENCRYPTION: "DISABLED" },
      { ...valid, REDIS_PASSWORD: "must-not-leak" },
      { ...valid, REDIS_TLS_CA_PEM: "-----BEGIN PRIVATE KEY-----\nnope" },
    ]) {
      expect(await redisTlsPing(env, (() => { throw new Error("must not connect"); }) as never))
        .toEqual({ ok: false, reason: "invalid_configuration" });
    }
  });

  test("uses CA verification and accepts only a Redis PONG", async () => {
    const ok = connector("+PONG\r\n");
    expect(await redisTlsPing(valid, ok.connect)).toEqual({ ok: true });
    expect(ok.options()).toMatchObject({
      host: "10.80.7.195",
      port: 6378,
      ca,
      rejectUnauthorized: true,
    });
    const checkServerIdentity = ok.options()?.checkServerIdentity as
      | ((host: string, cert: Record<string, unknown>) => Error | undefined)
      | undefined;
    expect(typeof checkServerIdentity).toBe("function");
    expect(checkServerIdentity?.("10.80.7.195", {
      raw: new X509Certificate(ca).raw,
    })).toBeUndefined();
    expect(checkServerIdentity?.("10.80.7.195", {
      raw: Buffer.from("wrong-ca"),
    })).toBeInstanceOf(Error);

    expect(await redisTlsPing(valid, connector("-NOAUTH nope\r\n").connect))
      .toEqual({ ok: false, reason: "unexpected_response" });
    expect(await redisTlsPing(valid, connector("+PONG\r\n", false).connect))
      .toEqual({ ok: false, reason: "tls_failed" });
  });

  test("refuses a public or hostname endpoint before opening a socket", async () => {
    for (const host of ["203.0.113.10", "redis.example.com", "127.0.0.1"]) {
      expect(await redisTlsPing({ ...valid, REDIS_HOST: host }, (() => { throw new Error("must not connect"); }) as never))
        .toEqual({ ok: false, reason: "invalid_configuration" });
    }
  });

  test("accepts a bounded rotation bundle and sanitizes connector failures", async () => {
    const rotation = connector("+PONG\r\n");
    expect(await redisTlsPing({ ...valid, REDIS_TLS_CA_PEM: `${ca}${ca}` }, rotation.connect))
      .toEqual({ ok: true });
    expect(rotation.options()?.ca).toBe(`${ca.trim()}\n${ca.trim()}\n`);
    expect(await redisTlsPing(valid, (() => { throw new Error("sensitive transport detail"); }) as never))
      .toEqual({ ok: false, reason: "tls_failed" });
  });
});
