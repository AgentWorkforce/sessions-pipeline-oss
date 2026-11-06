import { X509Certificate } from "node:crypto";
import { connect as tlsConnect, type ConnectionOptions } from "node:tls";

export interface RedisTlsEnv {
  REDIS_HOST?: string;
  REDIS_PORT?: string;
  REDIS_AUTH?: string;
  REDIS_PASSWORD?: string;
  REDIS_AUTH_ENABLED?: string;
  REDIS_TRANSIT_ENCRYPTION?: string;
  REDIS_TLS_CA_PEM?: string;
}

export type RedisTlsVerdict =
  | { ok: true }
  | { ok: false; reason: "invalid_configuration" | "tls_failed" | "unexpected_response" };

interface RedisTlsSocket {
  authorized: boolean;
  setTimeout(ms: number): this;
  once(event: "error" | "timeout" | "close", listener: (...args: unknown[]) => void): this;
  on(event: "data", listener: (chunk: Buffer) => void): this;
  write(data: string): boolean;
  destroy(): void;
}

export type RedisTlsConnector = (
  options: ConnectionOptions,
  secureConnectListener: () => void,
) => RedisTlsSocket;

const CERTIFICATE = /-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+\r?\n-----END CERTIFICATE-----/g;
const PING = "*1\r\n$4\r\nPING\r\n";
const PONG = "+PONG\r\n";

function privateIpv4(value: string): boolean {
  const parts = value.split(".").map(Number);
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
  return parts[0] === 10 ||
    (parts[0] === 172 && parts[1]! >= 16 && parts[1]! <= 31) ||
    (parts[0] === 192 && parts[1] === 168);
}

interface PeerChainCertificate {
  raw?: Buffer;
}

function hasExpectedIssuer(cert: PeerChainCertificate, expected: ReadonlySet<string>): boolean {
  if (!cert.raw) return false;
  try {
    return expected.has(new X509Certificate(cert.raw).issuer);
  } catch {
    return false;
  }
}

function configured(env: RedisTlsEnv): { host: string; port: number; ca: string; caSubjects: ReadonlySet<string> } | undefined {
  const host = env.REDIS_HOST?.trim() ?? "";
  const portText = env.REDIS_PORT?.trim() ?? "";
  const ca = env.REDIS_TLS_CA_PEM?.trim() ?? "";
  const certificates = [...ca.matchAll(CERTIFICATE)].map((match) => match[0].trim());
  const remainder = ca.replace(CERTIFICATE, "").trim();
  if (
    !host ||
    !privateIpv4(host) ||
    !/^[0-9]+$/.test(portText) ||
    Number(portText) < 1 ||
    Number(portText) > 65535 ||
    env.REDIS_AUTH_ENABLED !== "false" ||
    env.REDIS_TRANSIT_ENCRYPTION !== "SERVER_AUTHENTICATION" ||
    env.REDIS_AUTH !== undefined ||
    env.REDIS_PASSWORD !== undefined ||
    ca.length > 131_072 ||
    ca.includes("PRIVATE KEY") ||
    certificates.length < 1 ||
    certificates.length > 4 ||
    remainder.length > 0
  ) return;
  try {
    return {
      host,
      port: Number(portText),
      ca: `${certificates.join("\n")}\n`,
      caSubjects: new Set(certificates.map((certificate) => new X509Certificate(certificate).subject)),
    };
  } catch {
    return;
  }
}

/**
 * Perform one CA-validated Redis PING without AUTH. Errors are collapsed to an
 * allowlisted reason so endpoints, certificates, and transport details never
 * reach readiness responses or logs.
 */
export async function redisTlsPing(
  env: RedisTlsEnv,
  connect: RedisTlsConnector = tlsConnect as unknown as RedisTlsConnector,
  timeoutMs = 3_000,
): Promise<RedisTlsVerdict> {
  const config = configured(env);
  if (!config) return { ok: false, reason: "invalid_configuration" };

  return new Promise((resolve) => {
    let settled = false;
    let response = "";
    let socket: RedisTlsSocket | undefined;
    const finish = (verdict: RedisTlsVerdict) => {
      if (settled) return;
      settled = true;
      socket?.destroy();
      resolve(verdict);
    };
    try {
      socket = connect(
        {
          host: config.host,
          port: config.port,
          ca: config.ca,
          rejectUnauthorized: true,
          // Memorystore exposes a private IP and unique per-instance CAs, not
          // a DNS name. Require the verified peer chain to contain one of the
          // exact downloaded trust anchors and require the leaf issuer to be
          // one of those per-instance CAs. DNS/IP SAN matching is inapplicable.
          checkServerIdentity: (_host, cert) =>
            hasExpectedIssuer(cert as unknown as PeerChainCertificate, config.caSubjects)
              ? undefined
              : new Error("Redis peer does not chain to the expected per-instance CA"),
        },
        () => {
          if (!socket?.authorized) return finish({ ok: false, reason: "tls_failed" });
          socket.write(PING);
        },
      );
    } catch {
      finish({ ok: false, reason: "tls_failed" });
      return;
    }
    socket.setTimeout(timeoutMs);
    socket.once("timeout", () => finish({ ok: false, reason: "tls_failed" }));
    socket.once("error", () => finish({ ok: false, reason: "tls_failed" }));
    socket.once("close", () => {
      if (!settled) finish({ ok: false, reason: "tls_failed" });
    });
    socket.on("data", (chunk) => {
      response += chunk.toString("utf8");
      if (response === PONG) finish({ ok: true });
      else if (response.length >= PONG.length || response.length > 64)
        finish({ ok: false, reason: "unexpected_response" });
    });
  });
}
