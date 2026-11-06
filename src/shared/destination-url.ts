/**
 * One transport policy for every outbound destination that carries a
 * credential or decides which keys we trust: the RelayAuth mint URL, the Cloud
 * session URL, the courier's ingest URL, the RelayHistory origin (the `aud`
 * every assertion is bound to), the JWKS URL and the model endpoint.
 *
 *   - https: always allowed.
 *   - http to loopback (`localhost`, `127.0.0.1`, `[::1]`): always allowed.
 *   - http to a Kubernetes Service DNS name, `<service>.<namespace>.svc` or
 *     `<service>.<namespace>.svc.cluster.local`: allowed ONLY when the process
 *     sets `ALLOW_IN_CLUSTER_HTTP=true`. That is a statement by the operator that
 *     the pod runs inside the cluster and the cluster network is the transport
 *     boundary. Any other host (public names, IP literals, single labels, names
 *     that merely contain `.svc`) still needs https.
 *   - Credentials in the URL, a query or a fragment: always refused.
 *
 * The flag changes transport only. It never changes which origin a token is
 * bound to: audience comparisons stay exact string matches on the configured
 * origin, whatever scheme that origin uses.
 */

export const IN_CLUSTER_HTTP_FLAG = "ALLOW_IN_CLUSTER_HTTP";

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
/** RFC 1123 label, as Kubernetes requires for Service and Namespace names. */
const DNS_LABEL = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;
const IPV4_LITERAL = /^\d{1,3}(\.\d{1,3}){3}$/;

export interface DestinationPolicy {
  /** `ALLOW_IN_CLUSTER_HTTP=true`: plain http to Kubernetes Service DNS names. */
  allowInClusterHttp: boolean;
}

/**
 * Reads `ALLOW_IN_CLUSTER_HTTP`. Unset, empty or `false` is off; `true` is on;
 * anything else is a startup error, so a typo never silently means either.
 */
export function inClusterHttpAllowed(
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  const raw = env[IN_CLUSTER_HTTP_FLAG]?.trim().toLowerCase();
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${IN_CLUSTER_HTTP_FLAG} must be true or false`);
}

export function destinationPolicy(
  env: NodeJS.ProcessEnv = process.env,
): DestinationPolicy {
  return { allowInClusterHttp: inClusterHttpAllowed(env) };
}

export function isLoopbackHost(hostname: string): boolean {
  return LOOPBACK_HOSTS.has(hostname.toLowerCase());
}

/**
 * True only for a fully formed Service name: `<svc>.<ns>.svc` or
 * `<svc>.<ns>.svc.cluster.local`, each label an RFC 1123 label. Expects the
 * WHATWG-normalised `URL#hostname` (lower-cased, IDNA to punycode).
 */
export function isKubernetesServiceHost(hostname: string): boolean {
  const host = hostname.toLowerCase();
  if (!host || host.startsWith("[") || IPV4_LITERAL.test(host)) return false;
  const labels = host.split(".");
  let service: string[];
  if (labels.length === 3 && labels[2] === "svc") service = labels.slice(0, 2);
  else if (
    labels.length === 5 &&
    labels[2] === "svc" &&
    labels[3] === "cluster" &&
    labels[4] === "local"
  )
    service = labels.slice(0, 2);
  else return false;
  return service.every((label) => DNS_LABEL.test(label));
}

export interface DestinationOptions {
  /** Allow a path (e.g. `/cloud`, `/.well-known/jwks.json`). Default true. */
  allowPath?: boolean;
  /**
   * Shape check only: any http host. For local-only modes that carry no
   * durable credential (the Docker stack's `http://96bulls-ingest:8080`).
   */
  allowAnyHttpHost?: boolean;
}

/**
 * Validates `raw` as the value of env var `name` under `policy`. Returns the
 * parsed URL; throws a startup error that names the variable and the rule,
 * never the value (a mistyped secret can land in a URL variable).
 */
export function assertDestinationUrl(
  name: string,
  raw: string,
  policy: DestinationPolicy,
  options: DestinationOptions = {},
): URL {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`96bulls: ${name} must be an absolute http(s) URL`);
  }
  if (url.username || url.password || url.search || url.hash)
    throw new Error(
      `96bulls: ${name} must not contain credentials, query or fragment`,
    );
  if (options.allowPath === false && url.pathname !== "/")
    throw new Error(`96bulls: ${name} must be a bare origin (no path)`);
  if (url.protocol === "https:") return url;
  if (url.protocol !== "http:")
    throw new Error(`96bulls: ${name} must be an absolute http(s) URL`);
  if (options.allowAnyHttpHost || isLoopbackHost(url.hostname)) return url;
  if (policy.allowInClusterHttp && isKubernetesServiceHost(url.hostname))
    return url;
  throw new Error(
    policy.allowInClusterHttp
      ? `96bulls: ${name} must use https (plain http is loopback-only, or a Kubernetes Service name <svc>.<ns>.svc[.cluster.local] with ${IN_CLUSTER_HTTP_FLAG}=true)`
      : `96bulls: ${name} must use https (plain http is loopback-only; set ${IN_CLUSTER_HTTP_FLAG}=true to allow in-cluster <svc>.<ns>.svc[.cluster.local] Service URLs)`,
  );
}

/** Boolean form of `assertDestinationUrl` for tests and guards. */
export function isAllowedDestination(
  raw: string,
  policy: DestinationPolicy,
  options: DestinationOptions = {},
): boolean {
  try {
    assertDestinationUrl("URL", raw, policy, options);
    return true;
  } catch {
    return false;
  }
}
