/**
 * The vendored relayhistory-cloud sources reference Cloudflare Workers ambient
 * types in a few signatures we never call (queue bindings). Declaring the
 * names keeps the compiler quiet without pulling in workers-types, which
 * conflict with Node's fetch typings.
 */
declare interface Message<Body = unknown> {
  readonly id: string;
  readonly timestamp: Date;
  readonly body: Body;
  readonly attempts: number;
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}
declare interface Queue<Body = unknown> {
  send(message: Body, options?: { delaySeconds?: number; contentType?: string }): Promise<void>;
  sendBatch(messages: Iterable<{ body: Body; delaySeconds?: number }>, options?: { delaySeconds?: number }): Promise<void>;
}
declare interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
  passThroughOnException(): void;
  props: unknown;
}
declare interface MessageBatch<Body = unknown> {
  readonly queue: string;
  readonly messages: readonly Message<Body>[];
  ackAll(): void;
  retryAll(options?: { delaySeconds?: number }): void;
}
// Web Crypto types the vendored auth code names; Node provides the values.
declare type CryptoKey = import("node:crypto").webcrypto.CryptoKey;
declare type JsonWebKey = import("node:crypto").webcrypto.JsonWebKey;
