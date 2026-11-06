import type {
  AnalysisModelTransport,
  ModelTransportRequest,
  ModelTransportResponse,
} from "./model.ts";

const RESPONSES_URL = "https://api.openai.com/v1/responses";

export type SessionAnalysisProviderErrorCode =
  | "model_not_configured"
  | "model_auth_failed"
  | "model_rate_limited"
  | "model_unavailable"
  | "model_timeout"
  | "model_request_failed"
  | "model_output_incomplete"
  | "model_refusal"
  | "model_output_invalid";

/** Controlled failure metadata; never contains credentials, request text or provider response bodies. */
export class SessionAnalysisProviderError extends Error {
  constructor(
    readonly code: SessionAnalysisProviderErrorCode,
    readonly retryable: boolean,
  ) {
    super(code);
    this.name = "SessionAnalysisProviderError";
  }
}

export interface AnalysisTransportOptions {
  fetch?: typeof globalThis.fetch;
  apiKey: () => string | undefined | Promise<string | undefined>;
}

/** Exact Skip Responses request with the Worker OPENAI_API_KEY binding. */
export function createAnalysisTransport(
  options: AnalysisTransportOptions,
): AnalysisModelTransport {
  const keySupplier = options.apiKey;
  return {
    async complete(
      request: ModelTransportRequest,
    ): Promise<ModelTransportResponse> {
      let apiKey: string | undefined;
      try {
        apiKey = (await keySupplier())?.trim();
      } catch {
        throw new SessionAnalysisProviderError("model_not_configured", false);
      }
      if (!apiKey)
        throw new SessionAnalysisProviderError("model_not_configured", false);
      const signal = AbortSignal.timeout(request.timeoutMs);
      let response: Response;
      try {
        const init: RequestInit = {
          method: "POST",
          signal,
          // Workerd rejects `redirect: "error"` before it sends the request.
          // Manual mode keeps the Authorization header off any redirect target;
          // the non-2xx check below then rejects the redirect response.
          redirect: "manual",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            model: request.model,
            ...(request.store === undefined ? {} : { store: request.store }),
            ...(request.maxOutputTokens === undefined
              ? {}
              : { max_output_tokens: request.maxOutputTokens }),
            ...(request.reasoningEffort === undefined
              ? {}
              : { reasoning: { effort: request.reasoningEffort } }),
            instructions: request.instructions,
            input: request.input,
            text: {
              format: {
                type: "json_schema",
                name: request.schemaName,
                strict: true,
                schema: request.schema,
              },
            },
          }),
        };
        response = options.fetch
          ? await options.fetch(RESPONSES_URL, init)
          : await globalThis.fetch(RESPONSES_URL, init);
      } catch {
        throw new SessionAnalysisProviderError(
          signal.aborted ? "model_timeout" : "model_unavailable",
          true,
        );
      }
      if (!response.ok) throw statusError(response.status);
      let payload: unknown;
      try {
        payload = await response.json();
      } catch {
        throw new SessionAnalysisProviderError(
          signal.aborted ? "model_timeout" : "model_output_invalid",
          signal.aborted,
        );
      }
      if (signal.aborted)
        throw new SessionAnalysisProviderError("model_timeout", true);
      if (!record(payload))
        throw new SessionAnalysisProviderError("model_output_invalid", false);
      if (payload.status === "incomplete")
        throw new SessionAnalysisProviderError(
          "model_output_incomplete",
          false,
        );
      if (payload.status === "failed" || payload.status === "cancelled")
        throw new SessionAnalysisProviderError("model_unavailable", true);
      if (payload.status !== undefined && payload.status !== "completed") {
        throw new SessionAnalysisProviderError(
          "model_output_incomplete",
          false,
        );
      }
      if (!Array.isArray(payload.output))
        throw new SessionAnalysisProviderError("model_output_invalid", false);
      const fragments: string[] = [];
      for (const item of payload.output) {
        if (!record(item) || !Array.isArray(item.content)) continue;
        for (const content of item.content) {
          if (!record(content)) continue;
          if (content.type === "refusal")
            throw new SessionAnalysisProviderError("model_refusal", false);
          if (
            content.type === "output_text" &&
            typeof content.text === "string"
          )
            fragments.push(content.text);
        }
      }
      const outputText = fragments.join("");
      if (!outputText.trim())
        throw new SessionAnalysisProviderError("model_output_invalid", false);
      const usage = record(payload.usage) ? payload.usage : {};
      return {
        outputText,
        inputTokens: count(usage.input_tokens),
        outputTokens: count(usage.output_tokens),
        providerRequestId:
          typeof payload.id === "string" ? payload.id.slice(0, 255) : null,
        model:
          typeof payload.model === "string" && payload.model.trim()
            ? payload.model
            : request.model,
      };
    },
  };
}

function statusError(status: number): SessionAnalysisProviderError {
  if (status === 401 || status === 403)
    return new SessionAnalysisProviderError("model_auth_failed", false);
  if (status === 429)
    return new SessionAnalysisProviderError("model_rate_limited", true);
  if (status === 408 || status >= 500)
    return new SessionAnalysisProviderError("model_unavailable", true);
  return new SessionAnalysisProviderError("model_request_failed", false);
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function count(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
    ? value
    : 0;
}
