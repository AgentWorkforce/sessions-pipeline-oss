// Vendored subset of relayhistory-cloud packages/relayhistory/src/lib/session-analysis/engine/model.ts
// (see SOURCE.json). Only the quick-summary (brief) path is ported: the transport types and
// summarizeSessionInput, byte-for-byte apart from imports. analyzeSessionInput (the full cited
// analysis, model gpt-5.4) is not part of the brief and is deliberately left out.
import {
  SESSION_SUMMARY_JSON_SCHEMA,
  SESSION_SUMMARY_TIMEOUT_MS,
  validateSummaryInput,
  validateSummaryText,
  type SessionSummaryInput,
  type SessionSummaryText,
} from "./summary-contract.ts";
import { SUMMARY_INSTRUCTIONS } from "./summary-prompt.ts";

/** Skip API session-analysis.ts, commit 07a16203, prompt/contract v2.4. */
export const ANALYZER_VERSION = "session-analysis-v2.4";

export interface ModelTransportRequest {
  model: string;
  instructions: string;
  input: string;
  schemaName: "session_analysis" | "session_summary";
  schema: Record<string, unknown>;
  timeoutMs: number;
  /** Summary model parameters; absent for full analysis. */
  store?: false;
  maxOutputTokens?: 256;
  reasoningEffort?: "none";
}
export interface ModelTransportResponse {
  outputText: string;
  inputTokens: number;
  outputTokens: number;
  providerRequestId: string | null;
  model?: string;
}
export interface AnalysisModelTransport {
  complete(request: ModelTransportRequest): Promise<ModelTransportResponse>;
}

export async function summarizeSessionInput(
  input: SessionSummaryInput,
  transport: AnalysisModelTransport,
  model: string,
): Promise<{
  summary: SessionSummaryText;
  model: string;
  inputTokens: number;
  outputTokens: number;
}> {
  validateSummaryInput(input);
  const result = await transport.complete({
    model,
    instructions: SUMMARY_INSTRUCTIONS,
    input: JSON.stringify(input),
    schemaName: "session_summary",
    schema: SESSION_SUMMARY_JSON_SCHEMA as unknown as Record<string, unknown>,
    timeoutMs: SESSION_SUMMARY_TIMEOUT_MS,
    store: false,
    maxOutputTokens: 256,
    reasoningEffort: "none",
  });
  let summary: SessionSummaryText;
  try {
    summary = validateSummaryText(JSON.parse(result.outputText));
  } catch {
    throw new TypeError("model summary has an invalid shape");
  }
  return {
    summary,
    model: result.model ?? model,
    inputTokens: result.inputTokens,
    outputTokens: result.outputTokens,
  };
}
