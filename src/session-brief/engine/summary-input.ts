import {
  SESSION_SUMMARY_MAX_MESSAGES,
  SESSION_SUMMARY_TEXT_BUDGET,
  validateSummaryInput,
  type SessionSummaryInput,
} from "./summary-contract.ts";
import type { NormalizedEvent } from "./normalized-history.ts";

/** Skip's topLevelSessionMessages + summaryInput, ported without history I/O. */
export function summaryInput(
  source: string,
  sessionId: string,
  events: readonly NormalizedEvent[],
): SessionSummaryInput {
  const messages = events
    .filter(
      (event) =>
        event.source === source &&
        event.sessionId === sessionId &&
        (event.role === "user" || event.role === "assistant") &&
        event.kind === "text" &&
        event.text?.trim(),
    )
    .sort((a, b) => a.tsMs - b.tsMs || a.id - b.id);
  if (!messages.length)
    throw new Error(
      "No user or assistant messages are available to summarize.",
    );
  if (messages.length > SESSION_SUMMARY_MAX_MESSAGES)
    throw new Error(
      `This session has more than ${SESSION_SUMMARY_MAX_MESSAGES} messages; use full analysis for this unusually long conversation.`,
    );
  const perMessage = Math.floor(
    SESSION_SUMMARY_TEXT_BUDGET / Math.max(1, messages.length),
  );
  return validateSummaryInput({
    source,
    sessionId,
    messages: messages.map((message) => {
      const text = message.text!.trim();
      const truncated = text.length > perMessage;
      const head = Math.ceil((perMessage - 3) / 2);
      return {
        role: message.role as "user" | "assistant",
        text: truncated
          ? `${text.slice(0, head)} … ${text.slice(-(perMessage - 3 - head))}`
          : text,
        truncated,
      };
    }),
  });
}
