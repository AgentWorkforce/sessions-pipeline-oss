/**
 * Bob Costas summarisers. `extractive` runs with no model and is the default
 * (Khaliq's self-host decision: model calls off unless switched on). A model
 * summariser plugs in behind the same interface; its telemetry carries model
 * id and token counts only, never prompts or outputs.
 */
import { scrubText } from "../shared/vendor.ts";

export interface SessionDigest {
  harness: string;
  records: number;
  userTurns: string[];
  assistantTurns: number;
  toolCalls: Record<string, number>;
  models: string[];
  firstTs?: string;
  lastTs?: string;
}

export interface Summary {
  text: string;
  modelId: string;
  tokensIn: number;
  tokensOut: number;
}

export interface Summarizer {
  readonly id: string;
  summarize(kind: "short" | "long", digest: SessionDigest): Promise<Summary>;
}

/** Parses scrubbed JSONL transcript records into a digest. Harness-agnostic best effort. */
export function digestRecords(harness: string, lines: string[]): SessionDigest {
  const d: SessionDigest = { harness, records: 0, userTurns: [], assistantTurns: 0, toolCalls: {}, models: [] };
  for (const line of lines) {
    if (!line.trim()) continue;
    d.records += 1;
    let rec: Record<string, unknown>;
    try {
      rec = JSON.parse(line) as Record<string, unknown>;
    } catch {
      continue;
    }
    const ts = typeof rec.timestamp === "string" ? rec.timestamp : undefined;
    if (ts) {
      d.firstTs ??= ts;
      d.lastTs = ts;
    }
    const msg = rec.message as Record<string, unknown> | undefined;
    const role = (msg?.role ?? rec.role ?? rec.type) as string | undefined;
    const content = msg?.content ?? rec.content;
    if (role === "user") {
      const text = textOf(content);
      if (text) d.userTurns.push(text);
    } else if (role === "assistant") {
      d.assistantTurns += 1;
      if (typeof msg?.model === "string" && !d.models.includes(msg.model)) d.models.push(msg.model);
      if (Array.isArray(content))
        for (const part of content as Record<string, unknown>[])
          if (part?.type === "tool_use" && typeof part.name === "string") d.toolCalls[part.name] = (d.toolCalls[part.name] ?? 0) + 1;
    }
  }
  return d;
}

function textOf(content: unknown): string | undefined {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts = (content as Record<string, unknown>[]).filter((p) => p?.type === "text" && typeof p.text === "string").map((p) => p.text as string);
    return parts.length ? parts.join(" ") : undefined;
  }
  return undefined;
}

const clip = (s: string, n: number) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

export const extractiveSummarizer: Summarizer = {
  id: "extractive/v1",
  async summarize(kind, d) {
    const first = d.userTurns[0] ? clip(d.userTurns[0].replace(/\s+/g, " ").trim(), 160) : "(no user prompt)";
    const tools = Object.entries(d.toolCalls).sort((a, b) => b[1] - a[1]);
    const toolLine = tools.length ? tools.map(([n, c]) => `${n}×${c}`).join(", ") : "none";
    if (kind === "short") {
      const text = `${d.harness}: "${first}" — ${d.userTurns.length} user turns, ${d.assistantTurns} assistant turns, tools: ${toolLine}.`;
      return { text: scrubText(clip(text, 280)), modelId: this.id, tokensIn: 0, tokensOut: 0 };
    }
    const prompts = d.userTurns.slice(0, 10).map((t, i) => `${i + 1}. ${clip(t.replace(/\s+/g, " ").trim(), 200)}`).join("\n");
    const text = [
      `# Session summary (${d.harness})`,
      "",
      `- Records: ${d.records}`,
      `- User turns: ${d.userTurns.length}; assistant turns: ${d.assistantTurns}`,
      `- Models: ${d.models.length ? d.models.join(", ") : "unknown"}`,
      `- Tools: ${toolLine}`,
      `- Span: ${d.firstTs ?? "?"} → ${d.lastTs ?? "?"}`,
      "",
      "## Prompts",
      prompts || "(none)",
    ].join("\n");
    return { text: scrubText(text), modelId: this.id, tokensIn: 0, tokensOut: 0 };
  },
};

/** Chosen by BOB_COSTAS_MODEL: `off` (default) is extractive; anything else is not wired yet. */
export function summarizerFromEnv(): Summarizer {
  const model = (process.env.BOB_COSTAS_MODEL ?? "off").trim();
  if (model === "" || model === "off" || model === "extractive") return extractiveSummarizer;
  throw new Error(`96bulls: BOB_COSTAS_MODEL=${model} is not wired; only the extractive summariser ships in the local stack`);
}
