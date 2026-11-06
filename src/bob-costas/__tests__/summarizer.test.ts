import { describe, expect, test } from "bun:test";
import { digestRecords, extractiveSummarizer } from "../summarizer.ts";

const lines = [
  JSON.stringify({ type: "user", message: { role: "user", content: "Please fix the login bug" }, timestamp: "2026-10-28T10:00:00Z" }),
  JSON.stringify({ type: "assistant", message: { role: "assistant", model: "claude-sonnet-4-6", content: [{ type: "text", text: "On it" }, { type: "tool_use", name: "Bash", input: {} }] }, timestamp: "2026-10-28T10:00:05Z" }),
  JSON.stringify({ type: "assistant", message: { role: "assistant", model: "claude-sonnet-4-6", content: [{ type: "text", text: "Done" }] }, timestamp: "2026-10-28T10:00:09Z" }),
];

describe("bob costas extractive summariser", () => {
  test("digest counts turns, tools and models", () => {
    const d = digestRecords("claude", lines);
    expect(d.userTurns).toEqual(["Please fix the login bug"]);
    expect(d.assistantTurns).toBe(2);
    expect(d.toolCalls).toEqual({ Bash: 1 });
    expect(d.models).toEqual(["claude-sonnet-4-6"]);
  });
  test("short summary fits and names no model tokens", async () => {
    const s = await extractiveSummarizer.summarize("short", digestRecords("claude", lines));
    expect(s.text.length).toBeLessThanOrEqual(280);
    expect(s.text).toContain("login bug");
    expect(s.modelId).toBe("extractive/v1");
    expect(s.tokensIn + s.tokensOut).toBe(0);
  });
  test("summaries are scrubbed on the way out", async () => {
    const leaky = [JSON.stringify({ type: "user", message: { role: "user", content: "key AKIAIOSFODNN7EXAMPLE and mail a@b.com" } })];
    const s = await extractiveSummarizer.summarize("long", digestRecords("claude", leaky));
    expect(s.text).not.toContain("AKIAIOSFODNN7EXAMPLE");
    expect(s.text).not.toContain("a@b.com");
  });
});
