/** Synthetic OTLP inputs shared by the PR449 identity regression and its baseline receipt. */
export const attr = (key: string, value: string | number) => ({
  key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value },
});
export const reviewObservedAt = "2026-10-01T00:00:00.000Z";
const nano = String(BigInt(Date.parse(reviewObservedAt)) * 1_000_000n);
const resource = { attributes: [attr("service.name", "Codex_Desktop")] };

export const reviewCodexPayloads = [
  { kind: "span", route: "/v1/traces", payload: { resourceSpans: [{
    resource, scopeSpans: [{ spans: [{
      name: "handle_responses", traceId: "1".repeat(32), spanId: "2".repeat(16),
      startTimeUnixNano: nano,
      attributes: [attr("gen_ai.usage.input_tokens", 29), attr("gen_ai.usage.output_tokens", 7),
        attr("gen_ai.request.model", "gpt-5.4")],
    }] }],
  }] } },
  { kind: "log", route: "/v1/logs", payload: { resourceLogs: [{
    resource, scopeLogs: [{ logRecords: [{
      timeUnixNano: nano, traceId: "1".repeat(32),
      attributes: [attr("event.name", "codex.sse_event"), attr("event.kind", "response.completed"),
        attr("input_token_count", 29), attr("output_token_count", 7), attr("model", "gpt-5.4")],
    }] }],
  }] } },
] as const;
