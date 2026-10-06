import { nativeCodexFixture } from "./native-codex-fixture";
import { explodeOtlpPayload } from "../../packages/collector-cli/src/otlp";

/** buffer.ts:815 -> useWorkspace:1092 -> append:2732,2750 stamps the actual
 * fixture workspace/device/install binding. No raw row or witness is patched. */
export const homeCodexCaptureBinding = {
  workspaceId: "11111111-1111-4111-8111-000000000464",
  deviceId: "home-codex-proof",
  freshCaptureRootEpoch: "22222222-2222-4222-8222-000000000464",
};

/** Native request facts for normalized/OTLP project-attribution fixtures.
 * Rollout fixtures instead write a real turn_context with turn_id. */
export function homeCodexNativeRequest(id: string, sessionId: string, observedAt: string) {
  const native = nativeCodexFixture(id, "gpt-6.1-sol");
  const time = String(BigInt(Date.parse(observedAt)) * 1_000_000n);
  // otlp.ts:570-635 constructs the production native trace/request metadata.
  const normalized = explodeOtlpPayload({ resourceSpans: [{
    resource: { attributes: [{ key: "service.name", value: { stringValue: "codex-cli" } }] },
    scopeSpans: [{ scope: { name: "codex_otel" }, spans: [{
      traceId: native.metadata.traceId, spanId: native.metadata.traceId.slice(0, 16),
      name: "codex.model_request", startTimeUnixNano: time, endTimeUnixNano: time,
      attributes: [
        { key: "session.id", value: { stringValue: sessionId } },
        { key: "turn.id", value: { stringValue: id } },
        { key: "gen_ai.request.model", value: { stringValue: native.model } },
        { key: "gen_ai.usage.input_tokens", value: { intValue: "19" } },
        { key: "gen_ai.usage.output_tokens", value: { intValue: "2" } },
      ],
    }] }],
  }] }, { source: "codex", resolveGit: false });
  if (normalized.events.length !== 1) throw new Error("native_home_request_fixture_did_not_normalize");
  return normalized.events[0]!.event.metadata;
}
