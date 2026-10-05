import assert from "node:assert/strict";
import { test } from "node:test";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { admittedMetadataAttributes } from "../packages/shared/src/index";

const session = "11111111-1111-4111-8111-111111111111";
const stamp = "2026-09-20T10:00:00.000Z";
const nano = String(BigInt(Date.parse(stamp)) * 1_000_000n);
const attribute = (key: string, value: string | number) => ({ key,
  value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } });
const resource = { attributes: [attribute("service.name", "codex-app-server")] };

for (const cache of [7, 0, undefined]) test(`reader-first writer: cache ${cache ?? "unknown"} adds no tier key`, () => {
  const payload = { resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{ timeUnixNano: nano,
    attributes: [attribute("event.name", "codex.sse_event"), attribute("conversation.id", session),
      attribute("model", "synthetic-codex-model"), attribute("input_token_count", "100"),
      attribute("output_token_count", "10"), attribute("service_tier", "fast"),
      ...(cache === undefined ? [] : [attribute("cached_token_count", cache)])]
  }] }] }] };
  const parsed = explodeOtlpPayload(payload, { source: "codex", transportPath: "/v1/logs" });
  assert.equal(parsed.events.length, 1);
  assert.equal(parsed.events[0].event.cacheReadTokens, cache);
  assert.equal(Object.hasOwn(parsed.events[0].event.metadata, "serviceTier"), false);
  assert.equal(Object.hasOwn(parsed.events[0].event.metadata, "service_tier"), false);
});

test("reader preserves every bounded tier value exactly without enabling producer intake", () => {
  for (const service_tier of ["fast", "default", "standard", "priority", "flex", "batch"]) {
    for (const serviceTier of ["standard", "priority", "flex", "batch"]) {
      assert.deepEqual(admittedMetadataAttributes({ serviceTier, service_tier }).attributes, {});
      const sealed = sealOutboundEnvelope({ event: { id: session, source: "codex", dataMode: "metadata",
        eventType: "assistant_response", observedAt: stamp, inputTokens: 100, outputTokens: 10,
        metadata: { serviceTier, service_tier } }, suppressedFields: [] });
      assert.equal(sealed.ok, true);
      if (!sealed.ok) throw new Error("tier metadata failed sealing");
      assert.deepEqual(sealed.envelope.event.metadata, { serviceTier, service_tier });
      assert.deepEqual(sealOutboundEnvelope(sealed.envelope), sealed);
    }
  }
});
