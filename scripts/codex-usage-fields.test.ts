import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer, validateRolloutParserState } from "../packages/collector-cli/src/rollout-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { validatedMetadataAttribute, admittedMetadataAttributes } from "../packages/shared/src/index";

const session = "11111111-1111-4111-8111-111111111111";
const stamp = "2026-09-20T10:00:00.000Z";
const nano = String(BigInt(Date.parse(stamp)) * 1_000_000n);
const model = "synthetic-codex-model"; // No local tariff; fixtures isolate captured fields.
const trace = "a".repeat(32), spanId = "b".repeat(16);
const json = (value: unknown) => JSON.parse(JSON.stringify(value));
const attr = (key: string, value: string | number) => ({ key, value: typeof value === "number"
  ? { intValue: String(value) } : { stringValue: value } });
const resource = { attributes: [attr("service.name", "codex-app-server"), attr("service.version", "0.155.0-alpha.9.2")] };
const fields = (cache: number | undefined, tier: string | undefined, log = true) => ({
  ...(log ? { "event.name": "codex.sse_event" } : {}),
  "conversation.id": session, model,
  [log ? "input_token_count" : "gen_ai.usage.input_tokens"]: "100",
  [log ? "output_token_count" : "gen_ai.usage.output_tokens"]: "10",
  ...(cache === undefined ? {} : { [log ? "cached_token_count" : "gen_ai.usage.cache_read.input_tokens"]: String(cache) }),
  ...(tier === undefined ? {} : { service_tier: tier }),
});
const attributes = (values: Record<string, string>) => Object.entries(values).map(([key, value]) => attr(key, value));
const baseEvent = { tenantId: "00000000-0000-4000-8000-000000000001", sessionId: session, source: "codex", dataMode: "metadata",
  eventType: "assistant_response", observedAt: stamp, model, intent: "unknown", actionClass: "other", inputTokens: 100, outputTokens: 10 };

// These cases explicitly name reported values, a reported zero and an unknown.
const cases = [
  { name: "reported cached count and priority tier", cache: 7, tier: "priority", want: "priority" },
  { name: "reported zero and fast tier", cache: 0, tier: "fast", want: "priority" },
  { name: "absent cached count and flex tier", cache: undefined, tier: "flex", want: "flex" },
  { name: "reported zero and absent tier", cache: 0, tier: undefined, want: undefined },
  { name: "absent cached count and absent tier", cache: undefined, tier: undefined, want: undefined },
  { name: "explicit default routing", cache: 7, tier: "default", want: "standard" },
  { name: "canonical standard", cache: 7, tier: "standard", want: "standard" },
  { name: "canonical batch", cache: 7, tier: "batch", want: "batch" },
];

for (const item of cases) {
  test(`OTel log: ${item.name}`, () => {
    const attrs = fields(item.cache, item.tier);
    const record = { timeUnixNano: nano, attributes: [attr("event.kind", "response.completed"), ...attributes(attrs)] };
    const payload = { resourceLogs: [{ resource, scopeLogs: [{ logRecords: [record] }] }] };
    const result = explodeOtlpPayload(payload, { source: "codex", transportPath: "/v1/logs" });
    assert.equal(result.parseFailures, 0);
    assert.equal(result.events.length, 1);
    assert.equal(result.events[0].event.metadata.serviceTier, item.want);
    const expected = { ...baseEvent,
      id: deterministicEventId(["codex", "codex.sse_event", session, stamp, undefined, undefined, nano, JSON.stringify(attrs)]),
      ...(item.cache === undefined ? {} : { cacheReadTokens: item.cache }),
      metadata: { ...attrs, ...(item.want ? { serviceTier: item.want } : {}), otelEventName: "codex.sse_event",
        transport_path: "/v1/logs", serviceName: "codex-app-server", serviceVersion: "0.155.0-alpha.9.2" } };
    assert.deepEqual(json(result.events[0].event), expected);
    console.log(JSON.stringify({ fixture: `log/${item.name}`, input: payload, emitted: json(result.events[0].event) }));
  });
  test(`OTel trace: ${item.name}`, () => {
    const attrs = fields(item.cache, item.tier, false);
    const record = { traceId: trace, spanId, name: "handle_responses", startTimeUnixNano: nano,
      endTimeUnixNano: nano, attributes: attributes(attrs) };
    const payload = { resourceSpans: [{ resource, scopeSpans: [{ spans: [record] }] }] };
    const result = explodeOtlpPayload(payload, { source: "codex", transportPath: "/v1/traces" });
    assert.equal(result.parseFailures, 0);
    assert.equal(result.events.length, 1);
    const expected = { ...baseEvent, id: deterministicEventId(["codex", "span", "handle_responses", session, stamp, spanId, trace]),
      ...(item.cache === undefined ? {} : { cacheReadTokens: item.cache }),
      metadata: { ...attrs, ...(item.want ? { serviceTier: item.want } : {}), otelEventName: "handle_responses",
        otelSpanEndAt: stamp, traceId: trace, spanId, sessionLinkBasis: "span_attribute",
        transport_path: "/v1/traces", serviceName: "codex-app-server" } };
    assert.deepEqual(json(result.events[0].event), expected);
    console.log(JSON.stringify({ fixture: `trace/${item.name}`, input: payload, emitted: json(result.events[0].event) }));
  });
}

test("numeric cached attributes, including zero, survive as numbers", () => {
  for (const cache of [7, 0]) {
    const attrs = fields(undefined, undefined);
    const result = explodeOtlpPayload({ resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{ timeUnixNano: nano,
      attributes: [...attributes(attrs), attr("cached_token_count", cache)] }] }] }] }, { source: "codex" });
    assert.equal(result.events[0].event.cacheReadTokens, cache);
    assert.equal(result.events[0].event.metadata.cached_token_count, String(cache));
  }
});

test("tiers reject plan names and private/unbounded values on every surface", () => {
  for (const value of [undefined, null, "pro", "unknown", "experimental-tier", "Bearer SYNTHETIC", "/private/tier", "a".repeat(200)]) {
    for (const key of ["serviceTier", "service_tier"]) assert.equal(validatedMetadataAttribute(key, value).accepted, false);
  }
  for (const value of ["standard", "priority", "flex", "batch"]) {
    assert.equal(validatedMetadataAttribute("serviceTier", value).accepted, true);
    assert.deepEqual(admittedMetadataAttributes({ serviceTier: value }).attributes, {});
    assert.equal(admittedMetadataAttributes({ service_tier: value }).attributes.service_tier, value);
    for (const surface of ["resource", "scope"] as const)
      assert.deepEqual(admittedMetadataAttributes({ service_tier: value }, surface).attributes, {});
  }
  for (const value of ["fast", "default"]) assert.equal(validatedMetadataAttribute("serviceTier", value).accepted, false);
  const result = explodeOtlpPayload({ resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{ timeUnixNano: nano,
    attributes: attributes(fields(0, "pro")) }] }] }] }, { source: "codex" });
  assert.equal(result.events[0].event.metadata.serviceTier, undefined);
  assert.equal(result.events[0].event.metadata.service_tier, undefined);
});

test("trace completion supplies count and tier only for a unique matching response", () => {
  const completion = { name: "event", timeUnixNano: nano, attributes: [attr("event.kind", "response.completed"), ...attributes(fields(0, "priority"))] };
  for (const variant of ["unique", "ambiguous", "mismatch", "foreign", "conflict"] as const) {
    const parent = fields(undefined, variant === "conflict" ? "flex" : undefined, false);
    const child = json(completion);
    if (variant === "mismatch") child.attributes.find((a: { key: string }) => a.key === "input_token_count").value.stringValue = "101";
    if (variant === "foreign") child.attributes.find((a: { key: string }) => a.key === "conversation.id").value.stringValue = "22222222-2222-4222-8222-222222222222";
    const record = { traceId: trace, spanId, name: "handle_responses", startTimeUnixNano: nano, endTimeUnixNano: nano,
      attributes: attributes(parent), events: variant === "ambiguous" ? [child, child] : [child] };
    const payload = { resourceSpans: [{ resource, scopeSpans: [{ spans: [record] }] }] };
    const result = explodeOtlpPayload(payload, { source: "codex", transportPath: "/v1/traces" });
    const expected = { ...baseEvent, id: deterministicEventId(["codex", "span", "handle_responses", session, stamp, spanId, trace]),
      ...(variant === "unique" || variant === "conflict" ? { cacheReadTokens: 0 } : {}),
      metadata: { ...parent, ...(variant === "unique" ? { serviceTier: "priority" } : {}), otelEventName: "handle_responses",
        otelSpanEndAt: stamp, traceId: trace, spanId, sessionLinkBasis: "span_attribute", transport_path: "/v1/traces", serviceName: "codex-app-server" } };
    assert.deepEqual(json(result.events[0].event), expected);
    console.log(JSON.stringify({ fixture: `trace-event/${variant}`, input: payload, emitted: json(result.events[0].event) }));
  }
});

const line = (type: string, payload: unknown) => JSON.stringify({ timestamp: stamp, type, payload }) + "\n";
const totals = (input: number, cached: number | undefined, output: number) => ({ input_tokens: input,
  ...(cached === undefined ? {} : { cached_input_tokens: cached }), output_tokens: output, reasoning_output_tokens: 0, total_tokens: input + output });
const countLine = (input: number, cache: number | undefined, output: number) => line("event_msg", { type: "token_count",
  info: { total_token_usage: totals(input, cache, output) }, rate_limits: { plan_type: "pro" } });
const settingsLine = (tier?: string, owner?: string) => line("event_msg", { type: "thread_settings_applied",
  ...(owner ? { thread_id: owner } : {}), thread_settings: { model, ...(tier ? { service_tier: tier } : {}) } });

async function rolloutFixture(run: (buffer: LocalEventBuffer, tailer: RolloutTailer, file: string, sessions: string) => Promise<void>) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-usage-fields-"));
  const sessions = path.join(root, ".codex/sessions");
  const day = path.join(sessions, "2026/09/20");
  fs.mkdirSync(day, { recursive: true });
  const file = path.join(day, `rollout-2026-09-20T10-00-00-${session}.jsonl`);
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  const tailer = new RolloutTailer(buffer, sessions, () => []);
  try { await run(buffer, tailer, file, sessions); }
  finally { tailer.close(); buffer.close(); fs.rmSync(root, { recursive: true, force: true }); }
}
const rows = (buffer: LocalEventBuffer) => (buffer.database.prepare(
  "select payload_json as payload from buffered_events where event_type='usage_rollout' order by rowid",
).all() as Array<{ payload: string }>).map(row => JSON.parse(row.payload));
const expectedRollout = (index: number, input: number, output: number, cache?: number, tier?: string) => ({
  id: deterministicEventId(["codex-rollout", session, String(index)]), tenantId: "local", source: "codex", dataMode: "metadata",
  eventType: "usage_rollout", observedAt: stamp, sessionId: session, model, intent: "unknown", actionClass: "other", inputTokens: input, outputTokens: output,
  ...(cache === undefined ? {} : { cacheReadTokens: cache }), metadata: { usageSource: "rollout", turnIndex: index,
    cliVersion: "0.155.0-alpha.9.2", planType: "pro", ...(tier ? { serviceTier: tier } : {}) } });
const rolloutPrefix = line("session_meta", { id: session, cli_version: "0.155.0-alpha.9.2" }) + line("turn_context", { model });

for (const item of cases) test(`rollout turn: ${item.name}`, () => rolloutFixture(async (buffer, tailer, file) => {
  const input = rolloutPrefix + settingsLine(item.tier, session) + countLine(0, 0, 0) + countLine(100, item.cache, 10);
  fs.writeFileSync(file, input);
  const scan = await tailer.scan({ scope: "full" });
  assert.equal(scan.parseErrors, 0);
  assert.equal(scan.eventsAppended, 1);
  assert.deepEqual(rows(buffer), [expectedRollout(1, 100, 10, item.cache, item.want)]);
  console.log(JSON.stringify({ fixture: `rollout/${item.name}`, input, emitted: rows(buffer)[0] }));
}));

test("rollout checkpoints preserve tier, clear absent/invalid snapshots, and keep unknown counter boundaries", () => rolloutFixture(async (buffer, tailer, file, sessions) => {
  fs.writeFileSync(file, rolloutPrefix + settingsLine("fast", session) + countLine(0, 0, 0) + countLine(100, undefined, 10));
  await tailer.scan({ scope: "full" });
  tailer.close();
  const resumed = new RolloutTailer(buffer, sessions, () => []);
  try {
    fs.appendFileSync(file, countLine(200, 7, 20) + countLine(300, 7, 30) + settingsLine(undefined, session) +
      countLine(400, 7, 40) + settingsLine("pro", session) + countLine(500, 7, 50) +
      settingsLine("priority", "22222222-2222-4222-8222-222222222222") + countLine(600, 7, 60));
    const scan = await resumed.scan({ scope: "full" });
    assert.equal(scan.checkpointRebuilds, 0);
    const expected = [expectedRollout(1, 100, 10, undefined, "priority"), expectedRollout(2, 100, 10, undefined, "priority"),
      expectedRollout(3, 100, 10, 0, "priority"), expectedRollout(4, 100, 10, 0), expectedRollout(5, 100, 10, 0), expectedRollout(6, 100, 10, 0)];
    assert.deepEqual(rows(buffer), expected);
    console.log(JSON.stringify({ fixture: "rollout/checkpoint-and-clear", emitted: rows(buffer) }));
  } finally { resumed.close(); }
}));

test("unknown first lineage does not fabricate a cached zero", () => rolloutFixture(async (buffer, tailer, file) => {
  fs.writeFileSync(file, rolloutPrefix + settingsLine("priority") + countLine(100, undefined, 10));
  await tailer.scan({ scope: "full" });
  const expected = expectedRollout(0, 0, 0, undefined, "priority");
  Object.assign(expected.metadata, { counterLineage: "unknown_nonzero_first", sourceCumulativeInput: 100,
    sourceCumulativeOutput: 10, sourceCumulativeReasoningOutput: 0 });
  assert.deepEqual(rows(buffer), [expected]);
  console.log(JSON.stringify({ fixture: "rollout/unknown-first-lineage", emitted: rows(buffer)[0] }));
}));

test("legacy parser checkpoints and optional cached counters validate without guessing", () => {
  const state = { parserKind: "codex-rollout-v2", checkpointVersion: 2, previous: { input: 100, output: 10, reasoningOutput: 0 },
    tokenCountIndex: 1, contextOccurrenceIndex: -1, conversationId: session };
  assert.equal(validateRolloutParserState(state)?.previous.cachedInput, undefined);
  assert.equal(validateRolloutParserState({ ...state, serviceTier: "priority" })?.serviceTier, "priority");
  assert.equal(validateRolloutParserState({ ...state, serviceTier: "pro" }), undefined);
  assert.equal(validateRolloutParserState({ ...state, previous: { ...state.previous, cachedInput: -1 } }), undefined);
});

test("a decreasing cached cumulative counter remains unknown", () => rolloutFixture(async (buffer, tailer, file) => {
  fs.writeFileSync(file, rolloutPrefix + settingsLine("priority") + countLine(0, 0, 0) + countLine(100, 7, 10) + countLine(200, 5, 20));
  await tailer.scan({ scope: "full" });
  assert.deepEqual(rows(buffer), [expectedRollout(1, 100, 10, 7, "priority"), expectedRollout(2, 100, 10, undefined, "priority")]);
  console.log(JSON.stringify({ fixture: "rollout/decreasing-cached-counter", emitted: rows(buffer) }));
}));

// Commit A must accept precisely what the later writer can freeze. Exercise
// every bounded raw/canonical pair, including raw fast/default spellings.
test("reader allowlist preserves all tier spellings exactly on reseal", () => {
  for (const service_tier of ["fast", "default", "standard", "priority", "flex", "batch"]) {
    for (const serviceTier of ["standard", "priority", "flex", "batch"]) {
      const sealed = sealOutboundEnvelope({ event: { ...baseEvent, id: session,
        metadata: { serviceTier, service_tier } }, suppressedFields: [] });
      assert.equal(sealed.ok, true);
      if (!sealed.ok) throw new Error("tier metadata failed sealing");
      assert.deepEqual(sealed.envelope.event.metadata, { serviceTier, service_tier });
      assert.deepEqual(sealOutboundEnvelope(sealed.envelope), sealed);
    }
  }
});
