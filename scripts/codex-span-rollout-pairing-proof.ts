import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { captureCodexModel, recordCodexTurnModel, rememberCaptureGap } from "../packages/collector-cli/src/codex-model-capture";
import { createProofCompletion } from "./lib/proof-completion";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";

const regression = process.argv.includes("--regression");
const production = process.argv.includes("--production");
const edges = process.argv.includes("--edges");
const completion = createProofCompletion("codex-span-rollout-pairing", production ? 2 : regression ? 1 : edges ? 9 : 20);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "span-rollout-"));
const AT = Date.now() - 300_000;
const SESSION = "22222222-2222-4222-8222-222222222222";
const WORKSPACE = "11111111-1111-4111-8111-111111111111";
const MODEL = "gpt-6.1-sol";
const INPUT = 40_000, OUTPUT = 148, CACHE = 37_600;
const attr = (key: string, value: string | number) => ({ key, value:
  typeof value === "string" ? { stringValue: value } : { intValue: String(value) } });
const nano = (ms: number) => String(BigInt(ms) * 1_000_000n);
const resource = { attributes: [attr("service.name", "codex-app-server")] };

class Fixture {
  now = new Date(AT + 2000);
  readonly file: string;
  readonly sessions: string;
  buffer: LocalEventBuffer;
  readonly cloud = new Map<string, AiInteractionEvent>();
  constructor(readonly name: string) {
    this.file = path.join(root, name + ".sqlite");
    this.sessions = path.join(root, name + "-sessions");
    this.buffer = this.open();
  }
  open() {
    return new LocalEventBuffer(this.file, { workspaceId: WORKSPACE, deviceId: "fixture-device",
      enrollmentNow: () => new Date(AT - 1000), delivery: { enabled: true, now: () => this.now } });
  }
  span(options: { session?: boolean; sessionId?: string; turnId?: string; model?: boolean;
    modelName?: string; actorId?: string; delta?: number; id?: string } = {}) {
    const entry = explodeOtlpPayload({ resourceSpans: [{ resource, scopeSpans: [{ spans: [{
      name: "handle_responses", traceId: "a".repeat(32), spanId: options.id ?? "b".repeat(16),
      startTimeUnixNano: nano(AT), endTimeUnixNano: nano(AT + 1000),
      attributes: [attr("gen_ai.usage.input_tokens", INPUT + (options.delta ?? 0)),
        attr("gen_ai.usage.output_tokens", OUTPUT), attr("gen_ai.usage.cache_read.input_tokens", CACHE),
        ...(options.session ? [attr("conversation.id", options.sessionId ?? SESSION), attr("turn.id", options.turnId ?? "turn-1")] : []),
        ...(options.model ? [attr("model", options.modelName ?? MODEL)] : [])],
    }] }] }] }, { source: "codex", transportPath: "/v1/traces" }).events[0]!;
    assert.equal(entry.event.metadata.transport_path, "/v1/traces");
    if (options.actorId) entry.event.actorId = options.actorId;
    assert.equal(this.buffer.append(entry.event, entry.suppressedFields), true);
    return entry.event;
  }
  context(model = MODEL) {
    const entry = explodeOtlpPayload({ resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{
      timeUnixNano: nano(AT + 1000), traceId: "a".repeat(32),
      attributes: [attr("event.name", "codex.sse_event"), attr("event.kind", "response.output_item.done"),
        attr("conversation.id", SESSION), attr("model", model)],
    }] }] }] }, { source: "codex", transportPath: "/v1/logs" }).events[0]!;
    assert.equal(this.buffer.append(entry.event, entry.suppressedFields), true);
  }
  async rollout(options: { firstUnknown?: boolean; delta?: number; cacheDelta?: number } = {}) {
    const usage = (input: number, output: number, cache: number) => ({ timestamp: new Date(AT + 1000).toISOString(),
      type: "event_msg", payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: input, output_tokens: output, cached_input_tokens: cache,
      } } } });
    const dir = path.join(this.sessions, ...new Date(AT).toISOString().slice(0, 10).split("-"));
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `rollout-fixture-${SESSION}.jsonl`), [
      { timestamp: new Date(AT).toISOString(), type: "session_meta", payload: { id: SESSION } },
      { timestamp: new Date(AT).toISOString(), type: "turn_context", payload: { turn_id: "turn-1", model: MODEL } },
      ...(options.firstUnknown ? [] : [usage(0, 0, 0)]),
      usage(INPUT + (options.delta ?? 0), OUTPUT, CACHE + (options.cacheDelta ?? 0)),
    ].map(row => JSON.stringify(row)).join("\n") + "\n");
    const tailer = new RolloutTailer(this.buffer, this.sessions, () => []);
    try { return await tailer.scan({ scope: "full", now: this.now }); }
    finally { tailer.close(); }
  }
  nativeRollout(actorId?: string) {
    recordCodexTurnModel(this.buffer.database, SESSION, "turn-1", MODEL, actorId);
    const event = aiInteractionEventSchema.parse({
      id: "44444444-4444-4444-8444-444444444444", source: "codex", dataMode: "metadata",
      eventType: "usage_rollout", observedAt: new Date(AT + 1000).toISOString(),
      sessionId: SESSION, model: MODEL, inputTokens: INPUT, outputTokens: OUTPUT,
      cacheReadTokens: CACHE, ...(actorId ? { actorId } : {}),
      metadata: { usageSource: "rollout", codexTurnId: "turn-1" },
    });
    assert.equal(this.buffer.append(event), true);
    return event;
  }
  upload(at = AT + 65_000) {
    this.now = new Date(at);
    const lease = this.buffer.delivery.lease({ now: this.now });
    for (const item of lease.items) this.cloud.set(item.deliveryId, item.envelope.event);
    if (lease.items.length) this.buffer.delivery.acknowledge(lease.leaseId,
      lease.items.map(item => item.deliveryId), this.now);
    return lease;
  }
  usage() {
    return [...this.cloud.values()].filter(event => (event.inputTokens ?? 0) + (event.outputTokens ?? 0) > 0);
  }
  once() {
    const usage = this.usage();
    console.log(JSON.stringify({ case: this.name, usage: usage.map(e => ({ id: e.id,
      kind: e.eventType, model: e.model, input: e.inputTokens, output: e.outputTokens })) }));
    assert.equal(usage.length, 1, "one response counts once");
    assert.equal(usage[0]!.model, MODEL, "counted response has the known native model");
    assert.equal(usage[0]!.inputTokens, INPUT);
    assert.equal(usage[0]!.outputTokens, OUTPUT);
    assert.equal(usage[0]!.cacheReadTokens, CACHE);
  }
  close() { this.buffer.close(); }
}

function assertUnpaired(f: Fixture, spanId: string) {
  const exists = f.buffer.database.prepare("select 1 from sqlite_master where type='table' and name='codex_span_rollout_pairs'").get();
  assert.equal(exists ? f.buffer.database.prepare("select 1 from codex_span_rollout_pairs where span_id=?").get(spanId) : undefined,
    undefined, "ineligible span has no saved pairing decision");
  const raw = f.buffer.database.prepare("select usage_duplicate_reason as reason from buffered_events where id=?")
    .get(spanId) as { reason: string | null };
  assert.equal(raw.reason, null, "ineligible span is not erased as a duplicate");
}

async function durableGapComposition() {
  for (const state of ["sealed gap", "terminal gap replay", "saved pair then gap", "frozen pair then correction"] as const) {
    const f = new Fixture(state);
    try {
      const span = f.span({ model: state === "saved pair then gap" || state === "frozen pair then correction" });
      if (state === "saved pair then gap") {
        // Gap priority applies before capture. Establish the pair without a
        // named lease, then record its gap and keep the original assertions.
        // The old setup first froze named usage, contradicting finality (I2).
        await f.rollout();
        assert.ok(f.buffer.database.prepare("select 1 from codex_span_rollout_pairs where span_id=?").get(span.id));
        rememberCaptureGap(f.buffer.database, span.id, "late_native_conflict");
        const result = captureCodexModel(f.buffer.database, span, span.id);
        assert.equal(result.metadata.usageSource, "capture_gap", "durable gap precedes an existing pair");
        assert.equal(result.model, undefined);
        assert.equal(result.inputTokens, undefined);
        f.upload(AT + 400_000); f.once();
        assert.equal(f.cloud.has(span.id), false, "paired gap does not create another billable ID");
        console.log(JSON.stringify({ composition: state, status: "PASS" }));
        continue;
      }
      f.now = new Date(AT + 65_000);
      const first = f.buffer.delivery.lease({ now: f.now });
      const item = first.items.find(value => value.rawId === span.id)!;
      assert.ok(item);
      if (state === "frozen pair then correction") {
        assert.equal(item.envelope.event.model, MODEL);
        await f.rollout();
        rememberCaptureGap(f.buffer.database, span.id, "late_native_conflict");
        const result = captureCodexModel(f.buffer.database, span, span.id);
        assert.equal(result.model, MODEL, "a later correction cannot withdraw a captured native result");
        assert.equal(result.inputTokens, INPUT);
        assert.equal(result.outputTokens, OUTPUT);
        f.now = new Date(AT + 400_000);
        const retry = f.buffer.delivery.lease({ now: f.now });
        assert.equal(retry.locallyDead, 0);
        const original = retry.items.find(value => value.rawId === span.id)!;
        assert.ok(original);
        assert.equal(original.deliveryId, item.deliveryId);
        assert.equal(original.envelopeJson, item.envelopeJson);
        assert.equal(retry.items.filter(value => value.envelope.event.inputTokens !== undefined).length, 1);
      } else {
        assert.equal(item.envelope.event.metadata.usageSource, "capture_gap");
        if (state === "terminal gap replay") {
          assert.equal(f.buffer.delivery.deadLetterRemote(first.leaseId, [item.deliveryId], f.now), 1);
        }
        // An older gap reader has frozen bytes but no decision-table receipt.
        f.buffer.database.prepare("delete from codex_capture_decisions where raw_id=?").run(span.id);
        await f.rollout();
        assertUnpaired(f, span.id);
        if (state === "terminal gap replay") {
          const replay = f.buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now: f.now });
          assert.equal(replay.requeued, 1);
          assert.equal(f.buffer.delivery.restampUnsentRaw(span.id, JSON.stringify(span)), false);
        }
        const frozen = f.buffer.database.prepare("select sealed_envelope_json as bytes from upload_outbox where delivery_id=?")
          .get(item.deliveryId) as { bytes: string };
        assert.equal(frozen.bytes, item.envelopeJson, "late twin keeps frozen gap bytes and identity");
        f.upload(AT + 400_000);
        f.once();
        const deliveredGap = f.cloud.get(item.deliveryId)!;
        assert.equal(deliveredGap.model, undefined);
        assert.equal(deliveredGap.inputTokens, undefined);
      }
      console.log(JSON.stringify({ composition: state, status: "PASS" }));
    } finally { f.close(); }
  }
}

async function otherBoundaryRefusals() {
  for (const boundary of ["workspace", "device", "turn", "model", "account", "nested model", "gap peer"] as const) {
    const f = new Fixture("refuse-" + boundary);
    try {
      const span = f.span({ model: true, session: boundary === "turn",
        ...(boundary === "turn" ? { turnId: "turn-2" } : {}),
        ...(boundary === "model" ? { modelName: "gpt-6-astra" } : {}),
        ...(boundary === "account" ? { actorId: "sha256:1111111111111111" } : {}) });
      // Imported historical rows can belong to a prior boundary. Seed that
      // stored identity directly; live binding APIs correctly refuse to relabel
      // queued rows, and are not the pairing operation being exercised here.
      if (boundary === "workspace") f.buffer.database.prepare("update buffered_events set workspace_id=? where id=?")
        .run("55555555-5555-4555-8555-555555555555", span.id);
      if (boundary === "device") f.buffer.database.prepare("update buffered_events set device_id=? where id=?")
        .run("other-device", span.id);
      if (boundary === "nested model" || boundary === "gap peer") {
        const peer: AiInteractionEvent = { ...span, id: "66666666-6666-4666-8666-666666666666",
          eventType: "otel_span", model: undefined, inputTokens: undefined, outputTokens: undefined,
          cacheReadTokens: undefined, metadata: { traceId: span.metadata.traceId,
            otelEventName: "codex.sse_event", otelAttributes: { "gen_ai.request.model": "gpt-6-astra" } } };
        if (boundary === "gap peer") peer.metadata.usageSource = "capture_gap";
        assert.equal(f.buffer.append(peer), true);
      }
      f.nativeRollout(boundary === "account" ? "sha256:2222222222222222" : undefined);
      assertUnpaired(f, span.id);
      console.log(JSON.stringify({ boundary, status: "PASS", paired: false }));
    } finally { f.close(); }
  }
}

async function check(name: string, body: (f: Fixture) => Promise<void> | void) {
  const f = new Fixture(name);
  try { await body(f); completion.check(name); }
  finally { f.close(); }
}
async function main() { try {
  if (production) {
    await check("pr450-production-span-and-rollout", async f => {
      const span = f.span(); assert.equal(span.model, undefined); assert.equal(span.sessionId, undefined);
      await f.rollout(); f.upload(); f.once();
      assert.ok([...f.cloud.values()].every(e => e.inputTokens === undefined || e.model));
    });
    await check("pr450-production-span-only", f => {
      f.span(); f.upload(); assert.equal(f.usage().length, 0);
      assert.equal([...f.cloud.values()][0]!.metadata.usageSource, "capture_gap");
    });
    completion.complete(); return;
  }
  if (!edges) { await check("span-then-rollout", async f => {
    f.context(); f.span(); await f.rollout(); f.upload(); f.once();
  });
  if (!regression) {
    await check("rollout-then-span", async f => {
      f.context(); await f.rollout(); f.span(); f.upload(); f.once();
    });
    await check("span-only-with-native-model", f => { f.span({ model: true }); f.upload(); f.once(); });
    await check("rollout-only", async f => { await f.rollout(); f.upload(); f.once(); });
    await check("production-unknown-span-and-rollout", async f => {
      const span = f.span(); assert.equal(span.model, undefined); assert.equal(span.sessionId, undefined);
      await f.rollout(); f.upload(); f.once();
      assert.ok([...f.cloud.values()].every(e => e.inputTokens === undefined || e.model));
    });
    await check("production-gap-rejects-late-rollout-twin", async f => {
      const span = f.span();
      const gap = captureCodexModel(f.buffer.database, span, span.id, true);
      assert.equal(gap.metadata.usageSource, "capture_gap");
      await f.rollout();
      assertUnpaired(f, span.id);
      f.upload();
      const delivered = [...f.cloud.values()];
      assert.equal(delivered.filter(event => event.metadata.usageSource === "capture_gap").length, 1);
      assert.equal(f.usage().length, 1, "late rollout remains the one native usage observation");
      await durableGapComposition();
    });
    await check("session-bearing-span-first", async f => {
      f.span({ session: true }); await f.rollout(); f.upload(); f.once();
    });
    await check("late-rollout-after-named-span-ack", async f => {
      f.span({ model: true }); f.upload(); f.once(); await f.rollout(); f.upload(AT + 130_000); f.once();
    });
    await check("late-span-after-rollout-ack", async f => {
      await f.rollout(); f.upload(); f.once(); f.span(); f.upload(AT + 130_000); f.once();
    });
    await check("unvalidated-first-is-not-a-twin", async f => {
      f.span({ model: true }); await f.rollout({ firstUnknown: true }); f.upload(); f.once();
    });
    await check("near-twin-is-not-collapsed", async f => {
      f.span({ model: true }); await f.rollout({ delta: 1 }); f.upload();
      assert.equal(f.usage().length, 2, "different marginal counters are not identity evidence");
    });
  } }
  if (!regression) {
    await check("in-flight-span-restart-keeps-frozen-bytes", async f => {
      f.context(); f.span(); f.now = new Date(AT + 65_000);
      const first = f.buffer.delivery.lease({ now: f.now });
      const usage = first.items.find(item => item.envelope.event.inputTokens === INPUT)!;
      assert.ok(usage); f.buffer.close(); f.buffer = f.open();
      await f.rollout();
      const frozen = f.buffer.database.prepare("select sealed_envelope_json as bytes from upload_outbox where delivery_id=?")
        .get(usage.deliveryId) as { bytes: string };
      assert.equal(frozen.bytes, usage.envelopeJson);
      f.upload(AT + 400_000); f.once();
    });
    await check("ambiguous-two-spans-one-rollout", async f => {
      f.span({ model: true }); f.span({ model: true, id: "c".repeat(16) });
      await f.rollout(); f.upload(); assert.equal(f.usage().length, 3);
    });
    await check("native-session-mismatch", async f => {
      f.span({ model: true, session: true, sessionId: "33333333-3333-4333-8333-333333333333" });
      await f.rollout(); f.upload(); assert.equal(f.usage().length, 2);
      await otherBoundaryRefusals();
    });
    await check("installation-epoch-mismatch", async f => {
      f.span({ model: true });
      f.buffer.useWorkspace(WORKSPACE, "fixture-device", "44444444-4444-4444-8444-444444444444");
      await f.rollout(); f.upload(); assert.equal(f.usage().length, 2);
    });
    await check("conflicting-native-turn-stays-gap", async f => {
      f.span({ model: true });
      recordCodexTurnModel(f.buffer.database, SESSION, "turn-1", "gpt-6-astra");
      await f.rollout(); f.upload(); f.once();
      assert.ok([...f.cloud.values()].some(event => event.metadata.usageSource === "capture_gap"));
    });
    await check("different-cache-counts-stay-unpaired", async f => {
      f.span({ model: true }); await f.rollout({ cacheDelta: 1 }); f.upload(); assert.equal(f.usage().length, 2);
    });
    await check("legacy-span-only-session-authority", async f => {
      f.span({ session: true });
      f.buffer.database.prepare("insert into session_usage_authority values ('codex',?,'live',?)")
        .run(SESSION, new Date(AT).toISOString());
      assert.equal(f.buffer.sessionUsageAuthority("codex", SESSION), null);
      const scan = await f.rollout(); assert.equal(scan.sessionsSkippedOtlpCovered, 0);
      f.upload(); f.once();
    });
    await check("native-trace-model-disagrees-with-rollout", async f => {
      f.context("gpt-6-astra"); f.span(); await f.rollout(); f.upload();
      assert.equal(f.usage().length, 2, "conflicting native trace cannot attest a rollout pair");
      assert.deepEqual(new Set(f.usage().map(event => event.model)), new Set([MODEL, "gpt-6-astra"]));
    });
    await check("local-projection-has-one-named-usage-fact", async f => {
      f.span({ session: true }); await f.rollout(); f.upload(); f.once();
      f.buffer.projection.runMaintenance(new Date());
      const facts = f.buffer.database.prepare(`select model,input_tokens as input,output_tokens as output,
        cache_read_tokens as cache from dashboard_event_facts where coalesce(input_tokens,0)+coalesce(output_tokens,0)>0`)
        .all() as Array<{ model: string; input: number; output: number; cache: number }>;
      assert.deepEqual(facts, [{ model: MODEL, input: INPUT, output: OUTPUT, cache: CACHE }]);
    });
  }
  completion.complete();
} finally { fs.rmSync(root, { recursive: true, force: true }); } }
main().catch(error => { console.error(error); process.exitCode = 1; });
