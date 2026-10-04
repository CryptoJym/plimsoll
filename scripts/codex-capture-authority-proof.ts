import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { recordCodexTurnModel, rememberCaptureGap } from "../packages/collector-cli/src/codex-model-capture";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot, withReader } from "./lib/legacy-reader";

const completion = createProofCompletion("codex-capture-authority", 14);
const root = proofTempRoot("capture-authority");
const AT = Date.now() - 300_000;
const SESSION = "22222222-2222-4222-8222-222222222222";
const WORKSPACE = "11111111-1111-4111-8111-111111111111";
const MODEL = "gpt-6.1-sol";
const attr = (key: string, value: string | number) => ({key, value: typeof value === "number"
  ? {intValue: String(value)} : {stringValue: value}});
const nano = (at: number) => String(BigInt(at) * 1_000_000n);
let sequence = 0;
class Fixture {
  now = new Date(AT + 2000);
  dir = path.join(root, String(++sequence));
  file = path.join(this.dir, "ledger.sqlite");
  options = {workspaceId: WORKSPACE, deviceId: "authority-device",
    enrollmentNow: () => new Date(AT - 1_800_000), delivery: {enabled: true, now: () => this.now}};
  buffer: LocalEventBuffer;
  constructor() {fs.mkdirSync(this.dir); this.buffer = this.open();}
  open() {return new LocalEventBuffer(this.file, this.options);}
  lease(offset: number) {this.now = new Date(AT + offset); return this.buffer.delivery.lease({now: this.now});}
  reopen() {this.buffer.close(); this.buffer = this.open();}
  close() {this.buffer.close();}
}
function log(trace: string, model?: string, input?: number, output?: number, extra: ReturnType<typeof attr>[] = [], resourceModel = false) {
  return explodeOtlpPayload({resourceLogs: [{resource: {attributes: [attr("service.name", "codex-app-server"),
    ...(resourceModel ? [attr("model", MODEL)] : [])]}, scopeLogs: [{logRecords: [{
    timeUnixNano: nano(AT + 1000), traceId: trace,
    attributes: [attr("event.name", "codex.sse_event"), attr("conversation.id", SESSION),
      ...(model ? [attr("model", model)] : []), ...(input === undefined ? [] : [attr("input_token_count", input)]),
      ...(output === undefined ? [] : [attr("output_token_count", output)]), ...extra],
  }]}]}]}, {source: "codex", transportPath: "/v1/logs"}).events[0]!.event;
}
function span(trace: string) {
  return explodeOtlpPayload({resourceSpans: [{resource: {attributes: [attr("service.name", "codex-app-server")]},
    scopeSpans: [{spans: [{name: "handle_responses", traceId: trace, spanId: (++sequence).toString(16).padStart(16, "0"),
      startTimeUnixNano: nano(AT), endTimeUnixNano: nano(AT + 1000),
      attributes: [attr("gen_ai.usage.input_tokens", 19), attr("gen_ai.usage.output_tokens", 2)],
    }]}]}]}, {source: "codex", transportPath: "/v1/traces"}).events[0]!.event;
}
function nativeFile(dir: string, turn = "native-turn") {
  const sessions = path.join(dir, "sessions");
  const dated = path.join(sessions, ...new Date(AT).toISOString().slice(0, 10).split("-"));
  fs.mkdirSync(dated, {recursive: true});
  const row = (offset: number, type: string, payload: unknown) => ({timestamp: new Date(AT + offset).toISOString(), type, payload});
  const usage = (input: number, output: number) => ({type: "token_count", info: {
    total_token_usage: {input_tokens: input, output_tokens: output, cached_input_tokens: 0}}});
  fs.writeFileSync(path.join(dated, `rollout-fixture-${SESSION}.jsonl`), [
    row(10000, "session_meta", {id: SESSION}), row(10000, "turn_context", {turn_id: turn, model: MODEL}),
    row(10000, "event_msg", usage(0, 0)), row(11000, "event_msg", usage(19, 2)),
  ].map(e => JSON.stringify(e)).join("\n") + "\n");
  return sessions;
}
function usage(items: any[]) {return items.filter(i => i.envelope.event.metadata.usageSource !== "capture_gap" &&
  [i.envelope.event.inputTokens, i.envelope.event.outputTokens].some(v => v !== undefined));}
function assertGap(event: AiInteractionEvent) {
  assert.equal(event.metadata.usageSource, "capture_gap");
  assert.equal(event.model, undefined); assert.equal(event.inputTokens, undefined); assert.equal(event.outputTokens, undefined);
}
function drain(f: Fixture) {for (let i=0; i<40; i++) {f.buffer.projection.runMaintenance(f.now);
  const s = f.buffer.projection.status(); if (s.parityReady && !s.dirty) return;}
  assert.fail("bounded projection drain did not converge");}
async function authorityCase(name: string, peer: AiInteractionEvent, authoritative: boolean, diagnosticInput?: number) {
  const f = new Fixture(); let tailer: RolloutTailer | undefined;
  try {
    assert.equal(f.buffer.append(peer), true);
    assert.equal(f.buffer.sessionUsageAuthority("codex", SESSION), authoritative ? "live" : null);
    // A lookup must not record a financial gap before the hold/decision.
    const diagnostic = f.buffer.database.prepare("select 1 from sqlite_master where name='codex_model_capture_gaps'").get();
    assert.equal(Boolean(diagnostic), false);
    const first = f.lease(63000); const item = first.items.find(i => i.rawId === peer.id)!; assert.ok(item);
    if (!authoritative) assertGap(item.envelope.event);
    else assert.equal(item.envelope.event.model, MODEL);
    if (diagnosticInput !== undefined) {
      const stored = f.buffer.database.prepare("select base_envelope_json as payload from upload_outbox where raw_id=?")
        .get(peer.id) as {payload: string};
      assert.equal(JSON.parse(stored.payload).event.metadata.modelGapInputTokens, diagnosticInput);
    }
    f.buffer.delivery.acknowledge(first.leaseId, first.items.map(i => i.deliveryId), f.now);
    f.reopen(); assert.equal(f.buffer.sessionUsageAuthority("codex", SESSION), authoritative ? "live" : null);
    tailer = new RolloutTailer(f.buffer, nativeFile(f.dir), () => []);
    f.now = new Date(AT + 65000); const scan = await tailer.scan({scope: "full", now: f.now});
    tailer.close(); tailer = undefined;
    assert.equal(scan.sessionsSkippedOtlpCovered, authoritative ? 1 : 0);
    const later = f.lease(126000); const counted = usage(later.items);
    assert.equal(counted.length, authoritative ? 0 : 1);
    if (!authoritative) {assert.equal(counted[0].envelope.event.model, MODEL);
      assert.equal(counted[0].envelope.event.inputTokens, 19); assert.equal(counted[0].envelope.event.outputTokens, 2);}
    drain(f);
    const facts = f.buffer.database.prepare(`select e.id,f.model,f.input_tokens as input,f.output_tokens as output
      from buffered_events e join dashboard_event_facts f on f.raw_rowid=e.rowid`).all() as any[];
    const invalid = facts.find(r => r.id === peer.id);
    assert.ok(invalid);
    if (!authoritative) {assert.equal(invalid.model, null); assert.equal(invalid.input, null); assert.equal(invalid.output, null);
      assert.equal(facts.filter(r => r.model === MODEL && r.input === 19 && r.output === 2).length, 1);}
    console.log(JSON.stringify({case: name, passed: true, authority: authoritative ? "live" : "tailer",
      sessionsSkippedOtlpCovered: scan.sessionsSkippedOtlpCovered, nativeNamedDeliveries: counted.length,
      projectionFacts: facts})); completion.check(name);
  } finally {tailer?.close(); f.close();}
}
async function traceFactCase(kind: "model" | "account" | "session") {
  const f = new Fixture();
  try {
    const trace = String(sequence).padStart(32, "a");
    const clean = log(trace, MODEL);
    const other = log(trace, kind === "model" ? "gpt-6-astra" : MODEL, 17, 3);
    if (kind === "account") {clean.actorId = "sha256:aaaaaaaaaaaaaaaa"; other.actorId = "sha256:bbbbbbbbbbbbbbbb";}
    if (kind === "session") other.sessionId = "33333333-3333-4333-8333-333333333333";
    f.buffer.append(clean); f.buffer.append(other);
    const raw = () => (f.buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(other.id) as {payload: string}).payload;
    const before = raw(); const first = f.lease(63000); const peer = first.items.find(i => i.rawId === other.id)!;
    assert.ok(peer); assertGap(peer.envelope.event);
    f.buffer.delivery.acknowledge(first.leaseId, first.items.map(i => i.deliveryId), f.now);
    f.reopen(); const target = span(trace); f.buffer.append(target);
    const next = f.lease(124000); const result = next.items.find(i => i.rawId === target.id)!; assert.ok(result);
    assertGap(result.envelope.event); assert.equal(raw(), before);
    console.log(JSON.stringify({case: `gap-retains-clean-native-${kind}-conflict`, passed: true,
      target: result.envelope.event, rawPeerUnchanged: true})); completion.check(`gap retains native ${kind}`);
  } finally {f.close();}
}
async function localFactCase() {
  const f = new Fixture();
  try {
    const turn = "turn-1"; recordCodexTurnModel(f.buffer.database, SESSION, turn, MODEL);
    const old = aiInteractionEventSchema.parse({id: "00000000-0000-4000-8000-000000000701", source: "codex",
      dataMode: "metadata", observedAt: new Date(AT).toISOString(), eventType: "usage_rollout", sessionId: SESSION,
      model: "gpt-6-astra", inputTokens: 17, outputTokens: 3,
      metadata: {usageSource: "rollout", codexTurnId: turn, model: "gpt-6-astra"}});
    f.buffer.append(old); rememberCaptureGap(f.buffer.database, old.id, "old-native-gap");
    const target = aiInteractionEventSchema.parse({...old, id: "00000000-0000-4000-8000-000000000702", model: MODEL,
      inputTokens: 19, outputTokens: 2, metadata: {usageSource: "rollout", codexTurnId: turn}});
    f.buffer.append(target); const next = f.lease(63000); const item = next.items.find(i=>i.rawId===target.id)!;
    assert.ok(item); assertGap(item.envelope.event);
    completion.check("gap retains local-turn contradiction");
  } finally {f.close();}
}
async function duplicateNativeFact() {
  const f = new Fixture();
  try {
    const trace = "9".repeat(32), peer = span(trace);
    peer.model = MODEL; peer.metadata.model = MODEL;
    f.buffer.append(peer);
    f.buffer.database.prepare(`update buffered_events set usage_duplicate_reason='codex_sse_event_span',
      input_tokens=null,output_tokens=null,event_type='otel_span' where id=?`).run(peer.id);
    f.buffer.append(log(trace, "gpt-6-astra")); const target = span(trace); f.buffer.append(target);
    const item = f.lease(63000).items.find(i=>i.rawId===target.id)!; assert.ok(item); assertGap(item.envelope.event);
    completion.check("financial duplicate retains its contradictory native model fact");
  } finally {f.close();}
}
async function foreignBoundaryAuthority() {
  const f = new Fixture();
  try {
    const peer = log("8".repeat(32), MODEL, 17, 3); f.buffer.append(peer);
    assert.equal(f.buffer.sessionUsageAuthority("codex",SESSION),"live");
    f.buffer.database.prepare("update buffered_events set device_id='other-device' where id=?").run(peer.id);
    assert.equal(f.buffer.sessionUsageAuthority("codex",SESSION),null);
    completion.check("a different device cannot own the current session authority");
  } finally {f.close();}
}
async function distantTraceConflict() {
  const f = new Fixture();
  try {
    const trace = "7".repeat(32), distant = log(trace,"gpt-6-astra");
    distant.observedAt = new Date(AT - 1_200_000).toISOString();
    assert.equal(f.buffer.append(distant),true); assert.equal(f.buffer.append(log(trace,MODEL)),true); const target = span(trace); f.buffer.append(target);
    const item = f.lease(63000).items.find(i=>i.rawId===target.id)!; assert.ok(item); assertGap(item.envelope.event);
    completion.check("a farther native model on the same trace cannot be hidden by proximity");
  } finally {f.close();}
}
async function historicalUpgrade() {
  await withReader("a60590559403cace3db7cbbda49812c9e3dbfe62", async ({Buffer: Old, Tailer}) => {
    const f = new Fixture(); f.buffer.close();
    // The historical reader creates the ledger itself, not a current schema.
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(f.file + suffix, {force: true});
    let old: any; let tailer: any;
    try {
      old = new Old(f.file, f.options);
      const trace = "d".repeat(32); old.append(log(trace, MODEL)); const response = span(trace); old.append(response);
      tailer = new Tailer(old, nativeFile(f.dir), () => []);
      const scan = await tailer.scan({scope: "full", now: f.now}); tailer.close(); tailer = undefined;
      assert.equal(scan.eventsAppended, 1);
      const raw = old.database.prepare("select payload_json as payload from buffered_events where event_type='usage_rollout'").get();
      const native = JSON.parse(raw.payload); assert.equal(native.metadata.codexTurnId, undefined);
      assert.equal(typeof native.metadata.turnIndex, "number");
      assert.equal(old.database.prepare("select count(*) as n from buffered_events where input_tokens=19 and output_tokens=2").get().n, 2);
      old.close(); old = undefined; f.buffer = f.open(); const lease = f.lease(63000);
      const counted = usage(lease.items); assert.equal(counted.length, 1); assert.equal(counted[0].rawId, response.id);
      assert.equal(counted[0].envelope.event.model, MODEL); assert.equal(counted[0].envelope.event.inputTokens, 19);
      const gap = lease.items.find(i => i.rawId === native.id)!; assert.ok(gap); assertGap(gap.envelope.event);
      const table = f.buffer.database.prepare("select 1 from sqlite_master where name='codex_span_rollout_pairs'").get();
      assert.ok(!table || (f.buffer.database.prepare("select count(*) as n from codex_span_rollout_pairs").get() as {n:number}).n === 0);
      console.log(JSON.stringify({case: "actual-047-existing-twins-public-upgrade", passed: true, nativeMetadata: native.metadata,
        namedSpan: response.id, tokenlessRollout: native.id, pairs: 0}));
      completion.check("actual .47 native producer upgrade; no direct helper call");
    } finally {tailer?.close(); old?.close(); f.close();}
  });
}
async function historicalProjectionRepair() {
  const f = new Fixture();
  try {
    const bad = log("f".repeat(32), MODEL, 17, 3, [attr("gen_ai.request.model", "gpt-6-astra")]);
    f.buffer.append(bad); f.lease(63000); drain(f);
    // Simulate the old derived fact only; preserve raw and frozen bytes.
    f.buffer.database.prepare(`update dashboard_event_facts set model=?,input_tokens=17,output_tokens=3
      where raw_rowid=(select rowid from buffered_events where id=?)`).run(MODEL,bad.id);
    f.buffer.database.prepare("update codex_duplicate_fact_scan set authority_version=0,complete=1").run();
    f.reopen(); assert.equal(f.buffer.projection.status().parityReady, false); drain(f);
    const fact = f.buffer.database.prepare(`select model,input_tokens as input,output_tokens as output from dashboard_event_facts
      where raw_rowid=(select rowid from buffered_events where id=?)`).get(bad.id) as any;
    assert.deepEqual(fact, {model: null,input: null,output: null});
    completion.check("historical derived gap usage repaired through bounded public maintenance");
  } finally {f.close();}
}
async function main() {
  try {
    await authorityCase("invalid SSE gap ACK cannot suppress known native turn", log("a".repeat(32), MODEL, 17, 3,
      [attr("gen_ai.request.model", "gpt-6-astra")]), false, 17);
    await authorityCase("valid SSE keeps session deduplication", log("b".repeat(32), MODEL, 17, 3), true);
    await authorityCase("explicit zero SSE is known zero", log("c".repeat(32), MODEL, 0, 0), true);
    await authorityCase("resource model without request model grants no authority", log("d".repeat(32), undefined, 17, 3, [], true), false, 17);
    await authorityCase("partial counters with unknown model stay diagnostic", log("e".repeat(32), undefined, 17), false, 17);
    for (const kind of ["model", "account", "session"] as const) await traceFactCase(kind);
    await localFactCase(); await duplicateNativeFact(); await foreignBoundaryAuthority(); await distantTraceConflict(); await historicalUpgrade(); await historicalProjectionRepair();
    completion.complete();
  } finally {fs.rmSync(root, {recursive: true,force: true});}
}
main().catch(error => {console.error(error); process.exitCode = 1;});
