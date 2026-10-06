import assert from 'node:assert/strict';
import { createProofCompletion } from './lib/proof-completion';
import { withReader } from './lib/legacy-reader';
import fs from 'node:fs';import os from 'node:os';import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { recordCodexTurnModel } from '../packages/collector-cli/src/codex-model-capture';
import { explodeOtlpPayload } from '../packages/collector-cli/src/otlp';
import { aiInteractionEventSchema } from '../packages/shared/src/index';
const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-r5-new-'));
process.on('exit', () => { fs.rmSync(root, { recursive: true, force: true }); });
const at=Date.now()-300000,session='22222222-2222-4222-8222-222222222222';
const attr=(key:string,value:string|number)=>({key,value:typeof value==='number'?{intValue:String(value)}:{stringValue:value}});
const resource={attributes:[attr('service.name','codex-app-server')]};
const opts=(now:()=>Date)=>({workspaceId:'11111111-1111-4111-8111-111111111111',deviceId:'review-device',enrollmentNow:()=>new Date(at-1000),delivery:{enabled:true,now}});
const named=(id:string,trace:string)=>aiInteractionEventSchema.parse({id,sessionId:session,source:'codex',dataMode:'metadata',eventType:'assistant_response',observedAt:new Date(at).toISOString(),model:'gpt-6.1-sol',inputTokens:19,outputTokens:2,metadata:{traceId:trace,otelEventName:'handle_responses','gen_ai.request.model':'gpt-6.1-sol'}});
async function main() {
  const completion = createProofCompletion("codex-capture-review-r5-directed", 4);
  const outcomes: any[] = [];
  async function run(name: string, action: () => Promise<any> | any) {
    try {
      const result = await action();
      assert.equal(result.passed, true, name + " must pass");
      outcomes.push({ name, ...result });
      completion.check(name);
    } catch (error) {
      outcomes.push({ name, passed: false, error: String(error) });
      completion.check(name, false);
    }
  }
  await run("two-native-model-peer-remains-ambiguous-after-its-gap-is-acked", () => {
    let now = new Date(at + 2000);
    const b = new LocalEventBuffer(path.join(root, "ambiguous.sqlite"), opts(() => now));
    try {
      const trace = "a".repeat(32);
      const bad = explodeOtlpPayload({ resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{
        timeUnixNano: String(BigInt(at) * 1000000n), traceId: trace,
        attributes: [attr("event.name", "codex.sse_event"), attr("conversation.id", session),
          attr("model", "gpt-6.1-sol"), attr("gen_ai.request.model", "gpt-6-astra"),
          attr("input_token_count", 17), attr("output_token_count", 3)],
      }] }] }] }, { source: "codex" }).events[0]!.event;
      assert.equal(bad.metadata.model, "gpt-6.1-sol");
      assert.equal(bad.metadata["gen_ai.request.model"], "gpt-6-astra");
      b.append(bad); now = new Date(at + 63000);
      const initial = b.delivery.lease({ now });
      const gap = initial.items.find((item) => item.rawId === bad.id)!;
      assert.ok(gap); assert.equal(gap.envelope.event.metadata.usageSource, "capture_gap");
      b.delivery.acknowledge(initial.leaseId, [gap.deliveryId], now);
      const rawBefore = (b.database.prepare("select payload_json as p from buffered_events where id=?").get(bad.id) as any).p;
      const clean = explodeOtlpPayload({ resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{
        timeUnixNano: String(BigInt(at + 1000) * 1000000n), traceId: trace,
        attributes: [attr("event.name", "codex.sse_event"), attr("conversation.id", session), attr("model", "gpt-6.1-sol")],
      }] }] }] }, { source: "codex" }).events[0]!.event;
      const target = explodeOtlpPayload({ resourceSpans: [{ resource, scopeSpans: [{ spans: [{
        name: "handle_responses", traceId: trace, spanId: "1".repeat(16),
        startTimeUnixNano: String(BigInt(at) * 1000000n), endTimeUnixNano: String(BigInt(at + 1000) * 1000000n),
        attributes: [attr("conversation.id", session), attr("gen_ai.usage.input_tokens", 19),
          attr("gen_ai.usage.output_tokens", 2)],
      }] }] }] }, { source: "codex" }).events[0]!.event;
      b.append(clean); b.append(target); now = new Date(at + 124000);
      const output = b.delivery.lease({ now }).items.find((item) => item.rawId === target.id)!;
      assert.ok(output);
      const rawAfter = (b.database.prepare("select payload_json as p from buffered_events where id=?").get(bad.id) as any).p;
      assert.equal(rawAfter, rawBefore);
      return { passed: output.envelope.event.metadata.usageSource === "capture_gap" &&
        output.envelope.event.inputTokens === undefined, rawPeerUnchanged: true };
    } finally { b.close(); }
  });
  await run("native-turn-context-survives-reopen-before-first-token-row", () => {
    let now = new Date(at + 2000); const file = path.join(root, "turn.sqlite");
    let b = new LocalEventBuffer(file, opts(() => now));
    try {
      recordCodexTurnModel(b.database, session, "context-before-reopen", "gpt-6.1-sol");
      b.close(); b = new LocalEventBuffer(file, opts(() => now));
      const target = aiInteractionEventSchema.parse({ id: "00000000-0000-4000-8000-000000000852",
        sessionId: session, source: "codex", dataMode: "metadata", eventType: "usage_rollout",
        observedAt: new Date(at).toISOString(), inputTokens: 19, outputTokens: 2,
        metadata: { usageSource: "rollout", codexTurnId: "context-before-reopen" } });
      b.append(target); now = new Date(at + 63000);
      const first = b.delivery.lease({ now }); const wire = first.items.find((item) => item.rawId === target.id)!;
      assert.ok(wire); assert.equal(wire.envelope.event.model, "gpt-6.1-sol");
      const ack = b.delivery.acknowledge(first.leaseId, [wire.deliveryId], now);
      assert.equal(ack.acknowledged, 1); b.close(); b = new LocalEventBuffer(file, opts(() => now));
      assert.equal(b.delivery.lease({ now: new Date(at + 184000) }).items.some((item) => item.rawId === target.id), false);
      return { passed: true, deliveryId: wire.deliveryId, acknowledged: ack.acknowledged };
    } finally { b.close(); }
  });
  await run("expired-frozen-native-delivery-remote-replay-restamp", () => {
    let now = new Date(at + 2000); const b = new LocalEventBuffer(path.join(root, "restamp.sqlite"), opts(() => now));
    try {
      const target = named("00000000-0000-4000-8000-000000000853", "b".repeat(32)); b.append(target);
      now = new Date(at + 63000); const first = b.delivery.lease({ now });
      const frozen = first.items.find((item) => item.rawId === target.id)!; assert.ok(frozen);
      now = new Date(at + 184000); const expired = b.delivery.lease({ now });
      const retried = expired.items.find((item) => item.rawId === target.id)!; assert.ok(retried);
      assert.equal(retried.envelopeJson, frozen.envelopeJson);
      assert.equal(b.delivery.deadLetterRemote(expired.leaseId, [retried.deliveryId], now), 1);
      assert.equal(b.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now }).requeued, 1);
      const raw = JSON.parse((b.database.prepare("select payload_json as p from buffered_events where id=?").get(target.id) as any).p);
      const restamped = b.delivery.restampUnsentRaw(target.id, JSON.stringify({ ...raw,
        metadata: { ...raw.metadata, workItemId: "44444444-4444-4444-8444-444444444444" } }));
      now = new Date(at + 245000); const after = b.delivery.lease({ now }).items.find((item) => item.rawId === target.id)!;
      assert.ok(after); assert.equal(after.deliveryId, frozen.deliveryId);
      return { passed: !restamped && after.envelopeJson === frozen.envelopeJson, restamped, frozenBytesSame: true };
    } finally { b.close(); }
  });
  await run("native-named-usage-upgrades-from-real-0.7.47", async () => {
    let now = new Date(at + 2000); const file = path.join(root, "v0747.sqlite");
    let frozen: any;
    let current: InstanceType<typeof LocalEventBuffer> | undefined;
    try {
      await withReader("a60590559403cace3db7cbbda49812c9e3dbfe62", async ({ Buffer: Old047 }) => {
        const old = new Old047(file, opts(() => now));
        try {
          const target = named("00000000-0000-4000-8000-000000000854", "c".repeat(32)); old.append(target);
          now = new Date(at + 63000); const first = old.delivery.lease({ now });
          frozen = first.items.find((item: any) => item.rawId === target.id); assert.ok(frozen);
        } finally { old.close(); }
      });
      now = new Date(at + 184000); current = new LocalEventBuffer(file, opts(() => now));
      const currentLease = current.delivery.lease({ now }); const next = currentLease.items.find((item) => item.rawId === frozen.envelope.event.id);
      assert.ok(next); assert.equal(currentLease.locallyDead, 0); assert.equal(next.deliveryId, frozen.deliveryId);
      assert.equal(next.envelopeJson, frozen.envelopeJson); assert.equal(next.envelope.event.model, "gpt-6.1-sol");
      return { passed: true, oldVersion: "0.7.47", sameId: true, frozenBytesSame: true };
    } finally { current?.close(); }
  });
  console.log(JSON.stringify({ case: "r5-new-code-directed-cases", outcomes }, null, 2));
  if (outcomes.some((outcome) => !outcome.passed)) { process.exitCode = 1; return; }
  completion.complete();
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
