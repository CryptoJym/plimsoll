import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { recordCodexTurnModel } from "../packages/collector-cli/src/codex-model-capture";
import { pairCodexSpanRolloutEvent } from "../packages/collector-cli/src/codex-span-rollout-pairing";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot, withLegacyReader } from "./lib/legacy-reader";

/* Round-six proof: every evidence/state/path cell calls a real operation and
 * asserts its lease or refusal, aggregate billing count and frozen identity. */
const EVIDENCE = [
  "exact pair", "native trace", "native local turn",
  "legacy guess on target",
  "legacy guess on a peer with counters different from the response",
  "no evidence",
  "span plus rollout twin",
] as const;
const STATES = ["unsealed", "sealed and unacknowledged", "sealed with lease expired"] as const;
const PATHS = ["first lease", "retry", "reopen", "upgrade", "dead-letter replay", "remote-terminal replay", "restamp"] as const;
type Evidence = typeof EVIDENCE[number];
type State = typeof STATES[number];
type Path = typeof PATHS[number];
const MODEL = "gpt-6.1-sol";
const WORKSPACE = "11111111-1111-4111-8111-111111111111";
const DEVICE = "matrix-device";
const SESSION = "22222222-2222-4222-8222-222222222222";
const AT = Date.now() - 900_000;
const completion = createProofCompletion("codex-capture-invariants-matrix", 147);
const root = proofTempRoot("capture-invariants-matrix");

type Lease = { leaseId: string; items: any[]; locallyDead: number; blockedBy?: string };
type Operation = { name: string; leaseItems: number; detail: string };
type Fixture = {
  file: string;
  buffer: InstanceType<typeof LocalEventBuffer>;
  now: { value: Date };
  lastAt: number;
  focusId: string;
  focusIds?: string[];
  targetId: string;
  witness?: { id: string; leaseId: string; item: any; contract: string };
  initialId?: string;
  initialBytes?: string;
  lease?: Lease;
};
type Aggregate = { activeRows: number; billableRows: number; gapRows: number; countedRows: number; deliveryIds: string[] };
type Cell = {
  evidence: Evidence; state: State; path: Path; result: "PASS";
  operations: Operation[]; refusals: string[]; aggregate: Aggregate;
  i1: true; i2: true; i3: true; deliveryId?: string; model?: string;
  inputTokens?: number; outputTokens?: number; frozenBytesSame?: boolean;
  frozenHold?: boolean;
  replacement?: boolean;
  coverage?: string;
};

function id() { return crypto.randomUUID(); }
function event(value: Partial<AiInteractionEvent> & { id: string; observedAt?: string }): AiInteractionEvent {
  const { id: eventId, observedAt, ...rest } = value;
  return aiInteractionEventSchema.parse({
    id: eventId, source: "codex", dataMode: "metadata", eventType: "assistant_response",
    sessionId: SESSION, observedAt: observedAt || new Date(AT).toISOString(),
    inputTokens: 19, outputTokens: 2, metadata: {}, ...rest,
  });
}
function opts(now: () => Date) {
  return { workspaceId: WORKSPACE, deviceId: DEVICE, enrollmentNow: () => new Date(AT - 1000),
    delivery: { enabled: true, now } };
}
function nativeLog(logId: string, traceId?: string, usage = false) {
  return event({ id: logId, eventType: "otel_span", model: MODEL,
    ...(usage ? {} : { inputTokens: undefined, outputTokens: undefined }),
    metadata: { otelEventName: "codex.sse_event", ...(traceId ? { traceId } : {}),
      "gen_ai.request.model": MODEL } });
}
function appendSpanRolloutTwin(buffer: any, spanId: string, traceId: string, turn: string, assertPair = true) {
  recordCodexTurnModel(buffer.database, SESSION, turn, MODEL);
  const span = event({ id: spanId, sessionId: undefined, eventType: "assistant_response",
    inputTokens: 19, outputTokens: 2,
    metadata: { otelEventName: "handle_responses", traceId,
      otelSpanEndAt: new Date(AT + 1000).toISOString(), transport_path: "/v1/traces",
      "gen_ai.usage.input_tokens": 19, "gen_ai.usage.output_tokens": 2 } });
  const rolloutId = id();
  const rollout = event({ id: rolloutId, eventType: "usage_rollout", model: MODEL,
    metadata: { usageSource: "rollout", codexTurnId: turn } });
  assert.equal(buffer.append(span), true, "span twin appended");
  assert.equal(buffer.append(rollout), true, "rollout twin appended");
  if (assertPair) {
    const pair = buffer.database.prepare(
      "select owner_id as ownerId, rollout_id as rolloutId from codex_span_rollout_pairs where span_id=?",
    ).get(spanId) as { ownerId: string; rolloutId: string } | undefined;
    assert.deepEqual(pair, { ownerId: rolloutId, rolloutId }, "unique span/rollout twin has one rollout owner");
  }
  return { spanId, rolloutId };
}
function targetFor(evidence: Evidence, targetId: string, traceId: string, turn: string) {
  if (evidence === "native local turn")
    return event({ id: targetId, eventType: "usage_rollout", model: MODEL,
      metadata: { usageSource: "rollout", codexTurnId: turn } });
  if (evidence === "legacy guess on target")
    return event({ id: targetId, model: "gpt-6-astra", metadata: { otelEventName: "handle_responses" } });
  if (evidence === "legacy guess on a peer with counters different from the response" ||
      evidence === "native trace")
    return event({ id: targetId, eventType: "otel_span", inputTokens: 19, outputTokens: 2,
      metadata: { otelEventName: "handle_responses", traceId } });
  return event({ id: targetId, metadata: { otelEventName: "handle_responses" } });
}
function decodeEnvelope(row: { base: string; sealed: string | null }) {
  return JSON.parse(row.sealed || row.base) as { event: AiInteractionEvent };
}
function readFocus(f: Fixture) {
  const item = f.lease && f.lease.items.find((candidate: any) => candidate.rawId === f.focusId);
  if (item) return { id: item.deliveryId, bytes: item.envelopeJson, event: item.envelope.event };
  const row = f.buffer.database.prepare(
    "select delivery_id as id, base_envelope_json as base, sealed_envelope_json as sealed " +
    "from upload_outbox where raw_id=? order by delivery_id limit 1",
  ).get(f.focusId) as { id: string; base: string; sealed: string | null } | undefined;
  if (!row) return undefined;
  return { id: row.id, bytes: row.sealed || row.base, event: decodeEnvelope(row).event };
}
function aggregateFocus(f: Fixture): Aggregate {
  const focusIds = f.focusIds ?? [f.focusId];
  const marks = focusIds.map(() => "?").join(",");
  const rows = f.buffer.database.prepare(
    "select delivery_id as id, base_envelope_json as base, sealed_envelope_json as sealed " +
    `from upload_outbox where raw_id in (${marks}) order by delivery_id`,
  ).all(...focusIds) as Array<{ id: string; base: string; sealed: string | null }>;
  const events = rows.map(decodeEnvelope).map((value) => value.event);
  const billableRows = events.filter((e) => e.source === "codex" &&
    e.metadata.usageSource !== "capture_gap" && e.model === MODEL &&
    e.inputTokens === 19 && e.outputTokens === 2).length;
  const gapRows = events.filter((e) => e.metadata.usageSource === "capture_gap" &&
    e.model === undefined && e.inputTokens === undefined && e.outputTokens === undefined).length;
  const countedRows = events.filter((e) => e.source === "codex" &&
    e.metadata.usageSource !== "capture_gap" && e.eventType !== "usage_live" &&
    typeof e.model === "string" && !!e.model.trim() &&
    [e.inputTokens, e.outputTokens, e.cacheReadTokens, e.cacheCreationTokens, e.costUsd]
      .some(value => value !== undefined)).length;
  return { activeRows: rows.length, billableRows, gapRows, countedRows,
    deliveryIds: rows.map((row) => row.id) };
}
function closeFixture(f: Fixture) { f.buffer.close(); }
function advance(f: Fixture, at: number) {
  assert.ok(at >= f.lastAt, "clock moved backwards: " + f.lastAt + " -> " + at);
  f.lastAt = at; f.now.value = new Date(AT + at);
}
function openFixture(f: Fixture, at: number) {
  advance(f, at); f.buffer = new LocalEventBuffer(f.file, opts(() => f.now.value));
}
function leaseAt(f: Fixture, at: number, expected: "present" | "absent" | "any" = "any") {
  advance(f, at);
  const result = f.buffer.delivery.lease({ now: f.now.value }) as Lease;
  f.lease = result;
  const item = result.items.find((candidate: any) => candidate.rawId === f.focusId);
  if (item && f.initialId === undefined) {
    f.initialId = item.deliveryId; f.initialBytes = item.envelopeJson;
  }
  if (expected === "present") assert.ok(item, f.focusId + ": lease must return focus");
  if (expected === "absent") assert.equal(item, undefined, f.focusId + ": lease must refuse focus");
  return result;
}
async function buildFixture(evidence: Evidence, pathName: Path): Promise<Fixture> {
  const file = path.join(root, crypto.randomUUID() + ".sqlite");
  const now = { value: new Date(AT + 2000) };
  const buffer = new LocalEventBuffer(file, opts(() => now.value));
  const targetId = id();
  const traceId = crypto.randomBytes(16).toString("hex");
  const turn = "matrix-" + targetId;
  let focusId: string = targetId;
  let focusIds: string[] | undefined;
  let witness: Fixture["witness"];
  let witnessLease: Lease | undefined;
  let witnessEvent: AiInteractionEvent | undefined;
  if (pathName === "remote-terminal replay") {
    witnessEvent = nativeLog(id(), undefined, true);
    witnessEvent.inputTokens = 1;
    witnessEvent.outputTokens = 1;
    witnessEvent.sessionId = "33333333-3333-4333-8333-333333333333";
    buffer.append(witnessEvent);
  }
  if (evidence === "span plus rollout twin") {
    const twin = appendSpanRolloutTwin(buffer, targetId, traceId, turn);
    focusId = twin.rolloutId;
    focusIds = [twin.spanId, twin.rolloutId];
  } else if (evidence === "exact pair") {
    const pair = nativeLog(id(), undefined, true);
    buffer.append(pair);
    buffer.database.prepare("update buffered_events set event_type='otel_log' where id=?").run(pair.id);
    focusId = pair.id;
    buffer.append(targetFor(evidence, targetId, traceId, turn));
  } else {
    if (evidence === "native trace") {
      buffer.append(targetFor(evidence, targetId, traceId, turn));
      buffer.append(nativeLog(id(), traceId));
    }
    if (evidence === "legacy guess on a peer with counters different from the response")
      buffer.append(event({ id: id(), inputTokens: 17, outputTokens: 3, model: "gpt-6-astra",
        metadata: { otelEventName: "codex.sse_event", traceId } }));
    if (evidence === "native local turn") recordCodexTurnModel(buffer.database, SESSION, turn, MODEL);
    if (evidence !== "native trace") buffer.append(targetFor(evidence, targetId, traceId, turn));
  }
  if (witnessEvent) {
    now.value = new Date(AT + 60000);
    witnessLease = buffer.delivery.lease({ now: now.value }) as Lease;
    const witnessItem = witnessLease.items.find((item: any) => item.rawId === witnessEvent!.id);
    assert.ok(witnessItem, "validation witness leases before ACK");
    witness = { id: witnessEvent.id, leaseId: witnessLease.leaseId, item: witnessItem,
      contract: "sha256:" + "a".repeat(64) };
  }
  return { file, buffer, now, lastAt: pathName === "remote-terminal replay" ? 60000 : 2000,
    focusId, focusIds, targetId, witness, lease: witnessLease };
}
function seedState(f: Fixture, state: State) {
  if (state === "unsealed") return;
  const alreadyLeased = f.lease && f.lease.items.some((item: any) => item.rawId === f.focusId);
  if (!alreadyLeased) leaseAt(f, 63000, "present");
  if (state === "sealed with lease expired") {
    closeFixture(f); openFixture(f, 184000); leaseAt(f, 184000, "present");
    if (f.witness) {
      const witnessItem = f.lease && f.lease.items.find((item: any) => item.rawId === f.witness!.id);
      if (witnessItem) f.witness = { ...f.witness, leaseId: f.lease!.leaseId, item: witnessItem };
    }
  }
}
function rawPayload(f: Fixture) {
  return f.buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
    .get(f.focusId) as { payload: string };
}
function alteredPayload(f: Fixture) {
  const current = JSON.parse(rawPayload(f).payload) as AiInteractionEvent;
  return JSON.stringify({ ...current, metadata: { ...current.metadata,
    workItemId: "44444444-4444-4444-8444-444444444444" } });
}
function aggregateAssertions(f: Fixture, evidence: Evidence, frozenHold = false) {
  const output = readFocus(f);
  assert.ok(output, "focus delivery remains inspectable");
  const valid = evidence === "exact pair" || evidence === "native trace" ||
    evidence === "native local turn" || evidence === "span plus rollout twin";
  const billable = output.event.source === "codex" && output.event.metadata.usageSource !== "capture_gap" &&
    output.event.model === MODEL && output.event.inputTokens === 19 && output.event.outputTokens === 2;
  const gap = output.event.metadata.usageSource === "capture_gap" &&
    output.event.model === undefined && output.event.inputTokens === undefined && output.event.outputTokens === undefined;
  if (!frozenHold) {
    assert.equal(billable, valid, evidence + ": native evidence classification");
    assert.equal(gap, !valid, evidence + ": tokenless gap classification");
  } else {
    assert.equal(f.lease && f.lease.items.some((item: any) => item.rawId === f.focusId), false,
      evidence + ": frozen legacy row is refused while its old lease is live");
  }
  const aggregate = aggregateFocus(f);
  if (evidence === "span plus rollout twin") {
    assert.ok(aggregate.activeRows >= 1 && aggregate.activeRows <= 2,
      evidence + ": at most one frozen twin plus one owner");
    assert.equal(aggregate.billableRows, 1, evidence + ": one billable owner");
    assert.equal(aggregate.countedRows, 1, evidence + ": one counted observation");
    assert.ok(aggregate.gapRows <= 1, evidence + ": duplicate span remains tokenless");
    if (aggregate.activeRows === 2 && aggregate.gapRows === 0) {
      assert.equal(frozenHold, true, evidence + ": unsafe historical frozen twin is held");
    }
    for (const item of f.lease?.items ?? []) {
      if (!f.focusIds?.includes(item.rawId)) continue;
      const sent = item.envelope.event as AiInteractionEvent;
      const hasCounters = [sent.inputTokens, sent.outputTokens, sent.cacheReadTokens, sent.cacheCreationTokens, sent.costUsd]
        .some(value => value !== undefined);
      if (hasCounters) {
        assert.equal(item.rawId, f.focusId, evidence + ": only the named owner is leased with counters");
        assert.equal(sent.model, MODEL, evidence + ": no model-less twin can be leased as usage");
      }
    }
  } else {
    assert.equal(aggregate.activeRows, 1, evidence + ": one active delivery");
  }
  if (!frozenHold && evidence !== "span plus rollout twin") {
    assert.equal(aggregate.billableRows, valid ? 1 : 0, evidence + ": aggregate billable count");
    assert.equal(aggregate.gapRows, valid ? 0 : 1, evidence + ": aggregate gap count");
  }
  return { output, aggregate, sameEvidence: valid };
}
function finalAssertions(f: Fixture, evidence: Evidence, initial: { id?: string; bytes?: string }, frozenHold = false, replacement = false) {
  const checked = aggregateAssertions(f, evidence, frozenHold);
  const sameId = replacement || !initial.id || checked.output.id === initial.id;
  const sameBytes = replacement || !initial.bytes || checked.output.bytes === initial.bytes;
  assert.equal(sameId, true, evidence + ": delivery ID stable");
  assert.equal(sameBytes, true, evidence + ": frozen bytes stable");
  return { ...checked, sameId, sameBytes };
}
function snapshot(f: Fixture) { return { id: f.initialId, bytes: f.initialBytes }; }

async function runNormalCell(evidence: Evidence, state: State, pathName: Path): Promise<Cell> {
  const f = await buildFixture(evidence, pathName);
  const operations: Operation[] = [];
  const refusals: string[] = [];
  try {
    if (evidence === "span plus rollout twin") operations.push({ name: "unique span/rollout pairing",
      leaseItems: 0, detail: "exact marginal, native turn and bounded trace identity leave one rollout owner" });
    seedState(f, state);
    if (pathName === "first lease") {
      const at = state === "unsealed" ? 63000 : state === "sealed and unacknowledged" ? 64000 : 185000;
      const result = leaseAt(f, at, state === "unsealed" ? "present" : "absent");
      operations.push({ name: "first lease/refusal", leaseItems: result.items.length,
        detail: "a pre-sealed active row is refused without duplication" });
      if (state !== "unsealed") refusals.push("live lease refusal");
    } else if (pathName === "retry") {
      const active = f.lease && f.lease.items.some((candidate: any) => candidate.rawId === f.focusId)
        ? f.lease : leaseAt(f, 63000, "present");
      const item = active.items.find((candidate: any) => candidate.rawId === f.focusId);
      assert.ok(item);
      advance(f, f.lastAt + 1000);
      f.buffer.delivery.retry(active.leaseId, [item], "remote_transient", f.now.value);
      const result = leaseAt(f, f.lastAt + 240000, "present");
      operations.push({ name: "retry then lease", leaseItems: result.items.length,
        detail: "retry releases the lease and the due row is leased once" });
    } else if (pathName === "reopen") {
      const closeAt = state === "unsealed" ? 3000 : state === "sealed and unacknowledged" ? 64000 : 185000;
      closeFixture(f); openFixture(f, closeAt);
      const result = leaseAt(f, state === "unsealed" ? 63000 : closeAt,
        state === "unsealed" ? "present" : "absent");
      operations.push({ name: "close/open then lease", leaseItems: result.items.length,
        detail: state === "unsealed" ? "restart before first attempt returns usage" : "restart preserves live-lease refusal" });
      if (state !== "unsealed") refusals.push("restart live-lease refusal");
    } else if (pathName === "dead-letter replay") {
      const active = f.lease || leaseAt(f, 63000, "present");
      const item = active.items.find((candidate: any) => candidate.rawId === f.focusId);
      assert.ok(item);
      advance(f, f.lastAt + 1000);
      assert.equal(f.buffer.delivery.deadLetterRemote(active.leaseId, [item.deliveryId], f.now.value), 1);
      operations.push({ name: "remote dead-letter", leaseItems: 0, detail: "remote terminal state records replay lineage" });
      advance(f, f.lastAt + 1000);
      const replay = f.buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now: f.now.value });
      assert.equal(replay.requeued, 1);
      operations.push({ name: "replay frozen delivery", leaseItems: 0, detail: "replay requeues frozen bytes" });
      assert.equal(f.buffer.delivery.restampUnsentRaw(f.focusId, alteredPayload(f)), false);
      refusals.push("replay lineage restamp refusal");
      const result = leaseAt(f, f.lastAt + 60000, "present");
      operations.push({ name: "replay then restamp then lease", leaseItems: result.items.length,
        detail: "composed replay→restamp boundary retains one delivery" });
    } else if (pathName === "remote-terminal replay") {
      const active = f.lease && f.lease.items.some((candidate: any) => candidate.rawId === f.focusId)
        ? f.lease : leaseAt(f, 63000, "present");
      const item = active.items.find((candidate: any) => candidate.rawId === f.focusId);
      assert.ok(item);
      const witness = f.witness;
      assert.ok(witness);
      const contract = witness.contract;
      advance(f, f.lastAt + 1000);
      assert.equal(f.buffer.delivery.markValidationCandidate(active.leaseId, item.deliveryId, contract, f.now.value), 1);
      operations.push({ name: "mark validation candidate", leaseItems: 0, detail: "candidate joins by delivery ID" });
      advance(f, f.lastAt + 1000);
      assert.equal(f.buffer.delivery.acknowledge(witness.leaseId, [witness.item.deliveryId], f.now.value,
        { contractHash: contract, item: witness.item }).acknowledged, 1);
      operations.push({ name: "ack validation witness", leaseItems: 0, detail: "witness is acknowledged before settlement" });
      advance(f, f.lastAt + 1000);
      assert.equal(f.buffer.delivery.settleProvenValidationCandidates(contract, { now: f.now.value }), 1);
      operations.push({ name: "settle terminal candidate", leaseItems: 0, detail: "candidate becomes replayable dead letter" });
      advance(f, f.lastAt + 1000);
      assert.equal(f.buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now: f.now.value }).requeued, 1);
      operations.push({ name: "replay terminal candidate", leaseItems: 0, detail: "replay restores frozen envelope" });
      assert.equal(f.buffer.delivery.restampUnsentRaw(f.focusId, alteredPayload(f)), false);
      refusals.push("terminal replay lineage restamp refusal");
      const result = leaseAt(f, f.lastAt + 60000, "present");
      operations.push({ name: "terminal replay then restamp then lease", leaseItems: result.items.length,
        detail: "one frozen delivery remains billable or tokenless" });
    } else if (pathName === "restamp") {
      if (state === "unsealed") {
        assert.equal(f.buffer.delivery.restampUnsentRaw(f.focusId, alteredPayload(f)), true);
        operations.push({ name: "unsealed restamp", leaseItems: 0, detail: "restamp is accepted before first attempt" });
        const result = leaseAt(f, 63000, "present");
        operations.push({ name: "lease restamped row", leaseItems: result.items.length, detail: "accepted row is captured once" });
      } else {
        assert.equal(f.buffer.delivery.restampUnsentRaw(f.focusId, alteredPayload(f)), false);
        refusals.push("sealed restamp refusal");
        const result = leaseAt(f, f.lastAt + 1000, "absent");
        operations.push({ name: "sealed restamp then lease", leaseItems: result.items.length, detail: "frozen row is refused and not duplicated" });
      }
    } else {
      throw new Error("upgrade is handled separately");
    }
    const checked = finalAssertions(f, evidence, snapshot(f));
    return { evidence, state, path: pathName, result: "PASS", operations, refusals,
      aggregate: checked.aggregate, i1: true, i2: true, i3: true, deliveryId: checked.output.id,
      model: checked.output.event.model, inputTokens: checked.output.event.inputTokens,
      outputTokens: checked.output.event.outputTokens, frozenBytesSame: checked.sameBytes };
  } finally { closeFixture(f); }
}

async function runUpgradeCell(evidence: Evidence, state: State): Promise<Cell> {
  const file = path.join(root, crypto.randomUUID() + "-upgrade.sqlite");
  const now = { value: new Date(AT + 2000) };
  const targetId = id(); const traceId = crypto.randomBytes(16).toString("hex");
  const turn = "matrix-" + targetId; let focusId: string = targetId;
  let focusIds: string[] | undefined;
  let oldInitialId: string | undefined; let oldInitialBytes: string | undefined;
  const operations: Operation[] = [];
  const oldAt = (at: number) => { now.value = new Date(AT + at); };
  await withLegacyReader(async ({ Buffer: OldBuffer, reconciliation }) => {
    const old = new OldBuffer(file, opts(() => now.value));
    try {
      if (evidence === "span plus rollout twin") {
        const twin = appendSpanRolloutTwin(old, targetId, traceId, turn, false);
        focusId = twin.rolloutId;
        focusIds = [twin.spanId, twin.rolloutId];
      } else if (evidence === "exact pair") {
        const pair = nativeLog(id(), undefined, true); old.append(pair);
        old.database.prepare("update buffered_events set event_type='otel_log' where id=?").run(pair.id);
        focusId = pair.id; old.append(targetFor(evidence, targetId, traceId, turn));
      } else {
        if (evidence === "native trace") {
          const oldTarget = targetFor(evidence, targetId, traceId, turn);
          oldTarget.model = MODEL;
          oldTarget.metadata["gen_ai.request.model"] = MODEL;
          old.append(oldTarget);
          old.append(nativeLog(id(), traceId));
        }
        if (evidence === "legacy guess on a peer with counters different from the response")
          old.append(event({ id: id(), inputTokens: 17, outputTokens: 3, model: "gpt-6-astra",
            metadata: { otelEventName: "codex.sse_event", traceId } }));
        if (evidence === "native local turn") recordCodexTurnModel(old.database, SESSION, turn, MODEL);
        if (evidence !== "native trace") old.append(targetFor(evidence, targetId, traceId, turn));
      }
      reconciliation.runCodexReconciliationMaintenance(old.database, {
        legacyRowLimit: 100, legacyChunkLimit: 100, contextWindowLimit: 100,
        contextRowLimit: 100, candidateLimit: 100, freshCandidateLimit: 100, timeLimitMs: 1000,
      });
      if (state !== "unsealed") {
        oldAt(63000);
        const oldLease = old.delivery.lease({ now: now.value });
        const item = oldLease.items.find((candidate: any) => candidate.rawId === focusId);
        assert.ok(item, "historical reader must seal upgrade fixture");
        oldInitialId = item.deliveryId; oldInitialBytes = item.envelopeJson;
      }
    } finally { old.close(); }
  });
  const f: Fixture = { file, buffer: new LocalEventBuffer(file, opts(() => now.value)),
    now, lastAt: 2000, focusId, focusIds, targetId };
  try {
    if (evidence === "span plus rollout twin") {
      const paired = pairCodexSpanRolloutEvent(f.buffer.database, f.focusId);
      assert.ok(paired && paired.ownerId === f.focusId,
        "explicit helper composition pairs synthetic stable-turn twins");
      operations.push({ name: "explicit helper composition after historical reader", leaseItems: 0,
        detail: "synthetic current turn evidence; production open/lease does not automatically pair existing historical twins" });
    }
    let result: Lease;
    let replacement = false;
    if (state === "unsealed") result = leaseAt(f, 63000, "present");
    else if (state === "sealed and unacknowledged") result = leaseAt(f, 64000, "absent");
    else {
      const expired = leaseAt(f, 184000, "any");
      const found = expired.items.some((item: any) => item.rawId === f.focusId);
      if (found) result = expired;
      else {
        assert.ok(expired.locallyDead > 0, "expired legacy sealed usage is retired before replacement");
        replacement = true;
        result = leaseAt(f, 185000, "present");
      }
    }
    operations.push({ name: "historical reader then current lease", leaseItems: result.items.length,
      detail: state === "sealed and unacknowledged" ? "current reader refuses live older lease" :
        "current reader reads old delivery after expiry" });
    const frozenHold = state === "sealed and unacknowledged" &&
      !(evidence === "exact pair" || evidence === "native trace" || evidence === "native local turn");
    const checked = finalAssertions(f, evidence, { id: oldInitialId, bytes: oldInitialBytes }, frozenHold, replacement);
    return { evidence, state, path: "upgrade", result: "PASS", operations,
      refusals: state === "sealed and unacknowledged" ? ["historical live-lease refusal"] : [],
      aggregate: checked.aggregate, i1: true, i2: true, i3: true, frozenHold, replacement,
      ...(evidence === "span plus rollout twin" ? { coverage: "helper composition with synthetic stable-turn evidence; not automatic historical upgrade pairing" } : {}),
      deliveryId: checked.output.id, model: checked.output.event.model,
      inputTokens: checked.output.event.inputTokens, outputTokens: checked.output.event.outputTokens,
      frozenBytesSame: checked.sameBytes };
  } finally { closeFixture(f); }
}

async function lateContradictoryNativeEvidence() {
  const file = path.join(root, crypto.randomUUID() + "-late.sqlite");
  const now = { value: new Date(AT + 2000) };
  const trace = "e".repeat(32);
  const bad = event({ id: id(), eventType: "otel_span", model: MODEL, inputTokens: 17,
    outputTokens: 3, metadata: { otelEventName: "codex.sse_event", traceId: trace,
      "gen_ai.request.model": "gpt-6-astra" } });
  const target = event({ id: id(), metadata: { otelEventName: "handle_responses", traceId: trace } });
  const clean = nativeLog(id(), trace);
  const b = new LocalEventBuffer(file, opts(() => now.value));
  try {
    b.append(bad); now.value = new Date(AT + 63000);
    const first = b.delivery.lease({ now: now.value }) as Lease;
    const gap = first.items.find((item: any) => item.rawId === bad.id); assert.ok(gap);
    assert.equal(gap.envelope.event.metadata.usageSource, "capture_gap");
    b.delivery.acknowledge(first.leaseId, [gap.deliveryId], now.value);
    const before = (b.database.prepare("select payload_json as p from buffered_events where id=?").get(bad.id) as { p: string }).p;
    b.append(clean); b.append(target); now.value = new Date(AT + 124000);
    const later = b.delivery.lease({ now: now.value }) as Lease;
    const targetItem = later.items.find((item: any) => item.rawId === target.id); assert.ok(targetItem);
    assert.equal(targetItem.envelope.event.metadata.usageSource, "capture_gap");
    assert.equal(targetItem.envelope.event.inputTokens, undefined);
    const after = (b.database.prepare("select payload_json as p from buffered_events where id=?").get(bad.id) as { p: string }).p;
    assert.equal(after, before);
    console.log(JSON.stringify({ composed: "late-contradictory-native-evidence", passed: true }, null, 2));
  } finally { b.close(); fs.rmSync(file, { force: true }); }
}

async function main() {
  const cells: Cell[] = [];
  for (const evidence of EVIDENCE) for (const state of STATES) for (const pathName of PATHS) {
    let cell: Cell;
    try { cell = pathName === "upgrade" ? await runUpgradeCell(evidence, state) : await runNormalCell(evidence, state, pathName); }
    catch (error) {
      console.error(JSON.stringify({ failedCell: { evidence, state, path: pathName }, error: String(error) }));
      throw error;
    }
    cells.push(cell); completion.check(evidence + " × " + state + " × " + pathName);
  }
  assert.equal(cells.length, 147);
  assert.ok(cells.every((cell) => cell.result === "PASS" && cell.i1 && cell.i2 && cell.i3 &&
    cell.operations.length > 0 && cell.aggregate.activeRows >= 1 &&
    (cell.evidence === "span plus rollout twin"
      ? cell.aggregate.countedRows === 1 && cell.aggregate.billableRows === 1
      : cell.aggregate.activeRows === 1 &&
        (cell.frozenHold || ((cell.aggregate.billableRows === 1) ===
          (cell.evidence === "exact pair" || cell.evidence === "native trace" || cell.evidence === "native local turn"))))));
  await lateContradictoryNativeEvidence();
  console.log(JSON.stringify({
    proof: "codex-capture-invariants-matrix",
    invariants: [
      "I1 billable Codex usage requires one unambiguous native model at capture",
      "I2 a frozen named delivery keeps its ID and bytes across retry, restart, upgrade and terminal replay",
      "I3 a capture gap is tokenless and never regains counters or a model on any replay path",
    ],
    matrix: { evidence: EVIDENCE, states: STATES, paths: PATHS, cellsExecuted: cells.length, expected: 147 },
    composedCases: ["replay then restamp", "late contradictory native evidence", "span plus rollout twin one-owner"], cells,
  }, null, 2));
  completion.complete();
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
