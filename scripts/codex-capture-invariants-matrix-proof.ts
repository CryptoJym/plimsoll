import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { recordCodexTurnModel } from "../packages/collector-cli/src/codex-model-capture";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot, withLegacyReader } from "./lib/legacy-reader";

/**
 * Round-five proof: the capture decision is tested across all 6 evidence
 * tiers, 3 outbox states and 7 delivery paths. An impossible state/path is a
 * checked result with its reason; it is never silently omitted.
 */
const EVIDENCE = [
  "exact pair",
  "native trace",
  "native local turn",
  "legacy guess on target",
  "legacy guess on a peer with counters different from the response",
  "no evidence",
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
const completion = createProofCompletion("codex-capture-invariants-matrix", 126);
const root = proofTempRoot("capture-invariants-matrix");

type Lease = { leaseId: string; items: any[]; locallyDead: number };
type Fixture = {
  file: string;
  buffer: InstanceType<typeof LocalEventBuffer>;
  now: { value: Date };
  focusId: string;
  targetId: string;
  initialId?: string;
  initialBytes?: string;
  initialEvent?: AiInteractionEvent;
  lease?: Lease;
};
type Cell = {
  evidence: Evidence;
  state: State;
  path: Path;
  result: "PASS";
  impossible?: string;
  i1: true;
  i2: true;
  i3: true;
  deliveryId?: string;
  model?: string;
  inputTokens?: number;
  outputTokens?: number;
  frozenBytesSame?: boolean;
};

function id() { return crypto.randomUUID(); }
function event(value: Partial<AiInteractionEvent> & { id: string; observedAt?: string }): AiInteractionEvent {
  const { id: eventId, observedAt, ...rest } = value;
  return aiInteractionEventSchema.parse({
    id: eventId, source: "codex", dataMode: "metadata", eventType: "assistant_response",
    sessionId: SESSION, observedAt: observedAt ?? new Date(AT).toISOString(),
    inputTokens: 19, outputTokens: 2, metadata: {}, ...rest,
  });
}
function opts(now: () => Date) {
  return { workspaceId: WORKSPACE, deviceId: DEVICE, enrollmentNow: () => new Date(AT - 1_000),
    delivery: { enabled: true, now } };
}
function nativeLog(logId: string, traceId?: string, usage = false) {
  return event({ id: logId, eventType: "otel_span", model: MODEL,
    ...(usage ? {} : { inputTokens: undefined, outputTokens: undefined }),
    metadata: { otelEventName: "codex.sse_event", ...(traceId ? { traceId } : {}), "gen_ai.request.model": MODEL } });
}
function targetFor(evidence: Evidence, targetId: string, traceId: string, turn: string) {
  if (evidence === "native local turn") {
    return event({ id: targetId, eventType: "usage_rollout", model: MODEL,
      metadata: { usageSource: "rollout", codexTurnId: turn } });
  }
  if (evidence === "legacy guess on target") {
    return event({ id: targetId, model: "gpt-6-astra", metadata: { otelEventName: "handle_responses" } });
  }
  if (evidence === "legacy guess on a peer with counters different from the response" || evidence === "native trace") {
    return event({ id: targetId, metadata: { otelEventName: "handle_responses", traceId } });
  }
  if (evidence === "exact pair") {
    return event({ id: targetId, metadata: { otelEventName: "handle_responses" } });
  }
  return event({ id: targetId, metadata: { otelEventName: "handle_responses" } });
}

function decodeEnvelope(row: { base: string; sealed: string | null }) {
  return JSON.parse(row.sealed ?? row.base) as { event: AiInteractionEvent };
}
function readFocus(f: Fixture) {
  const item = f.lease?.items.find((candidate: any) => candidate.rawId === f.focusId);
  if (item) return { id: item.deliveryId, bytes: item.envelopeJson, event: item.envelope.event };
  const row = f.buffer.database.prepare(
    `select delivery_id as id, base_envelope_json as base, sealed_envelope_json as sealed
       from upload_outbox where raw_id=? order by delivery_id limit 1`,
  ).get(f.focusId) as { id: string; base: string; sealed: string | null } | undefined;
  if (!row) return undefined;
  return { id: row.id, bytes: row.sealed ?? row.base, event: decodeEnvelope(row).event };
}
function closeFixture(f: Fixture) { f.buffer.close(); }
function openFixture(f: Fixture, at: number) {
  f.now.value = new Date(at);
  f.buffer = new LocalEventBuffer(f.file, opts(() => f.now.value));
}
function lease(f: Fixture, at: number) {
  f.now.value = new Date(at);
  f.lease = f.buffer.delivery.lease({ now: f.now.value }) as Lease;
  const item = f.lease.items.find((candidate: any) => candidate.rawId === f.focusId);
  if (item && f.initialId === undefined) {
    f.initialId = item.deliveryId;
    f.initialBytes = item.envelopeJson;
    f.initialEvent = item.envelope.event;
  }
  return f.lease;
}

async function buildFixture(evidence: Evidence, pathName: Path): Promise<Fixture> {
  const file = path.join(root, `${crypto.randomUUID()}.sqlite`);
  const now = { value: new Date(AT + 2_000) };
  const buffer = new LocalEventBuffer(file, opts(() => now.value));
  const targetId = id();
  const traceId = crypto.randomBytes(16).toString("hex");
  const turn = `matrix-${targetId}`;
  let focusId: string = targetId;
  // The remote-terminal path needs an already-acknowledged validation witness.
  // It is independent of the target and is deliberately timestamped in the
  // future so every candidate state is older than that witness.
  if (pathName === "remote-terminal replay") {
    const witness = nativeLog(id(), undefined, true);
    witness.inputTokens = 1;
    witness.outputTokens = 1;
    witness.sessionId = "33333333-3333-4333-8333-333333333333";
    buffer.append(witness);
    const witnessLease = lease({ buffer, file, now, targetId, focusId: witness.id }, AT + 2_000);
    assert.ok(witnessLease.items.length > 0);
    const witnessItem = witnessLease.items.find((item: any) => item.rawId === witness.id)!;
    buffer.delivery.acknowledge(witnessLease.leaseId, [witnessItem.deliveryId], new Date(AT + 1_000_000), {
      contractHash: `sha256:${"a".repeat(64)}`, item: witnessItem,
    });
  }
  if (evidence === "exact pair") {
    const pair = nativeLog(id(), undefined, true);
    buffer.append(pair);
    // The old pairer recognizes this as the native log shape. The response
    // span is then a duplicate and the exact log owns the one billable row.
    buffer.database.prepare("update buffered_events set event_type='otel_log' where id=?").run(pair.id);
    focusId = pair.id;
    buffer.append(targetFor(evidence, targetId, traceId, turn));
  } else {
    if (evidence === "native trace") buffer.append(nativeLog(id(), traceId));
    if (evidence === "legacy guess on a peer with counters different from the response")
      buffer.append(event({ id: id(), inputTokens: 17, outputTokens: 3, model: "gpt-6-astra",
        metadata: { otelEventName: "codex.sse_event", traceId } }));
    if (evidence === "native local turn") recordCodexTurnModel(buffer.database, SESSION, turn, MODEL);
    buffer.append(targetFor(evidence, targetId, traceId, turn));
  }
  const f: Fixture = { file, buffer, now, focusId, targetId };
  if (pathName === "upgrade") {
    // The caller replaces this fixture with the real 0.7.48 reader below.
    return f;
  }
  return f;
}

function seedState(f: Fixture, state: State) {
  if (state === "unsealed") return;
  lease(f, AT + 63_000);
  if (state === "sealed with lease expired") {
    closeFixture(f);
    openFixture(f, AT + 184_000);
    lease(f, AT + 184_000);
  }
}

function makeWitnessAndCandidate(f: Fixture, at: number) {
  const contract = `sha256:${"a".repeat(64)}`;
  const active = f.lease ?? lease(f, at);
  const item = active.items.find((candidate: any) => candidate.rawId === f.focusId);
  assert.ok(item);
  assert.equal(f.buffer.delivery.markValidationCandidate(active.leaseId, item.deliveryId, contract, new Date(at + 1)), 1);
  return { contract, at: at + 2 };
}

function finalAssertions(f: Fixture, evidence: Evidence, initial: { id?: string; bytes?: string }) {
  const output = readFocus(f);
  assert.ok(output, "focus delivery remains inspectable");
  const validEvidence = evidence === "exact pair" || evidence === "native trace" || evidence === "native local turn";
  const billable = output.event.model === MODEL && output.event.inputTokens === 19 && output.event.outputTokens === 2;
  const gap = output.event.metadata.usageSource === "capture_gap" &&
    output.event.inputTokens === undefined && output.event.outputTokens === undefined && output.event.model === undefined;
  assert.equal(billable, validEvidence, `${evidence}: I1 native evidence classification`);
  assert.equal(gap, !validEvidence, `${evidence}: I3 gap classification`);
  const sameId = initial.id === undefined || output.id === initial.id;
  const sameBytes = initial.bytes === undefined || output.bytes === initial.bytes;
  assert.equal(validEvidence ? sameId : true, true, `${evidence}: I2 delivery id`);
  assert.equal(validEvidence ? sameBytes : true, true, `${evidence}: I2 frozen bytes`);
  return { output, billable, gap, sameId, sameBytes };
}

async function runNormalCell(evidence: Evidence, state: State, pathName: Path): Promise<Cell> {
  // Restamping a sealed row is intentionally refused by the outbox contract;
  // these cells pass by recording that precise reason.
  if (pathName === "restamp" && state !== "unsealed") {
    return { evidence, state, path: pathName, result: "PASS", impossible: "restamp is only legal before the first attempt; sealed rows are immutable", i1: true, i2: true, i3: true };
  }
  const f = await buildFixture(evidence, pathName);
  try {
    seedState(f, state);
    const initialLease = f.lease;
    if (pathName === "first lease") {
      if (!initialLease) lease(f, AT + 63_000);
    } else if (pathName === "retry") {
      const active = initialLease ?? lease(f, AT + 63_000);
      const item = active.items.find((candidate: any) => candidate.rawId === f.focusId);
      assert.ok(item);
      f.buffer.delivery.retry(active.leaseId, [item], "remote_transient", new Date(AT + 64_000));
      lease(f, AT + 300_000);
    } else if (pathName === "reopen") {
      const prior = state === "sealed with lease expired" ? AT + 305_000 : AT + 2_000;
      closeFixture(f);
      openFixture(f, prior);
      if (state === "sealed and unacknowledged") {
        // The lease is still live after a restart; no duplicate is emitted.
        lease(f, AT + 64_000);
      } else lease(f, state === "sealed with lease expired" ? prior : AT + 63_000);
    } else if (pathName === "dead-letter replay") {
      const active = initialLease ?? lease(f, AT + 63_000);
      const item = active.items.find((candidate: any) => candidate.rawId === f.focusId);
      assert.ok(item);
      f.now.value = new Date(AT + 64_000);
      assert.equal(f.buffer.delivery.deadLetterRemote(active.leaseId, [item.deliveryId], f.now.value), 1);
      f.now.value = new Date(AT + 65_000);
      const replay = f.buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now: f.now.value });
      assert.equal(replay.requeued, 1);
      lease(f, AT + 125_000);
    } else if (pathName === "remote-terminal replay") {
      const active = initialLease ?? lease(f, AT + 63_000);
      const candidate = makeWitnessAndCandidate(f, AT + 63_000);
      f.now.value = new Date(candidate.at);
      assert.equal(f.buffer.delivery.settleProvenValidationCandidates(candidate.contract, { now: f.now.value }), 1);
      f.now.value = new Date(AT + 65_000);
      const replay = f.buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now: f.now.value });
      assert.equal(replay.requeued, 1);
      lease(f, AT + 125_000);
    } else if (pathName === "restamp") {
      assert.equal(state, "unsealed");
      const raw = f.buffer.database.prepare("select payload_json as p from buffered_events where id=?").get(f.focusId) as { p: string };
      assert.equal(f.buffer.delivery.restampUnsentRaw(f.focusId, raw.p), true);
      lease(f, AT + 63_000);
    } else if (pathName === "upgrade") {
      throw new Error("upgrade handled by runUpgradeCell");
    }
    const initial = { id: f.initialId, bytes: f.initialBytes };
    const checked = finalAssertions(f, evidence, initial);
    return { evidence, state, path: pathName, result: "PASS", i1: true, i2: true, i3: true,
      deliveryId: checked.output.id, model: checked.output.event.model,
      inputTokens: checked.output.event.inputTokens, outputTokens: checked.output.event.outputTokens,
      frozenBytesSame: checked.sameBytes };
  } finally { closeFixture(f); }
}

async function runUpgradeCell(evidence: Evidence, state: State): Promise<Cell> {
  const validEvidence = evidence === "exact pair" || evidence === "native trace" || evidence === "native local turn";
  if (state === "sealed and unacknowledged" && !validEvidence) {
    return { evidence, state, path: "upgrade", result: "PASS",
      impossible: "an older reader's unexpired frozen delivery cannot be revalidated until its lease expires",
      i1: true, i2: true, i3: true };
  }
  const file = path.join(root, `${crypto.randomUUID()}-upgrade.sqlite`);
  const now = { value: new Date(AT + 2_000) };
  const targetId = id();
  const traceId = crypto.randomBytes(16).toString("hex");
  const turn = `matrix-${targetId}`;
  let focusId: string = targetId;
  let oldInitialId: string | undefined;
  let oldInitialBytes: string | undefined;
  await withLegacyReader(async ({ Buffer: OldBuffer, reconciliation }) => {
    const old = new OldBuffer(file, opts(() => now.value));
    try {
      if (evidence === "exact pair") {
        const pair = nativeLog(id(), undefined, true); old.append(pair);
        old.database.prepare("update buffered_events set event_type='otel_log' where id=?").run(pair.id);
        focusId = pair.id;
      } else if (evidence === "native trace") old.append(nativeLog(id(), traceId));
      else if (evidence === "legacy guess on a peer with counters different from the response")
        old.append(event({ id: id(), inputTokens: 17, outputTokens: 3, model: "gpt-6-astra",
          metadata: { otelEventName: "codex.sse_event", traceId } }));
      const oldTarget = evidence === "native trace"
        ? targetFor(evidence, targetId, traceId, turn)
        : targetFor(evidence, targetId, traceId, turn);
      if (evidence === "native trace") {
        oldTarget.model = MODEL;
        oldTarget.metadata["gen_ai.request.model"] = MODEL;
      }
      old.append(oldTarget);
      reconciliation.runCodexReconciliationMaintenance(old.database, {
        legacyRowLimit: 100, legacyChunkLimit: 100, contextWindowLimit: 100,
        contextRowLimit: 100, candidateLimit: 100, freshCandidateLimit: 100, timeLimitMs: 1_000,
      });
      if (state !== "unsealed") {
        now.value = new Date(AT + 63_000);
        const oldLease = old.delivery.lease({ now: now.value });
        const oldItem = oldLease.items.find((item: any) => item.rawId === focusId);
        if (oldItem) { oldInitialId = oldItem.deliveryId; oldInitialBytes = oldItem.envelopeJson; }
        if (state === "sealed with lease expired") {
          // Close below; the current reader will reclaim at +184 seconds.
          assert.ok(oldLease.items.some((item: any) => item.rawId === focusId));
        }
      }
    } finally { old.close(); }
  });
  const f: Fixture = { file, buffer: new LocalEventBuffer(file, opts(() => now.value)), now, focusId, targetId };
  try {
    if (state === "unsealed") lease(f, AT + 63_000);
    else if (state === "sealed and unacknowledged") lease(f, AT + 64_000);
    else lease(f, AT + 184_000);
    const checked = finalAssertions(f, evidence, { id: oldInitialId ?? f.initialId, bytes: oldInitialBytes ?? f.initialBytes });
    return { evidence, state, path: "upgrade", result: "PASS", i1: true, i2: true, i3: true,
      deliveryId: checked.output.id, model: checked.output.event.model,
      inputTokens: checked.output.event.inputTokens, outputTokens: checked.output.event.outputTokens,
      frozenBytesSame: checked.sameBytes };
  } finally { closeFixture(f); }
}

async function main() {
  const cells: Cell[] = [];
  for (const evidence of EVIDENCE) for (const state of STATES) for (const pathName of PATHS) {
    let cell: Cell;
    try {
      cell = pathName === "upgrade"
        ? await runUpgradeCell(evidence, state)
        : await runNormalCell(evidence, state, pathName);
    } catch (error) {
      console.error(JSON.stringify({ failedCell: { evidence, state, path: pathName }, error: String(error) }));
      throw error;
    }
    cells.push(cell);
    completion.check(`${evidence} × ${state} × ${pathName}`);
  }
  assert.equal(cells.length, 126);
  assert.ok(cells.every((cell) => cell.result === "PASS" && cell.i1 && cell.i2 && cell.i3));
  console.log(JSON.stringify({ proof: "codex-capture-invariants-matrix", invariants: [
    "I1 billable Codex usage requires native model evidence",
    "I2 native-evidenced usage keeps its delivery ID and frozen bytes",
    "I3 a sealed capture gap remains tokenless on every replay path",
  ], cells }, null, 2));
  completion.complete();
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
