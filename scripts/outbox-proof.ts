import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeHookPayload } from "../packages/collector-cli/src/normalizer";
import { DeliveryUploadError, uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema, DEFAULT_POLICY, remoteLinkageHash } from "../packages/shared/src/index";

type Check = { name: string; passed: boolean; detail: Record<string, unknown> };
const checks: Check[] = [];
const record = (name: string, passed: boolean, detail: Record<string, unknown> = {}) => {
  checks.push({ name, passed, detail });
  if (!passed) throw new Error(`${name} failed: ${JSON.stringify(detail)}`);
};

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-outbox-proof-"));
let ledgerIndex = 0;
const ledger = () => path.join(root, `ledger-${++ledgerIndex}.sqlite`);
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const proofBaseMs = Date.now() + 60 * 60 * 1_000;
const instant = (seconds = 0) => new Date(proofBaseMs + seconds * 1_000);
const config = (delivery: Record<string, number> = {}) =>
  collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1:1/ingest",
    tenantId: "00000000-0000-4000-8000-000000000001",
    installKey: "proof-install",
    delivery: {
      maxOldestAgeDays: 3650,
      maxBackoffSeconds: 60,
      requestTimeoutSeconds: 1,
      ...delivery,
    },
  });

function event(n: number, input: Record<string, unknown> = {}) {
  return aiInteractionEventSchema.parse({
    id: uuid(n),
    sessionId: uuid(100_000 + n),
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: instant(n).toISOString(),
    actionClass: "other",
    inputTokens: n + 1,
    outputTokens: 1,
    metadata: { proof: true },
    ...input,
  });
}

function enabledBuffer(file = ledger(), overrides: Record<string, number> = {}) {
  const cfg = config(overrides);
  return {
    cfg,
    buffer: new LocalEventBuffer(file, { delivery: { enabled: true, limits: cfg.delivery } }),
  };
}

function response(status: number, body: Record<string, unknown> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestIds(init?: RequestInit) {
  const parsed = JSON.parse(String(init?.body ?? "{}")) as {
    events?: Array<{ event?: { id?: string } }>;
  };
  return (parsed.events ?? []).map((entry) => entry.event?.id ?? "");
}

async function expectDeliveryError(run: () => Promise<unknown>, expected: string) {
  try {
    await run();
    return false;
  } catch (error) {
    return error instanceof DeliveryUploadError && error.failureClass === expected;
  }
}

async function atomicAndDuplicateProof() {
  const { buffer } = enabledBuffer();
  buffer.database.exec(`
    create trigger proof_abort_outbox after insert on upload_outbox
    begin select raise(abort, 'proof_abort'); end;
  `);
  let aborted = false;
  try {
    buffer.append(event(1));
  } catch {
    aborted = true;
  }
  const rolledBack = buffer.database
    .prepare(`select (select count(*) from buffered_events) as raw, (select count(*) from upload_outbox) as outbox`)
    .get() as { raw: number; outbox: number };
  buffer.database.exec(`drop trigger proof_abort_outbox`);
  buffer.append(event(1));
  const original = buffer.database
    .prepare(`select payload_json as payload, uploaded_at as uploadedAt from buffered_events where id = ?`)
    .get(uuid(1)) as { payload: string; uploadedAt: string | null };
  buffer.append(event(1, { inputTokens: 999_999 }));
  const replay = buffer.database
    .prepare(
      `select payload_json as payload, uploaded_at as uploadedAt,
       (select count(*) from upload_outbox) as outbox,
       (select outbox_enqueued_total from upload_control where singleton = 1) as enqueued
       from buffered_events where id = ?`,
    )
    .get(uuid(1)) as { payload: string; uploadedAt: string | null; outbox: number; enqueued: number };
  record(
    "atomic_crash_and_duplicate_zero_rewrite",
    aborted && rolledBack.raw === 0 && rolledBack.outbox === 0 && replay.payload === original.payload && replay.uploadedAt === null && replay.outbox === 1 && replay.enqueued === 1,
    { aborted, rolledBack, duplicateOutbox: replay.outbox, enqueueCount: replay.enqueued },
  );
  buffer.close();

  const local = new LocalEventBuffer(ledger());
  local.append(event(2));
  const localOutbox = (local.database.prepare(`select count(*) as n from upload_outbox`).get() as { n: number }).n;
  record("upload_unconfigured_does_not_duplicate_local_storage", localOutbox === 0, { localOutbox });
  local.close();
}

function migrationProof() {
  const file = ledger();
  let buffer = new LocalEventBuffer(file);
  for (let n = 10; n < 17; n += 1) buffer.append(event(n));
  buffer.markUploaded([uuid(10), uuid(11)], instant().toISOString());
  buffer.close();

  const cfg = config({ migrationBatchRows: 2, migrationBatchBytes: 1_000_000 });
  buffer = new LocalEventBuffer(file, { delivery: { enabled: true, limits: cfg.delivery } });
  const first = buffer.delivery.migrateLegacy({ maxRows: 2, now: instant() });
  const second = buffer.delivery.migrateLegacy({ maxRows: 2, now: instant(1) });
  const cursorBefore = buffer.delivery.status(instant()).migration.cursorRowid;
  buffer.close();

  buffer = new LocalEventBuffer(file, { delivery: { enabled: true, limits: cfg.delivery } });
  let maxVisited = 0;
  let loops = 0;
  while (!buffer.delivery.status(instant()).migration.complete && loops < 10) {
    const slice = buffer.delivery.migrateLegacy({ maxRows: 2, now: instant(2 + loops) });
    maxVisited = Math.max(maxVisited, slice.visited);
    loops += 1;
  }
  const status = buffer.delivery.status(instant(20));
  const active = (buffer.database.prepare(`select count(*) as n from upload_outbox`).get() as { n: number }).n;
  record(
    "bounded_resumable_migration_skips_uploaded",
    first.visited === 2 && first.skippedUploaded === 2 && first.enqueued === 0 && second.visited === 2 && second.enqueued === 2 && status.migration.cursorRowid > cursorBefore && status.migration.complete && maxVisited <= 2 && active === 5,
    { first, second, maxVisited, active, complete: status.migration.complete, lastSlice: status.migration.lastSlice },
  );
  buffer.close();

  const giantFile = ledger();
  const giantSeed = new LocalEventBuffer(giantFile);
  giantSeed.append(event(18, { metadata: { bounded: "x".repeat(8_000) } }));
  giantSeed.close();
  const giantConfig = config({ maxItemBytes: 1_024, migrationBatchRows: 10, migrationBatchBytes: 1_024 });
  const giantBuffer = new LocalEventBuffer(giantFile, {
    delivery: { enabled: true, limits: giantConfig.delivery },
  });
  const giantSlice = giantBuffer.delivery.migrateLegacy({ now: instant(30) });
  const giantReason = giantBuffer.database
    .prepare(`select reason from upload_receipts`)
    .get() as { reason: string };
  record(
    "legacy_oversize_classified_without_payload_materialization_budget",
    giantSlice.visited === 1 && giantSlice.bytes === 0 && giantSlice.dead === 1 && giantReason.reason === "local_item_oversize",
    { visited: giantSlice.visited, loadedBytes: giantSlice.bytes, dead: giantSlice.dead, reason: giantReason.reason },
  );
  giantBuffer.close();

  const sliceBoundFile = ledger();
  const sliceBoundSeed = new LocalEventBuffer(sliceBoundFile);
  sliceBoundSeed.append(event(19, { metadata: { bounded: "x".repeat(1_800) } }));
  sliceBoundSeed.close();
  const sliceBoundConfig = config({
    maxItemBytes: 10_000,
    migrationBatchRows: 10,
    migrationBatchBytes: 1_024,
  });
  const sliceBoundBuffer = new LocalEventBuffer(sliceBoundFile, {
    delivery: { enabled: true, limits: sliceBoundConfig.delivery },
  });
  const sliceBound = sliceBoundBuffer.delivery.migrateLegacy({
    maxRows: 10,
    maxBytes: 1_024,
    now: instant(31),
  });
  const pausedStatus = sliceBoundBuffer.delivery.status(instant(31));
  const sliceBoundOutbox = (sliceBoundBuffer.database
    .prepare(`select count(*) as n from upload_outbox`)
    .get() as { n: number }).n;
  const sliceBoundReceipts = (sliceBoundBuffer.database
    .prepare(`select count(*) as n from upload_receipts`)
    .get() as { n: number }).n;
  const pendingRawBeforeResume = (sliceBoundBuffer.database
    .prepare(`select count(*) as n from buffered_events where uploaded_at is null`)
    .get() as { n: number }).n;
  sliceBoundBuffer.delivery.configure({
    enabled: true,
    limits: { ...sliceBoundConfig.delivery, migrationBatchBytes: 4_096 },
  });
  const resumed = sliceBoundBuffer.delivery.migrateLegacy({
    maxRows: 10,
    maxBytes: 4_096,
    now: instant(32),
  });
  const resumedStatus = sliceBoundBuffer.delivery.status(instant(32));
  record(
    "legacy_item_above_slice_cap_pauses_without_loss_then_resumes",
    sliceBound.visited === 0 &&
      sliceBound.bytes === 0 &&
      sliceBound.dead === 0 &&
      !sliceBound.complete &&
      sliceBound.paused === "slice_budget_too_small" &&
      sliceBoundOutbox === 0 &&
      sliceBoundReceipts === 0 &&
      pendingRawBeforeResume === 1 &&
      pausedStatus.degraded &&
      pausedStatus.degradedReasons.includes("migration_slice_budget") &&
      pausedStatus.migration.cursorRowid === 0 &&
      resumed.visited === 1 &&
      resumed.enqueued === 1 &&
      resumed.dead === 0 &&
      resumed.complete &&
      resumedStatus.remainingDelivery === 1 &&
      !resumedStatus.degradedReasons.includes("migration_slice_budget"),
    {
      visited: sliceBound.visited,
      loadedBytes: sliceBound.bytes,
      dead: sliceBound.dead,
      complete: sliceBound.complete,
      paused: sliceBound.paused,
      active: sliceBoundOutbox,
      receipts: sliceBoundReceipts,
      pendingRawBeforeResume,
      pausedStatus: {
        degraded: pausedStatus.degraded,
        reasons: pausedStatus.degradedReasons,
        cursor: pausedStatus.migration.cursorRowid,
      },
      resumed,
      resumedRemaining: resumedStatus.remainingDelivery,
    },
  );
  sliceBoundBuffer.close();
}

async function migrationReopenProof() {
  const file = ledger();
  const cfg = config({ migrationBatchRows: 1, migrationBatchBytes: 1_000_000 });
  let buffer = new LocalEventBuffer(file, {
    delivery: { enabled: true, limits: cfg.delivery },
  });
  buffer.append(event(20));
  await uploadBufferedEvents(cfg, buffer, {
    limit: 1,
    fetchImpl: async () => response(200, { accepted: 1 }),
    now: () => instant(35),
  });
  buffer.delivery.migrateLegacy({ maxRows: 1, now: instant(36) });
  const completed = buffer.delivery.status(instant(36)).migration;
  buffer.close();

  buffer = new LocalEventBuffer(file);
  buffer.append(event(21));
  const invalidatedInAppend = buffer.delivery.status(instant(37)).migration;
  // Simulate a rollback-compatible producer that writes raw truth without the
  // current append helper. Re-enable must still compare the O(1) rowid high-water.
  insertLegacyPoison(buffer, 22, JSON.stringify(event(22)));
  buffer.close();

  buffer = new LocalEventBuffer(file, {
    delivery: { enabled: true, limits: cfg.delivery },
  });
  const reopened = buffer.delivery.status(instant(38)).migration;
  const sent: string[][] = [];
  const uploadOne = (seconds: number) =>
    uploadBufferedEvents(cfg, buffer, {
      limit: 1,
      fetchImpl: async (_input, init) => {
        sent.push(requestIds(init));
        return response(200, { accepted: 1 });
      },
      now: () => instant(seconds),
    });
  const second = await uploadOne(39);
  const mid = buffer.delivery.status(instant(40)).migration;
  const third = await uploadOne(41);
  buffer.delivery.migrateLegacy({ maxRows: 1, now: instant(42) });
  const final = buffer.delivery.status(instant(42));
  const raw = buffer.database
    .prepare(`select id, uploaded_at as uploadedAt from buffered_events order by rowid`)
    .all() as Array<{ id: string; uploadedAt: string | null }>;
  const receipts = buffer.database
    .prepare(`select delivery_id as id, terminal_state as state from upload_receipts order by created_at`)
    .all() as Array<{ id: string; state: string }>;
  record(
    "completed_migration_reopens_after_disabled_and_direct_raw_appends",
    completed.complete &&
      completed.cursorRowid === 1 &&
      !invalidatedInAppend.complete &&
      invalidatedInAppend.cursorRowid === 1 &&
      !reopened.complete &&
      second.uploadedEvents === 1 &&
      !mid.complete &&
      third.uploadedEvents === 1 &&
      final.migration.complete &&
      final.migration.cursorRowid === 3 &&
      sent.flat().join(",") === [uuid(21), uuid(22)].join(",") &&
      raw.every((row) => row.uploadedAt !== null) &&
      receipts.length === 3 &&
      new Set(receipts.map((row) => row.id)).size === 3,
    {
      completed,
      invalidatedInAppend,
      reopened,
      mid,
      final: final.migration,
      sent: sent.flat(),
      rawUploaded: raw.map((row) => Boolean(row.uploadedAt)),
      receipts,
    },
  );
  buffer.close();
}

function insertLegacyPoison(buffer: LocalEventBuffer, n: number, payloadJson: string) {
  buffer.database
    .prepare(
      `insert into buffered_events
        (id, source, event_type, data_mode, observed_at, payload_json,
         suppressed_fields_json, created_at, uploaded_at)
       values (?, 'codex', 'assistant_response', 'metadata', ?, ?, '[]', ?, null)`,
    )
    .run(uuid(n), instant(n).toISOString(), payloadJson, instant(n).toISOString());
}

async function localPoisonProof() {
  const file = ledger();
  const seed = new LocalEventBuffer(file);
  insertLegacyPoison(seed, 30, "{");
  insertLegacyPoison(seed, 31, JSON.stringify({ ...event(31), source: "invalid_source" }));
  seed.append(event(32));
  seed.close();

  const { buffer, cfg } = enabledBuffer(file);
  let httpCalls = 0;
  const fetchImpl: typeof fetch = async () => {
    httpCalls += 1;
    return response(200, { accepted: 1 });
  };
  const result = await uploadBufferedEvents(cfg, buffer, { fetchImpl, now: () => instant(50) });
  const reasons = buffer.database
    .prepare(`select reason from upload_receipts where terminal_state = 'dead' order by reason`)
    .all() as Array<{ reason: string }>;
  record(
    "local_poison_dead_once_without_http_and_valid_continues",
    httpCalls === 1 && result.uploadedEvents === 1 && reasons.length === 2 && reasons.some((row) => row.reason === "local_payload_unparseable") && reasons.some((row) => row.reason === "local_schema_invalid") && result.remainingDelivery === 0,
    { httpCalls, uploaded: result.uploadedEvents, reasons: reasons.map((row) => row.reason) },
  );
  const receiptCount = reasons.length;
  buffer.delivery.migrateLegacy({ now: instant(51) });
  const afterReplay = (buffer.database.prepare(`select count(*) as n from upload_receipts where terminal_state = 'dead'`).get() as { n: number }).n;
  record("local_poison_receipt_idempotent", afterReplay === receiptCount, { receiptCount, afterReplay });
  buffer.close();
}

async function remotePoisonPositionProof() {
  for (const poisonPosition of [0, 1, 2]) {
    const { buffer, cfg } = enabledBuffer();
    const items = [event(40 + poisonPosition * 10), event(41 + poisonPosition * 10), event(42 + poisonPosition * 10)];
    for (const item of items) buffer.append(item);
    const poisonId = items[poisonPosition].id;
    const bodies: string[] = [];
    const fetchImpl: typeof fetch = async (_input, init) => {
      bodies.push(String(init?.body ?? ""));
      const ids = requestIds(init);
      return ids.includes(poisonId) ? response(422, { privateProviderBody: "must-not-persist" }) : response(200, { accepted: ids.length });
    };
    const result = await uploadBufferedEvents(cfg, buffer, { fetchImpl, now: () => instant(100), maxProbes: 15 });
    const status = buffer.delivery.status(instant(101));
    const batchIds = result.batch?.events.map((entry) => entry.event.id) ?? [];
    record(
      `remote_poison_position_${poisonPosition}`,
      result.uploadedEvents === 2 && result.delivery.deadLetters === 1 && status.remainingDelivery === 0 && status.receipts.dead === 1 && status.receipts.acknowledged === 2 && bodies.length <= 7 && batchIds.length === 2 && !batchIds.includes(poisonId),
      { uploaded: result.uploadedEvents, dead: status.receipts.dead, probes: bodies.length, acknowledgedBatch: batchIds.length },
    );
    const persisted = buffer.database
      .prepare(`select group_concat(reason || ':' || status_class, '|') as text from upload_receipts`)
      .get() as { text: string | null };
    record(
      `remote_poison_provider_body_redacted_${poisonPosition}`,
      !String(persisted.text).includes("privateProviderBody") && !String(persisted.text).includes("must-not-persist"),
      { receiptBytes: Buffer.byteLength(persisted.text ?? "") },
    );
    buffer.close();
  }
}

async function limitOnePoisonFairnessProof() {
  for (const poisonPosition of [0, 1, 2]) {
    const base = 300 + poisonPosition * 10;
    const cfg = config({ maxBackoffSeconds: 30, maxProbesPerCycle: 2 });
    const buffer = new LocalEventBuffer(ledger(), {
      delivery: { enabled: true, limits: cfg.delivery },
    });
    const items = [event(base), event(base + 1), event(base + 2)];
    for (const item of items) buffer.append(item);
    const poisonId = items[poisonPosition].id;
    const rawBefore = buffer.database
      .prepare(`select id, payload_json as payload from buffered_events order by rowid`)
      .all() as Array<{ id: string; payload: string }>;
    const requestGroups: string[][] = [];
    const uploadedPerCall: number[] = [];
    for (let cycle = 0; cycle < 5; cycle += 1) {
      if (buffer.delivery.status(instant(2_000 + cycle * 31)).remainingDelivery === 0) break;
      const result = await uploadBufferedEvents(cfg, buffer, {
        limit: 1,
        maxProbes: 2,
        fetchImpl: async (_input, init) => {
          const ids = requestIds(init);
          requestGroups.push(ids);
          return ids.includes(poisonId)
            ? response(422, { privateProviderBody: "never-persist-limit-one" })
            : response(200, { accepted: ids.length });
        },
        now: () => instant(2_000 + cycle * 31),
      });
      uploadedPerCall.push(result.uploadedEvents);
    }
    const status = buffer.delivery.status(instant(2_200));
    const receipts = buffer.database
      .prepare(`select delivery_id as id, terminal_state as state, reason from upload_receipts order by delivery_id`)
      .all() as Array<{ id: string; state: string; reason: string }>;
    const rawAfter = buffer.database
      .prepare(`select id, payload_json as payload, uploaded_at as uploadedAt from buffered_events order by rowid`)
      .all() as Array<{ id: string; payload: string; uploadedAt: string | null }>;
    record(
      `limit_one_poison_position_${poisonPosition}_bounded_fairness`,
      uploadedPerCall.every((count) => count <= 1) &&
        requestGroups.every((ids) => ids.length === 1) &&
        status.remainingDelivery === 0 &&
        status.receipts.acknowledged === 2 &&
        status.receipts.dead === 1 &&
        receipts.filter((row) => row.id === poisonId && row.state === "dead").length === 1 &&
        rawBefore.every((before, index) => before.payload === rawAfter[index].payload) &&
        rawAfter.filter((row) => row.id !== poisonId).every((row) => row.uploadedAt !== null) &&
        rawAfter.find((row) => row.id === poisonId)?.uploadedAt === null,
      {
        uploadedPerCall,
        requestGroups,
        remaining: status.remainingDelivery,
        acknowledged: status.receipts.acknowledged,
        dead: status.receipts.dead,
        poisonReceipts: receipts.filter((row) => row.id === poisonId),
      },
    );
    buffer.close();
  }

  {
    const cfg = config({ maxBackoffSeconds: 30, maxProbesPerCycle: 2 });
    const buffer = new LocalEventBuffer(ledger(), {
      delivery: { enabled: true, limits: cfg.delivery },
    });
    const knownGood = event(340);
    buffer.append(knownGood);
    await uploadBufferedEvents(cfg, buffer, {
      limit: 1,
      fetchImpl: async () => response(200, { accepted: 1 }),
      now: () => instant(2_300),
    });
    const contractRows = [event(341)];
    for (const item of contractRows) buffer.append(item);
    let probes = 0;
    const failed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        limit: 1,
        maxProbes: 2,
        fetchImpl: async () => {
          probes += 1;
          return response(422, { globalPrivateBody: "never-persist-global" });
        },
        now: () => instant(2_331),
      }),
      "remote_contract",
    );
    const status = buffer.delivery.status(instant(2_331));
    record(
      "limit_one_global_422_with_prior_witness_quarantines_zero",
      failed &&
        probes === 2 &&
        status.receipts.dead === 0 &&
        status.receipts.acknowledged === 1 &&
        status.remainingDelivery === 1 &&
        status.circuit.kind === "contract_blocked",
      {
        failed,
        probes,
        dead: status.receipts.dead,
        acknowledged: status.receipts.acknowledged,
        remaining: status.remainingDelivery,
        circuit: status.circuit.kind,
      },
    );
    buffer.close();
  }
}

async function crashBetweenSiblingAckAndQuarantineProof() {
  const file = ledger();
  const cfg = config({ maxBackoffSeconds: 30, maxProbesPerCycle: 2, leaseSeconds: 120 });
  const poison = event(350);
  const valid = event(351);
  let buffer = new LocalEventBuffer(file, {
    delivery: { enabled: true, limits: cfg.delivery },
  });
  buffer.append(poison);
  buffer.append(valid);
  let crashed = false;
  try {
    await uploadBufferedEvents(cfg, buffer, {
      limit: 1,
      maxProbes: 2,
      fetchImpl: async (_input, init) =>
        requestIds(init).includes(poison.id)
          ? response(422, { privateProviderBody: "never-persist-crash" })
          : response(200, { accepted: 1 }),
      now: () => instant(2_400),
      afterSiblingAcknowledgement: () => {
        throw new Error("simulated_crash_after_sibling_ack_before_quarantine");
      },
    });
  } catch (error) {
    crashed = error instanceof Error && error.message.includes("simulated_crash");
  }
  const partial = buffer.delivery.status(instant(2_400));
  const durableWitness = (buffer.database
    .prepare(`select count(*) as n from upload_validation_witness`)
    .get() as { n: number }).n;
  buffer.close();

  buffer = new LocalEventBuffer(file, {
    delivery: { enabled: true, limits: cfg.delivery },
  });
  const replayRequests: string[][] = [];
  const replay = await uploadBufferedEvents(cfg, buffer, {
    limit: 1,
    maxProbes: 2,
    fetchImpl: async (_input, init) => {
      const ids = requestIds(init);
      replayRequests.push(ids);
      return ids.includes(poison.id)
        ? response(422, { privateProviderBody: "never-persist-replay" })
        : response(200, { accepted: ids.length });
    },
    now: () => instant(2_521),
  });
  await uploadBufferedEvents(cfg, buffer, {
    limit: 1,
    fetchImpl: async () => response(200, { accepted: 0 }),
    now: () => instant(2_522),
  });
  const final = buffer.delivery.status(instant(2_522));
  const receipts = buffer.database
    .prepare(`select delivery_id as id, terminal_state as state from upload_receipts order by delivery_id`)
    .all() as Array<{ id: string; state: string }>;
  record(
    "crash_after_sibling_ack_replays_quarantine_once_from_durable_witness",
    crashed &&
      partial.active.inFlight === 1 &&
      partial.receipts.acknowledged === 1 &&
      partial.receipts.dead === 0 &&
      durableWitness === 1 &&
      replay.uploadedEvents === 0 &&
      replay.delivery.deadLetters === 1 &&
      replayRequests.length === 0 &&
      final.remainingDelivery === 0 &&
      final.receipts.acknowledged === 1 &&
      final.receipts.dead === 1 &&
      receipts.filter((row) => row.id === poison.id && row.state === "dead").length === 1,
    {
      crashed,
      partial: { active: partial.active, receipts: partial.receipts },
      durableWitness,
      replay: { uploaded: replay.uploadedEvents, dead: replay.delivery.deadLetters },
      replayRequests,
      final: { remaining: final.remainingDelivery, receipts: final.receipts },
      receipts,
    },
  );
  buffer.close();
}

async function globalContractAndAuthProof() {
  {
    const { buffer, cfg } = enabledBuffer();
    for (const n of [80, 81, 82]) buffer.append(event(n));
    let probes = 0;
    const failed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async () => {
          probes += 1;
          return response(422, { error: "global" });
        },
        now: () => instant(120),
      }),
      "remote_contract",
    );
    const status = buffer.delivery.status(instant(120));
    const attempts = buffer.database
      .prepare(`select min(attempt_count) as min, max(attempt_count) as max from upload_outbox`)
      .get() as { min: number; max: number };
    record(
      "global_422_contract_circuit_zero_dead_letters",
      failed && probes <= cfg.delivery.maxProbesPerCycle && status.receipts.dead === 0 && status.remainingDelivery === 3 && status.circuit.kind === "contract_blocked" && attempts.min === 1 && attempts.max === 1 && status.counters.outboxAttempts === 3,
      { failed, probes, dead: status.receipts.dead, circuit: status.circuit.kind, attempts },
    );
    buffer.close();
  }
  {
    const { buffer, cfg } = enabledBuffer();
    buffer.append(event(90));
    let probes = 0;
    const failed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async () => {
          probes += 1;
          return response(401, { secret: "never-record" });
        },
        now: () => instant(140),
      }),
      "remote_auth",
    );
    const status = buffer.delivery.status(instant(140));
    record(
      "auth_circuit_no_bisection_or_dead_letter",
      failed && probes === 1 && status.circuit.kind === "auth_blocked" && status.receipts.dead === 0 && status.active.retry === 1,
      { failed, probes, circuit: status.circuit.kind, retry: status.active.retry },
    );
    buffer.close();
  }
}

async function retryAndCrashProof() {
  {
    const { buffer, cfg } = enabledBuffer();
    buffer.append(event(100));
    const bodies: string[] = [];
    const firstFailed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async (_input, init) => {
          bodies.push(String(init?.body ?? ""));
          return response(429, { responseBody: "redacted" });
        },
        now: () => instant(200),
      }),
      "remote_transient",
    );
    const retryRow = buffer.database
      .prepare(`select attempt_count as attemptCount, next_attempt_at as nextAttemptAt, sealed_envelope_json as sealed from upload_outbox`)
      .get() as { attemptCount: number; nextAttemptAt: string; sealed: string };
    const second = await uploadBufferedEvents(cfg, buffer, {
      fetchImpl: async (_input, init) => {
        bodies.push(String(init?.body ?? ""));
        return response(200, { accepted: 1 });
      },
      now: () => instant(205),
    });
    record(
      "transient_backoff_attempt_once_and_identical_retry_body",
      firstFailed && retryRow.attemptCount === 1 && Date.parse(retryRow.nextAttemptAt) > instant(200).getTime() && second.uploadedEvents === 1 && bodies.length === 2 && bodies[0] === bodies[1],
      { firstFailed, firstAttempt: retryRow.attemptCount, retried: second.uploadedEvents, identicalBody: bodies[0] === bodies[1] },
    );
    buffer.close();
  }
  {
    const { buffer, cfg } = enabledBuffer();
    buffer.append(event(110));
    const bodies: string[] = [];
    let crashed = false;
    try {
      await uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async (_input, init) => {
          bodies.push(String(init?.body ?? ""));
          return response(200, { accepted: 1 });
        },
        now: () => instant(300),
        afterRemote: () => {
          throw new Error("simulated_crash_after_remote");
        },
      });
    } catch {
      crashed = true;
    }
    const inFlight = buffer.delivery.status(instant(300));
    const replay = await uploadBufferedEvents(cfg, buffer, {
      fetchImpl: async (_input, init) => {
        bodies.push(String(init?.body ?? ""));
        return response(200, { accepted: 1 });
      },
      now: () => instant(421),
    });
    const receipt = buffer.database
      .prepare(`select attempt_count as attempts from upload_receipts where terminal_state = 'acknowledged'`)
      .get() as { attempts: number };
    record(
      "crash_after_remote_lease_recovery_reuses_exact_body",
      crashed && inFlight.active.inFlight === 1 && replay.uploadedEvents === 1 && bodies.length === 2 && bodies[0] === bodies[1] && receipt.attempts === 2,
      { crashed, inFlight: inFlight.active.inFlight, identicalBody: bodies[0] === bodies[1], attempts: receipt.attempts },
    );
    buffer.close();
  }
  {
    const { buffer, cfg } = enabledBuffer();
    buffer.append(event(120));
    const failed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async () => {
          throw new Error("network sentinel body not logged");
        },
        now: () => instant(500),
      }),
      "remote_transient",
    );
    const status = buffer.delivery.status(instant(500));
    record("network_failure_retryable_without_circuit", failed && status.active.retry === 1 && status.circuit.kind === "none", { failed, retry: status.active.retry, circuit: status.circuit.kind });
    buffer.close();
  }
}

async function linkageAndRetentionProof() {
  const { buffer, cfg } = enabledBuffer();
  const repoHash = remoteLinkageHash("https://example.invalid/private/repo")!;
  const branchHash = remoteLinkageHash("private-branch")!;
  buffer.append(event(130));
  buffer.database.prepare(`update buffered_events set repo_hash = ?, branch_hash = ? where id = ?`).run(repoHash, branchHash, uuid(130));
  let body = "";
  const linked = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: async (_input, init) => {
      body = String(init?.body ?? "");
      return response(200, { accepted: 1 });
    },
    now: () => instant(600),
  });
  const sent = JSON.parse(body) as { events: Array<{ event: { projectKey?: string; metadata: Record<string, unknown> } }> };
  record(
    "linkage_fill_before_seal",
    linked.uploadedEvents === 1 && sent.events[0].event.projectKey === repoHash && sent.events[0].event.metadata.branchHash === branchHash,
    { uploaded: linked.uploadedEvents, projectKeyFilled: sent.events[0].event.projectKey === repoHash },
  );

  buffer.append(event(131, { projectKey: "owner-supplied-project" }));
  buffer.database.prepare(`update buffered_events set repo_hash = ? where id = ?`).run(repoHash, uuid(131));
  const lease = buffer.delivery.lease({ now: instant(610), leaseId: "proof-seal" });
  const sealedBefore = lease.items[0].envelopeJson;
  buffer.database.prepare(`update buffered_events set repo_hash = ?, branch_hash = ? where id = ?`).run(remoteLinkageHash("another")!, remoteLinkageHash("another-branch")!, uuid(131));
  const sealedAfter = (buffer.database.prepare(`select sealed_envelope_json as sealed from upload_outbox where delivery_id = ?`).get(uuid(131)) as { sealed: string }).sealed;
  record(
    "linkage_after_seal_cannot_mutate_or_overwrite_project_key",
    sealedBefore === sealedAfter && (JSON.parse(sealedAfter) as { event: { projectKey: string } }).event.projectKey === "owner-supplied-project",
    { identicalSeal: sealedBefore === sealedAfter },
  );
  buffer.delivery.retry(lease.leaseId, lease.items, "remote_transient", instant(610));

  buffer.append(event(132));
  buffer.database.prepare(`update buffered_events set created_at = ? where id = ?`).run("2020-01-01T00:00:00.000Z", uuid(132));
  const prunePending = buffer.prune(0);
  const pendingExists = (buffer.database.prepare(`select count(*) as n from buffered_events where id = ?`).get(uuid(132)) as { n: number }).n;
  const status = buffer.delivery.status(instant(620));
  record(
    "retention_compatibility_gate_preserves_pending_raw",
    pendingExists === 1 && prunePending.events >= 1 && status.retention.mode === "compatibility_uploaded_only" && status.retention.rawTtlBlockedBy === "projection_parity",
    { pendingExists, prunedUploaded: prunePending.events, retention: status.retention },
  );
  buffer.close();
}

async function noMarkPressureAndPrivacyProof() {
  {
    const file = ledger();
    const buffer = new LocalEventBuffer(file);
    buffer.append(event(140));
    const before = buffer.database
      .prepare(`select uploaded_at as uploadedAt, (select count(*) from upload_outbox) as active, (select count(*) from upload_receipts) as receipts, (select outbox_attempts_total from upload_control where singleton = 1) as attempts from buffered_events`)
      .get() as Record<string, unknown>;
    let calls = 0;
    const result = await uploadBufferedEvents(config(), buffer, {
      markUploaded: false,
      fetchImpl: async () => {
        calls += 1;
        return response(200, { accepted: 1 });
      },
      now: () => instant(700),
    });
    const after = buffer.database
      .prepare(`select uploaded_at as uploadedAt, (select count(*) from upload_outbox) as active, (select count(*) from upload_receipts) as receipts, (select outbox_attempts_total from upload_control where singleton = 1) as attempts from buffered_events`)
      .get() as Record<string, unknown>;
    record("no_mark_is_stateless_zero_delivery_mutation", calls === 1 && result.uploadedEvents === 1 && JSON.stringify(before) === JSON.stringify(after), { calls, stateUnchanged: JSON.stringify(before) === JSON.stringify(after) });
    buffer.close();
  }
  {
    const { buffer } = enabledBuffer(undefined, { maxActiveRows: 2, maxActiveBytes: 10_000_000 });
    for (const n of [150, 151, 152]) buffer.append(event(n));
    const pressure = buffer.delivery.status(instant(800));
    const paused = buffer.delivery.migrateLegacy({ now: instant(800) });
    buffer.append(event(153));
    const afterNewCapture = buffer.delivery.status(instant(800));
    record(
      "pressure_degrades_pauses_migration_but_not_new_atomic_capture",
      pressure.pressure.degraded && pressure.pressure.reasons.includes("row_budget") && paused.paused === "pressure" && afterNewCapture.remainingDelivery === 4,
      { reasons: pressure.pressure.reasons, migrationPaused: paused.paused, afterNewCapture: afterNewCapture.remainingDelivery },
    );
    buffer.close();
  }
  {
    const { buffer, cfg } = enabledBuffer();
    const promptSentinel = "RAW_PROMPT_SENTINEL_OUTBOX_PROOF";
    const emailSentinel = "private.person@example.invalid";
    const pathSentinel = "/Users/private/secret/project";
    const tokenSentinel = "COOKIE_TOKEN_SENTINEL_OUTBOX_PROOF";
    const normalized = normalizeHookPayload(
      {
        id: uuid(160),
        source: "codex",
        event_type: "assistant_response",
        timestamp: instant(900).toISOString(),
        prompt: promptSentinel,
        response: tokenSentinel,
        cwd: pathSentinel,
        email: emailSentinel,
      },
      { policy: DEFAULT_POLICY, source: "codex" },
    );
    buffer.append(normalized.event, [...normalized.suppressedFields, pathSentinel]);
    let requestBody = "";
    await uploadBufferedEvents(cfg, buffer, {
      fetchImpl: async (_input, init) => {
        requestBody = String(init?.body ?? "");
        return response(200, { accepted: 1, providerSecret: tokenSentinel });
      },
      now: () => instant(901),
    });
    const persistedDelivery = buffer.database
      .prepare(
        `select coalesce(group_concat(text_value, '|'), '') as text from (
           select base_envelope_json as text_value from upload_outbox
           union all select sealed_envelope_json from upload_outbox
           union all select delivery_id || ':' || reason || ':' || status_class from upload_receipts
         )`,
      )
      .get() as { text: string };
    const rawPayload = (buffer.database.prepare(`select payload_json as payload from buffered_events where id = ?`).get(uuid(160)) as { payload: string }).payload;
    const statusJson = JSON.stringify(buffer.delivery.status(instant(901)));
    const sentinels = [promptSentinel, emailSentinel, pathSentinel, tokenSentinel];
    const absent = sentinels.every((sentinel) => !persistedDelivery.text.includes(sentinel) && !requestBody.includes(sentinel) && !rawPayload.includes(sentinel) && !statusJson.includes(sentinel));
    record("metadata_privacy_sentinels_absent_from_raw_delivery_request_receipt_status", absent, { sentinelCount: sentinels.length, requestBytes: Buffer.byteLength(requestBody), deliveryBytes: Buffer.byteLength(persistedDelivery.text) });
    buffer.close();
  }
}

async function hostilePrivacyAndLinkageProof() {
  const { buffer, cfg } = enabledBuffer();
  const hostile = [
    ["accessToken", "CAMEL_CREDENTIAL_SENTINEL_7a9f"],
    ["AccessToken", "PASCAL_CREDENTIAL_SENTINEL_7b9f"],
    ["refresh_token", "SNAKE_CREDENTIAL_SENTINEL_7c9f"],
    ["auth", "AUTH_CREDENTIAL_SENTINEL_7d9f"],
    ["cookie-value", "COOKIE_CREDENTIAL_SENTINEL_7e9f"],
    ["client.credential", "DOT_CREDENTIAL_SENTINEL_7f9f"],
    ["ownerEmail", "EMAIL_CREDENTIAL_SENTINEL_809f"],
    ["db-password", "PASSWORD_CREDENTIAL_SENTINEL_819f"],
    ["localPath", "PATH_CREDENTIAL_SENTINEL_829f"],
    ["rawPrompt", "PROMPT_CREDENTIAL_SENTINEL_839f"],
    ["providerResponse", "RESPONSE_CREDENTIAL_SENTINEL_849f"],
    ["clientSecret", "SECRET_CREDENTIAL_SENTINEL_859f"],
    ["requestURL", "URL_CREDENTIAL_SENTINEL_869f"],
    ["branchHash", "sha256:branch-digest-short-879f"],
    ["repoHash", "sha256:repo-digest-short-889f"],
    ["remoteHash", "sha256:remote-digest-short-899f"],
    ["remoteUrlHash", "sha256:remote-url-digest-short-8a9f"],
  ] as const;
  hostile.forEach(([key, sentinel], index) => {
    buffer.append(event(700 + index, { metadata: { [key]: sentinel } }));
  });

  const invalidLinkageId = uuid(720);
  buffer.append(event(720, {
    metadata: {
      transport_path: "/v1/traces",
      cacheReadTokens: 3,
      cache_creation_tokens: 4,
      cacheWhateverTokens: 5,
      "gen_ai.usage.output_tokens": 6,
      reasoningOutputTokens: 7,
      "gen_ai.usage.input_tokens": "8",
    },
  }));
  const invalidRowid = (buffer.database
    .prepare(`select rowid as rowid from buffered_events where id = ?`)
    .get(invalidLinkageId) as { rowid: number }).rowid;
  const malformedLinkage = "sha256:LINKAGE_SECRET_SENTINEL_8b4e";
  buffer.delivery.fillLinkageForRawRow(invalidRowid, malformedLinkage, null);

  const canonicalLinkageId = uuid(721);
  buffer.append(event(721, { metadata: { transport_path: "/v1/metrics" } }));
  const canonicalRowid = (buffer.database
    .prepare(`select rowid as rowid from buffered_events where id = ?`)
    .get(canonicalLinkageId) as { rowid: number }).rowid;
  const upperCanonical = `SHA256:${"AB".repeat(32)}`;
  const lowerCanonical = `sha256:${"ab".repeat(32)}`;
  buffer.delivery.fillLinkageForRawRow(canonicalRowid, upperCanonical, null);

  const metadataLinkageId = uuid(722);
  const branchCanonical = `sha256:${"cd".repeat(32)}`;
  const headSha = "e".repeat(40);
  buffer.append(event(722, {
    metadata: {
      transport_path: "/v1/logs",
      git: {
        remoteUrlHash: lowerCanonical,
        branchHash: branchCanonical,
        headSha,
      },
    },
  }));

  const beforeRows = buffer.database
    .prepare(
      `select delivery_id as id, base_envelope_json as base,
         coalesce(sealed_envelope_json, '') as sealed, repo_hash as repoHash
       from upload_outbox order by delivery_id`,
    )
    .all() as Array<{ id: string; base: string; sealed: string; repoHash: string | null }>;
  const deadBefore = buffer.database
    .prepare(`select delivery_id as id, reason from upload_receipts where terminal_state = 'dead' order by id`)
    .all() as Array<{ id: string; reason: string }>;
  let requestBody = "";
  const uploaded = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: async (_input, init) => {
      requestBody = String(init?.body ?? "");
      return response(200, { accepted: requestIds(init).length });
    },
    now: () => instant(3_000),
  });
  const sent = JSON.parse(requestBody) as {
    events: Array<{
      event: {
        id: string;
        projectKey?: string;
        inputTokens?: number;
        outputTokens?: number;
        metadata: Record<string, unknown>;
      };
    }>;
  };
  const persisted = buffer.database
    .prepare(
      `select coalesce(group_concat(text_value, '|'), '') as text from (
         select base_envelope_json as text_value from upload_outbox
         union all select coalesce(sealed_envelope_json, '') from upload_outbox
         union all select coalesce(repo_hash, '') || ':' || coalesce(branch_hash, '') from upload_outbox
         union all select envelope_json from upload_validation_witness
         union all select delivery_id || ':' || reason || ':' || status_class from upload_receipts
       )`,
    )
    .get() as { text: string };
  const statusJson = JSON.stringify(buffer.delivery.status(instant(3_001)));
  const hostileSentinels = hostile.map(([, sentinel]) => sentinel);
  const forbidden = [...hostileSentinels, malformedLinkage];
  const absentEverywhere = forbidden.every((sentinel) =>
    !beforeRows.some((row) => `${row.base}|${row.sealed}|${row.repoHash ?? ""}`.includes(sentinel)) &&
    !requestBody.includes(sentinel) &&
    !persisted.text.includes(sentinel) &&
    !statusJson.includes(sentinel),
  );
  const invalidSent = sent.events.find((entry) => entry.event.id === invalidLinkageId)?.event;
  const canonicalSent = sent.events.find((entry) => entry.event.id === canonicalLinkageId)?.event;
  const metadataLinkageSent = sent.events.find((entry) => entry.event.id === metadataLinkageId)?.event;
  const metadataGit = metadataLinkageSent?.metadata.git as Record<string, unknown> | undefined;
  record(
    "normalized_sensitive_keys_and_noncanonical_linkage_never_enter_delivery_surfaces",
    deadBefore.length === hostile.length &&
      deadBefore.every((row) => row.reason === "local_privacy_violation") &&
      beforeRows.length === 3 &&
      beforeRows.find((row) => row.id === invalidLinkageId)?.repoHash === null &&
      beforeRows.find((row) => row.id === canonicalLinkageId)?.repoHash === lowerCanonical &&
      uploaded.uploadedEvents === 3 &&
      absentEverywhere &&
      invalidSent?.projectKey === undefined &&
      canonicalSent?.projectKey === lowerCanonical &&
      invalidSent?.metadata.transport_path === "/v1/traces" &&
      invalidSent?.inputTokens === 721 &&
      invalidSent?.outputTokens === 1 &&
      invalidSent?.metadata.cacheReadTokens === 3 &&
      invalidSent?.metadata.cache_creation_tokens === 4 &&
      invalidSent?.metadata.cacheWhateverTokens === 5 &&
      invalidSent?.metadata["gen_ai.usage.output_tokens"] === 6 &&
      invalidSent?.metadata.reasoningOutputTokens === 7 &&
      invalidSent?.metadata["gen_ai.usage.input_tokens"] === "8" &&
      metadataLinkageSent?.projectKey === lowerCanonical &&
      metadataGit?.remoteUrlHash === lowerCanonical &&
      metadataGit?.branchHash === branchCanonical &&
      metadataGit?.headSha === headSha,
    {
      hostileDead: deadBefore.length,
      activeBefore: beforeRows.length,
      uploaded: uploaded.uploadedEvents,
      absentEverywhere,
      invalidProjectKey: invalidSent?.projectKey ?? null,
      canonicalProjectKey: canonicalSent?.projectKey ?? null,
      metadataProjectKey: metadataLinkageSent?.projectKey ?? null,
      metadataGit: metadataGit ?? null,
      transportPath: invalidSent?.metadata.transport_path,
      numericTokens: {
        input: invalidSent?.inputTokens,
        output: invalidSent?.outputTokens,
        cacheRead: invalidSent?.metadata.cacheReadTokens,
        cacheCreation: invalidSent?.metadata.cache_creation_tokens,
        cacheOther: invalidSent?.metadata.cacheWhateverTokens,
        namespacedOutput: invalidSent?.metadata["gen_ai.usage.output_tokens"],
        reasoningOutput: invalidSent?.metadata.reasoningOutputTokens,
        namespacedInputString: invalidSent?.metadata["gen_ai.usage.input_tokens"],
      },
    },
  );
  buffer.close();
}

function pressureAgeByteOversizeAndStatusProof() {
  {
    const file = ledger();
    const seed = new LocalEventBuffer(file);
    const old = event(170);
    seed.append(old);
    seed.database.prepare(`update buffered_events set created_at = ? where id = ?`).run("2020-01-01T00:00:00.000Z", old.id);
    seed.close();
    const { buffer } = enabledBuffer(file, { maxOldestAgeDays: 1, maxActiveBytes: 10_000_000 });
    buffer.delivery.migrateLegacy({ now: instant(1000), maxRows: 10 });
    const status = buffer.delivery.status(instant(1000));
    record("oldest_age_pressure_exact", status.pressure.reasons.includes("age_budget") && status.active.oldestCreatedAt === "2020-01-01T00:00:00.000Z", { reasons: status.pressure.reasons, oldest: status.active.oldestCreatedAt });
    buffer.close();
  }
  {
    const { buffer } = enabledBuffer(undefined, { maxActiveBytes: 200, maxItemBytes: 10_000 });
    buffer.append(event(171, { metadata: { bounded: "x".repeat(500) } }));
    const status = buffer.delivery.status(instant(1001));
    record("active_byte_pressure_exact", status.active.bytes > 200 && status.pressure.reasons.includes("byte_budget"), { activeBytes: status.active.bytes, budget: status.pressure.budgets.bytes });
    buffer.close();
  }
  {
    const { buffer } = enabledBuffer(undefined, { maxItemBytes: 1_024 });
    buffer.append(event(172, { metadata: { bounded: "x".repeat(2_000) } }));
    const status = buffer.delivery.status(instant(1002));
    const receipt = buffer.database.prepare(`select reason from upload_receipts`).get() as { reason: string };
    record("oversize_dead_before_http_or_active_queue", status.remainingDelivery === 0 && receipt.reason === "local_item_oversize" && status.receipts.dead === 1, { remaining: status.remainingDelivery, reason: receipt.reason });
    buffer.close();
  }
  {
    const { buffer } = enabledBuffer();
    for (let n = 180; n < 280; n += 1) buffer.append(event(n));
    const status = buffer.delivery.status(instant(1100));
    const plan = buffer.database
      .prepare(`explain query plan select * from upload_control where singleton = 1`)
      .all() as Array<{ detail: string }>;
    record(
      "status_uses_singleton_gauges_not_history_aggregates",
      status.remainingDelivery === 100 && status.work.controlRowsRead === 1 && status.work.activeRowsScanned === 0 && status.work.receiptRowsScanned === 0 && status.work.rawRowsScanned === 0 && plan.some((row) => /primary key|integer primary key/i.test(row.detail)),
      { remaining: status.remainingDelivery, work: status.work, plan: plan.map((row) => row.detail).join(" | ") },
    );
    buffer.close();
  }
}

async function main() {
  try {
    await atomicAndDuplicateProof();
    migrationProof();
    await migrationReopenProof();
    await localPoisonProof();
    await remotePoisonPositionProof();
    await limitOnePoisonFairnessProof();
    await crashBetweenSiblingAckAndQuarantineProof();
    await globalContractAndAuthProof();
    await retryAndCrashProof();
    await linkageAndRetentionProof();
    await noMarkPressureAndPrivacyProof();
    await hostilePrivacyAndLinkageProof();
    pressureAgeByteOversizeAndStatusProof();
    const failed = checks.filter((check) => !check.passed);
    console.log(
      JSON.stringify(
        {
          schema: "plimsoll.outbox-proof.v1",
          status: failed.length === 0 ? "pass" : "fail",
          checks: checks.length,
          failed: failed.length,
          names: checks.map((check) => check.name),
          liveStateTouched: false,
          providerNetworkCalled: false,
        },
        null,
        2,
      ),
    );
    if (failed.length > 0) process.exitCode = 1;
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
