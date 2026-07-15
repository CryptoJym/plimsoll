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

function bulkyAllowedMetadata(count = 16) {
  return {
    otelSignalNames: Array.from(
      { length: count },
      (_, index) => `signal_${String(index).padStart(2, "0")}_${"x".repeat(145)}`,
    ),
  };
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

  const utf8File = ledger();
  const utf8Seed = new LocalEventBuffer(utf8File);
  utf8Seed.append(event(23, { metadata: { note: "😀".repeat(300) } }));
  const utf8Lengths = utf8Seed.database
    .prepare(
      `select length(payload_json) as characters,
         length(cast(payload_json as blob)) as bytes
       from buffered_events where id = ?`,
    )
    .get(uuid(23)) as { characters: number; bytes: number };
  utf8Seed.close();
  const utf8Config = config({
    maxItemBytes: 8_192,
    migrationBatchRows: 10,
    migrationBatchBytes: 1_024,
  });
  const utf8Buffer = new LocalEventBuffer(utf8File, {
    delivery: { enabled: true, limits: utf8Config.delivery },
  });
  const utf8Paused = utf8Buffer.delivery.migrateLegacy({
    maxRows: 10,
    maxBytes: 1_024,
    now: instant(33),
  }) as {
    paused: "pressure" | "slice_budget_too_small" | null;
    visited: number;
    bytes: number;
  };
  const utf8ActiveWhilePaused = (utf8Buffer.database
    .prepare(`select count(*) as n from upload_outbox`)
    .get() as { n: number }).n;
  const utf8Resumed = utf8Buffer.delivery.migrateLegacy({
    maxRows: 10,
    maxBytes: 8_192,
    now: instant(34),
  });
  const utf8Active = utf8Buffer.database
    .prepare(
      `select base_bytes as baseBytes,
         length(cast(base_envelope_json as blob)) as actualBytes
       from upload_outbox`,
    )
    .get() as { baseBytes: number; actualBytes: number };
  record(
    "utf8_byte_caps_use_blob_and_buffer_bytes_not_characters",
    utf8Lengths.bytes > 1_024 &&
      utf8Lengths.characters < utf8Lengths.bytes &&
      utf8Paused.paused === "slice_budget_too_small" &&
      utf8Paused.visited === 0 &&
      utf8Paused.bytes === 0 &&
      utf8ActiveWhilePaused === 0 &&
      utf8Resumed.enqueued === 1 &&
      utf8Active.baseBytes === utf8Active.actualBytes,
    {
      rawCharacters: utf8Lengths.characters,
      rawBytes: utf8Lengths.bytes,
      paused: utf8Paused.paused,
      activeWhilePaused: utf8ActiveWhilePaused,
      resumed: utf8Resumed,
      activeBytes: utf8Active,
    },
  );
  utf8Buffer.close();
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
    const cfg = config({ maxBackoffSeconds: 30, maxProbesPerCycle: 1 });
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
    const failures: string[] = [];
    for (let cycle = 0; cycle < 12; cycle += 1) {
      if (buffer.delivery.status(instant(2_000 + cycle * 31)).remainingDelivery === 0) break;
      try {
        const result = await uploadBufferedEvents(cfg, buffer, {
          limit: 1,
          maxProbes: 1,
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
      } catch (error) {
        if (!(error instanceof DeliveryUploadError)) throw error;
        failures.push(error.failureClass);
      }
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
        failures,
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
    const cfg = config({ maxBackoffSeconds: 30, maxProbesPerCycle: 1 });
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
    const failures: string[] = [];
    for (let cycle = 0; cycle < 12; cycle += 1) {
      try {
        await uploadBufferedEvents(cfg, buffer, {
          limit: 1,
          maxProbes: 1,
          fetchImpl: async () => {
            probes += 1;
            return response(422, { globalPrivateBody: "never-persist-global" });
          },
          now: () => instant(2_331 + cycle * 31),
        });
      } catch (error) {
        if (!(error instanceof DeliveryUploadError)) throw error;
        failures.push(error.failureClass);
        if (error.failureClass === "remote_contract") break;
      }
    }
    const failed = failures.includes("remote_contract");
    const status = buffer.delivery.status(instant(2_400));
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
        failures,
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
      JSON.stringify(replayRequests) === JSON.stringify([[valid.id]]) &&
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

  const ownerSuppliedProject = `sha256:${"9a".repeat(32)}`;
  buffer.append(event(131, { projectKey: ownerSuppliedProject }));
  buffer.database.prepare(`update buffered_events set repo_hash = ? where id = ?`).run(repoHash, uuid(131));
  const lease = buffer.delivery.lease({ now: instant(610), leaseId: "proof-seal" });
  const sealedBefore = lease.items[0].envelopeJson;
  buffer.database.prepare(`update buffered_events set repo_hash = ?, branch_hash = ? where id = ?`).run(remoteLinkageHash("another")!, remoteLinkageHash("another-branch")!, uuid(131));
  const sealedAfter = (buffer.database.prepare(`select sealed_envelope_json as sealed from upload_outbox where delivery_id = ?`).get(uuid(131)) as { sealed: string }).sealed;
  record(
    "linkage_after_seal_cannot_mutate_or_overwrite_project_key",
    sealedBefore === sealedAfter && (JSON.parse(sealedAfter) as { event: { projectKey: string } }).event.projectKey === ownerSuppliedProject,
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
    const file = ledger();
    const buffer = new LocalEventBuffer(file);
    const tokenSentinel = "STATELESS_ACCESS_TOKEN_SENTINEL";
    const malformedLinkage = "sha256:not-a-canonical-linkage";
    buffer.append(event(141, { metadata: { accessToken: tokenSentinel } }));
    buffer.append(event(142, { projectKey: malformedLinkage }));
    const before = buffer.database
      .prepare(`select count(*) as raw, sum(uploaded_at is not null) as uploaded from buffered_events`)
      .get() as { raw: number; uploaded: number };
    let calls = 0;
    const rejectedOnly = await uploadBufferedEvents(config(), buffer, {
      markUploaded: false,
      fetchImpl: async () => {
        calls += 1;
        return response(200, { accepted: 1 });
      },
      now: () => instant(710),
    });
    const validId = uuid(143);
    buffer.append(event(143, { metadata: { serviceName: "collector-proof" } }));
    let requestBody = "";
    const withValid = await uploadBufferedEvents(config(), buffer, {
      markUploaded: false,
      fetchImpl: async (_input, init) => {
        calls += 1;
        requestBody = String(init?.body ?? "");
        return response(200, { accepted: requestIds(init).length });
      },
      now: () => instant(711),
    });
    const after = buffer.database
      .prepare(`select count(*) as raw, sum(uploaded_at is not null) as uploaded from buffered_events`)
      .get() as { raw: number; uploaded: number };
    record(
      "no_mark_uses_same_sealed_envelope_privacy_boundary",
      rejectedOnly.uploadedEvents === 0 &&
        calls === 1 &&
        withValid.uploadedEvents === 1 &&
        requestIds({ body: requestBody }).join(",") === validId &&
        !requestBody.includes(tokenSentinel) &&
        !requestBody.includes(malformedLinkage) &&
        before.raw === 2 &&
        before.uploaded === 0 &&
        after.raw === 3 &&
        after.uploaded === 0,
      {
        calls,
        rejectedUploaded: rejectedOnly.uploadedEvents,
        validUploaded: withValid.uploadedEvents,
        requestIds: requestIds({ body: requestBody }),
        stateBefore: before,
        stateAfter: after,
      },
    );
    buffer.close();
  }
  {
    const buffer = new LocalEventBuffer(ledger());
    const oversizedId = uuid(144);
    const laterId = uuid(145);
    buffer.append(event(144, { metadata: bulkyAllowedMetadata(64) }));
    buffer.append(aiInteractionEventSchema.parse({
      id: laterId,
      source: "codex",
      eventType: "unknown",
      observedAt: instant(712).toISOString(),
      metadata: {},
    }));
    const bodies: string[] = [];
    const result = await uploadBufferedEvents(config(), buffer, {
      markUploaded: false,
      maxBytes: 512,
      fetchImpl: async (_input, init) => {
        bodies.push(String(init?.body ?? ""));
        return response(200, { accepted: requestIds(init).length });
      },
      now: () => instant(713),
    });
    const state = buffer.database
      .prepare(`select count(*) as raw, sum(uploaded_at is not null) as uploaded from buffered_events`)
      .get() as { raw: number; uploaded: number };
    record(
      "no_mark_exact_request_cap_skips_local_oversize_for_later_eligible",
      bodies.length === 1 &&
        Buffer.byteLength(bodies[0]) <= 512 &&
        requestIds({ body: bodies[0] }).join(",") === laterId &&
        !bodies[0].includes(oversizedId) &&
        result.uploadedEvents === 1 &&
        state.raw === 2 &&
        state.uploaded === 0,
      {
        requestBytes: bodies.map((body) => Buffer.byteLength(body)),
        requestIds: requestIds({ body: bodies[0] }),
        uploadedEvents: result.uploadedEvents,
        state,
      },
    );
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
  const rejected: Array<{ metadata: Record<string, unknown>; sentinel: string }> = [
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
    ["remoteUrlHash", "sha256:remote-url-digest-short-8a9f"],
    ["apiKey", "API_KEY_SENTINEL"],
    ["API.Key", "API_DOT_KEY_SENTINEL"],
    ["privateKey", "PRIVATE_KEY_SENTINEL"],
    ["private-key", "PRIVATE_DASH_KEY_SENTINEL"],
    ["signingKey", "SIGNING_KEY_SENTINEL"],
    ["SIGNING_key", "SIGNING_SNAKE_KEY_SENTINEL"],
    ["sshKey", "SSH_KEY_SENTINEL"],
    ["homeDirectory", "HOME_DIRECTORY_SENTINEL"],
    ["home.directory", "HOME_DOT_DIRECTORY_SENTINEL"],
    ["working-directory", "WORKING_DIRECTORY_SENTINEL"],
    ["fileName", "FILE_NAME_SENTINEL"],
    ["file_name", "FILE_SNAKE_NAME_SENTINEL"],
    ["apiKeyHash", `sha256:${"10".repeat(32)}`],
    ["privateKeyHash", `sha256:${"20".repeat(32)}`],
    ["passwordRepoHash", `sha256:${"30".repeat(32)}`],
    ["authRemoteHash", `sha256:${"40".repeat(32)}`],
    ["credentialPathHash", `sha256:${"50".repeat(32)}`],
    ["secretRepoHash", `sha256:${"60".repeat(32)}`],
  ].map(([key, sentinel]) => ({ metadata: { [key]: sentinel }, sentinel }));
  rejected.push({
    metadata: { git: { passwordRepoHash: `sha256:${"70".repeat(32)}` } },
    sentinel: `sha256:${"70".repeat(32)}`,
  });
  const allowedStringKeys = [
    "action_class", "call_id", "cfo_one.action_class", "cliVersion",
    "db.operation.name", "db.system", "decision", "error.type", "event.name",
    "exception.type", "gen_ai.request.model", "gen_ai.response.id", "gen_ai.system",
    "gen_ai.tool.name", "http.request.method", "mcp_server", "model", "originator",
    "otelEventName", "otelOriginalActionClass", "planType", "plimsoll.action_class",
    "request_id", "rpc.method", "rpc.service", "rpc.system", "serviceName",
    "serviceVersion", "spanId", "status.code", "stitched", "tool",
    "toolClassDetail", "toolName", "tool_name", "traceId", "type", "usageSource",
  ];
  const credentialShapes = [
    "sk_live_OUTBOX_VALUE_SENTINEL",
    "sk_test_OUTBOX_VALUE_SENTINEL",
    "sk-OUTBOX_VALUE_SENTINEL",
    "ghp_OUTBOX_VALUE_SENTINEL",
    "github_pat_OUTBOX_VALUE_SENTINEL",
    "xoxb-OUTBOX-VALUE-SENTINEL",
    "eyJproofheader.payloadproof.signatureproof",
    "Bearer OUTBOX_VALUE_SENTINEL",
    "Basic T1VUQk9YX1ZBTFVFX1NFTlRJTkVM",
    "-----BEGIN PRIVATE KEY----- OUTBOX_VALUE_SENTINEL",
    "credential_OUTBOX_VALUE_SENTINEL",
    "secret_OUTBOX_VALUE_SENTINEL",
    "password_OUTBOX_VALUE_SENTINEL",
    "token_OUTBOX_VALUE_SENTINEL",
    "apiKeyOUTBOXValueSentinel",
    "privateKeyOUTBOXValueSentinel",
  ];
  for (const key of allowedStringKeys) {
    const keyIndex = allowedStringKeys.indexOf(key);
    for (const sentinel of [
      `relative/path/${key}`,
      `relative%2Fpath-${key}`,
      `relative\\path-${key}`,
      `https://private.invalid/${key}`,
      `private.${keyIndex}@example.invalid`,
      `multibyte-密-${keyIndex}`,
      `${credentialShapes[keyIndex % credentialShapes.length]}_${keyIndex}`,
    ]) {
      rejected.push({ metadata: { [key]: sentinel }, sentinel });
    }
  }
  rejected.forEach((entry, index) => {
    buffer.append(event(3_000 + index, { metadata: entry.metadata }));
  });

  const topLevelRejected: Array<{ field: string; sentinel: string }> = [];
  const topLevelFields = [
    "id", "sessionId", "tenantId", "actorId", "projectKey",
    "customerKey", "workflowKey", "model",
  ];
  for (const [fieldIndex, field] of topLevelFields.entries()) {
    for (const sentinel of [
      `${credentialShapes[fieldIndex]}_TOP_${fieldIndex}`,
      `relative/path/top-${fieldIndex}`,
      `https://private.invalid/top-${fieldIndex}`,
      `top.${fieldIndex}@example.invalid`,
      `relative\\top-${fieldIndex}`,
      `multibyte-密-top-${fieldIndex}`,
    ]) {
      topLevelRejected.push({ field, sentinel });
    }
  }
  topLevelRejected.push(
    { field: "projectKey", sentinel: "arbitrary-project-key" },
    { field: "projectKey", sentinel: "a".repeat(40) },
  );
  topLevelRejected.forEach((entry, index) => {
    buffer.append(event(3_700 + index, { [entry.field]: entry.sentinel }));
  });

  const omitted = [
    ["remoteHash", "UNKNOWN_REMOTE_HASH_SENTINEL"],
    ["cwd", "CWD_SENTINEL"],
    ["sеcret", "CYRILLIC_SECRET_SENTINEL"],
    ["passwоrd", "CYRILLIC_PASSWORD_SENTINEL"],
    ["api🔑Key", "EMOJI_KEY_SENTINEL"],
    ["密钥", "CJK_KEY_SENTINEL"],
  ] as const;
  const omittedIds = new Map<string, string>();
  omitted.forEach(([key, sentinel], index) => {
    const id = uuid(4_000 + index);
    omittedIds.set(id, key);
    buffer.append(event(4_000 + index, { metadata: { [key]: sentinel } }));
  });

  const invalidLinkageId = uuid(4_100);
  buffer.append(event(4_100, {
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

  const canonicalLinkageId = uuid(4_101);
  buffer.append(event(4_101, { metadata: { transport_path: "/v1/metrics" } }));
  const canonicalRowid = (buffer.database
    .prepare(`select rowid as rowid from buffered_events where id = ?`)
    .get(canonicalLinkageId) as { rowid: number }).rowid;
  const upperCanonical = `SHA256:${"AB".repeat(32)}`;
  const lowerCanonical = `sha256:${"ab".repeat(32)}`;
  buffer.delivery.fillLinkageForRawRow(canonicalRowid, upperCanonical, null);

  const metadataLinkageId = uuid(4_102);
  const branchCanonical = `sha256:${"cd".repeat(32)}`;
  const headSha = "e".repeat(40);
  buffer.append(event(4_102, {
    metadata: {
      transport_path: "/v1/logs",
      serviceName: "collector-proof",
      toolName: "codex",
      originator: "local_cli",
      "gen_ai.response.id": "resp_proof_001",
      otelEventName: "thread/resume",
      "event.name": "persist/rollout/items",
      otelSignalNames: ["thread/read", "safe_signal"],
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
  const rejectedSentinels = rejected.map((entry) => entry.sentinel);
  const omittedSentinels = omitted.map(([, sentinel]) => sentinel);
  const forbidden = [
    ...rejectedSentinels,
    ...topLevelRejected.map((entry) => entry.sentinel),
    ...omittedSentinels,
    malformedLinkage,
  ];
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
  const unknownKeysOmitted = [...omittedIds.entries()].every(([id, key]) => {
    const delivered = sent.events.find((entry) => entry.event.id === id)?.event;
    return delivered !== undefined && !(key in delivered.metadata);
  });
  record(
    "normalized_sensitive_keys_and_noncanonical_linkage_never_enter_delivery_surfaces",
    deadBefore.length === rejected.length + topLevelRejected.length &&
      deadBefore.every((row) => row.reason === "local_privacy_violation") &&
      beforeRows.length === omitted.length + 3 &&
      beforeRows.find((row) => row.id === invalidLinkageId)?.repoHash === null &&
      beforeRows.find((row) => row.id === canonicalLinkageId)?.repoHash === lowerCanonical &&
      uploaded.uploadedEvents === omitted.length + 3 &&
      absentEverywhere &&
      unknownKeysOmitted &&
      invalidSent?.projectKey === undefined &&
      canonicalSent?.projectKey === lowerCanonical &&
      invalidSent?.metadata.transport_path === "/v1/traces" &&
      invalidSent?.inputTokens === 4_101 &&
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
      metadataGit?.headSha === headSha &&
      metadataLinkageSent?.metadata.serviceName === "collector-proof" &&
      metadataLinkageSent?.metadata.toolName === "codex" &&
      metadataLinkageSent?.metadata.originator === "local_cli" &&
      metadataLinkageSent?.metadata["gen_ai.response.id"] === "resp_proof_001" &&
      metadataLinkageSent?.metadata.otelEventName === "thread/resume" &&
      metadataLinkageSent?.metadata["event.name"] === "persist/rollout/items" &&
      JSON.stringify(metadataLinkageSent?.metadata.otelSignalNames) === JSON.stringify(["thread/read", "safe_signal"]),
    {
      hostileDead: deadBefore.length,
      metadataValueCases: rejected.length,
      topLevelValueCases: topLevelRejected.length,
      activeBefore: beforeRows.length,
      uploaded: uploaded.uploadedEvents,
      absentEverywhere,
      unknownKeysOmitted,
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
  record(
    "allowed_metadata_and_top_level_string_value_matrix_fails_closed",
    allowedStringKeys.every((key) => rejected.filter((entry) => key in entry.metadata).length >= 7) &&
      topLevelFields.every((field) => topLevelRejected.filter((entry) => entry.field === field).length >= 6) &&
      credentialShapes.every((shape) => rejected.some((entry) => entry.sentinel.includes(shape))) &&
      absentEverywhere,
    {
      metadataKeys: allowedStringKeys.length,
      metadataCases: rejected.length,
      topLevelFields: topLevelFields.length,
      topLevelCases: topLevelRejected.length,
      credentialShapes: credentialShapes.length,
    },
  );
  buffer.close();
}

async function requestBudgetAndResumableValidationProof() {
  {
    const { buffer, cfg } = enabledBuffer(undefined, {
      maxItemBytes: 20_000,
      maxProbesPerCycle: 8,
    });
    const oversizedId = uuid(2_300);
    const laterId = uuid(2_301);
    buffer.append(event(2_300, { metadata: bulkyAllowedMetadata(64) }));
    buffer.append(aiInteractionEventSchema.parse({
      id: laterId,
      source: "codex",
      dataMode: "metadata",
      eventType: "unknown",
      observedAt: instant(4_000).toISOString(),
      metadata: {},
    }));
    const requestBodies: string[] = [];
    const partial = await uploadBufferedEvents(cfg, buffer, {
      limit: 2,
      maxBytes: 512,
      fetchImpl: async (_input, init) => {
        requestBodies.push(String(init?.body ?? ""));
        return response(200, { accepted: requestIds(init).length });
      },
      now: () => instant(4_001),
    });
    const deferred = buffer.database
      .prepare(
        `select state, last_failure_class as failure
         from upload_outbox where delivery_id = ?`,
      )
      .get(oversizedId) as { state: string; failure: string };
    const firstState = buffer.database
      .prepare(
        `select id, uploaded_at as uploadedAt from buffered_events
         where id in (?, ?) order by id`,
      )
      .all(oversizedId, laterId) as Array<{ id: string; uploadedAt: string | null }>;
    const deadBeforeRaise = (buffer.database
      .prepare(`select count(*) as n from upload_receipts where terminal_state = 'dead'`)
      .get() as { n: number }).n;
    const firstRequestBytes = requestBodies.map((body) => Buffer.byteLength(body));
    const raised = await uploadBufferedEvents(cfg, buffer, {
      limit: 2,
      maxBytes: 20_000,
      fetchImpl: async (_input, init) => {
        requestBodies.push(String(init?.body ?? ""));
        return response(200, { accepted: requestIds(init).length });
      },
      now: () => instant(4_100),
    });
    const finalStatus = buffer.delivery.status(instant(4_100));
    record(
      "serialized_request_cap_defers_oversize_without_starving_later_work",
      partial.uploadedEvents === 1 &&
        firstRequestBytes.length === 1 &&
        firstRequestBytes.every((bytes) => bytes <= 512) &&
        requestIds({ body: requestBodies[0] }).join(",") === laterId &&
        deferred.state === "retry" &&
        deferred.failure === "local_request_budget" &&
        firstState.find((row) => row.id === oversizedId)?.uploadedAt === null &&
        firstState.find((row) => row.id === laterId)?.uploadedAt !== null &&
        deadBeforeRaise === 0 &&
        raised.uploadedEvents === 1 &&
        requestIds({ body: requestBodies[1] }).join(",") === oversizedId &&
        finalStatus.remainingDelivery === 0 &&
        finalStatus.circuit.kind === "none",
      {
        firstRequestBytes,
        firstRequestIds: requestIds({ body: requestBodies[0] }),
        deferred,
        firstState,
        deadBeforeRaise,
        raisedUploaded: raised.uploadedEvents,
        finalRemaining: finalStatus.remainingDelivery,
      },
    );
    buffer.close();
  }

  {
    const { buffer, cfg } = enabledBuffer(undefined, {
      maxProbesPerCycle: 1,
      maxBackoffSeconds: 30,
    });
    const firstId = uuid(2_400);
    const poisonId = uuid(2_401);
    const laterId = uuid(2_402);
    buffer.append(event(2_400, { metadata: { serviceName: "valid-first" } }));
    buffer.append(event(2_401, { metadata: { serviceName: "poison" } }));
    buffer.append(event(2_402, { metadata: { serviceName: "valid-later" } }));
    const groups: string[][] = [];
    const rootLeaseSizes: number[] = [];
    const fetchImpl: typeof fetch = async (_input, init) => {
      const ids = requestIds(init);
      groups.push(ids);
      return ids.includes(poisonId)
        ? response(422, { error: "proof poison" })
        : response(200, { accepted: ids.length });
    };
    await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        limit: 12,
        maxProbes: 1,
        fetchImpl,
        now: () => instant(4_200),
      }),
      "remote_validation",
    );
    const capAfterSplit = (buffer.database
      .prepare(`select validation_probe_rows as n from upload_control where singleton = 1`)
      .get() as { n: number }).n;

    const backlogIds: string[] = [];
    for (let n = 2_410; n < 2_422; n += 1) {
      backlogIds.push(uuid(n));
      buffer.append(event(n, { metadata: { serviceName: `valid-backlog-${n}` } }));
    }
    const observedCaps: number[] = [];
    for (let cycle = 0; cycle < 12; cycle += 1) {
      try {
        const result = await uploadBufferedEvents(cfg, buffer, {
          limit: 12,
          maxProbes: 1,
          fetchImpl,
          now: () => instant(4_300 + cycle * 40),
        });
        rootLeaseSizes.push(
          "rootLeaseEvents" in result.delivery
            ? result.delivery.rootLeaseEvents
            : 0,
        );
      } catch (error) {
        if (!(error instanceof DeliveryUploadError)) throw error;
      }
      observedCaps.push((buffer.database
        .prepare(`select validation_probe_rows as n from upload_control where singleton = 1`)
        .get() as { n: number }).n);
      const uploadedValid = (buffer.database
        .prepare(
          `select count(*) as n from buffered_events
           where id <> ? and uploaded_at is not null`,
        )
        .get(poisonId) as { n: number }).n;
      if (uploadedValid === backlogIds.length + 2) break;
    }
    const validState = buffer.database
      .prepare(
        `select id, uploaded_at as uploadedAt from buffered_events
         where id <> ?`,
      )
      .all(poisonId) as Array<{ id: string; uploadedAt: string | null }>;
    const poisonReceipts = buffer.database
      .prepare(`select reason from upload_receipts where delivery_id = ?`)
      .all(poisonId) as Array<{ reason: string }>;
    const finalStatus = buffer.delivery.status(instant(4_500));
    const initialGroupRepeats = groups.filter((ids) => ids.join(",") === [firstId, poisonId, laterId].join(",")).length;
    record(
      "max_probe_one_resumes_isolation_and_adaptively_recovers_throughput",
      capAfterSplit === 1 &&
        rootLeaseSizes.slice(0, 3).join(",") === "1,2,4" &&
        observedCaps.includes(2) &&
        observedCaps.includes(4) &&
        validState.every((row) => row.uploadedAt !== null) &&
        initialGroupRepeats <= 2 &&
        groups.slice(1, 4).every((ids) => !ids.includes(poisonId)) &&
        poisonReceipts.every((row) => row.reason === "remote_validation_rejected") &&
        finalStatus.circuit.kind === "none",
      {
        capAfterSplit,
        rootLeaseSizes,
        observedCaps,
        groupSizes: groups.map((ids) => ids.length),
        initialGroupRepeats,
        uploadedValid: validState.filter((row) => row.uploadedAt !== null).length,
        expectedValid: backlogIds.length + 2,
        poisonReceipts,
        circuit: finalStatus.circuit.kind,
      },
    );
    buffer.close();
  }
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
    buffer.append(event(171, { metadata: bulkyAllowedMetadata(8) }));
    const status = buffer.delivery.status(instant(1001));
    record("active_byte_pressure_exact", status.active.bytes > 200 && status.pressure.reasons.includes("byte_budget"), { activeBytes: status.active.bytes, budget: status.pressure.budgets.bytes });
    buffer.close();
  }
  {
    const { buffer } = enabledBuffer(undefined, { maxItemBytes: 1_024 });
    buffer.append(event(172, { metadata: bulkyAllowedMetadata(16) }));
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
    const rawLinkagePlan = buffer.database
      .prepare(
        `explain query plan
         update upload_outbox set repo_hash = coalesce(repo_hash, ?)
         where raw_rowid = ? and sealed_envelope_json is null and attempt_count = 0`,
      )
      .all(`sha256:${"ab".repeat(32)}`, 1) as Array<{ detail: string }>;
    const witnessCandidatePlan = buffer.database
      .prepare(
        `explain query plan
         select 1 from upload_validation_candidates c
         join upload_outbox o on o.delivery_id = c.delivery_id
         where c.contract_hash = ? and c.failed_at >= ? limit 1`,
      )
      .all(`sha256:${"cd".repeat(32)}`, instant(1_100).toISOString()) as Array<{ detail: string }>;
    record(
      "status_uses_singleton_gauges_not_history_aggregates",
      status.remainingDelivery === 100 && status.work.controlRowsRead === 1 && status.work.activeRowsScanned === 0 && status.work.receiptRowsScanned === 0 && status.work.rawRowsScanned === 0 && plan.some((row) => /primary key|integer primary key/i.test(row.detail)),
      { remaining: status.remainingDelivery, work: status.work, plan: plan.map((row) => row.detail).join(" | ") },
    );
    record(
      "raw_rowid_fill_linkage_uses_dedicated_index",
      rawLinkagePlan.some((row) =>
        /search upload_outbox using index idx_upload_outbox_raw_rowid/i.test(row.detail),
      ),
      { plan: rawLinkagePlan.map((row) => row.detail).join(" | ") },
    );
    record(
      "validation_witness_reprobe_uses_contract_failure_index",
      witnessCandidatePlan.some((row) =>
        /idx_upload_validation_candidates_contract_failure/i.test(row.detail),
      ),
      { plan: witnessCandidatePlan.map((row) => row.detail).join(" | ") },
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
    await requestBudgetAndResumableValidationProof();
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
