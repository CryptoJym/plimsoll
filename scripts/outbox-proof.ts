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
    await localPoisonProof();
    await remotePoisonPositionProof();
    await globalContractAndAuthProof();
    await retryAndCrashProof();
    await linkageAndRetentionProof();
    await noMarkPressureAndPrivacyProof();
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
