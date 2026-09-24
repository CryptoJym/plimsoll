/**
 * eco-6hoxj.163.9 r2: session attribution on the upload path costs a bounded
 * amount independent of session size, and its results equal the per-event
 * rule (2c20400e) whenever no bound is reached.
 *
 * The file runs against any collector build.  APIs that only exist on the
 * bounded build are feature-detected; a missing one is a recorded failure, so
 * the same proof is red on 2c20400e and green on the fix.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";

import type Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { canonicalLinkage, sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
// Named imports, not namespace imports: proof:capacity treats a namespace
// import of a module that can reach capacity code as capacity consumption.
// Head-only exports resolve to undefined on older builds, which the feature
// checks below report.
import {
  applyProjectAttribution,
  SESSION_INHERIT_MAX_SCANNED_ROWS,
  SessionAttributionBatch,
  type SessionRepoContext,
} from "../packages/collector-cli/src/session-attribution";
import {
  prepareHistoryEvent,
  runWorkspaceHistoryUpload,
  sealHistoryEvent,
} from "../packages/collector-cli/src/upload-history";
import { buildIngestBatch } from "../packages/collector-cli/src/upload";
import {
  aiWorkIngestBatchSchema,
  aiWorkIngestEventSchema,
  type AiInteractionEvent,
  type AiWorkIngestEvent,
} from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const HOUR = 60 * 60 * 1_000;
const WINDOW_MS = 6 * HOUR;
const T0 = Date.parse("2026-09-23T12:00:00.000Z");
const WORKSPACE = "fixture-workspace";
const DEVICE = "fixture-device";
const REPO_A = `sha256:${"a".repeat(64)}`;
const REPO_B = `sha256:${"b".repeat(64)}`;
const REPO_C = `sha256:${"c".repeat(64)}`;
const BRANCH_A = `sha256:${"d".repeat(64)}`;
const REPO_VALUES = [REPO_A, REPO_B, REPO_C, `sha256:${"A".repeat(64)}`, `sha256:${"z".repeat(64)}`, "plain-text"];

// The brief's production shape: one Codex session with 300k events inside six
// hours, 150 of them (0.05%) carrying a resolved repo hash, and a 500-row
// upload batch of token rows at the end of that window.
const BUSY_SESSION = "busy-session";
const BUSY_EVENTS = 300_000;
const BUSY_REPO_EVERY = 2_000;
const UPLOAD_ROWS = 500;
// A regression guard, not a benchmark: the per-row rule this replaced cost
// 22.5 s for this batch on Studio3. The bounded path measured 44-70 ms there
// and 287 ms on the GitHub macos-14 runner. The lookup-count checks below are
// the structural proof that the cost no longer depends on session size.
const BUDGET_MS = 2_000;

type Check = { name: string; ok: boolean; detail?: unknown; error?: string };
const checks: Check[] = [];
const measurements: Record<string, unknown> = {};

async function check(name: string, run: () => unknown | Promise<unknown>) {
  try {
    const detail = await run();
    checks.push({ name, ok: true, ...(detail === undefined ? {} : { detail }) });
  } catch (error) {
    checks.push({ name, ok: false, error: error instanceof Error ? error.message : String(error) });
  }
}

function expect(condition: unknown, message: string, detail?: unknown): asserts condition {
  if (!condition) throw new Error(detail === undefined ? message : `${message}: ${JSON.stringify(detail)}`);
}

const iso = (ms: number) => new Date(ms).toISOString();
const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;

/** An observed_at whose text order differs from its time order. */
function offsetIso(ms: number, offsetMinutes: number) {
  const local = new Date(ms + offsetMinutes * 60_000).toISOString().slice(0, 23);
  const magnitude = Math.abs(offsetMinutes);
  return `${local}${offsetMinutes < 0 ? "-" : "+"}${String(Math.floor(magnitude / 60)).padStart(2, "0")}:${String(magnitude % 60).padStart(2, "0")}`;
}

function batchApi() {
  const Batch = SessionAttributionBatch as typeof SessionAttributionBatch | undefined;
  expect(Batch, "SessionAttributionBatch is not available in this build");
  return Batch;
}

function historyApi() {
  const prepare = prepareHistoryEvent as typeof prepareHistoryEvent | undefined;
  const seal = sealHistoryEvent as typeof sealHistoryEvent | undefined;
  expect(prepare && seal, "prepareHistoryEvent/sealHistoryEvent are not available in this build");
  return { prepare, seal };
}

const TOKEN_FIELDS = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens"] as const;

function regexLinkage(value: unknown) {
  if (typeof value !== "string") return null;
  const candidate = value.trim().toLowerCase();
  return /^sha256:[a-f0-9]{64}$/.test(candidate) ? candidate : null;
}

/**
 * The unbounded reference rule: 2c20400e's readSessionRepoContexts verbatim
 * (one query per event, returned rows capped at 257, scanned rows unbounded)
 * feeding the unchanged applyProjectAttribution.
 */
function unboundedAttribution(
  db: Database.Database,
  event: AiInteractionEvent,
  repoHash: string | null | undefined,
  branchHash: string | null | undefined,
) {
  let rows: SessionRepoContext[] = [];
  let truncated = false;
  if (
    event.sessionId && !event.projectKey && !regexLinkage(repoHash) &&
    TOKEN_FIELDS.some((field) => event[field] !== undefined)
  ) {
    const eventAt = Date.parse(event.observedAt);
    if (Number.isFinite(eventAt)) {
      const found = db
        .prepare(
          `select rowid, session_id as sessionId, observed_at as observedAt,
             repo_hash as repoHash
           from buffered_events
           where session_id = ? and repo_hash is not null
             and data_mode <> 'evidence' and privacy_disposition is null
             and observed_at >= ? and observed_at <= ?
           order by observed_at asc, rowid asc
           limit ?`,
        )
        .all(event.sessionId, iso(eventAt - WINDOW_MS), iso(eventAt + WINDOW_MS), 257) as SessionRepoContext[];
      truncated = found.length > 256;
      rows = found.slice(0, 256).filter((row) => regexLinkage(row.repoHash) !== null);
    }
  }
  return applyProjectAttribution(event, {
    repoHash,
    branchHash,
    sessionContexts: rows,
    sessionContextsTruncated: truncated,
  });
}

/** The fail-closed result: the rule with a truncated (unusable) session. */
function failClosedAttribution(
  event: AiInteractionEvent,
  repoHash: string | null | undefined,
  branchHash: string | null | undefined,
) {
  return applyProjectAttribution(event, {
    repoHash,
    branchHash,
    sessionContexts: [],
    sessionContextsTruncated: true,
  });
}

/** Counts executions of every session-index lookup statement prepared on `db`. */
const SESSION_LOOKUP_SQL = /from buffered_events(?:\s+indexed by idx_events_session)?\s+where session_id = \?/;
function instrumentSessionLookups(db: Database.Database) {
  const original = db.prepare;
  const statements: string[] = [];
  let executions = 0;
  db.prepare = function prepare(this: Database.Database, source: string) {
    const statement = original.call(this, source);
    if (!SESSION_LOOKUP_SQL.test(source)) return statement;
    statements.push(source);
    for (const method of ["get", "all", "iterate"] as const) {
      const run = statement[method].bind(statement) as (...args: unknown[]) => unknown;
      (statement as unknown as Record<string, unknown>)[method] = (...args: unknown[]) => {
        executions += 1;
        return run(...args);
      };
    }
    return statement;
  } as typeof db.prepare;
  return {
    executions: () => executions,
    statements: () => [...statements],
    restore: () => {
      db.prepare = original;
    },
  };
}

function openLedger(file: string) {
  return new LocalEventBuffer(file, {
    workspaceId: WORKSPACE,
    deviceId: DEVICE,
    enrollmentNow: () => new Date(T0 - 48 * HOUR),
    delivery: { enabled: true, now: () => new Date(T0) },
  });
}

/**
 * eco-6hoxj.163.21: a ledger whose capture-time context index is complete
 * attributes through it (session-context-index-proof). The checks that pin
 * the 0.7.36 session scan, which still serves every window of an older ledger
 * until its backfill completes, mark their fixture's index as not yet covered.
 * On builds without the index this is a no-op.
 */
function useSessionScan(buffer: LocalEventBuffer) {
  const installed = buffer.database
    .prepare(`select 1 from sqlite_master where type = 'table' and name = 'session_repo_context_control'`)
    .get();
  if (installed) {
    buffer.database.prepare(`update session_repo_context_control set backfill_complete = 0 where singleton = 1`).run();
  }
  return buffer;
}

function tokenEvent(
  id: string,
  sessionId: string | undefined,
  observedAt: string,
  extra: Partial<AiInteractionEvent> = {},
): AiInteractionEvent {
  return {
    id,
    ...(sessionId ? { sessionId } : {}),
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt,
    intent: "unknown",
    actionClass: "other",
    inputTokens: 1_000,
    outputTokens: 50,
    metadata: {},
    ...extra,
  };
}

function contextEvent(id: string, sessionId: string | undefined, observedAt: string): AiInteractionEvent {
  return {
    id,
    ...(sessionId ? { sessionId } : {}),
    source: "codex",
    dataMode: "metadata",
    eventType: "tool_result",
    observedAt,
    intent: "unknown",
    actionClass: "other",
    metadata: {},
  };
}

function appendAll(buffer: LocalEventBuffer, events: AiInteractionEvent[]) {
  for (const event of events) expect(buffer.append(event), `append refused ${event.id}`);
}

/** Simulates repo-context resolution: the ledger row, then fill-only outbox linkage. */
function resolveRepo(
  buffer: LocalEventBuffer,
  id: string,
  repoHash: string,
  branchHash: string | null,
  fillOutbox: boolean,
) {
  buffer.database
    .prepare("update buffered_events set repo_hash = ?, branch_hash = ? where id = ?")
    .run(repoHash, branchHash, id);
  if (!fillOutbox) return;
  const row = buffer.database.prepare("select rowid from buffered_events where id = ?").get(id) as { rowid: number };
  buffer.delivery.fillLinkageForRawRow(row.rowid, repoHash, branchHash);
}

function insertFiller(
  db: Database.Database,
  rows: Array<{ id: string; sessionId: string; observedAt: string; repoHash?: string | null; dataMode?: string }>,
) {
  const insert = db.prepare(
    `insert into buffered_events
       (id, source, event_type, data_mode, observed_at, payload_json,
        suppressed_fields_json, created_at, session_id, repo_hash,
        workspace_id, device_id, privacy_generation)
     values (@id, 'codex', 'otel_span', @dataMode, @observedAt, @payload, '[]',
       @observedAt, @sessionId, @repoHash, @workspace, @device, 'fixture-generation')`,
  );
  const payload = JSON.stringify({ spanName: "tool.call", attributes: "x".repeat(360) });
  db.transaction(() => {
    for (const row of rows) {
      insert.run({
        id: row.id,
        sessionId: row.sessionId,
        observedAt: row.observedAt,
        repoHash: row.repoHash ?? null,
        dataMode: row.dataMode ?? "metadata",
        payload,
        workspace: WORKSPACE,
        device: DEVICE,
      });
    }
  })();
}

type PendingInput = {
  deliveryId: string;
  envelope: AiWorkIngestEvent;
  event: AiInteractionEvent;
  repoHash: string | null;
  branchHash: string | null;
};

/** The unsealed outbox rows exactly as lease() parses them. */
function pendingInputs(db: Database.Database): PendingInput[] {
  const rows = db
    .prepare(
      `select delivery_id as deliveryId, base_envelope_json as baseEnvelopeJson,
         repo_hash as repoHash, branch_hash as branchHash
       from upload_outbox
       where sealed_envelope_json is null and state in ('pending','retry')`,
    )
    .all() as Array<{ deliveryId: string; baseEnvelopeJson: string; repoHash: string | null; branchHash: string | null }>;
  return rows.flatMap((row) => {
    const envelope = aiWorkIngestEventSchema.parse(JSON.parse(row.baseEnvelopeJson));
    if (envelope.event.dataMode === "evidence") return [];
    return [{
      deliveryId: row.deliveryId,
      envelope,
      event: envelope.event,
      repoHash: canonicalLinkage(row.repoHash),
      branchHash: canonicalLinkage(row.branchHash),
    }];
  });
}

/** What lease() must seal for each pending row under the unbounded rule. */
function expectedLeaseEnvelopes(db: Database.Database) {
  const expected = new Map<string, string>();
  for (const input of pendingInputs(db)) {
    const attributed = unboundedAttribution(db, input.event, input.repoHash, input.branchHash);
    const sealed = sealOutboundEnvelope({ ...input.envelope, event: attributed.event });
    if (sealed.ok) expected.set(input.deliveryId, JSON.stringify(sealed.envelope));
  }
  return expected;
}

function basisOf(event: AiInteractionEvent) {
  return typeof event.metadata?.projectBasis === "string" ? event.metadata.projectBasis : "none";
}

function mulberry32(seed: number) {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

/**
 * A small randomized ledger with every attribution shape: single- and
 * multi-repo sessions, future-only context, uppercase and invalid hashes,
 * evidence and privacy-disposed context rows, contexts outside the window,
 * offset timestamps whose text order differs from time order, explicit and
 * forged-marker projects, own repo linkage, non-token rows, rows without a
 * session, and (sometimes) a dense session over the 256-context cap.
 */
function buildScenario(file: string, seed: number) {
  const random = mulberry32(seed);
  const pick = <T>(values: readonly T[]) => values[Math.floor(random() * values.length)]!;
  const observedAt = (ms: number) => {
    const roll = random();
    if (roll < 0.8) return iso(ms);
    return offsetIso(ms, roll < 0.9 ? 120 : -300);
  };
  const buffer = openLedger(file);
  const sessions = ["s0", "s1", "s2"];
  let next = seed * 10_000;
  const at = () => T0 + Math.round((random() * 20 - 10) * HOUR);

  for (let index = 0; index < 60; index += 1) {
    const event = contextEvent(uuid(next += 1), random() < 0.95 ? pick(sessions) : undefined, observedAt(at()));
    appendAll(buffer, [event]);
    if (random() < 0.8) resolveRepo(buffer, event.id, pick(REPO_VALUES), random() < 0.5 ? BRANCH_A : null, random() < 0.5);
    if (random() < 0.1) {
      buffer.database
        .prepare("update buffered_events set privacy_disposition = 'local_privacy_violation', privacy_disposed_at = ? where id = ?")
        .run(iso(T0), event.id);
    }
  }
  insertFiller(buffer.database, Array.from({ length: 6 }, () => ({
    id: uuid(next += 1),
    sessionId: pick(sessions),
    observedAt: iso(at()),
    repoHash: pick([REPO_A, REPO_B]),
    dataMode: "evidence",
  })));
  if (random() < 0.5) {
    const dense = Array.from({ length: 300 }, (_, index) => contextEvent(uuid(next += 1), "dense", iso(T0 + 5 * HOUR + index * 10_000)));
    appendAll(buffer, dense);
    for (const event of dense) resolveRepo(buffer, event.id, REPO_B, null, false);
  }
  for (let index = 0; index < 120; index += 1) {
    const sessionId = random() < 0.9 ? pick([...sessions, "dense"]) : undefined;
    const roll = random();
    const extra: Partial<AiInteractionEvent> = roll < 0.08
      ? { projectKey: pick([REPO_A, "producer-project"]) }
      : roll < 0.12
        ? { projectKey: REPO_B, metadata: { projectBasis: "session_inherited" } }
        : {};
    const id = uuid(next += 1);
    const event = random() < 0.85
      ? tokenEvent(id, sessionId, observedAt(sessionId === "dense" ? T0 + Math.round((random() * 14 - 2) * HOUR) : at()), extra)
      : { ...contextEvent(id, sessionId, observedAt(at())), ...extra };
    appendAll(buffer, [event]);
    if (random() < 0.12) resolveRepo(buffer, event.id, pick(REPO_VALUES), random() < 0.5 ? BRANCH_A : null, random() < 0.7);
  }
  return buffer;
}

async function largeSessionBudget(dir: string) {
  const busy = useSessionScan(openLedger(path.join(dir, "busy.sqlite")));
  let started = performance.now();
  insertFiller(busy.database, Array.from({ length: BUSY_EVENTS }, (_, index) => ({
    id: `busy-${index}`,
    sessionId: BUSY_SESSION,
    observedAt: iso(T0 - WINDOW_MS + Math.floor((index * WINDOW_MS) / BUSY_EVENTS)),
  })));
  measurements.fixtureBuildMs = Math.round(performance.now() - started);
  measurements.fixture = {
    busySessionEvents: BUSY_EVENTS,
    busySessionRepoRows: Math.ceil(BUSY_EVENTS / BUSY_REPO_EVERY),
    spanHours: 6,
    uploadRows: UPLOAD_ROWS,
  };

  // The upload batch: 500 token rows at the end of the window, already in the
  // ledger (as legacy migration, replay and capture repair find them).
  const uploadEvents = Array.from({ length: UPLOAD_ROWS }, (_, index) =>
    tokenEvent(uuid(0x20_0000 + index), BUSY_SESSION, iso(T0 - 60_000 + index * 100)));
  const insertRaw = busy.database.prepare(
    `insert into buffered_events
       (id, source, event_type, data_mode, observed_at, payload_json,
        suppressed_fields_json, created_at, session_id, input_tokens,
        output_tokens, workspace_id, device_id, privacy_generation)
     values (@id, @source, @eventType, @dataMode, @observedAt, @payload, '[]',
       @observedAt, @sessionId, @inputTokens, @outputTokens, @workspace, @device,
       'fixture-generation')`,
  );
  busy.database.transaction(() => {
    for (const event of uploadEvents) {
      insertRaw.run({ ...event, payload: JSON.stringify(event), workspace: WORKSPACE, device: DEVICE });
    }
  })();

  // prepareDelivery for the whole batch (enqueueRaw via the replay entry point).
  const enqueueLookups = instrumentSessionLookups(busy.database);
  started = performance.now();
  let enqueued = 0;
  for (const event of uploadEvents) enqueued += busy.delivery.repairRawById(event.id).enqueued;
  const enqueueMs = performance.now() - started;
  enqueueLookups.restore();

  // Repo-context resolution lands after capture, the usual production order:
  // 150 of the session's rows (0.05%) now carry REPO_A.
  const resolve = busy.database.prepare("update buffered_events set repo_hash = ? where id = ?");
  busy.database.transaction(() => {
    for (let index = 0; index < BUSY_EVENTS; index += BUSY_REPO_EVERY) resolve.run(REPO_A, `busy-${index}`);
  })();

  // The per-row rule on this ledger, measured once: what each token row paid.
  started = performance.now();
  const reference = unboundedAttribution(busy.database, uploadEvents[0]!, null, null);
  measurements.unboundedLookupMsPerTokenRow = Math.round((performance.now() - started) * 10) / 10;
  measurements.unboundedRuleResultForBusyRow = {
    basis: basisOf(reference.event),
    projectKey: reference.event.projectKey ?? null,
  };

  const leaseLookups = instrumentSessionLookups(busy.database);
  started = performance.now();
  const lease = busy.delivery.lease({ maxRows: UPLOAD_ROWS, maxBytes: 10_000_000, now: new Date(T0 + 60_000) });
  const leaseMs = performance.now() - started;
  leaseLookups.restore();

  // Live capture of a further 500-row batch (the OTLP appendMany path).
  const captureEvents = Array.from({ length: UPLOAD_ROWS }, (_, index) =>
    tokenEvent(uuid(0x21_0000 + index), BUSY_SESSION, iso(T0 - 30_000 + index * 50)));
  const captureLookups = instrumentSessionLookups(busy.database);
  started = performance.now();
  const captured = busy.appendMany(captureEvents.map((event) => ({ event, suppressedFields: [] })));
  const captureMs = performance.now() - started;
  captureLookups.restore();

  measurements.prepareDeliveryBatch = {
    rows: enqueued,
    enqueueMs: Math.round(enqueueMs * 10) / 10,
    sessionLookupExecutions: enqueueLookups.executions(),
  };
  measurements.leaseBatch = {
    rows: lease.items.length,
    leaseMs: Math.round(leaseMs * 10) / 10,
    sessionLookupExecutions: leaseLookups.executions(),
  };
  measurements.captureBatch = {
    rows: UPLOAD_ROWS,
    appendManyMs: Math.round(captureMs),
    sessionLookupExecutions: captureLookups.executions(),
  };

  await check("prepare_delivery_for_500_token_rows_of_a_300k_event_session_is_under_2s", () => {
    expect(enqueued === UPLOAD_ROWS, "batch was not enqueued", enqueued);
    expect(enqueueMs < BUDGET_MS, `enqueue took ${enqueueMs.toFixed(1)} ms (budget ${BUDGET_MS} ms)`);
    expect(enqueueLookups.executions() === 0, "prepareDelivery still issues session lookups", enqueueLookups.executions());
    return measurements.prepareDeliveryBatch;
  });
  await check("lease_of_500_token_rows_from_a_300k_event_session_is_under_2s", () => {
    expect(lease.items.length === UPLOAD_ROWS, "lease did not return the whole batch", lease.items.length);
    expect(leaseMs < BUDGET_MS, `lease took ${leaseMs.toFixed(1)} ms (budget ${BUDGET_MS} ms)`);
    return measurements.leaseBatch;
  });
  await check("lease_issues_one_bounded_lookup_for_the_session_not_one_per_row", () => {
    // A lookup is one covering-index count plus, only when the window fits,
    // one bounded row read. The busy window does not fit.
    expect(leaseLookups.executions() <= 2, "lease issued more than one session lookup", leaseLookups.executions());
    return { executions: leaseLookups.executions() };
  });
  await check("bound_reached_fails_closed_to_unallocated_with_basis_label", () => {
    const wrong = lease.items.filter((item) =>
      item.envelope.event.projectKey !== undefined || basisOf(item.envelope.event) !== "unallocated");
    expect(wrong.length === 0, "a bound-limited row was attributed", wrong.slice(0, 3).map((item) => item.envelope.event));
    return {
      unallocated: lease.items.length,
      unboundedRuleWouldHaveSaid: measurements.unboundedRuleResultForBusyRow,
    };
  });
  await check("live_capture_of_token_rows_issues_no_session_attribution_lookup", () => {
    expect(captured.deduplicatedCount === 0 && captured.enrollmentRejectedEventCount === 0, "capture batch was not appended", captured);
    expect(captureLookups.executions() === 0, "capture/enqueue still issues session lookups", captureLookups.executions());
    return measurements.captureBatch;
  });
  await check("batch_stats_show_one_count_and_no_row_reads_for_an_oversized_window", () => {
    const Batch = batchApi();
    const batch = new Batch(busy.database, uploadEvents.map((event) => ({ event })));
    const stats = batch.stats();
    expect(
      stats.lookups === 1 && stats.boundReached === 1 && stats.rowReads === 0 &&
        stats.indexEntries === SESSION_INHERIT_MAX_SCANNED_ROWS + 1,
      "unexpected batch stats",
      stats,
    );
    measurements.busyBatchStats = stats;
    return stats;
  });
  busy.close();
}

async function equivalence(dir: string) {
  const config = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "http://127.0.0.1/fake-ingest",
    installKey: "session-attribution-proof-install",
    tenantId: WORKSPACE,
    deviceId: DEVICE,
  });
  const leaseBases = new Map<string, number>();
  let leaseCompared = 0;
  let noMarkCompared = 0;
  let historyCompared = 0;
  let boundedCompared = 0;
  let boundedFailClosed = 0;
  const failures: unknown[] = [];

  for (let seed = 1; seed <= 24; seed += 1) {
    const file = path.join(dir, `scenario-${seed}.sqlite`);
    const buffer = buildScenario(file, seed);
    const db = buffer.database;

    // Bounded batch with random small bounds: every result is the unbounded
    // rule's result or the fail-closed result, never another project.
    try {
      const Batch = batchApi();
      const random = mulberry32(seed + 1_000);
      const inputs = pendingInputs(db);
      const bounds = { maxScannedRows: Math.floor(random() * 120), maxBatchRowReads: Math.floor(random() * 400) };
      const batch = new Batch(db, inputs.map((input) => ({ event: input.event, repoHash: input.repoHash })), bounds);
      const stats = batch.stats();
      for (const input of inputs) {
        const actual = batch.attribute(input.event, { repoHash: input.repoHash, branchHash: input.branchHash });
        const reference = unboundedAttribution(db, input.event, input.repoHash, input.branchHash);
        boundedCompared += 1;
        if (isDeepStrictEqual(actual, reference)) continue;
        if (stats.boundReached + stats.budgetExhausted > 0 &&
            isDeepStrictEqual(actual, failClosedAttribution(input.event, input.repoHash, input.branchHash))) {
          boundedFailClosed += 1;
          continue;
        }
        failures.push({ path: "bounded_batch", seed, bounds, id: input.event.id, actual: actual.event, reference: reference.event });
      }
    } catch (error) {
      failures.push({ path: "bounded_batch", seed, error: error instanceof Error ? error.message : String(error) });
    }

    // The real lease path at default bounds equals the unbounded rule.
    const expectedLease = expectedLeaseEnvelopes(db);
    const lease = buffer.delivery.lease({ maxRows: 500, maxBytes: 10_000_000, now: new Date(T0 + 60_000) });
    for (const item of lease.items) {
      leaseCompared += 1;
      const basis = basisOf(item.envelope.event);
      leaseBases.set(basis, (leaseBases.get(basis) ?? 0) + 1);
      if (expectedLease.get(item.deliveryId) !== item.envelopeJson) {
        failures.push({ path: "lease", seed, id: item.deliveryId, actual: item.envelope.event, expected: expectedLease.get(item.deliveryId) ?? null });
      }
    }

    // `upload --no-mark` builds its batch from the ledger's own linkage.
    const stateless = buildIngestBatch(config, buffer, { limit: 500, maxBytes: 50_000_000 });
    stateless.rows.forEach((row, index) => {
      noMarkCompared += 1;
      const expected = sealOutboundEnvelope({
        event: unboundedAttribution(db, row.payload, row.repoHash, row.branchHash).event,
        suppressedFields: row.suppressedFields,
      });
      const actual = stateless.batch?.events[index];
      if (!expected.ok || JSON.stringify(expected.envelope) !== JSON.stringify(actual)) {
        failures.push({ path: "no_mark", seed, id: row.id, actual: actual ?? null, expected: expected.ok ? expected.envelope : expected.reason });
      }
    });

    // History upload: the real page loop (small pages, so several batches).
    try {
      const history = historyApi();
      const posted: Array<{ event: AiInteractionEvent }> = [];
      const result = await runWorkspaceHistoryUpload(config, {
        full: true,
        ledgerPath: file,
        statePath: path.join(dir, `history-${seed}.json`),
        batchSize: 25,
        pageSize: 40,
        delayMs: 0,
        sleep: async () => undefined,
        log: () => undefined,
        fetchImpl: async (_input, init) => {
          const body = String(init?.body ?? "");
          posted.push(...aiWorkIngestBatchSchema.parse(JSON.parse(body)).events);
          return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey)), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        },
      });
      expect(result.ok, "history upload failed", result.reason);
      const oracle = {
        attribute: (event: AiInteractionEvent, linkage: { repoHash?: string | null; branchHash?: string | null }) =>
          unboundedAttribution(db, event, linkage.repoHash, linkage.branchHash),
      } as unknown as SessionAttributionBatch;
      const rows = db
        .prepare(
          `select id, data_mode as dataMode, payload_json as payloadJson,
             suppressed_fields_json as suppressedFieldsJson,
             repo_hash as repoHash, branch_hash as branchHash
           from buffered_events`,
        )
        .all() as Array<{ id: string; dataMode: string; payloadJson: string; suppressedFieldsJson: string; repoHash: string | null; branchHash: string | null }>;
      const byId = new Map(rows.map((row) => [row.id, row]));
      for (const envelope of posted) {
        historyCompared += 1;
        const row = byId.get(envelope.event.id);
        const prepared = row ? history.prepare(row) : null;
        const expected = prepared?.ok ? history.seal(prepared, { ...row!, attribution: oracle }) : null;
        if (!expected?.ok || JSON.stringify(expected.envelope) !== JSON.stringify(envelope)) {
          failures.push({ path: "history", seed, id: envelope.event.id, actual: envelope, expected: expected?.ok ? expected.envelope : null });
        }
      }
    } catch (error) {
      failures.push({ path: "history", seed, error: error instanceof Error ? error.message : String(error) });
    }
    buffer.close();
  }

  measurements.equivalence = {
    scenarios: 24,
    leaseCompared,
    leaseBases: Object.fromEntries(leaseBases),
    noMarkCompared,
    historyCompared,
    boundedCompared,
    boundedFailClosed,
  };
  await check("lease_no_mark_and_history_equal_the_unbounded_rule_when_no_bound_is_reached", () => {
    expect(failures.length === 0, `${failures.length} divergent results`, failures.slice(0, 3));
    for (const basis of ["explicit", "repo_context", "session_inherited", "unallocated"]) {
      expect((leaseBases.get(basis) ?? 0) > 0, `fixture never produced basis ${basis}`);
    }
    expect(leaseCompared > 1_000 && noMarkCompared > 1_000 && historyCompared > 1_000, "too few comparisons", measurements.equivalence);
    expect(boundedFailClosed > 0 && boundedCompared > boundedFailClosed, "random bounds did not exercise both outcomes", measurements.equivalence);
    return measurements.equivalence;
  });
}

async function bounds(dir: string) {
  const Batch = (() => {
    try {
      return batchApi();
    } catch {
      return null;
    }
  })();
  const scanLimit = SESSION_INHERIT_MAX_SCANNED_ROWS;

  await check("default_scan_bound_is_exact_at_4096_index_entries", () => {
    expect(Batch, "SessionAttributionBatch is not available in this build");
    const buffer = useSessionScan(openLedger(path.join(dir, "edge.sqlite")));
    const event = tokenEvent(uuid(0x30_0000), "edge", iso(T0));
    appendAll(buffer, [event]);
    // Window [T0-6h, T0+6h] holds the token row, three REPO_A contexts and
    // fillers up to exactly the bound.
    insertFiller(buffer.database, Array.from({ length: scanLimit - 1 }, (_, index) => ({
      id: `edge-${index}`,
      sessionId: "edge",
      observedAt: iso(T0 - WINDOW_MS + Math.floor((index * 2 * WINDOW_MS) / scanLimit)),
      repoHash: index % 1_500 === 7 ? REPO_A : null,
    })));
    const reference = unboundedAttribution(buffer.database, event, null, null);
    const fits = new Batch!(buffer.database, [{ event }]);
    const atBound = fits.attribute(event);
    const fitsStats = fits.stats();
    insertFiller(buffer.database, [{ id: "edge-over", sessionId: "edge", observedAt: iso(T0 + HOUR) }]);
    const over = new Batch!(buffer.database, [{ event }]);
    const overBound = over.attribute(event);
    const overStats = over.stats();
    buffer.close();
    expect(basisOf(reference.event) === "session_inherited" && isDeepStrictEqual(atBound, reference), "window of exactly 4096 entries diverged", { atBound: atBound.event, reference: reference.event });
    expect(fitsStats.rowReads === scanLimit && fitsStats.boundReached === 0, "unexpected stats at the bound", fitsStats);
    expect(isDeepStrictEqual(overBound, failClosedAttribution(event, null, null)) && overBound.event.projectKey === undefined, "window over the bound was attributed", overBound.event);
    expect(overStats.boundReached === 1 && overStats.rowReads === 0, "unexpected stats over the bound", overStats);
    return { atBound: fitsStats, overBound: overStats };
  });

  await check("batch_read_budget_fails_later_lookups_closed", () => {
    expect(Batch, "SessionAttributionBatch is not available in this build");
    const buffer = useSessionScan(openLedger(path.join(dir, "budget.sqlite")));
    const first = tokenEvent(uuid(0x31_0000), "budget-1", iso(T0));
    const second = tokenEvent(uuid(0x31_0001), "budget-2", iso(T0));
    appendAll(buffer, [first, second]);
    for (const sessionId of ["budget-1", "budget-2"]) {
      insertFiller(buffer.database, Array.from({ length: 29 }, (_, index) => ({
        id: `${sessionId}-${index}`,
        sessionId,
        observedAt: iso(T0 - HOUR + index * 60_000),
        repoHash: index === 3 ? REPO_C : null,
      })));
    }
    const batch = new Batch!(buffer.database, [{ event: first }, { event: second }], { maxBatchRowReads: 40 });
    const firstResult = batch.attribute(first);
    const secondResult = batch.attribute(second);
    const reference = unboundedAttribution(buffer.database, first, null, null);
    const stats = batch.stats();
    buffer.close();
    expect(isDeepStrictEqual(firstResult, reference) && firstResult.event.projectKey === REPO_C, "first lookup diverged", firstResult.event);
    expect(isDeepStrictEqual(secondResult, failClosedAttribution(second, null, null)), "second lookup was not failed closed", secondResult.event);
    expect(stats.lookups === 2 && stats.rowReads === 30 && stats.budgetExhausted === 1, "unexpected budget stats", stats);
    return stats;
  });

  await check("one_lookup_per_session_per_six_hours_of_batch_span", () => {
    expect(Batch, "SessionAttributionBatch is not available in this build");
    const buffer = openLedger(path.join(dir, "span.sqlite"));
    const events = [0, 1, 5, 13, 13.5].map((hours, index) =>
      tokenEvent(uuid(0x32_0000 + index), "span", iso(T0 + hours * HOUR)));
    const contexts = [-1, 4, 12].map((hours, index) =>
      contextEvent(uuid(0x32_0100 + index), "span", iso(T0 + hours * HOUR)));
    appendAll(buffer, [...contexts, ...events]);
    resolveRepo(buffer, contexts[0]!.id, REPO_A, null, false);
    resolveRepo(buffer, contexts[1]!.id, REPO_B, null, false);
    resolveRepo(buffer, contexts[2]!.id, REPO_C, null, false);
    const batch = new Batch!(buffer.database, events.map((event) => ({ event })));
    const diverged = events.filter((event) =>
      !isDeepStrictEqual(batch.attribute(event), unboundedAttribution(buffer.database, event, null, null)));
    const stats = batch.stats();
    buffer.close();
    expect(diverged.length === 0, "span-split lookups diverged", diverged.map((event) => event.id));
    expect(stats.lookups === 2, "expected two lookups for a 13.5 h span", stats);
    return stats;
  });
}

async function sealTimeRules(dir: string) {
  await check("context_resolved_before_capture_is_uploaded_as_session_inherited", () => {
    // 2c20400e inherited at capture, then relabelled the marker `explicit`
    // when the lease re-read an event that already had a project.
    const buffer = openLedger(path.join(dir, "order.sqlite"));
    const tool = contextEvent(uuid(0x40_0000), "order", iso(T0 - 120_000));
    appendAll(buffer, [tool]);
    resolveRepo(buffer, tool.id, REPO_A, null, true);
    const token = tokenEvent(uuid(0x40_0001), "order", iso(T0 - 60_000));
    appendAll(buffer, [token]);
    const lease = buffer.delivery.lease({ maxRows: 10, now: new Date(T0 + 60_000) });
    buffer.close();
    const leased = lease.items.find((item) => item.deliveryId === token.id)?.envelope.event;
    expect(leased?.projectKey === REPO_A && basisOf(leased) === "session_inherited", "context-first token row", leased);
    return { projectKey: leased.projectKey, projectBasis: basisOf(leased) };
  });

  for (const contextFirst of [true, false]) {
    await check(`context_dead_lettered_${contextFirst ? "before" : "after"}_the_token_row_in_one_lease_matches_a_fresh_query`, () => {
      const buffer = openLedger(path.join(dir, `disposal-${contextFirst}.sqlite`));
      const tool = contextEvent(uuid(0x41_0000), "disposal", iso(T0 - 120_000));
      const token = tokenEvent(uuid(0x41_0001), "disposal", iso(T0 - 60_000));
      appendAll(buffer, [tool, token]);
      resolveRepo(buffer, tool.id, REPO_A, null, true);
      // A raw row already marked uploaded is a local privacy violation at the
      // lease boundary, so the context row is privacy-disposed mid-pass.
      buffer.database.prepare("update buffered_events set uploaded_at = ? where id = ?").run(iso(T0), tool.id);
      const order = buffer.database.prepare("update upload_outbox set next_attempt_at = ? where delivery_id = ?");
      order.run(iso(T0 - (contextFirst ? 2_000 : 1_000)), tool.id);
      order.run(iso(T0 - (contextFirst ? 1_000 : 2_000)), token.id);
      const lease = buffer.delivery.lease({ maxRows: 10, now: new Date(T0 + 60_000) });
      const disposed = buffer.database
        .prepare("select privacy_disposition as disposition from buffered_events where id = ?")
        .get(tool.id) as { disposition: string | null };
      buffer.close();
      const leased = lease.items.find((item) => item.deliveryId === token.id)?.envelope.event;
      expect(disposed.disposition === "local_privacy_violation" && lease.items.length === 1, "context row was not disposed in the pass", disposed);
      const expected = contextFirst ? "unallocated" : "session_inherited";
      expect(leased && basisOf(leased) === expected, `expected ${expected}`, leased);
      return { projectBasis: basisOf(leased!), projectKey: leased!.projectKey ?? null };
    });
  }
}

async function queryPlans(dir: string) {
  await check("lookup_statements_use_idx_events_session_and_count_from_the_covering_index", () => {
    const Batch = batchApi();
    const buffer = useSessionScan(openLedger(path.join(dir, "plan.sqlite")));
    const event = tokenEvent(uuid(0x50_0000), "plan", iso(T0));
    appendAll(buffer, [event]);
    const lookups = instrumentSessionLookups(buffer.database);
    new Batch(buffer.database, [{ event }]);
    lookups.restore();
    const plans = lookups.statements().map((sql) => ({
      counts: /count\(\*\)/.test(sql),
      details: (buffer.database
        .prepare(`explain query plan ${sql}`)
        .all("plan", iso(T0 - WINDOW_MS), iso(T0 + WINDOW_MS), 10) as Array<{ detail: string }>).map((row) => row.detail),
    }));
    buffer.close();
    expect(plans.length === 2, "expected a count and a row-read statement", plans);
    for (const plan of plans) {
      expect(!plan.details.some((detail) => /^SCAN buffered_events\b/.test(detail)), "full table scan", plan);
      const pattern = plan.counts
        ? /SEARCH buffered_events USING COVERING INDEX idx_events_session/
        : /SEARCH buffered_events USING INDEX idx_events_session/;
      expect(plan.details.some((detail) => pattern.test(detail)), "unexpected plan", plan);
    }
    return plans;
  });
}

async function main() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "session-attribution-bounded-"));
  try {
    await largeSessionBudget(dir);
    await equivalence(dir);
    await bounds(dir);
    await sealTimeRules(dir);
    await queryPlans(dir);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
  const failed = checks.filter((entry) => !entry.ok);
  console.log(JSON.stringify({
    status: failed.length === 0 ? "PASS" : "FAIL",
    checks: checks.length,
    failed: failed.length,
    results: checks,
    measurements,
  }, null, 2));
  if (failed.length > 0) process.exitCode = 1;
}

void main();
