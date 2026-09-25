/**
 * eco-6hoxj.163.21: busy sessions inherit their project through a
 * capture-time session context index.
 *
 * On the build under test this proves:
 *  1. the index always equals the ledger rows the 0.7.36 rule treats as
 *     session context (session, repo_hash, not evidence, not privacy
 *     disposed) through appends, repo linkage fills, changes and cleanup,
 *     privacy disposition, quarantine, retention and plain deletes;
 *  2. in every batch where the 0.7.36 scan reaches no bound, the index path
 *     returns identical results (randomized small ledgers, default and random
 *     bounds), and it equals the unbounded per-event rule whenever it reaches
 *     no bound itself;
 *  3. a 300k-event, six-hour session with 0.05% repo rows: 0.7.36 fails
 *     closed, the index attributes all 500 upload rows correctly within the
 *     250 ms budget, through the real lease;
 *  4. an older ledger opens with an empty index and no backfill, keeps the
 *     0.7.36 path until bounded maintenance batches finish the walk,
 *     converges exactly under concurrent writes, and defers (never fails the
 *     maintenance job) when another writer holds the lock;
 *  5. /status reports the index size and backfill progress.
 *
 * APIs that exist only on this build are loaded dynamically; on c03de03a
 * every check records a failure, so the proof is red there.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  runDeadlineMaintenanceStages,
  runRetentionDeletionStage,
  runSessionContextBackfillStage,
  SESSION_CONTEXT_BACKFILL_STAGE_MS,
  SESSION_CONTEXT_BACKFILL_UNIT_ROWS,
} from "../packages/collector-cli/src/maintenance-stage-primitives";
import type { LedgerOpenTimingSink } from "../packages/collector-cli/src/open-timing";
import { markRawPrivacyDisposition } from "../packages/collector-cli/src/privacy-disposition";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  applyProjectAttribution,
  SessionAttributionBatch,
  type SessionAttributionStats,
  type SessionRepoContext,
} from "../packages/collector-cli/src/session-attribution";
import type * as SessionContextIndex from "../packages/collector-cli/src/session-context-index";
import type { AiInteractionEvent } from "../packages/shared/src/index";

const HOUR = 60 * 60 * 1_000;
const WINDOW_MS = 6 * HOUR;
const T0 = Date.parse("2026-09-23T12:00:00.000Z");
const WORKSPACE = "fixture-workspace";
const DEVICE = "fixture-device";
const REPO_A = `sha256:${"a".repeat(64)}`;
const REPO_B = `sha256:${"b".repeat(64)}`;
const REPO_C = `sha256:${"c".repeat(64)}`;
const BRANCH_A = `sha256:${"d".repeat(64)}`;
const BRANCH_B = `sha256:${"e".repeat(64)}`;
const REPO_VALUES = [REPO_A, REPO_B, REPO_C, `sha256:${"A".repeat(64)}`, `sha256:${"z".repeat(64)}`, "plain-text"];

// The brief's busy shape: 300k events in six hours, 0.05% carrying a repo.
const BUSY_SESSION = "busy-session";
const BUSY_EVENTS = 300_000;
const BUSY_REPO_EVERY = 2_000;
const UPLOAD_ROWS = 500;
const BUDGET_MS = 250;

type IndexApi = typeof SessionContextIndex;
type Check = { name: string; ok: boolean; detail?: unknown; error?: string };
const checks: Check[] = [];
const measurements: Record<string, unknown> = {};
let indexModule: IndexApi | null = null;

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

function indexApi(): IndexApi {
  expect(indexModule, "session-context-index is not available in this build");
  return indexModule;
}

function backfillStage() {
  return runSessionContextBackfillStage;
}

const iso = (ms: number) => new Date(ms).toISOString();
const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;

function median(values: readonly number[]) {
  expect(values.length > 0, "median requires at least one sample");
  const sorted = [...values].sort((left, right) => left - right);
  const middle = sorted.length >> 1;
  return sorted.length % 2 === 1
    ? sorted[middle]!
    : (sorted[middle - 1]! + sorted[middle]!) / 2;
}

/** An observed_at whose text order differs from its time order. */
function offsetIso(ms: number, offsetMinutes: number) {
  const local = new Date(ms + offsetMinutes * 60_000).toISOString().slice(0, 23);
  const magnitude = Math.abs(offsetMinutes);
  return `${local}${offsetMinutes < 0 ? "-" : "+"}${String(Math.floor(magnitude / 60)).padStart(2, "0")}:${String(magnitude % 60).padStart(2, "0")}`;
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

function openLedger(file: string, onOpenStep?: LedgerOpenTimingSink) {
  return new LocalEventBuffer(file, {
    workspaceId: WORKSPACE,
    deviceId: DEVICE,
    enrollmentNow: () => new Date(T0 - 48 * HOUR),
    delivery: { enabled: true, now: () => new Date(T0) },
    ...(onOpenStep ? { onOpenStep } : {}),
  });
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

function contextEvent(
  id: string,
  sessionId: string | undefined,
  observedAt: string,
  repoHash?: string,
): AiInteractionEvent {
  return {
    id,
    ...(sessionId ? { sessionId } : {}),
    source: "codex",
    dataMode: "metadata",
    eventType: "tool_result",
    observedAt,
    intent: "unknown",
    actionClass: "other",
    metadata: repoHash ? { git: { remoteUrlHash: repoHash } } : {},
  };
}

function appendAll(buffer: LocalEventBuffer, events: AiInteractionEvent[]) {
  for (const event of events) expect(buffer.append(event), `append refused ${event.id}`);
}

type RawRow = {
  id: string;
  sessionId: string | null;
  observedAt: string;
  repoHash?: string | null;
  branchHash?: string | null;
  dataMode?: string;
  createdAt?: string;
  uploadedAt?: string | null;
  tokens?: boolean;
};

/** Raw ledger rows, as capture, legacy migration or an older build wrote them. */
function insertRaw(db: Database.Database, rows: RawRow[]) {
  const insert = db.prepare(
    `insert into buffered_events
       (id, source, event_type, data_mode, observed_at, payload_json,
        suppressed_fields_json, created_at, uploaded_at, session_id, repo_hash,
        branch_hash, input_tokens, output_tokens, workspace_id, device_id,
        privacy_generation)
     values (@id, 'codex', @eventType, @dataMode, @observedAt, @payload, '[]',
       @createdAt, @uploadedAt, @sessionId, @repoHash, @branchHash, @inputTokens,
       @outputTokens, @workspace, @device, 'fixture-generation')`,
  );
  const span = JSON.stringify({ spanName: "tool.call", attributes: "x".repeat(360) });
  db.transaction(() => {
    for (const row of rows) {
      const payload = row.tokens
        ? JSON.stringify(tokenEvent(row.id, row.sessionId ?? undefined, row.observedAt))
        : span;
      insert.run({
        id: row.id,
        eventType: row.tokens ? "assistant_response" : "otel_span",
        dataMode: row.dataMode ?? "metadata",
        observedAt: row.observedAt,
        payload,
        createdAt: row.createdAt ?? row.observedAt,
        uploadedAt: row.uploadedAt ?? null,
        sessionId: row.sessionId,
        repoHash: row.repoHash ?? null,
        branchHash: row.branchHash ?? null,
        inputTokens: row.tokens ? 1_000 : null,
        outputTokens: row.tokens ? 50 : null,
        workspace: WORKSPACE,
        device: DEVICE,
      });
    }
  })();
}

const TOKEN_FIELDS = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens"] as const;

function regexLinkage(value: unknown) {
  if (typeof value !== "string") return null;
  const candidate = value.trim().toLowerCase();
  return /^sha256:[a-f0-9]{64}$/.test(candidate) ? candidate : null;
}

/**
 * The unbounded reference rule, as in session-attribution-bounded-proof:
 * 2c20400e's readSessionRepoContexts verbatim (one query per event, returned
 * rows capped at 257) feeding the unchanged applyProjectAttribution.
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

type BatchStats = SessionAttributionStats;
const boundHits = (stats: BatchStats) => stats.boundReached + stats.budgetExhausted;

/** Rows the 0.7.36 rule treats as session context, in index key order. */
function contextGroundTruth(db: Database.Database) {
  return db.prepare(
    `select session_id as sessionId, observed_at as observedAt, rowid as sourceRowid,
       repo_hash as repoHash
     from buffered_events
     where session_id is not null and repo_hash is not null
       and data_mode <> 'evidence' and privacy_disposition is null
     order by session_id, observed_at, rowid`,
  ).all();
}

function indexContents(db: Database.Database) {
  return db.prepare(
    `select session_id as sessionId, observed_at as observedAt, source_rowid as sourceRowid,
       repo_hash as repoHash
     from session_repo_contexts order by session_id, observed_at, source_rowid`,
  ).all();
}

function expectIndexExact(db: Database.Database, label: string) {
  const truth = contextGroundTruth(db);
  const index = indexContents(db);
  if (!isDeepStrictEqual(index, truth)) {
    const truthKeys = new Set(truth.map((row) => JSON.stringify(row)));
    const indexKeys = new Set(index.map((row) => JSON.stringify(row)));
    throw new Error(`${label}: index diverged from the ledger: ${JSON.stringify({
      missing: [...truthKeys].filter((key) => !indexKeys.has(key)).slice(0, 3),
      extra: [...indexKeys].filter((key) => !truthKeys.has(key)).slice(0, 3),
      truth: truth.length,
      index: index.length,
    })}`);
  }
  const counted = db.prepare(
    `select indexed_rows as indexedRows, ledger_context_rows as ledgerRows
     from session_repo_context_control where singleton = 1`,
  ).get() as { indexedRows: number; ledgerRows: number };
  expect(counted.indexedRows === index.length && counted.ledgerRows === truth.length,
    `${label}: consistency counters drifted`, { counted, truth: truth.length, index: index.length });
  return index.length;
}

/** Counts executions of every session-context lookup statement prepared on `db`. */
const LOOKUP_SQL = /from (?:buffered_events(?:\s+indexed by idx_events_session)?|session_repo_contexts(?:\s+c)?)\s+where (?:c\.)?session_id = \?/;
function instrumentLookups(db: Database.Database, pattern = LOOKUP_SQL) {
  const original = db.prepare;
  const statements: string[] = [];
  let executions = 0;
  db.prepare = function prepare(this: Database.Database, source: string) {
    const statement = original.call(this, source);
    if (!pattern.test(source)) return statement;
    statements.push(source);
    for (const method of ["get", "all", "iterate", "run"] as const) {
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

function planOf(db: Database.Database, sql: string, params: unknown[]) {
  return (db.prepare(`explain query plan ${sql}`).all(...params) as Array<{ detail: string }>).map((row) => row.detail);
}

function basisOf(event: AiInteractionEvent) {
  return typeof event.metadata?.projectBasis === "string" ? event.metadata.projectBasis : "none";
}

type LedgerInput = {
  event: AiInteractionEvent;
  repoHash: string | null;
  branchHash: string | null;
};

/** Every ledger row as buildIngestBatch and history upload hand it to attribution. */
function ledgerInputs(db: Database.Database): LedgerInput[] {
  const rows = db.prepare(
    `select payload_json as payloadJson, repo_hash as repoHash, branch_hash as branchHash
     from buffered_events order by rowid`,
  ).all() as Array<{ payloadJson: string; repoHash: string | null; branchHash: string | null }>;
  return rows.map((row) => ({
    event: JSON.parse(row.payloadJson) as AiInteractionEvent,
    repoHash: row.repoHash,
    branchHash: row.branchHash,
  }));
}

/** Strip the index objects: the schema a 0.7.36 collector leaves behind. */
function downgradeTo0736(file: string) {
  const db = new Database(file);
  db.exec(`
    drop trigger if exists trg_events_session_context_insert;
    drop trigger if exists trg_events_session_context_update;
    drop trigger if exists trg_events_session_context_delete;
    drop table if exists session_repo_contexts;
    drop table if exists session_repo_context_control;
  `);
  const left = db.prepare(
    `select name from sqlite_master where name like '%session_repo_context%' or name like 'trg_events_session_context%'`,
  ).all();
  expect(left.length === 0, "0.7.36 schema still holds index objects", left);
  return db;
}

// ---------------------------------------------------------------------------
// 1. Consistency

async function consistency(dir: string) {
  await check("index_equals_ledger_context_rows_through_every_mutation_path", () => {
    indexApi();
    const buffer = openLedger(path.join(dir, "consistency.sqlite"));
    const db = buffer.database;
    const random = mulberry32(163_21);
    const pick = <T>(values: readonly T[]) => values[Math.floor(random() * values.length)]!;
    const sessions = ["s0", "s1", "s2", "s3"];
    const repoValues = [...REPO_VALUES, ""];
    const operations: Record<string, number> = {};
    let next = 0x10_0000;
    const randomRowid = () => {
      const max = (db.prepare(`select coalesce(max(rowid), 0) as n from buffered_events`).get() as { n: number }).n;
      if (max === 0) return null;
      const row = db.prepare(`select rowid from buffered_events where rowid >= ? order by rowid limit 1`)
        .get(1 + Math.floor(random() * max)) as { rowid: number } | undefined;
      return row?.rowid ?? null;
    };
    const at = () => T0 + Math.round((random() * 20 - 10) * HOUR);
    const observedAt = () => (random() < 0.85 ? iso(at()) : offsetIso(at(), random() < 0.5 ? 120 : -300));
    let verified = 0;
    for (let step = 0; step < 4_000; step += 1) {
      const roll = random();
      const rowid = randomRowid();
      let kind: string;
      if (roll < 0.22 || rowid === null) {
        kind = "append";
        const sessionId = random() < 0.9 ? pick(sessions) : undefined;
        const event = random() < 0.5
          ? tokenEvent(uuid(next += 1), sessionId, observedAt())
          : contextEvent(uuid(next += 1), sessionId, observedAt(), random() < 0.6 ? pick([REPO_A, REPO_B, REPO_C]) : undefined);
        appendAll(buffer, [event]);
      } else if (roll < 0.34) {
        kind = "raw_insert";
        const old = random() < 0.3;
        insertRaw(db, [{
          id: uuid(next += 1),
          sessionId: random() < 0.9 ? pick(sessions) : null,
          observedAt: old ? iso(Date.parse("2026-01-01T00:00:00.000Z") + Math.round(random() * HOUR)) : observedAt(),
          repoHash: random() < 0.6 ? pick(repoValues) : null,
          branchHash: random() < 0.5 ? BRANCH_A : null,
          dataMode: random() < 0.1 ? "evidence" : "metadata",
          createdAt: old ? "2026-01-01T00:00:00.000Z" : undefined,
          uploadedAt: old ? "2026-01-02T00:00:00.000Z" : null,
          tokens: random() < 0.3,
        }]);
      } else if (roll < 0.46) {
        kind = "repo_set";
        db.prepare(`update buffered_events set repo_hash = ? where rowid = ?`).run(pick(repoValues), rowid);
      } else if (roll < 0.52) {
        kind = "repo_fill_coalesce";
        db.prepare(`update buffered_events set repo_hash = coalesce(repo_hash, ?), branch_hash = coalesce(branch_hash, ?) where rowid = ?`)
          .run(pick([REPO_A, REPO_B]), BRANCH_B, rowid);
      } else if (roll < 0.58) {
        kind = "repo_cleared";
        db.prepare(`update buffered_events set repo_hash = null, branch_hash = null, head_sha = null where rowid = ?`).run(rowid);
      } else if (roll < 0.62) {
        kind = "branch_only";
        db.prepare(`update buffered_events set branch_hash = ? where rowid = ?`).run(pick([BRANCH_A, BRANCH_B, null]), rowid);
      } else if (roll < 0.66) {
        kind = "session_changed";
        db.prepare(`update buffered_events set session_id = ? where rowid = ?`).run(random() < 0.8 ? pick(sessions) : null, rowid);
      } else if (roll < 0.70) {
        kind = "observed_at_changed";
        db.prepare(`update buffered_events set observed_at = ? where rowid = ?`).run(observedAt(), rowid);
      } else if (roll < 0.76) {
        kind = "privacy_disposed";
        markRawPrivacyDisposition(db, rowid, "local_privacy_violation", iso(T0));
      } else if (roll < 0.80) {
        kind = "quarantined";
        markRawPrivacyDisposition(db, rowid, "local_evidence_quarantined", iso(T0));
      } else if (roll < 0.83) {
        kind = "evidence_marked";
        db.prepare(`update buffered_events set data_mode = 'evidence' where rowid = ?`).run(rowid);
      } else if (roll < 0.87) {
        kind = "retention_prune";
        buffer.prune(90, { maxRows: 4, now: new Date(T0) });
      } else if (roll < 0.90) {
        kind = "retention_stage";
        runRetentionDeletionStage(db, {
          remainingMs: 10_000, batchSize: 4, retentionDays: 90, parityReady: true,
          wallNow: () => T0,
        });
      } else if (roll < 0.95) {
        kind = "deleted";
        db.prepare(`delete from buffered_events where rowid = ?`).run(rowid);
      } else {
        kind = "unrelated_update";
        db.prepare(`update buffered_events set uploaded_at = ? where rowid = ?`).run(iso(T0), rowid);
      }
      operations[kind] = (operations[kind] ?? 0) + 1;
      if (step % 50 === 49) {
        expectIndexExact(db, `step ${step} (${kind})`);
        verified += 1;
      }
    }
    const indexed = expectIndexExact(db, "final");
    const ledgerRows = (db.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n;
    buffer.close();
    for (const kind of [
      "append", "raw_insert", "repo_set", "repo_fill_coalesce", "repo_cleared", "privacy_disposed",
      "quarantined", "evidence_marked", "retention_prune", "retention_stage", "deleted",
    ]) expect((operations[kind] ?? 0) > 0, `fixture never exercised ${kind}`, operations);
    return { operations, verifications: verified + 1, ledgerRows, indexedRows: indexed };
  });
}

function attributeOne(db: Database.Database, event: AiInteractionEvent) {
  const batch = new SessionAttributionBatch(db, [{ event }]);
  return { result: batch.attribute(event), stats: batch.stats() };
}

function finishBackfill(db: Database.Database, api: IndexApi) {
  let batches = 0;
  while (api.sessionContextIndexState(db) === "backfilling") {
    const result = api.backfillSessionContextIndex(db, 500);
    batches += 1;
    expect(result.visited > 0 || result.state === "complete", "backfill made no progress", result);
    expect(batches < 10_000, "backfill did not terminate");
  }
  return batches;
}

async function checksumBeyondSafeInteger(dir: string) {
  await check("checksum_crossing_2_to_the_53rd_power_completes_exactly", () => {
    const api = indexApi();
    const file = path.join(dir, "checksum-over-2-to-the-53rd.sqlite");
    const seed = openLedger(file);
    const insert = seed.database.prepare(`
      insert into buffered_events
        (rowid, id, source, event_type, data_mode, observed_at, payload_json,
         suppressed_fields_json, created_at, session_id, repo_hash, workspace_id,
         device_id, privacy_generation)
      values (?, ?, 'codex', 'tool_result', 'metadata', ?, '{}', '[]', ?, ?, ?, ?, ?, ?)
    `);
    seed.database.transaction(() => {
      for (let index = 0; index < 12; index += 1) {
        // Keep the per-row key near 1e15 while making the aggregate's low
        // bits observable. A changing timestamp adds the row index twice and
        // can accidentally land on a representable IEEE-754 value.
        const at = new Date(T0).toISOString();
        insert.run(
          1_000_000_000_000_000 + index,
          uuid(0x30_0000 + index),
          at,
          at,
          "checksum-session",
          REPO_A,
          WORKSPACE,
          DEVICE,
          `checksum-generation-${index}`,
        );
      }
    })();
    seed.close();
    downgradeTo0736(file).close();

    const buffer = openLedger(file);
    const db = buffer.database;
    let batches = 0;
    while (api.sessionContextIndexState(db) === "backfilling" && batches < 20) {
      api.backfillSessionContextIndex(db, 31);
      batches += 1;
    }
    const equality = db.prepare(`
      select indexed_rows = ledger_context_rows as counts_equal,
             indexed_key_checksum = ledger_key_checksum as checksums_equal,
             ledger_key_checksum > 9007199254740992 as crossed_safe_integer,
             backfill_complete as complete
        from session_repo_context_control where singleton = 1
    `).get() as {
      counts_equal: number;
      checksums_equal: number;
      crossed_safe_integer: number;
      complete: number;
    };
    const status = api.sessionContextIndexStatus(db);
    buffer.close();
    const detail = { batches, equality, status };
    expect(equality.counts_equal === 1 && equality.checksums_equal === 1 &&
      equality.crossed_safe_integer === 1 && equality.complete === 1 &&
      status.state === "complete", "checksum drifted after 2^53", detail);
    return detail;
  });
}

// ---------------------------------------------------------------------------
// 1b. Corruption, rollback and downgrade safety

async function corruptionSafety(dir: string) {
  await check("complete_index_corruption_falls_back_without_a_wrong_project_or_readonly_crash", () => {
    const api = indexApi();
    const missingFile = path.join(dir, "corrupt-missing.sqlite");
    let missing = openLedger(missingFile);
    const a = contextEvent(uuid(0x11_0001), "corrupt-session", iso(T0), REPO_A);
    const b = contextEvent(uuid(0x11_0002), "corrupt-session", iso(T0 + 60_000), REPO_B);
    const token = tokenEvent(uuid(0x11_0003), "corrupt-session", iso(T0 + 120_000));
    appendAll(missing, [a, b]);
    const baseline = attributeOne(missing.database, token);
    const bRow = missing.database.prepare("select rowid from buffered_events where id = ?")
      .get(b.id) as { rowid: number };
    missing.database.prepare("delete from session_repo_contexts where source_rowid = ?").run(bRow.rowid);
    const missingStatus = api.sessionContextIndexStatus(missing.database);
    const missingFallback = attributeOne(missing.database, token);
    missing.close();

    missing = openLedger(missingFile);
    const reopenedStatus = api.sessionContextIndexStatus(missing.database);
    const reopenedFallback = attributeOne(missing.database, token);
    const rebuildBatches = finishBackfill(missing.database, api);
    const rebuilt = attributeOne(missing.database, token);
    missing.close();

    const orphanFile = path.join(dir, "corrupt-orphan.sqlite");
    const orphan = openLedger(orphanFile);
    appendAll(orphan, [a]);
    orphan.database.prepare(
      `insert into session_repo_contexts
       (session_id, observed_at, source_rowid, repo_hash) values (?, ?, ?, ?)`,
    ).run("corrupt-session", iso(T0 + 60_000), 999_999, REPO_B);
    const orphanStatus = api.sessionContextIndexStatus(orphan.database);
    const orphanFallback = attributeOne(orphan.database, token);
    // Even if both cardinalities are maliciously made equal, the indexed
    // range's source-row point check must reject the orphan rather than B.
    orphan.database.exec(
      `update session_repo_context_control set ledger_context_rows = indexed_rows,
         ledger_key_checksum = indexed_key_checksum,
         integrity_state = 'valid', backfill_complete = 1,
         backfill_completed_at = installed_at
       where singleton = 1`,
    );
    orphan.close();
    // Use a fresh connection so the earlier fail-closed status observation
    // cannot mask the source-row point check with its in-process marker.
    const pointCheckedDb = new Database(orphanFile);
    const pointChecked = attributeOne(pointCheckedDb, token);
    const pointCheckedStatus = api.sessionContextIndexStatus(pointCheckedDb);
    pointCheckedDb.close();

    const missingTableFile = path.join(dir, "corrupt-table.sqlite");
    const tableWriter = openLedger(missingTableFile);
    appendAll(tableWriter, [a]);
    tableWriter.close();
    const damage = new Database(missingTableFile);
    damage.exec("drop table session_repo_contexts");
    damage.close();
    const readOnly = new Database(missingTableFile, { readonly: true, fileMustExist: true });
    const readOnlyFallback = attributeOne(readOnly, token);
    readOnly.close();
    const repaired = openLedger(missingTableFile);
    const repairedStatus = api.sessionContextIndexStatus(repaired.database);
    const repairedFallback = attributeOne(repaired.database, token);
    repaired.close();

    const detail = {
      missing: { baseline, status: missingStatus, fallback: missingFallback, reopenedStatus,
        reopenedFallback, rebuildBatches, rebuilt },
      orphan: { status: orphanStatus, fallback: orphanFallback, pointChecked, pointCheckedStatus },
      missingTable: { readOnlyFallback, repairedStatus, repairedFallback },
    };
    expect(baseline.result.event.projectKey === REPO_B && baseline.stats.contextIndex,
      "healthy baseline did not use B through the index", detail.missing);
    expect((missingStatus.state === "absent" || missingStatus.state === "invalid") &&
      missingStatus.backfill.completedAt === null && !missingFallback.stats.contextIndex &&
      missingFallback.result.event.projectKey === REPO_B,
    "a missing logical row did not select the correct bounded fallback", detail.missing);
    expect(reopenedStatus.state === "backfilling" && !reopenedFallback.stats.contextIndex &&
      reopenedFallback.result.event.projectKey === REPO_B && rebuilt.stats.contextIndex &&
      rebuilt.result.event.projectKey === REPO_B,
    "writable reopen trusted or failed to rebuild a mismatched index", detail.missing);
    expect((orphanStatus.state === "absent" || orphanStatus.state === "invalid") &&
      !orphanFallback.stats.contextIndex &&
      orphanFallback.result.event.projectKey === REPO_A,
    "an orphan row did not select the correct bounded fallback", detail.orphan);
    expect(pointChecked.stats.contextIndex && pointChecked.stats.integrityFailures === 1 &&
      pointCheckedStatus.state === "invalid" && pointCheckedStatus.backfill.completedAt === null &&
      pointChecked.result.basis === "unallocated" &&
      pointChecked.result.event.projectKey === undefined,
    "source-row validation let a count-preserving orphan authorize a project", detail.orphan);
    expect(!readOnlyFallback.stats.contextIndex && readOnlyFallback.result.event.projectKey === REPO_A,
      "a missing table crashed or changed the read-only fallback", detail.missingTable);
    expect(repairedStatus.state === "backfilling" && !repairedFallback.stats.contextIndex &&
      repairedFallback.result.event.projectKey === REPO_A,
    "writable reopen claimed a missing table was complete", detail.missingTable);
    return detail;
  });

  await check("checksum_rejects_count_preserving_key_moves_and_delete_plus_extra_rows", () => {
    const api = indexApi();
    const token = tokenEvent(uuid(0x11_1003), "variant-session", iso(T0 + 120_000));
    const a = contextEvent(uuid(0x11_1001), "variant-session", iso(T0), REPO_A);
    const b = contextEvent(uuid(0x11_1002), "variant-session", iso(T0 + 60_000), REPO_B);
    const setup = (file: string) => {
      const buffer = openLedger(file);
      appendAll(buffer, [a, b]);
      return buffer;
    };

    const moved = setup(path.join(dir, "corrupt-count-preserving-move.sqlite"));
    const bRow = moved.database.prepare("select rowid from buffered_events where id = ?").get(b.id) as { rowid: number };
    moved.database.prepare(
      `update session_repo_contexts set observed_at = ? where source_rowid = ?`,
    ).run(iso(T0 + 10 * HOUR), bRow.rowid);
    const movedStatus = api.sessionContextIndexStatus(moved.database);
    const movedFallback = attributeOne(moved.database, token);
    moved.close();
    const movedReadOnly = new Database(path.join(dir, "corrupt-count-preserving-move.sqlite"), { readonly: true });
    const movedReadOnlyStatus = api.sessionContextIndexStatus(movedReadOnly);
    const movedReadOnlyFallback = attributeOne(movedReadOnly, token);
    movedReadOnly.close();
    const movedReopen = setup(path.join(dir, "corrupt-count-preserving-move-reopen.sqlite"));
    // Recreate the corruption on a second copy, then let a writable open heal it.
    const reopenRow = movedReopen.database.prepare("select rowid from buffered_events where id = ?").get(b.id) as { rowid: number };
    movedReopen.database.prepare(`update session_repo_contexts set observed_at = ? where source_rowid = ?`)
      .run(iso(T0 + 10 * HOUR), reopenRow.rowid);
    movedReopen.close();
    const movedRepaired = openLedger(path.join(dir, "corrupt-count-preserving-move-reopen.sqlite"));
    const movedRepairedBefore = api.sessionContextIndexStatus(movedRepaired.database);
    finishBackfill(movedRepaired.database, api);
    const movedRepairedAttribution = attributeOne(movedRepaired.database, token);
    movedRepaired.close();

    const paired = setup(path.join(dir, "corrupt-count-preserving-pair.sqlite"));
    const pairedRow = paired.database.prepare("select rowid from buffered_events where id = ?").get(b.id) as { rowid: number };
    paired.database.prepare(`delete from session_repo_contexts where source_rowid = ?`).run(pairedRow.rowid);
    paired.database.prepare(
      `insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
       values ('other-session', ?, 999999, ?)`,
    ).run(iso(T0 + 30_000), REPO_A);
    const pairedStatus = api.sessionContextIndexStatus(paired.database);
    const pairedFallback = attributeOne(paired.database, token);
    paired.close();
    const pairedReadOnly = new Database(path.join(dir, "corrupt-count-preserving-pair.sqlite"), { readonly: true });
    const pairedReadOnlyStatus = api.sessionContextIndexStatus(pairedReadOnly);
    const pairedReadOnlyFallback = attributeOne(pairedReadOnly, token);
    pairedReadOnly.close();
    const pairedReopen = setup(path.join(dir, "corrupt-count-preserving-pair-reopen.sqlite"));
    const pairedReopenRow = pairedReopen.database.prepare("select rowid from buffered_events where id = ?").get(b.id) as { rowid: number };
    pairedReopen.database.prepare(`delete from session_repo_contexts where source_rowid = ?`).run(pairedReopenRow.rowid);
    pairedReopen.database.prepare(
      `insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
       values ('other-session', ?, 999999, ?)`,
    ).run(iso(T0 + 30_000), REPO_A);
    pairedReopen.close();
    const pairedRepaired = openLedger(path.join(dir, "corrupt-count-preserving-pair-reopen.sqlite"));
    const pairedRepairedBefore = api.sessionContextIndexStatus(pairedRepaired.database);
    finishBackfill(pairedRepaired.database, api);
    const pairedRepairedAttribution = attributeOne(pairedRepaired.database, token);
    pairedRepaired.close();

    const detail = {
      moved: { movedStatus, movedFallback, movedReadOnlyStatus, movedReadOnlyFallback,
        movedRepairedBefore, movedRepairedAttribution },
      paired: { pairedStatus, pairedFallback, pairedReadOnlyStatus, pairedReadOnlyFallback,
        pairedRepairedBefore, pairedRepairedAttribution },
    };
    for (const [label, value] of [["moved", detail.moved], ["paired", detail.paired]] as const) {
      expect((value[`${label}Status` as keyof typeof value] as { state: string }).state === "invalid",
        `${label} corruption did not invalidate the complete index`, value);
      const fallback = value[`${label}Fallback` as keyof typeof value] as ReturnType<typeof attributeOne>;
      expect(!fallback.stats.contextIndex && fallback.result.event.projectKey === REPO_B,
        `${label} corruption did not fail closed to the 0.7.36 result`, value);
    }
    expect(detail.moved.movedReadOnlyStatus.state === "invalid" &&
      !detail.moved.movedReadOnlyFallback.stats.contextIndex && detail.moved.movedReadOnlyFallback.result.event.projectKey === REPO_B &&
      detail.paired.pairedReadOnlyStatus.state === "invalid" &&
      !detail.paired.pairedReadOnlyFallback.stats.contextIndex && detail.paired.pairedReadOnlyFallback.result.event.projectKey === REPO_B,
    "a corrupt index was trusted after read-only reopen", detail);
    expect(detail.moved.movedRepairedBefore.state === "backfilling" &&
      detail.moved.movedRepairedAttribution.stats.contextIndex &&
      detail.moved.movedRepairedAttribution.result.event.projectKey === REPO_B,
    "a writable reopen did not rebuild a moved key", detail.moved);
    expect(detail.paired.pairedRepairedBefore.state === "backfilling" &&
      detail.paired.pairedRepairedAttribution.stats.contextIndex &&
      detail.paired.pairedRepairedAttribution.result.event.projectKey === REPO_B,
    "a writable reopen did not rebuild a delete-plus-extra corruption", detail.paired);
    return detail;
  });

  await check("trigger_definition_mutation_is_not_a_complete_index", () => {
    const api = indexApi();
    const file = path.join(dir, "corrupt-trigger-definition.sqlite");
    const buffer = openLedger(file);
    const context = contextEvent(uuid(0x11_2001), "trigger-session", iso(T0), REPO_A);
    const token = tokenEvent(uuid(0x11_2002), "trigger-session", iso(T0 + 60_000));
    appendAll(buffer, [context]);
    buffer.database.exec("drop trigger trg_session_repo_contexts_integrity_update");
    const status = api.sessionContextIndexStatus(buffer.database);
    const fallback = attributeOne(buffer.database, token);
    buffer.close();
    const repaired = openLedger(file);
    const repairedStatus = api.sessionContextIndexStatus(repaired.database);
    repaired.close();
    const detail = { status, fallback, repairedStatus };
    expect(status.state === "absent" && !fallback.stats.contextIndex && fallback.result.event.projectKey === REPO_A,
      "a mutated trigger definition was trusted", detail);
    expect(repairedStatus.state === "backfilling", "writable reopen did not rebuild after trigger mutation", detail);
    return detail;
  });

  await check("privacy_erasure_is_not_blocked_by_a_low_index_counter", () => {
    const api = indexApi();
    const buffer = openLedger(path.join(dir, "privacy-low-counter.sqlite"));
    const context = contextEvent(uuid(0x11_3001), "privacy-counter-session", iso(T0), REPO_A);
    appendAll(buffer, [context]);
    buffer.database.prepare(
      `update session_repo_context_control set indexed_rows = 0 where singleton = 1`,
    ).run();
    let error: string | null = null;
    try {
      markRawPrivacyDisposition(buffer.database,
        (buffer.database.prepare(`select rowid from buffered_events where id = ?`).get(context.id) as { rowid: number }).rowid,
        "local_privacy_violation", iso(T0));
    } catch (caught) {
      error = caught instanceof Error ? caught.message : String(caught);
    }
    const status = api.sessionContextIndexStatus(buffer.database);
    const remaining = (buffer.database.prepare(`select count(*) as n from session_repo_contexts`).get() as { n: number }).n;
    buffer.close();
    const detail = { error, status, remaining };
    expect(error === null && remaining === 0, "privacy erasure was blocked by index bookkeeping", detail);
    return detail;
  });

  await check("ledger_and_index_mutations_roll_back_together_on_rollback_or_connection_loss", () => {
    const api = indexApi();
    const file = path.join(dir, "rollback.sqlite");
    const buffer = openLedger(file);
    const row = contextEvent(uuid(0x12_0001), "rollback-session", iso(T0), REPO_A);
    buffer.database.exec("begin immediate");
    appendAll(buffer, [row]);
    const during = {
      ledger: (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      index: (buffer.database.prepare("select count(*) as n from session_repo_contexts").get() as { n: number }).n,
      control: buffer.database.prepare(
        `select indexed_rows as indexedRows, ledger_context_rows as ledgerRows
         from session_repo_context_control where singleton = 1`,
      ).get(),
    };
    buffer.database.exec("rollback");
    const afterRollback = {
      ledger: (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      index: (buffer.database.prepare("select count(*) as n from session_repo_contexts").get() as { n: number }).n,
      status: api.sessionContextIndexStatus(buffer.database),
    };
    buffer.close();

    const interrupted = openLedger(file);
    interrupted.database.exec("begin immediate");
    appendAll(interrupted, [contextEvent(uuid(0x12_0002), "rollback-session", iso(T0), REPO_B)]);
    interrupted.database.close();
    const recovered = openLedger(file);
    const afterConnectionLoss = {
      ledger: (recovered.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      index: (recovered.database.prepare("select count(*) as n from session_repo_contexts").get() as { n: number }).n,
      status: api.sessionContextIndexStatus(recovered.database),
    };
    recovered.close();
    const detail = { during, afterRollback, afterConnectionLoss };
    expect(during.ledger === 1 && during.index === 1,
      "transaction did not contain both ledger and index writes", detail);
    expect(afterRollback.ledger === 0 && afterRollback.index === 0 &&
      afterRollback.status.state === "complete", "explicit rollback left auxiliary state", detail);
    expect(afterConnectionLoss.ledger === 0 && afterConnectionLoss.index === 0 &&
      afterConnectionLoss.status.state === "complete", "connection loss left a partial transaction", detail);
    return detail;
  });

  await check("new_old_new_downgrade_cycle_preserves_the_complete_index", () => {
    const api = indexApi();
    const file = path.join(dir, "downgrade.sqlite");
    const first = openLedger(file);
    const a = contextEvent(uuid(0x13_0001), "downgrade-session", iso(T0), REPO_A);
    appendAll(first, [a]);
    const before = first.database.prepare(
      `select installed_at as installedAt, indexed_rows as indexedRows,
         ledger_context_rows as ledgerRows from session_repo_context_control where singleton = 1`,
    ).get() as { installedAt: string; indexedRows: number; ledgerRows: number };
    first.close();

    // A 0.7.37 writer knows only buffered_events. SQLite retains and runs the
    // newer triggers while that build writes B.
    const oldWriter = new Database(file);
    insertRaw(oldWriter, [{
      id: uuid(0x13_0002), sessionId: "downgrade-session",
      observedAt: iso(T0 + 60_000), repoHash: REPO_B,
    }]);
    oldWriter.close();

    const upgraded = openLedger(file);
    const after = upgraded.database.prepare(
      `select installed_at as installedAt, indexed_rows as indexedRows,
         ledger_context_rows as ledgerRows from session_repo_context_control where singleton = 1`,
    ).get() as { installedAt: string; indexedRows: number; ledgerRows: number };
    const token = tokenEvent(uuid(0x13_0003), "downgrade-session", iso(T0 + 120_000));
    const attribution = attributeOne(upgraded.database, token);
    const exact = expectIndexExact(upgraded.database, "after downgrade cycle");
    upgraded.close();
    const detail = { before, after, attribution, exact };
    expect(before.indexedRows === 1 && before.ledgerRows === 1 &&
      after.indexedRows === 2 && after.ledgerRows === 2 &&
      after.installedAt === before.installedAt,
    "re-upgrade rebuilt or distrusted the trigger-maintained index", detail);
    expect(attribution.stats.contextIndex && attribution.result.event.projectKey === REPO_B && exact === 2,
      "re-upgrade did not inherit the newer context", detail);
    return detail;
  });
}

// ---------------------------------------------------------------------------
// 2. Equivalence with the 0.7.36 rule

/**
 * A small randomized ledger with every attribution shape (as in
 * session-attribution-bounded-proof): single- and multi-repo sessions,
 * future-only context, uppercase and invalid hashes, evidence and
 * privacy-disposed context, context outside the window, offset timestamps,
 * explicit and forged-marker projects, own repo linkage, non-token rows, rows
 * without a session, and (sometimes) a dense session over the 256 cap.
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
  const db = buffer.database;
  const sessions = ["s0", "s1", "s2"];
  let next = seed * 10_000;
  const at = () => T0 + Math.round((random() * 20 - 10) * HOUR);
  const resolve = (id: string) => {
    db.prepare("update buffered_events set repo_hash = ?, branch_hash = ? where id = ?")
      .run(pick(REPO_VALUES), random() < 0.5 ? BRANCH_A : null, id);
  };
  for (let index = 0; index < 60; index += 1) {
    const event = contextEvent(uuid(next += 1), random() < 0.95 ? pick(sessions) : undefined, observedAt(at()));
    appendAll(buffer, [event]);
    if (random() < 0.8) resolve(event.id);
    if (random() < 0.1) {
      db.prepare("update buffered_events set privacy_disposition = 'local_privacy_violation', privacy_disposed_at = ? where id = ?")
        .run(iso(T0), event.id);
    }
  }
  insertRaw(db, Array.from({ length: 6 }, () => ({
    id: uuid(next += 1),
    sessionId: pick(sessions),
    observedAt: iso(at()),
    repoHash: pick([REPO_A, REPO_B]),
    dataMode: "evidence",
  })));
  if (random() < 0.5) {
    const dense = Array.from({ length: 300 }, (_, index) => contextEvent(uuid(next += 1), "dense", iso(T0 + 5 * HOUR + index * 10_000)));
    appendAll(buffer, dense);
    for (const event of dense) db.prepare("update buffered_events set repo_hash = ? where id = ?").run(REPO_B, event.id);
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
    if (random() < 0.12) resolve(event.id);
  }
  return buffer;
}

async function equivalence(dir: string) {
  await check("index_path_equals_0736_whenever_0736_reaches_no_bound_on_randomized_ledgers", () => {
    indexApi();
    const Batch = SessionAttributionBatch;
    const tally = {
      scenarios: 0,
      batches: 0,
      compared: 0,
      identicalTo0736: 0,
      equalToUnboundedRule: 0,
      scanBatchesAtABound: 0,
      indexBatchesAtABound: 0,
      failClosedByBothOrIndex: 0,
      inheritedWhere0736FailedClosed: 0,
      bases: {} as Record<string, number>,
    };
    const failures: unknown[] = [];
    for (let seed = 1; seed <= 40; seed += 1) {
      tally.scenarios += 1;
      const buffer = buildScenario(path.join(dir, `scenario-${seed}.sqlite`), seed);
      const db = buffer.database;
      const inputs = ledgerInputs(db);
      const reference = inputs.map((input) => unboundedAttribution(db, input.event, input.repoHash, input.branchHash));
      const random = mulberry32(seed + 5_000);
      const boundSets: Array<{ maxScannedRows?: number; maxBatchRowReads?: number }> = [{}];
      for (let round = 0; round < 6; round += 1) {
        boundSets.push({ maxScannedRows: Math.floor(random() * 120), maxBatchRowReads: Math.floor(random() * 400) });
      }
      for (const bounds of boundSets) {
        tally.batches += 1;
        const batchInputs = inputs.map((input) => ({ event: input.event, repoHash: input.repoHash }));
        const indexed = new Batch(db, batchInputs, bounds);
        const scanned = new Batch(db, batchInputs, { ...bounds, contextIndex: false });
        const indexStats = indexed.stats();
        const scanStats = scanned.stats();
        if (indexStats.contextIndex !== true || scanStats.contextIndex !== false) {
          failures.push({ seed, bounds, reason: "paths not selected", indexStats, scanStats });
          continue;
        }
        const scanHit = boundHits(scanStats) > 0;
        const indexHit = boundHits(indexStats) > 0;
        if (scanHit) tally.scanBatchesAtABound += 1;
        if (indexHit) tally.indexBatchesAtABound += 1;
        if (!scanHit && indexHit) failures.push({ seed, bounds, reason: "index path reached a bound 0.7.36 did not", indexStats, scanStats });
        inputs.forEach((input, index) => {
          const options = { repoHash: input.repoHash, branchHash: input.branchHash };
          const viaIndex = indexed.attribute(input.event, options);
          const viaScan = scanned.attribute(input.event, options);
          const expected = reference[index]!;
          const failClosed = failClosedAttribution(input.event, input.repoHash, input.branchHash);
          tally.compared += 1;
          tally.bases[viaIndex.basis] = (tally.bases[viaIndex.basis] ?? 0) + 1;
          if (!scanHit) {
            if (isDeepStrictEqual(viaIndex, viaScan)) tally.identicalTo0736 += 1;
            else failures.push({ seed, bounds, id: input.event.id, reason: "differs from 0.7.36", viaIndex: viaIndex.event, viaScan: viaScan.event });
          }
          if (isDeepStrictEqual(viaIndex, expected)) {
            tally.equalToUnboundedRule += 1;
            if (!isDeepStrictEqual(viaScan, expected) && isDeepStrictEqual(viaScan, failClosed)) {
              tally.inheritedWhere0736FailedClosed += 1;
            }
          } else if (indexHit && isDeepStrictEqual(viaIndex, failClosed)) {
            tally.failClosedByBothOrIndex += 1;
          } else {
            failures.push({ seed, bounds, id: input.event.id, reason: "neither the unbounded rule nor fail-closed", viaIndex: viaIndex.event, expected: expected.event });
          }
        });
      }
      buffer.close();
    }
    measurements.equivalence = tally;
    expect(failures.length === 0, `${failures.length} divergent results`, failures.slice(0, 3));
    for (const basis of ["explicit", "repo_context", "session_inherited", "unallocated"]) {
      expect((tally.bases[basis] ?? 0) > 0, `fixture never produced basis ${basis}`, tally.bases);
    }
    expect(tally.identicalTo0736 > 10_000, "too few 0.7.36 comparisons", tally);
    expect(tally.scanBatchesAtABound > 0 && tally.inheritedWhere0736FailedClosed > 0,
      "random bounds never showed the index attributing where 0.7.36 failed closed", tally);
    expect(tally.indexBatchesAtABound > 0 && tally.failClosedByBothOrIndex > 0,
      "random bounds never exercised the index path's own bounds", tally);
    return tally;
  });
}

// ---------------------------------------------------------------------------
// 3. The busy session

async function busySession(dir: string) {
  await check("busy_300k_event_session_index_uses_one_range_and_median_cpu_under_budget", () => {
    // Behaviour first: on a build without the index this check fails because
    // the busy rows stay unallocated, not merely because an API is missing.
    const busy = openLedger(path.join(dir, "busy.sqlite"));
    const db = busy.database;
    let started = performance.now();
    insertRaw(db, Array.from({ length: BUSY_EVENTS }, (_, index) => ({
      id: `busy-${index}`,
      sessionId: BUSY_SESSION,
      observedAt: iso(T0 - WINDOW_MS + Math.floor((index * WINDOW_MS) / BUSY_EVENTS)),
    })));
    const fixtureBuildMs = Math.round(performance.now() - started);
    const uploadEvents = Array.from({ length: UPLOAD_ROWS }, (_, index) =>
      tokenEvent(uuid(0x20_0000 + index), BUSY_SESSION, iso(T0 - 60_000 + index * 100)));
    insertRaw(db, uploadEvents.map((event) => ({
      id: event.id, sessionId: BUSY_SESSION, observedAt: event.observedAt, tokens: true,
    })));
    let enqueued = 0;
    for (const event of uploadEvents) enqueued += busy.delivery.repairRawById(event.id).enqueued;
    expect(enqueued === UPLOAD_ROWS, "upload batch was not enqueued", enqueued);
    // Repo-context resolution lands after capture: 150 rows (0.05%) carry REPO_A.
    const resolve = db.prepare("update buffered_events set repo_hash = ? where id = ?");
    db.transaction(() => {
      for (let index = 0; index < BUSY_EVENTS; index += BUSY_REPO_EVERY) resolve.run(REPO_A, `busy-${index}`);
    })();
    const repoRows = Math.ceil(BUSY_EVENTS / BUSY_REPO_EVERY);

    const inputs = uploadEvents.map((event) => ({ event }));
    const Batch = SessionAttributionBatch;
    started = performance.now();
    const scanned = new Batch(db, inputs, { contextIndex: false });
    const scannedResults = uploadEvents.map((event) => scanned.attribute(event));
    const scanMs = performance.now() - started;
    const lookups = instrumentLookups(db);
    started = performance.now();
    const indexed = new Batch(db, inputs);
    const indexedResults = uploadEvents.map((event) => indexed.attribute(event));
    const indexMs = performance.now() - started;
    lookups.restore();
    const indexSamples = [indexMs];
    const indexCpuSamples: number[] = [];
    {
      const cpuStarted = process.cpuUsage();
      const cpuBatch = new Batch(db, inputs);
      for (const event of uploadEvents) cpuBatch.attribute(event);
      const cpu = process.cpuUsage(cpuStarted);
      indexCpuSamples.push((cpu.user + cpu.system) / 1_000);
    }
    for (let sample = 0; sample < 4; sample += 1) {
      started = performance.now();
      const cpuStarted = process.cpuUsage();
      const repeated = new Batch(db, inputs);
      repeated.attribute(uploadEvents[0]!);
      for (let index = 1; index < uploadEvents.length; index += 1) repeated.attribute(uploadEvents[index]!);
      indexSamples.push(performance.now() - started);
      const cpu = process.cpuUsage(cpuStarted);
      indexCpuSamples.push((cpu.user + cpu.system) / 1_000);
    }
    const indexMedianMs = median(indexSamples);
    const scanStats = scanned.stats();
    const indexStats = indexed.stats();

    // The per-row rule, measured on three rows (it costs a full window scan each).
    const sampled = [0, UPLOAD_ROWS >> 1, UPLOAD_ROWS - 1];
    started = performance.now();
    const referenceResults = sampled.map((index) => unboundedAttribution(db, uploadEvents[index]!, null, null));
    const referenceMsPerRow = (performance.now() - started) / sampled.length;

    // A lease mutates delivery state, so measure independent copies of a
    // checkpointed seed. Median-of-five avoids one loaded host sample making
    // the proof flaky while retaining a real wall-time regression signal.
    db.pragma("wal_checkpoint(TRUNCATE)");
    const leaseSeed = path.join(dir, "busy-lease-seed.sqlite");
    fs.copyFileSync(path.join(dir, "busy.sqlite"), leaseSeed);
    const leaseSamples: number[] = [];
    const leaseCpuSamples: number[] = [];
    const leaseLookupSamples: number[] = [];
    let leaseItems: Array<{ envelope: { event: AiInteractionEvent } }> = [];
    for (let sample = 0; sample < 5; sample += 1) {
      const leaseFile = path.join(dir, `busy-lease-${sample}.sqlite`);
      fs.copyFileSync(leaseSeed, leaseFile);
      const leaseBuffer = openLedger(leaseFile);
      const leaseLookups = instrumentLookups(leaseBuffer.database);
      started = performance.now();
      const cpuStarted = process.cpuUsage();
      const leased = leaseBuffer.delivery.lease({ maxRows: UPLOAD_ROWS, maxBytes: 10_000_000, now: new Date(T0 + 60_000) });
      leaseSamples.push(performance.now() - started);
      const cpu = process.cpuUsage(cpuStarted);
      leaseCpuSamples.push((cpu.user + cpu.system) / 1_000);
      leaseLookups.restore();
      leaseLookupSamples.push(leaseLookups.executions());
      if (sample === 0) leaseItems = leased.items;
      leaseBuffer.close();
    }
    const leaseMedianMs = median(leaseSamples);
    let indexConsistency: string | number;
    try {
      indexConsistency = expectIndexExact(db, "busy fixture");
    } catch (error) {
      indexConsistency = error instanceof Error ? error.message : String(error);
    }
    busy.close();

    measurements.busySession = {
      fixture: { busySessionEvents: BUSY_EVENTS, busySessionRepoRows: repoRows, spanHours: 6, uploadRows: UPLOAD_ROWS },
      fixtureBuildMs,
      session0736: { stats: scanStats, attributionMs: Math.round(scanMs * 10) / 10 },
      contextIndex: {
        stats: indexStats,
        attributionMs: Math.round(indexMedianMs * 10) / 10,
        attributionSamplesMs: indexSamples.map((sample) => Math.round(sample * 10) / 10),
        attributionCpuMs: Math.round(median(indexCpuSamples) * 10) / 10,
        attributionCpuSamplesMs: indexCpuSamples.map((sample) => Math.round(sample * 10) / 10),
        lookupExecutions: lookups.executions(),
      },
      unboundedRuleMsPerRow: Math.round(referenceMsPerRow * 10) / 10,
      lease: {
        rows: leaseItems.length,
        leaseMs: Math.round(leaseMedianMs * 10) / 10,
        leaseSamplesMs: leaseSamples.map((sample) => Math.round(sample * 10) / 10),
        leaseCpuMs: Math.round(median(leaseCpuSamples) * 10) / 10,
        leaseCpuSamplesMs: leaseCpuSamples.map((sample) => Math.round(sample * 10) / 10),
        leaseCpuBudgetMs: Math.round(Math.max(BUDGET_MS * 1.5, Math.min(2_000, referenceMsPerRow * 3)) * 10) / 10,
        lookupExecutions: leaseLookupSamples,
      },
    };
    expect(scanStats.boundReached === 1 && scannedResults.every((result, index) =>
      isDeepStrictEqual(result, failClosedAttribution(uploadEvents[index]!, undefined, undefined))),
    "0.7.36 did not fail closed on the busy window", scanStats);
    const wrong = indexedResults.filter((result) => result.basis !== "session_inherited" || result.event.projectKey !== REPO_A);
    expect(wrong.length === 0, `${wrong.length} of ${UPLOAD_ROWS} busy rows were not inherited`, wrong.slice(0, 1).map((result) => result.event));
    const leasedWrong = leaseItems.filter((item) =>
      item.envelope.event.projectKey !== REPO_A || basisOf(item.envelope.event) !== "session_inherited");
    expect(leaseItems.length === UPLOAD_ROWS, "lease did not return the whole batch", leaseItems.length);
    expect(leasedWrong.length === 0, `${leasedWrong.length} leased busy rows were not inherited`, leasedWrong.slice(0, 1).map((item) => item.envelope.event));
    sampled.forEach((row, index) => {
      expect(isDeepStrictEqual(indexedResults[row], referenceResults[index]), "index result differs from the unbounded rule", {
        row, index: indexedResults[row]!.event, reference: referenceResults[index]!.event,
      });
    });
    const indexCpuMedianMs = median(indexCpuSamples);
    expect(indexCpuMedianMs < BUDGET_MS,
      `median index attribution CPU time took ${indexCpuMedianMs.toFixed(1)} ms`,
      { wallMedianMs: indexMedianMs, cpuMedianMs: indexCpuMedianMs });
    const leaseCpuMedianMs = median(leaseCpuSamples);
    const leaseCpuBudgetMs = Math.max(BUDGET_MS * 1.5, Math.min(2_000, referenceMsPerRow * 3));
    expect(leaseCpuMedianMs < leaseCpuBudgetMs,
      `median lease CPU time took ${leaseCpuMedianMs.toFixed(1)} ms (load-robust budget ${leaseCpuBudgetMs.toFixed(1)} ms)`,
      { wallMedianMs: leaseMedianMs, cpuMedianMs: leaseCpuMedianMs, referenceMsPerRow });
    expect(indexStats.contextIndex && indexStats.lookups === 1 && indexStats.boundReached === 0 &&
      indexStats.budgetExhausted === 0 && indexStats.rowReads === 0 && indexStats.contextRows === repoRows,
    "unexpected index stats", indexStats);
    expect(lookups.executions() === 1, "the batch issued more than one context read", lookups.executions());
    expect(leaseLookupSamples.every((executions) => executions === 1), "lease issued more than one context read", leaseLookupSamples);
    expect(indexConsistency === repoRows, "busy index is not exact", indexConsistency);
    return measurements.busySession;
  });
}

// ---------------------------------------------------------------------------
// 4. Older ledgers: first open, 0.7.36 until covered, bounded backfill

type LegacyShape = { rows: number; repoEvery: number };

/** A ledger shaped by 0.7.36: repo rows in many (repo, branch) groups, no index. */
function buildLegacyLedger(file: string, shape: LegacyShape, seed: number) {
  const buffer = openLedger(file);
  const random = mulberry32(seed);
  const pick = <T>(values: readonly T[]) => values[Math.floor(random() * values.length)]!;
  const sessions = Array.from({ length: 12 }, (_, index) => `legacy-${index}`);
  const rows: RawRow[] = [];
  for (let index = 0; index < shape.rows; index += 1) {
    const repo = index % shape.repoEvery === 0;
    rows.push({
      id: `legacy-${seed}-${index}`,
      sessionId: random() < 0.97 ? pick(sessions) : null,
      observedAt: iso(T0 - 12 * HOUR + Math.floor((index * 24 * HOUR) / shape.rows)),
      repoHash: repo ? pick([...REPO_VALUES, REPO_A, REPO_A]) : null,
      branchHash: repo && random() < 0.6 ? pick([BRANCH_A, BRANCH_B]) : null,
      dataMode: repo && random() < 0.05 ? "evidence" : "metadata",
      tokens: !repo && random() < 0.2,
    });
  }
  insertRaw(buffer.database, rows);
  buffer.close();
  downgradeTo0736(file).close();
}

function copyLedger(from: string, to: string) {
  fs.copyFileSync(from, to);
}

async function olderLedgers(dir: string) {
  const legacy = path.join(dir, "legacy.sqlite");
  const LEGACY_ROWS = 200_000;
  try {
    buildLegacyLedger(legacy, { rows: LEGACY_ROWS, repoEvery: 50 }, 7);
  } catch (error) {
    checks.push({ name: "legacy_fixture", ok: false, error: error instanceof Error ? error.message : String(error) });
    return;
  }

  await check("first_open_of_an_older_ledger_installs_an_empty_index_and_backfills_nothing", () => {
    const api = indexApi();
    const file = path.join(dir, "first-open.sqlite");
    copyLedger(legacy, file);
    const steps: Array<{ step: string; durationMs: number }> = [];
    const started = performance.now();
    const buffer = openLedger(file, (step) => steps.push({ step: step.step, durationMs: step.durationMs }));
    const openMs = performance.now() - started;
    const db = buffer.database;
    const install = steps.find((step) => step.step === "ledger.session_context_index");
    const status = api.sessionContextIndexStatus(db);
    const indexed = (db.prepare(`select count(*) as n from session_repo_contexts`).get() as { n: number }).n;
    const repoRows = (db.prepare(`select count(*) as n from buffered_events where repo_hash is not null`).get() as { n: number }).n;
    const probePlan = planOf(db, `select 1 from buffered_events indexed by idx_events_repo where repo_hash is not null limit 1`, []);
    // A second open of the same ledger finds everything installed.
    buffer.close();
    const reopenSteps: Array<{ step: string; durationMs: number }> = [];
    const reopened = openLedger(file, (step) => reopenSteps.push({ step: step.step, durationMs: step.durationMs }));
    const reopenInstall = reopenSteps.find((step) => step.step === "ledger.session_context_index");
    const reopenedStatus = api.sessionContextIndexStatus(reopened.database);
    reopened.close();
    measurements.firstOpen = {
      ledgerRows: LEGACY_ROWS,
      repoRows,
      openMs: Math.round(openMs),
      installStepMs: install ? Math.round(install.durationMs * 1000) / 1000 : null,
      reopenInstallStepMs: reopenInstall ? Math.round(reopenInstall.durationMs * 1000) / 1000 : null,
      slowestSteps: [...steps].sort((a, b) => b.durationMs - a.durationMs).slice(0, 4)
        .map((step) => ({ step: step.step, ms: Math.round(step.durationMs) })),
      probePlan,
    };
    expect(install, "open did not time the session context index step", steps.map((step) => step.step));
    expect(indexed === 0 && status.state === "backfilling" && status.backfill.rowsVisited === 0 &&
      status.indexedRows === 0 && status.inheritanceSource === "session_scan",
    "first open did backfill work or misreported its state", { indexed, status });
    expect(probePlan.some((detail) => /SEARCH buffered_events USING COVERING INDEX idx_events_repo \(repo_hash>\?\)/.test(detail)),
      "install probe is not a covering-index search", probePlan);
    expect(reopenInstall && isDeepStrictEqual(reopenedStatus, status),
      "reopen changed the index state", { reopenInstall, reopenedStatus });
    return measurements.firstOpen;
  });

  await check("until_the_backfill_completes_inheritance_is_exactly_0736", () => {
    const api = indexApi();
    const file = path.join(dir, "uncovered.sqlite");
    copyLedger(legacy, file);
    const buffer = openLedger(file);
    const db = buffer.database;
    // Token rows in every legacy session, including windows over the scan bound.
    const events = Array.from({ length: 240 }, (_, index) =>
      tokenEvent(uuid(0x60_0000 + index), `legacy-${index % 12}`, iso(T0 - 11 * HOUR + index * 5 * 60_000)));
    const inputs = events.map((event) => ({ event }));
    const Batch = SessionAttributionBatch;
    const before = new Batch(db, inputs);
    const scanned = new Batch(db, inputs, { contextIndex: false });
    const beforeStats = before.stats();
    const sameAs0736 = events.every((event) => isDeepStrictEqual(before.attribute(event), scanned.attribute(event)));
    // One bounded batch does not cover the ledger either.
    api.backfillSessionContextIndex(db, 100);
    const partial = new Batch(db, inputs);
    const partialSame = events.every((event) => isDeepStrictEqual(partial.attribute(event), scanned.attribute(event)));
    buffer.close();
    expect(beforeStats.contextIndex === false && isDeepStrictEqual({ ...beforeStats }, { ...scanned.stats() }),
      "an uncovered ledger did not use the 0.7.36 path", { beforeStats, scanStats: scanned.stats() });
    expect(beforeStats.boundReached > 0, "fixture never reached the 0.7.36 bound", beforeStats);
    expect(sameAs0736 && partialSame && partial.stats().contextIndex === false, "an uncovered window diverged from 0.7.36");
    return { stats: beforeStats };
  });

  await check("bounded_backfill_batches_converge_exactly_under_concurrent_writes", () => {
    const api = indexApi();
    const file = path.join(dir, "backfill.sqlite");
    buildLegacyLedger(file, { rows: 24_000, repoEvery: 12 }, 11);
    const buffer = openLedger(file);
    const db = buffer.database;
    const random = mulberry32(99);
    const pick = <T>(values: readonly T[]) => values[Math.floor(random() * values.length)]!;
    let next = 0x70_0000;
    const mutate = () => {
      const max = (db.prepare(`select max(rowid) as n from buffered_events`).get() as { n: number }).n;
      const rowid = (db.prepare(`select rowid from buffered_events where rowid >= ? order by rowid limit 1`)
        .get(1 + Math.floor(random() * max)) as { rowid: number } | undefined)?.rowid ?? max;
      const roll = random();
      if (roll < 0.25) {
        insertRaw(db, [{
          id: uuid(next += 1), sessionId: pick(["legacy-1", "legacy-2", null]), observedAt: iso(T0),
          repoHash: pick([REPO_A, REPO_B, REPO_C, null]), branchHash: pick([BRANCH_A, null]),
        }]);
      } else if (roll < 0.5) {
        // Moves the row's walk key to either side of the cursor.
        db.prepare(`update buffered_events set repo_hash = ?, branch_hash = ? where rowid = ?`)
          .run(pick([REPO_A, REPO_C, "plain-text", null]), pick([BRANCH_A, BRANCH_B, null]), rowid);
      } else if (roll < 0.65) {
        markRawPrivacyDisposition(db, rowid, "local_privacy_violation", iso(T0));
      } else if (roll < 0.8) {
        db.prepare(`delete from buffered_events where rowid = ?`).run(rowid);
      } else {
        db.prepare(`update buffered_events set session_id = ? where rowid = ?`).run(pick(["legacy-3", null]), rowid);
      }
    };
    const runs: unknown[] = [];
    for (const batchSize of [1, 2, 3, 7, 64, 500]) {
      // Reset to a first-install state: empty index, walk not started.
      db.exec(`delete from session_repo_contexts;
        update session_repo_context_control set backfill_complete = 0,
          backfill_cursor_repo_hash = null, backfill_cursor_branch_hash = null,
          backfill_cursor_rowid = null, backfill_rows_visited = 0, backfill_rows_indexed = 0,
          backfill_batches = 0, backfill_last_batch_at = null, backfill_completed_at = null,
          ledger_context_rows = 0, indexed_key_checksum = 0, ledger_key_checksum = 0,
          integrity_state = 'valid', integrity_failures = 0,
          integrity_last_failure_at = null, integrity_last_failure_reason = null
        where singleton = 1`);
      let batches = 0;
      let visited = 0;
      let maxVisited = 0;
      let state = api.sessionContextIndexState(db);
      while (state === "backfilling") {
        const batch = api.backfillSessionContextIndex(db, batchSize);
        batches += 1;
        visited += batch.visited;
        maxVisited = Math.max(maxVisited, batch.visited);
        state = batch.state;
        if (random() < 0.3) mutate();
        expect(batches < 1_000_000, "backfill did not terminate");
      }
      expect(maxVisited <= batchSize, "a batch visited more rows than its bound", { batchSize, maxVisited });
      const indexedRows = expectIndexExact(db, `batch size ${batchSize}`);
      const status = api.sessionContextIndexStatus(db);
      expect(status.state === "complete" && status.backfill.batches === batches && status.backfill.rowsVisited === visited,
        "control row does not match the walk", { status, batches, visited });
      runs.push({ batchSize, batches, visited, indexedRows });
    }
    buffer.close();
    measurements.backfillRuns = runs;
    return runs;
  });

  await check("maintenance_deadline_stages_run_the_backfill_in_budgeted_slices", () => {
    const api = indexApi();
    const stage = backfillStage();
    const file = path.join(dir, "stage.sqlite");
    copyLedger(legacy, file);
    const buffer = openLedger(file);
    const db = buffer.database;
    // The first row starts before the deadline and the second row observes it.
    // This proves the clock is checked inside the transaction's row loop.
    const readings = [0, 0, 0, 11, 11];
    let reading = 0;
    const one = stage(db, {
      remainingMs: 10,
      batchSize: 50,
      now: () => readings[Math.min(reading++, readings.length - 1)]!,
      wallNow: () => new Date("2026-09-24T09:00:00.000Z"),
    });
    const afterOne = api.sessionContextIndexStatus(db);
    const none = stage(db, { remainingMs: 0, batchSize: 50 });
    const hardBoundFile = path.join(dir, "hard-bound.sqlite");
    copyLedger(legacy, hardBoundFile);
    const hardBoundBuffer = openLedger(hardBoundFile);
    const hardBoundReadings = [0, 0, 301, 301];
    let hardBoundReading = 0;
    const hardBound = stage(hardBoundBuffer.database, {
      remainingMs: SESSION_CONTEXT_BACKFILL_STAGE_MS,
      batchSize: 500,
      now: () => hardBoundReadings[Math.min(hardBoundReading++, hardBoundReadings.length - 1)]!,
      wallNow: () => new Date("2026-09-24T09:00:01.000Z"),
    });
    hardBoundBuffer.close();
    const emitted: string[] = [];
    const deadline = runDeadlineMaintenanceStages(db, {
      deadlineMs: 30_000, teardownMarginMs: 1_000, retentionDays: 90, parityReady: true,
      onDurableCommit: (progress) => {
        emitted.push(progress.stage);
        return true;
      },
    });
    const afterJob = api.sessionContextIndexStatus(db);
    let jobs = 1;
    while (api.sessionContextIndexState(db) === "backfilling") {
      runDeadlineMaintenanceStages(db, { deadlineMs: 30_000, teardownMarginMs: 1_000, retentionDays: 90, parityReady: true });
      jobs += 1;
      expect(jobs < 10_000, "maintenance never completed the backfill");
    }
    const indexedRows = expectIndexExact(db, "after maintenance");
    const Batch = SessionAttributionBatch;
    const events = Array.from({ length: 120 }, (_, index) =>
      tokenEvent(uuid(0x61_0000 + index), `legacy-${index % 12}`, iso(T0 - 11 * HOUR + index * 10 * 60_000)));
    const covered = new Batch(db, events.map((event) => ({ event })));
    const coveredStats = covered.stats();
    const divergent = events.filter((event) =>
      !isDeepStrictEqual(covered.attribute(event), unboundedAttribution(db, event, null, null)));
    buffer.close();
    const result = {
      oneBatch: { rows: one.rows, state: one.state, rowsVisited: afterOne.backfill.rowsVisited },
      zeroBudget: { rows: none.rows, state: none.state },
      injectedDeadline: { rows: hardBound.rows, ms: hardBound.ms, batchSize: hardBound.batchSize, state: hardBound.state },
      firstJob: { emitted, backfill: deadline.sessionContextIndex, rowsVisited: afterJob.backfill.rowsVisited },
      jobsToComplete: jobs,
      indexedRows,
      coveredStats,
    };
    measurements.maintenance = result;
    expect(one.rows === 1 && one.rows <= SESSION_CONTEXT_BACKFILL_UNIT_ROWS &&
      one.ms === 11 && afterOne.backfill.rowsVisited === one.rows && one.state === "backfilling" &&
      afterOne.backfill.lastBatchAt === "2026-09-24T09:00:00.000Z" &&
      Date.parse(afterOne.backfill.lastBatchAt) > Date.parse("2020-01-01T00:00:00.000Z"),
      "the row loop did not stop and commit exactly one small unit before the deadline", result.oneBatch);
    expect(none.rows === 0 && none.state === "backfilling", "a zero budget still ran a batch", result.zeroBudget);
    expect(hardBound.rows === 0 && hardBound.ms === 301 &&
      hardBound.rows <= SESSION_CONTEXT_BACKFILL_UNIT_ROWS && hardBound.state === "backfilling",
      "the injected 301 ms batch was not stopped at the deadline", result.injectedDeadline);
    expect(isDeepStrictEqual(emitted, ["wal_checkpoint", "retention", "fill_pending_event_links"]),
      "deadline stages emitted a different progress list", emitted);
    expect(deadline.sessionContextIndex && deadline.sessionContextIndex.rows > 0 &&
      deadline.sessionContextIndex.batchSize === SESSION_CONTEXT_BACKFILL_UNIT_ROWS,
    "the deadline job did not use small committed backfill units", deadline.sessionContextIndex);
    expect(coveredStats.contextIndex && coveredStats.boundReached === 0 && divergent.length === 0,
      "the covered ledger did not attribute through the index exactly", { coveredStats, divergent: divergent.length });
    return result;
  });
}

async function contention(dir: string) {
  await check("backfill_slice_defers_on_writer_contention_instead_of_failing_the_job", () => {
    const api = indexApi();
    const stage = backfillStage();
    const file = path.join(dir, "contention.sqlite");
    buildLegacyLedger(file, { rows: 4_000, repoEvery: 4 }, 41);
    openLedger(file).close();
    // The collector parent holds the write lock; the maintenance child does not wait.
    const parent = new Database(file, { timeout: 0 });
    const child = new Database(file, { timeout: 0 });
    try {
      parent.exec("begin immediate");
      const contended = stage(child, { remainingMs: 1_000, batchSize: 50 });
      parent.exec("rollback");
      const free = stage(child, { remainingMs: 1_000, batchSize: 50 });
      const status = api.sessionContextIndexStatus(child);
      const result = {
        contended: { rows: contended.rows, ms: contended.ms, state: contended.state, contended: contended.contended },
        afterRelease: { rows: free.rows, ms: free.ms, state: free.state, contended: free.contended },
        rowsVisited: status.backfill.rowsVisited,
      };
      expect(contended.contended && contended.rows === 0 && contended.state === "backfilling",
        "a contended slice did not defer", result);
      expect(contended.ms < SESSION_CONTEXT_BACKFILL_STAGE_MS,
        "the optional stage inherited the maintenance worker's long busy timeout", result);
      expect(!free.contended && free.rows > 0 && status.backfill.rowsVisited === free.rows,
        "the next slice did not resume the walk", result);
      return result;
    } finally {
      parent.close();
      child.close();
    }
  });
}

// ---------------------------------------------------------------------------
// 5. Status

async function statusReporting(dir: string) {
  await check("status_reports_index_size_and_backfill_progress", async () => {
    const api = indexApi();
    const file = path.join(dir, "status.sqlite");
    buildLegacyLedger(file, { rows: 6_000, repoEvery: 10 }, 21);
    const buffer = openLedger(file);
    const db = buffer.database;
    api.backfillSessionContextIndex(db, 100);
    let refresh: ((failure?: "maintenance_failed") => boolean) | null = null;
    const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
      registerStatusRefresher: (refresher) => {
        refresh = refresher;
      },
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => resolve());
    });
    const address = server.address();
    expect(address && typeof address !== "string", "server did not bind");
    const read = async () => {
      const response = await fetch(`http://127.0.0.1:${address.port}/status`);
      expect(response.ok, `/status answered ${response.status}`);
      return ((await response.json()) as { sessionAttribution?: unknown }).sessionAttribution;
    };
    try {
      const during = await read();
      const expectedDuring = api.sessionContextIndexStatus(db);
      while (api.sessionContextIndexState(db) === "backfilling") api.backfillSessionContextIndex(db, 500);
      expect(refresh, "server did not register a status refresher");
      (refresh as (failure?: "maintenance_failed") => boolean)();
      const after = await read();
      const expectedAfter = api.sessionContextIndexStatus(db);
      const count = (db.prepare(`select count(*) as n from session_repo_contexts`).get() as { n: number }).n;
      measurements.status = { during, after };
      expect(isDeepStrictEqual(during, expectedDuring) && expectedDuring.state === "backfilling" &&
        expectedDuring.inheritanceSource === "session_scan" && expectedDuring.backfill.rowsVisited === 100,
      "/status did not report backfill progress", { during, expectedDuring });
      expect(isDeepStrictEqual(after, expectedAfter) && expectedAfter.state === "complete" &&
        expectedAfter.inheritanceSource === "context_index" && expectedAfter.indexedRows === count && count > 0 &&
        expectedAfter.backfill.completedAt !== null,
      "/status did not report the completed index", { after, count });
      return measurements.status;
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      buffer.close();
    }
  });
}

// ---------------------------------------------------------------------------
// 6. Query plans

async function queryPlans(dir: string) {
  await check("index_lookup_backfill_walk_and_install_probe_are_bounded_index_searches", () => {
    const api = indexApi();
    const file = path.join(dir, "plans.sqlite");
    buildLegacyLedger(file, { rows: 3_000, repoEvery: 5 }, 31);
    const buffer = openLedger(file);
    const db = buffer.database;
    const walk = instrumentLookups(db, /from buffered_events(?:\s+e)?\s+(?:indexed by idx_events_repo\s+)?where/);
    // Batches of 7 stop inside (repo, branch) groups, so every walk step runs.
    while (api.sessionContextIndexState(db) === "backfilling") api.backfillSessionContextIndex(db, 7);
    walk.restore();
    const lookup = instrumentLookups(db);
    new SessionAttributionBatch(db, [{ event: tokenEvent(uuid(0x62_0000), "legacy-1", iso(T0)) }]);
    lookup.restore();
    const walkPlans = [...new Set(walk.statements())].map((sql) => {
      const params = Array.from({ length: (sql.match(/\?/g) ?? []).length }, (_, index) => (index === 0 ? REPO_A : 1));
      return { sql: sql.replace(/\s+/g, " ").slice(0, 160), plan: planOf(db, sql, params) };
    });
    const lookupPlans = lookup.statements().map((sql) => planOf(db, sql, ["legacy-1", iso(T0 - WINDOW_MS), iso(T0 + WINDOW_MS), 10]));
    const triggerDelete = planOf(db,
      `delete from session_repo_contexts where session_id = ? and observed_at = ? and source_rowid = ?`,
      ["legacy-1", iso(T0), 1]);
    buffer.close();
    measurements.plans = { walkPlans, lookupPlans, triggerDelete };
    expect(walkPlans.length >= 4, "the walk did not exercise its keyset steps", walkPlans);
    for (const { sql, plan } of walkPlans) {
      expect(!plan.some((detail) => /^SCAN /.test(detail) || /TEMP B-TREE/.test(detail)), "walk statement scans or sorts", { sql, plan });
      expect(plan.some((detail) => /SEARCH (?:buffered_events|e) USING (?:COVERING INDEX idx_events_repo|INTEGER PRIMARY KEY)/.test(detail)),
        "walk statement is not an index search", { sql, plan });
    }
    expect(lookupPlans.length === 1 && lookupPlans[0]!.some((detail) =>
      /SEARCH (?:session_repo_contexts|c) USING PRIMARY KEY \(session_id=\? AND observed_at>\? AND observed_at<\?\)/.test(detail)) &&
      lookupPlans[0]!.some((detail) => /SEARCH e USING INTEGER PRIMARY KEY \(rowid=\?\)/.test(detail)) &&
      !lookupPlans[0]!.some((detail) => /TEMP B-TREE|^SCAN /.test(detail)),
    "context lookup is not one primary-key range", lookupPlans);
    expect(triggerDelete.some((detail) => /SEARCH session_repo_contexts USING PRIMARY KEY \(session_id=\? AND observed_at=\? AND source_rowid=\?\)/.test(detail)),
      "trigger maintenance is not a primary-key point write", triggerDelete);
    return measurements.plans;
  });
}

// ---------------------------------------------------------------------------
// 7. Capture-time cost

async function captureCost(dir: string) {
  await check("capture_time_index_adds_bounded_insert_cost", () => {
    indexApi();
    const rows = 30_000;
    const build = (name: string, withIndex: boolean) => {
      const buffer = openLedger(path.join(dir, name));
      if (!withIndex) {
        buffer.database.exec(`drop trigger trg_events_session_context_insert;
          drop trigger trg_events_session_context_update; drop trigger trg_events_session_context_delete;`);
      }
      const batch = Array.from({ length: rows }, (_, index) => ({
        id: `${name}-${index}`,
        sessionId: `cost-${index % 40}`,
        observedAt: iso(T0 + index * 1_000),
        repoHash: index % 200 === 0 ? REPO_A : null,
      }));
      const started = performance.now();
      insertRaw(buffer.database, batch);
      const insertMs = performance.now() - started;
      const updateStarted = performance.now();
      buffer.database.transaction(() => {
        const update = buffer.database.prepare(`update buffered_events set repo_hash = ? where id = ?`);
        for (let index = 1; index < rows; index += 100) update.run(REPO_B, `${name}-${index}`);
      })();
      const updateMs = performance.now() - updateStarted;
      buffer.close();
      return { insertMs, updateMs };
    };
    // Warm once, then alternate so neither side owns the cache or the load.
    build("cost-warm.sqlite", true);
    const without: Array<{ insertMs: number; updateMs: number }> = [];
    const withIndex: Array<{ insertMs: number; updateMs: number }> = [];
    for (let round = 0; round < 5; round += 1) {
      without.push(build(`cost-without-${round}.sqlite`, false));
      withIndex.push(build(`cost-with-${round}.sqlite`, true));
    }
    const typical = (runs: Array<{ insertMs: number; updateMs: number }>, key: "insertMs" | "updateMs") =>
      median(runs.map((run) => run[key]));
    const insertMsWithout = typical(without, "insertMs");
    const insertMsWith = typical(withIndex, "insertMs");
    const result = {
      rows,
      repoRowShare: 1 / 200,
      insertSamplesWithout: without.map((run) => Math.round(run.insertMs)),
      insertSamplesWith: withIndex.map((run) => Math.round(run.insertMs)),
      insertMsWithout: Math.round(insertMsWithout),
      insertMsWith: Math.round(insertMsWith),
      insertOverhead: Number((insertMsWith / insertMsWithout).toFixed(3)),
      repoUpdatesMsWithout: Math.round(typical(without, "updateMs")),
      repoUpdatesMsWith: Math.round(typical(withIndex, "updateMs")),
    };
    measurements.captureCost = result;
    // Five alternating samples and a median leave room for host noise while
    // still catching meaningful write amplification (>25%).
    expect(result.insertOverhead < 1.25, "the index triggers slowed capture inserts by 25% or more", result);
    return result;
  });
}

async function main() {
  try {
    indexModule = await import("../packages/collector-cli/src/session-context-index") as IndexApi;
  } catch {
    indexModule = null;
  }
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "session-context-index-"));
  try {
    if (process.env.PROBE_CASE === "checksum_over_2_53") {
      await checksumBeyondSafeInteger(dir);
      const failed = checks.filter((entry) => !entry.ok);
      console.log(JSON.stringify({ status: failed.length === 0 ? "PASS" : "FAIL", checks, failed: failed.length }, null, 2));
      if (failed.length > 0) process.exitCode = 1;
      return;
    }
    await checksumBeyondSafeInteger(dir);
    await consistency(dir);
    await corruptionSafety(dir);
    await equivalence(dir);
    await busySession(dir);
    await olderLedgers(dir);
    await contention(dir);
    await statusReporting(dir);
    await queryPlans(dir);
    await captureCost(dir);
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
