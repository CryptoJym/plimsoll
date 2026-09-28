import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { createProofCompletion } from "./lib/proof-completion";

const target = "00000000-0000-4000-8000-000000000106";
const other = "00000000-0000-4000-8000-000000000107";
const lateSession = "00000000-0000-4000-8000-000000000109";
const workspace = "00000000-0000-4000-8000-000000000108";
const until = "2026-09-25T00:00:00.000Z";
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const oldCommit = "375f277b85f7d4ede7db77bf4359c371c0e8a4aa";
const id = (n: number) => `10000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

async function load(repo: string) {
  const root = path.join(repo, "packages/collector-cli/src");
  const b = await import(pathToFileURL(path.join(root, "buffer.ts")).href);
  const s = await import(pathToFileURL(path.join(root, "session-summary.ts")).href);
  const sync = await import(pathToFileURL(path.join(root, "session-sync.ts")).href);
  const config = await import(pathToFileURL(path.join(root, "config.ts")).href);
  return { LocalEventBuffer: b.LocalEventBuffer, ...s,
    collectSessionSnapshots: sync.collectSessionSnapshots,
    runSessionSync: sync.runSessionSync,
    collectorConfigSchema: config.collectorConfigSchema };
}

function read(buffer: any) {
  return async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> =>
    queries.flatMap(query => buffer.database.prepare(query.sql).all(query.params) as T[]);
}

function insert(buffer: any, first: number, last: number, sessionId: string, observedAt: string) {
  const statement = buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
     cost_usd, workspace_id, privacy_generation)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]',
      '2026-09-20T00:00:00.000Z', ?, 2, 3, 0.25, ?, ?)`);
  for (let start = first; start <= last; start += 250) {
    buffer.database.transaction(() => {
      for (let n = start; n <= Math.min(last, start + 249); n++) {
        statement.run(id(n), observedAt, sessionId, workspace, `generation-${n}`);
      }
    }).immediate();
  }
}

async function drain(version: any, buffer: any, max = 10, sessionId = target) {
  let latest: any;
  for (let n = 0; n < max; n++) {
    latest = await version.updateSessionSummary(buffer.database, sessionId, until, { read: read(buffer) });
    if (latest.complete) return latest;
  }
  throw new Error(`summary_not_complete ${JSON.stringify(latest)}`);
}

function state(db: any, sessionId = target) {
  const cols = new Set((db.prepare("pragma table_info(session_sync_summary_state)").all() as any[])
    .map(row => row.name));
  const row = db.prepare(`select schema_version as version,
      ${cols.has("state_generation") ? "state_generation" : "null"} as generation,
      high_water as highWater,
      ${cols.has("scan_boundary") ? "scan_boundary" : "null"} as scanBoundary,
      ${cols.has("cursor_observed_at") ? "cursor_observed_at" : "null"} as cursorObservedAt,
      ${cols.has("cursor_rowid") ? "cursor_rowid" : "null"} as cursorRowid,
      accumulator_json as accumulatorJson
      from session_sync_summary_state where session_id = ?`).get(sessionId) as any;
  return { ...row, accumulator: JSON.parse(row.accumulatorJson) };
}

async function main() {
  const completion = createProofCompletion("session-summary-v44-downgrade", 6);
  const root = process.env.PLIMSOLL_PROOF_ROOT!;
  const dbPath = path.join(root, "downgrade-v44.sqlite");
  const headRepo = repo;
  const oldRepo = path.join(root, "collector-0744");
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", oldRepo, oldCommit], { cwd: repo });
  let buffer: any = null;
  try {
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(oldRepo, "node_modules"), "dir");
  const head = await load(headRepo);
  const old = await load(oldRepo);
  const delivery = await import(pathToFileURL(path.join(headRepo,
    "scripts/lib/delivery-fixture.ts")).href);
  const OldBuffer = old.LocalEventBuffer;
  const HeadBuffer = head.LocalEventBuffer;

  buffer = new OldBuffer(dbPath, { workspaceId: workspace });
  insert(buffer, 1, 5000, target, "2026-09-20T00:00:00.000Z");
  insert(buffer, 5001, 10000, other, "2026-09-21T00:00:00.000Z");
  old.ensureSessionSummarySchema(buffer.database);
  const v3 = await drain(old, buffer);
  assert.equal(v3.snapshot?.events, 5000);
  completion.check("real_0744_initial_v3_summary");
  buffer.close(); buffer = null;

  buffer = new HeadBuffer(dbPath, { workspaceId: workspace });
  head.ensureSessionSummarySchema(buffer.database);
  let converted = await head.updateSessionSummary(buffer.database, target, until,
    { read: read(buffer) });
  let conversionRowsRead = converted.rowsRead;
  for (let pass = 0; pass < 10 && !converted.complete; pass++) {
    converted = await head.updateSessionSummary(buffer.database, target, until,
      { read: read(buffer) });
    conversionRowsRead += converted.rowsRead;
  }
  const v4 = state(buffer.database);
  console.log(JSON.stringify({ phase: "converted", rowsRead: conversionRowsRead,
    fullRecompute: converted.fullRecompute, complete: converted.complete,
    version: v4.version, legacyHighWater: v4.accumulator.legacyHighWater,
    scanBoundary: v4.scanBoundary, cursorRowid: v4.cursorRowid }));
  assert.equal(v4.version, 4);
  assert.equal(v4.accumulator.legacyHighWater, 5000);
  assert.equal(conversionRowsRead, 0);
  assert.equal(v4.scanBoundary, v4.accumulator.scanBoundary);
  assert.equal(v4.cursorRowid, v4.accumulator.cursorRowid);
  completion.check("head_converts_v3_without_rescan");
  buffer.close(); buffer = null;

  buffer = new OldBuffer(dbPath, { workspaceId: workspace });
  old.ensureSessionSummarySchema(buffer.database);
  const db = buffer.database;
  insert(buffer, 10001, 16000, target, "2026-09-22T00:00:00.000Z");
  assert.equal(db.prepare("update buffered_events set session_id = ? where id = ?")
    .run(target, id(9000)).changes, 1);
  const first = await old.updateSessionSummary(db, target, until, { read: read(buffer) });
  const second = await old.updateSessionSummary(db, target, until, { read: read(buffer) });
  const drift = state(db);
  console.log(JSON.stringify({ phase: "downgraded_partial", first: { rowsRead: first.rowsRead,
    complete: first.complete }, second: { rowsRead: second.rowsRead,
    complete: second.complete }, persistedCursorRowid: drift.cursorRowid,
    jsonCursorRowid: drift.accumulator.cursorRowid, persistedScanBoundary: drift.scanBoundary,
    jsonScanBoundary: drift.accumulator.scanBoundary }));
  assert.equal(first.complete, false);
  assert.equal(second.complete, false);
  assert.ok(drift.highWater >= 9000);
  assert.equal(db.prepare("update buffered_events set output_tokens = 13 where id = ?")
    .run(id(9000)).changes, 1);
  const markers = {
    dirty: db.prepare("select 1 from session_sync_summary_dirty where session_id = ?").get(target),
    repairs: db.prepare("select 1 from session_sync_summary_repairs where session_id = ?").get(target),
    rows: db.prepare("select 1 from session_sync_summary_rows where session_id = ? and raw_rowid = 9000")
      .get(target),
  };
  console.log(JSON.stringify({ phase: "historical_edit_after_downgrade", markers }));
  const oldFinal = await drain(old, buffer);
  const oldExpected = old.collectSessionSnapshots(db, { sessionIds: [target], until })[0];
  const oldFenceAccepts = old.sessionSummaryCurrent(db, target, until,
    oldFinal.mutationRevision, oldFinal.highWater);
  const config = old.collectorConfigSchema.parse({ uploadUrl: "http://127.0.0.1:1/ingest",
    tenantId: workspace, installKey: "review-v44-downgrade",
    uploadSigningSecret: "review-v44-downgrade-secret" });
  const wire: any[] = [];
  const sent = await old.runSessionSync(config, { ledgerDb: db, incremental: true,
    sessionIds: [target], until, delayMs: 0, maxAttemptsPerBatch: 1,
    log: () => undefined, fetchImpl: (async (_input: unknown, init?: RequestInit) => {
      const body = String(init?.body ?? "");
      wire.push(...(JSON.parse(body).sessions ?? []));
      return new Response(JSON.stringify({
        ...delivery.acceptedFixtureDelivery(body, config.installKey),
        inserted: wire.length, updated: 0, skippedStale: 0,
      }), { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch });
  console.log(JSON.stringify({ phase: "v44_complete", cachedOutputTokens: oldFinal.snapshot?.outputTokens,
    scratchOutputTokens: oldExpected?.outputTokens, complete: oldFinal.complete,
    oldFenceAccepts, sentSessions: sent.sentSessions,
    wireOutputTokens: wire[0]?.totals?.outputTokens,
    state: { version: state(db).version, generation: state(db).generation } }));
  assert.equal(sent.sentSessions, 1);
  assert.equal(oldFenceAccepts, true);
  assert.equal(oldFinal.complete, true);
  assert.equal(oldFinal.snapshot?.outputTokens, 33_013,
    "real 0.7.44 must include the edited row before accepting the summary");
  assert.equal(wire[0]?.totals?.outputTokens, 33_013,
    "the actual 0.7.44 sync must send the corrected total");
  assert.deepEqual(oldFinal.snapshot, oldExpected,
    "v0.7.44 after v4 conversion must not accept a stale aggregate");
  completion.check("real_0744_wire_equals_scratch_33013");
  assert.equal(drift.cursorRowid, drift.accumulator.cursorRowid,
    "real 0.7.44 writes must update the trigger's materialized cursor");
  assert.equal(drift.scanBoundary, drift.accumulator.scanBoundary);
  assert.equal(drift.cursorObservedAt, drift.accumulator.cursorObservedAt);
  completion.check("0744_cursor_json_and_columns_stay_equal");
  // A downgraded binary can also create a state row after the v4 schema was
  // installed. Its INSERT omits every materialized cursor column.
  insert(buffer, 16_001, 16_001, lateSession, "2026-09-22T00:00:00.000Z");
  const inserted = await drain(old, buffer, 10, lateSession);
  assert.equal(inserted.complete, true);
  const insertedState = state(db, lateSession);
  assert.equal(insertedState.scanBoundary, insertedState.accumulator.scanBoundary);
  assert.equal(insertedState.cursorRowid, insertedState.accumulator.cursorRowid);
  assert.equal(insertedState.cursorObservedAt, insertedState.accumulator.cursorObservedAt);
  completion.check("0744_insert_projects_cursor_from_json");
  buffer.close(); buffer = null;

  buffer = new HeadBuffer(dbPath, { workspaceId: workspace });
  head.ensureSessionSummarySchema(buffer.database);
  let upgraded: any;
  let upgradeRowsRead = 0;
  let upgradeFullRecomputes = 0;
  for (let pass = 0; pass < 10; pass++) {
    upgraded = await head.updateSessionSummary(buffer.database, target, until,
      { read: read(buffer) });
    upgradeRowsRead += upgraded.rowsRead;
    upgradeFullRecomputes += Number(upgraded.fullRecompute);
    if (upgraded.complete) break;
  }
  const expected = head.collectSessionSnapshots(buffer.database, { sessionIds: [target], until })[0];
  console.log(JSON.stringify({ phase: "reupgraded", rowsRead: upgradeRowsRead,
    complete: upgraded.complete, fullRecomputes: upgradeFullRecomputes,
    cachedOutputTokens: upgraded.snapshot?.outputTokens,
    scratchOutputTokens: expected?.outputTokens, version: state(buffer.database).version }));
  assert.deepEqual(upgraded.snapshot, expected,
    "downgrade then lazy reupgrade must preserve a historical edit");
  assert.equal(upgraded.snapshot?.outputTokens, 33_013);
  completion.check("reupgrade_keeps_exact_wire");
  buffer.close(); buffer = null;
  completion.complete();
  } finally {
    if (buffer) buffer.close();
    execFileSync("git", ["worktree", "remove", "--force", oldRepo], { cwd: repo });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
