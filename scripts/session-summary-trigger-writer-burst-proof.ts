import assert from "node:assert/strict";

import { ensureSessionSummarySchema, updateSessionSummary } from
  "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { directRead, fixture, initialUntil, sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-trigger-writer-burst", 2);
const rows = 1_880_000;
const burstRows = 1_000;
const writerBudgetMs = 750;
const workspace = "00000000-0000-4000-8000-000000000106";

async function main() {
  const buffer = fixture("trigger-writer-burst.sqlite");
  try {
    const db = buffer.database;
    const insert = db.prepare(`with recursive seq(x) as (
      select @first union all select x + 1 from seq where x < @last
    ) insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, session_id, workspace_id, privacy_generation)
      select printf('00000000-0000-4000-8000-%012d', x), 'codex',
        'assistant_response', 'metadata', '2026-09-20T00:00:00.000Z', '{}', '[]',
        '2026-09-20T00:00:00.000Z', @sessionId, @workspace, printf('generation-%d', x)
      from seq`);
    for (let first = 1; first <= rows; first += 100_000) {
      insert.run({ first, last: Math.min(rows, first + 99_999), sessionId, workspace });
    }
    const burst = (first: number) => {
      const started = performance.now();
      db.transaction(() => insert.run({ first, last: first + burstRows - 1,
        sessionId, workspace })).immediate();
      return performance.now() - started;
    };
    const baselineMs = burst(rows + 1);
    ensureSessionSummarySchema(db);
    const first = await updateSessionSummary(db, sessionId, initialUntil, {
      read: directRead(buffer), maxRows: 1,
    });
    assert.equal(first.complete, false);
    const state = db.prepare(`select accumulator_json as accumulatorJson
      from session_sync_summary_state where session_id = ?`)
      .get(sessionId) as { accumulatorJson: string };
    const accumulator = JSON.parse(state.accumulatorJson) as {
      segments: Record<string, Record<string, unknown>>;
    };
    const sample = accumulator.segments["0"];
    assert.ok(sample);
    // Filled identity/future fields match the size of a live segment more
    // closely than the one-row seed, without scanning the 1.88M-row fixture.
    const realisticSegment = { ...sample,
      repoValue: "a".repeat(64), branchValue: "b".repeat(64),
      accountValue: "c".repeat(64), futureCreatedAt: "2026-10-01T00:00:00.000Z",
    };
    for (let segment = 0; segment < Math.ceil((rows + burstRows) / 4_096); segment += 1) {
      accumulator.segments[String(segment)] = { ...realisticSegment };
    }
    const accumulatorJson = JSON.stringify(accumulator);
    const jsonBytes = Buffer.byteLength(accumulatorJson);
    assert.ok(jsonBytes > 300_000, `fixture JSON too small: ${jsonBytes}`);
    db.prepare(`update session_sync_summary_state set accumulator_json = ?
      where session_id = ?`).run(accumulatorJson, sessionId);
    assert.equal((db.prepare(`select count(*) as n from buffered_events
      where session_id = ?`).get(sessionId) as { n: number }).n, rows + burstRows);
    completion.check("million_row_session_with_many_segments_installed");

    const chargedMs = burst(rows + burstRows + 1);
    console.log(JSON.stringify({ rows: rows + burstRows, segments: Object.keys(accumulator.segments).length,
      jsonBytes, burstRows, baselineMs, chargedMs, writerBudgetMs }));
    assert.ok(chargedMs < writerBudgetMs,
      `1,000-row burst held the writer for ${chargedMs.toFixed(1)} ms`);
    const jsonTriggers = db.prepare(`select name from sqlite_master
      where type = 'trigger' and name like 'trg_session_summary_%'
        and sql like '%accumulator_json%'`).all() as Array<{ name: string }>;
    assert.deepEqual(jsonTriggers, [], "event triggers must use materialized state columns");
    completion.check("thousand_row_burst_below_writer_hold_budget");
    completion.complete();
  } finally { buffer.close(); }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
