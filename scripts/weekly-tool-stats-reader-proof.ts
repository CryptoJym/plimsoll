import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { aggregateToolStatsWeek, planToolStatsWeekRead, recordToolStatsDimension,
  TOOL_STATS_READ_DEADLINE_ERROR } from "../packages/collector-cli/src/weekly-tool-stats";
import { aggregateToolStatsWeekOffThread } from "../packages/collector-cli/src/weekly-tool-stats-reader";
import { uploadCompletedToolStatsWeek } from "../packages/collector-cli/src/weekly-tool-stats-upload";

// 0.7.52: the week's read runs on a reader thread, through the observed_at
// index, in one-hour chunks, under a deadline. On 2026-11-01 the same read,
// planned without statistics through the workspace index, held a collector's
// main thread for 3.5 hours.
const WEEK = "2026-09-21";
const INPUT = { workspaceId: "tenant-1", deviceId: "device-1", weekStart: WEEK };

/** A week whose sessions straddle the one-hour read chunks and both week boundaries. */
function seed(file: string): LocalEventBuffer {
  const buffer = new LocalEventBuffer(file);
  const insert = buffer.database.prepare(`insert into buffered_events
    (id, source, session_id, event_type, data_mode, observed_at, payload_json, created_at, workspace_id, device_id)
    values (?, ?, ?, ?, ?, ?, '{}', ?, ?, 'device-1')`);
  const rows: Array<[string, string, string, string, string, string, string]> = [
    // s1: seen in hour 0 without a tool, its tool event in hour 1 (two chunks).
    ["e1", "codex", "s1", "message", "metadata", "2026-09-21T00:30:00.000Z", "tenant-1"],
    ["e2", "codex", "s1", "tool_use", "metadata", "2026-09-21T01:30:00.000Z", "tenant-1"],
    // s2: one chunk, no tool. s3: the week's last second, another runtime, a tool.
    ["e3", "codex", "s2", "message", "metadata", "2026-09-23T12:00:00.000Z", "tenant-1"],
    ["e4", "claude_code", "s3", "tool_result", "metadata", "2026-09-27T23:59:59.000Z", "tenant-1"],
    // Outside the week, on the far side of each boundary; another tenant; evidence.
    ["e5", "codex", "s4", "tool_use", "metadata", "2026-09-20T23:59:59.999Z", "tenant-1"],
    ["e6", "codex", "s5", "tool_use", "metadata", "2026-09-28T00:00:00.000Z", "tenant-1"],
    ["e7", "codex", "s6", "tool_use", "metadata", "2026-09-22T10:00:00.000Z", "other-tenant"],
    ["e8", "codex", "s7", "tool_use", "evidence", "2026-09-22T11:00:00.000Z", "tenant-1"],
  ];
  buffer.database.transaction(() => {
    for (const [id, source, session, type, mode, at, tenant] of rows) insert.run(id, source, session, type, mode, at, at, tenant);
  })();
  const attempt = buffer.database.prepare(`insert into tool_attempt_facts
    (operation_id,source,session_id,tool_class,tool_name,started_at,ended_at,duration_ms,result_status,error_category,retry_of,created_at,updated_at)
    values (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  // A retry chain across two days, and a late unknown in the last hour.
  for (const [id, at, status, retry] of [
    ["op1", "2026-09-21T00:31:00.000Z", "failure", null],
    ["op2", "2026-09-22T09:00:00.000Z", "success", "op1"],
    ["op3", "2026-09-27T23:00:00.000Z", "unknown", null],
  ] as const) {
    attempt.run(id, "codex", "s1", "compute", "shell", at, status === "unknown" ? null : at,
      status === "unknown" ? null : 0, status, status === "failure" ? "tool" : "unknown", retry, at, at);
    recordToolStatsDimension(buffer.database, { operationId: id, eventId: "e2", runtimeVersion: "0.153.0", collectorVersion: "0.7.52" });
  }
  return buffer;
}

const config = collectorConfigSchema.parse({ tenantId: "tenant-1", deviceId: "device-1",
  cloudDeviceId: "11111111-1111-4111-8111-111111111111", installKey: "pli_fixture_key",
  uploadUrl: "http://127.0.0.1:61777/api/work-intelligence/ingest" });

/** Freezes and delivers the week against a stub cloud that acknowledges what was stored. */
async function uploadWith(buffer: LocalEventBuffer, aggregate?: Parameters<typeof uploadCompletedToolStatsWeek>[2] extends infer O
  ? O extends { aggregate?: infer A } ? A : never : never) {
  const db = buffer.database;
  db.prepare(`insert or ignore into weekly_tool_stats_control(workspace_id,device_id,first_week) values('tenant-1','device-1',?)`).run(WEEK);
  const fetchImpl = (async (_url: RequestInfo | URL, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body ?? "")) as { reportSequence: number; deviceId: string; toolStats: { weekStart: string } };
    const pending = db.prepare(`select digest from weekly_tool_stats_uploads where week_start=?`).get(request.toolStats.weekStart) as { digest: string };
    return new Response(JSON.stringify({ schema: "fleet-device-report-receipt/v1", deviceId: request.deviceId,
      reportSequence: request.reportSequence, disposition: "accepted", nextReportSequence: request.reportSequence + 1,
      toolStatsDigest: pending.digest,
      storedToolStatsReport: { weekStart: request.toolStats.weekStart, reportSequence: request.reportSequence, digest: pending.digest } }),
    { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  const status = await uploadCompletedToolStatsWeek(config, db,
    { now: () => new Date("2026-09-28T00:05:00.000Z"), fetchImpl, ...(aggregate ? { aggregate } : {}) });
  const row = db.prepare(`select digest, delivered, body_json as bodyJson from weekly_tool_stats_uploads where week_start=?`)
    .get(WEEK) as { digest: string; delivered: number; bodyJson: string };
  return { status, ...row };
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "weekly-tool-stats-reader-"));
  const checks: Array<{ name: string; passed: boolean; data?: unknown }> = [];
  const check = async (name: string, run: () => unknown) => {
    try { checks.push({ name, passed: true, data: await run() }); }
    catch (error) { checks.push({ name, passed: false }); throw error; }
  };
  const buffer = seed(path.join(root, "week.sqlite"));
  const second = seed(path.join(root, "week-b.sqlite"));
  try {
    await check("coverage_read_is_planned_through_the_observed_at_index_in_hour_chunks", () => {
      const plan = planToolStatsWeekRead(buffer.database, INPUT);
      const [from, to] = plan.coverage.chunks[0]!;
      const lines = (buffer.database.prepare(`explain query plan ${plan.coverage.sql}`)
        .all(INPUT.workspaceId, INPUT.deviceId, from, to) as Array<{ detail: string }>).map((row) => row.detail);
      assert.ok(lines.some((line) => line.includes("idx_events_observed")), lines.join(" | "));
      assert.ok(!lines.some((line) => line.includes("idx_events_workspace_upload")), lines.join(" | "));
      assert.equal(plan.coverage.chunks.length, 168);
      assert.deepEqual([plan.coverage.chunks[0]![0], plan.coverage.chunks[167]![1]],
        ["2026-09-21T00:00:00.000Z", "2026-09-28T00:00:00.000Z"]);
      assert.deepEqual(plan.attempts.chunks, plan.coverage.chunks);
      return { plan: lines, chunks: plan.coverage.chunks.length };
    });
    await check("reader_thread_result_equals_the_in_process_aggregate", async () => {
      const inProcess = aggregateToolStatsWeek(buffer.database, INPUT);
      const offThread = await aggregateToolStatsWeekOffThread(buffer.database, INPUT);
      assert.deepEqual(offThread, inProcess);
      // s1's tool event sits in a later chunk than its first event; the merge keeps it.
      assert.deepEqual(inProcess.coverage, [
        { runtime: "claude_code", sessions: 1, sessionsWithToolEvents: 1 },
        { runtime: "codex", sessions: 2, sessionsWithToolEvents: 1 },
      ]);
      assert.equal(inProcess.cells.length, 1);
      const cell = inProcess.cells[0]!;
      assert.deepEqual({ attempts: cell.attempts, failures: cell.failures, unknown: cell.unknown,
        retries: cell.retries, longestChain: cell.longestChain }, { attempts: 3, failures: 1, unknown: 1, retries: 1, longestChain: 2 });
      assert.deepEqual(cell.sessions.map((session) => session.sessionId), ["s1"]);
      assert.doesNotMatch(JSON.stringify(inProcess), /other-tenant|s4|s5|s6|s7|e1|op1/);
      return inProcess.coverage;
    });
    await check("a_passed_deadline_ends_the_read_before_its_first_chunk", async () => {
      await assert.rejects(aggregateToolStatsWeekOffThread(buffer.database, INPUT, { maxMs: 1, now: () => 0 }),
        new RegExp(TOOL_STATS_READ_DEADLINE_ERROR));
      return { error: TOOL_STATS_READ_DEADLINE_ERROR };
    });
    await check("the_upload_freezes_the_same_report_from_either_read", async () => {
      const inProcess = await uploadWith(buffer);
      const reader = await uploadWith(second, (input) => aggregateToolStatsWeekOffThread(second.database, input));
      assert.deepEqual({ a: inProcess.status, b: reader.status, deliveredA: inProcess.delivered, deliveredB: reader.delivered },
        { a: "accepted", b: "accepted", deliveredA: 1, deliveredB: 1 });
      assert.equal(reader.digest, inProcess.digest, "the frozen report digest is the same from either read");
      assert.deepEqual(JSON.parse(reader.bodyJson), JSON.parse(inProcess.bodyJson));
      assert.doesNotMatch(reader.bodyJson, /other-tenant|op1|e2|PRIVATE/);
      return { digest: reader.digest };
    });
  } finally { buffer.close(); second.close(); }
  console.log(JSON.stringify({ schema: "plimsoll-weekly-tool-stats-reader-proof/v1", checks }, null, 2));
}

main().catch((error) => { console.error(error); process.exit(1); });
