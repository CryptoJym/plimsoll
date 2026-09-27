import assert from "node:assert/strict";
import { gunzipSync } from "node:zlib";
import Database from "better-sqlite3";

import { LearningFactStore } from "../packages/collector-cli/src/learning-facts";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { MAX_POST_BYTES } from "../packages/collector-cli/src/http-transport";
import { aggregateToolStatsWeek, ensureWeeklyToolStatsSchema } from "../packages/collector-cli/src/weekly-tool-stats";
import { uploadCompletedToolStatsWeek } from "../packages/collector-cli/src/weekly-tool-stats-upload";

async function main() {
  const db = new Database(":memory:");
  try {
    new LearningFactStore(db);
    db.exec(`create table buffered_events (
      id text primary key, source text, session_id text, event_type text, observed_at text,
      workspace_id text, device_id text, data_mode text
    );`);
    ensureWeeklyToolStatsSchema(db);
    const event = db.prepare(`insert into buffered_events values (?,?,?,?,?,?,?,?)`);
    const fact = db.prepare(`insert into tool_attempt_facts
      (operation_id,source,session_id,tool_class,tool_name,started_at,ended_at,duration_ms,
       result_status,error_category,retry_of,created_at,updated_at)
      values (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const dimension = db.prepare(`insert into tool_stat_attempt_dimensions
      (operation_id,event_id,workspace_id,device_id,runtime_version,collector_version)
      values (?,?,?,?,?,?)`);
    const addTool = (id: string, session: string, at: string, runtimeVersion: string) => {
      event.run(id, "codex", session, "tool_use", at, "tenant-oversize", "device-oversize", "metadata");
      fact.run(id, "codex", session, "compute", "shell", at, at, 0,
        "success", "unknown", null, at, at);
      dimension.run(id, id, "tenant-oversize", "device-oversize", runtimeVersion, "0.7.43");
    };
    db.transaction(() => {
      for (let index = 0; index < 16_000; index += 1) {
        const id = `tool-${index.toString().padStart(5, "0")}`;
        const session = `session-${index.toString().padStart(5, "0")}`;
        addTool(id, session, "2026-09-21T12:00:00.000Z", "0.153.0");
      }
      for (let cell = 0; cell < 800; cell += 1) {
        for (let index = 0; index < 50; index += 1) {
          const id = `second-${cell}-${index}`;
          addTool(id, `session-${id}`, "2026-09-28T12:00:00.000Z", `0.153.${cell}`);
        }
      }
      for (let cell = 0; cell < 10_000; cell += 1) {
        const id = `third-${cell}`;
        addTool(id, `session-${id}`, "2026-10-05T12:00:00.000Z", `1.0.${cell}`);
      }
      for (let cell = 0; cell < 10_000; cell += 1) {
        const id = `fourth-${cell}`;
        addTool(id, `session-${id}`, "2026-10-12T12:00:00.000Z", `2.0.${cell}`);
      }
      addTool("next-tool", "next-session", "2026-10-19T12:00:00.000Z", "0.153.0");
    })();
    const large = aggregateToolStatsWeek(db, {
      workspaceId: "tenant-oversize", deviceId: "device-oversize", weekStart: "2026-09-21",
    });
    assert.equal(large.cells[0]?.attempts, 16_000);
    assert.ok(Buffer.byteLength(JSON.stringify(large)) > MAX_POST_BYTES,
      "round-1 all-session shape must exceed the existing transport limit");
    db.prepare(`insert into weekly_tool_stats_control(workspace_id,device_id,first_week) values(?,?,?)`)
      .run("tenant-oversize", "device-oversize", "2026-09-21");
    const config = collectorConfigSchema.parse({
      tenantId: "tenant-oversize", deviceId: "device-oversize",
      cloudDeviceId: "11111111-1111-4111-8111-111111111111", installKey: "pli_fixture_key",
      uploadUrl: "http://127.0.0.1:61777/api/work-intelligence/ingest",
    });
    const received: Array<{ report: Record<string, unknown>; stats: Record<string, unknown>; bytes: number }> = [];
    const fetchImpl = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = String(init?.body ?? "");
      assert.ok(Buffer.byteLength(body) <= MAX_POST_BYTES);
      const report = JSON.parse(body) as Record<string, unknown>;
      const wireStats = report.toolStats as { weekStart?: string; encoding?: string; data?: string };
      const stats = wireStats.encoding === "gzip-base64"
        ? JSON.parse(gunzipSync(Buffer.from(wireStats.data!, "base64")).toString("utf8")) as Record<string, unknown>
        : wireStats as Record<string, unknown>;
      received.push({ report, stats, bytes: Buffer.byteLength(body) });
      const stored = db.prepare(`select digest from weekly_tool_stats_uploads where week_start=?`)
        .get(stats.weekStart) as { digest: string };
      return new Response(JSON.stringify({ schema: "fleet-device-report-receipt/v1",
        deviceId: report.deviceId, reportSequence: report.reportSequence,
        disposition: "accepted", nextReportSequence: Number(report.reportSequence) + 1,
        toolStatsDigest: stored.digest }), { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const clock = { now: () => new Date("2026-10-26T00:05:00.000Z"), fetchImpl };
    for (let week = 0; week < 3; week += 1) {
      assert.equal(await uploadCompletedToolStatsWeek(config, db, clock), "accepted");
    }
    assert.equal(await uploadCompletedToolStatsWeek(config, db, { ...clock, maxReportBytes: 2_000 }), "accepted");
    assert.equal(await uploadCompletedToolStatsWeek(config, db, clock), "accepted");
    assert.deepEqual(received.map((row) => row.stats.weekStart),
      ["2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19"]);
    assert.deepEqual(received.map((row) => row.report.reportSequence), [1, 2, 3, 4, 5]);
    const first = received[0]!.stats as {
      coverage: Array<{ sessions: number; sessionsWithToolEvents: number }>;
      cells: Array<{ attempts: number; sessions: unknown[]; sessionListState: string }>;
      countsOnly: boolean;
    };
    assert.deepEqual(first.coverage, [{ runtime: "codex", sessions: 16_000, sessionsWithToolEvents: 16_000 }]);
    assert.equal(first.cells[0]?.attempts, 16_000);
    assert.equal(first.cells[0]?.sessionListState, "capped");
    assert.ok(first.cells[0]!.sessions.length < 16_000);
    assert.equal(first.countsOnly, false);
    const second = received[1]!.stats as { countsOnly: boolean; cells: Array<{ attempts: number;
      sessions: unknown[]; sessionListState: string }> };
    assert.equal(second.countsOnly, true);
    assert.equal(second.cells.length, 800);
    assert.equal(second.cells.reduce((sum, cell) => sum + cell.attempts, 0), 40_000);
    assert.ok(second.cells.every((cell) => cell.sessions.length === 0 && cell.sessionListState === "counts_only"));
    const third = received[2]!.stats as { countsOnly: boolean; cells: Array<{ attempts: number;
      sessions: unknown[]; sessionListState: string }> };
    assert.equal((received[2]!.report.toolStats as { encoding?: string }).encoding, "gzip-base64");
    assert.equal(third.countsOnly, true);
    assert.equal(third.cells.length, 10_000);
    assert.equal(third.cells.reduce((sum, cell) => sum + cell.attempts, 0), 10_000);
    assert.ok(third.cells.every((cell) => cell.sessions.length === 0 && cell.sessionListState === "counts_only"));
    const fourth = received[3]!.stats as { countsOnly: boolean; versionsFolded: boolean;
      cells: Array<{ attempts: number; runtimeVersion: string; collectorVersion: string }> };
    assert.equal(fourth.countsOnly, true);
    assert.equal(fourth.versionsFolded, true);
    assert.equal(fourth.cells.length, 1);
    assert.deepEqual([fourth.cells[0]?.attempts, fourth.cells[0]?.runtimeVersion,
      fourth.cells[0]?.collectorVersion], [10_000, "unknown", "unknown"]);
    assert.ok(received[3]!.bytes <= 2_000);
    assert.equal((received[4]!.stats.cells as Array<{ attempts: number }>)[0]?.attempts, 1);
    const toolClasses = ["compute", "local_io", "network", "coordination", "other"];
    const toolNames = ["continue", "validate", "test", "edit", "read", "write", "shell", "mcp", "browser", "review", "other"];
    const runtimes = ["codex", "claude_code", "grok", "gemini_cli", "anthropic_admin",
      "anthropic_usage", "github", "openai_usage", "manual", "unknown"];
    const largestCount = 2_147_483_647;
    const boundedStats = { weekStart: "2026-09-21", countsOnly: true, versionsFolded: true,
      coverage: runtimes.map((runtime) => ({ runtime, sessions: largestCount,
        sessionsWithToolEvents: largestCount })),
      cells: toolClasses.flatMap((toolClass) => toolNames.flatMap((toolName) => runtimes.map((runtime) => ({
        toolClass, toolName, runtime, runtimeVersion: "unknown", collectorVersion: "unknown",
        attempts: largestCount, failures: 1_073_741_823, unknown: 1_073_741_824,
        retries: largestCount, longestChain: largestCount,
        sessionListState: "counts_only", sessions: [],
      })))) };
    const boundedBody = JSON.stringify({ schema: "fleet-device-report/v1", kind: "tool_stats",
      tenantId: "tenant-oversize", deviceId: config.cloudDeviceId, reportSequence: 5,
      observedAt: "2026-10-26T00:05:00.000Z", toolStats: boundedStats });
    assert.equal(boundedStats.cells.length, 550);
    assert.ok(Buffer.byteLength(boundedBody) < MAX_POST_BYTES,
      "every admitted class/name/runtime combination fits after the final fallback");
    console.log("oversized weekly tool statistics: 5/5 weeks accepted; capped, counts-only, compressed, folded and next-week liveness pass");
  } finally {
    db.close();
  }
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
