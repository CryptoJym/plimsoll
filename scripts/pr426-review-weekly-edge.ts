import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { LearningFactStore } from "../packages/collector-cli/src/learning-facts";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { ensureWeeklyToolStatsSchema } from "../packages/collector-cli/src/weekly-tool-stats";
import { uploadCompletedToolStatsWeek } from "../packages/collector-cli/src/weekly-tool-stats-upload";

const config = collectorConfigSchema.parse({ tenantId: "tenant-1", deviceId: "device-1",
  cloudDeviceId: "50000000-0000-4000-8000-000000000005", installKey: "fixture-install-key",
  uploadUrl: "http://127.0.0.1:49410/api/work-intelligence/ingest" });

async function run(receipt: (request: { reportSequence: number; deviceId: string }, digest: string) => Record<string, unknown>) {
  const db = new Database(":memory:");
  try {
    new LearningFactStore(db);
    db.exec(`create table buffered_events (
      id text primary key, source text, session_id text, event_type text, observed_at text,
      workspace_id text, device_id text, data_mode text, metadata text
    )`);
    ensureWeeklyToolStatsSchema(db);
    db.prepare("insert into weekly_tool_stats_control(workspace_id,device_id,first_week) values(?,?,?)")
      .run(config.tenantId, config.deviceId, "2026-09-28");
    const fetchImpl = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const request = JSON.parse(String(init?.body)) as { reportSequence: number; deviceId: string };
      const { digest } = db.prepare("select digest from weekly_tool_stats_uploads where week_start='2026-09-28'")
        .get() as { digest: string };
      return new Response(JSON.stringify({ schema: "fleet-device-report-receipt/v1",
        deviceId: request.deviceId, reportSequence: request.reportSequence,
        nextReportSequence: 42, toolStatsDigest: digest, ...receipt(request, digest) }),
      { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const result = await uploadCompletedToolStatsWeek(config, db,
      { now: () => new Date("2026-10-05T00:05:00.000Z"), fetchImpl });
    const row = db.prepare("select report_sequence as sequence, delivered from weekly_tool_stats_uploads")
      .get() as { sequence: number; delivered: number };
    return { result, ...row };
  } finally { db.close(); }
}

async function main() {
  const conflicting = await run((_request, digest) => ({ disposition: "replay_ignored",
    storedToolStatsReport: { weekStart: "2026-09-28", reportSequence: 41,
      digest: digest === "a".repeat(64) ? "b".repeat(64) : "a".repeat(64) } }));
  assert.deepEqual(conflicting, { result: "conflict", sequence: 1, delivered: 0 });
  console.log("PASS stored_different_digest_is_conflict");

  const malformed = await run(() => ({ disposition: "replay_ignored",
    storedToolStatsReport: { weekStart: "2026-09-21", reportSequence: 41, digest: "a".repeat(64) } }));
  assert.deepEqual(malformed, { result: "retry", sequence: 1, delivered: 0 });
  console.log("PASS malformed_stored_week_does_not_ack_or_rebase");

  const legacyAccepted = await run(() => ({ disposition: "accepted" }));
  assert.deepEqual(legacyAccepted, { result: "accepted", sequence: 1, delivered: 1 });
  console.log("PASS legacy_accepted_receipt_still_acknowledges");
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
