import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { switchFreshLedger, restoreArchivedLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicLearningFactId } from "../packages/collector-cli/src/learning-facts";
import { aggregateToolStatsWeek, utcWeekStart } from "../packages/collector-cli/src/weekly-tool-stats";
import { uploadCompletedToolStatsWeek } from "../packages/collector-cli/src/weekly-tool-stats-upload";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-rollback-week-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const freshAttemptPath = path.join(archiveDirectory, "fresh-attempt.sqlite");
const root = path.join(fixture, "codex");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const week = new Date(`${utcWeekStart(new Date())}T00:00:00.000Z`);
week.setUTCDate(week.getUTCDate() + 7);
const weekStart = week.toISOString().slice(0, 10);
const switchAt = new Date(week.getTime() + 12 * 3_600_000);
const dueAt = new Date(week.getTime() + 7 * 86_400_000 + 5 * 60_000);
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  cloudDeviceId: "50000000-0000-4000-8000-000000000005",
  installKey: "fixture-install-key", uploadUrl: "http://127.0.0.1:49410/api/work-intelligence/ingest",
  captureRoots: [{ source: "codex", rootId: "root", profileId: "profile",
    directory: root, installationEpochId: epoch }] });
const addAttempt = (buffer: LocalEventBuffer, suffix: string, timestamp: string) => {
  const eventId = `event-${suffix}`, sessionId = `session-${suffix}`;
  const operationId = deterministicLearningFactId(["r2-rollback-week", suffix]);
  buffer.database.prepare(`insert into buffered_events
    (id,source,session_id,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id,privacy_generation)
    values (?,?,?,?,?,?,?,?,?,?,?)`).run(eventId, "codex", sessionId, "tool_use", "metadata",
      timestamp, "{}", timestamp, workspace, device, "review-fixture");
  buffer.learningFacts.recordToolSignal({ kind: "attempt", operationId, source: "codex",
    sessionId, toolClass: "compute", toolName: "shell", startedAt: timestamp });
  buffer.database.prepare(`insert into tool_stat_attempt_dimensions
    (operation_id,event_id,workspace_id,device_id,runtime_version,collector_version)
    values (?,?,?,?,?,?)`).run(operationId, eventId, workspace, device, "unknown", "0.7.46");
};
let old: LocalEventBuffer | undefined;
let replacement: LocalEventBuffer | undefined;
async function main() {
  try {
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    old.database.prepare("insert into weekly_tool_stats_control(workspace_id,device_id,first_week) values(?,?,?)")
      .run(workspace, device, weekStart);
    old.close(); old = undefined;
    switchFreshLedger({ ledgerPath: ledger, archivePath, config, now: () => switchAt,
      authorityRoot: path.join(fixture, "lifecycle-authority") });
    replacement = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    addAttempt(replacement, "in-replacement", new Date(switchAt.getTime() + 3_600_000).toISOString());
    const replacementAttempts = aggregateToolStatsWeek(replacement.database,
      { workspaceId: workspace, deviceId: device, weekStart }).cells.reduce((n, c) => n + c.attempts, 0);
    replacement.close(); replacement = undefined;
    restoreArchivedLedger({ ledgerPath: ledger, archivePath, freshAttemptPath,
      authorityRoot: path.join(fixture, "lifecycle-authority") });
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    addAttempt(old, "after-restore", new Date(switchAt.getTime() + 2 * 3_600_000).toISOString());
    let posted: { toolStats: { cells: Array<{ attempts: number }> } } | null = null;
    const fetchImpl = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      posted = JSON.parse(String(init?.body));
      const pending = old!.database.prepare("select digest from weekly_tool_stats_uploads where week_start=?")
        .get(weekStart) as { digest: string };
      return new Response(JSON.stringify({ schema: "fleet-device-report-receipt/v1",
        deviceId: config.cloudDeviceId, reportSequence: 1, disposition: "accepted",
        nextReportSequence: 2, toolStatsDigest: pending.digest,
        storedToolStatsReport: { weekStart, reportSequence: 1, digest: pending.digest } }),
        { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const result = await uploadCompletedToolStatsWeek(config, old.database,
      { now: () => dueAt, fetchImpl });
    const reportedAttempts = posted!.toolStats.cells.reduce((n, c) => n + c.attempts, 0);
    console.log(JSON.stringify({ weekStart, result, replacementAttempts,
      reportedAttempts, actualWeekAttempts: replacementAttempts + reportedAttempts,
      archivePreserved: fs.existsSync(archivePath), freshAttemptPreserved: fs.existsSync(freshAttemptPath) }));
    assert.equal(result, "accepted");
    assert.equal(reportedAttempts, replacementAttempts + 1,
      "rollback must not publish a partial week after restoring the archive");
  } finally {
    replacement?.close(); old?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
