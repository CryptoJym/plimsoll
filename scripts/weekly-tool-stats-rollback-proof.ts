import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { deterministicLearningFactId } from "../packages/collector-cli/src/learning-facts";
import { aggregateToolStatsWeek } from "../packages/collector-cli/src/weekly-tool-stats";

async function main() {
  const oldRoot = process.argv[2];
  if (!oldRoot || !fs.existsSync(path.join(oldRoot, "packages/collector-cli/src/buffer.ts"))) {
    throw new Error("0.7.43 source tree required");
  }
  const oldPackage = JSON.parse(fs.readFileSync(path.join(oldRoot, "packages/collector-cli/package.json"), "utf8")) as { version: string };
  assert.equal(oldPackage.version, "0.7.43");
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? process.cwd(), "plimsoll-weekly-rollback-"));
  const ledger = path.join(root, "ledger.sqlite");
  const options = { workspaceId: "tenant-rollback", deviceId: "device-rollback", delivery: { enabled: true } };
  try {
    const current = new LocalEventBuffer(ledger, options);
    const operationId = deterministicLearningFactId(["weekly-stats-rollback"]);
    const eventId = "rollback-tool-event";
    current.database.prepare(`insert into buffered_events
      (id,source,session_id,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id,privacy_generation)
      values (?,?,?,?,?,?,?,?,?,?,?)`).run(eventId, "codex", "rollback-session", "tool_use", "metadata",
        "2026-09-21T12:00:00.000Z", "{}", "2026-09-21T12:00:00.000Z",
        options.workspaceId, options.deviceId, "rollback-fixture");
    current.learningFacts.recordToolSignal({ kind: "attempt", operationId,
      source: "codex", sessionId: "rollback-session", toolClass: "compute", toolName: "shell",
      startedAt: "2026-09-21T12:00:00.000Z" });
    current.database.prepare(`insert into tool_stat_attempt_dimensions
      (operation_id,event_id,workspace_id,device_id,runtime_version,collector_version) values (?,?,?,?,?,?)`)
      .run(operationId, eventId, options.workspaceId, options.deviceId, "unknown", "0.7.43");
    current.database.prepare(`insert into weekly_tool_stats_control(workspace_id,device_id,first_week) values(?,?,?)`)
      .run(options.workspaceId, options.deviceId, "2026-09-21");
    current.database.prepare(`insert into weekly_tool_stats_uploads
      (workspace_id,device_id,week_start,report_sequence,digest,body_json) values(?,?,?,?,?,?)`)
      .run(options.workspaceId, options.deviceId, "2026-09-21", 1, "a".repeat(64), "{}" );
    current.close();
    const oldModule = await import(pathToFileURL(path.join(oldRoot, "packages/collector-cli/src/buffer.ts")).href) as
      { LocalEventBuffer: typeof LocalEventBuffer };
    const rolledBack = new oldModule.LocalEventBuffer(ledger, options);
    assert.equal(rolledBack.learningFacts.attempts().length, 1);
    rolledBack.close();
    const reopened = new LocalEventBuffer(ledger, options);
    const report = aggregateToolStatsWeek(reopened.database, { workspaceId: options.workspaceId,
      deviceId: options.deviceId, weekStart: "2026-09-21" });
    assert.deepEqual([report.cells.length, report.cells[0]?.attempts, report.cells[0]?.unknown], [1, 1, 1]);
    const upload = reopened.database.prepare(`select report_sequence as sequence, digest, body_json as body,
      delivered from weekly_tool_stats_uploads`).get() as { sequence: number; digest: string; body: string; delivered: number };
    assert.deepEqual(upload, { sequence: 1, digest: "a".repeat(64), body: "{}", delivered: 0 });
    reopened.close();
    console.log("F14: 0.7.43 reopened additive ledger and current code recovered 1/1 tool-stat cell and upload state");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
