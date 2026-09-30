import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from
  "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicLearningFactId } from "../packages/collector-cli/src/learning-facts";
import { utcWeekStart } from "../packages/collector-cli/src/weekly-tool-stats";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-midweek-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const root = path.join(fixture, "codex");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const monday = new Date(`${utcWeekStart(new Date())}T00:00:00.000Z`);
monday.setUTCDate(monday.getUTCDate() + 7);
const isoAfterDays = (days: number, minutes = 0) =>
  new Date(monday.getTime() + days * 86_400_000 + minutes * 60_000).toISOString();
const weekStart = monday.toISOString().slice(0, 10);
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: [{
    source: "codex", rootId: "root", profileId: "profile", installationEpochId: epoch, directory: root,
  }] });
const input = { ledgerPath: ledger, archivePath, config };
let old: LocalEventBuffer | undefined;
try {
  old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch, enrollmentNow: () => monday });
  const at = isoAfterDays(2, 12 * 60);
  const operationId = deterministicLearningFactId(["pr426-review", "before-switch"]);
  old.database.prepare(`insert into buffered_events
    (id,source,session_id,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id,privacy_generation)
    values (?,?,?,?,?,?,?,?,?,?,?)`).run("event-before", "codex", "session-before", "tool_use",
      "metadata", at, "{}", at, workspace, device, "review-fixture");
  old.learningFacts.recordToolSignal({ kind: "attempt", operationId, source: "codex",
    sessionId: "session-before", toolClass: "compute", toolName: "shell", startedAt: at });
  old.database.prepare(`insert into tool_stat_attempt_dimensions
    (operation_id,event_id,workspace_id,device_id,runtime_version,collector_version)
    values (?,?,?,?,?,?)`).run(operationId, "event-before", workspace, device, "unknown", "0.7.44");
  old.close(); old = undefined;

  const midweek = planFreshLedgerCutover({ ...input,
    now: () => new Date(isoAfterDays(3, 12 * 60)) });
  assert.equal(midweek.status, "refused");
  assert.equal(midweek.reason, "current_utc_week_tool_attempts");
  assert.equal(midweek.currentUtcWeekToolAttempts, 1);
  assert.equal(midweek.nextSafeWindowAt, isoAfterDays(7));
  assert.equal(fs.existsSync(archivePath), false);

  const mondayBeforeAck = planFreshLedgerCutover({ ...input,
    now: () => new Date(isoAfterDays(7, 5)) });
  assert.equal(mondayBeforeAck.status, "refused");
  assert.equal(mondayBeforeAck.reason, "prior_week_report_not_acknowledged");
  assert.equal(mondayBeforeAck.priorUtcWeekToolAttempts, 1);

  old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  old.database.prepare(`insert into weekly_tool_stats_uploads
    (workspace_id,device_id,week_start,report_sequence,digest,body_json,delivered)
    values (?,?,?,?,?,?,1)`).run(workspace, device, weekStart, 1, "fixture-digest", "{}");
  old.close(); old = undefined;
  const mondayAfterAck = planFreshLedgerCutover({ ...input,
    now: () => new Date(isoAfterDays(7, 5)) });
  assert.equal(mondayAfterAck.status, "ready", mondayAfterAck.reason ?? "");
  switchFreshLedger({ ...input, now: () => new Date(isoAfterDays(7, 5)),
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  const replacement = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  assert.equal((replacement.database.prepare("select count(*) as n from tool_attempt_facts")
    .get() as { n: number }).n, 0);
  replacement.close();
  console.log(JSON.stringify({ midweek: midweek.reason, nextSafeWindowAt: midweek.nextSafeWindowAt,
    mondayBeforeAck: mondayBeforeAck.reason, mondayAfterAck: mondayAfterAck.status,
    archivePreserved: fs.existsSync(archivePath) }));
} finally {
  old?.close();
  fs.rmSync(fixture, { recursive: true, force: true });
}
