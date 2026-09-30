import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicLearningFactId } from "../packages/collector-cli/src/learning-facts";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-old-week-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const root = path.join(fixture, "codex");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: [{ source: "codex",
    rootId: "root", profileId: "profile", directory: root, installationEpochId: epoch }] });
let old: LocalEventBuffer | undefined;
try {
  old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date("2026-09-21T00:00:00.000Z") });
  old.database.prepare("insert into weekly_tool_stats_control(workspace_id,device_id,first_week) values(?,?,?)")
    .run(workspace, device, "2026-09-28");
  old.learningFacts.recordToolSignal({ kind: "attempt", operationId: deterministicLearningFactId(["r2", "older-week"]),
    source: "codex", sessionId: "old-week-session", toolClass: "compute", toolName: "shell",
    startedAt: "2026-09-29T12:00:00.000Z" });
  assert.equal((old.database.prepare("select count(*) as n from tool_attempt_facts").get() as { n: number }).n, 1);
  assert.equal((old.database.prepare("select count(*) as n from weekly_tool_stats_uploads").get() as { n: number }).n, 0);
  old.close(); old = undefined;
  const input = { ledgerPath: ledger, archivePath, config,
    now: () => new Date("2026-10-19T12:00:00.000Z"),
    authorityRoot: path.join(fixture, "lifecycle-authority") };
  const plan = planFreshLedgerCutover(input);
  if (plan.status === "ready") switchFreshLedger(input);
  const replacement = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  const newAttempts = (replacement.database.prepare("select count(*) as n from tool_attempt_facts").get() as { n: number }).n;
  replacement.close();
  console.log(JSON.stringify({ oldAttempts: 1, oldPendingReports: 0,
    oldControlFirstWeek: "2026-09-28", simulatedNow: "2026-10-19T12:00:00.000Z",
    planStatus: plan.status, planReason: plan.reason, priorWeekAttempts: plan.priorUtcWeekToolAttempts,
    newAttempts, archiveExists: fs.existsSync(archivePath) }));
  assert.notEqual(plan.status, "ready",
    "cutover must not discard an older unreported week when the report row was never created");
} finally {
  old?.close();
  fs.rmSync(fixture, { recursive: true, force: true });
}
