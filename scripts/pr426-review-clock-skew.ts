import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from
  "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-clock-skew-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const root = path.join(fixture, "claude");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: [{
    source: "claude_code", rootId: "root", profileId: "profile",
    directory: root, installationEpochId: epoch,
  }] });
const realNow = new Date();
const regressed = new Date(realNow.getTime() - 86_400_000);
const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
  freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(realNow.getTime() - 2 * 86_400_000) });
try {
  old.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,workspace_id,device_id)
    values (?,?,?,?,?,?,?,?,?)`).run("archive-latest", "claude_code", "usage_transcript",
      "metadata", realNow.toISOString(), "{}", realNow.toISOString(), workspace, device);
} finally { old.close(); }
try {
  const input = { ledgerPath: ledger, archivePath, config,
    now: () => regressed, authorityRoot: path.join(fixture, "lifecycle-authority") };
  const plan = planFreshLedgerCutover(input);
  assert.equal(plan.status, "refused");
  assert.equal(plan.reason, "archive_clock_regressed");
  assert.throws(() => switchFreshLedger(input), /archive_clock_regressed/);
  assert.equal(fs.existsSync(archivePath), false);
  assert.equal(fs.existsSync(`${ledger}.replacement-stage`), false);
  assert.equal(fs.existsSync(ledger), true);
  console.log(JSON.stringify({ archiveLatestRecordedAt: realNow.toISOString(),
    regressedAt: regressed.toISOString(), reason: plan.reason,
    replacementArtifacts: false, originalLedgerPreserved: true }));
} finally { fs.rmSync(fixture, { recursive: true, force: true }); }
