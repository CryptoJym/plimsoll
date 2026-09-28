import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { LifecycleMutationAuthority } from "../packages/collector-cli/src/lifecycle-authority";
import { planFreshLedgerCutover, readReplacementLedgerMarker,
  switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";

if (process.argv[2] === "--child") {
  const [ledgerPath, archivePath, configPath, authorityRoot] = process.argv.slice(3);
  const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath!, "utf8")));
  switchFreshLedger({ ledgerPath: ledgerPath!, archivePath: archivePath!, config,
    authorityRoot, onStep: step => {
      if (step === "stage_bound") process.kill(process.pid, "SIGKILL");
    } });
  process.exit(90);
}

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "pr426-hardkill-")));
try {
  const ledger = path.join(fixture, "work-ledger.sqlite");
  const archiveDir = path.join(fixture, "archive");
  const archivePath = path.join(archiveDir, "old-ledger.sqlite");
  const authorityRoot = path.join(fixture, "lifecycle-authority");
  const root = path.join(fixture, "codex");
  fs.mkdirSync(archiveDir, { mode: 0o700 });
  fs.mkdirSync(root);
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
      profileId: "profile", directory: root, installationEpochId: epoch }] });
  const configPath = path.join(fixture, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  old.close();
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const child = spawnSync(process.execPath,
    ["--import", loader, import.meta.filename, "--child", ledger, archivePath,
      configPath, authorityRoot],
    { cwd: path.resolve("."), encoding: "utf8", timeout: 120_000 });
  assert.equal(child.signal, "SIGKILL", child.stderr);
  assert.equal(readReplacementLedgerMarker(ledger), null);
  assert.equal(fs.existsSync(archivePath), false);
  const stage = `${ledger}.replacement-stage`;
  assert.equal(fs.existsSync(stage), true);
  const recoveredPlan = planFreshLedgerCutover({ ledgerPath: ledger, archivePath, config });
  assert.equal(recoveredPlan.status, "ready");
  assert.equal(recoveredPlan.recoveryStagePresent, true);
  const authority = new LifecycleMutationAuthority(authorityRoot).observe();
  assert.equal(authority.kind, "held", "a killed owner retains its fenced lease until expiry");
  assert.throws(() => switchFreshLedger({ ledgerPath: ledger, archivePath, config,
    authorityRoot }), /cutover_lifecycle_authority_unavailable/);
  // Exercise the expiry boundary without waiting ten wall-clock minutes.
  const actualNow = Date.now;
  try {
    Date.now = () => authority.kind === "held" ? authority.expiresAtMs + 1 : actualNow();
    switchFreshLedger({ ledgerPath: ledger, archivePath, config, authorityRoot });
  } finally { Date.now = actualNow; }
  assert.ok(readReplacementLedgerMarker(ledger));
  assert.equal(fs.existsSync(archivePath), true);
  assert.equal(fs.existsSync(stage), false);
  console.log(JSON.stringify({ killedAt: "stage_bound", oldLedgerSurvived: true,
    incompleteStageRecoveredAutomatically: true, planAfterKill: "ready", leaseWaitRequired: true,
    resumedSwitch: "complete" }));
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}
