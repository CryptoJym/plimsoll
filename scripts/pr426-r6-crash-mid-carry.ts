import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = process.argv[3] ?? fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-mid-carry-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const root = path.join(fixture, "codex");
const sentinel = path.join(fixture, "copied-rows.txt");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const input = {
  ledgerPath: ledger, archivePath,
  authorityRoot: path.join(fixture, "lifecycle-authority"),
  config: collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
      profileId: "profile", directory: root, installationEpochId: epoch }] }),
};

async function main() {
  if (process.argv[2] === "child") {
    const { switchFreshLedger } = await import("../packages/collector-cli/src/fresh-ledger-cutover");
    switchFreshLedger({ ...input, onCopyRow: (table, copied) => {
      if (table === "session_usage_authority" && copied === 10) {
        fs.writeFileSync(sentinel, "10");
        process.kill(process.pid, "SIGKILL");
      }
    } });
    throw new Error("fault injection did not kill the process");
  }
  fs.mkdirSync(root);
  fs.mkdirSync(archiveDirectory, { mode: 0o700 });
  try {
    const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    const insert = old.database.prepare("insert into session_usage_authority(source,session_id,authority,claimed_at) values('codex',?,'tailer',?)");
    old.database.transaction(() => {
      for (let n = 0; n < 30; n++) insert.run(`session-${n}`, new Date().toISOString());
    })();
    old.close();
    const child = spawnSync(process.execPath,
      ["--import", path.resolve("node_modules/tsx/dist/loader.mjs"), process.argv[1]!, "child", fixture],
      { cwd: process.cwd(), env: { ...process.env, PR426_CRASH_SENTINEL: sentinel },
        encoding: "utf8", timeout: 120_000 });
    assert.equal(child.signal, "SIGKILL", `child exit=${child.status} stderr=${child.stderr}`);
    assert.equal(fs.readFileSync(sentinel, "utf8"), "10");
    const active = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    const oldRows = (active.database.prepare("select count(*) as n from session_usage_authority").get() as { n: number }).n;
    const markerExists = Boolean(active.database.prepare(
      "select 1 from sqlite_master where type='table' and name='collector_replacement_ledger'").get());
    active.close();
    const stage = `${ledger}.replacement-stage`;
    const stageExists = fs.existsSync(stage);
    const archiveExists = fs.existsSync(archivePath);
    const recoveredPlan = planFreshLedgerCutover(input);
    assert.equal(recoveredPlan.status, "ready", recoveredPlan.reason ?? "");
    assert.equal(recoveredPlan.recoveryStagePresent, true);
    const realNow = Date.now;
    Date.now = () => realNow() + 11 * 60_000;
    try { switchFreshLedger(input); } finally { Date.now = realNow; }
    const replacement = new LocalEventBuffer(ledger, { workspaceId: workspace,
      deviceId: device, freshCaptureRootEpoch: epoch });
    const copiedRows = (replacement.database.prepare(
      "select count(*) as n from session_usage_authority").get() as { n: number }).n;
    replacement.close();
    console.log(JSON.stringify({ childSignal: child.signal, copiedRowsAtKill: 10,
      oldRows, markerExists, stageExists, archiveExists, plan: recoveredPlan.status,
      planRecoveryStagePresent: recoveredPlan.recoveryStagePresent,
      replacementCopiedRows: copiedRows, stageRemoved: !fs.existsSync(stage) }));
    assert.equal(oldRows, 30);
    assert.equal(markerExists, false);
    assert.equal(stageExists, true);
    assert.equal(archiveExists, false);
    assert.equal(copiedRows, 30);
    assert.equal(fs.existsSync(stage), false);
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
