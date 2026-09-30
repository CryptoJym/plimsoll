import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, readReplacementLedgerMarker,
  switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const steps = ["old_locked", "stage_bound", "archive_linked", "switched"] as const;
if (process.argv[2] === "--child") {
  const [fixture, point] = process.argv.slice(3);
  const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(path.join(fixture!, "config.json"), "utf8")));
  switchFreshLedger({ ledgerPath: path.join(fixture!, "work-ledger.sqlite"),
    archivePath: path.join(fixture!, "archive", "old-ledger.sqlite"), config,
    authorityRoot: path.join(fixture!, "lifecycle-authority"),
    onStep: step => { if (step === point) process.kill(process.pid, "SIGKILL"); } });
  process.exit(90);
} else {
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const results: Array<Record<string, unknown>> = [];
  for (const point of steps) {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
      `r3-crash-${point}-`)));
    try {
      const ledgerPath = path.join(fixture, "work-ledger.sqlite");
      const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
      const root = path.join(fixture, "codex");
      fs.mkdirSync(root);
      fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
      const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
        installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
          profileId: "profile", directory: root, installationEpochId: epoch }] });
      fs.writeFileSync(path.join(fixture, "config.json"), JSON.stringify(config));
      const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
        freshCaptureRootEpoch: epoch });
      old.close();
      const oldInode = fs.statSync(ledgerPath).ino;
      const child = spawnSync(process.execPath,
        ["--import", loader, import.meta.filename, "--child", fixture, point],
        { encoding: "utf8", timeout: 120_000 });
      assert.equal(child.signal, "SIGKILL", child.stderr);
      const marker = readReplacementLedgerMarker(ledgerPath);
      const archiveExists = fs.existsSync(archivePath);
      const stageExists = fs.existsSync(`${ledgerPath}.replacement-stage`);
      const oldActive = fs.statSync(ledgerPath).ino === oldInode;
      const archivedOld = archiveExists && fs.statSync(archivePath).ino === oldInode;
      const plan = oldActive ? planFreshLedgerCutover({ ledgerPath, archivePath, config }) : null;
      results.push({ point, oldActive, marker: marker?.minCollectorVersion ?? null,
        archivedOld, stageExists, plan: plan?.status ?? null,
        recoveryStagePresent: plan?.recoveryStagePresent ?? null });
      assert.equal(oldActive, point !== "switched");
      assert.equal(marker === null, point !== "switched");
      assert.equal(archiveExists, point === "archive_linked" || point === "switched");
      if (archiveExists) assert.equal(archivedOld, true);
      if (plan) assert.equal(plan.status, "ready", plan.reason ?? "");
    } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
  }
  console.log(JSON.stringify({ results }));
}
