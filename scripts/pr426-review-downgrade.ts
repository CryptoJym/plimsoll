import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { runLifecycleCommand } from "../packages/collector-cli/src/lifecycle-command";
import type { LifecycleAdapter } from "../packages/collector-cli/src/lifecycle";
import { assertReplacementRuntimeCompatible, planFreshLedgerCutover,
  readReplacementLedgerMarker, restoreArchivedLedger, switchFreshLedger } from
  "../packages/collector-cli/src/fresh-ledger-cutover";

async function main() {
  const oldRoot = process.env.PLIMSOLL_0744_ROOT;
  if (!oldRoot) throw new Error("PLIMSOLL_0744_ROOT required");
  const oldModule = await import(pathToFileURL(path.join(oldRoot,
    "packages/collector-cli/src/buffer.ts")).href) as { LocalEventBuffer: typeof LocalEventBuffer };
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-downgrade-")));
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
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: [{
      source: "codex", rootId: "root", profileId: "profile", directory: root,
      installationEpochId: epoch,
    }] });
  const input = { ledgerPath: ledger, archivePath, config,
    authorityRoot: path.join(fixture, "lifecycle-authority") };
  let old: LocalEventBuffer | undefined;
  try {
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    old.close(); old = undefined;
    assert.equal(planFreshLedgerCutover(input).status, "ready");
    assert.throws(() => switchFreshLedger({ ...input, onStep: step => {
      if (step === "stage_bound") throw new Error("injected_before_switch");
    } }), /injected_before_switch/);
    assert.equal(fs.existsSync(ledger), true);
    assert.equal(fs.existsSync(archivePath), false);
    assert.equal(fs.existsSync(`${ledger}.replacement-stage`), false);
    assert.equal(planFreshLedgerCutover(input).status, "ready");

    assert.throws(() => switchFreshLedger({ ...input, onStep: step => {
      if (step === "archive_linked") throw new Error("injected_after_archive_link");
    } }), /injected_after_archive_link/);
    assert.equal(fs.statSync(ledger).ino, fs.statSync(archivePath).ino);
    const interruptedPlan = planFreshLedgerCutover(input);
    assert.equal(interruptedPlan.status, "ready",
      `a crash after the archive link must be resumable: ${interruptedPlan.reason}`);

    let oldOpenRefused = false;
    switchFreshLedger({ ...input, onStep: step => {
      if (step !== "stage_bound") return;
      const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
      const oldPath = path.join(oldRoot, "packages/collector-cli/src/buffer.ts");
      const code = `import { LocalEventBuffer } from ${JSON.stringify(pathToFileURL(oldPath).href)};
        new LocalEventBuffer(${JSON.stringify(ledger)}, {
          workspaceId: ${JSON.stringify(workspace)}, deviceId: ${JSON.stringify(device)},
          databaseBusyTimeoutMs: 0 });`;
      const raced = spawnSync(process.execPath, ["--import", loader, "--input-type=module", "-e", code],
        { cwd: path.resolve("."), encoding: "utf8", timeout: 30_000 });
      oldOpenRefused = raced.status !== 0 && /SQLITE_BUSY|database is locked/.test(raced.stderr);
      assert.equal(oldOpenRefused, true, raced.stderr);
    } });
    const marker = readReplacementLedgerMarker(ledger);
    assert.ok(marker);
    assert.equal(marker.minCollectorVersion, "0.7.46");
    assert.throws(() => assertReplacementRuntimeCompatible(ledger, "0.7.44"),
      /replacement_ledger_requires_archive_restore_before_downgrade/);
    await assert.rejects(runLifecycleCommand({ argv: ["rollback", "--artifact", "self",
      "--operation-id", "replacement-downgrade-test"],
      adapter: { acquireLock: async () => true, releaseLock: async () => {} } as unknown as LifecycleAdapter,
      resolveArtifact: async () => ({ version: "0.7.44", platform: "darwin", architecture: "arm64",
        nodeMajor: 22, sha256: `sha256:${"0".repeat(64)}`, sourcePath: ledger }),
      beforeRuntimeSwitch: artifact => assertReplacementRuntimeCompatible(ledger, artifact.version),
    }), /replacement_ledger_requires_archive_restore_before_downgrade/);
    assert.throws(() => assertReplacementRuntimeCompatible(ledger, "0.7.45"),
      /replacement_ledger_requires_archive_restore_before_downgrade/);
    assert.doesNotThrow(() => assertReplacementRuntimeCompatible(ledger, "0.7.46"));

    const replacement = new LocalEventBuffer(ledger, { workspaceId: workspace,
      deviceId: device, freshCaptureRootEpoch: epoch });
    replacement.database.prepare(`insert into weekly_tool_stats_uploads
      (workspace_id,device_id,week_start,report_sequence,digest,body_json,delivered)
      values(?,?,?,?,?,?,0)`).run(workspace, device, "2026-09-21", 1, "unstored-week", "{}");
    replacement.close();
    const receipt = restoreArchivedLedger({ ledgerPath: ledger, archivePath, freshAttemptPath,
      authorityRoot: input.authorityRoot });
    assert.equal(receipt.archivePreserved, true);
    assert.equal(readReplacementLedgerMarker(ledger), null);
    assert.ok(readReplacementLedgerMarker(freshAttemptPath));
    assert.doesNotThrow(() => assertReplacementRuntimeCompatible(ledger, "0.7.44"));
    const restoredOld = new oldModule.LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device });
    assert.equal(restoredOld.workspaceBinding()!.currentInstallationEpochId, epoch);
    assert.equal((restoredOld.database.prepare("select count(*) as n from weekly_tool_stats_uploads")
      .get() as { n: number }).n, 0, "old binary must see the archived original, not replacement's stale week");
    restoredOld.close();
    assert.equal(fs.existsSync(archivePath), true);
    console.log(JSON.stringify({ prebindingCrashRecoveredOriginal: true, oldOpenDuringSwitchRefused: oldOpenRefused,
      markerMinimum: marker.minCollectorVersion, downgradeGuard: "refused",
      archiveRestored: true, freshAttemptPreserved: fs.existsSync(freshAttemptPath),
      oldRuntimeSawReplacementReport: false }));
  } finally {
    old?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
