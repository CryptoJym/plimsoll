import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "pr426-cli-cutover-")));
const home = path.join(fixture, "home");
const data = path.join(home, ".plimsoll");
const root = path.join(home, "codex");
const archiveDir = path.join(fixture, "archive");
const archive = path.join(archiveDir, "old-ledger.sqlite");
const fresh = path.join(archiveDir, "fresh-attempt.sqlite");
const ledger = path.join(data, "work-ledger.sqlite");
const epoch = "10000000-0000-4000-8000-000000000001";
const tenant = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";

try {
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  fs.mkdirSync(root);
  fs.mkdirSync(archiveDir, { mode: 0o700 });
  const config = collectorConfigSchema.parse({ tenantId: tenant, deviceId: device,
    installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
      profileId: "profile", directory: root, installationEpochId: epoch }] });
  fs.writeFileSync(path.join(data, "collector.config.json"), JSON.stringify(config));
  const old = new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device,
    freshCaptureRootEpoch: epoch });
  old.close();
  const env = { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: data,
    CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude") };
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const cli = path.resolve("packages/collector-cli/src/cli.ts");
  const invoke = (action: string, ...options: string[]) => {
    const result = spawnSync(process.execPath,
      ["--import", loader, cli, "capture-roots", action, ...options, "--json"],
      { cwd: path.resolve("."), env, encoding: "utf8", timeout: 120_000 });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    return JSON.parse(result.stdout) as Record<string, unknown>;
  };
  const plan = invoke("epoch-plan", "--archive", archive);
  assert.equal(plan.status, "capture_roots_epoch_plan");
  assert.equal(plan.installationEpochId, epoch);
  assert.equal(plan.rootCount, 1);
  const switched = invoke("epoch-switch", "--archive", archive);
  assert.equal(switched.status, "capture_roots_epoch_switched");
  assert.equal(readReplacementLedgerMarker(ledger)?.archivePath, archive);
  assert.equal(fs.existsSync(archive), true);
  const restored = invoke("epoch-restore", "--archive", archive, "--save-fresh", fresh);
  assert.equal(restored.status, "capture_roots_epoch_restored");
  assert.equal(readReplacementLedgerMarker(ledger), null);
  assert.ok(readReplacementLedgerMarker(fresh));
  assert.equal(fs.existsSync(archive), true);
  console.log(JSON.stringify({ plan: plan.status, switched: switched.status,
    restored: restored.status, archivePreserved: true, freshAttemptPreserved: true }));
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}
