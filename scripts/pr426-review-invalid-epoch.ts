import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-invalid-epoch-")));
const home = path.join(fixture, "home");
const plimsoll = path.join(home, ".plimsoll");
const root = path.join(home, "sessions");
const ledger = path.join(plimsoll, "work-ledger.sqlite");
const epoch = "legacy-epoch";
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
fs.mkdirSync(root);
try {
  const rawConfig = {
    tenantId: "30000000-0000-4000-8000-000000000003",
    deviceId: "40000000-0000-4000-8000-000000000004",
    installKey: "fixture-install-key", managed: true, port: 49141,
    captureRoots: [{ source: "codex", rootId: "fixture-root", profileId: "fixture-profile",
      directory: root, installationEpochId: epoch }],
  };
  const parsed = collectorConfigSchema.safeParse(rawConfig);
  assert.equal(parsed.success, false, "config validation must reject an unbindable root epoch");
  fs.writeFileSync(path.join(plimsoll, "collector.config.json"), JSON.stringify(rawConfig));
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const cli = path.resolve("packages/collector-cli/src/cli.ts");
  const plan = spawnSync(process.execPath, ["--import", loader, cli, "capture-roots", "epoch-plan", "--json"], {
    cwd: path.resolve("."), encoding: "utf8", timeout: 120_000,
    env: { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: plimsoll,
      CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude") },
  });
  let openError = "";
  try { new LocalEventBuffer(ledger, { workspaceId: rawConfig.tenantId, deviceId: rawConfig.deviceId,
    freshCaptureRootEpoch: epoch }); }
  catch (error) { openError = error instanceof Error ? error.message : String(error); }
  console.log(JSON.stringify({ configValid: parsed.success, planExit: plan.status,
    plan: plan.stdout ? JSON.parse(plan.stdout) : null, openError, ledgerExists: fs.existsSync(ledger) }));
  assert.equal(openError, "installation_epoch_id_invalid");
  assert.equal(plan.status, 1, "epoch-plan must refuse a root epoch the new ledger cannot bind");
  assert.equal(fs.existsSync(ledger), false);
  assert.equal(fs.existsSync(`${ledger}-wal`), false);
  assert.equal(fs.existsSync(`${ledger}-shm`), false);
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}
