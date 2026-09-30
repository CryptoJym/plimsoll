import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";

const EPOCH = "10000000-0000-4000-8000-000000000001";
const OTHER = "20000000-0000-4000-8000-000000000002";
const WORKSPACE = "30000000-0000-4000-8000-000000000003";
const DEVICE = "40000000-0000-4000-8000-000000000004";
const START = "2030-04-02T10:00:00.000Z";
const LATER = "2030-04-02T10:05:00.000Z";
const loader = path.resolve("node_modules/tsx/dist/loader.mjs");

if (process.argv[2] === "--partial" || process.argv[2] === "--bound") {
  const file = process.argv[3]!;
  const targetStep = process.argv[2] === "--partial" ? "ledger.privacy_schema" : "ledger.workspace_binding";
  assert.throws(() => new LocalEventBuffer(file, {
    workspaceId: WORKSPACE, deviceId: DEVICE, freshCaptureRootEpoch: EPOCH,
    enrollmentNow: () => new Date(START),
    onOpenStep: (step) => { if (step.step === targetStep) throw new Error("fixture_crash"); },
  }), /fixture_crash/);
  assert.ok(fs.existsSync(file));
  process.exit(0);
}

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-invariants-")));
const checks: string[] = [];
function check(name: string, run: () => void) { run(); checks.push(name); console.log(`PASS ${name}`); }
function buffer(file: string, epoch: string | null | undefined = EPOCH, at = START) {
  return new LocalEventBuffer(file, { workspaceId: WORKSPACE, deviceId: DEVICE,
    freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(at) });
}

try {
  const file = path.join(root, "fresh.sqlite");
  const first = buffer(file);
  check("fresh_adopts_root_epoch_and_new_cutoff", () => {
    const binding = first.workspaceBinding()!;
    assert.equal(binding.currentInstallationEpochId, EPOCH);
    assert.equal(binding.currentInstallationEpochStartedAt, START);
    assert.equal(first.eventAdmissionReason("2030-04-02T09:59:59.000Z", EPOCH), "before_enrollment");
    assert.equal(first.eventAdmissionReason(START, EPOCH), null);
  });
  first.close();
  const existing = buffer(file, OTHER, LATER);
  check("durable_binding_ignores_stale_root_epoch", () => {
    assert.equal(existing.workspaceBinding()!.currentInstallationEpochId, EPOCH);
    assert.equal(existing.workspaceBinding()!.currentInstallationEpochStartedAt, START);
  });
  existing.close();

  const partial = path.join(root, "partial.sqlite");
  const crash = spawnSync(process.execPath, ["--import", loader, import.meta.filename, "--partial", partial],
    { encoding: "utf8", timeout: 120_000 });
  check("pre_binding_crash_fixture_created_unbound_file", () => {
    assert.equal(crash.status, 0, crash.stderr);
    assert.ok(fs.existsSync(partial));
  });
  const resumed = buffer(partial, EPOCH, LATER);
  check("pre_binding_crash_restarts_with_root_epoch", () => {
    assert.equal(resumed.workspaceBinding()!.currentInstallationEpochId, EPOCH);
    assert.equal(resumed.workspaceBinding()!.currentInstallationEpochStartedAt, LATER);
  });
  resumed.close();
  const unboundRow = path.join(root, "unbound-row.sqlite");
  fs.copyFileSync(partial, unboundRow);
  const prepareUnbound = buffer(unboundRow);
  prepareUnbound.database.prepare("update collector_workspace_binding set current_installation_epoch_id=null, current_installation_epoch_started_at=null").run();
  prepareUnbound.close();
  const recoveredRow = buffer(unboundRow, EPOCH, LATER);
  check("persisted_row_without_epoch_can_adopt_root_epoch", () => {
    assert.equal(recoveredRow.workspaceBinding()!.currentInstallationEpochId, EPOCH);
    assert.equal(recoveredRow.workspaceBinding()!.currentInstallationEpochStartedAt, LATER);
  });
  recoveredRow.close();

  const afterBinding = path.join(root, "after-binding.sqlite");
  const boundCrash = spawnSync(process.execPath, ["--import", loader, import.meta.filename, "--bound", afterBinding],
    { encoding: "utf8", timeout: 120_000 });
  check("post_binding_crash_fixture_completed_transaction", () => {
    assert.equal(boundCrash.status, 0, boundCrash.stderr);
  });
  const afterCrash = buffer(afterBinding, EPOCH, LATER);
  check("post_binding_crash_keeps_durable_cutoff", () => {
    assert.equal(afterCrash.workspaceBinding()!.currentInstallationEpochId, EPOCH);
    assert.equal(afterCrash.workspaceBinding()!.currentInstallationEpochStartedAt, START);
  });
  afterCrash.close();

  const mixed = path.join(root, "mixed.sqlite");
  check("mixed_roots_refuse_before_file_creation", () => {
    assert.throws(() => buffer(mixed, null), /fresh_ledger_capture_root_epochs_conflict/);
    for (const suffix of ["", "-wal", "-shm"]) assert.equal(fs.existsSync(mixed + suffix), false);
  });
  const mixedPartial = path.join(root, "mixed-partial.sqlite");
  fs.copyFileSync(partial, mixedPartial);
  const db = buffer(mixedPartial);
  db.database.prepare("update collector_workspace_binding set current_installation_epoch_id=null, current_installation_epoch_started_at=null").run();
  db.close();
  check("mixed_roots_refuse_unbound_existing_file", () => {
    assert.throws(() => buffer(mixedPartial, null), /fresh_ledger_capture_root_epochs_conflict/);
  });

  const home = path.join(root, "home");
  const plimsoll = path.join(home, ".plimsoll");
  fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
  const roots = Array.from({ length: 23 }, (_, i) => {
    const directory = path.join(home, `sessions-${i}`);
    fs.mkdirSync(directory);
    return { source: "codex" as const, rootId: `50000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
      profileId: `60000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
      directory, installationEpochId: EPOCH };
  });
  const configPath = path.join(plimsoll, "collector.config.json");
  const config = collectorConfigSchema.parse({ tenantId: WORKSPACE, deviceId: DEVICE,
    installKey: "fixture-install-key", managed: true, port: 49401, captureRoots: roots });
  fs.writeFileSync(configPath, JSON.stringify(config));
  const env = { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: plimsoll,
    CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude") };
  const cli = path.resolve("packages/collector-cli/src/cli.ts");
  const plan = () => spawnSync(process.execPath, ["--import", loader, cli, "capture-roots", "epoch-plan", "--json"],
    { cwd: path.resolve("."), env, encoding: "utf8", timeout: 120_000 });
  const before = fs.readdirSync(plimsoll).sort();
  const good = plan();
  check("preflight_23_roots_agree_and_writes_nothing", () => {
    assert.equal(good.status, 0, good.stderr);
    assert.equal(JSON.parse(good.stdout).rootCount, 23);
    assert.equal(JSON.parse(good.stdout).installationEpochId, EPOCH);
    assert.deepEqual(fs.readdirSync(plimsoll).sort(), before);
  });
  const sidecar = path.join(plimsoll, "work-ledger.sqlite-wal");
  fs.writeFileSync(sidecar, "fixture");
  const refused = plan();
  check("preflight_refuses_orphan_sidecar", () => {
    assert.equal(refused.status, 1, refused.stderr);
    assert.equal(JSON.parse(refused.stdout).reason, "ledger_not_archived");
  });
  fs.unlinkSync(sidecar);

  console.log(`pr426 review invariants: ${checks.length}/${checks.length} pass`);
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
