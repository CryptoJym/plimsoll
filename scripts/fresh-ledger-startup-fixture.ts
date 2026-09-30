/** Copy this fixture into scripts/ of a collector checkout before running it. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateDeviceIdentity } from "../packages/collector-cli/src/device-identity";
import { useFixtureRoot } from "./lib/fixture-root";

const tenant = "6f4dbf9e-2d9b-4a61-a379-670bc742918a";
const device = "dev_fixture-studio0";
const keyId = "key_fixture-studio0";

if (process.argv[2] === "--fault-child") {
  const [, , , ledger, epoch, step] = process.argv;
  if (!ledger || !epoch || !step) throw new Error("fault child arguments missing");
  try {
    new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device,
      freshCaptureRootEpoch: epoch,
      onOpenStep: (observation) => {
        if (observation.step !== step) return;
        if (step === "ledger.core_schema") process.kill(process.pid, "SIGKILL");
        throw new Error("fixture_fault_before_binding");
      },
    });
    process.exit(90);
  } catch (error) {
    if (error instanceof Error && error.message === "fixture_fault_before_binding") process.exit(78);
    throw error;
  }
}

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
const script = path.join(repo, "scripts/fresh-ledger-startup-fixture.ts");
const tsx = path.join(repo, "node_modules/tsx/dist/loader.mjs");
const base = fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir());
const sandbox = fs.mkdtempSync(path.join(base, "fresh-ledger-startup-"));
const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "home") });
const data = fixture.env.PLIMSOLL_HOME;
const epoch = crypto.randomUUID();
const checks: Array<{ name: string; pass: boolean; detail?: unknown }> = [];
function check(name: string, pass: boolean, detail?: unknown) {
  checks.push({ name, pass, ...(detail === undefined ? {} : { detail }) });
}
function sha(file: string) { return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex"); }
function child(...args: string[]) {
  return spawnSync(process.execPath, ["--import", tsx, script, ...args], {
    cwd: repo, env: { ...process.env, ...fixture.env }, encoding: "utf8", timeout: 45_000,
  });
}
function epochPlan() {
  const result = spawnSync(process.execPath, ["--import", tsx, cli, "capture-roots", "epoch-plan", "--json"], {
    cwd: repo, env: { ...process.env, ...fixture.env }, encoding: "utf8", timeout: 45_000,
  });
  let answer: Record<string, unknown> = {};
  try { answer = JSON.parse(result.stdout); } catch { /* captured by assertion */ }
  return { code: result.status, answer, stderr: result.stderr.slice(-250) };
}

function partialBinding(pathname: string) {
  const db = new Database(pathname);
  try {
    const table = Boolean(db.prepare("select 1 from sqlite_master where type='table' and name='buffered_events'").get());
    const row = db.prepare("select current_installation_epoch_id from collector_workspace_binding where singleton=1").get();
    return { table, bindingPersisted: Boolean(row) };
  } finally { db.close(); }
}

function main() {
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  loadOrCreateDeviceIdentity(fixture.home, { seed: { deviceId: device, keyId } });
  const roots: CaptureRoot[] = ["claude_code", "codex"].map((source, index) => {
    const directory = path.join(fixture.home, "synthetic-profiles", `profile-${index}`,
      source === "codex" ? "sessions" : "projects");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    return { rootId: `root-${index}`, profileId: `profile-${index}`, installationEpochId: epoch,
      source: source as CaptureRoot["source"], directory };
  });
  const config = collectorConfigSchema.parse({ port: 48319, tenantId: tenant, deviceId: device,
    keyId, installKey: "fixture-install-only", managed: true, captureRoots: roots });
  const configPath = path.join(data, "collector.config.json");
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  const agreedEpoch = new Set(config.captureRoots!.map((root) => root.installationEpochId));
  assert.equal(agreedEpoch.size, 1);

  for (const [name, step] of [["killed_after_core_schema", "ledger.core_schema"],
    ["thrown_after_delivery_schema", "ledger.delivery_schema"]] as const) {
    const ledger = path.join(sandbox, `${name}.sqlite`);
    const failed = child("--fault-child", ledger, epoch, step);
    check(`${name}_actually_interrupted`, step === "ledger.core_schema"
      ? failed.signal === "SIGKILL" : failed.status === 78,
      { status: failed.status, signal: failed.signal });
    const partial = partialBinding(ledger);
    check(`${name}_left_schema_without_binding`, partial.table && !partial.bindingPersisted, partial);
    const resumed = new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device,
      freshCaptureRootEpoch: [...agreedEpoch][0] });
    try {
      const actual = resumed.workspaceBinding()!.currentInstallationEpochId;
      const at = new Date(Date.now() + 1_000).toISOString();
      const reasons = roots.map((root) => resumed.eventAdmissionReason(at, root.installationEpochId));
      check(`${name}_restart_adopts_configured_epoch`, actual === epoch,
        { expected: epoch, actual });
      check(`${name}_zero_epoch_mismatch`, reasons.every((reason) => reason !== "epoch_mismatch"), reasons);
    } finally { resumed.close(); }
  }

  // Copy replacement boundary: archive the old ledger, then try a mixed-root
  // configuration against the absent replacement path.
  const ledger = path.join(data, "work-ledger.sqlite");
  const old = new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device,
    freshCaptureRootEpoch: epoch });
  old.close();
  const archive = path.join(sandbox, "archived-old-ledger");
  fs.mkdirSync(archive);
  for (const suffix of ["", "-wal", "-shm"]) {
    if (fs.existsSync(`${ledger}${suffix}`)) fs.renameSync(`${ledger}${suffix}`,
      path.join(archive, `work-ledger.sqlite${suffix}`));
  }
  const archived = fs.readdirSync(archive).map((name) => [name, sha(path.join(archive, name))] as const);
  const agreedBefore = epochPlan();
  check("agreeing_epoch_plan_before_refusal", agreedBefore.code === 0 &&
    agreedBefore.answer.installationEpochId === epoch && agreedBefore.answer.ledgerAbsent === true,
    agreedBefore);

  config.captureRoots![1]!.installationEpochId = crypto.randomUUID();
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`);
  const mixedBytes = fs.readFileSync(configPath);
  const mixedBefore = epochPlan();
  let refusal = "";
  try {
    new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device,
      freshCaptureRootEpoch: null });
  } catch (error) { refusal = error instanceof Error ? error.message : String(error); }
  check("mixed_roots_refused", refusal === "fresh_ledger_capture_root_epochs_conflict", refusal);
  const sidecars = ["", "-wal", "-shm"].filter((suffix) => fs.existsSync(`${ledger}${suffix}`));
  check("refusal_left_no_ledger_or_sidecars", sidecars.length === 0, sidecars);
  const mixedAfter = epochPlan();
  check("mixed_epoch_plan_unchanged_and_config_unmutated",
    mixedAfter.code === mixedBefore.code &&
    JSON.stringify(mixedAfter.answer) === JSON.stringify(mixedBefore.answer) &&
    fs.readFileSync(configPath).equals(mixedBytes), { before: mixedBefore, after: mixedAfter });
  config.captureRoots![1]!.installationEpochId = epoch;
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`);
  const agreedAfter = epochPlan();
  check("agreeing_epoch_plan_still_succeeds_without_cleanup",
    agreedAfter.code === agreedBefore.code &&
    JSON.stringify(agreedAfter.answer) === JSON.stringify(agreedBefore.answer),
    { before: agreedBefore, after: agreedAfter });
  check("archived_old_ledger_untouched", archived.every(([name, digest]) =>
    sha(path.join(archive, name)) === digest));

  const passed = checks.filter((row) => row.pass).length;
  console.log(JSON.stringify({ schema: "plimsoll.fresh-ledger-startup-proof/v1", epoch, checks,
    passed, failed: checks.length - passed }, null, 2));
  if (passed !== checks.length) process.exitCode = 1;
}

try { main(); } catch (error) { console.error(error); process.exitCode = 1; }
finally { fixture.restore(); fs.rmSync(sandbox, { recursive: true, force: true }); }
