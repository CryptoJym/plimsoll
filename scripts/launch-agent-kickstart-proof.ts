/** Fake-launchctl proof for bootstrap's loaded-but-never-spawned state. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { installLaunchAgent } from "../packages/collector-cli/src/launch-agent";
import { LifecycleMutationAuthority } from "../packages/collector-cli/src/lifecycle-authority";
import { useFixtureRoot } from "./lib/fixture-root";

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
const tsx = path.join(repo, "node_modules/tsx/dist/loader.mjs");
const workspace = "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10";
const device = "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d";
const checks: string[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push(name);
}

async function freePort() {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

const serverSource = `
const fs = require('node:fs');
const http = require('node:http');
const crypto = require('node:crypto');
const cp = require('node:child_process');
const pid = process.pid;
const start = cp.execFileSync('ps', ['-p', String(pid), '-o', 'lstart='],
  {encoding:'utf8', env:{...process.env, LANG:'C', LC_ALL:'C', TZ:'UTC'}}).trim().replace(/\\s+/g,' ');
const identity = {instanceId: crypto.randomUUID(), pid,
  processStartFingerprint: 'sha256:' + crypto.createHash('sha256')
    .update('plimsoll-process-start-utc-v2\\0' + pid + '\\0' + start).digest('hex'),
  processStartFingerprintAlgorithm:'plimsoll-ps-lstart-utc-v2'};
const record = {...identity, version:3, command:process.argv,
  cwd:process.cwd(), label:'com.plimsoll.collector', startedAt:new Date().toISOString()};
const pidFile = process.env.FAKE_PID_FILE;
const server = http.createServer((req,res) => {
  if (req.url !== '/status') {res.writeHead(404); res.end(); return;}
  res.writeHead(200, {'content-type':'application/json'});
  res.end(JSON.stringify({ok:true, runtimeIdentity:identity}));
});
server.listen(Number(process.env.FAKE_PORT), '127.0.0.1', () => {
  fs.writeFileSync(pidFile, JSON.stringify(record)+'\\n', {mode:0o600});
  fs.appendFileSync(process.env.FAKE_PIDS, String(pid)+'\\n');
});
process.on('SIGTERM', () => server.close(() => {
  try {fs.unlinkSync(pidFile);} catch {}
  process.exit(0);
}));
`;

const launchctlSource = [
  "#!/bin/sh",
  "set -eu",
  "start_server() {",
  '  "$FAKE_NODE" "$FAKE_SERVER" >> "$FAKE_SERVER_LOG" 2>&1 &',
  "}",
  'case "$1" in',
  "  print)",
  '    if [ ! -f "$FAKE_STATE" ]; then',
  '      echo "Could not find service \\"com.plimsoll.collector\\" in domain for user gui: $(/usr/bin/id -u)" >&2',
  "      exit 113",
  "    fi",
  '    if [ -f "$FAKE_PID_FILE" ]; then',
  '      echo "state = running"; echo "runs = 1"; echo "pid = $(/usr/bin/sed -n \'s/.*\"pid\":\\([0-9]*\\).*/\\1/p\' "$FAKE_PID_FILE")"',
  "    else",
  '      echo "state = not running"; echo "runs = 0"',
  "    fi",
  "    ;;",
  "  bootstrap)",
  '    : > "$FAKE_STATE"; echo bootstrap >> "$FAKE_CALLS"',
  '    if [ "$FAKE_MODE" = "healthy" ]; then start_server; fi',
  "    ;;",
  "  kickstart)",
  '    case "$2" in gui/*/com.plimsoll.collector) ;; *) exit 64 ;; esac',
  '    if [ "$#" -ne 2 ]; then exit 64; fi',
  '    echo kickstart >> "$FAKE_CALLS"',
  '    if [ "$FAKE_MODE" = "kickstart_error" ]; then exit 5; fi',
  '    if [ "$FAKE_MODE" != "no_status" ]; then start_server; fi',
  "    ;;",
  "  bootout)",
  '    echo bootout >> "$FAKE_CALLS"',
  '    if [ -f "$FAKE_PID_FILE" ]; then',
  '      pid=$(/usr/bin/sed -n \'s/.*"pid":\\([0-9]*\\).*/\\1/p\' "$FAKE_PID_FILE")',
  '      if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi',
  '      rm -f "$FAKE_PID_FILE"',
  "    fi",
  '    rm -f "$FAKE_STATE"',
  "    ;;",
  "  *) exit 64 ;;",
  "esac",
  "exit 0",
].join("\n") + "\n";

type Fixture = Awaited<ReturnType<typeof makeFixture>>;

async function makeFixture(name: string, mode: string, withRoots = false) {
  const sandbox = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-kickstart-${name}-`)));
  const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "home") });
  const home = fixture.home;
  const data = path.join(home, ".plimsoll");
  const bin = path.join(sandbox, "bin");
  fs.mkdirSync(bin, { recursive: true, mode: 0o700 });
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  const port = await freePort();
  const calls = path.join(sandbox, "launchctl.calls");
  const state = path.join(sandbox, "launchctl.state");
  const pids = path.join(sandbox, "server.pids");
  const server = path.join(sandbox, "fake-collector.cjs");
  fs.writeFileSync(server, serverSource, { mode: 0o600 });
  fs.writeFileSync(path.join(bin, "launchctl"), launchctlSource, { mode: 0o700 });
  const pnpm = path.join(sandbox, "pnpm");
  fs.writeFileSync(pnpm, "#!/bin/sh\nexit 0\n", { mode: 0o700 });
  const installed = installLaunchAgent({ homeDir: home, repoRoot: path.join(sandbox, "repo"),
    pnpmPath: pnpm, mutationAuthority: new LifecycleMutationAuthority(path.join(sandbox, "install-authority")) });
  check(`${name}_manifest_installed_without_launchctl`, installed.receipt.status === "installed" &&
    fs.existsSync(installed.plistPath), installed.receipt.status);
  let rootToAdd: string | null = null;
  if (withRoots) {
    const registered = path.join(home, ".codex/sessions");
    rootToAdd = path.join(home, ".claude/projects");
    fs.mkdirSync(registered, { recursive: true, mode: 0o700 });
    fs.mkdirSync(rootToAdd, { recursive: true, mode: 0o700 });
    const buffer = new LocalEventBuffer(path.join(data, "work-ledger.sqlite"),
      { workspaceId: workspace, deviceId: device });
    let epoch: string;
    try {
      epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
      for (const source of ["codex", "claude_code"] as const) {
        const begun = beginAutomaticCaptureBaseline(buffer.database, source,
          { startedAt: "2026-01-01T00:00:00.000Z", filesDiscovered: 0 });
        completeAutomaticCaptureBaseline(buffer.database, source,
          { runId: begun.latestRun!.runId, completedAt: "2026-01-01T00:00:00.000Z" });
      }
    } finally { buffer.close(); }
    const config = collectorConfigSchema.parse({
      port, tenantId: workspace, deviceId: device, installKey: "fixture-install-key", managed: true,
      captureRoots: [{ ...deriveCaptureRootIdentity("fixture-machine", "codex", registered),
        installationEpochId: epoch, source: "codex", directory: registered }],
    });
    fs.writeFileSync(path.join(data, "collector.config.json"), `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  } else {
    fs.writeFileSync(path.join(data, "collector.config.json"), JSON.stringify({ port }) + "\n", { mode: 0o600 });
  }
  const env = { ...process.env, ...fixture.env, PATH: `${bin}:${process.env.PATH}`,
    FAKE_MODE: mode, FAKE_NODE: process.execPath, FAKE_SERVER: server,
    FAKE_SERVER_LOG: path.join(sandbox, "server.log"), FAKE_PORT: String(port),
    FAKE_PID_FILE: path.join(data, "collector.pid"), FAKE_CALLS: calls, FAKE_STATE: state, FAKE_PIDS: pids };
  const run = (args: string[], override: Record<string, string> = {}) => {
    const result = spawnSync(process.execPath, ["--import", tsx, cli, ...args], {
      cwd: sandbox, env: { ...env, ...override }, encoding: "utf8", timeout: 40_000,
    });
    const start = result.stdout?.indexOf("{") ?? -1;
    return { code: result.status, receipt: start >= 0 ? JSON.parse(result.stdout.slice(start)) as Record<string, any> : null,
      stdout: result.stdout, stderr: result.stderr };
  };
  const callCount = (action: string) => fs.existsSync(calls)
    ? fs.readFileSync(calls, "utf8").split("\n").filter((line) => line === action).length : 0;
  const cleanup = async () => {
    if (fs.existsSync(pids)) {
      for (const pid of fs.readFileSync(pids, "utf8").split("\n").filter(Boolean)) {
        try { process.kill(Number(pid), "SIGTERM"); } catch { /* already gone */ }
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  };
  return { sandbox, home, data, port, env, calls, state, rootToAdd, run, callCount, cleanup };
}

async function statusAnswers(port: number) {
  try { return (await fetch(`http://127.0.0.1:${port}/status`)).ok; }
  catch { return false; }
}

async function waitForStatus(port: number) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (await statusAnswers(port)) return true;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

async function main() {
  const fixtures: Fixture[] = [];
  try {
    const trapped = await makeFixture("trapped", "trap"); fixtures.push(trapped);
    const load = trapped.run(["load-launch-agent"]);
    check("loaded_unspawned_kickstarts_exactly_once_and_status_answers",
      load.code === 0 && load.receipt?.loaded === true &&
      load.receipt?.readiness?.verified === true && trapped.callCount("kickstart") === 1 &&
      await statusAnswers(trapped.port), { code: load.code, receipt: load.receipt,
        kickstarts: trapped.callCount("kickstart") });
    const secondLoad = trapped.run(["load-launch-agent"]);
    check("second_load_of_running_job_never_kickstarts_again",
      secondLoad.code === 0 && secondLoad.receipt?.status === "already_loaded" &&
      trapped.callCount("kickstart") === 1,
      { code: secondLoad.code, receipt: secondLoad.receipt, kickstarts: trapped.callCount("kickstart") });

    const already = await makeFixture("already-loaded", "trap"); fixtures.push(already);
    const accepted = spawnSync(path.join(already.sandbox, "bin/launchctl"),
      ["bootstrap", "gui/fixture", "fixture.plist"], { env: already.env, encoding: "utf8", timeout: 10_000 });
    const recovered = already.run(["load-launch-agent"]);
    check("already_loaded_zero_run_job_recovers_once",
      accepted.status === 0 && recovered.code === 0 &&
      recovered.receipt?.status === "already_loaded" && recovered.receipt?.readiness?.verified === true &&
      already.callCount("kickstart") === 1 && await statusAnswers(already.port),
      { code: recovered.code, receipt: recovered.receipt, kickstarts: already.callCount("kickstart") });

    const healthy = await makeFixture("healthy", "healthy"); fixtures.push(healthy);
    const healthyLoad = healthy.run(["load-launch-agent"]);
    check("healthy_bootstrap_never_kickstarts",
      healthyLoad.code === 0 && healthyLoad.receipt?.readiness?.verified === true &&
      healthy.callCount("kickstart") === 0 && await statusAnswers(healthy.port),
      { code: healthyLoad.code, receipt: healthyLoad.receipt, kickstarts: healthy.callCount("kickstart") });

    const failed = await makeFixture("no-status", "no_status"); fixtures.push(failed);
    const failedLoad = failed.run(["load-launch-agent"]);
    check("kickstart_without_status_fails_once_with_clear_receipt",
      failedLoad.code === 1 && failedLoad.receipt?.loaded === true &&
      failedLoad.receipt?.status === "kickstart_readiness_failed" &&
      failedLoad.receipt?.readiness?.verified === false && failed.callCount("kickstart") === 1,
      { code: failedLoad.code, receipt: failedLoad.receipt, kickstarts: failed.callCount("kickstart") });

    const error = await makeFixture("kickstart-error", "kickstart_error"); fixtures.push(error);
    const errorLoad = error.run(["load-launch-agent"]);
    check("kickstart_error_exits_one_without_second_attempt",
      errorLoad.code === 1 && errorLoad.receipt?.loaded === true &&
      errorLoad.receipt?.status === "kickstart_failed" && error.callCount("kickstart") === 1,
      { code: errorLoad.code, receipt: errorLoad.receipt, kickstarts: error.callCount("kickstart") });

    const add = await makeFixture("capture-add", "healthy", true); fixtures.push(add);
    const boot = spawnSync(path.join(add.sandbox, "bin/launchctl"), ["bootstrap", "gui/fixture", "fixture.plist"],
      { env: add.env, encoding: "utf8", timeout: 10_000 });
    check("capture_add_starts_with_a_live_fixture_collector",
      boot.status === 0 && await waitForStatus(add.port), { boot: boot.status });
    const added = add.run(["capture-roots", "add", "--source", "claude_code", "--directory", add.rootToAdd!,
      "--machine", "fixture-machine", "--json"], { FAKE_MODE: "trap" });
    check("capture_roots_add_restart_kickstarts_once_and_verifies_status",
      added.code === 0 && added.receipt?.status === "capture_roots_added" &&
      added.receipt?.restart?.verified === true &&
      added.receipt?.restart?.load?.readiness?.verified === true &&
      add.callCount("kickstart") === 1 && await statusAnswers(add.port),
      { code: added.code, receipt: added.receipt, kickstarts: add.callCount("kickstart") });

    const failedAdd = await makeFixture("capture-add-fails", "healthy", true); fixtures.push(failedAdd);
    const initial = spawnSync(path.join(failedAdd.sandbox, "bin/launchctl"),
      ["bootstrap", "gui/fixture", "fixture.plist"],
      { env: failedAdd.env, encoding: "utf8", timeout: 10_000 });
    check("failed_capture_add_starts_with_a_live_fixture_collector",
      initial.status === 0 && await waitForStatus(failedAdd.port), { bootstrap: initial.status });
    const addFailure = failedAdd.run(["capture-roots", "add", "--source", "claude_code",
      "--directory", failedAdd.rootToAdd!, "--machine", "fixture-machine", "--json"],
      { FAKE_MODE: "no_status" });
    check("capture_roots_add_reports_failed_kickstart_without_retry",
      addFailure.code === 1 && addFailure.receipt?.status === "capture_roots_add_failed" &&
      addFailure.receipt?.recovery === "config_applied_collector_not_running" &&
      addFailure.receipt?.restart?.load?.status === "kickstart_readiness_failed" &&
      addFailure.receipt?.restart?.failedStep === "load" &&
      failedAdd.callCount("kickstart") === 1,
      { code: addFailure.code, receipt: addFailure.receipt,
        kickstarts: failedAdd.callCount("kickstart") });

    console.log(JSON.stringify({ status: "pass", proof: "launch-agent-kickstart", checks }));
  } finally {
    for (const fixture of fixtures.reverse()) await fixture.cleanup();
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
