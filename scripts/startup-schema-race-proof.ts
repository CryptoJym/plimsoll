import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { createProofCompletion } from "./lib/proof-completion";

const workspaceId = "30000000-0000-4000-8000-000000000003";
const deviceId = "40000000-0000-4000-8000-000000000004";
const sleepArray = new Int32Array(new SharedArrayBuffer(4));
const trials = Number(process.env.STARTUP_SCHEMA_RACE_TRIALS ?? 6);
const daemonTrials = Number(process.env.STARTUP_SCHEMA_DAEMON_TRIALS ?? 3);
assert.ok(Number.isInteger(trials) && trials >= 1 && trials <= 20);
assert.ok(Number.isInteger(daemonTrials) && daemonTrials >= 1 && daemonTrials <= 5);

if (process.argv[2] === "--child") {
  const [file, startBarrier, schemaMarkers, name] = process.argv.slice(3);
  assert.ok(file && startBarrier && schemaMarkers && name);

  // The unfixed open path reads this schema outside a writer transaction.
  // Hold both readers after the read so their subsequent ALTERs use the same
  // stale result. The fixed path owns BEGIN IMMEDIATE before this read.
  const prototype = Database.prototype as unknown as { pragma: (...args: unknown[]) => unknown };
  const originalPragma = prototype.pragma;
  prototype.pragma = function (sql: unknown, ...args: unknown[]) {
    const result = Reflect.apply(originalPragma, this, [sql, ...args]);
    if (sql === "table_info(buffered_events)" && !(this as Database.Database).inTransaction) {
      fs.writeFileSync(path.join(schemaMarkers, name), "read\n");
      const deadline = Date.now() + 20_000;
      while (fs.readdirSync(schemaMarkers).length < 2) {
        if (Date.now() > deadline) throw new Error("schema_read_barrier_timeout");
        Atomics.wait(sleepArray, 0, 0, 10);
      }
    }
    return result;
  };

  process.stdout.write("ready\n");
  while (!fs.existsSync(startBarrier)) Atomics.wait(sleepArray, 0, 0, 10);
  const buffer = new LocalEventBuffer(file, { workspaceId, deviceId });
  try {
    assert.equal(buffer.workspaceBinding()?.currentWorkspaceId, workspaceId);
    assert.equal(buffer.workspaceBinding()?.currentDeviceId, deviceId);
    console.log(JSON.stringify({ status: "opened", binding: buffer.workspaceBinding() }));
  } finally {
    buffer.close();
  }
} else {
  void runProof().catch(error => { console.error(error); process.exitCode = 1; });
}

async function runProof() {
  const completion = createProofCompletion("startup-schema-race", trials + daemonTrials);
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const script = import.meta.filename;
  for (let trial = 1; trial <= trials; trial++) {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "startup-schema-race-")));
    const file = path.join(fixture, "ledger.sqlite");
    const startBarrier = path.join(fixture, "go");
    const schemaMarkers = path.join(fixture, "schema-markers");
    fs.mkdirSync(schemaMarkers);
    const children = ["a", "b"].map(name => spawn(process.execPath,
      ["--import", loader, script, "--child", file, startBarrier, schemaMarkers, name],
      { stdio: ["ignore", "pipe", "pipe"] }));
    let timer: NodeJS.Timeout | undefined;
    try {
      const results = children.map(child => new Promise<{ code: number | null; out: string; err: string }>((resolve, reject) => {
        let out = "", err = "";
        child.stdout.setEncoding("utf8").on("data", chunk => { out += chunk; });
        child.stderr.setEncoding("utf8").on("data", chunk => { err += chunk; });
        child.once("error", reject);
        child.once("close", code => resolve({ code, out, err }));
      }));
      await new Promise(resolve => setTimeout(resolve, 500));
      fs.writeFileSync(startBarrier, "go\n");
      const settled = await Promise.race([Promise.all(results), new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("startup_schema_race_timeout")), 30_000);
      })]);
      console.log(JSON.stringify({ trial, processResults: settled }));
      assert.deepEqual(settled.map(result => result.code), [0, 0],
        "both fresh openers must succeed after the same startup barrier");
      const reopened = new LocalEventBuffer(file, { workspaceId, deviceId });
      try {
        assert.equal(reopened.workspaceBinding()?.currentWorkspaceId, workspaceId);
        assert.ok(reopened.workspaceBinding()?.currentInstallationEpochId);
      } finally { reopened.close(); }
      completion.check(`fresh_open_${trial}`);
    } finally {
      if (timer) clearTimeout(timer);
      for (const child of children) if (child.exitCode === null && child.pid) child.kill("SIGTERM");
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
  for (let trial = 1; trial <= daemonTrials; trial++) {
    await realDaemonTrial(trial, loader);
    completion.check(`real_daemon_hook_status_${trial}`);
  }
  completion.complete();
}

async function realDaemonTrial(trial: number, loader: string) {
  const repo = process.cwd();
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "startup-schema-real-cli-")));
  const home = path.join(fixture, "home");
  for (const folder of [".plimsoll", ".codex", ".claude", ".grok", "tmp", ".config", ".cache", ".state"])
    fs.mkdirSync(path.join(home, folder), { recursive: true, mode: 0o700 });
  const config = { port: 49100 + trial, tenantId: workspaceId,
    deviceId: "dev_startup_schema_fixture", managedConfig: { reconcile: { enabled: false } } };
  fs.writeFileSync(path.join(home, ".plimsoll/collector.config.json"), JSON.stringify(config), { mode: 0o600 });
  const env = { ...process.env, HOME: home, USERPROFILE: home,
    PLIMSOLL_HOME: path.join(home, ".plimsoll"), CODEX_HOME: path.join(home, ".codex"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"), GROK_HOME: path.join(home, ".grok"),
    TMPDIR: path.join(home, "tmp"), XDG_CONFIG_HOME: path.join(home, ".config"),
    XDG_CACHE_HOME: path.join(home, ".cache"), XDG_STATE_HOME: path.join(home, ".state"),
    STARTUP_SCHEMA_MIXED_ROOT: fixture, NEXT_TELEMETRY_DISABLED: "1" };
  const preload = path.join(repo, "scripts/startup-schema-cli-preload.cjs");
  const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
  const children: Array<{ role: string; child: ReturnType<typeof spawn>;
    done: Promise<{ role: string; code: number | null; stdout: string; stderr: string }> }> = [];
  try {
    // Seed the identity before the barrier so this proof isolates ledger opens.
    const seed = spawnSync(process.execPath, ["--import", loader, "-e",
      'const {loadOrCreateDeviceIdentity}=require("./packages/collector-cli/src/device-identity.ts"); ' +
      'loadOrCreateDeviceIdentity(undefined,{seed:{deviceId:"dev_startup_schema_fixture"}});'],
      { cwd: repo, env, encoding: "utf8", timeout: 15_000 });
    assert.equal(seed.status, 0, seed.stderr || seed.error?.message);
    for (const [role, args] of Object.entries({ daemon: ["start"],
      hook: ["forward-hook", "codex"], cli: ["status"] })) {
      const child = spawn(process.execPath, ["--require", preload, "--import", loader, cli, ...args],
        { cwd: repo, env: { ...env, STARTUP_SCHEMA_MIXED_ROLE: role }, stdio: ["pipe", "pipe", "pipe"] });
      let stdout = "", stderr = "";
      child.stdout!.setEncoding("utf8").on("data", chunk => { stdout += chunk; });
      child.stderr!.setEncoding("utf8").on("data", chunk => { stderr += chunk; });
      const done = new Promise<{ role: string; code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
        child.once("error", reject);
        child.once("close", code => resolve({ role, code, stdout, stderr }));
      });
      child.stdin!.end(JSON.stringify({ session_id: "50000000-0000-4000-8000-000000000005",
        event_type: "assistant_response", input_tokens: 3, output_tokens: 1 }) + "\n");
      children.push({ role, child, done });
    }
    const readyDeadline = Date.now() + 15_000;
    while (!children.every(({ role }) => fs.existsSync(path.join(fixture, `ready-${role}`)))) {
      assert.ok(children.every(({ child }) => child.exitCode === null), "real CLI exited before WAL barrier");
      assert.ok(Date.now() < readyDeadline, "real CLI WAL barrier timeout");
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    fs.writeFileSync(path.join(fixture, "go"), "go");
    const daemon = children[0]!.child;
    const listening = path.join(fixture, "listening-daemon");
    const listenDeadline = Date.now() + 15_000;
    while (daemon.exitCode === null && !fs.existsSync(listening) && Date.now() < listenDeadline)
      await new Promise(resolve => setTimeout(resolve, 5));
    const listened = fs.existsSync(listening);
    if (daemon.exitCode === null) daemon.kill("SIGTERM");
    let exitTimer: NodeJS.Timeout | undefined;
    const settled = await Promise.race([Promise.all(children.map(({ done }) => done)),
      new Promise<never>((_, reject) => {
        exitTimer = setTimeout(() => reject(new Error("real_cli_exit_timeout")), 15_000);
      })]).finally(() => { if (exitTimer) clearTimeout(exitTimer); });
    console.log(JSON.stringify({ trial, realCli: true, listened,
      results: settled.map(({ role, code, stderr }) => ({ role, code, stderr })) }));
    assert.ok(listened, "the real daemon must reach its listener");
    assert.deepEqual(settled.map(({ code }) => code), [0, 0, 0],
      "daemon, hook and status must all exit zero");
  } finally {
    for (const { child } of children) if (child.exitCode === null && child.pid) child.kill("SIGKILL");
    await Promise.allSettled(children.map(({ done }) => done));
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
