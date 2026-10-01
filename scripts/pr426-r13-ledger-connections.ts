import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire, syncBuiltinESMExports } from "node:module";
import { Worker } from "node:worker_threads";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { ledgerConnectionWorkerSource, openLedgerDatabase } from "../packages/collector-cli/src/ledger-connection";
import { SqliteLedgerSnapshotAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import { LifecycleRestoreRefusal } from "../packages/collector-cli/src/lifecycle";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, restoreArchivedLedger, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { withJoinRootJournal } from "../packages/collector-cli/src/join-setup-journal";
import { acquireLedgerConnectionLock, ledgerConnectionLockPath, openLedgerCopyDatabase, readLedgerPublication, writeLedgerPublication } from "../packages/collector-cli/src/ledger-connection";

const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const epoch = "10000000-0000-4000-8000-000000000001";
const options = { workspaceId: workspace, deviceId: device, freshCaptureRootEpoch: epoch };
const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
const self = path.resolve("scripts/pr426-r13-ledger-connections.ts");
const cli = path.resolve("packages/collector-cli/src/cli.ts");
const require = createRequire(import.meta.url);
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function waitFor(test: () => boolean, label: string, timeout = 15_000) {
  const until = Date.now() + timeout;
  while (!test() && Date.now() < until) await sleep(20);
  assert.ok(test(), label);
}
async function stop(child: ChildProcess | null) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>(resolve => child.once("exit", () => resolve()));
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 5_000);
  await exited;
  clearTimeout(timer);
}
function syncWait(file: string) {
  const until = Date.now() + 15_000;
  while (!fs.existsSync(file) && Date.now() < until) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  assert.ok(fs.existsSync(file), "fixture child reached its boundary");
}

async function withArchiveLsofFault(archive: string, failures: number, action: () => Promise<void>) {
  const children = require("node:child_process") as { spawnSync: typeof spawnSync };
  const nativeSpawnSync = children.spawnSync;
  let calls = 0, injected = 0;
  children.spawnSync = ((...args: unknown[]) => {
    if (args[0] === "/usr/sbin/lsof" && Array.isArray(args[1]) && args[1].includes(archive)) {
      calls += 1;
      if (injected < failures) {
        injected += 1;
        return { status: 1, signal: null, stdout: "", stderr: "lsof: synthetic kernel timeout\n",
          pid: process.pid, output: [null, "", ""] };
      }
    }
    return Reflect.apply(nativeSpawnSync, children, args);
  }) as typeof spawnSync;
  syncBuiltinESMExports();
  try { await action(); }
  finally {
    children.spawnSync = nativeSpawnSync;
    syncBuiltinESMExports();
  }
  return { calls, injected };
}

// Fault only the admission probe. Recovery's separate check of both live and
// archived files still runs real lsof before it is allowed to restore.
function withStartupProbeFault(ledger: string, archive: string, failures: number,
  action: () => void, behavior: { consumeTimeout?: boolean; delayMs?: number;
    delayAfterFailures?: boolean; errorCode?: string } = {}) {
  const children = require("node:child_process") as { spawnSync: typeof spawnSync };
  const nativeSpawnSync = children.spawnSync;
  let calls = 0, recoveryCalls = 0;
  const timeouts: number[] = [], starts: number[] = [], ends: number[] = [];
  children.spawnSync = ((...args: unknown[]) => {
    if (args[0] === "/usr/sbin/lsof" && Array.isArray(args[1]) && args[1].includes(archive)) {
      if (args[1].includes(ledger)) recoveryCalls += 1;
      else {
        calls += 1;
        assert.ok(calls <= 3, "startup admission attempted a fourth probe");
        const timeout = (args[2] as { timeout: number }).timeout;
        timeouts.push(timeout);
        starts.push(performance.now());
        const delayMs = behavior.consumeTimeout ? timeout
          : behavior.delayAfterFailures && calls <= failures ? 0 : behavior.delayMs ?? 0;
        // The review's slow-lsof model: honor the supplied timeout, and run
        // real lsof if the delayed process would have survived that timeout.
        if (delayMs) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Math.min(delayMs, timeout));
        const timedOut = behavior.consumeTimeout || delayMs > timeout;
        if (calls <= failures || timedOut) {
          ends.push(performance.now());
          return { status: null, signal: null, stdout: "", stderr: "fixture private path/account/ledger details",
            error: { code: timedOut ? "ETIMEDOUT" : behavior.errorCode ?? "EPRIVATE_ACCOUNT_LEDGER" },
            pid: process.pid, output: [null, "", ""] };
        }
        const result = Reflect.apply(nativeSpawnSync, children, args);
        ends.push(performance.now());
        return result;
      }
    }
    return Reflect.apply(nativeSpawnSync, children, args);
  }) as typeof spawnSync;
  syncBuiltinESMExports();
  try { action(); }
  finally { children.spawnSync = nativeSpawnSync; syncBuiltinESMExports(); }
  return { calls, recoveryCalls, timeouts, starts, ends };
}

const startupProbeCases = ["transient", "two-transient", "persistent", "budget", "deadline",
  "slow", "stuck", "slow-after-transient", "fast-timeout", "slow-inconclusive", "near-deadline",
  "foreign-unused", "foreign-used", "foreign-after-transient", "ordinary"];

async function workerProbeChecks(ledger: string) {
  const cases = ["ETIMEDOUT", "ENOENT", "EACCES", "EPERM", "ENOBUFS", "EAGAIN", "EINTR",
    "EMFILE", "ENFILE", "ENOMEM", "EPRIVATE_ACCOUNT_LEDGER"].map(code =>
    ({ code, failures: 3, intentionalRename: false }));
  cases.push({ code: "EAGAIN", failures: 1, intentionalRename: true },
    { code: "EPRIVATE_ACCOUNT_LEDGER", failures: 3, intentionalRename: true });
  for (const test of cases) {
    const worker = new Worker(`
      const { parentPort, workerData } = require('node:worker_threads');
      const children = require('node:child_process');
      const native = children.spawnSync;
      let calls = 0;
      children.spawnSync = (...args) => {
        if (args[0] === '/usr/sbin/lsof' && ++calls <= workerData.failures)
          return { error: { code: workerData.code }, status: null, signal: null, stdout: '', stderr: '' };
        return native(...args);
      };
      const Database = require(workerData.sqliteModule);
      ${ledgerConnectionWorkerSource}
      try {
        const opened = openLedgerDatabase(workerData.ledger, { fileMustExist: true }, workerData.intentionalRename);
        opened.close();
        parentPort.postMessage({ ok: true, calls });
      } catch (error) {
        parentPort.postMessage({ ok: false, calls, code: error.code ?? null,
          cause: error.cause?.code ?? null, diagnostic: error.cause?.diagnostic ?? null });
      }
    `, { eval: true, execArgv: [], workerData: { ...test, ledger, sqliteModule: require.resolve("better-sqlite3") } });
    let timer: NodeJS.Timeout | undefined;
    try {
      const reply = await new Promise<unknown>((resolve, reject) => {
        worker.once("message", resolve);
        worker.once("error", reject);
        timer = setTimeout(() => reject(new Error("worker_archive_probe_timeout")), 15_000);
      });
      if (test.intentionalRename && test.failures === 1) {
        assert.deepEqual(reply, { ok: true, calls: 2 }, "serialized lifecycle opener retries an inconclusive probe");
      } else {
        const attempts = test.intentionalRename ? 3 : 1;
        assert.deepEqual(reply, { ok: false, calls: attempts, code: "LEDGER_PUBLICATION_INVALID",
          cause: "LEDGER_ARCHIVE_HANDLE_UNPROVEN", diagnostic: { stage: "archive_handle_probe",
            attempts, exitStatus: null, signal: null, stderr: false,
            errorCode: test.code === "EPRIVATE_ACCOUNT_LEDGER" ? "OTHER" : test.code } });
      }
      console.log(JSON.stringify({ variant: "worker-probe", intentionalRename: test.intentionalRename, reply }));
    } finally { if (timer) clearTimeout(timer); await worker.terminate(); }
  }
}

const mode = process.argv[2];
if (mode === "--join-journal-child") {
  try {
    withJoinRootJournal(process.argv[3]!, database => database.prepare("select 1").get());
    console.log("join journal opened");
  } catch (error) { console.error(String(error)); process.exitCode = 1; }
} else if (mode === "--copy-child") {
  const copy = openLedgerCopyDatabase(process.argv[3]!, process.argv[4]!, { fileMustExist: true });
  fs.writeFileSync(path.join(process.argv[5]!, "copy-ready"), String(process.pid));
  const timer = setInterval(() => {}, 1_000);
  process.once("SIGTERM", () => { clearInterval(timer); copy.close(); });
} else if (mode === "--raw-child") {
  const raw = new Database(process.argv[3]!, { fileMustExist: true });
  if (process.argv[5] === "used") raw.prepare("select name from sqlite_master limit 1").get();
  fs.writeFileSync(path.join(process.argv[4]!, "raw-ready"), String(process.pid));
  const timer = setInterval(() => {}, 1_000);
  process.once("SIGTERM", () => { clearInterval(timer); raw.close(); });
} else if (mode === "--open-child" || mode === "--held-child") {
  const ledger = process.argv[3]!, fixture = process.argv[4]!;
  let buffer: LocalEventBuffer | undefined;
  try {
    buffer = new LocalEventBuffer(ledger, options);
    if (mode === "--open-child") {
      console.log(JSON.stringify({ opened: true, marker: Boolean(readReplacementLedgerMarker(ledger)) }));
      buffer.close();
    } else {
      const statement = buffer.database.prepare("insert into maintenance_state(key,value,updated_at) values('r13-stale-write','bad',?)" +
        (process.argv[5] === "returning" ? " returning key" : ""));
      const transaction = buffer.database.transaction(() => statement.run(new Date().toISOString()));
      fs.writeFileSync(path.join(fixture, "ready"), String(process.pid));
      const timer = setInterval(() => {
        if (!fs.existsSync(path.join(fixture, "go"))) return;
        clearInterval(timer);
        try {
          if (process.argv[5] === "transaction") transaction.immediate();
          else if (process.argv[5] === "returning") statement.get(new Date().toISOString());
          else statement.run(new Date().toISOString());
          fs.writeFileSync(path.join(fixture, "result.json"), JSON.stringify({ wrote: true }));
        } catch (error) {
          fs.writeFileSync(path.join(fixture, "result.json"), JSON.stringify({ wrote: false,
            code: (error as { code?: string }).code, message: String(error), open: buffer!.database.open }));
        } finally { buffer!.close(); }
      }, 10);
    }
  } catch (error) {
    buffer?.close();
    console.error(String(error));
    process.exitCode = 1;
  }
} else {
  void main(mode ?? "cli").catch(error => { console.error(error); process.exitCode = 1; });
}

async function main(variant: string) {
  if (variant === "startup-probes") {
    for (const test of startupProbeCases) await main(`startup-probe-${test}`);
    return;
  }
  // Keep these regressions in an existing CI proof without changing its gates.
  if (variant === "startup-marker") await main("startup-probes");
  assert.ok(["cli", "supervised-restart", "shared-lifetime", "old-connection", "missing-marker",
    "integrity", "post-check-raw", "startup-marker", "startup-integrity", "worker-probes",
    ...startupProbeCases.map(test => `startup-probe-${test}`)].includes(variant));
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),
    variant.startsWith("startup-probe-foreign-") ? "PRIVATE_LEDGER_PATH_443-" : "r13-connection-")));
  const home = path.join(fixture, "home"), data = path.join(home, ".plimsoll");
  const archiveDir = path.join(fixture, "archive"), root = path.join(home, "sessions");
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  fs.mkdirSync(archiveDir, { mode: 0o700 });
  fs.mkdirSync(root);
  const ledger = path.join(data, "work-ledger.sqlite"), archive = path.join(archiveDir, "old.sqlite");
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", port: 49613, captureRoots: [{ source: "codex", rootId: "codex",
      profileId: "fixture", directory: root, installationEpochId: epoch }] });
  fs.writeFileSync(path.join(data, "collector.config.json"), JSON.stringify(config));
  const env = { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: data,
    CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    GROK_HOME: path.join(home, ".grok"), NEXT_TELEMETRY_DISABLED: "1" };
  const originalRename = fs.renameSync;
  let child: ChildProcess | null = null;
  try {
    const old = new LocalEventBuffer(ledger, { ...options,
      enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    old.database.prepare("insert into maintenance_state(key,value,updated_at) values('r13-archive-control','kept',?)")
      .run(new Date(Date.now() - 86_400_000).toISOString());
    old.close();
    const input = { ledgerPath: ledger, archivePath: archive, config,
      authorityRoot: path.join(fixture, "authority") };
    const invoke = (args: string[]) => spawnSync(process.execPath, ["--import", loader, ...args],
      { env, encoding: "utf8", timeout: 30_000 });

    if (variant === "worker-probes") {
      switchFreshLedger(input);
      await workerProbeChecks(ledger);
    } else if (variant.startsWith("startup-probe-")) {
      switchFreshLedger(input);
      const freshInode = fs.statSync(ledger).ino;
      const archiveBefore = fs.readFileSync(archive);
      const fresh = new LocalEventBuffer(ledger, options);
      fresh.database.prepare("insert into maintenance_state(key,value,updated_at) values('startup-fresh-control','kept',?)")
        .run(new Date().toISOString());
      fresh.close();
      const test = variant.slice("startup-probe-".length);
      if (test.startsWith("foreign-")) {
        child = spawn(process.execPath, ["--import", loader, self, "--raw-child", archive, fixture,
          test === "foreign-used" ? "used" : "unused"], { env, stdio: "ignore" });
        await waitFor(() => fs.existsSync(path.join(fixture, "raw-ready")), "real foreign archive connection is open");
      }
      const failures = test === "two-transient" ? 2
        : ["persistent", "budget", "deadline", "fast-timeout", "slow-inconclusive", "near-deadline"].includes(test) ? Infinity
        : ["foreign-used", "foreign-unused", "slow", "stuck"].includes(test) ? 0 : 1;
      const deadline = performance.now() + (test === "deadline" ? 250 : test === "near-deadline" ? 750 : 10_000);
      let refusal: (Error & { code?: string; cause?: { code?: string; diagnostic?: unknown } }) | undefined;
      const probes = withStartupProbeFault(ledger, archive, failures, () => {
        try {
          if (test === "ordinary") openLedgerDatabase(ledger, { fileMustExist: true, timeout: 0 }).close();
          else {
            const started = new LocalEventBuffer(ledger, { ...options, startupBusyDeadlineMs: deadline });
            try {
              assert.equal((started.database.prepare("select value from maintenance_state where key='startup-fresh-control'")
                .get() as { value: string }).value, "kept");
            } finally { started.close(); }
          }
        } catch (error) { refusal = error as typeof refusal; }
      }, { consumeTimeout: test === "budget" || test === "deadline",
        delayMs: test === "slow" || test === "slow-after-transient" ? 4_500
          : test === "stuck" ? 12_000 : test === "slow-inconclusive" ? 1_500 : 0,
        delayAfterFailures: test === "slow-after-transient",
        errorCode: test === "fast-timeout" ? "ETIMEDOUT" : undefined });
      const lock = acquireLedgerConnectionLock(ledger, "exclusive");
      let receipt;
      try { receipt = readLedgerPublication(lock)!; } finally { lock.release(); }
      if (["transient", "two-transient", "slow", "slow-after-transient"].includes(test)) {
        assert.equal(refusal, undefined, "a transient startup probe must start on the fresh ledger without restoring");
        assert.equal(probes.calls, failures + 1);
        assert.equal(probes.recoveryCalls, 0);
        assert.equal(receipt.state, "ready");
        assert.equal(receipt.freshAttemptPath, undefined);
        assert.equal(fs.statSync(ledger).ino, freshInode);
      } else if (test.startsWith("foreign-") || test === "ordinary") {
        assert.equal(refusal?.code, "LEDGER_PUBLICATION_INVALID");
        assert.equal(refusal?.cause?.code, test === "ordinary"
          ? "LEDGER_ARCHIVE_HANDLE_UNPROVEN" : "LEDGER_ARCHIVE_HANDLE_IN_USE");
        assert.equal(probes.calls, test === "foreign-after-transient" ? 2 : 1);
        assert.equal(probes.recoveryCalls, 0, "a reported foreign PID refuses immediately, without another probe");
        assert.equal(receipt.state, "ready");
        assert.equal(fs.statSync(ledger).ino, freshInode);
        if (test === "ordinary") assert.deepEqual(probes.timeouts, [10_000]);
        else {
          // The outer CLI handler must preserve the same immediate refusal.
          // Capture every console status/log as well as stdout/stderr. Neither
          // the private error property nor raw lsof stderr may cross that boundary.
          const cliDevice = "dev_PRIVATE_REFUSAL_443", cliKey = "key_PRIVATE_REFUSAL_443";
          const seed = invoke(["-e", `require('./packages/collector-cli/src/device-identity.ts')
            .loadOrCreateDeviceIdentity(undefined, { seed: ${JSON.stringify({ deviceId: cliDevice, keyId: cliKey })} });`]);
          assert.equal(seed.status, 0, "seed a valid device identity for the real daemon start");
          const preload = path.join(fixture, "refuse-recovery.cjs");
          const consoleLog = path.join(fixture, "console.jsonl");
          fs.writeFileSync(preload, `
            const fs = require('node:fs');
            const util = require('node:util');
            for (const method of ['log', 'error', 'warn', 'info', 'debug', 'dir']) {
              const original = console[method];
              console[method] = function (...args) {
                fs.appendFileSync(${JSON.stringify(consoleLog)}, JSON.stringify({
                  method, text: util.format(...args) }) + '\\n');
                return Reflect.apply(original, console, args);
              };
            }
            require('node:net').Server.prototype.listen = function () {
              throw new Error('unexpected_listener_before_foreign_handle_refusal');
            };
            const children = require('node:child_process');
            for (const method of ['spawn', 'spawnSync', 'execFile', 'execFileSync']) {
              const original = children[method];
              children[method] = function (command, ...args) {
                if (/(^|\\/)launchctl$/.test(String(command))) throw new Error('unexpected_launchctl');
                return Reflect.apply(original, this, [command, ...args]);
              };
            }
            const native = children.spawnSync;
            children.spawnSync = function (command, args, options) {
              if (command === '/usr/sbin/lsof' && args.includes(${JSON.stringify(ledger)}) &&
                  args.includes(${JSON.stringify(archive)})) throw new Error('unexpected_recovery_probe');
              const result = native(command, args, options);
              if (command === '/usr/sbin/lsof' && args.includes(${JSON.stringify(archive)}))
                result.stderr = ${JSON.stringify(`PRIVATE_LSOF_STDERR_443 ${ledger} ${receipt.marker.archiveIdentity}`)};
              return result;
            };
            require('node:module').syncBuiltinESMExports();
          `);
          for (const command of [["export", "--budget"], ["start"]]) {
            fs.writeFileSync(consoleLog, "");
            const cliRefusal = invoke(["--require", preload, cli, ...command]);
            assert.equal(cliRefusal.status, 1);
            const logged = fs.readFileSync(consoleLog, "utf8");
            const output = cliRefusal.stdout + cliRefusal.stderr + logged;
            assert.equal([fixture, ledger, archive, receipt.marker.archiveIdentity, workspace, device, epoch, cliDevice, cliKey,
              "PRIVATE_LEDGER_PATH_443", "PRIVATE_LSOF_STDERR_443", "ledgerPath:"].some(value => output.includes(value)),
            false, "foreign-handle stdout, stderr, status and logs must be value-free");
            assert.match(cliRefusal.stderr, /old inode or sidecar handle/);
            assert.doesNotMatch(output, /unexpected_recovery_probe|unexpected_listener|unexpected_launchctl/);
            const errors = logged.trim().split("\n").map(line => JSON.parse(line))
              .filter(row => row.method === "error").map(row => row.text);
            assert.deepEqual(errors, ["replacement_verification_failed: old inode or sidecar handle; command refused"]);
            assert.equal(fs.statSync(ledger).ino, freshInode);
          }
        }
      } else {
        assert.match(String(refusal), /replacement_verification_failed; archive restored; collector start refused/);
        assert.equal(probes.calls, test === "persistent" ? 3 : 1, "only fast inconclusive probes are retried");
        assert.equal(receipt.state, "restored");
        assert.ok(receipt.freshAttemptPath);
        assert.equal(fs.statSync(receipt.freshAttemptPath).ino, freshInode);
        assert.notEqual(fs.statSync(ledger).ino, freshInode);
        const restored = new LocalEventBuffer(ledger, options);
        try {
          assert.equal(restored.database.prepare("select value from maintenance_state where key='startup-fresh-control'").get(), undefined);
          assert.equal((restored.database.prepare("select value from maintenance_state where key='r13-archive-control'")
            .get() as { value: string }).value, "kept");
        } finally { restored.close(); }
        const diagnostic = (refusal?.cause as { cause?: { diagnostic?: unknown } })?.cause?.diagnostic;
        assert.deepEqual(diagnostic, { stage: "archive_handle_probe", attempts: probes.calls,
          exitStatus: null, signal: null, stderr: true,
          errorCode: ["budget", "deadline", "stuck", "fast-timeout"].includes(test) ? "ETIMEDOUT" : "OTHER" },
        "diagnostics contain only allowlisted values");
      }
      if (test !== "ordinary") {
        for (let i = 0; i < probes.calls; i += 1) {
          assert.ok(probes.timeouts[i]! > 0 && probes.timeouts[i]! <= 10_000);
          // Every probe gets the whole remaining deadline, never a new budget
          // or a shorter per-attempt cap. Allow only call/setup rounding here.
          assert.ok(Math.abs(probes.timeouts[i]! - (deadline - probes.starts[i]!)) < 20);
        }
        for (let i = 1; i < probes.calls; i += 1) assert.ok(probes.starts[i]! - probes.ends[i - 1]! >= 90);
        if (test === "deadline") assert.ok(probes.timeouts[0]! <= 250, "remaining startup budget caps lsof timeout");
      }
      assert.deepEqual(fs.readFileSync(archive), archiveBefore, "archive content is unchanged");
      console.log(JSON.stringify({ variant, admissionProbes: probes.calls, recoveryProbes: probes.recoveryCalls,
        timeouts: probes.timeouts, admissionElapsedMs: probes.ends.at(-1)! - probes.starts[0]!,
        publicationState: receipt.state, passed: true }));
    } else if (variant === "cli" || variant === "supervised-restart") {
      const args = variant === "cli" ? [cli, "export", "--budget"] : [self, "--open-child", ledger, fixture];
      let refused = false;
      switchFreshLedger({ ...input, onStep(step) {
        if (step !== "old_locked") return;
        const attempt = invoke(args);
        assert.equal(attempt.error, undefined);
        assert.equal(attempt.status, 1, attempt.stdout + attempt.stderr);
        assert.match(attempt.stderr, /ledger switch in progress/);
        refused = true;
        console.log(JSON.stringify({ variant, duringSwitch: { exit: attempt.status, stderr: attempt.stderr.trim() } }));
      } });
      assert.ok(refused);
      const retry = invoke(args);
      assert.equal(retry.status, 0, retry.stdout + retry.stderr);
      console.log(JSON.stringify({ variant, restartAfterSwitch: { exit: retry.status, stdout: retry.stdout.trim() } }));
    } else if (variant === "shared-lifetime") {
      child = spawn(process.execPath, ["--import", loader, self, "--held-child", ledger, fixture], { env, stdio: "ignore" });
      await waitFor(() => fs.existsSync(path.join(fixture, "ready")), "idle collector has its open connection");
      assert.throws(() => acquireLedgerConnectionLock(ledger, "exclusive"), /collector connections are still open/);
      await stop(child); child = null;
      const exclusive = acquireLedgerConnectionLock(ledger, "exclusive");
      try {
        assert.equal(exclusive.database.pragma("fullfsync", { simple: true }), 1);
        assert.equal(exclusive.database.pragma("synchronous", { simple: true }), 3);
        writeLedgerPublication(exclusive, null);
        const afterCommit = invoke([self, "--open-child", ledger, fixture]);
        assert.equal(afterCommit.status, 1, afterCommit.stdout + afterCommit.stderr);
        assert.match(afterCommit.stderr, /ledger switch in progress/);
        const joinJournal = invoke([self, "--join-journal-child", ledger, fixture]);
        assert.equal(joinJournal.status, 1, joinJournal.stdout + joinJournal.stderr);
        assert.match(joinJournal.stderr, /ledger switch in progress/);
      } finally { exclusive.release(); }
      const joinedAfterSwitch = invoke([self, "--join-journal-child", ledger, fixture]);
      assert.equal(joinedAfterSwitch.status, 0, joinedAfterSwitch.stdout + joinedAfterSwitch.stderr);
      const sidecar = ledgerConnectionLockPath(ledger);
      const lockInode = fs.statSync(sidecar).ino;
      const copy = `${ledger}.restore-fixture`;
      fs.copyFileSync(ledger, copy);
      child = spawn(process.execPath, ["--import", loader, self, "--copy-child", copy, ledger, fixture], { env, stdio: "ignore" });
      await waitFor(() => fs.existsSync(path.join(fixture, "copy-ready")), "private restore copy is open");
      assert.throws(() => acquireLedgerConnectionLock(ledger, "exclusive"), /collector connections are still open/);
      assert.equal(fs.existsSync(ledgerConnectionLockPath(copy)), false, "private copies use the destination lock");
      await stop(child); child = null;
      const afterCopy = acquireLedgerConnectionLock(ledger, "exclusive");
      afterCopy.release();
      assert.equal(fs.statSync(sidecar).ino, lockInode, "the stable lock is never replaced or deleted");
      const checkpoint = new LocalEventBuffer(ledger, options);
      checkpoint.database.pragma("wal_checkpoint(TRUNCATE)");
      checkpoint.close();
      switchFreshLedger(input);
      const fresh = new LocalEventBuffer(ledger, options);
      const originalStat = fs.statSync;
      let readStatCalls = 0;
      (fs as unknown as { statSync: typeof fs.statSync }).statSync = (() => { readStatCalls += 1; throw new Error("read_path_stat_forbidden"); }) as typeof fs.statSync;
      try {
        const read = fresh.database.prepare("select 1 as value");
        assert.equal(read.readonly, true);
        assert.deepEqual(read.get(), { value: 1 });
        assert.deepEqual(read.all(), [{ value: 1 }]);
        assert.deepEqual([...read.iterate()], [{ value: 1 }]);
      } finally { (fs as unknown as { statSync: typeof fs.statSync }).statSync = originalStat; }
      assert.equal(readStatCalls, 0, "read-only statements do not stat the ledger");
      fresh.database.prepare("insert into maintenance_state(key,value,updated_at) values('r13-fresh-control','kept',?)")
        .run(new Date().toISOString());
      fresh.close();
      const snapshot = path.join(fixture, "fresh-snapshot.sqlite");
      const snapshots = new SqliteLedgerSnapshotAdapter();
      await snapshots.snapshot({ source: ledger, destination: snapshot });
      const beforeRestore = fs.statSync(ledger).ino;
      child = spawn(process.execPath, ["--import", loader, self, "--raw-child", archive, fixture], { env, stdio: "ignore" });
      await waitFor(() => fs.existsSync(path.join(fixture, "raw-ready")), "raw archive handle is open");
      await assert.rejects(snapshots.restore({ source: snapshot, destination: ledger }), (error: unknown) =>
        error instanceof LifecycleRestoreRefusal && error.refusal.reason === "quiescence_unproven");
      assert.equal(fs.statSync(ledger).ino, beforeRestore, "foreign archive handle cannot change live inode");
      await stop(child); child = null;
      fs.rmSync(path.join(fixture, "raw-ready"));
      child = spawn(process.execPath, ["--import", loader, self, "--raw-child", ledger, fixture], { env, stdio: "ignore" });
      await waitFor(() => fs.existsSync(path.join(fixture, "raw-ready")), "raw live handle is open");
      await assert.rejects(snapshots.restore({ source: snapshot, destination: ledger }), (error: unknown) =>
        error instanceof LifecycleRestoreRefusal && error.refusal.reason === "ledger_in_use");
      assert.equal(fs.statSync(ledger).ino, beforeRestore, "foreign live handle cannot change live inode");
      await stop(child); child = null;
      let persistentDiagnostic: LifecycleRestoreRefusal["diagnostic"];
      const persistent = await withArchiveLsofFault(archive, Number.POSITIVE_INFINITY, async () => {
        await assert.rejects(snapshots.restore({ source: snapshot, destination: ledger }), (error: unknown) => {
          assert.ok(error instanceof LifecycleRestoreRefusal);
          assert.equal(error.refusal.reason, "quiescence_unproven");
          persistentDiagnostic = error.diagnostic;
          return true;
        });
      });
      assert.deepEqual(persistent, { calls: 3, injected: 3 }, "archive lsof retry is bounded");
      assert.deepEqual(persistentDiagnostic, { stage: "archive_handle_probe", attempts: 3,
        exitStatus: 1, signal: null, stderr: true, errorCode: null });
      assert.equal(fs.statSync(ledger).ino, beforeRestore, "unknown archive handles cannot change live inode");
      // An ordinary open outside startup recovery keeps main's single probe:
      // one inconclusive answer refuses at once.
      const ordinary = await withArchiveLsofFault(archive, 1, async () => {
        assert.throws(() => openLedgerDatabase(ledger, { fileMustExist: true, timeout: 0 }),
          (error: unknown) => (error as { code?: string }).code === "LEDGER_PUBLICATION_INVALID");
      });
      assert.deepEqual(ordinary, { calls: 1, injected: 1 }, "an ordinary open does not retry the archive probe");
      assert.equal(fs.statSync(ledger).ino, beforeRestore, "a refused ordinary open changes nothing");
      const transient = await withArchiveLsofFault(archive, 1, async () => {
        await snapshots.restore({ source: snapshot, destination: ledger });
      });
      assert.deepEqual(transient, { calls: 2, injected: 1 }, "one transient archive lsof error is retried");
      assert.notEqual(fs.statSync(ledger).ino, beforeRestore, "a valid snapshot restore replaces the inode");
      const resumed = new LocalEventBuffer(ledger, options);
      assert.equal((resumed.database.prepare("select value from maintenance_state where key='r13-fresh-control'").get() as { value: string }).value, "kept");
      assert.ok(readReplacementLedgerMarker(ledger), "the verified fresh snapshot remains active");
      resumed.close();
      assert.equal(fs.statSync(sidecar).ino, lockInode);
      await workerProbeChecks(ledger);
      console.log(JSON.stringify({ variant, idleConnectionBlocksSwitch: true, exitedOwnerReleasesLock: true,
        publicationCommitKeepsExclusive: true, privateCopyHoldsDestinationLock: true, noTemporaryLock: true,
        stableLockInode: true, readOnlyStatementsDoNotStat: true, validFreshSnapshotRestarts: true,
        foreignArchiveAndLiveHandlesRefuseRestore: true, archiveProbeRetryBounded: true, ordinaryOpenProbesOnce: true,
        transientArchiveProbeRecovered: true }));
    } else if (variant === "old-connection") {
      for (const action of ["statement", "transaction", "returning"]) {
        for (const name of ["ready", "go", "result.json"]) fs.rmSync(path.join(fixture, name), { force: true });
        child = spawn(process.execPath, ["--import", loader, self, "--held-child", ledger, fixture, action], { env, stdio: "ignore" });
        await waitFor(() => fs.existsSync(path.join(fixture, "ready")), "old collector is ready");
        const candidate = path.join(data, "candidate.sqlite");
        const replacement = new LocalEventBuffer(candidate, options);
        replacement.database.exec("create table fixture_fresh_marker (value text); insert into fixture_fresh_marker values('untouched')");
        replacement.close();
        // Deliberately bypass the cooperative barrier to simulate a surviving
        // pre-upgrade/manual rename. The next write must retire the old handle.
        fs.renameSync(ledger, `${archive}-${action}`);
        for (const suffix of ["-wal", "-shm"]) if (fs.existsSync(`${ledger}${suffix}`)) {
          fs.renameSync(`${ledger}${suffix}`, `${archive}-${action}${suffix}`);
        }
        fs.renameSync(candidate, ledger);
        const before = fs.readFileSync(ledger);
        fs.writeFileSync(path.join(fixture, "go"), "1");
        await waitFor(() => child!.exitCode !== null || child!.signalCode !== null, "stale collector exits for restart");
        const result = JSON.parse(fs.readFileSync(path.join(fixture, "result.json"), "utf8"));
        assert.equal(result.wrote, false);
        assert.equal(result.code, "LEDGER_REPLACED");
        assert.equal(result.open, false);
        assert.equal(child.exitCode, 75);
        assert.deepEqual(fs.readFileSync(ledger), before, "stale close and write must not touch the new main file");
        const active = new Database(ledger, { readonly: true });
        assert.equal(active.pragma("integrity_check", { simple: true }), "ok");
        assert.equal((active.prepare("select count(*) as n from maintenance_state where key='r13-stale-write'").get() as { n: number }).n, 0);
        assert.equal((active.prepare("select value from fixture_fresh_marker").get() as { value: string }).value, "untouched");
        active.close(); child = null;
        console.log(JSON.stringify({ variant, action, result, exit: 75, freshMainUnchanged: true }));
      }
    } else {
      const archiveBefore = fs.readFileSync(ledger);
      let injected = false;
      if (variant === "post-check-raw") {
        fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
          if (String(source) === `${ledger}.replacement-stage` && String(destination) === ledger && !injected) {
            const raw = `const fs=require('node:fs'); const Database=require(process.argv[1]);
              const db=new Database(process.argv[2],{fileMustExist:true,timeout:0});
              fs.writeFileSync(process.argv[3],String(process.pid)); setInterval(()=>{},1000);`;
            child = spawn(process.execPath, ["-e", raw, require.resolve("better-sqlite3"), ledger,
              path.join(fixture, "raw-ready")], { env, stdio: "ignore" });
            syncWait(path.join(fixture, "raw-ready"));
            injected = true;
          }
          return originalRename(source, destination);
        }) as typeof fs.renameSync;
      }
      const damage = () => {
        if (variant === "missing-marker" || variant === "startup-marker") {
          const candidate = new Database(ledger);
          candidate.exec("drop table collector_replacement_ledger");
          candidate.close();
        } else {
          const fd = fs.openSync(ledger, "r+");
          try { fs.writeSync(fd, Buffer.from("invalid SQLite header"), 0, 21, 0); }
          finally { fs.closeSync(fd); }
        }
        injected = true;
      };
      if (variant.startsWith("startup-")) {
        switchFreshLedger(input);
        damage();
        assert.throws(() => new LocalEventBuffer(ledger, options), /replacement_verification_failed.*archive restored/);
      } else assert.throws(() => switchFreshLedger({ ...input, onStep(step) {
        if (step === "candidate_published" && variant !== "post-check-raw") damage();
      } }), /replacement_verification_failed/);
      fs.renameSync = originalRename;
      assert.ok(injected);
      let lock = acquireLedgerConnectionLock(ledger, "exclusive");
      const receipt = readLedgerPublication(lock)!;
      lock.release();
      const suspect = receipt.freshAttemptPath!;
      assert.ok(fs.existsSync(suspect), "suspect candidate is retained");
      if (variant === "post-check-raw") {
        assert.equal(receipt.state, "failed");
        assert.equal(fs.statSync(ledger).ino, fs.statSync(archive).ino, "old inode restored before releasing the old lock");
        assert.throws(() => new LocalEventBuffer(ledger, options), /ledger switch in progress/);
        await stop(child); child = null;
        restoreArchivedLedger({ ledgerPath: ledger, archivePath: archive, freshAttemptPath: suspect,
          authorityRoot: input.authorityRoot });
      } else assert.equal(receipt.state, "restored", "verification failure uses archive restore before refusing");
      const active = new LocalEventBuffer(ledger, options);
      assert.equal(active.database.pragma("integrity_check", { simple: true }), "ok");
      assert.equal((active.database.prepare("select value from maintenance_state where key='r13-archive-control'").get() as { value: string }).value, "kept");
      active.close();
      assert.deepEqual(fs.readFileSync(archive), archiveBefore, "immutable archive content is unchanged");
      console.log(JSON.stringify({ variant, restored: true, suspectRetained: true, archiveUnchanged: true }));
    }
  } finally {
    fs.renameSync = originalRename;
    await stop(child);
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
