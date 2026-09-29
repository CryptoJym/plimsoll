import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { SqliteLedgerSnapshotAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, restoreArchivedLedger, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
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

const mode = process.argv[2];
if (mode === "--copy-child") {
  const copy = openLedgerCopyDatabase(process.argv[3]!, process.argv[4]!, { fileMustExist: true });
  fs.writeFileSync(path.join(process.argv[5]!, "copy-ready"), String(process.pid));
  const timer = setInterval(() => {}, 1_000);
  process.once("SIGTERM", () => { clearInterval(timer); copy.close(); });
} else if (mode === "--open-child" || mode === "--held-child") {
  const ledger = process.argv[3]!, fixture = process.argv[4]!;
  let buffer: LocalEventBuffer | undefined;
  try {
    buffer = new LocalEventBuffer(ledger, options);
    if (mode === "--open-child") {
      console.log(JSON.stringify({ opened: true, marker: Boolean(readReplacementLedgerMarker(ledger)) }));
      buffer.close();
    } else {
      const statement = buffer.database.prepare("insert into maintenance_state(key,value,updated_at) values('r13-stale-write','bad',?)");
      const transaction = buffer.database.transaction(() => statement.run(new Date().toISOString()));
      fs.writeFileSync(path.join(fixture, "ready"), String(process.pid));
      const timer = setInterval(() => {
        if (!fs.existsSync(path.join(fixture, "go"))) return;
        clearInterval(timer);
        try {
          if (process.argv[5] === "transaction") transaction.immediate();
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
  assert.ok(["cli", "supervised-restart", "shared-lifetime", "old-connection", "missing-marker",
    "integrity", "post-check-raw", "startup-marker", "startup-integrity"].includes(variant));
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "r13-connection-")));
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

    if (variant === "cli" || variant === "supervised-restart") {
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
      } finally { exclusive.release(); }
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
      fresh.database.prepare("insert into maintenance_state(key,value,updated_at) values('r13-fresh-control','kept',?)")
        .run(new Date().toISOString());
      fresh.close();
      const snapshot = path.join(fixture, "fresh-snapshot.sqlite");
      const snapshots = new SqliteLedgerSnapshotAdapter();
      await snapshots.snapshot({ source: ledger, destination: snapshot });
      const beforeRestore = fs.statSync(ledger).ino;
      await snapshots.restore({ source: snapshot, destination: ledger });
      assert.notEqual(fs.statSync(ledger).ino, beforeRestore, "a valid snapshot restore replaces the inode");
      const resumed = new LocalEventBuffer(ledger, options);
      assert.equal((resumed.database.prepare("select value from maintenance_state where key='r13-fresh-control'").get() as { value: string }).value, "kept");
      assert.ok(readReplacementLedgerMarker(ledger), "the verified fresh snapshot remains active");
      resumed.close();
      assert.equal(fs.statSync(sidecar).ino, lockInode);
      console.log(JSON.stringify({ variant, idleConnectionBlocksSwitch: true, exitedOwnerReleasesLock: true,
        publicationCommitKeepsExclusive: true, privateCopyHoldsDestinationLock: true, noTemporaryLock: true,
        stableLockInode: true, validFreshSnapshotRestarts: true }));
    } else if (variant === "old-connection") {
      for (const action of ["statement", "transaction"]) {
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
