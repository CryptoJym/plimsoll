import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const sessionId = "70000000-0000-4000-8000-000000000007";
const line = (id: string, at: Date) => JSON.stringify({ type: "assistant", timestamp: at.toISOString(),
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";
const require = createRequire(import.meta.url);

// A process can open the old pathname after the lsof preflight while the
// exclusive transaction is held. Keep its SQLite handle through the rename,
// then let it write after the switch releases the old transaction.
const childSource = String.raw`
const fs = require('node:fs');
const Database = require(process.argv[1]);
const [ledger, ready, go, result, id] = process.argv.slice(2);
let db;
try {
  db = new Database(ledger, {fileMustExist: true, timeout: 0});
  fs.writeFileSync(ready, String(process.pid));
  const timer = setInterval(() => {
    if (!fs.existsSync(go)) return;
    clearInterval(timer);
    try {
      const now = new Date().toISOString();
      db.prepare('insert into buffered_events (id,source,event_type,data_mode,observed_at,payload_json,created_at) values (?,?,?,?,?,?,?)')
        .run(id, 'claude_code', 'assistant_response', 'metadata', now, '{}', now);
      fs.writeFileSync(result, JSON.stringify({wrote: true}));
    } catch (error) {
      fs.writeFileSync(result, JSON.stringify({wrote: false, code: error.code, message: error.message}));
    } finally { db.close(); }
  }, 5);
} catch (error) {
  fs.writeFileSync(result, JSON.stringify({opened: false, code: error.code, message: error.message}));
  process.exitCode = 1;
}`;

async function main() {
  const postOpenControl = process.argv.includes("--post-open-control");
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "pr426-r8-old-handle-")));
  let child: ChildProcess | null = null;
  const originalRename = fs.renameSync;
  try {
    const ledgerPath = path.join(fixture, "work-ledger.sqlite");
    const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
    const root = path.join(fixture, "claude");
    const codexRoot = path.join(fixture, "codex");
    const file = path.join(root, "project", `${sessionId}.jsonl`);
    const ready = path.join(fixture, "opened");
    const go = path.join(fixture, "go");
    const result = path.join(fixture, "writer-result.json");
    const gapId = deterministicEventId(["claude-transcript", sessionId, "forward-gap"]);
    fs.mkdirSync(root);
    fs.mkdirSync(codexRoot);
    fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
    const captureRoot = { source: "claude_code" as const, rootId: "claude-root",
      profileId: "claude-profile", directory: root, installationEpochId: epoch };
    const codexCaptureRoot = { source: "codex" as const, rootId: "codex-root",
      profileId: "codex-profile", directory: codexRoot, installationEpochId: epoch };
    const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
      installKey: "fixture-install-key", captureRoots: [codexCaptureRoot, captureRoot] });
    const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    old.close();
    const originalInode = fs.statSync(ledgerPath).ino;
    let ledgerRenames = 0;
    let openerSeenByLsof = false;
    let gapCreated = false;
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if ([`${ledgerPath}.replacement-stage`, `${ledgerPath}-wal`, `${ledgerPath}-shm`].includes(String(source))) ledgerRenames++;
      if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, line("forward-gap", new Date(Date.now() + 3_600_000)));
        gapCreated = true;
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    try { switchFreshLedger({ ledgerPath, archivePath, config,
      authorityRoot: path.join(fixture, "lifecycle-authority"),
      onStep(step) {
        if (step !== "old_locked" || postOpenControl) return;
        child = spawn(process.execPath, ["-e", childSource, require.resolve("better-sqlite3"),
          ledgerPath, ready, go, result, gapId], { cwd: process.cwd(), stdio: "ignore" });
        const until = Date.now() + 5_000;
        while (!fs.existsSync(ready) && !fs.existsSync(result) && Date.now() < until) {
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
        }
        assert.equal(fs.existsSync(ready), true, "second process must open old pathname during lock");
        const lsof = spawnSync("/usr/sbin/lsof", ["-t", "-w", "--", ledgerPath],
          { encoding: "utf8", timeout: 10_000 });
        openerSeenByLsof = lsof.stdout.split("\n").includes(String(child.pid));
      } }); } catch (error) {
      if (!(error instanceof Error) || error.message !== "ledger_quiescence_unproven: foreign handle before rename") throw error;
      assert.equal(openerSeenByLsof, true);
      assert.equal(gapCreated, false);
      assert.equal(ledgerRenames, 0, "no ledger or sidecar may be renamed before the raw-handle refusal");
      assert.equal(fs.statSync(ledgerPath).ino, originalInode);
      const blocker = child as ChildProcess | null;
      if (blocker && blocker.exitCode === null) {
        const exited = new Promise<void>(resolve => blocker.once("exit", () => resolve()));
        blocker.kill("SIGTERM");
        await exited;
      }
      const active = new Database(ledgerPath, { readonly: true });
      try { assert.equal(active.pragma("integrity_check", { simple: true }), "ok"); }
      finally { active.close(); }
      console.log(JSON.stringify({ refused: true, reason: error.message, ledgerRenames,
        originalInodeRetained: true, activeIntegrity: "ok" }));
      return;
    }
    fs.renameSync = originalRename;
    assert.equal(gapCreated, true);
    if (postOpenControl) {
      child = spawn(process.execPath, ["-e", childSource, require.resolve("better-sqlite3"),
        ledgerPath, ready, go, result, gapId], { cwd: process.cwd(), stdio: "ignore" });
      const openedBy = Date.now() + 5_000;
      while (!fs.existsSync(ready) && !fs.existsSync(result) && Date.now() < openedBy) {
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
      }
      assert.equal(fs.existsSync(ready), true);
    }
    const preWriterMarker = readReplacementLedgerMarker(ledgerPath);
    assert.ok(preWriterMarker, "the published active ledger is readable before the late write");
    fs.writeFileSync(go, "1");
    const until = Date.now() + 5_000;
    while (!fs.existsSync(result) && Date.now() < until) {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
    }
    assert.equal(fs.existsSync(result), true, "late writer returned a result");
    const writerResult = JSON.parse(fs.readFileSync(result, "utf8"));
    if (child && child.exitCode === null) {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("late_writer_exit_timeout")), 5_000);
        child!.once("exit", () => { clearTimeout(timer); resolve(); });
      });
    }
    let markerError: string | null = null;
    let switchedAt: string | null = null;
    try { switchedAt = readReplacementLedgerMarker(ledgerPath)?.switchedAt ?? null; }
    catch (error) { markerError = String(error); }
    let archiveBeforeFresh: unknown;
    let archiveError: string | null = null;
    try {
      const archived = new Database(archivePath, { readonly: true, fileMustExist: true });
      try {
        archiveBeforeFresh = {
          integrity: archived.pragma("integrity_check", { simple: true }),
          matchingRows: (archived.prepare("select count(*) as n from buffered_events where id=?")
            .get(gapId) as { n: number }).n,
        };
      } finally { archived.close(); }
    } catch (error) { archiveError = String(error); }
    console.log(JSON.stringify({ postOpenControl, openerSeenByLsof,
      preWriterMarkerReadable: Boolean(preWriterMarker), writerResult, markerError,
      archiveBeforeFresh, archiveError, switchedAt }));
    assert.equal(markerError, null, "new active ledger must remain readable after late old-handle write");
    const fresh = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    const rollout = new RolloutTailer(fresh, undefined, () => [], undefined, [codexCaptureRoot]);
    const tailer = new TranscriptTailer(fresh, undefined, undefined, [captureRoot]);
    try {
      for (let pass = 0; pass < 30 && captureBaselineStatus(fresh.database).status !== "complete"; pass++) {
        await rollout.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
      for (let pass = 0; pass < 12; pass++) {
        await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      }
      const archived = new Database(archivePath, { readonly: true, fileMustExist: true });
      const archiveCount = (archived.prepare("select count(*) as n from buffered_events where id=?")
        .get(gapId) as { n: number }).n;
      archived.close();
      const freshCount = (fresh.database.prepare("select count(*) as n from buffered_events where id=?")
        .get(gapId) as { n: number }).n;
      console.log(JSON.stringify({ openerSeenByLsof, writerResult, archiveCount,
        freshCount, switchedAt, gapId }));
      assert.equal(archiveCount + freshCount, 1, "gap record must occur exactly once across ledgers");
    } finally { tailer.close(); rollout.close(); fresh.close(); }
  } finally {
    fs.renameSync = originalRename;
    if (child && child.exitCode === null) child.kill("SIGTERM");
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
