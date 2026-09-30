import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
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

// A process can open the old pathname after the lsof preflight while the
// exclusive transaction is held. Keep its SQLite handle through the rename,
// then let it write after the switch releases the old transaction.
const childSource = String.raw`
const fs = require('node:fs');
const [moduleUrl, ledger, ready, go, result, id, workspace, device, epoch] = process.argv.slice(1);
fs.writeFileSync(ready, String(process.pid));
(async () => {
  let buffer;
  try {
    const { LocalEventBuffer } = await import(moduleUrl);
    buffer = new LocalEventBuffer(ledger, {workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, databaseBusyTimeoutMs: 5000});
    const timer = setInterval(() => {
      if (!fs.existsSync(go)) return;
      clearInterval(timer);
      try {
        const now = new Date().toISOString();
        const wrote = buffer.append({id,tenantId:workspace,source:'claude_code',
          dataMode:'metadata',eventType:'assistant_response',observedAt:now,
          sessionId:'71000000-0000-4000-8000-000000000007',inputTokens:1,outputTokens:0,
          metadata:{installationEpochId:epoch,sourceEventId:'late-collector'}});
        fs.writeFileSync(result, JSON.stringify({opened:true,wrote}));
      } catch (error) {
        fs.writeFileSync(result, JSON.stringify({opened:true,wrote:false,
          code:error.code,message:error.message}));
      } finally { buffer.close(); }
    }, 5);
  } catch (error) {
    if (buffer) buffer.close();
    fs.writeFileSync(result, JSON.stringify({opened:false,code:error.code,message:error.message}));
  }
})();`;

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
    let openerSeenByLsof = false;
    let gapCreated = false;
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, line("forward-gap", new Date(Date.now() + 3_600_000)));
        gapCreated = true;
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    switchFreshLedger({ ledgerPath, archivePath, config,
      authorityRoot: path.join(fixture, "lifecycle-authority"),
      onStep(step) {
        if (step !== "old_locked" || postOpenControl) return;
        child = spawn(process.execPath, ["--import", path.resolve("node_modules/tsx/dist/loader.mjs"),
          "-e", childSource, new URL("../packages/collector-cli/src/buffer.ts", import.meta.url).href,
          ledgerPath, ready, go, result, gapId, workspace, device, epoch],
          { cwd: process.cwd(), stdio: "ignore" });
        const until = Date.now() + 5_000;
        while (!fs.existsSync(ready) && !fs.existsSync(result) && Date.now() < until) {
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
        }
        assert.equal(fs.existsSync(ready), true, "second process must open old pathname during lock");
        const openUntil = Date.now() + 5_000;
        while (!openerSeenByLsof && !fs.existsSync(result) && Date.now() < openUntil) {
          const lsof = spawnSync("/usr/sbin/lsof", ["-t", "-w", "--", ledgerPath],
            { encoding: "utf8", timeout: 10_000 });
          openerSeenByLsof = lsof.stdout.split("\n").includes(String(child.pid));
        }
        const refusal = fs.existsSync(result) ? JSON.parse(fs.readFileSync(result, "utf8")) : null;
        assert.ok(openerSeenByLsof || (refusal?.opened === false && refusal?.message === "ledger switch in progress"),
          "collector must either be observed on the old inode or refuse before opening it");
      } });
    fs.renameSync = originalRename;
    assert.equal(gapCreated, true);
    if (postOpenControl) {
      child = spawn(process.execPath, ["--import", path.resolve("node_modules/tsx/dist/loader.mjs"),
        "-e", childSource, new URL("../packages/collector-cli/src/buffer.ts", import.meta.url).href,
        ledgerPath, ready, go, result, gapId, workspace, device, epoch],
        { cwd: process.cwd(), stdio: "ignore" });
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
    let activeState: unknown;
    let activeError: string | null = null;
    try {
      const active = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      try {
        activeState = {
          integrity: active.pragma("integrity_check", { simple: true }),
          markerTables: (active.prepare(`select count(*) as n from sqlite_master
            where name='collector_replacement_ledger'`).get() as { n: number }).n,
          matchingRows: (active.prepare("select count(*) as n from buffered_events where id=?")
            .get(gapId) as { n: number }).n,
        };
      } finally { active.close(); }
    } catch (error) { activeError = String(error); }
    console.log(JSON.stringify({ postOpenControl, openerSeenByLsof,
      preWriterMarkerReadable: Boolean(preWriterMarker), writerResult, markerError,
      archiveBeforeFresh, archiveError, activeState, activeError, switchedAt }));
    assert.equal(markerError, null, "new active ledger must remain readable after late old-handle write");
    assert.ok(switchedAt, "published replacement marker must remain after late collector write");
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
