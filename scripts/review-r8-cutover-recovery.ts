import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, restoreArchivedLedger,
  switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { LifecycleMutationAuthority } from "../packages/collector-cli/src/lifecycle-authority";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const points = ["old_locked", "stage_bound", "archive_linked", "after_rename", "after_fsync", "switched"];

if (process.argv[2] === "--child") {
  const fixture = process.argv[3]!, point = process.argv[4]!;
  const ledgerPath = path.join(fixture, "work-ledger.sqlite");
  const stage = `${ledgerPath}.replacement-stage`;
  const originalRename = fs.renameSync;
  const originalFsync = fs.fsyncSync;
  let renamed = false;
  fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
    const result = originalRename(source, destination);
    if (String(source) === stage && String(destination) === ledgerPath) {
      renamed = true;
      if (point === "after_rename") process.kill(process.pid, "SIGKILL");
    }
    return result;
  }) as typeof fs.renameSync;
  fs.fsyncSync = ((fd: number) => {
    const result = originalFsync(fd);
    if (renamed && point === "after_fsync" && fs.fstatSync(fd).isDirectory()) {
      process.kill(process.pid, "SIGKILL");
    }
    return result;
  }) as typeof fs.fsyncSync;
  const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(path.join(fixture, "config.json"), "utf8")));
  switchFreshLedger({ ledgerPath, archivePath: path.join(fixture, "archive", "old-ledger.sqlite"), config,
    authorityRoot: path.join(fixture, "lifecycle-authority"),
    onStep: step => { if (step === point) process.kill(process.pid, "SIGKILL"); } });
  process.exit(90);
} else {
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const results: Array<Record<string, unknown>> = [];
  for (const point of points) {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
      `pr426-r7-crash-${point}-`)));
    try {
      const ledgerPath = path.join(fixture, "work-ledger.sqlite");
      const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
      const authorityRoot = path.join(fixture, "lifecycle-authority");
      const root = path.join(fixture, "codex");
      fs.mkdirSync(root);
      fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
      const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
        installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
          profileId: "profile", directory: root, installationEpochId: epoch }] });
      fs.writeFileSync(path.join(fixture, "config.json"), JSON.stringify(config));
      const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
        freshCaptureRootEpoch: epoch });
      old.close();
      const oldInode = fs.statSync(ledgerPath).ino;
      const child = spawnSync(process.execPath,
        ["--import", loader, import.meta.filename, "--child", fixture, point],
        { encoding: "utf8", timeout: 120_000 });
      assert.equal(child.signal, "SIGKILL", `${point}: ${child.stderr}`);
      const oldActive = fs.statSync(ledgerPath).ino === oldInode;
      const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      const table = db.prepare("select 1 from sqlite_master where name='collector_replacement_ledger'").get();
      const row = table ? db.prepare(`select post_switch_fence_pending as pending,
        switched_at as switchedAt, rename_to_sample_delay_ms as delayMs
        from collector_replacement_ledger where singleton=1`).get() as
        { pending: number; switchedAt: string; delayMs: number | null } : null;
      db.close();
      let openError: string | null = null;
      let opened: LocalEventBuffer | null = null;
      try { opened = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
        freshCaptureRootEpoch: epoch }); }
      catch (error) { openError = error instanceof Error ? error.message : String(error); }
      finally { opened?.close(); }
      const marker = row?.pending === 0 ? readReplacementLedgerMarker(ledgerPath) : null;
      const beforeRecovery = { point, oldActive, pending: row?.pending ?? null, openError,
        durableCutoverAt: marker?.switchedAt ?? null, delayMs: marker?.renameToSampleDelayMs ?? null };
      if (["old_locked", "stage_bound", "archive_linked"].includes(point)) {
        assert.equal(oldActive, true);
        assert.equal(row, null);
        assert.equal(openError, null);
      } else if (["after_rename", "after_fsync"].includes(point)) {
        assert.equal(oldActive, false);
        assert.equal(row?.pending, 1);
        assert.equal(openError, "replacement_post_switch_fence_pending");
      } else {
        assert.equal(oldActive, false);
        assert.equal(row?.pending, 0);
        assert.equal(openError, null);
        assert.ok(marker && Number.isFinite(Date.parse(marker.switchedAt)));
        assert.ok(marker.renameToSampleDelayMs !== null && marker.renameToSampleDelayMs >= 0);
      }
      let recovered: string;
      if (point === "switched") {
        recovered = "published";
      } else {
        const authority = new LifecycleMutationAuthority(authorityRoot).observe();
        assert.equal(authority.kind, "held");
        const actualNow = Date.now;
        try {
          Date.now = () => authority.kind === "held" ? authority.expiresAtMs + 1 : actualNow();
          if (oldActive) {
            switchFreshLedger({ ledgerPath, archivePath, config, authorityRoot });
            assert.ok(readReplacementLedgerMarker(ledgerPath));
            recovered = "switch_retry";
          } else {
            const attemptDir = path.join(fixture, "attempt");
            fs.mkdirSync(attemptDir, { mode: 0o700 });
            restoreArchivedLedger({ ledgerPath, archivePath,
              freshAttemptPath: path.join(attemptDir, "fresh.sqlite"), authorityRoot });
            assert.equal(readReplacementLedgerMarker(ledgerPath), null);
            const restored = new LocalEventBuffer(ledgerPath, { workspaceId: workspace,
              deviceId: device, freshCaptureRootEpoch: epoch });
            restored.close();
            recovered = "archive_restore";
          }
        } finally { Date.now = actualNow; }
      }
      results.push({ ...beforeRecovery, recovered });
    } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
  }
  console.log(JSON.stringify({ results }));
}
