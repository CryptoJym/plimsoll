import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, restoreArchivedLedger,
  switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const hash = (file: string) => crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");

if (process.argv[2] === "--child") {
  const fixture = process.argv[3]!, point = process.argv[4]!;
  const ledger = path.join(fixture, "work-ledger.sqlite");
  const stage = `${ledger}.restore-stage`;
  const journal = `${stage}.identity.json`;
  if (point === "fold_commit") {
    const original = Database.prototype.exec;
    (Database.prototype as any).exec = function (sql: string) {
      const result = original.call(this, sql);
      if (sql === "COMMIT") process.kill(process.pid, "SIGKILL");
      return result;
    };
  } else {
    const original = fs.fsyncSync;
    fs.fsyncSync = ((fd: number) => {
      original(fd);
      const stat = fs.fstatSync(fd);
      if (point === "clone_fsync" && fs.existsSync(stage) && !fs.existsSync(journal) &&
          stat.isFile() && stat.ino === fs.statSync(stage).ino) {
        process.kill(process.pid, "SIGKILL");
      }
      if (point === "journal_dir_fsync" && fs.existsSync(journal) &&
          fs.statSync(journal).size > 0 && stat.isDirectory()) {
        process.kill(process.pid, "SIGKILL");
      }
    }) as typeof fs.fsyncSync;
  }
  restoreArchivedLedger({ ledgerPath: ledger,
    archivePath: path.join(fixture, "archive", "old-ledger.sqlite"),
    freshAttemptPath: path.join(fixture, "archive", "fresh-attempt.sqlite"),
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  process.exit(90);
} else {
  const variant = process.argv[2] ?? "content";
  assert.ok(["content", "nonce", "clone_fsync", "journal_dir_fsync", "owned_clone_no_journal", "hardlink",
    "symlink", "active_clone"].includes(variant));
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    `r5-restore-${variant}-`)));
  const ledger = path.join(fixture, "work-ledger.sqlite");
  const archive = path.join(fixture, "archive", "old-ledger.sqlite");
  const fresh = path.join(fixture, "archive", "fresh-attempt.sqlite");
  const stage = `${ledger}.restore-stage`;
  try {
    fs.mkdirSync(path.join(fixture, "codex"));
    fs.mkdirSync(path.dirname(archive), { mode: 0o700 });
    const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
      installKey: "fixture-install-key", captureRoots: [{ source: "codex",
        rootId: "root", profileId: "profile", directory: path.join(fixture, "codex"),
        installationEpochId: epoch }] });
    const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    old.database.prepare("insert into maintenance_state(key,value,updated_at) values(?,?,?)")
      .run("archive-only-sentinel", "must-survive", new Date().toISOString());
    old.close();
    switchFreshLedger({ ledgerPath: ledger, archivePath: archive, config,
      authorityRoot: path.join(fixture, "lifecycle-authority") });
    const archiveHash = hash(archive);
    if (variant === "hardlink") fs.linkSync(ledger, stage);
    else if (variant === "symlink") fs.symlinkSync(ledger, stage);
    else if (variant === "active_clone") fs.copyFileSync(ledger, stage);
    else if (variant === "owned_clone_no_journal") {
      const clone = spawnSync("/bin/cp", ["-c", archive, stage]);
      assert.equal(clone.status, 0);
    }
    else {
      const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
      const point = variant === "content" || variant === "nonce" ? "fold_commit" : variant;
      const child = spawnSync(process.execPath,
        ["--import", loader, import.meta.filename, "--child", fixture, point],
        { encoding: "utf8", timeout: 120_000 });
      assert.equal(child.signal, "SIGKILL", `${point}: ${child.stderr}`);
      assert.equal(fs.existsSync(stage), true);
      if (variant === "content" || variant === "nonce") {
        const forged = new Database(stage);
        if (variant === "content") {
          forged.prepare("delete from maintenance_state where key=?").run("archive-only-sentinel");
        } else forged.prepare("update collector_restore_stage set stage_nonce=? where singleton=1")
          .run("00000000-0000-4000-8000-000000000000");
        forged.close();
      }
    }
    const actualNow = Date.now;
    const stagedHash = ["content", "nonce", "clone_fsync", "journal_dir_fsync", "owned_clone_no_journal"]
      .includes(variant) ? hash(stage) : null;
    Date.now = () => actualNow() + 70_000;
    let restoreError: string | null = null;
    try { restoreArchivedLedger({ ledgerPath: ledger, archivePath: archive,
      freshAttemptPath: fresh, authorityRoot: path.join(fixture, "lifecycle-authority") }); }
    catch (error) { restoreError = error instanceof Error ? error.message : String(error); }
    finally { Date.now = actualNow; }
    const active = new Database(ledger, { readonly: true, fileMustExist: true });
    const activeSentinel = (active.prepare("select value from maintenance_state where key=?")
      .get("archive-only-sentinel") as { value: string } | undefined)?.value ?? null;
    active.close();
    const archived = new Database(archive, { readonly: true, fileMustExist: true });
    const archiveSentinel = (archived.prepare("select value from maintenance_state where key=?")
      .get("archive-only-sentinel") as { value: string } | undefined)?.value ?? null;
    archived.close();
    const journalPresent = fs.existsSync(`${stage}.identity.json`);
    const archiveUnchanged = hash(archive) === archiveHash;
    const suspects = fs.readdirSync(path.dirname(fresh))
      .filter(name => name.startsWith(`${path.basename(fresh)}.restore-stage.suspect-`) &&
        !/(-wal|-shm|-journal|\.identity\.json)$/.test(name));
    console.log(JSON.stringify({ variant, restoreError, stagePresent: fs.existsSync(stage),
      journalPresent, activeMarker: Boolean(readReplacementLedgerMarker(ledger)),
      activeSentinel, archiveSentinel, archiveUnchanged, suspectCount: suspects.length }));
    assert.equal(archiveUnchanged, true);
    if (["hardlink", "symlink", "active_clone"].includes(variant)) {
      assert.ok(restoreError, "a foreign stage must be refused");
      assert.equal(readReplacementLedgerMarker(ledger) !== null, true);
      assert.equal(fs.existsSync(stage), true);
    } else {
      assert.equal(restoreError, null, "a durable owned stage must recover");
      assert.equal(activeSentinel, "must-survive", "restored image must match archive content");
      assert.ok(suspects.length >= 1, "a prior attempt's stage must be retained as suspect");
      assert.ok(suspects.some(name => hash(path.join(path.dirname(fresh), name)) === stagedHash),
        "the suspect stage must retain the prior attempt's exact bytes");
    }
  } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
}
