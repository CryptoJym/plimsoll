import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { restoreArchivedLedger, readReplacementLedgerMarker,
  switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicLearningFactId } from "../packages/collector-cli/src/learning-facts";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const points = ["fold_commit", "stage_checkpoint", "journal_delete", "stage_chmod",
  "fresh_link", "fresh_fsync", "active_rename", "active_fsync"] as const;
const hash = (file: string) => crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");

if (process.argv[2] === "--reclone-child") {
  const fixture = process.argv[3]!;
  const ledgerPath = path.join(fixture, "work-ledger.sqlite");
  const stage = `${ledgerPath}.restore-stage`;
  const actualNow = Date.now;
  Date.now = () => actualNow() + 70_000;
  const originalFsync = fs.fsyncSync;
  fs.fsyncSync = ((fd: number) => {
    originalFsync(fd);
    if (fs.existsSync(stage) && !fs.existsSync(`${stage}.identity.json`) &&
        fs.fstatSync(fd).isFile() && fs.fstatSync(fd).ino === fs.statSync(stage).ino) {
      if (process.env.R6_RECLONE_FAILURE === "ENOSPC") {
        fs.truncateSync(stage, 4096);
        throw Object.assign(new Error("simulated_reclone_disk_full"), {code: "ENOSPC"});
      }
      process.kill(process.pid, "SIGKILL");
    }
  }) as typeof fs.fsyncSync;
  restoreArchivedLedger({ ledgerPath,
    archivePath: path.join(fixture, "archive", "old-ledger.sqlite"),
    freshAttemptPath: path.join(fixture, "archive", "fresh-attempt.sqlite"),
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  process.exit(90);
} else if (process.argv[2] === "--child") {
  const fixture = process.argv[3]!, point = process.argv[4]!;
  const ledgerPath = path.join(fixture, "work-ledger.sqlite");
  const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
  const freshAttemptPath = path.join(fixture, "archive", "fresh-attempt.sqlite");
  const stage = `${ledgerPath}.restore-stage`;
  const fail = (name: string) => { if (point === name) process.kill(process.pid, "SIGKILL"); };
  const originalExec = Database.prototype.exec;
  (Database.prototype as any).exec = function (sql: string) {
    const result = originalExec.call(this, sql);
    if (sql === "COMMIT") fail("fold_commit");
    return result;
  };
  const originalPragma = Database.prototype.pragma;
  let checkpoints = 0;
  (Database.prototype as any).pragma = function (sql: string, options?: unknown) {
    const result = originalPragma.call(this, sql, options as any);
    if (sql === "wal_checkpoint(TRUNCATE)" && ++checkpoints === 2) fail("stage_checkpoint");
    if (sql === "journal_mode = DELETE") fail("journal_delete");
    return result;
  };
  const originalChmod = fs.chmodSync;
  fs.chmodSync = ((file: fs.PathLike, mode: fs.Mode) => {
    originalChmod(file, mode);
    if (String(file) === stage) fail("stage_chmod");
  }) as typeof fs.chmodSync;
  const originalLink = fs.linkSync;
  fs.linkSync = ((source: fs.PathLike, destination: fs.PathLike) => {
    originalLink(source, destination);
    if (String(destination) === freshAttemptPath) fail("fresh_link");
  }) as typeof fs.linkSync;
  const originalFsync = fs.fsyncSync;
  let directoryFsyncs = 0;
  fs.fsyncSync = ((fd: number) => {
    originalFsync(fd);
    if (fs.existsSync(freshAttemptPath)) {
      directoryFsyncs++;
      if (directoryFsyncs === 1) fail("fresh_fsync");
      if (directoryFsyncs === 2) fail("active_fsync");
    }
  }) as typeof fs.fsyncSync;
  const originalRename = fs.renameSync;
  fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
    originalRename(source, destination);
    if (String(source) === stage && String(destination) === ledgerPath) fail("active_rename");
  }) as typeof fs.renameSync;
  restoreArchivedLedger({ ledgerPath, archivePath, freshAttemptPath,
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  process.exit(90);
} else {
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const results: Array<Record<string, unknown>> = [];
  const selected = process.argv[2] === "enospc" ? points.filter(point => point === "fold_commit") :
    process.argv[2] ? points.filter(point => point === process.argv[2]) :
      points.filter(point => point !== "active_rename" && point !== "active_fsync");
  assert.ok(selected.length, "unknown restore crash point");
  for (const point of selected) {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
      `r3-restore-${point}-`)));
    try {
      const ledgerPath = path.join(fixture, "work-ledger.sqlite");
      const stage = `${ledgerPath}.restore-stage`;
      const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
      const freshAttemptPath = path.join(fixture, "archive", "fresh-attempt.sqlite");
      const root = path.join(fixture, "codex");
      fs.mkdirSync(root);
      fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
      const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
        installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
          profileId: "profile", directory: root, installationEpochId: epoch }] });
      const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace,
        deviceId: device, freshCaptureRootEpoch: epoch });
      old.close();
      switchFreshLedger({ ledgerPath, archivePath, config,
        authorityRoot: path.join(fixture, "lifecycle-authority") });
      const replacement = new LocalEventBuffer(ledgerPath, { workspaceId: workspace,
        deviceId: device, freshCaptureRootEpoch: epoch });
      const at = new Date().toISOString();
      const operationId = deterministicLearningFactId(["r3-restore-crash", point]);
      replacement.database.prepare(`insert into buffered_events
        (id,source,session_id,event_type,data_mode,observed_at,payload_json,created_at,
         workspace_id,device_id,privacy_generation) values (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(`event-${point}`, "codex", `session-${point}`, "tool_use", "metadata", at,
          "{}", at, workspace, device, "review-fixture");
      replacement.learningFacts.recordToolSignal({ kind: "attempt", operationId, source: "codex",
        sessionId: `session-${point}`, toolClass: "compute", toolName: "shell", startedAt: at });
      replacement.database.prepare(`insert into tool_stat_attempt_dimensions
        (operation_id,event_id,workspace_id,device_id,runtime_version,collector_version)
        values (?,?,?,?,?,?)`).run(operationId, `event-${point}`, workspace, device, "unknown", "0.7.46");
      replacement.close();
      const archiveHash = hash(archivePath);
      const child = spawnSync(process.execPath,
        ["--import", loader, import.meta.filename, "--child", fixture, point],
        { encoding: "utf8", timeout: 120_000 });
      assert.equal(child.signal, "SIGKILL", `${point}: ${child.stderr}`);
      const marker = readReplacementLedgerMarker(ledgerPath);
      const freshExists = fs.existsSync(freshAttemptPath);
      const beforeArtifacts = fs.readdirSync(fixture).filter(name => name.includes("restore-stage") || name.endsWith("-wal") || name.endsWith("-shm") || name.endsWith("-journal"));
      const stageWalBefore = beforeArtifacts.includes(path.basename(ledgerPath) + ".restore-stage-wal");
      const stagedSidecars = ["-wal", "-shm", "-journal"].filter(suffix =>
        fs.existsSync(`${stage}${suffix}`)).map(suffix => ({ suffix,
          hash: hash(`${stage}${suffix}`) }));
      console.log(JSON.stringify({ point, beforeArtifacts: beforeArtifacts.map(name => ({ name, bytes: fs.statSync(path.join(fixture, name)).size, hash: hash(path.join(fixture, name)) })) }));
      if (point === "fold_commit" || point === "stage_checkpoint") {
        assert.equal(stageWalBefore, true, "fixture must leave a stage WAL after SIGKILL");
      }
      const active = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      const activeAttempts = (active.prepare("select count(*) as n from tool_attempt_facts")
        .get() as { n: number }).n;
      active.close();
      const afterRename = point === "active_rename" || point === "active_fsync";
      results.push({ point, replacementActive: marker !== null, freshExists,
        stageExists: fs.existsSync(`${ledgerPath}.restore-stage`), activeAttempts,
        archiveUnchanged: hash(archivePath) === archiveHash });
      console.log(JSON.stringify(results.at(-1)));
      assert.equal(marker === null, afterRename);
      assert.equal(activeAttempts, 1);
      assert.equal(hash(archivePath), archiveHash);
      assert.equal(freshExists, points.indexOf(point) >= points.indexOf("fresh_link"));
      const second = spawnSync(process.execPath,
        ["--import", loader, import.meta.filename, "--reclone-child", fixture],
        { encoding: "utf8", timeout: 120_000,
          env: {...process.env, R6_RECLONE_FAILURE: process.argv[2] === "enospc" ? "ENOSPC" : ""} });
      if (process.argv[2] === "enospc") {
        assert.notEqual(second.status, 0);
        assert.match(second.stderr, /simulated_reclone_disk_full/);
      } else assert.equal(second.signal, "SIGKILL", `${point}: second crash: ${second.stderr}`);
      assert.equal(readReplacementLedgerMarker(ledgerPath) !== null, true);
      const secondNow = Date.now;
      Date.now = () => secondNow() + 140_000;
      try {
        restoreArchivedLedger({ ledgerPath, archivePath, freshAttemptPath,
          authorityRoot: path.join(fixture, "lifecycle-authority") });
      } finally { Date.now = secondNow; }
      const restored = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      const folded = (restored.prepare("select count(*) as n from tool_attempt_facts").get() as {n:number}).n;
      restored.close();
      const suspectMain = fs.readdirSync(path.dirname(freshAttemptPath)).filter(name =>
        name.startsWith(`${path.basename(freshAttemptPath)}.restore-stage.suspect-`) &&
        !/(-wal|-shm|-journal|\.identity\.json)$/.test(name));
      assert.equal(readReplacementLedgerMarker(ledgerPath), null);
      assert.equal(folded, 1);
      assert.equal(hash(archivePath), archiveHash);
      assert.equal(fs.existsSync(stage), false);
      assert.ok(suspectMain.length >= 2, `${point}: both killed stages retained`);
      console.log(JSON.stringify({point, secondCrash: second.signal, folded,
        suspectCount: suspectMain.length, archiveUnchanged: true}));
      // The crashed owner's immutable lease has a 60-second deadline. Advance
      // only this fixture's clock past it; the recovery code still acquires a
      // new revision and performs every on-disk operation unchanged.
      const actualNow = Date.now;
      Date.now = () => actualNow() + 70_000;
      let recovered: ReturnType<typeof restoreArchivedLedger>;
      try {
        recovered = restoreArchivedLedger({ ledgerPath, archivePath, freshAttemptPath,
          authorityRoot: path.join(fixture, "lifecycle-authority") });
      } finally { Date.now = actualNow; }
      assert.equal(recovered.archivePreserved, true);
      assert.equal(readReplacementLedgerMarker(ledgerPath), null,
        `${point}: a rerun must activate the complete archived image`);
      const after = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      const afterAttempts = (after.prepare("select count(*) as n from tool_attempt_facts")
        .get() as { n: number }).n;
      after.close();
      assert.equal(afterAttempts, 1, `${point}: folded attempts must survive retry exactly once`);
      assert.equal(hash(archivePath), archiveHash, `${point}: archive must stay byte-identical`);
      assert.equal(fs.existsSync(freshAttemptPath), true,
        `${point}: replacement must be retained at the fresh-attempt path`);
      assert.equal(fs.existsSync(`${ledgerPath}.restore-stage`), false,
        `${point}: successful recovery must leave no restore stage`);
      const afterArtifacts = fs.readdirSync(fixture).filter(name => name.includes("restore-stage") || name.endsWith("-wal") || name.endsWith("-shm") || name.endsWith("-journal"));
      const archiveArtifacts = fs.readdirSync(path.dirname(archivePath)).filter(name => name.includes("restore-stage") || name.endsWith("-wal") || name.endsWith("-shm") || name.endsWith("-journal"));
      console.log(JSON.stringify({ point, afterArtifacts: afterArtifacts.map(name => ({ name, bytes: fs.statSync(path.join(fixture, name)).size, hash: hash(path.join(fixture, name)) })), archiveArtifacts: archiveArtifacts.map(name => ({ name, bytes: fs.statSync(path.join(path.dirname(archivePath), name)).size, hash: hash(path.join(path.dirname(archivePath), name)) })) }));
      for (const sidecar of stagedSidecars) {
        const match = archiveArtifacts.find(name =>
          name.startsWith(path.basename(freshAttemptPath) +
            `.restore-stage${sidecar.suffix}.recovered-`) &&
          hash(path.join(path.dirname(archivePath), name)) === sidecar.hash);
        assert.ok(match,
          `retry must retain the original stage ${sidecar.suffix} bytes in a recovered artifact`);
      }
      console.log(JSON.stringify({ recovered: point, afterAttempts,
        archiveUnchanged: true, freshAttemptPreserved: true }));
    } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
  }
  console.log(JSON.stringify({ results }));
}
