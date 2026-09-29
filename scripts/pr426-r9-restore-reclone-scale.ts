import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { restoreArchivedLedger, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const mib = Number(process.argv[2]);
assert.ok(Number.isInteger(mib) && mib >= 64 && mib <= 4096 && mib % 64 === 0);
const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  `r5-restore-scale-${mib}-`)));
const ledger = path.join(fixture, "work-ledger.sqlite");
const stage = `${ledger}.restore-stage`;
const archive = path.join(fixture, "archive", "old-ledger.sqlite");
const fresh = path.join(fixture, "archive", "fresh-attempt.sqlite");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const free = () => {
  const row = fs.statfsSync(fixture);
  return row.bavail * row.bsize;
};
const gib = 1024 ** 3;
let buffer: LocalEventBuffer | undefined;
let originalFsync: typeof fs.fsyncSync | undefined;
try {
  const freeStart = free();
  assert.ok(freeStart > 100 * gib + 3 * mib * 1024 ** 2,
    "fixture or clone could cross 100 GiB free floor");
  fs.mkdirSync(path.join(fixture, "codex"));
  fs.mkdirSync(path.dirname(archive), { mode: 0o700 });
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
      profileId: "profile", directory: path.join(fixture, "codex"),
      installationEpochId: epoch }] });
  buffer = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  buffer.database.pragma("journal_mode = DELETE");
  buffer.database.exec("create table review_padding(id integer primary key, bytes blob not null)");
  const insert = buffer.database.prepare("insert into review_padding(bytes) values(zeroblob(?))");
  buffer.database.transaction(() => {
    for (let i = 0; i < mib / 64; i++) insert.run(64 * 1024 ** 2);
  })();
  buffer.close(); buffer = undefined;
  const archiveSize = fs.statSync(ledger).size;
  const freeBeforeSwitch = free();
  switchFreshLedger({ ledgerPath: ledger, archivePath: archive, config,
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  const archiveInode = fs.statSync(archive).ino;
  const freeBeforeFirstAttempt = free();
  assert.ok(freeBeforeFirstAttempt > 100 * gib + archiveSize,
    "restore clone could cross 100 GiB free floor");
  // A failure immediately after the first clone fsync leaves a durable stage
  // without a journal. The measured second invocation must retain it as
  // suspect and create a fresh APFS clone of the archive.
  originalFsync = fs.fsyncSync;
  let interrupted = false;
  fs.fsyncSync = ((fd: number) => {
    originalFsync!(fd);
    if (!interrupted && fs.existsSync(stage) &&
        fs.fstatSync(fd).ino === fs.statSync(stage).ino) {
      interrupted = true;
      throw new Error("simulated_after_clone_fsync");
    }
  }) as typeof fs.fsyncSync;
  let firstError: string | null = null;
  try { restoreArchivedLedger({ ledgerPath: ledger, archivePath: archive,
    freshAttemptPath: fresh, authorityRoot: path.join(fixture, "lifecycle-authority") }); }
  catch (error) { firstError = error instanceof Error ? error.message : String(error); }
  fs.fsyncSync = originalFsync; originalFsync = undefined;
  assert.equal(interrupted, true);
  assert.equal(firstError, "simulated_after_clone_fsync");
  assert.equal(fs.existsSync(stage), true);
  const freeBeforeRestore = free();
  const syncs: Array<{ kind: string; ms: number }> = [];
  originalFsync = fs.fsyncSync;
  fs.fsyncSync = ((fd: number) => {
    const stat = fs.fstatSync(fd);
    const kind = stat.isDirectory() ? "directory" : fs.existsSync(stage) &&
      stat.ino === fs.statSync(stage).ino ? "stage" : "other-file";
    const start = performance.now();
    originalFsync!(fd);
    syncs.push({ kind, ms: +(performance.now() - start).toFixed(3) });
  }) as typeof fs.fsyncSync;
  const start = performance.now();
  const result = restoreArchivedLedger({ ledgerPath: ledger, archivePath: archive,
    freshAttemptPath: fresh, authorityRoot: path.join(fixture, "lifecycle-authority") });
  const restoreMs = +(performance.now() - start).toFixed(3);
  fs.fsyncSync = originalFsync; originalFsync = undefined;
  const freeAfterRestore = free();
  const active = new Database(ledger, { readonly: true, fileMustExist: true });
  const row = active.prepare("select count(*) as n,sum(length(bytes)) as bytes from review_padding")
    .get() as { n: number; bytes: number };
  active.close();
  const measured = { mib, archiveSize, restoreMs,
    firstError, freeStart, freeBeforeSwitch, freeBeforeFirstAttempt,
    freeBeforeRestore, freeAfterRestore,
    restoreFreeDelta: freeBeforeRestore - freeAfterRestore,
    retainedSuspectCount: fs.readdirSync(path.dirname(fresh))
      .filter(name => name.includes(".restore-stage.suspect-")).length,
    stageFsyncMs: syncs.filter(row => row.kind === "stage").map(row => row.ms),
    allSyncs: syncs, archiveSameInode: fs.statSync(archive).ino === archiveInode,
    archivePreserved: result.archivePreserved, paddingRows: row.n,
    paddingBytes: row.bytes };
  console.log(JSON.stringify(measured));
  assert.equal(row.bytes, mib * 1024 ** 2);
  assert.ok(fs.statSync(ledger).size >= archiveSize);
  assert.equal(measured.archiveSameInode, true);
  assert.ok(measured.retainedSuspectCount >= 1);
  assert.ok(freeAfterRestore > 100 * gib);
} finally {
  if (originalFsync) fs.fsyncSync = originalFsync;
  buffer?.close();
  fs.rmSync(fixture, { recursive: true, force: true });
}
