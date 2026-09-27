import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

import {
  preflightMaintenanceRebuild,
  rebuildLedger,
  renameBackBeforeResume,
  recoverInterruptedRebuild,
  REQUIRED_REBUILD_WRITERS,
  readActiveRebuildWriterLeases,
  acquireRebuildOpenToken,
  releaseRebuildOpenToken,
  readMaintenanceRebuildHeadroomStatus,
  observeRebuildConnectionOwnership, connectionOwnershipClosed,
} from "../packages/collector-cli/src/maintenance-rebuild";

const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-rebuild-proof-")));
const ledger = path.join(root, "ledger.sqlite");

function fixture(file = ledger) {
  const db = new Database(file);
  db.pragma("journal_mode = WAL");
  db.exec(`create table maintenance_state(key text primary key,value text not null,updated_at text not null);
    create table buffered_events(id text primary key,payload_json text not null);
    create table upload_outbox(delivery_id text primary key,state text not null);
    create table dashboard_snapshots(days integer primary key,payload_json text not null);
    create table finance_publication_control(singleton integer primary key,revision integer not null);
    insert into buffered_events values ('e1','{"a":1}');
    insert into upload_outbox values ('d1','pending');
    insert into finance_publication_control values (1,7);
    insert into dashboard_snapshots values (30,'{"n":1}'),(90,'{"n":1}'),(182,'{"n":1}'),(365,'{"n":1}'),(1825,'{"n":1}');`);
  db.close();
}

async function main() {
  const cliLedger = path.join(root, "cli.sqlite");
  fixture(cliLedger);
  const cli = spawnSync(process.execPath, ["--import", "tsx", "packages/collector-cli/src/cli.ts",
    "maintenance", "rebuild", "--ledger", cliLedger, "--copy-drill", "--copy-root", root,
    "--stage", "S10", "--wal-high-water-bytes", "0"],
  { cwd: process.cwd(), env: { ...process.env, HOME: root, USERPROFILE: root,
    PLIMSOLL_HOME: path.join(root, "home"), CODEX_HOME: path.join(root, ".codex"),
    CLAUDE_CONFIG_DIR: path.join(root, ".claude"), TMPDIR: root }, encoding: "utf8", timeout: 60_000 });
  assert.equal(cli.status, 0, cli.stderr || cli.stdout);
  assert.match(cli.stdout, /"status":"rebuilt"/);
  console.log(JSON.stringify({ check: "cli_copy_rebuild", exit: cli.status }));
  const leasedLedger = path.join(root, "leases.sqlite");
  const writer = new LocalEventBuffer(leasedLedger);
  assert.deepEqual(readActiveRebuildWriterLeases(writer.database),
    [{ pid: process.pid, owner: "local_event_buffer" }]);
  const observedWriter = observeRebuildConnectionOwnership(leasedLedger);
  assert.equal(observedWriter.writerLeases.length, 1);
  assert.equal(observedWriter.openTokens.length, 1);
  writer.close();
  const leaseRead = new Database(leasedLedger, { readonly: true });
  assert.equal(readActiveRebuildWriterLeases(leaseRead).length, 0);
  leaseRead.close();
  console.log(JSON.stringify({ check: "observed_connection_owner", observedWriter }));
  const quiesce = async (file: string) => {
    const before = observeRebuildConnectionOwnership(file);
    const after = observeRebuildConnectionOwnership(file);
    return { before, after, connectionsClosed: connectionOwnershipClosed(after) };
  };
  fixture();
  const base = { ledgerPath: ledger, stage: "S10" as const, walHighWaterBytes: 0, copyDrill: true };
  const delayedWriterToken = acquireRebuildOpenToken(ledger);
  try {
    await assert.rejects(() => rebuildLedger({ ...base,
      quiesce: () => quiesce(ledger),
      resume: async () => undefined,
    }), /writer_not_quiesced/);
  } finally { releaseRebuildOpenToken(delayedWriterToken); }
  const gate = `${ledger}.maintenance-rebuild.lock`;
  fs.writeFileSync(gate, "fixture\n");
  try { assert.throws(() => acquireRebuildOpenToken(ledger), /maintenance_rebuild_paused/); }
  finally { fs.unlinkSync(gate); }
  console.log(JSON.stringify({ check: "preopen_token_and_writer_gate", delayedOpenRefused: true }));
  const sampled = path.join(root, "sampled-wal.sqlite");
  fixture(sampled);
  const sampleDb = new Database(sampled);
  sampleDb.exec("create table budget_samples(at_ms integer not null,sample_json text not null)");
  sampleDb.prepare("insert into budget_samples values (?,?)")
    .run(Date.now(), JSON.stringify({ walBytes: 12345 }));
  sampleDb.close();
  const sampledPreflight = preflightMaintenanceRebuild({ ...base, ledgerPath: sampled });
  assert.equal(sampledPreflight.sampledWalHighWaterBytes, 12345);
  assert.equal(sampledPreflight.walHighWaterBytes, 12345);
  const noSample = path.join(root, "no-sample.sqlite");
  fixture(noSample);
  const stagedDb = new Database(noSample);
  stagedDb.prepare("insert into maintenance_state values (?,?,?)")
    .run("lean_rebuild_stage", "S10", new Date().toISOString());
  stagedDb.close();
  assert.throws(() => preflightMaintenanceRebuild({ ...base, ledgerPath: noSample,
    copyDrill: false }), /wal_high_water_unavailable/);
  console.log(JSON.stringify({ check: "measured_wal_high_water", sampledBytes: 12345,
    missingSampleRefused: true }));
  assert.throws(() => preflightMaintenanceRebuild({ ...base, freeBytes: 1 }), /insufficient_headroom/);
  const shortage = readMaintenanceRebuildHeadroomStatus(ledger);
  assert.equal(shortage?.shortfallBytes, shortage!.requiredFreeBytes - 1);
  assert.match(shortage!.message, /reclaimable; needs .* GB free/);
  assert.throws(() => preflightMaintenanceRebuild({ ...base, copyDrill: false }), /stage_not_ready/);
  preflightMaintenanceRebuild(base);
  assert.equal(readMaintenanceRebuildHeadroomStatus(ledger), null);
  const before = fs.readFileSync(ledger);
  let resumed = 0;
  await assert.rejects(() => rebuildLedger({ ...base,
    quiesce: async () => ({ ...await quiesce(ledger), connectionsClosed: false }),
    resume: async () => { resumed += 1; },
  }), /writer_not_quiesced/);
  assert.deepEqual(fs.readFileSync(ledger), before);
  const result = await rebuildLedger({ ...base,
    quiesce: () => quiesce(ledger),
    resume: async () => {
      assert.equal(fs.existsSync(`${ledger}.maintenance-rebuild.lock`), false);
      resumed += 1;
    },
  });
  assert.equal(result.status, "rebuilt");
  assert.ok(result.pauseMs >= 0);
  assert.equal(result.outboxPending, 1);
  assert.equal(resumed, 2);
  assert.ok(fs.existsSync(result.backupPath));
  const db = new Database(ledger, { readonly: true });
  assert.equal((db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, 1);
  db.close();
  assert.throws(() => renameBackBeforeResume(ledger), /forward_repair_only/);
  console.log(JSON.stringify({ check: "rebuild_verify_swap", pauseMs: result.pauseMs,
    beforeBytes: before.length, afterBytes: fs.statSync(ledger).size, backupPath: result.backupPath }));

  const failing = path.join(root, "reopen.sqlite");
  fixture(failing);
  const oldHash = fs.readFileSync(failing);
  let resumeAfterFailure = 0;
  await assert.rejects(() => rebuildLedger({ ...base, ledgerPath: failing,
    quiesce: () => quiesce(failing),
    reopen: () => { throw new Error("forced_reopen_failure"); },
    resume: async () => { resumeAfterFailure += 1; },
  }), /forced_reopen_failure/);
  assert.equal(resumeAfterFailure, 1);
  assert.deepEqual(fs.readFileSync(failing), oldHash);
  console.log(JSON.stringify({ check: "rename_back_before_resume", restored: true }));

  const manual = path.join(root, "manual-rename.sqlite");
  fixture(manual);
  const manualBefore = fs.readFileSync(manual);
  await assert.rejects(() => rebuildLedger({ ...base, ledgerPath: manual,
    quiesce: () => quiesce(manual),
    reopen: (file) => { renameBackBeforeResume(file); throw new Error("manual_rename_back_complete"); },
    resume: async () => undefined,
  }), /manual_rename_back_complete/);
  assert.deepEqual(fs.readFileSync(manual), manualBefore);
  assert.equal(fs.existsSync(`${manual}.maintenance-rebuild.lock`), false);
  console.log(JSON.stringify({ check: "manual_rename_back_before_resume", restored: true }));

  const busy = path.join(root, "busy-wal.sqlite");
  fixture(busy);
  let resumedAfterBusy = false;
  await assert.rejects(() => rebuildLedger({ ...base, ledgerPath: busy,
    quiesce: () => quiesce(busy),
    checkpoint: () => [{ busy: 1, log: 1, checkpointed: 0 }],
    resume: async () => { resumedAfterBusy = true; },
  }), /wal_not_empty_after_checkpoint/);
  assert.equal(resumedAfterBusy, true);
  assert.equal(fs.existsSync(`${busy}.rebuild`), false);
  console.log(JSON.stringify({ check: "busy_checkpoint_refused", injectedBusy: 1 }));

  const interrupted = path.join(root, "interrupted.sqlite");
  fixture(interrupted);
  await assert.rejects(() => rebuildLedger({ ...base, ledgerPath: interrupted,
    quiesce: () => quiesce(interrupted),
    resume: async () => undefined,
    afterVacuum: () => { throw new Error("forced_vacuum_interrupt"); },
  }), /forced_vacuum_interrupt/);
  const recovered = recoverInterruptedRebuild(interrupted);
  assert.equal(recovered.status, "recovered_untouched_source");
  console.log(JSON.stringify({ check: "interrupted_vacuum_recovery", recovered }));
}

main().finally(() => fs.rmSync(root, { recursive: true, force: true })).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
