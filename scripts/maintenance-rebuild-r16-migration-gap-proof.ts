/** Reproduce an old writer landing between sequence-table and trigger DDL. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

async function main() {
  const oldRoot = path.resolve(process.env.PR424_0744_CHECKOUT ?? "../plimsoll-0744", "packages/collector-cli/src");
  if (process.argv[2] === "--old-writer") {
    const oldBufferModule = await import(pathToFileURL(path.join(oldRoot, "buffer.ts")).href);
    const oldForwarder = await import(pathToFileURL(path.join(oldRoot, "forwarder.ts")).href);
    const oldConfig = await import(pathToFileURL(path.join(oldRoot, "config.ts")).href);
    const old = new oldBufferModule.LocalEventBuffer(process.argv[3]!);
    try { oldForwarder.appendForwardedHook(JSON.parse(process.argv[4]!), {
      config: oldConfig.collectorConfigSchema.parse({}), source: "claude_code", buffer: old,
    }); }
    finally { old.close(); }
    return;
  }
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const fixtureRoot = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r8-migration-gap-")));
  const oldRepo = path.join(fixtureRoot, "collector-0744");
  let oldWorktreeAdded = false;
  try {
  // The proof must run from a clean clone with no sibling checkout.
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", oldRepo,
    "375f277b85f7d4ede7db77bf4359c371c0e8a4aa"], { cwd: repo });
  oldWorktreeAdded = true;
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(oldRepo, "node_modules"), "dir");
  const childEnv = { ...process.env, PR424_0744_CHECKOUT: oldRepo };
  const fixtureOldRoot = path.join(oldRepo, "packages/collector-cli/src");
  const oldBufferModule = await import(pathToFileURL(path.join(fixtureOldRoot, "buffer.ts")).href);
  const home = path.join(fixtureRoot, "home");
  fs.mkdirSync(home);
  const ledger = path.join(home, "ledger.sqlite");
  const old = new oldBufferModule.LocalEventBuffer(ledger);
  old.close();
  const body = JSON.stringify({ id: randomUUID(), session_id: randomUUID(),
    hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 9 });
  const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body,
    cause: "maintenance_rebuild" });
  assert.equal(saved.ok, true);
  const prototype = Database.prototype as typeof Database.prototype & { exec(sql: string): Database.Database };
  const original = prototype.exec;
  let injected = false;
  let insertTriggerPresentAtOldWrite = true;
  let tableVisibleBeforeTrigger = false;
  let atomicInstall = false;
  prototype.exec = function(sql: string) {
    const value = original.call(this, sql);
    if (!injected && sql.includes("create table if not exists maintenance_rebuild_event_order")) {
      injected = true;
      // The reviewer's original seam is an actual commit in the old code.
      // Under BEGIN IMMEDIATE it is uncommitted; inspect visibility from a
      // second connection, then let the initializer publish all three triggers.
      atomicInstall = (this as Database.Database).inTransaction;
      const observer = new Database(ledger, { readonly: true, fileMustExist: true });
      try {
        tableVisibleBeforeTrigger = !!observer.prepare(`select 1 from sqlite_master
          where type='table' and name='maintenance_rebuild_event_order'`).get();
      } finally { observer.close(); }
      if (!atomicInstall) {
        markMaintenanceRebuildPause(home);
        recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body,
          { spoolName: path.basename(saved.path) });
        insertTriggerPresentAtOldWrite = !!(this as Database.Database).prepare(`select 1 from sqlite_master
          where type='trigger' and name='trg_maintenance_rebuild_event_order_insert'`).get();
        execFileSync(process.execPath, [path.resolve("node_modules/tsx/dist/cli.mjs"), __filename,
          "--old-writer", ledger, body], { env: childEnv, stdio: "pipe", timeout: 60_000 });
        fs.unlinkSync(saved.path);
      }
    }
    return value;
  };
  let upgraded: LocalEventBuffer | null = null;
  try {
    upgraded = new LocalEventBuffer(ledger);
  } finally { prototype.exec = original; }
  try {
    assert.equal(injected, true);
    if (atomicInstall) {
      // A 503 cannot be published from inside the uncommitted schema. The
      // listener records its boundary only once initialization has committed.
      markMaintenanceRebuildPause(home);
      recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body,
        { spoolName: path.basename(saved.path) });
      insertTriggerPresentAtOldWrite = !!upgraded.database.prepare(`select 1 from sqlite_master
        where type='trigger' and name='trg_maintenance_rebuild_event_order_insert'`).get();
      execFileSync(process.execPath, [path.resolve("node_modules/tsx/dist/cli.mjs"), __filename,
        "--old-writer", ledger, body], { env: childEnv, stdio: "pipe", timeout: 60_000 });
      fs.unlinkSync(saved.path);
    }
    finishMaintenanceRebuildPause(home);
    const receiptDirectory = path.join(home, "maintenance-rebuild-refusals");
    const receiptPath = path.join(receiptDirectory, fs.readdirSync(receiptDirectory)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as
      { at: string; eventId: string; source: string; kind: string; sessionId: string;
        tenantId: string; ledgerAdmissionSequence: number };
    const row = upgraded.database.prepare(`select e.id, e.source, e.event_type as kind,
      e.session_id as sessionId, json_extract(e.payload_json,'$.tenantId') as tenantId,
      e.maintenance_hook_body_digest as digest,
      o.seq from buffered_events e left join maintenance_rebuild_event_order o on o.event_id=e.id
      where e.id=?`).get(receipt.eventId) as { id: string; source: string; kind: string;
        sessionId: string; tenantId: string; digest: string | null; seq: number | null };
    const state = reconcileMaintenanceRebuildRefusals(home, ledger,
      Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 365 * 24 * 60 * 60 * 1000);
    console.log(JSON.stringify({ check: "old_writer_between_order_table_and_insert_trigger",
      twoProcesses: true, insertedAfterRefusal: true, atomicInstall,
      tableVisibleBeforeTrigger, insertTriggerPresentAtOldWrite,
      ledgerAdmissionSequence: receipt.ledgerAdmissionSequence, row,
      fullIdentityMatches: row.id === receipt.eventId && row.source === receipt.source &&
        row.kind === receipt.kind && row.sessionId === receipt.sessionId &&
        row.tenantId === receipt.tenantId,
      state, receiptStillExists: fs.existsSync(receiptPath) }));
    assert.equal(receipt.ledgerAdmissionSequence, 0);
    assert.equal(atomicInstall, true, "order table and triggers need one writer transaction");
    assert.equal(tableVisibleBeforeTrigger, false, "the partial schema is invisible externally");
    assert.equal(insertTriggerPresentAtOldWrite, true);
    assert.equal(row.id, receipt.eventId);
    assert.equal(row.source, receipt.source);
    assert.equal(row.kind, receipt.kind);
    assert.equal(row.sessionId, receipt.sessionId);
    assert.equal(row.tenantId, receipt.tenantId);
    assert.equal(row.digest, null);
    assert.ok(row.seq !== null && row.seq > receipt.ledgerAdmissionSequence,
      "the old writer must receive a sequence after the refusal");
    assert.equal(state.count, 0, "a consumed old-binary retry must settle after its horizon");
    assert.equal(state.unverifiedHookRetries, 1);
  } finally {
    upgraded?.close();
  }
  } finally {
    try {
      if (oldWorktreeAdded) execFileSync("git", ["worktree", "remove", "--force", oldRepo], { cwd: repo });
    } finally { fs.rmSync(fixtureRoot, { recursive: true, force: true }); }
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
