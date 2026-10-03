/** Independently exercise the exact downgrade retirement gates and counter. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DEFAULT_POLICY } from "../packages/shared/src/index";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

function fixture(name: string, spool: boolean, preexisting = false) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), `r7-${name}-`)));
  const ledger = path.join(home, "ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger);
  const body = { id: randomUUID(), session_id: randomUUID(),
    hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 7 };
  if (preexisting) buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id)
    values (?,'claude_code','user_prompt_submit','safe',?,?,?,?)`)
    .run(body.id, body.timestamp, JSON.stringify({ tenantId: DEFAULT_POLICY.tenantId }),
      body.timestamp, body.session_id);
  markMaintenanceRebuildPause(home);
  let spoolPath: string | null = null;
  if (spool) {
    const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body: JSON.stringify(body),
      cause: "maintenance_rebuild" });
    assert.equal(saved.ok, true);
    spoolPath = saved.path;
  }
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body),
    spoolPath ? { spoolName: path.basename(spoolPath) } : {});
  const receiptFile = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { at: string; ledgerHighWater: number };
  finishMaintenanceRebuildPause(home);
  const insert = (id: string, digest: string | null) => buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id,maintenance_hook_body_digest)
    values (?,'claude_code','user_prompt_submit','safe',?,?,?,?,?)`)
      .run(id, body.timestamp, JSON.stringify({ tenantId: DEFAULT_POLICY.tenantId }),
        body.timestamp, body.session_id, digest);
  const cleanup = () => { buffer.close(); fs.rmSync(home, { recursive: true, force: true }); };
  return { home, ledger, buffer, body, receipt, receiptFile, spoolPath, insert, cleanup };
}

{
  const f = fixture("prior-row-boundary", true, true);
  try {
    assert.equal(f.receipt.ledgerHighWater, 1);
    fs.unlinkSync(f.spoolPath!);
    const state = reconcileMaintenanceRebuildRefusals(f.home, f.ledger,
      Date.parse(f.receipt.at) + MISSING_HOOK_RETRY_MS + 1_000);
    console.log(JSON.stringify({ check: "prior_row_cannot_retire_unverified", state,
      highWater: f.receipt.ledgerHighWater }));
    assert.equal(state.count, 1);
    assert.equal(state.unverifiedHookRetries, 0);
  } finally { f.cleanup(); }
}

{
  const f = fixture("spool-exists", true);
  try {
    f.insert(f.body.id, null);
    const whileSpool = reconcileMaintenanceRebuildRefusals(f.home, f.ledger,
      Date.parse(f.receipt.at) + MISSING_HOOK_RETRY_MS + 1_000);
    assert.equal(whileSpool.count, 1);
    assert.equal(whileSpool.unverifiedHookRetries, 0);
    assert.ok(fs.existsSync(f.receiptFile));
    fs.unlinkSync(f.spoolPath!);
    const afterDrain = reconcileMaintenanceRebuildRefusals(f.home, f.ledger);
    assert.equal(afterDrain.count, 0);
    assert.equal(afterDrain.unverifiedHookRetries, 1);
    f.buffer.close();
    const restarted = new LocalEventBuffer(f.ledger);
    try {
      const state = captureSpoolState(f.home);
      assert.equal(state.unverifiedHookRetries, 1);
      assert.equal(state.maintenanceRebuildPending, false);
      console.log(JSON.stringify({ check: "spool_gate_counter_restart", whileSpool,
        afterDrain, counterAfterRestart: state.unverifiedHookRetries,
        pendingAfterRestart: state.maintenanceRebuildPending }));
    } finally { restarted.close(); }
  } finally { fs.rmSync(f.home, { recursive: true, force: true }); }
}

{
  const f = fixture("digested-candidate", true);
  try {
    f.insert(f.body.id, null);
    f.insert(f.body.id.toUpperCase(), "f".repeat(64));
    fs.unlinkSync(f.spoolPath!);
    const state = reconcileMaintenanceRebuildRefusals(f.home, f.ledger,
      Date.parse(f.receipt.at) + MISSING_HOOK_RETRY_MS + 1_000);
    console.log(JSON.stringify({ check: "digested_candidate_blocks_unverified", state }));
    assert.equal(state.count, 1);
    assert.equal(state.unverifiedHookRetries, 0);
    assert.deepEqual(state.lost, []);
  } finally { f.cleanup(); }
}

{
  const f = fixture("no-candidate", false);
  try {
    const at = Date.parse(f.receipt.at);
    const before = reconcileMaintenanceRebuildRefusals(f.home, f.ledger,
      at + MISSING_HOOK_RETRY_MS - 1);
    const after = reconcileMaintenanceRebuildRefusals(f.home, f.ledger,
      at + MISSING_HOOK_RETRY_MS + 1);
    console.log(JSON.stringify({ check: "lost_only_after_horizon_no_candidate", before, after }));
    assert.deepEqual(before.lost, []);
    assert.equal(after.lost.length, 1);
  } finally { f.cleanup(); }
}
