/** A matching digestless row without an admission sequence cannot hold a claim forever. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { DEFAULT_POLICY } from "../packages/shared/src/index";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r16-unsequenced-")));
const ledger = path.join(home, "ledger.sqlite");
const body = { id: randomUUID(), session_id: randomUUID(), hook_event_name: "UserPromptSubmit",
  timestamp: new Date().toISOString(), input_tokens: 9 };
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(ledger);
  buffer.database.exec(`drop trigger trg_maintenance_rebuild_event_order_insert;
    drop trigger trg_maintenance_rebuild_event_order_delete;
    drop trigger trg_maintenance_rebuild_event_order_rekey;
    drop table maintenance_rebuild_event_order`);
  // Simulate an older schema's already committed row. Its identity matches,
  // but the missing sequence cannot prove it was admitted after refusal.
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id)
    values (?,'claude_code','user_prompt_submit','safe',?,?,?,?)`)
    .run(body.id, body.timestamp, JSON.stringify({ tenantId: DEFAULT_POLICY.tenantId }),
      body.timestamp, body.session_id);
  buffer.close(); buffer = null;
  const wire = JSON.stringify(body);
  const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
    cause: "maintenance_rebuild" });
  assert.equal(saved.ok, true);
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", wire,
    { spoolName: path.basename(saved.path) });
  finishMaintenanceRebuildPause(home);
  fs.unlinkSync(saved.path);
  const receipt = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const at = Date.parse((JSON.parse(fs.readFileSync(receipt, "utf8")) as { at: string }).at);
  const before = reconcileMaintenanceRebuildRefusals(home, ledger, at + MISSING_HOOK_RETRY_MS - 1);
  assert.equal(before.count, 1, "the receipt holds capture until the retry horizon");
  const after = reconcileMaintenanceRebuildRefusals(home, ledger, at + MISSING_HOOK_RETRY_MS + 1);
  const capture = captureSpoolState(home);
  const terminalPath = path.join(home, "maintenance-rebuild-terminal.jsonl");
  const journal = fs.existsSync(terminalPath) ? fs.readFileSync(terminalPath, "utf8") : "";
  console.log(JSON.stringify({ check: "unsequenced_legacy_candidate_is_visible_loss", before, after,
    capturePending: capture.maintenanceRebuildPending, captureLosses: capture.losses,
    receiptExists: fs.existsSync(receipt), terminalJournal: journal.trim() }));
  assert.equal(after.count, 0, "an unsequenced row must not hold capture forever");
  assert.equal(after.unverifiedHookRetries, 0, "an unsequenced row is not proven later");
  assert.equal(after.lost.length, 1, "the unknown retry is a durable visible loss");
  assert.equal(capture.maintenanceRebuildPending, false);
  assert.equal(capture.losses.length, 1);
  assert.equal(fs.existsSync(receipt), false);
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
