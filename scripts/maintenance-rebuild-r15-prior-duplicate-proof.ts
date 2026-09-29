/** A refused duplicate retry of an already captured identical hook must settle. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r7-prior-exact-")));
const ledger = path.join(home, "ledger.sqlite");
const buffer = new LocalEventBuffer(ledger);
try {
  const body = { id: randomUUID(), session_id: randomUUID(),
    hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 11 };
  const config = collectorConfigSchema.parse({});
  const first = appendForwardedHook(body, { config, source: "claude_code", buffer });
  assert.equal(first.deduplicated, undefined);
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body: JSON.stringify(body),
    cause: "maintenance_rebuild" });
  assert.equal(saved.ok, true);
  const receiptFile = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as
    { at: string; ledgerHighWater: number; receiptId: string };
  finishMaintenanceRebuildPause(home);
  const replay = appendForwardedHook(body, { config, source: "claude_code", buffer });
  const acknowledgement = buffer.database.prepare(`select count(*) as n
    from maintenance_rebuild_hook_admissions where receipt_id = ?`)
    .get(receipt.receiptId) as { n: number };
  assert.equal(acknowledgement.n, 1,
    "deduplicated retry acknowledges the exact stored body in its transaction");
  fs.unlinkSync(saved.path);
  const state = reconcileMaintenanceRebuildRefusals(home, ledger,
    Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 1_000);
  const capture = captureSpoolState(home);
  console.log(JSON.stringify({ check: "prior_identical_hook_deduplicated_retry",
    highWater: receipt.ledgerHighWater, deduplicated: replay.deduplicated,
    acknowledgementRows: acknowledgement.n,
    spoolExists: fs.existsSync(saved.path), state,
    maintenanceRebuildPending: capture.maintenanceRebuildPending }));
  assert.equal(replay.deduplicated, true);
  assert.equal(state.count, 0, "an identical retry captured before pause cannot block attestation forever");
} finally {
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
