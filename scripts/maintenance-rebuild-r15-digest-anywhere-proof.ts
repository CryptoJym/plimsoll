/** Immutable full-body evidence is sufficient even before a refused retry. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r15-digest-anywhere-")));
const ledger = path.join(home, "ledger.sqlite");
const buffer = new LocalEventBuffer(ledger);
try {
  const body = { id: randomUUID(), session_id: randomUUID(),
    hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 11 };
  appendForwardedHook(body, { config: collectorConfigSchema.parse({}), source: "claude_code", buffer });
  const earlier = buffer.database.prepare("select rowid from buffered_events where id = ?")
    .get(body.id) as { rowid: number };
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  finishMaintenanceRebuildPause(home);
  const receiptFile = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as
    { ledgerHighWater: number; ledgerAdmissionSequence: number };
  assert.equal(receipt.ledgerHighWater, earlier.rowid);
  const state = reconcileMaintenanceRebuildRefusals(home, ledger);
  console.log(JSON.stringify({ check: "digest_matches_prior_row", rowid: earlier.rowid,
    refusalRowid: receipt.ledgerHighWater, sequence: receipt.ledgerAdmissionSequence, state }));
  assert.equal(state.count, 0, "an exact immutable digest settles regardless of row order");
  assert.equal(state.unverifiedHookRetries, 0);
  assert.equal(fs.existsSync(receiptFile), false);
} finally {
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
