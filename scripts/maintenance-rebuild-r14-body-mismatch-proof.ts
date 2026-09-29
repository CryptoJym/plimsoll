/** A changed hook with the same ID and timestamp is not the refused body. */
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

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r6-same-time-content-")));
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const refused = { id: randomUUID(), hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date().toISOString(),
    input_tokens: 20, prompt: "refused content" };
  const different = { ...refused, input_tokens: 1, prompt: "different content" };
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(refused));
  finishMaintenanceRebuildPause(home);
  const admitted = appendForwardedHook(different, {
    config: collectorConfigSchema.parse({}), source: "claude_code", buffer });
  const row = buffer.database.prepare("select input_tokens from buffered_events where id=?")
    .get(refused.id) as { input_tokens: number } | undefined;
  const state = reconcileMaintenanceRebuildRefusals(home);
  console.log(JSON.stringify({ check: "same_id_time_changed_content", admittedId: admitted.event.id,
    rowTokens: row?.input_tokens, refusedTokens: refused.input_tokens, state }));
  assert.equal(admitted.event.id, refused.id);
  assert.equal(row?.input_tokens, 1);
  assert.equal(state.count, 1, "different content must not clear the refused body");
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
