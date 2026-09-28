/** A later different hook using the same UUID in another case is not this retry. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const upper = "B3F1C2D4-5E6A-4B7C-8D9E-0F1A2B3C4D5E";
const lower = upper.toLowerCase();

function scenario(exactRetry: boolean) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r4-mixed-id-")));
  const ledger = path.join(home, "ledger.sqlite");
  let buffer: LocalEventBuffer | null = null;
  try {
    buffer = new LocalEventBuffer(ledger);
    const config = collectorConfigSchema.parse({});
    const refused = { id: upper, hook_event_name: "UserPromptSubmit",
      session_id: "a3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e", timestamp: new Date().toISOString(),
      cwd: "/fixture", prompt: "first event" };
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(refused));
    finishMaintenanceRebuildPause(home);
    const accepted = exactRetry ? refused : { ...refused, id: lower,
      session_id: "c3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e", prompt: "different event" };
    appendForwardedHook(accepted, { config, source: "claude_code", buffer });
    const rows = buffer.database.prepare("select id,source,event_type,payload_json from buffered_events")
      .all() as Array<{ id: string; source: string; event_type: string; payload_json: string }>;
    const result = reconcileMaintenanceRebuildRefusals(home);
    console.log(JSON.stringify({ check: "mixed_case_other_event", exactRetry,
      ids: rows.map((row) => row.id), sessions: rows.map((row) => JSON.parse(row.payload_json).sessionId),
      receiptCount: result.count }));
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.id, exactRetry ? upper : lower);
    assert.equal(result.count, exactRetry ? 0 : 1,
      "a different later event with canonical UUID equality is not the refused hook");
  } finally {
    buffer?.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}
scenario(true);
scenario(false);
