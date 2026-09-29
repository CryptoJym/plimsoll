/** The admission acknowledgement checks nested OTLP time claims before clamp. */
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

for (const exact of [true, false]) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r13-nested-time-")));
  let buffer: LocalEventBuffer | null = null;
  try {
    buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
    const firstTime = "2026-09-27T00:00:00.000Z";
    const original = { id: randomUUID(), hook_event_name: "UserPromptSubmit",
      session_id: randomUUID(), otel: { attributes: [
        { key: "timestamp", value: { stringValue: firstTime } },
      ] } };
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(original));
    const file = path.join(home, "maintenance-rebuild-refusals",
      fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
    const receipt = JSON.parse(fs.readFileSync(file, "utf8")) as { receiptId: string;
      originalTimestampDigest: string };
    assert.match(receipt.originalTimestampDigest, /^[a-f0-9]{64}$/);
    finishMaintenanceRebuildPause(home);
    const retry = exact ? original : { ...original,
      otel: { attributes: [{ key: "timestamp", value: { stringValue: "2026-09-27T00:00:01.000Z" } }] } };
    appendForwardedHook(retry, { config: collectorConfigSchema.parse({}),
      source: "claude_code", buffer });
    const admission = buffer.database.prepare(`select outcome from maintenance_rebuild_hook_admissions
      where receipt_id = ?`).get(receipt.receiptId) as { outcome: string } | undefined;
    const state = reconcileMaintenanceRebuildRefusals(home);
    assert.equal(admission?.outcome, exact ? "accepted" : "mismatch");
    assert.equal(state.count, exact ? 0 : 1);
    console.log(JSON.stringify({ check: "nested_original_timestamp", exact,
      outcome: admission?.outcome, pending: state.count }));
  } finally {
    buffer?.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}
