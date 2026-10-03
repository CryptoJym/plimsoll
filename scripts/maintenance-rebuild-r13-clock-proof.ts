/** A successful retry after the host clock moves backward still settles its refusal. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r5-clock-digest-")));
let buffer: LocalEventBuffer | null = null;
try {
  const ledger = path.join(home, "ledger.sqlite");
  buffer = new LocalEventBuffer(ledger);
  const config = collectorConfigSchema.parse({});
  const receivedAt = Date.now();
  const body = { id: randomUUID(), hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date(receivedAt).toISOString(),
    cwd: "/fixture", prompt: "synthetic" };
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  const receiptPath = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as { receiptId: string };
  finishMaintenanceRebuildPause(home);
  // A clock adjustment makes the body timestamp future-dated at retry. The
  // normal admission path clamps it, but still commits this exact retry.
  const originalNow = Date.now;
  let admitted!: ReturnType<typeof appendForwardedHook>;
  try {
    Date.now = () => receivedAt - 3_600_000;
    admitted = appendForwardedHook(body, { config, source: "claude_code", buffer,
      now: () => receivedAt - 3_600_000 });
  } finally { Date.now = originalNow; }
  const row = buffer.database.prepare("select id,observed_at from buffered_events where id=?")
    .get(body.id) as { id: string; observed_at: string };
  const admission = buffer.database.prepare(`select admitted_event_id, admitted_rowid, outcome
    from maintenance_rebuild_hook_admissions where receipt_id = ?`).get(receipt.receiptId) as
    { admitted_event_id: string; admitted_rowid: number; outcome: string } | undefined;
  assert.equal(admission?.admitted_event_id, body.id);
  assert.equal(admission?.outcome, "accepted", "the hook insert and receipt acknowledgement must commit together");
  const result = reconcileMaintenanceRebuildRefusals(home);
  const later = reconcileMaintenanceRebuildRefusals(home, ledger,
    Date.now() + MISSING_HOOK_RETRY_MS + 1_000);
  console.log(JSON.stringify({ check: "accepted_retry_after_clock_rollback",
    originalTimestamp: body.timestamp, admittedId: admitted.event.id,
    storedObservedAt: row.observed_at, admission, afterRetry: result, afterHorizon: later }));
  assert.equal(admitted.event.id, body.id);
  assert.notEqual(row.observed_at, body.timestamp, "retry must exercise the clock clamp");
  assert.equal(result.count, 0, "accepted retry must settle the refusal despite the clock adjustment");
  assert.equal(later.lost.length, 0);
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
