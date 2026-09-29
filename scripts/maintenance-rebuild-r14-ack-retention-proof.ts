/** A retired refusal must not leave a permanent admission index entry. */
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

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r6-ack-retention-")));
let buffer: LocalEventBuffer | null = null;
try {
  const ledger = path.join(home, "ledger.sqlite");
  buffer = new LocalEventBuffer(ledger);
  const body = { id: randomUUID(), hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "synthetic" };
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  const receiptDir = path.join(home, "maintenance-rebuild-refusals");
  const retiredName = fs.readdirSync(receiptDir)[0]!;
  const retiredId = (JSON.parse(fs.readFileSync(path.join(receiptDir, retiredName), "utf8")) as
    { receiptId: string }).receiptId;
  finishMaintenanceRebuildPause(home);
  appendForwardedHook(body, { config: collectorConfigSchema.parse({}), source: "claude_code", buffer });
  const settled = reconcileMaintenanceRebuildRefusals(home);
  assert.equal(settled.count, 0);
  assert.equal(settled.unverifiedHookRetries, 0,
    "the new binary's exact body digest is acknowledged, not retired unverified");
  // A process can die after the receipt unlink but before acknowledgement
  // cleanup. Retention must also reap that orphan when it deletes the event.
  buffer.database.prepare(`insert into maintenance_rebuild_hook_admissions
    (receipt_id,admitted_event_id,admitted_rowid,outcome,receipt_name)
    values (?,?,?,?,?)`).run(retiredId, body.id, 1, "accepted", retiredName);
  const before = buffer.database.prepare("select count(*) as n from maintenance_rebuild_hook_admissions")
    .get() as { n: number };
  assert.equal(before.n, 1, "the fixture must contain an orphan acknowledgement");
  buffer.database.prepare("update buffered_events set created_at='2020-01-01T00:00:00.000Z' where id=?").run(body.id);
  const prune = buffer.prune(1, { now: new Date("2026-09-29T00:00:00.000Z") });
  const events = buffer.database.prepare("select count(*) as n from buffered_events where id=?")
    .get(body.id) as { n: number };
  const after = buffer.database.prepare("select count(*) as n from maintenance_rebuild_hook_admissions")
    .get() as { n: number };
  const receipts = fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"));
  console.log(JSON.stringify({ check: "retired_ack_after_retention", before: before.n,
    after: after.n, events: events.n, receipts: receipts.length, prune }));
  assert.equal(events.n, 0, "retention control must actually delete the event");
  assert.equal(after.n, 0, "retired receipt admission must not outlive retention indefinitely");

  const pending = { ...body, id: randomUUID(), prompt: "still pending" };
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(pending));
  finishMaintenanceRebuildPause(home);
  appendForwardedHook(pending, { config: collectorConfigSchema.parse({}), source: "claude_code", buffer });
  const pendingAdmission = buffer.database.prepare(`select receipt_id as receiptId from
    maintenance_rebuild_hook_admissions where admitted_event_id = ?`).get(pending.id) as
    { receiptId: string } | undefined;
  assert.ok(pendingAdmission, "the pending receipt has an acknowledgement");
  buffer.database.prepare("update buffered_events set created_at='2020-01-01T00:00:00.000Z' where id=?")
    .run(pending.id);
  buffer.prune(1, { now: new Date("2026-09-29T00:00:00.000Z") });
  const protectedAdmission = buffer.database.prepare(`select receipt_id from
    maintenance_rebuild_hook_admissions where receipt_id = ?`).get(pendingAdmission.receiptId);
  assert.ok(protectedAdmission, "retention must preserve acknowledgement evidence for a pending receipt");
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
