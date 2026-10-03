/** Failure to write the acknowledgement rolls back the hook insert too. */
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

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r13-atomic-")));
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const body = { id: randomUUID(), hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date().toISOString(), prompt: "synthetic" };
  const config = collectorConfigSchema.parse({});
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  finishMaintenanceRebuildPause(home);
  buffer.database.exec(`create trigger r13_abort_ack before insert on maintenance_rebuild_hook_admissions
    begin select raise(abort, 'r13_ack_failure'); end`);
  assert.throws(() => appendForwardedHook(body, { config, source: "claude_code", buffer: buffer! }),
    /r13_ack_failure/);
  const count = () => (buffer!.database.prepare("select count(*) as n from buffered_events where id = ?")
    .get(body.id) as { n: number }).n;
  assert.equal(count(), 0, "the event must roll back when its acknowledgement cannot commit");
  buffer.database.exec("drop trigger r13_abort_ack");
  appendForwardedHook(body, { config, source: "claude_code", buffer });
  const admissions = buffer.database.prepare("select receipt_id, admitted_event_id from maintenance_rebuild_hook_admissions")
    .all() as Array<{ receipt_id: string; admitted_event_id: string }>;
  assert.equal(count(), 1);
  assert.equal(admissions.length, 1);
  assert.equal(admissions[0]!.admitted_event_id, body.id);
  assert.equal(reconcileMaintenanceRebuildRefusals(home).count, 0);
  console.log(JSON.stringify({ check: "hook_ack_commit_is_atomic", rolledBackRows: 0,
    committedRows: count(), admissions: admissions.length }));
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
