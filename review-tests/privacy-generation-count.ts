import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-generation-count-"));
const oldAt = "2026-01-01T00:00:00.000Z";
const now = new Date("2026-09-28T00:00:00.000Z");
const id = "00000000-0000-4000-8000-000000004177";
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "generation-count", deviceId: "generation-device",
  delivery: { enabled: true },
});
try {
  const db = buffer.database;
  db.exec("drop trigger trg_events_privacy_generation_insert");
  db.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id)
    values (?,'codex','assistant_response','metadata',?,'{}',?,?,?)`)
    .run(id, oldAt, oldAt, "generation-count", "generation-device");
  const rawRowid = (db.prepare("select rowid as n from buffered_events where id=?")
    .get(id) as { n: number }).n;
  db.prepare(`insert into upload_receipts
    (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
     terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
    values (?,?,?,?,null,'dead','local_schema_invalid','local',0,?,?)`)
    .run(id, rawRowid, id, oldAt, oldAt, now.toISOString());
  const before = buffer.retentionProgressStatus(30, now);
  assert.equal(before.states.heldForUpload, 0);
  assert.equal(before.lastPass.heldForUploadExact, true);
  const revisionBefore = (db.prepare("select revision as n from retention_hold_revision")
    .get() as { n: number }).n;
  db.prepare("update buffered_events set privacy_generation=? where id=?")
    .run("generation-assigned-by-legacy-migration", id);
  const revisionAfter = (db.prepare("select revision as n from retention_hold_revision")
    .get() as { n: number }).n;
  const exact = buffer.retentionStatus(30, now).states.heldForUpload;
  const cached = buffer.retentionProgressStatus(30, now);
  console.log(JSON.stringify({ revisionBefore, revisionAfter,
    before: before.states.heldForUpload, exact,
    cached: cached.states.heldForUpload, cachedExact: cached.lastPass.heldForUploadExact }));
  assert.equal(cached.states.heldForUpload, exact,
    "privacy generation assignment must invalidate the exact held-count cache");
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
