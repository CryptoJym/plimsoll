import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-clock-step-"));
const createdAt = "2026-09-01T00:00:00.000Z";
const backward = new Date("2026-09-29T00:00:00.000Z");
const forward = new Date("2027-01-29T00:00:00.000Z");
const pending = "00000000-0000-4000-8000-000000004178";
const acknowledged = "00000000-0000-4000-8000-000000004179";
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "clock-step-review", deviceId: "clock-step-device",
  delivery: { enabled: true },
});
try {
  const insert = buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id,uploaded_at)
    values (?,'codex','assistant_response','metadata',?,'{}',?,
      'clock-step-review','clock-step-device',?)`);
  insert.run(pending, createdAt, createdAt, null);
  insert.run(acknowledged, createdAt, createdAt, createdAt);
  const first = buffer.prune(30, { maxRows: 10, now: forward });
  assert.equal(first.events, 1, "a previously uploaded expired raw can expire");
  const exists = (id: string) => Boolean(buffer.database.prepare(
    "select 1 from buffered_events where id=?").get(id));
  assert.equal(exists(pending), true, "a pending raw survives forward clock skew");
  assert.equal(exists(acknowledged), false);
  const oldClock = buffer.retentionProgressStatus(30, backward);
  assert.equal(oldClock.states.heldForUpload, 0);
  assert.equal(oldClock.lastPass.heldForUploadExact, true);
  assert.equal(buffer.prune(30, { maxRows: 10, now: backward }).events, 0);
  assert.equal(exists(pending), true);
  const resumed = buffer.retentionProgressStatus(30, forward);
  const exactForwardHeld = buffer.retentionStatus(30, forward).states.heldForUpload;
  assert.equal(exactForwardHeld, 1);
  assert.equal(resumed.lastPass.heldForUploadAsOfCutoff, oldClock.policy.cutoffAt);
  console.log(JSON.stringify({ forwardExpired: first.events,
    pendingPreserved: exists(pending), backwardHeld: oldClock.states.heldForUpload,
    resumedAsOfCutoff: resumed.lastPass.heldForUploadAsOfCutoff, exactForwardHeld }));
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
