import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-clock-rollback-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "review-clock", deviceId: "review-device", delivery: { enabled: true },
});
const realDateNow = Date.now;
try {
  const nowMs = realDateNow();
  const retentionMs = 90 * 86_400_000;
  const createdAt = new Date(nowMs - retentionMs - 30 * 60_000).toISOString();
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id)
    values ('00000000-0000-4000-8000-000000000301','codex',
      'assistant_response','metadata',?,'{}',?,
      'review-clock','review-device')`).run(createdAt, createdAt);
  const before = buffer.retentionProgressStatus(90, new Date(nowMs));
  assert.equal(before.states.heldForUpload, 1);
  assert.equal(before.lastPass.heldForUploadExact, true);

  const rolledBackMs = nowMs - 60 * 60_000;
  Date.now = () => rolledBackMs;
  const after = buffer.retentionProgressStatus(90, new Date(rolledBackMs));
  const currentExact = buffer.retentionStatus(90, new Date(rolledBackMs)).states.heldForUpload;
  console.log(JSON.stringify({ before: before.states.heldForUpload,
    after: after.states.heldForUpload, afterExactFlag: after.lastPass.heldForUploadExact,
    afterAsOfCutoff: after.lastPass.heldForUploadAsOfCutoff,
    currentPolicyCutoff: after.policy.cutoffAt, currentExact }));
  assert.equal(after.states.heldForUpload, currentExact,
    "a backward clock step must not keep a stale count fresh for the whole skew interval");
} finally {
  Date.now = realDateNow;
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
