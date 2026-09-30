import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-retention-atomicity-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "old-workspace", deviceId: "review-device", delivery: { enabled: true },
  enrollmentNow: () => new Date("2000-01-01T00:00:00.000Z"),
});
try {
  const db = buffer.database;
  const raw = aiInteractionEventSchema.parse({
    id: "00000000-0000-4000-8000-000000000101",
    sessionId: "00000000-0000-4000-8000-000000000101",
    source: "codex", eventType: "assistant_response", dataMode: "metadata",
    observedAt: "2000-01-01T00:00:00.000Z", actionClass: "other",
    inputTokens: 1, outputTokens: 1, metadata: { proof: "atomicity" },
  });
  buffer.delivery.configure({ enabled: false });
  assert.equal(buffer.append(raw), true);
  db.prepare("update buffered_events set created_at=? where id=?")
    .run("2000-01-01T00:00:00.000Z", raw.id);
  buffer.delivery.configure({ enabled: true });
  assert.equal(buffer.delivery.repairRawById(raw.id).enqueued, 1);
  assert.equal((db.prepare("select count(*) as n from upload_outbox where raw_id=?")
    .get(raw.id) as { n: number }).n, 1);
  buffer.transitionWorkspace("old-workspace", "new-workspace", "review-device");
  const count = (table: string) => (db.prepare(`select count(*) as n from ${table}`)
    .get() as { n: number }).n;
  const revision = () => (db.prepare("select revision from retention_hold_revision where singleton=1")
    .get() as { revision: number }).revision;
  const initialRevision = revision();
  const assertRolledBack = (label: string) => {
    assert.equal(count("buffered_events"), 1, label);
    assert.equal(count("upload_outbox"), 1, label);
    assert.equal(count("upload_receipts"), 0, label);
    assert.equal(count("raw_retention_receipts"), 0, label);
    assert.equal(revision(), initialRevision, label);
  };
  const now = new Date("2026-09-28T00:00:00.000Z");
  for (const [label, table] of [
    ["outbox_delete", "upload_outbox"],
    ["raw_delete", "buffered_events"],
  ] as const) {
    db.exec(`create temp trigger review_fault before delete on ${table}
      begin select raise(abort,'review_fault_${label}'); end;`);
    assert.throws(() => buffer.prune(90, { maxRows: 10, now }),
      new RegExp(`review_fault_${label}`));
    db.exec("drop trigger review_fault");
    assertRolledBack(label);
  }
  const result = buffer.prune(90, { maxRows: 10, now });
  assert.equal(result.events, 1);
  assert.equal(count("buffered_events"), 0);
  assert.equal(count("upload_outbox"), 0);
  assert.equal(count("upload_receipts"), 1);
  assert.equal(count("raw_retention_receipts"), 1);
  console.log(JSON.stringify({ faultPoints: 2, rollbackChecksPerFault: 5,
    expired: result.events, receiptReason: (db.prepare("select reason from upload_receipts")
      .get() as { reason: string }).reason }));

  const local = new LocalEventBuffer(path.join(root, "local-only.sqlite"), {
    workspaceId: "local-workspace", deviceId: "review-device", delivery: { enabled: true },
    enrollmentNow: () => new Date("2000-01-01T00:00:00.000Z"),
  });
  try {
    const localDb = local.database;
    const localRaw = aiInteractionEventSchema.parse({ ...raw,
      id: "00000000-0000-4000-8000-000000000102",
      sessionId: "00000000-0000-4000-8000-000000000102" });
    local.delivery.configure({ enabled: false });
    assert.equal(local.append(localRaw), true);
    localDb.prepare("update buffered_events set created_at=? where id=?")
      .run("2000-01-01T00:00:00.000Z", localRaw.id);
    local.delivery.configure({ enabled: true });
    assert.equal(local.delivery.repairRawById(localRaw.id).enqueued, 1);
    localDb.prepare("update buffered_events set data_mode='evidence' where id=?")
      .run(localRaw.id);
    local.delivery.configure({ enabled: false });
    const localCount = (table: string) => (localDb.prepare(`select count(*) as n from ${table}`)
      .get() as { n: number }).n;
    for (const [label, table] of [
      ["ineligible_outbox_delete", "upload_outbox"],
      ["ineligible_raw_delete", "buffered_events"],
    ] as const) {
      localDb.exec(`create temp trigger review_fault before delete on ${table}
        begin select raise(abort,'review_fault_${label}'); end;`);
      assert.throws(() => local.prune(90, { maxRows: 10, now }),
        new RegExp(`review_fault_${label}`));
      localDb.exec("drop trigger review_fault");
      assert.deepEqual([localCount("buffered_events"), localCount("upload_outbox"),
        localCount("upload_receipts"), localCount("raw_retention_receipts")], [1, 1, 0, 0]);
    }
    assert.equal(local.prune(90, { maxRows: 10, now }).events, 1);
    assert.deepEqual([localCount("buffered_events"), localCount("upload_outbox"),
      localCount("upload_receipts"), localCount("raw_retention_receipts")], [0, 0, 1, 1]);
    console.log(JSON.stringify({ ineligibleFaultPoints: 2,
      receiptReason: (localDb.prepare("select reason from upload_receipts")
        .get() as { reason: string }).reason }));
  } finally {
    local.close();
  }
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
