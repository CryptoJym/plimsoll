import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const baseRoot = process.env.PR417_BASE_WORKTREE;
assert.ok(baseRoot, "set PR417_BASE_WORKTREE to the exact 0.7.44 checkout");
const OldBuffer = require(path.join(baseRoot,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-reused-id-downgrade-"));
const ledger = path.join(root, "ledger.sqlite");
const id = "00000000-0000-4000-8000-000000004190";
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
const options = { workspaceId: "reused-id-review", deviceId: "reused-id-device",
  enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
const event = aiInteractionEventSchema.parse({ id, sessionId: id, source: "codex",
  eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
  actionClass: "other", inputTokens: 1, outputTokens: 1 });
const exists = (buffer: LocalEventBuffer) => Boolean(buffer.database.prepare(
  "select 1 from buffered_events where id=?").get(id));

try {
  // The first incarnation expires under the exact released binary.
  const first = new OldBuffer(ledger, { ...options, delivery: { enabled: false } });
  try {
    assert.equal(first.append(event), true);
    first.database.prepare("update buffered_events set created_at=? where id=?")
      .run(oldAt, id);
    assert.equal(first.prune(30, { now }).events, 1);
    assert.equal(exists(first), false);
  } finally { first.close(); }

  // A source can reuse a caller-controlled event ID after local raw expiry.
  // Give the new incarnation a distinct generation and a queued outbox copy.
  const upgraded = new LocalEventBuffer(ledger, {
    ...options, delivery: { enabled: false },
  });
  let newGeneration = "";
  try {
    assert.equal(upgraded.append(event), true);
    upgraded.database.prepare("update buffered_events set created_at=? where id=?")
      .run(oldAt, id);
    newGeneration = (upgraded.database.prepare(`select privacy_generation as g
      from buffered_events where id=?`).get(id) as { g: string }).g;
    upgraded.delivery.configure({ enabled: true });
    assert.equal(upgraded.delivery.repairRawById(id).enqueued, 1);
    assert.equal(upgraded.retentionStatus(30, now).states.heldForUpload, 1);
  } finally { upgraded.close(); }

  const downgraded = new OldBuffer(ledger, {
    ...options, delivery: { enabled: true },
  });
  try {
    assert.equal(downgraded.prune(30, { now }).events, 1);
    assert.equal(exists(downgraded), false);
    const receipt = downgraded.database.prepare(`select raw_generation as g
      from raw_retention_receipts where event_id=?`).get(id) as { g: string };
    assert.notEqual(receipt.g, newGeneration,
      "0.7.44 reused the first incarnation's expiry receipt");
  } finally { downgraded.close(); }

  const reupgraded = new LocalEventBuffer(ledger, {
    ...options, delivery: { enabled: true },
  });
  try {
    const remainingBefore = reupgraded.delivery.status(now).remainingDelivery;
    const heldBefore = reupgraded.retentionStatus(30, now).states.heldForUpload;
    const expiryMatchesNew = Boolean(reupgraded.database.prepare(`select 1
      from raw_retention_receipts where event_id=? and raw_generation=?`)
      .get(id, newGeneration));
    const lease = reupgraded.delivery.lease({ now: new Date(now.getTime() + 3_600_000) });
    const outbox = (reupgraded.database.prepare(`select count(*) as n from upload_outbox
      where raw_id=?`).get(id) as { n: number }).n;
    const receipt = reupgraded.database.prepare(`select terminal_state as state,
      reason from upload_receipts where delivery_id=?`).get(id);
    console.log(JSON.stringify({ phase: "reupgrade", remainingBefore,
      heldBefore, expiryMatchesNew, lease: lease.items.length,
      locallyDead: lease.locallyDead, outbox, receipt,
      remainingAfter: reupgraded.delivery.status(now).remainingDelivery }));
    assert.equal(lease.items.length, 1,
      "the queued copy whose raw 0.7.44 removed must remain deliverable");
  } finally { reupgraded.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
