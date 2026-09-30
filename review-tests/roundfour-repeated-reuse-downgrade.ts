import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const baseRoot = process.env.PR417_BASE_WORKTREE;
assert.ok(baseRoot, "set PR417_BASE_WORKTREE to exact 0.7.44");
const OldBuffer = require(path.join(baseRoot,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-repeated-reuse-"));
const ledger = path.join(root, "ledger.sqlite");
const id = "00000000-0000-4000-8000-000000004198";
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
const options = { workspaceId: "repeated-reuse", deviceId: "device",
  delivery: { enabled: false }, enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
const event = aiInteractionEventSchema.parse({ id, sessionId: id, source: "codex",
  eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
  actionClass: "other", inputTokens: 1, outputTokens: 1 });
const queued = new Set<string>();
const repairOutcomes: number[] = [];

try {
  for (let incarnation = 0; incarnation < 3; incarnation++) {
    const BufferClass = incarnation === 0 ? OldBuffer : LocalEventBuffer;
    const upgraded = new BufferClass(ledger, options);
    try {
      assert.equal(upgraded.append(event), true);
      upgraded.database.prepare("update buffered_events set created_at=? where id=?")
        .run(oldAt, id);
      if (incarnation > 0) {
        upgraded.delivery.configure({ enabled: true });
        const repaired = upgraded.delivery.repairRawById(id).enqueued;
        repairOutcomes.push(repaired);
        const copy = upgraded.database.prepare(`select delivery_id as id
          from upload_outbox where raw_id=? and raw_generation=(
            select privacy_generation from buffered_events where id=?)`).get(id, id) as
            { id: string } | undefined;
        if (copy) queued.add(copy.id);
        console.log(JSON.stringify({ phase: "new_queue", incarnation,
          repaired, matchingCopy: copy?.id ?? null }));
      }
    } finally { upgraded.close(); }
    const old = new OldBuffer(ledger, { ...options,
      delivery: { enabled: incarnation > 0 } });
    try {
      assert.equal(old.prune(30, { now }).events, 1);
      const receipts = old.database.prepare(`select raw_generation as generation
        from raw_retention_receipts where event_id=?`).all(id) as Array<{ generation: string }>;
      assert.equal(receipts.length, incarnation + 1,
        "the exact old pruner must record each expired incarnation");
      assert.equal(new Set(receipts.map((r) => r.generation)).size, incarnation + 1);
      assert.equal(old.database.prepare("select 1 from buffered_events where id=?").get(id), undefined);
      console.log(JSON.stringify({ phase: "old_prune", incarnation,
        expiries: receipts.length, queued: queued.size }));
    } finally { old.close(); }
  }
  const final = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
  try {
    const remaining = final.delivery.status(now).remainingDelivery;
    const lease = final.delivery.lease({ maxRows: 10, now: new Date(now.getTime() + 3_600_000) });
    console.log(JSON.stringify({ phase: "reupgrade", leased: lease.items.map((item) => item.deliveryId),
      dead: lease.locallyDead, remaining, observedQueued: [...queued], repairOutcomes }));
    assert.deepEqual(repairOutcomes, [1, 1],
      "each reincarnation must gain its own durable queued copy");
    assert.equal(lease.items.length, 2,
      "both queued copies whose raws 0.7.44 pruned must remain deliverable");
    assert.equal(remaining, 2);
    assert.equal(lease.locallyDead, 0);
  } finally { final.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
