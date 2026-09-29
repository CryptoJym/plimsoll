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
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-reuse-privacy-collision-"));
const ledger = path.join(root, "ledger.sqlite");
const id = "00000000-0000-4000-8000-000000004199";
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
const options = { workspaceId: "review-reuse-privacy", deviceId: "review-device",
  delivery: { enabled: false }, enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
const event = aiInteractionEventSchema.parse({ id, sessionId: id, source: "codex",
  eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
  actionClass: "other", inputTokens: 1, outputTokens: 1 });
let firstGeneration = "";

try {
  const first = new OldBuffer(ledger, options);
  try {
    assert.equal(first.append(event), true);
    first.database.prepare("update buffered_events set created_at=? where id=?").run(oldAt, id);
    first.delivery.configure({ enabled: true });
    assert.equal(first.delivery.repairRawById(id).enqueued, 1);
    firstGeneration = (first.database.prepare(`select raw_generation as generation
      from upload_outbox where delivery_id=?`).get(id) as { generation: string }).generation;
    assert.equal(first.prune(30, { now }).events, 1);
    assert.equal(first.database.prepare("select count(*) as n from upload_outbox").get()?.n, 1);
    assert.equal((first.database.prepare(`select raw_generation as generation
      from raw_retention_receipts where event_id=?`).get(id) as { generation: string }).generation,
      firstGeneration);
    // A second incarnation is rejected for privacy by the released binary
    // while the first one's sealed outbox copy is still pending.
    const privateEvent = aiInteractionEventSchema.parse({ ...event,
      metadata: { serviceName: "sk_live_private_12345678901234567890" } });
    assert.equal(first.append(privateEvent), true);
    const raw = first.database.prepare(`select rowid as rowid,privacy_generation as generation,
      privacy_disposition as disposition from buffered_events where id=?`).get(id);
    const receipt = first.database.prepare(`select reason,created_at as createdAt
      from upload_receipts where delivery_id=?`).get(id);
    console.log(JSON.stringify({ phase: "old_private_reuse", raw, receipt,
      outbox: first.database.prepare("select count(*) as n from upload_outbox").get() }));
    assert.equal(receipt?.reason, "local_privacy_violation");
    assert.notEqual(raw?.generation, firstGeneration);
    assert.notEqual(receipt?.createdAt, oldAt);
    if (process.env.PR417_DROP_SECOND_RECEIPT === "1") {
      // Isolate the second violation: looking up the current raw by recycled
      // rowid must not let its privacy disposition decide the expired copy.
      first.database.prepare("delete from upload_receipts where delivery_id=?").run(id);
    }
  } finally { first.close(); }

  const upgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
  try {
    const before = upgraded.database.prepare(`select delivery_id as id,raw_rowid as rowid,
      raw_generation as generation,length(base_envelope_json) as bytes from upload_outbox`).all();
    assert.equal(before.length, 1);
    assert.equal(before[0]?.generation, firstGeneration);
    assert.ok(before[0]?.bytes > 0);
    const receipt = upgraded.database.prepare(`select reason,raw_rowid as rowid,
      raw_generation as generation from upload_receipts where delivery_id=?`).get(id);
    const result = upgraded.delivery.lease({ now: new Date(now.getTime() + 3_600_000) });
    console.log(JSON.stringify({ phase: "upgrade", before, receipt,
      leased: result.items.length, dead: result.locallyDead,
      after: upgraded.database.prepare("select count(*) as n from upload_outbox").get() }));
    assert.equal(result.items.length, 1,
      "the first copy's exact expiry must not inherit the second raw's privacy rejection");
  } finally { upgraded.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
