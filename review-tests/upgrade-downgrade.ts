import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

async function main() {
  const baseRoot = process.env.PR417_BASE_WORKTREE;
  assert.ok(baseRoot, "set PR417_BASE_WORKTREE to a local 1ae7bbc8 checkout");
  const baseModule = await import(pathToFileURL(path.join(baseRoot,
    "packages/collector-cli/src/buffer.ts")).href);
  const OldBuffer = baseModule.LocalEventBuffer as typeof LocalEventBuffer;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-upgrade-downgrade-"));
  const ledger = path.join(root, "ledger.sqlite");
  const now = new Date();
  const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
  const enrollmentAt = new Date(now.getTime() - 60 * 86_400_000);
  const id = "00000000-0000-4000-8000-000000004170";
  const unqueuedId = "00000000-0000-4000-8000-000000004174";
  const options = { workspaceId: "upgrade-downgrade", deviceId: "upgrade-device",
    delivery: { enabled: false }, enrollmentNow: () => enrollmentAt };
  const exists = (buffer: LocalEventBuffer, rawId: string) => Boolean(buffer.database.prepare(
    "select 1 from buffered_events where id=?").get(rawId));
  const count = (buffer: LocalEventBuffer, table: string) =>
    (buffer.database.prepare(`select count(*) as n from ${table}`).get() as { n: number }).n;

try {
  const old = new OldBuffer(ledger, options);
  try {
    for (const rawId of [id, unqueuedId]) {
      const event = aiInteractionEventSchema.parse({ id: rawId, sessionId: rawId,
        source: "codex", eventType: "assistant_response", dataMode: "metadata",
        observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });
      assert.equal(old.append(event), true);
      old.database.prepare("update buffered_events set created_at=? where id=?")
        .run(oldAt, rawId);
    }
    old.delivery.configure({ enabled: true });
    assert.equal(old.delivery.repairRawById(id).enqueued, 1);
    assert.equal((old.database.prepare("select count(*) as n from upload_outbox where raw_id=?")
      .get(unqueuedId) as { n: number }).n, 0);
    assert.equal(count(old, "upload_outbox"), 1);
  } finally { old.close(); }

  const upgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
  try {
    const first = upgraded.prune(30, { maxRows: 10, now });
    assert.equal(first.events, 0);
    assert.equal(exists(upgraded, id), true);
    assert.equal(exists(upgraded, unqueuedId), true);
    assert.equal(upgraded.retentionStatus(30, now).states.heldForUpload, 2);
    console.log(JSON.stringify({ phase: "upgrade", first, queuedRawPresent: true,
      unqueuedRawPresent: true,
      outbox: count(upgraded, "upload_outbox") }));
  } finally { upgraded.close(); }

  const downgraded = new OldBuffer(ledger, { ...options, delivery: { enabled: true } });
  let downgradeExpired = 0;
  try {
    const pass = downgraded.prune(30, { maxRows: 10, now });
    downgradeExpired = pass.events;
    console.log(JSON.stringify({ phase: "downgrade", pass,
      queuedRawPresent: exists(downgraded, id),
      unqueuedRawPresent: exists(downgraded, unqueuedId),
      outbox: count(downgraded, "upload_outbox") }));
  } finally { downgraded.close(); }

  const reupgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
  try {
    const lease = reupgraded.delivery.lease({ now: new Date(now.getTime() + 3_600_000) });
    const observation = { phase: "reupgrade",
      queuedRawPresent: exists(reupgraded, id),
      unqueuedRawPresent: exists(reupgraded, unqueuedId),
      outbox: count(reupgraded, "upload_outbox"),
      unqueuedOutbox: (reupgraded.database.prepare(
        "select count(*) as n from upload_outbox where raw_id=?")
        .get(unqueuedId) as { n: number }).n,
      retentionReceipts: count(reupgraded, "raw_retention_receipts"),
      leasable: lease.items.map((item) => item.deliveryId), downgradeExpired };
    console.log(JSON.stringify(observation));
    assert.equal(observation.unqueuedRawPresent, true,
      "a rollback to 0.7.44 must not delete a raw that has no outbox copy yet");
    assert.equal(observation.queuedRawPresent, true,
      "a rollback to 0.7.44 must not delete a pending raw held by 0.7.45");
  } finally { reupgraded.close(); }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
