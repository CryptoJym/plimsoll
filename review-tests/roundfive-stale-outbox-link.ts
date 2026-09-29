import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const base = process.env.PR417_BASE_WORKTREE;
assert.ok(base, "exact 0.7.44 worktree required");
const OldBuffer = require(path.join(base,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-r5-stale-outbox-"));
const ledger = path.join(root, "ledger.sqlite");
const id = "00000000-0000-4000-8000-000000005720";
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
const options = { workspaceId: "r5-linkage", deviceId: "r5-device",
  delivery: { enabled: false }, enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
const event = aiInteractionEventSchema.parse({ id, sessionId: id, source: "codex",
  eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
  actionClass: "other", inputTokens: 1, outputTokens: 1 });

try {
  const first = new LocalEventBuffer(ledger, options);
  try {
    assert.equal(first.append(event), true);
    first.database.prepare("update buffered_events set created_at=? where id=?").run(oldAt, id);
    first.delivery.configure({ enabled: true });
    assert.equal(first.delivery.repairRawById(id).enqueued, 1);
  } finally { first.close(); }
  const old = new OldBuffer(ledger, { ...options, delivery: { enabled: true } });
  try {
    assert.equal(old.prune(30, { now }).events, 1);
  } finally { old.close(); }
  const reused = new LocalEventBuffer(ledger, options);
  try {
    const db = reused.database;
    const prior = db.prepare(`select raw_rowid as rowid,raw_id as id,
      raw_created_at as at,raw_generation as generation,repo_hash as repoHash
      from upload_outbox where delivery_id=?`).get(id) as {
        rowid: number; id: string; at: string; generation: string; repoHash: string | null;
      };
    assert.ok(prior);
    assert.equal(prior.repoHash, null);
    assert.equal(reused.append(event), true);
    const current = db.prepare(`select rowid as rowid,id,created_at as at,
      privacy_generation as generation from buffered_events where id=?`).get(id) as {
        rowid: number; id: string; at: string; generation: string;
      };
    assert.equal(current.rowid, prior.rowid, "SQLite reused the old raw rowid");
    assert.equal(current.id, prior.id);
    assert.notEqual(current.at, prior.at);
    assert.notEqual(current.generation, prior.generation);
    const newHash = `sha256:${"a".repeat(64)}`;
    db.prepare("update buffered_events set repo_hash=? where rowid=?").run(
      newHash, current.rowid);
    const after = db.prepare("select repo_hash as repoHash from upload_outbox where delivery_id=?")
      .get(id) as { repoHash: string | null };
    reused.delivery.configure({ enabled: true });
    const at = new Date(now.getTime() + 3_600_000);
    const lease = reused.delivery.lease({ now: at });
    const validated = reused.delivery.revalidateLeaseItems(lease.leaseId, lease.items, at);
    const leakedHash = validated.items.find((item) => item.deliveryId === id)
      ?.envelope.event.projectKey ?? null;
    console.log(JSON.stringify({ case: "new_raw_updates_older_copy", prior, current,
      after, leased: lease.items.length, validated: validated.items.length,
      leakedHash, unrelatedLineage: true }));
    assert.equal(leakedHash, newHash,
      "the older queued event would be uploaded with the newer raw's repo hash");
    assert.equal(after.repoHash, prior.repoHash,
      "repo linkage from a new incarnation must not alter an older queued copy");
  } finally { reused.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
