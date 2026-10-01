import assert from "node:assert/strict";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const repoHash = `sha256:${"b".repeat(64)}`;
const now = new Date();
const event = aiInteractionEventSchema.parse({
  id: "00000000-0000-4000-8000-000000005721",
  sessionId: "ordinary-tool-session",
  source: "codex",
  eventType: "tool_result",
  dataMode: "metadata",
  observedAt: now.toISOString(),
  actionClass: "other",
});
const buffer = new LocalEventBuffer(":memory:", {
  workspaceId: "ordinary-repo-proof",
  deviceId: "ordinary-repo-device",
  enrollmentNow: () => new Date(now.getTime() - 86_400_000),
  delivery: { enabled: true, now: () => now },
});

try {
  assert.equal(buffer.append(event), true);
  const raw = buffer.database.prepare("select rowid from buffered_events where id = ?")
    .get(event.id) as { rowid: number };
  buffer.database.prepare("update buffered_events set repo_hash = ? where rowid = ?")
    .run(repoHash, raw.rowid);
  assert.equal(buffer.delivery.fillLinkageForRawRow(raw.rowid, repoHash, null), 1);
  const outbox = buffer.database.prepare(
    "select raw_rowid as rawRowid, repo_hash as repoHash from upload_outbox where delivery_id = ?",
  ).get(event.id) as { rawRowid: number; repoHash: string };
  assert.equal(outbox.rawRowid, raw.rowid);
  assert.equal(outbox.repoHash, repoHash);

  const lease = buffer.delivery.lease({ now: new Date(now.getTime() + 3_600_000) });
  assert.equal(lease.items.length, 1);
  const projectKey = lease.items[0]!.envelope.event.projectKey;
  const projectBasis = lease.items[0]!.envelope.event.metadata?.projectBasis;
  assert.equal(projectKey, repoHash);
  assert.equal(projectBasis, "repo_context");
  console.log(JSON.stringify({ proof: "ordinary-non-usage-own-repo-lease", eventType: event.eventType,
    rawRowid: raw.rowid, outboxRepoHash: outbox.repoHash, projectKey, projectBasis,
    leased: lease.items.length, status: "PASS" }));
} finally {
  buffer.close();
}
