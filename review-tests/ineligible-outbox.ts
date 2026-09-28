import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-ineligible-outbox-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "review-ineligible", deviceId: "review-device", delivery: { enabled: true },
});
try {
  const now = new Date();
  const row = aiInteractionEventSchema.parse({
    id: "00000000-0000-4000-8000-000000000201",
    sessionId: "00000000-0000-4000-8000-000000000201",
    source: "codex", eventType: "assistant_response", dataMode: "metadata",
    observedAt: now.toISOString(), actionClass: "other",
    inputTokens: 1, outputTokens: 1, metadata: { proof: "ineligible" },
  });
  assert.equal(buffer.append(row), true);
  const db = buffer.database;
  assert.equal((db.prepare("select count(*) as n from upload_outbox where raw_id=?")
    .get(row.id) as { n: number }).n, 1);
  // Model an older/stale queued delivery whose raw row is now local-only.
  db.prepare("update buffered_events set data_mode='evidence' where id=?").run(row.id);
  buffer.delivery.configure({ enabled: false });
  const pruneAt = new Date(now.getTime() + 86_400_000);
  const result = buffer.prune(0, { maxRows: 10, now: pruneAt });
  const rawRemaining = Boolean(db.prepare("select 1 from buffered_events where id=?").get(row.id));
  console.log(JSON.stringify({ rawMode: "evidence", deliveryEnabled: false,
    linkedOutbox: true, expired: result.events, rawRemaining }));
  assert.equal(rawRemaining, false, "local-only evidence must expire after upload is disabled");
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
