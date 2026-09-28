import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { ensureUuidEventId } from "../packages/collector-cli/src/upload-history";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-receipt-collision-"));
const oldAt = "2000-01-01T00:00:00.000Z";
const now = new Date("2026-09-28T00:00:00.000Z");
const legacyId = "legacy-receipt-collision-pr417";
const literalUuidId = ensureUuidEventId(legacyId).id;
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "collision-workspace",
  deviceId: "collision-device",
  delivery: { enabled: true },
});

try {
  const db = buffer.database;
  const insert = db.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id)
    values (?,?,?,'metadata',?,'{}',?,'collision-workspace','collision-device')`);
  // The unuploaded legacy row has not reached the bounded outbox migration.
  insert.run(legacyId, "codex", "assistant_response", oldAt, oldAt);
  // This is a different raw row whose literal UUID equals the legacy row's
  // deterministic delivery UUID. Its invalid source gets a local-dead receipt.
  insert.run(literalUuidId, "unsupported_source", "unsupported_kind", oldAt, oldAt);
  const rejected = buffer.delivery.repairRawById(literalUuidId);
  assert.equal(rejected.dead, 1);
  const receipt = db.prepare(`select terminal_state as state,reason from upload_receipts
    where delivery_id=?`).get(literalUuidId) as { state: string; reason: string } | undefined;
  assert.deepEqual(receipt, { state: "dead", reason: "local_schema_invalid" });
  assert.equal((db.prepare(`select count(*) as n from upload_outbox
    where raw_id=?`).get(legacyId) as { n: number }).n, 0);

  const heldBefore = buffer.retentionStatus(90, now).states.heldForUpload;
  const pruned = buffer.prune(90, { maxRows: 10, now });
  const legacyStillPresent = Boolean(db.prepare(`select 1 from buffered_events where id=?`).get(legacyId));
  console.log(JSON.stringify({ legacyId, literalUuidId, receipt, heldBefore,
    prunedRows: pruned.events, legacyStillPresent }));
  assert.equal(legacyStillPresent, true,
    "an unrelated local-dead receipt must not erase unuploaded legacy raw");
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
