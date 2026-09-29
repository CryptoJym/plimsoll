import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-event-detail-outbox-"));
const oldAt = "2026-01-01T00:00:00.000Z";
const now = new Date("2026-09-28T00:00:00.000Z");
const id = "00000000-0000-4000-8000-000000004175";
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "event-detail-review", deviceId: "event-detail-device",
  delivery: { enabled: true },
});
try {
  const event = aiInteractionEventSchema.parse({ id, sessionId: id,
    source: "codex", eventType: "assistant_response", dataMode: "event_detail",
    observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,
     workspace_id,device_id)
    values (?,'codex','assistant_response','event_detail',?,?,?,?,?)`)
    .run(id, oldAt, JSON.stringify(event), oldAt,
      "event-detail-review", "event-detail-device");
  const enqueued = buffer.delivery.repairRawById(id);
  assert.equal(enqueued.enqueued, 1, "legacy event-detail can have an active outbox row");
  buffer.delivery.configure({ enabled: false });
  const before = buffer.retentionStatus(30, now).states.heldForUpload;
  const pass = buffer.prune(30, { maxRows: 10, now });
  const rawPresent = Boolean(buffer.database.prepare(
    "select 1 from buffered_events where id=?").get(id));
  const outbox = (buffer.database.prepare(
    "select count(*) as n from upload_outbox where raw_id=?")
    .get(id) as { n: number }).n;
  console.log(JSON.stringify({ mode: "event_detail", deliveryEnabled: false,
    enqueued: enqueued.enqueued, heldBefore: before, pass, rawPresent, outbox }));
  assert.equal(rawPresent, false,
    "terminally local-only event-detail raw should expire when delivery is disabled");
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
