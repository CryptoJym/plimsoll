/** Reviewer check of pending, leased, and acknowledged restamp boundaries. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";

const plimsoll = process.env.PLIMSOLL_HOME!;
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-restamp-state",
  uploadUrl: "http://127.0.0.1:1/unused" });
const ledgerPath = path.join(plimsoll, "review-ledger.sqlite");
const bufferOptions = {
  workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(Date.now() - 3_600_000), delivery: { enabled: true },
};
const buffer = new LocalEventBuffer(ledgerPath, bufferOptions);
let secondBuffer: LocalEventBuffer | undefined;
const readRaw = (id: string) => JSON.parse((buffer.database.prepare(
  "select payload_json as payload from buffered_events where id=?").get(id) as { payload: string }).payload);
const readOutbox = (id: string) => buffer.database.prepare(
  "select state,attempt_count as attempts,base_envelope_json as envelope from upload_outbox where raw_id=?")
  .get(id) as { state: string; attempts: number; envelope: string } | undefined;
const corrected = (id: string) => JSON.stringify({ ...readRaw(id), metadata: {
  workItemId: "beads:eco-6hoxj.165.97", attemptId: "11111111-1111-4111-8111-111111111111",
  workEvidenceRef: "dispatch:synthetic-review", dispatchProjectKey: `sha256:${"a".repeat(64)}`,
} });
const add = () => {
  const id = crypto.randomUUID();
  assert.equal(buffer.append({ id, source: "claude_code", eventType: "assistant_response",
    dataMode: "metadata", observedAt: new Date(Date.now() - 60_000).toISOString(),
    sessionId: "22222222-2222-4222-8222-222222222222",
    actionClass: "other", intent: "unknown", inputTokens: 1, outputTokens: 1, metadata: {} }, []), true);
  return id;
};
try {
  const pending = add();
  assert.equal(buffer.delivery.restampUnsentRaw(pending, corrected(pending)), true);
  assert.equal(readRaw(pending).metadata.workItemId, "beads:eco-6hoxj.165.97");
  assert.equal(JSON.parse(readOutbox(pending)!.envelope).event.metadata.work_ref.work_id, "eco-6hoxj.165.97");
  const firstLease = buffer.delivery.lease({ maxRows: 1 });
  assert.equal(firstLease.items.length, 1);
  assert.equal(buffer.delivery.acknowledge(firstLease.leaseId,
    [firstLease.items[0].deliveryId]).acknowledged, 1);
  assert.equal(buffer.delivery.restampUnsentRaw(pending, corrected(pending)), false);
  assert.equal(readOutbox(pending), undefined);

  const inFlight = add();
  const originalRaw = JSON.stringify(readRaw(inFlight));
  const originalOutbox = JSON.stringify(readOutbox(inFlight));
  const secondLease = buffer.delivery.lease({ maxRows: 1 });
  assert.equal(secondLease.items.length, 1);
  secondBuffer = new LocalEventBuffer(ledgerPath, bufferOptions);
  assert.equal(secondBuffer.delivery.restampUnsentRaw(inFlight, corrected(inFlight)), false);
  assert.equal(JSON.stringify(readRaw(inFlight)), originalRaw);
  assert.equal(readOutbox(inFlight)!.state, "in_flight");
  assert.equal(readOutbox(inFlight)!.attempts, 1);
  assert.notEqual(JSON.stringify(readOutbox(inFlight)), originalOutbox);
  const failedWrite = add();
  const preFailureRaw = JSON.stringify(readRaw(failedWrite));
  const preFailureOutbox = JSON.stringify(readOutbox(failedWrite));
  buffer.database.exec(`create temp trigger review_reject_outbox_update
    before update of base_envelope_json on upload_outbox
    begin select raise(abort, 'synthetic_step_failure'); end`);
  try {
    assert.throws(() => buffer.delivery.restampUnsentRaw(failedWrite, corrected(failedWrite)),
      /synthetic_step_failure/);
  } finally {
    buffer.database.exec("drop trigger review_reject_outbox_update");
  }
  assert.equal(JSON.stringify(readRaw(failedWrite)), preFailureRaw);
  assert.equal(JSON.stringify(readOutbox(failedWrite)), preFailureOutbox);
  console.log(JSON.stringify({ pending: "restamped_with_outbox", acknowledged: "unchanged",
    inFlight: "unchanged", interruptedUpdate: "rolled_back", hostedCalls: 0 }));
} finally {
  secondBuffer?.close();
  buffer.close();
}
