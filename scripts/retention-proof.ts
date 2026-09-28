import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("retention", 22);
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import fs from "node:fs";
import { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";

import { SqliteOnlineBackupAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  aiInteractionEventSchema,
  type AiInteractionEvent,
} from "../packages/shared/src/index";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { ensureUuidEventId } from "../packages/collector-cli/src/upload-history";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-retention-proof-"));
const oldCreatedAt = "2000-01-01T00:00:00.000Z";
let nextId = 1;

function event(): AiInteractionEvent {
  const id = `00000000-0000-4000-8000-${String(nextId++).padStart(12, "0")}`;
  return aiInteractionEventSchema.parse({
    id,
    sessionId: id,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: oldCreatedAt,
    actionClass: "other",
    inputTokens: 1,
    outputTokens: 1,
    metadata: { proof: "retention" },
  });
}

function prune(buffer: LocalEventBuffer, maxRows: number) {
  // Advance the supported prune clock; never mutate immutable raw/outbox lineage.
  return buffer.prune(0, { maxRows, now: new Date(Date.now() + 86_400_000) });
}

function receiptForRaw(db: Database.Database, rawId: string, deliveryId: string,
  state: "acknowledged" | "dead", reason: string, statusClass: string, at: string) {
  return db.prepare(`insert into upload_receipts
    (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
     terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
    select ?,rowid,id,created_at,privacy_generation,?,?,?,1,?,?
    from buffered_events where id=?`).run(deliveryId, state, reason, statusClass, at, at, rawId).changes;
}

async function main() {
try {
  const heldNow = new Date();
  const heldStatus = (buffer: LocalEventBuffer, expected: number) => {
    const status = buffer.retentionProgressStatus(90, heldNow);
    assert.equal(status.lastPass.heldForUploadExact, true);
    assert.equal(status.states.heldForUpload, expected);
  };
  const insertHeldRaw = (buffer: LocalEventBuffer, row: AiInteractionEvent,
    workspaceId: string, deviceId: string | null, dataMode = "metadata") => {
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id)
      values (?, 'codex', 'assistant_response', ?, ?, ?, ?, ?, ?)`).run(
        row.id, dataMode, oldCreatedAt, JSON.stringify(row), oldCreatedAt,
        workspaceId, deviceId);
  };

  {
    const buffer = new LocalEventBuffer(path.join(root, "cached-ack.sqlite"), {
      workspaceId: "cached-ack-workspace", deviceId: "cached-ack-device",
      delivery: { enabled: true }, enrollmentNow: () => new Date(oldCreatedAt),
    });
    const row = event();
    buffer.delivery.configure({ enabled: false });
    assert.equal(buffer.append(row), true);
    buffer.database.prepare("update buffered_events set created_at=? where id=?")
      .run(oldCreatedAt, row.id);
    buffer.delivery.configure({ enabled: true });
    assert.equal(buffer.delivery.repairRawById(row.id).enqueued, 1);
    heldStatus(buffer, 1);
    let refreshStatus: (() => boolean) | undefined;
    const server = createCollectorServer(collectorConfigSchema.parse({ retentionDays: 90 }), buffer, {
      registerStatusRefresher: (refresh) => { refreshStatus = refresh; },
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const statusUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/status`;
    const httpHeld = async () => {
      const response = await fetch(statusUrl);
      assert.equal(response.status, 200);
      const status = await response.json() as { retention?: {
        states?: { heldForUpload?: number };
        lastPass?: { heldForUploadExact?: boolean };
      } };
      return status.retention;
    };
    assert.equal((await httpHeld())?.states?.heldForUpload, 1);
    const leaseAt = new Date(heldNow.getTime() + 5_000);
    const lease = buffer.delivery.lease({ now: leaseAt });
    assert.deepEqual(lease.items.map((item) => item.deliveryId), [row.id]);
    assert.equal(buffer.delivery.acknowledge(lease.leaseId, [row.id], leaseAt).acknowledged, 1);
    heldStatus(buffer, 0);
    assert.equal(refreshStatus?.(), true);
    const afterAck = await httpHeld();
    assert.equal(afterAck?.lastPass?.heldForUploadExact, true);
    assert.equal(afterAck?.states?.heldForUpload, 0);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    completion.check("acknowledgement_invalidates_exact_held_count");
  }

  {
    const workspaceId = "cached-local-rejection";
    const buffer = new LocalEventBuffer(path.join(root, "cached-local-rejection.sqlite"), {
      workspaceId, deviceId: "cached-device", delivery: { enabled: true },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    const invalid = event();
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id)
      values (?, 'unsupported_source', 'assistant_response', 'metadata', ?, ?, ?, ?, ?)`).run(
        invalid.id, oldCreatedAt,
        JSON.stringify({ ...invalid, source: "unsupported_source", eventType: "unsupported_kind" }), oldCreatedAt,
        workspaceId, "cached-device");
    heldStatus(buffer, 1);
    assert.equal(buffer.delivery.repairRawById(invalid.id).dead, 1);
    assert.equal((buffer.database.prepare("select reason from upload_receipts where delivery_id=?")
      .get(invalid.id) as { reason: string }).reason, "local_schema_invalid");
    heldStatus(buffer, 0);
    buffer.close();
    completion.check("local_terminal_receipt_invalidates_exact_held_count");
  }

  for (const changed of ["workspace", "device"] as const) {
    const workspaceId = `cached-${changed}-prior`;
    const deviceId = changed === "workspace" ? `cached-${changed}-device` : null;
    const buffer = new LocalEventBuffer(path.join(root, `cached-${changed}-binding.sqlite`), {
      workspaceId, ...(deviceId ? { deviceId } : {}), delivery: { enabled: true },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    insertHeldRaw(buffer, event(), workspaceId, deviceId);
    heldStatus(buffer, 1);
    if (changed === "workspace") {
      buffer.transitionWorkspace(workspaceId, "cached-workspace-current", deviceId!);
    } else {
      buffer.useWorkspace(workspaceId, "cached-device-current");
    }
    heldStatus(buffer, 0);
    buffer.close();
    completion.check(`${changed}_binding_invalidates_exact_held_count`);
  }

  {
    const workspaceId = "cached-predicate-fields";
    const deviceId = "cached-device";
    const buffer = new LocalEventBuffer(path.join(root, "cached-fields.sqlite"), {
      workspaceId, deviceId, delivery: { enabled: true },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    const db = buffer.database;
    heldStatus(buffer, 0);
    const mutateHeldRaw = (sql: string, value: string) => {
      const row = event();
      insertHeldRaw(buffer, row, workspaceId, deviceId);
      heldStatus(buffer, 1);
      db.prepare(sql).run(value, row.id);
      heldStatus(buffer, 0);
      db.prepare("delete from buffered_events where id=?").run(row.id);
    };
    mutateHeldRaw("update buffered_events set uploaded_at=? where id=?", heldNow.toISOString());
    mutateHeldRaw("update buffered_events set privacy_disposition=? where id=?", "local_privacy_violation");
    mutateHeldRaw("update buffered_events set usage_duplicate_reason=? where id=?", "codex_sse_event_span");
    mutateHeldRaw("update buffered_events set data_mode=? where id=?", "evidence");
    mutateHeldRaw("update buffered_events set workspace_id=? where id=?", "old-workspace");
    mutateHeldRaw("update buffered_events set device_id=? where id=?", "old-device");
    mutateHeldRaw("update buffered_events set created_at=? where id=?", heldNow.toISOString());
    const row = event();
    insertHeldRaw(buffer, row, workspaceId, deviceId);
    heldStatus(buffer, 1);
    buffer.delivery.configure({ enabled: false });
    heldStatus(buffer, 0);
    buffer.delivery.configure({ enabled: true });
    heldStatus(buffer, 1);
    db.prepare("delete from buffered_events where id=?").run(row.id);
    heldStatus(buffer, 0);
    const evidence = event();
    insertHeldRaw(buffer, evidence, workspaceId, deviceId, "evidence");
    heldStatus(buffer, 0);
    db.prepare(`insert into upload_outbox
      (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,workspace_id,device_id,
       base_envelope_json,base_bytes,state,next_attempt_at,created_at,updated_at)
      select id,rowid,id,created_at,privacy_generation,workspace_id,device_id,
        '{}',2,'pending',created_at,created_at,created_at
      from buffered_events where id=?`).run(evidence.id);
    heldStatus(buffer, 0);
    db.prepare("delete from upload_outbox where delivery_id=?").run(evidence.id);
    heldStatus(buffer, 0);
    buffer.close();
    completion.check("raw_outbox_and_delivery_mode_mutations_invalidate_exact_held_count");
  }

  for (const changed of ["workspace", "device"] as const) {
    const firstWorkspace = `managed-prior-${changed}`;
    const nextWorkspace = `managed-current-${changed}`;
    const device = `device-${changed}`;
    const buffer = new LocalEventBuffer(path.join(root, `prior-${changed}.sqlite`), {
      workspaceId: firstWorkspace,
      ...(changed === "workspace" ? { deviceId: device } : {}),
      delivery: { enabled: true }, enrollmentNow: () => new Date(oldCreatedAt),
    });
    const db = buffer.database;
    const now = new Date();
    const [pending, retry, inFlight, acknowledged, remoteDead, localDead,
      legacyUploaded, neverUploaded] = Array.from({ length: 8 }, event);
    const priorRows = [pending, retry, inFlight, acknowledged, remoteDead,
      localDead, legacyUploaded, neverUploaded];
    buffer.delivery.configure({ enabled: false });
    for (const row of priorRows) {
      assert.equal(buffer.append(row), true);
      db.prepare("update buffered_events set created_at=? where id=?")
        .run(oldCreatedAt, row.id);
    }
    buffer.delivery.configure({ enabled: true });
    for (const row of [pending, retry, inFlight, acknowledged, remoteDead, localDead]) {
      assert.equal(buffer.delivery.repairRawById(row.id).enqueued, 1);
    }
    const at = now.toISOString();
    db.transaction(() => {
      db.prepare("update upload_outbox set state='retry',attempt_count=1 where raw_id=?")
        .run(retry.id);
      db.prepare(`update upload_outbox set state='in_flight',attempt_count=1,
        lease_id='prior-audience-proof',lease_expires_at=? where raw_id=?`)
        .run(new Date(now.getTime() + 120_000).toISOString(), inFlight.id);
      const remove = db.prepare("delete from upload_outbox where raw_id=?");
      const uploaded = db.prepare("update buffered_events set uploaded_at=? where id=?");
      remove.run(acknowledged.id);
      uploaded.run(at, acknowledged.id);
      receiptForRaw(db, acknowledged.id, acknowledged.id, "acknowledged", "remote_acknowledged", "success", at);
      remove.run(remoteDead.id);
      uploaded.run(at, remoteDead.id);
      receiptForRaw(db, remoteDead.id, remoteDead.id, "dead", "remote_validation_rejected", "remote", at);
      remove.run(localDead.id);
      receiptForRaw(db, localDead.id, localDead.id, "dead", "local_schema_invalid", "local", at);
      uploaded.run(at, legacyUploaded.id);
    })();
    const priorOutbox = [pending.id, retry.id, inFlight.id];
    // Make the status path use its exact off-thread worker on this audience.
    const extraPriorRows = 515;
    const priorAudience = db.prepare(`select workspace_id as workspaceId,
      device_id as deviceId from buffered_events where id=?`).get(pending.id) as
      { workspaceId: string; deviceId: string | null };
    const insert = db.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id,privacy_generation)
      values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?, ?, ?, 'fixture-generation')`);
    db.transaction(() => {
      for (let n = 0; n < extraPriorRows; n++) {
        insert.run(event().id, oldCreatedAt, oldCreatedAt,
          priorAudience.workspaceId, priorAudience.deviceId);
      }
    })();
    if (changed === "workspace") {
      buffer.transitionWorkspace(firstWorkspace, nextWorkspace, device);
    } else {
      // Managed workspace device binding is future-only: null -> device.
      buffer.useWorkspace(firstWorkspace, device);
    }
    const [activePending, activeUnmarked] = [event(), event()];
    buffer.delivery.configure({ enabled: false });
    for (const row of [activePending, activeUnmarked]) {
      assert.equal(buffer.append(row), true);
      db.prepare("update buffered_events set created_at=? where id=?")
        .run(oldCreatedAt, row.id);
    }
    buffer.delivery.configure({ enabled: true });
    assert.equal(buffer.delivery.repairRawById(activePending.id).enqueued, 1);
    assert.equal(buffer.retentionStatus(90, now).states.heldForUpload, 2,
      `${changed} prior audience cannot hold raw retention`);
    const first = buffer.retentionProgressStatus(90, now);
    assert.equal(first.lastPass.heldForUploadExact, false);
    assert.equal(await buffer.refreshRetentionHoldCount(90, now), 2);
    const afterWorker = buffer.retentionProgressStatus(90, now);
    assert.equal(afterWorker.states.heldForUpload, 2);
    assert.equal(afterWorker.lastPass.heldForUploadExact, true);
    assert.equal(afterWorker.lastPass.heldForUploadAsOfCutoff, first.policy.cutoffAt);
    for (let pass = 0; pass < 10 && !buffer.projection.status().ready; pass++) {
      buffer.projection.runMaintenance(now);
    }
    assert.equal(buffer.projection.status().ready, true);
    const server = createCollectorServer(collectorConfigSchema.parse({ retentionDays: 90 }), buffer);
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/status`);
      const body = await response.json() as { retention?: {
        states?: { heldForUpload?: number };
        lastPass?: { heldForUploadExact?: boolean; heldForUploadAsOfCutoff?: string | null };
      } };
      assert.equal(response.status, 200);
      assert.equal(body.retention?.states?.heldForUpload, 2);
      assert.equal(body.retention?.lastPass?.heldForUploadExact, true);
      assert.equal(body.retention?.lastPass?.heldForUploadAsOfCutoff, first.policy.cutoffAt);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    const leaseBefore = buffer.delivery.lease({ maxRows: 100, now });
    assert.equal(leaseBefore.items.some((item) => priorOutbox.includes(item.deliveryId)), false,
      "the current audience must never lease prior deliveries");
    const result = buffer.prune(90, { maxRows: 1_000, now });
    assert.equal(result.events, priorRows.length + extraPriorRows,
      "every overdue prior-audience raw row expires");
    assert.equal(buffer.retentionStatus(90, now).states.heldForUpload, 2);
    assert.equal((db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, 2);
    for (const id of priorOutbox) {
      assert.equal(db.prepare("select 1 from upload_outbox where delivery_id=?").get(id), undefined);
      assert.equal((db.prepare("select reason from upload_receipts where delivery_id=?")
        .get(id) as { reason: string }).reason, "local_privacy_violation");
    }
    if (changed === "workspace") {
      // Even if the owner selects the old workspace later, closed deliveries
      // have no active outbox row and cannot be leased again.
      buffer.transitionWorkspace(nextWorkspace, firstWorkspace, device);
      const leaseAgain = buffer.delivery.lease({ maxRows: 100, now });
      assert.equal(leaseAgain.items.some((item) => priorOutbox.includes(item.deliveryId)), false);
    }
    console.log(JSON.stringify({ fixture: `prior_${changed}_audience`,
      priorRaw: priorRows.length + extraPriorRows, expired: result.events,
      heldForUpload: 2, workerHeld: afterWorker.states.heldForUpload,
      priorOutboxClosed: priorOutbox.length }));
    buffer.close();
    completion.check(`prior_${changed}_audience_expires_and_active_audience_holds`);
  }

  {
    // A caller-controlled legacy id derives the literal UUID of a different
    // row. The linked delivery belongs only to that other row; an outbox row
    // with both lineage columns null still uses the legacy id fallback.
    const priorWorkspace = "collision-prior";
    const currentWorkspace = "collision-current";
    const deviceId = "collision-device";
    const buffer = new LocalEventBuffer(path.join(root, "delivery-id-collision.sqlite"), {
      workspaceId: priorWorkspace, deviceId, delivery: { enabled: false },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    const db = buffer.database;
    const now = new Date();
    const legacyId = `legacy-collision-${nextId++}`;
    const literalId = ensureUuidEventId(legacyId).id;
    const unlinkedId = `legacy-unlinked-${nextId++}`;
    const unlinkedDeliveryId = ensureUuidEventId(unlinkedId).id;
    const withId = (id: string) => aiInteractionEventSchema.parse({
      ...event(), id, sessionId: id,
    });
    assert.notEqual(legacyId, literalId);
    assert.equal(buffer.append(withId(legacyId)), true);
    assert.equal(buffer.append(withId(unlinkedId)), true);
    db.prepare("update buffered_events set created_at=? where id in (?,?)")
      .run(oldCreatedAt, legacyId, unlinkedId);
    db.prepare("update buffered_events set uploaded_at=? where id=?")
      .run(now.toISOString(), legacyId);
    db.prepare(`insert into upload_outbox
      (delivery_id,raw_rowid,raw_id,workspace_id,device_id,base_envelope_json,
       base_bytes,state,next_attempt_at,created_at,updated_at)
      values (?,null,null,?,?,'{}',2,'pending',?,?,?)`).run(
        unlinkedDeliveryId, priorWorkspace, deviceId,
        oldCreatedAt, oldCreatedAt, oldCreatedAt);

    buffer.transitionWorkspace(priorWorkspace, currentWorkspace, deviceId);
    buffer.delivery.configure({ enabled: true });
    assert.equal(buffer.append(withId(literalId)), true);
    const linked = db.prepare(`select raw_rowid as rawRowid,raw_id as rawId
      from upload_outbox where delivery_id=?`).get(literalId) as
      { rawRowid: number; rawId: string } | undefined;
    const literalRaw = db.prepare("select rowid as rawRowid from buffered_events where id=?")
      .get(literalId) as { rawRowid: number };
    assert.equal(linked?.rawRowid, literalRaw.rawRowid);
    assert.equal(linked?.rawId, literalId);

    // While the old workspace is selected, only its genuinely unlinked
    // outbox row holds retention. The linked UUID delivery is another row's.
    buffer.transitionWorkspace(currentWorkspace, priorWorkspace, deviceId);
    buffer.delivery.configure({ enabled: false });
    assert.equal(buffer.retentionStatus(90, now).states.heldForUpload, 1);
    buffer.delivery.configure({ enabled: true });
    buffer.transitionWorkspace(priorWorkspace, currentWorkspace, deviceId);

    assert.equal(buffer.prune(90, { maxRows: 10, now }).events, 2);
    assert.equal(db.prepare("select 1 from buffered_events where id=?").get(legacyId), undefined);
    assert.equal(db.prepare("select 1 from buffered_events where id=?").get(unlinkedId), undefined);
    assert.equal(db.prepare("select 1 from upload_outbox where delivery_id=?").get(unlinkedDeliveryId), undefined);
    assert.equal((db.prepare("select reason from upload_receipts where delivery_id=?")
      .get(unlinkedDeliveryId) as { reason: string }).reason, "local_privacy_violation");
    assert.equal(db.prepare("select 1 from upload_receipts where delivery_id=?").get(literalId), undefined);
    assert.equal((db.prepare("select privacy_disposition as disposition from buffered_events where id=?")
      .get(literalId) as { disposition: string | null }).disposition, null);
    assert.equal(db.prepare("select 1 from upload_outbox where delivery_id=?").get(literalId) !== undefined, true);
    const leaseAt = new Date(now.getTime() + 5_000);
    const lease = buffer.delivery.lease({ now: leaseAt });
    assert.deepEqual(lease.items.map((item) => item.deliveryId), [literalId]);
    assert.equal(buffer.delivery.acknowledge(lease.leaseId, [literalId], leaseAt).acknowledged, 1);
    console.log(JSON.stringify({ fixture: "delivery_id_lineage_collision",
      priorRowsExpired: 2, unrelatedDeliveryAcknowledged: true,
      unlinkedLegacyRetired: true }));
    buffer.close();
    completion.check("prior_audience_delivery_id_collision_preserves_linked_delivery");
  }

  {
    const buffer = new LocalEventBuffer(path.join(root, "free-local.sqlite"));
    const first = event();
    const second = event();
    buffer.append(first);
    buffer.append(second);
    const result = prune(buffer, 1);
    assert.equal(result.events, 1);
    assert.equal(result.eventRowsVisited, 1);
    assert.equal(result.hasMore, true);
    assert.equal(
      (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      1,
    );
    const receipt = buffer.database
      .prepare("select event_id as eventId, reason from raw_retention_receipts")
      .get() as { eventId: string; reason: string };
    assert.equal(receipt.eventId, first.id);
    assert.equal(receipt.reason, "retention_window_elapsed");
    buffer.close();
    completion.check("bounded_prune");
  }

  // B10a's four-row fixture: pending, remote-dead, unmarked, acknowledged.
  // The ordinary path must release only the acknowledged row. Run this same
  // proof on the pinned base and after restoring its old predicate.
  {
    const buffer = new LocalEventBuffer(path.join(root, "b10a-four-row.sqlite"), {
      workspaceId: "tenant-retention-proof",
      enrollmentNow: () => new Date(oldCreatedAt),
      delivery: { enabled: true },
    });
    const rows = [event(), event(), event(), event()];
    for (const row of rows) assert.equal(buffer.append(row), true);
    const [pending, dead, unmarked, acknowledged] = rows;
    const db = buffer.database;
    const at = new Date().toISOString();
    db.transaction(() => {
      db.prepare("delete from upload_outbox where raw_id in (?,?,?,?)")
        .run(...rows.map((row) => row.id));
      db.prepare("update buffered_events set created_at = ? where id in (?,?,?,?)")
        .run(oldCreatedAt, ...rows.map((row) => row.id));
      db.prepare(`insert into upload_outbox
        (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,workspace_id,device_id,
         base_envelope_json,base_bytes,state,next_attempt_at,created_at,updated_at)
        select id,rowid,id,created_at,privacy_generation,workspace_id,device_id,
          '{}',2,'pending',created_at,created_at,created_at
        from buffered_events where id=?`).run(pending.id);
      db.prepare("update buffered_events set uploaded_at = ? where id in (?,?)")
        .run(at, dead.id, acknowledged.id);
      receiptForRaw(db, dead.id, dead.id, "dead", "remote_validation_rejected", "remote", at);
      receiptForRaw(db, acknowledged.id, acknowledged.id, "acknowledged", "remote_acknowledged", "success", at);
    })();
    const before = (db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    const start = performance.now();
    const result = buffer.prune(90, { maxRows: 4,
      now: new Date(Date.parse(oldCreatedAt) + 90 * 86_400_000 + 1) });
    const pruneMs = performance.now() - start;
    const remaining = rows.map((row) => (db.prepare("select count(*) as n from buffered_events where id=?")
      .get(row.id) as { n: number }).n);
    const held = buffer.retentionProgressStatus(90).states.heldForUpload;
    console.log(JSON.stringify({ fixture: "b10a_four_row", rowsBefore: before,
      rowsAfter: before - result.events, rowsVisited: result.eventRowsVisited,
      pendingRawDeleted: remaining[0] === 0, remaining, heldForUpload: held, pruneMs }));
    assert.equal(result.eventRowsVisited, 4);
    assert.deepEqual(remaining, [1, 1, 1, 0], "only an acknowledged raw row may expire");
    assert.equal(held, 3);
    assert.equal(buffer.retentionStatus(90).states.heldForUpload, 3,
      "the collector CLI retention summary reports overdue upload holds");
    assert.equal((db.prepare("select count(*) as n from upload_outbox where raw_id=?")
      .get(pending.id) as { n: number }).n, 1);
    const server = createCollectorServer(collectorConfigSchema.parse({ retentionDays: 90 }), buffer);
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/status`);
      const body = await response.json() as { retention?: { states?: { heldForUpload?: number } } };
      assert.equal(response.status, 200);
      assert.equal(body.retention?.states?.heldForUpload, 3);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    assert.equal(db.prepare("delete from buffered_events where id=?").run(pending.id).changes, 1,
      "explicit erasure still deletes a held row");
    buffer.close();
    completion.check("b10a_four_row_acknowledged_only_and_erasure");
  }

  {
    const buffer = new LocalEventBuffer(path.join(root, "ack-gates.sqlite"), {
      workspaceId: "tenant-retention-proof", delivery: { enabled: true },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    const receiptOnly = event();
    const activeAfterAck = event();
    for (const row of [receiptOnly, activeAfterAck]) assert.equal(buffer.append(row), true);
    const db = buffer.database;
    const at = new Date().toISOString();
    db.prepare("update buffered_events set created_at=? where id in (?,?)")
      .run(oldCreatedAt, receiptOnly.id, activeAfterAck.id);
    db.prepare("delete from upload_outbox where delivery_id=?").run(receiptOnly.id);
    db.prepare("update buffered_events set uploaded_at=? where id=?").run(at, activeAfterAck.id);
    for (const row of [receiptOnly, activeAfterAck])
      receiptForRaw(db, row.id, row.id, "acknowledged", "remote_acknowledged", "success", at);
    assert.equal(prune(buffer, 10).events, 0,
      "a receipt without uploaded_at and an active outbox with an acknowledgement both hold");
    db.prepare("update buffered_events set uploaded_at=? where id=?").run(at, receiptOnly.id);
    db.prepare("delete from upload_outbox where delivery_id=?").run(activeAfterAck.id);
    assert.equal(prune(buffer, 10).events, 2,
      "both rows expire only after all three acknowledgement gates are satisfied");
    buffer.close();
    completion.check("acknowledgement_requires_uploaded_at_receipt_and_no_outbox");
  }

  {
    const buffer = new LocalEventBuffer(path.join(root, "legacy-id-ack.sqlite"), {
      workspaceId: "tenant-retention-proof", delivery: { enabled: true },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    const raw = { ...event(), id: "legacy-retention-ack" };
    assert.equal(buffer.append(raw), true);
    const deliveryId = ensureUuidEventId(raw.id).id;
    const db = buffer.database;
    const at = new Date().toISOString();
    db.prepare("delete from upload_outbox where delivery_id=?").run(deliveryId);
    db.prepare("update buffered_events set created_at=?,uploaded_at=? where id=?")
      .run(oldCreatedAt, at, raw.id);
    receiptForRaw(db, raw.id, deliveryId, "acknowledged", "remote_acknowledged", "success", at);
    assert.equal(prune(buffer, 10).events, 1,
      "a legacy raw ID uses its acknowledged outbox delivery ID for retention");
    buffer.close();
    completion.check("legacy_raw_id_acknowledgement_expires");
  }

  {
    // Open a pre-outbox ledger: historical uploaded_at is its only durable
    // successful-send marker. The current outbox schema must not be present.
    const file = path.join(root, "pre-outbox-upgrade.sqlite");
    const oldDb = new Database(file);
    oldDb.exec(`create table buffered_events (
      id text primary key, source text not null, event_type text not null,
      data_mode text not null, observed_at text not null,
      payload_json text not null, suppressed_fields_json text not null default '[]',
      created_at text not null, uploaded_at text
    )`);
    const legacyUploadedAt = "2020-01-01T00:00:00.000Z";
    const legacyUploadedRows = 515;
    const legacyNeverUploaded = event();
    const insertOld = oldDb.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,uploaded_at)
      values (?, 'codex', 'assistant_response', 'metadata', ?, ?, ?, ?)`);
    oldDb.transaction(() => {
      for (let n = 0; n < legacyUploadedRows; n++) {
        const row = event();
        insertOld.run(row.id, oldCreatedAt, JSON.stringify(row), oldCreatedAt, legacyUploadedAt);
      }
      insertOld.run(legacyNeverUploaded.id, oldCreatedAt,
        JSON.stringify(legacyNeverUploaded), oldCreatedAt, null);
    })();
    assert.equal(oldDb.prepare("select 1 from sqlite_master where name='upload_outbox'").get(), undefined);
    oldDb.close();

    const buffer = new LocalEventBuffer(file, {
      delivery: { enabled: true }, enrollmentNow: () => new Date(oldCreatedAt),
    });
    const db = buffer.database;
    const now = new Date();
    let migrationVisited = 0;
    let skippedUploaded = 0;
    let migrationEnqueued = 0;
    let migrationComplete = false;
    for (let pass = 0; pass < 20; pass++) {
      const slice = buffer.delivery.migrateLegacy({ maxRows: 64, now });
      assert.ok(slice.visited <= 64, "legacy migration must keep each writer slice bounded");
      migrationVisited += slice.visited;
      skippedUploaded += slice.skippedUploaded;
      migrationEnqueued += slice.enqueued;
      if (slice.complete) { migrationComplete = true; break; }
    }
    assert.equal(migrationComplete, true);
    assert.equal(migrationVisited, legacyUploadedRows + 1);
    assert.equal(skippedUploaded, legacyUploadedRows);
    assert.equal(migrationEnqueued, 1);
    assert.equal((db.prepare("select count(*) as n from upload_receipts").get() as { n: number }).n, 0,
      "legacy uploaded rows have no synthesized acknowledgement receipts");
    assert.equal((db.prepare("select count(*) as n from upload_outbox").get() as { n: number }).n, 1,
      "only the never-uploaded legacy row is enqueued");

    const [pending, retry, inFlight, acknowledged, remoteDead, localDead] =
      [event(), event(), event(), event(), event(), event()];
    buffer.delivery.configure({ enabled: false });
    for (const row of [pending, retry, inFlight, acknowledged, remoteDead, localDead]) {
      assert.equal(buffer.append(row), true);
      db.prepare("update buffered_events set created_at=? where id=?").run(oldCreatedAt, row.id);
    }
    buffer.delivery.configure({ enabled: true });
    for (const row of [pending, retry, inFlight, acknowledged, remoteDead, localDead]) {
      assert.equal(buffer.delivery.repairRawById(row.id).enqueued, 1);
    }
    const at = now.toISOString();
    db.transaction(() => {
      db.prepare("update upload_outbox set state='retry', attempt_count=1 where raw_id=?")
        .run(retry.id);
      db.prepare(`update upload_outbox set state='in_flight', attempt_count=1,
        lease_id='upgrade-proof', lease_expires_at=? where raw_id=?`)
        .run(new Date(now.getTime() + 120_000).toISOString(), inFlight.id);
      const remove = db.prepare("delete from upload_outbox where raw_id=?");
      const markUploaded = db.prepare("update buffered_events set uploaded_at=? where id=?");
      remove.run(acknowledged.id);
      markUploaded.run(at, acknowledged.id);
      receiptForRaw(db, acknowledged.id, acknowledged.id, "acknowledged", "remote_acknowledged", "success", at);
      remove.run(remoteDead.id);
      markUploaded.run(at, remoteDead.id);
      receiptForRaw(db, remoteDead.id, remoteDead.id, "dead", "remote_validation_rejected", "remote", at);
      remove.run(localDead.id);
      receiptForRaw(db, localDead.id, localDead.id, "dead", "local_schema_invalid", "local", at);
    })();
    const activeStates = db.prepare("select state,count(*) as n from upload_outbox group by state")
      .all() as Array<{ state: string; n: number }>;
    assert.deepEqual(Object.fromEntries(activeStates.map(({ state, n }) => [state, n])),
      { pending: 2, retry: 1, in_flight: 1 });
    for (let pass = 0; pass < 10 && !buffer.projection.status().ready; pass++) {
      buffer.projection.runMaintenance(now);
    }
    assert.equal(buffer.projection.status().ready, true,
      "upgraded projection must publish before /status serves the retention count");

    const synchronousHeld = buffer.retentionStatus(90, now).states.heldForUpload;
    assert.equal(synchronousHeld, 5);
    const first = buffer.retentionProgressStatus(90, now);
    assert.equal(first.lastPass.heldForUploadExact, false);
    const workerHeld = await buffer.refreshRetentionHoldCount(90, now);
    assert.equal(workerHeld, synchronousHeld);
    const afterWorker = buffer.retentionProgressStatus(90, now);
    assert.equal(afterWorker.states.heldForUpload, synchronousHeld);
    assert.equal(afterWorker.lastPass.heldForUploadExact, true);
    assert.equal(afterWorker.lastPass.heldForUploadAsOfCutoff, first.policy.cutoffAt);
    const server = createCollectorServer(collectorConfigSchema.parse({ retentionDays: 90 }), buffer);
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/status`);
      const body = await response.json() as { retention?: {
        states?: { heldForUpload?: number };
        lastPass?: { heldForUploadExact?: boolean; heldForUploadAsOfCutoff?: string | null };
      } };
      assert.equal(response.status, 200);
      assert.equal(body.retention?.states?.heldForUpload, synchronousHeld);
      assert.equal(body.retention?.lastPass?.heldForUploadExact, true);
      assert.equal(body.retention?.lastPass?.heldForUploadAsOfCutoff, first.policy.cutoffAt);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }

    const result = buffer.prune(90, { maxRows: 1_000, now });
    assert.equal(result.events, legacyUploadedRows + 2,
      "legacy uploaded, new acknowledged, and local-dead rows expire");
    const remaining = (db.prepare("select id from buffered_events order by id")
      .all() as Array<{ id: string }>).map((row) => row.id);
    assert.deepEqual(remaining, [legacyNeverUploaded, pending, retry, inFlight, remoteDead]
      .map((row) => row.id).sort(), "never-uploaded and active or remote-dead deliveries remain");
    assert.equal(buffer.retentionStatus(90, now).states.heldForUpload, 5);
    assert.equal(buffer.retentionProgressStatus(90, now).states.heldForUpload, 5);
    console.log(JSON.stringify({ fixture: "pre_outbox_upgrade", migrationVisited,
      skippedUploaded, migrationEnqueued, synchronousHeld, workerHeld,
      expired: result.events, remaining: remaining.length }));
    buffer.close();
    completion.check("pre_outbox_uploaded_rows_expire_and_unuploaded_rows_hold");
  }

  {
    // Legacy evidence and event-detail rows fail the final privacy upload
    // gate; invalid source/kind shapes get a terminal local schema receipt.
    // None should stop ordinary expiry.
    const buffer = new LocalEventBuffer(path.join(root, "ineligible.sqlite"), {
      workspaceId: "tenant-retention-proof", delivery: { enabled: true },
      enrollmentNow: () => new Date(oldCreatedAt),
    });
    const evidence = event();
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at)
      values (?, 'codex', 'assistant_response', 'evidence', ?, ?, ?)`).run(
        evidence.id, oldCreatedAt, JSON.stringify({ ...evidence, dataMode: "evidence" }), oldCreatedAt);
    const detail = event();
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at)
      values (?, 'codex', 'assistant_response', 'event_detail', ?, ?, ?)`).run(
        detail.id, oldCreatedAt, JSON.stringify({ ...detail, dataMode: "event_detail" }), oldCreatedAt);
    const invalid = { ...event(), id: "legacy-unsupported-kind" };
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at)
      values (?, 'unsupported_source', 'unsupported_kind', 'metadata', ?, ?, ?)`).run(
        invalid.id, oldCreatedAt,
        JSON.stringify({ ...invalid, source: "unsupported_source", eventType: "unsupported_kind" }), oldCreatedAt);
    assert.equal(buffer.delivery.repairRawById(invalid.id).dead, 1);
    assert.equal((buffer.database.prepare("select reason from upload_receipts where delivery_id=?")
      .get(ensureUuidEventId(invalid.id).id) as { reason: string }).reason, "local_schema_invalid");
    const result = prune(buffer, 10);
    assert.equal(result.events, 3);
    assert.equal((buffer.database.prepare("select count(*) as n from buffered_events where id=?")
      .get(evidence.id) as { n: number }).n, 0);
    assert.equal((buffer.database.prepare("select count(*) as n from buffered_events where id=?")
      .get(detail.id) as { n: number }).n, 0);
    assert.equal((buffer.database.prepare("select count(*) as n from buffered_events where id=?")
      .get(invalid.id) as { n: number }).n, 0);
    buffer.close();
    completion.check("ineligible_privacy_and_invalid_kind_expire");
  }

  {
    // The cached HTTP status refresh must never walk an offline backlog on
    // the collector's event loop. A read-only worker can count it exactly.
    const buffer = new LocalEventBuffer(path.join(root, "offline-status.sqlite"), {
      workspaceId: "tenant-retention-proof", delivery: { enabled: true },
    });
    const db = buffer.database;
    // Keep the worker's cutoff at the live HTTP clock; a backward policy
    // cutoff intentionally invalidates the cached count.
    const now = new Date();
    const overdueRows = 110_000;
    const uuidId = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
    const acknowledgedId = uuidId(overdueRows - 2);
    const locallyRejectedId = uuidId(overdueRows - 1);
    const expectedHeld = overdueRows - 2;
    const insert = db.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,privacy_generation)
      values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?,
        'tenant-retention-proof', 'fixture-generation')`);
    db.transaction(() => {
      for (let n = 0; n < overdueRows; n++) {
        insert.run(uuidId(n), oldCreatedAt, oldCreatedAt);
      }
    })();
    assert.equal(ensureUuidEventId(acknowledgedId).id, acknowledgedId);
    assert.equal(ensureUuidEventId(locallyRejectedId).id, locallyRejectedId);
    assert.equal(ensureUuidEventId(uuidId(overdueRows - 3)).id, uuidId(overdueRows - 3));
    const legacyId = "legacy-status-ack";
    insert.run(legacyId, oldCreatedAt, oldCreatedAt);
    const at = now.toISOString();
    db.prepare("update buffered_events set uploaded_at=? where id in (?,?)")
      .run(at, legacyId, acknowledgedId);
    receiptForRaw(db, legacyId, ensureUuidEventId(legacyId).id,
      "acknowledged", "remote_acknowledged", "success", at);
    receiptForRaw(db, acknowledgedId, acknowledgedId,
      "acknowledged", "remote_acknowledged", "success", at);
    receiptForRaw(db, locallyRejectedId, locallyRejectedId,
      "dead", "local_schema_invalid", "local", at);
    const started = performance.now();
    const first = buffer.retentionProgressStatus(90, now);
    const refreshMs = performance.now() - started;
    assert.equal(first.lastPass.heldForUploadExact, false);
    assert.ok(refreshMs < 150, `status refresh scanned the backlog: ${refreshMs} ms`);
    const synchronousHeld = buffer.retentionStatus(90, now).states.heldForUpload;
    assert.equal(synchronousHeld, expectedHeld);
    assert.equal(await buffer.refreshRetentionHoldCount(90, now), synchronousHeld);
    assert.equal(buffer.prune(90, { maxRows: 128, now }).events, 0);
    const after = buffer.retentionProgressStatus(90, now);
    assert.equal(after.states.heldForUpload, expectedHeld);
    assert.equal(after.lastPass.heldForUploadExact, true);
    assert.equal(after.lastPass.heldForUploadAsOfCutoff, first.policy.cutoffAt);

    // The worker's count is exact at its own cutoff. HTTP status is refreshed
    // with a new clock value, so its policy cutoff must not relabel that count.
    const originalRefresh = buffer.refreshRetentionHoldCount.bind(buffer);
    let laterWorkerRequests = 0;
    buffer.refreshRetentionHoldCount = (days, at) => {
      laterWorkerRequests += 1;
      return originalRefresh(days, at);
    };
    let refreshStatus: (() => boolean) | undefined;
    const server = createCollectorServer(collectorConfigSchema.parse({ retentionDays: 90 }), buffer, {
      registerStatusRefresher: (refresh) => { refreshStatus = refresh; },
    });
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const statusUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/status`;
      const readStatus = async () => {
        const response = await fetch(statusUrl);
        assert.equal(response.status, 200);
        return response.json() as Promise<{ retention?: {
          policy?: { cutoffAt?: string };
          states?: { heldForUpload?: number };
          lastPass?: { heldForUploadExact?: boolean; heldForUploadAsOfCutoff?: string | null };
        } }>;
      };
      const httpAfterWorker = await readStatus();
      assert.equal(httpAfterWorker.retention?.states?.heldForUpload, expectedHeld);
      assert.equal(httpAfterWorker.retention?.lastPass?.heldForUploadExact, true);
      assert.equal(httpAfterWorker.retention?.lastPass?.heldForUploadAsOfCutoff, first.policy.cutoffAt);
      assert.notEqual(httpAfterWorker.retention?.policy?.cutoffAt, first.policy.cutoffAt);

      assert.equal(refreshStatus?.(), true);
      const later = await readStatus();
      assert.equal(later.retention?.states?.heldForUpload, expectedHeld);
      assert.equal(later.retention?.lastPass?.heldForUploadExact, true);
      assert.equal(later.retention?.lastPass?.heldForUploadAsOfCutoff, first.policy.cutoffAt);
      assert.notEqual(later.retention?.policy?.cutoffAt, first.policy.cutoffAt);
      assert.equal(laterWorkerRequests, 0, "fresh cached status must not start another worker");
      console.log(JSON.stringify({ fixture: "offline_status_http", heldForUpload: expectedHeld,
        exact: later.retention?.lastPass?.heldForUploadExact,
        asOfCutoff: later.retention?.lastPass?.heldForUploadAsOfCutoff,
        currentPolicyCutoff: later.retention?.policy?.cutoffAt,
        laterWorkerRequests }));
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    // A count started before a committed delivery change must not replace the
    // cached count as exact, regardless of which snapshot the worker read.
    let staleWorkerPublications = 0;
    const stopRaceListener = buffer.onRetentionHoldCountChanged(() => {
      staleWorkerPublications += 1;
    });
    const racingCount = originalRefresh(90, now);
    db.prepare("update buffered_events set uploaded_at=? where id=?")
      .run(at, uuidId(overdueRows - 3));
    await racingCount;
    assert.equal(staleWorkerPublications, 0, "stale worker result was published");
    stopRaceListener();
    const afterRace = buffer.retentionProgressStatus(90, now);
    assert.equal(afterRace.lastPass.heldForUploadExact, false);
    assert.equal(await originalRefresh(90, now), expectedHeld - 1);
    const recounted = buffer.retentionProgressStatus(90, now);
    assert.equal(recounted.lastPass.heldForUploadExact, true);
    assert.equal(recounted.states.heldForUpload, expectedHeld - 1);
    console.log(JSON.stringify({ fixture: "offline_status", overdueRows, synchronousHeld,
      workerHeld: after.states.heldForUpload, acknowledgedId, locallyRejectedId,
      raceDiscarded: !afterRace.lastPass.heldForUploadExact, refreshMs }));
    buffer.close();
    completion.check("large_offline_status_is_bounded_and_exact_after_prune");
  }

  {
    const buffer = new LocalEventBuffer(path.join(root, "pending.sqlite"), {
      workspaceId: "tenant-retention-proof",
      enrollmentNow: () => new Date(oldCreatedAt),
      delivery: { enabled: true },
    });
    const captured = event();
    buffer.append(captured);
    const before = buffer.database
      .prepare("select count(*) as n from upload_outbox where raw_id = ?")
      .get(captured.id) as { n: number };
    assert.equal(before.n, 1);
    buffer.database.prepare(`insert into metric_samples
      (id,source,metric_name,observed_at,value,created_at)
      values (?, 'codex', 'retention_fixture', ?, 1, ?)`).run(
        `metric-${captured.id}`, oldCreatedAt, oldCreatedAt);
    const result = prune(buffer, 10);
    assert.equal(result.events, 0);
    assert.equal(result.metricSamples, 1, "metric retention is independent of raw upload holds");
    assert.equal(
      (buffer.database.prepare("select count(*) as n from buffered_events where id = ?").get(captured.id) as { n: number }).n,
      1,
    );
    assert.equal(
      (buffer.database.prepare("select count(*) as n from upload_outbox where raw_id = ?").get(captured.id) as { n: number }).n,
      1,
    );
    const retention = (buffer as unknown as {
      retentionStatus: (retentionDays?: number) => {
        inspection: string;
        states: { pendingDelivery: number; expired: number; notInspected: number };
      };
    }).retentionStatus(0);
    assert.equal(retention.inspection, "complete");
    assert.equal(retention.states.pendingDelivery, 1);
    assert.equal(retention.states.expired, 0);
    assert.equal(retention.states.notInspected, 0);

    const lease = buffer.delivery.lease({
      leaseId: "retention-proof-lease",
      now: new Date(),
    });
    assert.equal(lease.items.length, 1, JSON.stringify({ locallyDead: lease.locallyDead, blockedBy: lease.blockedBy }));
    const acknowledged = buffer.delivery.acknowledge(
      lease.leaseId,
      [captured.id],
      new Date(),
    );
    assert.equal(acknowledged.acknowledged, 1);
    assert.equal(acknowledged.markedUploaded, 1);
    const receipt = buffer.database
      .prepare("select reason from upload_receipts where delivery_id = ?")
      .get(captured.id) as { reason: string };
    assert.equal(receipt.reason, "remote_acknowledged");
    assert.equal(prune(buffer, 10).events, 1, "acknowledged raw expires after upload");
    buffer.close();
    completion.check("pending_raw_survives_until_acknowledged");
  }

  {
    const sourcePath = path.join(root, "snapshot-source.sqlite");
    const destinationPath = path.join(root, "snapshot.sqlite");
    const buffer = new LocalEventBuffer(sourcePath);
    const captured = event();
    buffer.append(captured);
    assert.equal(prune(buffer, 10).events, 1);
    assert.equal(await new SqliteOnlineBackupAdapter().snapshot({
      source: sourcePath,
      destination: destinationPath,
    }), true);
    buffer.close();
    // Opening a WAL-mode database can itself create sidecars, even read-only.
    // Verify the backup boundary before the independent readback opens it.
    assert.equal(fs.existsSync(`${destinationPath}-wal`), false);
    assert.equal(fs.existsSync(`${destinationPath}-shm`), false);
    const snapshot = new Database(destinationPath, { readonly: true, fileMustExist: true });
    assert.equal(
      (snapshot.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      0,
    );
    assert.equal(
      (snapshot.prepare("select count(*) as n from raw_retention_receipts where reason = 'retention_window_elapsed'").get() as { n: number }).n,
      1,
    );
    snapshot.close();
    completion.check("online_backup_preserves_retention_receipt");
  }

  {
    const buffer = new LocalEventBuffer(path.join(root, "status.sqlite"));
    // HTTP status must use the bounded maintenance receipt, never count raw history.
    buffer.retentionStatus = () => { throw new Error("status_must_not_inspect_full_retention"); };
    const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const response = await fetch(
        `http://127.0.0.1:${(server.address() as AddressInfo).port}/status`,
      );
      const body = await response.json() as {
        retention?: { inspection?: string; states?: Record<string, unknown> };
        enrollment?: { futureOnlyEnrollment?: boolean };
      };
      assert.equal(response.status, 200);
      assert.equal(body.enrollment?.futureOnlyEnrollment, true);
      assert.equal(body.retention?.inspection, "bounded");
      assert.deepEqual(body.retention?.states, {
        retained: null, pendingDelivery: null, heldForUpload: 0,
        quarantined: null, expired: 0, notInspected: 1,
      });
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      buffer.close();
    }
  }

  completion.check("retention_status_contract");
  for (const [name, proof] of [
    ["review_receipt_collision", "../review-tests/retention-collision.ts"],
    ["review_ineligible_outbox", "../review-tests/ineligible-outbox.ts"],
    ["review_retention_atomicity", "../review-tests/retention-atomicity.ts"],
    ["review_clock_rollback", "../review-tests/clock-rollback.ts"],
  ] as const) {
    await import(proof);
    completion.check(name);
  }
  console.log(JSON.stringify({ status: "pass", checks: 22 }));
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
completion.complete();
}

main().catch(error => { console.error(error); process.exitCode = 1; });
