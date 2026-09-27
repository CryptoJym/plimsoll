import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("retention", 9);
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

async function main() {
try {
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
      const receipt = db.prepare(`insert into upload_receipts
        (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
        values (?,?,?,?,1,?,?)`);
      receipt.run(dead.id, "dead", "remote_validation_rejected", "remote", at, at);
      receipt.run(acknowledged.id, "acknowledged", "remote_acknowledged", "success", at, at);
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
    const receipt = db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, 'acknowledged', 'remote_acknowledged', 'success', 1, ?, ?)`);
    for (const row of [receiptOnly, activeAfterAck]) receipt.run(row.id, at, at);
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
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, 'acknowledged', 'remote_acknowledged', 'success', 1, ?, ?)`).run(deliveryId, at, at);
    assert.equal(prune(buffer, 10).events, 1,
      "a legacy raw ID uses its acknowledged outbox delivery ID for retention");
    buffer.close();
    completion.check("legacy_raw_id_acknowledgement_expires");
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
    const now = new Date("2030-01-01T00:00:00.000Z");
    const overdueRows = 110_000;
    const uuidId = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
    const acknowledgedId = uuidId(overdueRows - 2);
    const locallyRejectedId = uuidId(overdueRows - 1);
    const expectedHeld = overdueRows - 2;
    const insert = db.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,privacy_generation)
      values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?, 'fixture-generation')`);
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
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, ?, ?, ?, 1, ?, ?)`).run(
        ensureUuidEventId(legacyId).id, "acknowledged", "remote_acknowledged", "success", at, at);
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, ?, ?, ?, 1, ?, ?)`).run(
        acknowledgedId, "acknowledged", "remote_acknowledged", "success", at, at);
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, ?, ?, ?, 1, ?, ?)`).run(
        locallyRejectedId, "dead", "local_schema_invalid", "local", at, at);
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
    console.log(JSON.stringify({ fixture: "offline_status", overdueRows, synchronousHeld,
      workerHeld: after.states.heldForUpload, acknowledgedId, locallyRejectedId, refreshMs }));
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
  console.log(JSON.stringify({ status: "pass", checks: 4 }));
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
completion.complete();
}

main().catch(error => { console.error(error); process.exitCode = 1; });
