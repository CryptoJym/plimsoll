import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("retention", 4);
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

  {
    const buffer = new LocalEventBuffer(path.join(root, "pending.sqlite"), {
      workspaceId: "tenant-retention-proof",
      delivery: { enabled: true },
    });
    const captured = event();
    buffer.append(captured);
    const before = buffer.database
      .prepare("select count(*) as n from upload_outbox where raw_id = ?")
      .get(captured.id) as { n: number };
    assert.equal(before.n, 1);
    const result = prune(buffer, 10);
    assert.equal(result.events, 1);
    assert.equal(
      (buffer.database.prepare("select count(*) as n from buffered_events where id = ?").get(captured.id) as { n: number }).n,
      0,
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
    assert.equal(retention.states.expired, 1);
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
    assert.equal(acknowledged.markedUploaded, 0);
    const receipt = buffer.database
      .prepare("select reason from upload_receipts where delivery_id = ?")
      .get(captured.id) as { reason: string };
    assert.equal(receipt.reason, "remote_acknowledged");
    buffer.close();
    completion.check("pending_delivery_survives_retention");
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
        retained: null, pendingDelivery: null, quarantined: null, expired: 0, notInspected: 1,
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
