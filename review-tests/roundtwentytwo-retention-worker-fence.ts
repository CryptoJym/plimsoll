import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { acquireLedgerConnectionLock, openLedgerDatabase,
  writeLedgerPublication } from "../packages/collector-cli/src/ledger-connection";
import { countRetentionHoldsOffThread } from "../packages/collector-cli/src/retention-hold-count";

const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "r22-retention-fence-")));
const ledgerPath = path.join(root, "ledger.sqlite");
const cutoff = "2026-02-01T00:00:00.000Z";
const holdSql = "retention_delivery_id(e.id) is not null";

async function count() {
  const task = countRetentionHoldsOffThread({ name: ledgerPath } as Database.Database, cutoff, holdSql);
  task.worker.ref();
  try { return await task.result; }
  finally { await task.exited; task.worker.unref(); }
}

async function main() {
  try {
    const buffer = new LocalEventBuffer(ledgerPath, {
      workspaceId: "30000000-0000-4000-8000-000000000003",
      deviceId: "40000000-0000-4000-8000-000000000004",
    });
    try {
      buffer.database.prepare(`insert into buffered_events
        (id,source,event_type,data_mode,observed_at,payload_json,created_at)
        values ('11111111-1111-4111-8111-111111111111','codex','assistant_response',
          'metadata','2026-01-01T00:00:00.000Z','{}','2026-01-01T00:00:00.000Z')`).run();
    } finally { await buffer.close(); }
    assert.equal(await count(), 1);

    const exclusive = acquireLedgerConnectionLock(ledgerPath, "exclusive");
    try {
      assert.throws(() => openLedgerDatabase(ledgerPath, { fileMustExist: true }),
        /ledger switch in progress/);
      await assert.rejects(count(), /ledger switch in progress/);
      console.log(JSON.stringify({ state: "exclusive_switch", count: "unavailable" }));
    } finally { exclusive.release(); }

    const pending = acquireLedgerConnectionLock(ledgerPath, "exclusive");
    try {
      const stat = fs.statSync(ledgerPath);
      writeLedgerPublication(pending, { state: "publishing", device: stat.dev, inode: stat.ino,
        marker: { archiveIdentity: "fixture", archivePath: path.join(root, "archive.sqlite"),
          minCollectorVersion: "0.7.46", switchedAt: new Date().toISOString(),
          renameToSampleDelayMs: null, inventoryToRenameDelayMs: null, cursorRows: 0 } });
    } finally { pending.release(); }
    assert.throws(() => openLedgerDatabase(ledgerPath, { fileMustExist: true }),
      /replacement_post_switch_fence_pending/);
    await assert.rejects(count(), /replacement_post_switch_fence_pending/);
    console.log(JSON.stringify({ state: "publishing", count: "unavailable" }));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
