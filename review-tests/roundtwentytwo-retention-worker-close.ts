import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";
import type { Worker } from "node:worker_threads";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { acquireLedgerConnectionLock } from "../packages/collector-cli/src/ledger-connection";

const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "r22-retention-close-")));
const ledgerPath = path.join(root, "ledger.sqlite");
const buffer = new LocalEventBuffer(ledgerPath, {
  workspaceId: "30000000-0000-4000-8000-000000000003",
  deviceId: "40000000-0000-4000-8000-000000000004",
});

function ledgerHandles() {
  const probe = spawnSync("/usr/sbin/lsof", ["-w", "-F", "n", "--", ledgerPath],
    { encoding: "utf8", timeout: 10_000 });
  assert.ok(probe.status === 0 || probe.status === 1, String(probe.stderr));
  return probe.stdout.split("\n").filter(line => line === `n${ledgerPath}`).length;
}

async function main() {
  try {
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at)
      values ('11111111-1111-4111-8111-111111111111','codex','assistant_response',
        'metadata','2026-01-01T00:00:00.000Z','{}','2026-01-01T00:00:00.000Z')`).run();
    const parentHandles = ledgerHandles();
    assert.ok(parentHandles >= 1, "buffer owns a ledger handle");
    // Keep the real count query inside SQLite until close's bounded grace expires.
    // The override is confined to this fixture; production SQL is unchanged.
    (buffer as unknown as { rawRetentionUploadHoldSql: () => string }).rawRetentionUploadHoldSql =
      () => `exists (with recursive spin(x) as
        (select 0 union all select x+1 from spin where x < 50000000)
        select 1 from spin where x = 50000000)`;
    const count = buffer.refreshRetentionHoldCount(30, new Date("2026-09-28T00:00:00.000Z"));
    void count.catch(() => undefined);
    const task = (buffer as unknown as { retentionHoldTask: {
      worker: Worker; exited: Promise<void> } | null }).retentionHoldTask;
    assert.ok(task);
    const deadline = performance.now() + 5_000;
    while (ledgerHandles() <= parentHandles && performance.now() < deadline) await sleep(10);
    assert.ok(ledgerHandles() > parentHandles, "count worker opened the ledger before close");

    const started = performance.now();
    const closing = buffer.close();
    assert.ok(closing instanceof Promise, "an active count makes close await worker exit");
    assert.equal(buffer.database.open, true, "parent lease stays open during count");
    await sleep(50);
    assert.equal(buffer.database.open, true, "close did not release the parent lease early");
    await closing;
    const elapsedMs = performance.now() - started;
    await task.exited;
    await assert.rejects(count, /retention_hold_count_exit/);
    assert.ok(elapsedMs >= 900, `close waited through its grace period: ${elapsedMs} ms`);
    assert.equal(buffer.database.open, false);
    assert.equal(ledgerHandles(), 0, "no old ledger handle survives close");
    const exclusive = acquireLedgerConnectionLock(ledgerPath, "exclusive");
    exclusive.release();
    console.log(JSON.stringify({ countInFlight: true, closeWaitMs: Math.round(elapsedMs),
      workerExited: true, openLedgerHandlesAfterClose: 0, exclusiveBarrierAvailable: true }));
  } finally {
    await buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
