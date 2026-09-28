import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DashboardProjectionStore } from "../packages/collector-cli/src/dashboard-projection";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

async function main() {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr419-live-rollback-"));
const originalDateNow = Date.now;
const now = new Date(originalDateNow());
Date.now = () => now.getTime();
let oldDb: Database.Database | undefined;
let newDb: Database.Database | undefined;

try {
  const oldRoot = path.join(root, "collector-0.7.44");
  fs.mkdirSync(oldRoot);
  const archive = spawnSync("git", ["archive", "--format=tar", "375f277b85f7d4ede7db77bf4359c371c0e8a4aa"],
    { cwd: process.cwd(), maxBuffer: 32 * 1024 * 1024 });
  assert.equal(archive.status, 0, archive.stderr.toString());
  const extract = spawnSync("tar", ["-xf", "-", "-C", oldRoot],
    { input: archive.stdout, maxBuffer: 1024 * 1024 });
  assert.equal(extract.status, 0, extract.stderr.toString());
  fs.symlinkSync(path.join(process.cwd(), "node_modules"), path.join(oldRoot, "node_modules"), "dir");
  const legacySource = path.join(oldRoot, "packages/collector-cli/src/dashboard-projection.ts");
  const legacy = await import(pathToFileURL(legacySource).href) as {
    DashboardProjectionStore: new (db: Database.Database, options?: { now?: Date }) =>
      Pick<DashboardProjectionStore, "runMaintenance" | "readSnapshot" | "status">;
  };

  const file = path.join(root, "ledger.sqlite");
  const seed = new LocalEventBuffer(file);
  const event = aiInteractionEventSchema.parse({
    id: "00000000-0000-4000-8000-000000000419",
    tenantId: "local", source: "codex", dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: new Date(now.getTime() - 86_400_000).toISOString(),
    sessionId: "pr419-review-session", actionClass: "other", model: "gpt-review",
    inputTokens: 1, outputTokens: 1, metadata: {},
  });
  assert.equal(seed.append(event), true);
  for (let i = 0; i < 10 && !seed.projection.status().parityReady; i++)
    seed.projection.runMaintenance(now);
  assert.equal(seed.projection.status().parityReady, true);
  seed.close();

  const reset = new Database(file);
  reset.exec(`drop trigger trg_dashboard_usage_duplicate_update;
    drop trigger trg_codex_duplicate_scan_repair_done;
    drop table codex_duplicate_fact_scan_repairs;
    drop table codex_duplicate_fact_scan;
    update dashboard_projection_control set schema_version=2 where singleton=1;
    update dashboard_snapshots set schema_version=2;
    update buffered_events set usage_duplicate_reason='codex_sse_event_span';`);
  assert.equal((reset.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n, 0);
  reset.close();

  oldDb = new Database(file);
  const oldReader = new legacy.DashboardProjectionStore(oldDb, { now });
  newDb = new Database(file);
  const upgraded = new DashboardProjectionStore(newDb, { now });
  const before = upgraded.status();
  assert.equal(before.backfill.duplicateFactScan.complete, false);
  assert.equal(before.parityReady, false);

  // A 0.7.44 process opened before upgrade retains its in-memory acceptance.
  // It can run after 0.7.45 stamps version 3 and before the scan repairs facts.
  let oldWriterRefused = false;
  try {
    oldReader.runMaintenance(now);
  } catch (error) {
    oldWriterRefused = error instanceof Error && error.message.includes("pending_duplicate_fact_scan");
    if (!oldWriterRefused) throw error;
  }
  assert.equal(oldWriterRefused, true,
    "the database must refuse a pre-opened 0.7.44 writer during an unfinished scan");
  const after = upgraded.status();
  const durable = newDb.prepare(`select parity_ready as parityReady,dirty
    from dashboard_projection_control where singleton=1`).get() as
    {parityReady:number;dirty:number};
  const snapshot = upgraded.readSnapshot(30);
  const events = snapshot.kind === "ready"
    ? Number((snapshot.snapshot.summary.totals as Record<string, number>).events) : null;
  console.log(JSON.stringify({ before: { parityReady: before.parityReady,
    scanComplete: before.backfill.duplicateFactScan.complete },
    after: { parityReady: after.parityReady, dirty: after.dirty,
      degradedReason: after.degradedReason, scanComplete: after.backfill.duplicateFactScan.complete },
    durable, snapshot: { kind: snapshot.kind,
      status: snapshot.kind === "ready" ? snapshot.snapshot.projection.status : null,
      events }, oracleEvents: 0 }));
  assert.equal(after.parityReady, false,
    "an unscanned duplicate fact must never regain trusted parity during a live rollback overlap");
  assert.deepEqual(durable,{parityReady:0,dirty:1},
    "the pre-opened old writer must not restore parity in the database");

  // Independently exercise the reader veto with corrupted legacy control
  // flags. Savepoint rollback restores the durable write fence afterward.
  newDb.exec(`savepoint reader_veto;
    drop trigger trg_dashboard_pending_duplicate_scan_fence;
    update dashboard_projection_control set parity_ready=1, dirty=0,
      degraded_reason=null where singleton=1;`);
  try {
    const guardedStatus = upgraded.status();
    const guardedRead = upgraded.readSnapshot(30);
    assert.equal(guardedStatus.parityReady, false);
    assert.equal(guardedStatus.dirty, true);
    assert.equal(guardedRead.kind, "ready");
    if (guardedRead.kind === "ready") {
      assert.equal(guardedRead.snapshot.projection.status, "stale",
        "the read path must veto green status while the duplicate scan is pending");
    }
  } finally {
    newDb.exec(`rollback to reader_veto; release reader_veto`);
  }
} finally {
  newDb?.close();
  oldDb?.close();
  Date.now = originalDateNow;
  fs.rmSync(root, { recursive: true, force: true });
}
}

main().catch(error => { console.error(error); process.exitCode = 1; });
