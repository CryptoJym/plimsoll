import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr419-scan-skew-"));
const file = path.join(root, "ledger.sqlite");
const now = new Date();
const future = new Date(now.getTime() + 86_400_000);
const scanAt = process.argv.includes("--no-skew") ? now : future;
let upgraded: LocalEventBuffer | undefined;

function event(n: number) {
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`,
    tenantId: "local", source: "codex", dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: new Date(now.getTime() - 86_400_000).toISOString(),
    sessionId: "pr419-skew-session", actionClass: "other", model: "gpt-review",
    inputTokens: 1, outputTokens: 1, metadata: {},
  });
}

function count(db: Database.Database, table: string) {
  return (db.prepare(`select count(*) as n from ${table}`).get() as { n: number }).n;
}

try {
  const seed = new LocalEventBuffer(file);
  assert.equal(seed.append(event(1)), true);
  for (let i = 0; i < 10 && !seed.projection.status().parityReady; i++)
    seed.projection.runMaintenance(now);
  assert.equal(seed.projection.status().parityReady, true);
  seed.close();

  const legacy = new Database(file);
  legacy.exec(`drop trigger trg_dashboard_usage_duplicate_update;
    drop trigger trg_codex_duplicate_scan_repair_done;
    drop table codex_duplicate_fact_scan_repairs;
    drop table codex_duplicate_fact_scan;
    update dashboard_projection_control set schema_version=2 where singleton=1;
    update dashboard_snapshots set schema_version=2;
    update buffered_events set usage_duplicate_reason='codex_sse_event_span';`);
  assert.equal(count(legacy, "dashboard_projection_repairs"), 0);
  legacy.close();

  upgraded = new LocalEventBuffer(file);
  let elapsed = 0;
  upgraded.projection.runMaintenance(scanAt, {
    maxActiveMs: 25, clock: () => elapsed,
    onWorkRowForProof: phase => { if (phase === "scan") elapsed = 26; },
  });
  assert.equal(count(upgraded.database, "codex_duplicate_fact_scan_repairs"), 1);

  const receipts: Array<{ scanDebt: number; queued: number; scanComplete: boolean }> = [];
  for (let tick = 0; tick < 5; tick++) {
    for (let n = 0; n < 8; n++) {
      const fresh = event(2 + tick * 8 + n);
      assert.equal(upgraded.append(fresh), true);
      upgraded.database.prepare(`update buffered_events set input_tokens=input_tokens+1 where id=?`)
        .run(fresh.id);
    }
    assert.equal(count(upgraded.database, "dashboard_projection_repairs"), 9);
    elapsed = 0;
    upgraded.projection.runMaintenance(now, {
      maxActiveMs: 25, clock: () => elapsed,
      onWorkRowForProof: phase => { if (phase === "repair") elapsed += 4; },
    });
    receipts.push({
      scanDebt: count(upgraded.database, "codex_duplicate_fact_scan_repairs"),
      queued: count(upgraded.database, "dashboard_projection_repairs"),
      scanComplete: upgraded.projection.status().backfill.duplicateFactScan.complete,
    });
  }
  console.log(JSON.stringify({ scanAt: scanAt.toISOString(), now: now.toISOString(), receipts,
    degradedReason: upgraded.projection.status().degradedReason }));
  assert.equal(receipts.at(-1)?.scanDebt, 0,
    "a corrected clock and continuous capture must not starve a legacy duplicate repair");
} finally {
  upgraded?.close();
  fs.rmSync(root, { recursive: true, force: true });
}
