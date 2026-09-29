/** The two receipt lookups used by captureSpoolState and upload are indexed. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { HOOK_ACK_LOOKUP_SQL, HOOK_ROW_LOOKUP_SQL } from
  "../packages/collector-cli/src/maintenance-hook-admission";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r13-plan-")));
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const id = randomUUID();
  const queries = [
    { name: "acknowledgement", sql: HOOK_ACK_LOOKUP_SQL, args: [randomUUID()],
      expected: "maintenance_rebuild_hook_admissions" },
    { name: "older_binary_fallback", sql: HOOK_ROW_LOOKUP_SQL,
      args: [id, id.toLowerCase(), id.toUpperCase()], expected: "e" },
  ];
  for (const query of queries) {
    const plan = buffer.database.prepare(`explain query plan ${query.sql}`).all(...query.args) as
      Array<{ detail: string }>;
    console.log(JSON.stringify({ check: "maintenance_receipt_query_plan", name: query.name,
      plan: plan.map((row) => row.detail) }));
    assert.ok(plan.length > 0);
    assert.ok(plan.every((row) => !row.detail.startsWith(`SCAN ${query.expected}`)),
      `${query.name} must not scan its table`);
    assert.ok(plan.some((row) => row.detail.includes(`SEARCH ${query.expected}`) &&
      row.detail.includes("USING")), `${query.name} must use an index`);
  }
  const body = JSON.stringify({ id, hook_event_name: "UserPromptSubmit", session_id: randomUUID(),
    timestamp: new Date().toISOString(), prompt: "synthetic" });
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body);
  finishMaintenanceRebuildPause(home);
  const state = captureSpoolState(home);
  assert.equal(state.maintenanceRebuildPending, true);
  console.log(JSON.stringify({ check: "capture_spool_state_pending", state }));
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
