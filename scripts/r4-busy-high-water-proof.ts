/** A concurrent in-flight insert must not strand a later accepted hook receipt. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

function scenario(busy: boolean) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r4-busy-water-")));
  const ledger = path.join(home, "ledger.sqlite");
  let buffer: LocalEventBuffer | null = null;
  let writer: Database.Database | null = null;
  try {
    buffer = new LocalEventBuffer(ledger);
    const config = collectorConfigSchema.parse({});
    const id = randomUUID();
    const body = JSON.stringify({ id, hook_event_name: "UserPromptSubmit", session_id: randomUUID(),
      timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "synthetic" });
    markMaintenanceRebuildPause(home);
    if (busy) {
      writer = new Database(ledger);
      writer.exec("BEGIN IMMEDIATE");
      writer.prepare(`insert into buffered_events
        (id, source, event_type, data_mode, observed_at, payload_json, created_at)
        values (?, ?, ?, ?, ?, ?, ?)`).run(randomUUID(), "codex", "session_start", "safe",
          new Date().toISOString(), "{}", new Date().toISOString());
    }
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body);
    const directory = path.join(home, "maintenance-rebuild-refusals");
    const receiptFile = path.join(directory, fs.readdirSync(directory)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { ledgerHighWater: number | null };
    writer?.exec("COMMIT"); writer?.close(); writer = null;
    finishMaintenanceRebuildPause(home);
    const admitted = appendForwardedHook(JSON.parse(body), { config, source: "claude_code", buffer });
    const row = buffer.database.prepare("select rowid,source,event_type from buffered_events where id = ?")
      .get(id) as { rowid: number; source: string; event_type: string };
    const state = reconcileMaintenanceRebuildRefusals(home);
    const afterHorizon = reconcileMaintenanceRebuildRefusals(home, ledger,
      Date.now() + MISSING_HOOK_RETRY_MS + 1_000);
    console.log(JSON.stringify({ check: "busy_high_water_then_exact_retry", busy,
      highWater: receipt.ledgerHighWater, admittedId: admitted.event.id, row,
      afterRetry: state, afterHorizon }));
    assert.equal(admitted.event.id, id);
    assert.equal(row.source, "claude_code");
    assert.equal(state.count, 0, "the admitted retry must retire its receipt");
    assert.equal(afterHorizon.lost.length, 0);
  } finally {
    try { writer?.exec("ROLLBACK"); } catch {}
    writer?.close();
    buffer?.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}
scenario(false);
scenario(true);
