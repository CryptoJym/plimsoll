/** A retention delete of the max rowid must not strand an accepted retry. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r4-rowid-reuse-")));
const ledger = path.join(home, "ledger.sqlite");
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(ledger);
  const config = collectorConfigSchema.parse({});
  const old = "2020-01-01T00:00:00.000Z";
  buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json, created_at)
    values (?, ?, ?, ?, ?, ?, ?)`).run(randomUUID(), "codex", "session_start", "safe", old, "{}", old);
  const id = randomUUID();
  const body = JSON.stringify({ id, hook_event_name: "UserPromptSubmit", session_id: randomUUID(),
    timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "synthetic" });
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body);
  const directory = path.join(home, "maintenance-rebuild-refusals");
  const receiptFile = path.join(directory, fs.readdirSync(directory)[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { ledgerHighWater: number };
  const pruned = buffer.prune(90);
  const remainingAfterPrune = buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number };
  finishMaintenanceRebuildPause(home);
  const admitted = appendForwardedHook(JSON.parse(body), { config, source: "claude_code", buffer });
  const row = buffer.database.prepare("select rowid from buffered_events where id = ?").get(id) as { rowid: number };
  const state = reconcileMaintenanceRebuildRefusals(home);
  const horizon = reconcileMaintenanceRebuildRefusals(home, ledger, Date.now() + MISSING_HOOK_RETRY_MS + 1_000);
  console.log(JSON.stringify({ check: "retention_rowid_reuse_after_refusal", highWater: receipt.ledgerHighWater,
    pruned, remainingAfterPrune, admittedId: admitted.event.id, rowid: row.rowid,
    afterRetry: state, afterHorizon: horizon }));
  assert.equal(receipt.ledgerHighWater, 1);
  assert.equal(remainingAfterPrune.n, 0);
  assert.equal(row.rowid, 1);
  assert.equal(state.count, 0, "the accepted retry must retire its refusal");
  assert.equal(horizon.lost.length, 0);
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
