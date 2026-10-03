/** Crash after ledger admission, then ordinary enrichment before receipt recovery. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { runRepoEnrichmentMaintenance } from "../packages/collector-cli/src/maintenance";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r5-enrichment-")));
let buffer: LocalEventBuffer | null = null;
try {
  const ledger = path.join(home, "ledger.sqlite");
  buffer = new LocalEventBuffer(ledger);
  const config = collectorConfigSchema.parse({});
  const sessionId = randomUUID();
  const observed = new Date().toISOString();
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id,repo_hash)
    values (?, 'claude_code', 'session_start', 'safe', ?, '{}', ?, ?, ?)`).run(
      randomUUID(), observed, observed, sessionId, "a".repeat(64));
  const body = { id: randomUUID(), hook_event_name: "UserPromptSubmit", session_id: sessionId,
    timestamp: observed, input_tokens: 123, prompt: "synthetic" };
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  const receiptPath = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as { receiptId: string };
  finishMaintenanceRebuildPause(home);
  appendForwardedHook(body, { config, source: "claude_code", buffer });
  const admission = buffer.database.prepare(`select admitted_event_id, outcome
    from maintenance_rebuild_hook_admissions where receipt_id = ?`).get(receipt.receiptId) as
    { admitted_event_id: string; outcome: string } | undefined;
  assert.equal(admission?.admitted_event_id, body.id);
  assert.equal(admission?.outcome, "accepted");
  const before = buffer.database.prepare("select payload_json,input_tokens from buffered_events where id=?")
    .get(body.id) as { payload_json: string; input_tokens: number | null };
  const result = runRepoEnrichmentMaintenance(buffer.database, { skipLegacyBackfill: true });
  const after = buffer.database.prepare("select payload_json,repo_hash from buffered_events where id=?")
    .get(body.id) as { payload_json: string; repo_hash: string | null };
  const state = reconcileMaintenanceRebuildRefusals(home);
  const horizon = reconcileMaintenanceRebuildRefusals(home, ledger,
    Date.now() + MISSING_HOOK_RETRY_MS + 1_000);
  console.log(JSON.stringify({ check: "normal_enrichment_after_accepted_hook", inputTokens: before.input_tokens,
    enrichment: result, admission, payloadChanged: before.payload_json !== after.payload_json,
    repoHashSet: after.repo_hash !== null, afterRetry: state, afterHorizon: horizon }));
  assert.equal(before.input_tokens, 123);
  assert.notEqual(before.payload_json, after.payload_json, "maintenance must mutate the stored payload");
  assert.equal(after.repo_hash, "a".repeat(64));
  assert.equal(state.count, 0, "accepted hook must settle even after normal enrichment");
  assert.equal(horizon.lost.length, 0);
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
