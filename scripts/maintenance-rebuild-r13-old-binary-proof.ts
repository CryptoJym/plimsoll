/** An older binary drains the retry, then enrichment mutates its payload. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier, CAPTURE_WRITE_LAG_MS } from "../packages/collector-cli/src/capture-frontier";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { runRepoEnrichmentMaintenance } from "../packages/collector-cli/src/maintenance";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r3-upper-id-")));
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const oldRepo = path.join(root, "collector-0744");
const oldCommit = "375f277b85f7d4ede7db77bf4359c371c0e8a4aa";
const upperId = "B3F1C2D4-5E6A-4B7C-8D9E-0F1A2B3C4D5E";
const sessionId = "b3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e";
const body = JSON.stringify({ id: upperId, hook_event_name: "UserPromptSubmit",
  session_id: sessionId, input_tokens: 123,
  timestamp: new Date().toISOString(), prompt: "synthetic" });

async function main() {
  let buffer: { close(): void; database: Database.Database } | null = null;
  let oldRepoCreated = false;
  try {
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", oldRepo, oldCommit], { cwd: repo });
    oldRepoCreated = true;
    fs.symlinkSync(path.join(repo, "node_modules"), path.join(oldRepo, "node_modules"), "dir");
    markMaintenanceRebuildPause(root);
    recordMaintenanceRebuildRefusal(root, "hook", "claude_code", body);
    const receiptDir = path.join(root, "maintenance-rebuild-refusals");
    const receiptFile = path.join(receiptDir, fs.readdirSync(receiptDir)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { eventId: string };
    const saved = writeHookSpoolEnvelope({ home: root, source: "claude_code", body,
      cause: "maintenance_rebuild" });
    assert.equal(saved.ok, true);
    finishMaintenanceRebuildPause(root);
    const oldRoot = path.join(oldRepo, "packages/collector-cli/src");
    const olderSpool = await import(pathToFileURL(path.join(oldRoot, "hook-spool.ts")).href);
    const olderServer = await import(pathToFileURL(path.join(oldRoot, "server.ts")).href);
    const olderBuffer = await import(pathToFileURL(path.join(oldRoot, "buffer.ts")).href);
    const olderConfig = await import(pathToFileURL(path.join(oldRoot, "config.ts")).href);
    assert.equal(olderSpool.readHookSpoolFile(saved.path).ok, true);
    buffer = new olderBuffer.LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const now = new Date().toISOString();
    buffer!.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id,repo_hash)
      values (?, 'claude_code', 'session_start', 'safe', ?, '{}', ?, ?, ?)`)
      .run(randomUUID(), now, now, sessionId, "a".repeat(64));
    const tick = await olderServer.createHookSpoolDrain(olderConfig.collectorConfigSchema.parse({}),
      buffer, { home: root }).tick();
    buffer?.close(); buffer = null;
    const forEnrichment = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const before = forEnrichment.database.prepare("select payload_json from buffered_events where id=?")
      .get(upperId) as { payload_json: string };
    const enrichment = runRepoEnrichmentMaintenance(forEnrichment.database, { skipLegacyBackfill: true });
    const after = forEnrichment.database.prepare("select payload_json from buffered_events where id=?")
      .get(upperId) as { payload_json: string };
    forEnrichment.close();
    assert.notEqual(before.payload_json, after.payload_json,
      "ordinary enrichment must mutate the row before the new binary reconciles");
    const ledger = new Database(path.join(root, "ledger.sqlite"), { readonly: true, fileMustExist: true });
    const rows = ledger.prepare("select id from buffered_events where event_type='user_prompt_submit'")
      .all() as Array<{ id: string }>;
    ledger.close();
    const state = captureSpoolState(root);
    const upgraded = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
      workspaceId: "33333333-3333-7333-8333-333333333333",
      deviceId: "44444444-4444-7444-8444-444444444444", delivery: { enabled: true },
    });
    let through: string | null | undefined;
    try {
      upgraded.delivery.migrateLegacy({ now: new Date() });
      const coveredAt = new Date(Date.now() + CAPTURE_WRITE_LAG_MS).toISOString();
      for (const producer of ["codex", "claude_code", "grok"] as const) {
        advanceCaptureFrontier(upgraded.database, producer, { complete: true, files: [] }, coveredAt);
      }
      through = upgraded.delivery.captureClaim([], state)?.through;
    } finally { upgraded.close(); }
    const pending = olderSpool.listHookSpoolFiles(root).length;
    const receipts = fs.readdirSync(receiptDir).filter((entry) => entry.endsWith(".receipt"));
    console.log(JSON.stringify({ check: "older_binary_drain_after_enrichment", upperId,
      receiptEventId: receipt.eventId, rows, recovered: tick.recovered, pending,
      enrichment, payloadChanged: before.payload_json !== after.payload_json,
      receipts: receipts.length, maintenanceRebuildPending: state.maintenanceRebuildPending,
      claimThrough: through }));
    assert.equal(tick.recovered, 1);
    assert.equal(pending, 0);
    assert.deepEqual(rows.map((row) => row.id), [upperId]);
    assert.equal(receipt.eventId, upperId, "the receipt retains the producer's original UUID spelling");
    assert.equal(receipts.length, 0, "exact old-version ledger acceptance must retire the receipt");
    assert.equal(state.maintenanceRebuildPending, false);
    assert.notEqual(through, null, "the old-version drain restores capture attestation");
  } finally {
    try { buffer?.close(); }
    finally {
      try {
        if (oldRepoCreated) execFileSync("git", ["worktree", "remove", "--force", oldRepo], { cwd: repo });
      } finally { fs.rmSync(root, { recursive: true, force: true }); }
    }
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
