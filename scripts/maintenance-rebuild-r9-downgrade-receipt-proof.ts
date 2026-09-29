/** A 0.7.44 drain cannot retire round-8's private hook refusal receipt. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier, CAPTURE_WRITE_LAG_MS } from "../packages/collector-cli/src/capture-frontier";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { countMaintenanceRebuildRefusals, finishMaintenanceRebuildPause,
  markMaintenanceRebuildPause, recordMaintenanceRebuildRefusal } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-old-drain-")));
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const oldRepo = path.join(root, "collector-0744");
const oldCommit = "375f277b85f7d4ede7db77bf4359c371c0e8a4aa";
const source = "claude_code";
const body = JSON.stringify({ hook_event_name: "UserPromptSubmit",
  session_id: "b3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e",
  timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "synthetic" });

async function main() {
  let buffer: { close(): void } | null = null;
  let oldRepoCreated = false;
  try {
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", oldRepo, oldCommit], { cwd: repo });
    oldRepoCreated = true;
    fs.symlinkSync(path.join(repo, "node_modules"), path.join(oldRepo, "node_modules"), "dir");
    markMaintenanceRebuildPause(root);
    recordMaintenanceRebuildRefusal(root, "hook", source, body);
    const receiptDir = path.join(root, "maintenance-rebuild-refusals");
    const receiptFile = path.join(receiptDir, fs.readdirSync(receiptDir)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { eventId: string };
    assert.match(receipt.eventId, /^[0-9a-f-]{36}$/);
    const saved = writeHookSpoolEnvelope({ home: root, source, body, cause: "maintenance_rebuild" });
    assert.equal(saved.ok, true);
    finishMaintenanceRebuildPause(root);
    const oldRoot = path.join(oldRepo, "packages/collector-cli/src");
    const olderSpool = await import(pathToFileURL(path.join(oldRoot, "hook-spool.ts")).href);
    const olderServer = await import(pathToFileURL(path.join(oldRoot, "server.ts")).href);
    const olderBuffer = await import(pathToFileURL(path.join(oldRoot, "buffer.ts")).href);
    const olderConfig = await import(pathToFileURL(path.join(oldRoot, "config.ts")).href);
    assert.equal(olderSpool.listHookSpoolFiles(root).length, 1);
    assert.equal(olderSpool.readHookSpoolFile(saved.path).ok, true);
    buffer = new olderBuffer.LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const drain = olderServer.createHookSpoolDrain(olderConfig.collectorConfigSchema.parse({}), buffer, { home: root });
    const tick = await drain.tick();
    const pending = olderSpool.listHookSpoolFiles(root).length;
    buffer?.close();
    buffer = null;
    const ledger = new Database(path.join(root, "ledger.sqlite"), { readonly: true, fileMustExist: true });
    try { assert.equal((ledger.prepare("select count(*) as count from buffered_events where id=?")
      .get(receipt.eventId) as { count: number }).count, 1,
    "the old reader committed precisely the receipt's stable ledger key"); }
    finally { ledger.close(); }
    const refusalCount = countMaintenanceRebuildRefusals(root);
    const upgradedState = captureSpoolState(root);
    const upgraded = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
      workspaceId: "33333333-3333-7333-8333-333333333333",
      deviceId: "44444444-4444-7444-8444-444444444444", delivery: { enabled: true },
    });
    let claim;
    let statusUnverifiedHookRetries: number | null | undefined;
    try {
      upgraded.delivery.migrateLegacy({ now: new Date() });
      const coveredAt = new Date(Date.now() + CAPTURE_WRITE_LAG_MS).toISOString();
      for (const producer of ["codex", "claude_code", "grok"] as const) {
        advanceCaptureFrontier(upgraded.database, producer, { complete: true, files: [] }, coveredAt);
      }
      claim = upgraded.delivery.captureClaim([], upgradedState);
      const auth = loadOrCreateLocalIngestAuth(root);
      const server = createCollectorServer(collectorConfigSchema.parse({}), upgraded,
        { localAuth: auth, localAuthHome: root });
      try {
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const port = (server.address() as { port: number }).port;
        const response = await fetch(`http://127.0.0.1:${port}/status`,
          { headers: { "x-plimsoll-token": auth.managementRead } });
        assert.equal(response.status, 200);
        const status = await response.json() as { captureRecovery?: { unverifiedHookRetries?: number | null } };
        statusUnverifiedHookRetries = status.captureRecovery?.unverifiedHookRetries;
      } finally { await new Promise<void>((resolve) => server.close(() => resolve())); }
    } finally { upgraded.close(); }
    console.log(JSON.stringify({ check: "downgrade_drain_then_upgrade", tick, pending,
      refusalCount, upgradedPending: upgradedState.maintenanceRebuildPending,
      unverifiedHookRetries: upgradedState.unverifiedHookRetries,
      statusUnverifiedHookRetries,
      claimUnverifiedHookRetries: claim?.unverifiedHookRetries,
      claimUnattested: claim?.unattested, claimThrough: claim?.through }));
    assert.equal(tick.recovered, 1);
    assert.equal(pending, 0);
    assert.equal(refusalCount, 0,
      "0.7.44 must not leave an orphaned private receipt after draining the compatible retry");
    assert.equal(upgradedState.maintenanceRebuildPending, false);
    assert.equal(upgradedState.unverifiedHookRetries, 1,
      "the 0.7.44 drain is retired unverified, never acknowledged as exact");
    assert.equal(claim?.unverifiedHookRetries, 1,
      "the attestation record carries the durable unverified count");
    assert.equal(statusUnverifiedHookRetries, 1,
      "the capture status exposes the durable unverified count");
    assert.notEqual(claim?.through, null, "the exact old-version drain restores capture attestation");
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
