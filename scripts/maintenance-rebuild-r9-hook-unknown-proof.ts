import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier, CAPTURE_WRITE_LAG_MS } from "../packages/collector-cli/src/capture-frontier";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  resolveMaintenanceRebuildRefusal } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";
import { createHookSpoolDrain } from "../packages/collector-cli/src/server";

async function main() {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r9-hook-unknown-")));
  const ledger = path.join(home, "ledger.sqlite");
  const horizonMs = Number.isFinite(MISSING_HOOK_RETRY_MS) ? MISSING_HOOK_RETRY_MS : 600_000;
  const enrolledAt = new Date(Date.now() - 60 * 60 * 1000);
  let buffer: LocalEventBuffer | null = null;
  try {
    buffer = new LocalEventBuffer(ledger, { workspaceId: "33333333-3333-7333-8333-333333333333",
      deviceId: "44444444-4444-7444-8444-444444444444", enrollmentNow: () => enrolledAt,
      delivery: { enabled: true } });
    buffer.delivery.migrateLegacy({ now: new Date() });
    const coveredAt = new Date(Date.now() + CAPTURE_WRITE_LAG_MS).toISOString();
    for (const source of ["codex", "claude_code", "grok"] as const) {
      advanceCaptureFrontier(buffer.database, source, { complete: true, files: [] }, coveredAt);
    }
    const id = randomUUID();
    const body = JSON.stringify({ id, hook_event_name: "UserPromptSubmit", session_id: randomUUID(),
      timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "synthetic" });
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body);
    finishMaintenanceRebuildPause(home);
    const before = captureSpoolState(home);
    assert.equal(before.maintenanceRebuildPending, true, "the absent retry is held before the horizon");
    assert.equal(buffer.delivery.captureClaim([], before)?.through, null);
    const directory = path.join(home, "maintenance-rebuild-refusals");
    const receiptFile = path.join(directory, fs.readdirSync(directory)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as Record<string, unknown>;
    receipt.at = new Date(Date.now() - horizonMs - 1_000).toISOString();
    fs.writeFileSync(receiptFile, `${JSON.stringify(receipt)}\n`);
    const unknown = captureSpoolState(home);
    const unknownClaim = buffer.delivery.captureClaim([], unknown);
    assert.equal(unknown.maintenanceRebuildPending, false);
    assert.equal(unknown.losses.length, 1, "the absent retry becomes an explicit known unknown");
    assert.equal(unknownClaim?.dead, 1);
    assert.equal(unknownClaim?.gaps.length, 1);
    assert.notEqual(unknownClaim?.through, null, "the claim can move only with its visible gap");
    assert.ok((JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { unknownAt?: string }).unknownAt,
      "the unknown remains durable across restart");
    buffer.close(); buffer = null;
    buffer = new LocalEventBuffer(ledger, { workspaceId: "33333333-3333-7333-8333-333333333333",
      deviceId: "44444444-4444-7444-8444-444444444444", enrollmentNow: () => enrolledAt,
      delivery: { enabled: true } });
    assert.equal(captureSpoolState(home).losses.length, 1);
    const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body });
    assert.equal(saved.ok, true);
    const drain = await createHookSpoolDrain(collectorConfigSchema.parse({}), buffer, { home }).tick();
    assert.equal(drain.recovered, 1);
    const resolved = captureSpoolState(home);
    assert.equal(resolved.losses.length, 0, "the exact ledger ID heals the known unknown");
    assert.equal(fs.readdirSync(directory).filter((entry) => entry.endsWith(".receipt")).length, 0);
    const uncommitted = JSON.stringify({ id: randomUUID(), hook_event_name: "UserPromptSubmit" });
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", uncommitted);
    finishMaintenanceRebuildPause(home);
    resolveMaintenanceRebuildRefusal(home, "hook", "claude_code", uncommitted,
      { acceptedEventId: (JSON.parse(uncommitted) as { id: string }).id, ledger: buffer.database });
    assert.equal(fs.readdirSync(directory).filter((entry) => entry.endsWith(".receipt")).length, 1,
      "an ID alone cannot retire a refusal without its committed ledger row");
    resolveMaintenanceRebuildRefusal(home, "hook", "claude_code", uncommitted, { outcome: "terminal" });
    assert.equal(fs.readdirSync(directory).filter((entry) => entry.endsWith(".receipt")).length, 0);
    assert.match(fs.readFileSync(path.join(home, "maintenance-rebuild-terminal.jsonl"), "utf8"),
      /"outcome":"terminal"/, "terminal rejection is durable before retirement");
    assert.equal(captureSpoolState(home).maintenanceRebuildPending, false);
    console.log(JSON.stringify({ check: "hook_retry_lost_known_unknown", horizonMs,
      before: before.maintenanceRebuildPending, gap: unknownClaim?.gaps,
      dead: unknownClaim?.dead, recovered: drain.recovered, resolved: resolved.losses.length }));
  } finally {
    buffer?.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
