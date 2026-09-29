/** A pending v5 refusal from the previous PR head must settle after upgrade. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash, randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { MISSING_HOOK_RETRY_MS, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

async function main() {
  const previous = path.resolve(process.env.PR424_R6_CHECKOUT ??
    path.resolve(process.cwd(), "../plimsoll-r13"), "packages/collector-cli/src");
  const oldBufferModule = await import(pathToFileURL(path.join(previous, "buffer.ts")).href);
  const oldPause = await import(pathToFileURL(path.join(previous, "maintenance-rebuild-pause-state.ts")).href);
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r7-v5-upgrade-")));
  const ledger = path.join(home, "ledger.sqlite");
  const body = { id: randomUUID(), session_id: randomUUID(),
    hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 9 };
  try {
    const oldBuffer = new oldBufferModule.LocalEventBuffer(ledger);
    try {
      oldPause.markMaintenanceRebuildPause(home);
      oldPause.recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
      oldPause.finishMaintenanceRebuildPause(home);
    } finally { oldBuffer.close(); }
    const receiptFile = path.join(home, "maintenance-rebuild-refusals",
      fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as
      { version: number; at: string; ledgerHighWater: number; receiptId: string };
    assert.equal(receipt.version, 5);
    const currentBuffer = new LocalEventBuffer(ledger);
    try {
      const admitted = appendForwardedHook(body, { config: collectorConfigSchema.parse({}),
        source: "claude_code", buffer: currentBuffer });
      const rows = currentBuffer.database.prepare(`select count(*) as n
        from maintenance_rebuild_hook_admissions where receipt_id=?`)
        .get(receipt.receiptId) as { n: number };
      const row = currentBuffer.database.prepare(`select maintenance_hook_body_digest as digest
        from buffered_events where id=?`).get(admitted.event.id) as { digest: string | null };
      const state = reconcileMaintenanceRebuildRefusals(home, ledger,
        Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 1_000);
      const capture = captureSpoolState(home);
      console.log(JSON.stringify({ check: "v5_receipt_upgrade_retry", oldVersion: receipt.version,
        highWater: receipt.ledgerHighWater, admittedRowDigest: row.digest,
        acknowledgementRows: rows.n, afterWindow: state,
        receiptStillExists: fs.existsSync(receiptFile) }));
      assert.equal(row.digest?.length, 64);
      assert.equal(state.count, 0, "an exact retry of the v5 refusal must settle after upgrade");
      assert.equal(state.unknownHookReceiptFormats, 1,
        "unreleased v5 lacks tenant/body evidence and must retire as a visible unknown");
      assert.equal(state.lost.length, 1);
      assert.equal(capture.unknownHookReceiptFormats, 1);
      assert.equal(capture.losses.length, 1, "unknown format is visible in the capture claim");
    } finally { currentBuffer.close(); }
    const unknownFile = path.join(home, "maintenance-rebuild-refusals",
      `${createHash("sha256").update(randomUUID()).digest("hex")}.receipt`);
    fs.writeFileSync(unknownFile, JSON.stringify({ version: 99, route: "hook",
      at: new Date(Date.now() - MISSING_HOOK_RETRY_MS - 2_000).toISOString(),
      source: "claude_code", eventId: randomUUID() }));
    const unknown = reconcileMaintenanceRebuildRefusals(home, ledger);
    console.log(JSON.stringify({ check: "unrecognized_receipt_format", state: unknown,
      receiptStillExists: fs.existsSync(unknownFile) }));
    assert.equal(unknown.count, 0);
    assert.equal(unknown.unknownHookReceiptFormats, 2);
    assert.equal(unknown.lost.length, 2);
    assert.equal(fs.existsSync(unknownFile), false);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
