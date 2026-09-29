/** Reconciliation needs immutable evidence; a legacy drain is a visible unknown. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { hookBodyDigest } from "../packages/collector-cli/src/maintenance-hook-fingerprint";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

for (const immutableDigest of [null, "matching", "different", "legacy_spooled"] as const) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r14-fallback-")));
  const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  try {
    const body = { id: randomUUID(), hook_event_name: "UserPromptSubmit",
      session_id: randomUUID(), timestamp: new Date().toISOString(), prompt: "refused" };
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body),
      immutableDigest === "legacy_spooled"
        ? { spoolName: "1790662638182-1000-abcdef.json" } : {});
    finishMaintenanceRebuildPause(home);
    const receiptDir = path.join(home, "maintenance-rebuild-refusals");
    const file = path.join(receiptDir, fs.readdirSync(receiptDir)[0]!);
    const receipt = JSON.parse(fs.readFileSync(file, "utf8")) as { at: string; bodyDigest: string };
    assert.equal(receipt.bodyDigest, hookBodyDigest(body));
    const timestamp = new Date().toISOString();
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id,maintenance_hook_body_digest)
      values (?,'claude_code','user_prompt_submit','safe',?,'{}',?,?,?)`)
      .run(body.id, timestamp, timestamp, body.session_id,
        immutableDigest === null || immutableDigest === "legacy_spooled" ? null
          : immutableDigest === "matching" ? receipt.bodyDigest : "f".repeat(64));
    const nowMs = Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 1_000;
    const state = reconcileMaintenanceRebuildRefusals(home, path.join(home, "ledger.sqlite"), nowMs);
    const spool = captureSpoolState(home);
    const remains = fs.existsSync(file);
    const unknownAt = remains ? (JSON.parse(fs.readFileSync(file, "utf8")) as { unknownAt?: string }).unknownAt : null;
    console.log(JSON.stringify({ check: "immutable_fallback", immutableDigest, state,
      pending: spool.maintenanceRebuildPending, remains, unknownAt }));
    assert.equal(state.count, immutableDigest === "matching" || immutableDigest === "legacy_spooled" ? 0 : 1);
    assert.deepEqual(state.lost, [], "a candidate without exact immutable evidence is unknown, not lost");
    assert.equal(spool.maintenanceRebuildPending,
      immutableDigest !== "matching" && immutableDigest !== "legacy_spooled");
    assert.equal(spool.unverifiedHookRetries, immutableDigest === "legacy_spooled" ? 1 : 0);
    if (immutableDigest === null || immutableDigest === "different") {
      assert.ok(unknownAt, "unknown status must be durable");
    }
  } finally {
    buffer.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}
