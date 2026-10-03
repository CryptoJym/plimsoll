/** A receipt compares the stored normalized event, including a real body time. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const config = collectorConfigSchema.parse({});
function scenario(bodyTime: string | null, retryTime: string | null, expectedPending: number) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r11-digest-")));
  const ledger = path.join(home, "ledger.sqlite");
  let buffer: LocalEventBuffer | null = null;
  try {
    buffer = new LocalEventBuffer(ledger);
    const base = { id: randomUUID(), hook_event_name: "UserPromptSubmit", session_id: randomUUID() };
    const refused = { ...base, ...(bodyTime ? { timestamp: bodyTime } : {}) };
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(refused));
    const directory = path.join(home, "maintenance-rebuild-refusals");
    const receipt = JSON.parse(fs.readFileSync(path.join(home, "maintenance-rebuild-refusals",
      fs.readdirSync(directory)[0]!), "utf8")) as { eventDigest: string; receiveClockFallback: boolean };
    assert.match(receipt.eventDigest, /^[a-f0-9]{64}$/);
    assert.equal(receipt.receiveClockFallback, bodyTime === null);
    finishMaintenanceRebuildPause(home);
    const retry = { ...base, ...(retryTime ? { timestamp: retryTime } : {}) };
    appendForwardedHook(retry, { config, source: "claude_code", buffer });
    const result = reconcileMaintenanceRebuildRefusals(home);
    console.log(JSON.stringify({ check: "normalized_event_digest", bodyTime, retryTime,
      receiveClockFallback: receipt.receiveClockFallback, pending: result.count }));
    assert.equal(result.count, expectedPending);
  } finally {
    buffer?.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}

scenario(null, null, 0);
scenario("2026-09-27T00:00:00.000Z", "2026-09-27T00:00:01.000Z", 1);
