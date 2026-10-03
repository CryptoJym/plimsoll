/** Canonical UUID equality must not settle another session or hook kind. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r5-mixed-multi-")));
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const config = collectorConfigSchema.parse({});
  const upper = "B3F1C2D4-5E6A-4B7C-8D9E-0F1A2B3C4D5E";
  const timestamp = new Date().toISOString();
  const sessionA = "a3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e";
  const sessionB = "c3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e";
  const primary = { id: upper, hook_event_name: "UserPromptSubmit", session_id: sessionA,
    timestamp, cwd: "/fixture", prompt: "first" };
  const otherSession = { ...primary, session_id: sessionB, prompt: "second" };
  const otherKind = { ...primary, hook_event_name: "SessionStart", prompt: "third" };
  markMaintenanceRebuildPause(home);
  for (const body of [primary, otherSession, otherKind]) {
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body));
  }
  finishMaintenanceRebuildPause(home);
  const before = reconcileMaintenanceRebuildRefusals(home);
  assert.equal(before.count, 3);
  appendForwardedHook({ ...primary, id: upper.toLowerCase() }, { config, source: "claude_code", buffer });
  const after = reconcileMaintenanceRebuildRefusals(home);
  console.log(JSON.stringify({ check: "mixed_case_sessions_and_kinds", before, after,
    remaining: fs.readdirSync(path.join(home, "maintenance-rebuild-refusals")).length }));
  assert.equal(after.count, 2, "only the exact normalized first session and kind may retire");
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
