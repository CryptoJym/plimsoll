/** A prior, unrelated buffered row with the same producer UUID is not this retry. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals, resolveMaintenanceRebuildRefusal } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r3-id-collision-")));
const id = randomUUID();
const ledger = path.join(home, "ledger.sqlite");
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(ledger);
  const config = collectorConfigSchema.parse({});
  const prior = appendForwardedHook({ id, hook_event_name: "SessionStart", session_id: randomUUID(),
    timestamp: new Date().toISOString(), cwd: "/fixture" }, { config, source: "codex", buffer });
  assert.equal(prior.event.id, id);
  const refusedBody = JSON.stringify({ id, hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date().toISOString(), cwd: "/fixture",
    prompt: "different event" });
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", refusedBody);
  finishMaintenanceRebuildPause(home);
  const before = fs.readdirSync(path.join(home, "maintenance-rebuild-refusals")).length;
  const result = reconcileMaintenanceRebuildRefusals(home);
  const state = captureSpoolState(home);
  const rows = buffer.database.prepare("select id,source,event_type from buffered_events where id=?")
    .all(id) as Array<{ id: string; source: string; event_type: string }>;
  console.log(JSON.stringify({ check: "unrelated_same_id_must_not_settle_receipt", before,
    after: result.count, rows, maintenanceRebuildPending: state.maintenanceRebuildPending }));
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.source, "codex");
  assert.equal(result.count, 1,
    "a Codex row with the same UUID cannot prove a refused Claude event was captured");
  assert.equal(state.maintenanceRebuildPending, true);
  resolveMaintenanceRebuildRefusal(home, "hook", "claude_code", refusedBody, { outcome: "terminal" });
  const sameId = randomUUID();
  const sameKind = { id: sameId, hook_event_name: "UserPromptSubmit", session_id: randomUUID(),
    timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "prior same-source event" };
  const earlier = appendForwardedHook(sameKind, { config, source: "claude_code", buffer });
  const earlierRowid = (buffer.database.prepare("select rowid from buffered_events where id=?")
    .get(earlier.event.id) as { rowid: number }).rowid;
  // Prompt text is privacy-suppressed and is not part of payload_json. Change
  // a persisted field as well so this really is a different normalized event.
  const laterBody = JSON.stringify({ ...sameKind, session_id: randomUUID(), prompt: "new refused event" });
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", laterBody);
  finishMaintenanceRebuildPause(home);
  const secondFile = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals")).find((entry) => entry.endsWith(".receipt"))!);
  const secondReceipt = JSON.parse(fs.readFileSync(secondFile, "utf8")) as { ledgerHighWater?: number;
    source?: string; kind?: string };
  assert.ok((secondReceipt.ledgerHighWater ?? -1) >= earlierRowid,
    "the receipt records the ledger high-water mark at refusal time");
  assert.equal(secondReceipt.source, "claude_code");
  assert.equal(secondReceipt.kind, "user_prompt_submit");
  assert.equal(reconcileMaintenanceRebuildRefusals(home).count, 1,
    "an older row with the same ID, source and kind cannot settle a new refusal");
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
