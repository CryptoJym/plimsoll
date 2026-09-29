/** Canonical session UUID identity can arrive in a nested OTLP attribute. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { hookBodyDigest } from "../packages/collector-cli/src/maintenance-hook-fingerprint";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r7-nested-uuid-")));
const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
try {
  const upper = "B3F1C2D4-5E6A-4B7C-8D9E-0F1A2B3C4D5E";
  const lower = upper.toLowerCase();
  const body = (session: string) => ({ id: "13f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e",
    hook_event_name: "UserPromptSubmit", timestamp: new Date("2026-09-29T08:00:00.000Z").toISOString(),
    otel: { attributes: [{ key: "gen_ai.session.id", value: { stringValue: session } }] } });
  const refused = body(upper);
  const retry = body(lower);
  const reordered = { otel: { attributes: [{ value: { stringValue: lower },
    key: "gen_ai.session.id" }] }, timestamp: refused.timestamp,
    hook_event_name: refused.hook_event_name, id: refused.id };
  assert.equal(hookBodyDigest(refused), hookBodyDigest(reordered),
    "nested key order and identity UUID case are canonical");
  assert.equal(hookBodyDigest(refused), hookBodyDigest(retry));
  const eventAttribute = { id: refused.id, otel: { attributes: [{
    key: "eventId", value: { stringValue: upper } }] } };
  const eventAttributeLower = { id: refused.id, otel: { attributes: [{
    value: { stringValue: lower }, key: "eventId" }] } };
  assert.equal(hookBodyDigest(eventAttribute), hookBodyDigest(eventAttributeLower),
    "nested event UUID authority also canonicalizes");
  assert.notEqual(hookBodyDigest({ id: refused.id, otel: { attributes: [{
    key: "other.value", value: { stringValue: upper } }] } }),
  hookBodyDigest({ id: refused.id, otel: { attributes: [{
    key: "other.value", value: { stringValue: lower } }] } }),
  "nonidentity retained attribute case remains exact");
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(refused));
  const file = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(file, "utf8")) as
    { sessionId: string | null; at: string };
  finishMaintenanceRebuildPause(home);
  const admitted = appendForwardedHook(retry, { config: collectorConfigSchema.parse({}),
    source: "claude_code", buffer });
  const state = reconcileMaintenanceRebuildRefusals(home, undefined,
    Date.parse(receipt.at) + 365 * 24 * 60 * 60 * 1000 + MISSING_HOOK_RETRY_MS);
  console.log(JSON.stringify({ check: "nested_session_uuid_case", receiptSession: receipt.sessionId,
    admittedSession: admitted.event.sessionId, sameCanonicalSession:
      receipt.sessionId?.toLowerCase() === admitted.event.sessionId?.toLowerCase(),
    refusedDigest: hookBodyDigest(refused), retryDigest: hookBodyDigest(retry), pending: state.count }));
  assert.equal(receipt.sessionId?.toLowerCase(), lower);
  assert.equal(admitted.event.sessionId?.toLowerCase(), lower);
  assert.equal(state.count, 0, "UUID case alone must not strand a matching retry");
} finally {
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
