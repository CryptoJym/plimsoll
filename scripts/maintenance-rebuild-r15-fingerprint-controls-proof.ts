/** Independent receipt matching controls for key order, privacy and identity. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { hookBodyDigest } from "../packages/collector-cli/src/maintenance-hook-fingerprint";
import { blankForbiddenRawContent } from "../packages/collector-cli/src/hook-spool-privacy";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

type Body = Record<string, unknown>;
function run(name: string, refused: Body, retry: Body, retrySource: "claude_code" | "codex",
  expectedPending: number) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), `r7-${name}-`)));
  let buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  try {
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(refused));
    finishMaintenanceRebuildPause(home);
    buffer.close();
    buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
    const admitted = appendForwardedHook(retry, { config: collectorConfigSchema.parse({}),
      source: retrySource, buffer });
    const row = buffer.database.prepare(`select maintenance_hook_body_digest as digest
      from buffered_events where id=?`).get(admitted.event.id) as { digest: string };
    assert.equal(row.digest, hookBodyDigest(retry), "admitted row keeps the original caller body digest");
    const pending = reconcileMaintenanceRebuildRefusals(home).count;
    console.log(JSON.stringify({ check: name, refusedDigest: hookBodyDigest(refused),
      retryDigest: hookBodyDigest(retry), refusedSource: "claude_code", retrySource,
      admittedId: admitted.event.id, pending }));
    assert.equal(pending, expectedPending);
  } finally {
    buffer.close();
    fs.rmSync(home, { recursive: true, force: true });
  }
}

const upperId = randomUUID().toUpperCase();
const upperSession = randomUUID().toUpperCase();
const now = new Date().toISOString();
const base: Body = { id: upperId, session_id: upperSession,
  hook_event_name: "UserPromptSubmit", timestamp: now, input_tokens: 4 };
const changedOrderAndCase: Body = { input_tokens: 4, timestamp: now,
  hook_event_name: "UserPromptSubmit", session_id: upperSession.toLowerCase(),
  id: upperId.toLowerCase() };
assert.equal(hookBodyDigest(base), hookBodyDigest(changedOrderAndCase));
run("top_level_key_order_uuid_case", base, changedOrderAndCase, "claude_code", 0);

const id2 = randomUUID();
const session2 = randomUUID();
const retained: Body = { id: id2, session_id: session2, hook_event_name: "UserPromptSubmit",
  timestamp: now, input_tokens: 4 };
const changedValue = { ...retained, input_tokens: 5 };
assert.notEqual(hookBodyDigest(retained), hookBodyDigest(changedValue));
run("retained_value_change", retained, changedValue, "claude_code", 1);

const privateA = { ...retained, id: randomUUID(), raw_prompt: "secret A" };
const privateB = { ...privateA, raw_prompt: "secret B" };
const blankedA = blankForbiddenRawContent(JSON.stringify(privateA));
const blankedB = blankForbiddenRawContent(JSON.stringify(privateB));
assert.ok(blankedA && blankedB && blankedA.blanked > 0 && blankedB.blanked > 0);
assert.equal(hookBodyDigest(privateA), hookBodyDigest(privateB));
run("privacy_blanked_value", privateA, privateB, "claude_code", 0);

const sourceBody = { ...retained, id: randomUUID() };
run("same_digest_different_source", sourceBody, sourceBody, "codex", 1);
