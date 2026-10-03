/** A prior different event sharing ID, source, kind and session must not settle a later refusal. */
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

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r6-prior-same-session-")));
let buffer: LocalEventBuffer | null = null;
try {
  buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const config = collectorConfigSchema.parse({});
  const id = randomUUID();
  const session_id = randomUUID();
  const oldBody = { id, hook_event_name: "UserPromptSubmit", session_id,
    timestamp: new Date(Date.now() - 20_000).toISOString(), input_tokens: 1 };
  const refusedBody = { ...oldBody, timestamp: new Date().toISOString(), input_tokens: 20 };
  const first = appendForwardedHook(oldBody, { config, source: "claude_code", buffer });
  assert.equal(first.event.id, id);
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(refusedBody));
  finishMaintenanceRebuildPause(home);
  const before = reconcileMaintenanceRebuildRefusals(home);
  console.log(JSON.stringify({ check: "prior_different_event_before_retry", before,
    oldTimestamp: oldBody.timestamp, refusedTimestamp: refusedBody.timestamp }));
  assert.equal(before.count, 1, "the prior row must not clear the new refusal before retry");
  assert.equal(before.unverifiedHookRetries, 0,
    "a row at or below the refusal high-water mark cannot retire unverified");
  const attempted = appendForwardedHook(refusedBody, { config, source: "claude_code", buffer });
  const rows = buffer.database.prepare("select id,observed_at,input_tokens from buffered_events where id=?")
    .all(id) as Array<{ id: string; observed_at: string; input_tokens: number }>;
  const after = reconcileMaintenanceRebuildRefusals(home);
  console.log(JSON.stringify({ check: "prior_different_event_same_session", before, after,
    deduplicated: attempted.deduplicated,
    collisionQuarantined: attempted.collisionQuarantined, rows }));
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.input_tokens, 1, "the refused event was not admitted");
  assert.equal(after.count, 1, "an unrelated older row cannot retire the refusal");
} finally {
  buffer?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
