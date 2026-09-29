/** The same caller body under a different configured tenant is not A's retry. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { DEFAULT_POLICY } from "../packages/shared/src/index";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r7-tenant-")));
const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
try {
  const body = { id: randomUUID(), session_id: randomUUID(), hook_event_name: "UserPromptSubmit",
    timestamp: new Date().toISOString(), input_tokens: 5 };
  const tenantA = collectorConfigSchema.parse({ tenantId: "tenant-a",
    policy: { ...DEFAULT_POLICY, tenantId: "tenant-a" } });
  const tenantB = collectorConfigSchema.parse({ tenantId: "tenant-b",
    policy: { ...DEFAULT_POLICY, tenantId: "tenant-b" } });
  markMaintenanceRebuildPause(home);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", JSON.stringify(body),
    { config: tenantA, spoolName: "1790662638182-1000-abcdef.json" });
  finishMaintenanceRebuildPause(home);
  const admitted = appendForwardedHook(body, { config: tenantB, source: "claude_code", buffer });
  const row = buffer.database.prepare("select payload_json from buffered_events where id=?")
    .get(body.id) as { payload_json: string };
  const stored = JSON.parse(row.payload_json) as { tenantId: string };
  const state = reconcileMaintenanceRebuildRefusals(home);
  console.log(JSON.stringify({ check: "cross_tenant_same_body", admittedTenant: admitted.event.tenantId,
    storedTenant: stored.tenantId, refusedTenant: tenantA.tenantId, pending: state.count,
    unverified: state.unverifiedHookRetries }));
  assert.equal(admitted.event.tenantId, tenantB.tenantId);
  assert.equal(stored.tenantId, tenantB.tenantId);
  assert.equal(state.count, 1, "tenant B's event must not settle tenant A's refused hook");
  // The same authority check must apply to a post-refusal, digestless
  // 0.7.44-shaped row. Its newer sequence alone cannot prove tenant A's retry.
  buffer.database.prepare("delete from buffered_events where id=?").run(body.id);
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id)
    values (?,'claude_code','user_prompt_submit','safe',?,?,?,?)`)
    .run(body.id, body.timestamp, JSON.stringify({ tenantId: tenantB.tenantId }),
      body.timestamp, body.session_id);
  const receiptFile = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { at: string };
  const fallback = reconcileMaintenanceRebuildRefusals(home, path.join(home, "ledger.sqlite"),
    Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 1_000);
  console.log(JSON.stringify({ check: "cross_tenant_digestless_fallback", fallback }));
  assert.equal(fallback.count, 0);
  assert.equal(fallback.unverifiedHookRetries, 0);
  assert.equal(fallback.lost.length, 1, "the foreign-tenant row cannot prove this retry");
  assert.ok(fs.existsSync(receiptFile), "a foreign-tenant row must not retire the receipt");
} finally {
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
