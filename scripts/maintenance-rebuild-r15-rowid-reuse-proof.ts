/** A 0.7.44-shaped drain can reuse the refusal's max rowid after retention. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DEFAULT_POLICY } from "../packages/shared/src/index";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r7-rowid-reuse-")));
const ledger = path.join(home, "ledger.sqlite");
const buffer = new LocalEventBuffer(ledger);
try {
  const now = new Date().toISOString();
  const filler = randomUUID();
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id)
    values (?,'claude_code','session_start','safe',?,'{}','2020-01-01T00:00:00.000Z',?)`)
    .run(filler, now, randomUUID());
  const oldRowid = (buffer.database.prepare("select rowid from buffered_events where id=?")
    .get(filler) as { rowid: number }).rowid;
  const body = JSON.stringify({ id: randomUUID(), session_id: randomUUID(),
    hook_event_name: "UserPromptSubmit", timestamp: now, input_tokens: 4 });
  markMaintenanceRebuildPause(home);
  const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body,
    cause: "maintenance_rebuild" });
  assert.equal(saved.ok, true);
  recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body,
    { spoolName: path.basename(saved.path) });
  const receiptPath = path.join(home, "maintenance-rebuild-refusals",
    fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
  const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as
    { at: string; ledgerHighWater: number; eventId: string; sessionId: string };
  finishMaintenanceRebuildPause(home);
  const pruned = buffer.prune(1, { now: new Date("2026-09-29T00:00:00.000Z") });
  assert.equal(pruned.events, 1);
  // This is the 0.7.44 INSERT shape: no maintenance_hook_body_digest column.
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id)
    values (?,'claude_code','user_prompt_submit','safe',?,?,?,?)`)
    .run(receipt.eventId, now, JSON.stringify({ tenantId: DEFAULT_POLICY.tenantId }),
      now, receipt.sessionId);
  const digestColumn = buffer.database.prepare(`select 1 from pragma_table_info('buffered_events')
    where name='maintenance_hook_body_digest'`).get();
  const admitted = buffer.database.prepare(digestColumn
    ? `select rowid,maintenance_hook_body_digest as digest from buffered_events where id=?`
    : `select rowid,null as digest from buffered_events where id=?`)
    .get(receipt.eventId) as { rowid: number; digest: string | null };
  fs.unlinkSync(saved.path);
  const at = Date.parse(receipt.at);
  const first = reconcileMaintenanceRebuildRefusals(home, ledger, at + MISSING_HOOK_RETRY_MS + 1000);
  const muchLater = reconcileMaintenanceRebuildRefusals(home, ledger,
    at + 365 * 24 * 60 * 60 * 1000);
  const spool = captureSpoolState(home);
  console.log(JSON.stringify({ check: "legacy_rowid_reuse_after_retention", oldRowid,
    refusalHighWater: receipt.ledgerHighWater, admittedRowid: admitted.rowid,
    admittedDigest: admitted.digest, spoolExists: fs.existsSync(saved.path),
    first, muchLater, maintenanceRebuildPending: spool.maintenanceRebuildPending }));
  assert.equal(oldRowid, receipt.ledgerHighWater);
  assert.equal(admitted.rowid, oldRowid);
  assert.equal(admitted.digest, null);
  assert.equal(muchLater.count, 0, "a consumed old-binary retry must not hold capture forever");
} finally {
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
