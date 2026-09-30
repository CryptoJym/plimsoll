import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const oldAt = "2026-01-01T00:00:00.000Z";
const now = new Date("2026-09-28T00:00:00.000Z");
const id = "00000000-0000-4000-8000-000000004173";
const options = { workspaceId: "crash-upgrade", deviceId: "crash-device",
  delivery: { enabled: true } };

function seedOldReceipt(ledger: string) {
  const seed = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: false } });
  try {
    seed.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id)
      values (?,'codex','assistant_response','metadata',?,'{}',?,?,?)`)
      .run(id, oldAt, oldAt, options.workspaceId, options.deviceId);
  } finally { seed.close(); }
  const db = new Database(ledger);
  try {
    db.exec(`drop table upload_receipts;
      create table upload_receipts (
        delivery_id text primary key,
        terminal_state text not null check (terminal_state in ('acknowledged','dead')),
        reason text not null,status_class text not null,attempt_count integer not null,
        created_at text not null,terminal_at text not null
      )`);
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_schema_invalid','local',0,?,?)`)
      .run(id, oldAt, now.toISOString());
    db.prepare(`update upload_control set privacy_migration_version=1,
      migration_cursor_rowid=(select max(rowid) from buffered_events),
      migration_complete=1 where singleton=1`).run();
  } finally { db.close(); }
}

function crashChild(ledger: string, step: string) {
  new LocalEventBuffer(ledger, { ...options,
    onOpenStep: (item) => { if (item.step === step) process.exit(71); } });
  throw new Error(`open never reached ${step}`);
}

function parent() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-upgrade-crash-"));
  const steps = ["ledger.privacy_schema", "ledger.delivery_schema",
    "ledger.workspace_binding", "ledger.raw_indexes_and_triggers",
    "ledger.receipt_lineage_slice"];
  try {
    for (const step of steps) {
      const ledger = path.join(root, `${step.replaceAll(".", "-")}.sqlite`);
      seedOldReceipt(ledger);
      const result = spawnSync(process.execPath, ["--import", "tsx", process.argv[1]!,
        "child", ledger, step], { cwd: process.cwd(), encoding: "utf8", timeout: 15_000 });
      assert.equal(result.status, 71, `${step}: ${result.stderr || result.error}`);
      const reopened = new LocalEventBuffer(ledger, options);
      try {
        const db = reopened.database;
        const lineage = db.prepare(`select raw_rowid as rawRowid,raw_id as rawId
          from upload_receipts where delivery_id=?`).get(id) as
          { rawRowid: number | null; rawId: string | null };
        const raw = db.prepare("select rowid as rowid from buffered_events where id=?")
          .get(id) as { rowid: number };
        assert.equal(lineage.rawRowid, raw.rowid);
        assert.equal(lineage.rawId, id);
        assert.equal(reopened.retentionStatus(30, now).states.heldForUpload, 0);
        const pass = reopened.prune(30, { maxRows: 10, now });
        assert.equal(pass.events, 1);
        assert.equal(Boolean(db.prepare("select 1 from buffered_events where id=?").get(id)), false);
        console.log(JSON.stringify({ crashAfter: step, childExit: result.status,
          lineageBound: true, expiredAfterRestart: pass.events }));
      } finally { reopened.close(); }
    }
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}

try {
  if (process.argv[2] === "child") crashChild(process.argv[3]!, process.argv[4]!);
  else parent();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
