import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-null-generation-privacy-"));
const ledger = path.join(root, "ledger.sqlite");
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
const id = "00000000-0000-4000-8000-000000004176";
const workspaceId = "null-generation-review";
const deviceId = "null-generation-device";
const baseRoot = process.env.PR417_BASE_WORKTREE;
const SeedBuffer = baseRoot
  ? require(path.join(baseRoot, "packages/collector-cli/src/buffer.ts"))
      .LocalEventBuffer as typeof LocalEventBuffer
  : LocalEventBuffer;
const event = aiInteractionEventSchema.parse({ id, sessionId: id,
  source: "codex", eventType: "assistant_response", dataMode: "metadata",
  observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });

try {
  const seed = new SeedBuffer(ledger, {
    workspaceId, deviceId, delivery: { enabled: false },
    enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000),
  });
  try {
    const db = seed.database;
    db.exec("drop trigger trg_events_privacy_generation_insert");
    db.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id)
      values (?,'codex','assistant_response','metadata',?,?,?,?,?)`)
      .run(id, oldAt, JSON.stringify(event), oldAt, workspaceId, deviceId);
    assert.equal((db.prepare("select privacy_generation as g from buffered_events where id=?")
      .get(id) as { g: string | null }).g, null);
  } finally { seed.close(); }

  const old = new Database(ledger);
  try {
    if (!baseRoot) old.exec(`drop table upload_receipts;
      create table upload_receipts (
        delivery_id text primary key,
        terminal_state text not null check (terminal_state in ('acknowledged','dead')),
        reason text not null,status_class text not null,attempt_count integer not null,
        created_at text not null,terminal_at text not null
      )`);
    old.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_privacy_violation','local',0,?,?)`)
      .run(id, oldAt, now.toISOString());
    old.prepare(`update upload_control set privacy_migration_version=1,
      migration_cursor_rowid=0,migration_complete=0 where singleton=1`).run();
  } finally { old.close(); }

  const upgraded = new LocalEventBuffer(ledger, {
    workspaceId, deviceId, delivery: { enabled: true },
    enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000),
  });
  try {
    const db = upgraded.database;
    const before = db.prepare(`select raw_rowid as rowid,raw_generation as generation
      from upload_receipts where delivery_id=?`).get(id) as
      { rowid: number | null; generation: string | null };
    assert.ok(before.rowid !== null, "upgrade must bind the old terminal receipt");
    assert.equal(before.generation, null);
    const migration = upgraded.delivery.migrateLegacy({ maxRows: 10, now });
    const after = db.prepare(`select privacy_generation as generation,
      privacy_disposition as disposition from buffered_events where id=?`)
      .get(id) as { generation: string | null; disposition: string | null };
    const active = db.prepare(`select delivery_id as deliveryId from upload_outbox
      where raw_id=?`).get(id) as { deliveryId: string } | undefined;
    const visible = upgraded.listUnuploaded({ maxRows: 10 }).some((row) => row.id === id);
    const lease = upgraded.delivery.lease({ now: new Date(now.getTime() + 3_600_000) });
    const observation = { before, migration, after, active: active?.deliveryId ?? null,
      visible, leasable: lease.items.map((item) => item.deliveryId) };
    console.log(JSON.stringify(observation));
    assert.equal(active, undefined,
      "a pre-upgrade local privacy rejection must not acquire a new delivery ID");
    assert.equal(visible, false,
      "a pre-upgrade local privacy rejection must stay excluded from local upload reads");
    assert.equal(lease.items.length, 0,
      "a pre-upgrade local privacy rejection must never become leasable");
  } finally { upgraded.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
