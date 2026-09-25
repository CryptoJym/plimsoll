import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import Database from "better-sqlite3";

import {
  BUDGET_RING_LIMIT,
  collectBudgetSample,
  ensureBudgetSchema,
  budgetStatus,
  budgetCsv,
  recordBudgetSample,
  recordDailyTableSizes,
  BudgetSampler,
} from "../packages/collector-cli/src/budget-sampler";

const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-budget-"));
const file = path.join(root, "ledger.sqlite");
const db = new Database(file);
db.pragma("journal_mode = WAL");
db.exec(`
  create table buffered_events(id text primary key, created_at text not null);
  create table upload_control(singleton integer primary key, active_pending integer,
    active_retry integer, active_in_flight integer, active_oldest_created_at text,
    receipt_dead integer);
  insert into upload_control values(1,2,1,0,'2026-09-25T11:00:00.000Z',2);
  create table upload_receipts(delivery_id text primary key, terminal_state text,
    created_at text, terminal_at text);
  create index idx_upload_receipts_state on upload_receipts(terminal_state);
  insert into upload_receipts values('a','dead','2026-09-25T10:00:00.000Z','2026-09-25T11:00:00.000Z');
  insert into upload_receipts values('b','dead','2026-09-25T12:00:00.000Z','2026-09-25T12:30:00.000Z');
  create table maintenance_state(key text primary key, value text);
  insert into maintenance_state values('session_sync_daemon_v1',
    '{"lastSuccessfulUntil":"2026-09-25T12:00:00.000Z"}');
  insert into buffered_events values('first','2026-09-25T13:00:00.000Z');
`);

async function main() {
  try {
    ensureBudgetSchema(db);
    db.prepare("insert into buffered_events values (?, ?)").run("second", "2026-09-25T13:10:00.000Z");
    assert.equal((db.prepare("select attempted_rows as n from budget_daily where day='2026-09-25'").get() as {n:number}).n, 1);
    assert.equal(await recordDailyTableSizes(db, file, "2026-09-25"), "measured");
    const pages = JSON.parse((db.prepare("select table_pages_json as pages from budget_daily where day='2026-09-25'").get() as {pages:string}).pages) as Record<string,number>;
    const actualPages = (db.prepare("select count(*) as n from dbstat s join sqlite_master m on m.name=s.name where m.tbl_name='buffered_events'").get() as {n:number}).n;
    assert.equal(pages.buffered_events, actualPages);

    const child = spawn(process.execPath, ["-e", "const a=Buffer.alloc(16*1024*1024);let n=0;while(n<2e8){n++}setTimeout(()=>process.exit(0),5000)", "__maintenance_worker"], { stdio: "ignore" });
    await new Promise((resolve) => setTimeout(resolve, 350));
    const first = await collectBudgetSample(db, file, {
      nowMs: Date.parse("2026-09-25T13:30:00.000Z"),
      processPid: process.pid,
    });
    assert.equal(first.dbBytes, fs.statSync(file).size);
    assert.equal(first.outboxPending, 3);
    assert.equal(first.outboxDead, 2);
    assert.equal(first.outboxOldestPendingAgeSeconds, 9000);
    assert.equal(first.outboxOldestDeadAgeSeconds, 12600);
    assert.equal(first.summaryLagSeconds, 4200);
    assert.ok(first.processes.some((entry) => entry.role === "maintenance" && entry.pid === child.pid), "maintenance child observed separately");
    await new Promise((resolve) => setTimeout(resolve, 250));
    const second = await collectBudgetSample(db, file, {
      nowMs: Date.parse("2026-09-25T13:31:00.000Z"), processPid: process.pid,
      previous: first,
    });
    assert.ok(second.processes.find((entry) => entry.pid === process.pid)?.cpuDeltaMs !== null);
    child.kill();

    const sampler = new BudgetSampler(db, file, 60_000);
    sampler.start();
    for (let i = 0; i < 40 && sampler.status().latest === null; i++) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.ok(sampler.status().latest, "timer writes an initial sample without a request");
    sampler.stop();

    for (let i = 0; i < 10_000; i++) {
      recordBudgetSample(db, { ...first, atMs: first.atMs + i * 60_000, dbBytes: i });
    }
    assert.equal((db.prepare("select count(*) as n from budget_samples").get() as {n:number}).n, BUDGET_RING_LIMIT);
    assert.ok((db.prepare("select count(*) as n from budget_daily").get() as {n:number}).n <= 8);
    const status = budgetStatus(db, Date.parse("2026-10-02T13:30:00.000Z"));
    assert.equal(status.mode, "advisory");
    assert.equal(status.latest?.dbBytes, 9999);
    assert.ok(status.p50?.dbBytes !== null && status.p95?.dbBytes !== null);
    assert.equal(budgetCsv(db).trim().split("\n").length, BUDGET_RING_LIMIT + 1);

    const huge = path.join(root, "huge.sqlite");
    fs.closeSync(fs.openSync(huge, "w"));
    fs.truncateSync(huge, 2 * 1024 ** 3 + 1);
    let called = false;
    const daily = await recordDailyTableSizes(db, huge, "2026-09-25", async () => { called = true; return []; });
    assert.equal(daily, "skipped: size");
    assert.equal(called, false);
    console.log("budget sampler fixture: PASS (file sizes, process RSS/CPU, outbox ages, summary lag, 10k ring, dbstat size gate)");
  } finally {
    db.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
