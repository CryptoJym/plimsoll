import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

if (process.env.PR417_CRASH_LEDGER) {
  const buffer = new LocalEventBuffer(process.env.PR417_CRASH_LEDGER, {
    delivery: { enabled: false },
    onOpenStep: (step) => {
      if (step.step === "ledger.core_schema") process.stdout.write("CORE_SCHEMA\n");
    },
  });
  process.stdout.write("MIGRATED\n");
  buffer.close();
} else {
  async function reviewMain() {
  const baseRoot = process.env.PR417_BASE_WORKTREE;
  assert.ok(baseRoot, "set PR417_BASE_WORKTREE to exact 0.7.44");
  const OldBuffer = require(path.join(baseRoot,
    "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-receipt-crash-"));
  const ledger = path.join(root, "ledger.sqlite");
  const rows = 400_000;
  let child: ReturnType<typeof spawn> | undefined;
  try {
    const old = new OldBuffer(ledger, { delivery: { enabled: false } });
    try {
      const insert = old.database.prepare(`insert into raw_retention_receipts
        (event_id,raw_rowid,raw_created_at,raw_generation,expired_at,reason)
        values (?,?,?,?,?,'retention_window_elapsed')`);
      old.database.transaction(() => {
        for (let i = 0; i < rows; i++) {
          insert.run(`receipt-${String(i).padStart(7, "0")}`, i + 1,
            "2026-01-01T00:00:00.000Z", null, "2026-06-01T00:00:00.000Z");
        }
      })();
    } finally { old.close(); }

    const run = async () => {
      let output = "";
      let exited = false;
      child = spawn(process.execPath, ["--import", "tsx", __filename], {
        env: { ...process.env, PR417_CRASH_LEDGER: ledger },
        stdio: ["ignore", "pipe", "pipe"],
      });
      child.stdout?.on("data", (chunk) => { output += String(chunk); });
      child.stderr?.on("data", (chunk) => { output += String(chunk); });
      child.on("exit", () => { exited = true; });
      const deadline = Date.now() + 30_000;
      let walBytes = 0;
      while (Date.now() < deadline) {
        try { walBytes = fs.statSync(`${ledger}-wal`).size; } catch { walBytes = 0; }
        if (output.includes("CORE_SCHEMA") && walBytes > 2_000_000 &&
            !output.includes("MIGRATED") && !exited) break;
        if (exited) throw new Error(`migration child exited before injected crash: ${output}`);
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.ok(output.includes("CORE_SCHEMA") && walBytes > 2_000_000 &&
        !output.includes("MIGRATED") && !exited,
      `could not intercept active receipt migration: bytes=${walBytes} output=${output}`);
      const killedPid = child.pid;
      assert.ok(killedPid);
      child.kill("SIGKILL");
      await new Promise<void>((resolve) => child!.once("exit", () => resolve()));
      console.log(JSON.stringify({ phase: "crash", killedPid, walBytes, rows }));

      const recovered = new Database(ledger);
      try {
        assert.equal(recovered.pragma("integrity_check", { simple: true }), "ok");
        const columns = recovered.pragma("table_info(raw_retention_receipts)") as
          Array<{ name: string; pk: number }>;
        assert.equal(columns.find((c) => c.name === "event_id")?.pk, 1,
          "a killed migration must roll back the whole receipts rebuild");
        assert.equal((recovered.prepare(`select count(*) as n from raw_retention_receipts`)
          .get() as { n: number }).n, rows);
        assert.equal(recovered.prepare(`select 1 from sqlite_master where type='table'
          and name='raw_retention_receipts_by_incarnation'`).get(), undefined);
      } finally { recovered.close(); }

      const reupgraded = new LocalEventBuffer(ledger, { delivery: { enabled: false } });
      try {
        const columns = reupgraded.database.pragma("table_info(raw_retention_receipts)") as
          Array<{ name: string; pk: number }>;
        assert.equal(columns.find((c) => c.name === "event_id")?.pk, 0);
        assert.ok(reupgraded.database.prepare(`select 1 from sqlite_master where type='index'
          and name='idx_raw_retention_incarnation'`).get());
        assert.equal((reupgraded.database.prepare(`select count(*) as n from raw_retention_receipts`)
          .get() as { n: number }).n, rows);
        console.log(JSON.stringify({ phase: "restart", rows,
          integrity: reupgraded.database.pragma("integrity_check", { simple: true }) }));
      } finally { reupgraded.close(); }
    };
    await run();
  } finally {
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await new Promise<void>((resolve) => child!.once("exit", () => resolve()));
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
  }
  reviewMain().catch((error) => { console.error(error); process.exitCode = 1; });
}
