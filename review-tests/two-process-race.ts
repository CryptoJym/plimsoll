import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const options = { workspaceId: "race-workspace", deviceId: "race-device",
  delivery: { enabled: true } };

async function child(ledger: string, ready: string, start: string, nowIso: string) {
  const buffer = new LocalEventBuffer(ledger, options);
  try {
    fs.writeFileSync(ready, "ready");
    const deadline = Date.now() + 15_000;
    while (!fs.existsSync(start) && Date.now() < deadline) await sleep(10);
    assert.ok(fs.existsSync(start), "parent did not release the race barrier");
    const pass = buffer.prune(30, { maxRows: 10, now: new Date(nowIso) });
    console.log(JSON.stringify({ phase: "child_prune", pass }));
  } finally { buffer.close(); }
}

async function parent() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-two-process-race-"));
  const ledger = path.join(root, "ledger.sqlite");
  const ready = path.join(root, "ready");
  const start = path.join(root, "start");
  const now = new Date();
  const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
  const ids = ["00000000-0000-4000-8000-000000004171",
    "00000000-0000-4000-8000-000000004172"];
  const buffer = new LocalEventBuffer(ledger, { ...options,
    delivery: { enabled: false },
    enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) });
  let worker: ChildProcess | null = null;
  try {
    const db = buffer.database;
    for (const id of ids) {
      const event = aiInteractionEventSchema.parse({ id, sessionId: id,
        source: "codex", eventType: "assistant_response", dataMode: "metadata",
        observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });
      assert.equal(buffer.append(event), true);
      db.prepare("update buffered_events set created_at=? where id=?").run(oldAt, id);
    }
    buffer.delivery.configure({ enabled: true });
    for (const id of ids) assert.equal(buffer.delivery.repairRawById(id).enqueued, 1);

    let stdout = "";
    let stderr = "";
    worker = spawn(process.execPath, ["--import", "tsx", process.argv[1]!,
      "child", ledger, ready, start, now.toISOString()], { cwd: process.cwd() });
    worker.stdout?.on("data", (chunk) => { stdout += String(chunk); });
    worker.stderr?.on("data", (chunk) => { stderr += String(chunk); });
    const deadline = Date.now() + 15_000;
    while (!fs.existsSync(ready) && worker.exitCode === null && Date.now() < deadline)
      await sleep(10);
    assert.ok(fs.existsSync(ready), `child did not open: ${stderr}`);

    db.exec("begin immediate");
    try {
      db.prepare("update buffered_events set uploaded_at=? where id=?")
        .run(now.toISOString(), ids[0]);
      db.prepare(`insert into upload_receipts
        (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
         terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
        select delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
          'acknowledged','uploaded','success',1,created_at,?
        from upload_outbox where raw_id=?`).run(now.toISOString(), ids[0]);
      db.prepare("delete from upload_outbox where raw_id=?").run(ids[0]);
      fs.writeFileSync(start, "start");
      await sleep(200);
      db.exec("commit");
    } catch (error) {
      db.exec("rollback");
      throw error;
    }

    const code = worker.exitCode !== null ? worker.exitCode :
      await new Promise<number | null>((resolve) => worker!.once("exit", resolve));
    console.log(stdout.trim());
    assert.equal(code, 0, stderr);
    const rawIds = (db.prepare("select id from buffered_events order by id")
      .all() as Array<{ id: string }>).map((row) => row.id);
    const outbox = (db.prepare("select count(*) as n from upload_outbox")
      .get() as { n: number }).n;
    const expired = (db.prepare("select count(*) as n from raw_retention_receipts")
      .get() as { n: number }).n;
    assert.deepEqual(rawIds, [ids[1]]);
    assert.equal(outbox, 1);
    assert.equal(expired, 1);
    console.log(JSON.stringify({ phase: "after_race", rawIds, outbox, expired,
      blockedWriterMs: 200 }));
  } finally {
    if (worker && worker.exitCode === null) worker.kill();
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

const run = process.argv[2] === "child"
  ? child(process.argv[3]!, process.argv[4]!, process.argv[5]!, process.argv[6]!)
  : parent();
run.catch((error) => { console.error(error); process.exitCode = 1; });
