/** A torn terminal append must not swallow the next durable terminal record. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, reconcileMaintenanceRebuildRefusals,
  resolveMaintenanceRebuildRefusal } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

function scenario(partial: boolean) {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r3-terminal-")));
  const originalUnlink = fs.unlinkSync;
  try {
    const db = new Database(path.join(home, "ledger.sqlite"));
    db.exec("create table buffered_events (id text primary key)");
    db.close();
    const body = JSON.stringify({ id: randomUUID(), hook_event_name: "UserPromptSubmit" });
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", body);
    finishMaintenanceRebuildPause(home);
    const directory = path.join(home, "maintenance-rebuild-refusals");
    const receipt = path.join(directory, fs.readdirSync(directory)[0]!);
    const journal = path.join(home, "maintenance-rebuild-terminal.jsonl");
    if (partial) fs.writeFileSync(journal, '{"version":1,"receipt":"torn', { mode: 0o600 });
    let injected = false;
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = ((file: fs.PathLike) => {
      if (String(file) === receipt && !injected) {
        injected = true;
        throw Object.assign(new Error("injected_crash_before_receipt_unlink"), { code: "EIO" });
      }
      return originalUnlink(file);
    }) as typeof fs.unlinkSync;
    assert.throws(() => resolveMaintenanceRebuildRefusal(home, "hook", "claude_code", body,
      { outcome: "terminal" }), /injected_crash_before_receipt_unlink/);
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = originalUnlink;
    const result = reconcileMaintenanceRebuildRefusals(home);
    const remains = fs.existsSync(receipt);
    const journalLines = fs.readFileSync(journal, "utf8").trimEnd().split("\n");
    const records = journalLines.map((line) => JSON.parse(line) as { event?: string; outcome?: string });
    console.log(JSON.stringify({ check: "terminal_journal_crash_recovery", partial, injected,
      journal: fs.readFileSync(journal, "utf8"), result, remains, records }));
    assert.ok(injected);
    if (partial) assert.equal(records[0]?.event, "recovered_torn_tail",
      "repair leaves a durable recovery note on its own line");
    assert.equal(records.at(-1)?.outcome, "terminal", "the next durable record occupies its own line");
    assert.equal(result.count, 0, "the durable terminal record must settle the exact receipt");
    assert.equal(remains, false);
  } finally {
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = originalUnlink;
    fs.rmSync(home, { recursive: true, force: true });
  }
}
scenario(false);
scenario(true);
