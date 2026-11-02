import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("ledger_compact", 12);
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { acquireLedgerConnectionLock } from "../packages/collector-cli/src/ledger-connection";
import { compactLedger, planLedgerCompaction } from "../packages/collector-cli/src/ledger-compact";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

/**
 * 0.7.52 `plimsoll ledger compact`: an offline VACUUM INTO of the stopped
 * ledger, verified by row counts, refused while a collector connection is
 * open or when the destination is unsafe. The source is never modified.
 */

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-ledger-compact-"));
fs.chmodSync(root, 0o700);
const sha256 = (file: string) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const tableCounts = (db: Database.Database) => {
  const tables = (db.prepare(`select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name`)
    .all() as Array<{ name: string }>).map((row) => row.name);
  return Object.fromEntries(tables.map((table) =>
    [table, (db.prepare(`select count(*) as n from "${table}"`).get() as { n: number }).n]));
};

try {
  const ledgerDir = path.join(root, "home");
  fs.mkdirSync(ledgerDir, { mode: 0o700 });
  const ledger = path.join(ledgerDir, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger);
  for (let index = 0; index < 200; index += 1) {
    buffer.append(aiInteractionEventSchema.parse({
      id: `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
      source: "codex", dataMode: "metadata", eventType: "assistant_response",
      observedAt: new Date(Date.UTC(2026, 9, 1, 0, 0, index)).toISOString(),
      inputTokens: 1, outputTokens: 1, sessionId: "compact-session", actionClass: "other",
    }));
  }
  const db = buffer.database;
  // Bloat the file with pages that are then freed: 256 MiB of zero blobs.
  db.exec("create table proof_pad (x blob)");
  const pad = db.prepare("insert into proof_pad values (zeroblob(1048576))");
  db.transaction(() => { for (let index = 0; index < 256; index += 1) pad.run(); })();
  db.exec("delete from proof_pad; drop table proof_pad;");
  db.pragma("wal_checkpoint(TRUNCATE)");
  const sourceCounts = tableCounts(db);
  buffer.close();
  const bloatedBytes = fs.statSync(ledger).size;
  const bloatedDigest = sha256(ledger);
  completion.check("fixture_holds_freed_pages", bloatedBytes > 200 * 1024 * 1024);

  const outDir = path.join(root, "out");
  fs.mkdirSync(outDir, { mode: 0o700 });
  const into = path.join(outDir, "work-ledger.compact.sqlite");

  // 1. A collector connection holds the opener barrier: refused, nothing written.
  const held = acquireLedgerConnectionLock(ledger, "shared");
  try {
    const plan = planLedgerCompaction({ ledgerPath: ledger, intoPath: into });
    assert.equal(plan.status, "refused");
    assert.match(plan.reason ?? "", /^ledger_in_use/);
    const attempt = compactLedger({ ledgerPath: ledger, intoPath: into });
    assert.equal(attempt.status, "refused");
    assert.match(attempt.reason ?? "", /^ledger_in_use/);
    assert.equal(fs.existsSync(into), false);
  } finally { held.release(); }
  completion.check("refuses_while_a_collector_connection_is_open", true);

  // 2. An existing destination is refused.
  fs.writeFileSync(into, "", { mode: 0o600 });
  const exists = planLedgerCompaction({ ledgerPath: ledger, intoPath: into });
  assert.equal(exists.status, "refused");
  assert.equal(exists.reason, "destination_exists:work-ledger.compact.sqlite");
  fs.unlinkSync(into);
  completion.check("refuses_an_existing_destination", true);

  // 3. A destination directory others can read is refused.
  const open = path.join(root, "open");
  fs.mkdirSync(open, { mode: 0o755 });
  fs.chmodSync(open, 0o755);
  const unsafe = planLedgerCompaction({ ledgerPath: ledger, intoPath: path.join(open, "copy.sqlite") });
  assert.equal(unsafe.status, "refused");
  assert.match(unsafe.reason ?? "", /^destination_directory_unsafe/);
  completion.check("refuses_a_destination_directory_that_is_not_private", true);

  // 4. The ledger's own directory and relative paths are refused.
  const sameDir = planLedgerCompaction({ ledgerPath: ledger, intoPath: path.join(ledgerDir, "copy.sqlite") });
  assert.equal(sameDir.reason, "destination_must_leave_ledger_directory");
  const relative = planLedgerCompaction({ ledgerPath: ledger, intoPath: "relative/copy.sqlite" });
  assert.equal(relative.reason, "paths_must_be_absolute");
  completion.check("refuses_the_ledger_directory_and_relative_paths", true);

  // 5. The plan reads page counts and free space; the copy is expected small.
  const plan = planLedgerCompaction({ ledgerPath: ledger, intoPath: into });
  assert.equal(plan.status, "ready");
  assert.ok((plan.freelistPages ?? 0) > 0, "freelist pages");
  assert.ok((plan.liveBytesEstimate ?? Infinity) < bloatedBytes / 4, `live ${plan.liveBytesEstimate} of ${bloatedBytes}`);
  assert.ok((plan.destinationFreeBytes ?? 0) >= (plan.requiredFreeBytes ?? Infinity), "free space");
  assert.equal(plan.sameDevice, true);
  completion.check("plan_reads_page_counts_and_free_space", true);

  // 6. Compact and verify by row counts.
  const steps: string[] = [];
  const receipt = compactLedger({ ledgerPath: ledger, intoPath: into, onProgress: (step) => steps.push(step) });
  assert.equal(receipt.status, "compacted", receipt.reason ?? "");
  assert.equal(receipt.verification.mode, "counts");
  assert.equal(receipt.verification.passed, true);
  assert.deepEqual(receipt.verification.mismatches, []);
  assert.ok((receipt.verification.tables ?? 0) >= 10, "tables compared");
  assert.ok((receipt.compactedBytes ?? Infinity) < bloatedBytes / 4, `compacted ${receipt.compactedBytes} of ${bloatedBytes}`);
  assert.equal(receipt.reclaimableBytes, bloatedBytes - (receipt.compactedBytes ?? 0));
  assert.ok(steps.includes("vacuum_into") && steps.includes("verify"));
  assert.ok(receipt.swap.length >= 4 && receipt.undo.length === 3);
  assert.equal(fs.statSync(into).mode & 0o777, 0o600);
  completion.check("compacts_and_verifies_row_counts", true);

  // 7. The receipt file sits next to the copy, private, and says the same.
  assert.equal(receipt.receiptPath, `${into}.compaction-receipt.json`);
  assert.equal(fs.statSync(receipt.receiptPath!).mode & 0o777, 0o600);
  const written = JSON.parse(fs.readFileSync(receipt.receiptPath!, "utf8"));
  assert.equal(written.status, "compacted");
  assert.equal(written.compactedBytes, receipt.compactedBytes);
  completion.check("writes_a_private_receipt_next_to_the_copy", true);

  // 8. Independent check: every table's rows are in the copy.
  const copy = new Database(into, { readonly: true, fileMustExist: true });
  try {
    assert.deepEqual(tableCounts(copy), sourceCounts);
    assert.equal((copy.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, 200);
  } finally { copy.close(); }
  completion.check("every_table_row_is_in_the_copy", true);

  // 9. The source is untouched.
  assert.equal(fs.statSync(ledger).size, bloatedBytes);
  assert.equal(sha256(ledger), bloatedDigest);
  completion.check("source_ledger_is_unchanged", true);

  // 10. quick_check and none verification modes.
  const quick = compactLedger({ ledgerPath: ledger, intoPath: path.join(outDir, "quick.sqlite"), verify: "quick_check" });
  assert.equal(quick.status, "compacted");
  assert.deepEqual({ passed: quick.verification.passed, detail: quick.verification.detail }, { passed: true, detail: "ok" });
  const none = compactLedger({ ledgerPath: ledger, intoPath: path.join(outDir, "none.sqlite"), verify: "none" });
  assert.equal(none.status, "compacted");
  assert.equal(none.verification.passed, null);
  completion.check("quick_check_and_none_verification_modes", true);

  // 11. The compacted copy opens as a collector ledger and answers status.
  const swapped = path.join(path.join(root, "swapped"), "work-ledger.sqlite");
  fs.mkdirSync(path.dirname(swapped), { mode: 0o700 });
  fs.copyFileSync(into, swapped);
  fs.chmodSync(swapped, 0o600);
  const reopened = new LocalEventBuffer(swapped);
  try {
    assert.equal((reopened.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, 200);
    assert.equal(String(reopened.database.pragma("journal_mode", { simple: true })), "wal");
    assert.equal(reopened.retentionProgressStatus(90).policy.retentionDays, 90);
  } finally { reopened.close(); }
  completion.check("compacted_copy_opens_as_a_collector_ledger", true);

  console.log(JSON.stringify({
    proof: "ledger_compact", bloatedBytes, compactedBytes: receipt.compactedBytes,
    reclaimableBytes: receipt.reclaimableBytes, tables: receipt.verification.tables, elapsedMs: receipt.elapsedMs,
  }));
  completion.complete();
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
