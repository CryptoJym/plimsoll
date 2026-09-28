/** B22: the coverage walk cannot advance a file or source frontier past a failed gap. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { applyCaptureCoverage, beginCaptureCoverage, finishCaptureCoverage,
  type CaptureCoverageFile } from "../packages/collector-cli/src/capture-frontier";
import { captureFileKeyDigest, fileGapId } from "../packages/collector-cli/src/lean/capture-gaps";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(), "b22-frontier-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "00000000-0000-4000-8000-000000000022",
  delivery: { enabled: true },
  enrollmentNow: () => new Date(Date.now() - 86_400_000),
});
const db = buffer.database;
const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
const startedAt = new Date().toISOString();
const check = beginCaptureCoverage(db, "codex", startedAt)!;
const key = "a".repeat(64);
const unread = {
  key, mtimeMs: check.epochStartMs - 1000, birthtimeMs: check.epochStartMs - 2000,
  extent: 90, progress: -1, fullyRead: false, generationIdentity: "gen-1", unreadBytes: 90,
} as CaptureCoverageFile;
const count = (table: string) => (db.prepare(`select count(*) as n from ${table}`).get() as {n:number}).n;
try {
  db.exec(`create trigger fail_b22_gap before insert on capture_gaps begin select raise(abort, 'gap write injected'); end`);
  assert.throws(() => applyCaptureCoverage(db, check, [unread]), /gap_record_unavailable/);
  assert.equal(count("capture_uncovered_files"), 0);
  assert.equal(count("capture_gaps"), 0);
  assert.equal(count("capture_coverage_state"), 0);
  console.log("PASS frontier_gap_failure_keeps_uncovered_cursor_and_gap_unchanged");

  db.exec("drop trigger fail_b22_gap");
  applyCaptureCoverage(db, check, [unread]);
  const id = fileGapId({ installationEpochId: epoch, source: "codex",
    fileKeyDigest: captureFileKeyDigest(key), generationIdentity: "gen-1" });
  const gap = db.prepare(`select interval_basis as basis, started_at_ms as startMs,
    ended_at_ms as endMs, count_basis as countBasis, unread_bytes as unreadBytes,
    resolved_at_ms as resolvedAtMs from capture_gaps where gap_id=?`).get(id) as {
    basis:string; startMs:number; endMs:number|null; countBasis:string;
    unreadBytes:number; resolvedAtMs:number|null;
  } | undefined;
  assert.deepEqual(gap, { basis:"epoch_open", startMs:check.epochStartMs,
    endMs:null, countBasis:"unknown", unreadBytes:90, resolvedAtMs:null });
  assert.equal(count("capture_uncovered_files"), 1);
  console.log("PASS frontier_retry_commits_open_gap_and_uncovered_cursor");

  db.prepare("delete from capture_gaps where gap_id=?").run(id);
  assert.throws(() => finishCaptureCoverage(db, check));
  assert.equal(count("capture_coverage_state"), 0);
  applyCaptureCoverage(db, check, [unread]);
  assert.ok(finishCaptureCoverage(db, check));
  console.log("PASS frontier_finish_requires_gap_proof");

  const next = beginCaptureCoverage(db, "codex", new Date(Date.now() + 1000).toISOString())!;
  applyCaptureCoverage(db, next, [{...unread, fullyRead:true, progress:90}]);
  const resolved = db.prepare("select resolved_at_ms as atMs from capture_gaps where gap_id=?")
    .get(id) as {atMs:number|null};
  assert.ok(resolved.atMs !== null);
  assert.equal(count("capture_uncovered_files"), 0);
  console.log("PASS frontier_exact_generation_eof_resolves_gap");

  const appended = beginCaptureCoverage(db, "codex", new Date(Date.now() + 2000).toISOString())!;
  applyCaptureCoverage(db, appended, [{...unread, extent:100, progress:90, unreadBytes:10}]);
  assert.ok(finishCaptureCoverage(db, appended));
  const reopened = db.prepare(`select resolved_at_ms as resolvedAtMs, ended_at_ms as endMs,
    unread_bytes as unreadBytes, revision from capture_gaps where gap_id=?`).get(id) as {
      resolvedAtMs:number|null; endMs:number|null; unreadBytes:number; revision:number;
    };
  assert.equal(reopened.resolvedAtMs, null);
  assert.equal(reopened.endMs, null);
  assert.equal(reopened.unreadBytes, 10);
  assert.ok(reopened.revision >= 3);
  console.log("PASS frontier_append_after_eof_reopens_same_generation_gap");
} finally {
  buffer.close();
  fs.rmSync(root, {recursive:true, force:true});
}
