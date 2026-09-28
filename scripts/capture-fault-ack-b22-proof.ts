/** B22: a repaired fault is held until its gap receipt and fresh walk. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier } from "../packages/collector-cli/src/capture-frontier";
import { declareUnresolvedFileGap, faultGapId, fileGapId, resolveCaptureGap, rolloutGapScope } from
  "../packages/collector-cli/src/lean/capture-gaps";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(), "b22-fault-ack-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "00000000-0000-4000-8000-000000000023",
  delivery: {enabled:true}, enrollmentNow: () => new Date(Date.now() - 86_400_000),
});
const db = buffer.database;
const scope = rolloutGapScope(db);
const file = { ...scope, source:"codex", fileKeyDigest:"b".repeat(64),
  generationIdentity:"gen-1", reason:"tailer_unread" as const,
  lastWriteAtMs:Date.now(), unreadBytes:12 };
const marker = path.join(root, "capture-fault.json");
try {
  db.exec(`create trigger fail_b22_fault_gap before insert on capture_gaps
    begin select raise(abort, 'gap write injected'); end`);
  assert.throws(() => buffer.transactionWithRepoContextHandoffs(() =>
    declareUnresolvedFileGap(db, file)), /gap_record_unavailable/);
  const live = buffer.captureDurability.status();
  assert.equal(live.faults.length, 1);
  assert.equal(live.faultMarkerPersisted, true);
  assert.equal(JSON.parse(fs.readFileSync(marker, "utf8")).faultId, live.faults[0]!.faultId);
  const fault = db.prepare(`select fault_id as faultId,kind,at_ms as atMs
    from capture_faults where resolved_at_ms is null`).get() as {
      faultId:string; kind:string; atMs:number;
    };
  assert.deepEqual(fault, {faultId:live.faults[0]!.faultId,
    kind:live.faults[0]!.kind, atMs:live.faults[0]!.atMs});
  console.log("PASS fault_marker_memory_and_sql_mirror_agree");

  db.exec("drop trigger fail_b22_fault_gap");
  buffer.transactionWithRepoContextHandoffs(() => declareUnresolvedFileGap(db, file));
  const gapId = faultGapId(fault.faultId);
  const gap = db.prepare(`select gap_id as gapId,revision,ended_at_ms as endMs,
    resolved_at_ms as resolvedAtMs,upload_state as uploadState
    from capture_gaps where gap_id=?`).get(gapId) as {
      gapId:string; revision:number; endMs:number|null;
      resolvedAtMs:number|null; uploadState:string;
    };
  assert.equal(gap.gapId, gapId);
  assert.ok(gap.endMs !== null && gap.resolvedAtMs === null);
  assert.equal(gap.uploadState, "pending");
  console.log("PASS retry_commits_fault_interval_with_source_gap");

  buffer.captureDurability.acknowledgeGaps([{gapId,revision:gap.revision+1}]);
  assert.equal((db.prepare("select resolved_at_ms as atMs from capture_faults where fault_id=?")
    .get(fault.faultId) as {atMs:number|null}).atMs, null);
  assert.ok(fs.existsSync(marker));
  buffer.captureDurability.acknowledgeGaps([{gapId,revision:gap.revision}]);
  assert.ok((db.prepare("select resolved_at_ms as atMs from capture_faults where fault_id=?")
    .get(fault.faultId) as {atMs:number|null}).atMs !== null);
  assert.ok(fs.existsSync(marker), "a receipt alone cannot erase the marker before a fresh walk");
  assert.equal(buffer.captureDurability.status().faults.length, 1);
  console.log("PASS stale_receipt_ignored_and_ack_waits_for_walk");

  buffer.captureDurability.markFreshWalkComplete();
  assert.equal(fs.existsSync(marker), false);
  assert.equal(buffer.captureDurability.status().faults.length, 0);
  console.log("PASS fresh_walk_after_fault_gap_ack_clears_marker_and_memory");

  db.exec(`create trigger fail_b22_fault_gap_again before insert on capture_gaps
    begin select raise(abort, 'gap write injected'); end`);
  const secondFile = {...file, generationIdentity:"gen-2"};
  assert.throws(() => buffer.transactionWithRepoContextHandoffs(() =>
    declareUnresolvedFileGap(db, secondFile)), /gap_record_unavailable/);
  const secondFaultId = buffer.captureDurability.status().faults[0]!.faultId;
  db.exec("drop trigger fail_b22_fault_gap_again");
  buffer.transactionWithRepoContextHandoffs(() => declareUnresolvedFileGap(db, secondFile));
  buffer.captureDurability.markFreshWalkComplete();
  assert.ok(fs.existsSync(marker));
  const secondGapId = faultGapId(secondFaultId);
  const secondGap = db.prepare("select revision from capture_gaps where gap_id=?")
    .get(secondGapId) as {revision:number};
  buffer.captureDurability.acknowledgeGaps([{gapId:secondGapId, revision:secondGap.revision}]);
  assert.equal(fs.existsSync(marker), false);
  assert.equal(buffer.captureDurability.status().faults.length, 0);
  console.log("PASS receipt_after_fresh_walk_also_clears_fault");

  for (const generationIdentity of ["gen-1", "gen-2"]) {
    resolveCaptureGap(db, fileGapId({installationEpochId:scope.installationEpochId,
      source:"codex", fileKeyDigest:file.fileKeyDigest, generationIdentity}), Date.now());
  }
  buffer.delivery.migrateLegacy({now:new Date()});
  for (const source of ["codex", "claude_code", "grok"] as const) {
    advanceCaptureFrontier(db, source, {complete:true, files:[]},
      new Date(Date.now() + 3_600_000).toISOString());
  }
  const claim = buffer.delivery.captureClaim([], {pendingFiles:0,oldestPendingMs:null,
    losses:[],unreadable:false});
  assert.ok(claim?.through, "a repaired, acknowledged fault need not hold later coverage");
  assert.ok(claim.gaps.some((item) => Date.parse(item.from) <= fault.atMs &&
    Date.parse(item.to) >= fault.atMs), "v1 carries the historical fault interval");
  console.log("PASS v1_later_claim_retains_closed_historical_fault_gap");
} finally {
  buffer.close();
  fs.rmSync(root, {recursive:true, force:true});
}
