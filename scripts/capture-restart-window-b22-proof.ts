/** B22: every unclean restart opens an unknown interval until a fresh walk. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(), "b22-restart-window-"));
const ledger = path.join(root, "ledger.sqlite");
const gap = (buffer: LocalEventBuffer) => buffer.database.prepare(`select ended_at_ms as endMs,
  revision from capture_gaps where reason='restart_unverified'`).get() as {
    endMs:number|null; revision:number;
  } | undefined;
try {
  const first = new LocalEventBuffer(ledger);
  first.captureDurability.markFreshWalkComplete();
  first.close();
  assert.equal(first.captureDurability.markCleanShutdown(), true);
  const clean = new LocalEventBuffer(ledger);
  assert.equal(clean.captureDurability.status().restartUnverified, false);
  clean.close(); // Simulates a crash without the clean-shutdown marker.

  const unclean = new LocalEventBuffer(ledger);
  assert.equal(unclean.captureDurability.status().restartUnverified, true);
  assert.equal(gap(unclean)?.endMs, null);
  unclean.captureDurability.markFreshWalkComplete();
  assert.equal(unclean.captureDurability.status().restartUnverified, false);
  const closed = gap(unclean);
  assert.ok(closed && closed.endMs !== null);
  unclean.close(); // Another crash must reopen the same durable gap ID.

  const again = new LocalEventBuffer(ledger);
  assert.equal(again.captureDurability.status().restartUnverified, true);
  const reopened = gap(again);
  assert.equal(reopened?.endMs, null);
  assert.ok(reopened.revision > closed.revision);
  again.close();
  console.log("PASS restart_gap_closes_after_walk_and_reopens_after_next_crash");
} finally { fs.rmSync(root, {recursive:true, force:true}); }
