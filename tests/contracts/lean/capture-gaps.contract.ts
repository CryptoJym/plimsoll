/**
 * B22 (collector): an unresolved or never-read file is declared as an OPEN gap (epoch_open, null end) and no period after its
 * last write is complete while it is unparsed (docs/lean/ARCHITECTURE.md §3.6, §6.2, §6.4; CONTRACTS.md C5). The behavioural
 * half of fixtures/b22_false_complete.py beside fixtures/sf5_unresolved_file_open_gap.py. Pending until B22 lands
 * packages/collector-cli/src/lean/capture-gaps.ts.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { fn, loadSurface, openTempBuffer, pending } from "./_pending";

const t = (hhmm: string) => Date.parse(`2026-09-25T${hhmm}:00.000Z`);
const EPOCH_START = t("00:00"), LAST_WRITE = t("10:00"), FIRST_PARSE = t("11:00"), STAMPED = t("10:05");

test("B22 C5: an unresolved file's gap is epoch_open from the epoch start with a null end; a period after its last write is not complete until it parses", pending("B22"), async () => {
  const surface = await loadSurface("../../../packages/collector-cli/src/lean/capture-gaps.ts");
  const declare = fn(surface, "declareUnresolvedFileGap") as (db: unknown, input: Record<string, unknown>) => { gapId: string };
  const complete = fn(surface, "coverageCompleteForPeriod") as (db: unknown, period: { startMs: number; endMs: number }, throughMs: number) => boolean;
  const resolve = fn(surface, "resolveCaptureGap") as (db: unknown, gapId: string, atMs: number) => void;
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract" });
  try {
    const db = buffer.database;
    const { gapId } = declare(db, { workspaceId: "tenant-lean-contract", installationEpochId: "epoch-1", source: "codex", fileKeyDigest: "f".repeat(64), reason: "tailer_unread", epochStartMs: EPOCH_START, lastWriteAtMs: LAST_WRITE, unreadBytes: 4096 });
    const gap = db.prepare("select interval_basis as basis, started_at_ms as started, ended_at_ms as ended, count_basis as countBasis, dropped_rows as dropped, resolved_at_ms as resolved from capture_gaps where gap_id = ?").get(gapId) as Record<string, unknown>;
    assert.deepEqual(gap, { basis: "epoch_open", started: EPOCH_START, ended: null, countBasis: "unknown", dropped: null, resolved: null });
    assert.equal(complete(db, { startMs: t("10:01"), endMs: FIRST_PARSE }, FIRST_PARSE), false, "the 10:05 event is unparsed");
    for (let h = 0; h < 11; h += 1) assert.equal(complete(db, { startMs: t(`${String(h).padStart(2, "0")}:00`), endMs: t(`${String(h + 1).padStart(2, "0")}:00`) }, FIRST_PARSE), false, `hour ${h}`);
    assert.ok(STAMPED > LAST_WRITE, "the clamp keeps a stamp after the file's last write (normalizer.ts:203-214)");
    resolve(db, gapId, FIRST_PARSE);
    assert.equal(complete(db, { startMs: t("10:01"), endMs: FIRST_PARSE }, FIRST_PARSE), true, "parsed to its end: resolved");
  } finally { close(); }
});
