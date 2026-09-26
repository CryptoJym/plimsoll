/**
 * B2a (collector): the history converter folds EVERY schema-accepted timestamp by the UTC day of its parsed instant, and a
 * string the parser rejects becomes one counted gap and one durable conversion_rejects row (docs/lean/ARCHITECTURE.md §3.5;
 * CONTRACTS.md C3). Ports fixtures/b5_non_iso_day_facts.py. Pending until B2a lands packages/collector-cli/src/lean/converter.ts.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { fn, loadSurface, openTempBuffer, pending } from "./_pending";

const ROWS: Array<[string, string, number, number | null, number | null]> = [
  ["c1", "2026-09-25T10:00:00.000Z", 1, 50, 5], ["c2", "2026-09-26T09:00:00.000Z", 1, 40, 4],
  ["r1", "Sat, 26 Sep 2026 00:00:00 GMT+00:00", 1, 100, 10], ["r2", "September 26, 2026 00:00:00 +00:00", 0, null, null],
  ["r3", "1 Sep 2026 00:00:00 +00:00", 1, 7, 1], ["r4", "Sat, 26 Sep 2026 23:30:00 -02:00", 1, 20, 2],
  ["x1", "26/09/2026 00:00:00 +00:00", 1, 999, 99],
];

test("B2a C3: day facts equal the raw-row oracle by UTC day; the NaN string is one counted gap plus one reject; nothing vanishes", pending("B2a"), async () => {
  const convert = fn(await loadSurface("../../../packages/collector-cli/src/lean/converter.ts"), "convertLedgerHistory") as (buffer: unknown, options: Record<string, unknown>) => Promise<{ folded: number; rejected: number }> | { folded: number; rejected: number };
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: false } });
  try {
    const db = buffer.database;
    const insert = db.prepare(`insert into buffered_events (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json, created_at, session_id, input_tokens, output_tokens, workspace_id, installation_epoch_id)
      values (@id, 'codex', @type, 'metadata', @observedAt, '{}', '[]', @observedAt, 'sess-1', @input, @output, 'tenant-lean-contract', 'epoch-1')`);
    for (const [id, observedAt, usage, input, output] of ROWS) insert.run({ id, type: usage ? "usage_rollout" : "tool_use", observedAt, input, output });
    const result = await convert(buffer, { chunkRows: 5 });
    assert.deepEqual({ folded: result.folded, rejected: result.rejected }, { folded: 6, rejected: 1 });
    const facts = db.prepare("select day, sum(input_tokens) as input, sum(calls) as calls from model_day_facts group by day order by day").all() as Array<{ day: string; input: number; calls: number }>;
    assert.deepEqual(facts, [{ day: "2026-09-01", input: 7, calls: 1 }, { day: "2026-09-25", input: 50, calls: 1 }, { day: "2026-09-26", input: 140, calls: 2 }, { day: "2026-09-27", input: 20, calls: 1 }]);
    const reject = db.prepare("select event_id as id, reason, gap_id as gapId, resolved_at_ms as resolvedAt from conversion_rejects").all() as Array<{ id: string; reason: string; gapId: string; resolvedAt: number | null }>;
    assert.equal(reject.length, 1);
    assert.equal(reject[0].id, "x1");
    assert.equal(reject[0].reason, "contract_violation");
    assert.equal(reject[0].resolvedAt, null);
    const gap = db.prepare("select reason, count_basis as countBasis, dropped_rows as dropped from capture_gaps where gap_id = ?").get(reject[0].gapId) as { reason: string; countBasis: string; dropped: number };
    assert.deepEqual(gap, { reason: "contract_violation", countBasis: "counted", dropped: 1 });
  } finally { close(); }
});
