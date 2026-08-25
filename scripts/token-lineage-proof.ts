/**
 * Adversarial proof for issue #153 (bead eco-7eics): nonzero-first cumulative
 * token counter lineage.
 *
 * A codex rollout whose FIRST observed `total_token_usage` is nonzero cannot
 * be attributed to marginals the tailer ever saw: it may be genuine catch-up
 * usage of a young session, or an inherited/forked/resumed/global counter.
 * The tailer must classify that first delta instead of silently counting it:
 * typed token columns stay zero, raw source totals persist in row metadata,
 * and scan results report the excluded volume. Lineage itself stays UNKNOWN.
 *
 * Scenarios here include three attacks on the classification itself: a
 * Studio0-shaped inherited-counter session (the real 2026-08 incident),
 * a mid-life fork/rewrite that forces parser rebuilds, and slice-boundary
 * smuggling where the first counter arrives in a LATER committed slice.
 *
 * Run: pnpm proof:token-lineage
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { sealOutboundEvent } from "../packages/collector-cli/src/outbound-envelope";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-token-lineage-proof-"));

function rolloutLine(timestamp: string, type: string, payload: Record<string, unknown>) {
  return JSON.stringify({ timestamp, type, payload });
}

function tokenCountLine(
  timestamp: string,
  input: number,
  cached: number,
  output: number,
  reasoning = 0,
  withTotals = true,
) {
  return rolloutLine(timestamp, "event_msg", {
    type: "token_count",
    info: withTotals
      ? {
          total_token_usage: {
            input_tokens: input,
            cached_input_tokens: cached,
            output_tokens: output,
            reasoning_output_tokens: reasoning,
            total_tokens: input + output,
          },
        }
      : {},
    rate_limits: { plan_type: "pro" },
  });
}

type Row = {
  inputTokens: number | null;
  cacheReadTokens: number | null;
  outputTokens: number | null;
  costUsd: number | null;
  payloadJson: string;
};

function usageRows(buffer: LocalEventBuffer, sessionId: string): Row[] {
  return buffer.database
    .prepare(
      `select input_tokens as inputTokens, cache_read_tokens as cacheReadTokens,
         output_tokens as outputTokens, cost_usd as costUsd, payload_json as payloadJson
       from buffered_events
       where event_type = 'usage_rollout' and session_id = ?
       order by observed_at`,
    )
    .all(sessionId) as Row[];
}

function metadata(row: Row): Record<string, unknown> {
  return JSON.parse(row.payloadJson).metadata;
}

async function main() {
  // ---------------------------------------------------------------------------
  // Scenario 1 — the Studio0 incident shape (issue #153 confirmed evidence):
  // one tailer-authority session, ~68 rows, first row carrying 961282526 input
  // / 953284608 cached / 2008151 output against an assumed-zero baseline, model
  // UNKNOWN, timestamps collapsed to ~2ms.
  // ---------------------------------------------------------------------------
  {
    const root = path.join(tempDir, "inherited-counter");
    const day = path.join(root, "2026", "08", "24");
    fs.mkdirSync(day, { recursive: true });
    const sessionId = "01990000-0000-7000-8000-000000000153";
    const file = path.join(day, `rollout-2026-08-24T09-00-00-${sessionId}.jsonl`);
    fs.writeFileSync(
      file,
      [
        rolloutLine("2026-08-24T09:00:00.000Z", "session_meta", { id: sessionId }),
        rolloutLine("2026-08-24T09:00:00.002Z", "event_msg", {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 961282526,
              cached_input_tokens: 953284608,
              output_tokens: 2008151,
              reasoning_output_tokens: 1500000,
              total_tokens: 963290677,
            },
          },
        }),
        tokenCountLine("2026-08-24T09:10:00.000Z", 961292526, 953284608, 2008251, 1500010),
        tokenCountLine("2026-08-24T09:20:00.000Z", 961302526, 953284608, 2008351, 1500020),
      ].join("\n") + "\n",
    );
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const scan = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });

    assert.equal(scan.eventsAppended, 3);
    // Validated marginal consumption: only the deltas between OBSERVED totals.
    assert.deepEqual(scan.tokensAppended, { input: 20_000, cachedInput: 0, output: 200 });
    // The excluded lump is reported, not hidden.
    assert.equal(scan.unvalidatedFirstRows, 1);
    assert.deepEqual(scan.tokensUnvalidated, {
      input: 961282526,
      cachedInput: 953284608,
      output: 2008151,
    });

    const rows = usageRows(buffer, sessionId);
    assert.equal(rows.length, 3);
    const [first, second, third] = rows;
    // Raw observation preserved; validated columns zero; never priced even if
    // the model had been known (here it is unknown, matching the incident).
    assert.deepEqual(metadata(first), {
      usageSource: "rollout",
      turnIndex: 0,
      planType: "pro",
      counterLineage: "unknown_nonzero_first",
      sourceCumulativeInput: 961282526,
      sourceCumulativeCachedInput: 953284608,
      sourceCumulativeOutput: 2008151,
      sourceCumulativeReasoningOutput: 1500000,
    });
    assert.deepEqual(
      {
        inputTokens: first!.inputTokens,
        cacheReadTokens: first!.cacheReadTokens,
        outputTokens: first!.outputTokens,
        costUsd: first!.costUsd,
      },
      { inputTokens: 0, cacheReadTokens: 0, outputTokens: 0, costUsd: null },
    );
    // Later rows are honest marginals (10_000 input / 100 output each).
    for (const row of [second!, third!]) {
      assert.deepEqual(
        { inputTokens: row.inputTokens, outputTokens: row.outputTokens },
        { inputTokens: 10_000, outputTokens: 100 },
      );
      assert.equal(metadata(row).counterLineage, undefined);
    }

    // Rescan idempotency: counters must not double-report the exclusion.
    const rescan = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    assert.equal(rescan.eventsAppended, 0);
    assert.equal(rescan.unvalidatedFirstRows ?? 0, 0);
    assert.deepEqual(usageRows(buffer, sessionId).length, 3);

    // Outbound boundary: the classified row seals fine; local-only lineage
    // fields ride suppression receipts and values never cross.
    const sealed = sealOutboundEvent(aiInteractionEventSchema.parse(JSON.parse(first!.payloadJson)));
    assert.equal(sealed.ok, true);
    if (sealed.ok) {
      const sealedMetadata = sealed.event.metadata as Record<string, unknown>;
      assert.equal(sealedMetadata.counterLineage, undefined);
      assert.equal(sealedMetadata.sourceCumulativeInput, undefined);
      assert.ok(sealed.omittedFields.length >= 5, "omissions must be receipted");
      assert.deepEqual(sealed.event.inputTokens, 0);
    }

    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }

  // ---------------------------------------------------------------------------
  // Scenario 2 — adversarial: mid-life FORK. A validated session is truncated
  // back to an early prefix and regrown from an inflated forked counter. The
  // forced rebuild must replay deterministically: the new lineage-first counter
  // is reclassified (duplicate ids dedupe) and validated sums cannot inflate.
  // ---------------------------------------------------------------------------
  {
    const root = path.join(tempDir, "fork-rewrite");
    const day = path.join(root, "2026", "08", "24");
    fs.mkdirSync(day, { recursive: true });
    const sessionId = "01990000-0000-7000-8000-000000000154";
    const file = path.join(day, `rollout-2026-08-24T10-00-00-${sessionId}.jsonl`);
    const prefix = [
      rolloutLine("2026-08-24T10:00:00.000Z", "session_meta", { id: sessionId }),
      tokenCountLine("2026-08-24T10:01:00.000Z", 500, 100, 50),
      tokenCountLine("2026-08-24T10:02:00.000Z", 900, 150, 90),
    ].join("\n");
    fs.writeFileSync(file, prefix + "\n");
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const first = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    assert.equal(first.unvalidatedFirstRows, 1);
    assert.deepEqual(first.tokensUnvalidated, { input: 500, cachedInput: 100, output: 50 });

    // Fork: same conversation id, rewritten history with an inflated baseline.
    fs.writeFileSync(
      file,
      [
        prefix,
        tokenCountLine("2026-08-24T10:03:00.000Z", 999_999_999, 999_999_000, 999_199),
      ].join("\n") + "\n",
    );
    const forked = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    // The fork delta IS attributable (previous total was observed): it counts.
    assert.deepEqual(forked.tokensAppended, {
      input: 999_999_099,
      cachedInput: 999_998_850,
      output: 999_109,
    });
    assert.equal(forked.unvalidatedFirstRows ?? 0, 0);

    // Stateless replay of the forked file: identical truth, zero inflation.
    buffer.database.prepare(`delete from rollout_scan_state`).run();
    const replay = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    assert.equal(replay.eventsAppended, 0);
    assert.equal(replay.unvalidatedFirstRows ?? 0, 0);
    const afterReplay = usageRows(buffer, sessionId);
    assert.equal(afterReplay.length, 3);
    assert.deepEqual(metadata(afterReplay[0]!).counterLineage, "unknown_nonzero_first");

    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }

  // ---------------------------------------------------------------------------
  // Scenario 3 — adversarial: SLICE-BOUNDARY SMUGGLING. The first counter
  // arrives only after a metadata-only slice already committed a cursor; the
  // classification must survive across slices because it keys on durable
  // parser state (index 0 + assumed-zero baseline), not on scan boundaries.
  // ---------------------------------------------------------------------------
  {
    const root = path.join(tempDir, "slice-smuggle");
    const day = path.join(root, "2026", "08", "24");
    fs.mkdirSync(day, { recursive: true });
    const sessionId = "01990000-0000-7000-8000-000000000155";
    const file = path.join(day, `rollout-2026-08-24T11-00-00-${sessionId}.jsonl`);
    fs.writeFileSync(
      file,
      [
        rolloutLine("2026-08-24T11:00:00.000Z", "session_meta", { id: sessionId }),
      ].join("\n") + "\n",
    );
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const metaSlice = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    assert.equal(metaSlice.slicesCommitted, 1);
    assert.equal(metaSlice.eventsAppended, 0);

    fs.appendFileSync(
      file,
      tokenCountLine("2026-08-24T11:05:00.000Z", 77_000_000, 70_000_000, 300_000) + "\n",
    );
    const smuggled = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    assert.equal(smuggled.unvalidatedFirstRows, 1);
    assert.deepEqual(smuggled.tokensAppended, { input: 0, cachedInput: 0, output: 0 });
    assert.deepEqual(smuggled.tokensUnvalidated, {
      input: 77_000_000,
      cachedInput: 70_000_000,
      output: 300_000,
    });
    const rows = usageRows(buffer, sessionId);
    assert.equal(rows.length, 1);
    assert.equal(metadata(rows[0]!).counterLineage, "unknown_nonzero_first");

    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }

  // ---------------------------------------------------------------------------
  // Scenario 4 — no over-classification: an OBSERVED all-zero total anchors
  // the baseline, so every later delta is a validated marginal. Also covers a
  // priced known-model unvalidated row staying free while marginals price.
  // ---------------------------------------------------------------------------
  {
    const root = path.join(tempDir, "observed-zero-anchor");
    const day = path.join(root, "2026", "08", "24");
    fs.mkdirSync(day, { recursive: true });
    const sessionId = "01990000-0000-7000-8000-000000000156";
    const file = path.join(day, `rollout-2026-08-24T12-00-00-${sessionId}.jsonl`);
    fs.writeFileSync(
      file,
      [
        rolloutLine("2026-08-24T12:00:00.000Z", "session_meta", { id: sessionId }),
        rolloutLine("2026-08-24T12:00:01.000Z", "turn_context", { model: "gpt-5.5" }),
        tokenCountLine("2026-08-24T12:00:02.000Z", 0, 0, 0),
        tokenCountLine("2026-08-24T12:01:00.000Z", 400, 40, 20),
        tokenCountLine("2026-08-24T12:02:00.000Z", 800, 80, 40),
      ].join("\n") + "\n",
    );
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const scan = await new RolloutTailer(buffer, root, () => []).scan({ scope: "full" });
    assert.equal(scan.eventsAppended, 2);
    assert.deepEqual(scan.tokensAppended, { input: 800, cachedInput: 80, output: 40 });
    assert.equal(scan.unvalidatedFirstRows ?? 0, 0);
    const rows = usageRows(buffer, sessionId);
    assert.deepEqual(rows.map((row) => row.costUsd !== null), [true, true]);

    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }

  fs.rmSync(tempDir, { recursive: true, force: true });
console.log(  JSON.stringify({
    proof: "token-lineage",
    status: "passed",
    scenarios: [
      "inherited_counter_studio0_shape_classified_and_preserved",
      "fork_rewrite_replay_cannot_inflate_validated_sums",
      "slice_boundary_smuggling_still_classified",
      "observed_zero_anchor_keeps_later_deltas_validated",
    ],
  }));

}

main();
