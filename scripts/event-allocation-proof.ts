#!/usr/bin/env node

import assert from "node:assert/strict";

import Database from "better-sqlite3";

import {
  allocateEvents,
  collectAllocationEvents,
  type AllocationEvent,
  type PullCandidate,
} from "./event-allocation";

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function hash(fill: string) {
  return `sha256:${fill.repeat(64).slice(0, 64)}`;
}

function sha(fill: string) {
  return fill.repeat(40).slice(0, 40);
}

function event(
  eventId: string,
  input: Partial<Omit<AllocationEvent, "eventId">> = {},
): AllocationEvent {
  return {
    eventId,
    sessionId: "session-one",
    observedAt: "2026-06-15T12:00:00.000Z",
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWriteTokens: 0,
    costUsd: 0,
    repoHash: null,
    branchHash: null,
    headSha: null,
    ...input,
  };
}

function pull(
  pullNumber: number,
  repoHash: string,
  branchHash: string,
  headSha: string,
  input: Partial<PullCandidate> = {},
): PullCandidate {
  return {
    pull: pullNumber,
    repoHash,
    branchHash,
    headSha,
    createdAt: "2026-06-10T00:00:00.000Z",
    updatedAt: "2026-06-20T00:00:00.000Z",
    closedAt: "2026-06-20T00:00:00.000Z",
    ...input,
  };
}

function totalPrimaryTokens(rows: Array<{ inputTokens: number; outputTokens: number }>) {
  return rows.reduce((sum, row) => sum + row.inputTokens + row.outputTokens, 0);
}

function main() {
  const repos = [hash("a"), hash("b"), hash("c")];
  const branches = [hash("d"), hash("e"), hash("f")];
  const heads = [sha("1"), sha("2"), sha("3")];
  const candidates = repos.map((repoHash, index) =>
    pull(index + 1, repoHash, branches[index], heads[index]),
  );
  const multiRepo = allocateEvents(
    [
      event("multi-a", {
        repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
        inputTokens: 35, outputTokens: 5, cacheReadTokens: 11, cacheWriteTokens: 7, costUsd: 0.01,
      }),
      event("multi-b", {
        repoHash: repos[1], branchHash: branches[1], headSha: heads[1],
        inputTokens: 27, outputTokens: 3, cacheReadTokens: 13, cacheWriteTokens: 5, costUsd: 0.02,
      }),
      event("multi-c", {
        repoHash: repos[2], branchHash: branches[2], headSha: heads[2],
        inputTokens: 28, outputTokens: 2, cacheReadTokens: 17, cacheWriteTokens: 3, costUsd: 0.03,
      }),
    ],
    candidates,
  );
  check(
    "one_100_token_session_spanning_three_repos_and_pulls_is_never_duplicated",
    multiRepo.pullRows.length === 3 &&
      totalPrimaryTokens(multiRepo.pullRows) === 100 &&
      totalPrimaryTokens([multiRepo.coverage.captured]) === 100 &&
      multiRepo.receipts.every((row) => row.weight === 1) &&
      multiRepo.coverage.reconciliation.exact,
    {
      allocatedTokens: totalPrimaryTokens(multiRepo.pullRows),
      capturedTokens: totalPrimaryTokens([multiRepo.coverage.captured]),
      pulls: multiRepo.pullRows.map((row) => row.pull),
    },
  );
  check(
    "all_token_classes_and_known_cost_reconcile_exactly",
    Object.values(multiRepo.coverage.reconciliation).every(Boolean) &&
      multiRepo.coverage.captured.inputTokens === 90 &&
      multiRepo.coverage.captured.outputTokens === 10 &&
      multiRepo.coverage.captured.cacheReadTokens === 41 &&
      multiRepo.coverage.captured.cacheWriteTokens === 15 &&
      multiRepo.coverage.captured.knownCostNanos === 60_000_000,
    { coverage: multiRepo.coverage },
  );

  const maxSafe = Number.MAX_SAFE_INTEGER;
  const boundaryCostNanosA = Math.floor(maxSafe / 3);
  const boundaryCostNanosB = maxSafe - boundaryCostNanosA;
  const safeBoundary = allocateEvents(
    [
      event("safe-boundary-a", {
        repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
        inputTokens: maxSafe - 1,
        outputTokens: maxSafe - 1,
        cacheReadTokens: maxSafe - 1,
        cacheWriteTokens: maxSafe - 1,
        costUsd: boundaryCostNanosA / 1_000_000_000,
      }),
      event("safe-boundary-b", {
        repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
        inputTokens: 1,
        outputTokens: 1,
        cacheReadTokens: 1,
        cacheWriteTokens: 1,
        costUsd: boundaryCostNanosB / 1_000_000_000,
      }),
    ],
    [candidates[0]],
  );
  check(
    "aggregate_exactly_at_safe_integer_boundary_serializes_and_reconciles",
    safeBoundary.coverage.captured.inputTokens === maxSafe &&
      safeBoundary.coverage.captured.outputTokens === maxSafe &&
      safeBoundary.coverage.captured.cacheReadTokens === maxSafe &&
      safeBoundary.coverage.captured.cacheWriteTokens === maxSafe &&
      safeBoundary.coverage.captured.knownCostNanos === maxSafe &&
      safeBoundary.pullRows[0].inputTokens === maxSafe &&
      safeBoundary.pullRows[0].knownCostNanos === maxSafe &&
      safeBoundary.coverage.reconciliation.exact &&
      JSON.parse(JSON.stringify(safeBoundary)).coverage.captured.inputTokens === maxSafe,
    { captured: safeBoundary.coverage.captured },
  );

  const tokenFields = [
    "inputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWriteTokens",
  ] as const;
  for (const field of tokenFields) {
    assert.throws(
      () => allocateEvents(
        [
          event(`overflow-${field}-a`, {
            repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
            [field]: maxSafe,
          }),
          event(`overflow-${field}-b`, {
            repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
            [field]: 1,
          }),
        ],
        [candidates[0]],
      ),
      (error: unknown) =>
        error instanceof RangeError &&
        error.message === `Allocation aggregate exceeds Number.MAX_SAFE_INTEGER for ${field}`,
    );
    checks.push({
      name: `${field}_aggregate_overflow_fails_closed`,
      detail: { boundary: maxSafe, rejectedIncrement: 1 },
    });
  }

  assert.throws(
    () => allocateEvents(
      [
        event("overflow-cost-a", {
          repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
          costUsd: maxSafe / 1_000_000_000,
        }),
        event("overflow-cost-b", {
          repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
          costUsd: 1 / 1_000_000_000,
        }),
      ],
      [candidates[0]],
    ),
    (error: unknown) =>
      error instanceof RangeError &&
      error.message ===
        "Allocation aggregate exceeds Number.MAX_SAFE_INTEGER for knownCostNanos",
  );
  checks.push({
    name: "known_cost_nanos_aggregate_overflow_fails_closed",
    detail: { boundary: maxSafe, rejectedIncrementNanos: 1 },
  });

  assert.throws(
    () => allocateEvents(
      Array.from({ length: 3 }, (_, index) => event(`independent-max-${index}`, {
        repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
        inputTokens: maxSafe,
        costUsd: maxSafe / 1_000_000_000,
      })),
      [candidates[0]],
    ),
    (error: unknown) =>
      error instanceof RangeError &&
      error.message ===
        "Allocation aggregate exceeds Number.MAX_SAFE_INTEGER for inputTokens",
  );
  checks.push({
    name: "independent_three_max_counterexample_cannot_emit_reconciled_totals",
    detail: { events: 3, perEventInputTokens: maxSafe, perEventKnownCostNanos: maxSafe },
  });

  const exactRepo = hash("4");
  const branchOne = hash("5");
  const branchTwo = hash("6");
  const headOne = sha("a");
  const headTwo = sha("b");
  const exactBeatsBranch = allocateEvents(
    [event("head-wins", {
      repoHash: exactRepo,
      branchHash: branchTwo,
      headSha: headOne,
      inputTokens: 10,
    })],
    [
      pull(21, exactRepo, branchOne, headOne),
      pull(22, exactRepo, branchTwo, headTwo),
    ],
  );
  check(
    "exact_head_beats_conflicting_branch",
    exactBeatsBranch.receipts[0].pull === 21 &&
      exactBeatsBranch.receipts[0].confidence === "direct" &&
      exactBeatsBranch.receipts[0].reason === "head_sha",
    { receipt: exactBeatsBranch.receipts[0] },
  );
  const commitMembershipBeatsBranch = allocateEvents(
    [event("commit-membership-wins", {
      repoHash: exactRepo,
      branchHash: branchTwo,
      headSha: sha("7"),
      inputTokens: 10,
    })],
    [
      pull(23, exactRepo, branchOne, sha("8"), { commitShas: [sha("7")] }),
      pull(24, exactRepo, branchTwo, sha("9")),
    ],
  );
  check(
    "exact_commit_membership_beats_conflicting_branch",
    commitMembershipBeatsBranch.receipts[0].pull === 23 &&
      commitMembershipBeatsBranch.receipts[0].confidence === "direct" &&
      commitMembershipBeatsBranch.receipts[0].reason === "commit_sha",
    { receipt: commitMembershipBeatsBranch.receipts[0] },
  );

  const reusedRepo = hash("7");
  const reusedBranch = hash("8");
  const reused = allocateEvents(
    [
      event("reused-old", {
        observedAt: "2026-01-05T12:00:00.000Z",
        repoHash: reusedRepo,
        branchHash: reusedBranch,
        inputTokens: 12,
      }),
      event("reused-new", {
        observedAt: "2026-02-05T12:00:00.000Z",
        repoHash: reusedRepo,
        branchHash: reusedBranch,
        inputTokens: 18,
      }),
    ],
    [
      pull(31, reusedRepo, reusedBranch, sha("c"), {
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-10T00:00:00.000Z",
        closedAt: "2026-01-10T00:00:00.000Z",
      }),
      pull(32, reusedRepo, reusedBranch, sha("d"), {
        createdAt: "2026-02-01T00:00:00.000Z",
        updatedAt: "2026-02-10T00:00:00.000Z",
        closedAt: "2026-02-10T00:00:00.000Z",
      }),
    ],
    { fallbackWindowMs: 24 * 60 * 60 * 1_000 },
  );
  check(
    "reused_branch_is_time_bounded_and_never_cross_joins",
    reused.pullRows.length === 2 &&
      reused.pullRows.find((row) => row.pull === 31)?.inputTokens === 12 &&
      reused.pullRows.find((row) => row.pull === 32)?.inputTokens === 18 &&
      reused.receipts.every((row) => row.reason === "branch_time"),
    { receipts: reused.receipts },
  );

  const forcePushed = allocateEvents(
    [event("force-pushed-old-head", {
      repoHash: reusedRepo,
      branchHash: reusedBranch,
      headSha: sha("e"),
      inputTokens: 9,
    })],
    [pull(33, reusedRepo, reusedBranch, sha("f"))],
  );
  check(
    "force_pushed_unknown_head_uses_one_bounded_fallback_not_a_head_guess",
    forcePushed.receipts[0].pull === 33 &&
      forcePushed.receipts[0].confidence === "inferred" &&
      forcePushed.receipts[0].reason === "branch_time",
    { receipt: forcePushed.receipts[0] },
  );

  const unknown = allocateEvents(
    [event("unknown", { sessionId: "isolated", inputTokens: 7, outputTokens: 3, costUsd: null })],
    candidates,
  );
  check(
    "unknown_linkage_stays_explicitly_unallocated",
    unknown.receipts[0].pull === null &&
      unknown.receipts[0].confidence === "unallocated" &&
      unknown.receipts[0].weight === 0 &&
      unknown.coverage.unallocated.inputTokens === 7 &&
      unknown.coverage.unallocated.outputTokens === 3,
    { receipt: unknown.receipts[0] },
  );

  const missingCost = allocateEvents(
    [event("missing-cost", {
      repoHash: repos[0], branchHash: branches[0], headSha: heads[0],
      inputTokens: 10, costUsd: null,
    })],
    [candidates[0]],
  );
  check(
    "missing_cost_stays_unknown_instead_of_becoming_zero",
    missingCost.pullRows[0].costStatus === "unknown" &&
      missingCost.pullRows[0].costUsd === null &&
      missingCost.pullRows[0].knownCostUsd === 0 &&
      missingCost.coverage.captured.unpricedEvents === 1,
    { pull: missingCost.pullRows[0] },
  );

  const segmentRepo = hash("9");
  const segmentBranch = hash("0");
  const segmentHead = sha("9");
  const segment = allocateEvents(
    [
      event("segment-before", {
        sessionId: "segment", observedAt: "2026-06-15T12:00:00.000Z",
        repoHash: segmentRepo, branchHash: segmentBranch, headSha: segmentHead,
        inputTokens: 1,
      }),
      event("segment-middle", {
        sessionId: "segment", observedAt: "2026-06-15T12:05:00.000Z",
        inputTokens: 8,
      }),
      event("segment-after", {
        sessionId: "segment", observedAt: "2026-06-15T12:10:00.000Z",
        repoHash: segmentRepo, branchHash: segmentBranch, headSha: segmentHead,
        inputTokens: 1,
      }),
    ],
    [pull(41, segmentRepo, segmentBranch, segmentHead)],
  );
  check(
    "bounded_stable_same_session_segment_is_inferred_once",
    segment.receipts.find((row) => row.eventId === "segment-middle")?.pull === 41 &&
      segment.receipts.find((row) => row.eventId === "segment-middle")?.confidence === "inferred" &&
      segment.receipts.find((row) => row.eventId === "segment-middle")?.reason === "session_segment_branch_time" &&
      segment.pullRows[0].inputTokens === 10,
    { receipts: segment.receipts },
  );

  const transition = allocateEvents(
    [
      event("transition-a", {
        sessionId: "transition", observedAt: "2026-06-15T12:00:00.000Z",
        repoHash: repos[0], branchHash: branches[0], headSha: heads[0], inputTokens: 1,
      }),
      event("transition-unknown", {
        sessionId: "transition", observedAt: "2026-06-15T12:05:00.000Z", inputTokens: 8,
      }),
      event("transition-b", {
        sessionId: "transition", observedAt: "2026-06-15T12:10:00.000Z",
        repoHash: repos[1], branchHash: branches[1], headSha: heads[1], inputTokens: 1,
      }),
    ],
    candidates,
  );
  check(
    "repository_switch_boundary_never_infers_across_repos",
    transition.receipts.find((row) => row.eventId === "transition-unknown")?.confidence === "unallocated" &&
      transition.coverage.unallocated.inputTokens === 8,
    { receipts: transition.receipts },
  );

  const rerunOne = allocateEvents([...multiRepo.receipts].map((row) => event(row.eventId, {
    sessionId: row.sessionId,
    observedAt: row.observedAt,
    inputTokens: row.amounts.inputTokens,
    outputTokens: row.amounts.outputTokens,
    cacheReadTokens: row.amounts.cacheReadTokens,
    cacheWriteTokens: row.amounts.cacheWriteTokens,
    costUsd: row.amounts.knownCostNanos / 1_000_000_000,
    repoHash: row.repoHash,
    branchHash: candidates.find((candidate) => candidate.pull === row.pull)?.branchHash ?? null,
    headSha: candidates.find((candidate) => candidate.pull === row.pull)?.headSha ?? null,
  })), candidates);
  const rerunTwo = allocateEvents([...multiRepo.receipts].reverse().map((row) => event(row.eventId, {
    sessionId: row.sessionId,
    observedAt: row.observedAt,
    inputTokens: row.amounts.inputTokens,
    outputTokens: row.amounts.outputTokens,
    cacheReadTokens: row.amounts.cacheReadTokens,
    cacheWriteTokens: row.amounts.cacheWriteTokens,
    costUsd: row.amounts.knownCostNanos / 1_000_000_000,
    repoHash: row.repoHash,
    branchHash: candidates.find((candidate) => candidate.pull === row.pull)?.branchHash ?? null,
    headSha: candidates.find((candidate) => candidate.pull === row.pull)?.headSha ?? null,
  })), [...candidates].reverse());
  check(
    "allocation_is_byte_deterministic_across_input_order",
    JSON.stringify(rerunOne) === JSON.stringify(rerunTwo),
    { bytes: JSON.stringify(rerunOne).length },
  );
  const labeledBefore = allocateEvents(
    [event("label-stability", {
      repoHash: repos[0], branchHash: branches[0], headSha: heads[0], inputTokens: 6,
    })],
    candidates.map((candidate) => ({ ...candidate, displayLabel: "old display name" })),
  );
  const labeledAfter = allocateEvents(
    [event("label-stability", {
      repoHash: repos[0], branchHash: branches[0], headSha: heads[0], inputTokens: 6,
    })],
    candidates.map((candidate) => ({ ...candidate, displayLabel: "new display name" })),
  );
  check(
    "display_label_changes_do_not_rewrite_allocation_truth",
    JSON.stringify(labeledBefore) === JSON.stringify(labeledAfter),
    { pull: labeledBefore.receipts[0].pull },
  );

  const ledger = new Database(":memory:");
  ledger.exec(`
    create table buffered_events (
      id text primary key, session_id text, observed_at text,
      input_tokens integer, output_tokens integer,
      cache_read_tokens integer, cache_creation_tokens integer,
      cost_usd real, repo_hash text, branch_hash text, head_sha text,
      payload_json text not null
    );
    create index idx_events_observed on buffered_events (observed_at);
  `);
  ledger.prepare(`insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
    "promoted-only", "sql-session", "2026-06-15T12:00:00.000Z",
    4, 2, 3, 1, null, repos[0], branches[0], heads[0],
    "NOT_JSON_RAW_PAYLOAD_SENTINEL",
  );
  const promoted = collectAllocationEvents(ledger, "2026-06-01T00:00:00.000Z");
  ledger.close();
  check(
    "ledger_read_is_time_bounded_and_uses_promoted_columns_without_raw_parsing",
    promoted.length === 1 &&
      promoted[0].inputTokens === 4 &&
      promoted[0].cacheWriteTokens === 1 &&
      !JSON.stringify(promoted).includes("RAW_PAYLOAD_SENTINEL") &&
      !("payloadJson" in promoted[0]),
    { row: promoted[0] },
  );

  assert.throws(
    () => allocateEvents([], Array.from({ length: 101 }, (_, index) =>
      pull(index + 1, hash(String(index % 10)), hash("a"), sha("f")),
    )),
    /candidate limit exceeded/,
  );
  checks.push({ name: "github_candidate_set_fails_closed_above_bound", detail: { limit: 100 } });

  console.log(JSON.stringify({
    schema: "plimsoll.event-allocation-proof.v1",
    status: "pass",
    checks: checks.length,
    names: checks.map((entry) => entry.name),
    evidence: {
      allocatedPrimaryTokens: totalPrimaryTokens(multiRepo.pullRows),
      capturedPrimaryTokens: totalPrimaryTokens([multiRepo.coverage.captured]),
      coverage: multiRepo.coverage,
      missingCost: missingCost.pullRows[0].costStatus,
      deterministicBytes: JSON.stringify(rerunOne).length,
    },
    rawPayloadParsed: false,
    liveStateTouched: false,
    providerNetworkCalled: false,
  }, null, 2));
}

main();
