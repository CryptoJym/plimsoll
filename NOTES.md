# Issue #181: Maintenance Deadline-Kill Loop — Starvation Backlog & Quarantine Blame

## Summary

Fixed three interconnected defects in the maintenance child deadline-kill loop that was starving repo-context enrichment (48,280 event-links stuck `fill_pending`, 709 dirty sessions, only 13 of 2,423 sessions ever carrying repo/branch/sha linkage).

### Defects Fixed

1. **Silent All-or-Nothing Kills**: Deadline kills discarded an entire batch's progress without recording what work had completed, forcing the next run to start from zero.
2. **Quarantine Misattribution**: The quarantine blamed whichever candidate happened to be on stage at kill time, rather than only those whose own measured slowness proved culpability.
3. **Starvation Invisibility**: No durable receipt surfaced the kill rate or pending-enrichment backlog, so enrichment starvation could persist undetected.

### Solution

1. **New Module**: `packages/collector-cli/src/maintenance-starvation.ts` — Durable ledger for deadline-kill rate, progress checkpoint, and backlog census.
2. **Modified**: `cli.ts` — Added `onDeadline` callback to record kills durably; added warning emission for starvation state.
3. **Modified**: `maintenance-boundary.ts` — Track attribution (proven vs unknown blame); record last checkpoint + blame reason.
4. **New Proof**: `scripts/maintenance-starvation-proof.ts` — Three integrated tests covering resumability, quarantine accuracy, and backlog visibility.

---

## Command Execution & Exit Codes

### TypeScript Compilation

```bash
npx tsc --noEmit
```

**Exit code: 0** ✓ PASS

### Proof Scripts (Green — All Pass)

#### proof:lane-receipts
```bash
pnpm run proof:lane-receipts
```

**Exit code: 0** ✓ PASS
Last 20 lines:
```
    },
    {
      "name": "explicit_failure_at_head_reports_distinct_reason_not_missing_pass",
      "detail": {
        "reasons": [
          "gate_failed_at_head:privacy"
        ]
      }
    },
    {
      "name": "failure_recorded_at_other_head_does_not_block_context_head_pass",
      "detail": {
        "reasons": []
      }
    },
    {
      "name": "proof_output_excludes_home_paths_and_fake_secrets"
    }
  ]
}
```

#### proof:maintenance
```bash
pnpm run proof:maintenance
```

**Exit code: 0** ✓ PASS
Last 20 lines:
```
    "discovery_directory_soft_cap_retains_consumed_subdirectory_for_next_cadence",
    "discovery_directory_soft_cap_does_not_advance_unadmitted_root",
    "codex_jsonl_open_soft_cap_retains_candidate_batch_until_admitted",
    "codex_jsonl_validation_cap_retains_candidate_until_cursor_commit",
    "claude_code_jsonl_open_soft_cap_retains_candidate_batch_until_admitted",
    "claude_code_jsonl_validation_cap_retains_candidate_until_cursor_commit",
    "maintenance_schema_is_idempotent",
    "legacy_backfills_use_bounded_partial_indexes",
    "unchanged_pricing_catalog_visits_zero_event_rows",
    "claude_5_family_models_price",
    "catalog_change_bounded_backfill_reprices_legacy_row",
    "completed_pricing_backfill_returns_to_zero_work",
    "dirty_session_backfill_is_bounded_and_stitches_backward",
    "unchanged_repo_inputs_visit_zero_event_rows",
    "unresolved_dirty_session_does_not_spin",
    "later_linkage_reactivates_and_stitches_unresolved_session",
    "integrated_unchanged_cycle_has_zero_parse_write_and_row_visits",
    "metadata_fixtures_persist_no_raw_content_sentinel"
  ]
}
```

#### proof:maintenance-starvation (New)
```bash
pnpm run proof:maintenance-starvation
```

**Exit code: 0** ✓ PASS

This is the new proof script testing all three fixed defects:

1. **Resumability** — A fixture batch larger than one deadline window makes measurable, disjoint progress across consecutive runs.
2. **Quarantine Accuracy** — Only candidates with proven slow measured time-on-stage are quarantined; fast candidates that merely happened to be on stage at kill time are recorded as UNKNOWN blame and never applied.
3. **Starvation Visibility** — The durable starvation receipt reflects a seeded pending-enrichment backlog and the deadline-kill rate.

---

## Code Changes

### New Files

1. **`packages/collector-cli/src/maintenance-starvation.ts`** (143 lines)
   - Exports: `recordMaintenanceDeadlineKill()`, `recordMaintenanceDeadlineBlame()`, `maintenanceBacklogSnapshot()`, `maintenanceStarvationReceipt()`
   - Durable state keys: `maintenance_deadline_kills`, `maintenance_last_deadline_kill_at`, `maintenance_progress_checkpoint`
   - State table: `maintenance_state(key text primary key, value text, updated_at text)`
   - Receipt fields:
     - `deadlineKills: number` — cumulative kill count
     - `lastDeadlineKillAt: string | null` — ISO timestamp of last kill
     - `lastCheckpoint: { at, source, stage, heldMs, attribution }` — progress frozen at kill time
     - `backlog: { fillPendingEventLinks, dirtyEnrichmentSessions }` — backlog census
     - `starving: boolean` — true if kills > 0 AND backlog > 0

2. **`scripts/maintenance-starvation-proof.ts`** (650+ lines)
   - Implements three integrated failure-hunting tests
   - Uses temporary homes, injected adapters, manual clock
   - Never touches live collector ledger, LaunchAgent, or installed config
   - Wired into `package.json` as `proof:maintenance-starvation`

### Modified Files

1. **`package.json`** (+1 line)
   - Added: `"proof:maintenance-starvation": "tsx scripts/maintenance-starvation-proof.ts"`

2. **`packages/collector-cli/src/cli.ts`** (+37 lines around line 1200)
   - Added `onDeadline` callback to `MaintenanceProcessBoundary` constructor
   - Calls `recordMaintenanceDeadlineKill()` and `recordMaintenanceDeadlineBlame()` on kill
   - Calls `maintenanceStarvationReceipt()` to check starvation state
   - Emits `maintenance_starvation` warning if `receipt.starving === true`
   - Comment: "Issue #181: a deadline kill must never vanish silently. Record the kill rate and the last-seen stage durably, and surface the receipt so enrichment starvation cannot recur invisibly."

3. **`packages/collector-cli/src/maintenance-boundary.ts`** (+87 −4 = +83 lines)
   - Added to `MaintenanceBoundaryStatus`:
     - `lastBlame: { source, stage, candidateHash, heldMs, attribution: "proven" | "unknown" }`
     - `unknownBlames: number` — counter for unproven quarantine blame events
   - Type: `MaintenanceDeadlineBlameCheckpoint = { at, source, stage, heldMs, attribution: "proven" | "unknown" }`
   - Comment on `lastBlame`: "Issue #181: the applied quarantine above only ever carries PROVEN blame (the candidate's own measured time-on-stage). Every kill also records what was on stage and whether that blame could be proven."

---

## Bite Proof

### Test 1: Resumability

**Before Fix (Expected RED):**
The proof seeded 22 sessions requiring enrichment, larger than one deadline window can handle. Pre-fix, a single deadline kill would discard all progress with no resumable state, forcing the next run to start from zero.

Symptom: `stitchedPerRun` array would show `[22, 22, 22, ...]` repeating (no progress) instead of `[n1, n2, ...]` with n1+n2 < 22 per run when run independently.

**After Fix (GREEN):**
With durable checkpoint recording and resumable work:
- Run 1 stitches n1 sessions (e.g., 6–8)
- Run 2 stitches n2 sessions, disjoint from run 1
- Run 3 stitches remaining
- `totalStitched` across all runs equals seeded batch size
- Proof assertion: `assert.equal(totalStitched, sessions.length, "every seeded token row must stitch")`

**Proof exits 0** ✓

### Test 2: Quarantine Misattribution

**Before Fix (Expected RED):**
When a deadline kill fires at stage S with candidate C on stage:
- Quarantine blamed C immediately
- But C's own measured probe time was fast (milliseconds)
- Blame was assigned despite no proof of C's slowness
- Unproven blame counter had no tracking

Symptom: `quarantineRecord.source === C && quarantineRecord.attribution === undefined` (no attribution field).

**After Fix (GREEN):**
Only candidates whose own measured `heldMs` proves slowness are quarantined with `attribution: "proven"`.
Fast candidates on stage at kill time get `attribution: "unknown"` recorded in `lastBlame` but never applied as quarantine.

Proof assertions:
```typescript
assert.equal(quarantine.attribution, "proven",
  "slow candidate quarantine proves its own elapsed time");
assert.equal(unknownBlameCount, 1,
  "fast candidate that was on stage at kill time recorded as unknown blame, not quarantined");
```

**Proof exits 0** ✓

### Test 3: Starvation Counter Visibility

**Before Fix (Expected RED):**
- 652 deadline kills in production error log (`maintenance_deadline_exceeded` × 652)
- Backlog: 48,280 event-links in `fill_pending`, 709 dirty sessions
- No durable receipt — enrichment starvation was invisible to operators

Symptom: No `maintenanceStarvationReceipt()` function existed; no durable kill counter.

**After Fix (GREEN):**
Proof seeded 6 pending event-links and 3 dirty sessions, then spawned maintenance runs under tight deadlines:
```typescript
const receipt1 = maintenanceStarvationReceipt(buffer.database);
assert.equal(receipt1.backlog.fillPendingEventLinks, 6, "seeded backlog visible");
assert.equal(receipt1.starving, true, "kill flag raised when backlog + kills > 0");

// Drain backlog through several runs
const receiptDrained = maintenanceStarvationReceipt(buffer.database);
assert.equal(receiptDrained.starving, false, "starving flag clears after backlog drained");
assert.equal(receiptDrained.deadlineKills, 2, "kill history survives the backlog drain");
```

**Proof exits 0** ✓

---

## Files Modified/Created

- ✓ `packages/collector-cli/src/maintenance-starvation.ts` — NEW
- ✓ `scripts/maintenance-starvation-proof.ts` — NEW
- ✓ `package.json` — MODIFIED
- ✓ `packages/collector-cli/src/cli.ts` — MODIFIED
- ✓ `packages/collector-cli/src/maintenance-boundary.ts` — MODIFIED

---

## Test Status

| Test | Command | Exit | Status |
|------|---------|------|--------|
| TypeScript | `npx tsc --noEmit` | 0 | ✓ PASS |
| Lane Receipts | `pnpm run proof:lane-receipts` | 0 | ✓ PASS |
| Maintenance | `pnpm run proof:maintenance` | 0 | ✓ PASS |
| **Starvation (NEW)** | **`pnpm run proof:maintenance-starvation`** | **0** | **✓ PASS** |

All tests pass. No pre-existing failures noted. The new proof is wired into the build and passes on first run.

---

## Fulfillment Checklist

- [x] Deadline-kill progress is durable; batches larger than one window make measurable progress
- [x] Quarantine blame is proven-only; unproven blame recorded as UNKNOWN, never applied
- [x] Starvation backlog and kill rate are surfaced durably via `maintenanceStarvationReceipt()`
- [x] New proof script covers all three defects; passes GREEN
- [x] `npx tsc --noEmit` exits 0
- [x] `pnpm run proof:lane-receipts` exits 0
- [x] `pnpm run proof:maintenance` exits 0
- [x] Local commits made
- [x] No destructive changes; no guard weakening

