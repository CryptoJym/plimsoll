# OX-NOTES — Issue #146: 0076 Dashboard proof: mobile DOM waits must be race-safe

## Root cause

`waitForText` in `scripts/dashboard-security-proof.ts` (pre-fix, lines ~885–892) evaluated
`document.body.textContent.includes(marker)` via CDP `Runtime.evaluate` immediately after
`Page.navigate`. Two races against navigation commit:

1. **Null dereference (the hosted failure).** Before the navigated document commits,
   `document.body` is still `null`, so the expression throws
   `TypeError: Cannot read properties of null (reading 'textContent')`, which surfaced through
   `evaluate()`'s `exceptionDetails` path and rejected the whole browser-proof operation.
   Hosted run `29708869818` attempt 1 died exactly this way in the Linux/mobile phase and
   attempt 2 passed on the identical SHA — proof nondeterminism, not a product signal.
2. **Stale-node acceptance.** On the second (mobile) viewport phase, the pre-commit document is
   still the *desktop* page render, which already contains the marker text. The old wait accepted
   it immediately (`elapsedMs === 0`), letting every later evaluation race the real load.

## Fix

- **New module `scripts/fixtures/dashboard-dom-wait.ts`**
  - `domTextReadinessExpression(marker)` — null-guarded probe expression: unmounted body ⇒
    `"missing"` (pending), body without marker ⇒ `"mounted"`, marker present ⇒ `"ready"`.
    It never dereferences a missing node.
  - `waitForDomText(env, { timeoutMs, pollMs })` — bounded poll loop with injectable
    clock/delay/signal. Timeout receipts distinguish `node_never_mounted`,
    `node_mounted_text_mismatch`, and `generation_not_committed`; page/browser failures
    (e.g. `cdp_socket_closed`) propagate as their own errors; overall-watchdog cancellation
    throws the watchdog error. Receipts carry only whitelisted booleans/counters/stage names —
    zero page content. No silent retries; every poll outcome lands in the receipt.
  - `LoaderGenerationGate` — arms each phase with the `frameId`+`loaderId` returned by its own
    `Page.navigate` call and only accepts readiness once `Page.frameNavigated` reports that exact
    generation committed. Stale prior-page nodes can never satisfy a later phase. (Note:
    `frameNavigated` carries the frame under `params.frame.id`, not `.frameId` — verified live.)
- **`scripts/dashboard-security-proof.ts` integration**: desktop and mobile phases now navigate,
  arm the gate with their own loaderId, wait via the race-safe module, emit a
  `browser_<phase>_readiness_generation_gated` check receipt, and throw a content-free
  `ProofTimeoutError("dashboard_readiness", detail)` on bounded expiry. Top-level stderr keeps the
  byte-exact `browser_proof_overall` surface (detail only added when present).
- **New gate `scripts/dashboard-dom-wait-proof.ts` + `pnpm proof:dashboard-dom-wait`** (also wired
  into `.github/workflows/proof.yml` right after the dashboard-security step): deterministic
  fake-clock fixtures, no browser needed.

## Files changed

- `scripts/fixtures/dashboard-dom-wait.ts` (new)
- `scripts/dashboard-dom-wait-proof.ts` (new)
- `scripts/dashboard-security-proof.ts` (wait path + stderr surface only; product untouched)
- `package.json` (`proof:dashboard-dom-wait`)
- `.github/workflows/proof.yml` (new CI step)

Local commits: `bcd678c`, `4c61483` on `main` (not pushed — push/railway forbidden).

## Failing → passing evidence (adversarial fixtures)

To prove the new tests fail on current code, I temporarily swapped
`scripts/fixtures/dashboard-dom-wait.ts` for a faithful shim of the OLD shipped algorithm
(dereferencing probe, no generation gate, opaque timeout), ran the suite, then restored the fix.

Command: `npm run proof:dashboard-dom-wait`

- **Against legacy-semantics shim (old code behavior): 4/14 passed, 10 FAILED**, including
  `dom_readiness_expression_null_safe_for_every_document_state` reproducing verbatim
  `TypeError: Cannot read properties of null (reading 'textContent')` and
  `dom_wait_stale_prior_page_node_gated_by_navigation_generation` showing stale acceptance at
  `elapsedMs 0`. Full output preserved at
  `/var/folders/.../T/opencode/p146/legacy-shim-run.txt` (summary line:
  `"passed":4,"failed":[...10 names...]`).
- **Against the fix: 14/14 PASS**, three consecutive runs, exit 0.

The suite also contains an inline behavioral port of the old algorithm (`legacyWaitForText`) so
every adversarial fixture asserts both sides forever: legacy must exhibit the bug, fixed module
must produce the correct bounded receipt.

## Acceptance coverage

| Required fixture | Where |
|---|---|
| Immediate mount | `dom_wait_immediate_mount_ready_on_first_poll` |
| Delayed mount just before deadline | `dom_wait_mount_just_before_deadline_accepted` |
| Never mount | `dom_wait_never_mount_timeout_receipt_distinguishes_missing` |
| Wrong text | `dom_wait_wrong_text_timeout_receipt_distinguishes_mismatch` |
| Node removal/reinsert | `dom_wait_removal_reinsert_recovers_within_deadline` |
| Stale prior-page node | `dom_wait_stale_prior_page_node_gated_by_navigation_generation` (+ real-browser gate check per phase) |
| Browser exit | `dom_wait_browser_exit_propagates_as_page_failure` |
| Outer-watchdog cancellation | `dom_wait_outer_watchdog_cancellation_settles_all_timers` |
| ≥100 repeated mobile iterations, identical semantic receipts, zero intermittents | `dom_wait_mobile_scenario_150_repeats_identical_semantic_receipts` — 150 seeded-jitter iterations, all `{"ok":true}`, 0 failures |

All timing is injected fake-clock (`FakeClock`); acceptance rests on receipts, not CI reruns.

## Exact proof commands + results (this machine, macOS arm64, Node v22.22.0)

| Command | Result |
|---|---|
| `npx tsc --noEmit` | exit 0 |
| `npm run proof:dashboard-dom-wait` | exit 0 — `{"checks":14,"passed":14,"failed":[]}` ×3 consecutive |
| `npm run proof:dashboard-dom-wait` (legacy shim swap) | exit 1 — `passed 4 / failed 10` (expected, evidence above) |
| `npm run proof:dashboard-security` | exit 0 — `{"proof":"dashboard-security","checks":19,"passed":19,"failed":[]}` ×3 consecutive exact-source runs (incl. `browser_mobile_readiness_generation_gated` and `browser_mobile_dom_inert`) |
| `npm run proof:privacy-mode -- --receipt evidence/privacy-mode-proof.json` | exit 0 — 15 checks, no private sentinel |
| `node tsx scripts/resource-proof/index.ts --require-integrated --receipt evidence/resource-proof.json` | exit 0 — `"overall":"pass"` (13 pass incl. dashboard_projection_budget, metadata_privacy_sentinels) |
| `node tsx scripts/resource-proof/finalization-proof.ts` | exit 0 — `"passed":true`, 0 private-term leaks |
| `npm run proof:system-e2e -- --receipt evidence/system-e2e-proof.json --expected-source-commit <HEAD>` | exit 0 |
| `npm run proof:system-e2e:verify -- --receipt evidence/system-e2e-proof.json …` | exit 0 |
| `npm run proof:system-e2e -- --receipt …repeat.json --compare-deterministic-receipt …json …` | exit 0 (deterministic digest match) |
| `npm run proof:system-e2e:verify -- --receipt …repeat.json …` | exit 0 |
| `npm run proof:system-e2e:tamper -- --receipt …repeat.json …` | exit 0 — all tamper cases rejected |

Notes on environment noise: this host ran at load average 75–190 during parts of the session.
Timing-budget gates (`resource`, `finalization`, `maintenance`, e2e subprocesses) flaked under that
load; each was re-run to green once load dropped (~25). Two failures were baseline-compared on an
untouched `HEAD~1` worktree to confirm they are pre-existing environment issues, not regressions:

- `proof:maintenance` fails identically on HEAD~1 (`candidate_metadata_soft_cap…` dirent cadence).
- `proof:dashboard` fails identically on HEAD~1 (`TypeError: fetch failed`).

## Honest acceptance status

- ✅ Deterministic fixtures for all eight required scenarios — done (table above).
- ✅ ≥100 repeated mobile iterations with identical semantic receipts, zero intermittents — 150/150.
- ⚠️ **Real Linux hosted proof, two exact-source runs without manual rerun — NOT DONE locally.**
  Pushing and `gh` are forbidden here, so no hosted run could be triggered from this clone. The
  local equivalents were executed instead: three consecutive full `proof:dashboard-security`
  runs green on the exact committed source (macOS + headless Chrome), plus the new CI step
  committed so the hosted two-run requirement executes automatically on next push. This is the one
  checkbox I cannot close from inside this sandbox.
- ✅ CSP, inert DOM, teardown escalation, socket cleanup, privacy, resource, system-E2E/verifier,
  and tamper gates remain green locally (commands/results above); no assertion weakened, no retry
  loop added, no dashboard product file touched (`packages/**` unchanged — `git show --stat bcd678c`).
