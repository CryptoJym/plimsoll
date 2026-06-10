# 0021 — Capture health: silence must scream (G7)

## TL;DR
- v1 died silently for 2.5 weeks. Today the sessions panel 500'd behind a green lamp, and 10 of
  11 codex sessions captured zero usage while the dashboard read "collecting".
- Outcome: the collector measures its own coverage and the dashboard turns visibly red/amber the
  moment capture degrades — per source, with the reason.

## Problem / Task
The lamp currently answers "is the server up", not "is the data whole". Operator-visible outcome:
1. Per-source freshness on the dashboard: time since last claude_code event, last codex event,
   last token-bearing event of each — amber/red thresholds, not buried in a drawer.
2. Local activity cross-check: compare ledger sessions against locally observable activity
   (claude transcript mtimes, codex rollout files) and show "N sessions ran here, M captured".
   This is the check that catches "telemetry off in that shell" — the failure mode reconciliation
   found (evidence/2026-06-10T21-25-26-000Z-reconciliation.md, verdicts 6–8).
3. Per-panel API errors render as errors (the /api/sessions 500 was invisible).
4. `collector status` (CLI) and `/status` (HTTP) expose the same coverage numbers for scripts.

## Evidence
- `collector.err.log`: repeated `{"warning":"collector_request_rejected","path":"/api/sessions",
  "message":"misuse of aggregate function max()"}` while lamp showed "collecting".
- 2026-06-10: codex ground truth 11 sessions / 183,512 output tokens; ledger 1 session / 35.

## Acceptance Criteria
- A proof check that seeds a stale ledger and asserts the health payload reports degraded
  per-source freshness (and that fresh data reports healthy).
- Dashboard renders a degraded state distinguishable at a glance (screenshot in evidence/).
- Killing telemetry in one source (simulated) flips the panel within one refresh interval.

## Operational Boundaries
- `pnpm proof` stays green; health computation reads the ledger + local files only (no network);
  nothing new leaves the machine.

## Open Questions
- Threshold defaults: what idle gap is "expected quiet" vs "broken pipe" on a machine that
  sleeps? (Owner works in bursts; transcript/rollout mtimes give the activity baseline.)
