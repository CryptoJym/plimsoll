# The Product Gate

> Anyone — a maintainer, an employee, a stranger from HN — goes from zero to
> seeing their real AI usage and efficiency numbers in under five minutes.
> Every displayed number is traceable to its receipts. The system loudly
> reports its own coverage and health, so silence can never again mean
> failure. The proof culture extends upward until it verifies the product
> promises themselves — and we grind until every gate is green.

Run `pnpm proof` for gates marked (proof). Others list their manual check
until automated. Re-verified every iteration; a gate that regresses reopens
its issue.

| # | Promise | Gate | Status |
|---|---|---|---|
| G1 | Capture is truthful (per-record tokens, classes, privacy) | proof: 14 core checks | ✅ |
| G2 | Codex tokens captured & reconciled to vendor-reported count | live session evidence (issue #4) | ✅ exact (30,677) |
| G3 | See it: local dashboard on the live ledger, offline | proof: dashboard_served/summary/receipts | ✅ |
| G4 | Traceability: number → receipts (join keys, events, coverage) | proof: dashboard_session_receipts_traceable | ✅ session-level |
| G5 | Easy link-up: fresh Mac → numbers < 5 min, no hand-editing | scripted fresh-Mac walkthrough (issues #3, #11) | 🔴 |
| G6 | Fleet: teammate Mac joins a workspace with one command | join flow (issue #16) | 🔴 |
| G7 | Self-honesty: capture-health alarm when telemetry stops | health check + alert (issue TBD) | 🔴 |
| G8 | Reconciliation: local sums vs vendor bill as displayed coverage | vendor import + gauge (private cloud) | 🔴 |
| G9 | Codex economics session-complete | issue #14 stitching | 🔴 |
| G10 | CI guards all of the above on every PR | GitHub Actions proof | ✅ |
