# 0027 — Parent: identity emails, true plan leverage, the five-minute promise, per-event repos

## TL;DR
- Owner directive (2026-06-10 eve): accounts are tied to emails — store them safely (local-only);
  then ship priorities 1–4 from the Phase-A close-out: plan leverage, the five-minute promise,
  codex identity in the tailer, per-event repo attribution. Define the goal and the
  post-completion checks BEFORE implementation; finish with a workflow audit.

## Goal
Every number on the dashboard is attributable to a person (email), priced against what that
person actually pays (their real plans, read from local tool config — never guessed), installable
on a fresh machine in under five minutes, and attributed to the right repo at event grain.

## Child lanes
- **0028 / Lane A — account emails + codex identity.** `account_labels.email` (LOCAL-ONLY);
  settings UI per identity; auto-suggest from `~/.claude.json` oauthAccount.emailAddress and
  `~/.codex/auth.json` email; rollout tailer stamps actorId = hashProtectedValue(chatgpt_account_id)
  going forward (history stays identity-less — honest, no retro-guessing).
- **0029 / Lane B — leverage correctness + real plans.** dashboardAccounts sums ALL matching
  subscriptions (today: find-first drops the second plan); per-vendor spend split on account rows;
  vendor-aware leverage. Plans auto-suggested from local config (claude userRateLimitTier,
  codex chatgpt_plan_type) with public prices; owner-editable.
- **#8 / Lane C — per-event repo attribution.** Usage/token events get the session's
  current repo stamped at ingest; enrichment pass for history; repos view event-grain
  with session-grain fallback.
- **#3 + #11 + #1 / Lane D — five-minute promise.** `plimsoll setup` applies Claude env +
  codex otel config idempotently (diff + confirm + backup); `@plimsoll/cli` packaged and
  npx-verified from a local tarball (public npm publish is the owner's button); timed fresh-
  environment walkthrough recorded as evidence under #1.

## Key checks after completion (the close-out gate)
1. `pnpm proof` green; every lane lands ≥1 check that fails without it.
2. Privacy: a raw-email sentinel ingested via fixtures appears in NO upload body
   (alongside existing hostname/username/label checks). Emails live in account_labels only.
3. Reconciliation invariants re-run clean: cross-view totals equal; today's codex ledger ==
   rollout finals; capture watch green both sources after every restart.
4. Leverage column live: shows real ×-multiple computed from ≥2 summed plans; per-vendor
   numbers visible; editing/removing a plan updates it.
5. Repos view: a multi-repo session's cost splits by event-grain repo (proof fixture +
   live spot-check); totals still reconcile to summary.
6. Five-minute walkthrough: fresh environment from zero → dashboard with real numbers,
   timed, transcript recorded; `plimsoll setup` second run reports no-op.
7. Dashboard stays responsive during scans (no event-loop blocking regressions).
8. Then: workflow audit session over the whole system with the owner.

## Operational Boundaries
- Raw emails, plan names, and identities NEVER enter event payloads or upload bodies —
  hash-locally/label-locally pattern, proof-enforced.
- No npm publish without the owner pressing the button. No invented prices: tier→price
  mapping uses public list prices, anything unknown stays blank for the owner.
