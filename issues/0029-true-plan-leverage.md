# 0029 — True plan leverage: sum every plan, split by vendor

Parent: 0027 (#37)

## TL;DR
- One person carries several plans (Claude Max + ChatGPT Pro). dashboardAccounts matched
  subscriptions with find-first — every plan after the first was silently dropped, and
  leverage compared total cross-vendor spend against one plan's cost.
- Now: ALL subscriptions matched by hash, label, **or email** (0028) are summed; account
  rows carry per-vendor spend (claudeUsd/codexUsd); leverage is reported combined and
  per-vendor (plans, window cost, spend, ×) with the split on hover.
- Plan facts come from local truth where available: codex `chatgpt_plan_type` rides
  auth.json (verified "pro" on this machine). Claude tier is not in local config here, so
  the owner's Claude plan is an editable entry, never an invention.

## Acceptance
- [x] Proof `multi_subscription_leverage_sums_plans`: two vendor plans on one identity →
      joined plan name, summed window cost, byVendor length 2 (fails on find-first).
- [x] Existing single-plan check unchanged.
- [ ] Live: owner's plans configured; leverage column shows a real ×-multiple; editing
      a plan updates it (verified post-deploy).
