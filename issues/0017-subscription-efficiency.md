# 0017 — Plan leverage: subscription cost vs API-equivalent usage

## TL;DR
- "We pay $200/mo for a Claude Max account — what did we get out of it?" Answer: API-equivalent consumption ÷ subscription price = plan leverage (e.g. 9.2×).
- Telemetry already reports claude cost_usd at API-equivalent rates; codex costs now computed from tokens. Missing: a local subscriptions registry and the panel.

## Scope
Local config `subscriptions` (account label → {vendor, plan, usdPerMonth}); dashboard panel: per-account leverage, monthly. Depends on 0018 account labeling.

## Acceptance Criteria
- [ ] Configure one Max account → panel shows subscription $, API-equivalent $, leverage multiple for the window.
- [ ] Unconfigured accounts grouped as "unassigned spend" — never silently dropped.
