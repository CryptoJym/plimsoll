# 0015 — Dashboard: local instrument panel served by the collector

## TL;DR
- SHIPPED with this commit: GET / on 127.0.0.1:48271 serves a self-contained, offline instrument panel reading the live ledger — spend, tokens, coverage load-line gauge, daily sparkline, model/repo breakdowns, session table with receipts drawer (join keys, action mix, event types, suppression count).
- Proof checks 15–17 gate it (served / summary reads ledger / receipts traceable).

## Context
`packages/collector-cli/src/dashboard.html` (single file, no CDN/webfonts), `dashboard-api.ts` (read-only SQL), routes in `server.ts`. Localhost-bound only.

## Acceptance Criteria
- [x] Dashboard renders real ledger data offline; auto-refresh 30s.
- [x] Session drill shows lineage (G4 session-level).
- [ ] Follow-up: event-level receipts page (post-v0.1, pairs with coaching layer).

## Notes For Future Agents
Design language: ship instrument panel — load-line mark = coverage gauge, draught rail, mono-everything. Keep it dependency-free; the single-file constraint is a feature (works on any Mac the moment the collector runs).
