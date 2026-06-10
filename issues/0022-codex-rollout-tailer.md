# 0022 — Codex adapter v2: tail rollout files for 11/11 session coverage

## TL;DR
- OTLP usage spans only arrive from some codex frontends: today `codex exec` delivered exact
  tokens, while 10 `codex-app-server`-driven sessions (fan-out harness) sent operational spans
  and metrics but no usage spans → ledger captured 0.96% of the day's codex input tokens.
- Codex always writes rollouts (`~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`) with
  `token_count` events carrying full totals. Tail them as the codex source of truth; keep OTLP
  as enrichment/cross-check.

## Evidence
- 2026-06-10 ground truth across 11 rollouts: 42,748,572 input / 39,364,736 cached / 183,512
  output. Ledger: one session, 32,562 / 1,920 / 35 — which matches its rollout to the digit, so
  the pipeline is exact when fed.
- App-server spans observed in ledger: `rpc.method initialize`, `serviceName codex-app-server`,
  `client_name codex-browser-use` — no `gen_ai.usage.*` anywhere in today's 1,962 codex spans.

## Problem / Task
A codex session on this machine shows up with tokens in the ledger regardless of how it was
launched (TUI, exec, app-server, IDE). Coverage line in `collector status` reads 11/11.

## Acceptance Criteria
- Fixture rollout file (mirroring codex 0.137 `token_count` shape) ingested by proof → events
  with exact tokens, session id from filename/conversation, model from `codex.turn.token_usage`
  metric or rollout, priced via shared/pricing.ts with `costEstimated` flagged.
- Dedupe against OTLP-delivered usage for the same session (no double count) — proof check.
- Live: day's ledger codex totals == sum of rollout `token_count` finals (reconciliation rerun).

## Operational Boundaries
- Metadata mode: no prompt/content fields from rollouts, ever — token/timing/model only.
- `pnpm proof` stays green; existing span reconciliation (`buffer.reconcileCodexUsage`) keeps
  working where spans do arrive.

## Notes For Future Agents
- May-2026 rollouts predate token telemetry in places; absence stays $0 (honest history).
- Multiple codex accounts exist on this machine — account attribution from rollout auth context
  if present; never guess.
