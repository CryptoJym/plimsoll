# 0014 — Codex: stitch sessionless usage spans to conversations

## TL;DR
- Live-verified (codex 0.137.0, 2026-06-10): token usage arrives on `handle_responses` spans with NO `conversation.id` and NO model attr; `codex.turn.token_usage` metrics carry model + full token_type breakdown but no conversation either.
- Codex tokens are captured exactly (reconciled to the digit: 32,562 in − 1,920 cached + 35 out = 30,677 = codex's own "tokens used") but land sessionless → excluded from per-session/PR economics.
- Fix: correlate via traceId — sibling spans/log events in the same trace carry `conversation.id`.

## Scope
Collector-side stitching at receive time (preferred) or report-side join. No codex changes assumed.

## Context
- Usage span stored with `metadata.traceId` + `metadata.spanId` (buildSpanEvent in `packages/collector-cli/src/otlp.ts`).
- Codex log events (`codex.user_prompt`, `codex.tool_result`) carry `conversation.id`; whether they share the trace's id needs one live capture to confirm. Alternative: time-window + originator match (single-machine, exec sessions are serialized — weaker).
- `codex.turn.token_usage` histogram (token_type ∈ input/output/total/cached_input/reasoning_output, model attr) is a second token source — must not double-count with spans (rule: spans canonical, metrics reconciliation-only, same as claude).

## Evidence
Live session 2026-06-10 18:4x: ledger rows — assistant_response {in:32562, out:35, cache:1920, session:null, name:handle_responses}; codex.turn.token_usage samples {input:32562, output:35, cached_input:1920, reasoning_output:24, total:32597, model:gpt-5.5}; 154 sibling otel_span rows, 1 with conversation.id; zero parse stubs (json traces work).

## Acceptance Criteria
- [ ] After one real codex session: `tokenCoverageLast7d` shows codex `sessionsWithTokens ≥ 1`.
- [ ] Usage events gain sessionId + model via stitching; proof check encodes the trace-correlation fixture.
- [ ] No double counting: summed codex tokens match codex's own "tokens used" line for the session.

## Operational Boundaries
- Proof stays green; stitching must not delay ingest (async backfill of session ids is acceptable).
