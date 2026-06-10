# 0004 — Codex: verify live span token attribution on a real session

## TL;DR
- Codex records token usage on trace spans (`gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.usage.cache_read.input_tokens`) — our fixture mirrors codex-rs main as of 2026-06-10.
- Nobody has confirmed a real codex 0.137+ session produces token-attributed events through the collector yet.
- Also confirm spans carry `conversation.id` (session attribution) — unverified.

## Scope
Verification + key-map fixes only. If shapes differ, extend `usageFieldKeys` / span handling in `packages/collector-cli/src/{normalizer,otlp}.ts` and add a fixture matching reality.

## Context
- Evidence that logs alone carry zero usage: 91,406 archived codex log events, zero usage attributes (probed `input_tokens|output_tokens|token_count` — only `max_output_tokens` config echoes).
- Span emission source: `codex-rs/otel/src/events/session_telemetry.rs` (`handle_responses_span.record("gen_ai.usage.input_tokens", ...)`).
- Generated config enables `[otel.trace_exporter."otlp-http"]` → `/v1/traces`, protocol json.

## Problem / Task
Run a real codex session against a live collector; inspect `pnpm collector status` → `tokenCoverageLast7d` for `source=codex`. If zero: capture one raw `/v1/traces` envelope (debug ring or temp logging), map the actual attribute names, fix, re-verify.

## Acceptance Criteria
- [ ] `tokenCoverageLast7d` shows codex `sessionsWithTokens ≥ 1` after one real session.
- [ ] Proof fixture updated to the verified live shape (not just codex-rs main).
- [ ] If `conversation.id` is absent on spans: file follow-up for trace→session correlation.

## Open Questions
- Does codex batch spans with `protocol = "json"` correctly, or is binary protobuf forced on traces? (Collector stores a metadata-only stub for non-JSON — that stub appearing is itself a useful diagnostic.)
