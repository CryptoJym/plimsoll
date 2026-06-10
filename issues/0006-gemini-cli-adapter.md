# 0006 — Adapter: Gemini CLI

## TL;DR
- Same lane as 0005 for Gemini CLI, which ships OpenTelemetry support natively (logs + metrics, configurable OTLP endpoint).
- Likely the easiest adapter: point its OTLP at `127.0.0.1:48271`, map attribute names.

## Scope
Capture + normalization. Mirror 0005's checklist.

## Context
- Gemini CLI telemetry config exposes an OTLP exporter (verify current flag names against its docs before wiring — they have changed before).
- Token usage attribute names will differ from Claude/Codex; extend `usageFieldKeys` lists rather than forking the exploder.

## Acceptance Criteria
- [ ] Real Gemini CLI session → ledger events with tokens attributed and session id mapped.
- [ ] Forbidden-list entries + sentinel for Gemini's raw-content attributes.
- [ ] `docs/adapters.md` updated with the worked example.
