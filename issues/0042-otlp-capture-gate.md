# 0042 — OTLP capture gate: retain signal, count discarded noise

## TL;DR

- Admit OTLP records only after privacy sanitization and normalized signal classification.
- Drop only the measured generic wrapper/control-plane name set when a span has no usage, error, action, lifecycle, or analytical linkage.
- Unknown vendor spans fail open; every drop increments a durable, bounded `(source, reason)` counter exposed by `/status`.

## Scope

OTLP span admission, durable discard counters, status readback, and a synthetic adversarial proof. Logs and metrics retain their existing ingest behavior. This does not delete or migrate existing event rows and does not alter external telemetry configuration.

## Context

- Parent: [GitHub #75](https://github.com/CryptoJym/plimsoll/issues/75)
- Tracker: [GitHub #78](https://github.com/CryptoJym/plimsoll/issues/78)
- Baseline: `origin/main@9fc0af4cb59b01245f7a1862ba1647a152c8b537`
- Trace: `46be3ad1-514a-42d2-9f14-2212fdab14dc`

The gate is deliberately conservative. Technical trace/span IDs alone do not make a known Codex wrapper analytically valuable, but a session, actor, work key, git linkage, request/call identifier, usage value, error/exception, tool/action, or lifecycle classification does. An unfamiliar span name is retained until evidence establishes a safe bounded rule.

## Evidence

The 2026-07-15 read-only survey recorded in #78 found:

```text
codex otel_span rows       4,130,123
codex otel_span payload       3,296.4 MiB
token/cost carried                   0
```

A follow-up read-only `rowid % 1000` sample found that `handle_responses` alone represented only 239 sampled rows. The exact 16-name wrapper/control-plane set implemented here represented 4,020 rows, approximately 97.3% of the roughly 4,130 rows expected from the measured Codex zero-value population at that sampling rate. The largest names were `app_server.serialized_request_queue` (856), `codex.websocket_event` (833), `thread/resume` (431), `thread/read` (385), `thread/list` (343), and `thread/goal/get` (338). The sample suggests roughly 4.02 million existing-shape rows per 4.13 million would be rejected at admission; this is an extrapolation, not a live deletion or migration.

## Acceptance Criteria

- [x] The focused proof covers every measured generic name plus an ambiguous/unknown span, error status, exception event, token span, tool span, lifecycle span, linkage-only span, logs, metrics, and raw-content sentinels.
- [x] A signal-free known generic wrapper produces no `buffered_events` row.
- [x] Unknown spans and every retained dimension above remain persisted.
- [x] Error messages, stack traces, prompts, and arbitrary log bodies do not persist in metadata mode.
- [x] `/status` exposes durable dropped counts by bounded source and reason without scanning `buffered_events`.
- [x] Counter state survives a buffer reopen.
- [x] `pnpm exec tsx scripts/otlp-admission-proof.ts` and the CLI build pass.
- [x] `pnpm proof` was rerun: the three OTLP compatibility checks pass; the known calendar-aged dashboard fixture failures remain owned by [GitHub #82](https://github.com/CryptoJym/plimsoll/issues/82).

## Operational Boundaries

No existing live rows are deleted. No installed service, live ledger, telemetry source, environment variable, or provider is changed. The proof uses a temporary HOME, database, and port. The metadata privacy invariant remains mandatory.

## Notes For Future Agents

Do not broaden the generic-name set from intuition. Add a name only with measured volume/value evidence and an adversarial fixture proving error/action/linkage variants remain admitted. The counters are cumulative collector-lifetime telemetry stored in their own constant-cardinality table; they are not derived by scanning event history.
