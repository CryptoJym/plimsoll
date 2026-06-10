# 0005 — Adapter: Cursor

## TL;DR
- Add Cursor as a `ToolSource` so Cursor sessions land in the ledger with tokens (if exposed), tool events, and action classes.
- Template lane for all future adapters — document the pattern as you go.

## Scope
Capture + normalization only. No outcome-join changes (linkage is tool-agnostic once cwd/workdir is captured).

## Context
- `packages/shared/src/schemas.ts` → `toolSourceSchema` enum gains `"cursor"`.
- `packages/collector-cli/src/normalizer.ts` → `inferSource` + action-class table handle new tool names.
- Investigate what Cursor exposes: local logs? OTLP? hooks? An adapter may be a small watcher process that POSTs to `/hooks/cursor` or replays into `/v1/logs` — the receiver accepts any source via the `x-plimsoll-source` header.

## Acceptance Criteria
- [ ] A real Cursor session produces ledger events with source=cursor, sessionId, and actionClass ≠ other for ≥80% of tool events.
- [ ] Raw-content fields from Cursor's surface added to the forbidden list, with a sentinel proof check.
- [ ] `docs/adapters.md` started: the four things an adapter must provide (source id, transport, session key, raw-content field names).

## Operational Boundaries
- Proof stays green; metadata-mode promise extends to the new source from day one.
