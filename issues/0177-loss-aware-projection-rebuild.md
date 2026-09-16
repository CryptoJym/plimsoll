# 0177 — Loss-aware dashboard projection rebuild

## TL;DR
- A collector that opens a projection stamped by a newer `DASHBOARD_SCHEMA_VERSION` fails closed (`degraded_reason='projection_schema_newer'`) and withholds every session count.
- If the newer binary is gone, that withholding has no recovery except reinstalling a binary that understands the stored version. The heavier alternative — rebuild the derived projection from the raw ledger, accepting that some derived facts may have aged out — was deferred and is **owed**.
- This sounding is that owed work. It does not ship the rebuild.

## Scope
In: design and implement a loss-aware rebuild (or an explicit later decision not to), so a host stuck on `projection_schema_newer` has a filed remedy that does not require the newer binary.

Out: changing the fail-closed guard; serving frozen-green counts; silently dropping the stored schema version so a downgraded binary can half-read a newer shape.

## Context
REVIEW-149-145 NOTE-1 (MEDIUM, missing-record) against PR #365 / squash `4c839b98`. eco-6hoxj.145 shipped the guard; eco-6hoxj.149 shipped recovery-doc truthfulness. The README already names rebuilding from the raw ledger as the "heavier alternative" and now points here.

A projection newer than the running binary is refused rather than served. Maintenance does no derived work. On a host where the newer collector is gone, counts stay withheld indefinitely.

## Problem / Task
Give a stuck-withholding host a recovery that this binary can run: rebuild derived dashboard tables from `buffered_events` (and whatever compact/repair facts still exist), record what could not be reconstructed, and restamp `schema_version` to this binary's `DASHBOARD_SCHEMA_VERSION` only after the rebuild is coherent enough to serve.

## Acceptance Criteria
- [ ] A rolled-back ledger whose stored `schema_version` is greater than `DASHBOARD_SCHEMA_VERSION` can be rebuilt without the newer binary, or an explicit filed decision documents why that path will not exist.
- [ ] The rebuild never serves a frozen-green count from the pre-rebuild snapshot.
- [ ] Loss is visible: derived facts that cannot be reconstructed from the remaining raw/compact ledger are named, not filled with zeros that read as health.
- [ ] `pnpm proof:dashboard` covers the rebuild (or the explicit decision) and stays green.
- [ ] The fail-closed guard remains the default open path until the rebuild is invoked.

## Operational Boundaries
- `pnpm proof` stays green. No raw content persists in metadata mode.
- Do not weaken the downgrade guard to finish this issue.
- No live ledger, service, LaunchAgent, provider, cloud, or deployment mutation from the design work.

## Notes For Future Agents
- Guard and stamp live in `packages/collector-cli/src/dashboard-projection.ts` (`reconcileSchemaVersion`, `projection_schema_newer`).
- Proof of the current withhold path: `scripts/dashboard-projection-proof.ts` (`a_projection_written_by_a_newer_schema_is_refused_not_served_frozen_green`).
- README section "Rolling the collector back: the projection schema version".
- A missing `dashboard_projection_control` row on an existing control table is a separate refusal (`projection_control_missing`), not this rebuild.

## Open Questions
- Is the rebuild an operator command (`plimsoll projection rebuild --confirm-exact …`), an automatic open-time path behind an explicit flag, or neither (leave withhold + reinstall as the only recovery)?
