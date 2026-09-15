# Finance monetary source contract

**Bead:** `eco-6hoxj.124.2`  
**Status:** contract frozen; source qualification pending  
**Owner:** `session-e827b29512a12e76e8233ab00031bccb`  
**Recorded:** 2026-09-15

This document freezes the boundary that the next reader (`eco-6hoxj.124.3`) may consume. It does not assert that a qualified monetary source is currently available and it does not create a ledger, bank connection, provider credential, database table or financial amount.

## Current source finding

The cloud economics engine already has an attested `FinanceSnapshot` boundary and exact minor-unit reconciliation. The default loader does not provide a Finance reader, so the real workspace currently reports Finance as unavailable. The Connections Finance observation is an operational health summary only; its counts do not attest recognized expense, revenue or cash. The existing signed workspace readback records Finance as unavailable/partial, and the UX audit explicitly found no qualified monetary publication. The referenced source-owner work (`nrf-j86.17.4.3`) is an input to qualification, not proof that this adapter exists.

Evidence pointers:

- Contract types: `plimsoll-cloud/src/lib/economics/contracts.ts` (`FinanceLine`, `ExpenseAllocationPlan`, `AllocationReversal`, `FinanceSnapshot`).
- Validation and conservation: `plimsoll-cloud/src/lib/economics/finance.ts` (`reconcileFinance`).
- Reader seam: `plimsoll-cloud/src/lib/economics/loader.ts` (`EconomicsEvidenceSources.finance`).
- Operational-source boundary: `plimsoll-cloud/docs/suite-source-bridge.md` and `plimsoll-cloud/docs/audits/mvp-ux-2026-09-15/README.md`.
- Native product finding: `plimsoll/docs/audits/2026-09-14-functional-throughput/LIVE-WORKSPACE-READBACK.md`.

## Frozen adapter interface

The source owner must expose one tenant- and period-bound read equivalent to:

```ts
readFinanceSnapshot(
  tenantId: string,
  period: { start: string; end: string },
): Promise<FinanceSnapshot | null>
```

`null` means the source is unavailable or not qualified for that exact request. The reader must never synthesize a snapshot from usage telemetry, quota, provider reference pricing or the operational Finance health feed.

A returned snapshot is accepted only when all of these are true:

1. `tenantId` is the authenticated workspace tenant, and every line, plan and reversal has the same tenant. The adapter must also bind the source's company scope to the authorized workspace; the current health publisher's import-workspace scope is insufficient evidence for this check.
2. `period` is an exact half-open UTC interval `[start, end)`. The snapshot period equals the requested period; a full-period source is not prorated into an arbitrary sub-period.
3. `observedThrough` is canonical UTC, is not in the future, and is not earlier than the snapshot start. `complete=true` is meaningful only when the watermark reaches the requested end (the half-open period's exclusive boundary).
4. `manifestDigest` is a 64-character lowercase SHA-256 digest of the source edition/manifest. `attestationRef` is a bounded source-owner attestation identifier.
5. `lines` and `plans` are bounded to 10,000 entries. Each monetary amount is a signed decimal string of minor units; no floating-point dollars cross the boundary.
6. Every line carries its company, source line identity/version, statement reference, attestation reference, evidence reference, currency/exponent, tax treatment and contained line period. A line kind is exactly one of `recognized_expense`, `recognized_revenue` or `cash_movement`.
7. A source line ID is idempotent within its source version. Repeating the same line is allowed only when its canonical content is identical; a conflicting duplicate is rejected.
8. Allocation plans name the same source line, source version, period and manifest. Their splits may claim only part of a positive recognized expense; any remainder remains an explicit unallocated allocation. The plan generation and policy evidence are preserved.
9. A reversal names a negative recognized-expense credit and the exact prior allocation IDs it reverses. Credits must conserve against those original rows; duplicate or foreign reversals are rejected.
10. The source owns recognition policy and cash timing. The adapter does not infer revenue, taxes, bank identity, provider account identity or project attribution from token usage.

## Required source acceptance packet

The Finance source owner must provide a source-linked packet before `.124.2` can close and before `.124.3` implementation is accepted:

- exact source artifact path or immutable URL, revision/edition and SHA-256 manifest digest;
- native owner readback naming the company/workspace scope and the source authority;
- one exact requested period, recognition period and cash period, with `observedThrough` and `complete` status;
- line identity/version rules and the statement, attestation and evidence references for each line;
- currency/exponent and tax treatment rules;
- allocation policy version, generation, split/remainder rules and reversal/credit rules;
- duplicate, replay, missing, stale, wrong-tenant and wrong-period refusal results;
- conservation result for expense lines and exact reversal result;
- explicit list of fields that remain unknown or unavailable.

The packet may omit monetary values from the shared report when the source owner cannot authorize them; it must still prove the shape, authority, period and integrity of the publication. A fixture is proof of the validator only, never proof of a real source.

## Negative cases that must stay fail-closed

The reader must return unavailable or refuse the request for a missing source, a failed source qualification check, a stale watermark, a future watermark, a foreign tenant/company, a mismatched period, an invalid manifest or attestation, a conflicting duplicate line, an allocation overclaim, an unbound reversal or an incomplete period presented as reconciled.

## Handoff to `.124.3`

`.124.3` may implement a bounded adapter and loader wiring only after this document has a source-owner/native qualification packet. It must pass the accepted `FinanceSnapshot` unchanged to the existing reconciliation engine, preserve the unavailable/partial states, and prove tenant, period, stale/failure, duplicate, allocation and reversal refusals. It must not add a second Finance pipeline or write the source ledger.
