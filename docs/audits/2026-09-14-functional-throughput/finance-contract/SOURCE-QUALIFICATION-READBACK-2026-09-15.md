# Finance source qualification readback — 2026-09-15

## Result

**Not qualified.** The latest native Plimsoll suite bridge readback reports Finance `unavailable` with `reason=qualified_export_failed` at `stage=summary_verify`. No monetary snapshot, source artifact digest, statement, attestation, company binding, or recognized amount was published.

## Native evidence

- Readback: `/Users/utlyze/Library/Memory/ops/plimsoll-suite-bridge/latest.json`
- Publisher source commit: `d215601c38e970b5ee08539d38ec6bc3c90f0652`
- Finance result: `accepted=true`, `state=unavailable`, `observedAt=null`
- Failure record: `financeSource.state=unavailable`, `financeSource.reason=qualified_export_failed`, `financeSource.stage=summary_verify`, `financeSource.errorType=RuntimeError`
- Bridge receipt timestamp: `2026-09-15T18:10:26.426521+00:00`
- `secretPrinted=false`

The bridge's `accepted=true` means the health result was accepted as a publication outcome; it does not mean the monetary source was accepted. The source remains unavailable until the owner supplies an immutable artifact and native qualification packet.

## Consequence for the delivery graph

`eco-6hoxj.124.2` remains in progress. `eco-6hoxj.124.3` must remain blocked from production reader wiring until the source-owner packet satisfies the contract. The current Finance health feed cannot be relabeled as recognized expense, cash movement, revenue, or a zero-value snapshot.

The next valid input is the source-owner response for `nrf-j86.17.4.3`: immutable artifact/version and digest, authorized company/workspace scope, exact requested period and watermark, statement/attestation/evidence references, identity and replay rules, allocation/reversal rules, and explicit refusal cases. Monetary values are not required in the shared packet.

## Check performed

Read-only JSON readback and contract comparison completed from the existing publisher path. No publisher, source ledger, credentials, schedule, database, cloud application, or deployment was changed.

## Bounded failure analysis

A read-only inspection of the immutable edition and the publisher validator identifies the current deterministic blocker: the oldest successful source timestamp is `2026-09-08T02:24:27.698Z`, while the edition ends at `2026-09-08T00:00:00.000Z`. The validator permits the period watermark but rejects a source older than its 24-hour freshness limit; on the September 15 bridge run this produces an unavailable result (the bridge intentionally records only the safe `qualified_export_failed` category).

The edition's structural checks are otherwise observable as complete: the manifest and export agree on the period, immutable identities, four-account scope, sixteen exported/observed rows, one removed row, zero unknown accounts/rows and zero scope violations; the Plaid reconciliation receipt has two successful items and zero scope violations. These checks establish source-shape integrity only. The files are explicitly classified `UNKNOWN`, the company scope is `import_workspace_not_legal_entity`, and no recognized-expense or project-allocation publication exists. They cannot qualify the FinanceSnapshot contract or feed `.124.3`.
