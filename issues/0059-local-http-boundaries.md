# 0059 — Local HTTP boundary: authenticate producers and bound request work

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/108

## TL;DR
- Make loopback ingestion source-authenticated and resistant to spoofing, compression bombs, oversized/deep/high-cardinality OTLP, and browser-origin attacks.
- Keep only minimal health unauthenticated; separate producer write credentials from management/read access.
- Bounded slice of 0056 / #104.

## Scope
Local server parser/routes, generated Claude/Codex config, local credential creation/rotation, admission receipts, and adversarial proof. Dashboard rendering is 0060.

## Acceptance Criteria
- [ ] `Host` allowlist enforced and browser `Origin` rejected on hook/OTLP writes; no permissive CORS.
- [ ] Claude and Codex use distinct Plimsoll-local producer tokens bound to claimed source; wrong/missing/source-swapped tokens fail before ledger mutation.
- [ ] Settings/identity/export require separate management credential; minimal liveness exposes no sensitive state.
- [ ] Compressed, decoded, ratio, JSON depth, resources/scopes/records/attributes, string/numeric ranges, request deadline, and per-source rate have explicit ceilings.
- [ ] Oversize/compression-bomb/deep/high-cardinality/future-time/huge-cost fixtures stay within fixed CPU/RSS/write/receipt budgets and do not leak rejected values.
- [ ] Same ID/same digest dedupes; same ID/different digest creates a bounded collision/quarantine receipt.
- [ ] Setup is idempotent and never reuses Claude/Codex account credentials.

## Operational Boundaries
- Tests use isolated local servers/homes only; no live tool config or collector state.
