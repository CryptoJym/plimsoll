# 0056 — Local security: authenticated bounded ingestion and inert dashboard rendering

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/104

## TL;DR
- Loopback is not authentication: local processes can currently spoof source data, read settings, and poison metrics.
- Bound compressed/decoded request work and OTLP cardinality so malformed local traffic cannot recreate the CPU/RAM problem.
- Eliminate stored dashboard XSS by rendering data as text with strict browser headers.

## Scope
Local HTTP server, generated producer config, request parser/admission, settings API, dashboard DOM/CSP, and adversarial E2E.

## Acceptance Criteria
- [ ] Validate `Host`; reject browser `Origin` on ingestion; only minimal health is unauthenticated.
- [ ] Claude and Codex use distinct local producer credentials bound to claimed source; management/settings use a separate read credential.
- [ ] Same ID/same digest dedupes; same ID/different digest creates a collision/quarantine receipt.
- [ ] Compressed bytes, decoded bytes, compression ratio, JSON depth, resources/scopes/records/attributes, string/numeric ranges, per-source rate, and request time are bounded before ledger mutation.
- [ ] Unknown/high-cardinality floods remain within fixed CPU, memory, row, and receipt budgets.
- [ ] Labels, email, repo/model/tool/error categories render through `textContent`/DOM nodes; no untrusted `innerHTML` or inline handlers.
- [ ] Dashboard sends CSP, `frame-ancestors 'none'`, `nosniff`, no permissive CORS, and malicious-label fixtures remain inert.
- [ ] Company policy hard-disables evidence mode until a separate encrypted vault is implemented.
- [ ] Existing privacy/resource proofs remain green and add auth, compression-bomb, poisoning, collision, and XSS cases.

## Operational Boundaries
- Producer credentials are local Plimsoll credentials, never Claude/Codex account credentials.
- Raw request bodies and rejected secret/PII values never enter logs or receipts.
