# 0045 — Resource-budget E2E and runtime architecture ADR

## TL;DR

- Establish the accepted architecture for a resource-bounded local collector and a machine-readable integration gate.
- Use deterministic work counters as the primary proof; wall-clock observations are secondary.
- The scaffold runs only against temporary resources and reports sibling-dependent integration scenarios as `not_wired`, not passed.
- Parent: [#75](https://github.com/CryptoJym/plimsoll/issues/75). Tracker: [#81](https://github.com/CryptoJym/plimsoll/issues/81).

## Scope

Architecture documents, synthetic resource-proof scaffolding, a sounding, and verification receipts. This lane does not edit collector production code, package scripts, CI, the live ledger, the installed LaunchAgent, or hosted services.

## Context

Baseline: `origin/main@9fc0af4cb59b01245f7a1862ba1647a152c8b537` on 2026-07-15.

The target flow is:

```text
sources -> privacy + value admission -> short-retention raw evidence
                                      -> incremental projections -> dashboard/status
                                      -> durable outbox -> retry/ack
                                                           -> dead letter
```

The deployment remains a single local process and SQLite database with logical failure boundaries. ADR-0001 documents why microservices, a second database, and an external event stream are not the 80/20 answer.

## Problem / Task

Resource behavior is currently observed ad hoc. Build an executable proof contract that can integrate the #76–#80 lanes without declaring unwired behavior green. The proof must use a temporary `HOME`, `PLIMSOLL_HOME`, database, session roots, and loopback port, then emit a compact JSON receipt.

## Evidence

2026-07-15 read-only survey:

```text
ledger bytes              6.8G
buffered events           4,810,030
generic Codex OTLP spans  4,130,123 (~3,296.4 MiB payload)
unuploaded events         4,007,788
dashboard bundle          13.15s wall / 12.26 CPU seconds / 256MB RSS
port conflict failures    16,079
sync failures             624 (max streak 107)
```

Baseline proof dependency:

```text
pnpm proof was red on origin/main@9fc0af4 because fixed May fixtures
aged out of 30-day windows. #82 delivered deterministic time on
origin/main@196d35f; the rebased lane verifies the full proof green.
```

## Acceptance Criteria

- [x] Accepted-style ADR documents requirements, target flow, failure modes, security/privacy, alternatives, and 80/20 migration order.
- [x] NFR budget matrix defines deterministic counters plus secondary latency observations.
- [x] Harness creates only temporary local resources and emits a schema-versioned JSON receipt.
- [x] Current architecture/isolation/empty-ledger checks run; optional existing proof execution is captured without credentials or provider calls.
- [x] Integration-only scenarios are represented as required `not_wired` checks until sibling lanes provide seams.
- [ ] E2E covers capture -> durable evidence -> projections -> dashboard -> outbox on integrated #76–#80 heads.
- [ ] No-change asserts zero full-file rereads, raw-event rewrites, and overlapping maintenance jobs.
- [ ] Duplicate-start, poison-continuation, and dashboard p95/counter scenarios pass.
- [ ] CI runs the resource proof and emits a compact receipt.
- [x] Existing `pnpm proof` is green after #82; CLI build remains green.

## Operational Boundaries

No real home, live database, installed service, provider network, credential, or raw-content use. The scaffolding is not a mandatory CI gate until every required integration scenario is wired and green.

## Notes For Future Agents

- Run scaffold: `pnpm exec tsx scripts/resource-proof/index.ts --receipt /tmp/plimsoll-resource-proof.json`.
- Run optional baseline proof inside the receipt: add `--run-existing-proof`.
- Enforce integration readiness: add `--require-integrated`; expected to fail while required scenarios are `not_wired`.
- Counter names and meanings are a versioned contract. Additive fields are safe within v1; semantic changes require v2.
- Do not make `not_wired` exit green under `--require-integrated`.
- #82 delivered fixture-clock repair; this lane does not edit the monolithic proof.

## Open Questions

- Which sibling lane should own the final small composition module that binds all production counter providers: #81 or the last merging sibling?
- What byte budget defaults should ship after #79 measures a representative post-admission ledger?
