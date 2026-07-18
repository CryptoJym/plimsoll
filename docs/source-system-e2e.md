# Source-only system E2E gate

`pnpm proof:system-e2e` is the deterministic, source-only integration gate for
issue #105. It creates two temporary machine homes and two temporary SQLite
ledgers. It does not install or start a real LaunchAgent, read or write the
operator's ledger/config, use credentials, call a provider, publish a package,
deploy cloud code, or write an installed skill or memory.

The shared fixture is a real artifact chain rather than a list of independent
unit proofs:

1. Four captured event IDs total exactly 100 primary tokens across two
   project/repository and pull-request candidates.
2. The same ledger goes offline, reconnects, accepts each valid event once,
   and quarantines one validation-poisoned event without starving siblings.
3. Allocation consumes the captured event IDs, conserves all token classes,
   leaves 10 tokens explicitly unallocated, and preserves one unpriced event.
4. Pull 101 carries an immutable failed-check, corrected revision, review
   correction, merge, and in-window revert timeline.
5. The same session and event IDs produce a failed attempt, an explicit retry,
   a work episode, and a prospective treatment exposure. A second isolated
   machine supplies the matched control exposure.
6. Those facts compile into a deterministic, non-causal, non-prescriptive
   learning evidence packet. Re-running the exact source fingerprint performs
   zero statistical work and cannot publish or install a skill.
7. Existing adversarial installer/doctor, transactional join, metadata-only
   privacy, lifecycle rollback/uninstall/purge, and resource proofs are bound
   to the same flow fingerprint. A strict parser closes each actual child
   result, normalizes only enumerated volatile measurements, and includes its
   semantic artifact and digest in the final receipt. Raw stdout is excluded.
8. Seven synthetic skill, memory, and operator-live-shadow roots are populated,
   made write-denied, and content-digested before the flow. Their modes, entry
   counts, and after-digests must remain identical.

The content-free receipt is written to
`evidence/system-e2e-proof.json`. It records actual controller-plus-child CPU,
controller and child block I/O, maximum resident memory, wall time, captured
output, SQLite write/read work, supporting resource row work, and exact margins
against fixed budgets. It also asserts that unchanged idle
cycles perform zero raw writes, file reads, and overlapping jobs, while warm
dashboard reads perform zero raw-ledger or filesystem scans.

The direct-row budget is a fixed 500 operations. The deterministic fixture
currently proves 332 direct operations (including 306 actual SQLite changes),
leaving 168 operations of explicit headroom; it is not recalculated from the
observed run. A committed 501-operation adversarial case must fail. Wall, CPU,
resident-memory, block-I/O, total-row, and captured-output thresholds are also
fixed constants, and each has an over-budget negative case.

The standalone verifier does not trust the receipt's declared hashes. It
recomputes every stage, phase, shared-flow, evidence, and final deterministic
digest; re-derives outcomes and deterministic fact identities; recompiles the
learning packet; checks exact delivery/allocation sets; and validates the
resource totals and margins. Thirty-seven committed tamper cases include
re-signed semantic attacks, ensuring failures are not merely caused by stale
outer hashes.

## Honest external gates

Public source cannot prove hosted member identity, device revocation, or
credential rotation. It also cannot prove a real signed package install or a
real token-bearing session without mutating an authorized Mac and hosted
workspace. The receipt therefore records these as
`not_run_requires_hosted_authorization` or
`not_run_requires_owner_authorization`; it never simulates them as passing.

Run the source gate with Node 22:

```sh
pnpm install --frozen-lockfile
pnpm proof:system-e2e -- --receipt evidence/system-e2e-proof.json
pnpm proof:system-e2e:verify -- --receipt evidence/system-e2e-proof.json
pnpm proof:system-e2e -- --receipt evidence/system-e2e-proof-repeat.json \
  --compare-deterministic-receipt evidence/system-e2e-proof.json
pnpm proof:system-e2e:tamper -- --receipt evidence/system-e2e-proof-repeat.json
```

The proof fails closed if a cross-stage ID is dropped, tokens no longer
conserve, the poison row starves valid work, evidence becomes prescriptive, a
supporting proof loses a required assertion, a resource counter regresses, or
a fixed budget is exceeded.
