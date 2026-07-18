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
   to the same flow fingerprint. Their raw output is hashed, never copied into
   the final receipt.

The content-free receipt is written to
`evidence/system-e2e-proof.json`. It records fixed row, wall-time, CPU, RSS,
block-I/O, and captured-output budgets. It also asserts that unchanged idle
cycles perform zero raw writes, file reads, and overlapping jobs, while warm
dashboard reads perform zero raw-ledger or filesystem scans.

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
pnpm proof:system-e2e
```

The proof fails closed if a cross-stage ID is dropped, tokens no longer
conserve, the poison row starves valid work, evidence becomes prescriptive, a
supporting proof loses a required assertion, a resource counter regresses, or
a fixed budget is exceeded.
