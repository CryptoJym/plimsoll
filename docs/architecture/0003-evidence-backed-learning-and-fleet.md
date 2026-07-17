# ADR-0003: Evidence-backed learning and fleet architecture

## Status
Proposed — implementation slices may proceed; live rollout and skill publication remain separately gated.

## Context
Plimsoll captures useful AI work economics, but current outcome and project joins can duplicate or misclassify work, correction history is not immutable, technique exposure is absent, and dormant recommendation schemas have no evidence lifecycle. The public package/install path also trails accepted source and lacks a safe company lifecycle.

The system must learn from the full dataset without reintroducing continuous background load, leaking raw work content, turning correlation into doctrine, or moving credentials between computers.

## Requirements

### Functional
- Allocate token/cost vectors to projects and work units without duplication.
- Preserve commit/check/review/rework timelines and derive correction metrics.
- Record explicit technique/skill exposure and compare like cohorts.
- Produce reviewable candidate evidence and measure subsequent versions.
- Enroll, identify, update, roll back, revoke, and uninstall company devices.

### Non-functional
- Deterministic/versioned metrics; coverage and unknowns first-class.
- Metadata-only default; no raw content in analytical or skill evidence.
- Incremental/bounded work; dashboard request path stays constant-work.
- Per-device/per-workspace credentials; no credential transfer.
- Reproducible releases, atomic rollback, and explicit owner gates.

## Decision

```text
AI tools
  -> authenticated bounded local capture
  -> SQLite raw evidence + incremental facts + sealed outbox
                         |
                         +-> local descriptive views
                         |
                         +-> privacy-safe work episodes / exposures / outcomes
                                      |
                                      v
                           hosted immutable fact store
                                      |
                             versioned attribution run
                                      |
                       metric facts + coverage + uncertainty
                                      |
                         associative candidate analysis
                                      |
                       reviewable evidence packet only
                                      |
                  doctrine hub PR + independent verification
                                      |
                              owner approval
                                      |
                        versioned canary / rollback
```

Use a layered modular architecture rather than another always-on service:

1. **Local evidence layer:** privacy-safe event/tool/episode/exposure facts and exact allocation edges.
2. **Outcome lineage layer:** immutable GitHub revisions/checks/reviews/rework with completeness receipts.
3. **Metric mart:** deterministic formula registry and versioned facts; no LLM calculations.
4. **Learning layer:** bounded offline cohort/association analysis over aggregate facts.
5. **Doctrine layer:** human-reviewed skill source, tests, signatures, rollout, monitoring, and rollback.
6. **Fleet layer:** pinned packaged runtime, per-device identity, declarative policy, registry, health, rotation/revocation, and lifecycle receipts.

Local/open remains descriptive for one operator's data. Hosted/paid owns organization comparisons, interventions, candidate discovery, fleet administration, and benchmarks. The doctrine hub owns executable durable skills.

## Metric principles
- Token vectors remain input/output/cache-read/cache-write; dollars never weight token rows.
- `allocated + unallocated = captured`; event weights never exceed 1.
- Validation requires known required checks passing.
- Stability requires a matured observation window.
- Correction needs an observed fail/change-request -> newer revision -> pass/approval sequence.
- Technique results are associations unless randomized/pre-registered evidence supports causality.
- Every value carries formula version, source snapshot, window/as-of, coverage, sample, method, and claim class.

## Skill promotion gate
`observed -> candidate -> reviewed playbook -> approved pilot -> evaluated -> skill proposal -> independent verification -> owner approval -> canary -> active -> stale/deprecated/rollback`.

Models may summarize aggregate evidence and propose drafts. They cannot publish, edit installed skill trees, overwrite evidence, or promote open-web content into doctrine.

## Fleet decision
One versioned runtime per Mac with an atomic `current` pointer; one LaunchAgent; one local SQLite ledger; one per-device key; one hosted device record. Enrollment stages credentials, sends only a synthetic probe, and activates atomically. History transfer, workspace reassignment, live rollout, and package publish are explicit operations.

## Consequences

### Positive
- Desired learning metrics become reproducible and auditable.
- Bad attribution, missing history, and small/confounded samples cannot silently become recommendations.
- Background load stays bounded and no-op work can be fingerprint-skipped.
- Team devices can be installed and governed without credential copying or mutable checkouts.

### Negative
- Historical telemetry cannot retroactively reveal techniques; explicit exposures start prospectively.
- Correct attribution leaves more work honestly unallocated until lineage improves.
- Company rollout is blocked until enrollment, local-surface, package, and rollback gates pass.

### Neutral
- Existing local reports remain useful but some current metrics become labeled legacy/provisional.
- Hosted draft D3/D4 work needs formula and provenance corrections before ranking or skill evidence use.

## Alternatives Considered
- **Infer techniques from prompts/commands:** rejected; violates metadata privacy and produces weak semantic labels.
- **Autonomous skill writer:** rejected; correlation and tainted inputs can institutionalize harmful patterns.
- **Continuous background LLM analyst:** rejected; recreates CPU/cost churn and non-reproducible metrics.
- **Dominant-session allocation:** rejected for learning metrics; can duplicate/misplace long-lived sessions.
- **Remote-management agent with arbitrary shell:** rejected; fleet needs declarative policy, not remote execution.

## Failure Modes and Mitigations
- Missing GitHub pages/check policy -> `UNKNOWN`, metric not estimable.
- Multi-project/session ambiguity -> explicit unallocated bucket.
- Skill/model/time confounding -> stratification, clustering, version markers, held-out pilot.
- Metric gaming -> balanced measures, no composite score, immutable receipts.
- Workspace reassignment -> pending credentials, synthetic-only handshake, explicit history authorization.
- Bad update -> staged runtime and automatic rollback.
- Poison/local attack -> authenticated producers, bounded parsing/cardinality, quarantine, CSP/text rendering.

## References
- Parent sounding 0048 and children 0049–0057.
- ADR-0001 resource-bounded local collector.
- `CryptoJym/plimsoll-cloud#24` and draft PR #27.
