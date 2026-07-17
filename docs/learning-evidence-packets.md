# Local learning evidence packets

`packages/shared/src/learning-evidence.ts` is Plimsoll's versioned, local/open
contract for turning an operator's own deterministic facts into a reviewable
association packet. It is an offline compiler, not a background analyst. It
does not fetch data, call an LLM, rank people, recommend techniques, or create,
install, publish, or edit a skill.

## Truth boundary

An admissible input contains explicit, prospective exposure and control
receipts plus matched outcome pairs. Each pair must match exactly on project,
work type, complexity band, model, tool version, actor cluster, repo cluster,
and calendar epoch. Outcome metric/version/unit/direction must also match.

The runtime rejects:

- missing, implicit, inferred, or retrospective technique exposure;
- open-web or model-generated content as gating evidence;
- incomparable cohort, epoch, model, tool, metric, or unit identities;
- stale row digests, non-finite/unsafe numbers, row/runtime budget overflow;
- winner-only hypothesis selection and uncorrected multi-hypothesis families.

Valid but incomplete, underpowered, privacy-suppressed, low-coverage, or
Simpson-reversing inputs produce an explicit `not_estimable` packet. Null
outcomes, unallocated work, and unknown allocation are counted; none becomes
zero or silently disappears.

Every estimable result is labeled `observational_association` with
`causalClaim: false` and `prescriptiveClaim: false`. The effect is a paired
mean difference with a family-wise confidence interval. The packet also names
the source snapshot/query/row hashes, metric versions, window/as-of, exposed
and control counts, pair/actor-cluster/repo-cluster statistical gates,
independent privacy gate, attribution coverage/mix, conservative clustered
uncertainty, confounders, and bounded counterexamples.

One explicit `outcomeContract` (metric id, formula version, unit, and effect
direction) governs the entire run. A pair that is internally matched still
fails closed if it drifts from that run-level contract; ratios and dollars,
or higher-is-better and lower-is-better outcomes, can never be averaged.

## Versioned schema surface

The public TypeScript/runtime schema exports are:

- `LEARNING_EVIDENCE_SCHEMA_VERSION` and `LEARNING_ANALYSIS_VERSION`;
- `LearningEvidenceManifest`, `LearningOutcomePair`, and
  `LearningEvidencePacket`;
- `validateLearningEvidenceManifest`;
- `computeLearningPairDigest` and `computeLearningSourceFingerprint`;
- `compileLearningEvidencePacket`.

`source.rowDigest` binds canonical sorted pair rows. The smaller source
fingerprint binds that digest, the snapshot/query identity, metric versions,
hypothesis family, gates, window, and analysis configuration. Passing the same
fingerprint as `previousSourceFingerprint` returns:

```json
{
  "status": "unchanged",
  "analysisWorkUnits": 0,
  "packet": null
}
```

The compiler always recomputes and validates the canonical row digest before
accepting a no-op. A stale digest, changed outcome, forged identity, or future
exposure therefore fails closed even if the caller supplies the old source
fingerprint. `analysisWorkUnits: 0` means zero cohort/effect/inference work,
not zero bounded input validation. This is the scheduled-job contract: verify
one bounded source, skip statistical work when unchanged, and never run a
continuous LLM loop.

## Statistical and privacy minimums

`statisticalMinCompletePairs`, `statisticalMinActorClusters`, and
`statisticalMinRepoClusters` are independent analysis-quality floors.
`privacyMinCompletePairs` is an independent disclosure floor. They are never
collapsed. A cohort can satisfy one and fail the other, and the packet records
the exact reason. Effect uncertainty uses the maximum of ordinary paired,
actor-cluster-robust, and repo-cluster-robust standard errors, with the declared
multiple-hypothesis correction. Eleven pairs from only two actor clusters are
therefore `not_estimable`, not eleven independent observations. Local operators
may choose a privacy threshold appropriate for their own-data artifact, while
hosted organization comparisons must apply organization policy separately.

## CLI

From a checked-out workspace with Node 22:

```sh
pnpm learning:evidence -- \
  --input evidence/learning-manifest.json \
  --out evidence/learning-packet.json
```

On a later run, the CLI automatically reads the existing output fingerprint
or accepts `--previous <packet.json>`. An unchanged source does not rewrite the
output file. Output must be a JSON file inside the current workspace. URL,
workspace-escape, `SKILL.md`, `MEMORY.md`, installed skill tree, and global
memory targets fail closed. The CLI has no network, device-management,
publication, or skill-writing path.

Run the adversarial fixtures with:

```sh
pnpm proof:learning-evidence
```

## Review-only skill lifecycle

Every packet contains a fixed `skillCandidateReview` artifact. Its initial
state is `observed`; its inventory disposition is
`insufficient_evidence`; it contains no executable instructions and grants no
publication or installation authority. The declared lifecycle is:

`observed -> candidate -> reviewed_playbook -> owner_approved_pilot -> evaluated -> skill_proposal -> owner_approved_publication -> monitored -> stale/deprecated/rolled_back`

Inventory review may later choose `new_skill`, `enhance_existing`,
`compose_existing`, `duplicate`, `conflict`, `quarantine`, or
`insufficient_evidence`. Those are human review dispositions, not autonomous
analysis outputs. Promotion still requires prospective validation,
independent verification, doctrine/privacy/security review, and owner
approval in the doctrine repository.

## Deliberately outside this slice

- organization comparisons, benchmarks, prescriptions, and hosted learning
  marts;
- durable skill proposal content, doctrine PR creation, pilot execution,
  canary rollout, monitoring, expiry, rollback, or publication;
- raw prompt/model/open-web content and historical inference of technique
  exposure;
- writes to `~/.codex/skills`, `~/.claude/skills`, global memory, installed
  team machines, or any live service.
