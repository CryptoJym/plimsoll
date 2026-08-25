# ADR-0006: Capacity is a separate fact/projection lane, never an interaction event or performance metric

## Status

Proposed for issue #168 (child of #167). Freezes the canonical Capacity Rail
contract — one local-rich shape, one cloud-sanitized derivative — BEFORE any
adapter or cloud integration work can diverge.

## Context

Issues #173 and #167 established capacity planning intelligence and a
provider-report ingestion lane as pure, descriptive local modules. Two
integration pressures now threaten that boundary:

1. Adapter and cloud work could be tempted to express capacity facts as
   interaction events (the existing `AiWorkSession`/`AiInteractionEvent`
   lanes) or as fields on performance surfaces, collapsing a separate fact
   into someone else's denominator.
2. A cloud sync contract could drift toward carrying tenant, device, or actor
   assertions in its body, or toward storing derived values (headroom) whose
   staleness semantics silently decay into fabricated zeros.

The scientific unit of measurement must stay fixed:
`device + provider profile + quota window + adapter version` — never a person.
Capacity, depletion, unused headroom, event volume, and working hours are
operational context only; they cannot independently prove quality, quantity,
effort, availability, diligence, productivity, or performance.

## Decision

### One contract, two projections

- `ProviderCapacitySnapshotV1` (`plimsoll.provider-capacity-snapshot.v1`) is
  the canonical LOCAL-RICH fact: unit identity (device install id, provider
  profile id, generic window label, adapter version) plus one observation.
- `provider_capacity_sync` (`plimsoll.provider-capacity-sync.v1`) is the only
  sanctioned CLOUD-SANITIZED derivative. Its body asserts no tenant, no
  device, and no actor; identity rides authenticated transport metadata via
  the repository-wide batch envelope (`kind`/`tenantId`/`installKey`/
  `appVersion`), exactly like every sibling ingest batch. Rows are produced
  only by `projectSnapshotsToSyncRows`, which picks an explicit field
  allowlist by construction.

### Storage and derivation

Only `usedBasisPoints` (integer 0–10000, window normalized to 10000 bp) is
stored. Remaining headroom is DERIVED at read time by
`deriveRemainingBasisPoints`. No `remaining*` field is representable in any
schema. Missing quota windows remain absent: they are never defaulted,
backfilled, or emitted as zero rows. Stale, future-dated, or absent evidence
yields null — UNKNOWN, never zero — while a FRESH observation of full
depletion legitimately derives zero. Unknown/stale never become zero; fresh
zeros are earned by evidence.

### Provenance and freshness

Every observation carries its source (`provider_report` | `local_telemetry`),
adapter version, capture time, and a freshness classification with fail-closed
semantics (future-dated captures are UNKNOWN per the issue #195 doctrine).
Windows are generic bounded identifiers; nothing provider-specific is
hard-coded.

### Unrepresentability

Prompts, commands, transcript paths, emails, credentials, raw provider
bodies, billing details, and productivity fields are NOT REPRESENTABLE.
Three independent layers enforce this: closed strict schemas (unknown keys
refused outright), bounded identifier charsets for every string slot (no
whitespace, path separators, or `@`; secret-shaped prefixes refused by name),
and a value-shape privacy seal over the whole sanitized body before upload.

### Consent gate

Before the FIRST upload, owner consent must bind BOTH the exact reviewed git
source head AND the exact artifact digest (the capacity protocol fingerprint,
which covers contract structure and every golden fixture byte). A change to
EITHER invalidates consent and requires renewed approval. The template lives
at `docs/consent/provider-capacity-upload-consent-template-v1.md` and is
machine-checked against the code's exact field list.

### Drift-proof fixtures and cross-repo compatibility

Golden valid/invalid fixtures live under
`packages/shared/fixtures/capacity/provider-capacity-snapshot/`. The protocol
fingerprint hashes the contract structure TOGETHER WITH every fixture file's
digest, so any edit to either flips the fingerprint and stales the committed
`protocol-receipt.json` until it is regenerated deliberately.
Cross-repository compatibility means EXACT fingerprint match — anything else
is incompatible and fails closed.

This decision extends ADR-0005's gated-cloud posture and the #173/#167
doctrine; the static dependency reachability proof continues to block any
capacity symbol from reaching routing, coaching, ranking, compensation,
discipline, intervention, or verdict surfaces.

## Consequences

### Positive

- Adapter and cloud integration inherit one frozen, versioned contract instead
  of inventing shapes per consumer.
- Privacy properties are structural, not procedural: they survive refactors
  because unrepresentable data cannot parse, let alone upload.
- Fixture drift is detectable mechanically rather than by review vigilance.

### Negative

- New capacity facts (a new source kind, a new envelope need) require a v2
  contract and receipt regeneration rather than an ad-hoc field.
- The consent binding makes even doc-only fixture edits require renewed
  approval before uploads resume. This is deliberate friction.

### Neutral

- The cloud side of `provider_capacity_sync` is not implemented here; this ADR
  freezes only what the collector may send and how consent gates it.

## Rejected alternatives

- **Reuse of interaction-event lanes for capacity:** rejected — different
  unit, different retention, different privacy posture; joining them would let
  operational context masquerade as performance evidence.
- **Storing derived headroom alongside usage:** rejected — stored derivatives
  rot silently; derivation at read time keeps staleness honest.
- **Tenant/device assertions inside sync rows:** rejected — identity belongs
  to authenticated transport metadata, where it is verified, not asserted.
- **Consent bound to a product version string alone:** rejected — versions are
  not artifacts; exact head + exact digest is checkable, versions are not.

## Proof required before status changes

- `pnpm proof:capacity-contract` green (this ADR's mechanical checks).
- Existing `pnpm proof:capacity` stays green (no doctrine regression).
- Cross-repository compatibility receipts compared on both sides when the
  cloud implementation lands; exact match or no integration.
