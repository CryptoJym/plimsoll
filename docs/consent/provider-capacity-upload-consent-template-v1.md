# Provider Capacity Upload Consent Template — v1

Template for owner-granted consent before the FIRST
`provider_capacity_sync` upload of provider capacity snapshots from a device.
This template is bound to code: the "Exact fields disclosed" block below is
checked against `PROVIDER_CAPACITY_SYNC_ROW_FIELDS` in
`packages/shared/src/provider-capacity-snapshot.ts`, and the proof fails if
either side drifts.

## What is being consented to

Periodic upload of **provider quota capacity facts** — how much of each named
provider quota window was reported or locally observed as used, expressed in
basis points of that window — for operational capacity context only.

## Exact fields disclosed

Each uploaded row contains exactly these fields and nothing else:

```json
["adapterVersion", "capturedAt", "providerProfileId", "source", "usedBasisPoints", "window"]
```

No tenant identifier, device identifier, actor identifier, email, prompt,
command, transcript path, credential, raw provider body, billing figure, or
productivity field exists anywhere in the upload body. The measurement unit is
`device + provider profile + quota window + adapter version`; it is never a
person.

## Purpose

Operational capacity context only: showing the device owner whether a provider
quota window is filling up, when it resets, and how much unused headroom
remains. This data cannot independently prove quality, quantity, effort,
availability, diligence, productivity, or performance, and must never be used
on any surface that claims otherwise.

## Who can view

- The device owner (same views as the local product).
- The enrolled cloud workspace's administrators, under the enrollment terms in
  force at approval time.
- No public or benchmark surface receives this data unless a separate,
  explicit opt-in consent is granted.

## Retention

Rolling retention window: `<OWNER-SET: retention_days>` days from capture,
then deleted. `<OWNER-SET>` placeholders are deliberate: retention length is
an owner decision recorded here before first upload, not an implementation
default invented by code.

## Pausing

The owner may pause uploads at any time from the local management surface.
Pausing stops new uploads immediately; already-uploaded rows remain until the
retention window elapses or deletion is requested.

## Deletion

The owner may request deletion of all uploaded capacity rows for the device.
Deletion removes server-side rows; the local ledger remains under local
control. A deletion request receipt records what was requested and completed.

## No-performance-use rule

Capacity, depletion, unused headroom, event volume, and working hours are
operational context ONLY. They must never be used to evaluate, rank, coach,
discipline, compensate, or issue verdicts about any person. Any consumer that
feeds such a surface is outside this consent and outside the architecture
(ADR-0006).

## Consent binding (renewal triggers)

Before the first upload, the granted consent record binds BOTH:

- `sourceHead`: the exact git commit sha of the reviewed source; and
- `artifactDigest`: the sha256 digest of the exact reviewed artifact — the
  capacity protocol fingerprint (`plimsoll.provider-capacity.protocol.v1`),
  which covers the contract structure and every golden fixture byte.

A change to EITHER binding invalidates the consent; renewed approval is
required before any further upload. There is no grace period and no fallback.

## Renewal record

```json
{
  "approved": true,
  "approvedAt": "<ISO timestamp>",
  "approvedBy": "<owner identity>",
  "binding": {
    "artifactDigest": "sha256:<64 hex — current protocol fingerprint>",
    "protocolId": "plimsoll.provider-capacity.protocol.v1",
    "sourceHead": "<40-hex git commit sha>"
  },
  "consentKind": "plimsoll.provider-capacity-upload-consent",
  "scope": {
    "noPerformanceUse": true,
    "purpose": "operational_capacity_context_only",
    "surfaces": ["provider_capacity_sync"]
  },
  "version": 1
}
```
