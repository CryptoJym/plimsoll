# ADR-0005: One-click local product and gated optional cloud

## Status

Proposed source architecture for issue #164. James remains the owner and sole
approver of the open product, merchant, and launch decisions. Implementation,
package publish, provider configuration, legal approval, and paid launch remain
separate gates.

## Context

Plimsoll needs a trustworthy path for two different jobs:

1. a private local product that reaches a useful number with minimal setup; and
2. an optional hosted product for teams that deliberately enroll devices.

The local collector requires host access to supported AI-tool telemetry. The
hosted service receives only the explicitly allowed managed-data contract. A
deployment orchestrator does not remove the need for Plimsoll to own producer
authentication, privacy, package provenance, lifecycle, enrollment, tenant
isolation, retention, billing integrity, or rollback.

This decision incorporates a read-only Fable 5 architecture review and an
independent source reconciliation. The adviser is not the approver; only
recommendations that survived source and threat-model review are adopted.

## Decision

### Product boundary

Plimsoll remains two modular monoliths with a narrow versioned protocol:

```text
Native macOS Plimsoll
  - local authenticated capture
  - owner-only SQLite ledger
  - authenticated local management surface
  - offline-first reports
  - optional explicit enrollment
               |
               | minimized, versioned, future-only sync
               v
Optional Plimsoll Cloud
  - per-install machine trust
  - tenant-isolated ingest and storage
  - human organization authentication
  - one simple hosted entitlement
```

The local product is useful without a cloud account and must not be disabled by
billing or cloud availability. Cloud enrollment is an additive user decision,
not activation of the local product. A one-click local install can finish
successfully with the literal state `not_enrolled`; hosted join is a separate
action. Issue #103's canonical install contract must preserve this optional
boundary rather than requiring enrollment for local readiness.

### Local distribution and lifecycle

Plimsoll owns its package, install, health, update, rollback, backup, restore,
uninstall, and purge contracts.

- The near-term bootstrap may be one exact-version command only after the
  registry artifact, checksum/provenance, cold install, and runtime version are
  proven to be the same immutable release.
- The launch target is a signed and notarized macOS package that installs an
  immutable runtime and a least-privilege LaunchAgent without a source checkout.
- Update is staged and health-gated. A failed update restores the prior binary,
  configuration, database-compatible state, and service.
- Uninstall preserves the ledger by default. Destructive purge is a separate,
  exact-confirmation operation that fences the service and removes SQLite
  sidecars, snapshots, owned configuration, and service state truthfully.
- Backup/restore and doctor must be executable proof, not documentation claims.

Current implementation work is tracked by #103, #155, #158, #159, and #162.
Producer and management authentication remains tracked by #108. The accepted
resource, privacy, retention, and durability gates in ADR-0001 and issues #104,
#105, and #117 remain release prerequisites; this ADR does not supersede them.

### Enrollment and data history

Enrollment is future-only by default.

- A trusted browser flow redeems a short-lived, single-use join token through a
  masked prompt or standard input, never a required command-line argument.
- Pending credentials perform exactly one synthetic handshake before activation.
- Pre-enrollment events and outbox rows remain local and quarantined. A join or
  same-workspace rejoin cannot relabel or release them.
- History transfer, if retained, is a separate previewed and confirmed operation
  with its own authorization and receipt.
- Workspace reassignment, rotation, leave, reinstall, suspension, and revocation
  are explicit state transitions. Credentials never move between machines.

Issue #163 is the first future-only enforcement slice; #102 owns the broader
device identity and lifecycle contract.

### Optional paid cloud

The recommended first hosted offer has one flat entitlement, provisionally
named `team_cloud`. This is an unresolved owner/product decision, not a claim
about current source: the cloud repository presently implements Team and
Business seat tiers plus Enterprise contact-sales behavior. Reconciliation of
that schema, route, UI, and migration surface must be separately issue-sized
before any plan is removed or renamed. Usage metering, a larger tier matrix,
overages, evidence storage, and automatic plan optimization remain deferred.

Billing is disabled by default and may run only in a fake or provider test mode
until all paid-launch gates in this ADR pass. A billing failure may suspend cloud
features according to an owner-approved policy; it cannot disable or erase the
local product.

Cloud launch requires, at minimum:

- per-install credentials, rotation, revocation, and replay protection;
- immutable, fail-closed install-to-tenant binding;
- database-enforced tenant isolation or an equivalently adversarially proven
  boundary for every machine and human path;
- server-derived evidence/data scopes rather than caller assertions;
- explicit minimization, retention, erasure, and deletion receipts;
- protocol version negotiation and compatibility proofs;
- production human authentication;
- durable Stripe event idempotency and ordering, authoritative subscription and
  payment retrieval, and exact customer/organization/tenant/product/price
  binding;
- approved privacy, terms, security, support, cancellation, refund, and billing
  policy surfaces; and
- an explicit owner-signed go-live receipt before checkout is enabled.

The first fail-closed tenant-binding slice is tracked in
`CryptoJym/plimsoll-cloud#36`. Fleet registry work in cloud issue #29 does not
replace that fix. Cloud issue #38 establishes the default-off billing launch
gate; it does not decide or migrate the product plan.
Cloud issue #39 owns that unresolved plan decision and its eventual migration
contract.

### Business identity and provider custody

Plimsoll must not be described as a subsidiary unless a separately formed legal
entity is evidenced and approved. If the owner later chooses Utlyze as merchant
of record, a safer factual description is "Plimsoll is a product of Utlyze",
but even that customer-facing wording requires owner/legal/provider approval and
consistent merchant disclosure, receipts, terms, support identity, and payment
statement descriptors.

Runtime secrets stay in provider-native secret stores. Human administrator
logins may be held in the approved password manager, but Stripe keys, webhook
secrets, deployment tokens, cookies, or machine credentials are never copied
between machines, profiles, or implementation lanes.

No custom domain is selected or owned for this architecture. As of this
decision, the only verified public address is
`https://plimsoll-cloud.vercel.app`. Custom-domain purchase, DNS, and production
authentication-domain work are not part of implementation.

## Dependency and execution order

```text
S0 exact main/PR/provenance truth
  +-- Local prerequisites
  |     L1 producer + management authentication (#108)
  |     L2 future-only transactional enrollment (#163 -> #102)
  |     L3 resource/privacy/retention (#104, #117; ADR-0001)
  |       -> L4 immutable package + truthful lifecycle (#103 and children)
  |       -> L5 independent fresh-Mac local launch proof
  +-- Cloud prerequisites
        C1 immutable install/tenant binding (cloud #36)
          -> C2 per-install rotation/revocation/replay protection
          -> C3 DB tenant isolation + server-derived data scopes
          -> C4 retention/erasure + protocol compatibility

L1 + L2 + L4 + C1 + C2 + C3 + C4
  -> X1 packaged durability + controlled two-machine cross-repo gate (#105)

C4 + X1
  -> C5 default-off billing gate (cloud #38)
  -> C6 plan reconciliation + Stripe integrity in test mode
  -> G1 production auth + policies + merchant approval
  -> G2 explicit paid go-live
```

Security and privacy slices stay separate from legal copy, provider settings,
and billing enablement. Existing dirty or stacked pull requests are rebased and
adversarially re-reviewed on their new exact heads; green historical checks are
not merge proof.

## Launch-ready definitions

### Local

Local is launch-ready only when a fresh supported Mac can install an immutable,
verified artifact and reach a real first number within five minutes; it operates
offline; identity reads are explicit and honestly support `UNKNOWN`; producer
and management boundaries are authenticated; doctor is read-only and truthful;
resource ceilings, raw-retention behavior, SQLite durability, metadata privacy,
and evidence quarantine pass their accepted gates; and backup, restore, update,
failed-health rollback, uninstall, and purge pass failure-injection tests without
moving credentials.

### Paid cloud

Paid cloud is launch-ready only when machine trust, tenant isolation, protocol
compatibility, minimization, retention/erasure, billing integrity, production
human authentication, policy surfaces, support operations, merchant identity,
provider-native secret custody, and an explicit owner go-live approval all have
current proof. Source-complete, preview-deployed, and test-mode states are not
paid-launch-ready.

## Rejected and deferred alternatives

- **Openship as package/lifecycle/cloud owner:** rejected. It would be a
  load-bearing control plane without resolving Plimsoll's trust boundaries.
- **Optional future Openship/Compose target:** deferred. It may later be tested
  only as a replaceable customer-selected target for a standard artifact.
- **Electron or a menu-bar application for the first release:** deferred; it
  adds surface area without proving collection, privacy, or lifecycle.
- **Remote arbitrary-shell fleet agent:** rejected; desired state is
  declarative and bounded.
- **Automatic history upload on join:** rejected.
- **Multiple plans, metering, overages, or multi-region infrastructure:**
  deferred until one flat hosted entitlement is safe and useful.
- **Live billing before legal/provider gates:** rejected.

## Unresolved owner and implementation decisions

- James has not yet approved this ADR as the product decision of record.
- Whether Utlyze will be merchant of record, and the exact customer-facing
  product/merchant wording, remain owner/legal/provider decisions.
- The recommended one-flat-entitlement launch shape conflicts with the current
  Team/Business/Enterprise implementation and needs a dedicated reconciliation
  issue before source changes.
- Production Clerk needs a domain and provider configuration that have not been
  selected or authorized.
- The exact per-install credential rotation/replay construction and the exact
  database-enforced tenant-isolation mechanism remain implementation decisions
  subject to adversarial review.
- Current local retention does not yet prove that local-only raw rows expire;
  uploaded-only pruning cannot satisfy the launch gate.

## Consequences

### Positive

- Local value, privacy, and portability do not depend on a hosted vendor or
  billing state.
- The shortest user path is also the product's tested lifecycle path.
- Paid launch has an explicit fail-closed gate instead of an environment-variable
  accident.
- Each P0 can land and be refuted independently.

### Negative

- A signed one-click package takes longer than documenting an `npx` command.
- Paid launch waits for security, provider, merchant, and policy proof.
- Existing unbound history cannot be made visible in cloud without a separate
  migration decision.

### Neutral

- The Vercel deployment may remain available for test/hardening work, but its
  existence is not evidence of production authentication or paid readiness.
- No legal entity, custom domain, Stripe object, or production provider state is
  created by this decision.

## Proof required before status changes

- Exact source commit, artifact digest/provenance, clean install, rollback, and
  uninstall receipts for local release.
- Cross-token, cross-install, replay, null-tenant, and cross-tenant adversarial
  fixtures before cloud security claims.
- Duplicate/out-of-order/mismatched-customer and stale-event billing fixtures
  before entitlement claims.
- Current authenticated provider readback plus owner/legal approval before any
  production-auth, merchant, checkout, DNS, or public-policy claim.
