# 0171 — Capacity release qualification: exact-artifact gates and canary receipts

## TL;DR
- The first signed release gets mechanical four-stage qualification — builder handoff → independent adversarial review → integration rerun → owner approval of ONE digest — where any code/config/dependency/schema/digest change resets approval.
- The canary sequence gets two receipt types: isolated-home read-only compatibility receipts (Studio0 + authorized MacBook; attestations must be literally true) and a Studio 3 preflight that stays `BLOCKED_MISSING_SSH_MAPPING` until an SSH mapping exists.
- Shipped as `packages/shared/src/release-qualification.ts` + hostile fixtures + `pnpm proof:release-qualification` (64 checks).
- Workflow instrumentation only — no merge, install, deploy, remote shell, credential, or machine authority is granted or representable.

## Scope
In: builder-handoff record (base SHA, head SHA, changed files, test receipt, fixture fingerprint, unresolved findings, artifact digest, optional config/dependency/schema fingerprints), attach-time binding guards, gate evaluation with per-change reset reasons, single-use owner rollout consumption, canary compatibility receipts (pseudonymous key hash, OS, provider versions, Plimsoll version, four mandatory attestations), Studio 3 preflight with derived blocked status, canary-vs-stage-gated scope gate, performance-evidence doctrine scanner.
Out: automatic fleet update (stays disabled), packaging/auto-update/stable-promotion execution (gated by #103 #105 #128 #131 #133 #135 #148 #154 #155 #158 #159 #162), creating the Studio 3 SSH account itself, cryptographic signatures with real keys (actor-id records, matching repo consent-record precedent), any network/fs/process side effects.

## Context
- Parent: #167. Sibling prior art: #176 lane receipts (`lane-receipts.ts`), #168 provider capacity snapshots (`provider-capacity-snapshot.ts`).
- The capacity doctrine gate (`scripts/capacity-dependency-reachability.ts`) constrains this module: it must not consume capacity symbols and must export no decision verbs (`ReviewVerdict` was renamed `ReviewDisposition`; its hash helper is local, not imported from `provider-capacity-snapshot.ts`).

## Problem / Task
Qualify ONE exact artifact for one rollout without any step being skippable, reusable across changes, or convertible into employee-performance evidence.

## Acceptance Criteria
- `pnpm proof:release-qualification` passes (64 checks), covering every bullet below.
- Handoff fails closed on malformed SHAs/digests, unknown fields, unbounded lists, test receipt without suite; digest is deterministic and order-insensitive; edits to handed-off facts change the digest.
- Review must be independent of the builder actor and bound to the same head+digest at attach time; reject verdict blocks with a distinct reason.
- Integration rerun must be at the exact head; a failed suite blocks.
- Owner approval binds only the handed-off digest and grants exactly one rollout; consumption is irreversible; second rollout refused.
- Drift resets: current head, artifact digest, config/dependency/schema fingerprints each produce a distinct reset reason; pinned-but-unreported and unpinned-but-present fingerprints both fail closed.
- Compatibility receipts require all four attestations literally true, pseudonymous 64-hex machine key hash, closed vocabulary; credential-shaped values and home paths refused whole and redacted.
- Studio 3 preflight derives `BLOCKED_MISSING_SSH_MAPPING` from `sshMapping:"missing"` and refuses progression; usernames are unrepresentable.
- Only the two canary scopes are open; packaging/automatic_update/stable_fleet_promotion claims return the full stage-gate issue set.
- Performance-evidence fields (productivity/rating/verdict/ranking/coach/discipline/compensation/intervention concepts) are refused by closed schemas and flagged by `findPerformanceEvidenceViolations` in foreign records.

## Operational Boundaries
- `pnpm proof`, `pnpm proof:capacity`, `proof:capacity-adapters`, `proof:capacity-contract`, `proof:fleet-operations`, `proof:lane-receipts` stay green; `tsc --noEmit` clean.
- All functions pure data-in/data-out; no fs writes, processes, network, or credentials.
- Records never carry home paths, hostnames, usernames, prompts, or raw machine keys.

## Notes For Future Agents
- Library: `packages/shared/src/release-qualification.ts`. Proof: `scripts/release-qualification-proof.ts`. Fixtures: `scripts/fixtures/release-qualification/{valid,hostile}/`.
- Mutation-tested: removing the reviewer-independence guard or the fingerprint-drift push makes the proof fail naming the exact check.
- Do not import from `provider-capacity-snapshot.ts` here — the doctrine gate flags capacity-symbol consumption outside capacity modules, including identifier-name collisions like a local `sha256Linkage`.
- The per-stage mapping of gating issues (#103…#162) is deliberately NOT invented; `evaluateScopeClaim` returns the whole set.

## Open Questions
- Should owner approval carry a real signature envelope (e.g., detached sig over the chain digest) once #103/#105 packaging lands?
- Should compatibility receipts gain an adapter-version matrix keyed to the #168 protocol fingerprint?
