# 0054 — Fleet security: transactional enrollment, workspace isolation, and revocation

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/102

## TL;DR
- A join must never drain workspace-A history into workspace B or leave half-activated credentials after a failed handshake.
- Enroll one fresh device identity, prove it with one synthetic probe, then require separate explicit approval for history upload.
- Add workspace/device binding, rotation, leave/reassign/revoke semantics, and real version/policy reporting.

## Scope
Collector join/config/outbox identity and paired hosted device registry contract. No credential copying and no live rollout.

## Acceptance Criteria
- [ ] New credentials remain pending until a dedicated synthetic handshake succeeds; failure preserves the prior config byte-for-byte.
- [ ] Handshake sends exactly the probe, never arbitrary buffered history.
- [ ] Rejoin replaces the complete credential set; changing workspace requires explicit `--reassign` preview/confirmation.
- [ ] Ledger/outbox rows are workspace/device-bound; joining B sends zero A rows unless a separately authorized migration exists.
- [ ] HTTPS is required outside explicit loopback development; redirects and cross-audience upload URLs are rejected.
- [ ] Join secrets are accepted through masked prompt/stdin/file descriptor, never required in argv or logs.
- [ ] Stable device ID, key ID, actual app version, policy version, created/last-seen/last-upload, queue age, and status (`pending/active/suspended/revoked`) are represented.
- [ ] Rotation, leave, revoke, reinstall, and reassign fixtures are idempotent and cannot revive revoked credentials.
- [ ] Tenant-scoped actor pseudonyms converge inside one workspace and are unlinkable across workspaces.

## Operational Boundaries
- Credentials never move between machines.
- Server derives tenant/device authorization from authenticated credentials, not client assertions.
