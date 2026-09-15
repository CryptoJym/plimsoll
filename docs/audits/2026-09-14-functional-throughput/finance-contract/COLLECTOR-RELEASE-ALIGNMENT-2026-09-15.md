# Collector release alignment readback — 2026-09-15

## Decision

The hosted enrollment command is being aligned from `plimsoll-cli` `v0.7.5` to the qualified `v0.7.30` release. This is a bounded cloud-surface fix; it does not qualify a Finance monetary source and does not change credentials, database schema, collector runtime code, or deployment state.

## Evidence

- Native authenticated Connections readback showed the setup command still pinned `plimsoll-cli-0.7.5.tgz` while enrolled machines reported `v0.7.30`.
- GitHub release `v0.7.30` is published and contains `plimsoll-cli-0.7.30.tgz` with asset digest `sha256:ab1713dc30d7f0b5543a19d26b26847b33cace5940d266a27a743bc18f5fa3c9`.
- The release target commit is `8101f32b` and the release acceptance notes report `v0.7.30` with `refused=0`.
- The v0.7.30 collector source supports the existing hosted command flags: `join --token-prompt --url`.
- No invite token is embedded in the command; the existing origin validation and shell quoting proof remains unchanged.

## Change receipt

- Repository: `CryptoJym/plimsoll-cloud`
- Follow-up PR: https://github.com/CryptoJym/plimsoll-cloud/pull/121
- Branch: `codex/plimsoll-handoff-collector-20260915`
- Commit: `6f38b4f2b5c085e3abd7257792750d742001b719`
- Base: PR120 UX commit `83a371b86cf9c7211ccd0abb733f84349f0e85c8`
- Changed files: `src/lib/collector-release.ts`, `scripts/collector-release-proof.ts`
- PR121 merge commit: `c712b4c411fdfe8e723bf0df85a0b59f72b58dc6`

## Checks

Local checks passed:

- `pnpm exec tsx scripts/collector-release-proof.ts`
- `pnpm typecheck`
- `pnpm lint`
- `pnpm test` — 214 passed, 14 skipped, 0 failed
- `pnpm build`

PR121 Vercel preview is deployed successfully. GitHub `build-and-proof` and `install-tenant-race-proof` both passed, and PR121 is merged into the PR120 UX branch at `c712b4c411fdfe8e723bf0df85a0b59f72b58dc6`. The integrated PR120 head is clean with `build-and-proof`, `install-tenant-race-proof`, and Vercel all passing. Preview: https://plimsoll-cloud-git-codex-plimsoll-ux-ui-slice-utlyze-2f74afdb.vercel.app. Production release remains open; no production deployment or Finance source mutation has occurred.

## Recovery

If compatibility evidence changes, revert commit `6f38b4f2b5c085e3abd7257792750d742001b719` and restore the prior package pin, then repeat the collector proof and native Connections readback. Do not update the pin again without a release asset, CLI-flag compatibility, and live fleet readback.

## Boundary

This evidence closes the enrollment release mismatch only. Finance remains unqualified: the current native bridge reports `qualified_export_failed`, the imported edition is stale beyond its 24-hour freshness limit, and its accounting classification and company scope are unknown. The Finance reader bead remains blocked on the source-owner qualification packet.

## Beads reconciliation note

The canonical Beads workspace was not available from this recovered checkout (`bd where` reported no active workspace). This file is the durable handoff record; reconcile the release receipt to `eco-6hoxj.124.8` and keep `eco-6hoxj.124.2` in progress until PR121 CI, review, merge, deploy, and native production readback are complete.
