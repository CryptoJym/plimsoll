# 0013 — Docs: published privacy spec ("what leaves the machine")

## TL;DR
- One canonical page enumerating: collected-plain, collected-hashed, suppressed-always, never-collected — generated from the actual source lists so it cannot drift.
- This page is the sales pitch and the trust contract; reviewers at adopting companies will read exactly this.

## Scope
`docs/privacy-spec.md` + a generator script that extracts the forbidden/protected lists from `packages/shared/src/{schemas,policy}.ts` and fails CI if the doc is stale.

## Context
- Forbidden list: `forbiddenRawContentFieldNames` (schemas.ts). Protected/hashed: `protectedMetadataFieldNames` (policy.ts). Upload payload shape: `aiWorkIngestBatchSchema`.
- The proof's sentinel checks are the behavioral guarantee; link each spec claim to its check name.

## Acceptance Criteria
- [ ] `pnpm docs:privacy` regenerates the page; CI diff-checks it.
- [ ] Every claim maps to a source list or a named proof check — zero hand-written assertions about behavior.
