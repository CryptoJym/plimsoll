# 0002 — CI: signal-fidelity proof on every PR

## TL;DR
- GitHub Actions: run `pnpm proof` on macOS runner for every PR and push to main.
- Proof needs git, Node 20+, pnpm, better-sqlite3 build — all available on `macos-14` runners.
- Badge in README once green.

## Scope
One workflow file. No release automation, no publishing — that's 0011.

## Context
- Proof: `scripts/signal-fidelity-proof.ts`, exits non-zero on any failed check, writes evidence/ artifacts (gitignored).
- better-sqlite3 needs its native build allowed: `pnpm-workspace.yaml` already sets `onlyBuiltDependencies: [better-sqlite3]`.
- Proof spins ephemeral servers on port 0 and creates a temp git repo — no network, no secrets needed.

## Acceptance Criteria
- [ ] `.github/workflows/proof.yml` runs on pull_request + push(main), completes < 5 min.
- [ ] A PR that breaks token extraction (e.g. comment out usage keys) fails CI.
- [ ] Evidence JSON uploaded as a workflow artifact for inspection.

## Operational Boundaries
- macOS runner (the collector targets macOS first); add ubuntu matrix only after 0007.
