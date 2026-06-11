# Contributing to Plimsoll

Plimsoll watches developers work, so the bar is different from a normal dev tool:
**trust is the product.** Every contribution is judged first on whether it keeps
the capture path honest and the privacy promises inspectable.

## Ground rules

1. **`pnpm proof` must stay green.** The signal-fidelity proof (14+ checks) is the
   release gate. If your change touches capture, normalization, suppression, or
   sync, add a check that would fail without your change.
2. **Nothing raw persists in metadata mode.** Prompts, outputs, commands, file
   bodies, tool arguments — if you add a new telemetry source, its raw-content
   attribute names go in the forbidden list (`packages/shared/src/schemas.ts`)
   and a sentinel for them goes in the proof.
3. **Features never migrate from open to paid.** If you're unsure whether
   something belongs in this repo or the hosted product: descriptive analytics
   about the user's own data → here. Comparative/prescriptive across users or
   orgs → hosted. Concretely: `pnpm report -- --patterns` describes your own
   ledger (counts, ratios, distributions) and contains **no** recommendation,
   score, or benchmark — the proof check `patterns_report_descriptive_only`
   enforces that line in CI. Cohorts, comparisons, and advice are hosted-only.

## High-leverage contributions

- **Agent adapters** — get a new tool's telemetry (Cursor, Gemini CLI, Copilot
  CLI, aider…) into the normalizer. The `ToolSource` enum, action-class table,
  and per-record OTLP exploder are built for extension. Start from
  `issues/0005-cursor-adapter.md` as the template lane.
- **Signal-fidelity checks** — find a real-world telemetry shape that the
  capture path mishandles, encode it as a proof check, then fix it.
- **Platform support** — Linux paths/systemd, Windows.

## Workflow

```bash
pnpm install
pnpm proof          # must pass before and after your change
pnpm collector help # explore the CLI
```

- Branch from `main`, one logical change per PR.
- PRs that touch capture include before/after evidence (proof output, or a
  `collector status` coverage diff from a real session).
- Issues live in `issues/` as operating records — see ISSUE_GUIDE.md. Claiming
  one: comment on the GitHub issue mirror or open a draft PR referencing it.

## Privacy review

Any PR that adds a stored field, a new attribute key, or a new upload payload
gets a privacy review pass: what is it, why is it needed, is it raw/hashed/plain,
and which proof check covers it. Reviewers will ask; answering in the PR
description up front speeds everything up.
