# Plimsoll Issue Guide — "Soundings"

Plimsoll issues are written as **soundings**: compact operating records a human
can scan on a board and an agent can pick up without rediscovering the work.
(A sounding is the depth measurement a ship takes before committing to a
channel — know the bottom before you steer.)

## Shape

```md
## TL;DR
- 2–4 bullets: outcome first, then the load-bearing facts

## Scope
What this issue does and explicitly does not cover.

## Context
Exact state of the world: file paths, commands, versions, links, SHAs.

## Problem / Task
The user-visible or operator-visible outcome, before implementation detail.

## Evidence
Errors, transcripts, query output, screenshots — verbatim, not paraphrased.

## Acceptance Criteria
Checkable by a test, proof check, command output, or live-state inspection.

## Operational Boundaries
What must not change: privacy invariants, schema compat, proof must stay green.

## Notes For Future Agents
Traps, prior art, related files — what you'd want known on pickup.

## Open Questions
Unknowns stated as questions, never guessed at.
```

Remove empty sections. Keep the top half useful on a board view.

## Rules

- Outcome before implementation. "Codex sessions show token coverage in
  `collector status`" beats "parse span attributes".
- Preserve exact evidence: real errors, real paths, real SHAs. Separate facts
  from hypotheses (`Suspected:`).
- Acceptance criteria must be verifiable — prefer "proof check X passes" or a
  command + expected output.
- Every capture-path issue inherits two boundaries automatically:
  `pnpm proof` stays green, and no raw content persists in metadata mode.
- Parent issues (multi-PR outcomes) list child lanes and the evidence required
  to close. Child issues are sized for one PR.
- Don't invent owners, dates, or priorities. Use Open Questions instead.

## Numbering

Files are `issues/NNNN-slug.md`, numbered in creation order. When an issue
ships, add a `Closed:` line at the top with the PR link and the evidence that
satisfied the acceptance criteria — closed soundings stay in the folder as the
project's memory.
