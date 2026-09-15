# BUILDER-EVIDENCE — bead eco-6hoxj.86 (REVIEW-68-r2 §8 N1–N4)

Proof/evidence only. `packages/collector-cli/src/hook-spool.ts`, `server.ts` and
`normalizer.ts` are blob-identical to base `790147cfda1ef9e6f94cc2632566c0add99c4ded`;
no file under any `packages/*/src/` appears in the diff.

| File | Answers |
|---|---|
| `nc-n1-rendered-page-leg.log` | N1 — three negative controls for the new, independent third leg. NC-N1c is the one that matters: the generator's table rendering changes, the page is regenerated, `docs:privacy --check` stays green, and at base the proof passes 121/121 while eleven rule-2 names quietly leave the operator's privacy page. |
| `nc-normalizer-copy.log` | N2 — the control the .68 lane owed: perturb the normalizer COPY (`normalizedSpelling`) and watch `r_every_protected_identity_name_is_blanked_or_declared` red with `normalizerCopyDisagrees` naming the nine affected names. |
| `nc-drain-future-time.log` | N2 — the drain future-time perturbation the .68 lane filed under the wrong name (`nc4-skewed-predicate.log`), kept under its own name because it is a real control for a different pin. |
| `nc-n4-call-site-and-conjunction.log` | N4 — the call-site guard measured as a no-op (121/121 at base, 123/123 here, with the line deleted in a throwaway worktree), and the conjunction inside `spoolKeepsProtectedIdentityRaw` shown load-bearing. No runtime change was made. |
| `strip-comments.mjs` | N3 — comment stripping through the TypeScript compiler API, replacing the .68 lane's character state machine. |
| `strip-comments-naive.mjs` | N3 — a faithful reconstruction of that state machine, kept only so its regex-literal defect is reproducible. |
| `fixtures/regex-literal.ts` | N3 — the counterexample: real code the naive strip truncates. |
| `comment-strip-digests.txt` | N3 — tool digests, the counterexample divergence, and comment-stripped digests of every file in this delta at base and at HEAD. |

## Running the tools

    node BUILDER-EVIDENCE/strip-comments.mjs <file.ts> [...]        # digests
    node BUILDER-EVIDENCE/strip-comments.mjs --emit=code  <file.ts> # stripped text
    node BUILDER-EVIDENCE/strip-comments.mjs --emit=spans <file.ts>

## What the control logs hold

Each log carries the exact perturbation, the full detail of every check that
red, and the proof's summary line. The unabridged ~74 KB run output for each
control stays beside the lane in `logs/` rather than in the repository; the
lead harvests it from there.

Every control ran in a throwaway `git worktree` under the lane, reverted between
runs, removed and pruned before the lane finished. Fixture homes and
`TMPDIR=/private/tmp/b86t` only; the real home was never written, the live
collector never touched.
