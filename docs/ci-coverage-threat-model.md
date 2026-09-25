# CI Coverage Gate Threat Model

The CI coverage gate protects against accidental proof drift. It inventories
proof entry files, checks that each one is run by a workflow step or has a
reviewed exception, and models the workflow conditions, command form,
environment, package-manager settings, and quarantine dates that determine
whether a proof is present in CI. The gate also self-tests those rules and
emits a completion receipt with a fixed assertion count.

The gate is a static hygiene check. It reads workflow and repository files; it
does not execute every shell command or prove that a command's implementation
keeps all of its stages. Deliberate workflow tampering by someone who can edit
the repository remains a code-review concern.

Known accepted limits from the round-3 replay are:

- package-manager rc files or pnpmfiles written through arbitrary tools such as
  `dd`, `ln`, `python`, `node`, `printf`, or a variable-expanded path;
- newline or heredoc injection through an otherwise allow-listed
  `$GITHUB_ENV` value;
- prepared-copy `cd`, `working-directory`, or `env -u` tricks that change the
  runtime checkout or environment through shell behavior the static model
  cannot resolve;
- cache contents or paths assembled indirectly at runtime; and
- a gate step disabled with `if: false` or made green with
  `continue-on-error`.

The static rules do cover the accidental forms found in Round 4: literal
`actions/cache` paths for `pnpm-workspace.yaml` and `.config/pnpm/rc`, and a
literal `XDG_CONFIG_HOME` that resolves to a checked-in `pnpm/rc`. They also
require `PROJECTION_PUBLICATION_COST_SCALE` to be a finite value in the
documented inclusive range `0.01` through `1.0`, and reject a local-only
declaration whose named input is read only through an optional `||` fallback.
An indirect path or fallback remains a code-review and runtime concern.

The accepted-limit accounting is deliberate. `b2_config_flag_after_script`
and `b3_set_a_source_earlier_step` are harmless in the runtime evidence: the
configuration assignment happens after the relevant proof or in an earlier
step whose effect does not reach it. The suite reduction cases
`suite_code_hardcodes_six_via_run_proof` and `suite_code_skips_cadence` are
runtime backstop cases: their completion receipts fail when stages or cadence
are reduced, even though the static inventory cannot infer those values.
`resource_proof_help_arg` is accidental invocation drift, not deliberate shell indirection; it needs the resource proof's passed receipt.

The final CI receipt owned by eco-6hoxj.163.32 is the runtime backstop. It must
read `evidence/completion/ci-coverage-proof.json` from the checkout, require
`status: "passed"`, `directNode: false`, `forwardedArgs: []`, `root` equal to
the checkout real path, and `expectedChecks == FIXTURES.length + 2`. It must
also require the nested completion receipt to be passed with matching counts
and no failed checks. A skipped gate has no receipt; a continued-on-error gate
has a failed receipt, so both fail this final check. Proof-specific reduced
stage runs are covered by each proof's own completion count and receipt.

This division keeps the static gate focused on accidental drift while leaving
intentional shell-level changes to review and runtime execution receipts.
