# Reproduce collector changed-file lint

Use Node 22 and the checked-in `scripts/collector-eslint.config.mjs`. Its
TypeScript recommended profile keeps unused-variable and prefer-const checks;
unused arguments/bindings explicitly named with `_` are permitted. In particular,
`validatedTransportUrl(raw, _label)` deliberately does not echo its label in a
value-blind transport error. Existing boundary `any` casts, CJS imports and empty
catch handlers remain permitted. No pre-existing CI check is changed.

Install the pinned tooling into an isolated directory, without modifying package
scripts, the lockfile or user settings:

```sh
collector_lint_tools="$PWD/ci-home/lint-tools"
mkdir -p "$collector_lint_tools"
npm install --prefix "$collector_lint_tools" --no-save --no-audit --no-fund \
  eslint@9.39.1 typescript-eslint@8.46.1
PLIMSOLL_LINT_TOOLS="$collector_lint_tools" \
  node "$collector_lint_tools/node_modules/eslint/bin/eslint.js" \
  --config scripts/collector-eslint.config.mjs \
  packages/collector-cli/src/project-intent-command.ts \
  packages/collector-cli/src/project-intent-producer.ts \
  packages/collector-cli/src/http-transport.ts \
  scripts/lib/project-intent-fixture.ts tests/project-intent-cli.test.ts
```

Use the same profile for other changed `.ts` files. Round-two evidence includes
the resolved tooling versions, config hash, full command and exit code; the
transport file is included to make `_label` reproducible.
