# 0003 — Enrollment: install.sh hardening + config apply mode

## TL;DR
- `install.sh` works but prints config snippets the user must paste; add `pnpm collector apply-config` to patch `~/.claude/settings.json` and `~/.codex/config.toml` in place (with backup + diff + confirmation).
- Goal: clean Mac → collecting in under 5 minutes, no manual JSON editing.

## Scope
macOS only. Claude Code + Codex config surfaces. No MDM/fleet provisioning (hosted-product territory).

## Context
- `packages/collector-config/src/templates.ts` already generates both configs.
- Claude Code env/hook config merges into settings.json `env` + `hooks` keys; Codex `[otel]` appends to config.toml — both need idempotent merge, not blind append (a re-run must not duplicate sections).
- Backup convention: `<file>.bak-plimsoll-<date>` before first write.

## Acceptance Criteria
- [ ] `pnpm collector apply-config claude-code|codex|all [--yes]` shows a diff, asks, applies, and is idempotent (second run = no-op).
- [ ] `install.sh --apply` flag chains it.
- [ ] Doctor verifies the applied config points at the running port.

## Operational Boundaries
- Never modify tool configs without showing the diff first; `--yes` is the only silent path.

## Notes For Future Agents
Codex hook trust: `~/.codex/hooks.json` entries get trusted-hash pinned by Codex; editing hooks there invalidates trust prompts. v0.1 deliberately uses OTLP only for Codex — don't add Codex hooks in apply-config.
