# Plimsoll

**The load line for your AI spend.**

Plimsoll is a local-first telemetry collector for AI coding agents (Claude Code and Codex today) that answers the question every team is guessing at:

> **What did we actually get for those tokens?**

It captures token usage, cost, tool behavior, and session structure on your machine, joins sessions to shipped outcomes (merged PRs, passing checks), and computes the economics — tokens per merged PR, cost per validated outcome, and where your spend is producing nothing.

```
$ pnpm report -- --repository your-org/your-repo

  PR #28 — merged ✓ checks passed ✓
  1 session · 41,799 in / 188,834 out tokens · 30.4M cache reads
  cost: $48.46
```

That's a real number from Plimsoll measuring the pull request that built Plimsoll.

## Why "Plimsoll"

In 1876 Samuel Plimsoll forced shipowners to paint a load line on every hull. Overloading deaths collapsed — not because anyone wrote a smarter regulation, but because the limit became **visible to anyone standing on the dock**.

AI spend today is a hull with no line: invisible loading, vendor dashboards that stop at org-level totals, and no connection to what shipped. Plimsoll paints the line — and like the original, it's painted on the *outside*: every byte that this collector records, suppresses, hashes, or uploads is open source and inspectable in this repository.

## How it works

```
Claude Code ── hooks (HTTP) ─────────────┐
Claude Code ── OTLP logs + metrics ──────┤
Codex ──────── OTLP logs/traces/metrics ─┤
                                         ▼
                          Plimsoll collector (localhost:48271)
                          · explodes OTLP per record — every API call's
                            tokens, cost, model, session captured
                          · derives action classes from tool names
                            (shell / edit / read / mcp / browser)
                          · suppresses raw content BEFORE persistence:
                            prompts, outputs, commands, file bodies,
                            tool arguments — never stored in metadata mode
                          · hashes identifiers (emails, paths, branches)
                          · resolves git linkage keys (hashed remote,
                            hashed branch, plain commit sha)
                                         ▼
                          local SQLite ledger (~/Library/Application
                          Support/Plimsoll, 90-day retention, indexed)
                                         ▼
            ┌────────────────────────────┴───────────────────┐
   local reports (free, forever)              optional hosted sync
   tokens/cost per repo, per PR,              (team rollups, benchmarks —
   per model, per session;                    watermark-based, signed,
   Validated Delivery Yield                   off by default)
```

The outcome join uses **linkage keys**: both Plimsoll and the GitHub side hash the same normalized inputs (remote URL, branch name), so sessions and pull requests join by construction while the raw strings never leave your machine. Commit shas stay plain — they're already public on GitHub.

## Quickstart

Requirements: macOS, Node >=20 <25.

```bash
# wire Claude Code + Codex telemetry (idempotent, takes backups; --dry-run to preview)
npx -y @plimsoll/cli setup

# run the collector + dashboard → http://127.0.0.1:48271
npx -y @plimsoll/cli start

# inspect readiness without creating config, a ledger, or service files
npx -y @plimsoll/cli doctor --read-only --json
```

`doctor` is a diagnostic gate, not an installer and not capture proof by
itself. Its readiness progresses through `not_installed` → `configured` →
`service_ready` → `signal_verified`; only `signal_verified` returns `ok:true`
and exit 0. A cold ledger therefore fails honestly until a real token-bearing
Claude Code or Codex event reaches the collector. Background LaunchAgent mode
for npm installs is still being fitted — until then `start` runs in a terminal.

**Contributors / running from source** (adds pnpm + git):

```bash
git clone https://github.com/CryptoJym/plimsoll.git
cd plimsoll
./install.sh --dry-run                        # preflight and exact mutation plan only
./install.sh                                  # setup + development LaunchAgent + strict gate

# Equivalent manual source commands:
pnpm install
pnpm collector setup --yes                    # idempotent; backs up changed tool configs
pnpm collector install-launch-agent --dev --repo-root "$PWD" --pnpm "$(command -v pnpm)" --load
pnpm collector doctor --read-only --json      # exits 0 only after a real token signal
pnpm report -- --repository your-org/your-repo   # after a few sessions: the economics
```

`setup` applies the tool configs for you (idempotent, takes backups,
`--dry-run` to preview); `generate-config` prints exactly what to add to
`~/.claude/settings.json` and `~/.codex/config.toml` if you'd rather paste
by hand — hooks plus OTLP exporters pointed at `127.0.0.1:48271`. Nothing
is configured behind your back.

The source install script's `--dry-run` does not clone, install dependencies,
write Claude/Codex or Plimsoll files, register a LaunchAgent, or start a
collector. The real install fails closed if the final doctor gate is below
`signal_verified`; the JSON report names the incomplete readiness level and
each missing/conflicted requirement.

> **Codex note:** Codex records token usage on *trace spans* (`gen_ai.usage.*`), not log events. The generated config enables logs, traces, and metrics — if you disable the trace exporter, codex token attribution silently drops to zero. We learned this the hard way (see "The audit story" below).

## What gets collected — and what never does

Plimsoll's default is **metadata mode**. In metadata mode, these are *removed before the local database write* — not redacted later, never stored:

- prompts and model outputs
- tool inputs/outputs, command bodies, file contents, diffs/patches
- codex tool `arguments` (raw command lines, workdir paths)
- clipboard, screenshots, keystrokes, browser history (never collected in any mode)

These are **hashed** before storage: emails, user/account IDs, file paths, working directories, repo remotes, branch names.

These are stored plain: timestamps, event types, tool *names*, action classes, models, token counts, costs, durations, session IDs, commit shas.

Managed or upload-enabled installs are locked to the literal
`metadata_only` privacy mode. Attempts to enable raw evidence through the
environment, collector config, CLI config generation, setup, join, or start
fail before the collector/config write; there is no silent downgrade. Existing
legacy rows marked `evidence` stay local and are excluded from both ordinary
sync and `upload-history`, which report
`local_quarantine_migration_required`. Plimsoll does not scan, migrate, or
delete those rows automatically. An encrypted evidence vault is **not
implemented**.

The suppression engine is [`packages/shared/src/policy.ts`](packages/shared/src/policy.ts) and the forbidden-field list is [`packages/shared/src/schemas.ts`](packages/shared/src/schemas.ts). The signal-fidelity proof plants sentinel commands, paths, and prompts and fails if any survive to disk:

```bash
pnpm proof   # 14 checks, writes evidence to evidence/
pnpm proof:privacy-mode # managed-mode, legacy quarantine, temp-home surface proof
```

## The audit story (why this exists in this shape)

Plimsoll's first incarnation ran for five weeks inside our own company and silently captured **0% of codex tokens and ~1% of claude tokens** — OTLP envelopes were flattened into single events, metric datapoints were never parsed, and a 7.5 GB local spool grew with almost no signal in it. The contract tests all passed the entire time; nothing asserted that a *real session* produced attributed tokens.

The rebuild produced this architecture — parse-at-ingest, per-record events, deterministic IDs, retention, watermark sync — plus the rule the project now lives by: **a capture pipeline is only as good as its signal-fidelity proof.** Re-parsing the old spool recovered $141 of attributed spend and 142k behavioral events, so the lesson cost us nothing but pride.

## Free vs. hosted — where the line is

Everything in this repository is free, Apache-2.0, and complete for an individual or a single machine — and features here never migrate to paid:

- the collector, suppression engine, local ledger, schemas
- local reports: tokens/cost per repo, per PR, per model, per session; join rates; Validated Delivery Yield
- backfill, proofs, config generation

The hosted product (separate, commercial) is the **comparative and prescriptive** layer: multi-machine team rollups, manager pattern views, finance cost-to-serve allocation, cross-company benchmarks, and the weekly Efficiency Brief. Descriptive analytics about your own data stay open; "how do we compare and what should we change" is what we sell.

## Status

| Area | State |
|---|---|
| Claude Code capture (hooks + OTLP logs/metrics) | ✅ verified on real sessions |
| Codex capture (hooks + OTLP logs/traces/metrics) | ⚠️ wired; live span-shape verification pending ([issue 0004](issues/0004-codex-live-span-verification.md)) |
| Action-class derivation, privacy suppression | ✅ proof-gated (14 checks) |
| Git linkage + PR efficiency report | ✅ verified on a real PR |
| Retention, watermark sync, backfill | ✅ |
| Linux / Windows | 🚧 [issue 0007](issues/0007-linux-windows-support.md) |
| More agents (Cursor, Gemini CLI, …) | 🚧 community lanes ([0005](issues/0005-cursor-adapter.md), [0006](issues/0006-gemini-cli-adapter.md)) |

## Roadmap

1. **v0.1 — Open release** (now): macOS collector, Claude Code + Codex, local efficiency reports, signal-fidelity CI. ([issue 0001](issues/0001-v0.1-release-readiness.md))
2. **v0.2 — Coverage**: codex live verification, per-event repo attribution for multi-repo sessions, rework-window detection for true Validated Delivery Yield, Linux support.
3. **v0.3 — Reach**: adapters for more agents (Cursor, Gemini CLI, Copilot CLI), npx one-line install, menubar app, signed standalone binary.
4. **Hosted beta** (commercial, separate repo): team rollups, benchmarks, Efficiency Brief.

The full backlog lives in [`issues/`](issues/) as operating records — each one is written so a human can scan it on a board and an agent can pick it up without rediscovering the work. See [ISSUE_GUIDE.md](ISSUE_GUIDE.md).

## Contributing

The highest-leverage contributions are **adapters** (get a new agent's telemetry into the normalizer — the source enum and action-class table are built for it) and **signal-fidelity checks** (find a way real telemetry breaks the capture path, encode it as a proof check). See [CONTRIBUTING.md](CONTRIBUTING.md).

Every PR must keep `pnpm proof` green. If your change touches capture, add a check that would have caught its absence.

## License

[Apache-2.0](LICENSE). The collector that watches your work should be software you can read.

---

*Plimsoll is built by [Utlyze](https://utlyze.com). The hosted analytics product is separate and commercial; this collector is complete, free, and will stay that way.*
