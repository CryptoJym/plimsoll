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

Requirements for the published foreground CLI: macOS, Node >=20 <25. The
team source-canary installer below is narrower and pins Node 22.

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

The source tree also contains the adapter-driven update/rollback/uninstall and
sanitized support-bundle transaction primitive described in
[docs/local-lifecycle.md](docs/local-lifecycle.md). It is isolated-proofed but
is **not yet a published `plimsoll lifecycle` command or an authorized live
rollout**; release signing, npm publication, and real-Mac service integration
remain under [#103](https://github.com/CryptoJym/plimsoll/issues/103).

## Team Mac source-canary runbook

This is the reviewed development-checkout path for team canaries. It is not
the final signed package lifecycle. Get one approved, full 40-character commit
from the canary owner; never substitute `main`, another branch, or a shortened
SHA. The installer fetches that exact remote object, verifies the fetched and
checked-out commits, uses `pnpm install --frozen-lockfile`, and records a
sanitized JSON receipt on stdout.

Requirements: macOS arm64 or x86_64, git, one absolute Node 22 executable, one
absolute pnpm executable, and a bootstrap checkout containing `install.sh`.
Node 25 may remain the interactive-shell default: pass Node 22 explicitly.
pnpm does not need to be on a non-interactive shell's `PATH`. The source
LaunchAgent path uses the default per-user
`$HOME/Library/Application Support/Plimsoll`; a custom `PLIMSOLL_HOME` is
refused before mutation because that path is not yet propagated through the
service contract.

```bash
# Values are machine-local except the approved commit; do not share credentials.
SOURCE_SHA=<approved-full-40-character-commit>
NODE22=/absolute/path/to/node
PNPM=/absolute/path/to/pnpm

./install.sh --dry-run --ref "$SOURCE_SHA" --node "$NODE22" --pnpm "$PNPM"
./install.sh apply --ref "$SOURCE_SHA" --node "$NODE22" --pnpm "$PNPM"
# success is service_ready on a new/cold ledger, not capture proof

# Quit and start a new, already locally authenticated Codex or Claude session.
# Perform one ordinary token-bearing local interaction, then:
./install.sh verify --ref "$SOURCE_SHA" --node "$NODE22" --pnpm "$PNPM"
# only this phase succeeds as signal_verified
```

The fresh-machine and partial-install path is the same command. `setup`
reconciles only the Plimsoll-owned Claude/Codex telemetry fields, preserves
unrelated settings, keeps exact preimage backups for changed files, and makes
no new backups on a no-op retry. Interrupted apply is resumable with the same
commit and runtime arguments. A failure receipt names the literal retained
checkout/dependency/config/service state; it does not claim rollback.
Apply also stores a private, hash-only source/runtime identity inside the
checkout's Git metadata, including the apply-time token-count baseline and
runtime-instance hash. Apply never promotes an already-present historical
signal. Verify requires a strictly larger token count from that same runtime
instance and refuses a different commit, dirty tracked source, remote, Node
executable, pnpm executable, or version; it does not fetch or upgrade anything.

`--dry-run` is a byte no-op and plan-selection gate. It validates the immutable
input syntax and selects executable paths, but deliberately does not execute
pnpm or claim its version was validated. It does not fetch, clone, install, write tool or
Plimsoll files/backups, touch the plist/ledger, inspect credentials, bind a
port, or start/stop a process. Apply defaults to local metadata capture and its
postcondition rejects hosted sync/signing. Enrollment is a separate,
per-device future step; credentials and agent authentication never move
between Macs.

To stop while retaining the checkout, tool config, plist, and ledger, use the
same pinned runtime and source directory:

```bash
PATH="$(dirname "$NODE22"):$(dirname "$PNPM"):$PATH" \
  "$PNPM" --dir "$HOME/.plimsoll/app" collector unload-launch-agent

# Resume/reconcile after diagnosing a stale PID, plist, or occupied port:
./install.sh apply --ref "$SOURCE_SHA" --node "$NODE22" --pnpm "$PNPM"
./install.sh verify --ref "$SOURCE_SHA" --node "$NODE22" --pnpm "$PNPM"
```

Do not delete a stale PID or kill an unknown port owner just to make the gate
green. Inspect ownership first (`lsof -nP -iTCP:48271 -sTCP:LISTEN`), preserve
the stopped/state-retained boundary, and escalate a conflicting owner. Apply
reconciles one Plimsoll LaunchAgent. If a service loaded by the current apply
fails readiness, the installer requests an unload and reports the unload
command outcome as `stop_unverified`; command success alone is never promoted
to proof that the job, process, and listener stopped. Verify changes no
checkout, config, service, plist, or ledger data; apply and verify both use one
transient per-UID machine coordination lock, shared across HOME overrides for
that account.

Proof of done is: the apply receipt says `service_ready`, a newly started
native agent emits a real token signal, the verify receipt says
`signal_verified`, `pnpm proof:source-installer` passes under Node 22, and the
service can be stopped and resumed without a second owner. Source install does
not provide signed upgrades, rollback, uninstall/purge, or hosted fleet
control. Those remain open under
[#103](https://github.com/CryptoJym/plimsoll/issues/103); retain state rather
than describing manual deletion as rollback.

The installer proofs use a guarded temporary coordination-lock
namespace whose HOME, checkout, and collector home must all be inside the same
private proof root. It never creates, truncates, or removes the production
per-UID lock under `/private/tmp`.

The source installer PR is intentionally not promotion-ready by itself. It
must remain blocked until the atomic Claude config contract
[#130](https://github.com/CryptoJym/plimsoll/issues/130), live runtime/source
attestation [#131](https://github.com/CryptoJym/plimsoll/issues/131), and atomic
LaunchAgent manifest ownership
[#132](https://github.com/CryptoJym/plimsoll/issues/132), plus the single
machine-readable CLI contract
[#133](https://github.com/CryptoJym/plimsoll/issues/133), and custom collector
home isolation [#135](https://github.com/CryptoJym/plimsoll/issues/135), merge
and this lane is rebased onto them. The installer rejects an obvious existing plist symlink and
enforces a private regular-file postcondition, but those checks do not replace
#132's race-safe source implementation. Compatibility parsing on this branch
does not replace #133's requirement to invoke a real built CLI mode that emits
exactly one typed JSON receipt. Until #131 lands, plist/PID/cwd/runtime checks
are useful local evidence, not cryptographic proof of the running source SHA.
Do not use this PR alone for either real canary.

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
pnpm proof:system-e2e   # isolated two-machine, cross-stage source release gate
```

See [`docs/source-system-e2e.md`](docs/source-system-e2e.md) for the exact
source boundary and the hosted/controlled-Mac gates that remain not run.

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
