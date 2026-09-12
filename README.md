# Plimsoll

**The load line for your AI spend.**

Plimsoll is a local-first telemetry collector for AI coding agents (Claude Code, Gemini CLI, Grok, and Codex today) that answers the question every team is guessing at:

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
# wire Claude Code, Gemini CLI, Grok, and Codex telemetry (independent targets;
# idempotent, takes backups; --dry-run to preview)
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

The packaged CLI also ships transactional lifecycle commands backed by an
immutable, version-pinned runtime
([docs/local-lifecycle.md](docs/local-lifecycle.md)):

```bash
# Pin the running packaged bundle as the immutable runtime and repoint the
# owned LaunchAgent manifest at it; any readiness failure restores the
# previous runtime, config, database, and manifest automatically.
npx -y @plimsoll/cli@<version> lifecycle update --operation-id <id> --artifact self

# Restart the daemon on the new immutable runtime (explicit, never automatic):
npx -y @plimsoll/cli@<version> load-launch-agent

# Preview-default uninstall of ONLY owned targets; data purge is a separate,
# exact-confirmation operation. Support bundles are sanitized and bounded.
```

These commands never invoke `launchctl`, never run from a source checkout via
`self`, and print one JSON receipt each. Release signing, npm publication,
and live-fleet rollout remain gated under
[#103](https://github.com/CryptoJym/plimsoll/issues/103).

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
`~/.claude/settings.json`, `~/.gemini/settings.json`,
`${GROK_HOME:-~/.grok}/hooks/plimsoll.json`, and `~/.codex/config.toml` if
you'd rather paste by hand. Managed hooks/exporters point only at
`127.0.0.1:48271`. Nothing is configured behind your back.

Claude Code can also run against a separate config home
(`CLAUDE_CONFIG_DIR=~/.claude-seats/<slug>`), which fleet lanes use to keep one
seat per account. Those sessions read the seat's own `settings.json`, not
`~/.claude/settings.json`, so `setup` manages each of them as its own target,
reported as `claudeSeat[<slug>]`: it discovers every
`~/.claude-seats/*/settings.json` and merges exactly the same exporter
environment and Plimsoll hooks it merges into `~/.claude/settings.json`, with
the same additive merge, the same backups and the same second-run no-op. Your
seat's own hooks and keys are preserved byte-for-byte, a seat directory without
a `settings.json` is skipped rather than created, and a seat added later is
picked up by the next `setup` run. `plimsoll doctor --read-only --json` lists
each discovered seat under `telemetry.claudeSeats` and flags an unmanaged one
with `claude_seat_settings_unmanaged` — a coverage diagnostic that does not
change doctor's readiness verdict.

Codex has the same split: fleet lanes run with
`CODEX_HOME=~/.codex-profiles/<slug>`, so those sessions read the profile's own
`config.toml` and got neither the `[otel]` exporters nor the Plimsoll hooks. As
of 0.7.20 `setup` manages each discovered `~/.codex-profiles/*/config.toml` as
its own target, reported as `codexProfile[<slug>]`, merging exactly the content
it merges into `~/.codex/config.toml` through the same additive TOML
reconciliation: your own hooks, tables, comments and unknown keys stay
byte-identical, backups are written before any change, and the second run is a
no-op. The profile's hook commands reference the same mode-0600
`~/.codex/plimsoll.headers` file the default target references, so no profile
config carries a token in a hook command, and no per-profile header file is
created. A profile directory without a `config.toml` is skipped rather than
created, and a malformed profile config is reported and left alone — it never
fails `setup`, whose own declared targets still decide the exit code.
`plimsoll doctor --read-only --json` lists each discovered profile under
`telemetry.codexProfiles` and flags an unmanaged or unreadable one with
`codex_profile_config_unmanaged`, again without changing the readiness verdict.

The seat and conductor tooling rewrites those files whenever a seat or profile
churns, and a rewritten file silently loses the managed block. As of 0.7.21 the
collector heals that itself: `plimsoll setup --reconcile` re-runs exactly the
Claude and Codex half of setup's target composition — `~/.claude/settings.json`,
`~/.codex/config.toml` and every discovered seat and profile — and applies only
where the plan says `added` or `updated`. It is strictly weaker than
`setup --yes`: it never creates a file that is not there, never mints a
credential, never rewrites a file it cannot parse, and stands down on any file
another writer changes while the plan is being computed, or that the
transactional write itself finds changed underneath it. A seat or profile
directory that exists with no config file in it yet is reported as
`skipped: absent` rather than created. A run that applied or
refused something writes `<collector home>/receipts/managed-config-reconcile-<ts>.json`
with the per-target status, plan lines and backups; a healthy home plans every
target `unchanged` and writes nothing at all. The running collector calls the
same reconcile in-process every `managedConfig.reconcile.intervalSeconds`
(default 600), and only when its own doctor readback reports at least one
drifted target, so a healthy host does zero writes; the tick yields to the event
loop between targets, so a fully churned fleet-scale host never blocks the
collector's HTTP loop for a whole reconcile. A file Plimsoll could not parse is
not retried for an hour, unless it changes on disk first — fixing it heals the
target on the next tick rather than after the hour. Losing a race with another
writer is not a refusal and arms no backoff.

The cadence keeps its own litter bounded: at most five `.plimsoll-backup-*`
files per managed file (never deleting one less than 24 hours old) and at most
twenty reconcile receipts, oldest first. `setup --yes` keeps its existing backup
policy and prunes nothing.

Set `managedConfig.reconcile.enabled` to `false` in `collector.config.json` to
turn the schedule off — the collector re-reads that flag and the interval from
the file on every tick, so the kill-switch takes effect without a restart, and a
disabled collector then performs no managed-config read or write of its own.
`plimsoll doctor --read-only --json` reports the schedule under
`managedConfig.reconcile` (`enabled`, `intervalSeconds`, `lastRunAt`,
`lastResult`, `lastApplied`, `lastRefused`, `lastAbsent`, `nextEligibleAt`).
`lastRunAt` advances on every tick that ran its readback, including one that
found nothing to do (`lastResult: "unchanged"`), so a clean cadence is
distinguishable from one that never ran.

Telemetry `setup` manages a seat's *config*; what the collector *captures* from
is its capture-root inventory (`collector.config.json` → `captureRoots[]`),
which is minted once, at enrollment. A host that gains a native root later — a
new Claude seat's `projects/`, a new Codex profile's `sessions/`, or a
`~/.claude/projects` an older enrollment never registered — keeps emitting
spans while its transcripts and rollouts go uncaptured. `capture-roots` closes
that gap without touching enrollment:

```bash
# what native roots exist under $HOME, and which are already registered
plimsoll capture-roots discover --json     # state: registered | candidate | missing

# preview, then append one (repeat --directory to add several at once)
plimsoll capture-roots add --source claude_code --directory ~/.claude/projects \
  --machine <fleet label> --dry-run
plimsoll capture-roots add --source claude_code --directory ~/.claude/projects \
  --machine <fleet label>
```

`add` is append-only. It derives the new root's identity exactly as the
enrolled roots' identities derive, and refuses — with a reason code and no
write — a duplicate directory or id, a path that is not a physical directory,
a path outside `$HOME`, an unknown source, a new directory whose contents
cannot be enumerated unambiguously (`capture_root_scan_ambiguous`), a config
that would lose a top-level field this schema does not know
(`append_only_violation`), or a config whose existing roots do not reproduce
their own ids under the label given (`identity_derivation_mismatch`).

`capture_root_scan_ambiguous` names the entries it could not resolve — a
symlinked or non-regular `.jsonl`, an unreadable subdirectory — because the
fence would not be provable over them. `--allow-scan-errors` registers the
root anyway: the same entries are listed in the receipt under
`scanAmbiguities` and simply left out of the fence, which means they are
**not** excluded and the tailer will capture them like any other file it
finds. Use it when the ambiguous entries are files you are content to have
captured; without it the default refusal is unchanged.

A `$HOME` that is itself a symlink is refused by the repo-wide no-follow
LaunchAgent path guard (`launch_agent_manifest_invalid`, detail
`LAUNCH_AGENT_UNSAFE_HOME`) before anything is written — spell the directory
through the physical home when adding a root.

The identity is derived from the host's **fleet machine label**, which is
stored nowhere — only its digests are. It is not derived from the hostname, so
on a fleet host **`--machine <fleet label>` is the expected form**: `add`
recovers the label from the roots already configured only when a hostname
candidate happens to reproduce their ids, and otherwise refuses
`identity_machine_unresolved` (distinct from `identity_derivation_mismatch`,
which means a label *was* given and this config contradicts it).

A real run stops the collector through the same path as `unload-launch-agent`,
writes a timestamped backup, fences the new root by recording the files it
already holds as pre-existing generations — so those transcripts and rollouts
are excluded rather than replayed as today's work — writes the config
transactionally, starts the collector again and verifies it, then writes a
receipt (before/after config sha256, the roots added, the generations fenced,
restart result) under `<collector home>/receipts/`. The fence is scoped to the
new directory: the provider's baseline cutoff is **not** moved, so every root
already registered keeps capturing exactly what it was capturing. Top-level
config fields this schema does not know are carried through the write
untouched and listed in the receipt as `carriedUnknownKeys`.

Nothing is left half-applied, and the receipt never claims a recovery that
did not happen. Whatever fails from the unload onwards — including a throw
after the bootout, when the daemon is already down — `add` starts the
collector again, records the failed step and the resulting state, and exits 1.
`recovery` is one of:

| `recovery` | What it means |
|---|---|
| `config_applied_collector_restarted` | The write completed. The config names the new roots and the fence belongs to them. |
| `config_unchanged_fence_rolled_back` | The config is byte-identical to the backup, and every generation row this run fenced was removed again. |
| `ledger_fence_retained` | The config was **not** written, but the fence could not be rolled back. `fenceRollback.retainedFiles` lists the files this run fenced (a superset of what is still excluded if the rollback was partial) — remove their rows or re-run the add to register the root they belong to. |
| `config_unchanged_no_backup_written` | Nothing was written at all: the failure was at or before the backup step. `backupPath` is then `null`. |
| `config_unchanged_restored_state_matches_backup` | The config is byte-identical to the backup and this run fenced nothing. |

`backupPath` is only ever a backup that exists on disk; `backupWritten` says
whether the backup step completed. A retry whose fence is already in place
reports `baseline.seals[].reason: "already_sealed"` with the count found and
`baseline.seededAt: null` — the fence is real, this run did not write it. A
restart or daemon verification that does not come back also exits 1 with the
failed step named. Without an installed LaunchAgent the restart is skipped and
the receipt says so. Existing roots, the installation epoch and every other enrollment field
are never changed. `plimsoll doctor --read-only --json` reports what is still
unregistered under `captureRoots.unregisteredCandidates` — like the seat
diagnostic, it does not change doctor's readiness verdict.

The Grok and Codex hook commands carry no secret: each reads its producer
token from a mode-0600 `plimsoll.headers` file beside its own config
(`${GROK_HOME:-~/.grok}/hooks/plimsoll.headers` and `~/.codex/plimsoll.headers`),
so a config search or a pasted command string never exposes the token. Codex's
own OTLP exporter has no file or environment source for a header value, so
`[otel.*_exporter."otlp-http"] headers` keeps the token inline; rotate it with
`plimsoll rotate-producer-token --source codex`, which rewrites the header file,
`config.toml` and every discovered `~/.codex-profiles/<slug>/config.toml` that
already carries the managed block — with a backup per file, inside the same
grace window — and accepts the superseded token only until that window closes
(`--grace-seconds`, default 900). A profile without the managed block is left
untouched (`setup` owns provisioning it) and a malformed one is reported under
`profilesSkipped` and never rewritten, neither of them failing the rotation.
`--dry-run` lists the `codexProfile[<slug>].otel.<exporter>.headers updated`
lines it would write. `plimsoll doctor` reports the rotation deadline and never
prints a token.

The source install script's `--dry-run` does not clone, install dependencies,
write Claude/Gemini/Grok/Codex or Plimsoll files, register a LaunchAgent, or start a
collector. The real install fails closed if the final doctor gate is below
`signal_verified`; the JSON report names the incomplete readiness level and
each missing/conflicted requirement.

> **Codex note:** Codex records token usage on *trace spans* (`gen_ai.usage.*`), not log events. The generated config enables logs, traces, and metrics — if you disable the trace exporter, codex token attribution silently drops to zero. We learned this the hard way (see "The audit story" below).

## Local outcome performance

`backfill-outcome-timeline` remains the explicit, bounded GitHub collection
command. After each run, Plimsoll deterministically materializes its immutable
pull facts into local performance rows for the dashboard. This is not a new
feed and does not fetch in the dashboard request path. `MERGED`,
`FIRST_PASS`, and `REWORK_OBSERVED` are only emitted when the timeline proves
them; missing evidence or required-check policy coverage is the literal
`UNKNOWN`, never a fabricated failure, zero, or no-rework result.

```bash
# Re-derive an existing local outcome store without making a provider request.
pnpm backfill:outcome-performance -- --required-checks ./required-checks.json

# Create a local seven-day Markdown + JSON summary on demand (no scheduler).
pnpm weekly:performance -- --out-dir ./performance-rollups
```

The dashboard's **Outcome performance** panel uses the same materialized rows
as the weekly rollup. Its time buckets use the observed merge timestamp when
available, otherwise the observed pull-creation timestamp; rows without either
stay undated rather than being assigned an invented date.

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

1. **v0.1 — Open release** (now): macOS collector, Claude Code + Gemini CLI + Codex, local efficiency reports, signal-fidelity CI. ([issue 0001](issues/0001-v0.1-release-readiness.md))
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
