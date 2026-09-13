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

A hook event the collector cannot accept right now is **spooled, not dropped**.
The spool is one private file per event under `hook-spool/` in the Plimsoll
home, carrying no credentials, which the collector drains back through the
ordinary hook path every five seconds once the ledger is free — so a recovered
event lands in the ledger attributed exactly like a live one.

Spooling happens in **two** places, because hooks reach the collector in two
ways:

- **The collector's own intake**, for every hook path. The managed hooks post
  straight to `http://127.0.0.1:<port>/hooks/<source>` — Claude Code's is an
  `http` hook, the Codex and Grok hooks are `curl` commands — and never run a
  Plimsoll command. When such a post is authorized and admitted but the local
  ledger stays busy past its retry budget, the collector writes the spool file
  itself and answers `202 {"status":"hook_spooled"}` instead of the old
  `503 storage_busy_retry`. It answers 202 only after flushing the private
  temporary file, publishing it by rename, and flushing the containing directory
  (including the home entry for a newly created spool directory). A failed flush
  is not acknowledged; if the spool cannot be written (its bounds are reached,
  the disk refuses, or `PLIMSOLL_HOOK_SPOOL=off`), the answer stays exactly
  today's 503 so the loss stays visible.
- **The `forward-hook-http` client**, for a host whose hook runs that command.
  It spools when the collector answers 503, answers 408
  (`request_deadline_exceeded`), or is not listening at all (connection refused
  during a managed update window).

What is still lost: a request whose **body never finished arriving** (the 408 —
there is nothing whole to spool), and, for the `http`/`curl` hooks, a post that
never reaches a listening collector at all (connection refused — the collector
is not there to spool it, and those hooks have no Plimsoll process of their own
to do it for them; `forward-hook-http` hosts do spool that case).

The spool is **at-most-once**: every one of those outcomes proves the collector
stored nothing, so a spooled event is never a duplicate. At the intake, the 503
class is raised only when the ledger write did not commit — the durable append
runs in a single `BEGIN IMMEDIATE` transaction that SQLite has rolled back by
the time the busy error escapes it — and it is the only outcome that is spooled
there. The window the spool does not close is the collector dying mid-request —
a connection reset or a closed socket after the body was sent may mean the row
was already written, so that event still fails loudly and is lost rather than
risk double-counting it in cost and usage. Closing that window needs idempotent
replay (a client-minted event id) and is tracked separately.

What is on disk is **not** the raw body. Before the file is written — by either
writer, through the same function — the collector's own pre-write suppression
rule is applied to it: every key the collector would strip — raw prompt, output
and tool content, credential-like names, file and transcript paths — keeps its
name and loses its value. So
`hook-spool/` holds no more than the local ledger is allowed to hold, and the
collector's own suppression produces exactly the same receipts when the event is
replayed. The deliberate exceptions are two short lists of keys. The first
is what the collector reads *before* suppressing it to derive something it
stores — the working directory it turns into repository linkage, and the hook
event name it turns into the event type. The second is the protected identity
names (`username`, `user_id`, `account_id`, `user.id`, `workspace_root`, …):
the ledger keeps the *hash* of those values rather than dropping them, so an
emptied one would persist the hash of an empty string instead of the hash of the
real identity — the spool file holds the raw value until the collector applies
the event, and holds it for that reason. Blanking either list would quietly make
a recovered event worse than a live one, so their values stay; every one of them
is listed by name, with the reason, in
[docs/privacy-spec.md](docs/privacy-spec.md) under *Where captured data rests on
disk*, which is generated from the code that enforces it.

A recovered event carries the time it **arrived**, not the time the drain got to
it: the file is stamped when it is spooled — with the hook process's own time
when the client spools it, with the daemon's request receive time when the
intake spools it — and the drain hands that stamp to the collector as the
event's timestamp, so a spooled event lands in the same cost and usage window
it would have live. A body that carries a
timestamp the collector can actually use keeps it, untouched — usable is the
collector's own test, so a numeric epoch, an empty string or a future-dated
time is not a timestamp for this purpose and the hook's stamp is used instead,
exactly as it would be live.

`deferred` counts failed drain attempts, not unique events: the same queued
file can add one on multiple ticks. Use `pendingFiles` and its age for the
current backlog. The counter is cumulative and does not reset when it drains.

File/directory synchronization uses the same OS primitives as the other durable
local writers. This is not a hardware power-cut certification. If publication's
directory flush fails, the writer tries to hide its unacknowledged envelope as a
bounded orphan temporary and returns failure. If the filesystem also refuses
that rollback, a visible unacknowledged file can remain; no universal exactly-once
claim is made for storage failures or interrupted delivery.

The counters (`recovered`, `rejected`, `deferred`, `spooledAtIntake` — how many
events the collector's own intake spooled — pending files and their age) are in
`plimsoll status`, `plimsoll doctor`, and the collector's `/status` under
`hookSpool`; `enabled` there is the running collector's kill-switch state, read
from the collector itself — `plimsoll status` asks the daemon for it in one
request bounded by `PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS` (3 s by default), and
is otherwise a purely local read. It reads `null` with `enabledSource:
"collector_unreachable"` when the collector cannot be asked, and `null` with
`enabledSource: "collector_too_old"` when a collector older than 0.7.22 answers
— reachable and healthy, but with no drain, so the spool holds its events until
that collector is updated. Doctor says so plainly if anything has been pending
for more than ten minutes. The counters are written once per drain tick, after
the tick has applied its files, so `status`/`doctor` can lag the ledger by the
remainder of a 5 s tick: an event can be queryable in the ledger a moment before
`recovered` counts it. Contract rejections are **not** spooled: a body the collector refuses on its merits (4xx other than 408)
still fails the hook loudly, and a spooled file the drain cannot apply is
quarantined under `hook-spool/rejected/` rather than retried forever. An
operator watching `collector.err.log` sees intake spooling happen without any
body: the first spool in each minute prints `hook_spooled_at_intake` with its
source and client class, and the rest of that minute is reported by one
`hook_spooled_at_intake_summary` line with the count — while the
`storage_busy_retry` rejection summary now counts only the posts that were
really refused, the ones whose spool write failed. Set
`PLIMSOLL_HOOK_SPOOL=off` in the collector's environment to restore the previous
drop-and-log behaviour on both paths.

Residual behaviour worth knowing:

- A working directory reported **only** inside `args`/`arguments`/`tool_arguments`
  is not recovered: those keys are raw command content and are blanked before the
  write, so such an event replays without repository linkage. The event itself is
  not lost. Report the working directory at the top level (`cwd`) to keep it.
  `pnpm proof:hook-spool` measures this divergence on every run rather than
  leaving it to be discovered.
- A body carrying a live-usage protocol string under a suppressed key is refused
  live (403) but admitted on replay, because the string is inside the value the
  spool blanked. No live-usage figure is ever admitted — only the event.
- Spool counters in `status`/`doctor` can lag the ledger by the remainder of a
  5 s drain tick, as above. `spooledAtIntake` is written to the counters file
  the moment the intake spools, so `plimsoll status`/`doctor`, which read that
  file, are immediate; the collector's own `/status` serves the drain's cached
  snapshot and so lags by the same tick.
- An `http`/`curl` hook whose post never reaches a listening collector is still
  lost: there is no Plimsoll process on that path to spool it.

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

The optional macOS menubar companion provides a status-bar glance at collector
state, event count, and token coverage. See
[packages/mac-menubar](packages/mac-menubar) for SwiftPM build instructions;
App Store packaging is separate release work.

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
churns, and a rewritten file silently loses the managed block. The collector
heals that itself: `plimsoll setup --reconcile` re-runs exactly the
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
files per managed file and at most twenty reconcile receipts, oldest first. It
prunes only the backups it wrote itself — each one is recorded in its state file
when it is written — and it never deletes the oldest backup a managed file has,
so the copy of a host's pre-Plimsoll bytes that `setup --yes` wrote is never a
pruning candidate. `setup --yes` keeps its existing backup policy and prunes
nothing. The count bound binds only once a backup is more than 24 hours old: a
younger one is live rollback material and is kept whatever the count says, so a
file some other writer churns every tick carries the five older backups plus one
day of young ones rather than five in total.

Set `managedConfig.reconcile.enabled` to `false` in `collector.config.json` to
turn the schedule off — the collector re-reads that flag and the interval from
the file on every tick, so the kill-switch takes effect without a restart, and a
disabled collector then performs no managed-config read or write of its own.
`plimsoll doctor --read-only --json` reports the schedule under
`managedConfig.reconcile` (`enabled`, `intervalSeconds`, `lastRunAt`,
`lastResult`, `lastApplied`, `lastRefused`, `lastAbsent`, `nextEligibleAt`).
`lastRunAt` advances on every tick that ran its readback, including one that
found nothing to do (`lastResult: "unchanged"`), so a clean cadence is
distinguishable from one that never ran. A host with no Plimsoll-local
credentials manages no targets at all and stamps `lastResult: "unavailable"`
instead, so it does not read as a healthy host. The state file is read and
written under the same cross-process mutation lock the collector config uses, so
an operator's `setup --reconcile` and a daemon tick cannot drop each other's
backoff entries or run stamp.

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

**Zero-value span admission.** Codex's `codex-app-server` exports its whole
internal tracing tree over OTLP: on one conductor host that was 95,739 of
106,668 ingested events in a single hour, spread over 147 span names
(`realtime_conversation.running_state`, `environments.snapshot`,
`mcp.runtime.refresh_wait`, …) that carry no tokens, no cost, no tool name and
no session or actor linkage. Plimsoll discards those spans at admission,
*before* the ledger write, so neither the local buffer nor the upload queue
grows from them. An app-server span that does carry a retained dimension —
usage, an error or exception, an explicit action class, a tool name, or
analytical linkage (session, actor, project, customer, workflow, git, request
or call id) — is admitted exactly as before; a model name with no usage is not
a retained dimension, because it joins to nothing. The typed Codex events the
reports are built from (`user_prompt_submit`, `assistant_response`, `tool_use`,
`tool_result`, `session_*`, usage events) are never `otel_span` and are
untouched, and every other service and source still fails open. Each drop is
counted durably in the ledger by source and reason and is readable on the
collector's local status endpoint under `otlpAdmission.dropped`
(`reason: "app_server_internal_span"`, alongside the older
`generic_zero_value_span`).

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
