# @plimsoll/cli

**The load line for your AI spend.** Local-first telemetry collector and
dashboard for AI coding agents — Claude Code and Codex today.

Everything runs on your machine. Token usage, cost, sessions, and repos are
captured locally, summed honestly (floors, never guesses), and painted on a
dashboard you can read from the dock.

## Quickstart

```sh
npx @plimsoll/cli setup     # wire Claude Code + Codex telemetry (idempotent, takes backups; --dry-run to preview)
npx @plimsoll/cli start     # run the local collector + dashboard
```

Then open **http://127.0.0.1:48271** — live spend, per-model and per-repo
breakdowns, plan leverage, and capture health.

Check the rigging any time:

```sh
npx @plimsoll/cli doctor --read-only --json
```

Doctor is read-only diagnosis, not installation or capture proof. It creates
no config, ledger, plist, logs, WAL/SHM, or directories. Readiness advances
through `not_installed`, `configured`, `service_ready`, and `signal_verified`;
only the last state has a live matching collector identity plus a real
token-bearing signal, returns `ok:true`, and exits 0.

The lifecycle commands below are real operator surfaces backed by the
transaction core: staged update with automatic rollback, preview-default
uninstall, separate exact-confirmation purge, and allowlisted support output —
proven end to end against the packaged bundle (`pnpm proof:lifecycle-operator`).
They never invoke `launchctl`; loading, unloading, and restart stay explicit
operator steps. npm publication, release signing, and live-fleet rollout
remain gated under
[plimsoll#103](https://github.com/CryptoJym/plimsoll/issues/103).

## Lifecycle

```sh
# Pin the currently running packaged bundle as the immutable runtime, repoint
# the owned LaunchAgent manifest at it, verify durable readiness; any failure
# restores the previous runtime, config, database, and manifest automatically.
npx @plimsoll/cli@<version> lifecycle update --operation-id <id> --artifact self

# Afterwards, restart the daemon on the new immutable runtime explicitly:
npx @plimsoll/cli@<version> load-launch-agent

# Preview (default) or apply removal of ONLY owned targets:
npx @plimsoll/cli@<version> lifecycle uninstall --operation-id <id> [--apply]

# Data deletion is a separate operation requiring the exact confirmation:
npx -y @plimsoll/cli@<version> lifecycle purge --operation-id <id> \
  --apply --confirm-exact "PURGE PLIMSOLL LOCAL DATA"

# Sanitized, bounded diagnostics (versions, readiness, aggregate log codes):
npx @plimsoll/cli@<version> lifecycle support-bundle --operation-id <id>
```

`lifecycle update --artifact self` refuses to run from a source checkout or a
shell shim; it pins only a real packaged bundle. Every operation prints one
JSON receipt naming exactly what it owns, what it retained, and what only a
separate purge may remove.

## Commands

| Command | What it does |
| --- | --- |
| `setup` | Apply Claude Code + Codex telemetry config (idempotent; `--yes`, `--dry-run`) |
| `start` / `stop` | Run / stop the local hook + OTLP receiver |
| `status` | Print local buffer and policy status |
| `doctor --read-only --json` | Verify Node, collector/tool config, LaunchAgent, runtime identity, connectivity, and token signal without writing |
| `install-launch-agent` / `load-launch-agent` | Write the user LaunchAgent plist / load an installed one |
| `uninstall-launch-agent` / `unload-launch-agent` | Remove the plist / unload without removing |
| `lifecycle update\|rollback\|uninstall\|purge\|support-bundle` | Transactional immutable-runtime updates with automatic rollback, preview-default uninstall, exact-confirmation purge, sanitized support bundle |
| `scan-rollouts` | One-time full-history walk of Codex rollout files into the ledger |
| `scan-transcripts` | One-time full-history walk of Claude Code transcripts into the ledger |
| `sync-outcomes --repository owner/repo` | Preview or send the bounded, idempotent session-to-PR outcome join for one named GitHub repository |
| `label account HASH NAME` | Local-only display label for a hashed account |
| `priority add\|remove\|list` | Manage the priority-repo list (hashed; URLs stay local) |
| `purge-local-data` | Dry-run or explicit purge of local buffered events |

Background (LaunchAgent) mode for npm installs is still being fitted — track
[plimsoll#11](https://github.com/CryptoJym/plimsoll/issues/11). For now run
`start` in a terminal or from the git checkout.

## Hosted outcome sync

After joining a workspace, send the local session-to-PR outcome join for one
explicitly named repository. Preview the bounded join first, then repeat
without `--dry-run` to send it:

```sh
npx @plimsoll/cli sync-outcomes --repository owner/repo --dry-run
npx @plimsoll/cli sync-outcomes --repository owner/repo
```

The feed contains merge status, fetched check results, and bounded rework
signals keyed by linkage hashes and deterministic IDs. It does not send PR
titles, bodies, diffs, paths, or branch names. The command is explicit and
stateless; it is not part of the background collector cycle.

## What leaves your machine

Nothing, unless you configure an upload target. Identifying values are
hashed at capture; human-readable labels (repo names, account emails) live
in local-only tables and are **structurally excluded from uploads** — a rule
enforced by the proof suite that runs on every PR.

Managed or upload-enabled collectors support `metadata_only` mode. Raw
evidence mode is rejected before setup, join, config write, or collector
start; it is never silently downgraded. Legacy `evidence` rows are held in a
local quarantine and never uploaded. They require an explicit future
migration; the collector does not inspect, migrate, or delete them
automatically. The separately reviewed encrypted evidence vault is not
implemented.

The full privacy posture, capture format, and proof checks are open and
inspectable: **https://github.com/CryptoJym/plimsoll**

## License

Apache-2.0
