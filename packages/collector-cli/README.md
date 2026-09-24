# @plimsoll/cli

**The load line for your AI spend.** Local-first telemetry collector and
dashboard for AI coding agents — Claude Code and Codex today.

Everything runs on your machine. Token usage, cost, sessions, and repos are
captured locally, summed honestly (floors, never guesses), and painted on a
dashboard you can read from the dock.

## Quickstart

```sh
npx @plimsoll/cli setup     # wire Claude Code + Gemini CLI + Grok + Codex telemetry (idempotent; --dry-run to preview)
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

# Update snapshots and runtimes: list, preview (default) or remove what
# retention does not keep; check disk before stopping the service:
npx @plimsoll/cli@<version> lifecycle snapshots list
npx @plimsoll/cli@<version> lifecycle snapshots prune [--keep N] [--apply]
npx @plimsoll/cli@<version> lifecycle update --preflight
```

`lifecycle update --artifact self` refuses to run from a source checkout or a
shell shim; it pins only a real packaged bundle. It refuses while any other
process has the ledger open, so stop the collector first. With the collector
stopped on APFS, its ledger snapshot is a clone that costs no disk when taken;
every completed update keeps only the two newest completed snapshots and the
runtimes they restore (see `docs/local-lifecycle.md`). Every operation prints one
JSON receipt naming exactly what it owns, what it retained, and what only a
separate purge may remove.

## Commands

| Command | What it does |
| --- | --- |
| `setup` | Apply Claude Code, Gemini CLI, Grok, and Codex telemetry config (idempotent; `--yes`, `--dry-run`) |
| `start` / `stop` | Run / stop the local hook + OTLP receiver |
| `status` | Print local buffer and policy status |
| `doctor --read-only --json` | Verify Node, collector/tool config, LaunchAgent, runtime identity, connectivity, and token signal without writing |
| `install-launch-agent` / `load-launch-agent` | Write the user LaunchAgent plist / load an installed one |
| `uninstall-launch-agent` / `unload-launch-agent` | Remove the plist / unload without removing |
| `lifecycle update\|rollback\|uninstall\|purge\|support-bundle\|snapshots` | Transactional immutable-runtime updates with automatic rollback and bounded snapshot retention, preview-default uninstall, exact-confirmation purge, sanitized support bundle, snapshot list/prune |
| `scan-rollouts` | One-time full-history walk of Codex rollout files into the ledger |
| `scan-transcripts` | One-time full-history walk of Claude Code transcripts into the ledger |
| `sync-outcomes --repository owner/repo` | Send pull request outcomes for one named GitHub repository to the joined workspace (`--dry-run` previews) |
| `label account HASH NAME` | Local-only display label for a hashed account |
| `priority add\|remove\|list` | Manage the priority-repo list (hashed; URLs stay local) |
| `purge-local-data` | Dry-run or explicit purge of local buffered events |

Background (LaunchAgent) mode for npm installs is still being fitted — track
[plimsoll#11](https://github.com/CryptoJym/plimsoll/issues/11). For now run
`start` in a terminal or from the git checkout.

Before opening the HTTP listener, `start` makes one bounded
`wal_checkpoint(TRUNCATE)` attempt when `work-ledger.sqlite-wal` is larger
than `startupWalCheckpointBytes` (default 1 GiB), and emits a structured
before/after receipt. A checkpoint can truncate the WAL only when no other
process retains a conflicting SQLite reader or writer; maintenance orphan
recovery is what prevents an abandoned worker from defeating later attempts.

## Outcome sync

`sync-outcomes` sends the joined workspace what happened to the pull requests
your local sessions worked on (merge status, check results, reverts and
reopens) for one GitHub repository you name. It never runs in the background,
and running it again updates the same rows instead of adding new ones.

```sh
npx @plimsoll/cli sync-outcomes --repository owner/repo --dry-run   # preview
npx @plimsoll/cli sync-outcomes --repository owner/repo
```

`--repository` is the GitHub `owner/repo` from the repository URL. Spaces
around it and letter case do not matter: ` Acme/Widgets ` and `acme/widgets`
are the same repository. Both parts follow GitHub's naming rules in plain
ASCII: the owner is letters, digits and single hyphens (up to 39 characters,
or a managed user's `name_SHORTCODE`), and the repository name is letters,
digits, `.`, `-` and `_` (up to 100). Anything else, such as a URL, a third
path segment or a look-alike Unicode character, is refused before any
request. Some older GitHub accounts and organizations have names that start
or end with a hyphen or contain two hyphens in a row. GitHub no longer allows
such names and `sync-outcomes` does not support them: it refuses them with a
message that says so. Rename the account or organization on GitHub, or move
the repository to an owner with a current name, then run it again. For a
private repository, set
`GITHUB_TOKEN` or `GH_TOKEN`. The owner and name are sent to the workspace;
pull request titles, bodies, diffs and file paths are not, and branch names
travel only as hashes.

## Upload URL overrides

`upload`, `upload-history` (including `--sessions` and `--repair-attribution`),
`push-repo-labels` and `sync-outcomes` accept `--url` to send to another path
on the joined workspace. Every request carries that workspace's install key
and signature, so a `--url` on any other origin is refused before anything is
sent. Without a joined workspace, `--url` is refused as well.

For local development against a test server on this machine, add
`--dev-loopback-url` to that one command. It allows only a plainly written
`http://` or `https://` URL on `localhost`, `127.x.x.x` or `[::1]`, and
refuses user info, look-alike or encoded host names, and other names that
merely resolve to this machine. Every use prints a warning to stderr and a
`development_upload_url_used` line in the command's output. It is a
command-line flag, not a setting, so it never carries over to other commands
or processes.

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
