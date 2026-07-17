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

## Commands

| Command | What it does |
| --- | --- |
| `setup` | Apply Claude Code + Codex telemetry config (idempotent; `--yes`, `--dry-run`) |
| `start` / `stop` | Run / stop the local hook + OTLP receiver |
| `status` | Print local buffer and policy status |
| `doctor --read-only --json` | Verify Node, collector/tool config, LaunchAgent, runtime identity, connectivity, and token signal without writing |
| `scan-rollouts` | One-time full-history walk of Codex rollout files into the ledger |
| `scan-transcripts` | One-time full-history walk of Claude Code transcripts into the ledger |
| `label account HASH NAME` | Local-only display label for a hashed account |
| `priority add\|remove\|list` | Manage the priority-repo list (hashed; URLs stay local) |
| `purge-local-data` | Dry-run or explicit purge of local buffered events |

Background (LaunchAgent) mode for npm installs is still being fitted — track
[plimsoll#11](https://github.com/CryptoJym/plimsoll/issues/11). For now run
`start` in a terminal or from the git checkout.

## What leaves your machine

Nothing, unless you configure an upload target. Identifying values are
hashed at capture; human-readable labels (repo names, account emails) live
in local-only tables and are **structurally excluded from uploads** — a rule
enforced by the proof suite that runs on every PR.

The full privacy posture, capture format, and proof checks are open and
inspectable: **https://github.com/CryptoJym/plimsoll**

## License

Apache-2.0
