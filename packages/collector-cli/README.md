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

The repository's lifecycle transaction core now has isolated source proof for
staged update, rollback, preview-default uninstall, separate exact-confirmation
purge, and allowlisted support output. Those primitives are not yet exposed as
a published CLI command. Do not infer package publication or live service
activation from their presence; track
[plimsoll#103](https://github.com/CryptoJym/plimsoll/issues/103).

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

Background mode for the published npm package, plus signed upgrade, rollback,
and uninstall, remains under
[plimsoll#103](https://github.com/CryptoJym/plimsoll/issues/103). Do not infer
that lifecycle from the source tree. Team development canaries use the
deterministic Node 22, exact-commit, two-phase source runbook in the repository
[README](../../README.md#team-mac-source-canary-runbook): apply may finish
truthfully at `service_ready`; only verify after a newly started locally
authenticated agent may claim `signal_verified`. Credentials never move
between machines. That source lane remains blocked from real-canary promotion
until the atomic Claude-config, live source/runtime-attestation, and atomic
LaunchAgent-manifest contracts, plus the single machine-readable CLI receipt,
in
[issues #130](https://github.com/CryptoJym/plimsoll/issues/130),
[#131](https://github.com/CryptoJym/plimsoll/issues/131), and
[#132](https://github.com/CryptoJym/plimsoll/issues/132), and
[#133](https://github.com/CryptoJym/plimsoll/issues/133), plus custom
collector-home isolation
[#135](https://github.com/CryptoJym/plimsoll/issues/135), merge and the
installer is rebased; compatibility parsing or an installer receipt from the
standalone PR is not live proof.

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
