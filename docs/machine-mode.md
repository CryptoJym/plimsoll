# Machine mode (`plimsoll machine …`) — automation contract

Issue #133. One explicit automation surface over the supported operator commands.
Installers and fleet tooling invoke the real built CLI runtime directly and receive
exactly one schema-validated JSON receipt whose exit code and fields unambiguously
describe the literal result.

## Invocation

```
node ./node_modules/tsx/dist/cli.mjs packages/collector-cli/src/cli.ts machine <command> [flags…]
```

(When running a packaged build, substitute the built entry for the tsx runner + source pair;
the wrapper re-spawns `process.execPath` + `process.execArgv` + the entry script, so the
inner command runs under the identical runtime.)

Supported commands and allowed flags:

| Command | Allowed flags |
|---|---|
| `setup` | `--yes`, `--dry-run`, `--claude-settings <path>`, `--codex-config <path>` |
| `doctor` | `--read-only`, `--json` |
| `install-launch-agent` | `--dev`, `--repo-root <path>`, `--pnpm <path>`, `--load`, `--dry-run` |
| `load-launch-agent` | — |
| `unload-launch-agent` | — |
| `start` | — |
| `status` | — |
| `stop` | — |

Anything else is a usage error (exit `64`) reported in its own valid receipt.

## Receipt envelope (version 1)

`receiptVersion` is `1` and `schema` is `"plimsoll.machine.receipt"`. The authoritative
definition is `machineReceiptSchema` in `packages/collector-cli/src/machine.ts`
(zod-validated before emission; if validation ever fails, a minimal fallback receipt with
exit code `71` is emitted instead).

```json
{
  "receiptVersion": 1,
  "schema": "plimsoll.machine.receipt",
  "command": "doctor",
  "argv": ["--read-only"],
  "ok": false,
  "exitCode": 1,
  "signal": null,
  "result": { "…the inner command's own JSON output, privacy-redacted…": null },
  "stdout": { "parse": "single_json_object", "bytes": 3477, "sha256": "…" },
  "stderr": { "bytes": 0, "sha256": null, "forwardedSanitizedChars": 0, "truncated": false },
  "wrapperError": null
}
```

Fields:

- `ok` — true iff `exitCode === 0` and no wrapper-level failure occurred.
- `result` — the child command's own JSON object with every string passed through the
  sanitizer and values under path-bearing keys (`*Path`, `repoRoot`, `homePath`, …)
  replaced by deterministic sha256 digests. No local paths, config values, hook text,
  prompts, credentials, or environment values appear anywhere in the receipt.
- `stdout.parse` — `single_json_object`, `empty`, `invalid`, or `not_captured`
  (only `start`, which runs detached, uses `not_captured`).
- `stdout` / `stderr` — byte counts and sha256 digests of the raw child streams.
  Raw stream content is never embedded.
- `wrapperError` — bounded, content-free reason for wrapper-level failures.

## Stdout guarantee

Machine-mode stdout is **exactly one JSON object followed by exactly one trailing
newline** (`\n`). No banners, progress, logs, warnings, ANSI escapes, or human text are
emitted to stdout in machine mode — including on failures, usage errors, and crashes of
the inner command.

## Exit-code contract

| Exit code | Meaning |
|---|---|
| `0` | `ok: true`; the command completed with its success postcondition |
| child's own code (typically `1`) | The command ran and reported failure; `result` describes it |
| `64` | Invalid machine invocation (unsupported command, unknown flag, bad bounds) |
| `65` | The child runtime could not be spawned at all |
| `70` | Protocol violation: the child exited `0` but stdout was not exactly one JSON object |
| `71` | The wrapper could not emit a schema-valid receipt (fallback receipt present) |
| `128+N` | The child was killed by signal N (e.g. `143` = SIGTERM) |

## Diagnostics channel

Child stderr that cannot enter the receipt is forwarded to **machine-mode stderr**:
bounded to 6000 characters, scrubbed of ANSI escapes and control characters, with the
absolute bases (home, cwd, tmpdir, `PLIMSOLL_HOME`) and any secret-shaped environment
values replaced by `<redacted>`. Child stdout content is never forwarded at all.

## Installer contract

The installer invokes this exact runtime directly (never `pnpm collector …`: package
managers add lifecycle framing around stdout that corrupts single-value parsing), captures
machine-mode stdout into a file, validates the envelope (`receipt === 1`, boolean `ok`),
and branches on `exitCode`. It never scrapes a last line and never searches mixed output
for JSON.

## Notes

- Human commands (`plimsoll setup`, `doctor --json`, …) are unchanged; machine mode is an
  additive wrapper.
- `machine start` launches the foreground daemon detached so automation gets a bounded,
  truthful postcondition receipt (`started` / `alreadyRunning` / failure reason) instead
  of blocking forever; deadline default 20 s, override with
  `PLIMSOLL_MACHINE_START_TIMEOUT_MS` (1000–120000 ms).
