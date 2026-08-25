#!/usr/bin/env bash
# Plimsoll source enrollment script — macOS.
# Clones (or updates) the repo, installs deps, applies the generated telemetry
# config, installs the development LaunchAgent, and runs the strict doctor.
#
# Issue #133: every CLI gate runs in machine mode through the built CLI runtime
# invoked DIRECTLY (node + tsx loader + cli.ts). Package-manager wrappers add
# lifecycle framing around stdout, so the workspace "collector" script is never
# used here; each step's stdout is exactly one versioned JSON receipt, validated
# before branching.
set -euo pipefail

PLIMSOLL_DIR="${PLIMSOLL_DIR:-$HOME/.plimsoll/app}"
REPO_URL="${PLIMSOLL_REPO:-https://github.com/CryptoJym/plimsoll.git}"
DRY_RUN="${PLIMSOLL_INSTALL_DRY_RUN:-0}"

usage() {
  echo "Usage: ./install.sh [--dry-run]"
}

for argument in "$@"; do
  case "$argument" in
    --dry-run) DRY_RUN=1 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $argument" >&2; usage >&2; exit 2 ;;
  esac
done

command -v git >/dev/null || { echo "git is required"; exit 1; }
command -v node >/dev/null || { echo "Node >=20 <25 is required (https://nodejs.org)"; exit 1; }
command -v pnpm >/dev/null || { echo "pnpm is required: corepack enable && corepack prepare pnpm@latest --activate"; exit 1; }

NODE_MAJOR="$(node -p 'Number(process.versions.node.split(".")[0])')"
case "$NODE_MAJOR" in
  ''|*[!0-9]*) echo "Could not determine the Node major version." >&2; exit 1 ;;
esac
if [ "$NODE_MAJOR" -lt 20 ] || [ "$NODE_MAJOR" -ge 25 ]; then
  echo "Unsupported Node $(node -p 'process.versions.node'); Plimsoll requires >=20 <25." >&2
  exit 1
fi

if [ "$DRY_RUN" = "1" ]; then
  cat <<EOF
Plimsoll source install dry-run (no files, config, service, or processes will be changed)
  repository: $REPO_URL
  target: $PLIMSOLL_DIR
  Node: $(node -p 'process.versions.node') (supported: >=20 <25)
  plan: clone/update; pnpm install
  plan: collector runtime invoked directly (node + tsx loader + cli.ts), never via pnpm
  step: machine setup --yes            (one receipt; exit code gates)
  step: machine install-launch-agent --dev --repo-root "$PLIMSOLL_DIR" --pnpm "$(command -v pnpm)" --load
  gate: machine doctor --read-only     (receipt ok:false stops installation)
EOF
  exit 0
fi

if [ -d "$PLIMSOLL_DIR/.git" ]; then
  git -C "$PLIMSOLL_DIR" pull --ff-only
else
  mkdir -p "$(dirname "$PLIMSOLL_DIR")"
  git clone "$REPO_URL" "$PLIMSOLL_DIR"
fi

cd "$PLIMSOLL_DIR"
pnpm install

RUNTIME_NODE="$(command -v node)"
RUNTIME_TSX="$PWD/node_modules/tsx/dist/cli.mjs"
RUNTIME_CLI="$PWD/packages/collector-cli/src/cli.ts"
if [ ! -f "$RUNTIME_TSX" ] || [ ! -f "$RUNTIME_CLI" ]; then
  echo "Built CLI runtime entrypoints not found after pnpm install." >&2
  exit 65
fi
RECEIPT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/plimsoll-install-receipts.XXXXXX")"
trap 'rm -rf "$RECEIPT_DIR"' EXIT

# Run one machine-mode step: capture its single-JSON receipt to a file, validate
# the envelope strictly, and propagate the literal exit code. On failure the
# privacy-redacted receipt is printed to stderr for diagnosis. No output scraping.
machine_step() {
  local step="$1"
  shift
  local receipt_file="$RECEIPT_DIR/$step.json"
  local code=0
  "$RUNTIME_NODE" "$RUNTIME_TSX" "$RUNTIME_CLI" machine "$@" > "$receipt_file" || code=$?
  if [ "$code" -ne 0 ]; then
    cat "$receipt_file" >&2 || true
    echo "Plimsoll $step failed (exit $code); its machine receipt is above." >&2
    exit "$code"
  fi
  "$RUNTIME_NODE" -e '
    const fs = require("fs");
    let raw;
    try { raw = fs.readFileSync(process.argv[1], "utf8"); } catch { process.exit(65); }
    const newlineAt = raw.lastIndexOf("\n");
    if (newlineAt !== raw.length - 1 || newlineAt === -1) process.exit(70);
    if (raw.slice(0, -1).includes("\n")) process.exit(70);
    let receipt;
    try { receipt = JSON.parse(raw.slice(0, -1)); } catch { process.exit(70); }
    if (
      receipt === null || typeof receipt !== "object" || Array.isArray(receipt) ||
      receipt.receiptVersion !== 1 || receipt.schema !== "plimsoll.machine.receipt" ||
      typeof receipt.ok !== "boolean" || !Number.isInteger(receipt.exitCode)
    ) process.exit(70);
    process.exit(receipt.ok && receipt.exitCode === 0 ? 0 : 1);
  ' "$receipt_file" || {
    local validated=$?
    cat "$receipt_file" >&2 || true
    case "$validated" in
      70) echo "Plimsoll $step produced an invalid machine receipt envelope." >&2 ;;
      65) echo "Plimsoll $step receipt file could not be read." >&2 ;;
      *) echo "Plimsoll $step reported failure in its machine receipt (exit $validated)." >&2 ;;
    esac
    exit "$validated"
  }
}

echo
echo "── Configuring Claude Code and Codex telemetry ──────────────"
machine_step setup setup --yes

echo
echo "── Installing collector LaunchAgent ──────────────────────────"
machine_step launch_agent install-launch-agent --dev \
  --repo-root "$PLIMSOLL_DIR" --pnpm "$(command -v pnpm)" --load

echo
echo "── Doctor ─────────────────────────────────────────────────────"
machine_step doctor doctor --read-only

echo
echo "Plimsoll installed. Ledger: ~/Library/Application Support/Plimsoll"
echo "Readiness: signal_verified. Re-run doctor after future config or service changes."
