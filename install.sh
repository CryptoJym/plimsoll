#!/usr/bin/env bash
# Plimsoll source enrollment script — macOS and Linux.
# Clones (or updates) the repo, installs deps, applies the generated telemetry
# config, installs the development daemon, and runs the strict doctor.
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

PLIMSOLL_PLATFORM="$(uname -s)"
case "$PLIMSOLL_PLATFORM" in
  Darwin)
    PLIMSOLL_DAEMON_LABEL="LaunchAgent"
    PLIMSOLL_LEDGER_PATH="$HOME/Library/Application Support/Plimsoll"
    ;;
  Linux)
    command -v systemctl >/dev/null || { echo "systemctl is required for the Linux user daemon"; exit 1; }
    PLIMSOLL_DAEMON_LABEL="systemd user service"
    if [[ "${XDG_DATA_HOME:-}" == /* ]]; then
      PLIMSOLL_LEDGER_PATH="$XDG_DATA_HOME/plimsoll"
    else
      PLIMSOLL_LEDGER_PATH="$HOME/.local/share/plimsoll"
    fi
    ;;
  *)
    echo "Unsupported platform: $PLIMSOLL_PLATFORM. Plimsoll supports macOS and Linux." >&2
    exit 1
    ;;
esac

if [ -n "${PLIMSOLL_HOME:-}" ]; then
  PLIMSOLL_LEDGER_PATH="$PLIMSOLL_HOME"
fi

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
  platform: $PLIMSOLL_PLATFORM
  Node: $(node -p 'process.versions.node') (supported: >=20 <25)
  plan: clone/update; pnpm install; collector setup --yes
  plan: collector install-daemon --dev --repo-root "$PLIMSOLL_DIR" --pnpm "$(command -v pnpm)" --load
  gate: collector doctor --read-only --json (failure stops installation)
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

echo
echo "── Configuring Claude Code and Codex telemetry ──────────────"
pnpm collector setup --yes

echo
echo "── Installing collector $PLIMSOLL_DAEMON_LABEL ───────────────"
pnpm collector install-daemon --dev --repo-root "$PLIMSOLL_DIR" --pnpm "$(command -v pnpm)" --load

echo
echo "── Doctor ─────────────────────────────────────────────────────"
pnpm collector doctor --read-only --json

echo
echo "Plimsoll installed. Ledger: $PLIMSOLL_LEDGER_PATH"
echo "Readiness: signal_verified. Re-run doctor after future config or service changes."
