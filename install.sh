#!/usr/bin/env bash
# Plimsoll enrollment script — macOS.
# Clones (or updates) the repo, installs deps, installs the LaunchAgent,
# and prints the tool-config snippets to add to Claude Code / Codex.
set -euo pipefail

PLIMSOLL_DIR="${PLIMSOLL_DIR:-$HOME/.plimsoll/app}"
REPO_URL="${PLIMSOLL_REPO:-https://github.com/CryptoJym/plimsoll.git}"

command -v git >/dev/null || { echo "git is required"; exit 1; }
command -v node >/dev/null || { echo "Node 20+ is required (https://nodejs.org)"; exit 1; }
command -v pnpm >/dev/null || { echo "pnpm is required: corepack enable && corepack prepare pnpm@latest --activate"; exit 1; }

if [ -d "$PLIMSOLL_DIR/.git" ]; then
  git -C "$PLIMSOLL_DIR" pull --ff-only
else
  mkdir -p "$(dirname "$PLIMSOLL_DIR")"
  git clone "$REPO_URL" "$PLIMSOLL_DIR"
fi

cd "$PLIMSOLL_DIR"
pnpm install

echo
echo "── Installing collector LaunchAgent ──────────────────────────"
pnpm collector install-launch-agent --repo-root "$PLIMSOLL_DIR" --pnpm "$(command -v pnpm)" --load

echo
echo "── Doctor ─────────────────────────────────────────────────────"
pnpm collector doctor || true

echo
echo "── Add these to your AI tools ────────────────────────────────"
pnpm collector generate-config all

echo
echo "Plimsoll installed. Ledger: ~/Library/Application Support/Plimsoll"
echo "Verify capture after your next session:  cd $PLIMSOLL_DIR && pnpm collector status"
