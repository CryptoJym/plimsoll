#!/usr/bin/env bash
# Deterministic, two-phase Plimsoll source installer for supported team Macs.
#
# This installer intentionally does not read, copy, export, or enroll any
# credentials. Each teammate starts a new locally authenticated agent session
# between `apply` and `verify` so the already-running agent can pick up the
# telemetry configuration written by setup.
set -uo pipefail

PLIMSOLL_DIR="${PLIMSOLL_DIR:-$HOME/.plimsoll/app}"
REPO_URL="${PLIMSOLL_REPO:-https://github.com/CryptoJym/plimsoll.git}"
SOURCE_REF="${PLIMSOLL_SOURCE_REF:-}"
NODE_REQUEST="${PLIMSOLL_NODE:-}"
PNPM_REQUEST="${PLIMSOLL_PNPM:-}"
MODE="apply"
DEFAULT_COLLECTOR_HOME="$HOME/Library/Application Support/Plimsoll"
REQUESTED_COLLECTOR_HOME="${PLIMSOLL_HOME:-$DEFAULT_COLLECTOR_HOME}"
LAUNCH_AGENT_PLIST="$HOME/Library/LaunchAgents/com.plimsoll.collector.plist"
case "$REPO_URL" in
  https://github.com/CryptoJym/plimsoll.git|git@github.com:CryptoJym/plimsoll.git)
    REPO_PROVENANCE="canonical_public_repository"
    ;;
  *) REPO_PROVENANCE="operator_supplied_remote" ;;
esac

NODE_BIN=""
NODE_VERSION="unknown"
PNPM_BIN=""
PNPM_VERSION="not_run"
ARCHITECTURE="unknown"
SOURCE_VERIFIED="false"
CHECKOUT_RESULT="not_started"
DEPENDENCIES_RESULT="not_started"
CONFIG_RESULT="not_started"
SERVICE_RESULT="not_started"
READINESS="not_checked"
ERROR_STAGE="none"
RECEIPT_EMITTED=0
INSTALL_LOCK=""
LOCK_HELD=0
LOCK_NONCE=""
INSTALL_IDENTITY_PATH="$PLIMSOLL_DIR/.git/plimsoll-source-install.v1.json"
INSTALLED_IDENTITY_MATCHED="false"
SERVICE_LOADED_BY_INSTALLER=0
DOCTOR_TOKEN_EVENTS="unknown"
DOCTOR_RUNTIME_HASH="unknown"
BASELINE_TOKEN_EVENTS="unknown"
BASELINE_RUNTIME_HASH="unknown"
LOCAL_ONLY_CONFIRMED="false"

usage() {
  cat <<'EOF'
Usage:
  ./install.sh --dry-run --ref <40-character-commit> [--node PATH] [--pnpm PATH]
  ./install.sh apply     --ref <40-character-commit> [--node PATH] [--pnpm PATH]
  ./install.sh verify    --ref <40-character-commit> [--node PATH] [--pnpm PATH]

Modes:
  --dry-run  Validate immutable-input syntax and select absolute runtime paths
             without executing pnpm or writing.
  apply      Pin the checkout, frozen-install, reconcile config, and reach
             service_ready (the default mode).
  verify     Read-only gate requiring a real token signal from a newly started,
             locally authenticated Codex or Claude session.

Environment equivalents:
  PLIMSOLL_SOURCE_REF, PLIMSOLL_NODE, PLIMSOLL_PNPM, PLIMSOLL_DIR,
  PLIMSOLL_REPO

The source installer supports Node 22 only. It does not implement or claim
package upgrade, rollback, uninstall, hosted enrollment, or credential setup.
EOF
}

need_value() {
  if [ "$#" -lt 2 ] || [ -z "$2" ]; then
    echo "Missing value for $1." >&2
    usage >&2
    exit 2
  fi
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    apply) MODE="apply" ;;
    verify|--verify) MODE="verify" ;;
    --dry-run) MODE="dry-run" ;;
    --ref)
      need_value "$@"
      SOURCE_REF="$2"
      shift
      ;;
    --node)
      need_value "$@"
      NODE_REQUEST="$2"
      shift
      ;;
    --pnpm)
      need_value "$@"
      PNPM_REQUEST="$2"
      shift
      ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

case "$SOURCE_REF" in
  [0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]) ;;
  "")
    echo "An explicit immutable source commit is required: --ref <40-character-commit>." >&2
    exit 2
    ;;
  *)
    echo "Mutable or ambiguous source ref refused. Use the full 40-character commit SHA." >&2
    exit 2
    ;;
esac
SOURCE_REF="$(printf '%s' "$SOURCE_REF" | tr 'A-F' 'a-f')"

absolute_executable() {
  candidate="$1"
  [ -n "$candidate" ] || return 1
  case "$candidate" in
    /*) ;;
    *) candidate="$(command -v "$candidate" 2>/dev/null || true)" ;;
  esac
  case "$candidate" in
    /*) [ -f "$candidate" ] && [ -x "$candidate" ] || return 1 ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$candidate"
}

node_is_22() {
  candidate="$1"
  [ -x "$candidate" ] || return 1
  major="$($candidate -p 'Number(process.versions.node.split(".")[0])' 2>/dev/null || true)"
  [ "$major" = "22" ]
}

select_node() {
  if [ -n "$NODE_REQUEST" ]; then
    NODE_BIN="$(absolute_executable "$NODE_REQUEST" || true)"
    if [ -z "$NODE_BIN" ]; then
      echo "The requested Node executable is not an absolute executable path." >&2
      return 1
    fi
    if ! node_is_22 "$NODE_BIN"; then
      version="$($NODE_BIN -p 'process.versions.node' 2>/dev/null || printf 'unknown')"
      echo "Unsupported Node $version at the selected executable; source canaries require Node 22." >&2
      return 1
    fi
  else
    default_node="$(command -v node 2>/dev/null || true)"
    for candidate in \
      "$default_node" \
      "$HOME"/.nvm/versions/node/v22*/bin/node \
      "$HOME"/.volta/bin/node \
      /opt/homebrew/opt/node@22/bin/node \
      /usr/local/opt/node@22/bin/node
    do
      case "$candidate" in *\**) continue ;; esac
      resolved="$(absolute_executable "$candidate" || true)"
      if [ -n "$resolved" ] && node_is_22 "$resolved"; then
        NODE_BIN="$resolved"
        break
      fi
    done
    if [ -z "$NODE_BIN" ]; then
      echo "Node 22 was not found. Pass its absolute executable with --node /absolute/path/to/node." >&2
      return 1
    fi
  fi
  NODE_VERSION="$($NODE_BIN -p 'process.versions.node')"
}

select_pnpm() {
  candidate="$PNPM_REQUEST"
  if [ -z "$candidate" ]; then
    candidate="$(command -v pnpm 2>/dev/null || true)"
  fi
  if [ -z "$candidate" ] && [ -x "$HOME/Library/pnpm/pnpm" ]; then
    candidate="$HOME/Library/pnpm/pnpm"
  fi
  PNPM_BIN="$(absolute_executable "$candidate" || true)"
  if [ -z "$PNPM_BIN" ]; then
    echo "pnpm was not found as an absolute executable. Pass --pnpm /absolute/path/to/pnpm." >&2
    return 1
  fi
}

select_node || exit 1
select_pnpm || exit 1

if [ "$REQUESTED_COLLECTOR_HOME" != "$DEFAULT_COLLECTOR_HOME" ]; then
  echo "Custom PLIMSOLL_HOME is not supported by the source LaunchAgent installer. Use the default per-user Plimsoll home." >&2
  exit 1
fi

case "$PLIMSOLL_DIR" in
  /*) ;;
  *) echo "PLIMSOLL_DIR must be an absolute path." >&2; exit 1 ;;
esac
PATH_INPUTS="$PLIMSOLL_DIR\n$NODE_BIN\n$PNPM_BIN" "$NODE_BIN" -e '
  if (/[\u0000-\u001f\u007f-\u009f]/.test(process.env.PATH_INPUTS ?? "")) process.exit(1);
' || { echo "Target and runtime paths must not contain control characters." >&2; exit 1; }

SYSTEM_NAME="$(uname -s 2>/dev/null || true)"
ARCHITECTURE="$(uname -m 2>/dev/null || true)"
if [ "$SYSTEM_NAME" != "Darwin" ]; then
  echo "The source canary installer supports macOS only." >&2
  exit 1
fi
case "$ARCHITECTURE" in
  arm64|x86_64) ;;
  *) echo "Unsupported macOS architecture: $ARCHITECTURE." >&2; exit 1 ;;
esac

node_dir="$(dirname "$NODE_BIN")"
pnpm_dir="$(dirname "$PNPM_BIN")"
RUNTIME_PATH="$node_dir:$pnpm_dir:${PATH:-/usr/bin:/bin:/usr/sbin:/sbin}"

emit_receipt() {
  state="$1"
  RECEIPT_EMITTED=1
  RECEIPT_STATE="$state" \
  RECEIPT_MODE="$MODE" \
  RECEIPT_SHA="$SOURCE_REF" \
  RECEIPT_REPO_PROVENANCE="$REPO_PROVENANCE" \
  RECEIPT_SOURCE_VERIFIED="$SOURCE_VERIFIED" \
  RECEIPT_NODE_VERSION="$NODE_VERSION" \
  RECEIPT_PNPM_VERSION="$PNPM_VERSION" \
  RECEIPT_ARCH="$ARCHITECTURE" \
  RECEIPT_CHECKOUT="$CHECKOUT_RESULT" \
  RECEIPT_DEPENDENCIES="$DEPENDENCIES_RESULT" \
  RECEIPT_CONFIG="$CONFIG_RESULT" \
  RECEIPT_SERVICE="$SERVICE_RESULT" \
  RECEIPT_READINESS="$READINESS" \
  RECEIPT_ERROR_STAGE="$ERROR_STAGE" \
  RECEIPT_INSTALLED_IDENTITY_MATCHED="$INSTALLED_IDENTITY_MATCHED" \
  RECEIPT_LOCAL_ONLY_CONFIRMED="$LOCAL_ONLY_CONFIRMED" \
  "$NODE_BIN" -e '
    const env = process.env;
    const state = env.RECEIPT_STATE;
    const readiness = env.RECEIPT_READINESS;
    const nextAction = state === "service_ready"
      ? "restart_a_locally_authenticated_agent_then_run_verify"
      : state === "signal_verified"
        ? "none"
        : env.RECEIPT_MODE === "verify" && env.RECEIPT_READINESS === "service_ready"
          ? "restart_a_locally_authenticated_agent_then_run_verify"
        : state === "plan_selected_no_execution"
          ? "run_apply_with_the_same_exact_commit"
          : "inspect_retained_state_then_resume_apply";
    console.log(JSON.stringify({
      schemaVersion: 1,
      operation: "source_canary_install",
      mode: env.RECEIPT_MODE,
      state,
      source: {
        commit: env.RECEIPT_SHA,
        input: "full_commit_sha",
        provenance: env.RECEIPT_REPO_PROVENANCE,
        remoteObjectVerified: env.RECEIPT_SOURCE_VERIFIED === "true",
        localCheckoutMatched: ["pinned_exact", "existing_exact"].includes(env.RECEIPT_CHECKOUT),
      },
      runtime: {
        nodeVersion: env.RECEIPT_NODE_VERSION,
        nodeMajor: 22,
        pnpmVersion: env.RECEIPT_PNPM_VERSION,
        architecture: env.RECEIPT_ARCH,
        absoluteExecutablesSelected: true,
        identityBoundByDoctor: ["service_ready", "signal_verified"].includes(readiness),
        installedIdentityMatched: env.RECEIPT_INSTALLED_IDENTITY_MATCHED === "true",
      },
      retainedState: {
        checkout: env.RECEIPT_CHECKOUT,
        dependencies: env.RECEIPT_DEPENDENCIES,
        toolConfig: env.RECEIPT_CONFIG,
        service: env.RECEIPT_SERVICE,
        ledger: "not_inspected_or_modified_directly",
      },
      readiness,
      localOnlyRequired: true,
      localOnlyConfirmed: env.RECEIPT_LOCAL_ONLY_CONFIRMED === "true",
      hostedEnrollmentPerformed: false,
      credentialOperations: 0,
      rollbackClaimed: false,
      errorStage: env.RECEIPT_ERROR_STAGE,
      nextAction,
    }));
  '
}

stop_loaded_service_after_failure() {
  if [ "$SERVICE_LOADED_BY_INSTALLER" != "1" ]; then return 0; fi
  if PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector unload-launch-agent >/dev/null 2>&1; then
    SERVICE_RESULT="unload_succeeded_stop_unverified"
  else
    SERVICE_RESULT="unload_failed_stop_unverified"
  fi
  SERVICE_LOADED_BY_INSTALLER=0
}

fail() {
  ERROR_STAGE="$1"
  code="${2:-1}"
  stop_loaded_service_after_failure
  echo "Source install stopped at $ERROR_STAGE. Retained state is reported below; no rollback or uninstall is claimed." >&2
  emit_receipt "failed"
  exit "$code"
}

interrupted() {
  ERROR_STAGE="interrupted"
  stop_loaded_service_after_failure
  echo "Source install interrupted. Re-run apply with the same exact commit to resume." >&2
  emit_receipt "interrupted"
  exit 130
}

cleanup_lock() {
  if [ "$LOCK_HELD" != "1" ] || [ -z "$LOCK_NONCE" ]; then return 0; fi
  LOCK_PATH="$INSTALL_LOCK" LOCK_PID="$$" LOCK_NONCE_VALUE="$LOCK_NONCE" "$NODE_BIN" -e '
    const fs = require("node:fs");
    const lock = process.env.LOCK_PATH;
    try {
      if (lock !== `/private/tmp/com.plimsoll.source-install.${process.getuid()}.lock`) process.exit(0);
      const before = fs.lstatSync(lock);
      if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1 || before.uid !== process.getuid()) process.exit(0);
      const descriptor = fs.openSync(lock, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      let value;
      try {
        const opened = fs.fstatSync(descriptor);
        if (opened.dev !== before.dev || opened.ino !== before.ino || opened.size > 4096) process.exit(0);
        value = JSON.parse(fs.readFileSync(descriptor, "utf8"));
      } finally { fs.closeSync(descriptor); }
      if (value?.version !== 1 || value.pid !== Number(process.env.LOCK_PID) ||
          value.ownerNonce !== process.env.LOCK_NONCE_VALUE) process.exit(0);
      const visible = fs.lstatSync(lock);
      if (visible.dev !== before.dev || visible.ino !== before.ino || visible.isSymbolicLink()) process.exit(0);
      fs.unlinkSync(lock);
    } catch (error) {
      if (error?.code !== "ENOENT") process.exit(0);
    }
  ' >/dev/null 2>&1 || true
  LOCK_HELD=0
  LOCK_NONCE=""
}

prepare_default_collector_home() {
  SECURE_HOME="$HOME" SECURE_TARGET="$DEFAULT_COLLECTOR_HOME" "$NODE_BIN" -e '
    const fs = require("node:fs");
    const path = require("node:path");
    const home = path.resolve(process.env.SECURE_HOME);
    const target = path.resolve(process.env.SECURE_TARGET);
    if (target !== path.join(home, "Library", "Application Support", "Plimsoll")) process.exit(1);
    const ensureDirectory = (directory, mode, create) => {
      try {
        const stat = fs.lstatSync(directory);
        if (!stat.isDirectory() || stat.isSymbolicLink()) process.exit(1);
      } catch (error) {
        if (error?.code !== "ENOENT" || !create) process.exit(1);
        fs.mkdirSync(directory, { mode });
        const stat = fs.lstatSync(directory);
        if (!stat.isDirectory() || stat.isSymbolicLink()) process.exit(1);
      }
    };
    ensureDirectory(home, 0o700, false);
    ensureDirectory(path.join(home, "Library"), 0o700, true);
    ensureDirectory(path.join(home, "Library", "Application Support"), 0o700, true);
    ensureDirectory(target, 0o700, true);
    const descriptor = fs.openSync(target, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    try {
      const stat = fs.fstatSync(descriptor);
      if (!stat.isDirectory()) process.exit(1);
      fs.fchmodSync(descriptor, 0o700);
      fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
  ' >/dev/null 2>&1
}

acquire_lock() {
  lock_uid="$($NODE_BIN -p 'process.getuid()' 2>/dev/null || true)"
  case "$lock_uid" in ''|*[!0-9]*) fail "install_lock_identity" 1 ;; esac
  INSTALL_LOCK="/private/tmp/com.plimsoll.source-install.${lock_uid}.lock"
  LOCK_NONCE="$(LOCK_PATH="$INSTALL_LOCK" LOCK_PID="$$" "$NODE_BIN" -e '
    const crypto = require("node:crypto");
    const fs = require("node:fs");
    const path = require("node:path");
    const { spawnSync } = require("node:child_process");
    const lock = process.env.LOCK_PATH;
    const ownerPid = Number(process.env.LOCK_PID);
    const expectedLock = `/private/tmp/com.plimsoll.source-install.${process.getuid()}.lock`;
    if (lock !== expectedLock) process.exit(74);
    for (const directory of ["/private", "/private/tmp"]) {
      const stat = fs.lstatSync(directory);
      if (!stat.isDirectory() || stat.isSymbolicLink()) process.exit(74);
    }
    const fingerprint = pid => {
      const result = spawnSync("/bin/ps", ["-p", String(pid), "-o", "lstart="], {
        encoding: "utf8",
        env: { LANG: "C", LC_ALL: "C", PATH: "/usr/bin:/bin" },
        stdio: ["ignore", "pipe", "ignore"],
        timeout: 2000,
      });
      if (result.status !== 0 || !result.stdout.trim()) return null;
      return `sha256:${crypto.createHash("sha256").update(result.stdout.trim()).digest("hex")}`;
    };
    const ownerStartFingerprint = fingerprint(ownerPid);
    if (!Number.isSafeInteger(ownerPid) || ownerPid <= 1 || !ownerStartFingerprint) process.exit(74);
    const ownerNonce = crypto.randomUUID();
    const payload = `${JSON.stringify({
      version: 1,
      pid: ownerPid,
      processStartFingerprint: ownerStartFingerprint,
      ownerNonce,
    })}\n`;
    const syncParent = () => {
      const directory = fs.openSync(path.dirname(lock), fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    };
    const create = () => {
      const descriptor = fs.openSync(
        lock,
        fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
        0o600,
      );
      try {
        fs.writeFileSync(descriptor, payload, "utf8");
        fs.fsyncSync(descriptor);
      } finally { fs.closeSync(descriptor); }
      syncParent();
      fs.writeSync(1, ownerNonce);
    };
    try { create(); process.exit(0); } catch (error) {
      if (error?.code !== "EEXIST") process.exit(74);
    }
    let before;
    let value;
    try {
      before = fs.lstatSync(lock);
      if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1 || before.uid !== process.getuid() ||
          (before.mode & 0o077) !== 0 || before.size > 4096) process.exit(74);
      const descriptor = fs.openSync(lock, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      try {
        const opened = fs.fstatSync(descriptor);
        if (opened.dev !== before.dev || opened.ino !== before.ino) process.exit(75);
        value = JSON.parse(fs.readFileSync(descriptor, "utf8"));
      } finally { fs.closeSync(descriptor); }
    } catch { process.exit(74); }
    if (Object.keys(value).sort().join(",") !== "ownerNonce,pid,processStartFingerprint,version" ||
        value.version !== 1 || !Number.isSafeInteger(value.pid) || value.pid <= 1 ||
        typeof value.ownerNonce !== "string" || !/^[0-9a-f-]{36}$/i.test(value.ownerNonce) ||
        !/^sha256:[0-9a-f]{64}$/.test(value.processStartFingerprint)) process.exit(74);
    let live = false;
    try { process.kill(value.pid, 0); live = true; } catch (error) {
      if (error?.code === "EPERM") process.exit(74);
      if (error?.code !== "ESRCH") process.exit(74);
    }
    if (live) {
      const currentFingerprint = fingerprint(value.pid);
      if (!currentFingerprint) process.exit(74);
      if (currentFingerprint === value.processStartFingerprint) process.exit(73);
    }
    try {
      const visible = fs.lstatSync(lock);
      if (visible.dev !== before.dev || visible.ino !== before.ino || visible.isSymbolicLink()) process.exit(75);
      fs.unlinkSync(lock);
      syncParent();
      create();
    } catch { process.exit(75); }
  ' 2>/dev/null)"
  lock_status=$?
  case "$lock_status" in
    0) LOCK_HELD=1 ;;
    73) fail "install_already_running" 1 ;;
    75) fail "install_lock_race" 1 ;;
    *) fail "install_lock_conflict" 1 ;;
  esac
  prepare_default_collector_home || fail "collector_home_parent" 1
}

prepare_launch_agent_destination() {
  SECURE_HOME="$HOME" SECURE_PLIST="$LAUNCH_AGENT_PLIST" "$NODE_BIN" -e '
    const fs = require("node:fs");
    const path = require("node:path");
    const home = path.resolve(process.env.SECURE_HOME);
    const plist = path.resolve(process.env.SECURE_PLIST);
    const launchAgents = path.join(home, "Library", "LaunchAgents");
    if (plist !== path.join(launchAgents, "com.plimsoll.collector.plist")) process.exit(1);
    const ensureDirectory = (directory, mode, create) => {
      try {
        const stat = fs.lstatSync(directory);
        if (!stat.isDirectory() || stat.isSymbolicLink()) process.exit(1);
      } catch (error) {
        if (error?.code !== "ENOENT" || !create) process.exit(1);
        fs.mkdirSync(directory, { mode });
        const stat = fs.lstatSync(directory);
        if (!stat.isDirectory() || stat.isSymbolicLink()) process.exit(1);
      }
    };
    ensureDirectory(home, 0o700, false);
    ensureDirectory(path.join(home, "Library"), 0o700, true);
    ensureDirectory(launchAgents, 0o700, true);
    try {
      const stat = fs.lstatSync(plist);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1) process.exit(1);
    } catch (error) {
      if (error?.code !== "ENOENT") process.exit(1);
    }
  ' >/dev/null 2>&1
}

harden_launch_agent_destination() {
  SECURE_PLIST="$LAUNCH_AGENT_PLIST" "$NODE_BIN" -e '
    const fs = require("node:fs");
    const descriptor = fs.openSync(
      process.env.SECURE_PLIST,
      fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW,
    );
    try {
      const stat = fs.fstatSync(descriptor);
      if (!stat.isFile() || stat.nlink !== 1) process.exit(1);
      fs.fchmodSync(descriptor, 0o600);
      fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
    const visible = fs.lstatSync(process.env.SECURE_PLIST);
    if (!visible.isFile() || visible.isSymbolicLink() || visible.nlink !== 1 || (visible.mode & 0o077) !== 0) process.exit(1);
  ' >/dev/null 2>&1
}

persist_install_identity() {
  IDENTITY_PATH="$INSTALL_IDENTITY_PATH" \
  IDENTITY_SHA="$SOURCE_REF" \
  IDENTITY_PROVENANCE="$REPO_PROVENANCE" \
  IDENTITY_REMOTE="$REPO_URL" \
  IDENTITY_NODE_PATH="$NODE_BIN" \
  IDENTITY_NODE_VERSION="$NODE_VERSION" \
  IDENTITY_PNPM_PATH="$PNPM_BIN" \
  IDENTITY_PNPM_VERSION="$PNPM_VERSION" \
  IDENTITY_BASELINE_TOKENS="$DOCTOR_TOKEN_EVENTS" \
  IDENTITY_RUNTIME_HASH="$DOCTOR_RUNTIME_HASH" \
  "$NODE_BIN" -e '
    const crypto = require("node:crypto");
    const fs = require("node:fs");
    const path = require("node:path");
    const hash = value => `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
    const target = process.env.IDENTITY_PATH;
    const baselineTokens = Number(process.env.IDENTITY_BASELINE_TOKENS);
    if (!Number.isSafeInteger(baselineTokens) || baselineTokens < 0 ||
        !/^sha256:[a-f0-9]{64}$/.test(process.env.IDENTITY_RUNTIME_HASH ?? "")) process.exit(2);
    const payload = `${JSON.stringify({
      schema: "plimsoll.source-install.v1",
      sourceCommit: process.env.IDENTITY_SHA,
      sourceProvenance: process.env.IDENTITY_PROVENANCE,
      sourceRemoteHash: hash(process.env.IDENTITY_REMOTE),
      nodeVersion: process.env.IDENTITY_NODE_VERSION,
      nodePathHash: hash(process.env.IDENTITY_NODE_PATH),
      pnpmVersion: process.env.IDENTITY_PNPM_VERSION,
      pnpmPathHash: hash(process.env.IDENTITY_PNPM_PATH),
      baselineTokenAttributedEvents: baselineTokens,
      runtimeInstanceHash: process.env.IDENTITY_RUNTIME_HASH,
    }, null, 2)}\n`;
    try {
      const stat = fs.lstatSync(target);
      if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) process.exit(2);
      const current = fs.readFileSync(target, "utf8");
      if (current === payload) process.exit(0);
    } catch (error) {
      if (error?.code !== "ENOENT") process.exit(2);
    }
    const temporary = `${target}.prepared-${process.pid}`;
    try {
      fs.writeFileSync(temporary, payload, { encoding: "utf8", flag: "wx", mode: 0o600 });
      const handle = fs.openSync(temporary, "r");
      try { fs.fsyncSync(handle); } finally { fs.closeSync(handle); }
      fs.renameSync(temporary, target);
      const directory = fs.openSync(path.dirname(target), "r");
      try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    } finally {
      try { fs.unlinkSync(temporary); } catch (error) { if (error?.code !== "ENOENT") throw error; }
    }
  ' >/dev/null 2>&1 || return 1
  INSTALLED_IDENTITY_MATCHED="true"
}

verify_install_identity() {
  identity_baseline="$(IDENTITY_PATH="$INSTALL_IDENTITY_PATH" \
    IDENTITY_SHA="$SOURCE_REF" \
    IDENTITY_PROVENANCE="$REPO_PROVENANCE" \
    IDENTITY_REMOTE="$REPO_URL" \
    IDENTITY_NODE_PATH="$NODE_BIN" \
    IDENTITY_NODE_VERSION="$NODE_VERSION" \
    IDENTITY_PNPM_PATH="$PNPM_BIN" \
    IDENTITY_PNPM_VERSION="$PNPM_VERSION" \
    "$NODE_BIN" -e '
    const crypto = require("node:crypto");
    const fs = require("node:fs");
    const hash = value => `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
    try {
      const stat = fs.lstatSync(process.env.IDENTITY_PATH);
      if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) process.exit(1);
      const value = JSON.parse(fs.readFileSync(process.env.IDENTITY_PATH, "utf8"));
      const keys = Object.keys(value).sort().join(",");
      if (
        keys !== "baselineTokenAttributedEvents,nodePathHash,nodeVersion,pnpmPathHash,pnpmVersion,runtimeInstanceHash,schema,sourceCommit,sourceProvenance,sourceRemoteHash" ||
        value.schema !== "plimsoll.source-install.v1" ||
        value.sourceCommit !== process.env.IDENTITY_SHA ||
        value.sourceProvenance !== process.env.IDENTITY_PROVENANCE ||
        value.sourceRemoteHash !== hash(process.env.IDENTITY_REMOTE) ||
        value.nodeVersion !== process.env.IDENTITY_NODE_VERSION ||
        value.nodePathHash !== hash(process.env.IDENTITY_NODE_PATH) ||
        value.pnpmVersion !== process.env.IDENTITY_PNPM_VERSION ||
        value.pnpmPathHash !== hash(process.env.IDENTITY_PNPM_PATH) ||
        !Number.isSafeInteger(value.baselineTokenAttributedEvents) || value.baselineTokenAttributedEvents < 0 ||
        !/^sha256:[a-f0-9]{64}$/.test(value.runtimeInstanceHash)
      ) process.exit(1);
      process.stdout.write(`${value.baselineTokenAttributedEvents}|${value.runtimeInstanceHash}`);
    } catch { process.exit(1); }
  ' 2>/dev/null)" || return 1
  BASELINE_TOKEN_EVENTS="${identity_baseline%%|*}"
  BASELINE_RUNTIME_HASH="${identity_baseline#*|}"
  if [ -z "$BASELINE_TOKEN_EVENTS" ] || [ "$BASELINE_RUNTIME_HASH" = "$identity_baseline" ]; then return 1; fi
  INSTALLED_IDENTITY_MATCHED="true"
}

verify_live_service_binding() {
  BINDING_PLIST="$LAUNCH_AGENT_PLIST" \
  BINDING_PID="$DEFAULT_COLLECTOR_HOME/collector.pid" \
  BINDING_TARGET="$PLIMSOLL_DIR" \
  BINDING_NODE_DIR="$node_dir" \
  BINDING_PNPM="$PNPM_BIN" \
  BINDING_RUNTIME_HASH="$DOCTOR_RUNTIME_HASH" \
  "$NODE_BIN" -e '
    const crypto = require("node:crypto");
    const fs = require("node:fs");
    const path = require("node:path");
    const { spawnSync } = require("node:child_process");
    const hash = value => `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
    try {
      const plistStat = fs.lstatSync(process.env.BINDING_PLIST);
      const pidStat = fs.lstatSync(process.env.BINDING_PID);
      if (!plistStat.isFile() || plistStat.isSymbolicLink() || plistStat.nlink !== 1 || (plistStat.mode & 0o077) !== 0 ||
          !pidStat.isFile() || pidStat.isSymbolicLink() || pidStat.nlink !== 1 || (pidStat.mode & 0o077) !== 0) process.exit(1);
      const converted = spawnSync("/usr/bin/plutil", ["-convert", "json", "-o", "-", "--", process.env.BINDING_PLIST], {
        encoding: "utf8",
        env: { LANG: "C", LC_ALL: "C", PATH: "/usr/bin:/bin" },
        stdio: ["ignore", "pipe", "pipe"],
        timeout: 2000,
      });
      if (converted.status !== 0 || !converted.stdout) process.exit(1);
      const plist = JSON.parse(converted.stdout);
      const expectedArguments = [process.env.BINDING_PNPM, "--dir", process.env.BINDING_TARGET, "collector", "start"];
      if (JSON.stringify(plist.ProgramArguments) !== JSON.stringify(expectedArguments) ||
          plist.WorkingDirectory !== process.env.BINDING_TARGET) process.exit(1);
      const pathEntries = String(plist.EnvironmentVariables?.PATH ?? "").split(":");
      if (path.resolve(pathEntries[0] ?? "") !== path.resolve(process.env.BINDING_NODE_DIR) ||
          !pathEntries.map(entry => path.resolve(entry)).includes(path.resolve(path.dirname(process.env.BINDING_PNPM)))) process.exit(1);
      const pid = JSON.parse(fs.readFileSync(process.env.BINDING_PID, "utf8"));
      if (pid.version !== 2 || pid.label !== "com.plimsoll.collector" || pid.cwd !== process.env.BINDING_TARGET ||
          hash(pid.instanceId) !== process.env.BINDING_RUNTIME_HASH || !Array.isArray(pid.command) ||
          pid.command.at(-1) !== "start") process.exit(1);
      const cli = pid.command.find(argument => typeof argument === "string" && argument.endsWith("packages/collector-cli/src/cli.ts"));
      if (!cli || path.resolve(pid.cwd, cli) !== path.join(process.env.BINDING_TARGET, "packages", "collector-cli", "src", "cli.ts")) process.exit(1);
    } catch { process.exit(1); }
  ' >/dev/null 2>&1
}
trap interrupted INT TERM HUP
trap cleanup_lock EXIT

if [ "$MODE" = "dry-run" ]; then
  CHECKOUT_RESULT="planned_exact_commit"
  DEPENDENCIES_RESULT="planned_frozen_lockfile"
  CONFIG_RESULT="planned_surgical_reconciliation"
  SERVICE_RESULT="planned_single_launch_agent_reconcile"
  READINESS="not_checked"
  PNPM_VERSION="not_executed_in_dry_run"
  emit_receipt "plan_selected_no_execution"
  exit 0
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required." >&2
  exit 1
fi

PATH="$RUNTIME_PATH" "$PNPM_BIN" --version >/dev/null 2>&1
pnpm_status=$?
if [ "$pnpm_status" -ne 0 ]; then
  fail "pnpm_runtime" "$pnpm_status"
fi
PNPM_VERSION="$(PATH="$RUNTIME_PATH" "$PNPM_BIN" --version 2>/dev/null || true)"
case "$PNPM_VERSION" in
  ''|*[!0-9.]*) fail "pnpm_version" 1 ;;
esac

acquire_lock
if [ "$MODE" = "verify" ]; then
  if [ ! -d "$PLIMSOLL_DIR/.git" ] || [ -L "$PLIMSOLL_DIR" ] || [ -L "$PLIMSOLL_DIR/.git" ]; then
    CHECKOUT_RESULT="missing"
    fail "checkout_missing" 1
  fi
  head_sha="$(git -C "$PLIMSOLL_DIR" rev-parse --verify 'HEAD^{commit}' 2>/dev/null || true)"
  if [ "$head_sha" != "$SOURCE_REF" ]; then
    CHECKOUT_RESULT="different_commit"
    fail "checkout_identity" 1
  fi
  existing_remote="$(git -C "$PLIMSOLL_DIR" remote get-url origin 2>/dev/null || true)"
  if [ "$existing_remote" != "$REPO_URL" ]; then
    CHECKOUT_RESULT="remote_conflict"
    fail "checkout_remote" 1
  fi
  verify_dirty="$(git -C "$PLIMSOLL_DIR" status --porcelain --untracked-files=no 2>/dev/null)"
  verify_status=$?
  if [ "$verify_status" -ne 0 ]; then
    CHECKOUT_RESULT="inspection_failed_retained"
    fail "checkout_inspection" "$verify_status"
  fi
  if [ -n "$verify_dirty" ]; then
    CHECKOUT_RESULT="dirty_retained"
    fail "checkout_dirty" 1
  fi
  CHECKOUT_RESULT="existing_exact"
  DEPENDENCIES_RESULT="retained_not_reinstalled"
  CONFIG_RESULT="read_only_verification"
  SERVICE_RESULT="read_only_verification"
  verify_install_identity || fail "installed_identity" 1
else
  mkdir -p "$(dirname "$PLIMSOLL_DIR")" || fail "checkout_parent" $?
  if [ -L "$PLIMSOLL_DIR" ] || [ -L "$PLIMSOLL_DIR/.git" ]; then
    CHECKOUT_RESULT="symlink_conflict"
    fail "checkout_target" 1
  fi
  if [ -e "$PLIMSOLL_DIR" ] && [ ! -d "$PLIMSOLL_DIR/.git" ]; then
    CHECKOUT_RESULT="occupied_non_git"
    fail "checkout_target" 1
  fi
  if [ ! -d "$PLIMSOLL_DIR/.git" ]; then
    git init "$PLIMSOLL_DIR" >/dev/null 2>&1 || fail "checkout_init" $?
    CHECKOUT_RESULT="initialized_retained"
    git -C "$PLIMSOLL_DIR" remote add origin "$REPO_URL" >/dev/null 2>&1 || fail "checkout_remote" $?
  else
    existing_remote="$(git -C "$PLIMSOLL_DIR" remote get-url origin 2>/dev/null || true)"
    if [ "$existing_remote" != "$REPO_URL" ]; then
      CHECKOUT_RESULT="remote_conflict"
      fail "checkout_remote" 1
    fi
    CHECKOUT_RESULT="existing_repository"
  fi

  dirty="$(git -C "$PLIMSOLL_DIR" status --porcelain --untracked-files=no 2>/dev/null)"
  dirty_status=$?
  if [ "$dirty_status" -ne 0 ]; then
    CHECKOUT_RESULT="inspection_failed_retained"
    fail "checkout_inspection" "$dirty_status"
  fi
  if [ -n "$dirty" ]; then
    CHECKOUT_RESULT="dirty_retained"
    fail "checkout_dirty" 1
  fi

  git -C "$PLIMSOLL_DIR" fetch --no-tags origin "$SOURCE_REF" >/dev/null 2>&1 || fail "source_fetch" $?
  fetched_sha="$(git -C "$PLIMSOLL_DIR" rev-parse --verify 'FETCH_HEAD^{commit}' 2>/dev/null || true)"
  if [ "$fetched_sha" != "$SOURCE_REF" ]; then
    fail "source_verification" 1
  fi
  git -C "$PLIMSOLL_DIR" cat-file -e "$SOURCE_REF^{commit}" >/dev/null 2>&1 || fail "source_object" $?
  SOURCE_VERIFIED="true"
  git -C "$PLIMSOLL_DIR" checkout --detach "$SOURCE_REF" >/dev/null 2>&1 || fail "checkout_pin" $?
  head_sha="$(git -C "$PLIMSOLL_DIR" rev-parse --verify 'HEAD^{commit}' 2>/dev/null || true)"
  if [ "$head_sha" != "$SOURCE_REF" ]; then
    fail "checkout_postcondition" 1
  fi
  CHECKOUT_RESULT="pinned_exact"

  PATH="$RUNTIME_PATH" "$PNPM_BIN" --dir "$PLIMSOLL_DIR" install --frozen-lockfile >/dev/null 2>&1 || fail "frozen_install" $?
  DEPENDENCIES_RESULT="frozen_lockfile_installed"
  dirty="$(git -C "$PLIMSOLL_DIR" status --porcelain --untracked-files=no 2>/dev/null)"
  dirty_status=$?
  if [ "$dirty_status" -ne 0 ]; then
    CHECKOUT_RESULT="inspection_failed_retained"
    fail "checkout_inspection" "$dirty_status"
  fi
  if [ -n "$dirty" ]; then
    CHECKOUT_RESULT="dirty_retained"
    fail "frozen_install_changed_source" 1
  fi

  setup_output="$(PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector setup --yes 2>/dev/null)"
  setup_status=$?
  if [ "$setup_status" -ne 0 ]; then
    CONFIG_RESULT="failed_retained"
    fail "config_reconciliation" "$setup_status"
  fi
  CONFIG_RESULT="$(printf '%s' "$setup_output" | "$NODE_BIN" -e '
    let body = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => body += chunk);
    process.stdin.on("end", () => {
      const starts = [...body].flatMap((character, index) => character === "{" ? [index] : []);
      let value;
      for (let index = starts.length - 1; index >= 0 && !value; index -= 1) {
        try { value = JSON.parse(body.slice(starts[index]).trim()); } catch {}
      }
      const status = value?.status === "setup_noop"
        ? "unchanged"
        : value?.status === "setup_applied"
          ? "applied_with_exact_preimage_backups"
          : "unknown";
      process.stdout.write(status);
    });
  ')"
  if [ "$CONFIG_RESULT" = "unknown" ]; then
    fail "config_receipt" 1
  fi

  preflight_output="$(PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector doctor --read-only --json 2>/dev/null)"
  preflight_status=$?
  printf '%s' "$preflight_output" | PREFLIGHT_STATUS="$preflight_status" SELECTED_NODE_VERSION="$NODE_VERSION" "$NODE_BIN" -e '
    let body = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => body += chunk);
    process.stdin.on("end", () => {
      const starts = [...body].flatMap((character, index) => character === "{" ? [index] : []);
      let value;
      for (let index = starts.length - 1; index >= 0 && !value; index -= 1) {
        try { value = JSON.parse(body.slice(starts[index]).trim()); } catch {}
      }
      try {
        const ok =
          value?.readOnly === true &&
          value.node?.version === process.env.SELECTED_NODE_VERSION &&
          value.node?.supported === true &&
          value.config?.status === "valid" && value.config?.valid === true &&
          value.config?.createdDuringCommand === false &&
          value.telemetry?.ok === true &&
          value.telemetry?.claude?.ok === true &&
          value.telemetry?.codex?.ok === true &&
          value.dataMode === "metadata" &&
          value.privacyMode === "metadata_only" &&
          value.privacy?.mode === "metadata_only" &&
          value.privacy?.rawEvidenceCapture === "disabled" &&
          value.syncConfigured === false &&
          value.uploadSigningConfigured === false;
        process.exit(ok ? 0 : 1);
      } catch { process.exit(1); }
    });
  ' >/dev/null 2>&1 || fail "local_only_preflight" 1
  LOCAL_ONLY_CONFIRMED="true"

  dirty="$(git -C "$PLIMSOLL_DIR" status --porcelain --untracked-files=no 2>/dev/null)"
  dirty_status=$?
  if [ "$dirty_status" -ne 0 ]; then
    CHECKOUT_RESULT="inspection_failed_retained"
    fail "checkout_inspection" "$dirty_status"
  fi
  if [ -n "$dirty" ]; then
    CHECKOUT_RESULT="dirty_retained"
    fail "config_step_changed_source" 1
  fi

  # Reconcile one service owner. A failed bootout is the normal absent-job case;
  # the following install/bootstrap postcondition is the authoritative gate.
  prepare_launch_agent_destination || {
    SERVICE_RESULT="manifest_destination_refused_retained"
    fail "service_manifest_destination" 1
  }
  PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector unload-launch-agent >/dev/null 2>&1 || true
  SERVICE_RESULT="stopped_or_absent"
  PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector install-launch-agent --dev --repo-root "$PLIMSOLL_DIR" --pnpm "$PNPM_BIN" >/dev/null 2>&1 || fail "service_manifest" $?
  harden_launch_agent_destination || fail "service_manifest_postcondition" 1
  SERVICE_RESULT="manifest_installed"
  SERVICE_LOADED_BY_INSTALLER=1
  PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector load-launch-agent >/dev/null 2>&1 || fail "service_load" $?
  SERVICE_RESULT="load_requested"
fi

doctor_attempts="${PLIMSOLL_INSTALL_DOCTOR_ATTEMPTS:-20}"
case "$doctor_attempts" in ''|*[!0-9]*) doctor_attempts=20 ;; esac
[ "$doctor_attempts" -gt 0 ] || doctor_attempts=1
attempt=1
doctor_output=""
while [ "$attempt" -le "$doctor_attempts" ]; do
  doctor_output="$(PATH="$RUNTIME_PATH" "$PNPM_BIN" --silent --dir "$PLIMSOLL_DIR" collector doctor --read-only --json 2>/dev/null)"
  doctor_status=$?
  doctor_parsed="$(printf '%s' "$doctor_output" | DOCTOR_STATUS="$doctor_status" SELECTED_NODE_VERSION="$NODE_VERSION" "$NODE_BIN" -e '
    const crypto = require("node:crypto");
    let body = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => body += chunk);
    process.stdin.on("end", () => {
      try {
        const starts = [...body].flatMap((character, index) => character === "{" ? [index] : []);
        let value;
        for (let index = starts.length - 1; index >= 0 && !value; index -= 1) {
          try { value = JSON.parse(body.slice(starts[index]).trim()); } catch {}
        }
        if (!value) throw new Error("missing doctor JSON");
        const tokenEvents = value.connectivity?.signal?.tokenAttributedEvents;
        const runtimeInstanceId = value.connectivity?.runtimeIdentity?.instanceId;
        const runtimeHash = typeof runtimeInstanceId === "string"
          ? `sha256:${crypto.createHash("sha256").update(runtimeInstanceId).digest("hex")}`
          : "invalid";
        const common =
          value && typeof value === "object" &&
          value.readOnly === true &&
          value.node?.version === process.env.SELECTED_NODE_VERSION &&
          value.node?.range === ">=20 <25" &&
          value.node?.supported === true &&
          Number(String(value.node?.version).split(".")[0]) === 22 &&
          value.config?.status === "valid" &&
          value.config?.valid === true &&
          value.config?.createdDuringCommand === false &&
          value.telemetry?.ok === true &&
          value.telemetry?.claude?.ok === true &&
          value.telemetry?.codex?.ok === true &&
          value.launchAgent?.ok === true &&
          value.launchAgent?.path?.ok === true &&
          value.runtime?.ok === true &&
          value.runtime?.ownershipVersion?.expected === 2 &&
          value.runtime?.ownershipVersion?.actual === 2 &&
          value.runtime?.processLive === true &&
          value.runtime?.identityMatchesStatus === true &&
          value.connectivity?.reachable === true &&
          typeof runtimeInstanceId === "string" && runtimeInstanceId.length >= 32 &&
          Number.isSafeInteger(tokenEvents) && tokenEvents >= 0 &&
          value.dataMode === "metadata" &&
          value.privacyMode === "metadata_only" &&
          value.privacy?.mode === "metadata_only" &&
          value.privacy?.configuredDataMode === "metadata" &&
          value.privacy?.rawEvidenceCapture === "disabled" &&
          value.syncConfigured === false &&
          value.uploadSigningConfigured === false;
        const serviceReady =
          common && value.readiness === "service_ready" && value.ok === false &&
          value.connectivity?.signal?.verified === false &&
          Number(process.env.DOCTOR_STATUS) !== 0;
        const signalVerified =
          common && value.readiness === "signal_verified" && value.ok === true &&
          value.connectivity?.signal?.verified === true &&
          Number(process.env.DOCTOR_STATUS) === 0;
        const readiness = serviceReady ? "service_ready" : signalVerified ? "signal_verified" : "invalid";
        process.stdout.write(`${readiness}|${Number.isSafeInteger(tokenEvents) ? tokenEvents : "unknown"}|${runtimeHash}`);
      } catch { process.stdout.write("invalid|unknown|unknown"); }
    });
  ')"
  READINESS="${doctor_parsed%%|*}"
  doctor_remainder="${doctor_parsed#*|}"
  DOCTOR_TOKEN_EVENTS="${doctor_remainder%%|*}"
  DOCTOR_RUNTIME_HASH="${doctor_remainder#*|}"
  if [ "$READINESS" = "service_ready" ] || [ "$READINESS" = "signal_verified" ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -le "$doctor_attempts" ]; then sleep 0.5; fi
done

if [ "$READINESS" = "service_ready" ] || [ "$READINESS" = "signal_verified" ]; then
  LOCAL_ONLY_CONFIRMED="true"
  verify_live_service_binding || fail "service_identity_binding" 1
fi

if [ "$MODE" = "verify" ]; then
  if [ "$READINESS" != "signal_verified" ] ||
     [ "$DOCTOR_RUNTIME_HASH" != "$BASELINE_RUNTIME_HASH" ] ||
     [ "$DOCTOR_TOKEN_EVENTS" -le "$BASELINE_TOKEN_EVENTS" ] 2>/dev/null; then
    SERVICE_RESULT="running_but_signal_unverified"
    fail "fresh_signal_verification" 1
  fi
  SERVICE_RESULT="signal_verified"
  emit_receipt "signal_verified"
  exit 0
fi

if [ "$READINESS" = "service_ready" ] || [ "$READINESS" = "signal_verified" ]; then
  READINESS="service_ready"
  SERVICE_RESULT="service_ready_pending_fresh_signal"
  persist_install_identity || fail "installed_identity_write" 1
  emit_receipt "service_ready"
  exit 0
fi

SERVICE_RESULT="not_ready_retained"
fail "service_readiness" 1
