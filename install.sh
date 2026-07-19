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
INSTALL_LOCK="${PLIMSOLL_DIR}.plimsoll-install.lock"
LOCK_HELD=0
INSTALL_IDENTITY_PATH="$PLIMSOLL_DIR/.git/plimsoll-source-install.v1.json"
INSTALLED_IDENTITY_MATCHED="false"

usage() {
  cat <<'EOF'
Usage:
  ./install.sh --dry-run --ref <40-character-commit> [--node PATH] [--pnpm PATH]
  ./install.sh apply     --ref <40-character-commit> [--node PATH] [--pnpm PATH]
  ./install.sh verify    --ref <40-character-commit> [--node PATH] [--pnpm PATH]

Modes:
  --dry-run  Validate the immutable input and runtime plan without writing.
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
    /*) [ -x "$candidate" ] || return 1 ;;
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
        : state === "plan_validated"
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
      localOnlyDefault: true,
      hostedEnrollmentPerformed: false,
      credentialOperations: 0,
      rollbackClaimed: false,
      errorStage: env.RECEIPT_ERROR_STAGE,
      nextAction,
    }));
  '
}

fail() {
  ERROR_STAGE="$1"
  code="${2:-1}"
  echo "Source install stopped at $ERROR_STAGE. Retained state is reported below; no rollback or uninstall is claimed." >&2
  emit_receipt "failed"
  exit "$code"
}

interrupted() {
  ERROR_STAGE="interrupted"
  echo "Source install interrupted. Re-run apply with the same exact commit to resume." >&2
  emit_receipt "interrupted"
  exit 130
}

cleanup_lock() {
  if [ "$LOCK_HELD" = "1" ] && [ -d "$INSTALL_LOCK" ] && [ ! -L "$INSTALL_LOCK" ]; then
    lock_owner="$(cat "$INSTALL_LOCK/owner.pid" 2>/dev/null || true)"
    if [ "$lock_owner" = "$$" ]; then
      rm -f "$INSTALL_LOCK/owner.pid" 2>/dev/null || true
      rmdir "$INSTALL_LOCK" 2>/dev/null || true
    fi
  fi
}

acquire_lock() {
  mkdir -p "$(dirname "$PLIMSOLL_DIR")" || fail "checkout_parent" $?
  if mkdir "$INSTALL_LOCK" 2>/dev/null; then
    printf '%s\n' "$$" > "$INSTALL_LOCK/owner.pid" || fail "install_lock_owner" $?
    LOCK_HELD=1
    return 0
  fi
  if [ -L "$INSTALL_LOCK" ] || [ ! -d "$INSTALL_LOCK" ]; then
    fail "install_lock_conflict" 1
  fi
  lock_owner="$(cat "$INSTALL_LOCK/owner.pid" 2>/dev/null || true)"
  case "$lock_owner" in
    ''|*[!0-9]*) ;;
    *)
      if kill -0 "$lock_owner" 2>/dev/null; then
        fail "install_already_running" 1
      fi
      ;;
  esac
  # Only a dead/invalid lock with the one expected regular owner file is
  # reclaimed. Any extra content is retained for operator inspection.
  if [ -L "$INSTALL_LOCK/owner.pid" ]; then
    fail "install_lock_conflict" 1
  fi
  rm -f "$INSTALL_LOCK/owner.pid" 2>/dev/null || fail "install_lock_conflict" $?
  rmdir "$INSTALL_LOCK" 2>/dev/null || fail "install_lock_conflict" $?
  mkdir "$INSTALL_LOCK" 2>/dev/null || fail "install_lock_race" $?
  printf '%s\n' "$$" > "$INSTALL_LOCK/owner.pid" || fail "install_lock_owner" $?
  LOCK_HELD=1
}

persist_install_identity() {
  IDENTITY_PATH="$INSTALL_IDENTITY_PATH" \
  IDENTITY_SHA="$SOURCE_REF" \
  IDENTITY_PROVENANCE="$REPO_PROVENANCE" \
  IDENTITY_NODE_PATH="$NODE_BIN" \
  IDENTITY_NODE_VERSION="$NODE_VERSION" \
  IDENTITY_PNPM_PATH="$PNPM_BIN" \
  IDENTITY_PNPM_VERSION="$PNPM_VERSION" \
  "$NODE_BIN" -e '
    const crypto = require("node:crypto");
    const fs = require("node:fs");
    const path = require("node:path");
    const hash = value => `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
    const target = process.env.IDENTITY_PATH;
    const payload = `${JSON.stringify({
      schema: "plimsoll.source-install.v1",
      sourceCommit: process.env.IDENTITY_SHA,
      sourceProvenance: process.env.IDENTITY_PROVENANCE,
      nodeVersion: process.env.IDENTITY_NODE_VERSION,
      nodePathHash: hash(process.env.IDENTITY_NODE_PATH),
      pnpmVersion: process.env.IDENTITY_PNPM_VERSION,
      pnpmPathHash: hash(process.env.IDENTITY_PNPM_PATH),
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
  IDENTITY_PATH="$INSTALL_IDENTITY_PATH" \
  IDENTITY_SHA="$SOURCE_REF" \
  IDENTITY_PROVENANCE="$REPO_PROVENANCE" \
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
        keys !== "nodePathHash,nodeVersion,pnpmPathHash,pnpmVersion,schema,sourceCommit,sourceProvenance" ||
        value.schema !== "plimsoll.source-install.v1" ||
        value.sourceCommit !== process.env.IDENTITY_SHA ||
        value.sourceProvenance !== process.env.IDENTITY_PROVENANCE ||
        value.nodeVersion !== process.env.IDENTITY_NODE_VERSION ||
        value.nodePathHash !== hash(process.env.IDENTITY_NODE_PATH) ||
        value.pnpmVersion !== process.env.IDENTITY_PNPM_VERSION ||
        value.pnpmPathHash !== hash(process.env.IDENTITY_PNPM_PATH)
      ) process.exit(1);
    } catch { process.exit(1); }
  ' >/dev/null 2>&1 || return 1
  INSTALLED_IDENTITY_MATCHED="true"
}
trap interrupted INT TERM HUP
trap cleanup_lock EXIT

if [ "$MODE" = "dry-run" ]; then
  CHECKOUT_RESULT="planned_exact_commit"
  DEPENDENCIES_RESULT="planned_frozen_lockfile"
  CONFIG_RESULT="planned_surgical_reconciliation"
  SERVICE_RESULT="planned_single_launch_agent_reconcile"
  READINESS="not_checked"
  emit_receipt "plan_validated"
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
  CHECKOUT_RESULT="existing_exact"
  DEPENDENCIES_RESULT="retained_not_reinstalled"
  CONFIG_RESULT="read_only_verification"
  SERVICE_RESULT="read_only_verification"
  verify_install_identity || fail "installed_identity" 1
else
  acquire_lock
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

  dirty="$(git -C "$PLIMSOLL_DIR" status --porcelain --untracked-files=no 2>/dev/null || true)"
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

  setup_output="$(PATH="$RUNTIME_PATH" "$PNPM_BIN" --dir "$PLIMSOLL_DIR" collector setup --yes 2>/dev/null)"
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
      try {
        const value = JSON.parse(body);
        process.stdout.write(value.status === "setup_noop" ? "unchanged" : value.status === "setup_applied" ? "applied_with_exact_preimage_backups" : "unknown");
      } catch { process.stdout.write("unknown"); }
    });
  ')"
  if [ "$CONFIG_RESULT" = "unknown" ]; then
    fail "config_receipt" 1
  fi

  # Reconcile one service owner. A failed bootout is the normal absent-job case;
  # the following install/bootstrap postcondition is the authoritative gate.
  PATH="$RUNTIME_PATH" "$PNPM_BIN" --dir "$PLIMSOLL_DIR" collector unload-launch-agent >/dev/null 2>&1 || true
  SERVICE_RESULT="stopped_or_absent"
  PATH="$RUNTIME_PATH" "$PNPM_BIN" --dir "$PLIMSOLL_DIR" collector install-launch-agent --dev --repo-root "$PLIMSOLL_DIR" --pnpm "$PNPM_BIN" >/dev/null 2>&1 || fail "service_manifest" $?
  SERVICE_RESULT="manifest_installed"
  PATH="$RUNTIME_PATH" "$PNPM_BIN" --dir "$PLIMSOLL_DIR" collector load-launch-agent >/dev/null 2>&1 || fail "service_load" $?
  SERVICE_RESULT="load_requested"
fi

doctor_attempts="${PLIMSOLL_INSTALL_DOCTOR_ATTEMPTS:-20}"
case "$doctor_attempts" in ''|*[!0-9]*) doctor_attempts=20 ;; esac
[ "$doctor_attempts" -gt 0 ] || doctor_attempts=1
attempt=1
doctor_output=""
while [ "$attempt" -le "$doctor_attempts" ]; do
  doctor_output="$(PATH="$RUNTIME_PATH" "$PNPM_BIN" --dir "$PLIMSOLL_DIR" collector doctor --read-only --json 2>/dev/null)"
  doctor_status=$?
  READINESS="$(printf '%s' "$doctor_output" | DOCTOR_STATUS="$doctor_status" SELECTED_NODE_VERSION="$NODE_VERSION" "$NODE_BIN" -e '
    let body = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => body += chunk);
    process.stdin.on("end", () => {
      try {
        const value = JSON.parse(body);
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
        process.stdout.write(serviceReady ? "service_ready" : signalVerified ? "signal_verified" : "invalid");
      } catch { process.stdout.write("invalid"); }
    });
  ')"
  if [ "$READINESS" = "service_ready" ] || [ "$READINESS" = "signal_verified" ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -le "$doctor_attempts" ]; then sleep 0.5; fi
done

if [ "$MODE" = "verify" ]; then
  if [ "$READINESS" != "signal_verified" ]; then
    SERVICE_RESULT="running_but_signal_unverified"
    fail "signal_verification" 1
  fi
  SERVICE_RESULT="signal_verified"
  emit_receipt "signal_verified"
  exit 0
fi

if [ "$READINESS" = "service_ready" ]; then
  SERVICE_RESULT="service_ready"
  persist_install_identity || fail "installed_identity_write" 1
  emit_receipt "service_ready"
  exit 0
fi
if [ "$READINESS" = "signal_verified" ]; then
  SERVICE_RESULT="signal_verified"
  persist_install_identity || fail "installed_identity_write" 1
  emit_receipt "signal_verified"
  exit 0
fi

SERVICE_RESULT="not_ready_retained"
fail "service_readiness" 1
