#!/usr/bin/env node
import { AutomaticRetentionCadence } from "./retention-cadence";
import Database from "better-sqlite3";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline/promises";
import { isDeepStrictEqual } from "node:util";
import { parse as parseToml } from "smol-toml";

const privatePathReceipt = (value: string) =>
  `sha256:${createHash("sha256").update(path.resolve(value)).digest("hex")}`;

const pidCleanupStateReceipt = (state: CollectorPidCleanupState) => ({
  ambiguous: state.ambiguous,
  markerState: state.markerState,
  claimCount: state.claimCount,
  quarantineCount: state.quarantineCount,
  inventoryTruncated: state.inventoryTruncated,
  unsafeArtifactCount: state.unsafeArtifactCount,
});

const pidCleanupReconciliationReceipt = (
  result: CollectorPidCleanupReconciliationResult,
) => ({
  reconciled: result.reconciled,
  eligible: result.eligible,
  disposition: result.disposition,
  before: pidCleanupStateReceipt(result.before),
  after: pidCleanupStateReceipt(result.after),
});

const pidCleanupAttemptReceipt = (result: CollectorPidCleanupResult | null) =>
  result
    ? {
        attempted: true,
        removed: result.removed,
        ambiguous: result.ambiguous,
        quarantined: result.quarantined,
        disposition: result.disposition,
      }
    : {
        attempted: false,
        removed: false,
        ambiguous: false,
        quarantined: false,
        disposition: null,
      };

import { LocalEventBuffer } from "./buffer";
import {
  collectorHomeIdentityHash,
  defaultCollectorHome,
  resolveCollectorHome,
  resolveGrokHome,
} from "./collector-home";
import {
  assertCollectorPrivacyMode,
  collectorHome,
  collectorBufferPath,
  collectorConfigPath,
  collectorLogPath,
  collectorPrivacyReadiness,
  collectorConfigSchema,
  ensureCollectorHome,
  loadCollectorConfig,
  readCollectorConfig,
  writeCollectorConfigTransactionally,
  type CollectorConfig,
} from "./config";
import { appendForwardedHook } from "./forwarder";
import { forwardHookOverLoopback } from "./local-hook-client";
import {
  DEFAULT_PRODUCER_ROTATION_GRACE_MS,
  MAX_PRODUCER_ROTATION_GRACE_MS,
  loadOrCreateLocalIngestAuth,
  producerRotationState,
  readLocalIngestAuth,
  rotateLocalProducerToken,
  unpersistedProducerAudiences,
  type LocalIngestAuth,
} from "./local-auth";
import { enrollCodexLiveProducer } from "./codex-live-usage-auth";
import {
  installLaunchAgent,
  inspectLaunchAgentManifest,
  LAUNCH_AGENT_LABEL,
  LAUNCH_AGENT_SYSTEM_PATHS,
  launchAgentPlistPath,
  launchctlBootoutCommand,
  launchctlBootstrapCommand,
  launchctlPrintCommand,
  uninstallLaunchAgent,
} from "./launch-agent";
import {
  defaultLifecycleAuthorityRoot,
  LifecycleMutationAuthority,
  type LifecycleMutationLease,
} from "./lifecycle-authority";
import {
  cleanupStaleJoinHandshakeDirectories,
  finalizeActivatedPendingJoin,
  performJoin,
  resumePendingJoin,
} from "./join";
import { createProfileCapture } from "./profile-capture";
import {
  AutomaticMaintenanceCadence,
  automaticRepairServiceStatus,
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
  automaticCaptureRuntimeStatus,
  isMaintenancePartialOutcome,
  type MaintenanceAttemptOutcome,
} from "./maintenance";
import { codexReconciliationStatus } from "./codex-reconciliation";
import {
  historyCoverageStatus,
  recordExplicitFullHistoryCoverage,
} from "./history-coverage";
import {
  captureBaselineStatus,
  sealCaptureBaselineGenerations,
  unsealCaptureBaselineGenerations,
  type CaptureBaselineSealResult,
} from "./capture-baseline";
import {
  captureRootBaselineFiles,
  captureRootBaselineObservations,
  captureRootsDeriveFrom,
  configuredCaptureRootDirectory,
  deriveCaptureRootIdentity,
  discoverCaptureRootCandidates,
  discoverCaptureRoots,
  physicalCaptureRootDirectory,
  resolveCaptureRootMachineLabel,
  resolveDiscoveryHome,
  validateCaptureRoots,
  type CaptureRoot,
} from "./capture-root-inventory";
import { createCollectorServer } from "./server";
import { MaintenanceFailureError, MaintenanceProcessBoundary } from "./maintenance-boundary";
import { checkpointWalInBoundedChild, runStartupWalSelfHeal } from "./startup-wal-self-heal";
import {
  maintenanceStarvationReceipt,
  recordMaintenanceDeadlineBlame,
  recordMaintenanceDeadlineKill,
} from "./maintenance-starvation";
import { runMaintenanceWorkerService } from "./maintenance-worker";
import {
  AutomaticEnrichmentCadence,
  EnrichmentProcessBoundary,
  IdleEnrichmentScheduler,
  lowerEnrichmentProcessPriority,
  runEnrichmentWorkerService,
} from "./enrichment-job";
import { runEnrichmentMaintenanceJob } from "./maintenance-stage-primitives";
import { readLocalIdentities } from "./local-identity";
import {
  loadOrCreateDeviceIdentity,
  readDeviceIdentity,
  recordDeviceSeen,
  recordDeviceUpload,
} from "./device-identity";
import { runLifecycleCommand } from "./lifecycle-command";
import {
  composeLifecycleAdapter,
  resolveArtifactFromBundle,
  resolveSelfArtifact,
} from "./lifecycle-adapters";
import { PURGE_CONFIRMATION } from "./lifecycle";
import { PLIMSOLL_VERSION } from "./version";
import {
  applyCodexConfig,
  applyCodexHookHeaderFile,
  applyGeminiSettings,
  applyGrokHookHeaderFile,
  applyGrokHookFile,
  claudeSeatsRoot,
  codexProfilesRoot,
  diagnoseManagedCodexHookCommand,
  diagnoseManagedGrokHookCommand,
  discoverClaudeSeats,
  discoverCodexProfiles,
  generateClaudeCodeSettings,
  generateCodexConfigToml,
  generateCodexHookHeader,
  generateGeminiCliSettings,
  generateGrokHookHeader,
  generateGrokHookSettings,
  generateSetupInstructions,
} from "../../collector-config/src/index";
import {
  DEFAULT_MANAGED_CONFIG_RECONCILE_INTERVAL_SECONDS,
  type ManagedConfigDriftReport,
  type ManagedConfigReadback,
  type ManagedConfigReconcileDecision,
  type ManagedConfigReconcileResult,
  type ManagedConfigTarget,
  composeManagedClaudeTargets,
  composeManagedCodexTargets,
  decideManagedConfigReconcileAsync,
  type ManagedConfigReconcileSettings,
  managedConfigDriftReportAsync,
  managedConfigReconcileDoctorSection,
  readManagedConfigReconcileSettings,
  readManagedConfigReconcileState,
  runManagedConfigReconcile,
  runManagedConfigReconcileAsync,
  stampManagedConfigReconcileDecision,
} from "./managed-config-reconcile";
import { runOutcomesSync } from "./outcomes-sync";
import {
  GitHubRestOutcomeTimelineAdapter,
  readRequiredCheckPolicy,
  runOutcomeTimelineBackfill,
} from "./github-outcome-backfill";
import { OutcomeTimelineStore } from "./outcome-timeline-store";
import { formatWeeklyPerformanceMarkdown } from "./performance-layer";
import { runLearningMaterialization } from "./learning-materializer";
import { prepareRepoLabelsPush, pushRepoLabels } from "./repo-labels";
import { runSessionSync, sessionIdsFromBatches } from "./session-sync";
import { uploadBufferedEvents } from "./upload";
import { SyncStorageBusyError, SyncStorageRetryController } from "./sqlite-contention";
import { runAttributionRepair, runWorkspaceHistoryUpload } from "./upload-history";
import {
  ACCOUNT_ASSERTION_SOURCES,
  accountAssertionStatus,
  formatAccountAssertionStatusLine,
  readAccountAssertionAdapterState,
  setAccountAssertionAdapterEnabled,
  type AccountAssertionSource,
} from "./account-assertion";
import { syncAccountActorSalt } from "./account-salt";
import {
  acquireCollectorStartOwnership,
  classifyProcessIdentity,
  CollectorStartOwnershipError,
  captureLaunchAgentUnloadPriorState,
  createCollectorRuntimeIdentity,
  observeCollectorListener,
  observeLaunchAgentUnloadTerminalState,
  processIdentityIsLive,
  readCollectorPidCleanupState,
  readCollectorPidFile,
  readUtcProcessStartFingerprint,
  reconcileCollectorPidCleanupState,
  removeCollectorPidFileIfOwned,
  runtimeIdentityMatches,
  UTC_PROCESS_START_ALGORITHM,
  verifyCollectorRuntimeIdentity,
  type CollectorListenerObservation,
  type LaunchAgentLabelObservation,
  type LaunchAgentUnloadOutcome,
  type LaunchAgentUnloadPriorState,
  type CollectorPidRecord,
  type CollectorPidCleanupResult,
  type CollectorPidCleanupReconciliationResult,
  type CollectorPidCleanupState,
  type CollectorRuntimeIdentity,
} from "./runtime-ownership";

const command = process.argv[2] ?? "help";

function printHelp() {
  console.log(`Plimsoll Collector

Commands:
  start                 Start the local hook/OTLP receiver in the foreground
  status                Print local buffer and policy status
  maintenance --disable-account-assertion SOURCE --yes
                        Toggle one adapter; writes only account assertion state
  --disable-account-assertion SOURCE
                        Disable one account assertion adapter (requires --yes)
  --enable-account-assertion SOURCE
                        Enable one account assertion adapter (requires --yes)
  join TOKEN|URL        Join a hosted workspace: redeem the admin's single-use
                        token, write sync credentials, verify with a handshake
                        (use --reassign for an explicit workspace change)
  sync-account-salt     Refresh the tenant-scoped actor salt over the
                        authenticated device channel (no raw salt is printed)
  enroll-codex-live-producer --producer-id ID --credential-id ID --capture-root-id ID
                        Provision a same-user Codex live producer; account
                        assertion discovery is best-effort and never printed
  capture-roots discover [--json]
                        List native capture roots under $HOME with their state
                        (registered | candidate | missing); read-only
  capture-roots add --source codex|claude_code --directory DIR [--directory DIR]
                        [--machine LABEL] [--allow-scan-errors] [--dry-run] [--json]
                        Append a newly discovered capture root: derives the
                        enrolled identity, backs the config up, fences the
                        files the new root already holds, restarts the
                        collector and writes a receipt. Never changes an
                        existing root, epoch or enrollment field.
                        --allow-scan-errors registers a root whose walk is
                        ambiguous: the entries are named in the receipt and
                        left unfenced (so they are captured, not excluded)
  doctor --read-only --json
                        Read-only readiness check; never creates config, ledger, plist, logs, or directories
  export                Print buffered events as JSON
  forward-hook SOURCE   Read hook JSON from stdin and append it without requiring the receiver
  forward-hook-http SOURCE
                        Forward stdin to the authenticated loopback hook boundary without argv secrets
  self-test-hook SOURCE Emit one synthetic hook event into the local buffer
  generate-config TOOL  Print Claude Code, Codex, Gemini CLI, or Grok config for metadata collection
  setup                 APPLY Claude Code, Gemini CLI, Grok, and Codex telemetry independently
                        (idempotent; --yes, --dry-run)
  rotate-producer-token --source codex
                        Mint a new Codex producer token, rewrite the managed header file and
                        config.toml with backups, and accept the superseded token only until
                        the grace window closes (--grace-seconds, --dry-run)
  upload                Drain un-uploaded events to the tenant ingest API (marks rows, keeps local copies)
  upload-history        Workspace backfill: push the FULL ledger history to the joined
                        workspace, idempotently, then print a reconciliation audit.
                        Ledger is opened read-only; rows are never marked uploaded.
                        Safe alongside the live 5-minute sync: the cloud dedupes by
                        event id, so overlap deduplicates instead of duplicating.
  upload-replay         Re-queue dead-lettered deliveries after the remote contract that
                        rejected them was fixed (remote reasons only; local privacy,
                        quarantine, oversize and schema receipts stay final). Only
                        re-queues — delivery happens on the normal upload cycles, so it
                        is safe to run while the upload circuit is open. This is the
                        recovery path for the one-way door in the upload cycle: once a
                        durable validation witness proves the endpoint contract, every
                        delivery of the rejected source that the cycle isolates is
                        dead-lettered per delivery and the rest are deferred to the next
                        cycle, instead of the whole host being held by a circuit.
  push-repo-labels      Disclose repo display names to the joined workspace so dashboards
                        show github.com/owner/name instead of sha256 hashes. Previews the
                        exact payload first; --dry-run to only preview.
  sync-outcomes         Push the locally-computed session↔PR outcome join (issue 0038)
                        for ONE named repository to the joined workspace: merge status,
                        check results, and short-horizon rework, keyed by the same
                        linkage hashes sessions and events carry. Idempotent by
                        deterministic id; re-running converges instead of duplicating.
  backfill-outcome-timeline
                        Explicit, bounded GitHub recovery for immutable PR revisions,
                        every completed check attempt, reviews, lifecycle events,
                        linked issues, and full-SHA reverts. Resumes from local state;
                        never runs in the collector server/background path.
  backfill-outcome-performance
                        Derive local, materialized pull outcomes from the immutable
                        outcome timeline; missing evidence remains literal UNKNOWN.
  weekly-performance-rollup
                        Write local weekly outcome-performance JSON + Markdown on demand;
                        never scheduled or uploaded.
  scan-rollouts         Read codex rollout files into the ledger once (full history walk)
  scan-transcripts      Read Claude Code transcript usage into the ledger once (full history walk)
  drain-projections     Drain a stalled dashboard-projection repair backlog at full
                        budget until the dashboard is caught up (run with the
                        collector stopped; safe to interrupt and re-run)
  install-launch-agent  Write the user LaunchAgent plist
  load-launch-agent     Load an installed user LaunchAgent plist
  unload-launch-agent   Unload the user LaunchAgent without removing the plist
  uninstall-launch-agent Remove the user LaunchAgent plist
  lifecycle             Transactional runtime update/rollback, preview-default
                        uninstall/purge, and sanitized support bundle (see below)
  label account HASH NAME    Set a local-only display label for a hashed account
  priority add|remove URL    Manage the priority-repo list (hashed; URL kept locally)
  priority list              Show priority repos
  purge-local-data      Dry-run or explicitly purge local buffered event data
  stop                  Stop the foreground daemon using the local PID file

Config tools:
  join "<join-url>#<token>" | join --token-prompt --url <cloud-base-url> | join --token-stdin --url <cloud-base-url> | join --token-fd FD --url <cloud-base-url> | join --resume
      Add --reassign only after reviewing the explicit A → B boundary.
      Prefer --token-prompt, --token-stdin, --token-fd, or join - so the single-use secret never enters
      shell history or process arguments. Workspace URL env: PLIMSOLL_CLOUD_URL.
      join --dry-run is unsupported and fails before token, network, or local-state mutation.
  generate-config claude-code|codex|gemini-cli|grok|all   (metadata-only; encrypted evidence vault not implemented)
  upload [--url URL --limit 500] [--ingest-key KEY] [--signing-secret SECRET] [--no-mark] [--max-batches 20]
  upload-history [--dry-run] [--full] [--until ISO] [--limit N] [--batch-size 500] [--concurrency 1..8] [--delay-ms 250] [--url URL]
      Default resumes from the local watermark (workspace-backfill-state.json) and scopes
      to rows created at-or-before the run start. --full re-walks everything (re-runs are
      safe: identical event ids upsert in place — run twice, nothing duplicates). --dry-run
      audits eligibility with zero network. Skipped rows are itemized with reasons; unpriced
      events stay unpriced in the audit.
  upload-history --repair-attribution [--until ISO] [--batch-size 500] [--concurrency 1..8] [--delay-ms 100] [--dry-run] [--url URL]
      Fill projectKey on already-uploaded workspace rows from the ledger's repo_hash
      column (the bulk ingest lane is first-writer-wins, so re-uploading cannot).
      Set-based and fill-only server-side; re-running reports updated: 0.
  upload-history --sessions [--until ISO] [--batch-size 500] [--concurrency 1..8] [--delay-ms 100] [--dry-run] [--url URL]
      Push one snapshot per stitched ledger session (issue 0037) so the workspace
      holds REAL session rows that join to their events. The cloud upserts
      grow-only by deterministic session id — re-running over the same --until
      changes nothing. The daemon refreshes touched sessions after each 5-minute
      sync; this command is the full backfill and the post-restart recovery tool.
  upload-replay --reason <receipt reason> [--since ISO-8601] [--limit N] [--dry-run]
      Supersede dead upload receipts whose reason is remote (remote_validation_rejected,
      remote_rejected_exhausted) and hand their raw rows back to the normal enqueue path.
      Local reasons are refused. --limit defaults to 500 and is capped at 5000; --since
      filters on when the delivery died. A delivery already re-queued or already
      acknowledged is counted as skipped, so re-running is a no-op — and an
      already-replayed row never consumes a slot of --limit and is never truncated by
      it, so a lifetime of replays can never crowd out or hide a dead letter written
      today. When a full --limit of ACTIONABLE candidates re-queues nothing the JSON
      carries a hint naming --since; inert skips alone never raise it. --dry-run
      classifies with zero writes.
  push-repo-labels [--dry-run] [--yes] [--url URL]
  sync-outcomes --repository owner/repo [--since-days 30] [--rework-window-days 14] [--until ISO] [--dry-run] [--url URL]
      Same fetch surface as the local efficiency report (pull list, check-runs and
      rework scan for joined PRs only — bounded; GITHUB_TOKEN/GH_TOKEN honored, optional
      for public repos). Naming the repo is the same deliberate disclosure as
      push-repo-labels: owner/name + remoteUrlHash cross; titles/diffs/paths never do.
      --dry-run computes the join and prints the audit without pushing.
  backfill-outcome-timeline --repository owner/repo [--since ISO] [--until ISO]
      [--max-prs 25] [--rework-window-days 14] [--store PATH] [--required-checks POLICY.json]
      Reads GitHub only. POLICY.json is {"requiredChecks":["check name"]};
      without it required-check coverage and check-derived metrics are UNKNOWN.
      GITHUB_TOKEN/GH_TOKEN stays provider-side and is never persisted or printed.
  backfill-outcome-performance [--repository owner/repo] [--store PATH]
      [--required-checks POLICY.json] [--rework-window-days 14]
      Local-only deterministic materialization of existing outcome-timeline facts.
   weekly-performance-rollup [--store PATH] [--out-dir DIR] [--until ISO]
       Generates weekly-performance-YYYY-MM-DD.{json,md}; explicit/on-demand only.
   materialize-learning-evidence [--ledger PATH] [--store PATH] [--state PATH]
       [--out PATH] [--until ISO] [--window-days N] [--max-new-events N]
       One deterministic high-watered pass joining allocation, token origin,
       materialized outcomes, work episodes, attempts, and prospective
       technique exposure into one versioned learning evidence packet.
       Manual/on-demand only; never scheduled continuously. Missing
       dependencies are reported as not_estimable, never substituted.
  install-launch-agent [--load] [--dry-run]
  install-launch-agent --dev [--repo-root PATH] [--pnpm PATH] [--load]
  load-launch-agent
  unload-launch-agent
  uninstall-launch-agent [--unload] [--dry-run]
  purge-local-data [--confirm] [--include-config]
  lifecycle update --operation-id ID --artifact self|BUNDLE.mjs --artifact-version V [--readiness-timeout-ms MS]
  lifecycle rollback --operation-id ID --artifact self|BUNDLE.mjs --artifact-version V
      Stage the digest-verified bundle (plus its vendored native dependencies)
      into the immutable versions/VERSION/darwin-ARCH runtime, repoint the owned
      LaunchAgent manifest at it, verify durable readiness, and restore the
      previous runtime/config/database/manifest on any failure. Never invokes
      launchctl; run "load-launch-agent" afterwards to restart the daemon on
      the new immutable runtime. "self" pins the currently running packaged
      bundle (npx/source checkouts are refused).
  lifecycle uninstall --operation-id ID [--apply]
      Preview (default) or remove ONLY owned targets: service manifest,
      runtime pointer and versions. Ledger, history, credentials, config, and
      workspace membership are retained; purge-only data is never touched.
  lifecycle purge --operation-id ID [--apply --confirm-exact "${PURGE_CONFIRMATION}"]
      Preview (default) or delete collector config, workspace credentials,
      ledger, history, and lifecycle snapshots. Requires both --apply and the
      exact confirmation string. Leaving a workspace or revoking a device are
      separate hosted operations and are NOT performed here.
  lifecycle support-bundle --operation-id ID
      Sanitized, bounded diagnostics: versions, coarse readiness, counters,
      aggregate log codes. No paths, prompts, tokens, or secrets.
`);
}

function openBuffer(
  config: CollectorConfig,
  deliveryOverride = false,
  databaseBusyTimeoutMs = 5_000,
) {
  ensureCollectorHome();
  const identity = loadOrCreateDeviceIdentity(undefined, {
    seed: { deviceId: config.deviceId, keyId: config.keyId },
  });
  recordDeviceSeen();
  return new LocalEventBuffer(collectorBufferPath(), {
    workspaceId: config.tenantId,
    deviceId: identity.deviceId,
    delivery: {
      enabled: Boolean(config.uploadUrl) || deliveryOverride,
      limits: config.delivery,
    },
    databaseBusyTimeoutMs,
  });
}

function flag(name: string) {
  return process.argv.includes(name);
}

function optionValue(name: string) {
  const index = process.argv.indexOf(name);
  if (index === -1) return undefined;
  return process.argv[index + 1];
}

function accountAssertionSourceFromArg(value: string | undefined): AccountAssertionSource {
  const normalized = value?.trim().toLowerCase().replace(/-/g, "_");
  if (normalized && (ACCOUNT_ASSERTION_SOURCES as readonly string[]).includes(normalized)) {
    return normalized as AccountAssertionSource;
  }
  throw new Error("Expected account assertion source codex, claude_code, or conductor.");
}

function accountAssertionMutationFromArgs() {
  const direct = process.argv[2] === "--disable-account-assertion" || process.argv[2] === "--enable-account-assertion" ||
    process.argv[2]?.startsWith("--disable-account-assertion=") || process.argv[2]?.startsWith("--enable-account-assertion=");
  const maintenance = process.argv[2] === "maintenance";
  if (!direct && !maintenance) return null;
  const inlineDisable = process.argv.findIndex(argument => argument.startsWith("--disable-account-assertion="));
  const inlineEnable = process.argv.findIndex(argument => argument.startsWith("--enable-account-assertion="));
  const disableIndex = inlineDisable >= 0 ? inlineDisable : process.argv.indexOf("--disable-account-assertion");
  const enableIndex = inlineEnable >= 0 ? inlineEnable : process.argv.indexOf("--enable-account-assertion");
  const actionCount = process.argv.filter(argument => argument === "--disable-account-assertion" ||
    argument === "--enable-account-assertion" || argument.startsWith("--disable-account-assertion=") ||
    argument.startsWith("--enable-account-assertion=")).length;
  if (actionCount > 1) throw new Error("Choose only one account assertion adapter action.");
  if (disableIndex !== -1 && enableIndex !== -1) throw new Error("Choose only one account assertion adapter action.");
  const index = disableIndex !== -1 ? disableIndex : enableIndex;
  if (index === -1) throw new Error("Usage: plimsoll maintenance --disable-account-assertion <source> --yes");
  const action = process.argv[index] ?? "";
  const inlineSource = action.includes("=") ? action.slice(action.indexOf("=") + 1) : undefined;
  const source = accountAssertionSourceFromArg(inlineSource ?? process.argv[index + 1]);
  if (!inlineSource && process.argv[index + 1]?.startsWith("--")) throw new Error("Account assertion source is required.");
  const allowed = new Set(["--yes", "--dry-run"]);
  for (const argument of process.argv.slice(3)) {
    if (argument === process.argv[index + 1] || argument === action || allowed.has(argument)) continue;
    if (argument === "--disable-account-assertion" || argument === "--enable-account-assertion") continue;
    throw new Error(`Unsupported account assertion maintenance option: ${argument}`);
  }
  const yes = flag("--yes");
  const dryRun = flag("--dry-run");
  if (yes && dryRun) throw new Error("Choose either --yes or --dry-run, not both.");
  return { enabled: enableIndex !== -1, source, yes, dryRun };
}

function collectorSourceFromArg(
  value: string | undefined,
): "claude_code" | "codex" | "grok" {
  if (value === "claude-code") return "claude_code";
  if (value === "codex") return "codex";
  if (value === "grok") return "grok";
  throw new Error("Expected source to be claude-code, codex, or grok.");
}

async function readStdin() {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }

  return Buffer.concat(chunks).toString("utf8");
}

function readSecretFromPrompt() {
  if (!process.stdin.isTTY || typeof process.stdin.setRawMode !== "function") {
    throw new Error("join --token-prompt requires an interactive terminal; use --token-stdin or --token-fd in automation.");
  }
  process.stderr.write("Join token (input hidden): ");
  process.stdin.setRawMode(true);
  process.stdin.resume();
  return new Promise<string>((resolve, reject) => {
    let secret = "";
    let onData: (chunk: Buffer | string) => void;
    const cleanup = () => {
      process.stdin.removeListener("data", onData);
      process.stdin.setRawMode?.(false);
      process.stderr.write("\n");
    };
    onData = (chunk: Buffer | string) => {
      for (const character of String(chunk)) {
        if (character === "\u0003") {
          cleanup();
          reject(new Error("Join token prompt cancelled."));
          return;
        } else if (character === "\r" || character === "\n") {
          cleanup();
          resolve(secret);
          return;
        } else if (character === "\u0008" || character === "\u007f") {
          secret = secret.slice(0, -1);
        } else {
          secret += character;
        }
      }
    };
    process.stdin.on("data", onData);
  });
}

function readSecretFromFd(value: string) {
  const fd = Number(value);
  if (!value.trim() || !Number.isSafeInteger(fd) || fd < 0) {
    throw new Error("join --token-fd requires a non-negative file descriptor.");
  }
  return fs.readFileSync(fd, "utf8");
}

function runLaunchctl(args: string[], setExitCode = true) {
  const result = spawnSync(args[0] ?? "launchctl", args.slice(1), {
    stdio: "inherit",
  });

  if (setExitCode && result.status !== 0) {
    process.exitCode = result.status ?? 1;
  }
  return result.status === 0;
}

function launchctlJobState(): LaunchAgentLabelObservation & {
  exitCode: number | null;
  errorCode: string | null;
} {
  const args = launchctlPrintCommand();
  const result = spawnSync(args[0] ?? "launchctl", args.slice(1), {
    encoding: "utf8",
    env: { ...process.env, LANG: "C", LC_ALL: "C" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const errorCode = (result.error as NodeJS.ErrnoException | undefined)?.code ?? null;
  const details = { exitCode: result.status, errorCode };
  if (result.error) return { kind: "query_failed", ...details };
  if (result.status !== 0) {
    const uid = typeof process.getuid === "function" ? process.getuid() : null;
    const notFoundLine = uid === null
      ? null
      : `Could not find service "${LAUNCH_AGENT_LABEL}" in domain for user gui: ${uid}`;
    const normalizedStderr = result.stderr.replace(/\r\n/g, "\n").trim();
    const knownNotFound =
      result.status === 113 &&
      result.stdout.trim() === "" &&
      notFoundLine !== null &&
      (normalizedStderr === notFoundLine ||
        normalizedStderr === `Bad request.\n${notFoundLine}`);
    return { kind: knownNotFound ? "not_reported" : "query_failed", ...details };
  }
  const pidMatch = result.stdout.match(/^\s*pid\s*=\s*(\d+)\s*$/m);
  const pid = pidMatch ? Number(pidMatch[1]) : null;
  // The label observation is compared against persisted identities, so it
  // must use the same UTC algorithm and carry the explicit tag.
  const processStartFingerprint = pid ? readUtcProcessStartFingerprint(pid) : null;
  return {
    kind: "reported",
    processIdentity: pid && processStartFingerprint
      ? {
          pid,
          processStartFingerprint,
          processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
        }
      : null,
    ...details,
  };
}

// Issue #158: one canonical mutation authority for every LaunchAgent
// mutation this CLI performs (install, uninstall, load, unload, owned-PID
// cleanup). Read-only inspection and doctor never acquire a lease.
function launchAgentMutationAuthority() {
  return new LifecycleMutationAuthority(defaultLifecycleAuthorityRoot());
}

type LaunchAgentFence =
  | { kind: "unfenced" }
  | { kind: "held"; lease: LifecycleMutationLease }
  | { kind: "busy" }
  | { kind: "ambiguous" };

function acquireLaunchAgentFence(authority?: LifecycleMutationAuthority): LaunchAgentFence {
  if (!authority) return { kind: "unfenced" };
  const acquisition = authority.acquire();
  if (acquisition.kind === "acquired") return { kind: "held", lease: acquisition.lease };
  return acquisition.kind === "busy" ? { kind: "busy" } : { kind: "ambiguous" };
}

function releaseLaunchAgentFence(fence: LaunchAgentFence) {
  if (fence.kind !== "held") return;
  // Release touches only this process's own record; superseded or expired
  // owners cannot affect a successor.
  fence.lease.release();
}

function assertLaunchAgentFence(fence: LaunchAgentFence) {
  if (fence.kind !== "held") return;
  const state = fence.lease.assertCurrent();
  if (!state.ok) throw new Error(`LIFECYCLE_FENCE_${state.reason.toUpperCase()}`);
}

/** Wraps owned-PID-file removal so a stale authority authorizes no
 * destructive cleanup: the fence is revalidated immediately before the
 * removal attempt, and a lost fence reports a non-attempted cleanup while
 * the surrounding observer keeps reporting true terminal state. */
function fencedPidRemover(fence: LaunchAgentFence) {
  if (fence.kind !== "held") return undefined;
  return (pidPath: string, identity: Parameters<typeof removeCollectorPidFileIfOwned>[1], label: string) => {
    const state = fence.lease.assertCurrent();
    if (!state.ok) {
      return {
        removed: false,
        ambiguous: false,
        quarantined: false,
        persistent: readCollectorPidCleanupState(pidPath, label),
        disposition: "operation_failed" as const,
      };
    }
    return removeCollectorPidFileIfOwned(pidPath, identity, label);
  };
}

/** Wraps cleanup-state reconciliation so retirement of another actor's
 * artifacts also revalidates the fence immediately before acting. */
function fencedReconciler(fence: LaunchAgentFence) {
  if (fence.kind !== "held") return undefined;
  return (pidPath: string, label: string) => {
    const state = fence.lease.assertCurrent();
    if (!state.ok) {
      return {
        reconciled: false,
        eligible: false,
        disposition: "clear_race" as const,
        before: readCollectorPidCleanupState(pidPath, label),
        after: readCollectorPidCleanupState(pidPath, label),
      };
    }
    return reconcileCollectorPidCleanupState(pidPath, label);
  };
}

// Issue #148: a launchctl bootstrap success says launchd accepted the job —
// it does NOT say a collector is actually serving. The load receipt must
// carry both facts separately, so after bootstrap we probe /status for a
// bounded window and record what was proven.
const LOAD_READINESS_TIMEOUT_MS = 2_000;
const LOAD_READINESS_POLL_MS = 100;

export type LaunchAgentLoadReadiness = {
  verified: boolean;
  listenerState: CollectorListenerObservation["kind"];
  runtimeLive: boolean | null;
  elapsedMs: number;
  observations: number;
  deadlineCrossed: boolean;
};

async function verifyPostBootstrapReadiness(
  port: number,
  options: { timeoutMs?: number; pollIntervalMs?: number } = {},
): Promise<LaunchAgentLoadReadiness> {
  const timeoutMs = Math.max(0, options.timeoutMs ?? LOAD_READINESS_TIMEOUT_MS);
  const pollIntervalMs = Math.max(1, options.pollIntervalMs ?? LOAD_READINESS_POLL_MS);
  const startedAt = Date.now();
  const deadline = startedAt + timeoutMs;
  let observations = 0;
  let listenerState: CollectorListenerObservation["kind"] = "absent";
  let runtimeLive: boolean | null = null;
  let deadlineCrossed = false;
  while (true) {
    const observed = await observeCollectorListener(port);
    observations += 1;
    listenerState = observed.kind;
    runtimeLive =
      observed.kind === "collector" ? processIdentityIsLive(observed.runtimeIdentity) : null;
    if (observed.kind === "collector" && runtimeLive === true) break;
    const now = Date.now();
    if (now >= deadline) {
      deadlineCrossed = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(pollIntervalMs, deadline - now)));
  }
  return {
    verified: listenerState === "collector" && runtimeLive === true,
    listenerState,
    runtimeLive,
    elapsedMs: Math.max(0, Date.now() - startedAt),
    observations,
    deadlineCrossed,
  };
}

async function loadVisibleLaunchAgent(
  plistPath: string,
  port: number,
  manifestChanged = false,
  authority?: LifecycleMutationAuthority,
) {
  const visible = inspectLaunchAgentManifest();
  if (!visible.ok || visible.plistPath !== plistPath) {
    return { loaded: false, status: "visible_manifest_invalid" as const, manifestDigest: null };
  }
  const fence = acquireLaunchAgentFence(authority);
  if (fence.kind === "busy" || fence.kind === "ambiguous") {
    return {
      loaded: false,
      status: fence.kind === "busy" ? "lifecycle_fence_busy" as const : "lifecycle_fence_ambiguous" as const,
      manifestDigest: visible.manifestDigest,
    };
  }
  try {
    const pidPath = collectorLogPath("collector.pid");
    const observeLabel = () => launchctlJobState();
    const observeListener = () => observeCollectorListener(port);
    const prior = await captureLaunchAgentUnloadPriorState({
      label: LAUNCH_AGENT_LABEL,
      pidPath,
      port,
      observeLabel,
      observeListener,
      reconcileCleanupState: fencedReconciler(fence),
    });
    if (prior.label.kind === "reported") {
      if (prior.ownership !== "consistent") {
        return {
          loaded: false,
          status: `prior_owner_${prior.ownership}` as const,
          manifestDigest: visible.manifestDigest,
          manifestIdentityDigest: visible.manifestIdentityDigest,
          prior: unloadPriorReceipt(prior),
        };
      }
      if (manifestChanged) {
        return {
          loaded: false,
          status: "loaded_job_requires_explicit_reload" as const,
          manifestDigest: visible.manifestDigest,
          manifestIdentityDigest: visible.manifestIdentityDigest,
        };
      }
      return {
        loaded: true,
        status: "already_loaded" as const,
        manifestDigest: visible.manifestDigest,
        manifestIdentityDigest: visible.manifestIdentityDigest,
      };
    }
    const terminal = await observeLaunchAgentUnloadTerminalState({
      label: LAUNCH_AGENT_LABEL,
      pidPath,
      port,
      prior,
      timeoutMs: 0,
      observeLabel,
      observeListener,
      reconcileCleanupState: fencedReconciler(fence),
      removePidFile: fencedPidRemover(fence),
    });
    if (!terminal.stopped) {
      return {
        loaded: false,
        status: `prior_state_${terminal.state}` as const,
        manifestDigest: visible.manifestDigest,
        manifestIdentityDigest: visible.manifestIdentityDigest,
        prior: unloadPriorReceipt(prior),
        terminal: terminal.final,
      };
    }
    assertLaunchAgentFence(fence);
    const loaded = runLaunchctl(launchctlBootstrapCommand(plistPath));
    if (!loaded) {
      return {
        loaded: false,
        status: "launchctl_failed" as const,
        manifestDigest: visible.manifestDigest,
        manifestIdentityDigest: visible.manifestIdentityDigest,
      };
    }
    let after: ReturnType<typeof inspectLaunchAgentManifest> | null = null;
    try {
      after = inspectLaunchAgentManifest();
    } catch {
      after = null;
    }
    const unchangedAfterBootstrap = Boolean(
      after?.ok &&
      after.plistPath === plistPath &&
      after.manifestDigest === visible.manifestDigest &&
      after.manifestIdentityDigest === visible.manifestIdentityDigest &&
      after.mode === visible.mode,
    );
    if (!unchangedAfterBootstrap) {
      assertLaunchAgentFence(fence);
      const bootoutSucceeded = runLaunchctl(launchctlBootoutCommand());
      const labelStateAfterBootout = launchctlJobState();
      const labelReportedAfterBootout = labelStateAfterBootout.kind === "reported";
      return {
        loaded: false,
        status: "post_bootstrap_manifest_changed" as const,
        manifestDigest: visible.manifestDigest,
        manifestIdentityDigest: visible.manifestIdentityDigest,
        postBootstrapManifestDigest: after?.ok ? after.manifestDigest : null,
        postBootstrapManifestIdentityDigest: after?.ok ? after.manifestIdentityDigest : null,
        cleanup: {
          bootoutAttempted: true,
          bootoutSucceeded,
          labelReportedAfterBootout,
          labelQueryExitCode: labelStateAfterBootout.exitCode,
          labelQueryErrorCode: labelStateAfterBootout.errorCode,
          labelState: labelReportedAfterBootout
            ? "reported" as const
            : labelStateAfterBootout.kind === "query_failed"
              ? "query_failed" as const
              : "not_reported" as const,
          status: !bootoutSucceeded
            ? "bootout_failed" as const
            : labelReportedAfterBootout
              ? "bootout_succeeded_label_still_reported" as const
              : labelStateAfterBootout.kind === "query_failed"
                ? "bootout_succeeded_label_query_failed" as const
                : "bootout_succeeded_label_not_reported" as const,
        },
      };
    }
    return {
      loaded: true,
      status: "bootstrap_succeeded" as const,
      manifestDigest: visible.manifestDigest,
      manifestIdentityDigest: visible.manifestIdentityDigest,
      // Issue #148: bootstrap truth and collector readiness are separate
      // facts. `loaded` stays literal; `readiness` records whether a live
      // collector actually answered /status within the bounded window.
      readiness: await verifyPostBootstrapReadiness(port),
    };
  } finally {
    releaseLaunchAgentFence(fence);
  }
}

function unloadPriorReceipt(prior: LaunchAgentUnloadPriorState) {
  return {
    labelState: prior.label.kind,
    labelProcessIdentity:
      prior.label.kind === "reported" ? prior.label.processIdentity : null,
    listenerState: prior.listener.kind,
    listenerRuntimeIdentity: prior.listenerRuntimeIdentity,
    pidRecordState: prior.pidRecordKind,
    pidRuntimeIdentity: prior.pidRuntimeIdentity,
    pidCleanup: pidCleanupStateReceipt(prior.pidCleanupState),
    pidCleanupReconciliation: pidCleanupReconciliationReceipt(
      prior.pidCleanupReconciliation,
    ),
    ownership: prior.ownership,
  };
}

async function executeLaunchAgentUnload(port: number, authority?: LifecycleMutationAuthority): Promise<{
  unloaded: boolean;
  reason: "launchctl_failed" | "lifecycle_fence_busy" | "lifecycle_fence_ambiguous" | LaunchAgentUnloadOutcome["state"] | null;
  status: "already_stopped" | "stopped" | "stopped_after_launchctl_failure" | "refused";
  bootoutAttempted: boolean;
  bootoutSucceeded: boolean | null;
  prior: ReturnType<typeof unloadPriorReceipt> | null;
  outcome: LaunchAgentUnloadOutcome | null;
}> {
  const fence = acquireLaunchAgentFence(authority);
  if (fence.kind === "busy" || fence.kind === "ambiguous") {
    return {
      unloaded: false,
      reason: fence.kind === "busy" ? "lifecycle_fence_busy" : "lifecycle_fence_ambiguous",
      status: "refused",
      bootoutAttempted: false,
      bootoutSucceeded: null,
      prior: null,
      outcome: null,
    };
  }
    try {
    const pidPath = collectorLogPath("collector.pid");
    const observeLabel = () => launchctlJobState();
    const observeListener = () => observeCollectorListener(port);
    const prior = await captureLaunchAgentUnloadPriorState({
      label: LAUNCH_AGENT_LABEL,
      pidPath,
      port,
      observeLabel,
      observeListener,
      reconcileCleanupState: fencedReconciler(fence),
    });

    if (prior.label.kind !== "reported") {
      const outcome = await observeLaunchAgentUnloadTerminalState({
        label: LAUNCH_AGENT_LABEL,
        pidPath,
        port,
        prior,
        timeoutMs: 0,
        observeLabel,
        observeListener,
        reconcileCleanupState: fencedReconciler(fence),
        removePidFile: fencedPidRemover(fence),
      });
      return {
        unloaded: outcome.stopped,
        reason: outcome.stopped ? null : outcome.state,
        status: outcome.stopped ? "already_stopped" : "refused",
        bootoutAttempted: false,
        bootoutSucceeded: null,
        prior: unloadPriorReceipt(prior),
        outcome,
      };
    }

    assertLaunchAgentFence(fence);
    const bootoutSucceeded = runLaunchctl(launchctlBootoutCommand(), false);
    const outcome = await observeLaunchAgentUnloadTerminalState({
      label: LAUNCH_AGENT_LABEL,
      pidPath,
      port,
      prior,
      timeoutMs: bootoutSucceeded ? 4_000 : 0,
      observeLabel,
      observeListener,
      reconcileCleanupState: fencedReconciler(fence),
      removePidFile: fencedPidRemover(fence),
    });
    // Provider/action truth remains literal in bootoutSucceeded, while terminal
    // state truth decides whether the requested unload has actually completed.
    const unloaded = outcome.stopped;
    return {
      unloaded,
      reason: unloaded ? null : bootoutSucceeded ? outcome.state : "launchctl_failed",
      status: unloaded
        ? bootoutSucceeded ? "stopped" : "stopped_after_launchctl_failure"
        : "refused",
      bootoutAttempted: true,
      bootoutSucceeded,
      prior: unloadPriorReceipt(prior),
      outcome,
    };
  } finally {
    releaseLaunchAgentFence(fence);
  }
}

function launchAgentUnloadReceipt(
  result: Awaited<ReturnType<typeof executeLaunchAgentUnload>>,
) {
  const pidPath = collectorLogPath("collector.pid");
  return {
    unloaded: result.unloaded,
    status: result.status,
    reason: result.reason,
    label: LAUNCH_AGENT_LABEL,
    bootoutAttempted: result.bootoutAttempted,
    bootoutSucceeded: result.bootoutSucceeded,
    ...(result.outcome
      ? {
          pidCleaned: result.outcome.pidCleaned,
          removedPidFile: result.outcome.removedPidFile,
          pidCleanupAmbiguous: result.outcome.pidCleanupAmbiguous,
          pidCleanupQuarantined: result.outcome.pidCleanupQuarantined,
          prior: result.prior,
          terminal: result.outcome.final,
          timing: result.outcome.timing,
        }
      : { lifecycleFenceRefused: true as const }),
    pidPathHash: privatePathReceipt(pidPath),
  };
}

async function checkCollectorConnectivity(port: number, managementToken?: string) {
  const controller = new AbortController();
  const timeoutMs = Number(process.env.PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS ?? "3000");
  const timeout = setTimeout(() => controller.abort(), Number.isFinite(timeoutMs) ? timeoutMs : 3000);

  try {
    const response = await fetch(`http://127.0.0.1:${port}/status`, {
      signal: controller.signal,
      // Issue 0056 (#104): enforcing daemons gate status behind the
      // provisioned management credential; doctor presents it when present.
      headers: managementToken ? { "x-plimsoll-token": managementToken } : {},
    });
    let body: Record<string, unknown> | null = null;
    try {
      const candidate = await response.json();
      if (candidate && typeof candidate === "object" && !Array.isArray(candidate)) {
        body = candidate as Record<string, unknown>;
      }
    } catch {
      // A non-JSON or malformed response is not a Plimsoll-ready service.
    }

    const runtimeCandidate = body?.runtimeIdentity;
    const runtimeIdentity =
      runtimeCandidate &&
      typeof runtimeCandidate === "object" &&
      Number.isInteger((runtimeCandidate as Partial<CollectorRuntimeIdentity>).pid) &&
      typeof (runtimeCandidate as Partial<CollectorRuntimeIdentity>).instanceId === "string" &&
      (runtimeCandidate as Partial<CollectorRuntimeIdentity>).instanceId!.length >= 32 &&
      typeof (runtimeCandidate as Partial<CollectorRuntimeIdentity>).processStartFingerprint === "string" &&
      (runtimeCandidate as Partial<CollectorRuntimeIdentity>).processStartFingerprint!.startsWith("sha256:")
        ? (runtimeCandidate as CollectorRuntimeIdentity)
        : null;
    // Issue #135 runtime attestation: the live daemon reports the path-free
    // identity of the home it actually runs against. Null means the daemon
    // predates home attestation; an explicit mismatch is drift.
    const daemonHomeHash = body?.homeIdentityHash;
    const homeIdentityHash =
      typeof daemonHomeHash === "string" && /^sha256:[0-9a-f]{64}$/.test(daemonHomeHash)
        ? daemonHomeHash
        : null;
    const health = body?.health && typeof body.health === "object"
      ? (body.health as { sources?: unknown })
      : null;
    const healthSources = Array.isArray(health?.sources) ? health.sources : [];
    const sources = healthSources.flatMap((candidate) => {
      if (!candidate || typeof candidate !== "object") return [];
      const source = (candidate as { source?: unknown }).source;
      const lastTokenEventAt = (candidate as { lastTokenEventAt?: unknown }).lastTokenEventAt;
      if (source !== "claude_code" && source !== "codex") return [];
      return [{
        source,
        lastTokenEventAt: typeof lastTokenEventAt === "string" ? lastTokenEventAt : null,
      }];
    });
    const stats = body?.stats && typeof body.stats === "object"
      ? (body.stats as { tokenAttributedEvents?: unknown })
      : null;
    const tokenAttributedEvents = Number(stats?.tokenAttributedEvents ?? 0);
    const signalVerified =
      sources.some((source) => source.lastTokenEventAt !== null) ||
      (Number.isFinite(tokenAttributedEvents) && tokenAttributedEvents > 0);

    const retentionValue = body?.retention;
    const retention = retentionValue && typeof retentionValue === "object" && !Array.isArray(retentionValue)
      ? retentionValue as Record<string, unknown>
      : null;
    const retentionStates = retention?.states;
    const retentionPolicy = retention?.policy;
    const retentionLastPass = retention?.lastPass;
    const safeRetention = response.ok && body?.ok === true && retention &&
      (retention.inspection === "complete" || retention.inspection === "bounded") &&
      retentionPolicy && typeof retentionPolicy === "object" && !Array.isArray(retentionPolicy) &&
      retentionStates && typeof retentionStates === "object" && !Array.isArray(retentionStates) &&
      retentionLastPass && typeof retentionLastPass === "object" && !Array.isArray(retentionLastPass)
      ? {
          inspection: retention.inspection as "complete" | "bounded",
          policy: {
            retentionDays: Number((retentionPolicy as Record<string, unknown>).retentionDays),
            cutoffAt: String((retentionPolicy as Record<string, unknown>).cutoffAt),
          },
          states: {
            retained: (retentionStates as Record<string, unknown>).retained === null ? null
              : Number((retentionStates as Record<string, unknown>).retained),
            pendingDelivery: (retentionStates as Record<string, unknown>).pendingDelivery === null ? null
              : Number((retentionStates as Record<string, unknown>).pendingDelivery),
            quarantined: (retentionStates as Record<string, unknown>).quarantined === null ? null
              : Number((retentionStates as Record<string, unknown>).quarantined),
            expired: Number((retentionStates as Record<string, unknown>).expired),
            notInspected: Number((retentionStates as Record<string, unknown>).notInspected),
          },
          lastPass: {
            rowsVisited: Number((retentionLastPass as Record<string, unknown>).rowsVisited),
            rowsExpired: Number((retentionLastPass as Record<string, unknown>).rowsExpired),
            hasMore: Boolean((retentionLastPass as Record<string, unknown>).hasMore),
            at: typeof (retentionLastPass as Record<string, unknown>).at === "string"
              ? (retentionLastPass as Record<string, unknown>).at as string
              : null,
          },
        }
      : null;
    const enrollmentValue = body?.enrollment;
    const enrollment = enrollmentValue && typeof enrollmentValue === "object" && !Array.isArray(enrollmentValue)
      ? enrollmentValue as Record<string, unknown>
      : null;
    const safeEnrollment = response.ok && body?.ok === true && enrollment?.futureOnlyEnrollment === true
      ? {
          futureOnlyEnrollment: true as const,
          quarantinedHistoryRows: Number.isSafeInteger(enrollment.quarantinedHistoryRows)
            ? Number(enrollment.quarantinedHistoryRows)
            : null,
        }
      : null;

    return {
      reachable: response.ok && body?.ok === true,
      status: response.status,
      statusUrl: `http://127.0.0.1:${port}/status`,
      runtimeIdentity,
      homeIdentityHash,
      retention: safeRetention,
      enrollment: safeEnrollment,
      signal: {
        verified: signalVerified,
        tokenAttributedEvents:
          Number.isFinite(tokenAttributedEvents) && tokenAttributedEvents >= 0
            ? tokenAttributedEvents
            : null,
        sources,
      },
    };
  } catch (error) {
    return {
      reachable: false,
      error: error instanceof Error ? error.name : String(error),
      statusUrl: `http://127.0.0.1:${port}/status`,
      runtimeIdentity: null,
      homeIdentityHash: null,
      retention: null,
      enrollment: null,
      signal: {
        verified: false,
        tokenAttributedEvents: null,
        sources: [],
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

function readClaudeTelemetryConfig(file: string, expected: ReturnType<typeof generateClaudeCodeSettings>) {
  if (!fs.existsSync(file)) {
    return { ok: false, status: "missing" as const, path: file, missing: ["settings file"] };
  }
  try {
    const current = JSON.parse(fs.readFileSync(file, "utf8")) as {
      env?: Record<string, unknown>;
      hooks?: Record<string, unknown[]>;
    };
    const missing: string[] = [];
    for (const [key, value] of Object.entries(expected.env)) {
      if (current.env?.[key] !== value) missing.push(`env.${key}`);
    }
    for (const [event, entries] of Object.entries(expected.hooks ?? {})) {
      const currentEntries = Array.isArray(current.hooks?.[event]) ? current.hooks[event] : [];
      for (const entry of entries) {
        if (!currentEntries.some((candidate) => isDeepStrictEqual(candidate, entry))) {
          missing.push(`hooks.${event}`);
        }
      }
    }
    return {
      ok: missing.length === 0,
      status: missing.length === 0 ? "valid" as const : "incomplete" as const,
      path: file,
      missing,
    };
  } catch {
    return { ok: false, status: "invalid" as const, path: file, missing: ["readable JSON"] };
  }
}

/**
 * Value-blind fill receipt for doctor: the producer audiences a legacy
 * credential file is missing and cannot be given, because the credential home
 * refuses the fill write. The collector serves those audiences from memory, so
 * no config on this host can hold them and those sources are refused after a
 * restart. Reported only when the condition holds, so a healthy doctor payload
 * is byte-identical to before; readiness is unchanged (see REPORT).
 */
function producerAudienceFillReceipt(home: string) {
  const audiences = unpersistedProducerAudiences(home);
  if (audiences.length === 0) return {};
  return {
    producerAudiencesUnpersisted: {
      code: "local_ingest_auth_audiences_unpersisted",
      audiences,
    },
  };
}

/** Value-blind rotation receipt for doctor: state and deadline, never a token. */
function producerTokenRotationReceipt(auth: LocalIngestAuth | null) {
  return Object.fromEntries(
    (["claude_code", "codex", "gemini_cli", "grok"] as const).map((source) => [
      source,
      producerRotationState(auth, source),
    ]),
  );
}

function readCodexTelemetryConfig(file: string, expectedToml: string) {
  if (!fs.existsSync(file)) {
    return { ok: false, status: "missing" as const, path: file, missing: ["config file"] };
  }
  try {
    const isRecord = (value: unknown): value is Record<string, unknown> => {
      return Boolean(value && typeof value === "object" && !Array.isArray(value));
    };
    const containsExpected = (actual: unknown, expected: unknown): boolean => {
      if (Array.isArray(expected)) {
        return Array.isArray(actual) &&
          expected.every((expectedEntry) =>
            actual.some((actualEntry) => containsExpected(actualEntry, expectedEntry))
          );
      }
      if (isRecord(expected)) {
        return isRecord(actual) &&
          Object.entries(expected).every(([key, value]) =>
            Object.hasOwn(actual, key) && containsExpected(actual[key], value)
          );
      }
      return isDeepStrictEqual(actual, expected);
    };
    const expected = parseToml(expectedToml) as Record<string, unknown>;
    const current = parseToml(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
    const missing: string[] = [];
    const collectMissing = (actual: unknown, required: unknown, keyPath: string) => {
      if (containsExpected(actual, required)) return;
      if (Array.isArray(required)) {
        if (!Array.isArray(actual)) {
          missing.push(keyPath);
          return;
        }
        for (let index = 0; index < required.length; index += 1) {
          if (!actual.some((candidate) => containsExpected(candidate, required[index]))) {
            missing.push(`${keyPath}[${index}]`);
          }
        }
        return;
      }
      if (isRecord(required)) {
        if (!isRecord(actual)) {
          missing.push(keyPath);
          return;
        }
        for (const [key, value] of Object.entries(required)) {
          const childPath = keyPath ? `${keyPath}.${key}` : key;
          if (!Object.hasOwn(actual, key)) {
            missing.push(childPath);
          } else {
            collectMissing(actual[key], value, childPath);
          }
        }
        return;
      }
      missing.push(keyPath);
    };
    collectMissing(current, expected, "");
    return {
      ok: missing.length === 0,
      status: missing.length === 0 ? "valid" as const : "incomplete" as const,
      path: file,
      missing,
    };
  } catch {
    return { ok: false, status: "invalid" as const, path: file, missing: ["valid TOML"] };
  }
}

/**
 * Doctor's readback for one managed-config target (bead eco-6hoxj.50). The
 * self-healing cadence decides from exactly the same readback that produces the
 * `claude_seat_settings_unmanaged` / `codex_profile_config_unmanaged`
 * diagnostics, so a healthy doctor payload and a zero-write cadence are the
 * same fact.
 */
function managedConfigTargetReadback(
  target: ManagedConfigTarget,
  options: Parameters<typeof generateClaudeCodeSettings>[0],
): ManagedConfigReadback {
  return target.family === "claude"
    ? readClaudeTelemetryConfig(target.path, generateClaudeCodeSettings(options))
    : readCodexTelemetryConfig(target.path, generateCodexConfigToml(options));
}

/**
 * One self-healing reconcile tick for the running collector (bead eco-6hoxj.50).
 *
 * Composed from the same targets `setup` manages and gated by
 * `decideManagedConfigReconcile`, so the kill-switch is honoured before any
 * clock read and the drift readback is performed only when the cadence is
 * actually due. A host with no Plimsoll-local credentials, or whose managed
 * Codex header file is absent, has nothing this cadence may write: reconcile
 * re-applies what `setup --yes` installed and never provisions.
 *
 * `repoRoot` is the daemon's cwd exactly as doctor uses it; neither generated
 * Claude settings nor the generated Codex config reads it, so the managed
 * content a cadence plans is byte-identical to the content setup plans.
 */
async function runManagedConfigReconcileTick(
  bootConfig: CollectorConfig,
  options: { now?: number } = {},
): Promise<{
  decision: ManagedConfigReconcileDecision;
  result: ManagedConfigReconcileResult | null;
  settings: ManagedConfigReconcileSettings;
}> {
  const now = options.now ?? Date.now();
  const home = collectorHome();
  // The kill-switch and the cadence are read from the config file on every
  // tick, not from the object captured at boot (review r1, F3).
  const settings = readManagedConfigReconcileSettings(
    collectorConfigPath(),
    bootConfig.managedConfig.reconcile,
  );
  const claudeFile = path.join(os.homedir(), ".claude", "settings.json");
  const codexFile = path.join(os.homedir(), ".codex", "config.toml");
  const codexHeaderFile = path.join(path.dirname(codexFile), "plimsoll.headers");
  const auth = readLocalIngestAuth(home);
  const toolOptions = {
    repoRoot: process.cwd(),
    port: bootConfig.port,
    dataMode: bootConfig.policy.dataMode,
    codexHeaderFile,
    grokHeaderFile: path.join(resolveGrokHome().home, "hooks", "plimsoll.headers"),
    ...(auth
      ? {
          claudeCodeProducerToken: auth.claudeCodeProducer,
          codexProducerToken: auth.codexProducer,
          geminiCliProducerToken: auth.geminiCliProducer,
          grokProducerToken: auth.grokProducer,
        }
      : {}),
  };
  // `includeAbsent` composes a seat/profile directory whose config file does
  // not exist yet, so it is reported as `skipped: absent` instead of silently
  // dropping out of the report (review r1, F7). It is still never provisioned.
  const targets: ManagedConfigTarget[] = auth === null
    ? []
    : [
        ...composeManagedClaudeTargets(claudeFile, os.homedir(), { includeAbsent: true }),
        ...(fs.existsSync(codexHeaderFile)
          ? composeManagedCodexTargets(codexFile, os.homedir(), { includeAbsent: true })
          : []),
      ];
  const state = readManagedConfigReconcileState(home);
  const lastRunAt = state.lastRunAt ? Date.parse(state.lastRunAt) : null;
  const drift: { report: ManagedConfigDriftReport | null } = { report: null };
  const decision = await decideManagedConfigReconcileAsync({
    enabled: settings.enabled,
    intervalSeconds: settings.intervalSeconds,
    now,
    lastRunAt: Number.isFinite(lastRunAt) ? lastRunAt : null,
    // The readback yields between targets so a fleet-scale host never owes the
    // collector's HTTP loop a whole drift pass in one chunk (review r1, F6).
    drift: async () => {
      if (targets.length === 0) return 0;
      drift.report = await managedConfigDriftReportAsync(
        targets,
        toolOptions,
        managedConfigTargetReadback,
      );
      return drift.report.drifted;
    },
  });
  if (!decision.run) {
    // A tick that ran its readback and found nothing still stamps, so doctor
    // can answer "the cadence ran and found nothing" (review r1, F2).
    stampManagedConfigReconcileDecision(home, decision, {
      at: new Date(now).toISOString(),
      absent: drift.report
        ? drift.report.targets.filter((entry) => entry.status === "missing").length
        : 0,
      // A host with no Plimsoll-local credentials manages nothing at all, so it
      // must not stamp the same `unchanged` a healthy host stamps
      // (review r2, R6).
      ...(auth === null ? { result: "unavailable" as const } : {}),
    });
    return { decision, result: null, settings };
  }
  return {
    decision,
    settings,
    result: await runManagedConfigReconcileAsync({
      collectorHome: home,
      targets,
      toolOptions,
      now: () => now,
    }),
  };
}

/**
 * A dry run mints nothing, so the rotation plan for a discovered profile is
 * previewed against this placeholder instead of a real successor token. It is
 * never written (the preview is a dry-run apply), never printed, and is not a
 * credential: it only makes the reconciler report the managed exporter header
 * tables a real rotation would rewrite.
 */
const ROTATION_PREVIEW_TOKEN = "plimsoll-rotation-preview-placeholder";

const CODEX_MANAGED_EXPORTERS = ["exporter", "trace_exporter", "metrics_exporter"] as const;

/**
 * Whether a discovered Codex seat profile is an authenticated consumer of the
 * codex producer token (bead eco-6hoxj.54).
 *
 * `managed` means the file already carries the managed token header in at least
 * one `[otel]` exporter table, whatever its value, so a profile that missed an
 * earlier rotation is still rotated back into service rather than left holding
 * a dead token. `unmanaged` means the fleet conductor's own config was never
 * given the managed block: rotation supersedes credentials, it never
 * provisions, so that profile is left byte-identical for the next
 * `setup --yes`. `malformed` is fleet-conductor state that Plimsoll cannot
 * parse; it is reported and never rewritten, exactly as `setup` reports it
 * without failing the run.
 */
function codexProfileTokenState(file: string): "managed" | "unmanaged" | "malformed" {
  const record = (value: unknown) =>
    value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : undefined;
  let parsed: Record<string, unknown> | undefined;
  try {
    parsed = record(parseToml(fs.readFileSync(file, "utf8")));
  } catch {
    return "malformed";
  }
  const otel = record(parsed?.otel);
  const carriesToken = CODEX_MANAGED_EXPORTERS.some((exporter) => {
    const headers = record(record(record(otel?.[exporter])?.["otlp-http"])?.headers);
    return headers !== undefined && Object.hasOwn(headers, "x-plimsoll-token");
  });
  return carriesToken ? "managed" : "unmanaged";
}

/**
 * Value-blind receipt for the discovered profiles a rotation did not rewrite:
 * slug, path and reason, never a token and never a malformed byte. Omitted
 * entirely when there is nothing to report, so a host without fleet profiles
 * prints exactly the payload it printed before this bead.
 */
function skippedProfilesReceipt(
  skipped: ReadonlyArray<{ profile: { slug: string; path: string }; state: string }>,
) {
  if (skipped.length === 0) return {};
  return {
    profilesSkipped: skipped.map(({ profile, state }) => ({
      slug: profile.slug,
      path: profile.path,
      status: "skipped" as const,
      reason: state === "malformed"
        ? "codex_profile_config_unreadable"
        : "codex_profile_config_unmanaged",
    })),
  };
}

/**
 * A doctor-reportable path for a discovered seat or profile: never an absolute
 * path outside `$HOME` (bead eco-6hoxj.54, review r1 finding 3).
 *
 * Discovery deliberately reports a symlinked seat/profile at its resolved path
 * so the managed-config guard and every apply receipt name the file actually
 * written (claude-seats.ts / codex-profiles.ts). For a seat the tooling
 * relocated to shared storage that resolved path is outside the home, and a
 * doctor receipt shipped off-box would then carry a filesystem layout that is
 * none of Plimsoll's business. The link path under `$HOME` names the same seat
 * without it, so that is what doctor reports, with `outsideHome: true` so the
 * receipt still says the file is not where it looks. Nothing else changes: a
 * seat that lives under the home is reported exactly as before.
 */
function homeScopedDiscoveredPath(resolved: string, linkPath: string, home: string) {
  return isInsideHome(resolved, home) ? { path: resolved } : { path: linkPath, outsideHome: true as const };
}

/** True when `file` is the home or sits under it, before or after the home itself resolves. */
function isInsideHome(file: string, home: string) {
  const roots = new Set([path.resolve(home)]);
  try {
    roots.add(fs.realpathSync(home));
  } catch {
    // An unreadable home cannot widen the boundary; the literal path still holds.
  }
  return [...roots].some((root) => file === root || file.startsWith(root + path.sep));
}

function readLaunchAgentState(plistPath: string) {
  if (!fs.existsSync(plistPath)) {
    return {
      ok: false,
      installed: false,
      status: "missing" as const,
      label: LAUNCH_AGENT_LABEL,
      plistPath,
      runtime: null,
      homeIdentity: null,
      path: null,
    };
  }
  try {
    const parsed = spawnSync(
      "/usr/bin/plutil",
      ["-convert", "json", "-o", "-", "--", plistPath],
      {
        encoding: "utf8",
        env: { LANG: "C", LC_ALL: "C", PATH: "/usr/bin:/bin" },
        stdio: ["ignore", "pipe", "pipe"],
        timeout: 2_000,
      },
    );
    if (parsed.error || parsed.status !== 0 || typeof parsed.stdout !== "string" || !parsed.stdout.trim()) {
      return {
        ok: false,
        installed: true,
        status: "invalid" as const,
        label: LAUNCH_AGENT_LABEL,
        plistPath,
        runtime: null,
        path: null,
      };
    }
    const plist = JSON.parse(parsed.stdout) as Record<string, unknown>;
    const programArguments = Array.isArray(plist.ProgramArguments) &&
      plist.ProgramArguments.every((value) => typeof value === "string")
      ? plist.ProgramArguments as string[]
      : [];
    const workingDirectory = typeof plist.WorkingDirectory === "string"
      ? plist.WorkingDirectory
      : null;
    const developmentRuntime =
      programArguments.length === 5 &&
      path.isAbsolute(programArguments[0] ?? "") &&
      path.basename(programArguments[0] ?? "") === "pnpm" &&
      programArguments[1] === "--dir" &&
      path.isAbsolute(programArguments[2] ?? "") &&
      programArguments[2] === workingDirectory &&
      programArguments[3] === "collector" &&
      programArguments[4] === "start";
    const packagedRuntime =
      programArguments.length === 3 &&
      programArguments[0] === process.execPath &&
      path.isAbsolute(programArguments[1] ?? "") &&
      /\.(mjs|cjs|js)$/.test(programArguments[1] ?? "") &&
      programArguments[2] === "start" &&
      path.dirname(programArguments[1] ?? "") === workingDirectory;
    const runtime = developmentRuntime
      ? "development" as const
      : packagedRuntime
        ? "packaged" as const
        : null;
    const keepAlive = plist.KeepAlive && typeof plist.KeepAlive === "object"
      ? plist.KeepAlive as Record<string, unknown>
      : null;
    const environment = plist.EnvironmentVariables && typeof plist.EnvironmentVariables === "object"
      ? plist.EnvironmentVariables as Record<string, unknown>
      : null;
    // Keep read-only inspection aligned with launch-agent.ts's strict parser:
    // an extra environment override (notably HOME) changes the daemon's
    // os.homedir() boundary and cannot be treated as a default-home manifest.
    const environmentKeys = environment ? Object.keys(environment).sort() : [];
    const environmentKeysOk =
      isDeepStrictEqual(environmentKeys, ["PATH", "PLIMSOLL_COLLECTOR_DATA_MODE"].sort()) ||
      isDeepStrictEqual(environmentKeys, ["PATH", "PLIMSOLL_COLLECTOR_DATA_MODE", "PLIMSOLL_HOME"].sort());
    const launchAgentPath = typeof environment?.PATH === "string" ? environment.PATH : "";
    // Issue #135: compare the manifest's propagated collector home with the
    // home this command resolved, using path-free identity hashes only. The
    // daemon runs whatever PLIMSOLL_HOME the manifest carries (default home
    // when absent), so a mismatch here means setup/doctor/status would inspect
    // one home while launchd starts the daemon against another.
    const expectedHomeHash = collectorHomeIdentityHash(collectorHome());
    const manifestHomeValue = environment?.PLIMSOLL_HOME;
    const manifestHomePresent = environment !== null &&
      Object.hasOwn(environment, "PLIMSOLL_HOME");
    const manifestHome = typeof manifestHomeValue === "string"
      ? manifestHomeValue
      : null;
    const manifestHomeValid = manifestHome !== null &&
      manifestHome.length > 0 &&
      path.isAbsolute(manifestHome) &&
      !/[\u0000-\u001f\u007f-\u009f]/.test(manifestHome);
    const observedHomeHash = !manifestHomePresent
      ? collectorHomeIdentityHash(defaultCollectorHome())
      : manifestHomeValid
        ? privatePathReceipt(manifestHome)
        : null;
    const homeIdentity = {
      ok: observedHomeHash === expectedHomeHash,
      manifestHomePresent,
      expectedHash: expectedHomeHash,
      observedHash: observedHomeHash,
    };
    const launchAgentPathEntries = launchAgentPath.split(path.delimiter);
    const normalizedPathEntries = launchAgentPathEntries.map((entry) => path.resolve(entry));
    const requiredPathEntries = [...new Set([
      path.resolve(path.dirname(process.execPath)),
      path.resolve(path.dirname(programArguments[0] ?? "")),
      ...LAUNCH_AGENT_SYSTEM_PATHS.map((entry) => path.resolve(entry)),
    ])];
    const pathValidation = {
      nonempty: launchAgentPath.length > 0 && launchAgentPathEntries.every((entry) => entry.length > 0),
      absolute: launchAgentPathEntries.every((entry) => path.isAbsolute(entry)),
      controlFree: launchAgentPathEntries.every(
        (entry) => !/[\u0000-\u001f\u007f-\u009f]/.test(entry),
      ),
      unique: new Set(normalizedPathEntries).size === normalizedPathEntries.length,
      missingRequiredEntries: requiredPathEntries.filter(
        (required) => !normalizedPathEntries.includes(required),
      ),
    };
    const pathOk =
      pathValidation.nonempty &&
      pathValidation.absolute &&
      pathValidation.controlFree &&
      pathValidation.unique &&
      pathValidation.missingRequiredEntries.length === 0;
    const matchesExpectedRuntime = Boolean(
      plist.Label === LAUNCH_AGENT_LABEL &&
      runtime &&
      workingDirectory &&
      path.isAbsolute(workingDirectory) &&
      plist.RunAtLoad === true &&
      keepAlive?.SuccessfulExit === false &&
      plist.ThrottleInterval === 30 &&
      plist.StandardOutPath === collectorLogPath("collector.out.log") &&
      plist.StandardErrorPath === collectorLogPath("collector.err.log") &&
      environment?.PLIMSOLL_COLLECTOR_DATA_MODE === "metadata" &&
      environmentKeysOk &&
      pathOk &&
      homeIdentity.ok,
    );
    return {
      ok: matchesExpectedRuntime,
      installed: true,
      status: matchesExpectedRuntime ? "valid" as const : "conflicted" as const,
      label: LAUNCH_AGENT_LABEL,
      plistPath,
      runtime,
      homeIdentity,
      path: {
        ok: pathOk,
        ...pathValidation,
      },
    };
  } catch {
    return {
      ok: false,
      installed: true,
      status: "unreadable" as const,
      label: LAUNCH_AGENT_LABEL,
      plistPath,
      runtime: null,
      homeIdentity: null,
      path: null,
    };
  }
}

function collectorPidRecord(runtimeIdentity: CollectorRuntimeIdentity): CollectorPidRecord {
  return {
    command: process.argv.slice(1),
    cwd: process.cwd(),
    instanceId: runtimeIdentity.instanceId,
    label: LAUNCH_AGENT_LABEL,
    pid: runtimeIdentity.pid,
    processStartFingerprint: runtimeIdentity.processStartFingerprint,
    processStartFingerprintAlgorithm:
      runtimeIdentity.processStartFingerprintAlgorithm ?? UTC_PROCESS_START_ALGORITHM,
    startedAt: new Date().toISOString(),
    version: 3,
  };
}

/**
 * The installation epoch a newly registered capture root joins (bead
 * eco-6hoxj.53). The ledger binding is authoritative; a config whose roots all
 * carry one epoch answers for an install whose ledger cannot be read.
 */
function readInstallationEpochId(roots: readonly CaptureRoot[]): string | null {
  const bufferPath = collectorBufferPath();
  if (fs.existsSync(bufferPath)) {
    try {
      const database = new Database(bufferPath, { readonly: true, fileMustExist: true });
      try {
        const row = database
          .prepare("select current_installation_epoch_id as epoch from collector_workspace_binding where singleton = 1")
          .get() as { epoch: string | null } | undefined;
        if (row?.epoch) return row.epoch;
      } finally {
        database.close();
      }
    } catch {
      // An absent or unreadable binding falls through to the configured roots.
    }
  }
  const epochs = new Set(roots.map((root) => root.installationEpochId));
  return epochs.size === 1 ? [...epochs][0] : null;
}

async function main() {
  if (command === "help" || command === "--help" || command === "-h") {
    printHelp();
    return;
  }

  if (command === "__maintenance_worker") {
    const spawnNonce = process.argv[3] ?? "";
    const environmentNonce = process.env.PLIMSOLL_MAINTENANCE_SPAWN_NONCE ?? "";
    if (
      !/^[a-f0-9-]{16,80}$/i.test(spawnNonce) ||
      spawnNonce !== environmentNonce ||
      !process.send
    ) {
      process.exitCode = 64;
      return;
    }
    runMaintenanceWorkerService({
      spawnNonce,
      initialize: () => {
        const workerConfig = loadCollectorConfig();
        assertCollectorPrivacyMode(workerConfig, "automatic maintenance worker");
        const workerBuffer = openBuffer(workerConfig, false, 900);
        const capture = createProfileCapture(workerBuffer, workerConfig);
        const workerMaintenance = new CollectorMaintenance(workerBuffer, capture.rollout, capture.transcript);
        return {
          maintenance: workerMaintenance,
          buffer: workerBuffer,
          retentionDays: workerConfig.retentionDays,
          repoContextDrain: {
            config: workerConfig.repoContextDrain,
            captureRoots: workerConfig.captureRoots,
          },
        };
      },
    });
    return;
  }

  if (command === "__enrichment_worker") {
    const spawnNonce = process.argv[3] ?? "";
    if (
      !/^[a-f0-9-]{16,80}$/i.test(spawnNonce) ||
      spawnNonce !== process.env.PLIMSOLL_ENRICHMENT_SPAWN_NONCE ||
      !process.send
    ) {
      process.exitCode = 64;
      return;
    }
    let workerBuffer: ReturnType<typeof openBuffer> | null = null;
    lowerEnrichmentProcessPriority();
    runEnrichmentWorkerService({
      spawnNonce,
      execute: (deadlineMs) => {
        const workerConfig = loadCollectorConfig();
        assertCollectorPrivacyMode(workerConfig, "automatic enrichment worker");
        workerBuffer = openBuffer(workerConfig, false, 900);
        const result = runEnrichmentMaintenanceJob(workerBuffer.database, { remainingMs: deadlineMs });
        return { rows: result.rows, ms: result.ms };
      },
      close: () => {
        workerBuffer?.close();
        workerBuffer = null;
      },
    });
    return;
  }

  if (command === "__startup_wal_checkpoint") {
    const nonce = process.argv[3] ?? "";
    const timeoutMs = Number(process.argv[4]);
    if (!/^[a-f0-9-]{16,80}$/i.test(nonce) || nonce !== process.env.PLIMSOLL_STARTUP_WAL_NONCE ||
      !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) {
      process.exitCode = 64;
      return;
    }
    const database = new Database(collectorBufferPath(), { timeout: Math.max(1, timeoutMs - 250) });
    try {
      database.pragma(`busy_timeout = ${Math.max(1, timeoutMs - 250)}`);
      const rows = database.pragma("wal_checkpoint(TRUNCATE)") as Array<{ busy: number; log: number; checkpointed: number }>;
      process.stdout.write(JSON.stringify(rows[0] ?? { busy: 1, log: 0, checkpointed: 0 }));
    } finally { database.close(); }
    return;
  }


  if (command === "join") {
    // Join runs before ordinary config loading because loadCollectorConfig()
    // creates a default file. A refused/failed join must leave even a missing
    // active config untouched.
    if (flag("--dry-run")) {
      throw new Error(
        "join --dry-run is unsupported because redeeming a single-use token is not a preview. " +
          "No token was read, no request was sent, and no local state was changed.",
      );
    }
    const rawJoinArguments = process.argv.slice(3);
    const reassign = rawJoinArguments.includes("--reassign");
    if (rawJoinArguments.filter((argument) => argument === "--reassign").length > 1) {
      throw new Error("join --reassign may be provided only once.");
    }
    const joinArguments = rawJoinArguments.filter((argument) => argument !== "--reassign");
    const targetArgument = joinArguments[0];
    const resume = targetArgument === "--resume";
    if (resume && (joinArguments.length !== 1 || reassign)) {
      throw new Error("join --resume does not accept another token or URL.");
    }
    const tokenFromPrompt = targetArgument === "--token-prompt";
    const tokenFromStdin = targetArgument === "--token-stdin" || targetArgument === "-";
    const tokenFromFd = targetArgument === "--token-fd";
    if (
      !resume &&
      (!targetArgument ||
        (targetArgument.startsWith("--") &&
          !tokenFromPrompt &&
          !tokenFromStdin &&
          !tokenFromFd))
    ) {
      throw new Error(
        'Usage: plimsoll join --token-prompt --url <cloud-base-url>  |  plimsoll join --token-stdin --url <cloud-base-url>  |  plimsoll join "<join-url>#<token>"  |  plimsoll join --resume',
      );
    }
    let joinBaseUrl: string | undefined;
    let joinTokenFd: string | undefined;
    if (tokenFromFd) {
      const value = joinArguments[1];
      if (value === undefined || !value.trim() || value.startsWith("--")) {
        throw new Error("join --token-fd requires a file descriptor value.");
      }
      joinTokenFd = value;
    }
    for (let index = tokenFromFd ? 2 : 1; !resume && index < joinArguments.length; index += 1) {
      const argument = joinArguments[index];
      if (argument === "--token-stdin") {
        throw new Error("Choose either a positional join token/URL or --token-stdin, not both.");
      }
      if (argument === "--token-fd") {
        if (!tokenFromFd || joinTokenFd !== undefined) {
          throw new Error("join --token-fd may be provided only once as the token source.");
        }
        const value = joinArguments[index + 1];
        if (value === undefined || !value.trim() || value.startsWith("--")) {
          throw new Error("join --token-fd requires a file descriptor value.");
        }
        joinTokenFd = value;
        index += 1;
        continue;
      }
      if (argument !== "--url") {
        throw new Error(`Unsupported join option or argument: ${argument}`);
      }
      if (joinBaseUrl !== undefined) throw new Error("join --url may be provided only once.");
      const value = joinArguments[index + 1];
      if (!value || value.startsWith("--")) {
        throw new Error("join --url requires a URL value.");
      }
      joinBaseUrl = value;
      index += 1;
    }

    // Only a fully validated, real join/resume may scavenge stale handshake
    // state. Unsupported preview/options must be observably read-only.
    cleanupStaleJoinHandshakeDirectories();
    const result = resume
      ? await resumePendingJoin()
      : await (async () => {
          const target = tokenFromStdin
            ? (await readStdin()).trim()
            : tokenFromPrompt
              ? (await readSecretFromPrompt()).trim()
              : tokenFromFd
                ? readSecretFromFd(joinTokenFd ?? "").trim()
                : targetArgument;
          if (!target) {
            throw new Error(
              'Usage: plimsoll join --token-prompt --url <cloud-base-url>  |  plimsoll join --token-stdin --url <cloud-base-url>  |  plimsoll join "<join-url>#<token>"  |  plimsoll join --resume',
            );
          }
          return performJoin({
            target,
            baseUrl: joinBaseUrl ?? process.env.PLIMSOLL_CLOUD_URL,
            reassign,
          });
        })();
    if (!result.joined) {
      console.error(
        JSON.stringify(
          {
            status: "join_refused",
            reason: result.reason,
            httpStatus: result.httpStatus,
            message: result.message,
            configTouched: result.configTouched,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
      return;
    }
    console.log(
      JSON.stringify(
        {
          status: "joined",
          configPath: result.configPath,
          tenantId: result.tenantId,
          deviceId: result.deviceId,
          keyId: result.keyId,
          policyVersion: result.policyVersion,
          deviceStatus: result.status,
          installCredentialsConfigured: true,
          uploadUrl: result.uploadUrl,
          uploadSigningConfigured: result.uploadSigningConfigured,
          workspaceBoundary: result.workspaceBoundary,
          enrollment: result.enrollment,
          syncConfigured: true,
          privacyMode: "metadata_only",
          handshake: result.handshake,
          accountSaltSynced: result.accountSaltSynced ?? false,
          nextSteps: [
            "plimsoll status   # syncConfigured: true; existing history was not part of the handshake",
            "restart a running collector (or: plimsoll install-launch-agent && plimsoll load-launch-agent) so the daemon picks up sync",
          ],
        },
        null,
        2,
      ),
    );
    return;
  }

  if (command === "sync-account-salt") {
    const config = loadCollectorConfig();
    if (!config.uploadUrl || !config.installKey || !config.tenantId || !config.deviceId) {
      throw new Error("Account salt sync requires an active joined workspace.");
    }
    const result = await syncAccountActorSalt({
      collectorHome: collectorHome(), tenantId: config.tenantId,
      cloudDeviceId: config.cloudDeviceId,
      uploadUrl: config.uploadUrl, installKey: config.installKey, ingestKey: config.ingestKey,
      signingSecret: config.uploadSigningSecret,
      endpointUrl: config.accountActorSaltEndpoint,
    });
    const status = result.reason === "synced" ? "account_salt_synced" :
      result.reason === "unallocated" ? "account_salt_unallocated" :
      result.reason === "unbound" ? "account_salt_device_unbound" : "account_salt_refused";
    console.log(JSON.stringify({ status,
      tenantId: result.tenantId, saltVersion: result.saltVersion, synced: result.synced,
      ...(result.reason === "unbound"
        ? { action: "upload once with delivery enabled, or rejoin" }
        : {}),
    }, null, 2));
    if (result.reason === "refused" || result.reason === "unbound") process.exitCode = 1;
    return;
  }

  const accountAssertionMutation = accountAssertionMutationFromArgs();
  if (accountAssertionMutation) {
    if (!accountAssertionMutation.yes && !accountAssertionMutation.dryRun) {
      throw new Error("Account assertion adapter changes require --yes (or use --dry-run).");
    }
    const databasePath = collectorBufferPath();
    let database: Database.Database | null = null;
    try {
      if (accountAssertionMutation.yes) {
        ensureCollectorHome();
        database = new Database(databasePath, { timeout: 5_000 });
        const state = setAccountAssertionAdapterEnabled(database, accountAssertionMutation.source, accountAssertionMutation.enabled);
        console.log(JSON.stringify({
          status: "account_assertion_adapter_updated",
          source: accountAssertionMutation.source,
          enabled: state.adapters[accountAssertionMutation.source].enabled,
          stateKey: "account_assertion_adapters_v1",
        }, null, 2));
      } else if (fs.existsSync(databasePath)) {
        database = new Database(databasePath, { readonly: true, timeout: 5_000 });
        const hasState = Boolean(database.prepare("select 1 from sqlite_master where type='table' and name='maintenance_state'").get());
        const state = hasState ? readAccountAssertionAdapterState(database) : null;
        console.log(JSON.stringify({
          status: "account_assertion_adapter_preview",
          source: accountAssertionMutation.source,
          enabled: accountAssertionMutation.enabled,
          currentEnabled: state?.adapters[accountAssertionMutation.source].enabled ?? true,
          stateKey: "account_assertion_adapters_v1",
          dryRun: true,
        }, null, 2));
      } else {
        console.log(JSON.stringify({
          status: "account_assertion_adapter_preview",
          source: accountAssertionMutation.source,
          enabled: accountAssertionMutation.enabled,
          currentEnabled: true,
          stateKey: "account_assertion_adapters_v1",
          dryRun: true,
        }, null, 2));
      }
    } finally {
      database?.close();
    }
    return;
  }

  if (command === "start") {
    cleanupStaleJoinHandshakeDirectories();
    finalizeActivatedPendingJoin();
  }

  const noCreateConfigCommands = new Set([
    "capture-roots",
    "doctor",
    "setup",
    "install-launch-agent",
    "load-launch-agent",
    "unload-launch-agent",
    "uninstall-launch-agent",
    "lifecycle",
  ]);
  const configRead = noCreateConfigCommands.has(command) ? readCollectorConfig() : null;
  let strictSetupConfig: CollectorConfig | null = null;
  if (command === "setup" && configRead?.status === "invalid") {
    // Strict parsing preserves the specific privacy/error reason without the
    // create-on-missing behavior that setup preview must avoid.
    strictSetupConfig = loadCollectorConfig();
  }
  const configPath = configRead?.path ?? collectorConfigPath();
  const config = configRead?.config ?? strictSetupConfig ??
    (noCreateConfigCommands.has(command) ? collectorConfigSchema.parse({}) : loadCollectorConfig());
  assertCollectorPrivacyMode(config, command, {
    willEnableUpload: command === "join" || Boolean(optionValue("--url")),
  });

  if (command === "enroll-codex-live-producer") {
    const optionNames = new Set(["--producer-id", "--credential-id", "--capture-root-id"]);
    const seen = new Set<string>();
    for (let index = 3; index < process.argv.length; index += 2) {
      const name = process.argv[index] ?? "";
      const value = process.argv[index + 1];
      if (!optionNames.has(name) || seen.has(name) || !value || value.startsWith("--")) {
        throw new Error(
          "Usage: plimsoll enroll-codex-live-producer --producer-id ID --credential-id ID --capture-root-id ID",
        );
      }
      seen.add(name);
    }
    if (seen.size !== optionNames.size) {
      throw new Error(
        "Usage: plimsoll enroll-codex-live-producer --producer-id ID --credential-id ID --capture-root-id ID",
      );
    }
    const buffer = openBuffer(config);
    try {
      const enrollment = await enrollCodexLiveProducer({
        home: collectorHome(),
        buffer,
        config,
        producerId: optionValue("--producer-id")!,
        credentialId: optionValue("--credential-id")!,
        captureRootId: optionValue("--capture-root-id")!,
      });
      console.log(JSON.stringify({
        status: "codex_live_producer_enrolled",
        producerId: enrollment.producerId,
        credentialId: enrollment.credentialId,
        captureRootId: enrollment.binding.captureRootId,
        installationEpochId: enrollment.binding.installationEpochId,
        accountAssertionAttached: enrollment.accountAssertionAttached,
      }));
    } finally {
      buffer.close();
    }
    return;
  }

  if (command === "start") {
    let pidPath = "";
    let runtimeIdentity: CollectorRuntimeIdentity;
    let ownership: Awaited<ReturnType<typeof acquireCollectorStartOwnership>>;
    try {
      pidPath = collectorLogPath("collector.pid");
      runtimeIdentity = createCollectorRuntimeIdentity();
      ownership = await acquireCollectorStartOwnership({
        candidateIdentity: runtimeIdentity,
        label: LAUNCH_AGENT_LABEL,
        pidPath,
        port: config.port,
      });
    } catch (error) {
      console.error(
        JSON.stringify({
          status: "error",
          code:
            error instanceof CollectorStartOwnershipError
              ? error.code
              : "ownership_failed",
          pidPathHash: pidPath ? privatePathReceipt(pidPath) : null,
          port: config.port,
        }),
      );
      process.exitCode = 1;
      return;
    }
    if (ownership.kind === "already_running") {
      console.log(
        JSON.stringify(
          {
            status: "already_running",
            pid: ownership.runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(ownership.pidPath),
            port: ownership.port,
            runtimeIdentity: ownership.runtimeIdentity,
          },
          null,
          2,
        ),
      );
      return;
    }

    const startupWalNonce = randomUUID();
    const startupWalReceipt = await runStartupWalSelfHeal({
      ledgerPath: collectorBufferPath(),
      thresholdBytes: config.startupWalCheckpointBytes,
      runCheckpoint: (timeoutMs) => checkpointWalInBoundedChild({
        entryPath: process.argv[1]!, execArgv: process.execArgv, nonce: startupWalNonce, timeoutMs,
      }),
    });
    console.log(JSON.stringify(startupWalReceipt));

    // This connection owns the HTTP event loop. Never inherit better-sqlite3's
    // five-second busy wait when the maintenance child briefly owns a writer.
    const buffer = openBuffer(config, false, 0);
    // Outcome facts intentionally live outside the capture ledger. Opening the
    // local read model here does not schedule collection; the only writer is
    // an explicit backfill command.
    const outcomeTimelineStore = new OutcomeTimelineStore(
      path.join(collectorHome(), "outcome-timeline-v1.sqlite"),
    );
    let outcomeTimelineStoreClosed = false;
    const closeOutcomeTimelineStore = () => {
      if (outcomeTimelineStoreClosed) return;
      outcomeTimelineStore.close();
      outcomeTimelineStoreClosed = true;
    };
    // Runtime ownership is already proven above. Recover ID-only handoff and
    // inflight receipts once, before intake or the child can create live work.
    buffer.recoverRepoContextState();
    let scheduler: CoalescingMaintenanceScheduler<MaintenanceAttemptOutcome> | undefined;
    let maintenanceCadence: AutomaticMaintenanceCadence<MaintenanceAttemptOutcome> | undefined;
    let enrichmentScheduler: IdleEnrichmentScheduler | undefined;
    let enrichmentCadence: AutomaticEnrichmentCadence | undefined;
    let cachedBaseline = captureBaselineStatus(buffer.database);
    const maintenanceBoundary = new MaintenanceProcessBoundary({
      entryPath: process.argv[1]!,
      execArgv: process.execArgv,
      env: process.env,
      // Cold start of the tsx child (compile + opening a large WAL ledger) alone
      // can exceed 1s, which killed every run before its first slice committed
      // (issue #177: projections frozen behind 2M buffered events). The child's
      // writer holds stay bounded by its own slice budget (maxActiveMs <= 5s in
      // maintenance.ts), so these deadlines govern startup + coordination only.
      deadlineMs: 30_000,
      teardownMarginMs: 1_000,
      readyDeadlineMs: 45_000, // 2026-09-04: guards process start only (ready is sent before the ledger opens); 10 s flaked under heavy disk I/O from the lane sweeps
      // Issue #181: a deadline kill must never vanish silently. Record the
      // kill rate and the last-seen stage durably, and surface the receipt
      // so enrichment starvation cannot recur invisibly.
      onDeadline: (info) => {
        if (info.outcome === "PARTIAL_OK" && info.jobProgress) {
          console.log(JSON.stringify({
            status: "maintenance_partial_ok",
            ...info.jobProgress,
          }));
          return;
        }
        try {
          recordMaintenanceDeadlineKill(buffer.database);
          recordMaintenanceDeadlineBlame(buffer.database, {
            at: new Date().toISOString(),
            source: info.progress?.source ?? null,
            stage: info.progress?.stage ?? null,
            heldMs: info.heldMs,
            attribution: info.attribution,
          });
          const receipt = maintenanceStarvationReceipt(buffer.database);
          if (receipt.starving) {
            console.warn(JSON.stringify({
              warning: "maintenance_starvation",
              ...receipt,
            }));
          }
        } catch {
          // Starvation bookkeeping must never mask the boundary failure.
        }
      },
      onOrphanRecovery: (info) => {
        console.warn(JSON.stringify({
          warning: "maintenance_orphan_recovery",
          ...info,
        }));
      },
    });
    const enrichmentBoundary = new EnrichmentProcessBoundary({
      entryPath: process.argv[1]!,
      execArgv: process.execArgv,
      env: process.env,
      deadlineMs: 15_000,
      readyDeadlineMs: 10_000,
      termGraceMs: 250,
      killGraceMs: 750,
    });
    let detectedIdentities: Array<Record<string, unknown>> = [];
    try {
      detectedIdentities = readLocalIdentities().map((entry) => ({
        source: entry.source,
        email: entry.email ?? null,
        planType: entry.planType ?? null,
        actorHash: entry.actorHash ?? null,
      }));
    } catch {
      detectedIdentities = [];
    }
    let refreshStatusSnapshot: (failure?: "maintenance_failed") => boolean = () => false;
    let retentionCadence: AutomaticRetentionCadence | undefined;
    const readStarvationReceipt = () => {
      try {
        return maintenanceStarvationReceipt(buffer.database);
      } catch {
        return null;
      }
    };
    let cachedStarvationReceipt = readStarvationReceipt();
    const server = createCollectorServer(config, buffer, {
      runtimeIdentity,
      homeIdentityHash: collectorHomeIdentityHash(collectorHome()),
      // Issue 0056 (#104): the daemon provisions (first start) or loads the
      // Plimsoll-local producer/management credentials and enforces them.
      localAuth: loadOrCreateLocalIngestAuth(collectorHome()),
      localAuthHome: collectorHome(),
      maintenanceStatus: () => ({
        boundary: maintenanceBoundary.status(),
        scheduler: scheduler?.status() ?? null,
        cadence: maintenanceCadence?.status() ?? null,
        retentionCadence: retentionCadence?.status() ?? null,
        starvation: cachedStarvationReceipt,
      }),
      detectedIdentities: () => detectedIdentities,
      outcomePerformance: (days, asOf) => outcomeTimelineStore.performanceSummary(days, asOf),
      registerStatusRefresher: (refresh) => {
        refreshStatusSnapshot = (failure) => {
          cachedStarvationReceipt = readStarvationReceipt();
          try { cachedBaseline = captureBaselineStatus(buffer.database); } catch { /* retain last observation */ }
          return refresh(failure);
        };
      },
    });
    let ownsPidFile = false;
    let shuttingDown = false;
    const timers: NodeJS.Timeout[] = [];
    /** The managed-config reconcile cadence reschedules itself, so it owns one live handle. */
    let managedConfigReconcileTimer: NodeJS.Timeout | undefined;
    let syncFailureStreak = 0;
    let syncInFlight = false;

    let syncSkipUntil = 0;
    // Sessions whose snapshot push failed (or was interrupted) carry over to
    // the next cycle in memory. A daemon restart drops the set — the
    // `upload-history --sessions` backfill is the stateless recovery tool,
    // exactly as the event side's recovery is upload-history itself.
    let pendingSessionIds: string[] = [];

    const runSync = async () => {
      if (!config.uploadUrl || syncInFlight || shuttingDown) return;
      if (Date.now() < syncSkipUntil) return;
      syncInFlight = true;
      const storageRetry = new SyncStorageRetryController();
      const uploadedBatches: Array<Awaited<ReturnType<typeof uploadBufferedEvents>>["batch"]> = [];
      const carrySessions = () => {
        pendingSessionIds = [
          ...new Set([...pendingSessionIds, ...sessionIdsFromBatches(uploadedBatches)]),
        ];
      };
      try {
        let batches = 0;
        let uploaded = 0;
        while (batches < config.delivery.maxBatchesPerCycle) {
          const result = await uploadBufferedEvents(config, buffer, {
            includeLegacyRemainingUnuploaded: false,
            storageRetry,
          });
          if (result.uploadedEvents === 0) break;
          uploadedBatches.push(result.batch);
          uploaded += result.uploadedEvents;
          batches += 1;
          if (result.remainingDelivery === 0) break;
        }
        if (uploaded > 0) {
          console.log(
            JSON.stringify({
              status: "synced",
              uploadedEvents: uploaded,
              remainingUnuploaded: buffer.delivery.status().remainingDelivery,
              remainingDelivery: buffer.delivery.status().remainingDelivery,
            }),
          );
        }
        syncFailureStreak = 0;
        syncSkipUntil = 0;

        // Session sync (issue 0037): the sessions whose events just crossed
        // get their snapshots refreshed — recomputed over the FULL ledger,
        // pushed as a kind:"session_sync" batch the cloud upserts grow-only.
        // Isolated failure domain: events are already marked uploaded, so a
        // session-push error must never look like a sync failure or trigger
        // the event backoff; the ids simply carry to the next cycle.
        const touchedSessionIds = [
          ...new Set([...pendingSessionIds, ...sessionIdsFromBatches(uploadedBatches)]),
        ];
        if (touchedSessionIds.length > 0) {
          try {
            const sessionResult = await runSessionSync(config, {
              sessionIds: touchedSessionIds,
              ledgerDb: buffer.database,
              log: () => undefined,
            });
            pendingSessionIds = sessionResult.ok ? [] : touchedSessionIds;
            if (sessionResult.ok && sessionResult.sentSessions > 0) {
              console.log(
                JSON.stringify({
                  status: "session_sync",
                  sessions: sessionResult.sentSessions,
                  inserted: sessionResult.insertedSessions,
                  updated: sessionResult.updatedSessions,
                  skippedStale:
                    sessionResult.insertedSessions === null || sessionResult.updatedSessions === null
                      ? null
                      : sessionResult.acceptedSessions -
                        sessionResult.insertedSessions -
                        sessionResult.updatedSessions,
                }),
              );
            } else if (!sessionResult.ok) {
              console.warn(
                JSON.stringify({ warning: "session_sync_failed", message: sessionResult.reason }),
              );
            }
          } catch (error) {
            pendingSessionIds = touchedSessionIds;
            console.warn(
              JSON.stringify({
                warning: "session_sync_failed",
                message: error instanceof Error ? error.message : String(error),
              }),
            );
          }
        }
      } catch (error) {
        carrySessions();
        if (error instanceof SyncStorageBusyError) {
          syncSkipUntil = 0;
          console.warn(
            JSON.stringify({
              warning: "sync_storage_busy",
              waitMs: error.waitMs,
              retries: error.retries,
            }),
          );
          return;
        }
        syncFailureStreak += 1;
        const backoffMs = Math.min(
          config.syncIntervalSeconds * 1000 * 2 ** Math.min(syncFailureStreak, 4),
          60 * 60 * 1000,
        );
        syncSkipUntil = Date.now() + backoffMs;
        console.warn(
          JSON.stringify({
            warning: "sync_failed",
            failureStreak: syncFailureStreak,
            backoffMs,
            message: error instanceof Error ? error.message : String(error),
          }),
        );
      } finally {
        syncInFlight = false;
      }
    };

    // First boot records a metadata-only, whole-generation exclusion baseline.
    // Later automatic cadences tail only new generations within hard work
    // limits; full history remains an explicit operator command.
    scheduler = new CoalescingMaintenanceScheduler(async () => {
      // If the low-priority child won the idle check immediately before this
      // main trigger, let that single bounded row finish or be reaped first.
      await enrichmentScheduler?.waitForIdle();
      const drainedRepoContexts = buffer.takeRepoContextBatch();
      const repoContexts = buffer.beginRepoContextResolution(drainedRepoContexts);
      let result: MaintenanceAttemptOutcome;
      try {
        result = await maintenanceBoundary.run({
          acceptPartial: true,
          repoContexts,
          onRepoContexts: (resolved) => {
            buffer.applyRepoContextResults(resolved);
          },
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "";
        const boundaryUnavailable = new Set([
          "maintenance_boundary_stopping",
          "maintenance_circuit_open",
          "maintenance_child_not_reaped",
          "maintenance_job_already_in_flight",
          "maintenance_worker_spawn_failed",
          "maintenance_worker_unavailable",
          "maintenance_child_not_ready",
          "maintenance_worker_ready_timeout",
          "maintenance_ready_pid_mismatch",
          "maintenance_send_failed",
          "maintenance_repo_context_prepare_failed",
        ]).has(message);
        try {
          buffer.failRepoContextRun(
            repoContexts,
            boundaryUnavailable ? "boundary_unavailable" : "worker_crash",
          );
        } catch {
          // Preserve the boundary's literal failure. Parent startup remains
          // the final recovery boundary without exposing any raw cwd.
        }
        throw error;
      }
      if (isMaintenancePartialOutcome(result)) {
        try {
          buffer.failRepoContextRun(repoContexts, "boundary_unavailable");
        } catch {
          // The acknowledged stage commit is still successful; startup recovery
          // owns any unresolved parent handoff left by the disposable child.
        }
        refreshStatusSnapshot();
        return result;
      }
      try {
        cachedBaseline = captureBaselineStatus(buffer.database);
      } catch {
        // Keep the last coherent parent snapshot; the child receipt remains
        // authoritative for whether this generation completed.
      }
      refreshStatusSnapshot();
      const { rollout, transcript, reconciliation, repricing, enrichment } = result;
      if (rollout.eventsAppended > 0 || rollout.parseErrors > 0) {
        console.log(JSON.stringify({ status: "rollout_scan", ...rollout }));
      }
      if (transcript.eventsAppended > 0 || transcript.parseErrors > 0) {
        console.log(JSON.stringify({ status: "transcript_scan", ...transcript }));
      }
      if (reconciliation.rowsChanged > 0) {
        console.log(JSON.stringify({ status: "codex_reconciliation", ...reconciliation }));
      }
      if (repricing.repriced > 0) {
        console.log(JSON.stringify({ status: "repriced", ...repricing }));
      }
      if (enrichment.backward > 0 || enrichment.forward > 0) {
        console.log(JSON.stringify({ status: "repo_stitch", ...enrichment }));
      }
      return result;
    });
    retentionCadence = new AutomaticRetentionCadence(
      () => buffer.prune(config.retentionDays, { maxRows: 128 }),
      {
        canRun: () => !scheduler?.status().inFlight && !enrichmentScheduler?.status().inFlight,
        onPass: (receipt) => {
          if (receipt.events || receipt.metricSamples) console.log(JSON.stringify({ status: "pruned", ...receipt }));
        },
        onError: () => console.warn(JSON.stringify({ warning: "prune_failed" })),
      },
    );
    maintenanceCadence = new AutomaticMaintenanceCadence(
      scheduler,
      () => cachedBaseline,
      {
        repairProgress: () => {
          const projection = buffer.projection.status();
          const repairs = automaticRepairServiceStatus(buffer.database);
          return {
            pending: Object.values(projection.backlog).some(n => n > 0) ||
              !projection.backfill.complete || !projection.backfill.parityComplete || !projection.backfill.metricComplete,
            units: Object.values(repairs.stages).reduce((sum, stage) => sum + stage.rowsVisited, 0) +
              projection.counters.snapshotBuilds + projection.counters.expiryFacts + projection.counters.compactGcItemsVisited,
          };
        },
        retryNotBefore: () => {
          const at = maintenanceBoundary.status().circuit.openUntil;
          return at ? Date.parse(at) : null;
        },
        onError: (error) => {
          if (shuttingDown && error instanceof Error &&
            error.message === "maintenance_boundary_stopping") return;
          refreshStatusSnapshot("maintenance_failed");
          console.warn(
            JSON.stringify({
              warning: "maintenance_failed",
              ...(error instanceof MaintenanceFailureError
                ? {
                    errorClass: error.errorClass,
                    message: error.message,
                    stage: error.stage,
                    elapsedMs: error.elapsedMs,
                    progressAcknowledged: error.progressAcknowledged,
                  }
                : {
                    errorClass: error instanceof Error ? error.name : "UnknownError",
                    message: error instanceof Error ? error.message : String(error),
                    stage: "boundary",
                    elapsedMs: null,
                    progressAcknowledged: false,
                  }),
            }),
          );
        },
      },
    );
    enrichmentScheduler = new IdleEnrichmentScheduler(
      () => !scheduler!.status().inFlight,
      async () => {
        const result = await enrichmentBoundary.run({ acceptPartial: true });
        if (result.outcome === "PARTIAL_OK") {
          console.log(JSON.stringify({ status: "enrichment_partial_ok", rows: result.rows, ms: result.ms }));
        } else if (result.rows > 0) {
          console.log(JSON.stringify({ status: "repo_stitch", rows: result.rows, ms: result.ms }));
        }
        return result;
      },
    );
    enrichmentCadence = new AutomaticEnrichmentCadence(enrichmentScheduler, {
      intervalMs: 5 * 60_000,
      onError: (error) => {
        if (shuttingDown && error instanceof Error &&
            error.message === "enrichment_boundary_stopping") return;
        console.warn(JSON.stringify({
          warning: "enrichment_failed",
          message: error instanceof Error ? error.message : String(error),
        }));
      },
    });

    retentionCadence.start();
    // Boot capture is deferred so the OTLP receiver binds first, but it uses
    // the exact same bounded recent-tail entrypoint as the interval. Historical
    // files are available only through the explicit scan commands below.
    maintenanceCadence.start();
    enrichmentCadence.start();
    if (config.uploadUrl) {
      timers.push(setInterval(() => void runSync(), config.syncIntervalSeconds * 1000));
    }
    // Self-healing managed-config reconcile (bead eco-6hoxj.50). The fleet's
    // seat and conductor tooling rewrites ~/.claude-seats/<slug>/settings.json
    // and ~/.codex-profiles/<slug>/config.toml whenever a seat or profile
    // churns, and the rewritten file silently loses the managed block. The tick
    // is a decision first: disabled by config, or not yet due, and it touches
    // no managed file at all; due but with no drifted target, and it plans
    // nothing. Only a drifted target makes it re-apply the managed keys, and
    // only where the plan says added|updated.
    //
    // The cadence is a self-rescheduling timeout rather than a fixed interval
    // because both `enabled` and `intervalSeconds` are re-read from the config
    // file on every tick (review r1, F3): the kill-switch and a changed cadence
    // take effect without a daemon restart, which is what doctor already
    // reports. A disabled tick reads Plimsoll's own config and nothing else.
    //
    // The tick is async and yields to the event loop between targets
    // (review r1, F6), so a fully churned fleet-scale host never holds this
    // process — which also serves /hooks/* and the OTLP receiver — for a whole
    // reconcile in one synchronous chunk. Only one tick is ever outstanding:
    // the next one is scheduled from this one's `finally`.
    const managedConfigReconcilePeriodMs = () =>
      Math.max(
        1,
        readManagedConfigReconcileSettings(collectorConfigPath(), config.managedConfig.reconcile)
          .intervalSeconds,
      ) * 1000;
    const scheduleManagedConfigReconcile = (delayMs: number) => {
      if (shuttingDown) return;
      managedConfigReconcileTimer = setTimeout(() => void managedConfigReconcileTick(), delayMs);
      managedConfigReconcileTimer.unref();
    };
    const managedConfigReconcileTick = async () => {
      if (shuttingDown) return;
      try {
        const { decision, result } = await runManagedConfigReconcileTick(config);
        if (result && (result.applied > 0 || result.refused > 0)) {
          console.log(JSON.stringify({
            status: result.status,
            trigger: decision.reason,
            applied: result.applied,
            unchanged: result.unchanged,
            skipped: result.skipped,
            refused: result.refused,
            absent: result.absent,
            receiptPath: result.receiptPath,
          }));
        }
      } catch (error) {
        // Managed config is never load-bearing for capture: a failed tick is
        // reported and the next one re-plans from the files as they are.
        console.warn(JSON.stringify({
          warning: "managed_config_reconcile_failed",
          message: error instanceof Error ? error.message : String(error),
        }));
      } finally {
        scheduleManagedConfigReconcile(managedConfigReconcilePeriodMs());
      }
    };
    scheduleManagedConfigReconcile(managedConfigReconcilePeriodMs());
    for (const timer of timers) timer.unref();

    const stopMaintenanceBeforeFatalExit = async () => {
      maintenanceCadence?.stop();
      retentionCadence?.stop();
      enrichmentCadence?.stop();
      for (const timer of timers) clearInterval(timer);
      if (managedConfigReconcileTimer) clearTimeout(managedConfigReconcileTimer);
      scheduler?.stopAccepting();
      enrichmentScheduler?.stopAccepting();
      ownership.release();
      const [idle, child, enrichmentIdle, enrichmentChild] = await Promise.allSettled([
        scheduler?.waitForIdle() ?? Promise.resolve(),
        maintenanceBoundary.shutdown(),
        enrichmentScheduler?.waitForIdle() ?? Promise.resolve(),
        enrichmentBoundary.shutdown(),
      ]);
      const maintenanceIdle = idle.status === "fulfilled";
      const maintenanceChildReaped = child.status === "fulfilled" && child.value;
      const enrichmentStopped = enrichmentIdle.status === "fulfilled" &&
        enrichmentChild.status === "fulfilled" && enrichmentChild.value;
      // The maintenance child may own the SQLite writer. Never close the
      // parent's connection while either the scheduler or child can still use it.
      if (maintenanceIdle && maintenanceChildReaped && enrichmentStopped) {
        buffer.close();
        closeOutcomeTimelineStore();
      }
      return {
        maintenanceIdle: maintenanceIdle && enrichmentIdle.status === "fulfilled",
        maintenanceChildReaped: maintenanceChildReaped && enrichmentStopped,
      };
    };

    const flushRejectionSummaries = () => {
      // Issue #0075 (#144): at most one bounded summary per active rejection
      // reason on the way out; restart only loses ephemeral suppression state.
      for (const line of server.plimsollHttpDiagnostics.flush()) {
        console.warn(JSON.stringify(line));
      }
    };

    const shutdown = (signal: string) => {
      if (shuttingDown) {
        server.closeIdleConnections?.();
        server.closeAllConnections?.();
        return;
      }
      shuttingDown = true;
      flushRejectionSummaries();
      maintenanceCadence?.stop();
      retentionCadence?.stop();
      enrichmentCadence?.stop();
      for (const timer of timers) clearInterval(timer);
      if (managedConfigReconcileTimer) clearTimeout(managedConfigReconcileTimer);
      scheduler?.stopAccepting();
      enrichmentScheduler?.stopAccepting();
      ownership.release();
      const hardDeadlineMs = 2_500;
      const forceAfterMs = 750;
      const deadlineAt = performance.now() + hardDeadlineMs;
      let serverClosed = false;
      let maintenanceIdle = false;
      let maintenanceChildReaped = false;
      let enrichmentChildReaped = false;
      const serverClose = new Promise<void>((resolve) => {
        server.close(() => {
          serverClosed = true;
          resolve();
        });
      });
      const forceTimer = setTimeout(() => {
        server.closeIdleConnections?.();
        server.closeAllConnections?.();
      }, forceAfterMs);
      forceTimer.unref();
      const idle = (scheduler?.waitForIdle() ?? Promise.resolve()).then(() => {
        maintenanceIdle = true;
      });
      const childShutdown = maintenanceBoundary.shutdown().then((stopped) => {
        maintenanceChildReaped = stopped;
      });
      const enrichmentShutdown = enrichmentBoundary.shutdown().then((stopped) => {
        enrichmentChildReaped = stopped;
      });
      const deadline = new Promise<void>((resolve) => {
        setTimeout(resolve, hardDeadlineMs);
      });
      void (async () => {
        try {
          await Promise.race([
            Promise.allSettled([serverClose, idle, childShutdown, enrichmentShutdown]).then(() => undefined),
            deadline,
          ]);
          if (!serverClosed) {
            server.closeIdleConnections?.();
            server.closeAllConnections?.();
            const remaining = Math.max(0, deadlineAt - performance.now());
            if (remaining > 0) {
              await Promise.race([
                serverClose,
                new Promise<void>((resolve) => {
                  const timer = setTimeout(resolve, remaining);
                  timer.unref();
                }),
              ]);
            }
          }
          // Never close SQLite until the child is reaped and the parent
          // scheduler no longer has a boundary call in flight.
          if (maintenanceIdle && maintenanceChildReaped && enrichmentChildReaped) {
            buffer.close();
            closeOutcomeTimelineStore();
          }
        } catch {
          // PID ownership cleanup below is the non-negotiable finalizer.
        } finally {
          closeOutcomeTimelineStore();
          clearTimeout(forceTimer);
          server.closeIdleConnections?.();
          server.closeAllConnections?.();
          ownership.release();
          const cleanupAttempt = ownsPidFile
            ? removeCollectorPidFileIfOwned(pidPath, runtimeIdentity, LAUNCH_AGENT_LABEL)
            : null;
          const remaining = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
          const persistentCleanup = readCollectorPidCleanupState(
            pidPath,
            LAUNCH_AGENT_LABEL,
          );
          const pidCleaned =
            remaining.kind === "missing" &&
            !persistentCleanup.ambiguous &&
            !cleanupAttempt?.ambiguous;
          const shutdownReady = pidCleaned && serverClosed && maintenanceChildReaped && enrichmentChildReaped;
          console.log(
            JSON.stringify({
              // The process is still executing this receipt. Only the stop or
              // unload observer may promote a later absent process to stopped.
              status: shutdownReady ? "shutdown_ready" : "shutdown_incomplete",
              signal,
              pid: process.pid,
              pidCleaned,
              pidRecordState: remaining.kind,
              pidCleanup: pidCleanupStateReceipt(persistentCleanup),
              cleanupAttempt: pidCleanupAttemptReceipt(cleanupAttempt),
              maintenanceIdle,
              maintenanceChildReaped,
              enrichmentChildReaped,
              listenerClosed: serverClosed,
              listenerState: serverClosed ? "closed" : "close_incomplete",
              processState: "exiting",
              processLiveAtReceipt: processIdentityIsLive(runtimeIdentity),
              hardDeadlineMs,
            }),
          );
          process.exit(shutdownReady ? 0 : 1);
        }
      })();
    };

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));
    server.on("error", (error: NodeJS.ErrnoException) => {
      if (shuttingDown) return;
      shuttingDown = true;
      flushRejectionSummaries();
      void (async () => {
        const maintenance = await stopMaintenanceBeforeFatalExit();
        const cleanupAttempt = ownsPidFile
          ? removeCollectorPidFileIfOwned(pidPath, runtimeIdentity, LAUNCH_AGENT_LABEL)
          : null;
        const remaining = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
        const persistentCleanup = readCollectorPidCleanupState(pidPath, LAUNCH_AGENT_LABEL);
        console.error(
          JSON.stringify(
            {
              status: "error",
              code: error.code === "EADDRINUSE" ? "port_in_use" : "listen_failed",
              port: config.port,
              message: error.message,
              pidCleaned: remaining.kind === "missing" && !persistentCleanup.ambiguous,
              pidRecordState: remaining.kind,
              pidCleanup: pidCleanupStateReceipt(persistentCleanup),
              cleanupAttempt: pidCleanupAttemptReceipt(cleanupAttempt),
              ...maintenance,
            },
            null,
            2,
          ),
        );
        process.exit(1);
      })();
    });
    server.listen(config.port, "127.0.0.1", () => {
      try {
        ownership.writePidFile(collectorPidRecord(runtimeIdentity));
        ownsPidFile = true;
      } catch (error) {
        shuttingDown = true;
        void (async () => {
          const listenerClosed = await new Promise<boolean>((resolve) => {
            server.close((closeError) => resolve(!closeError));
          });
          const maintenance = await stopMaintenanceBeforeFatalExit();
          console.error(
            JSON.stringify(
              {
                status: "error",
                code: "ownership_failed",
                pidFileOwned: ownsPidFile,
                port: config.port,
                message: error instanceof Error ? error.message : String(error),
                listenerClosed,
                ...maintenance,
              },
              null,
              2,
            ),
          );
          process.exit(1);
        })();
        return;
      }
      ownership.release();
      console.log(
        JSON.stringify({
          status: "active",
          mode: "metadata_only",
          dataMode: config.policy.dataMode,
          port: config.port,
          pid: process.pid,
          pidFileOwned: ownsPidFile,
          runtimeIdentity,
          hookEndpoints: {
            claudeCode: `http://127.0.0.1:${config.port}/hooks/claude-code`,
            codex: `http://127.0.0.1:${config.port}/hooks/codex`,
          },
          otlpEndpoints: {
            logs: `http://127.0.0.1:${config.port}/v1/logs`,
            traces: `http://127.0.0.1:${config.port}/v1/traces`,
            metrics: `http://127.0.0.1:${config.port}/v1/metrics`,
            geminiCli: `http://127.0.0.1:${config.port}/gemini`,
          },
          privacy: {
            ...collectorPrivacyReadiness(config),
            screenshots: false,
            keystrokes: false,
            clipboardBody: false,
            browserHistory: false,
            rawPromptDefault: false,
            rawOutputDefault: false,
          },
        }),
      );
    });
    return;
  }

  if (command === "status") {
    const buffer = openBuffer(config);
    const accountAssertions = accountAssertionStatus(buffer.database);
    const device = readDeviceIdentity();
    const bufferPath = collectorBufferPath();
    const resolvedHome = resolveCollectorHome();
    const projected = buffer.projection.readSnapshot(30, config.subscriptions);
    const projectedStatus = projected.kind === "ready" ? projected.snapshot.status : null;
    console.log(
      JSON.stringify(
        {
          homeIdentityHash: collectorHomeIdentityHash(collectorHome()),
          homeSource: resolvedHome.source,
          configPathHash: privatePathReceipt(collectorConfigPath()),
          bufferPathHash: privatePathReceipt(bufferPath),
          bufferFileBytes: fs.existsSync(bufferPath) ? fs.statSync(bufferPath).size : 0,
          pidPathHash: privatePathReceipt(collectorLogPath("collector.pid")),
          port: config.port,
          appVersion: PLIMSOLL_VERSION,
          policyVersion: config.policy.version,
          device: device
            ? {
                deviceId: device.deviceId,
                tenantId: config.tenantId,
                keyId: device.keyId,
                appVersion: PLIMSOLL_VERSION,
                policyVersion: config.policy.version,
                createdAt: device.createdAt,
                lastSeenAt: device.lastSeenAt,
                lastUploadAt: device.lastUploadAt,
                queueAgeSeconds: buffer.delivery.queueAgeSeconds(),
                status: device.status,
              }
            : null,
          dataMode: config.policy.dataMode,
          privacyMode: "metadata_only",
          privacy: collectorPrivacyReadiness(config),
          retentionDays: config.retentionDays,
          syncConfigured: Boolean(config.uploadUrl),
          reconciliation: codexReconciliationStatus(buffer.database),
          stats: projectedStatus?.stats ?? null,
          retention: buffer.retentionStatus(config.retentionDays),
          delivery: buffer.delivery.status(),
          projection: buffer.projection.status(),
          captureHealth: projectedStatus?.health ?? {
            generatedAt: new Date().toISOString(),
            overall: "amber",
            sources: [],
            reason: "projection backfill has not published a coherent health snapshot",
          },
          historyCoverage: historyCoverageStatus(buffer.database),
          enrollment: buffer.enrollmentStatus(),
          captureBaseline: captureBaselineStatus(buffer.database),
          automaticCapture: automaticCaptureRuntimeStatus(buffer.database),
          accountAssertions,
          accountAssertionAdapters: accountAssertions,
          accountAssertionStatusLine: formatAccountAssertionStatusLine(buffer.database),
          accountLabelCompatibility:
            "label account <sha256:hash> \"<display name>\" remains a local-only compatibility label; it never changes assertions or history",
        },
        null,
        2,
      ),
    );
    buffer.close();
    return;
  }

  if (command === "setup") {
    // Config apply mode (issue 0003): the no-terminal path still exists via
    // the dashboard; this is the one command an installer runs. Surgical
    // merges with backups; second run reports no-op.
    // Setup's flags, parsed as flags (review r1, F8).
    //
    // `argValue` used to scan the whole of process.argv with indexOf and the
    // mode flags used `process.argv.includes`, so a token matched wherever it
    // appeared — including as another flag's *value*. `setup --claude-settings
    // --reconcile` was read as both a reconcile request and a Claude settings
    // path of "--reconcile", which is two wrong answers to one typo. Each value
    // flag now consumes exactly one following token, a value that is itself a
    // flag is a usage error, and the mode flags are read from the parsed set.
    const SETUP_VALUE_FLAGS = [
      "--claude-settings",
      "--gemini-settings",
      "--codex-config",
      "--grok-hooks",
    ];
    const SETUP_MODE_FLAGS = ["--yes", "--dry-run", "--reconcile"];
    const setupValues = new Map<string, string>();
    const setupModes = new Set<string>();
    const setupArguments = process.argv.slice(3);
    for (let index = 0; index < setupArguments.length; index += 1) {
      const argument = setupArguments[index]!;
      if (SETUP_MODE_FLAGS.includes(argument)) {
        setupModes.add(argument);
        continue;
      }
      if (SETUP_VALUE_FLAGS.includes(argument)) {
        const value = setupArguments[index + 1];
        if (value === undefined || value.startsWith("--")) {
          console.error(`setup: ${argument} needs a path.`);
          process.exitCode = 2;
          return;
        }
        setupValues.set(argument, value);
        index += 1;
        continue;
      }
      console.error(
        `setup: unknown argument ${JSON.stringify(argument)}. Usage: setup [--yes|--dry-run|--reconcile] ` +
          `[--claude-settings <path>] [--gemini-settings <path>] [--codex-config <path>] [--grok-hooks <path>]`,
      );
      process.exitCode = 2;
      return;
    }
    const argValue = (name: string) => setupValues.get(name);
    const yes = setupModes.has("--yes");
    const dryRun = setupModes.has("--dry-run");
    const reconcileMode = setupModes.has("--reconcile");
    const claudeFile = argValue("--claude-settings") ?? path.join(os.homedir(), ".claude", "settings.json");
    const geminiFile = argValue("--gemini-settings") ?? path.join(os.homedir(), ".gemini", "settings.json");
    const codexFile = argValue("--codex-config") ?? path.join(os.homedir(), ".codex", "config.toml");
    const grokHome = resolveGrokHome().home;
    const grokFile = argValue("--grok-hooks") ?? path.join(grokHome, "hooks", "plimsoll.json");
    const grokHeaderFile = path.join(path.dirname(grokFile), "plimsoll.headers");
    const codexHeaderFile = path.join(path.dirname(codexFile), "plimsoll.headers");
    // Setup is the installer: it provisions the Plimsoll-local credentials so
    // generated tool configs bind each producer to its own source-bound token.
    // Planning may need producer tokens, but provisioning them belongs only
    // after a valid plan and explicit confirmation. A dry-run and a rejected
    // plan must leave the Plimsoll home byte-absent.
    const localAuth = loadOrCreateLocalIngestAuth(collectorHome(), { dryRun: true });
    const toolOptions = {
      repoRoot: process.cwd(),
      port: config.port,
      dataMode: config.policy.dataMode,
      claudeCodeProducerToken: localAuth.claudeCodeProducer,
      codexProducerToken: localAuth.codexProducer,
      geminiCliProducerToken: localAuth.geminiCliProducer,
      grokProducerToken: localAuth.grokProducer,
      grokHeaderFile,
      codexHeaderFile,
    };
    // Seat discovery is a plain read of the process home: the seat tooling
    // owns ~/.claude-seats/<slug>, so setup manages what is already there and
    // a seat created later is picked up by the next run. Codex seat profiles
    // are discovered the same way (bead eco-6hoxj.52) under
    // ~/.codex-profiles/<slug>, and neither family is ever provisioned.
    //
    // Both families are composed by managed-config-reconcile.ts (bead
    // eco-6hoxj.50) so that `setup --yes`, `setup --reconcile` and the
    // collector's self-healing maintenance cadence all manage exactly the same
    // target set from one definition.
    const managedClaudeTargets = composeManagedClaudeTargets(claudeFile, os.homedir());
    const managedCodexTargets = composeManagedCodexTargets(codexFile, os.homedir());
    type SetupTargetName =
      | "claude"
      | `claudeSeat[${string}]`
      | "gemini"
      | "grokHeaders"
      | "grok"
      | "codexHeaders"
      | "codex"
      | `codexProfile[${string}]`;
    type SetupTarget = {
      name: SetupTargetName;
      path: string;
      /** True for a target found on disk rather than declared by Plimsoll. */
      discovered?: true;
      run: (options: typeof toolOptions, dryRun: boolean) => ReturnType<typeof applyCodexConfig>;
    };
    type SetupTargetState = {
      target: SetupTarget;
      plan?: ReturnType<typeof applyCodexConfig>;
      refusal?: string;
    };
    const targets: SetupTarget[] = [
      // The `claude` target plus every fleet Claude seat (bead eco-6hoxj.48):
      // a lane launched with CLAUDE_CONFIG_DIR=~/.claude-seats/<slug> reads
      // that seat's settings.json instead of ~/.claude/settings.json, so it got
      // no exporter and no hooks. Each discovered seat is its own target with
      // the same managed content and the same additive merge as the `claude`
      // target; the seat's own hooks and unknown keys survive untouched, and a
      // seat directory without settings.json is skipped rather than created.
      ...managedClaudeTargets,
      {
        name: "gemini",
        path: geminiFile,
        run: (options, preview) =>
          applyGeminiSettings(geminiFile, generateGeminiCliSettings(options), { dryRun: preview }),
      },
      {
        name: "grokHeaders",
        path: grokHeaderFile,
        run: (options, preview) =>
          applyGrokHookHeaderFile(grokHeaderFile, generateGrokHookHeader(options), { dryRun: preview }),
      },
      {
        name: "grok",
        path: grokFile,
        run: (options, preview) =>
          applyGrokHookFile(grokFile, generateGrokHookSettings(options), { dryRun: preview }),
      },
      {
        name: "codexHeaders",
        path: codexHeaderFile,
        run: (options, preview) =>
          applyCodexHookHeaderFile(codexHeaderFile, generateCodexHookHeader(options), { dryRun: preview }),
      },
      // The `codex` target plus every fleet Codex seat profile (bead
      // eco-6hoxj.52): a lane launched with CODEX_HOME=~/.codex-profiles/<slug>
      // reads that profile's config.toml instead of ~/.codex/config.toml, so it
      // ran with no [otel] exporters and no Plimsoll hooks — captured only by
      // the rollout scanner. Each discovered profile is its own target with the
      // same generated content and the same additive TOML merge as the `codex`
      // target: the fleet's own hooks and every unknown key survive untouched,
      // the hook command stays token-free by pointing at the same per-user
      // header file, and a profile directory without config.toml is skipped
      // rather than created.
      ...managedCodexTargets,
    ];
    // A hook command that references a header file Plimsoll could not write
    // would post without its producer token, so the config target is refused
    // with its header target rather than left pointing at a missing secret.
    const headerDependencies: ReadonlyArray<{ header: SetupTargetName; dependent: SetupTargetName }> = [
      { header: "grokHeaders", dependent: "grok" },
      { header: "codexHeaders", dependent: "codex" },
      // A profile's hooks reference the same per-user header file as the
      // default Codex target, so they share its fate rather than pointing at a
      // secret Plimsoll could not write.
      ...managedCodexTargets
        .filter((target) => target.discovered)
        .map((target) => ({
          header: "codexHeaders" as const,
          dependent: target.name as SetupTargetName,
        })),
    ];
    // Two sources must never share one header file: each source's hook would
    // then send the other's token, collapsing the per-source audience boundary
    // that makes a producer token unable to impersonate another tool.
    const headerTargetPaths = [...new Set(headerDependencies.map(({ header }) => header))].map(
      (header) => targets.find((target) => target.name === header)!.path,
    );
    const collidingHeaderPaths = new Set(
      headerTargetPaths.filter((value, index) => headerTargetPaths.indexOf(value) !== index),
    );
    // Self-healing reconcile (bead eco-6hoxj.50). `--reconcile` is a mode of
    // `setup` rather than a second command because everything it needs is
    // already composed here: the same managed Claude/Codex target set, the same
    // --claude-settings/--codex-config overrides, and the same
    // owned-vs-discovered exit-code rule. It is strictly weaker than a setup
    // run — it never provisions a file, never mints a credential, and writes
    // only where the plan says added|updated — so it is safe to run on a
    // cadence against files the fleet tooling owns.
    if (reconcileMode) {
      // The audience boundary is a property of the host's config, not of the
      // command that noticed it, so the same audit `setup --yes` performs runs
      // here — before any reconcile plan or apply, with the same refusal text
      // and the same exit code (review r1, F8). A host with a hand-edited
      // --grok-hooks/--codex-config pair that resolves to one header file was
      // refused by `setup --yes` and silently reconciled by `setup --reconcile`.
      const collidingHeaderTargets = targets.filter(
        (target) =>
          collidingHeaderPaths.has(target.path) &&
          headerDependencies.some((entry) => entry.header === target.name),
      );
      if (collidingHeaderTargets.length > 0) {
        const refusedHeaders = new Set(collidingHeaderTargets.map((target) => target.name));
        const refusals = [
          ...collidingHeaderTargets.map((target) => ({
            name: target.name,
            path: target.path,
            reason: `${target.path}: two managed sources resolve to the same header file; refusing this target.`,
          })),
          ...headerDependencies
            .filter((entry) => refusedHeaders.has(entry.header))
            .map((entry) => {
              const dependent = targets.find((target) => target.name === entry.dependent)!;
              return {
                name: entry.dependent,
                path: dependent.path,
                reason: `${dependent.path}: dependent managed header target was refused.`,
              };
            }),
        ];
        for (const refusal of refusals) {
          console.log(`${refusal.path}: target refused: ${refusal.reason}`);
        }
        console.log(
          JSON.stringify(
            {
              status: "managed_config_header_audience_conflict",
              reason: "two managed sources resolve to the same header file; refusing to reconcile this host.",
              applied: 0,
              unchanged: 0,
              skipped: 0,
              refused: refusals.length,
              absent: 0,
              ownedRefusal: true,
              targets: refusals.map((refusal) => ({
                name: refusal.name,
                path: refusal.path,
                status: "refused",
                reason: refusal.reason,
              })),
              receiptPath: null,
            },
            null,
            2,
          ),
        );
        process.exitCode = 1;
        return;
      }
      // Reconcile re-applies what setup installed; it is not an installer, so
      // a host without Plimsoll-local producer credentials has nothing to
      // reconcile and says so instead of provisioning them.
      const configured = readLocalIngestAuth(collectorHome()) !== null;
      // A seat/profile directory with no config file is reported as
      // `skipped: absent` rather than dropped from the report (review r1, F7).
      // Setup's own target list stays exactly what it may write.
      const reconcileClaudeTargets = composeManagedClaudeTargets(claudeFile, os.homedir(), {
        includeAbsent: true,
      });
      const reconcileCodexTargets = composeManagedCodexTargets(codexFile, os.homedir(), {
        includeAbsent: true,
      });
      // The Codex hook command points at the per-user header file. Writing a
      // Codex target while that file is absent would leave a hook referencing a
      // secret that does not exist, which is exactly what setup's header
      // dependency refuses; provisioning it belongs to `setup --yes`, so the
      // Codex family stands down for this run instead.
      const codexHeaderPresent = fs.existsSync(codexHeaderFile);
      const result = configured
        ? runManagedConfigReconcile({
            collectorHome: collectorHome(),
            targets: [
              ...reconcileClaudeTargets,
              ...(codexHeaderPresent ? reconcileCodexTargets : []),
            ],
            toolOptions,
            dryRun,
          })
        : null;
      for (const target of result?.targets ?? []) {
        for (const entry of target.plan ?? []) {
          console.log(`${target.path}: ${entry.key} ${entry.action}`);
        }
        if (target.status !== "unchanged") {
          console.log(
            `${target.path}: target ${target.status}${target.reason ? `: ${target.reason}` : ""}`,
          );
        }
      }
      console.log(
        JSON.stringify(
          result
            ? { ...result, codexHeaderFilePresent: codexHeaderPresent }
            : {
                status: "managed_config_not_configured",
                reason: "no Plimsoll-local producer credentials on this host; run `plimsoll setup --yes` first.",
                applied: 0,
                unchanged: 0,
                skipped: 0,
                refused: 0,
                absent: 0,
                ownedRefusal: false,
                targets: [],
                receiptPath: null,
              },
          null,
          2,
        ),
      );
      if (result?.ownedRefusal) process.exitCode = 1;
      return;
    }
    const planned: SetupTargetState[] = targets.map((target) => {
      if (
        collidingHeaderPaths.has(target.path) &&
        headerDependencies.some((entry) => entry.header === target.name)
      ) {
        return {
          target,
          refusal: `${target.path}: two managed sources resolve to the same header file; refusing this target.`,
        };
      }
      try {
        const plan = target.run(toolOptions, true);
        return plan.conflict ? { target, plan, refusal: plan.conflict } : { target, plan };
      } catch (error) {
        return { target, refusal: error instanceof Error ? error.message : String(error) };
      }
    });
    for (const { header, dependent } of headerDependencies) {
      const plannedHeader = planned.find((state) => state.target.name === header);
      const plannedDependent = planned.find((state) => state.target.name === dependent);
      if (plannedHeader?.refusal && plannedDependent && !plannedDependent.refusal) {
        plannedDependent.refusal = `${plannedDependent.target.path}: dependent managed header target was refused.`;
      }
    }
    for (const state of planned) {
      for (const entry of state.plan?.plan ?? []) {
        console.log(`${state.target.path}: ${entry.key} ${entry.action}`);
      }
      if (state.refusal) console.log(`${state.target.path}: target refused: ${state.refusal}`);
    }
    const summarize = (
      states: SetupTargetState[],
      changedStatus: "would_apply" | "applied",
    ) => Object.fromEntries(states.map((state) => [
      state.target.name,
      state.refusal
        ? { path: state.target.path, status: "refused", reason: state.refusal }
        : {
            path: state.target.path,
            status: state.plan?.changed ? changedStatus : "unchanged",
            ...(changedStatus === "applied" ? { backup: state.plan?.backupPath ?? null } : {}),
          },
    ]));
    // A discovered target is not Plimsoll's to own: the seat tooling writes
    // ~/.claude-seats/<slug>/settings.json, so a seat file that is malformed or
    // unreadable is reported in the JSON like any other refusal but never sets
    // the exit code and never counts as a failed target. An installer or CI step
    // that runs `plimsoll setup --yes` must not fail because another tool left
    // one of N seats half-written; the six declared targets keep deciding the
    // outcome exactly as before.
    const ownedRefusal = (states: SetupTargetState[]) =>
      states.some((state) => Boolean(state.refusal) && !state.target.discovered);
    const hasOwnedRefusal = ownedRefusal(planned);
    const hasChange = planned.some((state) => !state.refusal && state.plan?.changed);
    if (dryRun) {
      console.log(JSON.stringify({ status: "setup_dry_run", targets: summarize(planned, "would_apply") }));
      if (hasOwnedRefusal) process.exitCode = 1;
      return;
    }
    if (!hasChange) {
      if (!hasOwnedRefusal && configRead?.status === "missing") loadCollectorConfig();
      console.log(JSON.stringify({ status: "setup_noop", targets: summarize(planned, "applied") }));
      if (hasOwnedRefusal) process.exitCode = 1;
      return;
    }
    if (!yes) {
      if (!process.stdin.isTTY) {
        console.error("Refusing to write config without confirmation. Re-run with --yes (or --dry-run to preview).");
        process.exitCode = 1;
        return;
      }
      const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
      const answer = (await rl.question("Apply these changes? Backups are written first. [y/N] ")).trim().toLowerCase();
      rl.close();
      if (answer !== "y" && answer !== "yes") {
        console.log("Nothing written.");
        return;
      }
    }
    const appliedAuth = loadOrCreateLocalIngestAuth(collectorHome());
    const appliedToolOptions = {
      ...toolOptions,
      claudeCodeProducerToken: appliedAuth.claudeCodeProducer,
      codexProducerToken: appliedAuth.codexProducer,
      geminiCliProducerToken: appliedAuth.geminiCliProducer,
      grokProducerToken: appliedAuth.grokProducer,
    };
    if (configRead?.status === "missing") loadCollectorConfig();
    const refusedHeaderTargets = new Set<SetupTargetName>();
    const applied: SetupTargetState[] = planned.map((state) => {
      const dependency = headerDependencies.find((entry) => entry.dependent === state.target.name);
      if (dependency && refusedHeaderTargets.has(dependency.header)) {
        return {
          target: state.target,
          refusal: `${state.target.path}: dependent managed header target was refused.`,
        };
      }
      const isHeaderTarget = headerDependencies.some((entry) => entry.header === state.target.name);
      if (state.refusal) {
        if (isHeaderTarget) refusedHeaderTargets.add(state.target.name);
        return state;
      }
      try {
        const result = state.target.run(appliedToolOptions, false);
        if (isHeaderTarget && result.conflict) refusedHeaderTargets.add(state.target.name);
        return result.conflict
          ? { target: state.target, plan: result, refusal: result.conflict }
          : { target: state.target, plan: result };
      } catch (error) {
        if (isHeaderTarget) refusedHeaderTargets.add(state.target.name);
        return {
          target: state.target,
          refusal: error instanceof Error ? error.message : String(error),
        };
      }
    });
    console.log(
      JSON.stringify(
        {
          status: "setup_applied",
          privacyMode: "metadata_only",
          ...summarize(applied, "applied"),
          nextSteps: [
            "plimsoll install-launch-agent && plimsoll load-launch-agent",
            "open http://127.0.0.1:" + config.port + "/",
            "restart any running Claude Code / Gemini CLI / Grok / Codex sessions so they pick up telemetry",
          ],
        },
        null,
        2,
      ),
    );
    if (ownedRefusal(applied)) process.exitCode = 1;
    return;
  }

  if (command === "rotate-producer-token") {
    // Explicit operator boundary: mint a new Codex producer token, rewrite the
    // managed header file and config.toml with the backup convention setup
    // already uses, and keep the superseded token acceptable for a bounded
    // grace window so an already-running Codex keeps reporting until restart.
    const argValue = (name: string) => {
      const index = process.argv.indexOf(name);
      return index === -1 ? undefined : process.argv[index + 1];
    };
    const rotateSource = argValue("--source");
    if (rotateSource !== "codex") {
      console.error("Usage: plimsoll rotate-producer-token --source codex [--grace-seconds N] [--dry-run]");
      process.exitCode = 1;
      return;
    }
    const graceArgument = argValue("--grace-seconds");
    const graceSeconds = graceArgument === undefined
      ? DEFAULT_PRODUCER_ROTATION_GRACE_MS / 1000
      : Number(graceArgument);
    const maxGraceSeconds = MAX_PRODUCER_ROTATION_GRACE_MS / 1000;
    if (!Number.isSafeInteger(graceSeconds) || graceSeconds <= 0 || graceSeconds > maxGraceSeconds) {
      console.error(`Expected --grace-seconds to be a whole number between 1 and ${maxGraceSeconds}.`);
      process.exitCode = 1;
      return;
    }
    const rotateDryRun = process.argv.includes("--dry-run");
    const rotateCodexFile = argValue("--codex-config") ?? path.join(os.homedir(), ".codex", "config.toml");
    const rotateHeaderFile = path.join(path.dirname(rotateCodexFile), "plimsoll.headers");
    // Rotation never provisions. Without existing credentials there is nothing
    // to supersede, and minting here would add a second provisioning boundary.
    const currentAuth = readLocalIngestAuth(collectorHome());
    if (!currentAuth) {
      console.error("No Plimsoll-local credentials to rotate. Run `plimsoll setup --yes` first.");
      process.exitCode = 1;
      return;
    }
    const rotateOptions = {
      repoRoot: process.cwd(),
      port: config.port,
      dataMode: config.policy.dataMode,
      claudeCodeProducerToken: currentAuth.claudeCodeProducer,
      codexProducerToken: currentAuth.codexProducer,
      geminiCliProducerToken: currentAuth.geminiCliProducer,
      grokProducerToken: currentAuth.grokProducer,
      codexHeaderFile: rotateHeaderFile,
    };
    // Fleet Codex seat profiles (bead eco-6hoxj.54). Since eco-6hoxj.52 every
    // discovered ~/.codex-profiles/<slug>/config.toml carries this producer
    // token inline in its managed [otel] exporter headers, so a rotation that
    // rewrote only ~/.codex left every profile lane posting a superseded token
    // the moment the grace window closed. Each managed profile is rotated as
    // its own target with the same backup-and-commit discipline the default
    // target uses. A profile without the managed token is not an authenticated
    // consumer and is never provisioned here — `setup` owns that — and one the
    // conductor left unparseable is reported rather than rewritten.
    const rotateProfiles = discoverCodexProfiles(os.homedir())
      .filter((profile) => profile.hasConfig)
      .map((profile) => ({ profile, state: codexProfileTokenState(profile.path) }));
    const rotateProfilesSkipped = rotateProfiles.filter((entry) => entry.state !== "managed");
    type RotateTarget = {
      path: string;
      /** True for a target found on disk rather than declared by Plimsoll. */
      discovered?: true;
      run: (options: typeof rotateOptions, preview: boolean) => ReturnType<typeof applyCodexConfig>;
    };
    const rotateTargets: RotateTarget[] = [
      {
        path: rotateHeaderFile,
        run: (options: typeof rotateOptions, preview: boolean) =>
          applyCodexHookHeaderFile(rotateHeaderFile, generateCodexHookHeader(options), { dryRun: preview }),
      },
      {
        path: rotateCodexFile,
        run: (options: typeof rotateOptions, preview: boolean) =>
          applyCodexConfig(rotateCodexFile, generateCodexConfigToml(options), { dryRun: preview }),
      },
      ...rotateProfiles
        .filter((entry) => entry.state === "managed")
        .map(({ profile }): RotateTarget => ({
          path: profile.path,
          discovered: true,
          run: (options: typeof rotateOptions, preview: boolean) =>
            applyCodexConfig(profile.path, generateCodexConfigToml(options), {
              dryRun: preview,
              managedTarget: `codexProfile[${profile.slug}]`,
            }),
        })),
    ];
    // Preflight against the CURRENT token. A refusal here leaves the credential
    // file untouched, so an installed config is never left holding a token the
    // collector has already superseded.
    const preflight = rotateTargets.map((target) => {
      try {
        return { target, refusal: target.run(rotateOptions, true).conflict };
      } catch (error) {
        return { target, refusal: error instanceof Error ? error.message : String(error) };
      }
    });
    // A discovered profile is the fleet conductor's file, not Plimsoll's, so it
    // refuses alone: one half-written profile must not block the rotation of
    // ~/.codex or of the other profiles, exactly as `setup` refuses a discovered
    // target without failing the run (see ownedRefusal above).
    const refusedTargets = preflight
      .filter((entry) => entry.refusal && !entry.target.discovered)
      .map((entry) => ({ path: entry.target.path, refusal: entry.refusal }));
    if (refusedTargets.length > 0) {
      console.log(JSON.stringify({
        status: "rotation_refused",
        source: "codex",
        rotated: false,
        targets: refusedTargets.map((entry) => ({
          path: entry.path,
          status: "refused",
          reason: entry.refusal,
        })),
      }, null, 2));
      process.exitCode = 1;
      return;
    }
    if (rotateDryRun) {
      // A dry run mints nothing, so the plan lines for the discovered profiles
      // are previewed against a placeholder that is never written and never
      // printed: it only makes the reconciler report the managed exporter
      // headers a real rotation would rewrite.
      for (const target of rotateTargets.filter((entry) => entry.discovered)) {
        let preview: ReturnType<typeof applyCodexConfig> | undefined;
        try {
          preview = target.run({ ...rotateOptions, codexProducerToken: ROTATION_PREVIEW_TOKEN }, true);
        } catch {
          preview = undefined;
        }
        for (const entry of preview?.plan ?? []) {
          console.log(`${target.path}: ${entry.key} ${entry.action}`);
        }
      }
      console.log(JSON.stringify({
        status: "rotation_dry_run",
        source: "codex",
        rotated: false,
        graceSeconds,
        targets: rotateTargets.map((target) => ({ path: target.path, status: "would_rotate" })),
        ...skippedProfilesReceipt(rotateProfilesSkipped),
      }, null, 2));
      return;
    }
    // Order matters: the credential file accepts the old and the new token
    // before either config file changes, so no producer is locked out mid-run.
    const rotation = rotateLocalProducerToken(collectorHome(), "codex", {
      graceMs: graceSeconds * 1000,
    });
    const rotatedOptions = { ...rotateOptions, codexProducerToken: rotation.auth.codexProducer };
    const rotateResults: Array<{ path: string; status: string; backup: string | null; reason?: string }> = [];
    let rotateFailure = false;
    for (const target of rotateTargets) {
      if (rotateFailure) {
        rotateResults.push({ path: target.path, status: "not_attempted", backup: null });
        continue;
      }
      const preflightRefusal = preflight.find((entry) => entry.target === target)?.refusal;
      if (preflightRefusal) {
        // Only a discovered target reaches here: an owned refusal returned above.
        rotateResults.push({ path: target.path, status: "refused", backup: null, reason: preflightRefusal });
        continue;
      }
      try {
        const result = target.run(rotatedOptions, false);
        if (result.conflict) {
          if (!target.discovered) rotateFailure = true;
          rotateResults.push({ path: target.path, status: "refused", backup: null, reason: result.conflict });
          continue;
        }
        rotateResults.push({
          path: target.path,
          status: result.changed ? "rotated" : "unchanged",
          backup: result.backupPath ?? null,
        });
      } catch (error) {
        if (!target.discovered) rotateFailure = true;
        rotateResults.push({
          path: target.path,
          status: "failed",
          backup: null,
          reason: error instanceof Error ? error.message : String(error),
        });
      }
    }
    console.log(JSON.stringify({
      status: rotateFailure ? "rotation_incomplete" : "rotation_applied",
      source: "codex",
      rotated: true,
      graceSeconds,
      previousTokenExpiresAt: new Date(rotation.expiresAt).toISOString(),
      targets: rotateResults,
      ...skippedProfilesReceipt(rotateProfilesSkipped),
      nextSteps: rotateFailure
        ? [
            "the new token is already provisioned; re-run rotate-producer-token after resolving the refusal",
            "the superseded token keeps working only until previousTokenExpiresAt",
          ]
        : [
            "restart any running Codex sessions before previousTokenExpiresAt",
            // A discovered profile that refused keeps the superseded token, so
            // the operator is told how to repair it while the window is open.
            ...(rotateResults.some((entry) =>
              entry.status === "refused" || entry.status === "failed"
            )
              ? ["plimsoll setup --yes   # a discovered Codex seat profile refused the rotation"]
              : []),
            "plimsoll doctor --read-only --json   # producerTokenRotation.codex reports the deadline",
          ],
    }, null, 2));
    if (rotateFailure) process.exitCode = 1;
    return;
  }

  if (command === "scan-rollouts") {
    const buffer = openBuffer(config);
    const capture = createProfileCapture(buffer, config);
    const result = await capture.rollout.scan({ scope: "full" });
    const historyCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      result,
    );
    console.log(JSON.stringify({ ...result, historyCoverage }, null, 2));
    capture.close();
    buffer.close();
    return;
  }

  if (command === "scan-transcripts") {
    const buffer = openBuffer(config);
    const capture = createProfileCapture(buffer, config);
    const result = await capture.transcript.scan({ scope: "full" });
    const historyCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      result,
    );
    console.log(JSON.stringify({ ...result, historyCoverage }, null, 2));
    capture.close();
    buffer.close();
    return;
  }

  if (command === "drain-projections") {
    // Explicit recovery for a stalled projection backlog (issue #177). The
    // background cadence gives the drain only post-capture budget scraps
    // (maxSlices: 1, <=25ms), which can never catch up once repairs pile up
    // behind an outage. This loops the same slice machinery at full budget
    // until the dashboard is caught up. Run it with the collector stopped to
    // avoid writer contention; it is safe to interrupt and re-run.
    const buffer = openBuffer(config);
    const projection = buffer.projection;
    const startedAt = Date.now();
    let rounds = 0;
    for (;;) {
      projection.runMaintenance();
      rounds++;
      if (rounds % 25 === 0 || rounds === 1) {
        const status = projection.status();
        const backlogTotal =
          status.backlog.repairs + status.backlog.compactMutations +
          status.backlog.compactGcDays + status.backlog.dirtySessions +
          status.backlog.accountInvalidations + status.backlog.expiryWindows;
        console.log(JSON.stringify({
          status: "drain_progress", rounds,
          elapsedSeconds: Math.round((Date.now() - startedAt) / 1000),
          backlog: status.backlog,
          migrationComplete: status.backfill.complete &&
            status.backfill.parityComplete && status.backfill.metricComplete,
        }));
        if (backlogTotal === 0 && status.backfill.complete &&
          status.backfill.parityComplete && status.backfill.metricComplete) break;
      }
      // Yield between synchronous slices so signals stay responsive.
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    const final = projection.status();
    console.log(JSON.stringify({ status: "drain_complete", rounds, backlog: final.backlog }, null, 2));
    buffer.close();
    return;
  }

  if (command === "doctor") {
    const plistPath = launchAgentPlistPath();
    const pidPath = collectorLogPath("collector.pid");
    const claudePath = path.join(os.homedir(), ".claude", "settings.json");
    const codexPath = path.join(os.homedir(), ".codex", "config.toml");
    const grokHookPath = path.join(resolveGrokHome().home, "hooks", "plimsoll.json");
    const grokHeaderPath = path.join(path.dirname(grokHookPath), "plimsoll.headers");
    const codexHeaderPath = path.join(path.dirname(codexPath), "plimsoll.headers");
    // Doctor is read-only: it compares against provisioned credentials when
    // they exist and never creates the credential file as a side effect.
    const localAuth = readLocalIngestAuth(collectorHome());
    const toolOptions = {
      repoRoot: process.cwd(),
      port: config.port,
      dataMode: config.policy.dataMode,
      ...(localAuth
        ? {
            claudeCodeProducerToken: localAuth.claudeCodeProducer,
            codexProducerToken: localAuth.codexProducer,
            geminiCliProducerToken: localAuth.geminiCliProducer,
            grokProducerToken: localAuth.grokProducer,
          }
        : {}),
      grokHeaderFile: grokHeaderPath,
      codexHeaderFile: codexHeaderPath,
    };
    const claude = readClaudeTelemetryConfig(claudePath, generateClaudeCodeSettings(toolOptions));
    // Fleet Claude seat coverage (bead eco-6hoxj.48). A seat whose settings.json
    // carries no managed exporter or hooks emits transcript rows only, so it is
    // reported here as a coverage diagnostic — slug and managed key names, never
    // a value — without changing `ok`, which stays a health verdict.
    const claudeSeats = discoverClaudeSeats(os.homedir()).map((seat) => {
      // The same path hygiene the Codex profiles get below: a seat relocated
      // out of the home is named by its link under the home, never by an
      // absolute path outside it.
      const seatPath = homeScopedDiscoveredPath(
        seat.path,
        path.join(claudeSeatsRoot(os.homedir()), seat.slug, "settings.json"),
        os.homedir(),
      );
      if (!seat.hasSettings) {
        return { slug: seat.slug, ...seatPath, status: "skipped" as const, missing: [] as string[] };
      }
      const read = readClaudeTelemetryConfig(seat.path, generateClaudeCodeSettings(toolOptions));
      return {
        slug: seat.slug,
        ...seatPath,
        // A seat file Plimsoll cannot parse or read is seat-tooling state, and
        // `setup` deliberately does not fail on it, so doctor is where it has to
        // show up: named `unreadable`, with the same coverage diagnostic and
        // still never a value.
        status: read.status === "invalid" ? ("unreadable" as const) : read.status,
        missing: read.missing,
        ...(read.ok ? {} : { diagnostic: "claude_seat_settings_unmanaged" }),
      };
    });
    const codex = readCodexTelemetryConfig(codexPath, generateCodexConfigToml(toolOptions));
    // Fleet Codex seat profile coverage (bead eco-6hoxj.52). A profile whose
    // config.toml carries no managed [otel] exporters and no Plimsoll hooks is
    // captured by the rollout scanner only, so it is reported here as a
    // coverage diagnostic — slug and managed key names, never a value — without
    // changing `ok`, which stays a health verdict.
    const codexProfiles = discoverCodexProfiles(os.homedir()).map((profile) => {
      // A profile the conductor relocated to shared storage resolves outside
      // the home; the receipt names its link under ~/.codex-profiles instead,
      // so no doctor payload ever carries a filesystem layout outside $HOME.
      const profilePath = homeScopedDiscoveredPath(
        profile.path,
        path.join(codexProfilesRoot(os.homedir()), profile.slug, "config.toml"),
        os.homedir(),
      );
      if (!profile.hasConfig) {
        return { slug: profile.slug, ...profilePath, status: "skipped" as const, missing: [] as string[] };
      }
      const read = readCodexTelemetryConfig(profile.path, generateCodexConfigToml(toolOptions));
      return {
        slug: profile.slug,
        ...profilePath,
        // A profile file Plimsoll cannot parse or read is fleet-conductor
        // state, and `setup` deliberately does not fail on it, so doctor is
        // where it has to show up: named `unreadable`, with the same coverage
        // diagnostic and still never a value.
        status: read.status === "invalid" ? ("unreadable" as const) : read.status,
        missing: read.missing,
        ...(read.ok ? {} : { diagnostic: "codex_profile_config_unmanaged" }),
      };
    });
    const grokHookCommand = diagnoseManagedGrokHookCommand(grokHookPath);
    const codexHookCommand = diagnoseManagedCodexHookCommand(codexPath);
    const launchAgent = readLaunchAgentState(plistPath);
    const connectivity = await checkCollectorConnectivity(
      config.port,
      readLocalIngestAuth(collectorHome())?.managementRead,
    );
    const pidRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
    const pidCleanupReconciliation = reconcileCollectorPidCleanupState(
      pidPath,
      LAUNCH_AGENT_LABEL,
      { apply: false },
    );
    const pidCleanup = pidCleanupReconciliation.after;
    const pidRecord = pidRead.kind === "current" ? pidRead.record : null;
    const expectedHomeHash = collectorHomeIdentityHash(collectorHome());
    const resolvedHome = resolveCollectorHome();
    const daemonHomeMatches = connectivity.homeIdentityHash === null
      ? "unattested" as const
      : connectivity.homeIdentityHash === expectedHomeHash;
    const runtime = {
      ok: Boolean(
        pidRecord &&
        !pidCleanup.ambiguous &&
        processIdentityIsLive(pidRecord) &&
        runtimeIdentityMatches(pidRecord, connectivity.runtimeIdentity),
      ),
      pidPath,
      pidFileStatus: pidRead.kind,
      pidCleanup: pidCleanupStateReceipt(pidCleanup),
      pidCleanupReconciliation: pidCleanupReconciliationReceipt(
        pidCleanupReconciliation,
      ),
      ownershipVersion: {
        expected: 3,
        actual: pidRecord?.version ?? null,
      },
      processLive: pidRecord ? processIdentityIsLive(pidRecord) : false,
      identityMatchesStatus: pidRecord
        ? runtimeIdentityMatches(pidRecord, connectivity.runtimeIdentity)
        : false,
      home: {
        source: resolvedHome.source,
        identityHash: expectedHomeHash,
        custom: resolvedHome.source === "env",
        daemonReportedHash: connectivity.homeIdentityHash,
        daemonHomeMatches,
      },
    };
    const nodeMajor = Number(process.versions.node.split(".")[0]);
    const node = {
      version: process.versions.node,
      range: ">=20 <25",
      supported: Number.isInteger(nodeMajor) && nodeMajor >= 20 && nodeMajor < 25,
    };
    const configured = Boolean(
      node.supported &&
      configRead?.status === "valid" &&
      claude.ok &&
      codex.ok &&
      grokHookCommand === null &&
      codexHookCommand === null,
    );
    // Issue #135: an explicit daemon-reported home drift (not merely an
    // unattested daemon) blocks service readiness — doctor must never bless a
    // service that runs against a different collector home.
    const serviceReady =
      configured &&
      launchAgent.ok &&
      connectivity.reachable &&
      runtime.ok &&
      daemonHomeMatches !== false;
    const signalVerified = serviceReady && connectivity.signal.verified;
    const readiness = signalVerified
      ? "signal_verified"
      : serviceReady
        ? "service_ready"
        : configured
          ? "configured"
          : "not_installed";
    const ok = readiness === "signal_verified";
    const configuredRootDirectories = new Set(
      (config.captureRoots ?? []).map((root) => configuredCaptureRootDirectory(root)),
    );
    const unregisteredCaptureRootCandidates = discoverCaptureRootCandidates(os.homedir())
      .filter((candidate) => !configuredRootDirectories.has(candidate.directory));
    const bufferPath = collectorBufferPath();
    console.log(
      JSON.stringify(
        {
          ok,
          readiness,
          version: PLIMSOLL_VERSION,
          readOnly: true,
          node,
          configPath,
          bufferPath,
          pidPath,
          port: config.port,
          config: {
            status: configRead?.status ?? "invalid",
            valid: configRead?.status === "valid",
            createdDuringCommand: false,
          },
          telemetry: {
            ok: claude.ok && codex.ok,
            claude,
            codex,
            claudeSeats,
            codexProfiles,
          },
          // Self-healing reconcile of the managed Claude/Codex config (bead
          // eco-6hoxj.50): whether the collector's cadence is armed, when it
          // last ran, and how many targets that run applied or refused. Counts
          // and stamps only — never a managed value and never a path.
          managedConfig: {
            reconcile: managedConfigReconcileDoctorSection(collectorHome(), {
              enabled: config.managedConfig.reconcile.enabled,
              intervalSeconds: config.managedConfig.reconcile.intervalSeconds,
            }),
          },
          ...(grokHookCommand ? { grokHookCommand } : {}),
          ...(codexHookCommand ? { codexHookCommand } : {}),
          // Hosts that gained a native root after enrollment (bead
          // eco-6hoxj.53): the lead needs to see which ones still need
          // `capture-roots add`. A coverage diagnostic only — never a value,
          // never a path outside $HOME, and never part of `ok`.
          captureRoots: {
            configured: (config.captureRoots ?? []).length,
            unregisteredCandidates: {
              count: unregisteredCaptureRootCandidates.length,
              directories: unregisteredCaptureRootCandidates.map((candidate) => candidate.relativeDirectory),
            },
          },
          producerTokenRotation: producerTokenRotationReceipt(localAuth),
          ...producerAudienceFillReceipt(collectorHome()),
          launchAgent,
          runtime,
          connectivity,
          otelEndpoints: {
            logs: {
              reachable: connectivity.reachable,
              url: `http://127.0.0.1:${config.port}/v1/logs`,
            },
            traces: {
              reachable: connectivity.reachable,
              url: `http://127.0.0.1:${config.port}/v1/traces`,
            },
            metrics: {
              reachable: connectivity.reachable,
              url: `http://127.0.0.1:${config.port}/v1/metrics`,
            },
          },
          dataMode: config.policy.dataMode,
          privacyMode: "metadata_only",
          privacy: collectorPrivacyReadiness(config),
          retentionDays: config.retentionDays,
          syncConfigured: Boolean(config.uploadUrl),
          uploadSigningConfigured: Boolean(config.uploadSigningSecret),
          retention: connectivity.retention ?? {
            inspection: "not_inspected",
            policy: { retentionDays: config.retentionDays, cutoffAt: null },
            states: {
              retained: null,
              pendingDelivery: null,
              quarantined: null,
              expired: null,
              notInspected: 1,
            },
            lastPass: null,
          },
          enrollment: {
            futureOnlyEnrollment: true,
            managed: Boolean(config.uploadUrl),
            inspection: connectivity.enrollment ? "complete" : "not_inspected",
            quarantinedHistoryRows: connectivity.enrollment?.quarantinedHistoryRows ?? null,
          },
          sqlite: {
            exists: fs.existsSync(bufferPath),
            walExists: fs.existsSync(`${bufferPath}-wal`),
            shmExists: fs.existsSync(`${bufferPath}-shm`),
            opened: false,
          },
          invasivePermissionsRequested: {
            screenRecording: false,
            accessibilityKeyboard: false,
            clipboardBody: false,
            browserHistory: false,
          },
        },
        null,
        2,
      ),
    );
    if (!ok) process.exitCode = 1;
    return;
  }

  // Append-only registration of a native capture root a host gained after
  // enrollment (bead eco-6hoxj.53). Enrollment mints roots; nothing until now
  // registered one that appeared later, so a new Claude seat, a new Codex
  // profile, or a `~/.claude/projects` an older enrollment skipped stayed
  // invisible and was repaired by host-sealed one-off scripts. This command is
  // those scripts' reviewed semantics: derive the same identity, append, never
  // change an existing root, epoch or enrollment field.
  if (command === "capture-roots") {
    const action = process.argv[3] ?? "";
    if (!["discover", "add"].includes(action)) {
      throw new Error("Expected capture-roots discover|add");
    }
    const home = os.homedir();
    const configuredRoots = configRead?.status === "valid" ? config.captureRoots ?? [] : [];
    const refuse = (reason: string, detail: Record<string, unknown> = {}) => {
      console.log(JSON.stringify({ status: "capture_roots_add_refused", reason, ...detail }, null, 2));
      process.exitCode = 1;
    };

    if (action === "discover") {
      const entries = discoverCaptureRoots(home, configuredRoots);
      console.log(
        JSON.stringify(
          {
            status: "capture_roots_discovered",
            readOnly: true,
            config: { status: configRead?.status ?? "invalid", path: configPath },
            counts: {
              registered: entries.filter((entry) => entry.state === "registered").length,
              candidate: entries.filter((entry) => entry.state === "candidate").length,
              missing: entries.filter((entry) => entry.state === "missing").length,
            },
            roots: entries,
          },
          null,
          2,
        ),
      );
      return;
    }

    if (configRead?.status !== "valid") {
      refuse("config_not_valid", { config: { status: configRead?.status ?? "invalid", path: configPath } });
      return;
    }
    const sourceArgument = optionValue("--source");
    if (sourceArgument !== "codex" && sourceArgument !== "claude_code") {
      refuse("unknown_source", { source: sourceArgument ?? null });
      return;
    }
    const source = sourceArgument;
    const requested = process.argv
      .map((argument, index) => (argument === "--directory" ? process.argv[index + 1] : undefined))
      .filter((value): value is string => typeof value === "string" && !value.startsWith("--"));
    if (!requested.length) {
      refuse("directory_required");
      return;
    }

    // Every path is resolved to the physical directory that will be captured,
    // the same identity `inspectCaptureRoots` requires of a configured root —
    // and the home is resolved exactly as `discover` resolves it, so a
    // directory discovery reports as a candidate is never refused
    // `path_outside_home` because a component of `$HOME` is a symlink.
    const resolvedHome = resolveDiscoveryHome(home);
    // The fence is only provable when every candidate under the new root can
    // be enumerated and stat-ed. `--allow-scan-errors` keeps the refusal's
    // information and drops its veto: the entries are named in the receipt
    // and left out of the seal.
    const allowScanErrors = flag("--allow-scan-errors");
    const added: CaptureRoot[] = [];
    const preexisting: Array<{
      source: CaptureRoot["source"];
      directory: string;
      relative: string;
      observations: ReturnType<typeof captureRootBaselineObservations>["observations"];
      ambiguous: Array<{ path: string; reason: string }>;
    }> = [];
    const seen = new Set(configuredRoots.map((root) => configuredCaptureRootDirectory(root)));
    const seenIds = new Set(configuredRoots.map((root) => root.rootId));
    // The machine label is a fleet label, not a hostname, and it is stored
    // nowhere — only its digests are. On this fleet no hostname candidate
    // reproduces it, so `--machine LABEL` is the expected form and the two
    // failures are reported apart: a label the operator gave that the config
    // contradicts is `identity_derivation_mismatch` (a tampered or foreign
    // config); no label at all with nothing to recover one from is
    // `identity_machine_unresolved` (pass `--machine`).
    const machineArgument = optionValue("--machine");
    const machineCandidates = machineArgument
      ? [machineArgument]
      : [os.hostname(), os.hostname().split(".")[0] ?? ""].filter((value) => value.length > 0);
    let machine: string;
    if (machineArgument) {
      if (configuredRoots.length && !captureRootsDeriveFrom(configuredRoots, machineArgument)) {
        refuse("identity_derivation_mismatch", {
          configuredRoots: configuredRoots.length,
          machine: machineArgument,
        });
        return;
      }
      machine = machineArgument;
    } else {
      const recovered = resolveCaptureRootMachineLabel(configuredRoots, machineCandidates);
      if (recovered === null) {
        refuse("identity_machine_unresolved", {
          configuredRoots: configuredRoots.length,
          machineCandidates,
          hint: "pass --machine <fleet label>",
        });
        return;
      }
      machine = recovered;
    }

    const installationEpochId = readInstallationEpochId(configuredRoots);
    if (installationEpochId === null) {
      refuse("installation_epoch_unavailable");
      return;
    }

    for (const candidate of requested) {
      const requestedPath = path.resolve(candidate);
      const directory = physicalCaptureRootDirectory(requestedPath);
      if (directory === undefined) {
        refuse(fs.existsSync(requestedPath) ? "not_a_directory" : "directory_missing", {
          directory: privatePathReceipt(requestedPath),
        });
        return;
      }
      const relative = path.relative(resolvedHome, directory);
      if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
        refuse("path_outside_home", { directory: privatePathReceipt(directory) });
        return;
      }
      if (seen.has(directory)) {
        refuse("duplicate_directory", { directory: relative });
        return;
      }
      const identity = deriveCaptureRootIdentity(machine, source, directory);
      if (seenIds.has(identity.rootId)) {
        refuse("duplicate_root_id", { directory: relative, rootId: identity.rootId });
        return;
      }
      // The fence this command publishes is exactly the files this directory
      // already holds, so the walk must be exhaustive before anything is
      // written. An entry the tailer's own discovery would count as an error
      // (a symlinked or non-regular `.jsonl`) makes it unprovable: refuse
      // rather than register a root whose history would be replayed —
      // unless the operator accepts that consequence explicitly with
      // `--allow-scan-errors`, in which case the ambiguous entries are named
      // in the receipt and simply left unfenced (they are not excluded, so
      // the tailer will capture them like any other file it later finds).
      const scan = captureRootBaselineFiles(source, directory);
      const observed = captureRootBaselineObservations(scan.files);
      const ambiguous = [...scan.errorEntries, ...observed.errorEntries].map((entry) => ({
        path: path.relative(resolvedHome, entry.path),
        reason: entry.reason,
      }));
      if (ambiguous.length > 0 && !allowScanErrors) {
        refuse("capture_root_scan_ambiguous", {
          directory: relative,
          files: scan.files.length,
          errors: ambiguous.length,
          entries: ambiguous,
          hint: "re-run with --allow-scan-errors to register the root and leave these entries unfenced",
        });
        return;
      }
      seen.add(directory);
      seenIds.add(identity.rootId);
      added.push({ ...identity, installationEpochId, source, directory });
      preexisting.push({ source, directory, relative, observations: observed.observations, ambiguous });
    }

    const beforeBytes = fs.readFileSync(configPath);
    const next = { ...config, captureRoots: [...configuredRoots, ...added] };
    let validated: CollectorConfig;
    try {
      validated = collectorConfigSchema.parse(next);
      validateCaptureRoots(validated.captureRoots ?? []);
    } catch (error) {
      refuse("capture_root_inventory_rejected", {
        detail: error instanceof Error ? error.message.slice(0, 200) : "invalid",
      });
      return;
    }
    // Append-only, proved on the exact object that will be written: the
    // existing roots keep their bytes and every other enrollment field is the
    // one already on disk.
    const withoutRoots = (value: CollectorConfig) => ({ ...value, captureRoots: undefined });
    if (
      !isDeepStrictEqual(withoutRoots(validated), withoutRoots(config)) ||
      !isDeepStrictEqual((validated.captureRoots ?? []).slice(0, configuredRoots.length), configuredRoots)
    ) {
      refuse("append_only_violation");
      return;
    }
    // `collectorConfigSchema` is a plain `z.object`, so it strips top-level
    // keys it does not know — and `captureRoots` is written by fleet
    // enrollment tooling outside this repository, whose config shape is not
    // guaranteed to be a subset of this schema. The write is therefore
    // composed on the raw parsed object, with the validated values layered
    // over it, so an unknown field survives untouched; the raw key sets are
    // then compared and a field this writer would still drop refuses before
    // any byte is published.
    let rawConfigObject: Record<string, unknown>;
    try {
      const parsedRaw: unknown = JSON.parse(beforeBytes.toString("utf8"));
      if (!parsedRaw || typeof parsedRaw !== "object" || Array.isArray(parsedRaw)) {
        throw new Error("collector_config_not_an_object");
      }
      rawConfigObject = parsedRaw as Record<string, unknown>;
    } catch (error) {
      refuse("config_not_valid", {
        config: { status: "unreadable", path: configPath },
        detail: error instanceof Error ? error.message.slice(0, 200) : "invalid",
      });
      return;
    }
    const preserveUnknownFields = Object.fromEntries(
      Object.entries(rawConfigObject).filter(([key]) => !(key in validated)),
    );
    const carried = { ...preserveUnknownFields, ...validated } as CollectorConfig;
    const afterBytes = Buffer.from(`${JSON.stringify(carried, null, 2)}\n`);
    const beforeKeys = Object.keys(rawConfigObject).sort();
    const afterKeys = new Set(Object.keys(JSON.parse(afterBytes.toString("utf8")) as Record<string, unknown>));
    const droppedKeys = beforeKeys.filter((key) => !afterKeys.has(key));
    if (droppedKeys.length) {
      refuse("append_only_violation", { droppedKeys });
      return;
    }
    const carriedUnknownKeys = Object.keys(preserveUnknownFields);
    const digest = (value: Buffer) => createHash("sha256").update(value).digest("hex");
    const plan = {
      configPath,
      beforeSha256: digest(beforeBytes),
      afterSha256: digest(afterBytes),
      // A config written by an older helper can carry byte-level formatting
      // this writer would normalize; the receipt discloses it rather than
      // blocking a repair, because append-only is proved above on the values
      // and on the raw key set.
      configCanonicalBefore: beforeBytes.equals(
        Buffer.from(`${JSON.stringify({ ...preserveUnknownFields, ...collectorConfigSchema.parse(config) }, null, 2)}\n`),
      ),
      /** Top-level fields this schema does not know, written through as-is. */
      carriedUnknownKeys,
      /** `--allow-scan-errors`: entries left unfenced, named, never silent. */
      allowScanErrors,
      scanAmbiguities: preexisting
        .filter((entry) => entry.ambiguous.length > 0)
        .map((entry) => ({ directory: entry.relative, entries: entry.ambiguous })),
      machine,
      installationEpochId,
      rootCountBefore: configuredRoots.length,
      rootCountAfter: (validated.captureRoots ?? []).length,
      addedRoots: added.map((root, index) => ({
        rootId: root.rootId,
        profileId: root.profileId,
        installationEpochId: root.installationEpochId,
        source: root.source,
        directory: path.relative(resolvedHome, root.directory),
        /** Files already present, fenced as pre-existing generations. */
        preexistingFiles: preexisting[index]!.observations.length,
        /** Entries the walk could not resolve, left unfenced (N5). */
        ambiguousEntries: preexisting[index]!.ambiguous.length,
      })),
    };
    if (flag("--dry-run")) {
      console.log(JSON.stringify({ status: "capture_roots_add_plan", applied: false, ...plan }, null, 2));
      return;
    }

    const startedAt = Date.now();
    const appendedAt = new Date().toISOString();
    // Stop before the write and start after it, through the same lifecycle
    // code paths `unload-launch-agent` and `load-launch-agent` use, so a
    // running collector never reads a half-published inventory. A host with no
    // LaunchAgent (development) has no service to cycle.
    let manifest: ReturnType<typeof inspectLaunchAgentManifest> | null = null;
    try {
      manifest = inspectLaunchAgentManifest();
    } catch (error) {
      // A plist that exists but is not the owned manifest means a service this
      // command cannot safely cycle; the inventory stays as it is.
      refuse("launch_agent_manifest_invalid", {
        detail: error instanceof Error ? error.message.slice(0, 200) : "invalid",
      });
      return;
    }
    const installed = manifest.ok && manifest.status === "valid";
    const authority = installed ? launchAgentMutationAuthority() : undefined;
    let restart: Record<string, unknown> = {
      attempted: false,
      skipped: true,
      reason: "launch_agent_not_installed",
    };
    const backupPath = `${configPath}.plimsoll-backup-${appendedAt.replace(/[:.]/g, "-")}`;
    let failure: { step: string; error: string } | null = null;
    let configApplied = false;
    let backupWritten = false;
    let writtenSha256: string | null = null;
    let baseline: Record<string, unknown> | null = null;
    let step = "unload";
    // The unload is inside the failure discipline too (bead eco-6hoxj.55,
    // review N4): its bootout is the point the daemon goes down, and every
    // step after that — the terminal-state observation, the PID reconciler,
    // the fence release — runs while it is down. A throw there, or a
    // terminal state that cannot be proven stopped after a bootout was
    // issued, must still reach the restart block below rather than return
    // and leave the collector stopped.
    if (installed) {
      try {
        const unload = await executeLaunchAgentUnload(config.port, authority);
        restart = { attempted: true, skipped: false, unload: launchAgentUnloadReceipt(unload) };
        if (!unload.unloaded) {
          if (!unload.bootoutAttempted) {
            // Nothing was stopped — a busy fence or a refusal before the
            // bootout. There is no service to bring back, and the inventory
            // stays exactly as it is.
            console.log(JSON.stringify({
              status: "capture_roots_add_refused",
              reason: "restart_unload_failed",
              unload: launchAgentUnloadReceipt(unload),
            }, null, 2));
            process.exitCode = 1;
            return;
          }
          failure = { step, error: `unload_not_proven:${unload.reason ?? unload.status}` };
        }
      } catch (error) {
        restart = { attempted: true, skipped: false, unloadThrew: true };
        failure = { step, error: error instanceof Error ? error.message.slice(0, 200) : "failed" };
      }
    }

    // From here the collector is stopped, so every remaining step runs under
    // one failure path: whatever throws, the agent is loaded again, the
    // receipt says which step failed and what state the config is in, and the
    // command exits non-zero. Leaving the collector down was the r1 failure.
    //
    // The ledger connection is held open across the config write so a failure
    // after the fence is committed can remove exactly the generation rows
    // this run inserted (review N1). Nothing else runs between them.
    let buffer: ReturnType<typeof openBuffer> | null = null;
    let sealedThisRun: Array<{ source: CaptureRoot["source"]; runId: string; keys: string[] }> = [];
    let fenceRollback: Record<string, unknown> | null = null;
    try {
      // A failed unload has already recorded its step: skip every remaining
      // step, touch nothing, and let the restart block below run.
      if (failure) throw new Error("unload_step_failed");
      step = "backup";
      const backupDescriptor = fs.openSync(backupPath, "wx", 0o600);
      try {
        fs.writeFileSync(backupDescriptor, beforeBytes);
        fs.fsyncSync(backupDescriptor);
      } finally {
        fs.closeSync(backupDescriptor);
      }
      backupWritten = true;

      // Fence exactly the new roots, before the config names them: the files
      // already sitting in each new directory are recorded as pre-existing
      // generations in the provider's live baseline run, so they are excluded
      // instead of replayed. The provider's `started_at` is deliberately NOT
      // moved — that cutoff is keyed by source alone, so advancing it would
      // re-fence every root the provider already captures and silently stop
      // their current sessions. Sealing first also means a failure here
      // leaves the inventory exactly as it was.
      step = "baseline_seed";
      buffer = openBuffer(config);
      const baselineSources = [...new Set(added.map((root) => root.source))];
      const statusFor = () => Object.fromEntries(
        captureBaselineStatus(buffer!.database).sources
          .filter((row) => baselineSources.includes(row.source))
          .map((row) => [row.source, { status: row.status, startedAt: row.latestRun?.startedAt ?? null }]),
      );
      const before = statusFor();
      const seals: CaptureBaselineSealResult[] = baselineSources.map((baselineSource) =>
        sealCaptureBaselineGenerations(
          buffer!.database,
          baselineSource,
          preexisting
            .filter((entry) => entry.source === baselineSource)
            .flatMap((entry) => entry.observations),
          appendedAt,
        ));
      // Exactly what this run inserted, keyed as the ledger keys it, so the
      // failure path below can remove those rows and only those rows.
      sealedThisRun = seals
        .filter((seal) => seal.runId !== null && seal.sealedGenerationKeys.length > 0)
        .map((seal) => ({ source: seal.source, runId: seal.runId!, keys: seal.sealedGenerationKeys }));
      const generationsSealed = seals.reduce((total, seal) => total + seal.generationsSealed, 0);
      baseline = {
        // Only ever a time a generation row was actually written. A source
        // whose baseline is not yet complete reports the true state: the
        // tailer establishes its fence at first start, which already
        // excludes everything present by then. A retry whose rows are
        // already in place reports `already_sealed` with the count found and
        // no seeding time (review N3) — the fence is real, this run did not
        // write it.
        seededAt: generationsSealed > 0 ? appendedAt : null,
        sources: baselineSources,
        providerCutoffMoved: false,
        generationsSealed,
        generationsAlreadySealed: seals.reduce((total, seal) => total + seal.generationsAlreadySealed, 0),
        filesFenced: preexisting.reduce((total, entry) => total + entry.observations.length, 0),
        seals,
        before,
        after: statusFor(),
      };

      step = "config_write";
      writeCollectorConfigTransactionally(carried, configPath, { preserveUnknownFields });
      configApplied = true;
      step = "config_readback";
      writtenSha256 = digest(fs.readFileSync(configPath));
    } catch (error) {
      failure = failure ?? { step, error: error instanceof Error ? error.message.slice(0, 200) : "failed" };
      // A backup step that did not complete leaves no backup to compare
      // against: remove a partial file rather than publish a path that says
      // "byte-identical to the config" and is not (review N2).
      if (!backupWritten) {
        try { fs.rmSync(backupPath, { force: true }); } catch { /* reported below */ }
      }
      // The fence is committed before the config names the root. If the
      // config was never published, those rows fence files in a directory
      // nothing registered — remove exactly them (review N1). If that cannot
      // be done, the receipt says so and names the files still fenced; it
      // never claims a rollback that did not happen.
      const sealedCount = sealedThisRun.reduce((total, entry) => total + entry.keys.length, 0);
      if (!configApplied && sealedCount > 0) {
        let removed = 0;
        let rollbackError: string | null = null;
        try {
          if (!buffer) throw new Error("ledger_unavailable");
          for (const entry of sealedThisRun) {
            removed += unsealCaptureBaselineGenerations(
              buffer.database,
              entry.source,
              entry.runId,
              entry.keys,
              new Date().toISOString(),
            ).removed;
          }
        } catch (rollback) {
          rollbackError = rollback instanceof Error ? rollback.message.slice(0, 200) : "failed";
        }
        const complete = rollbackError === null && removed === sealedCount;
        fenceRollback = {
          attempted: true,
          generationsSealed: sealedCount,
          generationsRemoved: removed,
          complete,
          error: rollbackError,
          // The files this run fenced under the new roots: the operator's
          // list for a manual repair when the rollback did not complete. A
          // partial rollback makes it a superset of what is still fenced.
          retainedFiles: complete
            ? []
            : preexisting.flatMap((entry) =>
              entry.observations.map((observation) => path.relative(resolvedHome, observation.path))),
        };
      }
    } finally {
      if (buffer) {
        // A close that throws must not escape past the restart block either.
        try { buffer.close(); } catch (error) {
          failure = failure ?? {
            step: "ledger_close",
            error: error instanceof Error ? error.message.slice(0, 200) : "failed",
          };
        }
      }
    }

    if (restart.attempted) {
      const load = await loadVisibleLaunchAgent(manifest.plistPath, config.port, false, authority);
      const connectivity = await checkCollectorConnectivity(
        config.port,
        readLocalIngestAuth(collectorHome())?.managementRead,
      );
      const pidRead = readCollectorPidFile(collectorLogPath("collector.pid"), LAUNCH_AGENT_LABEL);
      const pidRecord = pidRead.kind === "current" ? pidRead.record : null;
      const daemon = {
        reachable: connectivity.reachable,
        processLive: pidRecord ? processIdentityIsLive(pidRecord) : false,
        runtimeIdentityMatches: pidRecord
          ? runtimeIdentityMatches(pidRecord, connectivity.runtimeIdentity)
          : false,
        homeIdentityHash: connectivity.homeIdentityHash,
        homeMatches: connectivity.homeIdentityHash === null
          ? null
          : connectivity.homeIdentityHash === collectorHomeIdentityHash(collectorHome()),
      };
      // A collector that did not come back is a failure of this command, not
      // a note in a receipt an operator may never read.
      const failedStep = !load.loaded
        ? "load"
        : !(daemon.reachable && daemon.processLive && daemon.runtimeIdentityMatches)
          ? "daemon_verification"
          : null;
      restart = { ...restart, load, daemon, verified: failedStep === null, failedStep };
    }

    const restartFailed = restart.attempted === true && restart.verified !== true;
    const backupOnDisk = backupWritten && (() => {
      try { return fs.existsSync(backupPath); } catch { return false; }
    })();
    const receipt = {
      status: failure || restartFailed ? "capture_roots_add_failed" : "capture_roots_added",
      applied: configApplied,
      version: PLIMSOLL_VERSION,
      appendedAt,
      durationMs: Date.now() - startedAt,
      ...plan,
      writtenSha256,
      // Only ever a backup that exists: a failed backup step publishes null
      // rather than a path a recovery script would hash to ENOENT (N2).
      backupPath: backupOnDisk ? backupPath : null,
      backupWritten,
      failure,
      // What an operator can rely on after a failure, enumerated:
      //   config_applied_collector_restarted        the write completed; the
      //     config names the new roots and the fence belongs to them.
      //   ledger_fence_retained                     the config was NOT
      //     written but the fence could not be rolled back; `fenceRollback
      //     .retainedFiles` lists exactly the files still excluded.
      //   config_unchanged_fence_rolled_back        the config is
      //     byte-identical to the backup and every generation row this run
      //     sealed was removed.
      //   config_unchanged_no_backup_written        nothing was written at
      //     all — the failure was at or before the backup step.
      //   config_unchanged_restored_state_matches_backup   the config is
      //     byte-identical to the backup and this run sealed nothing.
      recovery: failure
        ? configApplied
          ? "config_applied_collector_restarted"
          : fenceRollback && fenceRollback.complete !== true
            ? "ledger_fence_retained"
            : !backupWritten
              ? "config_unchanged_no_backup_written"
              : fenceRollback
                ? "config_unchanged_fence_rolled_back"
                : "config_unchanged_restored_state_matches_backup"
        : null,
      fenceRollback,
      baseline,
      restart,
    };
    const receiptsDirectory = path.join(collectorHome(), "receipts");
    fs.mkdirSync(receiptsDirectory, { recursive: true, mode: 0o700 });
    const receiptPath = path.join(
      receiptsDirectory,
      `capture-roots-add-${appendedAt.replace(/[:.]/g, "-")}.json`,
    );
    fs.writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });
    console.log(JSON.stringify({ ...receipt, receiptPath }, null, 2));
    process.exitCode = failure || restartFailed || writtenSha256 !== plan.afterSha256 ? 1 : 0;
    return;
  }

  if (command === "export") {
    const buffer = openBuffer(config);
    const requestedLimit = optionValue("--limit") ? Number(optionValue("--limit")) : 5;
    const limit = Number.isFinite(requestedLimit)
      ? Math.max(1, Math.min(Math.trunc(requestedLimit), 1_000))
      : 5;
    console.log(
      JSON.stringify(
        {
          limit,
          events: buffer.list(limit),
        },
        null,
        2,
      ),
    );
    buffer.close();
    return;
  }

  if (command === "upload") {
    const buffer = openBuffer(config, Boolean(optionValue("--url")));
    const markUploaded = !flag("--no-mark");
    const maxBatches = optionValue("--max-batches") ? Number(optionValue("--max-batches")) : 20;
    let uploadedEvents = 0;
    let batches = 0;
    let lastResult: Awaited<ReturnType<typeof uploadBufferedEvents>> | null = null;
    while (batches < Math.max(1, maxBatches)) {
      const result = await uploadBufferedEvents(config, buffer, {
        url: optionValue("--url"),
        limit: optionValue("--limit") ? Number(optionValue("--limit")) : undefined,
        ingestKey: optionValue("--ingest-key"),
        signingSecret: optionValue("--signing-secret"),
        markUploaded,
      });
      lastResult = result;
      uploadedEvents += result.uploadedEvents;
      batches += 1;
      if (result.uploadedEvents === 0 || !markUploaded || result.remainingDelivery === 0) {
        break;
      }
    }
    if (uploadedEvents > 0) recordDeviceUpload();
    console.log(
      JSON.stringify(
        {
          uploadedEvents,
          batches,
          markedUploaded: markUploaded,
          remainingUnuploaded:
            lastResult?.remainingUnuploaded ?? buffer.delivery.status().remainingDelivery,
          remainingDelivery:
            lastResult?.remainingDelivery ?? buffer.delivery.status().remainingDelivery,
          signedUpload: lastResult?.signedUpload ?? false,
          response: lastResult?.response ?? null,
          localBufferRetained: true,
        },
        null,
        2,
      ),
    );
    buffer.close();
    return;
  }

  if (command === "upload-history") {
    // Workspace backfill (issue 0035): the full ledger history, read-only,
    // idempotent by event id. Progress and the final reconciliation audit go
    // to stdout; the server response is never echoed (it can contain the
    // install key).
    const numberOption = (name: string) => {
      const raw = optionValue(name);
      if (raw === undefined) return undefined;
      const value = Number(raw);
      if (!Number.isFinite(value)) throw new Error(`${name} expects a number, got: ${raw}`);
      return value;
    };
    if (flag("--repair-attribution")) {
      const repair = await runAttributionRepair(config, {
        until: optionValue("--until"),
        batchSize: numberOption("--batch-size"),
        concurrency: numberOption("--concurrency"),
        delayMs: numberOption("--delay-ms"),
        dryRun: flag("--dry-run"),
        url: optionValue("--url"),
      });
      if (!repair.ok) process.exitCode = 1;
      return;
    }
    if (flag("--sessions")) {
      // Session backfill (issue 0037): push one snapshot per stitched ledger
      // session; the cloud upserts grow-only by deterministic session id, so
      // re-running over the same --until changes nothing.
      const sessions = await runSessionSync(config, {
        until: optionValue("--until"),
        batchSize: numberOption("--batch-size"),
        concurrency: numberOption("--concurrency"),
        delayMs: numberOption("--delay-ms"),
        dryRun: flag("--dry-run"),
        url: optionValue("--url"),
      });
      if (!sessions.ok) process.exitCode = 1;
      return;
    }
    const result = await runWorkspaceHistoryUpload(config, {
      until: optionValue("--until"),
      batchSize: numberOption("--batch-size"),
      concurrency: numberOption("--concurrency"),
      delayMs: numberOption("--delay-ms"),
      limit: numberOption("--limit"),
      full: flag("--full"),
      dryRun: flag("--dry-run"),
      url: optionValue("--url"),
    });
    if (!result.ok) process.exitCode = 1;
    return;
  }

  if (command === "upload-replay") {
    // Bead .46: a dead letter written for a remote reason records the cloud
    // rejecting an envelope, not a decision about the row. Once that contract
    // is fixed the delivery is viable again, so replay supersedes the dead
    // receipt and re-queues the raw row. It never uploads: delivery happens on
    // the ordinary `upload` cycles, which keeps it safe to run while the
    // upload circuit is open.
    const reason = optionValue("--reason");
    if (!reason) {
      throw new Error(
        "upload-replay requires --reason <receipt reason>, e.g. --reason remote_validation_rejected",
      );
    }
    const limitRaw = optionValue("--limit");
    let limit: number | undefined;
    if (limitRaw !== undefined) {
      limit = Number(limitRaw);
      if (!Number.isFinite(limit) || Math.trunc(limit) < 1) {
        throw new Error(`upload-replay --limit expects a positive number, got: ${limitRaw}`);
      }
    }
    // Replay is a recovery tool: it must work on a host whose upload URL is
    // not configured in this invocation, so delivery bookkeeping is enabled
    // explicitly rather than inferred from the config.
    const buffer = openBuffer(config, true);
    try {
      const summary = buffer.delivery.replayDeadLetters({
        reason,
        since: optionValue("--since"),
        limit,
        dryRun: flag("--dry-run"),
      });
      console.log(JSON.stringify(summary, null, 2));
    } finally {
      buffer.close();
    }
    return;
  }

  if (command === "push-repo-labels") {
    // Repo labels are deliberate owner disclosures (issue 0036): show the
    // exact payload, then require explicit consent before anything is sent.
    const prepared = prepareRepoLabelsPush();
    console.log(prepared.preview);
    if (prepared.candidates.length === 0) {
      console.log(JSON.stringify({ status: "repo_labels_noop", reason: "no labels recorded locally" }));
      return;
    }
    if (flag("--dry-run")) {
      console.log(JSON.stringify({ status: "repo_labels_dry_run", wouldPush: prepared.candidates.length }));
      return;
    }
    if (!flag("--yes")) {
      if (!process.stdin.isTTY) {
        console.error("Refusing to push labels without confirmation. Re-run with --yes (or --dry-run to preview).");
        process.exitCode = 1;
        return;
      }
      const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
      const answer = (await rl.question("Push these labels to the workspace? [y/N] ")).trim().toLowerCase();
      rl.close();
      if (answer !== "y" && answer !== "yes") {
        console.log("Nothing sent.");
        return;
      }
    }
    const pushed = await pushRepoLabels(config, prepared.candidates, {
      url: optionValue("--url"),
    });
    console.log(
      JSON.stringify(
        {
          status: "repo_labels_pushed",
          pushed: pushed.pushed,
          created: pushed.created,
          updated: pushed.updated,
          batches: pushed.batches,
          skippedInvalid: prepared.skippedInvalid,
        },
        null,
        2,
      ),
    );
    return;
  }

  if (command === "sync-outcomes") {
    // Outcomes feed (issue 0038 / cloud Phase D2): push the local session↔PR
    // join for one named repo. The audit table and honest sent/accepted
    // counters go to stdout; the server response is never echoed raw.
    const repository = optionValue("--repository");
    if (!repository) {
      console.error("sync-outcomes requires --repository owner/repo (the explicit disclosure that scopes the run).");
      process.exitCode = 1;
      return;
    }
    const numberOption = (name: string) => {
      const raw = optionValue(name);
      if (raw === undefined) return undefined;
      const value = Number(raw);
      if (!Number.isFinite(value)) throw new Error(`${name} expects a number, got: ${raw}`);
      return value;
    };
    const outcomes = await runOutcomesSync(config, {
      repository,
      sinceDays: numberOption("--since-days"),
      reworkWindowDays: numberOption("--rework-window-days"),
      until: optionValue("--until"),
      dryRun: flag("--dry-run"),
      url: optionValue("--url"),
    });
    if (!outcomes.ok) process.exitCode = 1;
    return;
  }

  if (command === "backfill-outcome-timeline") {
    const repository = optionValue("--repository");
    const match = repository?.match(/^([^/]+)\/([^/]+)$/);
    if (!match) {
      console.error("backfill-outcome-timeline requires --repository owner/repo.");
      process.exitCode = 1;
      return;
    }
    const until = optionValue("--until") ?? new Date().toISOString();
    const since =
      optionValue("--since") ?? new Date(Date.parse(until) - 30 * 24 * 60 * 60 * 1000).toISOString();
    const maxPullsRaw = optionValue("--max-prs") ?? "25";
    const maxPulls = Number(maxPullsRaw);
    if (!Number.isInteger(maxPulls)) throw new Error(`--max-prs expects an integer, got: ${maxPullsRaw}`);
    const reworkWindowDaysRaw = optionValue("--rework-window-days") ?? "14";
    const reworkWindowDays = Number(reworkWindowDaysRaw);
    if (!Number.isInteger(reworkWindowDays)) {
      throw new Error(`--rework-window-days expects an integer, got: ${reworkWindowDaysRaw}`);
    }
    const databasePath =
      optionValue("--store") ?? path.join(collectorHome(), "outcome-timeline-v1.sqlite");
    const store = new OutcomeTimelineStore(databasePath);
    try {
      const receipt = await runOutcomeTimelineBackfill({
        owner: match[1]!,
        repo: match[2]!,
        since,
        until,
        maxPulls,
        reworkWindowDays,
        store,
        adapter: new GitHubRestOutcomeTimelineAdapter({
          token: process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN,
        }),
        requiredChecks: readRequiredCheckPolicy(optionValue("--required-checks")),
      });
      console.log(JSON.stringify(receipt, null, 2));
      if (receipt.status === "incomplete" || receipt.status === "unknown") process.exitCode = 1;
    } finally {
      store.close();
    }
    return;
  }

  if (command === "backfill-outcome-performance") {
    const repository = optionValue("--repository");
    const match = repository?.match(/^([^/]+)\/([^/]+)$/);
    if (repository && !match) {
      console.error("backfill-outcome-performance --repository must be owner/repo.");
      process.exitCode = 1;
      return;
    }
    const reworkWindowDaysRaw = optionValue("--rework-window-days") ?? "14";
    const reworkWindowDays = Number(reworkWindowDaysRaw);
    if (!Number.isInteger(reworkWindowDays) || reworkWindowDays < 1 || reworkWindowDays > 365) {
      throw new Error(`--rework-window-days expects an integer from 1 through 365, got: ${reworkWindowDaysRaw}`);
    }
    const databasePath = optionValue("--store") ?? path.join(collectorHome(), "outcome-timeline-v1.sqlite");
    const store = new OutcomeTimelineStore(databasePath);
    try {
      const repositoryExternalId = match
        ? `github:repository:${match[1]!.toLowerCase()}/${match[2]!.toLowerCase()}`
        : undefined;
      const receipt = store.materializePerformance({
        repositoryExternalId,
        requiredChecks: readRequiredCheckPolicy(optionValue("--required-checks")),
        reworkWindowDays,
      });
      console.log(JSON.stringify({
        schema: "plimsoll.outcome-performance-backfill.v1",
        localOnly: true,
        repositoryExternalId: repositoryExternalId ?? null,
        ...receipt,
      }, null, 2));
    } finally {
      store.close();
    }
    return;
  }

  if (command === "weekly-performance-rollup") {
    const until = optionValue("--until") ?? new Date().toISOString();
    const untilMs = Date.parse(until);
    if (!Number.isFinite(untilMs)) throw new Error(`--until expects an ISO timestamp, got: ${until}`);
    const asOf = new Date(untilMs).toISOString();
    const databasePath = optionValue("--store") ?? path.join(collectorHome(), "outcome-timeline-v1.sqlite");
    const outputDirectory = path.resolve(optionValue("--out-dir") ?? path.join(process.cwd(), "performance-rollups"));
    const week = asOf.slice(0, 10);
    const jsonPath = path.join(outputDirectory, `weekly-performance-${week}.json`);
    const markdownPath = path.join(outputDirectory, `weekly-performance-${week}.md`);
    const store = new OutcomeTimelineStore(databasePath);
    try {
      const summary = store.performanceSummary(7, asOf);
      fs.mkdirSync(outputDirectory, { recursive: true, mode: 0o700 });
      fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, { mode: 0o600 });
      fs.writeFileSync(markdownPath, formatWeeklyPerformanceMarkdown(summary), { mode: 0o600 });
      console.log(JSON.stringify({
        schema: "plimsoll.weekly-performance-rollup.v1",
        scheduled: false,
        localOnly: true,
        summary,
        outputs: { jsonPath, markdownPath },
      }, null, 2));
    } finally {
      store.close();
    }
    return;
  }

  if (command === "materialize-learning-evidence") {
    const until = optionValue("--until") ?? new Date().toISOString();
    if (!Number.isFinite(Date.parse(until))) throw new Error(`--until expects an ISO timestamp, got: ${until}`);
    const windowDaysRaw = optionValue("--window-days") ?? "7";
    const windowDays = Number(windowDaysRaw);
    if (!Number.isInteger(windowDays) || windowDays < 1 || windowDays > 365) {
      throw new Error(`--window-days expects an integer from 1 through 365, got: ${windowDaysRaw}`);
    }
    const maxNewEventsRaw = optionValue("--max-new-events") ?? "100000";
    const maxNewEvents = Number(maxNewEventsRaw);
    if (!Number.isInteger(maxNewEvents) || maxNewEvents < 0) {
      throw new Error(`--max-new-events expects a non-negative integer, got: ${maxNewEventsRaw}`);
    }
    const receipt = runLearningMaterialization({
      ledgerPath: optionValue("--ledger") ?? path.join(collectorHome(), "work-ledger.sqlite"),
      outcomeStorePath: optionValue("--store") ?? path.join(collectorHome(), "outcome-timeline-v1.sqlite"),
      statePath: optionValue("--state") ?? path.join(collectorHome(), "learning-materialization-v1.sqlite"),
      outPath: optionValue("--out") ?? path.join(process.cwd(), "evidence", "learning-evidence-packet.json"),
      until,
      windowDays,
      maxNewUsageEvents: maxNewEvents,
    });
    console.log(JSON.stringify(receipt, null, 2));
    if (receipt.status === "blocked_dependencies") process.exitCode = 1;
    return;
  }

  if (command === "forward-hook") {
    const source = collectorSourceFromArg(process.argv[3]);
    const body = await readStdin();
    let payload: unknown;
    try {
      payload = JSON.parse(body || "{}") as unknown;
    } catch {
      payload = {
        id: `malformed_hook_${Date.now()}`,
        event_type: "unknown",
        body_bytes: Buffer.byteLength(body),
        body_parse_error: "invalid_json",
      };
    }
    const buffer = openBuffer(config);
    try {
      appendForwardedHook(payload, { config, buffer, source });
    } catch (error) {
      console.error(
        JSON.stringify({
          warning: "hook_forward_failed",
          message: error instanceof Error ? error.message : String(error),
        }),
      );
    } finally {
      buffer.close();
    }
    return;
  }

  if (command === "forward-hook-http") {
    const source = collectorSourceFromArg(process.argv[3]);
    const body = await readStdin();
    const auth = loadOrCreateLocalIngestAuth(collectorHome());
    await forwardHookOverLoopback(body, {
      source,
      port: config.port,
      auth,
    });
    return;
  }

  if (command === "self-test-hook") {
    const source = collectorSourceFromArg(process.argv[3]);
    const buffer = openBuffer(config);
    const normalized = appendForwardedHook(
      {
        id: `self_test_${Date.now()}`,
        source,
        event_type: "UserPromptSubmit",
        project: "ai-work-intelligence",
        prompt: "self-test raw prompt should be suppressed in metadata mode",
      },
      { config, buffer, source },
    );
    console.log(
      JSON.stringify(
        {
          accepted: true,
          eventId: normalized.event.id,
          suppressedFields: normalized.suppressedFields,
        },
        null,
        2,
      ),
    );
    buffer.close();
    return;
  }

  if (command === "generate-config") {
    const tool = process.argv[3] ?? "all";
    // Printing stays side-effect free: tokens appear only when provisioned.
    const localAuth = readLocalIngestAuth(collectorHome());
    const grokHeaderFile = path.join(resolveGrokHome().home, "hooks", "plimsoll.headers");
    const codexHeaderFile = path.join(os.homedir(), ".codex", "plimsoll.headers");
    const options = {
      repoRoot: optionValue("--repo-root") ?? process.cwd(),
      port: config.port,
      dataMode: flag("--evidence") ? "evidence" as const : config.policy.dataMode,
      confirmEvidence: flag("--confirm-evidence"),
      pnpmCommand: optionValue("--pnpm") ?? "pnpm",
      grokHeaderFile,
      codexHeaderFile,
      ...(localAuth
        ? {
            claudeCodeProducerToken: localAuth.claudeCodeProducer,
            codexProducerToken: localAuth.codexProducer,
            geminiCliProducerToken: localAuth.geminiCliProducer,
            grokProducerToken: localAuth.grokProducer,
          }
        : {}),
    };

    if (tool === "claude-code") {
      console.log(JSON.stringify(generateClaudeCodeSettings(options), null, 2));
      return;
    }

    if (tool === "codex") {
      console.log(generateCodexConfigToml(options));
      return;
    }

    if (tool === "gemini-cli") {
      console.log(JSON.stringify(generateGeminiCliSettings(options), null, 2));
      return;
    }

    if (tool === "grok") {
      console.log(JSON.stringify(generateGrokHookSettings(options), null, 2));
      return;
    }

    if (tool === "all") {
      console.log(
        JSON.stringify(
          {
            instructions: generateSetupInstructions(options),
            claudeCodeSettings: generateClaudeCodeSettings(options),
            codexConfigToml: generateCodexConfigToml(options),
            geminiCliSettings: generateGeminiCliSettings(options),
            grokHookSettings: generateGrokHookSettings(options),
          },
          null,
          2,
        ),
      );
      return;
    }

    throw new Error("Expected tool to be claude-code, codex, gemini-cli, grok, or all.");
  }

  if (command === "lifecycle") {
    const action = process.argv[3] ?? "";
    if (!["update", "rollback", "uninstall", "purge", "support-bundle"].includes(action)) {
      throw new Error("Expected lifecycle update|rollback|uninstall|purge|support-bundle");
    }
    const resolveArtifact = async (reference: string) => {
      if (reference === "self") return resolveSelfArtifact();
      if (!path.isAbsolute(reference) || !fs.existsSync(reference)) {
        throw new Error("--artifact must be `self` or an absolute path to a built plimsoll bundle");
      }
      const version = optionValue("--artifact-version");
      if (!version) {
        throw new Error("explicit --artifact paths require --artifact-version (the version to record)");
      }
      return resolveArtifactFromBundle({
        bundlePath: fs.realpathSync(reference),
        version,
      });
    };
    const readinessTimeoutOption = Number(optionValue("--readiness-timeout-ms"));
    const result = await runLifecycleCommand({
      argv: [action, ...process.argv.slice(4)],
      adapter: composeLifecycleAdapter(),
      resolveArtifact,
      ...(optionValue("--readiness-timeout-ms") !== undefined && Number.isFinite(readinessTimeoutOption)
        ? { readinessTimeoutMs: readinessTimeoutOption }
        : {}),
    });
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "install-launch-agent") {
    const runningScript = process.argv[1] ?? "";
    const development = flag("--dev");
    const packaged = /\.(mjs|cjs|js)$/.test(runningScript) && fs.existsSync(runningScript);
    if (!development && !packaged) {
      throw new Error(
        "Source-tree LaunchAgent installs require --dev. Packaged installs run the stable plimsoll executable directly.",
      );
    }
    if (!development && (optionValue("--repo-root") || optionValue("--pnpm"))) {
      throw new Error("--repo-root and --pnpm are development-only options; add --dev.");
    }

    const stableCliPath = packaged ? fs.realpathSync(runningScript) : null;
    const repoRoot = development
      ? optionValue("--repo-root") ?? process.cwd()
      : path.dirname(stableCliPath ?? process.cwd());
    const dryRun = flag("--dry-run");
    const result = installLaunchAgent({
      repoRoot,
      pnpmPath: optionValue("--pnpm") ?? "pnpm",
      programArguments: development
        ? undefined
        : [process.execPath, stableCliPath ?? runningScript, "start"],
      workingDirectory: development ? repoRoot : path.dirname(stableCliPath ?? runningScript),
      dryRun,
      // Issue #158: real installs hold the shared lifecycle mutation lease;
      // previews never acquire it.
      ...(dryRun ? {} : { mutationAuthority: launchAgentMutationAuthority() }),
    });
    if (dryRun) {
      console.log(JSON.stringify({
        ...result.receipt,
        runtime: development ? "development" : "packaged",
        loadIntent: flag("--load") ? "would_load_after_visible_postcondition" : "not_requested",
      }, null, 2));
      return;
    }
    const visible = inspectLaunchAgentManifest();
    if (!visible.ok || visible.manifestDigest !== result.receipt.manifestDigest) {
      throw new Error("LaunchAgent visible manifest postcondition failed after install.");
    }
    const load = flag("--load")
      ? await loadVisibleLaunchAgent(result.plistPath, config.port, result.receipt.changed, launchAgentMutationAuthority())
      : { loaded: false, status: "not_requested" as const, manifestDigest: visible.manifestDigest };
    console.log(
      JSON.stringify(
        {
          ...result.receipt,
          installed: true,
          runtime: development ? "development" : "packaged",
          plistPath: result.plistPath,
          load,
        },
        null,
        2,
      ),
    );
    if (flag("--load") && !load.loaded && process.exitCode === undefined) process.exitCode = 1;
    return;
  }

  if (command === "load-launch-agent") {
    const plistPath = launchAgentPlistPath();
    const visible = inspectLaunchAgentManifest();
    if (!visible.ok) {
      console.log(
        JSON.stringify(
          {
            loaded: false,
            reason: visible.status === "missing" ? "plist_missing" : "plist_invalid",
            plistPath,
            label: LAUNCH_AGENT_LABEL,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
      return;
    }
    const load = await loadVisibleLaunchAgent(plistPath, config.port, false, launchAgentMutationAuthority());
    console.log(JSON.stringify({ ...load, plistPath, label: LAUNCH_AGENT_LABEL }, null, 2));
    if (!load.loaded && process.exitCode === undefined) process.exitCode = 1;
    return;
  }

  if (command === "unload-launch-agent") {
    console.log(
      JSON.stringify(
        {
          unloading: true,
          label: LAUNCH_AGENT_LABEL,
          unloadCommand: launchctlBootoutCommand().join(" "),
        },
        null,
        2,
      ),
    );
    const result = await executeLaunchAgentUnload(config.port, launchAgentMutationAuthority());
    console.log(JSON.stringify(launchAgentUnloadReceipt(result), null, 2));
    if (!result.unloaded) process.exitCode = 1;
    return;
  }

  if (command === "uninstall-launch-agent") {
    if (flag("--dry-run")) {
      const preview = uninstallLaunchAgent({ dryRun: true });
      console.log(JSON.stringify(preview.receipt, null, 2));
      return;
    }
    const mutationAuthority = launchAgentMutationAuthority();
    let unloadResult: Awaited<ReturnType<typeof executeLaunchAgentUnload>> | null = null;
    if (flag("--unload")) {
      unloadResult = await executeLaunchAgentUnload(config.port, mutationAuthority);
      if (!unloadResult.unloaded) {
        console.log(JSON.stringify({
          removed: false,
          ...launchAgentUnloadReceipt(unloadResult),
        }, null, 2));
        process.exitCode = 1;
        return;
      }
    }
    // Issue #158: the manifest removal itself fences on the same authority.
    const removed = uninstallLaunchAgent({ mutationAuthority });
    console.log(
      JSON.stringify(
        {
          ...removed.receipt,
          removed: removed.receipt.status === "removed",
          ...(unloadResult
            ? {
                unloaded: true,
                unload: launchAgentUnloadReceipt(unloadResult),
              }
            : {}),
        },
        null,
        2,
      ),
    );
    return;
  }

  if (command === "label") {
    const kind = process.argv[3];
    const hash = process.argv[4];
    const name = process.argv.slice(5).join(" ").trim();
    if (kind !== "account" || !hash || !name) {
      throw new Error('Usage: label account <sha256:hash> "<display name>"');
    }
    const buffer = openBuffer(config);
    buffer.setAccountLabel(hash, name);
    console.log(JSON.stringify({
      labeled: true,
      accountHash: hash,
      label: name,
      compatibilityNote:
        "Account labels are local-only presentation mappings; they never rewrite assertions or event history.",
    }, null, 2));
    buffer.close();
    return;
  }

  if (command === "priority") {
    const action = process.argv[3];
    const buffer = openBuffer(config);
    try {
      if (action === "list") {
        console.log(JSON.stringify({ priorityRepos: buffer.listPriorityRepos() }, null, 2));
        return;
      }
      const url = process.argv[4];
      const { remoteLinkageHash, normalizeGitRemote } = await import("../../shared/src/index");
      const repoHash = remoteLinkageHash(url);
      if (!url || !repoHash) {
        throw new Error("Usage: priority add|remove <git-repo-url> (e.g. https://github.com/org/repo)");
      }
      if (action === "add") {
        buffer.setPriorityRepo(repoHash, normalizeGitRemote(url) ?? url);
        console.log(JSON.stringify({ added: true, repoHash, url: normalizeGitRemote(url) }, null, 2));
        return;
      }
      if (action === "remove") {
        const removed = buffer.removePriorityRepo(repoHash);
        console.log(JSON.stringify({ removed: removed > 0, repoHash }, null, 2));
        return;
      }
      throw new Error("Expected priority add|remove|list");
    } finally {
      buffer.close();
    }
  }

  if (command === "purge-local-data") {
    const confirmed = flag("--confirm");
    const includeConfig = flag("--include-config");
    const targets = [
      {
        exists: fs.existsSync(collectorBufferPath()),
        label: "local event buffer",
        path: collectorBufferPath(),
        purged: false,
      },
      {
        exists: fs.existsSync(collectorLogPath("collector.pid")),
        label: "foreground daemon pid file",
        path: collectorLogPath("collector.pid"),
        purged: false,
      },
      ...(includeConfig
        ? [
            {
              exists: fs.existsSync(collectorConfigPath()),
              label: "collector config",
              path: collectorConfigPath(),
              purged: false,
            },
          ]
        : []),
    ];

    if (confirmed) {
      for (const target of targets) {
        if (!target.exists) continue;
        fs.rmSync(target.path, { force: true, recursive: false });
        target.purged = true;
      }
    }

    console.log(
      JSON.stringify(
        {
          confirmed,
          dryRun: !confirmed,
          homePath: collectorHome(),
          includeConfig,
          invasivePermissionsRequested: {
            screenRecording: false,
            accessibilityKeyboard: false,
            clipboardBody: false,
            browserHistory: false,
          },
          launchAgentTouched: false,
          targets,
        },
        null,
        2,
      ),
    );
    return;
  }

  if (command === "stop") {
    const pidPath = collectorLogPath("collector.pid");
    const launchAgentStopCommand = launchctlBootoutCommand().join(" ");
    const initialReconciliation = reconcileCollectorPidCleanupState(
      pidPath,
      LAUNCH_AGENT_LABEL,
    );
    const initialCleanup = initialReconciliation.after;
    const pidRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
    if (initialCleanup.ambiguous) {
      const listener = await observeCollectorListener(config.port);
      const recordedProcessLive = pidRead.kind === "current"
        ? processIdentityIsLive(pidRead.record)
        : null;
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "pid_cleanup_ambiguous",
            pid: pidRead.kind === "current" ? pidRead.record.pid : null,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned: false,
            pidRecordState: pidRead.kind,
            pidCleanup: pidCleanupStateReceipt(initialCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(null),
            processState:
              recordedProcessLive === null
                ? "unknown"
                : recordedProcessLive
                  ? "live"
                  : "not_live",
            listenerState: listener.kind,
            listenerRuntimeIdentity:
              listener.kind === "collector" ? listener.runtimeIdentity : null,
            removedPidFile: false,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
      return;
    }
    if (pidRead.kind !== "current") {
      const pid = pidRead.kind === "legacy" ? pidRead.pid : null;
      const listener = await observeCollectorListener(config.port);
      const alreadyStopped = pidRead.kind === "missing" && listener.kind === "absent";
      const reason = alreadyStopped
        ? null
        : pidRead.kind === "legacy"
          ? "legacy_pid_file_blocked"
          : pidRead.kind === "invalid"
            ? "invalid_pid_file"
            : pidRead.kind === "unsafe"
              ? "unsafe_pid_file"
              : listener.kind === "indeterminate"
                ? "listener_indeterminate"
                : "listener_still_present";
      console.log(
        JSON.stringify(
          {
            stopped: alreadyStopped,
            status: alreadyStopped ? "already_stopped" : "refused",
            reason,
            pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned: pidRead.kind === "missing",
            pidRecordState: pidRead.kind,
            pidCleanup: pidCleanupStateReceipt(initialCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(null),
            processState: pidRead.kind === "missing" ? "not_present" : "unknown",
            listenerState: listener.kind,
            listenerRuntimeIdentity:
              listener.kind === "collector" ? listener.runtimeIdentity : null,
            removedPidFile: false,
          },
          null,
          2,
        ),
      );
      if (!alreadyStopped) process.exitCode = 1;
      return;
    }

    const runtimeIdentity: CollectorRuntimeIdentity = {
      instanceId: pidRead.record.instanceId,
      pid: pidRead.record.pid,
      processStartFingerprint: pidRead.record.processStartFingerprint,
      processStartFingerprintAlgorithm:
        pidRead.record.processStartFingerprintAlgorithm,
    };
    const identityClass = classifyProcessIdentity(runtimeIdentity);
    if (identityClass === "indeterminate") {
      // Fail closed: no signal, no cleanup, no mutation.
      const listener = await observeCollectorListener(config.port);
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "runtime_identity_indeterminate",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned: false,
            pidRecordState: pidRead.kind,
            pidCleanup: pidCleanupStateReceipt(initialCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(null),
            processState: "indeterminate",
            listenerState: listener.kind,
            listenerRuntimeIdentity:
              listener.kind === "collector" ? listener.runtimeIdentity : null,
            removedPidFile: false,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
      return;
    }
    if (identityClass === "stale") {
      const cleanupAttempt = removeCollectorPidFileIfOwned(
        pidPath,
        runtimeIdentity,
        LAUNCH_AGENT_LABEL,
      );
      const after = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
      const persistentCleanup = readCollectorPidCleanupState(pidPath, LAUNCH_AGENT_LABEL);
      const listener = await observeCollectorListener(config.port);
      const pidCleaned =
        after.kind === "missing" &&
        !cleanupAttempt.ambiguous &&
        !persistentCleanup.ambiguous;
      const stopped = pidCleaned && listener.kind === "absent";
      console.log(
        JSON.stringify(
          {
            stopped,
            reason: stopped ? null : "process_not_running_or_reused",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned,
            pidRecordState: after.kind,
            pidCleanup: pidCleanupStateReceipt(persistentCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(cleanupAttempt),
            processState: "not_live",
            listenerState: listener.kind,
            listenerRuntimeIdentity:
              listener.kind === "collector" ? listener.runtimeIdentity : null,
            removedPidFile: cleanupAttempt.removed,
          },
          null,
          2,
        ),
      );
      if (!stopped) process.exitCode = 1;
      return;
    }

    const identityVerified = await verifyCollectorRuntimeIdentity(config.port, runtimeIdentity, {
      probeCount: 2,
    });
    if (
      !identityVerified ||
      classifyProcessIdentity(runtimeIdentity) === "indeterminate"
    ) {
      const processLive = processIdentityIsLive(runtimeIdentity);
      const listener = await observeCollectorListener(config.port);
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "runtime_identity_unverified",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned: false,
            pidRecordState: pidRead.kind,
            pidCleanup: pidCleanupStateReceipt(initialCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(null),
            processState: processLive ? "live" : "not_live",
            listenerState: listener.kind,
            listenerRuntimeIdentity:
              listener.kind === "collector" ? listener.runtimeIdentity : null,
            removedPidFile: false,
            runtimeIdentity,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
      return;
    }

    try {
      process.kill(runtimeIdentity.pid, "SIGTERM");
      const stopDeadlineAt = Date.now() + 4_000;
      let processLive = true;
      let removedPidFile = false;
      let cleanupAmbiguous = false;
      let cleanupQuarantined = false;
      let lastCleanupAttempt: CollectorPidCleanupResult | null = null;
      let lastPidRead: ReturnType<typeof readCollectorPidFile> = pidRead;
      let lastPersistentCleanup = initialCleanup;
      let lastListener = await observeCollectorListener(config.port);
      while (Date.now() < stopDeadlineAt) {
        processLive = processIdentityIsLive(runtimeIdentity);
        const currentPid = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
        const stillOwned =
          currentPid.kind === "current" &&
          currentPid.record.instanceId === runtimeIdentity.instanceId &&
          currentPid.record.pid === runtimeIdentity.pid &&
          currentPid.record.processStartFingerprint === runtimeIdentity.processStartFingerprint;
        if (!processLive && stillOwned) {
          const cleanupAttempt = removeCollectorPidFileIfOwned(
            pidPath,
            runtimeIdentity,
            LAUNCH_AGENT_LABEL,
          );
          lastCleanupAttempt = cleanupAttempt;
          removedPidFile = cleanupAttempt.removed || removedPidFile;
          cleanupAmbiguous = cleanupAttempt.ambiguous || cleanupAmbiguous;
          cleanupQuarantined = cleanupAttempt.quarantined || cleanupQuarantined;
        }
        lastPidRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
        lastPersistentCleanup = readCollectorPidCleanupState(pidPath, LAUNCH_AGENT_LABEL);
        const pidCleaned =
          lastPidRead.kind === "missing" &&
          !cleanupAmbiguous &&
          !lastPersistentCleanup.ambiguous;
        if (!processLive && pidCleaned) {
          lastListener = await observeCollectorListener(config.port);
        }
        if (!processLive && pidCleaned && lastListener.kind === "absent") {
          console.log(
            JSON.stringify(
              {
                stopped: true,
                pid: runtimeIdentity.pid,
                pidCleaned: true,
                pidRecordState: lastPidRead.kind,
                pidCleanup: pidCleanupStateReceipt(lastPersistentCleanup),
                pidCleanupReconciliation: pidCleanupReconciliationReceipt(
                  initialReconciliation,
                ),
                cleanupAttempt: pidCleanupAttemptReceipt(lastCleanupAttempt),
                processState: "not_live",
                listenerState: lastListener.kind,
                listenerRuntimeIdentity: null,
                removedPidFile,
                pidCleanupAmbiguous: false,
                pidCleanupQuarantined: cleanupQuarantined,
                runtimeIdentity,
              },
              null,
              2,
            ),
          );
          return;
        }
        await new Promise<void>((resolve) => setTimeout(resolve, 50));
      }
      processLive = processIdentityIsLive(runtimeIdentity);
      lastPidRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
      lastPersistentCleanup = readCollectorPidCleanupState(pidPath, LAUNCH_AGENT_LABEL);
      lastListener = await observeCollectorListener(config.port);
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason:
              cleanupAmbiguous || lastPersistentCleanup.ambiguous
                ? "pid_cleanup_ambiguous"
                : "shutdown_timeout",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned: false,
            pidRecordState: lastPidRead.kind,
            pidCleanup: pidCleanupStateReceipt(lastPersistentCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(lastCleanupAttempt),
            processState: processLive ? "live" : "not_live",
            listenerState: lastListener.kind,
            listenerRuntimeIdentity:
              lastListener.kind === "collector" ? lastListener.runtimeIdentity : null,
            removedPidFile,
            pidCleanupAmbiguous: cleanupAmbiguous || lastPersistentCleanup.ambiguous,
            pidCleanupQuarantined:
              cleanupQuarantined || lastPersistentCleanup.quarantineCount > 0,
            runtimeIdentity,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
    } catch (error) {
      const processLive = processIdentityIsLive(runtimeIdentity);
      const cleanupAttempt = processLive
        ? null
        : removeCollectorPidFileIfOwned(pidPath, runtimeIdentity, LAUNCH_AGENT_LABEL);
      const after = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
      const persistentCleanup = readCollectorPidCleanupState(pidPath, LAUNCH_AGENT_LABEL);
      const listener = await observeCollectorListener(config.port);
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "kill_failed",
            message: error instanceof Error ? error.message : String(error),
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            pidCleaned:
              after.kind === "missing" &&
              !cleanupAttempt?.ambiguous &&
              !persistentCleanup.ambiguous,
            pidRecordState: after.kind,
            pidCleanup: pidCleanupStateReceipt(persistentCleanup),
            pidCleanupReconciliation: pidCleanupReconciliationReceipt(
              initialReconciliation,
            ),
            cleanupAttempt: pidCleanupAttemptReceipt(cleanupAttempt),
            processState: processLive ? "live" : "not_live",
            listenerState: listener.kind,
            listenerRuntimeIdentity:
              listener.kind === "collector" ? listener.runtimeIdentity : null,
            removedPidFile: cleanupAttempt?.removed ?? false,
            runtimeIdentity,
          },
          null,
          2,
        ),
      );
      process.exitCode = 1;
    }
    return;
  }

  printHelp();
  process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
