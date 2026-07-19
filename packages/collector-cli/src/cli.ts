#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline/promises";
import { isDeepStrictEqual } from "node:util";
import { parse as parseToml } from "smol-toml";

const privatePathReceipt = (value: string) =>
  `sha256:${createHash("sha256").update(path.resolve(value)).digest("hex")}`;

import { LocalEventBuffer } from "./buffer";
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
  type CollectorConfig,
} from "./config";
import { appendForwardedHook } from "./forwarder";
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
  cleanupStaleJoinHandshakeDirectories,
  finalizeActivatedPendingJoin,
  performJoin,
  resumePendingJoin,
} from "./join";
import { RolloutTailer } from "./rollout-tailer";
import { TranscriptTailer } from "./transcript-tailer";
import {
  AutomaticMaintenanceCadence,
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
  automaticCaptureRuntimeStatus,
} from "./maintenance";
import { codexReconciliationStatus } from "./codex-reconciliation";
import {
  historyCoverageStatus,
  recordExplicitFullHistoryCoverage,
} from "./history-coverage";
import { captureBaselineStatus } from "./capture-baseline";
import { createCollectorServer } from "./server";
import {
  applyClaudeSettings,
  applyCodexConfig,
  generateClaudeCodeSettings,
  generateCodexConfigToml,
  generateSetupInstructions,
} from "../../collector-config/src/index";
import type { ToolSource } from "../../shared/src/index";
import { runOutcomesSync } from "./outcomes-sync";
import {
  GitHubRestOutcomeTimelineAdapter,
  readRequiredCheckPolicy,
  runOutcomeTimelineBackfill,
} from "./github-outcome-backfill";
import { OutcomeTimelineStore } from "./outcome-timeline-store";
import { prepareRepoLabelsPush, pushRepoLabels } from "./repo-labels";
import { runSessionSync, sessionIdsFromBatches } from "./session-sync";
import { uploadBufferedEvents } from "./upload";
import { runAttributionRepair, runWorkspaceHistoryUpload } from "./upload-history";
import {
  acquireCollectorStartOwnership,
  captureLaunchAgentUnloadPriorState,
  createCollectorRuntimeIdentity,
  observeCollectorListener,
  observeLaunchAgentUnloadTerminalState,
  processIdentityIsLive,
  readCollectorPidFile,
  readProcessStartFingerprint,
  removeCollectorPidFileIfOwned,
  runtimeIdentityMatches,
  verifyCollectorRuntimeIdentity,
  type LaunchAgentLabelObservation,
  type LaunchAgentUnloadOutcome,
  type LaunchAgentUnloadPriorState,
  type CollectorPidRecord,
  type CollectorRuntimeIdentity,
} from "./runtime-ownership";

const command = process.argv[2] ?? "help";

function printHelp() {
  console.log(`Plimsoll Collector

Commands:
  start                 Start the local hook/OTLP receiver in the foreground
  status                Print local buffer and policy status
  join TOKEN|URL        Join a hosted workspace: redeem the admin's single-use
                        token, write sync credentials, verify with a handshake
  doctor --read-only --json
                        Read-only readiness check; never creates config, ledger, plist, logs, or directories
  export                Print buffered events as JSON
  forward-hook SOURCE   Read hook JSON from stdin and append it without requiring the receiver
  self-test-hook SOURCE Emit one synthetic hook event into the local buffer
  generate-config TOOL  Print Claude Code or Codex config for metadata collection
  setup                 APPLY the Claude Code + Codex telemetry config (idempotent; --yes, --dry-run)
  upload                Drain un-uploaded events to the tenant ingest API (marks rows, keeps local copies)
  upload-history        Workspace backfill: push the FULL ledger history to the joined
                        workspace, idempotently, then print a reconciliation audit.
                        Ledger is opened read-only; rows are never marked uploaded.
                        Safe alongside the live 5-minute sync: the cloud dedupes by
                        event id, so overlap deduplicates instead of duplicating.
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
  scan-rollouts         Read codex rollout files into the ledger once (full history walk)
  scan-transcripts      Read Claude Code transcript usage into the ledger once (full history walk)
  install-launch-agent  Write the user LaunchAgent plist
  load-launch-agent     Load an installed user LaunchAgent plist
  unload-launch-agent   Unload the user LaunchAgent without removing the plist
  uninstall-launch-agent Remove the user LaunchAgent plist
  label account HASH NAME    Set a local-only display label for a hashed account
  priority add|remove URL    Manage the priority-repo list (hashed; URL kept locally)
  priority list              Show priority repos
  purge-local-data      Dry-run or explicitly purge local buffered event data
  stop                  Stop the foreground daemon using the local PID file

Config tools:
  join "<join-url>#<token>" | join --token-stdin --url <cloud-base-url> | join --resume
      Prefer --token-stdin (or join -) so the single-use secret never enters
      shell history or process arguments. Workspace URL env: PLIMSOLL_CLOUD_URL.
      join --dry-run is unsupported and fails before token, network, or local-state mutation.
  generate-config claude-code|codex|all   (metadata-only; encrypted evidence vault not implemented)
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
  install-launch-agent [--load] [--dry-run]
  install-launch-agent --dev [--repo-root PATH] [--pnpm PATH] [--load]
  load-launch-agent
  unload-launch-agent
  uninstall-launch-agent [--unload] [--dry-run]
  purge-local-data [--confirm] [--include-config]
`);
}

function openBuffer(config: CollectorConfig, deliveryOverride = false) {
  ensureCollectorHome();
  return new LocalEventBuffer(collectorBufferPath(), {
    workspaceId: config.tenantId,
    delivery: {
      enabled: Boolean(config.uploadUrl) || deliveryOverride,
      limits: config.delivery,
    },
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

function collectorSourceFromArg(value: string | undefined): ToolSource {
  if (value === "claude-code") return "claude_code";
  if (value === "codex") return "codex";
  throw new Error("Expected source to be claude-code or codex.");
}

async function readStdin() {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }

  return Buffer.concat(chunks).toString("utf8");
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
  if (result.status !== 0) return { kind: "not_reported", ...details };
  const pidMatch = result.stdout.match(/^\s*pid\s*=\s*(\d+)\s*$/m);
  const pid = pidMatch ? Number(pidMatch[1]) : null;
  const processStartFingerprint = pid ? readProcessStartFingerprint(pid) : null;
  return {
    kind: "reported",
    processIdentity: pid && processStartFingerprint
      ? {
          pid,
          processStartFingerprint,
        }
      : null,
    ...details,
  };
}

async function loadVisibleLaunchAgent(
  plistPath: string,
  port: number,
  manifestChanged = false,
) {
  const visible = inspectLaunchAgentManifest();
  if (!visible.ok || visible.plistPath !== plistPath) {
    return { loaded: false, status: "visible_manifest_invalid" as const, manifestDigest: null };
  }
  const pidPath = collectorLogPath("collector.pid");
  const observeLabel = () => launchctlJobState();
  const observeListener = () => observeCollectorListener(port);
  const prior = await captureLaunchAgentUnloadPriorState({
    label: LAUNCH_AGENT_LABEL,
    pidPath,
    port,
    observeLabel,
    observeListener,
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
  };
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
    ownership: prior.ownership,
  };
}

async function executeLaunchAgentUnload(port: number): Promise<{
  unloaded: boolean;
  reason: "launchctl_failed" | LaunchAgentUnloadOutcome["state"] | null;
  status: "already_stopped" | "stopped" | "stopped_after_launchctl_failure" | "refused";
  bootoutAttempted: boolean;
  bootoutSucceeded: boolean | null;
  prior: ReturnType<typeof unloadPriorReceipt>;
  outcome: LaunchAgentUnloadOutcome;
}> {
  const pidPath = collectorLogPath("collector.pid");
  const observeLabel = () => launchctlJobState();
  const observeListener = () => observeCollectorListener(port);
  const prior = await captureLaunchAgentUnloadPriorState({
    label: LAUNCH_AGENT_LABEL,
    pidPath,
    port,
    observeLabel,
    observeListener,
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

  const bootoutSucceeded = runLaunchctl(launchctlBootoutCommand(), false);
  const outcome = await observeLaunchAgentUnloadTerminalState({
    label: LAUNCH_AGENT_LABEL,
    pidPath,
    port,
    prior,
    timeoutMs: bootoutSucceeded ? 4_000 : 0,
    observeLabel,
    observeListener,
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
    pidCleaned: result.outcome.pidCleaned,
    removedPidFile: result.outcome.removedPidFile,
    prior: result.prior,
    terminal: result.outcome.final,
    timing: result.outcome.timing,
    pidPathHash: privatePathReceipt(pidPath),
  };
}

async function checkCollectorConnectivity(port: number) {
  const controller = new AbortController();
  const timeoutMs = Number(process.env.PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS ?? "3000");
  const timeout = setTimeout(() => controller.abort(), Number.isFinite(timeoutMs) ? timeoutMs : 3000);

  try {
    const response = await fetch(`http://127.0.0.1:${port}/status`, {
      signal: controller.signal,
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

    return {
      reachable: response.ok && body?.ok === true,
      status: response.status,
      statusUrl: `http://127.0.0.1:${port}/status`,
      runtimeIdentity,
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

function readLaunchAgentState(plistPath: string) {
  if (!fs.existsSync(plistPath)) {
    return {
      ok: false,
      installed: false,
      status: "missing" as const,
      label: LAUNCH_AGENT_LABEL,
      plistPath,
      runtime: null,
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
    const launchAgentPath = typeof environment?.PATH === "string" ? environment.PATH : "";
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
      pathOk,
    );
    return {
      ok: matchesExpectedRuntime,
      installed: true,
      status: matchesExpectedRuntime ? "valid" as const : "conflicted" as const,
      label: LAUNCH_AGENT_LABEL,
      plistPath,
      runtime,
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
    startedAt: new Date().toISOString(),
    version: 2,
  };
}

async function main() {
  if (command === "help" || command === "--help" || command === "-h") {
    printHelp();
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
    const joinArguments = process.argv.slice(3);
    const targetArgument = joinArguments[0];
    const resume = targetArgument === "--resume";
    if (resume && joinArguments.length !== 1) {
      throw new Error("join --resume does not accept another token or URL.");
    }
    const tokenFromStdin = targetArgument === "--token-stdin" || targetArgument === "-";
    if (!resume && (!targetArgument || (targetArgument.startsWith("--") && !tokenFromStdin))) {
      throw new Error(
        'Usage: plimsoll join --token-stdin --url <cloud-base-url>  |  plimsoll join "<join-url>#<token>"  |  plimsoll join --resume',
      );
    }
    let joinBaseUrl: string | undefined;
    for (let index = 1; !resume && index < joinArguments.length; index += 1) {
      const argument = joinArguments[index];
      if (argument === "--token-stdin") {
        throw new Error("Choose either a positional join token/URL or --token-stdin, not both.");
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
          const target = tokenFromStdin ? (await readStdin()).trim() : targetArgument;
          if (!target) {
            throw new Error(
              'Usage: plimsoll join --token-stdin --url <cloud-base-url>  |  plimsoll join "<join-url>#<token>"  |  plimsoll join --resume',
            );
          }
          return performJoin({
            target,
            baseUrl: joinBaseUrl ?? process.env.PLIMSOLL_CLOUD_URL,
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
          installCredentialsConfigured: true,
          uploadUrl: result.uploadUrl,
          uploadSigningConfigured: result.uploadSigningConfigured,
          workspaceBoundary: result.workspaceBoundary,
          syncConfigured: true,
          privacyMode: "metadata_only",
          handshake: result.handshake,
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

  if (command === "start") {
    cleanupStaleJoinHandshakeDirectories();
    finalizeActivatedPendingJoin();
  }

  const noCreateConfigCommands = new Set([
    "doctor",
    "setup",
    "install-launch-agent",
    "load-launch-agent",
    "unload-launch-agent",
    "uninstall-launch-agent",
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

  if (command === "start") {
    const pidPath = collectorLogPath("collector.pid");
    const runtimeIdentity = createCollectorRuntimeIdentity();
    const ownership = await acquireCollectorStartOwnership({
      candidateIdentity: runtimeIdentity,
      label: LAUNCH_AGENT_LABEL,
      pidPath,
      port: config.port,
    });
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

    const buffer = openBuffer(config);
    let scheduler: CoalescingMaintenanceScheduler | undefined;
    let maintenanceCadence: AutomaticMaintenanceCadence | undefined;
    let maintenance: CollectorMaintenance | undefined;
    const maintenanceAbort = new AbortController();
    const server = createCollectorServer(config, buffer, {
      runtimeIdentity,
      maintenanceStatus: () => ({
        scheduler: scheduler?.status() ?? null,
        cadence: maintenanceCadence?.status() ?? null,
        capture: maintenance?.status() ?? null,
      }),
    });
    let ownsPidFile = false;
    let shuttingDown = false;
    const timers: NodeJS.Timeout[] = [];
    let syncFailureStreak = 0;
    let syncInFlight = false;

    const runPrune = () => {
      try {
        const pruned = buffer.prune(config.retentionDays);
        if (pruned.events > 0 || pruned.metricSamples > 0) {
          console.log(JSON.stringify({ status: "pruned", ...pruned }));
        }
      } catch (error) {
        console.warn(
          JSON.stringify({
            warning: "prune_failed",
            message: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    };

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
          const result = await uploadBufferedEvents(config, buffer, {});
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
    maintenance = new CollectorMaintenance(
      buffer,
      new RolloutTailer(buffer),
      new TranscriptTailer(buffer),
      maintenanceAbort.signal,
    );
    scheduler = new CoalescingMaintenanceScheduler(async () => {
      const result = await maintenance.runRecent();
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
    maintenanceCadence = new AutomaticMaintenanceCadence(
      scheduler,
      () => captureBaselineStatus(buffer.database),
      {
        onError: (error) => {
          if (
            maintenanceAbort.signal.aborted &&
            error instanceof Error &&
            error.message === "automatic_maintenance_aborted"
          ) return;
          console.warn(
            JSON.stringify({
              warning: "maintenance_failed",
              message: error instanceof Error ? error.message : String(error),
            }),
          );
        },
      },
    );

    runPrune();
    // Boot capture is deferred so the OTLP receiver binds first, but it uses
    // the exact same bounded recent-tail entrypoint as the interval. Historical
    // files are available only through the explicit scan commands below.
    maintenanceCadence.start();
    timers.push(setInterval(runPrune, 6 * 60 * 60 * 1000));
    if (config.uploadUrl) {
      timers.push(setInterval(() => void runSync(), config.syncIntervalSeconds * 1000));
    }
    for (const timer of timers) timer.unref();

    const shutdown = (signal: string) => {
      if (shuttingDown) {
        server.closeIdleConnections?.();
        server.closeAllConnections?.();
        return;
      }
      shuttingDown = true;
      maintenanceCadence?.stop();
      for (const timer of timers) clearInterval(timer);
      scheduler?.stopAccepting();
      maintenanceAbort.abort();
      maintenance?.close();
      ownership.release();
      const hardDeadlineMs = 2_500;
      const forceAfterMs = 750;
      const deadlineAt = performance.now() + hardDeadlineMs;
      let serverClosed = false;
      let maintenanceIdle = false;
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
      const deadline = new Promise<void>((resolve) => {
        setTimeout(resolve, hardDeadlineMs);
      });
      void (async () => {
        try {
          await Promise.race([Promise.allSettled([serverClose, idle]).then(() => undefined), deadline]);
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
          // Never close SQLite while maintenance may still be inside a write.
          if (maintenanceIdle) buffer.close();
        } catch {
          // PID ownership cleanup below is the non-negotiable finalizer.
        } finally {
          clearTimeout(forceTimer);
          server.closeIdleConnections?.();
          server.closeAllConnections?.();
          ownership.release();
          let pidCleaned = !ownsPidFile;
          if (ownsPidFile) {
            removeCollectorPidFileIfOwned(pidPath, runtimeIdentity, LAUNCH_AGENT_LABEL);
            const remaining = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
            pidCleaned =
              remaining.kind !== "current" ||
              remaining.record.instanceId !== runtimeIdentity.instanceId ||
              remaining.record.pid !== runtimeIdentity.pid ||
              remaining.record.processStartFingerprint !== runtimeIdentity.processStartFingerprint;
          }
          console.log(
            JSON.stringify({
              status: pidCleaned ? "stopped" : "shutdown_incomplete",
              signal,
              pid: process.pid,
              pidCleaned,
              maintenanceIdle,
              listenerClosed: serverClosed,
              hardDeadlineMs,
            }),
          );
          process.exit(pidCleaned ? 0 : 1);
        }
      })();
    };

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));
    server.on("error", (error: NodeJS.ErrnoException) => {
      ownership.release();
      buffer.close();
      if (ownsPidFile) {
        removeCollectorPidFileIfOwned(pidPath, runtimeIdentity, LAUNCH_AGENT_LABEL);
      }
      console.error(
        JSON.stringify(
          {
            status: "error",
            code: error.code === "EADDRINUSE" ? "port_in_use" : "listen_failed",
            port: config.port,
            message: error.message,
          },
          null,
          2,
        ),
      );
      process.exit(1);
    });
    server.listen(config.port, "127.0.0.1", () => {
      try {
        ownership.writePidFile(collectorPidRecord(runtimeIdentity));
        ownsPidFile = true;
      } catch (error) {
        ownership.release();
        server.close();
        buffer.close();
        console.error(
          JSON.stringify(
            {
              status: "error",
              code: "ownership_failed",
              pidFileOwned: ownsPidFile,
              port: config.port,
              message: error instanceof Error ? error.message : String(error),
            },
            null,
            2,
          ),
        );
        process.exit(1);
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
    const bufferPath = collectorBufferPath();
    const projected = buffer.projection.readSnapshot(30, config.subscriptions);
    const projectedStatus = projected.kind === "ready" ? projected.snapshot.status : null;
    console.log(
      JSON.stringify(
        {
          configPathHash: privatePathReceipt(collectorConfigPath()),
          bufferPathHash: privatePathReceipt(bufferPath),
          bufferFileBytes: fs.existsSync(bufferPath) ? fs.statSync(bufferPath).size : 0,
          pidPathHash: privatePathReceipt(collectorLogPath("collector.pid")),
          port: config.port,
          dataMode: config.policy.dataMode,
          privacyMode: "metadata_only",
          privacy: collectorPrivacyReadiness(config),
          retentionDays: config.retentionDays,
          syncConfigured: Boolean(config.uploadUrl),
          reconciliation: codexReconciliationStatus(buffer.database),
          stats: projectedStatus?.stats ?? null,
          delivery: buffer.delivery.status(),
          projection: buffer.projection.status(),
          captureHealth: projectedStatus?.health ?? {
            generatedAt: new Date().toISOString(),
            overall: "amber",
            sources: [],
            reason: "projection backfill has not published a coherent health snapshot",
          },
          historyCoverage: historyCoverageStatus(buffer.database),
          captureBaseline: captureBaselineStatus(buffer.database),
          automaticCapture: automaticCaptureRuntimeStatus(buffer.database),
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
    const argValue = (name: string) => {
      const index = process.argv.indexOf(name);
      return index === -1 ? undefined : process.argv[index + 1];
    };
    const yes = process.argv.includes("--yes");
    const dryRun = process.argv.includes("--dry-run");
    const claudeFile = argValue("--claude-settings") ?? path.join(os.homedir(), ".claude", "settings.json");
    const codexFile = argValue("--codex-config") ?? path.join(os.homedir(), ".codex", "config.toml");
    const toolOptions = {
      repoRoot: process.cwd(),
      port: config.port,
      dataMode: config.policy.dataMode,
    };
    const claudeGenerated = generateClaudeCodeSettings(toolOptions);
    const codexToml = generateCodexConfigToml(toolOptions);

    const planClaude = applyClaudeSettings(claudeFile, claudeGenerated, { dryRun: true });
    const planCodex = applyCodexConfig(codexFile, codexToml, { dryRun: true });
    for (const plan of [planClaude, planCodex]) {
      for (const change of plan.changes) console.log(`${plan.path}: ${change}`);
      if (plan.conflict) console.warn(`${plan.path}: ${plan.conflict}`);
    }
    if (planCodex.conflict) {
      console.error("Codex config conflict blocks setup; no config was written.");
      process.exitCode = 1;
      return;
    }
    if (!planClaude.changed && !planCodex.changed) {
      if (!dryRun && configRead?.status === "missing") loadCollectorConfig();
      console.log(JSON.stringify({ status: "setup_noop", claude: claudeFile, codex: codexFile, conflict: planCodex.conflict ?? null }));
      return;
    }
    if (dryRun) {
      console.log(JSON.stringify({ status: "setup_dry_run", wouldChange: [planClaude, planCodex].filter((plan) => plan.changed).map((plan) => plan.path) }));
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
    if (configRead?.status === "missing") loadCollectorConfig();
    const resultClaude = applyClaudeSettings(claudeFile, claudeGenerated);
    const resultCodex = applyCodexConfig(codexFile, codexToml);
    console.log(
      JSON.stringify(
        {
          status: "setup_applied",
          privacyMode: "metadata_only",
          claude: { path: resultClaude.path, changed: resultClaude.changed, backup: resultClaude.backupPath ?? null },
          codex: { path: resultCodex.path, changed: resultCodex.changed, backup: resultCodex.backupPath ?? null, conflict: resultCodex.conflict ?? null },
          nextSteps: [
            "plimsoll install-launch-agent && plimsoll load-launch-agent",
            "open http://127.0.0.1:" + config.port + "/",
            "restart any running Claude Code / Codex sessions so they pick up telemetry",
          ],
        },
        null,
        2,
      ),
    );
    return;
  }

  if (command === "scan-rollouts") {
    const buffer = openBuffer(config);
    const result = await new RolloutTailer(buffer).scan({ scope: "full" });
    const historyCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      result,
    );
    console.log(JSON.stringify({ ...result, historyCoverage }, null, 2));
    buffer.close();
    return;
  }

  if (command === "scan-transcripts") {
    const buffer = openBuffer(config);
    const result = await new TranscriptTailer(buffer).scan({ scope: "full" });
    const historyCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      result,
    );
    console.log(JSON.stringify({ ...result, historyCoverage }, null, 2));
    buffer.close();
    return;
  }

  if (command === "doctor") {
    const plistPath = launchAgentPlistPath();
    const pidPath = collectorLogPath("collector.pid");
    const claudePath = path.join(os.homedir(), ".claude", "settings.json");
    const codexPath = path.join(os.homedir(), ".codex", "config.toml");
    const toolOptions = {
      repoRoot: process.cwd(),
      port: config.port,
      dataMode: config.policy.dataMode,
    };
    const claude = readClaudeTelemetryConfig(claudePath, generateClaudeCodeSettings(toolOptions));
    const codex = readCodexTelemetryConfig(codexPath, generateCodexConfigToml(toolOptions));
    const launchAgent = readLaunchAgentState(plistPath);
    const connectivity = await checkCollectorConnectivity(config.port);
    const pidRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
    const pidRecord = pidRead.kind === "current" ? pidRead.record : null;
    const runtime = {
      ok: Boolean(
        pidRecord &&
        processIdentityIsLive(pidRecord) &&
        runtimeIdentityMatches(pidRecord, connectivity.runtimeIdentity),
      ),
      pidPath,
      pidFileStatus: pidRead.kind,
      ownershipVersion: {
        expected: 2,
        actual: pidRecord?.version ?? null,
      },
      processLive: pidRecord ? processIdentityIsLive(pidRecord) : false,
      identityMatchesStatus: pidRecord
        ? runtimeIdentityMatches(pidRecord, connectivity.runtimeIdentity)
        : false,
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
      codex.ok,
    );
    const serviceReady = configured && launchAgent.ok && connectivity.reachable && runtime.ok;
    const signalVerified = serviceReady && connectivity.signal.verified;
    const readiness = signalVerified
      ? "signal_verified"
      : serviceReady
        ? "service_ready"
        : configured
          ? "configured"
          : "not_installed";
    const ok = readiness === "signal_verified";
    const bufferPath = collectorBufferPath();
    console.log(
      JSON.stringify(
        {
          ok,
          readiness,
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
          },
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
    const options = {
      repoRoot: optionValue("--repo-root") ?? process.cwd(),
      port: config.port,
      dataMode: flag("--evidence") ? "evidence" as const : config.policy.dataMode,
      confirmEvidence: flag("--confirm-evidence"),
      pnpmCommand: optionValue("--pnpm") ?? "pnpm",
    };

    if (tool === "claude-code") {
      console.log(JSON.stringify(generateClaudeCodeSettings(options), null, 2));
      return;
    }

    if (tool === "codex") {
      console.log(generateCodexConfigToml(options));
      return;
    }

    if (tool === "all") {
      console.log(
        JSON.stringify(
          {
            instructions: generateSetupInstructions(options),
            claudeCodeSettings: generateClaudeCodeSettings(options),
            codexConfigToml: generateCodexConfigToml(options),
          },
          null,
          2,
        ),
      );
      return;
    }

    throw new Error("Expected tool to be claude-code, codex, or all.");
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
      ? await loadVisibleLaunchAgent(result.plistPath, config.port, result.receipt.changed)
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
    const load = await loadVisibleLaunchAgent(plistPath, config.port);
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
    const result = await executeLaunchAgentUnload(config.port);
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
    let unloadResult: Awaited<ReturnType<typeof executeLaunchAgentUnload>> | null = null;
    if (flag("--unload")) {
      unloadResult = await executeLaunchAgentUnload(config.port);
      if (!unloadResult.unloaded) {
        console.log(JSON.stringify({
          removed: false,
          ...launchAgentUnloadReceipt(unloadResult),
        }, null, 2));
        process.exitCode = 1;
        return;
      }
    }
    const removed = uninstallLaunchAgent({});
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
    console.log(JSON.stringify({ labeled: true, accountHash: hash, label: name }, null, 2));
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
    const pidRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
    if (pidRead.kind !== "current") {
      const pid = pidRead.kind === "legacy" ? pidRead.pid : null;
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason:
              pidRead.kind === "legacy"
                ? "legacy_pid_file_blocked"
                : pidRead.kind === "invalid"
                  ? "invalid_pid_file"
                  : "pid_file_missing",
            pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            removedPidFile: false,
          },
          null,
          2,
        ),
      );
      return;
    }

    const runtimeIdentity: CollectorRuntimeIdentity = {
      instanceId: pidRead.record.instanceId,
      pid: pidRead.record.pid,
      processStartFingerprint: pidRead.record.processStartFingerprint,
    };
    if (!processIdentityIsLive(runtimeIdentity)) {
      const removedPidFile = removeCollectorPidFileIfOwned(
        pidPath,
        runtimeIdentity,
        LAUNCH_AGENT_LABEL,
      );
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "process_not_running_or_reused",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            removedPidFile,
          },
          null,
          2,
        ),
      );
      return;
    }

    const identityVerified = await verifyCollectorRuntimeIdentity(config.port, runtimeIdentity, {
      probeCount: 2,
    });
    if (
      !identityVerified ||
      readProcessStartFingerprint(runtimeIdentity.pid) !==
        runtimeIdentity.processStartFingerprint
    ) {
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "runtime_identity_unverified",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            removedPidFile: false,
            runtimeIdentity,
          },
          null,
          2,
        ),
      );
      return;
    }

    try {
      process.kill(runtimeIdentity.pid, "SIGTERM");
      const stopDeadlineAt = Date.now() + 4_000;
      let processLive = true;
      let removedPidFile = false;
      while (Date.now() < stopDeadlineAt) {
        processLive = processIdentityIsLive(runtimeIdentity);
        const currentPid = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
        const stillOwned =
          currentPid.kind === "current" &&
          currentPid.record.instanceId === runtimeIdentity.instanceId &&
          currentPid.record.pid === runtimeIdentity.pid &&
          currentPid.record.processStartFingerprint === runtimeIdentity.processStartFingerprint;
        if (!processLive && stillOwned) {
          removedPidFile = removeCollectorPidFileIfOwned(
            pidPath,
            runtimeIdentity,
            LAUNCH_AGENT_LABEL,
          ) || removedPidFile;
        }
        const after = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
        const pidClean =
          after.kind !== "current" ||
          after.record.instanceId !== runtimeIdentity.instanceId ||
          after.record.pid !== runtimeIdentity.pid ||
          after.record.processStartFingerprint !== runtimeIdentity.processStartFingerprint;
        if (!processLive && pidClean) {
          console.log(
            JSON.stringify(
              {
                stopped: true,
                pid: runtimeIdentity.pid,
                pidCleaned: true,
                removedPidFile,
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
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "shutdown_timeout",
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            runtimeIdentity,
          },
          null,
          2,
        ),
      );
    } catch (error) {
      const removedPidFile = removeCollectorPidFileIfOwned(
        pidPath,
        runtimeIdentity,
        LAUNCH_AGENT_LABEL,
      );
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "kill_failed",
            message: error instanceof Error ? error.message : String(error),
            pid: runtimeIdentity.pid,
            pidPathHash: privatePathReceipt(pidPath),
            launchAgentStopCommand,
            removedPidFile,
            runtimeIdentity,
          },
          null,
          2,
        ),
      );
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
