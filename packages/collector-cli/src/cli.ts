#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline/promises";

import { LocalEventBuffer } from "./buffer";
import {
  assertCollectorPrivacyMode,
  collectorHome,
  collectorBufferPath,
  collectorConfigPath,
  collectorLogPath,
  collectorPrivacyReadiness,
  ensureCollectorHome,
  loadCollectorConfig,
  type CollectorConfig,
} from "./config";
import { appendForwardedHook } from "./forwarder";
import {
  installLaunchAgent,
  LAUNCH_AGENT_LABEL,
  launchAgentPlistPath,
  launchctlBootoutCommand,
  launchctlBootstrapCommand,
  uninstallLaunchAgent,
} from "./launch-agent";
import { performJoin } from "./join";
import { RolloutTailer } from "./rollout-tailer";
import { TranscriptTailer } from "./transcript-tailer";
import {
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
} from "./maintenance";
import { codexReconciliationStatus } from "./codex-reconciliation";
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
import { prepareRepoLabelsPush, pushRepoLabels } from "./repo-labels";
import { runSessionSync, sessionIdsFromBatches } from "./session-sync";
import { uploadBufferedEvents } from "./upload";
import { runAttributionRepair, runWorkspaceHistoryUpload } from "./upload-history";
import {
  acquireCollectorStartOwnership,
  createCollectorRuntimeIdentity,
  processIdentityIsLive,
  readCollectorPidFile,
  readProcessStartFingerprint,
  removeCollectorPidFileIfOwned,
  verifyCollectorRuntimeIdentity,
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
  doctor                Verify paths, SQLite buffer, LaunchAgent, data mode, and privacy posture
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
  join "<join-url>#<token>" | join <token> --url <cloud-base-url>   (env: PLIMSOLL_CLOUD_URL)
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
  install-launch-agent [--load]
  install-launch-agent --dev [--repo-root PATH] [--pnpm PATH] [--load]
  load-launch-agent
  unload-launch-agent
  uninstall-launch-agent [--unload]
  purge-local-data [--confirm] [--include-config]
`);
}

function openBuffer(config: CollectorConfig, deliveryOverride = false) {
  ensureCollectorHome();
  return new LocalEventBuffer(collectorBufferPath(), {
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

function runLaunchctl(args: string[]) {
  const result = spawnSync(args[0] ?? "launchctl", args.slice(1), {
    stdio: "inherit",
  });

  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
  }
}

async function checkCollectorConnectivity(port: number) {
  const controller = new AbortController();
  const timeoutMs = Number(process.env.PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS ?? "3000");
  const timeout = setTimeout(() => controller.abort(), Number.isFinite(timeoutMs) ? timeoutMs : 3000);

  try {
    const response = await fetch(`http://127.0.0.1:${port}/status`, {
      signal: controller.signal,
    });
    return {
      reachable: response.ok,
      status: response.status,
      statusUrl: `http://127.0.0.1:${port}/status`,
    };
  } catch (error) {
    return {
      reachable: false,
      error: error instanceof Error ? error.name : String(error),
      statusUrl: `http://127.0.0.1:${port}/status`,
    };
  } finally {
    clearTimeout(timeout);
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

  const configPath = collectorConfigPath();
  const configExistedBeforeLoad = fs.existsSync(configPath);
  const config = loadCollectorConfig();
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
            pidPath: ownership.pidPath,
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
    const server = createCollectorServer(config, buffer, {
      runtimeIdentity,
      maintenanceStatus: () => scheduler?.status() ?? null,
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

    // Codex usage truth rides rollout files (issue 0022): full walk on boot
    // (backfills any uncaptured history, idempotent), then a recent-days tail.
    const maintenance = new CollectorMaintenance(
      buffer,
      new RolloutTailer(buffer),
      new TranscriptTailer(buffer),
    );
    scheduler = new CoalescingMaintenanceScheduler(async (recentOnly) => {
      const result = await maintenance.run(recentOnly);
      const { rollout, transcript, reconciliation, repricing, enrichment } = result;
      if (rollout.eventsAppended > 0 || rollout.parseErrors > 0) {
        console.log(JSON.stringify({ status: "rollout_scan", recentOnly, ...rollout }));
      }
      if (transcript.eventsAppended > 0 || transcript.parseErrors > 0) {
        console.log(JSON.stringify({ status: "transcript_scan", recentOnly, ...transcript }));
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
    const runRolloutScan = async (recentOnly: boolean) => {
      try {
        await scheduler.trigger(recentOnly);
      } catch (error) {
        console.warn(
          JSON.stringify({
            warning: "maintenance_failed",
            message: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    };

    runPrune();
    // Boot backfill is deferred so the OTLP receiver binds first — the
    // first-ever walk reads every historical rollout (2,669 files at first
    // deploy) and must not delay ingest. Later boots skip unchanged files
    // via rollout_scan_state, so the deferred walk is cheap from then on.
    timers.push(setTimeout(() => void runRolloutScan(false), 5_000));
    timers.push(setInterval(() => void runRolloutScan(true), 60 * 1000));
    timers.push(setInterval(runPrune, 6 * 60 * 60 * 1000));
    if (config.uploadUrl) {
      timers.push(setInterval(() => void runSync(), config.syncIntervalSeconds * 1000));
    }
    for (const timer of timers) timer.unref();

    const shutdown = (signal: string) => {
      if (shuttingDown) return;
      shuttingDown = true;
      for (const timer of timers) clearInterval(timer);
      ownership.release();
      server.close(() => {
        buffer.close();
        if (ownsPidFile) {
          removeCollectorPidFileIfOwned(pidPath, runtimeIdentity, LAUNCH_AGENT_LABEL);
        }
        console.log(
          JSON.stringify({
            status: "stopped",
            signal,
            pid: process.pid,
            pidPath,
          }),
        );
        process.exit(0);
      });
    };

    process.once("SIGINT", () => shutdown("SIGINT"));
    process.once("SIGTERM", () => shutdown("SIGTERM"));
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
              pidPath,
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
          pidPath,
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
          configPath: collectorConfigPath(),
          bufferPath,
          bufferFileBytes: fs.existsSync(bufferPath) ? fs.statSync(bufferPath).size : 0,
          pidPath: collectorLogPath("collector.pid"),
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
        },
        null,
        2,
      ),
    );
    buffer.close();
    return;
  }

  if (command === "join") {
    // Fleet join (issue 0016): one command from installed to syncing. The
    // config is only written when the server accepts the token; refusals
    // leave it untouched and say exactly why.
    const target = process.argv[3];
    if (!target || target.startsWith("--")) {
      throw new Error(
        'Usage: plimsoll join "<join-url>#<token>"  |  plimsoll join <token> --url <cloud-base-url>',
      );
    }
    const result = await performJoin({
      target,
      baseUrl: optionValue("--url") ?? process.env.PLIMSOLL_CLOUD_URL,
    });
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
          // The full key lives only in collector.config.json (mode 0600).
          installKey: `${result.installKey.slice(0, 8)}…`,
          uploadUrl: result.uploadUrl,
          uploadSigningConfigured: result.uploadSigningConfigured,
          syncConfigured: true,
          privacyMode: "metadata_only",
          handshake: result.handshake,
          nextSteps: [
            "plimsoll status   # syncConfigured: true, with the handshake already drained",
            "restart a running collector (or: plimsoll install-launch-agent && plimsoll load-launch-agent) so the daemon picks up sync",
          ],
        },
        null,
        2,
      ),
    );
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
    if (!planClaude.changed && !planCodex.changed) {
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
    const result = await new RolloutTailer(buffer).scan({ recentOnly: false });
    console.log(JSON.stringify(result, null, 2));
    buffer.close();
    return;
  }

  if (command === "scan-transcripts") {
    const buffer = openBuffer(config);
    const result = await new TranscriptTailer(buffer).scan({ recentOnly: false });
    console.log(JSON.stringify(result, null, 2));
    buffer.close();
    return;
  }

  if (command === "doctor") {
    const buffer = openBuffer(config);
    const plistPath = launchAgentPlistPath();
    const connectivity = await checkCollectorConnectivity(config.port);
    console.log(
      JSON.stringify(
        {
          ok: true,
          configPath,
          bufferPath: collectorBufferPath(),
          pidPath: collectorLogPath("collector.pid"),
          port: config.port,
          config: {
            existedBeforeLoad: configExistedBeforeLoad,
            createdDuringCommand: !configExistedBeforeLoad && fs.existsSync(configPath),
          },
          launchAgent: {
            label: LAUNCH_AGENT_LABEL,
            plistPath,
            installed: fs.existsSync(plistPath),
          },
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
          sqlite: (() => {
            const projected = buffer.projection.readSnapshot(30, config.subscriptions);
            return projected.kind === "ready" ? projected.snapshot.status.stats : null;
          })(),
          delivery: buffer.delivery.status(),
          projection: buffer.projection.status(),
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
    buffer.close();
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
    const plistPath = installLaunchAgent({
      repoRoot,
      pnpmPath: optionValue("--pnpm") ?? "pnpm",
      programArguments: development
        ? undefined
        : [process.execPath, stableCliPath ?? runningScript, "start"],
      workingDirectory: development ? repoRoot : path.dirname(stableCliPath ?? runningScript),
    });
    console.log(
      JSON.stringify(
        {
          installed: true,
          runtime: development ? "development" : "packaged",
          plistPath,
          label: LAUNCH_AGENT_LABEL,
          loadCommand: launchctlBootstrapCommand(plistPath).join(" "),
        },
        null,
        2,
      ),
    );
    if (flag("--load")) {
      runLaunchctl(launchctlBootstrapCommand(plistPath));
    }
    return;
  }

  if (command === "load-launch-agent") {
    const plistPath = launchAgentPlistPath();
    if (!fs.existsSync(plistPath)) {
      console.log(
        JSON.stringify(
          {
            loaded: false,
            reason: "plist_missing",
            plistPath,
            label: LAUNCH_AGENT_LABEL,
          },
          null,
          2,
        ),
      );
      return;
    }

    console.log(
      JSON.stringify(
        {
          loading: true,
          plistPath,
          label: LAUNCH_AGENT_LABEL,
          loadCommand: launchctlBootstrapCommand(plistPath).join(" "),
        },
        null,
        2,
      ),
    );
    runLaunchctl(launchctlBootstrapCommand(plistPath));
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
    runLaunchctl(launchctlBootoutCommand());
    return;
  }

  if (command === "uninstall-launch-agent") {
    if (flag("--unload")) {
      runLaunchctl(launchctlBootoutCommand());
    }
    const removed = uninstallLaunchAgent({});
    console.log(
      JSON.stringify(
        {
          removed,
          label: LAUNCH_AGENT_LABEL,
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
            pidPath,
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
            pidPath,
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
            pidPath,
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
      console.log(
        JSON.stringify(
          {
            stopped: true,
            pid: runtimeIdentity.pid,
            pidPath,
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
            pidPath,
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
