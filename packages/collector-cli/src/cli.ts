#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "./buffer";
import {
  collectorHome,
  collectorBufferPath,
  collectorConfigPath,
  collectorLogPath,
  ensureCollectorHome,
  loadCollectorConfig,
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
import { createCollectorServer } from "./server";
import {
  generateClaudeCodeSettings,
  generateCodexConfigToml,
  generateSetupInstructions,
} from "../../collector-config/src/index";
import type { ToolSource } from "../../shared/src/index";
import { uploadBufferedEvents } from "./upload";

const command = process.argv[2] ?? "help";

type CollectorPidFile = {
  command: string[];
  cwd: string;
  label: typeof LAUNCH_AGENT_LABEL;
  pid: number;
  startedAt: string;
  version: 1;
};

function printHelp() {
  console.log(`Plimsoll Collector

Commands:
  start                 Start the local hook/OTLP receiver in the foreground
  status                Print local buffer and policy status
  doctor                Verify paths, SQLite buffer, LaunchAgent, data mode, and privacy posture
  export                Print buffered events as JSON
  forward-hook SOURCE   Read hook JSON from stdin and append it without requiring the receiver
  self-test-hook SOURCE Emit one synthetic hook event into the local buffer
  generate-config TOOL  Print Claude Code or Codex config for metadata collection
  upload                Drain un-uploaded events to the tenant ingest API (marks rows, keeps local copies)
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
  generate-config claude-code|codex|all [--evidence --confirm-evidence]
  upload [--url URL --limit 500] [--ingest-key KEY] [--signing-secret SECRET] [--no-mark] [--max-batches 20]
  install-launch-agent [--repo-root PATH] [--pnpm PATH] [--load]
  load-launch-agent
  unload-launch-agent
  uninstall-launch-agent [--unload]
  purge-local-data [--confirm] [--include-config]
`);
}

function openBuffer() {
  ensureCollectorHome();
  return new LocalEventBuffer(collectorBufferPath());
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

function writePidFile() {
  const pidPath = collectorLogPath("collector.pid");
  const pidFile: CollectorPidFile = {
    command: process.argv.slice(1),
    cwd: process.cwd(),
    label: LAUNCH_AGENT_LABEL,
    pid: process.pid,
    startedAt: new Date().toISOString(),
    version: 1,
  };
  fs.writeFileSync(pidPath, `${JSON.stringify(pidFile, null, 2)}\n`, { mode: 0o600 });
  return pidPath;
}

function removePidFile(pidPath = collectorLogPath("collector.pid")) {
  if (fs.existsSync(pidPath)) {
    fs.unlinkSync(pidPath);
  }
}

function readPidFile(pidPath: string):
  | { ok: true; legacy: boolean; record: CollectorPidFile }
  | { ok: false; reason: "invalid_pid_file" | "pid_file_missing"; raw?: string } {
  if (!fs.existsSync(pidPath)) {
    return { ok: false, reason: "pid_file_missing" };
  }

  const raw = fs.readFileSync(pidPath, "utf8").trim();
  const legacyPid = Number(raw);
  if (Number.isInteger(legacyPid) && legacyPid > 0) {
    return {
      ok: true,
      legacy: true,
      record: {
        command: [],
        cwd: "",
        label: LAUNCH_AGENT_LABEL,
        pid: legacyPid,
        startedAt: "legacy",
        version: 1,
      },
    };
  }

  try {
    const parsed = JSON.parse(raw) as Partial<CollectorPidFile>;
    if (
      parsed.version === 1 &&
      parsed.label === LAUNCH_AGENT_LABEL &&
      Number.isInteger(parsed.pid) &&
      (parsed.pid ?? 0) > 0 &&
      Array.isArray(parsed.command) &&
      typeof parsed.cwd === "string" &&
      typeof parsed.startedAt === "string"
    ) {
      return { ok: true, legacy: false, record: parsed as CollectorPidFile };
    }
  } catch {
    // Fall through to invalid file handling.
  }

  return { ok: false, reason: "invalid_pid_file", raw };
}

function processCommandForPid(pid: number) {
  const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  if (result.status !== 0) return null;
  return result.stdout.trim();
}

function looksLikeCollectorStartCommand(commandLine: string | null) {
  if (!commandLine) return false;
  const normalized = commandLine.toLowerCase();
  return (
    normalized.includes("collector") &&
    normalized.includes("start") &&
    (normalized.includes("collector-cli") ||
      normalized.includes("packages/collector-cli") ||
      normalized.includes("pnpm") ||
      normalized.includes("tsx"))
  );
}

function pathLooksCurrentRepo(value: string) {
  if (!value) return false;
  return path.resolve(value) === process.cwd();
}

function validatePidFileForStop(pidPath: string) {
  const read = readPidFile(pidPath);
  if (!read.ok) {
    removePidFile(pidPath);
    return {
      ok: false,
      pid: null,
      reason: read.reason,
      removedPidFile: true,
    };
  }

  const { record } = read;
  try {
    process.kill(record.pid, 0);
  } catch {
    removePidFile(pidPath);
    return {
      ok: false,
      pid: record.pid,
      reason: "process_not_running",
      removedPidFile: true,
    };
  }

  const commandLine = processCommandForPid(record.pid);
  const commandLooksSafe = looksLikeCollectorStartCommand(commandLine);
  const cwdLooksSafe = read.legacy || pathLooksCurrentRepo(record.cwd);
  if (!commandLooksSafe || !cwdLooksSafe) {
    removePidFile(pidPath);
    return {
      commandLine,
      cwd: record.cwd,
      legacyPidFile: read.legacy,
      ok: false,
      pid: record.pid,
      reason: "pid_guard_rejected",
      removedPidFile: true,
    };
  }

  return {
    commandLine,
    cwd: record.cwd,
    legacyPidFile: read.legacy,
    ok: true,
    pid: record.pid,
    reason: null,
    removedPidFile: false,
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

  if (command === "start") {
    const buffer = openBuffer();
    const server = createCollectorServer(config, buffer);
    const pidPath = writePidFile();
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

    const runSync = async () => {
      if (!config.uploadUrl || syncInFlight || shuttingDown) return;
      if (Date.now() < syncSkipUntil) return;
      syncInFlight = true;
      try {
        let batches = 0;
        let uploaded = 0;
        while (batches < 5) {
          const result = await uploadBufferedEvents(config, buffer, {});
          if (result.uploadedEvents === 0) break;
          uploaded += result.uploadedEvents;
          batches += 1;
          if (result.remainingUnuploaded === 0) break;
        }
        if (uploaded > 0) {
          console.log(
            JSON.stringify({
              status: "synced",
              uploadedEvents: uploaded,
              remainingUnuploaded: buffer.stats().unuploadedCount,
            }),
          );
        }
        syncFailureStreak = 0;
        syncSkipUntil = 0;
      } catch (error) {
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

    runPrune();
    timers.push(setInterval(runPrune, 6 * 60 * 60 * 1000));
    if (config.uploadUrl) {
      timers.push(setInterval(() => void runSync(), config.syncIntervalSeconds * 1000));
    }
    for (const timer of timers) timer.unref();

    const shutdown = (signal: string) => {
      if (shuttingDown) return;
      shuttingDown = true;
      for (const timer of timers) clearInterval(timer);
      server.close(() => {
        buffer.close();
        removePidFile(pidPath);
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
      buffer.close();
      removePidFile(pidPath);
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
      console.log(
        JSON.stringify({
          status: "active",
          mode: config.policy.dataMode,
          port: config.port,
          pid: process.pid,
          pidPath,
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
    const buffer = openBuffer();
    const bufferPath = collectorBufferPath();
    console.log(
      JSON.stringify(
        {
          configPath: collectorConfigPath(),
          bufferPath,
          bufferFileBytes: fs.existsSync(bufferPath) ? fs.statSync(bufferPath).size : 0,
          pidPath: collectorLogPath("collector.pid"),
          port: config.port,
          dataMode: config.policy.dataMode,
          retentionDays: config.retentionDays,
          syncConfigured: Boolean(config.uploadUrl),
          stats: buffer.stats(),
          tokenCoverageLast7d: buffer.tokenCoverage(7),
        },
        null,
        2,
      ),
    );
    buffer.close();
    return;
  }

  if (command === "doctor") {
    const buffer = openBuffer();
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
          retentionDays: config.retentionDays,
          syncConfigured: Boolean(config.uploadUrl),
          uploadSigningConfigured: Boolean(config.uploadSigningSecret),
          sqlite: buffer.stats(),
          tokenCoverageLast7d: buffer.tokenCoverage(7),
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
    const buffer = openBuffer();
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
    const buffer = openBuffer();
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
      if (result.uploadedEvents === 0 || !markUploaded || result.remainingUnuploaded === 0) {
        break;
      }
    }
    console.log(
      JSON.stringify(
        {
          uploadedEvents,
          batches,
          markedUploaded: markUploaded,
          remainingUnuploaded: lastResult?.remainingUnuploaded ?? buffer.stats().unuploadedCount,
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
    const buffer = openBuffer();
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
    const buffer = openBuffer();
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
    const plistPath = installLaunchAgent({
      repoRoot: optionValue("--repo-root") ?? process.cwd(),
      pnpmPath: optionValue("--pnpm") ?? "pnpm",
    });
    console.log(
      JSON.stringify(
        {
          installed: true,
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
    const buffer = openBuffer();
    buffer.setAccountLabel(hash, name);
    console.log(JSON.stringify({ labeled: true, accountHash: hash, label: name }, null, 2));
    buffer.close();
    return;
  }

  if (command === "priority") {
    const action = process.argv[3];
    const buffer = openBuffer();
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

    const pidGuard = validatePidFileForStop(pidPath);
    if (!pidGuard.ok) {
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: pidGuard.reason,
            pid: pidGuard.pid,
            pidPath,
            launchAgentStopCommand,
            removedPidFile: pidGuard.removedPidFile,
            pidGuard: {
              commandLine: "commandLine" in pidGuard ? pidGuard.commandLine : null,
              cwd: "cwd" in pidGuard ? pidGuard.cwd : null,
              legacyPidFile: "legacyPidFile" in pidGuard ? pidGuard.legacyPidFile : false,
            },
          },
          null,
          2,
        ),
      );
      return;
    }

    const pid = pidGuard.pid;
    if (pid === null) {
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "invalid_pid_file",
            pid,
            pidPath,
            launchAgentStopCommand,
            removedPidFile: true,
          },
          null,
          2,
        ),
      );
      return;
    }

    try {
      process.kill(pid, "SIGTERM");
      console.log(
        JSON.stringify(
          {
            stopped: true,
            pid,
            pidPath,
            launchAgentStopCommand,
            pidGuard: {
              commandLine: pidGuard.commandLine,
              cwd: pidGuard.cwd,
              legacyPidFile: pidGuard.legacyPidFile,
            },
          },
          null,
          2,
        ),
      );
    } catch (error) {
      removePidFile(pidPath);
      console.log(
        JSON.stringify(
          {
            stopped: false,
            reason: "kill_failed",
            message: error instanceof Error ? error.message : String(error),
            pid,
            pidPath,
            launchAgentStopCommand,
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
