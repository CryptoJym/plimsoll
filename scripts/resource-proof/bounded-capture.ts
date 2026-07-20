import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import {
  beginAutomaticCaptureBaseline,
  captureBaselineStatus,
  completeAutomaticCaptureBaseline,
} from "../../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../../packages/collector-cli/src/capture-work-budget";
import { collectorConfigSchema } from "../../packages/collector-cli/src/config";
import { historyCoverageStatus, recordExplicitFullHistoryCoverage } from "../../packages/collector-cli/src/history-coverage";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { IncrementalJsonlDiscovery } from "../../packages/collector-cli/src/incremental-jsonl-discovery";
import { CollectorMaintenance } from "../../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import { createCollectorServer } from "../../packages/collector-cli/src/server";
import { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";
import { emptyWorkCounters, type ScenarioReceipt } from "./types";
import type { ResourceSandbox } from "./scenarios";

const PREINSTALL_BYTES = 500 * 1024 * 1024;
const DENSE_TOKEN_RECORDS = 6_336;
const DENSE_EVENT_STRIDE = 64;
const DENSE_EXPECTED_EVENTS = Math.ceil(DENSE_TOKEN_RECORDS / DENSE_EVENT_STRIDE);
const RAW_SENTINEL = "PLIMSOLL_RAW_DENSE_SENTINEL_127";

function uuid(suffix: number) {
  return `019e7000-0000-7000-8000-${String(suffix).padStart(12, "0")}`;
}

function rolloutDay(root: string, now = new Date()) {
  return path.join(root, ...now.toISOString().slice(0, 10).split("-"));
}

function tokenLine(sessionId: string, index: number, padding = "") {
  const total = Math.ceil(index / DENSE_EVENT_STRIDE);
  return JSON.stringify({
    timestamp: new Date(1_750_000_000_000 + index).toISOString(),
    type: "event_msg",
    payload: {
      type: "token_count",
      info: {
        total_token_usage: {
          input_tokens: total,
          cached_input_tokens: Math.floor(total / 4),
          output_tokens: total,
          reasoning_output_tokens: 0,
          total_tokens: total * 2,
        },
      },
      padding,
      sessionHint: sessionId,
    },
  });
}

function writeDenseRollout(file: string, sessionId: string, records = DENSE_TOKEN_RECORDS) {
  const padding = RAW_SENTINEL + "x".repeat(1_900);
  const lines = [
    JSON.stringify({
      timestamp: new Date(1_750_000_000_000).toISOString(),
      type: "session_meta",
      payload: { id: sessionId, originator: "bounded-proof" },
    }),
    JSON.stringify({
      timestamp: new Date(1_750_000_000_001).toISOString(),
      type: "turn_context",
      payload: { model: "gpt-5.5" },
    }),
  ];
  for (let index = 1; index <= records; index += 1) {
    lines.push(tokenLine(sessionId, index, padding));
  }
  fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
  return fs.statSync(file).size;
}

async function collectDiscovery(root: string) {
  const discovery = new IncrementalJsonlDiscovery([root], {
    recursive: true,
    matches: (name) => name.endsWith(".jsonl"),
    maxEntries: 100,
  });
  const files: string[] = [];
  let errors = 0;
  let entriesVisited = 0;
  let done = false;
  try {
    for (let cadence = 0; cadence < 4 && !done; cadence += 1) {
      const chunk = await discovery.collect(new CaptureWorkBudget(), { maxFiles: 100 });
      files.push(...chunk.files.map((entry) => entry.file));
      errors += chunk.errors;
      entriesVisited += chunk.entriesVisited;
      done = chunk.done;
    }
  } finally {
    discovery.close();
  }
  return { files, errors, entriesVisited, done };
}

async function proveDiscoveryEntryPolicy(root: string) {
  const policyRoot = path.join(root, "discovery-entry-policy");
  const externalDirectory = path.join(policyRoot, "external-transcripts");
  const externalFile = path.join(policyRoot, "external.jsonl");
  const ignoredAliasRoot = path.join(policyRoot, "ignored-alias");
  const matchingAliasRoot = path.join(policyRoot, "matching-alias");
  const ignoredNonregularRoot = path.join(policyRoot, "ignored-nonregular");
  const matchingNonregularRoot = path.join(policyRoot, "matching-nonregular");
  const realDirectoryRoot = path.join(policyRoot, "real-directory");
  for (const directory of [
    externalDirectory,
    ignoredAliasRoot,
    matchingAliasRoot,
    ignoredNonregularRoot,
    matchingNonregularRoot,
    path.join(realDirectoryRoot, "nested"),
  ]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(externalDirectory, "hidden.jsonl"), "{}\n", { mode: 0o600 });
  fs.writeFileSync(externalFile, "{}\n", { mode: 0o600 });
  fs.writeFileSync(path.join(realDirectoryRoot, "nested", "visible.jsonl"), "{}\n", { mode: 0o600 });
  fs.symlinkSync(externalDirectory, path.join(ignoredAliasRoot, "external-alias"), "dir");
  fs.symlinkSync(externalFile, path.join(matchingAliasRoot, "trap.jsonl"), "file");

  const ignoredFifo = path.join(ignoredNonregularRoot, "collector.pipe");
  const matchingFifo = path.join(matchingNonregularRoot, "trap.jsonl");
  for (const fifo of [ignoredFifo, matchingFifo]) {
    const created = spawnSync("mkfifo", [fifo], {
      env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C" },
      stdio: "ignore",
    });
    if (created.status !== 0) throw new Error("bounded_discovery_fifo_fixture_unavailable");
  }

  const ignoredAlias = await collectDiscovery(ignoredAliasRoot);
  const matchingAlias = await collectDiscovery(matchingAliasRoot);
  const ignoredNonregular = await collectDiscovery(ignoredNonregularRoot);
  const matchingNonregular = await collectDiscovery(matchingNonregularRoot);
  const realDirectory = await collectDiscovery(realDirectoryRoot);
  return {
    irrelevantExternalDirectorySymlinkIgnored:
      ignoredAlias.done && ignoredAlias.errors === 0 && ignoredAlias.files.length === 0,
    matchingSymlinkFailsClosed:
      matchingAlias.done && matchingAlias.errors === 1 && matchingAlias.files.length === 0,
    nonmatchingNonregularIgnored:
      ignoredNonregular.done && ignoredNonregular.errors === 0 && ignoredNonregular.files.length === 0,
    matchingNonregularFailsClosed:
      matchingNonregular.done && matchingNonregular.errors === 1 && matchingNonregular.files.length === 0,
    realDirectoriesStillRecurse:
      realDirectory.done && realDirectory.errors === 0 && realDirectory.files.length === 1,
    ignoredAliasEntriesVisited: ignoredAlias.entriesVisited,
  };
}

function percentile(values: number[], fraction: number) {
  if (values.length === 0) return Number.POSITIVE_INFINITY;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)]!;
}

async function listen(server: ReturnType<typeof createCollectorServer>) {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("bounded_status_listener_missing");
  return address.port;
}

async function closeServer(server: ReturnType<typeof createCollectorServer>) {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

async function freePort() {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("bounded_child_port_missing");
  const port = address.port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

function establishEmptyBaseline(buffer: LocalEventBuffer, at: string) {
  for (const source of ["codex", "claude_code"] as const) {
    const began = beginAutomaticCaptureBaseline(buffer.database, source, {
      startedAt: at,
      filesDiscovered: 0,
    });
    if (!began.latestRun) throw new Error("bounded_child_baseline_begin_missing");
    completeAutomaticCaptureBaseline(buffer.database, source, {
      runId: began.latestRun.runId,
      completedAt: at,
      observations: [],
    });
  }
}

async function proveSignalCleanup(root: string, repoRoot: string) {
  const home = path.join(root, "child-home");
  const collectorHome = path.join(root, "child-collector");
  const sessions = rolloutDay(path.join(home, ".codex", "sessions"));
  fs.mkdirSync(sessions, { recursive: true, mode: 0o700 });
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const ledger = path.join(collectorHome, "work-ledger.sqlite");
  const seed = new LocalEventBuffer(ledger);
  establishEmptyBaseline(seed, new Date().toISOString());
  seed.close();

  const dense = path.join(sessions, `rollout-child-${uuid(90)}.jsonl`);
  writeDenseRollout(dense, uuid(90));
  const port = await freePort();
  const config = collectorConfigSchema.parse({ port });
  fs.writeFileSync(
    path.join(collectorHome, "collector.config.json"),
    `${JSON.stringify(config, null, 2)}\n`,
    { mode: 0o600 },
  );

  // Keep the process under test as the collector itself. Invoking the tsx CLI
  // creates a signal-relaying wrapper whose PID and shutdown semantics differ
  // from the collector child, making the ownership assertion nondeterministic.
  const child = spawn(
    process.execPath,
    ["--import", "tsx", "packages/collector-cli/src/cli.ts", "start"],
    {
      cwd: repoRoot,
      env: {
        PATH: process.env.PATH ?? "",
        HOME: home,
        PLIMSOLL_HOME: collectorHome,
        LANG: "C",
        LC_ALL: "C",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += String(chunk)));
  child.stderr.on("data", (chunk) => (stderr += String(chunk)));
  const pidFile = path.join(collectorHome, "collector.pid");
  const started = performance.now();
  while ((!fs.existsSync(pidFile) || !stdout.includes('"status":"active"')) && performance.now() - started < 8_000) {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  if (!fs.existsSync(pidFile)) throw new Error("bounded_child_pid_missing");
  const activePidFilePid = Number(
    (JSON.parse(fs.readFileSync(pidFile, "utf8")) as { pid?: unknown }).pid,
  );
  const pidMatchesCollectorProcess =
    Number.isSafeInteger(activePidFilePid) && activePidFilePid === child.pid;
  const heldHeader = net.createConnection({ host: "127.0.0.1", port });
  await new Promise<void>((resolve, reject) => {
    heldHeader.once("connect", resolve);
    heldHeader.once("error", reject);
  });
  heldHeader.write("GET /status HTTP/1.1\r\nHost: 127.0.0.1");

  let observedInFileWork = false;
  const pollDeadline = performance.now() + 9_000;
  while (performance.now() < pollDeadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/status`);
      const status = (await response.json()) as {
        maintenance?: {
          boundary?: {
            inFlight?: boolean;
            stage?: string;
            source?: string;
            generation?: number;
          };
        };
      };
      const capture = status.maintenance?.boundary;
      if (
        capture?.inFlight &&
        capture.stage === "automatic_capture" &&
        capture.source === "codex" &&
        Number(capture.generation ?? 0) > 0
      ) {
        observedInFileWork = true;
        child.kill("SIGTERM");
        break;
      }
    } catch {
      // The listener may not have bound on the first poll.
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  if (!observedInFileWork) child.kill("SIGTERM");
  const exit = await Promise.race([
    new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
      child.once("exit", (code, signal) => resolve({ code, signal })),
    ),
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("bounded_child_shutdown_timeout")), 4_000),
    ),
  ]);
  heldHeader.destroy();
  let reachable = true;
  try {
    await fetch(`http://127.0.0.1:${port}/status`);
  } catch {
    reachable = false;
  }
  return {
    observedInFileWork,
    exitedCleanly: exit.code === 0 && exit.signal === null,
    pidCleaned: !fs.existsSync(pidFile),
    listenerClosed: !reachable,
    stderrEmpty: stderr.trim().length === 0,
    stdoutPrivate: !stdout.includes(root) && !stdout.includes(home) && !stdout.includes(collectorHome),
    pidMatchesCollectorProcess,
    nodeMajor: Number(process.versions.node.split(".")[0]),
    activePidFileOwned: stdout.includes('"pidFileOwned":true'),
    shutdownReportedReady: stdout.includes('"status":"shutdown_ready"'),
    shutdownReportedIncomplete: stdout.includes('"status":"shutdown_incomplete"'),
  };
}

export async function runBoundedCaptureContract(
  sandbox: ResourceSandbox,
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const counters = emptyWorkCounters();
  const root = path.join(sandbox.root, "bounded-capture-127");
  const codexRoot = path.join(root, "codex");
  const claudeRoot = path.join(root, "claude");
  const day = rolloutDay(codexRoot);
  fs.mkdirSync(day, { recursive: true, mode: 0o700 });
  fs.mkdirSync(claudeRoot, { recursive: true, mode: 0o700 });
  const preinstall = path.join(day, `rollout-preinstall-${uuid(1)}.jsonl`);
  fs.writeFileSync(preinstall, "", { mode: 0o600 });
  fs.truncateSync(preinstall, PREINSTALL_BYTES);
  const recoveryPreinstall = path.join(day, `rollout-recovery-${uuid(8)}.jsonl`);
  fs.writeFileSync(recoveryPreinstall, "{}\n", { mode: 0o600 });
  const externalClaudeDirectory = path.join(root, "external-claude-directory");
  fs.mkdirSync(externalClaudeDirectory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(externalClaudeDirectory, "hidden.jsonl"), "{}\n", { mode: 0o600 });
  fs.symlinkSync(externalClaudeDirectory, path.join(claudeRoot, "external-alias"), "dir");
  const claudePreinstall = path.join(claudeRoot, "preinstall.jsonl");
  fs.writeFileSync(claudePreinstall, "{}\n", { mode: 0o600 });
  await new Promise<void>((resolve) => setTimeout(resolve, 2));

  let preinstallBodyReads = 0;
  let recoveryBodyReads = 0;
  let claudePreinstallBodyReads = 0;
  let totalBodyOpens = 0;
  let totalBodyBytesRead = 0;
  const io = {
    ...DEFAULT_JSONL_TAILER_IO,
    readTail: (...args: Parameters<typeof DEFAULT_JSONL_TAILER_IO.readTail>) => {
      const read = DEFAULT_JSONL_TAILER_IO.readTail(...args);
      if (read) {
        if (path.resolve(args[0]) === path.resolve(preinstall)) preinstallBodyReads += 1;
        if (path.resolve(args[0]) === path.resolve(recoveryPreinstall)) recoveryBodyReads += 1;
        totalBodyOpens += 1;
        totalBodyBytesRead += read.bytesRead;
      }
      return read;
    },
  };
  const transcriptIo = {
    ...DEFAULT_JSONL_TAILER_IO,
    readTail: (...args: Parameters<typeof DEFAULT_JSONL_TAILER_IO.readTail>) => {
      if (path.resolve(args[0]) === path.resolve(claudePreinstall)) {
        claudePreinstallBodyReads += 1;
      }
      return DEFAULT_JSONL_TAILER_IO.readTail(...args);
    },
  };
  const discoveryEntryPolicy = await proveDiscoveryEntryPolicy(root);
  let buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  let maintenance = new CollectorMaintenance(
    buffer,
    new RolloutTailer(buffer, codexRoot, () => [], io),
    new TranscriptTailer(buffer, claudeRoot, transcriptIo),
  );
  const baselineRun = await maintenance.runRecent();
  for (let cadence = 0; cadence < 12 && captureBaselineStatus(buffer.database).status !== "complete"; cadence += 1) {
    await maintenance.runRecent();
  }
  const excludedRun = await maintenance.runRecent();
  const baseline = captureBaselineStatus(buffer.database);
  const claudeBaseline = baseline.sources.find((source) => source.source === "claude_code");
  const irrelevantSymlinkBaselineSafe =
    baseline.status === "complete" &&
    claudeBaseline?.status === "complete" &&
    claudeBaseline.latestRun?.discoveryErrors === 0 &&
    claudeBaseline.excludedGenerations === 1 &&
    claudePreinstallBodyReads === 0;
  const baselineNoBody =
    baseline.status === "complete" &&
    baselineRun.rawEventWrites === 0 &&
    baselineRun.rollout.bytesRead === 0 &&
    preinstallBodyReads === 0 &&
    excludedRun.rollout.excludedGenerations === 2 &&
    irrelevantSymlinkBaselineSafe;

  fs.appendFileSync(preinstall, `${tokenLine(uuid(1), 1)}\n`);
  const growthRun = await maintenance.runRecent();
  const preinstallGrowthExcluded =
    growthRun.rollout.filesRead === 0 &&
    growthRun.rollout.eventsAppended === 0 &&
    preinstallBodyReads === 0;
  const automaticPreinstallBodyReads = preinstallBodyReads;

  fs.truncateSync(recoveryPreinstall, 0);
  const ambiguityRun = await maintenance.runRecent();
  const ambiguityStatus = captureBaselineStatus(buffer.database);
  const truncationBlockedWithoutRead =
    ambiguityStatus.progress.state === "ambiguous" &&
    ambiguityRun.rawEventWrites === 0 &&
    recoveryBodyReads === 0;
  fs.unlinkSync(recoveryPreinstall);
  fs.writeFileSync(
    recoveryPreinstall,
    `${JSON.stringify({ type: "session_meta", payload: { id: uuid(8) } })}\n${tokenLine(uuid(8), 1)}\n`,
    { mode: 0o600 },
  );
  const recoveryRuns: Array<Awaited<ReturnType<CollectorMaintenance["runRecent"]>>> = [];
  for (let run = 0; run < 8; run += 1) {
    recoveryRuns.push(await maintenance.runRecent());
    const count = (buffer.database
      .prepare(`select count(*) as count from buffered_events where session_id = ?`)
      .get(uuid(8)) as { count: number }).count;
    if (count === 1 && captureBaselineStatus(buffer.database).status === "complete") break;
  }
  await maintenance.runRecent();
  const replacementRecoveredExactlyOnce =
    captureBaselineStatus(buffer.database).status === "complete" &&
    (buffer.database.prepare(`select count(*) as count from buffered_events where session_id = ?`)
      .get(uuid(8)) as { count: number }).count === 1 &&
    recoveryBodyReads === 1 &&
    recoveryRuns.some((run) => run.rollout.eventsAppended === 1);

  const denseSession = uuid(2);
  const dense = path.join(day, `rollout-dense-${denseSession}.jsonl`);
  const denseBytes = writeDenseRollout(dense, denseSession);
  const config = collectorConfigSchema.parse({});
  let server = createCollectorServer(config, buffer, {
    maintenanceStatus: () => ({ capture: maintenance.status() }),
  });
  let port = await listen(server);
  const latencies: number[] = [];
  let peakRss = process.memoryUsage().rss;
  const rssBefore = peakRss;
  let firstDenseRun: Awaited<ReturnType<CollectorMaintenance["runRecent"]>> | null = null;
  let restartPerformed = false;

  for (let cadence = 0; cadence < 180; cadence += 1) {
    const probe = new Promise<number>((resolve) => {
      setTimeout(async () => {
        const probeStarted = performance.now();
        try {
          const response = await fetch(`http://127.0.0.1:${port}/status`);
          if (!response.ok) throw new Error("bounded_status_not_ok");
          await response.arrayBuffer();
          resolve(performance.now() - probeStarted);
        } catch {
          resolve(5_000);
        }
      }, 0);
    });
    const run = maintenance.runRecent();
    const [receipt, latency] = await Promise.all([run, probe]);
    firstDenseRun ??= receipt;
    latencies.push(latency);
    peakRss = Math.max(peakRss, process.memoryUsage().rss);
    const count = (buffer.database
      .prepare(`select count(*) as count from buffered_events where session_id = ?`)
      .get(denseSession) as { count: number }).count;
    if (!restartPerformed && count > 0) {
      await closeServer(server);
      maintenance.close();
      buffer.close();
      buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
      maintenance = new CollectorMaintenance(
        buffer,
        new RolloutTailer(buffer, codexRoot, () => [], io),
        new TranscriptTailer(buffer, claudeRoot, transcriptIo),
      );
      server = createCollectorServer(config, buffer, {
        maintenanceStatus: () => ({ capture: maintenance.status() }),
      });
      port = await listen(server);
      restartPerformed = true;
    }
    const resumedCount = (buffer.database
      .prepare(`select count(*) as count from buffered_events where session_id = ?`)
      .get(denseSession) as { count: number }).count;
    const denseCursor = buffer.database
      .prepare(`select committed_offset as committedOffset from rollout_scan_state where file = ?`)
      .get(jsonlScanStateKey(dense)) as { committedOffset: number } | undefined;
    if (resumedCount === DENSE_EXPECTED_EVENTS && denseCursor?.committedOffset === denseBytes) break;
  }

  const denseTotals = buffer.database
    .prepare(
      `select count(*) as events,
         sum(input_tokens) as inputTokens,
         sum(output_tokens) as outputTokens,
         max(input_tokens) as maxInputDelta,
         count(distinct id) as uniqueIds
       from buffered_events where session_id = ?`,
    )
    .get(denseSession) as {
      events: number;
      inputTokens: number;
      outputTokens: number;
      maxInputDelta: number;
      uniqueIds: number;
    };
  const denseExact =
    denseTotals.events === DENSE_EXPECTED_EVENTS &&
    denseTotals.uniqueIds === DENSE_EXPECTED_EVENTS &&
    denseTotals.inputTokens === DENSE_EXPECTED_EVENTS &&
    denseTotals.outputTokens === DENSE_EXPECTED_EVENTS &&
    denseTotals.maxInputDelta === 1;
  const firstCadenceBounded = Boolean(
    firstDenseRun &&
      firstDenseRun.rollout.eventsAppended > 0 &&
      firstDenseRun.rollout.recordsParsed < DENSE_TOKEN_RECORDS &&
      firstDenseRun.rollout.automaticBudget &&
      firstDenseRun.rollout.automaticBudget.bytesRead <= 512 * 1024 &&
      firstDenseRun.rollout.automaticBudget.recordsParsed <= 512 &&
      firstDenseRun.rollout.cooperativeYields > 0,
  );
  const warmP95Ms = percentile(latencies, 0.95);
  const rssGrowthBytes = peakRss - rssBefore;
  const responsiveAndBounded = warmP95Ms <= 500 && rssGrowthBytes < 768 * 1024 * 1024;

  // Malformed, partial, oversized and CRLF boundary generations remain
  // metadata-only failures while a valid boundary record captures once.
  const malformed = path.join(day, `rollout-malformed-${uuid(3)}.jsonl`);
  const partial = path.join(day, `rollout-partial-${uuid(4)}.jsonl`);
  const oversized = path.join(day, `rollout-oversized-${uuid(5)}.jsonl`);
  const boundary = path.join(day, `rollout-boundary-${uuid(6)}.jsonl`);
  fs.writeFileSync(malformed, '{"type":"event_msg","payload":{"type":"token_count"\n', { mode: 0o600 });
  fs.writeFileSync(partial, tokenLine(uuid(4), 1).slice(0, 70), { mode: 0o600 });
  fs.writeFileSync(oversized, `${tokenLine(uuid(5), 1, RAW_SENTINEL + "y".repeat(200_000))}\n`, { mode: 0o600 });
  fs.writeFileSync(
    boundary,
    `${JSON.stringify({ type: "session_meta", payload: { id: uuid(6) } })}\r\n${tokenLine(uuid(6), 1)}\r\n`,
    { mode: 0o600 },
  );
  const adversarialReceipts = [];
  for (let run = 0; run < 4; run += 1) adversarialReceipts.push(await maintenance.runRecent());
  const adversarialHandled =
    adversarialReceipts.some((receipt) => receipt.rollout.unresolvedRecords > 0) &&
    adversarialReceipts.some((receipt) => receipt.rollout.parseErrors > 0) &&
    (buffer.database.prepare(`select count(*) as count from buffered_events where session_id = ?`)
      .get(uuid(6)) as { count: number }).count === 1;

  fs.unlinkSync(boundary);
  fs.writeFileSync(
    boundary,
    `${JSON.stringify({ type: "session_meta", payload: { id: uuid(7) } })}\n${tokenLine(uuid(7), 1)}\n`,
    { mode: 0o600 },
  );
  for (let run = 0; run < 4; run += 1) await maintenance.runRecent();
  const rotationExactlyOnce =
    (buffer.database.prepare(`select count(*) as count from buffered_events where session_id in (?, ?)`)
      .get(uuid(6), uuid(7)) as { count: number }).count === 2;

  const full = await new RolloutTailer(buffer, codexRoot, () => [], io).scan({ scope: "full" });
  const fullCoverage = recordExplicitFullHistoryCoverage(buffer.database, "codex", full);
  const exclusionAfterFull = captureBaselineStatus(buffer.database).sources.find(
    (source) => source.source === "codex",
  );
  const historyTruth =
    !fullCoverage.promoted &&
    historyCoverageStatus(buffer.database).status === "incomplete" &&
    exclusionAfterFull?.excludedGenerations === 2;

  const statusResponse = await fetch(`http://127.0.0.1:${port}/status`);
  const statusText = await statusResponse.text();
  const cursorRows = buffer.database
    .prepare(`select file from rollout_scan_state`)
    .all() as Array<{ file: string }>;
  const persistedText = [
    ...buffer.database.prepare(`select payload_json as value from buffered_events`).all() as Array<{ value: string }>,
    ...buffer.database.prepare(`select value from maintenance_state`).all() as Array<{ value: string }>,
  ].map((row) => row.value).join("\n");
  const privacyClean =
    !persistedText.includes(RAW_SENTINEL) &&
    !persistedText.includes(root) &&
    !statusText.includes(RAW_SENTINEL) &&
    !statusText.includes(root) &&
    cursorRows.every((row) => /^[0-9a-f]{64}$/.test(row.file));
  await closeServer(server);
  maintenance.close();
  buffer.close();

  const signalCleanup = await proveSignalCleanup(root, path.resolve("."));
  const signalSafe =
    signalCleanup.observedInFileWork &&
    signalCleanup.exitedCleanly &&
    signalCleanup.pidCleaned &&
    signalCleanup.listenerClosed &&
    signalCleanup.stderrEmpty &&
    signalCleanup.stdoutPrivate &&
    signalCleanup.pidMatchesCollectorProcess &&
    signalCleanup.nodeMajor === 22;
  const discoveryEntryPolicySafe =
    discoveryEntryPolicy.irrelevantExternalDirectorySymlinkIgnored &&
    discoveryEntryPolicy.matchingSymlinkFailsClosed &&
    discoveryEntryPolicy.nonmatchingNonregularIgnored &&
    discoveryEntryPolicy.matchingNonregularFailsClosed &&
    discoveryEntryPolicy.realDirectoriesStillRecurse;

  counters.filesOpened = totalBodyOpens;
  counters.fileBytesRead = totalBodyBytesRead;
  counters.rawEventWrites = denseTotals.events + (rotationExactlyOnce ? 2 : 0) +
    (replacementRecoveredExactlyOnce ? 1 : 0);
  counters.maintenanceRuns = latencies.length + adversarialReceipts.length;
  counters.listenersCreated = 3;
  const passed =
    PREINSTALL_BYTES >= 500 * 1024 * 1024 &&
    denseBytes >= 13 * 1024 * 1024 &&
    baselineNoBody &&
    preinstallGrowthExcluded &&
    truncationBlockedWithoutRead &&
    replacementRecoveredExactlyOnce &&
    firstCadenceBounded &&
    denseExact &&
    restartPerformed &&
    responsiveAndBounded &&
    adversarialHandled &&
    rotationExactlyOnce &&
    historyTruth &&
    privacyClean &&
    signalSafe &&
    discoveryEntryPolicySafe;
  return {
    id: "bounded_generation_capture",
    required: true,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "A 500 MiB pre-install generation and an irrelevant external-directory alias were metadata-only handled without body reads; candidate aliases/nonregular entries failed closed, while a dense 13+ MiB new generation resumed across bounded cadences/restart with responsive HTTP, exact tokens, private state, and graceful in-work SIGTERM cleanup."
      : "Generation exclusion, discovery entry policy, bounded cadence, exact resume, HTTP latency, privacy, malformed-input, history, or shutdown assertions failed.",
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters,
    measurements: {
      preinstallBytes: PREINSTALL_BYTES,
      automaticPreinstallBodyReads,
      explicitFullPreinstallBodyReads: preinstallBodyReads - automaticPreinstallBodyReads,
      denseBytes,
      denseTokenRecords: DENSE_TOKEN_RECORDS,
      baselineNoBody,
      irrelevantSymlinkBaselineSafe,
      irrelevantExternalDirectorySymlinkIgnored:
        discoveryEntryPolicy.irrelevantExternalDirectorySymlinkIgnored,
      matchingSymlinkFailsClosed: discoveryEntryPolicy.matchingSymlinkFailsClosed,
      nonmatchingNonregularIgnored: discoveryEntryPolicy.nonmatchingNonregularIgnored,
      matchingNonregularFailsClosed: discoveryEntryPolicy.matchingNonregularFailsClosed,
      realDirectoriesStillRecurse: discoveryEntryPolicy.realDirectoriesStillRecurse,
      ignoredAliasEntriesVisited: discoveryEntryPolicy.ignoredAliasEntriesVisited,
      claudePreinstallBodyReads,
      preinstallGrowthExcluded,
      truncationBlockedWithoutRead,
      replacementRecoveredExactlyOnce,
      recoveryBodyReads,
      firstCadenceBounded,
      denseExact,
      crashResumeReopenedLedger: restartPerformed,
      statusProbes: latencies.length,
      warmStatusP95Ms: Number(warmP95Ms.toFixed(3)),
      rssGrowthBytes,
      adversarialHandled,
      rotationExactlyOnce,
      historyTruth,
      privacyClean,
      signalObservedDuringDenseSlice: signalCleanup.observedInFileWork,
      signalExitClean: signalCleanup.exitedCleanly,
      signalPidCleaned: signalCleanup.pidCleaned,
      signalListenerClosed: signalCleanup.listenerClosed,
      signalStderrEmpty: signalCleanup.stderrEmpty,
      signalStdoutPrivate: signalCleanup.stdoutPrivate,
      signalPidMatchesCollectorProcess: signalCleanup.pidMatchesCollectorProcess,
      signalNodeMajor: signalCleanup.nodeMajor,
      signalActivePidFileOwned: signalCleanup.activePidFileOwned,
      signalShutdownReportedReady: signalCleanup.shutdownReportedReady,
      signalShutdownReportedIncomplete: signalCleanup.shutdownReportedIncomplete,
    },
  };
}
