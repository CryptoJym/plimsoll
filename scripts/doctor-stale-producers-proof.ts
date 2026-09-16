/**
 * Focused proof for bead eco-6hoxj.153: `doctor` names producer processes
 * older than their managed config, and `status` names the count while
 * `source_required` / `producer_token_required` rejections are open.
 *
 * The process table, the process environments and `launchctl print` are read
 * from a fixture selected by PLIMSOLL_PRODUCER_PROCESS_FIXTURE. The spawned
 * doctor runs with stub `ps` and `launchctl` first on PATH that record any
 * call, so the proof also shows the real process table is never read. Every
 * path is under a temporary directory.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  PRODUCER_PROCESS_FIXTURE_ENV,
  PRODUCER_PROCESS_ROW_LIMIT,
  STALE_PRODUCER_SCAN_MAX_AGE_MS,
  annotateCaptureHealthWithStaleProducers,
  createStaleProducerScanCache,
  resolveProducerProcessProvider,
  scanProducerProcesses,
  type ProducerProcessScan,
} from "../packages/collector-cli/src/producer-processes";
import type { RejectionDiagnosticsCounters } from "../packages/collector-cli/src/rejection-diagnostics";
import { useFixtureRoot } from "./lib/fixture-root";

type Check = { name: string; passed: boolean; detail: unknown };
const checks: Check[] = [];
function check(name: string, condition: unknown, detail: unknown = null) {
  checks.push({ name, passed: Boolean(condition), detail });
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

const root = path.resolve(import.meta.dirname, "..");
const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
const tsx = path.join(root, "node_modules", "tsx", "dist", "cli.mjs");
const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-doctor-stale-producers-proof-"));
const fixture = useFixtureRoot(sandbox);
const home = fixture.home;
const collectorHome = fixture.env.PLIMSOLL_HOME!;
const CANARY = "plimsoll-proof-canary-token-7f3a";
const UID = 501;

const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** `ps -o lstart=` in the C locale: local time, day padded to two columns. */
function lstart(date: Date) {
  const two = (value: number) => String(value).padStart(2, "0");
  return `${DAYS[date.getDay()]} ${MONTHS[date.getMonth()]} ${String(date.getDate()).padStart(2, " ")} ` +
    `${two(date.getHours())}:${two(date.getMinutes())}:${two(date.getSeconds())} ${date.getFullYear()}`;
}
const at = (iso: string) => new Date(iso);
const backupName = (file: string, iso: string) => `${file}.plimsoll-backup-${iso.replace(/[:.]/g, "-")}`;

function write(file: string, body: string, mtimeIso?: string) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, body);
  if (mtimeIso) fs.utimesSync(file, at(mtimeIso), at(mtimeIso));
}

fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
fs.chmodSync(collectorHome, 0o700);
// Managed surfaces and the records the collector keeps for them.
const codexConfig = path.join(home, ".codex", "config.toml");
const pro2Config = path.join(home, ".codex-profiles", "pro2", "config.toml");
const pro3Config = path.join(home, ".codex-profiles", "pro3", "config.toml");
const seatSettings = path.join(home, ".claude-seats", "seat-a", "settings.json");
const claudeSettings = path.join(home, ".claude", "settings.json");
write(codexConfig, "# managed\n", "2026-09-11T22:13:36.000Z");
write(backupName(codexConfig, "2026-09-11T22:13:36.000Z"), "# pre\n");
write(pro2Config, "# managed\n", "2026-09-12T14:00:00.000Z");
write(backupName(pro2Config, "2026-09-12T14:00:00.000Z"), "# pre\n");
write(pro3Config, "# managed\n", "2026-09-12T14:00:00.000Z");
write(backupName(pro3Config, "2026-09-12T14:00:00.000Z"), "# pre\n");
write(seatSettings, "{}\n", "2026-09-13T08:00:00.000Z");
write(claudeSettings, "{}\n", "2026-09-10T08:00:00.000Z");
write(
  path.join(collectorHome, "receipts", "managed-config-reconcile-2026-09-15T10-00-00-000Z.json"),
  `${JSON.stringify({ startedAt: "2026-09-15T10:00:00.000Z", targets: [{ name: "codexProfile[pro3]", status: "applied" }] })}\n`,
);

type FixtureProcess = { pid: number; ppid: number; started: string; command: string; env: string | null };
const envLine = (entries: Record<string, string>) =>
  Object.entries({ PATH: "/usr/bin:/bin", PLIMSOLL_PRODUCER_TOKEN: CANARY, ...entries })
    .map(([key, value]) => `${key}=${value}`)
    .join(" ");
const processes: FixtureProcess[] = [
  { pid: 1, ppid: 0, started: "2026-08-27T09:32:38.000Z", command: "/sbin/launchd", env: null },
  // The MacBook finding: a launchd-owned Codex app-server started 09-11, the
  // profile config managed from 09-12.
  {
    pid: 7539,
    ppid: 1,
    started: "2026-09-11T13:26:18.000Z",
    command: `${home}/.codex-profiles/pro2/packages/standalone/current/codex app-server --remote-control -c otel.headers.x-plimsoll-token=${CANARY}`,
    env: envLine({ HOME: home, CODEX_HOME: `${home}/.codex-profiles/pro2` }),
  },
  // The Studio0 finding: a conductor child on the default home, started before
  // the producer token was provisioned into ~/.codex/config.toml.
  {
    pid: 38948,
    ppid: 1,
    started: "2026-09-07T17:20:00.000Z",
    command: `/usr/local/bin/node ${home}/.local/state/nr-conductors/support-utlyze/conductor.mjs`,
    env: null,
  },
  {
    pid: 38970,
    ppid: 38948,
    started: "2026-09-07T17:20:01.000Z",
    command: `${home}/.local/share/codex-runtime/0.153.3/bin/codex app-server`,
    env: envLine({ HOME: home }),
  },
  // Fresh: started after the reconcile receipt that applied pro3.
  {
    pid: 41000,
    ppid: 1,
    started: "2026-09-15T12:00:00.000Z",
    command: `${home}/.codex-profiles/pro3/packages/standalone/current/codex app-server`,
    env: envLine({ HOME: home, CODEX_HOME: `${home}/.codex-profiles/pro3` }),
  },
  // Fresh: default Claude home, started after its settings were last written.
  { pid: 42000, ppid: 1, started: "2026-09-14T08:00:00.000Z", command: "claude --resume", env: envLine({ HOME: home }) },
  // Stale by file mtime only: a Claude seat started before its settings changed.
  {
    pid: 43000,
    ppid: 1,
    started: "2026-09-12T08:00:00.000Z",
    command: "/usr/local/bin/node /usr/local/lib/node_modules/@anthropic-ai/claude-code/cli.js",
    env: null,
  },
  {
    pid: 43001,
    ppid: 1,
    started: "2026-09-12T08:00:00.000Z",
    command: "/opt/homebrew/bin/claude",
    env: envLine({ HOME: home, CLAUDE_CONFIG_DIR: `${home}/.claude-seats/seat-a` }),
  },
  // Environment not readable: started years before any managed config.
  { pid: 50002, ppid: 1, started: "2020-01-01T00:00:00.000Z", command: "/opt/bin/codex exec --json", env: null },
  // Not a producer.
  { pid: 60000, ppid: 1, started: "2026-09-01T00:00:00.000Z", command: "/usr/bin/codexbar --help", env: null },
];

function renderTable(rows: FixtureProcess[]) {
  return `${rows.map((row) => `${String(row.pid).padStart(5)} ${String(row.ppid).padStart(5)} ${lstart(at(row.started))}     ${row.command}`).join("\n")}\n`;
}
function renderEnvironments(rows: FixtureProcess[]) {
  return `${rows.map((row) => `${String(row.pid).padStart(5)} ${row.command}${row.env ? ` ${row.env}` : ""}`).join("\n")}\n`;
}
const launchctl = [
  `gui/${UID} = {`,
  "\ttype = gui",
  "\tservices = {",
  "\t\t    7539      - \tcom.jamesbrady.codex-profile.pro2",
  "\t\t   98839      - \tapplication.org.mozilla.firefox.1163773.144541237",
  "\t\t       0      0 \tcom.example.not-running",
  "\t}",
  "}",
].join("\n");

function writeFixture(name: string, rows: FixtureProcess[]) {
  const file = path.join(sandbox, `${name}.json`);
  fs.writeFileSync(file, JSON.stringify({
    uid: UID,
    processTable: renderTable(rows),
    environments: renderEnvironments(rows.filter((row) => producerLike(row.command))),
    launchctl,
  }));
  return file;
}
function producerLike(command: string) {
  return /(?:^|\/)(?:codex|claude|gemini|grok)(?:\s|$)|claude-code\/cli\.js/.test(command);
}

function providerFor(file: string) {
  const provider = resolveProducerProcessProvider({ [PRODUCER_PROCESS_FIXTURE_ENV]: file });
  assert.ok(provider);
  return provider;
}

async function main() {
  const mainFixture = writeFixture("processes", processes);
  const scan = await scanProducerProcesses({ collectorHome, home, provider: providerFor(mainFixture) });
  const byPid = new Map(scan.processes.map((entry) => [entry.pid, entry]));

  check("fixture_provider_is_used", scan.provider === "fixture" && scan.inspection === "complete", scan.inspection);
  check(
    "non_producers_are_not_reported",
    !byPid.has(1) && !byPid.has(38948) && !byPid.has(60000) && scan.producerRows === 7,
    [...byPid.keys()],
  );

  const launchd = byPid.get(7539);
  check("stale_launchd_codex_app_server_is_named", launchd && launchd.staleConfig === true &&
    launchd.source === "codex" &&
    launchd.home === "~/.codex-profiles/pro2" &&
    launchd.homeSource === "env" &&
    launchd.startedAt === "2026-09-11T13:26:18.000Z" &&
    launchd.managedSurface === "codexProfile[pro2]" &&
    launchd.managedAppliedAt === "2026-09-12T14:00:00.000Z" &&
    launchd.managedAppliedAtSource === "apply_backup", launchd);
  check("launchd_owner_comes_from_launchctl_print", launchd?.owner === "launchd:com.jamesbrady.codex-profile.pro2" &&
    launchd.restartHint === `launchctl kickstart -k gui/${UID}/com.jamesbrady.codex-profile.pro2`, launchd);

  const conductor = byPid.get(38970);
  check("stale_conductor_child_is_named", conductor && conductor.staleConfig === true &&
    conductor.home === "~/.codex" &&
    conductor.homeSource === "default" &&
    conductor.managedSurface === "codex" &&
    conductor.managedAppliedAt === "2026-09-11T22:13:36.000Z", conductor);
  check("conductor_seat_comes_from_parent_command_line", conductor?.owner === "conductor:support-utlyze" &&
    conductor.restartHint?.includes("support-utlyze") &&
    conductor.restartHint.includes("not by killing pid 38970"), conductor);

  const fresh = byPid.get(41000);
  check("fresh_process_is_not_stale", fresh && fresh.staleConfig === false &&
    fresh.managedAppliedAt === "2026-09-15T10:00:00.000Z" &&
    fresh.managedAppliedAtSource === "reconcile_receipt" &&
    fresh.restartHint === null, fresh);
  const freshClaude = byPid.get(42000);
  check("fresh_claude_default_home_uses_mtime_fallback_and_says_so", freshClaude && freshClaude.staleConfig === false &&
    freshClaude.managedSurface === "claude" &&
    freshClaude.managedAppliedAtSource === "file_mtime", freshClaude);
  const seat = byPid.get(43001);
  check("mtime_only_stale_verdict_is_labelled", seat && seat.staleConfig === true &&
    seat.managedSurface === "claudeSeat[seat-a]" &&
    seat.managedAppliedAtSource === "file_mtime" &&
    seat.owner === "other", seat);
  const node = byPid.get(43000);
  check("node_hosted_claude_code_is_recognised", node?.source === "claude_code", node);

  const unreadable = byPid.get(50002);
  check("unreadable_home_is_unknown_and_never_stale", unreadable && unreadable.home === "unknown" &&
    unreadable.homeSource === "unreadable" &&
    unreadable.staleConfig === false &&
    unreadable.managedSurface === null &&
    unreadable.restartHint === null, unreadable);
  check("node_hosted_claude_without_environment_is_unknown", node?.home === "unknown" && node.staleConfig === false, node);

  check("stale_count_and_summary_line", scan.staleCount === 3 && scan.summary ===
    "3 producer process(es) older than their managed config (1 judged by file mtime only, no managed-apply record) — they send stale headers; restart: " +
    `launchctl kickstart -k gui/${UID}/com.jamesbrady.codex-profile.pro2; ` +
    "restart conductor seat support-utlyze through the conductor (not by killing pid 38970); " +
    "restart pid 43001 (claude_code) where it was started", scan.summary);
  check("no_token_or_command_line_in_scan", !JSON.stringify(scan).includes(CANARY) &&
    !JSON.stringify(scan).includes("app-server") && !JSON.stringify(scan).includes(sandbox), null);

  // The 400-row bound.
  const many: FixtureProcess[] = Array.from({ length: PRODUCER_PROCESS_ROW_LIMIT + 50 }, (_, index) => ({
    pid: 70000 + index,
    ppid: 1,
    started: "2026-09-16T00:00:00.000Z",
    command: "/opt/bin/grok",
    env: envLine({ HOME: home }),
  }));
  let environmentPids = 0;
  const boundedProvider = providerFor(writeFixture("bounded", many));
  const bounded = await scanProducerProcesses({
    collectorHome,
    home,
    provider: {
      ...boundedProvider,
      environments: async (pids) => {
        environmentPids = pids.length;
        return boundedProvider.environments(pids);
      },
    },
  });
  check("row_bound_is_400_producer_rows", bounded.inspection === "truncated" && bounded.truncated === true &&
    bounded.producerRows === PRODUCER_PROCESS_ROW_LIMIT + 50 &&
    bounded.processes.length === PRODUCER_PROCESS_ROW_LIMIT &&
    environmentPids === PRODUCER_PROCESS_ROW_LIMIT, {
    inspection: bounded.inspection,
    producerRows: bounded.producerRows,
    reported: bounded.processes.length,
    environmentPids,
  });

  const timedOut = await scanProducerProcesses({
    collectorHome,
    home,
    provider: { ...providerFor(mainFixture), processTable: async () => ({ ok: false, error: "timeout" }) },
  });
  check("process_table_timeout_is_reported_not_guessed", timedOut.inspection === "timeout" &&
    timedOut.processes.length === 0 && timedOut.summary === null, timedOut.inspection);

  check("proof_context_never_reads_the_real_process_table", resolveProducerProcessProvider({ ...process.env,
    [PRODUCER_PROCESS_FIXTURE_ENV]: "" }) === null, null);

  // `status` captureHealth reason.
  const counters = (reasons: RejectionDiagnosticsCounters["reasons"]): RejectionDiagnosticsCounters => ({
    counterLifetime: "ephemeral_process",
    intervalMs: 60_000,
    counterCap: Number.MAX_SAFE_INTEGER,
    acceptedBySource: { claude_code: 0, codex: 0, gemini_cli: 0, grok: 0 },
    totals: { acceptedTotal: 0, rejectedTotal: 0, suppressedTotal: 0, emittedFirstTotal: 0, summarizedTotal: 0 },
    reasons,
  });
  const openSourceRequired = counters([{
    reason: "source_required",
    clientClass: "otlp_exporter",
    rejected: 12,
    suppressed: 11,
    emittedFirst: 1,
    summarized: 0,
    openWindow: { count: 12, suppressed: 11 },
  }]);
  const health = {
    generatedAt: "2026-09-16T04:00:00.000Z",
    overall: "green",
    sources: [
      { source: "codex", status: "green", reason: "capture current — 4 session(s) with tokens today",
        rootsStarted: 2, rootsEligible: 3, rootsTotal: 4 },
      { source: "claude_code", status: "green", reason: "capture current — 1 session(s) with tokens today",
        rootsStarted: 1, rootsEligible: 1, rootsTotal: 1 },
    ],
  };
  const scanNow = Date.parse(scan.scannedAt);
  const annotated = annotateCaptureHealthWithStaleProducers(health, openSourceRequired, scan, scanNow);
  check("status_reason_names_the_stale_producer_count", annotated.sources[0]!.reason ===
    "capture current — 4 session(s) with tokens today; source_required rejections open: " +
    "2 codex producer process(es) older than their managed config — plimsoll doctor --read-only --json names them" &&
    annotated.sources[1]!.reason.endsWith("1 claude_code producer process(es) older than their managed config — plimsoll doctor --read-only --json names them"),
  annotated.sources.map((entry) => entry.reason));
  const withoutReason = (value: typeof health) => value.sources.map(({ reason: _reason, ...rest }) => rest);
  check("status_reason_is_never_a_fault_shape", annotated.overall === health.overall &&
    JSON.stringify(withoutReason(annotated)) === JSON.stringify(withoutReason(health)) &&
    annotated.sources.every((entry) => entry.rootsStarted <= entry.rootsEligible && entry.rootsEligible <= entry.rootsTotal),
  annotated);
  const tokenRequired = counters([{
    reason: "producer_token_required",
    clientClass: "codex",
    rejected: 3,
    suppressed: 2,
    emittedFirst: 1,
    summarized: 0,
    openWindow: { count: 3, suppressed: 2 },
  }]);
  const tokenAnnotated = annotateCaptureHealthWithStaleProducers(health, tokenRequired, scan, scanNow);
  check("producer_token_required_names_only_its_source", tokenAnnotated.sources[0]!.reason.includes("producer_token_required rejections open: 2 codex") &&
    tokenAnnotated.sources[1]!.reason === health.sources[1]!.reason, tokenAnnotated.sources.map((entry) => entry.reason));
  const closed = counters([{ ...openSourceRequired.reasons[0]!, openWindow: null }]);
  check("no_open_window_leaves_status_untouched", annotateCaptureHealthWithStaleProducers(health, closed, scan, scanNow) === health, null);
  check("scan_older_than_60s_is_not_used", annotateCaptureHealthWithStaleProducers(
    health, openSourceRequired, scan, scanNow + STALE_PRODUCER_SCAN_MAX_AGE_MS + 1,
  ).sources[0]!.reason.endsWith("source_required rejections open: stale-producer scan pending"), null);

  let scans = 0;
  let clock = scanNow;
  const cache = createStaleProducerScanCache(async () => {
    scans += 1;
    return { ...scan, scannedAt: new Date(clock).toISOString() } satisfies ProducerProcessScan;
  }, () => clock);
  const empty = cache.latest();
  await Promise.all([cache.refresh(), cache.refresh()]);
  await cache.refresh();
  clock += STALE_PRODUCER_SCAN_MAX_AGE_MS + 1;
  const expired = cache.latest();
  await cache.refresh();
  check("status_scan_is_cached_at_most_60s_and_single_flight", empty === null && expired === null && scans === 2 &&
    cache.latest() !== null, { scans });
  let failures = 0;
  const failing = createStaleProducerScanCache(async () => {
    failures += 1;
    throw new Error("process table read failed");
  }, () => clock);
  const failed = await failing.refresh();
  await failing.refresh();
  check("failed_status_scan_is_cached_not_retried_per_read", failed.inspection === "unavailable" && failures === 1 &&
    annotateCaptureHealthWithStaleProducers(health, openSourceRequired, failing.latest(), clock)
      .sources[0]!.reason.endsWith("stale-producer scan unavailable"), { failures, inspection: failed.inspection });

  // doctor --read-only --json end to end, against the fixture.
  const stubBin = path.join(sandbox, "stub-bin");
  const stubLog = path.join(sandbox, "stub-calls.log");
  fs.mkdirSync(stubBin, { recursive: true });
  for (const name of ["ps", "launchctl"]) {
    fs.writeFileSync(path.join(stubBin, name), `#!/bin/sh\necho "${name} $*" >> "${stubLog}"\nexit 1\n`, { mode: 0o755 });
  }
  function snapshot(directory: string) {
    const entries: string[] = [];
    const walk = (current: string) => {
      for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
        const full = path.join(current, entry.name);
        const stat = fs.lstatSync(full);
        entries.push(`${path.relative(directory, full)}:${stat.size}:${stat.mtimeMs}`);
        if (entry.isDirectory()) walk(full);
      }
    };
    walk(directory);
    return entries.sort().join("\n");
  }
  function doctor(extraEnv: Record<string, string>) {
    const result = spawnSync(process.execPath, [tsx, cli, "doctor", "--read-only", "--json"], {
      cwd: sandbox,
      encoding: "utf8",
      timeout: 120_000,
      env: {
        ...process.env,
        ...fixture.env,
        PATH: `${stubBin}:${process.env.PATH ?? ""}`,
        PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "100",
        ...extraEnv,
      },
    });
    return { code: result.status, stdout: result.stdout, stderr: result.stderr };
  }
  const noStaleFixture = writeFixture("fresh-only", processes.filter((row) => ![7539, 38970, 43001].includes(row.pid)));
  const before = snapshot(sandbox);
  const doctorRun = doctor({ [PRODUCER_PROCESS_FIXTURE_ENV]: mainFixture });
  check("doctor_prints_json", doctorRun.stdout.trim().startsWith("{"), { code: doctorRun.code, stderr: doctorRun.stderr.slice(0, 2000) });
  const receipt = JSON.parse(doctorRun.stdout) as {
    readOnly: boolean;
    readiness: string;
    summary?: string[];
    producerProcesses: ProducerProcessScan;
  };
  check("doctor_reports_producer_processes", receipt.readOnly === true &&
    receipt.producerProcesses.staleCount === 3 &&
    receipt.producerProcesses.processes.find((entry) => entry.pid === 7539)?.owner === "launchd:com.jamesbrady.codex-profile.pro2" &&
    receipt.producerProcesses.processes.find((entry) => entry.pid === 50002)?.home === "unknown", receipt.producerProcesses);
  check("doctor_summary_line_when_stale", Array.isArray(receipt.summary) && receipt.summary.length === 1 &&
    receipt.summary[0]!.startsWith("3 producer process(es) older than their managed config") &&
    receipt.summary[0]!.includes("they send stale headers; restart: "), receipt.summary);
  check("doctor_readiness_is_not_changed_by_the_diagnostic", receipt.readiness === "not_installed", receipt.readiness);
  check("doctor_output_carries_no_token", !doctorRun.stdout.includes(CANARY) && !doctorRun.stderr.includes(CANARY), null);
  const quiet = JSON.parse(doctor({ [PRODUCER_PROCESS_FIXTURE_ENV]: noStaleFixture }).stdout) as typeof receipt;
  check("doctor_has_no_summary_without_stale_producers", quiet.summary === undefined &&
    quiet.producerProcesses.staleCount === 0 && quiet.producerProcesses.processes.length === 4, quiet.summary ?? null);
  const proofContext = JSON.parse(doctor({}).stdout) as typeof receipt;
  check("doctor_in_proof_context_without_fixture_is_not_inspected", proofContext.producerProcesses.inspection === "not_inspected" &&
    proofContext.producerProcesses.reason === "proof_context_without_process_fixture", proofContext.producerProcesses);
  check("doctor_never_ran_ps_or_launchctl", !fs.existsSync(stubLog), fs.existsSync(stubLog) ? fs.readFileSync(stubLog, "utf8") : null);
  check("doctor_stays_read_only", snapshot(sandbox) === before, null);

  console.log(JSON.stringify({
    proof: "doctor-stale-producers",
    bead: "eco-6hoxj.153",
    passed: checks.every((entry) => entry.passed),
    checks: checks.map(({ name, passed }) => ({ name, passed })),
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    console.log(JSON.stringify({ proof: "doctor-stale-producers", bead: "eco-6hoxj.153", passed: false, checks }, null, 2));
    process.exitCode = 1;
  })
  .finally(() => {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  });
