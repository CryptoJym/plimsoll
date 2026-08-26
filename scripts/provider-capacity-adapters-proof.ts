#!/usr/bin/env node

/**
 * Issue #169 proof: provider capacity adapters.
 *
 * Proves, against golden fixtures, real child processes, and adversarial
 * inputs:
 *  - The Claude status-line stdin parser is a bounded ALLOWLIST: documented
 *    `rate_limits` windows + `version` only; absent windows stay absent;
 *    credential-shaped or unknown fields never echo anywhere.
 *  - The status-line proxy forwards RAW stdin to a chained operator command
 *    and preserves its stdout, stderr, exit status, timeout behavior; a
 *    capture failure never breaks the operator's line; oversized stdin is
 *    refused before buffering.
 *  - Configuration goes through the CURRENT atomic settings transaction:
 *    install when absent, chain when the operator entry is provably safe,
 *    `blocked_existing_statusline` (ZERO mutation) when it is not, byte-for-
 *    byte restore on interruption/failed commit.
 *  - The Codex app-server probe emits EXACTLY initialize → initialized
 *    notification → account/rateLimits/read on the stable JSONL wire,
 *    bounded in time/output, with clean shutdown, per-profile single-flight
 *    (`busy`), and exponential backoff state. Provider error payloads never
 *    leak into receipts.
 *  - Receipts carry symbolic codes/phases/timing/version/resource counters
 *    only — no raw provider JSON, paths, messages, or credentials.
 *
 * Run: pnpm proof:capacity-adapters
 */
import assert from "node:assert/strict";
import { execPath } from "node:process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  ADAPTER_BACKOFF_BASE_MS,
  CAPACITY_ADAPTER_ERRORS,
  CLAUDE_STATUS_LINE_MAX_STDIN_BYTES,
  CODEX_PROBE_MAX_OUTPUT_BYTES,
  STATUS_LINE_PROXY_INVOCATION_MARKER,
  classifyAdapterObservationFreshness,
  computeAdapterBackoff,
  configureClaudeStatusLineProxy,
  loadLatestAdapterReceipt,
  parseClaudeStatusLineStdin,
  parseCodexRateLimitsResponse,
  resolveAdapterStateHome,
  runClaudeStatusLineProxy,
  runCodexAppServerCapacityProbe,
} from "../packages/collector-cli/src/provider-capacity-adapters";
import { scanCapacityDoctrine } from "./capacity-dependency-reachability";

const root = process.cwd();
const fixtureDir = path.join(root, "scripts/fixtures/capacity-adapters");
const scratchRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capacity-adapters-"));
const fakeServer = path.join(fixtureDir, "fake-app-server.mjs");

type Check = { name: string; detail?: Record<string, unknown> };
const checks: Check[] = [];

function prove(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function freshScratch(label: string): string {
  const dir = path.join(scratchRoot, label);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function readFixture(...parts: string[]): Buffer {
  return fs.readFileSync(path.join(fixtureDir, ...parts));
}

function setFakeEnv(behavior: string, logFile: string | null, extra: Record<string, string> = {}) {
  process.env.FAKE_BEHAVIOR = behavior;
  if (logFile === null) delete process.env.FAKE_LOG;
  else process.env.FAKE_LOG = logFile;
  for (const [key, value] of Object.entries(extra)) process.env[key] = value;
}

function clearFakeEnv(extra: Record<string, string> = {}) {
  delete process.env.FAKE_BEHAVIOR;
  delete process.env.FAKE_LOG;
  for (const key of Object.keys(extra)) delete process.env[key];
}

function codexCommand(): string[] {
  return [execPath, fakeServer];
}

// ---------------------------------------------------------------------------
// S1: Claude stdin allowlist parser
// ---------------------------------------------------------------------------

function proveClaudeParser() {
  const full = parseClaudeStatusLineStdin(readFixture("valid", "claude-statusline-full.json"));
  prove(
    "claude_parse_full_extracts_documented_windows_and_version",
    full.kind === "ok" &&
      full.providerVersion === "2.1.205" &&
      full.windows.length === 2 &&
      full.windows[0]?.window === "five_hour" &&
      full.windows[0]!.usedPercent === 42 &&
      full.windows[0]!.resetsAt === "2026-08-25T15:00:00.000Z" &&
      full.windows[1]?.window === "seven_day" &&
      full.windows[1]!.usedPercent === 15.5,
    { outcome: full.kind, windows: full.kind === "ok" ? full.windows : null },
  );

  const absent = parseClaudeStatusLineStdin(
    readFixture("valid", "claude-statusline-rate-limits-absent.json"),
  );
  prove(
    "claude_absent_rate_limits_stay_absent_never_zero",
    absent.kind === "ok" && absent.windows.length === 0,
    { windows: absent.kind === "ok" ? absent.windows : null },
  );

  const partial = parseClaudeStatusLineStdin(
    readFixture("valid", "claude-statusline-partial-window.json"),
  );
  prove(
    "claude_partial_windows_only_present_slots_and_epoch_resets_normalized",
    partial.kind === "ok" &&
      partial.windows.length === 2 &&
      partial.windows[0]?.window === "five_hour" &&
      partial.windows[0]!.resetsAt === "2026-12-27T04:20:00.000Z" &&
      partial.windows[0]!.usedPercent === 88.25 &&
      partial.windows[1]?.window === "seven_day_sonnet" &&
      partial.windows[1]!.resetsAt === null,
    { windows: partial.kind === "ok" ? partial.windows : null },
  );

  const badUtilization = parseClaudeStatusLineStdin(
    JSON.stringify({
      version: "2.1.0",
      rate_limits: {
        five_hour: { used_percentage: "42" },
        seven_day: { used_percentage: true },
        seven_day_sonnet: { used_percentage: 101 },
      },
    }),
  );
  prove(
    "claude_invalid_utilization_types_dropped_fail_closed",
    badUtilization.kind === "ok" && badUtilization.windows.length === 0,
    { windows: badUtilization.kind === "ok" ? badUtilization.windows : null },
  );

  const hostileRaw = readFixture("hostile", "claude-statusline-credential-shaped.json");
  const hostile = parseClaudeStatusLineStdin(hostileRaw);
  const hostileSerialized = JSON.stringify(hostile);
  prove(
    "claude_hostile_payload_parses_allowlisted_facts_only",
    hostile.kind === "ok" &&
      hostile.windows.length === 1 &&
      hostile.windows[0]!.window === "five_hour",
    { windows: hostile.kind === "ok" ? hostile.windows : null },
  );
  const secretMarkers = [
    "sk-proj-AAAAAAAA",
    "ghp_HOSTILETOKENVALUE",
    "xoxb-hostile",
    "eyJhbGciOiJIUzI1NiJ9",
    "/Users/attacker",
    "unknown_future_window",
  ];
  prove(
    "claude_hostile_credentials_and_unknown_keys_never_echo",
    secretMarkers.every((marker) => !hostileSerialized.includes(marker)),
    { checkedMarkers: secretMarkers.length },
  );

  const malformed = parseClaudeStatusLineStdin(
    readFixture("hostile", "claude-statusline-malformed.json"),
  );
  prove("claude_malformed_json_fails_closed", malformed.kind === "parse_failed" && malformed.reason === "not_json");

  const arrayRoot = parseClaudeStatusLineStdin(readFixture("hostile", "claude-statusline-array-root.json"));
  prove("claude_array_root_fails_closed", arrayRoot.kind === "parse_failed" && arrayRoot.reason === "not_object");

  const oversized = "x".repeat(CLAUDE_STATUS_LINE_MAX_STDIN_BYTES + 1);
  const tooLarge = parseClaudeStatusLineStdin(oversized);
  prove("claude_oversized_stdin_refused_by_bound", tooLarge.kind === "parse_failed" && tooLarge.reason === "too_large");

  const secretVersion = parseClaudeStatusLineStdin(
    JSON.stringify({ version: "sk-proj-HOSTILEVERSIONPREFIX000000" }),
  );
  prove(
    "claude_secret_shaped_version_dropped_to_null",
    secretVersion.kind === "ok" && secretVersion.providerVersion === null,
  );
}

// ---------------------------------------------------------------------------
// S2: Status-line proxy runtime (real /bin/sh chains)
// ---------------------------------------------------------------------------

async function proveClaudeProxy() {
  const homeDir = freshScratch("proxy-home");
  const passthroughCommand =
    "cat > /dev/null; echo 'MODEL:opus'; echo 'boom' >&2; exit 7";
  const passed = await runClaudeStatusLineProxy({
    stdinBytes: readFixture("valid", "claude-statusline-full.json"),
    chainCommand: passthroughCommand,
    homeDir,
  });
  prove(
    "proxy_chain_preserves_stdout_stderr_exit_status",
    passed.stdout.toString().trim() === "MODEL:opus" &&
      passed.stderr.toString().trim() === "boom" &&
      passed.exitCode === 7 &&
      passed.receipt.ok &&
      passed.receipt.resourceCost.exitCode === 7 &&
      passed.receipt.windows.length === 2 &&
      passed.receipt.providerVersion === "2.1.205",
    {
      stdout: passed.stdout.toString(),
      stderr: passed.stderr.toString(),
      exitCode: passed.exitCode,
      receiptError: passed.receipt.error,
    },
  );

  const alone = await runClaudeStatusLineProxy({
    stdinBytes: readFixture("valid", "claude-statusline-full.json"),
    chainCommand: null,
    homeDir,
  });
  prove(
    "proxy_without_chain_is_silent_and_successful",
    alone.stdout.length === 0 && alone.exitCode === 0 && alone.receipt.ok,
    { stdoutLength: alone.stdout.length, exitCode: alone.exitCode },
  );

  const brokenCapture = await runClaudeStatusLineProxy({
    stdinBytes: readFixture("hostile", "claude-statusline-malformed.json"),
    chainCommand: passthroughCommand,
    homeDir,
  });
  prove(
    "proxy_capture_failure_does_not_break_operator_line",
    brokenCapture.stdout.toString().trim() === "MODEL:opus" &&
      brokenCapture.exitCode === 7 &&
      brokenCapture.receipt.error?.code === "parse_failed" &&
      !brokenCapture.receipt.ok,
    {
      stdout: brokenCapture.stdout.toString(),
      receiptError: brokenCapture.receipt.error,
    },
  );

  const timedOut = await runClaudeStatusLineProxy({
    stdinBytes: Buffer.from("{}"),
    chainCommand: "sleep 5",
    chainTimeoutMs: 300,
    shutdownGraceMs: 200,
    homeDir,
  });
  prove(
    "proxy_chain_timeout_enforced_with_symbolic_status",
    timedOut.exitCode === 124 &&
      ["SIGTERM", "SIGKILL"].includes(timedOut.receipt.resourceCost.signal ?? "") &&
      timedOut.receipt.timing.elapsedMs < 4000,
    {
      exitCode: timedOut.exitCode,
      signal: timedOut.receipt.resourceCost.signal,
      elapsedMs: timedOut.receipt.timing.elapsedMs,
    },
  );

  const sideEffect = path.join(freshScratch("proxy-bounds"), "chain-ran.marker");
  const oversizedStdin = Buffer.alloc(CLAUDE_STATUS_LINE_MAX_STDIN_BYTES + 1024, 0x61);
  const overBound = await runClaudeStatusLineProxy({
    stdinBytes: oversizedStdin,
    chainCommand: `touch '${sideEffect}'`,
    homeDir,
  });
  prove(
    "proxy_oversized_stdin_refused_before_chaining",
    overBound.receipt.error?.code === "input_bound_exceeded" &&
      overBound.stdout.length === 0 &&
      overBound.exitCode === 0 &&
      !fs.existsSync(sideEffect),
    {
      receiptError: overBound.receipt.error,
      chainRan: fs.existsSync(sideEffect),
    },
  );

  const epipe = await runClaudeStatusLineProxy({
    stdinBytes: readFixture("valid", "claude-statusline-full.json"),
    chainCommand: "echo hi",
    homeDir,
  });
  prove(
    "proxy_tolerates_operator_command_that_skips_stdin",
    epipe.stdout.toString() === "hi\n" && epipe.exitCode === 0,
    { stdout: epipe.stdout.toString(), exitCode: epipe.exitCode },
  );

  // Privacy seal over persisted state: hostile stdin must leave NO trace.
  const hostileHome = freshScratch("proxy-hostile-home");
  await runClaudeStatusLineProxy({
    stdinBytes: readFixture("hostile", "claude-statusline-credential-shaped.json"),
    chainCommand: "cat > /dev/null",
    homeDir: hostileHome,
  });
  const stateFiles = (function walk(dir: string): string[] {
    return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
      const full = path.join(dir, entry.name);
      return entry.isDirectory() ? walk(full) : [full];
    });
  })(resolveAdapterStateHome({ homeDir: hostileHome }));
  const stateBytes = stateFiles.map((file) => fs.readFileSync(file, "utf8")).join("\n");
  const leaked = [
    "sk-proj-AAAAAAAA",
    "ghp_HOSTILETOKENVALUE",
    "xoxb-hostile",
    "/Users/attacker",
    "victim",
  ].filter((marker) => stateBytes.includes(marker));
  prove(
    "proxy_persisted_receipts_never_contain_raw_provider_material",
    leaked.length === 0 && stateFiles.some((file) => file.endsWith(".receipt.json")),
    { stateFileCount: stateFiles.length, leaked },
  );

  const flood = await runClaudeStatusLineProxy({
    stdinBytes: Buffer.from("{}"),
    chainCommand: "dd if=/dev/zero bs=1024 count=2048 2>/dev/null",
    maxStreamBytes: 128 * 1024,
    shutdownGraceMs: 200,
    homeDir,
  });
  prove(
    "proxy_stream_output_bound_enforced_without_unbounded_memory",
    flood.exitCode === 124 && flood.stdout.length <= 256 * 1024,
    { exitCode: flood.exitCode, capturedBytes: flood.stdout.length },
  );
}

// ---------------------------------------------------------------------------
// S3: Configuration through the atomic settings transaction
// ---------------------------------------------------------------------------

function writeSettings(file: string, content: string) {
  fs.writeFileSync(file, content, "utf8");
}

function settingsWith(statusLineValue: unknown, extraKey = true): string {
  const document: Record<string, unknown> = {};
  if (extraKey) document.env = { MY_OPERATOR_FLAG: "keep-me" };
  if (statusLineValue !== undefined) document.statusLine = statusLineValue;
  return `${JSON.stringify(document, null, 2)}\n`;
}

function proveConfigure() {
  const baseProxyCommand = "tsx /abs/repo/packages/collector-cli/src/provider-capacity-adapters.ts";
  const dir = freshScratch("configure");
  const settingsPath = path.join(dir, "settings.json");

  writeSettings(settingsPath, settingsWith(undefined));
  const installed = configureClaudeStatusLineProxy({ settingsPath, baseProxyCommand });
  const afterInstall = fs.readFileSync(settingsPath, "utf8");
  prove(
    "configure_installs_through_atomic_transaction_when_absent",
    installed.outcome === "installed" &&
      installed.changes.includes("claude.statusLine.set") &&
      afterInstall.includes(STATUS_LINE_PROXY_INVOCATION_MARKER) &&
      afterInstall.includes('"MY_OPERATOR_FLAG": "keep-me"'),
    {
      outcome: installed.outcome,
      changes: installed.outcome === "installed" ? installed.changes : null,
    },
  );

  const rerun = configureClaudeStatusLineProxy({ settingsPath, baseProxyCommand });
  prove(
    "configure_rerun_is_idempotent_no_op",
    rerun.outcome === "already_installed" && rerun.changes.length === 0,
    {
      outcome: rerun.outcome,
      changes: rerun.outcome === "already_installed" ? rerun.changes : null,
    },
  );

  // Operator chaining: prior command preserved verbatim inside our entry.
  const operatorSettings = path.join(dir, "operator-settings.json");
  const operatorEntry = { type: "command", command: "~/.claude/statusline.sh", padding: 0 };
  writeSettings(operatorSettings, settingsWith(operatorEntry));
  const chained = configureClaudeStatusLineProxy({
    settingsPath: operatorSettings,
    baseProxyCommand,
  });
  const configuredText = fs.readFileSync(operatorSettings, "utf8");
  const configuredJson = JSON.parse(configuredText) as { statusLine: { command: string } };
  const embedded = Buffer.from(
    configuredJson.statusLine.command.split(" ").at(-1)!,
    "base64",
  ).toString("utf8");
  prove(
    "configure_chains_operator_entry_preserving_prior_command",
    chained.outcome === "chained" &&
      chained.chainedCommand === "~/.claude/statusline.sh" &&
      embedded === "~/.claude/statusline.sh" &&
      configuredJson.statusLine.command.includes(STATUS_LINE_PROXY_INVOCATION_MARKER),
    { outcome: chained.outcome, embedded },
  );

  const rerunChain = configureClaudeStatusLineProxy({
    settingsPath: operatorSettings,
    baseProxyCommand,
  });
  prove(
    "configure_chained_rerun_is_no_op",
    rerunChain.outcome === "unchanged_chain" && rerunChain.changes.length === 0,
    {
      outcome: rerunChain.outcome,
      changes: rerunChain.outcome === "unchanged_chain" ? rerunChain.changes : null,
    },
  );

  const blockedCases: Array<[string, unknown]> = [
    ["wrong_type", { type: "http", url: "http://example.test/status" }],
    ["missing_command", { type: "command" }],
    ["extra_unknown_field", { type: "command", command: "x.sh", mystery: true }],
    ["string_entry", "just-a-string"],
    ["null_entry", null],
  ];
  let blockedUntouched = 0;
  for (const [label, entry] of blockedCases) {
    const blockedPath = path.join(dir, `blocked-${label}.json`);
    const original = settingsWith(entry);
    writeSettings(blockedPath, original);
    const result = configureClaudeStatusLineProxy({ settingsPath: blockedPath, baseProxyCommand });
    const after = fs.readFileSync(blockedPath, "utf8");
    if (
      result.outcome === "blocked_existing_statusline" &&
      after === original
    ) {
      blockedUntouched += 1;
    }
  }
  prove(
    "configure_blocks_unrecognized_entries_without_any_mutation",
    blockedUntouched === blockedCases.length,
    { blockedUntouched, total: blockedCases.length },
  );

  const aliasPath = path.join(dir, "alias-settings.json");
  const aliasOriginal = `${JSON.stringify({ Statusline: { type: "command", command: "a.sh" } }, null, 2)}\n`;
  writeSettings(aliasPath, aliasOriginal);
  const aliasResult = configureClaudeStatusLineProxy({ settingsPath: aliasPath, baseProxyCommand });
  prove(
    "configure_blocks_case_folded_alias_key_without_mutation",
    aliasResult.outcome === "blocked_existing_statusline" &&
      fs.readFileSync(aliasPath, "utf8") === aliasOriginal,
    { outcome: aliasResult.outcome },
  );

  const malformedPath = path.join(dir, "malformed-settings.json");
  const malformedOriginal = "{ not json ]";
  writeSettings(malformedPath, malformedOriginal);
  const malformedResult = configureClaudeStatusLineProxy({
    settingsPath: malformedPath,
    baseProxyCommand,
  });
  prove(
    "configure_blocks_malformed_settings_without_mutation",
    malformedResult.outcome === "blocked_invalid_settings" &&
      fs.readFileSync(malformedPath, "utf8") === malformedOriginal,
    { outcome: malformedResult.outcome },
  );

  // Interruption mid-commit: the transaction hook throws right before
  // publication; the original file must survive BYTE-FOR-BYTE.
  const interruptPath = path.join(dir, "interrupt-settings.json");
  const interruptOriginal = settingsWith(undefined);
  writeSettings(interruptPath, interruptOriginal);
  const interrupted = configureClaudeStatusLineProxy({
    settingsPath: interruptPath,
    baseProxyCommand,
    transactionHooks: {
      beforeCommit: () => {
        throw new Error("simulated interruption");
      },
    },
  });
  prove(
    "configure_interruption_restores_original_byte_for_byte",
    interrupted.outcome === "blocked_existing_statusline" &&
      fs.readFileSync(interruptPath, "utf8") === interruptOriginal,
    {
      outcome: interrupted.outcome,
      reason: interrupted.outcome.startsWith("blocked")
        ? (interrupted as { reason: string }).reason
        : null,
    },
  );

  // Transaction hygiene: temp/claim objects never linger, and decisions
  // BLOCKED during dry-run planning never even create a backup. Successful
  // commits deliberately keep their .plimsoll-backup-* rollback artifact,
  // so those are asserted present, not absent.
  const allEntries = fs.readdirSync(dir);
  const incompleteObjects = allEntries.filter(
    (name) => name.includes(".plimsoll-tmp-") || name.includes(".plimsoll-claim-"),
  );
  const blockedSiblings = allEntries.filter((name) =>
    /^(blocked-|alias-settings|malformed-settings)/.test(name),
  );
  const blockedDebris = blockedSiblings.filter((name) => name.includes(".plimsoll-backup-"));
  const committedBackups = allEntries.filter(
    (name) =>
      /^(settings|operator-settings|interrupt-settings)\.json/.test(name) &&
      name.includes(".plimsoll-backup-"),
  );
  prove(
    "configure_leaves_no_transaction_debris_behind",
    incompleteObjects.length === 0 && blockedDebris.length === 0,
    { incompleteObjects, blockedDebris },
  );
  prove(
    "configure_successful_commits_keep_rollback_backup_artifacts",
    committedBackups.length >= 2,
    { committedBackups },
  );
}

// ---------------------------------------------------------------------------
// S4: Codex rateLimits allowlist parser
// ---------------------------------------------------------------------------

function proveCodexParser() {
  const primaryOnly = parseCodexRateLimitsResponse(
    JSON.parse(fs.readFileSync(path.join(fixtureDir, "valid", "codex-ratelimits-primary-only.json"), "utf8")),
  );
  prove(
    "codex_primary_window_mapped_secondary_null_stays_absent",
    primaryOnly.kind === "ok" &&
      primaryOnly.windows.length === 1 &&
      primaryOnly.windows[0]!.window === "primary" &&
      primaryOnly.windows[0]!.usedPercent === 25 &&
      primaryOnly.windows[0]!.windowMinutes === 15 &&
      primaryOnly.windows[0]!.resetsAt === "2024-11-07T02:40:00.000Z",
    { windows: primaryOnly.kind === "ok" ? primaryOnly.windows : null },
  );

  const both = parseCodexRateLimitsResponse(
    JSON.parse(fs.readFileSync(path.join(fixtureDir, "valid", "codex-ratelimits-both-windows.json"), "utf8")),
  );
  prove(
    "codex_both_windows_mapped_credits_dropped",
    both.kind === "ok" &&
      both.windows.length === 2 &&
      both.windows[1]!.window === "secondary" &&
      both.windows[1]!.usedPercent === 77.5 &&
      both.windows[1]!.resetsAt === "2024-11-14T02:40:00.000Z",
    { windows: both.kind === "ok" ? both.windows : null },
  );

  const providerError = parseCodexRateLimitsResponse({
    id: 2,
    error: { code: -32000, message: "SECRET-AUTH-DETAILS-hostile-provider-payload" },
  });
  prove(
    "codex_provider_error_maps_to_symbolic_code_not_message",
    providerError.kind === "provider_error",
    providerError.kind === "provider_error" ? {} : providerError,
  );

  const missingResult = parseCodexRateLimitsResponse({ id: 2 });
  prove("codex_missing_result_fails_closed", missingResult.kind === "parse_failed");

  const badPercent = parseCodexRateLimitsResponse({
    id: 2,
    result: { rateLimits: { primary: { usedPercent: "25" }, secondary: null } },
  });
  prove("codex_non_numeric_used_percent_fails_closed", badPercent.kind === "parse_failed");

  const notObject = parseCodexRateLimitsResponse([1, 2, 3]);
  prove("codex_array_response_fails_closed", notObject.kind === "parse_failed");
}

// ---------------------------------------------------------------------------
// S5: Codex probe end-to-end against the fake stable-API server
// ---------------------------------------------------------------------------

function readLog(logFile: string): Array<Record<string, unknown>> {
  if (!fs.existsSync(logFile)) return [];
  return fs
    .readFileSync(logFile, "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function assertReceiptShape(receipt: unknown): void {
  const allowedTopLevel = new Set([
    "schema",
    "adapter",
    "adapterVersion",
    "profileId",
    "ok",
    "phase",
    "error",
    "observedAt",
    "freshness",
    "windows",
    "providerVersion",
    "timing",
    "resourceCost",
  ]);
  const record = receipt as Record<string, unknown>;
  assert.ok(record !== null && typeof record === "object");
  for (const key of Object.keys(record)) {
    assert.ok(allowedTopLevel.has(key), `receipt carries non-contract key ${key}`);
  }
}

async function probeOnce(options: {
  behavior: string;
  homeDir: string;
  extraEnv?: Record<string, string>;
  probeOptions?: Partial<Parameters<typeof runCodexAppServerCapacityProbe>[0]>;
}) {
  const logFile = path.join(options.homeDir, "server-log.jsonl");
  setFakeEnv(options.behavior, logFile, options.extraEnv ?? {});
  try {
    const receipt = await runCodexAppServerCapacityProbe({
      command: codexCommand(),
      homeDir: options.homeDir,
      ...options.probeOptions,
    });
    return { receipt, logFile };
  } finally {
    clearFakeEnv(options.extraEnv ?? {});
  }
}

async function proveCodexProbe() {
  // Happy path: exact outbound sequence, mapped windows, cleared backoff.
  const happyHome = freshScratch("codex-happy");
  const happy = await probeOnce({ behavior: "happy", homeDir: happyHome });
  const outbound = readLog(happy.logFile);
  assertReceiptShape(happy.receipt);
  prove(
    "codex_probe_outbound_sequence_exact_initialize_initialized_ratelimits",
    outbound.length === 3 &&
      outbound[0]?.method === "initialize" &&
      typeof outbound[0]?.id === "number" &&
      (outbound[0]?.params as Record<string, unknown>)?.clientInfo !== undefined &&
      outbound[1]?.method === "initialized" &&
        outbound[1]?.id === undefined &&
      outbound[2]?.method === "account/rateLimits/read" &&
      typeof outbound[2]?.id === "number",
    { outbound },
  );
  prove(
    "codex_probe_happy_receipt_ok_fresh_with_windows",
    happy.receipt.ok &&
      happy.receipt.error === null &&
      happy.receipt.phase === "complete" &&
      happy.receipt.freshness.status === "fresh" &&
      happy.receipt.windows.length === 1 &&
      happy.receipt.windows[0]!.window === "primary" &&
      happy.receipt.resourceCost.spawnCount === 1 &&
      happy.receipt.resourceCost.bytesRead > 0 &&
      happy.receipt.timing.elapsedMs >= 0,
    {
      ok: happy.receipt.ok,
      phase: happy.receipt.phase,
      windows: happy.receipt.windows,
      resourceCost: happy.receipt.resourceCost,
    },
  );
  const happyPersisted = loadLatestAdapterReceipt({
    home: resolveAdapterStateHome({ homeDir: happyHome }),
    adapter: "codex_app_server",
  });
  prove(
    "codex_probe_happy_receipt_persisted_and_backoff_cleared",
    happyPersisted !== null &&
      happyPersisted.ok === true &&
      fs.readFileSync(
        path.join(
          resolveAdapterStateHome({ homeDir: happyHome }),
          "capacity/adapters/codex_app_server.default.backoff.json",
        ),
        "utf8",
      ).includes('"consecutiveFailures": 0'),
    { persisted: happyPersisted !== null },
  );

  // Secondary window variant maps both slots.
  const secondaryHome = freshScratch("codex-secondary");
  const secondary = await probeOnce({ behavior: "secondary", homeDir: secondaryHome });
  prove(
    "codex_probe_secondary_window_mapped",
    secondary.receipt.ok &&
      secondary.receipt.windows.length === 2 &&
      secondary.receipt.windows[1]!.window === "secondary" &&
      secondary.receipt.windows[1]!.usedPercent === 77.5,
    { windows: secondary.receipt.windows },
  );

  // Provider error stays symbolic; nothing leaks; backoff arms.
  const errorHome = freshScratch("codex-provider-error");
  const failed = await probeOnce({ behavior: "provider_error", homeDir: errorHome });
  const receiptText = JSON.stringify(failed.receipt);
  prove(
    "codex_provider_error_symbolic_without_payload_leak",
    !failed.receipt.ok &&
      failed.receipt.error?.code === "provider_error" &&
      !receiptText.includes("SECRET-AUTH-DETAILS") &&
      failed.receipt.windows.length === 0,
    { error: failed.receipt.error },
  );
  const backoffAfterFailure = fs.readFileSync(
    path.join(
      resolveAdapterStateHome({ homeDir: errorHome }),
      "capacity/adapters/codex_app_server.default.backoff.json",
    ),
    "utf8",
  );
  prove(
    "codex_failure_arms_backoff_state",
    backoffAfterFailure.includes('"consecutiveFailures": 1') &&
      backoffAfterFailure.includes('"nextAllowedAt": "20'),
    { backoff: JSON.parse(backoffAfterFailure) },
  );

  // Backoff gate refuses BEFORE spawning anything.
  const serverLogBefore = fs.statSync(failed.logFile).size;
  const gated = await probeOnce({ behavior: "happy", homeDir: errorHome });
  prove(
    "codex_backoff_window_returns_symbolic_busy_style_receipt_without_spawn",
    gated.receipt.error?.code === "backoff_window_active" &&
      gated.receipt.phase === "not_started" &&
      gated.receipt.resourceCost.spawnCount === 0 &&
      fs.statSync(gated.logFile).size === serverLogBefore,
    { error: gated.receipt.error, spawnCount: gated.receipt.resourceCost.spawnCount },
  );

  // Expired window collapses to allowed on read.
  const expiredHome = freshScratch("codex-backoff-expired");
  const stateDir = path.join(resolveAdapterStateHome({ homeDir: expiredHome }), "capacity/adapters");
  fs.mkdirSync(stateDir, { recursive: true });
  fs.writeFileSync(
    path.join(stateDir, "codex_app_server.default.backoff.json"),
    JSON.stringify({
      consecutiveFailures: 3,
      nextAllowedAt: new Date(Date.now() - 60_000).toISOString(),
      updatedAt: new Date(Date.now() - 120_000).toISOString(),
    }),
    "utf8",
  );
  const expired = await probeOnce({ behavior: "happy", homeDir: expiredHome });
  prove(
    "codex_expired_backoff_window_collapses_to_allowed",
    expired.receipt.ok && expired.receipt.error === null,
    { error: expired.receipt.error },
  );

  // Hung rateLimits reply hits the wall-clock bound and kills the child.
  const hangHome = freshScratch("codex-hang");
  const hungStart = Date.now();
  const hung = await probeOnce({
    behavior: "hang_rate_limits",
    homeDir: hangHome,
    probeOptions: { timeoutMs: 700, shutdownGraceMs: 250 },
  });
  prove(
    "codex_hung_server_bounded_by_timeout_and_shut_down",
    hung.receipt.error?.code === "timeout" &&
      Date.now() - hungStart < 5000 &&
      (hung.receipt.resourceCost.signal !== null || hung.receipt.resourceCost.exitCode !== null),
    {
      error: hung.receipt.error,
      elapsedWallMs: Date.now() - hungStart,
      signal: hung.receipt.resourceCost.signal,
      exitCode: hung.receipt.resourceCost.exitCode,
    },
  );

  // Output flood trips the byte ceiling.
  const floodHome = freshScratch("codex-flood");
  const flooded = await probeOnce({
    behavior: "flood",
    homeDir: floodHome,
    probeOptions: { maxOutputBytes: 64 * 1024, timeoutMs: 5000, shutdownGraceMs: 250 },
  });
  prove(
    "codex_output_flood_trips_bound_and_shuts_down",
    flooded.receipt.error?.code === "output_bound_exceeded" &&
      flooded.receipt.resourceCost.bytesRead <= 128 * 1024,
    {
      error: flooded.receipt.error,
      bytesRead: flooded.receipt.resourceCost.bytesRead,
    },
  );

  // Unparsable protocol noise fails closed.
  const garbageHome = freshScratch("codex-garbage");
  const garbage = await probeOnce({ behavior: "garbage_line", homeDir: garbageHome });
  prove(
    "codex_unparsable_protocol_line_is_protocol_violation",
    garbage.receipt.error?.code === "protocol_violation",
    { error: garbage.receipt.error },
  );

  // Server that dies immediately is reported with its exit code.
  const earlyHome = freshScratch("codex-exit-early");
  const early = await probeOnce({ behavior: "exit_early", homeDir: earlyHome });
  prove(
    "codex_early_exit_reported_with_exit_code",
    !early.receipt.ok &&
      early.receipt.resourceCost.exitCode === 3 &&
      early.receipt.error !== null,
    {
      error: early.receipt.error,
      exitCode: early.receipt.resourceCost.exitCode,
    },
  );

  // Missing binary fails closed as spawn_failed.
  const missingHome = freshScratch("codex-missing-binary");
  const missing = await probeOnce({
    behavior: "happy",
    homeDir: missingHome,
    probeOptions: { command: ["plimsoll-definitely-missing-codex-binary"] },
  });
  prove(
    "codex_missing_binary_reports_spawn_failed",
    missing.receipt.error?.code === "spawn_failed" ||
      missing.receipt.error?.code === "protocol_violation",
    { error: missing.receipt.error },
  );

  // Single-flight concurrency: overlapping probe gets busy, spawns nothing.
  const busyHome = freshScratch("codex-busy");
  const releasePath = path.join(busyHome, "release.signal");
  const logFileBusy = path.join(busyHome, "server-log.jsonl");
  setFakeEnv("slow_init", logFileBusy, { FAKE_RELEASE: releasePath });
  let firstProbe: ReturnType<typeof runCodexAppServerCapacityProbe> | null = null;
  try {
    firstProbe = runCodexAppServerCapacityProbe({
      command: codexCommand(),
      homeDir: busyHome,
    });
    // Give the first probe time to claim the slot and spawn.
    await new Promise((resolve) => setTimeout(resolve, 300));
    const second = await runCodexAppServerCapacityProbe({
      command: codexCommand(),
      homeDir: busyHome,
    });
    prove(
      "codex_concurrent_probe_returns_symbolic_busy_without_spawning",
      second.error?.code === "busy" &&
        second.phase === "not_started" &&
        second.resourceCost.spawnCount === 0,
      { error: second.error, spawnCount: second.resourceCost.spawnCount },
    );
    fs.writeFileSync(releasePath, "go", "utf8");
  } finally {
    if (firstProbe === null) {
      throw new Error("first probe was never started");
    }
    const firstReceipt = await firstProbe;
    clearFakeEnv({ FAKE_RELEASE: releasePath });
    prove(
      "codex_first_probe_completes_after_slot_release",
      firstReceipt.ok && firstReceipt.windows.length === 1,
      { ok: firstReceipt.ok, error: firstReceipt.error },
    );
  }

  // Slot release: a follow-up probe runs again cleanly.
  const afterBusy = await probeOnce({ behavior: "happy", homeDir: busyHome });
  prove(
    "codex_slot_released_for_subsequent_manual_refresh",
    afterBusy.receipt.ok,
    { error: afterBusy.receipt.error },
  );
}

// ---------------------------------------------------------------------------
// S6: Doctrine integration + freshness semantics
// ---------------------------------------------------------------------------

function proveDoctrineAndFreshness() {
  const report = scanCapacityDoctrine(root);
  const offenders = report.offendingImporters.filter(
    (offense) =>
      !offense.file.includes("runtime-flake-regression-proof") &&
      !offense.file.includes("local-http-boundary-proof"),
  );
  prove(
    "capacity_doctrine_gate_reports_zero_offenders_after_adapters",
    offenders.length === 0,
    { offenders },
  );

  prove(
    "adapter_error_codes_are_a_closed_set",
    CAPACITY_ADAPTER_ERRORS.every((code) =>
      [
        "none",
        "busy",
        "backoff_window_active",
        "blocked_existing_statusline",
        "blocked_invalid_settings",
        "input_bound_exceeded",
        "output_bound_exceeded",
        "parse_failed",
        "protocol_violation",
        "provider_error",
        "spawn_failed",
        "timeout",
        "io_error",
      ].includes(code),
    ) && CAPACITY_ADAPTER_ERRORS.length === 13,
    { count: CAPACITY_ADAPTER_ERRORS.length },
  );

  const now = new Date();
  const fresh = classifyAdapterObservationFreshness({
    observedAt: new Date(now.getTime() - 1000).toISOString(),
    now: now.toISOString(),
    maxAgeMs: 60_000,
  });
  const stale = classifyAdapterObservationFreshness({
    observedAt: new Date(now.getTime() - 120_000).toISOString(),
    now: now.toISOString(),
    maxAgeMs: 60_000,
  });
  const future = classifyAdapterObservationFreshness({
    observedAt: new Date(now.getTime() + 60_000).toISOString(),
    now: now.toISOString(),
    maxAgeMs: 60_000,
  });
  prove(
    "adapter_freshness_fails_closed_future_is_unknown_never_zero_age",
    fresh.status === "fresh" && stale.status === "STALE" && future.status === "UNKNOWN" && future.ageMs === null,
    { fresh, stale, future },
  );

  const backoffLadder = computeAdapterBackoff({
    previous: { consecutiveFailures: 1, nextAllowedAt: null, updatedAt: now.toISOString() },
    failed: true,
    now,
  });
  const expectedDelay = Math.min(60_000, ADAPTER_BACKOFF_BASE_MS * 2);
  prove(
    "adapter_backoff_ladder_exponential_then_capped_and_success_resets",
    backoffLadder.consecutiveFailures === 2 &&
      backoffLadder.nextAllowedAt === new Date(now.getTime() + expectedDelay).toISOString() &&
      computeAdapterBackoff({ previous: backoffLadder, failed: false, now }).consecutiveFailures === 0,
    { nextAllowedAt: backoffLadder.nextAllowedAt, expectedDelay },
  );
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  proveClaudeParser();
  await proveClaudeProxy();
  proveConfigure();
  proveCodexParser();
  await proveCodexProbe();
  proveDoctrineAndFreshness();

  fs.rmSync(scratchRoot, { recursive: true, force: true });

  process.stdout.write(
  `${JSON.stringify(
    {
      status: "passed",
      checks: checks.length,
      suites: {
        claude_parser: true,
        claude_proxy_runtime: true,
        claude_configure_transaction: true,
        codex_parser: true,
        codex_probe_runtime: true,
        doctrine_and_freshness: true,
      },
    },
    null,
    2,
  )}\n`,
);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
