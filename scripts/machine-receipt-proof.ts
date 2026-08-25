/**
 * Focused proof for issue #133: one versioned machine-readable receipt.
 *
 * Every fixture uses a temporary HOME and PLIMSOLL_HOME. The proof runs the real
 * CLI runtime directly (node + tsx loader + cli.ts) exactly as the installer
 * must; it never registers a real LaunchAgent outside the sandbox, never reads
 * operator tool config or credentials, and never touches the network beyond
 * loopback ports it owns.
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import {
  DIAGNOSTICS_FORWARD_LIMIT_CHARS,
  MACHINE_RECEIPT_SCHEMA_NAME,
  MACHINE_SUPPORTED_COMMANDS,
  createMachineSanitizer,
  evaluateChildStdout,
  machineReceiptSchema,
  redactResult,
} from "../packages/collector-cli/src/machine";

type CommandResult = {
  code: number | null;
  stdout: string;
  stderr: string;
};

type Check = {
  name: string;
  passed: boolean;
  detail: unknown;
};

const root = path.resolve(import.meta.dirname, "..");
const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
const tsx = path.join(root, "node_modules", "tsx", "dist", "cli.mjs");
const installScript = path.join(root, "install.sh");
const machineDoc = path.join(root, "docs", "machine-mode.md");
const checks: Check[] = [];
/** Every receipt captured by integration checks; schema-validated at the end. */
const capturedReceipts: Array<Record<string, unknown>> = [];

function check(name: string, condition: unknown, detail: unknown) {
  checks.push({ name, passed: Boolean(condition), detail });
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function command(
  executable: string,
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv },
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, {
      cwd: options.cwd,
      env: options.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code) => resolve({ code, stdout, stderr }));
  });
}

function sandboxRoot(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-machine-proof-"));
}

function cliEnv(home: string, plimsollHome: string, overrides: NodeJS.ProcessEnv = {}) {
  return {
    ...process.env,
    HOME: home,
    PLIMSOLL_HOME: plimsollHome,
    PATH: `${path.dirname(process.execPath)}:${process.env.PATH ?? ""}`,
    COREPACK_ENABLE_DOWNLOAD_PROMPT: "0",
    ...overrides,
  };
}

function runMachine(
  innerArgv: string[],
  home: string,
  plimsollHome: string,
  overrides: NodeJS.ProcessEnv = {},
) {
  return command(
    process.execPath,
    [tsx, cli, "machine", ...innerArgv],
    { cwd: root, env: cliEnv(home, plimsollHome, overrides) },
  );
}

function assertSingleNewlineEnvelope(name: string, result: CommandResult) {
  const raw = result.stdout;
  const lastNewline = raw.lastIndexOf("\n");
  check(`${name}_stdout_nonempty`, raw.length > 0, { bytes: raw.length, stderr: result.stderr.slice(0, 400) });
  check(`${name}_exactly_one_trailing_newline`, lastNewline === raw.length - 1 && !raw.slice(0, -1).includes("\n"), {
    bytes: raw.length,
    newlineCount: raw.split("\n").length - 1,
  });
  let receipt: Record<string, unknown>;
  try {
    receipt = JSON.parse(raw.slice(0, -1)) as Record<string, unknown>;
  } catch (error) {
    throw new Error(`${name}_json_parse: ${(error as Error).message}`);
  }
  check(`${name}_envelope_version`, receipt.receiptVersion === 1 && receipt.schema === MACHINE_RECEIPT_SCHEMA_NAME, receipt);
  check(`${name}_envelope_exit_matches_process`, receipt.exitCode === result.code, {
    receiptExitCode: receipt.exitCode,
    processCode: result.code,
    signal: receipt.signal,
  });
  capturedReceipts.push(receipt);
  return receipt;
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address() as net.AddressInfo;
      server.close(() => resolve(address.port));
    });
    server.once("error", reject);
  });
}

async function probeStatus(port: number): Promise<"reachable" | "absent"> {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/status`, { signal: AbortSignal.timeout(1000) });
    if (!response.ok) return "absent";
    const body = (await response.json()) as { runtimeIdentity?: unknown };
    return body.runtimeIdentity ? "reachable" : "absent";
  } catch {
    return "absent";
  }
}

function writePortConfig(plimsollHome: string, port: number) {
  fs.mkdirSync(plimsollHome, { recursive: true });
  fs.writeFileSync(path.join(plimsollHome, "collector.config.json"), JSON.stringify({ port }));
}

async function main() {
  check("runtime_entrypoint_exists", fs.existsSync(tsx) && fs.existsSync(cli), { tsx, cli });
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  check("runtime_node_supported_range", nodeMajor >= 20 && nodeMajor < 25, { version: process.versions.node });

  // ---------------------------------------------------------------- unit: stdout evaluator
  const evaluatorFixtures: Array<[string, string, string]> = [
    ["compact_single_object_accepted", '{"ok":true}\n', "single_json_object"],
    ["pretty_multiline_single_value_accepted", '{\n  "ok": true\n}\n', "single_json_object"],
    ["banner_prefix_rejected", 'pnpm > installing...\n{"ok":true}\n', "invalid"],
    ["two_objects_rejected", '{"a":1}\n{"b":2}\n', "invalid"],
    ["missing_trailing_newline_rejected", '{"ok":true}', "invalid"],
    ["double_trailing_newline_rejected", '{"ok":true}\n\n', "invalid"],
    ["array_value_rejected", '[{"a":1}]\n', "invalid"],
    ["primitive_value_rejected", '42\n', "invalid"],
    ["empty_rejected_as_empty", '', "empty"],
  ];
  for (const [name, fixture, expected] of evaluatorFixtures) {
    check(`stdout_evaluator_${name}`, evaluateChildStdout(fixture).parse === expected, {
      fixture: JSON.stringify(fixture),
      got: evaluateChildStdout(fixture).parse,
      expected,
    });
  }

  // ---------------------------------------------------------------- unit: sanitizer hostility
  const hostileHome = "/Users/alice.secret";
  const hostileToken = "sk-live-9f8e7d6c5b4a3210";
  const sanitizer = createMachineSanitizer({
    HOME: hostileHome,
    PLIMSOLL_TOKEN: hostileToken,
  });
  const hostileInput = [
    "\u001b[31mfailed\u001b[0m",
    `token=${hostileToken}`,
    `home=${hostileHome}/.claude/settings.json`,
    `cwd=${process.cwd()}/packages`,
    "hook prompt: /Users/bob/.codex/prompts/x.md",
    "control\u0007bell",
  ].join("\n");
  const sanitizedHostile = sanitizer.text(hostileInput);
  for (const forbidden of [hostileToken, hostileHome, process.cwd(), "\u001b[31m", "\u0007", "/Users/bob"]) {
    check(`sanitizer_strips_${createHash("sha1").update(forbidden).digest("hex").slice(0, 8)}`,
      !sanitizedHostile.includes(forbidden), { snippet: sanitizedHostile.slice(0, 200) });
  }
  check("sanitizer_marks_redactions", sanitizedHostile.includes("<redacted"), { snippet: sanitizedHostile.slice(0, 200) });
  const longGarbage = "x".repeat(DIAGNOSTICS_FORWARD_LIMIT_CHARS + 5000);
  check("sanitizer_bounds_length", sanitizer.text(longGarbage).length <= DIAGNOSTICS_FORWARD_LIMIT_CHARS, {
    limit: DIAGNOSTICS_FORWARD_LIMIT_CHARS,
    got: sanitizer.text(longGarbage).length,
  });

  // ---------------------------------------------------------------- unit: deep result redaction
  const redactedResult = redactResult(
    {
      configPath: `${hostileHome}/.plimsoll/app/collector.config.json`,
      nested: { bufferPath: `${hostileHome}/work-ledger.sqlite` },
      list: [{ pidPath: `${hostileHome}/collector.pid` }],
      note: `leak attempt ${hostileHome} ${hostileToken}`,
      counts: { ok: true, n: 3 },
    },
    sanitizer,
  ) as Record<string, unknown>;
  check("redact_result_path_keys_hashed",
    /^sha256:[0-9a-f]{64}$/.test(String(redactedResult.configPath)) &&
    /^sha256:[0-9a-f]{64}$/.test((redactedResult.nested as Record<string, unknown>).bufferPath as string) &&
    /^sha256:[0-9a-f]{64}$/.test((redactedResult.list as Array<Record<string, unknown>>)[0].pidPath as string),
    redactedResult);
  const serializedRedacted = JSON.stringify(redactedResult);
  check("redact_result_no_raw_leak",
    !serializedRedacted.includes(hostileHome) && !serializedRedacted.includes(hostileToken),
    serializedRedacted);

  // ---------------------------------------------------------------- integration: doctor on a fresh machine
  const freshSandbox = sandboxRoot();
  const freshHome = path.join(freshSandbox, "home");
  const freshPlimsoll = path.join(freshSandbox, "plimsoll-home");
  const doctorRun = await runMachine(["doctor", "--read-only"], freshHome, freshPlimsoll);
  const doctorReceipt = assertSingleNewlineEnvelope("doctor_fresh_home", doctorRun);
  check("doctor_fresh_home_literal_failure",
    doctorRun.code === 1 && doctorReceipt.ok === false &&
    (doctorReceipt.result as Record<string, unknown>).readiness === "not_installed" &&
    (doctorReceipt.stdout as Record<string, unknown>).parse === "single_json_object",
    doctorReceipt);
  check("doctor_fresh_home_no_side_effects", !fs.existsSync(freshPlimsoll), freshPlimsoll);

  // ---------------------------------------------------------------- integration: setup dry-run → apply → noop
  const setupHome = path.join(freshSandbox, "setup-home");
  const setupPlimsoll = path.join(freshSandbox, "setup-plimsoll");
  const setupToolDir = path.join(freshSandbox, "tool-config");
  fs.mkdirSync(setupToolDir, { recursive: true });
  const setupClaudeFile = path.join(setupToolDir, "settings.json");
  const setupCodexFile = path.join(setupToolDir, "config.toml");
  const setupArgs = ["--claude-settings", setupClaudeFile, "--codex-config", setupCodexFile];
  const dryRunReceipt = assertSingleNewlineEnvelope("setup_dry_run",
    await runMachine(["setup", "--dry-run", ...setupArgs], setupHome, setupPlimsoll));
  check("setup_dry_run_status",
    (dryRunReceipt.result as Record<string, unknown>).status === "setup_dry_run" &&
    dryRunReceipt.exitCode === 0 &&
    !fs.existsSync(setupClaudeFile),
    dryRunReceipt);
  const applyReceipt = assertSingleNewlineEnvelope("setup_apply",
    await runMachine(["setup", "--yes", ...setupArgs], setupHome, setupPlimsoll));
  check("setup_apply_status",
    (applyReceipt.result as Record<string, unknown>).status === "setup_applied" &&
    applyReceipt.ok === true &&
    fs.existsSync(setupClaudeFile) && fs.existsSync(setupCodexFile),
    applyReceipt);
  const noopReceipt = assertSingleNewlineEnvelope("setup_noop",
    await runMachine(["setup", "--yes", ...setupArgs], setupHome, setupPlimsoll));
  check("setup_noop_status", (noopReceipt.result as Record<string, unknown>).status === "setup_noop" && noopReceipt.exitCode === 0, noopReceipt);

  // ---------------------------------------------------------------- integration: usage errors exit 64
  for (const badInvocation of [["definitely-not-a-command"], ["doctor", "--frobnicate"], [], ["status", "--load"], ["stop", "extra"]]) {
    const usageRun = await runMachine(badInvocation, freshHome, freshPlimsoll);
    const usageReceipt = assertSingleNewlineEnvelope(`usage_${badInvocation.join("_") || "empty"}`, usageRun);
    check(`usage_error_exit_64_${badInvocation.join("_") || "empty"}`,
      usageRun.code === 64 && usageReceipt.ok === false &&
      typeof usageReceipt.wrapperError === "string" && usageReceipt.wrapperError.startsWith("usage"),
      { code: usageRun.code, wrapperError: usageReceipt.wrapperError });
  }

  // ---------------------------------------------------------------- integration: failure path still one clean receipt
  const failureSandbox = sandboxRoot();
  const failureHome = path.join(failureSandbox, "home");
  const failurePlimsoll = path.join(failureSandbox, "plimsoll-home");
  const failureToolDir = path.join(failureSandbox, "tool-config");
  fs.mkdirSync(failureToolDir);
  const malformedCodex = '[otel]\nenvironment = "first"\n[otel]\nenvironment = "duplicate"\n';
  const failureCodex = path.join(failureToolDir, "config.toml");
  fs.writeFileSync(failureCodex, malformedCodex);
  const failureRun = await runMachine(
    ["setup", "--yes", "--codex-config", failureCodex],
    failureHome,
    failurePlimsoll,
  );
  const failureReceipt = assertSingleNewlineEnvelope("setup_invalid_codex", failureRun);
  check("failure_path_honest_failure_receipt",
    failureRun.code !== 0 && failureReceipt.ok === false,
    { code: failureRun.code, ok: failureReceipt.ok, stderrHead: failureRun.stderr.slice(0, 200) });
  check("failure_path_no_partial_writes",
    fs.readFileSync(failureCodex, "utf8") === malformedCodex && !fs.existsSync(failurePlimsoll),
    { codexUnchanged: true, plimsollHomeCreated: fs.existsSync(failurePlimsoll) });

  // ---------------------------------------------------------------- differential privacy: legacy vs machine doctor
  const legacyRun = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: root, env: cliEnv(setupHome, setupPlimsoll) },
  );
  check("differential_legacy_doctor_does_leak_paths",
    legacyRun.stdout.includes(setupPlimsoll),
    { leakedAsExpected: legacyRun.stdout.includes(setupPlimsoll) });
  const machineCombined = doctorRun.stdout + doctorRun.stderr;
  check("differential_machine_output_scrubbed",
    !machineCombined.includes(setupPlimsoll) && !machineCombined.includes(setupHome) && !machineCombined.includes(os.homedir()),
    { scrubbed: true });

  // ---------------------------------------------------------------- integration: status and stop when stopped
  const statusReceipt = assertSingleNewlineEnvelope("status_fresh_home",
    await runMachine(["status"], setupHome, setupPlimsoll));
  check("status_ok_envelope", statusReceipt.ok === true, statusReceipt);
  // Isolated port: the host's real default-port collector must never be touched.
  const idlePort = await freePort();
  const idleSandbox = sandboxRoot();
  const idlePlimsoll = path.join(idleSandbox, "plimsoll-home");
  writePortConfig(idlePlimsoll, idlePort);
  const stopReceipt = assertSingleNewlineEnvelope("stop_when_stopped",
    await runMachine(["stop"], path.join(idleSandbox, "home"), idlePlimsoll));
  check("stop_when_stopped_reports_already_stopped",
    (stopReceipt.result as Record<string, unknown>).stopped === true && stopReceipt.exitCode === 0,
    stopReceipt);

  // ---------------------------------------------------------------- integration: real start lifecycle
  const livePort = await freePort();
  const liveSandbox = sandboxRoot();
  const liveHome = path.join(liveSandbox, "home");
  const livePlimsoll = path.join(liveSandbox, "plimsoll-home");
  writePortConfig(livePlimsoll, livePort);
  const startReceipt = assertSingleNewlineEnvelope("start_live",
    await runMachine(["start"], liveHome, livePlimsoll));
  const startResult = startReceipt.result as Record<string, unknown>;
  check("start_live_postcondition", startReceipt.ok === true && startResult.started === true && startResult.listenerState === "collector",
    startReceipt);
  check("start_live_listener_probe_independent", await probeStatus(livePort) === "reachable", { port: livePort });
  const againReceipt = assertSingleNewlineEnvelope("start_already_running",
    await runMachine(["start"], liveHome, livePlimsoll));
  check("start_already_running_truthful",
    (againReceipt.result as Record<string, unknown>).alreadyRunning === true && againReceipt.exitCode === 0,
    againReceipt);
  const stopLiveReceipt = assertSingleNewlineEnvelope("stop_live",
    await runMachine(["stop"], liveHome, livePlimsoll));
  check("stop_live_postcondition",
    (stopLiveReceipt.result as Record<string, unknown>).stopped === true && stopLiveReceipt.exitCode === 0,
    stopLiveReceipt);
  check("stop_live_listener_gone", await probeStatus(livePort) === "absent", { port: livePort });

  // ---------------------------------------------------------------- adversarial: foreign listener refused
  const foreignPort = await freePort();
  const foreignServer = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end("{}");
  });
  await new Promise<void>((resolve) => foreignServer.listen(foreignPort, "127.0.0.1", resolve));
  const foreignSandbox = sandboxRoot();
  const foreignPlimsoll = path.join(foreignSandbox, "plimsoll-home");
  writePortConfig(foreignPlimsoll, foreignPort);
  const foreignStart = await runMachine(["start"], path.join(foreignSandbox, "home"), foreignPlimsoll);
  const foreignReceipt = assertSingleNewlineEnvelope("start_foreign_port", foreignStart);
  check("start_foreign_port_refused_honestly",
    foreignStart.code !== 0 && foreignReceipt.ok === false &&
    (foreignReceipt.result as Record<string, unknown>).reason === "port_not_clear_before_start",
    foreignReceipt);
  foreignServer.close();

  // ---------------------------------------------------------------- adversarial: corrupt config refused before spawn
  const corruptSandbox = sandboxRoot();
  const corruptPlimsoll = path.join(corruptSandbox, "plimsoll-home");
  fs.mkdirSync(corruptPlimsoll, { recursive: true });
  fs.writeFileSync(path.join(corruptPlimsoll, "collector.config.json"), "{not json");
  const corruptRun = await runMachine(["start"], path.join(corruptSandbox, "home"), corruptPlimsoll);
  const corruptReceipt = assertSingleNewlineEnvelope("start_corrupt_config", corruptRun);
  check("start_corrupt_config_refused_without_port_guess",
    corruptRun.code === 1 && corruptReceipt.ok === false &&
    (corruptReceipt.result as Record<string, unknown>).reason === "collector_config_invalid",
    corruptReceipt);

  // ---------------------------------------------------------------- adversarial: bounded deadline honored
  const deadlinePort = await freePort();
  const deadlineSandbox = sandboxRoot();
  const deadlinePlimsoll = path.join(deadlineSandbox, "plimsoll-home");
  // Valid config, but the daemon is forced to die before binding (raw evidence
  // mode is unavailable and rejected at startup), so the supervisor must report
  // an honest bounded failure instead of hanging or guessing success.
  writePortConfig(deadlinePlimsoll, deadlinePort);
  const deadlineRun = await runMachine(["start"], path.join(deadlineSandbox, "home"), deadlinePlimsoll, {
    PLIMSOLL_MACHINE_START_TIMEOUT_MS: "1000",
    PLIMSOLL_EVIDENCE_MODE: "raw",
  });
  const deadlineReceipt = assertSingleNewlineEnvelope("start_deadline", deadlineRun);
  check("start_deadline_honest_bounded_failure",
    deadlineRun.code !== 0 && deadlineReceipt.ok === false &&
    String((deadlineReceipt.result as Record<string, unknown>).reason) === "start_timeout",
    deadlineReceipt);
  check("start_deadline_respected",
    Number((deadlineReceipt.result as Record<string, unknown>).elapsedMs) < 5000,
    deadlineReceipt);

  // ---------------------------------------------------------------- pnpm framing demonstration (why direct invocation)
  let pnpmFramingDetail: unknown;
  let pnpmFramingProven = false;
  try {
    const framingRun = await command(
      "pnpm",
      ["collector", "machine", "doctor", "--read-only"],
      { cwd: root, env: cliEnv(setupHome, setupPlimsoll) },
    );
    const lines = framingRun.stdout.split("\n").filter((line) => line.length > 0);
    // Package-manager lifecycle framing surrounds the single JSON value on
    // stdout when the CLI is reached through `pnpm collector`; this is exactly
    // why the installer invokes the runtime directly instead.
    pnpmFramingProven = lines.length > 1 || framingRun.stdout.includes("> ");
    pnpmFramingDetail = { lineCount: lines.length, head: lines.slice(0, 4) };
  } catch (error) {
    pnpmFramingDetail = { error: (error as Error).message };
  }
  check("pnpm_framing_justifies_direct_invocation", pnpmFramingProven, pnpmFramingDetail);

  // ---------------------------------------------------------------- installer contract
  const installText = fs.readFileSync(installScript, "utf8");
  check("installer_never_uses_pnpm_collector", !/\bpnpm\s+collector\b/.test(installText), {});
  check("installer_invokes_runtime_directly",
    installText.includes('node_modules/tsx/dist/cli.mjs') &&
    installText.includes('"$RUNTIME_NODE" "$RUNTIME_TSX" "$RUNTIME_CLI" machine'),
    {});
  check("installer_validates_envelope_strictly",
    installText.includes("receiptVersion !== 1") && installText.includes("typeof receipt.ok !== \"boolean\""),
    {});
  const dryRunHome = sandboxRoot();
  const installDryRun = await command("bash", [installScript, "--dry-run"], {
    cwd: root,
    env: cliEnv(path.join(dryRunHome, "home"), path.join(dryRunHome, "ph")),
  });
  check("installer_dry_run_announces_machine_steps",
    installDryRun.code === 0 &&
    installDryRun.stdout.includes("machine setup --yes") &&
    installDryRun.stdout.includes("machine doctor --read-only") &&
    installDryRun.stdout.includes("never via pnpm"),
    { code: installDryRun.code, stdout: installDryRun.stdout });

  check("machine_mode_documented_with_exit_contract",
    fs.existsSync(machineDoc) &&
    ["| `64` |", "| `65` |", "| `70` |", "| `71` |", "| `128+N` |", "one trailing", "receiptVersion"].every((needle) =>
      fs.readFileSync(machineDoc, "utf8").includes(needle)),
    machineDoc);

  check("supported_commands_covered", MACHINE_SUPPORTED_COMMANDS.length === 8, MACHINE_SUPPORTED_COMMANDS);

  // ---------------------------------------------------------------- every captured receipt validates against product schema
  for (const [index, receipt] of capturedReceipts.entries()) {
    const parsed = machineReceiptSchema.safeParse(receipt);
    check(`captured_receipt_${index}_schema_valid`, parsed.success,
      parsed.success ? {} : parsed.error.issues.slice(0, 3));
  }

  const failed = checks.filter((entry) => !entry.passed);
  console.log(JSON.stringify({
    proof: "machine-receipt",
    checks: checks.length,
    passed: checks.filter((entry) => entry.passed).length,
    failed: failed.map((entry) => entry.name),
  }));
  if (failed.length > 0) process.exitCode = 1;
}

main()
  .catch((error) => {
    console.error(error);
    console.log(JSON.stringify({
      proof: "machine-receipt",
      checks: checks.length,
      passed: checks.filter((entry) => entry.passed).length,
      failed: [...checks.filter((entry) => !entry.passed).map((entry) => entry.name), error instanceof Error ? error.message : String(error)],
    }));
    process.exitCode = 1;
  });
