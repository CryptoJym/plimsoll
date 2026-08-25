import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import os from "node:os";
import { z } from "zod";

import { DEFAULT_COLLECTOR_PORT, readCollectorConfig } from "./config";
import { observeCollectorListener } from "./runtime-ownership";

/**
 * Machine mode (issue #133): one explicit automation surface over the operator
 * commands. The wrapper re-runs the SAME runtime (process.execPath +
 * process.execArgv + entry script) as a child, captures its streams, and emits
 * exactly one schema-validated JSON receipt followed by exactly one newline on
 * its own stdout. Nothing else ever reaches machine-mode stdout: not banners,
 * progress, warnings, or package-manager framing. Content that cannot enter the
 * receipt goes to stderr, bounded and scrubbed of paths, secret-shaped env
 * values, ANSI escapes, and control characters.
 */

export const MACHINE_RECEIPT_VERSION = 1;
export const MACHINE_RECEIPT_SCHEMA_NAME = "plimsoll.machine.receipt";

export const MACHINE_SUPPORTED_COMMANDS = [
  "setup",
  "doctor",
  "install-launch-agent",
  "load-launch-agent",
  "unload-launch-agent",
  "start",
  "status",
  "stop",
] as const;

export type MachineCommand = (typeof MACHINE_SUPPORTED_COMMANDS)[number];

/** Documented exit-code contract (docs/machine-mode.md). */
export const MACHINE_EXIT_CODES = {
  /** Receipt ok:true. */
  OK: 0,
  /** The inner command ran and reported failure; the child's exit code passes through (usually 1). */
  COMMAND_FAILED: 1,
  /** Invalid machine invocation (unsupported command, unknown flag, bad bounds). */
  USAGE: 64,
  /** The child runtime could not be spawned at all. */
  SPAWN_FAILED: 65,
  /** The child exited 0 but did not produce exactly one JSON object on stdout: a protocol breach. */
  PROTOCOL_VIOLATION: 70,
  /** The wrapper could not emit a schema-valid receipt (fallback receipt is emitted instead). */
  RECEIPT_EMISSION_FAILED: 71,
  /** The child was killed by signal N; reported as 128+N. */
  SIGNAL_BASE: 128,
} as const;

const SIGNAL_NUMBERS: Record<string, number> = {
  SIGHUP: 1,
  SIGINT: 2,
  SIGQUIT: 3,
  SIGILL: 4,
  SIGTRAP: 5,
  SIGABRT: 6,
  SIGEMT: 7,
  SIGFPE: 8,
  SIGKILL: 9,
  SIGBUS: 10,
  SIGSEGV: 11,
  SIGSYS: 12,
  SIGPIPE: 13,
  SIGALRM: 14,
  SIGTERM: 15,
};

const ALLOWED_FLAGS: Record<MachineCommand, readonly string[]> = {
  setup: ["--yes", "--dry-run", "--claude-settings", "--codex-config"],
  doctor: ["--read-only", "--json"],
  "install-launch-agent": ["--dev", "--repo-root", "--pnpm", "--load", "--dry-run"],
  "load-launch-agent": [],
  "unload-launch-agent": [],
  start: [],
  status: [],
  stop: [],
};

const VALUE_FLAGS = new Set([
  "--claude-settings",
  "--codex-config",
  "--repo-root",
  "--pnpm",
]);

const MAX_INNER_ARGUMENTS = 24;
const MAX_ARGUMENT_LENGTH = 512;
const MAX_TOTAL_ARGUMENT_CHARS = 4096;
const MAX_CAPTURE_BYTES = 4_000_000;
export const DIAGNOSTICS_FORWARD_LIMIT_CHARS = 6000;
const RESULT_STRING_CHAR_LIMIT = 2000;
const RESULT_DEPTH_LIMIT = 12;

const START_DEFAULT_DEADLINE_MS = 20_000;
const START_POLL_INTERVAL_MS = 250;

export const machineReceiptSchema = z.object({
  receiptVersion: z.literal(MACHINE_RECEIPT_VERSION),
  schema: z.literal(MACHINE_RECEIPT_SCHEMA_NAME),
  command: z.string().min(1),
  argv: z.array(z.string()).max(MAX_INNER_ARGUMENTS),
  ok: z.boolean(),
  exitCode: z.number().int().min(0).max(255),
  signal: z.string().regex(/^SIG[A-Z0-9]+$/).nullable(),
  result: z.union([z.record(z.string(), z.unknown()), z.null()]),
  stdout: z.object({
    parse: z.enum(["single_json_object", "empty", "invalid", "not_captured"]),
    bytes: z.number().int().min(0),
    sha256: z.string().length(64).nullable(),
  }),
  stderr: z.object({
    bytes: z.number().int().min(0),
    sha256: z.string().length(64).nullable(),
    forwardedSanitizedChars: z.number().int().min(0),
    truncated: z.boolean(),
  }),
  wrapperError: z.string().max(120).nullable(),
});

export type MachineReceipt = z.infer<typeof machineReceiptSchema>;

export type ChildStdoutEvaluation =
  | { parse: "empty"; value: null }
  | { parse: "single_json_object"; value: Record<string, unknown> }
  | { parse: "invalid"; value: null };

/**
 * Strict stdout contract evaluator: the child must have written exactly one
 * JSON object terminated by exactly one trailing newline. Pretty-printed JSON
 * is multi-line but still one value and passes; banners, a second value, a
 * missing or doubled trailing newline, arrays, primitives, and emptiness on a
 * zero exit are all rejected.
 */
export function evaluateChildStdout(raw: string): ChildStdoutEvaluation {
  if (raw.length === 0) return { parse: "empty", value: null };
  if (!raw.endsWith("\n")) return { parse: "invalid", value: null };
  const body = raw.slice(0, -1);
  if (body.endsWith("\n")) return { parse: "invalid", value: null };
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return { parse: "invalid", value: null };
  }
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    Array.isArray(parsed)
  ) {
    return { parse: "invalid", value: null };
  }
  return { parse: "single_json_object", value: parsed as Record<string, unknown> };
}

export interface MachineSanitizer {
  /** Scrub one diagnostic string (paths, secret-shaped values, ANSI, controls, length cap). */
  text(value: string, charLimit?: number): string;
}

/**
 * Builds a sanitizer from the environment the CHILD will inherit: absolute
 * bases (home, cwd, tmpdir, PLIMSOLL_HOME) and the values of secret-shaped env
 * vars become "<redacted>"; generic POSIX user-path prefixes are stripped; ANSI
 * escapes and C0/C1 controls (except \n and \t) are removed; output is capped.
 */
export function createMachineSanitizer(env: NodeJS.ProcessEnv): MachineSanitizer {
  const needles = new Set<string>();
  const addNeedle = (value: string | undefined) => {
    if (value && value.length >= 4) needles.add(value);
  };
  for (const base of [
    env.HOME,
    env.PLIMSOLL_HOME,
    os.homedir(),
    process.cwd(),
    os.tmpdir(),
  ]) {
    addNeedle(base);
  }
  for (const [name, value] of Object.entries(env)) {
    if (
      value &&
      value.length >= 6 &&
      /TOKEN|SECRET|PASSWORD|PASSPHRASE|CREDENTIAL|API_?KEY|INGEST/i.test(name)
    ) {
      needles.add(value);
    }
  }
  const ordered = [...needles].sort((a, b) => b.length - a.length);

  const stripAnsiAndControls = (value: string) =>
    value
      .replace(/\x1b\[[0-9;?]*[ -/]*[@-~]/g, "")
      .replace(/\x1b[@-_]/g, "")
      // eslint-disable-next-line no-control-regex
      .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/g, "");

  const scrub = (value: string): string => {
    let scrubbed = stripAnsiAndControls(value);
    for (const needle of ordered) {
      if (scrubbed.includes(needle)) {
        scrubbed = scrubbed.split(needle).join("<redacted>");
      }
    }
    scrubbed = scrubbed
      .replace(/\/Users\/[^/\s"'`:]{1,64}/g, "<redacted-user-path>")
      .replace(/\/home\/[^/\s"'`:]{1,64}/g, "<redacted-user-path>");
    return scrubbed;
  };

  return {
    text(value, charLimit = DIAGNOSTICS_FORWARD_LIMIT_CHARS) {
      const scrubbed = scrub(String(value));
      if (scrubbed.length <= charLimit) return scrubbed;
      return `${scrubbed.slice(0, Math.max(0, charLimit - 14))}\n<truncated>`;
    },
  };
}

const RECEIPT_PATH_KEYS = new Set([
  "path",
  "paths",
  "configPath",
  "bufferPath",
  "pidPath",
  "plistPath",
  "homePath",
  "repoRoot",
  "claudeFile",
  "codexFile",
  "claudeSettings",
  "codexConfig",
]);

/**
 * Deep-redaction of an embedded child result: values under path-bearing keys
 * become deterministic sha256 digests (never reversible, never leaking the
 * literal), every other string passes the sanitizer, and strings are capped.
 */
export function redactResult(
  value: unknown,
  sanitizer: MachineSanitizer,
  key: string | null = null,
  depth = 0,
): unknown {
  if (depth > RESULT_DEPTH_LIMIT) return "<depth-limit>";
  if (typeof value === "string") {
    if (key !== null && RECEIPT_PATH_KEYS.has(key)) {
      return `sha256:${createHash("sha256").update(value).digest("hex")}`;
    }
    return sanitizer.text(value, RESULT_STRING_CHAR_LIMIT);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => redactResult(entry, sanitizer, key, depth + 1));
  }
  if (value !== null && typeof value === "object") {
    const redacted: Record<string, unknown> = {};
    for (const [entryKey, entryValue] of Object.entries(value)) {
      redacted[entryKey] = redactResult(entryValue, sanitizer, entryKey, depth + 1);
    }
    return redacted;
  }
  return value;
}

interface InvocationParse {
  readonly command: MachineCommand | null;
  readonly argv: readonly string[];
  readonly error: string | null;
}

function safeToken(value: string): string | null {
  return /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(value) ? value : null;
}

export function parseMachineInvocation(innerArgv: readonly string[]): InvocationParse {
  if (innerArgv.length > MAX_INNER_ARGUMENTS) {
    return { command: null, argv: [], error: "usage_argument_bounds" };
  }
  if (innerArgv.join(" ").length > MAX_TOTAL_ARGUMENT_CHARS) {
    return { command: null, argv: [], error: "usage_argument_bounds" };
  }
  const requested = innerArgv[0];
  if (
    requested === undefined ||
    !(MACHINE_SUPPORTED_COMMANDS as readonly string[]).includes(requested)
  ) {
    return { command: null, argv: [], error: "usage_unsupported_command" };
  }
  if ([...innerArgv].some((argument) => argument.length > MAX_ARGUMENT_LENGTH)) {
    return { command: null, argv: [], error: "usage_argument_bounds" };
  }
  const command = requested as MachineCommand;
  const allowed = new Set(ALLOWED_FLAGS[command]);
  const argv: string[] = [];
  for (let index = 1; index < innerArgv.length; index += 1) {
    const argument = innerArgv[index];
    if (argument === "--") {
      return { command, argv, error: "usage_unsupported_separator" };
    }
    if (argument.startsWith("-")) {
      if (!allowed.has(argument)) {
        const safe = safeToken(argument.replace(/^--+/, ""));
        return { command, argv, error: safe ? `usage_unknown_flag:${safe}` : "usage_unknown_flag" };
      }
      if (VALUE_FLAGS.has(argument)) {
        const value = innerArgv[index + 1];
        if (value === undefined || value.startsWith("-")) {
          return { command, argv, error: `usage_missing_value:${argument}` };
        }
        argv.push(argument, value);
        index += 1;
        continue;
      }
      argv.push(argument);
      continue;
    }
    return { command, argv, error: "usage_unexpected_positional" };
  }
  return { command, argv, error: null };
}

function sha256Of(text: string): string | null {
  return text.length === 0 ? null : createHash("sha256").update(text).digest("hex");
}

export interface MachineModeOptions {
  readonly execPath: string;
  readonly execArgv: readonly string[];
  readonly entryScript: string;
  readonly innerArgv: readonly string[];
  readonly env: NodeJS.ProcessEnv;
  /** Writes the final receipt text (without newline) to the real stdout. Defaults to process.stdout.write. */
  readonly emit?: (line: string) => void;
  /** Writes sanitized diagnostics to the real stderr. Defaults to process.stderr.write. */
  readonly writeDiagnostics?: (text: string) => void;
  /** Millisecond budget for `machine start` postcondition polling (tests inject smaller budgets). */
  readonly startDeadlineMsOverride?: number;
}

interface StreamCapture {
  text: string;
  bytes: number;
  truncated: boolean;
}

function captureStream(stream: NodeJS.ReadableStream | null): StreamCapture {
  const capture: StreamCapture = { text: "", bytes: 0, truncated: false };
  if (!stream) return capture;
  stream.setEncoding("utf8");
  stream.on("data", (chunk: string) => {
    const incoming = Buffer.byteLength(chunk, "utf8");
    capture.bytes += incoming;
    if (capture.bytes > MAX_CAPTURE_BYTES) {
      capture.truncated = true;
      return;
    }
    capture.text += chunk;
  });
  return capture;
}

function fallbackReceipt(command: string, detail: string): MachineReceipt {
  return {
    receiptVersion: MACHINE_RECEIPT_VERSION,
    schema: MACHINE_RECEIPT_SCHEMA_NAME,
    command,
    argv: [],
    ok: false,
    exitCode: MACHINE_EXIT_CODES.RECEIPT_EMISSION_FAILED,
    signal: null,
    result: null,
    stdout: { parse: "not_captured", bytes: 0, sha256: null },
    stderr: { bytes: 0, sha256: null, forwardedSanitizedChars: 0, truncated: false },
    wrapperError: detail,
  };
}

function emitReceipt(
  receipt: MachineReceipt,
  options: Pick<MachineModeOptions, "emit" | "writeDiagnostics">,
): number {
  let validated: MachineReceipt;
  const parsed = machineReceiptSchema.safeParse(receipt);
  if (parsed.success) {
    validated = parsed.data;
  } else {
    options.writeDiagnostics?.(
      `plimsoll machine: receipt failed schema validation (${parsed.error.issues.length} issues)\n`,
    );
    validated = fallbackReceipt(receipt.command, "receipt_schema_violation");
  }
  options.emit?.(`${JSON.stringify(validated)}\n`);
  return validated.exitCode;
}

function childExitInformation(
  code: number | null,
  signal: string | null,
): { exitCode: number; signal: string | null } {
  if (signal !== null) {
    const signum = SIGNAL_NUMBERS[signal];
    return { exitCode: MACHINE_EXIT_CODES.SIGNAL_BASE + (signum ?? 0), signal };
  }
  return { exitCode: code ?? 1, signal: null };
}

async function runWrappedCommand(
  invocation: InvocationParse,
  options: MachineModeOptions,
  sanitizer: MachineSanitizer,
): Promise<number> {
  const commandName = invocation.command as MachineCommand;
  const child = spawn(
    options.execPath,
    [...options.execArgv, options.entryScript, commandName, ...invocation.argv],
    {
      cwd: process.cwd(),
      env: { ...options.env, PLIMSOLL_MACHINE_CHILD: "1" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const stdoutCapture = captureStream(child.stdout);
  const stderrCapture = captureStream(child.stderr);
  const spawnFailure = await new Promise<Error | null>((resolve) => {
    child.once("error", (error) => resolve(error));
    child.once("close", () => resolve(null));
  });
  if (spawnFailure) {
    return emitReceipt(
      {
        receiptVersion: MACHINE_RECEIPT_VERSION,
        schema: MACHINE_RECEIPT_SCHEMA_NAME,
        command: commandName,
        argv: [...invocation.argv],
        ok: false,
        exitCode: MACHINE_EXIT_CODES.SPAWN_FAILED,
        signal: null,
        result: null,
        stdout: { parse: "not_captured", bytes: 0, sha256: null },
        stderr: { bytes: 0, sha256: null, forwardedSanitizedChars: 0, truncated: false },
        wrapperError: "spawn_failed",
      },
      options,
    );
  }

  const evaluation = evaluateChildStdout(stdoutCapture.text);
  const { exitCode, signal } = childExitInformation(child.exitCode, child.signalCode ?? null);
  let wrapperError: string | null = null;
  let finalExitCode = exitCode;
  if (exitCode === 0 && evaluation.parse !== "single_json_object") {
    // A zero-exit child without exactly one JSON object breached the machine
    // protocol; automation must see the breach, not trust the exit code.
    wrapperError = "stdout_not_single_json_object";
    finalExitCode = MACHINE_EXIT_CODES.PROTOCOL_VIOLATION;
  }

  const sanitizedDiagnostics = sanitizer.text(stderrCapture.text);
  if (sanitizedDiagnostics.length > 0) {
    options.writeDiagnostics?.(`${sanitizedDiagnostics}\n`);
  }

  return emitReceipt(
    {
      receiptVersion: MACHINE_RECEIPT_VERSION,
      schema: MACHINE_RECEIPT_SCHEMA_NAME,
      command: commandName,
      argv: [...invocation.argv],
      ok: finalExitCode === 0 && wrapperError === null,
      exitCode: finalExitCode,
      signal,
      result: evaluation.value
        ? (redactResult(evaluation.value, sanitizer) as Record<string, unknown>)
        : null,
      stdout: {
        parse: evaluation.parse,
        bytes: stdoutCapture.bytes,
        sha256: sha256Of(stdoutCapture.text),
      },
      stderr: {
        bytes: stderrCapture.bytes,
        sha256: sha256Of(stderrCapture.text),
        forwardedSanitizedChars: sanitizedDiagnostics.length,
        truncated: stderrCapture.truncated ||
          stderrCapture.text.length > sanitizedDiagnostics.length &&
          sanitizedDiagnostics.includes("<truncated>"),
      },
      wrapperError,
    },
    options,
  );
}

function startDeadlineMs(options: MachineModeOptions): number {
  if (options.startDeadlineMsOverride !== undefined) {
    return Math.min(Math.max(options.startDeadlineMsOverride, 100), 120_000);
  }
  const raw = Number(options.env.PLIMSOLL_MACHINE_START_TIMEOUT_MS);
  if (Number.isFinite(raw) && raw > 0) {
    return Math.min(Math.max(Math.trunc(raw), 1000), 120_000);
  }
  return START_DEFAULT_DEADLINE_MS;
}

async function observedPort(port: number) {
  return observeCollectorListener(port);
}

/**
 * `machine start`: the foreground daemon cannot own machine-mode stdout forever,
 * so the wrapper launches the SAME runtime detached (the daemon keeps running
 * after the wrapper exits) and reports the observable postcondition — a live
 * collector listener on the configured port — within a bounded deadline.
 * Human `start` semantics are untouched.
 */
async function runStartSupervisor(
  options: MachineModeOptions,
  sanitizer: MachineSanitizer,
): Promise<number> {
  const baseReceipt: Pick<MachineReceipt, "receiptVersion" | "schema" | "command" | "argv"> = {
    receiptVersion: MACHINE_RECEIPT_VERSION,
    schema: MACHINE_RECEIPT_SCHEMA_NAME,
    command: "start",
    argv: [],
  };
  const notCapturedStreams = {
    stdout: { parse: "not_captured" as const, bytes: 0, sha256: null },
    stderr: { bytes: 0, sha256: null, forwardedSanitizedChars: 0, truncated: false },
  };

  // An unreadable config means the intended port is unknown. Probing the
  // default port instead could mistake an unrelated collector for success, so
  // refuse before spawning and report the literal state.
  const startedAtMs = Date.now();
  const configRead = readCollectorConfig();
  if (configRead.status === "invalid") {
    return emitReceipt(
      {
        ...baseReceipt,
        ok: false,
        exitCode: MACHINE_EXIT_CODES.COMMAND_FAILED,
        signal: null,
        result: {
          started: false,
          alreadyRunning: false,
          reason: "collector_config_invalid",
          elapsedMs: Date.now() - startedAtMs,
        },
        ...notCapturedStreams,
        wrapperError: null,
      },
      options,
    );
  }
  const port = configRead.config?.port ?? DEFAULT_COLLECTOR_PORT;

  const precondition = await observedPort(port);
  if (precondition.kind === "collector") {
    return emitReceipt(
      {
        ...baseReceipt,
        ok: true,
        exitCode: MACHINE_EXIT_CODES.OK,
        signal: null,
        result: { started: false, alreadyRunning: true, port, listenerState: "collector", elapsedMs: Date.now() - startedAtMs },
        ...notCapturedStreams,
        wrapperError: null,
      },
      options,
    );
  }
  if (precondition.kind !== "absent") {
    return emitReceipt(
      {
        ...baseReceipt,
        ok: false,
        exitCode: MACHINE_EXIT_CODES.COMMAND_FAILED,
        signal: null,
        result: {
          started: false,
          alreadyRunning: false,
          port,
          listenerState: precondition.kind,
          reason: "port_not_clear_before_start",
          elapsedMs: Date.now() - startedAtMs,
        },
        ...notCapturedStreams,
        wrapperError: null,
      },
      options,
    );
  }

  const child = spawn(
    options.execPath,
    [...options.execArgv, options.entryScript, "start"],
    {
      cwd: process.cwd(),
      env: { ...options.env, PLIMSOLL_MACHINE_CHILD: "1" },
      stdio: "ignore",
      detached: true,
    },
  );
  child.unref();

  const deadlineAt = Date.now() + startDeadlineMs(options);
  while (Date.now() < deadlineAt) {
    await new Promise<void>((resolve) => setTimeout(resolve, START_POLL_INTERVAL_MS));
    const observation = await observedPort(port);
    if (observation.kind === "collector") {
      return emitReceipt(
        {
          ...baseReceipt,
          ok: true,
          exitCode: MACHINE_EXIT_CODES.OK,
          signal: null,
          result: {
            started: true,
            alreadyRunning: false,
            port,
            listenerState: "collector",
            elapsedMs: Date.now() - startedAtMs,
          },
          ...notCapturedStreams,
          wrapperError: null,
        },
        options,
      );
    }
    if (child.exitCode !== null || child.signalCode !== null) {
      // The daemon died on its own; report the literal early exit.
      break;
    }
  }

  const observation = await observedPort(port);
  const timedOut = observation.kind !== "collector";
  if (timedOut && child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
    try {
      process.kill(child.pid, "SIGTERM");
    } catch {
      // The daemon already exited; nothing to clean up here.
    }
  }
  return emitReceipt(
    {
      ...baseReceipt,
      ok: false,
      exitCode: MACHINE_EXIT_CODES.COMMAND_FAILED,
      signal: null,
      result: {
        started: false,
        alreadyRunning: false,
        port,
        listenerState: observation.kind,
        reason: timedOut ? "start_timeout" : "daemon_exited_before_ready",
        daemonExitCode: child.exitCode,
        daemonSignal: child.signalCode,
        elapsedMs: Date.now() - startedAtMs,
      },
      ...notCapturedStreams,
      wrapperError: null,
    },
    options,
  );
}

/**
 * Entry point wired into cli.ts for `plimsoll machine …`.
 * Returns the process exit code for the machine invocation.
 */
export async function runMachineMode(options: MachineModeOptions): Promise<number> {
  const sanitizer = createMachineSanitizer(options.env);
  const emit = options.emit ?? ((line: string) => {
    process.stdout.write(line);
  });
  const writeDiagnostics = options.writeDiagnostics ?? ((text: string) => {
    process.stderr.write(text);
  });
  const wrappedOptions: MachineModeOptions = { ...options, emit, writeDiagnostics };

  const invocation = parseMachineInvocation(options.innerArgv);
  if (invocation.error !== null || invocation.command === null) {
    return emitReceipt(
      {
        receiptVersion: MACHINE_RECEIPT_VERSION,
        schema: MACHINE_RECEIPT_SCHEMA_NAME,
        command: invocation.command ?? "none",
        argv: [...invocation.argv],
        ok: false,
        exitCode: MACHINE_EXIT_CODES.USAGE,
        signal: null,
        result: null,
        stdout: { parse: "not_captured", bytes: 0, sha256: null },
        stderr: { bytes: 0, sha256: null, forwardedSanitizedChars: 0, truncated: false },
        wrapperError: invocation.error ?? "usage_unsupported_command",
      },
      wrappedOptions,
    );
  }
  if (invocation.command === "start") {
    return runStartSupervisor(wrappedOptions, sanitizer);
  }
  return runWrappedCommand(invocation, wrappedOptions, sanitizer);
}
