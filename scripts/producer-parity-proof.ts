/**
 * Bead eco-6hoxj.29: producer-to-ledger parity instrumentation.
 *
 * Proves the generated curl hooks fail on HTTP errors and retry inside the
 * hook timeout, that a producer-minted event id survives a 750 ms+ writer
 * hold, and that the read-only parity report joins those ids to the ledger.
 * Circuit-open timestamps are covered by proof:maintenance-boundary.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { useFixtureRoot } from "./lib/fixture-root";
import {
  HOOK_RETRY_CONTRACT,
  applyCodexConfig,
  applyCodexConfigOrHookCommands,
  applyCodexHookCommandsOnly,
  generateClaudeCodeSettings,
  generateCodexConfigToml,
  generateGrokHookSettings,
  generateHookForwardCommand,
  hookCommandHasRetryContract,
} from "../packages/collector-config/src/index";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  buildProducerParityReport,
  producerParityLogPath,
  readProducerObservations,
} from "../packages/collector-cli/src/producer-parity";

type Check = { name: string; passed: true; detail?: unknown };
const checks: Check[] = [];
function check(name: string, passed: unknown, detail?: unknown) {
  assert.ok(passed, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

const require = createRequire(path.resolve("package.json"));
const Database = require("better-sqlite3");

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function holdWriteLock(ledgerPath: string) {
  const lock = new Database(ledgerPath, { timeout: 0 });
  lock.pragma("journal_mode = WAL");
  lock.exec("begin immediate");
  lock.prepare(
    `insert or ignore into buffered_events
       (id, source, event_type, data_mode, observed_at, payload_json, created_at)
     values ('producer-parity-lock-holder', 'codex', 'assistant_response', 'metadata', ?, '{}', ?)`,
  ).run("2099-01-01T00:00:00.000Z", "2099-01-01T00:00:00.000Z");
  let released = false;
  return {
    release() {
      if (released) return;
      released = true;
      try {
        lock.exec("rollback");
      } catch {
        /* already unwound */
      }
      lock.close();
    },
  };
}

function runShell(command: string, input: string, env: NodeJS.ProcessEnv) {
  return new Promise<{ status: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn("/bin/sh", ["-c", command], { env, stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (status) => resolve({ status, stdout, stderr }));
    child.stdin.end(input);
  });
}

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-producer-parity-"));
  const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "must-remain-absent-operator-home") });
  try {
    check(
      "retry_contract_fits_inside_hook_timeout",
      HOOK_RETRY_CONTRACT.curlRetryMaxTimeSeconds <= HOOK_RETRY_CONTRACT.timeoutSeconds &&
        HOOK_RETRY_CONTRACT.curlMaxTimeSeconds < HOOK_RETRY_CONTRACT.curlRetryMaxTimeSeconds,
      HOOK_RETRY_CONTRACT,
    );

    const headerFile = path.join(sandbox, "codex", "plimsoll.headers");
    fs.mkdirSync(path.dirname(headerFile), { recursive: true, mode: 0o700 });
    const command = generateHookForwardCommand({
      repoRoot: "/synthetic/plimsoll",
      port: 48271,
      dataMode: "metadata",
      codexHeaderFile: headerFile,
    }, "codex");
    check(
      "codex_curl_fails_on_http_errors_and_retries_without_or_true",
      hookCommandHasRetryContract(command) &&
        command.includes("x-plimsoll-event-id:$id") &&
        command.includes("uuidgen") &&
        command.includes("producer-parity/hooks.jsonl") &&
        !command.includes("|| true"),
      command,
    );

    const grokHeader = path.join(sandbox, "grok", "plimsoll.headers");
    fs.mkdirSync(path.dirname(grokHeader), { recursive: true, mode: 0o700 });
    const grok = generateGrokHookSettings({
      repoRoot: "/synthetic/plimsoll",
      port: 48271,
      dataMode: "metadata",
      grokProducerToken: "a".repeat(43),
      grokHeaderFile: grokHeader,
    });
    check(
      "grok_command_hooks_share_the_retry_contract",
      Object.values(grok.hooks).flat().every((group) =>
        group.hooks.every((hook) =>
          hook.timeout === HOOK_RETRY_CONTRACT.timeoutSeconds &&
          hookCommandHasRetryContract(hook.command)
        )
      ),
    );

    const claude = generateClaudeCodeSettings({
      repoRoot: "/synthetic/plimsoll",
      port: 48271,
      dataMode: "metadata",
    });
    const claudeHooks = Object.values(claude.hooks).flatMap((groups) =>
      groups.flatMap((group) => group.hooks)
    );
    check(
      "claude_http_hooks_carry_equivalent_timeout_and_retry",
      claudeHooks.length === 3 &&
        claudeHooks.every((hook) =>
          hook.type === "http" &&
          hook.timeout === HOOK_RETRY_CONTRACT.timeoutSeconds &&
          hook.retry === HOOK_RETRY_CONTRACT.curlRetry
        ),
      claudeHooks,
    );

    const home = path.join(sandbox, ".plimsoll");
    fs.mkdirSync(home, { recursive: true, mode: 0o700 });
    const ledgerPath = path.join(home, "work-ledger.sqlite");
    const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 0 });
    const auth = loadOrCreateLocalIngestAuth(home);
    fs.writeFileSync(headerFile, `x-plimsoll-token: ${auth.codexProducer}\n`, { mode: 0o600 });
    const server = createCollectorServer(
      collectorConfigSchema.parse({}),
      buffer,
      {
        localAuth: auth,
        localAuthHome: home,
        env: { ...process.env, PLIMSOLL_HOOK_SPOOL: "off", PLIMSOLL_HOME: home },
      },
    );
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const liveCommand = generateHookForwardCommand({
      repoRoot: "/synthetic/plimsoll",
      port,
      dataMode: "metadata",
      codexHeaderFile: headerFile,
    }, "codex");
    const body = JSON.stringify({
      hook_event_name: "UserPromptSubmit",
      session_id: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
      timestamp: "2026-09-16T12:00:00.000Z",
      cwd: "/Users/proof/workspace/plimsoll",
      prompt: "RAW_PROMPT_MUST_NOT_REACH_DISK",
    });
    const lock = holdWriteLock(ledgerPath);
    const env = {
      ...process.env,
      HOME: sandbox,
      PLIMSOLL_HOME: home,
      PLIMSOLL_HOOK_SPOOL: "off",
      PATH: "/usr/bin:/bin",
    };
    const pending = runShell(liveCommand, body, env);
    await sleep(900);
    lock.release();
    const result = await pending;
    const observations = readProducerObservations(home);
    const producerIds = [...new Set(observations.map((row) => row.id))];
    const ledger = buffer.database.prepare(
      `select id, created_at as createdAt, source from buffered_events where id != 'producer-parity-lock-holder'`,
    ).all() as Array<{ id: string; createdAt: string; source: string }>;
    const report = buildProducerParityReport({
      home,
      windowHours: 6,
      ledger,
    });
    check(
      "writer_hold_retries_then_accepts_with_zero_drops",
      result.status === 0 &&
        producerIds.length === 1 &&
        report.counters.retry >= 1 &&
        report.dropped === 0 &&
        report.producerAccepted === report.ledgerDurable &&
        report.parity &&
        ledger.some((row) => row.id === producerIds[0]),
      { status: result.status, observations, report, ledgerIds: ledger.map((row) => row.id) },
    );

    const sixHourLedger = [
      { id: producerIds[0] ?? "00000000-0000-4000-8000-000000000000", createdAt: new Date().toISOString(), source: "codex" },
    ];
    const sixHour = buildProducerParityReport({
      home,
      windowHours: 6,
      ledger: sixHourLedger,
    });
    check(
      "six_hour_window_joins_producer_accepted_to_ledger_durable",
      sixHour.parity && sixHour.producerAccepted === sixHour.ledgerDurable && sixHour.dropped === 0,
      sixHour,
    );

    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();

    const layoutFile = path.join(sandbox, "interactive-codex.toml");
    const staleCommand = `curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H @${headerFile} --data-binary @- http://127.0.0.1:48271/hooks/codex || true`;
    fs.writeFileSync(
      layoutFile,
      [
        "[otel.exporter.otlp-http.headers]",
        'x-plimsoll-source = "codex"',
        "",
        "[features]",
        "hooks = true",
        "",
        "[[hooks.UserPromptSubmit.hooks]]",
        'type = "command"',
        `command = ${JSON.stringify(staleCommand)}`,
        "timeout = 5",
        "",
        "[[hooks.PostToolUse.hooks]]",
        'type = "command"',
        `command = ${JSON.stringify(staleCommand)}`,
        "timeout = 5",
        "",
        "[[hooks.Stop.hooks]]",
        'type = "command"',
        `command = ${JSON.stringify(staleCommand)}`,
        "timeout = 5",
        "",
      ].join("\n"),
      { mode: 0o600 },
    );
    const generated = generateCodexConfigToml({
      repoRoot: "/synthetic/plimsoll",
      port: 48271,
      dataMode: "metadata",
      codexProducerToken: "b".repeat(43),
      codexHeaderFile: headerFile,
    });
    let fullMessage = "";
    try {
      applyCodexConfig(layoutFile, generated);
    } catch (error) {
      fullMessage = error instanceof Error ? error.message : String(error);
    }
    const bypass = applyCodexConfigOrHookCommands(layoutFile, generated);
    const after = fs.readFileSync(layoutFile, "utf8");
    check(
      "hook_command_bypass_updates_retry_curl_when_otel_layout_refuses",
      /unsupported|layout|subset|dotted/.test(fullMessage) &&
        bypass.changed === true &&
        after.includes("--fail") &&
        after.includes("--retry-all-errors") &&
        !after.includes("|| true") &&
        applyCodexHookCommandsOnly(layoutFile, generated).changed === false,
      { fullMessage, bypass, log: producerParityLogPath(home) },
    );

    console.log(JSON.stringify({ ok: true, checks: checks.length, results: checks }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

void main();
