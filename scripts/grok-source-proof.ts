/** Focused producer, setup, boundary, and capture-root proof for Grok Build. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import type http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";

import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import {
  applyGrokHookFile,
  diagnoseManagedGrokHookCommand,
  generateGrokHookSettings,
} from "../packages/collector-config/src/index";
import * as collectorConfigModule from "../packages/collector-config/src/index";
import {
  HttpBoundaryRejection,
  assertHookSource,
  hookSourceFromPath,
} from "../packages/collector-cli/src/http-boundary";
import {
  assertProducerToken,
  loadOrCreateLocalIngestAuth,
} from "../packages/collector-cli/src/local-auth";
import { discoverGrokSessionSummaries } from "../packages/collector-cli/src/grok-session-discovery";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import * as collectorHomeModule from "../packages/collector-cli/src/collector-home";
import { forwardHookOverLoopback } from "../packages/collector-cli/src/local-hook-client";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  classifyRejectionClient,
  createRejectionDiagnostics,
} from "../packages/collector-cli/src/rejection-diagnostics";
import { inferSource } from "../packages/collector-cli/src/normalizer";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { toolSourceSchema } from "../packages/shared/src/index";

type Check = { name: string; passed: true; detail: Record<string, unknown> };
const checks: Check[] = [];
const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

function sha256(value: string | Buffer) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function rejection(action: () => unknown) {
  try {
    action();
    return undefined;
  } catch (error) {
    return error instanceof HttpBoundaryRejection ? error.reason : String(error);
  }
}

function errorMessage(action: () => unknown) {
  try {
    action();
    return "";
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

function request(url: string, headers: http.IncomingHttpHeaders = {}) {
  return { url, headers } as http.IncomingMessage;
}

async function dispatchRequest(server: http.Server, incoming: http.IncomingMessage) {
  return new Promise<{ statusCode: number | undefined; body: string }>((resolve, reject) => {
    let statusCode: number | undefined;
    let headersSent = false;
    const response = {
      get headersSent() {
        return headersSent;
      },
      writeHead(status: number) {
        statusCode = status;
        headersSent = true;
      },
      end(body?: string | Buffer) {
        resolve({ statusCode, body: body === undefined ? "" : String(body) });
      },
      destroy(error?: Error) {
        reject(error ?? new Error("response_destroyed"));
      },
    } as unknown as http.ServerResponse;
    server.emit("request", incoming, response);
  });
}

function runCommand(
  executable: string,
  args: string[],
  input: string,
  env: NodeJS.ProcessEnv,
) {
  return new Promise<{ status: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn(executable, args, {
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
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

function runShellCommand(command: string, input: string, env: NodeJS.ProcessEnv) {
  return runCommand("/bin/sh", ["-c", command], input, env);
}

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-grok-source-proof-"));
  try {
    const hookFile = path.join(sandbox, ".grok", "hooks", "plimsoll.json");
    const foreignHook = path.join(sandbox, ".grok", "hooks", "operator-memory.json");
    const foreignBytes = Buffer.from('{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"foreign"}]}]}}\n');
    fs.mkdirSync(path.dirname(foreignHook), { recursive: true, mode: 0o700 });
    fs.writeFileSync(foreignHook, foreignBytes, { mode: 0o600 });

    const commandAuth = loadOrCreateLocalIngestAuth(path.join(sandbox, "command-auth"));
    const token = commandAuth.grokProducer;
    assert.ok(token, "current fixture auth must include the Grok producer audience");
    const headerApi = collectorConfigModule as typeof collectorConfigModule & {
      applyGrokHookHeaderFile?: (file: string, generated: string, options?: { dryRun?: boolean }) => {
        path: string;
        changed: boolean;
        changes: string[];
        plan?: Array<{ key: string; action: string }>;
        backupPath?: string;
        conflict?: string;
      };
      generateGrokHookHeader?: (options: { repoRoot: string; grokProducerToken?: string }) => string;
    };
    check(
      "grok_managed_header_file_api_is_available",
      typeof headerApi.applyGrokHookHeaderFile === "function" &&
        typeof headerApi.generateGrokHookHeader === "function",
    );
    const headerFile = path.join(sandbox, ".grok", "hooks", "plimsoll.headers");
    const generatedHeader = headerApi.generateGrokHookHeader!({
      repoRoot: "/synthetic/plimsoll",
      grokProducerToken: token,
    });
    const headerPreview = headerApi.applyGrokHookHeaderFile!(headerFile, generatedHeader, { dryRun: true });
    check(
      "grok_header_fresh_dry_run_reports_its_own_target_without_writing",
      headerPreview.changed && headerPreview.path === headerFile && !fs.existsSync(headerFile) &&
        headerPreview.plan?.length === 1 && headerPreview.plan[0]?.key === "grok.headers.token",
      { changed: headerPreview.changed, path: headerPreview.path, plan: headerPreview.plan },
    );
    const headerApplied = headerApi.applyGrokHookHeaderFile!(headerFile, generatedHeader);
    check(
      "grok_header_fresh_apply_is_mode_0600_and_contains_the_only_command_secret",
      headerApplied.changed && !headerApplied.backupPath &&
        (fs.statSync(headerFile).mode & 0o777) === 0o600 &&
        fs.readFileSync(headerFile, "utf8") === `x-plimsoll-token: ${token}\n`,
      { changed: headerApplied.changed, mode: fs.statSync(headerFile).mode & 0o777 },
    );
    const fakeBin = path.join(sandbox, "fake-bin");
    const fakeCurl = path.join(fakeBin, "curl");
    const childArgvFile = path.join(sandbox, "hook-child-argv.txt");
    fs.mkdirSync(fakeBin, { mode: 0o700 });
    fs.writeFileSync(
      fakeCurl,
      `#!/bin/sh
printf '%s\\n' "$@" > "$PLIMSOLL_FAKE_ARGV"
exec /usr/bin/curl "$@"
`,
      { mode: 0o700 },
    );
    const commandConfig = collectorConfigSchema.parse({ port: 48271 });
    const commandBuffer = new LocalEventBuffer(path.join(sandbox, "command-ledger.sqlite"));
    const commandServer = createCollectorServer(commandConfig, commandBuffer, { localAuth: commandAuth });
    await new Promise<void>((resolve, reject) => {
      commandServer.once("error", reject);
      commandServer.listen(0, "127.0.0.1", resolve);
    });
    const commandPort = (commandServer.address() as AddressInfo).port;
    const generated = generateGrokHookSettings({
      repoRoot: "/synthetic/plimsoll",
      port: commandPort,
      dataMode: "metadata",
      grokProducerToken: token,
      grokCurlCommand: fakeCurl,
      grokHeaderFile: headerFile,
    });
    const events = Object.keys(generated.hooks).sort();
    const commands = Object.values(generated.hooks)
      .flatMap((groups) => groups)
      .flatMap((group) => group.hooks)
      .map((hook) => hook.command);
    check(
      "grok_template_uses_exact_managed_events_and_guarded_metadata_hook",
      JSON.stringify(events) === JSON.stringify(["PostToolUse", "Stop", "UserPromptSubmit"]) &&
        commands.length === 3 && commands.every((command) =>
          command.includes("GROK_HOOK_EVENT") &&
          command.includes(`http://127.0.0.1:${commandPort}/hooks/grok`) &&
          command.includes(`-H @${headerFile}`) &&
          command.includes("|| true") &&
          !command.includes(token) &&
          !command.includes("pnpm") &&
          !command.includes("--dir") &&
          !command.includes("/synthetic/plimsoll")),
      { events, commands },
    );
    const hookPayload = JSON.stringify({
      hookEventName: "user_prompt_submit",
      sessionId: "b03567bc-f454-43af-86f9-747625a4376e",
      timestamp: "2026-09-11T18:00:00.000Z",
    });
    const hookExecution = await runShellCommand(commands[0] ?? "", hookPayload, {
      ...process.env,
      GROK_HOOK_EVENT: "UserPromptSubmit",
      PATH: "/usr/bin:/bin",
      PLIMSOLL_FAKE_ARGV: childArgvFile,
    });
    const childArgv = fs.existsSync(childArgvFile) ? fs.readFileSync(childArgvFile, "utf8") : "";
    const headerBytes = fs.readFileSync(headerFile);
    const admitted = commandBuffer.database.prepare(
      "select source, event_type as eventType from buffered_events",
    ).get() as { source?: string; eventType?: string } | undefined;
    check(
      "grok_managed_hook_runs_in_posix_sh_without_pnpm_and_keeps_token_out_of_curl_argv",
      hookExecution.status === 0 && commands.every((command) => !command.includes(token)) &&
        !JSON.stringify(["/bin/sh", "-c", commands[0] ?? ""]).includes(token) &&
        !childArgv.includes(token) && !hookExecution.stdout.includes(token) &&
        !hookExecution.stderr.includes(token) &&
        headerBytes.equals(Buffer.from(`x-plimsoll-token: ${token}\n`)) &&
        (fs.statSync(headerFile).mode & 0o777) === 0o600 &&
        admitted?.source === "grok" && admitted.eventType === "user_prompt_submit",
      {
        status: hookExecution.status,
        shell: "/bin/sh -c",
        pathHasPnpm: false,
        commandUsesAbsoluteExecutable: commands.every((command) => command.includes(fakeCurl)),
        commandTokenFree: commands.every((command) => !command.includes(token)),
        shellArgvTokenFree: !JSON.stringify(["/bin/sh", "-c", commands[0] ?? ""]).includes(token),
        childArgvTokenFree: !childArgv.includes(token),
        stdoutTokenFree: !hookExecution.stdout.includes(token),
        stderrTokenFree: !hookExecution.stderr.includes(token),
        headerMode: fs.statSync(headerFile).mode & 0o777,
        headerSha256: sha256(headerBytes),
        admitted,
      },
    );
    const guardEnv: NodeJS.ProcessEnv = {
      ...process.env,
      PATH: "/usr/bin:/bin",
      PLIMSOLL_FAKE_ARGV: childArgvFile,
    };
    delete guardEnv.GROK_HOOK_EVENT;
    const bufferedEventCount = () => Number((commandBuffer.database.prepare(
      "select count(*) as count from buffered_events",
    ).get() as { count: number }).count);
    const eventsBeforeGuard = bufferedEventCount();
    const guardExecution = await runShellCommand(commands[0] ?? "", hookPayload, guardEnv);
    const eventsAfterGuard = bufferedEventCount();
    check(
      "grok_managed_hook_sends_nothing_without_guard_event",
      guardExecution.status === 0 && eventsAfterGuard === eventsBeforeGuard,
      { status: guardExecution.status, eventsBeforeGuard, eventsAfterGuard },
    );
    await new Promise<void>((resolve, reject) => commandServer.close((error) => error ? reject(error) : resolve()));
    const downStarted = performance.now();
    const downExecution = await runShellCommand(commands[0] ?? "", hookPayload, {
      ...process.env,
      GROK_HOOK_EVENT: "UserPromptSubmit",
      PATH: "/usr/bin:/bin",
      PLIMSOLL_FAKE_ARGV: childArgvFile,
    });
    const downElapsedMs = performance.now() - downStarted;
    commandBuffer.close();
    check(
      "grok_managed_hook_exits_zero_with_collector_down",
      downExecution.status === 0 && downElapsedMs < 2_500,
      { status: downExecution.status, elapsedMs: downElapsedMs },
    );

    const nonExecutableCurl = path.join(sandbox, "non-executable-curl");
    fs.writeFileSync(nonExecutableCurl, "synthetic fixture\n", { mode: 0o600 });
    const unresolvableGrokCases = [
      { name: "relative", executable: "curl" },
      { name: "missing", executable: "/definitely/missing/curl" },
      { name: "not_executable", executable: nonExecutableCurl },
    ];
    const unresolvableGrokResults = [];
    for (const fixture of unresolvableGrokCases) {
      const fixtureHome = path.join(sandbox, `grok-command-${fixture.name}-home`);
      const fixturePlimsoll = path.join(sandbox, `grok-command-${fixture.name}-plimsoll`);
      const fixtureGrokHome = path.join(fixtureHome, ".grok");
      const fixtureHook = path.join(fixtureGrokHome, "hooks", "plimsoll.json");
      fs.mkdirSync(path.dirname(fixtureHook), { recursive: true, mode: 0o700 });
      fs.mkdirSync(fixturePlimsoll, { recursive: true, mode: 0o700 });
      fs.writeFileSync(
        fixtureHook,
        `${JSON.stringify(generateGrokHookSettings({
          repoRoot: "/synthetic/plimsoll",
          port: commandPort,
          dataMode: "metadata",
          grokProducerToken: token,
          grokCurlCommand: fixture.executable,
          grokHeaderFile: path.join(fixtureGrokHome, "hooks", "plimsoll.headers"),
        }), null, 2)}\n`,
        { mode: 0o600 },
      );
      fs.writeFileSync(
        path.join(fixtureGrokHome, "hooks", "plimsoll.headers"),
        `x-plimsoll-token: ${token}\n`,
        { mode: 0o600 },
      );
      fs.writeFileSync(
        path.join(fixturePlimsoll, "collector.config.json"),
        `${JSON.stringify(collectorConfigSchema.parse({ port: commandPort }), null, 2)}\n`,
        { mode: 0o600 },
      );
      const hookDigest = sha256(fs.readFileSync(fixtureHook));
      const configDigest = sha256(fs.readFileSync(path.join(fixturePlimsoll, "collector.config.json")));
      const result = await runCommand(
        process.execPath,
        ["--import", loader, cli, "doctor", "--read-only", "--json"],
        "",
        {
          ...process.env,
          HOME: fixtureHome,
          GROK_HOME: fixtureGrokHome,
          PATH: "/usr/bin:/bin",
          PLIMSOLL_HOME: fixturePlimsoll,
          PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "200",
        },
      );
      const receipt = JSON.parse(result.stdout) as Record<string, any>;
      unresolvableGrokResults.push({
        name: fixture.name,
        exitCode: result.status,
        diagnosticCode: receipt.grokHookCommand?.code,
        reason: receipt.grokHookCommand?.reason,
        readiness: receipt.readiness,
        byteReadOnly:
          sha256(fs.readFileSync(fixtureHook)) === hookDigest &&
          sha256(fs.readFileSync(path.join(fixturePlimsoll, "collector.config.json"))) === configDigest,
      });
    }
    check(
      "doctor_fails_closed_for_relative_missing_and_non_executable_managed_grok_commands",
      unresolvableGrokResults.every((result) =>
        result.exitCode !== 0 &&
        result.diagnosticCode === "grok_hook_command_unresolvable" &&
        result.reason === result.name &&
        result.readiness === "not_installed" &&
        result.byteReadOnly),
      { results: unresolvableGrokResults },
    );

    const unresolvableHeaderCases = [
      { name: "relative", reference: "relative.headers", mode: undefined },
      { name: "missing", reference: undefined, mode: undefined },
      { name: "not_private", reference: undefined, mode: 0o644 },
    ];
    const unresolvableHeaderResults = [];
    for (const fixture of unresolvableHeaderCases) {
      const fixtureHome = path.join(sandbox, `grok-header-${fixture.name}-home`);
      const fixturePlimsoll = path.join(sandbox, `grok-header-${fixture.name}-plimsoll`);
      const fixtureGrokHome = path.join(fixtureHome, ".grok");
      const fixtureHook = path.join(fixtureGrokHome, "hooks", "plimsoll.json");
      const fixtureHeader = path.join(fixtureGrokHome, "hooks", "plimsoll.headers");
      const headerReference = fixture.reference ?? fixtureHeader;
      fs.mkdirSync(path.dirname(fixtureHook), { recursive: true, mode: 0o700 });
      fs.mkdirSync(fixturePlimsoll, { recursive: true, mode: 0o700 });
      const fixtureDocument = generateGrokHookSettings({
        repoRoot: "/synthetic/plimsoll",
        port: commandPort,
        dataMode: "metadata",
        grokCurlCommand: fakeCurl,
        grokHeaderFile: fixtureHeader,
      });
      for (const groups of Object.values(fixtureDocument.hooks)) {
        for (const group of groups) {
          for (const hook of group.hooks) {
            hook.command = hook.command.replace(`@${fixtureHeader}`, `@${headerReference}`);
          }
        }
      }
      fs.writeFileSync(fixtureHook, `${JSON.stringify(fixtureDocument, null, 2)}\n`, { mode: 0o600 });
      if (fixture.mode !== undefined) {
        fs.writeFileSync(fixtureHeader, generatedHeader, { mode: fixture.mode });
        fs.chmodSync(fixtureHeader, fixture.mode);
      }
      const configFile = path.join(fixturePlimsoll, "collector.config.json");
      fs.writeFileSync(
        configFile,
        `${JSON.stringify(collectorConfigSchema.parse({ port: commandPort }), null, 2)}\n`,
        { mode: 0o600 },
      );
      const hookDigest = sha256(fs.readFileSync(fixtureHook));
      const configDigest = sha256(fs.readFileSync(configFile));
      const headerDigest = fs.existsSync(fixtureHeader) ? sha256(fs.readFileSync(fixtureHeader)) : undefined;
      const result = await runCommand(
        process.execPath,
        ["--import", loader, cli, "doctor", "--read-only", "--json"],
        "",
        {
          ...process.env,
          HOME: fixtureHome,
          GROK_HOME: fixtureGrokHome,
          PATH: "/usr/bin:/bin",
          PLIMSOLL_HOME: fixturePlimsoll,
          PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "200",
        },
      );
      const receipt = JSON.parse(result.stdout) as Record<string, any>;
      unresolvableHeaderResults.push({
        name: fixture.name,
        exitCode: result.status,
        diagnosticCode: receipt.grokHookCommand?.code,
        reason: receipt.grokHookCommand?.reason,
        readiness: receipt.readiness,
        byteReadOnly:
          sha256(fs.readFileSync(fixtureHook)) === hookDigest &&
          sha256(fs.readFileSync(configFile)) === configDigest &&
          (headerDigest === undefined || sha256(fs.readFileSync(fixtureHeader)) === headerDigest),
      });
    }
    check(
      "doctor_fails_closed_for_relative_missing_and_non_private_managed_grok_headers",
      unresolvableHeaderResults.every((result) =>
        result.exitCode !== 0 &&
        result.diagnosticCode === "grok_hook_header_file_unresolvable" &&
        result.reason === result.name &&
        result.readiness === "not_installed" &&
        result.byteReadOnly),
      { results: unresolvableHeaderResults },
    );
    check(
      "doctor_keeps_absent_and_healthy_managed_grok_diagnostics_unchanged",
      diagnoseManagedGrokHookCommand(path.join(sandbox, "absent-grok", "plimsoll.json")) === null &&
        diagnoseManagedGrokHookCommand(hookFile) === null,
    );

    const beforeForeign = sha256(fs.readFileSync(foreignHook));
    const preview = applyGrokHookFile(hookFile, generated, { dryRun: true });
    check(
      "grok_fresh_dry_run_reports_complete_plan_without_writes",
      preview.changed && !fs.existsSync(hookFile) &&
        preview.plan?.length === 3 && preview.plan.every((entry) => entry.action === "added"),
      { changed: preview.changed, plan: preview.plan?.map((entry) => entry.key) },
    );
    const applied = applyGrokHookFile(hookFile, generated);
    const firstBytes = fs.readFileSync(hookFile);
    check(
      "grok_fresh_apply_is_private_and_preserves_foreign_hook_files_byte_for_byte",
      applied.changed && !applied.backupPath &&
        (fs.statSync(hookFile).mode & 0o777) === 0o600 &&
        sha256(fs.readFileSync(foreignHook)) === beforeForeign,
      { mode: fs.statSync(hookFile).mode & 0o777, foreignHash: beforeForeign },
    );
    const repeated = applyGrokHookFile(hookFile, generated);
    const repeatedHeader = headerApi.applyGrokHookHeaderFile!(headerFile, generatedHeader);
    check(
      "grok_reconcile_is_byte_idempotent_without_backup_churn",
      !repeated.changed && !repeatedHeader.changed && fs.readFileSync(hookFile).equals(firstBytes) &&
        fs.readdirSync(path.dirname(hookFile)).every((name) => !name.includes(".plimsoll-backup-")),
      { hookChanged: repeated.changed, headerChanged: repeatedHeader.changed },
    );

    const publicHeaderFile = path.join(sandbox, "public-managed", "plimsoll.headers");
    const priorHeaderBytes = Buffer.from(`x-plimsoll-token: ${"p".repeat(43)}\n`);
    fs.mkdirSync(path.dirname(publicHeaderFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(publicHeaderFile, priorHeaderBytes, { mode: 0o644 });
    fs.chmodSync(publicHeaderFile, 0o644);
    const privateHeaderUpdate = headerApi.applyGrokHookHeaderFile!(publicHeaderFile, generatedHeader);
    const privateHeaderBackup = privateHeaderUpdate.backupPath
      ? fs.readFileSync(privateHeaderUpdate.backupPath)
      : Buffer.alloc(0);
    check(
      "grok_header_replacement_has_private_target_and_byte_exact_private_backup",
      privateHeaderUpdate.changed && Boolean(privateHeaderUpdate.backupPath) &&
        (fs.statSync(publicHeaderFile).mode & 0o777) === 0o600 &&
        (fs.statSync(privateHeaderUpdate.backupPath!).mode & 0o777) === 0o600 &&
        privateHeaderBackup.equals(priorHeaderBytes),
      {
        targetMode: fs.statSync(publicHeaderFile).mode & 0o777,
        backupMode: fs.statSync(privateHeaderUpdate.backupPath!).mode & 0o777,
        backupSha256: sha256(privateHeaderBackup),
      },
    );
    const foreignHeaderFile = path.join(sandbox, "foreign-header", "plimsoll.headers");
    fs.mkdirSync(path.dirname(foreignHeaderFile), { recursive: true, mode: 0o700 });
    const foreignHeaderBytes = Buffer.from("Authorization: operator-owned\n");
    fs.writeFileSync(foreignHeaderFile, foreignHeaderBytes, { mode: 0o600 });
    const foreignHeaderResult = headerApi.applyGrokHookHeaderFile!(foreignHeaderFile, generatedHeader);
    check(
      "grok_foreign_header_file_is_refused_without_mutation_or_backup",
      !foreignHeaderResult.changed && Boolean(foreignHeaderResult.conflict) &&
        fs.readFileSync(foreignHeaderFile).equals(foreignHeaderBytes) &&
        fs.readdirSync(path.dirname(foreignHeaderFile)).length === 1,
      { changed: foreignHeaderResult.changed, conflict: Boolean(foreignHeaderResult.conflict) },
    );

    const publicManagedFile = path.join(sandbox, "public-managed", "plimsoll.json");
    fs.mkdirSync(path.dirname(publicManagedFile), { recursive: true, mode: 0o700 });
    const legacyToken = "o".repeat(43);
    const legacyCommand = `if [ -n "\${GROK_HOOK_EVENT:-}" ]; then curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-source: grok' -H 'x-plimsoll-token: ${legacyToken}' --data-binary @- http://127.0.0.1:49321/hooks/grok || true; fi`;
    const priorManaged = {
      hooks: {
        UserPromptSubmit: [{ hooks: [{ type: "command", command: legacyCommand, timeout: 5 }] }],
        PostToolUse: [{ matcher: ".*", hooks: [{ type: "command", command: legacyCommand, timeout: 5 }] }],
        Stop: [{ hooks: [{ type: "command", command: legacyCommand, timeout: 5 }] }],
      },
    };
    fs.writeFileSync(publicManagedFile, `${JSON.stringify(priorManaged, null, 2)}\n`, { mode: 0o644 });
    fs.chmodSync(publicManagedFile, 0o644);
    const privateUpdate = applyGrokHookFile(publicManagedFile, generated);
    const privateBackupMode = privateUpdate.backupPath
      ? fs.statSync(privateUpdate.backupPath).mode & 0o777
      : undefined;
    check(
      "grok_reconcile_forces_private_target_and_backup_from_0644_preimage",
      privateUpdate.changed && Boolean(privateUpdate.backupPath) &&
        (fs.statSync(publicManagedFile).mode & 0o777) === 0o600 && privateBackupMode === 0o600 &&
        !fs.readFileSync(publicManagedFile, "utf8").includes(legacyToken),
      {
        changed: privateUpdate.changed,
        targetMode: fs.statSync(publicManagedFile).mode & 0o777,
        backupMode: privateBackupMode,
      },
    );

    const blockedHome = path.join(sandbox, "blocked-setup-home");
    const blockedGrokHome = path.join(blockedHome, ".grok");
    const blockedPlimsollHome = path.join(sandbox, "blocked-setup-plimsoll");
    const blockedHookFile = path.join(blockedGrokHome, "hooks", "plimsoll.json");
    const blockedHeaderFile = path.join(blockedGrokHome, "hooks", "plimsoll.headers");
    fs.mkdirSync(path.dirname(blockedHookFile), { recursive: true, mode: 0o700 });
    const blockedHookBytes = Buffer.from(`${JSON.stringify(priorManaged, null, 2)}\n`);
    const blockedHeaderBytes = Buffer.from("Authorization: operator-owned\n");
    fs.writeFileSync(blockedHookFile, blockedHookBytes, { mode: 0o600 });
    fs.writeFileSync(blockedHeaderFile, blockedHeaderBytes, { mode: 0o600 });
    const blockedSetup = spawnSync(
      process.execPath,
      ["--import", loader, cli, "setup", "--yes"],
      {
        cwd: repoRoot,
        env: {
          ...process.env,
          HOME: blockedHome,
          GROK_HOME: blockedGrokHome,
          PLIMSOLL_HOME: blockedPlimsollHome,
        },
        encoding: "utf8",
      },
    );
    check(
      "setup_header_refusal_blocks_the_dependent_grok_hook_update",
      blockedSetup.status === 1 &&
        fs.readFileSync(blockedHookFile).equals(blockedHookBytes) &&
        fs.readFileSync(blockedHeaderFile).equals(blockedHeaderBytes),
      {
        status: blockedSetup.status,
        hookUnchanged: fs.readFileSync(blockedHookFile).equals(blockedHookBytes),
        headerUnchanged: fs.readFileSync(blockedHeaderFile).equals(blockedHeaderBytes),
      },
    );

    const conflictFile = path.join(sandbox, "conflict", "plimsoll.json");
    fs.mkdirSync(path.dirname(conflictFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(conflictFile, '{"hooks":{"SessionStart":[]}}\n', { mode: 0o600 });
    const conflictBytes = fs.readFileSync(conflictFile);
    const conflict = applyGrokHookFile(conflictFile, generated);
    check(
      "grok_owned_basename_with_foreign_shape_is_refused_without_mutation",
      !conflict.changed && Boolean(conflict.conflict) &&
        fs.readFileSync(conflictFile).equals(conflictBytes),
      { changed: conflict.changed, conflict: conflict.conflict },
    );
    const malformedFile = path.join(sandbox, "malformed", "plimsoll.json");
    fs.mkdirSync(path.dirname(malformedFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(malformedFile, "{not-json\n", { mode: 0o600 });
    const malformedError = errorMessage(() => applyGrokHookFile(malformedFile, generated));
    const symlinkFile = path.join(sandbox, "symlink", "plimsoll.json");
    const symlinkTarget = path.join(sandbox, "symlink-target.json");
    fs.mkdirSync(path.dirname(symlinkFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(symlinkTarget, JSON.stringify(generated), { mode: 0o600 });
    fs.symlinkSync(symlinkTarget, symlinkFile);
    const symlinkError = errorMessage(() => applyGrokHookFile(symlinkFile, generated));
    check(
      "grok_malformed_and_symlink_targets_fail_closed",
      malformedError === "GROK_CONFIG_MALFORMED_JSON" &&
        symlinkError === "GROK_CONFIG_UNSAFE_LEAF_SYMLINK",
      { malformedError, symlinkError },
    );

    const dryHome = path.join(sandbox, "dry-home");
    const dryPlimsollHome = path.join(sandbox, "dry-plimsoll-home");
    const dryGrokHome = path.join(sandbox, "dry-grok-home");
    const drySetup = spawnSync(
      process.execPath,
      ["--import", loader, cli, "setup", "--dry-run"],
      {
        cwd: repoRoot,
        env: {
          ...process.env,
          HOME: dryHome,
          PLIMSOLL_HOME: dryPlimsollHome,
          GROK_HOME: dryGrokHome,
        },
        encoding: "utf8",
      },
    );
    check(
      "setup_dry_run_reports_grok_target_without_creating_any_home",
      drySetup.status === 0 && drySetup.stdout.includes('"status":"setup_dry_run"') &&
        drySetup.stdout.includes('"grok":{"path":') &&
        drySetup.stdout.includes('"grokHeaders":{"path":') &&
        drySetup.stdout.includes(path.join(dryGrokHome, "hooks", "plimsoll.json")) &&
        drySetup.stdout.includes(path.join(dryGrokHome, "hooks", "plimsoll.headers")) &&
        !fs.existsSync(dryHome) && !fs.existsSync(dryPlimsollHome) && !fs.existsSync(dryGrokHome),
      {
        status: drySetup.status,
        grokTargetReported: drySetup.stdout.includes('"grok":{"path":'),
        grokHeaderTargetReported: drySetup.stdout.includes('"grokHeaders":{"path":'),
        homesAbsent: !fs.existsSync(dryHome) && !fs.existsSync(dryPlimsollHome) && !fs.existsSync(dryGrokHome),
      },
    );

    const guardMarker = path.join(sandbox, "grok-home-guard.marker");
    fs.writeFileSync(guardMarker, "guard-bytes\n", { mode: 0o600 });
    const invalidHomeCases = [
      { value: "", receipt: "grok_home_ambiguous" },
      { value: "relative-grok-home", receipt: "grok_home_not_absolute" },
      { value: `${dryGrokHome}/../normalization-unsafe`, receipt: "grok_home_not_normalized" },
      { value: `${dryGrokHome}\nunsafe`, receipt: "grok_home_control_characters" },
    ];
    const invalidHomeResults = invalidHomeCases.map(({ value, receipt }) => {
      const before = sha256(fs.readFileSync(guardMarker));
      const result = spawnSync(
        process.execPath,
        ["--import", loader, cli, "setup", "--dry-run"],
        {
          cwd: repoRoot,
          env: {
            ...process.env,
            HOME: dryHome,
            PLIMSOLL_HOME: dryPlimsollHome,
            GROK_HOME: value,
          },
          encoding: "utf8",
        },
      );
      return {
        receipt,
        status: result.status,
        exactReceipt: result.stderr.includes(receipt),
        bytesUnchanged: sha256(fs.readFileSync(guardMarker)) === before &&
          !fs.existsSync(dryHome) && !fs.existsSync(dryPlimsollHome),
      };
    });
    const resolveGrokHome = (
      collectorHomeModule as unknown as {
        resolveGrokHome?: (options: { env: NodeJS.ProcessEnv; homeDir: string }) => unknown;
      }
    ).resolveGrokHome;
    const nulReceipt = errorMessage(() => {
      if (!resolveGrokHome) throw new Error("resolveGrokHome_missing");
      resolveGrokHome({ env: { GROK_HOME: `${dryGrokHome}\0unsafe` }, homeDir: dryHome });
    });
    check(
      "setup_rejects_ambiguous_relative_unnormalized_and_control_grok_home_before_planning",
      invalidHomeResults.every((result) =>
        result.status === 1 && result.exactReceipt && result.bytesUnchanged) &&
        nulReceipt.includes("grok_home_control_characters"),
      { invalidHomeResults, nulReceipt },
    );

    const authHome = path.join(sandbox, "auth");
    const auth = loadOrCreateLocalIngestAuth(authHome);
    check(
      "grok_has_a_distinct_private_producer_token_audience",
      typeof auth.grokProducer === "string" &&
        new Set([
          auth.claudeCodeProducer,
          auth.codexProducer,
          auth.geminiCliProducer,
          auth.grokProducer,
          auth.managementRead,
        ]).size === 5 &&
        (fs.statSync(path.join(authHome, "local-ingest-auth.json")).mode & 0o777) === 0o600,
      { distinctAudiences: 5 },
    );
    let internalForward: { url: string; tokenMatched: boolean; sourceMatched: boolean; bodyMatched: boolean } | undefined;
    await forwardHookOverLoopback('{"hookEventName":"user_prompt_submit"}', {
      source: "grok",
      port: 49321,
      auth,
      fetchImpl: async (input, init) => {
        const headers = new Headers(init?.headers);
        internalForward = {
          url: String(input),
          tokenMatched: headers.get("x-plimsoll-token") === auth.grokProducer,
          sourceMatched: headers.get("x-plimsoll-source") === "grok",
          bodyMatched: init?.body === '{"hookEventName":"user_prompt_submit"}',
        };
        return new Response("", { status: 202 });
      },
    });
    check(
      "value_blind_forwarder_loads_the_grok_audience_inside_the_trusted_client",
      internalForward?.url === "http://127.0.0.1:49321/hooks/grok" &&
        internalForward.tokenMatched && internalForward.sourceMatched && internalForward.bodyMatched,
      { internalForward },
    );
    const migrationHome = path.join(sandbox, "auth-migration");
    fs.mkdirSync(migrationHome, { mode: 0o700 });
    const priorAuth = {
      version: 1,
      claudeCodeProducer: "a".repeat(43),
      codexProducer: "b".repeat(43),
      geminiCliProducer: "c".repeat(43),
      managementRead: "d".repeat(43),
    };
    fs.writeFileSync(
      path.join(migrationHome, "local-ingest-auth.json"),
      `${JSON.stringify(priorAuth)}\n`,
      { mode: 0o600 },
    );
    const migrationPreimage = fs.readFileSync(path.join(migrationHome, "local-ingest-auth.json"));
    const migrationPreview = loadOrCreateLocalIngestAuth(migrationHome, { dryRun: true });
    check(
      "local_auth_dry_run_plans_the_grok_audience_without_writing",
      typeof migrationPreview.grokProducer === "string" &&
        fs.readFileSync(path.join(migrationHome, "local-ingest-auth.json")).equals(migrationPreimage),
      { grokPlanned: typeof migrationPreview.grokProducer === "string", preimagePreserved: true },
    );
    const migrated = loadOrCreateLocalIngestAuth(migrationHome);
    check(
      "existing_local_auth_adds_only_the_missing_grok_audience",
      typeof migrated.grokProducer === "string" &&
        migrated.claudeCodeProducer === priorAuth.claudeCodeProducer &&
        migrated.codexProducer === priorAuth.codexProducer &&
        migrated.geminiCliProducer === priorAuth.geminiCliProducer &&
        migrated.managementRead === priorAuth.managementRead,
      { grokAdded: typeof migrated.grokProducer === "string", existingAudiencesPreserved: true },
    );

    const route = "/hooks/grok";
    const valid = request(route, {
      "x-plimsoll-source": "grok",
      "x-plimsoll-token": auth.grokProducer,
    });
    const missing = request(route, { "x-plimsoll-source": "grok" });
    const invalid = request(route, {
      "x-plimsoll-source": "grok",
      "x-plimsoll-token": "invalid",
    });
    const mismatch = request(route, {
      "x-plimsoll-source": "codex",
      "x-plimsoll-token": auth.grokProducer,
    });
    const source = hookSourceFromPath(route);
    check(
      "grok_hook_boundary_is_source_qualified_and_token_required",
      source === "grok" &&
        rejection(() => assertHookSource(valid, source)) === undefined &&
        rejection(() => assertProducerToken(valid, auth, source, new URL(route, "http://127.0.0.1"))) === undefined &&
        rejection(() => assertProducerToken(missing, auth, source, new URL(route, "http://127.0.0.1"))) === "producer_token_required" &&
        rejection(() => assertProducerToken(invalid, auth, source, new URL(route, "http://127.0.0.1"))) === "producer_token_invalid" &&
        rejection(() => assertHookSource(mismatch, source)) === "source_mismatch" &&
        rejection(() => assertProducerToken(valid, auth, "codex", new URL("/hooks/codex", "http://127.0.0.1"))) === "producer_token_invalid",
      {
        source,
        missing: "producer_token_required",
        invalid: "producer_token_invalid",
        mismatch: "source_mismatch",
        crossAudience: "producer_token_invalid",
      },
    );

    const ledgerHome = path.join(sandbox, "ledger");
    fs.mkdirSync(ledgerHome, { mode: 0o700 });
    const ledgerConfig = collectorConfigSchema.parse({
      managed: true,
      uploadUrl: "http://127.0.0.1/fake-ingest",
      installKey: "grok-source-proof-install",
    });
    const buffer = new LocalEventBuffer(path.join(ledgerHome, "ledger.sqlite"), {
      delivery: { enabled: true, limits: ledgerConfig.delivery },
    });
    const server = createCollectorServer(ledgerConfig, buffer, { localAuth: auth });
    const contentSentinel = "GROK_CONTENT_MUST_NOT_PERSIST";
    const payload = {
      hookEventName: "post_tool_use",
      hook_event_name: "PostToolUse",
      sessionId: "b03567bc-f454-43af-86f9-747625a4376e",
      cwd: `/private/${contentSentinel}`,
      workspaceRoot: `/workspace/${contentSentinel}`,
      permissionMode: "default",
      promptId: "prompt-1",
      toolName: "Bash",
      toolUseId: "tool-1",
      toolInputTruncated: false,
      toolResultTruncated: false,
      timestamp: "2026-09-11T18:00:00.000Z",
      inputTokens: 7,
      outputTokens: 5,
      cacheReadTokens: 3,
      costUsd: 0.01,
      model: "fixture-model",
      projectKey: `sha256:${"a".repeat(64)}`,
      prompt: contentSentinel,
      toolInput: { command: contentSentinel },
      toolResult: contentSentinel,
      tool_response: contentSentinel,
      stopHookActive: false,
      lastAssistantMessage: contentSentinel,
      backgroundTasks: [{
        id: "background-1",
        type: "monitor",
        status: "running",
        command: contentSentinel,
        description: contentSentinel,
        agentType: "fixture-agent",
      }],
      sessionCrons: [{
        id: "cron-1",
        schedule: "every_5_minutes",
        recurring: true,
        prompt: contentSentinel,
      }],
      inputText: contentSentinel,
      input_text: contentSentinel,
      text: contentSentinel,
      description: contentSentinel,
      content: contentSentinel,
      message: contentSentinel,
      body: contentSentinel,
      output: contentSentinel,
      stdout: contentSentinel,
      stderr: contentSentinel,
      command: contentSentinel,
      args: [contentSentinel],
    };
    const serverRequest = Object.assign(Readable.from([JSON.stringify(payload)]), {
      method: "POST",
      url: route,
      headers: {
        host: "127.0.0.1",
        "content-type": "application/json",
        "x-plimsoll-source": "grok",
        "x-plimsoll-token": auth.grokProducer,
      },
      rawHeaders: ["Host", "127.0.0.1"],
    }) as unknown as http.IncomingMessage;
    try {
      const response = await dispatchRequest(server, serverRequest);
      const stored = buffer.database.prepare(
        "select source, event_type as eventType, session_id as sessionId, payload_json as payloadJson from buffered_events",
      ).get() as Record<string, unknown> | undefined;
      const uploadBodies: string[] = [];
      const upload = await uploadBufferedEvents(ledgerConfig, buffer, {
        fetchImpl: async (_input, init) => {
          uploadBodies.push(String(init?.body ?? ""));
          return new Response(
            JSON.stringify(acceptedFixtureDelivery(String(init?.body ?? ""), ledgerConfig.installKey)),
            { status: 200, headers: { "content-type": "application/json" } },
          );
        },
      });
      const storedPayload = stored?.payloadJson ? JSON.parse(String(stored.payloadJson)) as {
        metadata?: Record<string, unknown>;
      } : undefined;
      const admittedMetadataKeys = Object.keys(storedPayload?.metadata ?? {}).sort();
      const expectedMetadataKeys = [
        "cacheReadTokens",
        "costUsd",
        "inputTokens",
        "model",
        "otelAttributes",
        "outputTokens",
        "permissionMode",
        "projectKey",
        "promptId",
        "sessionId",
        "stopHookActive",
        "toolInputTruncated",
        "toolName",
        "toolResultTruncated",
        "toolUseId",
        "workspaceRoot",
      ];
      const r1AdmittedMetadataKeys = [
        "backgroundTasks",
        "cacheReadTokens",
        "costUsd",
        "description",
        "inputText",
        "inputTokens",
        "input_text",
        "model",
        "otelAttributes",
        "outputTokens",
        "permissionMode",
        "projectKey",
        "sessionCrons",
        "sessionId",
        "stopHookActive",
        "text",
        "toolInputTruncated",
        "toolName",
        "toolResultTruncated",
        "toolUseId",
        "workspaceRoot",
      ];
      check(
        "grok_documented_and_plausible_text_fields_never_reach_ledger_or_upload",
        response.statusCode === 202 && JSON.parse(response.body).accepted === true &&
          stored?.source === "grok" && stored.eventType === "tool_result" &&
          stored.sessionId === payload.sessionId &&
          !String(stored.payloadJson).includes(contentSentinel) &&
          !String(stored.payloadJson).includes(String(auth.grokProducer)) &&
          upload.uploadedEvents === 1 && uploadBodies.length === 1 &&
          uploadBodies.every((body) =>
            !body.includes(contentSentinel) && !body.includes(String(auth.grokProducer))) &&
          JSON.stringify(admittedMetadataKeys) === JSON.stringify(expectedMetadataKeys),
        {
          statusCode: response.statusCode,
          source: stored?.source,
          eventType: stored?.eventType,
          sessionId: stored?.sessionId,
          contentAbsent: !String(stored?.payloadJson).includes(contentSentinel),
          tokenAbsent: !String(stored?.payloadJson).includes(String(auth.grokProducer)),
          uploadContentAbsent: uploadBodies.every((body) => !body.includes(contentSentinel)),
          admittedMetadataKeys,
          r1InventoryDiff: {
            added: admittedMetadataKeys.filter((key) => !r1AdmittedMetadataKeys.includes(key)),
            removed: r1AdmittedMetadataKeys.filter((key) => !admittedMetadataKeys.includes(key)),
          },
          expectedInventoryDiff: {
            added: admittedMetadataKeys.filter((key) => !expectedMetadataKeys.includes(key)),
            removed: expectedMetadataKeys.filter((key) => !admittedMetadataKeys.includes(key)),
          },
        },
      );
    } finally {
      buffer.close();
    }

    const diagnostics = createRejectionDiagnostics();
    diagnostics.recordAccepted("grok");
    const counters = diagnostics.counters();
    check(
      "grok_has_a_bounded_rejection_diagnostics_client_class",
      classifyRejectionClient(valid) === "grok" && counters.acceptedBySource.grok === 1,
      { clientClass: classifyRejectionClient(valid), accepted: counters.acceptedBySource.grok },
    );

    check(
      "grok_is_a_canonical_tool_source",
      toolSourceSchema.safeParse("grok").success && inferSource({ source: "grok-build" }) === "grok",
      { inferred: inferSource({ source: "grok-build" }) },
    );

    const grokHome = path.join(sandbox, "capture", ".grok");
    const sessionId = "13fbdfe8-3333-4d86-8550-12764848d5aa";
    const session = path.join(grokHome, "sessions", "%2Ftmp%2Fproject", sessionId);
    fs.mkdirSync(session, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(session, "summary.json"), '{"prompt":"CONTENT_SENTINEL"}\n', { mode: 0o600 });
    fs.writeFileSync(path.join(session, "updates.jsonl"), '{"prompt":"CONTENT_SENTINEL"}\n', { mode: 0o600 });
    fs.writeFileSync(path.join(grokHome, "sessions", "%2Ftmp%2Fproject", "prompt_history.jsonl"), "CONTENT_SENTINEL\n", { mode: 0o600 });
    const outside = path.join(sandbox, "outside-summary.json");
    fs.writeFileSync(outside, "CONTENT_SENTINEL\n", { mode: 0o600 });
    fs.symlinkSync(outside, path.join(session, "linked-summary.json"));
    const discovered = discoverGrokSessionSummaries(grokHome);
    check(
      "grok_capture_root_discovery_returns_only_regular_session_summary_paths",
      discovered.sessionsRoot === path.join(grokHome, "sessions") &&
        !discovered.truncated && discovered.summaryPaths.length === 1 &&
        discovered.summaryPaths[0] === path.join(session, "summary.json") &&
        JSON.stringify(discovered).includes("summary.json") &&
        !JSON.stringify(discovered).includes("CONTENT_SENTINEL") &&
        !JSON.stringify(discovered).includes("updates.jsonl") &&
        !JSON.stringify(discovered).includes("prompt_history.jsonl"),
      { summaries: discovered.summaryPaths.length, truncated: discovered.truncated },
    );

    console.log(JSON.stringify({ ok: true, checks: checks.length, results: checks }, null, 2));
  } finally {
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

void main();
