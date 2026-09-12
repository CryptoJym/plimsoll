/**
 * Focused proof for the Codex producer token: the managed hook commands stay
 * secret-free behind a 0600 header file, doctor fails closed on a broken
 * reference, `setup` migrates a legacy inline command, and
 * `rotate-producer-token --source codex` replaces the token with a bounded
 * grace window that a running collector honours without a restart.
 *
 * Every path is under a temporary directory. The proof never reads or writes
 * the operator's HOME, Codex config, credentials, Plimsoll ledger, or service.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import type http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import { parse as parseToml } from "smol-toml";

import {
  applyCodexConfig,
  applyCodexHookHeaderFile,
  diagnoseManagedCodexHookCommand,
  generateCodexConfigToml,
  generateCodexHookHeader,
} from "../packages/collector-config/src/index";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { HttpBoundaryRejection } from "../packages/collector-cli/src/http-boundary";
import {
  assertProducerToken,
  loadOrCreateLocalIngestAuth,
  producerRotationState,
  readLocalIngestAuth,
  rotateLocalProducerToken,
} from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { useFixtureRoot } from "./lib/fixture-root";

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

function errorMessage(action: () => unknown) {
  try {
    action();
    return "";
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

function rejection(action: () => unknown) {
  try {
    action();
    return undefined;
  } catch (error) {
    return error instanceof HttpBoundaryRejection ? error.reason : String(error);
  }
}

function tokenRequest(token: string | undefined) {
  return {
    url: "/hooks/codex",
    headers: token === undefined ? {} : { "x-plimsoll-token": token },
  } as http.IncomingMessage;
}

function runShellCommand(command: string, input: string, env: NodeJS.ProcessEnv) {
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

function runCli(args: string[], env: NodeJS.ProcessEnv) {
  return spawnSync(process.execPath, ["--import", loader, cli, ...args], {
    cwd: repoRoot,
    env: { ...process.env, ...env },
    encoding: "utf8",
  });
}

function backupFiles(directory: string) {
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-")).sort();
}

function managedCommands(toml: string) {
  const document = parseToml(toml) as Record<string, any>;
  const commands: string[] = [];
  const visit = (value: unknown) => {
    if (Array.isArray(value)) return value.forEach(visit);
    if (!value || typeof value !== "object") return;
    for (const [key, entry] of Object.entries(value)) {
      if (key === "command" && typeof entry === "string" && entry.includes("/hooks/codex")) {
        commands.push(entry);
      }
      visit(entry);
    }
  };
  visit(document.hooks);
  return commands;
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => { setTimeout(resolve, ms); });
}

/** Value-blind: how many rotation rows the stored credential file still has. */
function storedRotationRows(home: string) {
  return Object.keys(readLocalIngestAuth(home)?.rotations ?? {}).length;
}

function exporterHeaders(toml: string) {
  const document = parseToml(toml) as Record<string, any>;
  return ["exporter", "trace_exporter", "metrics_exporter"].map((table) =>
    document.otel?.[table]?.["otlp-http"]?.headers as Record<string, string> | undefined
  );
}

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-codex-producer-token-proof-"));
  const operatorHome = process.env.HOME;
  const syntheticHome = path.join(sandbox, "must-remain-absent-home");
  // Declare the fixture-root contract (eco-6hoxj.43): every managed apply in
  // this proof, in-process or in a child `setup`/`rotate`, targets the sandbox.
  const fixture = useFixtureRoot(sandbox, {
    home: syntheticHome,
    plimsollHome: path.join(sandbox, "fixture-plimsoll-home"),
  });
  let commandServer: http.Server | undefined;
  let commandBuffer: LocalEventBuffer | undefined;
  const rotationServers: http.Server[] = [];
  const rotationBuffers: LocalEventBuffer[] = [];
  try {
    const authHome = path.join(sandbox, "command-auth");
    const auth = loadOrCreateLocalIngestAuth(authHome);
    const token = auth.codexProducer;
    const codexHome = path.join(sandbox, "codex-home");
    const headerFile = path.join(codexHome, "plimsoll.headers");
    const configFile = path.join(codexHome, "config.toml");
    fs.mkdirSync(codexHome, { recursive: true, mode: 0o700 });

    // ---- A. The generated surfaces --------------------------------------
    const generatedHeader = generateCodexHookHeader({ repoRoot: "/synthetic/plimsoll", codexProducerToken: token });
    const generated = generateCodexConfigToml({
      repoRoot: "/synthetic/plimsoll",
      port: 48271,
      dataMode: "metadata",
      codexProducerToken: token,
      codexHeaderFile: headerFile,
    });
    const commands = managedCommands(generated);
    check(
      "codex_hook_commands_reference_the_header_file_and_carry_no_token",
      commands.length === 3 &&
        commands.every((command) =>
          command === `curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H @${headerFile} --data-binary @- http://127.0.0.1:48271/hooks/codex || true`
        ) &&
        !generated.slice(generated.indexOf("[hooks]")).includes(token) &&
        generatedHeader === `x-plimsoll-token: ${token}\n`,
      { commands, hookSectionTokenFree: !generated.slice(generated.indexOf("[hooks]")).includes(token) },
    );
    // Codex 0.146.0 rejects every file/env form for an exporter header value
    // (see RESEARCH.md), so the exporter tables deliberately stay inline and
    // are covered by rotation instead.
    check(
      "codex_exporter_tables_keep_the_documented_inline_token",
      exporterHeaders(generated).every((headers) =>
        headers?.["x-plimsoll-source"] === "codex" && headers["x-plimsoll-token"] === token
      ),
      { tables: exporterHeaders(generated).map((headers) => Object.keys(headers ?? {})) },
    );
    check(
      "codex_generation_fails_closed_without_an_absolute_header_file",
      errorMessage(() => generateCodexConfigToml({ repoRoot: "/synthetic/plimsoll", codexProducerToken: token })) ===
        "Codex managed header file must be an absolute path." &&
        errorMessage(() => generateCodexConfigToml({
          repoRoot: "/synthetic/plimsoll",
          codexProducerToken: token,
          codexHeaderFile: "relative/plimsoll.headers",
        })) === "Codex managed header file must be an absolute path." &&
        errorMessage(() => generateCodexHookHeader({ repoRoot: "/synthetic/plimsoll" })) ===
          "Codex producer token must be a 43-character URL-safe value.",
    );

    const headerPreview = applyCodexHookHeaderFile(headerFile, generatedHeader, { dryRun: true });
    check(
      "codex_header_fresh_dry_run_reports_its_own_target_without_writing",
      headerPreview.changed && headerPreview.path === headerFile && !fs.existsSync(headerFile) &&
        headerPreview.plan?.length === 1 && headerPreview.plan[0]?.key === "codex.headers.token",
      { changed: headerPreview.changed, plan: headerPreview.plan },
    );
    const headerApplied = applyCodexHookHeaderFile(headerFile, generatedHeader);
    check(
      "codex_header_fresh_apply_is_mode_0600_and_holds_only_the_token",
      headerApplied.changed && !headerApplied.backupPath &&
        (fs.statSync(headerFile).mode & 0o777) === 0o600 &&
        fs.readFileSync(headerFile, "utf8") === `x-plimsoll-token: ${token}\n`,
      { mode: fs.statSync(headerFile).mode & 0o777, sha256: sha256(fs.readFileSync(headerFile)) },
    );
    check(
      "codex_header_reconcile_is_byte_idempotent_without_backup_churn",
      !applyCodexHookHeaderFile(headerFile, generatedHeader).changed &&
        backupFiles(codexHome).length === 0,
      { backups: backupFiles(codexHome) },
    );

    const publicHeaderDir = path.join(sandbox, "public-header");
    const publicHeader = path.join(publicHeaderDir, "plimsoll.headers");
    fs.mkdirSync(publicHeaderDir, { recursive: true, mode: 0o700 });
    const priorHeaderBytes = Buffer.from(`x-plimsoll-token: ${"p".repeat(43)}\n`);
    fs.writeFileSync(publicHeader, priorHeaderBytes, { mode: 0o644 });
    fs.chmodSync(publicHeader, 0o644);
    const privateUpdate = applyCodexHookHeaderFile(publicHeader, generatedHeader);
    check(
      "codex_public_header_is_made_private_with_a_byte_exact_private_backup",
      privateUpdate.changed && Boolean(privateUpdate.backupPath) &&
        (fs.statSync(publicHeader).mode & 0o777) === 0o600 &&
        (fs.statSync(privateUpdate.backupPath!).mode & 0o777) === 0o600 &&
        fs.readFileSync(privateUpdate.backupPath!).equals(priorHeaderBytes),
      {
        targetMode: fs.statSync(publicHeader).mode & 0o777,
        backupMode: fs.statSync(privateUpdate.backupPath!).mode & 0o777,
      },
    );
    const foreignHeaderDir = path.join(sandbox, "foreign-header");
    const foreignHeader = path.join(foreignHeaderDir, "plimsoll.headers");
    fs.mkdirSync(foreignHeaderDir, { recursive: true, mode: 0o700 });
    const foreignHeaderBytes = Buffer.from("Authorization: operator-owned\n");
    fs.writeFileSync(foreignHeader, foreignHeaderBytes, { mode: 0o600 });
    const foreignResult = applyCodexHookHeaderFile(foreignHeader, generatedHeader);
    check(
      "codex_foreign_header_file_is_refused_without_mutation_or_backup",
      !foreignResult.changed && Boolean(foreignResult.conflict) &&
        fs.readFileSync(foreignHeader).equals(foreignHeaderBytes) &&
        fs.readdirSync(foreignHeaderDir).length === 1,
      { conflict: foreignResult.conflict },
    );

    // ---- A. The command actually authenticates over loopback -------------
    const commandConfig = collectorConfigSchema.parse({ port: 48271 });
    commandBuffer = new LocalEventBuffer(path.join(sandbox, "command-ledger.sqlite"));
    commandServer = createCollectorServer(commandConfig, commandBuffer, {
      localAuth: auth,
      localAuthHome: authHome,
    });
    await new Promise<void>((resolve, reject) => {
      commandServer!.once("error", reject);
      commandServer!.listen(0, "127.0.0.1", resolve);
    });
    const port = (commandServer.address() as AddressInfo).port;
    const liveHeaderFile = path.join(codexHome, "live.headers");
    fs.writeFileSync(liveHeaderFile, `x-plimsoll-token: ${token}\n`, { mode: 0o600 });
    const liveToml = generateCodexConfigToml({
      repoRoot: "/synthetic/plimsoll",
      port,
      dataMode: "metadata",
      codexProducerToken: token,
      codexHeaderFile: liveHeaderFile,
    });
    const liveCommand = managedCommands(liveToml)[0]!;
    const fakeBin = path.join(sandbox, "fake-bin");
    const childArgvFile = path.join(sandbox, "hook-child-argv.txt");
    fs.mkdirSync(fakeBin, { mode: 0o700 });
    fs.writeFileSync(
      path.join(fakeBin, "curl"),
      `#!/bin/sh\nprintf '%s\\n' "$@" > "$PLIMSOLL_FAKE_ARGV"\nexec /usr/bin/curl "$@"\n`,
      { mode: 0o700 },
    );
    const hookPayload = JSON.stringify({
      hookEventName: "user_prompt_submit",
      sessionId: "5f7d1d0c-9a0f-4f0b-9a0c-0b6f4b0a1c22",
      timestamp: "2026-09-12T02:00:00.000Z",
    });
    const hookExecution = await runShellCommand(liveCommand, hookPayload, {
      ...process.env,
      PATH: `${fakeBin}:/usr/bin:/bin`,
      PLIMSOLL_FAKE_ARGV: childArgvFile,
    });
    const childArgv = fs.readFileSync(childArgvFile, "utf8");
    const admitted = commandBuffer.database.prepare(
      "select source, event_type as eventType from buffered_events",
    ).get() as { source?: string; eventType?: string } | undefined;
    check(
      "codex_managed_hook_authenticates_without_the_token_in_any_argv",
      hookExecution.status === 0 &&
        !liveCommand.includes(token) &&
        !JSON.stringify(["/bin/sh", "-c", liveCommand]).includes(token) &&
        !childArgv.includes(token) &&
        !hookExecution.stdout.includes(token) && !hookExecution.stderr.includes(token) &&
        admitted?.source === "codex" && admitted.eventType === "user_prompt_submit",
      {
        status: hookExecution.status,
        commandTokenFree: !liveCommand.includes(token),
        childArgvTokenFree: !childArgv.includes(token),
        admitted,
      },
    );
    const unauthenticatedHeader = path.join(codexHome, "wrong.headers");
    fs.writeFileSync(unauthenticatedHeader, `x-plimsoll-token: ${"z".repeat(43)}\n`, { mode: 0o600 });
    const beforeRejected = Number((commandBuffer.database.prepare(
      "select count(*) as count from buffered_events",
    ).get() as { count: number }).count);
    const rejected = await runShellCommand(
      liveCommand.replace(`@${liveHeaderFile}`, `@${unauthenticatedHeader}`),
      hookPayload,
      { ...process.env, PATH: "/usr/bin:/bin" },
    );
    check(
      "codex_hook_with_a_foreign_header_file_is_rejected_and_buffers_nothing",
      rejected.status === 0 &&
        Number((commandBuffer.database.prepare(
          "select count(*) as count from buffered_events",
        ).get() as { count: number }).count) === beforeRejected,
      { status: rejected.status, beforeRejected },
    );

    // ---- A. Doctor fails closed on a broken header reference -------------
    const legacyCommand = (headerToken: string, hookPort: number) =>
      `curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: ${headerToken}' --data-binary @- http://127.0.0.1:${hookPort}/hooks/codex || true`;
    const diagnosticCases = [
      { name: "missing_reference", command: legacyCommand(token, port), header: 0o600 as number | null },
      { name: "relative", reference: "relative.headers", header: 0o600 as number | null },
      { name: "missing", header: null as number | null },
      { name: "not_private", header: 0o644 as number | null },
    ];
    const diagnosticResults = [];
    for (const fixture of diagnosticCases) {
      const caseHome = path.join(sandbox, `diagnostic-${fixture.name}`);
      const caseCodex = path.join(caseHome, ".codex");
      const casePlimsoll = path.join(sandbox, `diagnostic-${fixture.name}-plimsoll`);
      const caseGrok = path.join(caseHome, ".grok");
      const caseHeader = path.join(caseCodex, "plimsoll.headers");
      fs.mkdirSync(caseCodex, { recursive: true, mode: 0o700 });
      fs.mkdirSync(casePlimsoll, { recursive: true, mode: 0o700 });
      const caseConfig = path.join(caseCodex, "config.toml");
      let toml = generateCodexConfigToml({
        repoRoot: "/synthetic/plimsoll",
        port,
        dataMode: "metadata",
        codexProducerToken: token,
        codexHeaderFile: caseHeader,
      });
      if (fixture.command) {
        toml = toml.replaceAll(
          JSON.stringify(managedCommands(toml)[0]!),
          JSON.stringify(fixture.command),
        );
      } else if (fixture.reference) {
        toml = toml.replaceAll(`@${caseHeader} `, `@${fixture.reference} `);
      }
      fs.writeFileSync(caseConfig, toml, { mode: 0o600 });
      if (fixture.header !== null) {
        fs.writeFileSync(caseHeader, `x-plimsoll-token: ${token}\n`, { mode: fixture.header });
        fs.chmodSync(caseHeader, fixture.header);
      }
      fs.writeFileSync(
        path.join(casePlimsoll, "collector.config.json"),
        `${JSON.stringify(collectorConfigSchema.parse({ port }), null, 2)}\n`,
        { mode: 0o600 },
      );
      const configDigest = sha256(fs.readFileSync(caseConfig));
      const doctor = runCli(["doctor", "--read-only", "--json"], {
        HOME: caseHome,
        GROK_HOME: caseGrok,
        PLIMSOLL_HOME: casePlimsoll,
        PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "200",
      });
      const receipt = JSON.parse(doctor.stdout) as Record<string, any>;
      diagnosticResults.push({
        name: fixture.name,
        exitCode: doctor.status,
        code: receipt.codexHookCommand?.code,
        reason: receipt.codexHookCommand?.reason,
        readiness: receipt.readiness,
        byteReadOnly: sha256(fs.readFileSync(caseConfig)) === configDigest,
      });
    }
    check(
      "doctor_fails_closed_for_inline_relative_missing_and_non_private_codex_headers",
      diagnosticResults.every((result) =>
        result.exitCode !== 0 &&
        result.code === "codex_hook_header_file_unresolvable" &&
        result.reason === result.name &&
        result.readiness === "not_installed" &&
        result.byteReadOnly),
      { results: diagnosticResults },
    );
    check(
      "doctor_reports_no_codex_header_diagnostic_for_absent_or_healthy_configs",
      diagnoseManagedCodexHookCommand(path.join(sandbox, "absent", "config.toml")) === null &&
        diagnoseManagedCodexHookCommand(path.join(codexHome, "config.toml.absent")) === null,
    );
    fs.writeFileSync(configFile, generated, { mode: 0o600 });
    check(
      "healthy_codex_config_and_header_pair_produce_no_diagnostic",
      diagnoseManagedCodexHookCommand(configFile) === null,
    );

    // ---- A. setup migrates a legacy inline command, Codex-only -----------
    const migrateHome = path.join(sandbox, "migrate-home");
    const migrateCodex = path.join(migrateHome, ".codex");
    const migrateGrok = path.join(migrateHome, ".grok");
    const migratePlimsoll = path.join(sandbox, "migrate-plimsoll");
    fs.mkdirSync(migrateCodex, { recursive: true, mode: 0o700 });
    const migrateEnv = {
      HOME: migrateHome,
      GROK_HOME: migrateGrok,
      PLIMSOLL_HOME: migratePlimsoll,
    };
    const migrateSetup = runCli(["setup", "--yes"], migrateEnv);
    check("migrate_fixture_first_setup_applies", migrateSetup.status === 0, {
      status: migrateSetup.status,
      stderr: migrateSetup.stderr,
    });
    const migrateAuth = readLocalIngestAuth(migratePlimsoll)!;
    const migrateConfigFile = path.join(migrateCodex, "config.toml");
    const migrateHeaderFile = path.join(migrateCodex, "plimsoll.headers");
    // Rewrite the applied config back into the legacy inline-token shape.
    fs.writeFileSync(
      migrateConfigFile,
      fs.readFileSync(migrateConfigFile, "utf8").replaceAll(
        `-H @${migrateHeaderFile} `,
        `-H 'x-plimsoll-token: ${migrateAuth.codexProducer}' `,
      ),
      { mode: 0o600 },
    );
    fs.rmSync(migrateHeaderFile);
    for (const name of backupFiles(migrateCodex)) fs.rmSync(path.join(migrateCodex, name));
    const migrateDryRun = runCli(["setup", "--dry-run"], migrateEnv);
    const migrateDryReceipt = JSON.parse(
      migrateDryRun.stdout.slice(migrateDryRun.stdout.indexOf('{"status":"setup_dry_run"')),
    ) as Record<string, any>;
    const migrateStatuses = Object.fromEntries(
      Object.entries(migrateDryReceipt.targets as Record<string, any>)
        .map(([name, value]) => [name, value.status]),
    );
    check(
      "setup_dry_run_reports_the_legacy_inline_migration_as_codex_only",
      migrateDryRun.status === 0 &&
        migrateStatuses.claude === "unchanged" &&
        migrateStatuses.gemini === "unchanged" &&
        migrateStatuses.grokHeaders === "unchanged" &&
        migrateStatuses.grok === "unchanged" &&
        migrateStatuses.codexHeaders === "would_apply" &&
        migrateStatuses.codex === "would_apply" &&
        !fs.existsSync(migrateHeaderFile) &&
        Object.values(migrateAuth).every((value) =>
          typeof value !== "string" || !migrateDryRun.stdout.includes(value)
        ),
      { statuses: migrateStatuses },
    );
    const migrateApply = runCli(["setup", "--yes"], migrateEnv);
    const migratedToml = fs.readFileSync(migrateConfigFile, "utf8");
    const migratedAuth = readLocalIngestAuth(migratePlimsoll)!;
    const migrateNoop = runCli(["setup", "--yes"], migrateEnv);
    check(
      "setup_migrates_the_inline_command_to_the_private_header_file_and_settles",
      migrateApply.status === 0 &&
        managedCommands(migratedToml).length === 3 &&
        managedCommands(migratedToml).every((command) =>
          command.includes(`-H @${migrateHeaderFile} `) && !command.includes("x-plimsoll-token:")
        ) &&
        (fs.statSync(migrateHeaderFile).mode & 0o777) === 0o600 &&
        fs.readFileSync(migrateHeaderFile, "utf8") === `x-plimsoll-token: ${migratedAuth.codexProducer}\n` &&
        migrateNoop.status === 0 && migrateNoop.stdout.includes('"status":"setup_noop"') &&
        Object.values(migratedAuth).every((value) =>
          typeof value !== "string" ||
          (!migrateApply.stdout.includes(value) && !migrateNoop.stdout.includes(value))
        ),
      {
        status: migrateApply.status,
        headerMode: fs.statSync(migrateHeaderFile).mode & 0o777,
        secondRunSettled: migrateNoop.stdout.includes('"status":"setup_noop"'),
      },
    );

    // ---- C. Rotation ------------------------------------------------------
    const backupsBeforeRotation = backupFiles(migrateCodex).length;
    const rotateDry = runCli(["rotate-producer-token", "--source", "codex", "--dry-run"], migrateEnv);
    check(
      "rotate_dry_run_mints_nothing_and_writes_nothing",
      rotateDry.status === 0 &&
        rotateDry.stdout.includes('"status": "rotation_dry_run"') &&
        readLocalIngestAuth(migratePlimsoll)!.codexProducer === migratedAuth.codexProducer &&
        fs.readFileSync(migrateConfigFile, "utf8") === migratedToml &&
        backupFiles(migrateCodex).length === backupsBeforeRotation,
      { status: rotateDry.status, backupsBeforeRotation, backups: backupFiles(migrateCodex) },
    );
    const rotateApply = runCli(
      ["rotate-producer-token", "--source", "codex", "--grace-seconds", "60"],
      migrateEnv,
    );
    const rotatedAuth = readLocalIngestAuth(migratePlimsoll)!;
    const rotatedToml = fs.readFileSync(migrateConfigFile, "utf8");
    const rotatedHeader = fs.readFileSync(migrateHeaderFile, "utf8");
    check(
      "rotate_rewrites_header_and_config_with_backups_and_prints_no_token",
      rotateApply.status === 0 &&
        rotateApply.stdout.includes('"status": "rotation_applied"') &&
        rotatedAuth.codexProducer !== migratedAuth.codexProducer &&
        rotatedAuth.rotations?.codex?.token === migratedAuth.codexProducer &&
        rotatedHeader === `x-plimsoll-token: ${rotatedAuth.codexProducer}\n` &&
        (fs.statSync(migrateHeaderFile).mode & 0o777) === 0o600 &&
        exporterHeaders(rotatedToml).every((headers) =>
          headers?.["x-plimsoll-token"] === rotatedAuth.codexProducer
        ) &&
        !rotatedToml.includes(migratedAuth.codexProducer) &&
        backupFiles(migrateCodex).length === backupsBeforeRotation + 2 &&
        [rotatedAuth.codexProducer, migratedAuth.codexProducer, rotatedAuth.managementRead]
          .every((value) => !rotateApply.stdout.includes(value) && !rotateApply.stderr.includes(value)),
      {
        status: rotateApply.status,
        backupsBeforeRotation,
        backups: backupFiles(migrateCodex).length,
        graceRecorded: Boolean(rotatedAuth.rotations?.codex),
      },
    );
    const rotatedDoctor = runCli(["doctor", "--read-only", "--json"], {
      ...migrateEnv,
      PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "200",
    });
    const rotatedReceipt = JSON.parse(rotatedDoctor.stdout) as Record<string, any>;
    check(
      "doctor_reports_the_rotation_window_value_blind",
      rotatedReceipt.producerTokenRotation?.codex?.state === "active" &&
        typeof rotatedReceipt.producerTokenRotation.codex.expiresAt === "string" &&
        rotatedReceipt.producerTokenRotation.codex.secondsRemaining > 0 &&
        rotatedReceipt.producerTokenRotation.claude_code.state === "none" &&
        rotatedReceipt.codexHookCommand === undefined &&
        [rotatedAuth.codexProducer, migratedAuth.codexProducer]
          .every((value) => !rotatedDoctor.stdout.includes(value)),
      { rotation: rotatedReceipt.producerTokenRotation },
    );

    const graceHome = path.join(sandbox, "grace-plimsoll");
    fs.mkdirSync(graceHome, { recursive: true, mode: 0o700 });
    const graceBefore = loadOrCreateLocalIngestAuth(graceHome);
    const graceRotated = rotateLocalProducerToken(graceHome, "codex", { graceMs: 60_000 });
    const expired = {
      ...graceRotated.auth,
      rotations: { codex: { token: graceBefore.codexProducer, expiresAt: Date.now() - 1 } },
    };
    check(
      "rotation_grace_accepts_old_and_new_then_only_new_after_expiry",
      rejection(() => assertProducerToken(tokenRequest(graceRotated.auth.codexProducer), graceRotated.auth, "codex", new URL("http://127.0.0.1/hooks/codex"))) === undefined &&
        rejection(() => assertProducerToken(tokenRequest(graceBefore.codexProducer), graceRotated.auth, "codex", new URL("http://127.0.0.1/hooks/codex"))) === undefined &&
        rejection(() => assertProducerToken(tokenRequest(graceBefore.claudeCodeProducer), graceRotated.auth, "codex", new URL("http://127.0.0.1/hooks/codex"))) === "producer_token_invalid" &&
        rejection(() => assertProducerToken(tokenRequest(graceBefore.codexProducer), expired, "codex", new URL("http://127.0.0.1/hooks/codex"))) === "producer_token_invalid" &&
        rejection(() => assertProducerToken(tokenRequest(graceBefore.codexProducer), graceRotated.auth, "claude_code", new URL("http://127.0.0.1/hooks/claude-code"))) === "producer_token_invalid",
      {
        graceState: producerRotationState(graceRotated.auth, "codex").state,
        expiredState: producerRotationState(expired, "codex").state,
      },
    );

    // A running collector loaded its authority at start; the rotated token must
    // still be admitted without restarting the daemon.
    const rotatedLive = rotateLocalProducerToken(authHome, "codex", { graceMs: 60_000 });
    const liveResponse = async (headerToken: string) => {
      const response = await fetch(`http://127.0.0.1:${port}/hooks/codex`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-plimsoll-token": headerToken },
        body: JSON.stringify({
          hookEventName: "stop",
          sessionId: "2f1a4c88-2c6f-4f56-9a4a-6b4f5f0a7d31",
          timestamp: "2026-09-12T02:05:00.000Z",
        }),
      });
      return response.status;
    };
    const newTokenStatus = await liveResponse(rotatedLive.auth.codexProducer);
    const oldTokenStatus = await liveResponse(token);
    const foreignTokenStatus = await liveResponse("y".repeat(43));
    check(
      "running_collector_admits_the_rotated_token_without_a_restart",
      newTokenStatus === 202 && oldTokenStatus === 202 && foreignTokenStatus === 401,
      { newTokenStatus, oldTokenStatus, foreignTokenStatus },
    );

    // The reload seam is decided before the admission decision, so a running
    // daemon follows a rotation whatever it was probed with first. Each case
    // below gets its own credential home and its own listener; every request
    // goes to a collector that loaded its authority *before* the rotation.
    const startRotationCollector = async (label: string, authorityHome: string) => {
      fs.mkdirSync(authorityHome, { recursive: true, mode: 0o700 });
      const authority = loadOrCreateLocalIngestAuth(authorityHome);
      const ledger = new LocalEventBuffer(path.join(sandbox, `${label}-ledger.sqlite`));
      rotationBuffers.push(ledger);
      const server = createCollectorServer(collectorConfigSchema.parse({ port: 48271 }), ledger, {
        localAuth: authority,
        localAuthHome: authorityHome,
      });
      rotationServers.push(server);
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const listenPort = (server.address() as AddressInfo).port;
      const post = async (headerToken: string) => {
        const response = await fetch(`http://127.0.0.1:${listenPort}/hooks/codex`, {
          method: "POST",
          headers: { "content-type": "application/json", "x-plimsoll-token": headerToken },
          body: JSON.stringify({
            hookEventName: "stop",
            sessionId: "8c1d6a20-4f3e-4c0a-9c1e-7b2d5a9f3e10",
            timestamp: "2026-09-12T02:06:00.000Z",
          }),
        });
        return response.status;
      };
      return { authority, post };
    };

    // 1. The reviewer's adversarial order: a foreign miss before the new token
    //    must not consume the reload, and the expired token must stay out.
    const retireHome = path.join(sandbox, "retire-auth");
    const retire = await startRotationCollector("retire", retireHome);
    const retireBeforeRotation = await retire.post(retire.authority.codexProducer);
    const retired = rotateLocalProducerToken(retireHome, "codex", { graceMs: 1 });
    const retireRowsAfterRotation = storedRotationRows(retireHome);
    await sleep(50);
    const retireOldFirst = await retire.post(retire.authority.codexProducer);
    const retireForeign = await retire.post("y".repeat(43));
    const retireOldAfterForeign = await retire.post(retire.authority.codexProducer);
    const retireNewLast = await retire.post(retired.auth.codexProducer);
    check(
      "expired_token_is_refused_and_the_rotated_token_admitted_in_any_probe_order",
      retireBeforeRotation === 202 && retireOldFirst === 401 && retireForeign === 401 &&
        retireOldAfterForeign === 401 && retireNewLast === 202,
      {
        beforeRotation: retireBeforeRotation,
        oldAfterExpiry: retireOldFirst,
        foreign: retireForeign,
        oldAfterForeignMiss: retireOldAfterForeign,
        newAfterMisses: retireNewLast,
      },
    );
    check(
      "closed_rotation_window_is_pruned_from_the_credential_file",
      retireRowsAfterRotation === 1 && storedRotationRows(retireHome) === 0 &&
        (fs.statSync(path.join(retireHome, "local-ingest-auth.json")).mode & 0o777) === 0o600 &&
        readLocalIngestAuth(retireHome)!.codexProducer === retired.auth.codexProducer,
      {
        rowsAfterRotation: retireRowsAfterRotation,
        rowsAfterExpiry: storedRotationRows(retireHome),
        mode: fs.statSync(path.join(retireHome, "local-ingest-auth.json")).mode & 0o777,
      },
    );

    // 2. A client that only ever presents the old token never misses, so the
    //    window has to close on its own schedule, not on a rejection.
    const windowHome = path.join(sandbox, "window-auth");
    const windowGraceMs = 1_500;
    const onlyOld = await startRotationCollector("window", windowHome);
    const windowBeforeRotation = await onlyOld.post(onlyOld.authority.codexProducer);
    const windowRotated = rotateLocalProducerToken(windowHome, "codex", { graceMs: windowGraceMs });
    const windowOldInside = await onlyOld.post(onlyOld.authority.codexProducer);
    await sleep(Math.max(windowRotated.expiresAt - Date.now(), 0) + 50);
    const windowOldOutside = await onlyOld.post(onlyOld.authority.codexProducer);
    check(
      "old_token_only_traffic_is_cut_off_when_the_window_closes",
      windowBeforeRotation === 202 && windowOldInside === 202 && windowOldOutside === 401 &&
        storedRotationRows(windowHome) === 0,
      {
        beforeRotation: windowBeforeRotation,
        insideWindow: windowOldInside,
        afterWindow: windowOldOutside,
        graceMs: windowGraceMs,
      },
    );

    // 3. No preceding rejection anywhere: the first request that carries the
    //    rotated token is admitted.
    const firstHome = path.join(sandbox, "first-request-auth");
    const first = await startRotationCollector("first-request", firstHome);
    const firstBeforeRotation = await first.post(first.authority.codexProducer);
    const firstRotated = rotateLocalProducerToken(firstHome, "codex", { graceMs: 60_000 });
    const firstNewToken = await first.post(firstRotated.auth.codexProducer);
    const firstOldInsideGrace = await first.post(first.authority.codexProducer);
    check(
      "rotated_token_is_admitted_on_its_first_request_without_a_preceding_miss",
      firstBeforeRotation === 202 && firstNewToken === 202 && firstOldInsideGrace === 202 &&
        storedRotationRows(firstHome) === 1,
      {
        beforeRotation: firstBeforeRotation,
        firstNewToken,
        oldInsideGrace: firstOldInsideGrace,
        rows: storedRotationRows(firstHome),
      },
    );

    // 4. Pruning happens on the request path, so a credential home the daemon
    //    cannot write must still decide the request instead of erroring.
    const sealedHome = path.join(sandbox, "sealed-auth");
    const sealed = await startRotationCollector("sealed", sealedHome);
    const sealedBeforeRotation = await sealed.post(sealed.authority.codexProducer);
    const sealedRotated = rotateLocalProducerToken(sealedHome, "codex", { graceMs: 1 });
    await sleep(50);
    fs.chmodSync(sealedHome, 0o500);
    let sealedOld = 0;
    let sealedNew = 0;
    try {
      sealedOld = await sealed.post(sealed.authority.codexProducer);
      sealedNew = await sealed.post(sealedRotated.auth.codexProducer);
    } finally {
      fs.chmodSync(sealedHome, 0o700);
    }
    check(
      "an_unwritable_credential_home_still_retires_the_expired_token",
      sealedBeforeRotation === 202 && sealedOld === 401 && sealedNew === 202 &&
        storedRotationRows(sealedHome) === 1,
      {
        beforeRotation: sealedBeforeRotation,
        oldAfterExpiry: sealedOld,
        newAfterExpiry: sealedNew,
        rowsLeftOnDisk: storedRotationRows(sealedHome),
      },
    );

    // 4. CI must keep running this proof: install-doctor's standalone gate list
    //    is what stops the workflow step from being dropped silently.
    const gateCommand = "pnpm proof:codex-producer-token";
    const installDoctorProof = fs.readFileSync(
      path.join(repoRoot, "scripts", "install-doctor-proof.ts"),
      "utf8",
    );
    const gateList = installDoctorProof.slice(
      installDoctorProof.indexOf("const requiredStandaloneGates = ["),
    ).split("];")[0] ?? "";
    const workflowRuns = [
      ...fs.readFileSync(path.join(repoRoot, ".github", "workflows", "proof.yml"), "utf8")
        .matchAll(/^\s+run:\s*(.+?)\s*$/gm),
    ].filter((match) => match[1] === gateCommand).length;
    check(
      "ci_gate_list_locks_this_proof_into_the_workflow",
      gateList.includes(`"${gateCommand}"`) && workflowRuns === 1,
      { gateListed: gateList.includes(`"${gateCommand}"`), workflowRuns },
    );

    // ---- C. Rotation fails closed ----------------------------------------
    const refuseHome = path.join(sandbox, "refuse-home");
    const refuseCodex = path.join(refuseHome, ".codex");
    const refusePlimsoll = path.join(sandbox, "refuse-plimsoll");
    fs.mkdirSync(refuseCodex, { recursive: true, mode: 0o700 });
    fs.mkdirSync(refusePlimsoll, { recursive: true, mode: 0o700 });
    const unrotatable = loadOrCreateLocalIngestAuth(refusePlimsoll);
    const foreignConfigBytes = '[otel]\nenvironment = "first"\n[otel]\nenvironment = "duplicate"\n';
    fs.writeFileSync(path.join(refuseCodex, "config.toml"), foreignConfigBytes, { mode: 0o600 });
    const refused = runCli(["rotate-producer-token", "--source", "codex"], {
      HOME: refuseHome,
      GROK_HOME: path.join(refuseHome, ".grok"),
      PLIMSOLL_HOME: refusePlimsoll,
    });
    check(
      "rotate_refuses_an_unreconcilable_config_without_touching_credentials",
      refused.status === 1 &&
        refused.stdout.includes('"status": "rotation_refused"') &&
        refused.stdout.includes('"rotated": false') &&
        readLocalIngestAuth(refusePlimsoll)!.codexProducer === unrotatable.codexProducer &&
        fs.readFileSync(path.join(refuseCodex, "config.toml"), "utf8") === foreignConfigBytes &&
        backupFiles(refuseCodex).length === 0,
      { status: refused.status, backups: backupFiles(refuseCodex) },
    );
    const unprovisionedHome = path.join(sandbox, "unprovisioned-home");
    const unprovisionedPlimsoll = path.join(sandbox, "unprovisioned-plimsoll");
    const unprovisioned = runCli(["rotate-producer-token", "--source", "codex"], {
      HOME: unprovisionedHome,
      GROK_HOME: path.join(unprovisionedHome, ".grok"),
      PLIMSOLL_HOME: unprovisionedPlimsoll,
    });
    const badSource = runCli(["rotate-producer-token", "--source", "claude-code"], {
      HOME: unprovisionedHome,
      GROK_HOME: path.join(unprovisionedHome, ".grok"),
      PLIMSOLL_HOME: unprovisionedPlimsoll,
    });
    const badGrace = runCli(
      ["rotate-producer-token", "--source", "codex", "--grace-seconds", "0"],
      { HOME: migrateHome, GROK_HOME: migrateGrok, PLIMSOLL_HOME: migratePlimsoll },
    );
    check(
      "rotate_never_provisions_and_rejects_an_unknown_source_or_grace",
      unprovisioned.status === 1 &&
        !fs.existsSync(path.join(unprovisionedPlimsoll, "local-ingest-auth.json")) &&
        badSource.status === 1 && badSource.stderr.includes("--source codex") &&
        badGrace.status === 1 && badGrace.stderr.includes("--grace-seconds") &&
        readLocalIngestAuth(migratePlimsoll)!.codexProducer === rotatedAuth.codexProducer,
      {
        unprovisioned: unprovisioned.status,
        badSource: badSource.status,
        badGrace: badGrace.status,
      },
    );

    check(
      "all_paths_leave_the_synthetic_operator_home_absent",
      !fs.existsSync(syntheticHome),
      { syntheticHome },
    );
    console.log(JSON.stringify({ proof: "codex-producer-token", checks }, null, 2));
  } finally {
    if (commandServer) {
      await new Promise<void>((resolve) => commandServer!.close(() => resolve()));
    }
    for (const server of rotationServers) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    commandBuffer?.close();
    for (const buffer of rotationBuffers) buffer.close();
    fixture.restore();
    if (operatorHome === undefined) delete process.env.HOME;
    else process.env.HOME = operatorHome;
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
