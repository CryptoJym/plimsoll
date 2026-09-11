/** Focused producer, setup, boundary, and capture-root proof for Grok Build. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import type http from "node:http";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";

import {
  applyGrokHookFile,
  generateGrokHookSettings,
} from "../packages/collector-config/src/index";
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
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  classifyRejectionClient,
  createRejectionDiagnostics,
} from "../packages/collector-cli/src/rejection-diagnostics";
import { inferSource } from "../packages/collector-cli/src/normalizer";
import { toolSourceSchema } from "../packages/shared/src/index";

type Check = { name: string; passed: true; detail: Record<string, unknown> };
const checks: Check[] = [];

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

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-grok-source-proof-"));
  try {
    const hookFile = path.join(sandbox, ".grok", "hooks", "plimsoll.json");
    const foreignHook = path.join(sandbox, ".grok", "hooks", "operator-memory.json");
    const foreignBytes = Buffer.from('{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"foreign"}]}]}}\n');
    fs.mkdirSync(path.dirname(foreignHook), { recursive: true, mode: 0o700 });
    fs.writeFileSync(foreignHook, foreignBytes, { mode: 0o600 });

    const token = "g".repeat(43);
    const generated = generateGrokHookSettings({
      repoRoot: "/synthetic/plimsoll",
      port: 49321,
      dataMode: "metadata",
      grokProducerToken: token,
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
          command.includes("/hooks/grok") &&
          command.includes("x-plimsoll-source: grok") &&
          command.includes(`x-plimsoll-token: ${token}`)),
      { events, commands: commands.map((command) => command.replace(token, "<redacted>")) },
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
    check(
      "grok_reconcile_is_byte_idempotent_without_backup_churn",
      !repeated.changed && fs.readFileSync(hookFile).equals(firstBytes) &&
        fs.readdirSync(path.dirname(hookFile)).every((name) => !name.includes(".plimsoll-backup-")),
      { changed: repeated.changed },
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

    const repoRoot = path.resolve(import.meta.dirname, "..");
    const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
    const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
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
        drySetup.stdout.includes(path.join(dryGrokHome, "hooks", "plimsoll.json")) &&
        !fs.existsSync(dryHome) && !fs.existsSync(dryPlimsollHome) && !fs.existsSync(dryGrokHome),
      {
        status: drySetup.status,
        grokTargetReported: drySetup.stdout.includes('"grok":{"path":'),
        homesAbsent: !fs.existsSync(dryHome) && !fs.existsSync(dryPlimsollHome) && !fs.existsSync(dryGrokHome),
      },
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
    const buffer = new LocalEventBuffer(path.join(ledgerHome, "ledger.sqlite"));
    const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, { localAuth: auth });
    const contentSentinel = "GROK_CONTENT_MUST_NOT_PERSIST";
    const payload = {
      hookEventName: "UserPromptSubmit",
      hook_event_name: "UserPromptSubmit",
      sessionId: "b03567bc-f454-43af-86f9-747625a4376e",
      cwd: `/private/${contentSentinel}`,
      workspaceRoot: `/workspace/${contentSentinel}`,
      timestamp: "2026-09-11T18:00:00.000Z",
      prompt: contentSentinel,
      toolInput: { command: contentSentinel },
      toolResult: contentSentinel,
      lastAssistantMessage: contentSentinel,
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
      check(
        "grok_authenticated_hook_reaches_the_metadata_only_ledger",
        response.statusCode === 202 && JSON.parse(response.body).accepted === true &&
          stored?.source === "grok" && stored.eventType === "user_prompt_submit" &&
          stored.sessionId === payload.sessionId &&
          !String(stored.payloadJson).includes(contentSentinel) &&
          !String(stored.payloadJson).includes(String(auth.grokProducer)),
        {
          statusCode: response.statusCode,
          source: stored?.source,
          eventType: stored?.eventType,
          sessionId: stored?.sessionId,
          contentAbsent: !String(stored?.payloadJson).includes(contentSentinel),
          tokenAbsent: !String(stored?.payloadJson).includes(String(auth.grokProducer)),
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
