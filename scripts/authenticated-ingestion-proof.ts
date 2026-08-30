/**
 * Focused proof for the authenticated portion of issue #108 / 0059.
 *
 * The fixture uses only a temporary Plimsoll home, an in-memory loopback
 * listener, and a temporary SQLite ledger. It never reads installed Claude or
 * Codex configuration and never contacts a live service.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { guardProofCompletion } from "./lib/proof-completion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  loadOrCreateLocalIngestAuth,
  readLocalIngestAuth,
  rotateLocalIngestAuth,
} from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  generateClaudeCodeSettings,
  generateCodexConfigToml,
} from "../packages/collector-config/src/templates";

type Result = {
  status: number;
  body: Record<string, unknown>;
};

const checks: Array<{ name: string; passed: boolean; detail: unknown }> = [];
const SENTINEL = "AUTH_PROOF_SECRET_MUST_NOT_LEAK";

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
}

function request(
  port: number,
  route: string,
  method: "GET" | "POST",
  body = "",
  headers: Record<string, string> = {},
): Promise<Result> {
  return new Promise((resolve, reject) => {
    const payload = Buffer.from(body);
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method,
        headers: {
          connection: "close",
          ...(method === "POST"
            ? {
                "content-type": "application/json",
                "content-length": String(payload.length),
              }
            : {}),
          ...headers,
        },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          let parsed: Record<string, unknown> = {};
          try {
            parsed = JSON.parse(text) as Record<string, unknown>;
          } catch {
            // The proof treats non-JSON as a failed receipt.
          }
          resolve({ status: response.statusCode ?? 0, body: parsed });
        });
      },
    );
    client.setTimeout(5_000, () => client.destroy(new Error("proof_request_timeout")));
    client.on("error", reject);
    client.end(payload);
  });
}

function totalChanges(buffer: LocalEventBuffer) {
  return Number(
    (buffer.database.prepare("select total_changes() as count").get() as { count: number }).count,
  );
}

// Refuses two silent-green failure modes: an early event-loop drain and a
// hang that never exits. See scripts/lib/proof-completion.ts.
const guard = guardProofCompletion({ countChecks: () => checks.length });

async function main() {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-authenticated-ingestion-"));
  fs.chmodSync(home, 0o700);
  const auth = loadOrCreateLocalIngestAuth(home);
  const secondRead = loadOrCreateLocalIngestAuth(home);
  const authFile = path.join(home, "local-ingest-auth.json");
  const mode = fs.statSync(authFile).mode & 0o777;
  check(
    "local_credentials_are_private_distinct_and_idempotent",
    auth.claudeCodeProducer !== auth.codexProducer &&
      auth.claudeCodeProducer !== auth.managementRead &&
      auth.codexProducer !== auth.managementRead &&
      secondRead.claudeCodeProducer === auth.claudeCodeProducer &&
      secondRead.codexProducer === auth.codexProducer &&
      secondRead.managementRead === auth.managementRead &&
      mode === 0o600,
    { mode, distinct: new Set([auth.claudeCodeProducer, auth.codexProducer, auth.managementRead]).size },
  );

  const rotated = rotateLocalIngestAuth(home);
  check(
    "explicit_rotation_changes_all_local_credentials_without_tool_account_access",
    rotated.claudeCodeProducer !== auth.claudeCodeProducer &&
      rotated.codexProducer !== auth.codexProducer &&
      rotated.managementRead !== auth.managementRead &&
      readLocalIngestAuth(home)?.managementRead === rotated.managementRead,
    { rotated: true },
  );

  const claude = generateClaudeCodeSettings({
    repoRoot: "/proof/repo",
    claudeCodeProducerToken: rotated.claudeCodeProducer,
    codexProducerToken: rotated.codexProducer,
  });
  const codex = generateCodexConfigToml({
    repoRoot: "/proof/repo",
    claudeCodeProducerToken: rotated.claudeCodeProducer,
    codexProducerToken: rotated.codexProducer,
  });
  const claudeJson = JSON.stringify(claude);
  check(
    "generated_claude_and_codex_surfaces_carry_only_their_own_producer_token",
    claudeJson.includes(rotated.claudeCodeProducer) &&
      !claudeJson.includes(rotated.codexProducer) &&
      codex.includes(rotated.codexProducer) &&
      !codex.includes(rotated.claudeCodeProducer),
    { claudeHasClaude: claudeJson.includes(rotated.claudeCodeProducer), codexHasCodex: codex.includes(rotated.codexProducer) },
  );

  const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const server = createCollectorServer(
    collectorConfigSchema.parse({}),
    buffer,
    { localAuth: rotated, perSourceRequestLimit: 10_000 },
  );
  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const health = await request(port, "/healthz", "GET");
    const healthKeys = Object.keys(health.body).sort().join(",");
    check("healthz_is_the_only_minimal_unauthenticated_surface", health.status === 200 && healthKeys === "ok" && health.body.ok === true, health);

    const changesBefore = totalChanges(buffer);
    const statusWithoutManagement = await request(port, "/status", "GET");
    const apiWithoutManagement = await request(port, "/api/settings", "GET");
    check(
      "management_reads_require_the_separate_management_credential",
      statusWithoutManagement.status === 401 &&
        statusWithoutManagement.body.reason === "management_credential_required" &&
        apiWithoutManagement.status === 401 &&
        apiWithoutManagement.body.reason === "management_credential_required" &&
        totalChanges(buffer) === changesBefore,
      { statusWithoutManagement, apiWithoutManagement },
    );

    const hookBody = JSON.stringify({
      id: "11111111-2222-4333-8444-555555555555",
      hook_event_name: "Stop",
      timestamp: "2026-08-20T12:00:00.000Z",
    });
    const missingToken = await request(port, "/hooks/claude-code", "POST", hookBody);
    const wrongToken = await request(port, "/hooks/claude-code", "POST", hookBody, {
      "x-plimsoll-token": rotated.codexProducer,
    });
    const sourceSwapped = await request(port, "/v1/logs", "POST", "{}", {
      "x-plimsoll-source": "claude_code",
      "x-plimsoll-token": rotated.codexProducer,
    });
    const originAttack = await request(port, "/hooks/claude-code", "POST", hookBody, {
      origin: "https://evil.example",
      "x-plimsoll-token": rotated.claudeCodeProducer,
    });
    check(
      "missing_wrong_source_swapped_and_browser_tokens_fail_before_ledger_mutation",
      missingToken.status === 401 &&
        missingToken.body.reason === "producer_token_required" &&
        wrongToken.status === 401 &&
        wrongToken.body.reason === "producer_token_invalid" &&
        sourceSwapped.status === 401 &&
        sourceSwapped.body.reason === "producer_token_invalid" &&
        originAttack.status === 403 &&
        originAttack.body.reason === "browser_origin_not_allowed" &&
        totalChanges(buffer) === changesBefore,
      { missingToken, wrongToken, sourceSwapped, originAttack },
    );

    const management = await request(port, "/status", "GET", "", {
      "x-plimsoll-token": rotated.managementRead,
    });
    check("management_credential_grants_status_without_producer_privilege", management.status === 200 && management.body.ok === true, { status: management.status });

    const acceptedHook = await request(port, "/hooks/claude-code", "POST", hookBody, {
      "x-plimsoll-token": rotated.claudeCodeProducer,
    });
    const acceptedOtlp = await request(port, "/v1/logs", "POST", JSON.stringify({
      resourceLogs: [{
        resource: { attributes: [{ key: "service.name", value: { stringValue: "claude_code" } }] },
        scopeLogs: [{ logRecords: [{ attributes: [{ key: "gen_ai.usage.input_tokens", value: { intValue: "3" } }] }] }],
      }],
    }), {
      "x-plimsoll-source": "codex",
      "x-plimsoll-token": rotated.codexProducer,
    });
    const sourceFact = buffer.database.prepare(
      "select source from buffered_events order by rowid desc limit 1",
    ).get() as { source?: string } | undefined;
    check(
      "valid_source_bound_credentials_write_and_transport_identity_wins_over_service_name",
      acceptedHook.status === 202 &&
        acceptedOtlp.status === 202 &&
        sourceFact?.source === "codex",
      { acceptedHook, acceptedOtlp, sourceFact },
    );

    const sameId = await request(port, "/hooks/claude-code", "POST", hookBody, {
      "x-plimsoll-token": rotated.claudeCodeProducer,
    });
    const collision = await request(port, "/hooks/claude-code", "POST", JSON.stringify({
      id: "11111111-2222-4333-8444-555555555555",
      hook_event_name: "Stop",
      timestamp: "2026-08-20T12:00:00.000Z",
      action_class: "read",
    }), {
      "x-plimsoll-token": rotated.claudeCodeProducer,
    });
    const integrity = buffer.eventCollisionSummary();
    check(
      "same_id_same_digest_dedupes_and_different_digest_is_quarantined",
      sameId.status === 202 && sameId.body.deduplicated === true &&
        collision.status === 202 && collision.body.collisionQuarantined === true &&
        integrity.quarantinedEventIds === 1 && integrity.totalConflicts === 1,
      { sameId, collision, integrity },
    );

    const warningText = warnings.join("\n");
    check(
      "auth_rejection_receipts_and_logs_are_bounded_and_secret_free",
      warnings.length >= 4 &&
        !warningText.includes(SENTINEL) &&
        !warningText.includes(rotated.claudeCodeProducer) &&
        !warningText.includes(rotated.codexProducer) &&
        !warningText.includes(rotated.managementRead) &&
        [missingToken, wrongToken, sourceSwapped, originAttack].every((result) => Object.keys(result.body).length === 2),
      { warningCount: warnings.length, warningBytes: Buffer.byteLength(warningText) },
    );
  } finally {
    console.warn = originalWarn;
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    fs.rmSync(home, { recursive: true, force: true });
  }

  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(JSON.stringify({ checks: checks.length, passed: checks.length - failed.length, failed: failed.length }));
  if (failed.length > 0) process.exitCode = 1;
}

main().then(() => guard.complete()).catch((error) => {
  console.error(JSON.stringify({ error: "authenticated_ingestion_proof_failed", reason: error instanceof Error ? error.message : "unknown" }));
  process.exitCode = 1;
});
