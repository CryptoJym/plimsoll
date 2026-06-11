/**
 * Signal-fidelity proof for the v2 collector capture path.
 *
 * The 2026-06-10 audit found the v1 collector flattened OTLP batches to one
 * event (0% codex / ~1% claude token capture), never derived actionClass, and
 * never drained uploads. This proof asserts the failure modes stay fixed:
 *
 *   1. Multi-record OTLP logs envelopes explode into one event per record.
 *   2. claude_code.api_request records carry tokens/cost/model/session and
 *      map to assistant_response.
 *   3. Metric datapoints (claude_code.token.usage) are parsed with values.
 *   4. Codex trace spans carrying gen_ai.usage.* produce token-attributed events.
 *   5. Codex tool_result `arguments` (cmd/workdir) are suppressed; no raw
 *      command bodies or absolute paths persist in metadata mode.
 *   6. Hook tool_name maps to actionClass (Bash→shell, Edit→edit, mcp__*→mcp).
 *   7. Upload drains oldest-first and advances the uploaded_at watermark.
 *   8. Retention prune deletes aged rows.
 *   9. Every dashboard query executes against a real ledger, the views
 *      reconcile to one total, and session attribution follows dominant
 *      cost — not lexicographic max(hash). (/api/sessions shipped a query
 *      SQLite rejects at run time; proof was green because nothing ran it.)
 *  10. Capture health judges the ledger against local tool activity: idle is
 *      green, a broken pipe is red within minutes, uncaptured/partial codex
 *      coverage is red/amber. Silence must scream (G7, issue 0021).
 *  11. Codex rollout tailer ingests token_count lines exactly (telescoped
 *      cumulative totals), idempotently, with repo linkage from cwd, OTLP
 *      first-writer-wins dedupe, and zero content persistence (issue 0022).
 *  12. Account aliases (local-only) merge split identities at read time,
 *      reversibly, via settings endpoint and dashboard queries (issue 0023).
 *  13. Config apply mode merges telemetry settings surgically (user keys
 *      preserved, backups written, idempotent, foreign [otel] never
 *      clobbered) — the five-minute promise's first step (issue 0003).
 *  14. Claude transcript tailer recovers per-message usage history exactly
 *      (dedupe by message id, sourced Anthropic estimates, repo linkage,
 *      live-covered sessions skipped, content never persisted).
 *
 * Run: pnpm plimsoll:signal-fidelity-proof
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import {
  branchLinkageHash,
  remoteLinkageHash,
} from "../packages/shared/src/linkage";
import {
  dashboardAccounts,
  dashboardRepoDetail,
  dashboardRepos,
  dashboardSessionDetail,
  dashboardSessions,
  dashboardSummary,
} from "../packages/collector-cli/src/dashboard-api";
import { computeCaptureHealth } from "../packages/collector-cli/src/health";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { MODEL_PRICING } from "../packages/shared/src/pricing";
import { buildPatternsReport, validatedDeliveryYieldV2 } from "./efficiency-report";
import { readLocalIdentities } from "../packages/collector-cli/src/local-identity";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import {
  applyClaudeSettings,
  applyCodexConfig,
  generateClaudeCodeSettings,
  generateCodexConfigToml,
} from "../packages/collector-config/src/index";

type Check = { name: string; passed: boolean; detail: string };

const checks: Check[] = [];
function check(name: string, passed: boolean, detail: string | undefined) {
  checks.push({ name, passed, detail: detail ?? "(no detail)" });
}

const RAW_CMD_SENTINEL = "RAW_CMD_SENTINEL rg -n secret";
const RAW_PATH_SENTINEL = "/Users/sentinel-user/secret-project";
const SESSION = "11111111-2222-4333-8444-555555555555";
const CODEX_SESSION = "019e0000-aaaa-7bbb-8ccc-dddddddddddd";
const PROOF_EMAIL = "sentinel-mailbox@example.com";

function otelAttr(key: string, value: string | number | boolean) {
  if (typeof value === "number" && Number.isInteger(value)) {
    return { key, value: { intValue: String(value) } };
  }
  if (typeof value === "number") {
    return { key, value: { doubleValue: value } };
  }
  if (typeof value === "boolean") {
    return { key, value: { boolValue: value } };
  }
  return { key, value: { stringValue: value } };
}

const claudeLogsEnvelope = {
  resourceLogs: [
    {
      resource: {
        attributes: [otelAttr("service.name", "claude-code"), otelAttr("service.version", "2.1.150")],
      },
      scopeLogs: [
        {
          scope: { name: "com.anthropic.claude_code.events", version: "2.1.150" },
          logRecords: [
            {
              timeUnixNano: "1781400000000000000",
              body: { stringValue: "claude_code.hook_registered" },
              attributes: [
                otelAttr("event.name", "hook_registered"),
                otelAttr("session.id", SESSION),
              ],
            },
            {
              timeUnixNano: "1781400001000000000",
              body: { stringValue: "claude_code.api_request" },
              attributes: [
                otelAttr("event.name", "api_request"),
                otelAttr("user.id", "sha256:proofaccount0001"),
                otelAttr("session.id", SESSION),
                otelAttr("model", "claude-fable-5"),
                otelAttr("input_tokens", 1200),
                otelAttr("output_tokens", 350),
                otelAttr("cache_read_tokens", 9000),
                otelAttr("cache_creation_input_tokens", 4096),
                otelAttr("cost_usd", 0.0421),
                otelAttr("duration_ms", 1800),
                otelAttr("request_id", "req_proof_001"),
              ],
            },
            {
              timeUnixNano: "1781400002000000000",
              body: { stringValue: "claude_code.tool_decision" },
              attributes: [
                otelAttr("event.name", "tool_decision"),
                otelAttr("session.id", SESSION),
                otelAttr("tool_name", "Edit"),
                otelAttr("decision", "accept"),
              ],
            },
          ],
        },
      ],
    },
  ],
};

const claudeMetricsEnvelope = {
  resourceMetrics: [
    {
      resource: { attributes: [otelAttr("service.name", "claude-code")] },
      scopeMetrics: [
        {
          scope: { name: "com.anthropic.claude_code" },
          metrics: [
            {
              name: "claude_code.token.usage",
              sum: {
                isMonotonic: true,
                dataPoints: ["input", "output", "cacheRead"].map((type, index) => ({
                  attributes: [
                    otelAttr("session.id", SESSION),
                    otelAttr("model", "claude-fable-5"),
                    otelAttr("type", type),
                  ],
                  timeUnixNano: "1781400003000000000",
                  asDouble: [1200, 350, 9000][index],
                })),
              },
            },
          ],
        },
      ],
    },
  ],
};

const codexLogsEnvelope = {
  resourceLogs: [
    {
      resource: {
        attributes: [otelAttr("service.name", "codex_exec"), otelAttr("service.version", "0.137.0")],
      },
      scopeLogs: [
        {
          scope: { name: "codex_otel.log_only" },
          logRecords: [
            {
              observedTimeUnixNano: "1781400004000000000",
              attributes: [
                otelAttr("event.name", "codex.tool_result"),
                otelAttr("tool_name", "exec_command"),
                otelAttr("call_id", "call_proof_001"),
                otelAttr(
                  "arguments",
                  JSON.stringify({ cmd: RAW_CMD_SENTINEL, workdir: RAW_PATH_SENTINEL }),
                ),
                otelAttr("duration_ms", "402"),
                otelAttr("success", "true"),
                otelAttr("mcp_server", ""),
                otelAttr("conversation.id", CODEX_SESSION),
                otelAttr("model", "gpt-5.5"),
              ],
            },
          ],
        },
      ],
    },
  ],
};

const codexTracesEnvelope = {
  resourceSpans: [
    {
      resource: { attributes: [otelAttr("service.name", "codex_exec")] },
      scopeSpans: [
        {
          scope: { name: "codex_otel" },
          spans: [
            {
              name: "handle_responses",
              traceId: "abcdef0123456789abcdef0123456789",
              spanId: "abcdef0123456789",
              startTimeUnixNano: "1781400005000000000",
              // Live-verified codex 0.137 shape: usage spans carry tokens only —
              // no conversation.id, no model (those ride sibling signals; see issue 0014).
              attributes: [
                otelAttr("gen_ai.usage.input_tokens", 2400),
                otelAttr("gen_ai.usage.output_tokens", 510),
                otelAttr("gen_ai.usage.cache_read.input_tokens", 1800),
              ],
            },
          ],
        },
      ],
    },
  ],
};

const hookFixtures = [
  { body: { hook_event_name: "PostToolUse", session_id: SESSION, tool_name: "Bash", tool_input: { command: RAW_CMD_SENTINEL } }, expectClass: "shell" },
  { body: { hook_event_name: "PostToolUse", session_id: SESSION, tool_name: "Edit" }, expectClass: "edit" },
  { body: { hook_event_name: "PostToolUse", session_id: SESSION, tool_name: "mcp__github__create_issue" }, expectClass: "mcp" },
  { body: { hook_event_name: "UserPromptSubmit", session_id: SESSION, prompt: "RAW_PROMPT_SENTINEL" }, expectClass: "other" },
];

async function postJson(port: number, route: string, body: unknown, headers: Record<string, string> = {}) {
  const response = await fetch(`http://127.0.0.1:${port}${route}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as Record<string, unknown> };
}

function createTempGitRepo(baseDir: string) {
  const repoDir = path.join(baseDir, "proof-repo");
  fs.mkdirSync(repoDir, { recursive: true });
  const git = (...args: string[]) =>
    spawnSync("git", ["-C", repoDir, "-c", "user.name=proof", "-c", "user.email=proof@local", ...args], {
      encoding: "utf8",
    });
  git("init", "-b", "proof-branch");
  git("remote", "add", "origin", "git@github.com:Proof-Owner/Proof-Repo.git");
  fs.writeFileSync(path.join(repoDir, "file.txt"), "proof\n");
  git("add", ".");
  git("commit", "-m", "proof commit", "--no-gpg-sign", "--no-verify");
  const headSha = git("rev-parse", "HEAD").stdout.trim();
  return { repoDir, headSha };
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "wi-signal-fidelity-"));
  process.env.PLIMSOLL_HOME = tempDir; // keep config writes off the real machine
  const bufferPath = path.join(tempDir, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(bufferPath);
  const config = collectorConfigSchema.parse({});
  const server = createCollectorServer(config, buffer);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as AddressInfo).port;

  // 1-2. Claude logs explode with usage extraction.
  const logsResponse = await postJson(port, "/v1/logs", claudeLogsEnvelope, {
    "x-plimsoll-source": "claude_code",
  });
  check(
    "otlp_logs_envelope_explodes_per_record",
    logsResponse.body.events === 3 && logsResponse.body.recordCount === 3,
    `expected 3 events from 3 records, got ${JSON.stringify(logsResponse.body)}`,
  );

  // 3. Claude metrics datapoints.
  const metricsResponse = await postJson(port, "/v1/metrics", claudeMetricsEnvelope, {
    "x-plimsoll-source": "claude_code",
  });
  check(
    "otlp_metric_datapoints_parsed",
    metricsResponse.body.metricSamples === 3,
    `expected 3 token.usage samples, got ${JSON.stringify(metricsResponse.body)}`,
  );

  // 4. Codex logs + traces.
  await postJson(port, "/v1/logs", codexLogsEnvelope, { "x-plimsoll-source": "codex" });
  const tracesResponse = await postJson(port, "/v1/traces", codexTracesEnvelope, {
    "x-plimsoll-source": "codex",
  });
  check(
    "codex_trace_span_accepted",
    tracesResponse.body.events === 1,
    `expected 1 span event, got ${JSON.stringify(tracesResponse.body)}`,
  );

  // Hooks.
  for (const fixture of hookFixtures) {
    await postJson(port, "/hooks/claude-code", fixture.body);
  }

  // Linkage fixtures: a real temp git repo referenced by hook cwd and by a
  // codex tool_result arguments.workdir (which itself must stay suppressed).
  const proofRepo = createTempGitRepo(tempDir);
  await postJson(port, "/hooks/claude-code", {
    hook_event_name: "SessionStart",
    session_id: SESSION,
    cwd: proofRepo.repoDir,
  });
  const codexLinkageEnvelope = JSON.parse(JSON.stringify(codexLogsEnvelope)) as typeof codexLogsEnvelope;
  const linkageRecord = codexLinkageEnvelope.resourceLogs[0].scopeLogs[0].logRecords[0];
  linkageRecord.attributes = linkageRecord.attributes.map((attribute) =>
    attribute.key === "arguments"
      ? otelAttr("arguments", JSON.stringify({ cmd: RAW_CMD_SENTINEL, workdir: proofRepo.repoDir }))
      : attribute.key === "call_id"
        ? otelAttr("call_id", "call_proof_linkage")
        : attribute,
  );
  await postJson(port, "/v1/logs", codexLinkageEnvelope, { "x-plimsoll-source": "codex" });

  // Settings writes: CSRF-guarded, then a full non-technical roundtrip.
  const noHeader = await fetch(`http://127.0.0.1:${port}/api/settings/priority`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ url: "https://github.com/evil/evil" }),
  });
  const evilOrigin = await fetch(`http://127.0.0.1:${port}/api/settings/priority`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-plimsoll-local": "1",
      origin: "https://evil.example",
    },
    body: JSON.stringify({ url: "https://github.com/evil/evil" }),
  });
  check(
    "settings_writes_csrf_guarded",
    noHeader.status === 403 && evilOrigin.status === 403,
    `no-header=${noHeader.status} evil-origin=${evilOrigin.status}`,
  );

  const localHeaders = { "content-type": "application/json", "x-plimsoll-local": "1" };
  await fetch(`http://127.0.0.1:${port}/api/settings/priority`, {
    method: "POST",
    headers: localHeaders,
    body: JSON.stringify({ url: "https://github.com/Proof-Owner/Other-Repo" }),
  });
  await fetch(`http://127.0.0.1:${port}/api/settings/subscriptions`, {
    method: "POST",
    headers: localHeaders,
    body: JSON.stringify({
      subscriptions: [{ account: "Proof Person", plan: "Max", usdPerMonth: 200, vendor: "anthropic" }],
    }),
  });
  const settingsState = (await fetch(`http://127.0.0.1:${port}/api/settings`).then((r) => r.json())) as {
    priorityRepos: Array<{ url: string }>;
    subscriptions: Array<{ plan: string }>;
  };
  check(
    "settings_roundtrip_no_cli",
    settingsState.priorityRepos.some((row) => row.url === "github.com/proof-owner/other-repo") &&
      settingsState.subscriptions.some((row) => row.plan === "Max"),
    JSON.stringify({ prio: settingsState.priorityRepos.length, subs: settingsState.subscriptions.length }),
  );

  await fetch(`http://127.0.0.1:${port}/api/settings/account-merge`, {
    method: "POST",
    headers: localHeaders,
    body: JSON.stringify({ aliasHash: "sha256:proofalias0001", canonicalHash: "sha256:proofcanon0001" }),
  });
  const aliasState = (await fetch(`http://127.0.0.1:${port}/api/settings`).then((r) => r.json())) as {
    accountAliases: Array<{ aliasHash: string; canonicalHash: string }>;
  };
  await fetch(`http://127.0.0.1:${port}/api/settings/account-merge`, {
    method: "POST",
    headers: localHeaders,
    body: JSON.stringify({ aliasHash: "sha256:proofalias0001", action: "remove" }),
  });
  const aliasCleared = (await fetch(`http://127.0.0.1:${port}/api/settings`).then((r) => r.json())) as {
    accountAliases: Array<{ aliasHash: string }>;
  };
  await fetch(`http://127.0.0.1:${port}/api/settings/account-email`, {
    method: "POST",
    headers: localHeaders,
    body: JSON.stringify({ accountHash: "sha256:proofemail0001", email: "roundtrip@example.com" }),
  });
  const emailState = (await fetch(`http://127.0.0.1:${port}/api/settings`).then((r) => r.json())) as {
    accounts: Array<{ accountHash: string; email: string | null }>;
    detectedIdentities: unknown;
  };
  check(
    "account_email_settings_roundtrip",
    emailState.accounts.some(
      (row) => row.accountHash === "sha256:proofemail0001" && row.email === "roundtrip@example.com",
    ) && Array.isArray(emailState.detectedIdentities),
    JSON.stringify({ stored: emailState.accounts.filter((row) => row.email).length >= 1 }),
  );

  check(
    "account_merge_settings_roundtrip",
    aliasState.accountAliases.some(
      (row) => row.aliasHash === "sha256:proofalias0001" && row.canonicalHash === "sha256:proofcanon0001",
    ) && !aliasCleared.accountAliases.some((row) => row.aliasHash === "sha256:proofalias0001"),
    JSON.stringify({ during: aliasState.accountAliases.length, after: aliasCleared.accountAliases.length }),
  );

  // Dashboard: the display surface reads the same ledger it serves.
  const dashHtml = await fetch(`http://127.0.0.1:${port}/`).then((r) => r.text());
  // The inline script must PARSE: a stray top-level await once killed every
  // fresh page load while all API-level checks stayed green. new Function
  // rejects top-level await exactly like a classic <script> does.
  const inlineScript = dashHtml.split("<script>")[1]?.split("</" + "script>")[0] ?? "";
  let scriptParseError: string | null = null;
  try {
    new Function(inlineScript);
  } catch (error) {
    scriptParseError = String(error);
  }
  check(
    "dashboard_inline_script_parses",
    inlineScript.length > 1000 && scriptParseError === null,
    scriptParseError ?? `script length ${inlineScript.length}`,
  );
  const dashSummary = (await fetch(`http://127.0.0.1:${port}/api/summary`).then((r) => r.json())) as {
    totals: Record<string, number>;
  };
  const dashSession = (await fetch(
    `http://127.0.0.1:${port}/api/session?id=${SESSION}`,
  ).then((r) => r.json())) as { rollup: Record<string, unknown>; receipts: { linkage: unknown[] } };
  check(
    "dashboard_served_locally",
    dashHtml.includes("Plimsoll") && dashHtml.includes("Receipts") && dashHtml.includes("settings"),
    "GET / returns the instrument panel",
  );
  check(
    "dashboard_summary_reads_ledger",
    dashSummary.totals.inputTokens >= 1200 && dashSummary.totals.sessionsWithTokens >= 1,
    JSON.stringify(dashSummary.totals),
  );
  const dashRepos = (await fetch(`http://127.0.0.1:${port}/api/repos`).then((r) => r.json())) as Array<{
    repoHash?: string;
    label?: string;
  }>;
  check(
    "repo_label_displayed_locally",
    dashRepos.some((row) => row.label === "github.com/proof-owner/proof-repo"),
    JSON.stringify(dashRepos.map((row) => row.label)),
  );
  check(
    "dashboard_session_receipts_traceable",
    Boolean(dashSession.rollup) && dashSession.receipts.linkage.length >= 1,
    `linkage rows: ${dashSession.receipts?.linkage?.length}`,
  );

  await new Promise<void>((resolve) => server.close(() => resolve()));

  const rows = buffer.list(1000);
  const apiRequest = rows.find(
    (row) => row.payload.eventType === "assistant_response" && row.source === "claude_code",
  );
  check(
    "claude_api_request_token_attribution",
    Boolean(
      apiRequest &&
        apiRequest.payload.inputTokens === 1200 &&
        apiRequest.payload.outputTokens === 350 &&
        apiRequest.payload.cacheReadTokens === 9000 &&
        apiRequest.payload.costUsd === 0.0421 &&
        apiRequest.payload.model === "claude-fable-5" &&
        apiRequest.payload.sessionId === SESSION,
    ),
    apiRequest ? JSON.stringify({ in: apiRequest.payload.inputTokens, out: apiRequest.payload.outputTokens, cost: apiRequest.payload.costUsd }) : "api_request event missing",
  );

  const codexSpan = rows.find(
    (row) => row.source === "codex" && row.payload.eventType === "assistant_response",
  );
  check(
    "codex_span_token_attribution",
    Boolean(
      codexSpan &&
        codexSpan.payload.inputTokens === 2400 &&
        codexSpan.payload.outputTokens === 510 &&
        codexSpan.payload.cacheReadTokens === 1800,
    ),
    codexSpan ? JSON.stringify({ in: codexSpan.payload.inputTokens, out: codexSpan.payload.outputTokens }) : "codex usage span missing",
  );
  // Reconciler adopts session + model from nearest codex rows, then prices:
  // (2400-1800)*$5 + 1800*$0.50 + 510*$30 per 1M = $0.0192 (gpt-5.5, 2026-06-10).
  check(
    "codex_usage_stitched_and_priced",
    Boolean(
      codexSpan &&
        codexSpan.payload.sessionId === CODEX_SESSION &&
        codexSpan.payload.model === "gpt-5.5" &&
        Math.abs((codexSpan.payload.costUsd ?? 0) - 0.0192) < 0.0001,
    ),
    codexSpan ? JSON.stringify({ session: codexSpan.payload.sessionId, model: codexSpan.payload.model, cost: codexSpan.payload.costUsd }) : "missing",
  );

  const codexTool = rows.find(
    (row) => row.source === "codex" && row.payload.eventType === "tool_result",
  );
  check(
    "codex_tool_action_class_derived",
    Boolean(codexTool && codexTool.payload.actionClass === "shell" && codexTool.payload.sessionId === CODEX_SESSION),
    codexTool ? JSON.stringify({ actionClass: codexTool.payload.actionClass, session: codexTool.payload.sessionId }) : "codex tool event missing",
  );

  const persisted = JSON.stringify(rows);
  check(
    "raw_command_and_path_suppressed",
    !persisted.includes(RAW_CMD_SENTINEL) &&
      !persisted.includes(RAW_PATH_SENTINEL) &&
      !persisted.includes("RAW_PROMPT_SENTINEL") &&
      !persisted.includes(proofRepo.repoDir),
    "no raw cmd/workdir/prompt sentinel or real repo path present in persisted rows",
  );

  const hookClasses = rows
    .filter(
      (row) =>
        row.payload.eventType === "tool_use" &&
        row.source === "claude_code" &&
        typeof (row.payload.metadata as Record<string, unknown>).hook_event_name === "string",
    )
    .map((row) => row.payload.actionClass)
    .sort();
  check(
    "hook_action_class_mapping",
    JSON.stringify(hookClasses) === JSON.stringify(["edit", "mcp", "shell"]),
    `tool_use classes: ${JSON.stringify(hookClasses)}`,
  );

  const actionClassOtherRate =
    rows.filter((row) => row.payload.eventType.startsWith("tool") && row.payload.actionClass === "other").length /
    Math.max(1, rows.filter((row) => row.payload.eventType.startsWith("tool")).length);
  check(
    "tool_events_classified",
    actionClassOtherRate <= 0.1,
    `actionClass=other rate across tool events: ${(actionClassOtherRate * 100).toFixed(1)}%`,
  );

  // Linkage assertions: hook cwd and codex workdir resolve to identical
  // hashed keys that the GitHub importer can reproduce.
  const expectedRemoteHash = remoteLinkageHash("git@github.com:Proof-Owner/Proof-Repo.git");
  const expectedBranchHash = branchLinkageHash("proof-branch");
  const sessionStart = rows.find((row) => row.payload.eventType === "session_start");
  const sessionGit = (sessionStart?.payload.metadata as Record<string, unknown> | undefined)?.git as
    | Record<string, unknown>
    | undefined;
  check(
    "hook_cwd_git_linkage_captured",
    Boolean(
      sessionGit &&
        sessionGit.remoteUrlHash === expectedRemoteHash &&
        sessionGit.branchHash === expectedBranchHash &&
        sessionGit.headSha === proofRepo.headSha,
    ),
    sessionGit ? JSON.stringify(sessionGit) : "session_start git context missing",
  );

  const codexLinkage = rows.find(
    (row) =>
      row.source === "codex" &&
      row.payload.eventType === "tool_result" &&
      (row.payload.metadata as Record<string, unknown>).call_id === "call_proof_linkage",
  );
  const codexGit = (codexLinkage?.payload.metadata as Record<string, unknown> | undefined)?.git as
    | Record<string, unknown>
    | undefined;
  check(
    "codex_workdir_git_linkage_captured",
    Boolean(
      codexGit &&
        codexGit.remoteUrlHash === expectedRemoteHash &&
        codexGit.headSha === proofRepo.headSha,
    ),
    codexGit ? JSON.stringify(codexGit) : "codex tool_result git context missing",
  );

  const linkageColumns = new Database(bufferPath, { readonly: true })
    .prepare(
      `select count(*) as n from buffered_events where repo_hash = ? and branch_hash = ? and head_sha = ?`,
    )
    .get(expectedRemoteHash, expectedBranchHash, proofRepo.headSha) as { n: number };
  check(
    "linkage_promoted_to_columns",
    linkageColumns.n >= 2,
    `rows with repo/branch/sha columns: ${linkageColumns.n}`,
  );

  // Attribution: hashed account promoted (sanitizer re-hashes protected ids,
  // so read the stored value back), machine stamped, label local-only.
  const storedAccount = new Database(bufferPath, { readonly: true })
    .prepare(
      `select account_hash as accountHash, count(*) as n from buffered_events
       where session_id = ? and account_hash is not null and machine = ?
       group by account_hash`,
    )
    .get(SESSION, os.hostname()) as { accountHash: string; n: number } | undefined;
  check(
    "account_and_machine_attributed",
    Boolean(storedAccount && storedAccount.n >= 1 && storedAccount.accountHash.startsWith("sha256:")),
    JSON.stringify({ account: storedAccount?.accountHash, machine: os.hostname(), rows: storedAccount?.n }),
  );

  // Buckets + leverage: proof repo is priority; codex stitched session is unlinked.
  buffer.setPriorityRepo(expectedRemoteHash!, "github.com/proof-owner/proof-repo");
  const accounts = dashboardAccounts(buffer.database, [
    { account: storedAccount?.accountHash ?? "none", plan: "Max", usdPerMonth: 200, vendor: "anthropic" },
  ]);
  const proofAccount = accounts.accounts.find((a) => a.accountHash === storedAccount?.accountHash);
  // Both the claude api_request session and the stitched codex session link to
  // the proof repo, so the full $0.0613 lands in priority — nothing leaks to
  // other/unlinked. Fully-accounted buckets are the strongest assertion.
  check(
    "priority_buckets_computed",
    Math.abs(accounts.buckets.priorityUsd - 0.0613) < 0.001 &&
      accounts.buckets.otherUsd === 0 &&
      accounts.buckets.unlinkedUsd === 0,
    JSON.stringify(accounts.buckets),
  );
  const multiPlan = dashboardAccounts(buffer.database, [
    { account: storedAccount?.accountHash ?? "none", plan: "Max", usdPerMonth: 200, vendor: "anthropic" },
    { account: storedAccount?.accountHash ?? "none", plan: "Pro", usdPerMonth: 100, vendor: "openai" },
  ]);
  const multiRow = multiPlan.accounts.find((row) => row.accountHash === storedAccount?.accountHash);
  check(
    "multi_subscription_leverage_sums_plans",
    Boolean(
      multiRow?.subscription &&
        multiRow.subscription.plan === "Max + Pro" &&
        Math.abs((multiRow.subscription.planCostWindow ?? 0) - 300 * (30 / 30.44)) < 0.5 &&
        (multiRow.subscription as { byVendor?: Array<{ vendor: string }> }).byVendor?.length === 2,
    ),
    JSON.stringify(multiRow?.subscription ?? null),
  );
  check(
    "plan_leverage_computed",
    Boolean(
      proofAccount?.subscription &&
        proofAccount.subscription.leverage !== null &&
        proofAccount.subscription.leverage > 0 &&
        Math.abs((proofAccount.subscription.planCostWindow ?? 0) - 200 * (30 / 30.44)) < 0.5,
    ),
    JSON.stringify(proofAccount?.subscription),
  );

  // 9. Dashboard query soundness. A short-lived second server (same buffer,
  // real ingest path) lands a straddle fixture on SESSION: a second account
  // whose cost dominates but whose STORED hash sorts below the existing proof
  // account's. The sanitizer re-hash is unsalted sha256 (policy.ts), so the
  // stored values are stable: proofaccount-dominant → sha256:25ee3b6c…,
  // proofaccount0001 → sha256:a3f10975…. Lexicographic max() and
  // cost-dominance therefore pick different owners on every run — the
  // attribution check fails on any max(hash) shape. Posted after sections
  // 1-6 so their exact-value assertions stay untouched.
  const straddleAccount = "sha256:proofaccount-dominant";
  const straddleServer = createCollectorServer(config, buffer);
  await new Promise<void>((resolve) => straddleServer.listen(0, "127.0.0.1", () => resolve()));
  const straddlePort = (straddleServer.address() as AddressInfo).port;
  await postJson(
    straddlePort,
    "/v1/logs",
    {
      resourceLogs: [
        {
          resource: { attributes: [otelAttr("service.name", "claude-code")] },
          scopeLogs: [
            {
              scope: { name: "com.anthropic.claude_code.events" },
              logRecords: [
                {
                  timeUnixNano: "1781400007000000000",
                  body: { stringValue: "claude_code.api_request" },
                  attributes: [
                    otelAttr("event.name", "api_request"),
                    otelAttr("user.id", straddleAccount),
                    otelAttr("session.id", SESSION),
                    otelAttr("model", "claude-fable-5"),
                    otelAttr("input_tokens", 90000),
                    otelAttr("output_tokens", 12000),
                    otelAttr("cost_usd", 5.1),
                    otelAttr("request_id", "req_proof_straddle"),
                  ],
                },
                {
                  timeUnixNano: "1781400008000000000",
                  body: { stringValue: "claude_code.api_request" },
                  attributes: [
                    otelAttr("event.name", "api_request"),
                    otelAttr("session.id", "99999999-8888-4777-8666-555555555555"),
                    otelAttr("model", "proof-unknown-model"),
                    otelAttr("input_tokens", 5000),
                    otelAttr("output_tokens", 700),
                    otelAttr("request_id", "req_proof_unpriced"),
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
    { "x-plimsoll-source": "claude_code" },
  );
  await new Promise<void>((resolve) => straddleServer.close(() => resolve()));

  const straddleHashes = buffer.database
    .prepare(
      `select account_hash as hash, sum(coalesce(cost_usd, 0)) as cost from buffered_events
       where session_id = ? and account_hash is not null group by account_hash`,
    )
    .all(SESSION) as Array<{ hash: string; cost: number }>;
  const dominantByCost = [...straddleHashes].sort((a, b) => b.cost - a.cost)[0];
  const lexicographicMax = [...straddleHashes].sort((a, b) => (a.hash < b.hash ? 1 : -1))[0];
  check(
    "straddle_fixture_separates_dominant_from_max",
    straddleHashes.length === 2 && dominantByCost.hash !== lexicographicMax.hash,
    JSON.stringify(straddleHashes),
  );

  const accountsAfterStraddle = dashboardAccounts(buffer.database, []);
  const sessionCost = (
    buffer.database
      .prepare(`select coalesce(sum(cost_usd), 0) as c from buffered_events where session_id = ?`)
      .get(SESSION) as { c: number }
  ).c;
  const dominantRow = accountsAfterStraddle.accounts.find((row) => row.accountHash === dominantByCost.hash);
  check(
    "session_attribution_follows_dominant_cost",
    Boolean(dominantRow && Math.abs(Number(dominantRow.totalUsd) - sessionCost) < 1e-3),
    JSON.stringify({
      expectedOwner: dominantByCost.hash,
      sessionCost,
      rows: accountsAfterStraddle.accounts.map((row) => ({ hash: row.accountHash, usd: row.totalUsd })),
    }),
  );

  let sessionsRows: Array<Record<string, unknown>> = [];
  let sessionsError: string | null = null;
  try {
    sessionsRows = dashboardSessions(buffer.database) as Array<Record<string, unknown>>;
  } catch (error) {
    sessionsError = String(error);
  }
  check(
    "dashboard_sessions_query_executes",
    sessionsError === null && sessionsRows.length >= 2,
    sessionsError ?? `rows: ${sessionsRows.length}`,
  );
  const sessionRow = sessionsRows.find((row) => row.sessionId === SESSION);
  check(
    "session_list_resolves_dominant_repo",
    Boolean(sessionRow && sessionRow.repoHash === expectedRemoteHash),
    JSON.stringify({ repoHash: sessionRow?.repoHash, repoLabel: sessionRow?.repoLabel ?? null }),
  );

  const summaryView = dashboardSummary(buffer.database);
  const reposView = dashboardRepos(buffer.database) as Array<{ costUsd: number }>;
  const sessionDetail = dashboardSessionDetail(buffer.database, SESSION);
  const repoDetail = dashboardRepoDetail(buffer.database, expectedRemoteHash!);
  check(
    "dashboard_detail_queries_execute",
    Boolean(sessionDetail && repoDetail),
    JSON.stringify({ sessionDetail: Boolean(sessionDetail), repoDetail: Boolean(repoDetail) }),
  );

  // Cache-WRITE tokens are a first-class column (issue 0024 / #26): the
  // api_request fixture carries 4096 cache_creation_input_tokens, which must
  // land in the column, in the per-event payload, and in session-detail
  // receipts — the priciest input class can no longer go unrecorded.
  const cacheWriteColumn = (
    buffer.database
      .prepare(
        `select coalesce(sum(cache_creation_tokens), 0) as c from buffered_events where session_id = ?`,
      )
      .get(SESSION) as { c: number }
  ).c;
  const sessionRollup = sessionDetail?.rollup as { cacheCreationTokens?: number } | undefined;
  check(
    "claude_cache_write_first_class",
    cacheWriteColumn === 4096 &&
      apiRequest?.payload.cacheCreationTokens === 4096 &&
      sessionRollup?.cacheCreationTokens === 4096,
    JSON.stringify({
      column: cacheWriteColumn,
      payload: apiRequest?.payload.cacheCreationTokens ?? null,
      receipt: sessionRollup?.cacheCreationTokens ?? null,
    }),
  );

  const totalsCost = Number((summaryView.totals as Record<string, unknown>).costUsd);
  const bySourceCost = (summaryView.bySource as Array<{ costUsd: number }>).reduce((sum, row) => sum + row.costUsd, 0);
  const sessionedCost = (
    buffer.database
      .prepare(`select coalesce(sum(cost_usd), 0) as c from buffered_events where session_id is not null`)
      .get() as { c: number }
  ).c;
  const reposCost = reposView.reduce((sum, row) => sum + row.costUsd, 0);
  const accountsCost = accountsAfterStraddle.accounts.reduce((sum, row) => sum + Number(row.totalUsd ?? 0), 0);
  const bucketsCost =
    accountsAfterStraddle.buckets.priorityUsd +
    accountsAfterStraddle.buckets.otherUsd +
    accountsAfterStraddle.buckets.unlinkedUsd;
  const unpricedRow = (summaryView.byModel as Array<Record<string, unknown>>).find(
    (row) => row.model === "proof-unknown-model",
  );
  check(
    "unpriced_model_distinguished_from_free",
    Boolean(unpricedRow && Number(unpricedRow.unpricedCalls) >= 1 && Number(unpricedRow.costUsd) === 0),
    JSON.stringify(unpricedRow ?? { missing: true }),
  );

  // Aliases merge identities at the root (issue 0023): declare the straddle
  // session's two hashes the same person, canonical = the lexicographic-max
  // account, so the merge visibly moves attribution away from cost-dominance.
  buffer.setAccountAlias(dominantByCost.hash, lexicographicMax.hash);
  const mergedAccounts = dashboardAccounts(buffer.database, []);
  const mergedRow = mergedAccounts.accounts.find((row) => row.accountHash === lexicographicMax.hash);
  const aliasRowGone = !mergedAccounts.accounts.some((row) => row.accountHash === dominantByCost.hash);
  check(
    "account_alias_merges_identities_at_read_time",
    Boolean(mergedRow && aliasRowGone && Math.abs(Number(mergedRow.totalUsd) - sessionCost) < 1e-3),
    JSON.stringify({ mergedTotal: mergedRow?.totalUsd ?? null, aliasRowGone }),
  );
  buffer.removeAccountAlias(dominantByCost.hash);
  const unmergedAccounts = dashboardAccounts(buffer.database, []);
  check(
    "account_alias_removal_restores_split",
    Boolean(
      unmergedAccounts.accounts.find(
        (row) => row.accountHash === dominantByCost.hash && Math.abs(Number(row.totalUsd) - sessionCost) < 1e-3,
      ),
    ),
    "unmerge restored cost-dominant attribution",
  );

  // 9b. Per-event repo attribution (issue 0008): a session that moves between
  // repos must split its cost by WHERE EACH CALL HAPPENED, not lump onto the
  // dominant repo. Token events carry no repo at capture; enrichEventRepos
  // stitches each one to the session's repo at that moment.
  const MR_SESSION = "33334444-5555-4666-8777-888899990000";
  const OTHER_REPO_HASH = remoteLinkageHash("git@github.com:Proof-Owner/Other-Repo.git")!;
  const mrEvent = (id: string, eventType: string, observedAt: string, extra: Record<string, unknown>) =>
    buffer.append(
      aiInteractionEventSchema.parse({
        id,
        tenantId: "local",
        source: "claude_code",
        dataMode: "metadata",
        eventType,
        observedAt,
        sessionId: MR_SESSION,
        actionClass: "other",
        metadata: {},
        ...extra,
      }),
      [],
    );
  mrEvent("mr-hook-a", "tool_use", "2026-06-10T13:00:00.000Z", {
    actionClass: "shell",
    metadata: { git: { remoteUrlHash: expectedRemoteHash, branchHash: expectedBranchHash } },
  });
  mrEvent("mr-cost-a", "assistant_response", "2026-06-10T13:01:00.000Z", {
    model: "claude-fable-5",
    inputTokens: 1000,
    outputTokens: 100,
    costUsd: 1.0,
  });
  mrEvent("mr-hook-b", "tool_use", "2026-06-10T13:02:00.000Z", {
    actionClass: "shell",
    metadata: { git: { remoteUrlHash: OTHER_REPO_HASH, branchHash: expectedBranchHash } },
  });
  mrEvent("mr-cost-b", "assistant_response", "2026-06-10T13:03:00.000Z", {
    model: "claude-fable-5",
    inputTokens: 2000,
    outputTokens: 200,
    costUsd: 2.0,
  });
  const stitchResult = buffer.enrichEventRepos();
  const stitchedRepoRows = buffer.database
    .prepare(`select id, repo_hash as repo from buffered_events where session_id = ? order by observed_at`)
    .all(MR_SESSION) as Array<{ id: string; repo: string | null }>;
  const stitchMap = Object.fromEntries(stitchedRepoRows.map((row) => [row.id, row.repo]));
  check(
    "token_events_stitched_to_session_repo_at_that_moment",
    stitchResult.backward >= 2 &&
      stitchMap["mr-cost-a"] === expectedRemoteHash &&
      stitchMap["mr-cost-b"] === OTHER_REPO_HASH,
    JSON.stringify({ backward: stitchResult.backward, a: stitchMap["mr-cost-a"] === expectedRemoteHash, b: stitchMap["mr-cost-b"] === OTHER_REPO_HASH }),
  );
  const reposSplit = dashboardRepos(buffer.database) as Array<{ repoHash: string | null; costUsd: number; sessions: number }>;
  const otherRepoRow = reposSplit.find((row) => row.repoHash === OTHER_REPO_HASH);
  check(
    "multi_repo_session_cost_splits_per_repo",
    Boolean(otherRepoRow && Math.abs(otherRepoRow.costUsd - 2.0) < 1e-9 && otherRepoRow.sessions === 1),
    JSON.stringify({ otherRepoUsd: otherRepoRow?.costUsd ?? null }),
  );
  check(
    "cross_view_costs_reconcile",
    Math.abs(bySourceCost - totalsCost) < 1e-3 &&
      Math.abs(reposCost - sessionedCost) < 1e-3 &&
      Math.abs(accountsCost - sessionedCost) < 1e-3 &&
      Math.abs(bucketsCost - sessionedCost) < 1e-3,
    JSON.stringify({ totalsCost, bySourceCost, sessionedCost, reposCost, accountsCost, bucketsCost }),
  );

  // 10. Capture health: silence must scream (issue 0021). The baseline for
  // "telemetry should be arriving" is LOCAL TOOL ACTIVITY (transcript and
  // rollout files), so an idle machine stays green and a broken pipe goes red
  // the moment the tools demonstrably run without the ledger hearing it.
  const healthDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-health-"));
  const healthNow = new Date("2026-06-10T12:00:00.000Z");
  const minutesAgo = (m: number) => new Date(healthNow.getTime() - m * 60_000);
  const touch = (file: string, when: Date) => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, "{}\n");
    fs.utimesSync(file, when, when);
  };
  const seedEvent = (
    target: LocalEventBuffer,
    id: string,
    source: string,
    observedAt: Date,
    sessionId: string,
    inputTokens: number | null,
  ) =>
    target.database
      .prepare(
        `insert into buffered_events (id, source, event_type, data_mode, observed_at,
           payload_json, suppressed_fields_json, created_at, session_id, input_tokens)
         values (?, ?, 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, ?)`,
      )
      .run(id, source, observedAt.toISOString(), healthNow.toISOString(), sessionId, inputTokens);
  const healthOptsFor = (scenario: string) => ({
    claudeProjectsDir: path.join(healthDir, scenario, "claude-projects"),
    codexSessionsDir: path.join(healthDir, scenario, "codex-sessions"),
    now: healthNow,
  });

  // Idle: no local activity, empty ledger → green ("quiet is expected").
  const idleBuffer = new LocalEventBuffer(path.join(healthDir, "idle.sqlite"));
  const idleHealth = computeCaptureHealth(idleBuffer.database, healthOptsFor("idle"));
  check(
    "capture_health_idle_is_green",
    idleHealth.overall === "green",
    JSON.stringify(idleHealth.sources.map((row) => [row.source, row.status])),
  );

  // Fresh: local activity minutes old, token-bearing events right behind it.
  const freshOpts = healthOptsFor("fresh");
  touch(path.join(freshOpts.claudeProjectsDir, "proj", "session-a.jsonl"), minutesAgo(4));
  touch(path.join(freshOpts.codexSessionsDir, "2026/06/10", "rollout-a.jsonl"), minutesAgo(4));
  const freshBuffer = new LocalEventBuffer(path.join(healthDir, "fresh.sqlite"));
  seedEvent(freshBuffer, "h-claude-1", "claude_code", minutesAgo(2), "h-claude-sess", 100);
  seedEvent(freshBuffer, "h-codex-1", "codex", minutesAgo(2), "h-codex-sess", 100);
  const freshHealth = computeCaptureHealth(freshBuffer.database, freshOpts);
  check(
    "capture_health_fresh_is_green",
    freshHealth.overall === "green" && freshHealth.sources.every((row) => row.status === "green"),
    JSON.stringify(freshHealth.sources.map((row) => [row.source, row.status, row.reason])),
  );

  // Broken pipe: claude transcript touched 1 minute ago, ledger last heard 40
  // minutes ago → red. Codex side: 3 rollouts today but only 1 token session
  // → amber (the 2026-06-10 1-of-11 failure mode, issue 0022).
  const gapOpts = healthOptsFor("gap");
  touch(path.join(gapOpts.claudeProjectsDir, "proj", "session-b.jsonl"), minutesAgo(1));
  for (const name of ["rollout-a", "rollout-b", "rollout-c"]) {
    touch(path.join(gapOpts.codexSessionsDir, "2026/06/10", `${name}.jsonl`), minutesAgo(95));
  }
  const gapBuffer = new LocalEventBuffer(path.join(healthDir, "gap.sqlite"));
  seedEvent(gapBuffer, "g-claude-1", "claude_code", minutesAgo(40), "g-claude-sess", 100);
  seedEvent(gapBuffer, "g-codex-1", "codex", minutesAgo(95), "g-codex-sess", 100);
  const gapHealth = computeCaptureHealth(gapBuffer.database, gapOpts);
  const gapClaude = gapHealth.sources.find((row) => row.source === "claude_code");
  const gapCodex = gapHealth.sources.find((row) => row.source === "codex");
  check(
    "capture_health_pipe_break_is_red",
    gapHealth.overall === "red" &&
      gapClaude?.status === "red" &&
      gapClaude.reason.includes("not reaching"),
    JSON.stringify({ claude: gapClaude?.reason }),
  );
  check(
    "capture_health_partial_codex_coverage_is_amber",
    gapCodex?.status === "amber" && gapCodex.reason.includes("only 1"),
    JSON.stringify({ codex: gapCodex?.reason }),
  );

  // Total silence with activity: rollouts ran today, ledger heard nothing → red.
  const silentOpts = healthOptsFor("silent");
  touch(path.join(silentOpts.codexSessionsDir, "2026/06/10", "rollout-z.jsonl"), minutesAgo(90));
  const silentBuffer = new LocalEventBuffer(path.join(healthDir, "silent.sqlite"));
  const silentHealth = computeCaptureHealth(silentBuffer.database, silentOpts);
  const silentCodex = silentHealth.sources.find((row) => row.source === "codex");
  check(
    "capture_health_uncaptured_sessions_are_red",
    silentHealth.overall === "red" && silentCodex?.status === "red",
    JSON.stringify({ codex: silentCodex?.reason }),
  );

  idleBuffer.close();
  freshBuffer.close();
  gapBuffer.close();
  silentBuffer.close();
  fs.rmSync(healthDir, { recursive: true, force: true });

  // 11. Codex rollout tailer (issue 0022). Fixtures mirror live codex 0.137
  // rollout shapes. Deltas telescope from cumulative totals; ids are
  // deterministic; sessions already covered by OTLP usage are skipped; no
  // message content is parsed or persisted.
  const ROLLOUT_SESSION = "019e1111-2222-7333-8444-555555555555";
  const ROLLOUT_SENTINEL = "ROLLOUT_PROMPT_SENTINEL do not persist";
  const rolloutDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-rollouts-"));
  const rolloutDay = path.join(rolloutDir, "2026", "06", "10");
  fs.mkdirSync(rolloutDay, { recursive: true });
  const rolloutLine = (timestamp: string, type: string, payload: Record<string, unknown>) =>
    JSON.stringify({ timestamp, type, payload });
  const tokenCountLine = (timestamp: string, totals: Record<string, number>) =>
    rolloutLine(timestamp, "event_msg", {
      type: "token_count",
      info: { total_token_usage: totals, last_token_usage: totals, model_context_window: 258400 },
      rate_limits: { limit_id: "codex", plan_type: "pro" },
    });
  fs.writeFileSync(
    path.join(rolloutDay, `rollout-2026-06-10T10-00-00-${ROLLOUT_SESSION}.jsonl`),
    [
      rolloutLine("2026-06-10T10:00:00.000Z", "session_meta", {
        id: ROLLOUT_SESSION,
        cwd: proofRepo.repoDir,
        originator: "codex_exec",
        cli_version: "0.137.0",
        source: "exec",
      }),
      rolloutLine("2026-06-10T10:00:01.000Z", "turn_context", {
        turn_id: "t-1",
        cwd: proofRepo.repoDir,
        model: "gpt-5.5",
        approval_policy: "never",
      }),
      rolloutLine("2026-06-10T10:00:02.000Z", "event_msg", {
        type: "user_message",
        message: ROLLOUT_SENTINEL,
      }),
      tokenCountLine("2026-06-10T10:00:10.000Z", {
        input_tokens: 1000,
        cached_input_tokens: 200,
        output_tokens: 50,
        reasoning_output_tokens: 10,
        total_tokens: 1050,
      }),
      rolloutLine("2026-06-10T10:00:11.000Z", "event_msg", {
        type: "agent_message",
        message: ROLLOUT_SENTINEL,
      }),
      tokenCountLine("2026-06-10T10:00:20.000Z", {
        input_tokens: 3000,
        cached_input_tokens: 600,
        output_tokens: 130,
        reasoning_output_tokens: 30,
        total_tokens: 3130,
      }),
    ].join("\n") + "\n",
  );
  // Second fixture: a session that already has OTLP-delivered usage (the
  // stitched codex span from section 4) — first-writer-wins, tailer skips it.
  fs.writeFileSync(
    path.join(rolloutDay, `rollout-2026-06-10T11-00-00-${CODEX_SESSION}.jsonl`),
    [
      rolloutLine("2026-06-10T11:00:00.000Z", "session_meta", { id: CODEX_SESSION, cwd: proofRepo.repoDir }),
      tokenCountLine("2026-06-10T11:00:10.000Z", {
        input_tokens: 999999,
        cached_input_tokens: 0,
        output_tokens: 999,
        reasoning_output_tokens: 0,
        total_tokens: 1000998,
      }),
    ].join("\n") + "\n",
  );

  // Local identity fixtures (issue 0028): emails and the codex account id are
  // read from each tool's own config files; nothing is guessed from telemetry.
  const PROOF_CHATGPT_ACCOUNT = "11112222-3333-4444-8555-999900001111";
  const identityDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-identity-"));
  fs.writeFileSync(
    path.join(identityDir, "claude.json"),
    JSON.stringify({ oauthAccount: { emailAddress: PROOF_EMAIL, accountUuid: "cafe0000-1111-4222-8333-444455556666" } }),
  );
  const proofJwt = [
    Buffer.from(JSON.stringify({ alg: "none" })).toString("base64url"),
    Buffer.from(
      JSON.stringify({ "https://api.openai.com/auth": { chatgpt_account_id: PROOF_CHATGPT_ACCOUNT, chatgpt_plan_type: "pro" } }),
    ).toString("base64url"),
    "sig",
  ].join(".");
  fs.writeFileSync(
    path.join(identityDir, "auth.json"),
    JSON.stringify({ email: PROOF_EMAIL, last_refresh: "2026-06-10T11:00:00.000Z", tokens: { id_token: proofJwt } }),
  );
  const proofIdentities = () =>
    readLocalIdentities({
      claudeConfigPath: path.join(identityDir, "claude.json"),
      codexAuthPath: path.join(identityDir, "auth.json"),
    });
  const detectedIdentities = proofIdentities();
  const codexIdentity = detectedIdentities.find((entry) => entry.source === "codex");
  check(
    "local_identities_read_from_tool_configs",
    detectedIdentities.some((entry) => entry.source === "claude_code" && entry.email === PROOF_EMAIL) &&
      Boolean(
        codexIdentity?.actorHash?.startsWith("sha256:") &&
          codexIdentity.email === PROOF_EMAIL &&
          codexIdentity.planType === "pro" &&
          codexIdentity.validFrom === "2026-06-10T11:00:00.000Z",
      ),
    JSON.stringify({ sources: detectedIdentities.map((entry) => entry.source), actor: codexIdentity?.actorHash }),
  );

  const tailer = new RolloutTailer(buffer, rolloutDir, proofIdentities);
  const firstScan = await tailer.scan();
  const rolloutRows = buffer.database
    .prepare(
      `select session_id as sessionId, model, input_tokens as inputTokens,
         cache_read_tokens as cacheReadTokens, output_tokens as outputTokens,
         cost_usd as costUsd, repo_hash as repoHash, payload_json as payloadJson
       from buffered_events where event_type = 'usage_rollout' order by observed_at`,
    )
    .all() as Array<Record<string, unknown>>;
  const rolloutSums = rolloutRows
    .filter((row) => row.sessionId === ROLLOUT_SESSION)
    .reduce<{ input: number; cached: number; output: number; cost: number }>(
      (sum, row) => ({
        input: sum.input + Number(row.inputTokens ?? 0),
        cached: sum.cached + Number(row.cacheReadTokens ?? 0),
        output: sum.output + Number(row.outputTokens ?? 0),
        cost: sum.cost + Number(row.costUsd ?? 0),
      }),
      { input: 0, cached: 0, output: 0, cost: 0 },
    );
  check(
    "rollout_usage_ingested_exact",
    rolloutRows.filter((row) => row.sessionId === ROLLOUT_SESSION).length === 2 &&
      rolloutSums.input === 3000 &&
      rolloutSums.cached === 600 &&
      rolloutSums.output === 130 &&
      rolloutRows.every((row) => row.sessionId !== ROLLOUT_SESSION || row.model === "gpt-5.5") &&
      rolloutSums.cost > 0 &&
      rolloutRows.every(
        (row) => row.sessionId !== ROLLOUT_SESSION || String(row.payloadJson).includes('"costEstimated":true'),
      ),
    JSON.stringify({ events: firstScan.eventsAppended, ...rolloutSums }),
  );
  check(
    "rollout_repo_linkage_from_cwd",
    rolloutRows
      .filter((row) => row.sessionId === ROLLOUT_SESSION)
      .every((row) => row.repoHash === expectedRemoteHash),
    JSON.stringify({ repoHash: rolloutRows[0]?.repoHash, expected: expectedRemoteHash }),
  );
  check(
    "rollout_otlp_covered_session_skipped",
    firstScan.sessionsSkippedOtlpCovered >= 1 &&
      rolloutRows.every((row) => row.sessionId !== CODEX_SESSION),
    JSON.stringify({ skipped: firstScan.sessionsSkippedOtlpCovered }),
  );
  // Clearing the persistent scan state forces a true re-parse — which must
  // not change counts or sums (deterministic ids + insert-or-replace).
  buffer.database.prepare(`delete from rollout_scan_state`).run();
  const rescan = await new RolloutTailer(buffer, rolloutDir, proofIdentities).scan();
  const afterRescan = buffer.database
    .prepare(
      `select count(*) as n, coalesce(sum(input_tokens),0) as input from buffered_events
       where event_type = 'usage_rollout'`,
    )
    .get() as { n: number; input: number };
  check(
    "rollout_rescan_idempotent",
    afterRescan.n === 2 && afterRescan.input === 3000 && rescan.sessionsSkippedOtlpCovered >= 1,
    JSON.stringify({ rows: afterRescan.n, input: afterRescan.input }),
  );
  // Rate-table updates must heal existing rows: a model unpriced at ingest
  // gains cost the moment its rate lands (issue 0025 / GH #32). Only
  // null-cost rows are touched.
  const UNPRICED_SESSION = "019e2222-3333-7444-8555-666666666666";
  fs.writeFileSync(
    path.join(rolloutDay, `rollout-2026-06-10T12-00-00-${UNPRICED_SESSION}.jsonl`),
    [
      rolloutLine("2026-06-10T12:00:00.000Z", "session_meta", { id: UNPRICED_SESSION, cwd: proofRepo.repoDir }),
      rolloutLine("2026-06-10T12:00:01.000Z", "turn_context", { turn_id: "t-u", model: "proof-unpriceable-model" }),
      tokenCountLine("2026-06-10T12:00:10.000Z", {
        input_tokens: 800,
        cached_input_tokens: 0,
        output_tokens: 90,
        reasoning_output_tokens: 0,
        total_tokens: 890,
      }),
    ].join("\n") + "\n",
  );
  const unpricedScan = await new RolloutTailer(buffer, rolloutDir, proofIdentities).scan();
  const unpricedBefore = buffer.database
    .prepare(`select cost_usd as cost from buffered_events where session_id = ? and event_type = 'usage_rollout'`)
    .get(UNPRICED_SESSION) as { cost: number | null } | undefined;
  MODEL_PRICING["proof-unpriceable-model"] = {
    input: 1.0,
    cachedInput: 0.1,
    output: 10.0,
    vendor: "openai",
    asOf: "proof",
  };
  const repriceScan = await new RolloutTailer(buffer, rolloutDir, proofIdentities).scan();
  delete MODEL_PRICING["proof-unpriceable-model"];
  const unpricedAfter = buffer.database
    .prepare(
      `select cost_usd as cost, payload_json as payload from buffered_events
       where session_id = ? and event_type = 'usage_rollout'`,
    )
    .get(UNPRICED_SESSION) as { cost: number | null; payload: string } | undefined;
  check(
    "rate_table_update_reprices_null_cost_rows",
    Boolean(
      unpricedScan.eventsAppended >= 1 &&
        unpricedBefore?.cost == null &&
        repriceScan.repriced >= 1 &&
        unpricedAfter?.cost != null &&
        unpricedAfter.cost > 0 &&
        unpricedAfter.payload.includes('"costEstimated":true'),
    ),
    JSON.stringify({
      before: unpricedBefore?.cost ?? null,
      repriced: repriceScan.repriced,
      after: unpricedAfter?.cost ?? null,
    }),
  );

  const actorRows = buffer.database
    .prepare(
      `select session_id as sid, account_hash as actor from buffered_events where event_type = 'usage_rollout'`,
    )
    .all() as Array<{ sid: string; actor: string | null }>;
  check(
    "codex_identity_stamped_within_honest_window",
    actorRows.filter((row) => row.sid === UNPRICED_SESSION).every((row) => row.actor === codexIdentity?.actorHash) &&
      actorRows.filter((row) => row.sid === UNPRICED_SESSION).length >= 1 &&
      actorRows.filter((row) => row.sid === ROLLOUT_SESSION).every((row) => row.actor === null),
    JSON.stringify({
      stamped: actorRows.filter((row) => row.actor !== null).length,
      preWindowUnstamped: actorRows.filter((row) => row.sid === ROLLOUT_SESSION).every((row) => row.actor === null),
    }),
  );
  const emailRow = buffer.database
    .prepare(`select email from account_labels where account_hash = ?`)
    .get(codexIdentity?.actorHash ?? "") as { email: string | null } | undefined;
  check(
    "codex_identity_email_recorded_locally",
    emailRow?.email === PROOF_EMAIL,
    JSON.stringify({ recorded: Boolean(emailRow?.email) }),
  );
  fs.rmSync(identityDir, { recursive: true, force: true });

  const codexPersisted = JSON.stringify(
    buffer.database.prepare(`select payload_json from buffered_events where source = 'codex'`).all(),
  );
  check(
    "rollout_content_never_persisted",
    !codexPersisted.includes(ROLLOUT_SENTINEL) && !codexPersisted.includes("base_instructions"),
    "rollout message/instruction content absent from all persisted codex rows",
  );
  fs.rmSync(rolloutDir, { recursive: true, force: true });

  // 13. Config apply mode (issue 0003): surgical, backed-up, idempotent.
  const setupDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-setup-"));
  const claudeSettingsPath = path.join(setupDir, "settings.json");
  fs.writeFileSync(
    claudeSettingsPath,
    JSON.stringify({ env: { USER_KEY: "keep-me" }, theme: "dark" }, null, 2),
  );
  const codexConfigPath = path.join(setupDir, "config.toml");
  fs.writeFileSync(codexConfigPath, 'model = "gpt-5.5"\n');
  const applyOptions = { repoRoot: tempDir, port: 49999, dataMode: "metadata" as const };
  const generatedClaude = generateClaudeCodeSettings(applyOptions);
  const generatedToml = generateCodexConfigToml(applyOptions);
  const firstApply = applyClaudeSettings(claudeSettingsPath, generatedClaude);
  const firstCodexApply = applyCodexConfig(codexConfigPath, generatedToml);
  const appliedSettings = JSON.parse(fs.readFileSync(claudeSettingsPath, "utf8")) as {
    env: Record<string, string>;
    theme?: string;
  };
  const secondApply = applyClaudeSettings(claudeSettingsPath, generatedClaude);
  const secondCodexApply = applyCodexConfig(codexConfigPath, generatedToml);
  const codexApplied = fs.readFileSync(codexConfigPath, "utf8");
  check(
    "setup_apply_is_surgical_and_idempotent",
    firstApply.changed &&
      firstCodexApply.changed &&
      appliedSettings.env.USER_KEY === "keep-me" &&
      appliedSettings.theme === "dark" &&
      appliedSettings.env.OTEL_EXPORTER_OTLP_ENDPOINT === "http://127.0.0.1:49999" &&
      codexApplied.includes('model = "gpt-5.5"') &&
      codexApplied.includes("49999") &&
      Boolean(firstApply.backupPath && fs.existsSync(firstApply.backupPath)) &&
      !secondApply.changed &&
      !secondCodexApply.changed,
    JSON.stringify({ firstChanges: firstApply.changes.length, secondChanged: secondApply.changed }),
  );
  const foreignPath = path.join(setupDir, "foreign.toml");
  fs.writeFileSync(foreignPath, '[otel]\nexporter = "elsewhere"\n');
  const conflicted = applyCodexConfig(foreignPath, generatedToml);
  check(
    "setup_never_clobbers_foreign_otel",
    !conflicted.changed &&
      Boolean(conflicted.conflict) &&
      fs.readFileSync(foreignPath, "utf8") === '[otel]\nexporter = "elsewhere"\n',
    JSON.stringify({ conflict: Boolean(conflicted.conflict) }),
  );
  fs.rmSync(setupDir, { recursive: true, force: true });

  // 14. Claude transcript tailer (history reach). Fixtures mirror live
  // transcript shapes; the duplicated message id proves stream/retry dedupe.
  const TRANSCRIPT_SESSION = "44445555-6666-4777-8888-99990000aaaa";
  const TRANSCRIPT_SENTINEL = "TRANSCRIPT_CONTENT_SENTINEL never persist";
  const projectsDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-transcripts-"));
  const projDir = path.join(projectsDir, "-tmp-proof-project");
  fs.mkdirSync(projDir, { recursive: true });
  const tline = (obj: Record<string, unknown>) => JSON.stringify(obj);
  fs.writeFileSync(
    path.join(projDir, `${TRANSCRIPT_SESSION}.jsonl`),
    [
      tline({ type: "user", sessionId: TRANSCRIPT_SESSION, cwd: proofRepo.repoDir, timestamp: "2026-04-10T10:00:00.000Z", message: { role: "user", content: TRANSCRIPT_SENTINEL } }),
      tline({ type: "assistant", sessionId: TRANSCRIPT_SESSION, cwd: proofRepo.repoDir, timestamp: "2026-04-10T10:00:05.000Z", message: { id: "msg_proof_1", model: "claude-fable-5", content: [{ type: "text", text: TRANSCRIPT_SENTINEL }], usage: { input_tokens: 400, cache_read_input_tokens: 50000, cache_creation_input_tokens: 1200, output_tokens: 100 } } }),
      tline({ type: "assistant", sessionId: TRANSCRIPT_SESSION, cwd: proofRepo.repoDir, timestamp: "2026-04-10T10:00:06.000Z", message: { id: "msg_proof_1", model: "claude-fable-5", content: [{ type: "text", text: TRANSCRIPT_SENTINEL }], usage: { input_tokens: 1000, cache_read_input_tokens: 200000, cache_creation_input_tokens: 1500, output_tokens: 500 } } }),
      tline({ type: "assistant", sessionId: TRANSCRIPT_SESSION, cwd: proofRepo.repoDir, timestamp: "2026-04-10T10:01:00.000Z", message: { id: "msg_proof_2", model: "claude-fable-5", content: [], usage: { input_tokens: 200, cache_read_input_tokens: 0, cache_creation_input_tokens: 0, output_tokens: 50 } } }),
    ].join("\n") + "\n",
  );
  // Live-covered session: SESSION already has OTLP token events → skipped.
  fs.writeFileSync(
    path.join(projDir, `${SESSION}.jsonl`),
    tline({ type: "assistant", sessionId: SESSION, timestamp: "2026-06-10T10:00:00.000Z", message: { id: "msg_skip_1", model: "claude-fable-5", usage: { input_tokens: 999, output_tokens: 999 } } }) + "\n",
  );
  const transcriptScan = await new TranscriptTailer(buffer, projectsDir).scan();
  const transcriptRows = buffer.database
    .prepare(
      `select session_id as sid, model, input_tokens as i, cache_read_tokens as c, output_tokens as o,
         cost_usd as cost, repo_hash as repo, payload_json as payload
       from buffered_events where event_type = 'usage_transcript' order by observed_at`,
    )
    .all() as Array<Record<string, unknown>>;
  const tRows = transcriptRows.filter((row) => row.sid === TRANSCRIPT_SESSION);
  const tSum = tRows.reduce<{ i: number; c: number; o: number; cost: number }>(
    (sum, row) => ({ i: sum.i + Number(row.i), c: sum.c + Number(row.c), o: sum.o + Number(row.o), cost: sum.cost + Number(row.cost ?? 0) }),
    { i: 0, c: 0, o: 0, cost: 0 },
  );
  // fable rates (sourced 2026-06-10), now INCLUDING cache writes (issue 0024 /
  // #26 retired the floor): msg1(last write wins)=1000in + 200k reads + 1500
  // cache-writes + 500out → (1000*10 + 200000*1 + 1500*12.5 + 500*50)/1e6 =
  // 0.25375; msg2 = (200*10 + 50*50)/1e6 = 0.0045; total 0.25825.
  check(
    "transcript_usage_ingested_exact_and_deduped",
    tRows.length === 2 &&
      tSum.i === 1200 &&
      tSum.c === 200000 &&
      tSum.o === 550 &&
      Math.abs(tSum.cost - 0.25825) < 1e-6 &&
      tRows.every((row) => row.repo === expectedRemoteHash) &&
      tRows.every((row) => String(row.payload).includes('"costEstimated":true')),
    JSON.stringify({ rows: tRows.length, ...tSum }),
  );
  check(
    "transcript_live_covered_session_skipped",
    transcriptScan.sessionsSkippedLiveCovered >= 1 &&
      !transcriptRows.some((row) => row.sid === SESSION),
    JSON.stringify({ skipped: transcriptScan.sessionsSkippedLiveCovered }),
  );
  const transcriptRescan = await new TranscriptTailer(buffer, projectsDir).scan();
  const transcriptPersisted = JSON.stringify(
    buffer.database.prepare(`select payload_json from buffered_events where source = 'claude_code'`).all(),
  );
  check(
    "transcript_rescan_idempotent_and_content_free",
    transcriptRescan.eventsAppended === 0 && !transcriptPersisted.includes("TRANSCRIPT_CONTENT_SENTINEL"),
    JSON.stringify({ rescanAppended: transcriptRescan.eventsAppended }),
  );
  fs.rmSync(projectsDir, { recursive: true, force: true });

  // Descriptive patterns report (issue 0010 / #10): the four required blocks
  // render over the seeded ledger, and the output carries ZERO advice language
  // — the open/paid boundary is that this tier describes, never prescribes.
  const patterns = buildPatternsReport(buffer.database, 3650);
  const requiredBlocks = [
    "## Tokens & cost by model",
    "## Cache-read ratio by model",
    "## Action-class distribution",
    "## Top sessions by cost",
  ];
  const adviceWords =
    /\b(should|recommend|recommended|consider|suggest|optimi[sz]e|improve|reduce|increase|better|best practice|you (?:could|might|can)|try to)\b/i;
  const adviceHit = adviceWords.exec(patterns);
  check(
    "patterns_report_descriptive_only",
    requiredBlocks.every((b) => patterns.includes(b)) && !adviceHit,
    JSON.stringify({
      blocks: requiredBlocks.filter((b) => patterns.includes(b)).length,
      adviceLanguage: adviceHit ? adviceHit[0] : "none",
    }),
  );

  // Validated Delivery Yield v2 (issue 0009 / #9): a known-reverted PR inside
  // the stability window drops from the numerator and is named; a clean PR and
  // a revert landing AFTER the window both survive.
  const v2MergeAt = "2026-05-01T00:00:00.000Z";
  const v2 = validatedDeliveryYieldV2(
    [
      { pull: 101, mergedAt: v2MergeAt }, // reverted 2 days later → drops
      { pull: 102, mergedAt: v2MergeAt }, // clean → survives
      { pull: 103, mergedAt: v2MergeAt }, // reverted 40 days later → outside window, survives
    ],
    [
      { pull: 101, kind: "revert", evidence: "revert deadbeef0", at: "2026-05-03T00:00:00.000Z" },
      { pull: 103, kind: "revert", evidence: "revert cafef00d0", at: "2026-06-10T00:00:00.000Z" },
    ],
    14,
  );
  check(
    "validated_delivery_yield_v2_drops_reverted_pr",
    v2.numerator === 2 &&
      v2.excluded.length === 1 &&
      v2.excluded[0].pull === 101 &&
      v2.excluded[0].reason === "revert" &&
      v2.excluded[0].evidence.includes("deadbeef0"),
    JSON.stringify(v2),
  );

  // 7. Upload watermark drains oldest-first against a stub ingest endpoint.
  const received: number[] = [];
  const uploadBodies: string[] = [];
  const stub = http.createServer((request, response) => {
    let body = "";
    request.on("data", (chunk) => (body += chunk));
    request.on("end", () => {
      uploadBodies.push(body);
      received.push((JSON.parse(body).events as unknown[]).length);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ accepted: true }));
    });
  });
  await new Promise<void>((resolve) => stub.listen(0, "127.0.0.1", () => resolve()));
  const stubPort = (stub.address() as AddressInfo).port;
  const uploadConfig = collectorConfigSchema.parse({
    uploadUrl: `http://127.0.0.1:${stubPort}/ingest`,
  });
  const before = buffer.stats().unuploadedCount;
  const first = await uploadBufferedEvents(uploadConfig, buffer, { limit: 4 });
  const second = await uploadBufferedEvents(uploadConfig, buffer, { limit: 500 });
  await new Promise<void>((resolve) => stub.close(() => resolve()));
  check(
    "upload_watermark_drains",
    before > 0 &&
      first.markedUploaded === 4 &&
      second.remainingUnuploaded === 0 &&
      buffer.stats().unuploadedCount === 0,
    JSON.stringify({ before, firstMarked: first.markedUploaded, after: buffer.stats().unuploadedCount, batches: received }),
  );

  check(
    "repo_label_never_uploaded",
    uploadBodies.length > 0 && uploadBodies.every((body) => !body.includes("proof-owner/proof-repo")),
    `checked ${uploadBodies.length} upload bodies for the local-only label`,
  );
  check(
    "machine_and_account_label_never_uploaded",
    uploadBodies.every(
      (body) => !body.includes(os.hostname()) && !body.includes(os.userInfo().username),
    ),
    "hostname/username absent from all upload bodies",
  );
  check(
    "account_email_never_uploaded",
    uploadBodies.length > 0 && uploadBodies.every((body) => !body.includes(PROOF_EMAIL)),
    "raw account email absent from all upload bodies (lives in account_labels only)",
  );

  // 8. Retention prune: uploaded rows age out; local history NEVER does.
  await new Promise((resolve) => setTimeout(resolve, 5));
  buffer.append(
    aiInteractionEventSchema.parse({
      id: "prune-survivor-0000",
      tenantId: "local",
      source: "claude_code",
      dataMode: "metadata",
      eventType: "usage_transcript",
      observedAt: "2020-01-01T00:00:00.000Z",
      sessionId: "99990000-1111-4222-8333-444455556666",
      actionClass: "other",
      inputTokens: 1,
      outputTokens: 1,
      metadata: { usageSource: "transcript" },
    }),
    [],
  );
  const pruned = buffer.prune(0);
  const survivor = buffer.database
    .prepare(`select count(*) as n from buffered_events where uploaded_at is null`)
    .get() as { n: number };
  check(
    "retention_prune_spares_unuploaded_history",
    pruned.events > 0 && survivor.n === 1 && buffer.stats().count === 1,
    JSON.stringify({ pruned: pruned.events, unuploadedSurvivors: survivor.n }),
  );

  buffer.close();
  fs.rmSync(tempDir, { recursive: true, force: true });

  const passed = checks.every((entry) => entry.passed);
  const generatedAt = new Date().toISOString();
  const stamp = generatedAt.replace(/[:.]/g, "-");
  const evidenceDir = path.join(process.cwd(), "evidence");
  fs.mkdirSync(evidenceDir, { recursive: true });
  const artifact = {
    proof: "plimsoll-signal-fidelity",
    generatedAt,
    passed,
    checks,
    context: {
      audit: "https://github.com/CryptoJym/ai-costs-review (private audit, summarized in README)",
      findings: ["F1 otlp flattening", "F2 metric datapoints", "F3 codex usage", "F4 actionClass", "F6 upload drain", "F8 codex arguments leak"],
      liveVerificationPending: [
        "codex 0.137 span attribute names against a real session (fixture mirrors codex-rs/otel main)",
      ],
    },
  };
  fs.writeFileSync(path.join(evidenceDir, `${stamp}-signal-fidelity.json`), `${JSON.stringify(artifact, null, 2)}\n`);
  fs.writeFileSync(
    path.join(evidenceDir, `${stamp}-signal-fidelity.md`),
    [
      "# Work Intelligence Signal-Fidelity Proof",
      "",
      `Generated: ${generatedAt}`,
      `Overall: ${passed ? "PASSED" : "FAILED"}`,
      "",
      "| Check | Result | Detail |",
      "|---|---|---|",
      ...checks.map((entry) => `| ${entry.name} | ${entry.passed ? "pass" : "FAIL"} | ${entry.detail.replace(/\|/g, "\\|")} |`),
      "",
    ].join("\n"),
  );

  console.log(JSON.stringify(artifact, null, 2));
  if (!passed) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
