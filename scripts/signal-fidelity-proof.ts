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
import os from "node:os";

import {
  branchLinkageHash,
  remoteLinkageHash,
} from "../packages/shared/src/linkage";
import { dashboardAccounts } from "../packages/collector-cli/src/dashboard-api";

type Check = { name: string; passed: boolean; detail: string };

const checks: Check[] = [];
function check(name: string, passed: boolean, detail: string | undefined) {
  checks.push({ name, passed, detail: detail ?? "(no detail)" });
}

const RAW_CMD_SENTINEL = "RAW_CMD_SENTINEL rg -n secret";
const RAW_PATH_SENTINEL = "/Users/sentinel-user/secret-project";
const SESSION = "11111111-2222-4333-8444-555555555555";
const CODEX_SESSION = "019e0000-aaaa-7bbb-8ccc-dddddddddddd";

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

  // Dashboard: the display surface reads the same ledger it serves.
  const dashHtml = await fetch(`http://127.0.0.1:${port}/`).then((r) => r.text());
  const dashSummary = (await fetch(`http://127.0.0.1:${port}/api/summary`).then((r) => r.json())) as {
    totals: Record<string, number>;
  };
  const dashSession = (await fetch(
    `http://127.0.0.1:${port}/api/session?id=${SESSION}`,
  ).then((r) => r.json())) as { rollup: Record<string, unknown>; receipts: { linkage: unknown[] } };
  check(
    "dashboard_served_locally",
    dashHtml.includes("Plimsoll") && dashHtml.includes("Receipts"),
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

  // 8. Retention prune.
  await new Promise((resolve) => setTimeout(resolve, 5));
  const pruned = buffer.prune(0);
  check(
    "retention_prune_deletes_aged_rows",
    pruned.events > 0 && buffer.stats().count === 0,
    JSON.stringify(pruned),
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
