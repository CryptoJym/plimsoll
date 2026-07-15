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
 *  15. Fleet join (issue 0016): `plimsoll join` writes sync credentials into
 *      collector.config.json only when the server accepts the token, the
 *      handshake upload rides the real signed sync path, and a refused
 *      token leaves the config byte-identical with a clear reason.
 *  16. Workspace backfill (issue 0035): `upload-history` pushes the FULL
 *      ledger history read-only through the signed ingest path — stitch-null
 *      payloads are repaired, non-UUID ids derive deterministically, batches
 *      obey the ≤500/byte contract, the reconciliation audit's math is honest
 *      (unpriced never renders as $0.00), forbidden metadata never crosses the
 *      wire, a second full run grows the workspace by exactly zero rows, an
 *      interrupted run resumes from the watermark without double-counting,
 *      and an unjoined machine is refused before any network.
 *
 * Run: pnpm plimsoll:signal-fidelity-proof
 */
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigPath, collectorConfigSchema } from "../packages/collector-cli/src/config";
import { performJoin } from "../packages/collector-cli/src/join";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import {
  buildRepoLabelCandidates,
  parseRepoSlug,
  pushRepoLabels,
  renderRepoLabelPreview,
} from "../packages/collector-cli/src/repo-labels";
import { attachRepoLinkage } from "../packages/collector-cli/src/upload";
import {
  buildAttributionRepairRows,
  chunkHistoryEnvelopes,
  createHistoryAudit,
  ensureUuidEventId,
  historyAuditTotals,
  normalizeHistoryEvent,
  readBackfillState,
  recordHistoryEligible,
  recordHistorySkip,
  renderHistoryAudit,
  runAttributionRepair,
  runWorkspaceHistoryUpload,
} from "../packages/collector-cli/src/upload-history";
import {
  buildSessionSyncRow,
  collectSessionSnapshots,
  ensureUuidSessionId,
  runSessionSync,
  sessionIdsFromBatches,
} from "../packages/collector-cli/src/session-sync";
import {
  buildOutcomePush,
  collectSessionLinks,
  joinSessionsToPulls,
  reworkSignalsInWindow,
  runOutcomesSync,
  type PullOutcome,
} from "../packages/collector-cli/src/outcomes-sync";
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
import {
  aiInteractionEventSchema,
  aiWorkIngestBatchSchema,
  aiWorkSessionSyncBatchSchema,
  findForbiddenRawContentFields,
  type AiInteractionEvent,
  type AiWorkSessionSyncRow,
} from "../packages/shared/src/index";
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

const SYSTEM_DATE_NOW = Date.now.bind(Date);
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_PROOF_NOW = "2026-06-15T12:00:00.000Z";

class ProofClock {
  private currentMs: number;

  constructor(nowIso: string) {
    this.currentMs = ProofClock.parse(nowIso);
  }

  private static parse(nowIso: string) {
    if (!/(?:Z|[+-]\d{2}:\d{2})$/.test(nowIso)) {
      throw new Error(`PLIMSOLL_PROOF_NOW must include an explicit timezone: ${nowIso}`);
    }
    const parsed = Date.parse(nowIso);
    if (!Number.isFinite(parsed)) throw new Error(`Invalid PLIMSOLL_PROOF_NOW: ${nowIso}`);
    return parsed;
  }

  nowMs() {
    return this.currentMs;
  }

  iso(offsetMs = 0) {
    return new Date(this.currentMs + offsetMs).toISOString();
  }

  unixNanos(offsetMs = 0) {
    return String(BigInt(this.currentMs + offsetMs) * 1_000_000n);
  }
}

// The proof owns its clock. Production modules are unmodified; only this test
// process receives a scoped Date.now() while window-sensitive checks run, so
// dashboard fixtures cannot drift while retention/sync keep real semantics.
const proofClock = new ProofClock(process.env.PLIMSOLL_PROOF_NOW ?? DEFAULT_PROOF_NOW);
const SIGNAL_BASE_OFFSET_MS = -DAY_MS;

function installProofDateNow() {
  const previous = Date.now;
  Date.now = () => proofClock.nowMs();
  return () => {
    Date.now = previous;
  };
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
              timeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS),
              body: { stringValue: "claude_code.hook_registered" },
              attributes: [
                otelAttr("event.name", "hook_registered"),
                otelAttr("session.id", SESSION),
              ],
            },
            {
              timeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 1_000),
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
              timeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 2_000),
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
                  timeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 3_000),
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
              observedTimeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 4_000),
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
              startTimeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 5_000),
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
  {
    body: {
      hook_event_name: "PostToolUse",
      session_id: SESSION,
      timestamp: proofClock.iso(SIGNAL_BASE_OFFSET_MS + 10_000),
      tool_name: "Bash",
      tool_input: { command: RAW_CMD_SENTINEL },
    },
    expectClass: "shell",
  },
  {
    body: {
      hook_event_name: "PostToolUse",
      session_id: SESSION,
      timestamp: proofClock.iso(SIGNAL_BASE_OFFSET_MS + 11_000),
      tool_name: "Edit",
    },
    expectClass: "edit",
  },
  {
    body: {
      hook_event_name: "PostToolUse",
      session_id: SESSION,
      timestamp: proofClock.iso(SIGNAL_BASE_OFFSET_MS + 12_000),
      tool_name: "mcp__github__create_issue",
    },
    expectClass: "mcp",
  },
  {
    body: {
      hook_event_name: "UserPromptSubmit",
      session_id: SESSION,
      timestamp: proofClock.iso(SIGNAL_BASE_OFFSET_MS + 13_000),
      prompt: "RAW_PROMPT_SENTINEL",
    },
    expectClass: "other",
  },
];

type ProofClockWindowResult = {
  passed: boolean;
  signature: {
    events: number;
    inputTokens: number;
    sessions: number;
    sessionRows: number;
    repoCostUsd: number;
    priorityUsd: number;
  };
};

function proofClockWindowFixture(): ProofClockWindowResult {
  const buffer = new LocalEventBuffer(":memory:");
  const sessionId = "88888888-7777-4666-8555-444444444444";
  const repoHash = remoteLinkageHash("git@github.com:Proof-Owner/Clock-Regression.git")!;
  try {
    buffer.append(
      aiInteractionEventSchema.parse({
        id: "77777777-6666-4555-8444-333333333333",
        tenantId: "local",
        source: "codex",
        dataMode: "metadata",
        eventType: "assistant_response",
        observedAt: proofClock.iso(-5 * DAY_MS),
        sessionId,
        actorId: "sha256:proof-clock-account",
        actionClass: "other",
        model: "gpt-5.5",
        inputTokens: 1234,
        outputTokens: 56,
        costUsd: 1.25,
        metadata: { git: { remoteUrlHash: repoHash } },
      }),
      [],
    );
    buffer.setPriorityRepo(repoHash, "github.com/proof-owner/clock-regression");

    const summary = dashboardSummary(buffer.database);
    const sessions = dashboardSessions(buffer.database);
    const repos = dashboardRepos(buffer.database);
    const accounts = dashboardAccounts(buffer.database, []);
    const repo = repos.find((row) => row.repoHash === repoHash);
    const signature = {
      events: Number(summary.totals.events),
      inputTokens: Number(summary.totals.inputTokens),
      sessions: Number(summary.totals.sessions),
      sessionRows: sessions.length,
      repoCostUsd: Number(repo?.costUsd ?? 0),
      priorityUsd: accounts.buckets.priorityUsd,
    };
    return {
      passed:
        signature.events === 1 &&
        signature.inputTokens === 1234 &&
        signature.sessions === 1 &&
        signature.sessionRows === 1 &&
        Math.abs(signature.repoCostUsd - 1.25) < 1e-9 &&
        Math.abs(signature.priorityUsd - 1.25) < 1e-9,
      signature,
    };
  } finally {
    buffer.close();
  }
}

function proofClockMatrix() {
  const script = process.argv[1];
  const tsx = path.join(process.cwd(), "node_modules", ".bin", "tsx");
  const cases = [
    { now: "2026-06-15T12:00:00.000Z", tz: "UTC" },
    { now: "2028-06-15T12:00:00.000Z", tz: "UTC" },
    { now: "2026-06-15T12:00:00.000Z", tz: "America/Denver" },
    { now: "2028-06-15T12:00:00.000Z", tz: "America/Denver" },
  ];
  const results = cases.map((entry) => {
    const child = spawnSync(tsx, [script], {
      encoding: "utf8",
      env: {
        ...process.env,
        TZ: entry.tz,
        PLIMSOLL_PROOF_NOW: entry.now,
        PLIMSOLL_PROOF_CLOCK_CASE: "1",
      },
    });
    let result: ProofClockWindowResult | null = null;
    try {
      result = JSON.parse(child.stdout.trim()) as ProofClockWindowResult;
    } catch {
      // The caller reports the stderr/stdout below; malformed output is a failure.
    }
    return {
      ...entry,
      status: child.status,
      result,
      stderr: child.stderr.trim(),
      stdout: child.stdout.trim(),
    };
  });
  const signatures = results.map((entry) => JSON.stringify(entry.result?.signature ?? null));
  check(
    "proof_clock_window_stable_across_years",
    results.slice(0, 2).every((entry) => entry.status === 0 && entry.result?.passed) &&
      signatures[0] === signatures[1],
    JSON.stringify(
      results
        .slice(0, 2)
        .map(({ now, tz, status, result, stderr }) => ({ now, tz, status, result, stderr })),
    ),
  );
  check(
    "proof_clock_window_timezone_invariant",
    results.every((entry) => entry.status === 0 && entry.result?.passed) &&
      new Set(signatures).size === 1,
    JSON.stringify(
      results.map(({ now, tz, status, result, stderr }) => ({ now, tz, status, result, stderr })),
    ),
  );
}

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
  proofClockMatrix();
  const restoreProofDateNow = installProofDateNow();
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
    timestamp: proofClock.iso(SIGNAL_BASE_OFFSET_MS + 14_000),
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
                  timeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 7_000),
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
                  timeUnixNano: proofClock.unixNanos(SIGNAL_BASE_OFFSET_MS + 8_000),
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
  mrEvent("mr-hook-a", "tool_use", proofClock.iso(SIGNAL_BASE_OFFSET_MS + 60 * 60_000), {
    actionClass: "shell",
    metadata: { git: { remoteUrlHash: expectedRemoteHash, branchHash: expectedBranchHash } },
  });
  mrEvent("mr-cost-a", "assistant_response", proofClock.iso(SIGNAL_BASE_OFFSET_MS + 61 * 60_000), {
    model: "claude-fable-5",
    inputTokens: 1000,
    outputTokens: 100,
    costUsd: 1.0,
  });
  mrEvent("mr-hook-b", "tool_use", proofClock.iso(SIGNAL_BASE_OFFSET_MS + 62 * 60_000), {
    actionClass: "shell",
    metadata: { git: { remoteUrlHash: OTHER_REPO_HASH, branchHash: expectedBranchHash } },
  });
  mrEvent("mr-cost-b", "assistant_response", proofClock.iso(SIGNAL_BASE_OFFSET_MS + 63 * 60_000), {
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
  restoreProofDateNow();

  // 10. Capture health: silence must scream (issue 0021). The baseline for
  // "telemetry should be arriving" is LOCAL TOOL ACTIVITY (transcript and
  // rollout files), so an idle machine stays green and a broken pipe goes red
  // the moment the tools demonstrably run without the ledger hearing it.
  const healthDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-health-"));
  const healthNow = new Date(proofClock.nowMs());
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

  // 15. Fleet join (issue 0016): credentials land in collector.config.json
  // only on acceptance; the handshake rides the REAL signed sync path; a
  // refused token leaves the config byte-identical with a clear reason.
  const joinHome = path.join(tempDir, "join-home");
  fs.mkdirSync(joinHome, { recursive: true });
  const previousPlimsollHome = process.env.PLIMSOLL_HOME;
  process.env.PLIMSOLL_HOME = joinHome; // collectorHome() prefers the env var — keep the real config out of reach
  try {
    const JOIN_TENANT = "33333333-4444-4555-8666-777777777777";
    const JOIN_INSTALL_KEY = "pli_proofproofproofproofproofproo";
    const JOIN_SECRET = "proof-join-signing-secret-0123456789";
    let joinPort = 0;
    const joinRequests: Array<{ url: string; body: string }> = [];
    const handshakeHeaders: Array<NodeJS.Dict<string | string[]>> = [];
    const handshakeBodies: string[] = [];
    const joinServer = http.createServer((request, response) => {
      let body = "";
      request.on("data", (chunk) => (body += chunk));
      request.on("end", () => {
        joinRequests.push({ url: request.url ?? "", body });
        if (request.url?.startsWith("/api/work-intelligence/join")) {
          const token = (JSON.parse(body) as { token?: string }).token ?? "";
          if (token.endsWith("expired-fixture")) {
            response.writeHead(410, { "content-type": "application/json" });
            response.end(JSON.stringify({ error: "join_refused", reason: "expired" }));
            return;
          }
          response.writeHead(201, { "content-type": "application/json" });
          response.end(
            JSON.stringify({
              ok: true,
              tenantId: JOIN_TENANT,
              installKey: JOIN_INSTALL_KEY,
              uploadUrl: `http://127.0.0.1:${joinPort}/api/work-intelligence/ingest`,
              uploadSigningSecret: JOIN_SECRET,
            }),
          );
          return;
        }
        handshakeHeaders.push(request.headers);
        handshakeBodies.push(body);
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, accepted: (JSON.parse(body).events as unknown[]).length }));
      });
    });
    await new Promise<void>((resolve) => joinServer.listen(0, "127.0.0.1", () => resolve()));
    joinPort = (joinServer.address() as AddressInfo).port;

    const joined = await performJoin({ target: `http://127.0.0.1:${joinPort}#pljt_proofproofproofproofproofproofpr` });
    const writtenConfig = JSON.parse(fs.readFileSync(collectorConfigPath(), "utf8")) as Record<string, unknown>;
    const handshakeBatch = handshakeBodies[0] ? (JSON.parse(handshakeBodies[0]) as Record<string, unknown>) : {};
    check(
      "join_writes_config_and_handshake_syncs",
      joined.joined === true &&
        writtenConfig.uploadUrl === `http://127.0.0.1:${joinPort}/api/work-intelligence/ingest` &&
        writtenConfig.installKey === JOIN_INSTALL_KEY &&
        writtenConfig.tenantId === JOIN_TENANT &&
        writtenConfig.uploadSigningSecret === JOIN_SECRET &&
        handshakeBatch.installKey === JOIN_INSTALL_KEY &&
        handshakeBatch.tenantId === JOIN_TENANT &&
        (handshakeBatch.events as unknown[]).length > 0 &&
        handshakeHeaders[0]?.["x-plimsoll-install-key"] === JOIN_INSTALL_KEY &&
        String(handshakeHeaders[0]?.["x-plimsoll-upload-signature"] ?? "").startsWith("sha256=") &&
        joined.handshake.signedUpload === true &&
        joined.handshake.uploadedEvents > 0,
      JSON.stringify({
        configFields: ["uploadUrl", "installKey", "tenantId", "uploadSigningSecret"].filter((f) => f in writtenConfig),
        handshakeEvents: joined.joined ? joined.handshake.uploadedEvents : 0,
        signed: joined.joined ? joined.handshake.signedUpload : false,
      }),
    );

    const configSnapshot = fs.readFileSync(collectorConfigPath(), "utf8");
    const refused = await performJoin({ target: `http://127.0.0.1:${joinPort}#pljt_expired-fixture` });
    const configAfterRefusal = fs.readFileSync(collectorConfigPath(), "utf8");
    check(
      "join_refused_token_clear_error_config_untouched",
      refused.joined === false &&
        refused.reason === "expired" &&
        refused.httpStatus === 410 &&
        /expired/.test(refused.message) &&
        /admin/.test(refused.message) &&
        configAfterRefusal === configSnapshot,
      JSON.stringify({
        reason: refused.joined === false ? refused.reason : null,
        configByteIdentical: configAfterRefusal === configSnapshot,
      }),
    );

    await new Promise<void>((resolve) => joinServer.close(() => resolve()));
  } finally {
    if (previousPlimsollHome === undefined) {
      delete process.env.PLIMSOLL_HOME;
    } else {
      process.env.PLIMSOLL_HOME = previousPlimsollHome;
    }
  }

  // 16. Workspace backfill (issue 0035): upload-history pushes the FULL
  // ledger history, read-only, idempotent by event id, with an honest
  // reconciliation audit. Pure pieces first, then a loopback end-to-end
  // against a stub workspace that verifies signatures exactly like the cloud.
  {
    // 16a. Row → envelope normalization: the reconcileCodexUsage stitch
    // artifact (json_set writes `sessionId: null`) must repair into a
    // schema-valid envelope without inventing values; a null REQUIRED field
    // stays a skip with a reason.
    const stitchArtifact = JSON.stringify({
      id: "9c1c1111-2222-5333-9444-555555555555",
      sessionId: null,
      model: null,
      costUsd: null,
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: "2026-03-01T00:00:00.000Z",
      intent: "unknown",
      actionClass: "other",
      inputTokens: 120,
      outputTokens: 34,
      metadata: { stitched: null },
    });
    const repaired = normalizeHistoryEvent({ payloadJson: stitchArtifact, suppressedFieldsJson: '["arguments"]' });
    const nullRequired = normalizeHistoryEvent({
      payloadJson: JSON.stringify({ id: "11111111-2222-5333-9444-555555555555", source: "codex", eventType: "assistant_response", observedAt: null }),
      suppressedFieldsJson: "[]",
    });
    const unparseable = normalizeHistoryEvent({ payloadJson: "{not json", suppressedFieldsJson: "[]" });
    check(
      "history_normalize_repairs_stitch_nulls_honestly",
      repaired.ok &&
        !("sessionId" in repaired.envelope.event) &&
        !("model" in repaired.envelope.event) &&
        repaired.envelope.event.costUsd === undefined &&
        repaired.envelope.event.inputTokens === 120 &&
        repaired.envelope.event.outputTokens === 34 &&
        repaired.envelope.suppressedFields.includes("arguments") &&
        aiInteractionEventSchema.safeParse(repaired.envelope.event).success &&
        !nullRequired.ok &&
        nullRequired.reason === "schema_invalid" &&
        !unparseable.ok &&
        unparseable.reason === "payload_unparseable",
      JSON.stringify({
        repairedKeysDropped: repaired.ok ? ["sessionId", "model", "costUsd"].filter((key) => !(key in repaired.envelope.event)) : [],
        nullRequired: nullRequired.ok ? null : nullRequired.reason,
        unparseable: unparseable.ok ? null : unparseable.reason,
      }),
    );

    // 16b. Event-id rule: anything Postgres' uuid column accepts passes
    // through VERBATIM (the daemon uploads ledger ids as-is — re-deriving
    // those would split one row into two cloud rows); anything else derives
    // the SAME uuid on every run and keeps the original in metadata.
    const v4 = "2b866c49-6f64-4ac1-9d6b-d9ce0aa64166";
    const v7 = "019e0000-aaaa-7bbb-8ccc-dddddddddddd";
    const derivedOnce = ensureUuidEventId("self_test_1749670000000");
    const derivedTwice = ensureUuidEventId("self_test_1749670000000");
    const derivedOther = ensureUuidEventId("self_test_1749670000001");
    const uuidShape = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    const legacyNormalized = normalizeHistoryEvent({
      payloadJson: JSON.stringify({
        id: "self_test_1749670000000",
        source: "claude_code",
        eventType: "user_prompt_submit",
        observedAt: "2026-03-02T00:00:00.000Z",
      }),
      suppressedFieldsJson: "[]",
    });
    check(
      "history_event_ids_idempotent_uuid_mapping",
      ensureUuidEventId(v4).id === v4 &&
        !ensureUuidEventId(v4).derived &&
        ensureUuidEventId(v7).id === v7 &&
        !ensureUuidEventId(v7).derived &&
        derivedOnce.derived &&
        derivedOnce.id === derivedTwice.id &&
        derivedOnce.id !== derivedOther.id &&
        uuidShape.test(derivedOnce.id) &&
        legacyNormalized.ok &&
        legacyNormalized.envelope.event.id === derivedOnce.id &&
        legacyNormalized.envelope.event.metadata.externalEventId === "self_test_1749670000000",
      JSON.stringify({ derived: derivedOnce.id, stable: derivedOnce.id === derivedTwice.id, v7Passthrough: ensureUuidEventId(v7).id === v7 }),
    );

    // 16c. Chunking obeys the ingest contract: ≤500 events, byte budget
    // splits but never drops, order preserved, every batch parses against
    // aiWorkIngestBatchSchema.
    const syntheticItems = Array.from({ length: 1234 }, (_, index) => ({ bytes: 100, index }));
    const chunked = chunkHistoryEnvelopes(syntheticItems, {});
    const flat = chunked.flat().map((item) => item.index);
    const byteItems = [{ bytes: 500, index: 0 }, { bytes: 400, index: 1 }, { bytes: 2_000_000, index: 2 }, { bytes: 10, index: 3 }];
    const byteChunked = chunkHistoryEnvelopes(byteItems, { maxEvents: 500, maxBytes: 1000 });
    const contractBatch = aiWorkIngestBatchSchema.safeParse({
      installKey: "pli_history_contract",
      events: Array.from({ length: 500 }, (_, index) => ({
        event: {
          id: deterministicEventId(["history-contract", index]),
          source: "codex",
          eventType: "assistant_response",
          observedAt: "2026-03-01T00:00:00.000Z",
        },
        suppressedFields: [],
      })),
    });
    check(
      "history_batches_obey_ingest_contract",
      chunked.length === Math.ceil(1234 / 500) &&
        chunked.every((batch) => batch.length <= 500) &&
        flat.length === 1234 &&
        flat.every((value, index) => value === index) &&
        byteChunked.length === 3 &&
        byteChunked[0].length === 2 &&
        byteChunked[1].length === 1 &&
        byteChunked[2].length === 1 &&
        contractBatch.success,
      JSON.stringify({ batches: chunked.map((batch) => batch.length), byteBatches: byteChunked.map((batch) => batch.length) }),
    );

    // 16d. Audit math stays honest: exact counts and token sums per
    // source×month, unpriced cells render as "unpriced" — never a fabricated
    // $0.00 — and skips are itemized.
    const audit = createHistoryAudit();
    const auditEvent = (overrides: Partial<AiInteractionEvent>): AiInteractionEvent =>
      aiInteractionEventSchema.parse({
        id: deterministicEventId(["history-audit", JSON.stringify(overrides)]),
        source: "codex",
        eventType: "assistant_response",
        observedAt: "2026-01-10T00:00:00.000Z",
        ...overrides,
      });
    for (let index = 0; index < 3; index += 1) {
      recordHistoryEligible(audit, auditEvent({ inputTokens: 10, outputTokens: 5, costUsd: 0.5, observedAt: "2026-01-10T00:00:00.000Z" }));
    }
    recordHistoryEligible(audit, auditEvent({ source: "claude_code", inputTokens: 7, observedAt: "2026-02-01T00:00:00.000Z" }));
    recordHistorySkip(audit, "forbidden_content");
    recordHistorySkip(audit, "schema_invalid", 2);
    const totals = historyAuditTotals(audit);
    const table = renderHistoryAudit(audit);
    check(
      "history_audit_math_is_honest",
      audit.cells["codex|2026-01"]?.localEvents === 3 &&
        audit.cells["codex|2026-01"]?.inputTokens === 30 &&
        audit.cells["codex|2026-01"]?.costUsd === 1.5 &&
        audit.cells["codex|2026-01"]?.pricedEvents === 3 &&
        audit.cells["claude_code|2026-02"]?.localEvents === 1 &&
        audit.cells["claude_code|2026-02"]?.pricedEvents === 0 &&
        totals.localEvents === 4 &&
        totals.inputTokens === 37 &&
        /claude_code\s+2026-02\s+1\s+7\s+0\s+unpriced/.test(table) &&
        !table.includes("$0.00") &&
        /forbidden_content: 1/.test(table) &&
        /schema_invalid: 2/.test(table),
      JSON.stringify({ totals: { localEvents: totals.localEvents, inputTokens: totals.inputTokens }, unpricedRendered: /unpriced/.test(table) }),
    );

    // 16e–16i. Loopback end-to-end: a seeded ledger (incl. one live-shape
    // stitch artifact via the same json_set SQL, one forbidden-metadata row,
    // one unparseable row, one bogus event type) pushed to a stub workspace
    // that verifies HMAC signatures exactly like the cloud and upserts by id.
    const historyHome = path.join(tempDir, "history-home");
    fs.mkdirSync(historyHome, { recursive: true });
    const historyLedgerPath = path.join(historyHome, "history-ledger.sqlite");
    const HISTORY_RAW_SENTINEL = "RAW_PROMPT_SENTINEL_HISTORY rm -rf /tmp/secret";
    const HISTORY_REPO_HASH = "sha256:proofrepoaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HISTORY_BRANCH_HASH = "sha256:proofbranchbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const seedBuffer = new LocalEventBuffer(historyLedgerPath);
    const seededIds: string[] = [];
    const seedEvent = (index: number): AiInteractionEvent =>
      aiInteractionEventSchema.parse({
        id: deterministicEventId(["history-e2e", index]),
        sessionId: SESSION,
        source: index % 2 === 0 ? "codex" : "claude_code",
        dataMode: "metadata",
        eventType: "assistant_response",
        observedAt: new Date(Date.UTC(2026, index < 600 ? 0 : 1, 1) + index * 1000).toISOString(),
        model: "proof-model",
        inputTokens: 10,
        outputTokens: 5,
        ...(index < 100 ? { costUsd: 0.01 } : {}),
        // Repo linkage (issue 0036): metadata.git drives the ledger's
        // repo_hash/branch_hash columns at append, exactly like live capture.
        ...(index < 100
          ? { metadata: { git: { remoteUrlHash: HISTORY_REPO_HASH, branchHash: HISTORY_BRANCH_HASH } } }
          : {}),
      });
    for (let index = 0; index < 250; index += 1) {
      const event = seedEvent(index);
      seededIds.push(event.id);
      seedBuffer.append(event, ["tool_input"]);
    }
    // Live-shape stitch artifact: the exact json_set the daemon runs.
    const stitchId = deterministicEventId(["history-e2e-stitch", 1]);
    seedBuffer.append(
      aiInteractionEventSchema.parse({
        id: stitchId,
        source: "codex",
        eventType: "assistant_response",
        observedAt: "2026-02-10T00:00:00.000Z",
        inputTokens: 11,
        outputTokens: 3,
      }),
      [],
    );
    seededIds.push(stitchId);
    seedBuffer.database
      .prepare(`update buffered_events set payload_json = json_set(payload_json, '$.sessionId', null, '$.model', null) where id = ?`)
      .run(stitchId);
    // Forbidden metadata row (must be skipped client-side, never uploaded).
    seedBuffer.append(
      aiInteractionEventSchema.parse({
        id: deterministicEventId(["history-e2e-forbidden", 1]),
        source: "claude_code",
        eventType: "user_prompt_submit",
        observedAt: "2026-02-11T00:00:00.000Z",
        metadata: { prompt: HISTORY_RAW_SENTINEL },
      }),
      [],
    );
    // Unparseable payload + bogus event type (skip reasons must be itemized).
    seedBuffer.database
      .prepare(
        `insert into buffered_events (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json, created_at)
         values ('history-broken-json', 'codex', 'unknown', 'metadata', '2026-02-12T00:00:00.000Z', '{not json', '[]', '2026-02-12T00:00:00.000Z')`,
      )
      .run();
    seedBuffer.append(
      {
        ...seedEvent(9_999),
        id: deterministicEventId(["history-e2e-bogus", 1]),
        eventType: "totally_bogus" as AiInteractionEvent["eventType"],
      },
      [],
    );
    for (let index = 250; index < 1_120; index += 1) {
      const event = seedEvent(index);
      seededIds.push(event.id);
      seedBuffer.append(event, ["tool_input"]);
    }
    seedBuffer.close();
    const expectedEligible = 1_121; // 1,120 normal + repaired stitch artifact

    const HISTORY_INSTALL_KEY = "pli_historyproofkey00000000000001";
    const HISTORY_SECRET = "history-proof-signing-secret-0123456789";
    const historyStore = new Map<string, unknown>();
    let historyRequests = 0;
    let historySignatureFailures = 0;
    let historyMaxBatch = 0;
    let historyForbiddenHits = 0;
    const historyBodies: string[] = [];
    const historyServer = http.createServer((request, response) => {
      let body = "";
      request.on("data", (chunk) => (body += chunk));
      request.on("end", () => {
        historyRequests += 1;
        historyBodies.push(body);
        const timestamp = String(request.headers["x-plimsoll-upload-timestamp"] ?? "");
        const signature = String(request.headers["x-plimsoll-upload-signature"] ?? "");
        const expected = `sha256=${crypto.createHmac("sha256", HISTORY_SECRET).update(`${timestamp}.${body}`).digest("hex")}`;
        if (!timestamp || signature !== expected) {
          historySignatureFailures += 1;
          response.writeHead(401, { "content-type": "application/json" });
          response.end(JSON.stringify({ error: "bad_upload_signature" }));
          return;
        }
        const batch = aiWorkIngestBatchSchema.parse(JSON.parse(body));
        historyMaxBatch = Math.max(historyMaxBatch, batch.events.length);
        // Fast-lane semantics (cloud PR #19): createMany(skipDuplicates) —
        // already-present ids are accepted but insert nothing.
        let inserted = 0;
        for (const entry of batch.events) {
          if (findForbiddenRawContentFields(entry.event.metadata).length > 0) {
            historyForbiddenHits += 1;
          }
          if (!historyStore.has(entry.event.id)) inserted += 1;
          historyStore.set(entry.event.id, entry.event);
        }
        response.writeHead(200, { "content-type": "application/json" });
        // Mirrors production: the response echoes the install key — the CLI
        // must never print it.
        response.end(JSON.stringify({ ok: true, accepted: batch.events.length, inserted, installKey: HISTORY_INSTALL_KEY }));
      });
    });
    await new Promise<void>((resolve) => historyServer.listen(0, "127.0.0.1", () => resolve()));
    const historyPort = (historyServer.address() as AddressInfo).port;
    const historyConfig = collectorConfigSchema.parse({
      uploadUrl: `http://127.0.0.1:${historyPort}/api/work-intelligence/ingest`,
      installKey: HISTORY_INSTALL_KEY,
      uploadSigningSecret: HISTORY_SECRET,
      tenantId: "44444444-5555-4666-8777-888888888888",
    });

    const historyLogs: string[] = [];
    const historyStatePath = path.join(historyHome, "workspace-backfill-state.json");
    const run1 = await runWorkspaceHistoryUpload(historyConfig, {
      ledgerPath: historyLedgerPath,
      statePath: historyStatePath,
      batchSize: 200,
      pageSize: 500,
      concurrency: 3,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: (line) => historyLogs.push(line),
    });
    const state1 = readBackfillState(historyStatePath);
    check(
      "history_e2e_full_ledger_reaches_workspace",
      run1.ok &&
        run1.completed &&
        run1.eligibleEvents === expectedEligible &&
        run1.acceptedEvents === expectedEligible &&
        run1.sentEvents === expectedEligible &&
        historyStore.size === expectedEligible &&
        seededIds.every((id) => historyStore.has(id)) &&
        run1.skippedEvents === 3 &&
        run1.audit.skipped.forbidden_content === 1 &&
        run1.audit.skipped.payload_unparseable === 1 &&
        run1.audit.skipped.schema_invalid === 1 &&
        historyMaxBatch <= 200 &&
        historySignatureFailures === 0 &&
        historyAuditTotals(run1.audit).localEvents === expectedEligible &&
        historyAuditTotals(run1.audit).inputTokens === expectedEligible * 10 + 1 &&
        run1.insertedEvents === expectedEligible &&
        state1?.completedAt !== null &&
        state1?.watermark !== null,
      JSON.stringify({
        accepted: run1.acceptedEvents,
        inserted: run1.insertedEvents,
        storeSize: historyStore.size,
        skipped: run1.audit.skipped,
        batches: run1.batchesSent,
        maxBatch: historyMaxBatch,
      }),
    );

    check(
      "history_upload_bodies_stay_metadata_only",
      historyBodies.length > 0 &&
        historyBodies.every((requestBody) => !requestBody.includes(HISTORY_RAW_SENTINEL)) &&
        historyBodies.every((requestBody) => !requestBody.includes(os.hostname())) &&
        historyLogs.every((line) => !line.includes(HISTORY_INSTALL_KEY)) &&
        run1.auditTable.length > 0 &&
        !run1.auditTable.includes(HISTORY_INSTALL_KEY),
      JSON.stringify({ bodies: historyBodies.length, sentinelLeaked: historyBodies.some((requestBody) => requestBody.includes(HISTORY_RAW_SENTINEL)) }),
    );

    // 16f. Idempotency: a second FULL run over the same scope re-sends the
    // same ids — the workspace grows by exactly zero rows, and the server's
    // additive `inserted` field reports 0 throughout.
    const storeSizeBeforeRun2 = historyStore.size;
    const idsBeforeRun2 = [...historyStore.keys()].sort().join(",");
    const run2 = await runWorkspaceHistoryUpload(historyConfig, {
      ledgerPath: historyLedgerPath,
      statePath: historyStatePath,
      until: run1.until,
      full: true,
      batchSize: 200,
      pageSize: 500,
      concurrency: 3,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: (line) => historyLogs.push(line),
    });
    check(
      "history_second_run_grows_workspace_by_zero",
      run2.ok &&
        run2.completed &&
        run2.acceptedEvents === expectedEligible &&
        run2.insertedEvents === 0 &&
        historyStore.size === storeSizeBeforeRun2 &&
        [...historyStore.keys()].sort().join(",") === idsBeforeRun2 &&
        run2.skippedEvents === 3 &&
        /server-reported NEW rows this backfill: 0/.test(run2.auditTable),
      JSON.stringify({
        before: storeSizeBeforeRun2,
        after: historyStore.size,
        accepted: run2.acceptedEvents,
        inserted: run2.insertedEvents,
      }),
    );

    // 16g. Resume: an interrupted run continues from the watermark, the union
    // covers everything, and the cumulative audit never double-counts.
    const resumeStatePath = path.join(historyHome, "workspace-backfill-state-resume.json");
    const resumeStore = new Map<string, unknown>();
    const resumeFetch: typeof fetch = async (input, init) => {
      const body = String(init?.body ?? "");
      const batch = aiWorkIngestBatchSchema.parse(JSON.parse(body));
      let inserted = 0;
      for (const entry of batch.events) {
        if (!resumeStore.has(entry.event.id)) inserted += 1;
        resumeStore.set(entry.event.id, entry.event);
      }
      void input;
      return new Response(JSON.stringify({ ok: true, accepted: batch.events.length, inserted }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };
    const resumeA = await runWorkspaceHistoryUpload(historyConfig, {
      ledgerPath: historyLedgerPath,
      statePath: resumeStatePath,
      limit: 200,
      batchSize: 200,
      pageSize: 500,
      concurrency: 1,
      delayMs: 0,
      fetchImpl: resumeFetch,
      log: (line) => historyLogs.push(line),
    });
    const resumeStateMid = readBackfillState(resumeStatePath);
    const resumeB = await runWorkspaceHistoryUpload(historyConfig, {
      ledgerPath: historyLedgerPath,
      statePath: resumeStatePath,
      batchSize: 200,
      pageSize: 500,
      concurrency: 1,
      delayMs: 0,
      fetchImpl: resumeFetch,
      log: (line) => historyLogs.push(line),
    });
    check(
      "history_resume_covers_everything_without_double_count",
      resumeA.ok &&
        !resumeA.completed &&
        resumeA.sentEvents === 200 &&
        resumeStateMid?.completedAt === null &&
        resumeStateMid?.watermark !== null &&
        resumeB.ok &&
        resumeB.completed &&
        resumeB.resumedFromRowid !== null &&
        resumeStore.size === expectedEligible &&
        seededIds.every((id) => resumeStore.has(id)) &&
        historyAuditTotals(resumeB.audit).localEvents === expectedEligible &&
        historyAuditTotals(resumeB.audit).sentEvents === expectedEligible &&
        resumeB.insertedEvents === expectedEligible &&
        resumeB.skippedEvents === 3,
      JSON.stringify({
        firstRunSent: resumeA.sentEvents,
        resumedFromRowid: resumeB.resumedFromRowid,
        unionSize: resumeStore.size,
        cumulativeLocal: historyAuditTotals(resumeB.audit).localEvents,
        cumulativeInserted: resumeB.insertedEvents,
      }),
    );

    // 16h. Unjoined machines are refused before any network happens.
    let unjoinedFetches = 0;
    const unjoinedFetch: typeof fetch = async () => {
      unjoinedFetches += 1;
      throw new Error("must not be called");
    };
    let unjoinedError = "";
    try {
      await runWorkspaceHistoryUpload(collectorConfigSchema.parse({}), {
        ledgerPath: historyLedgerPath,
        statePath: path.join(historyHome, "unjoined-state.json"),
        fetchImpl: unjoinedFetch,
        log: () => undefined,
      });
    } catch (error) {
      unjoinedError = error instanceof Error ? error.message : String(error);
    }
    check(
      "history_upload_requires_join",
      /join/.test(unjoinedError) && /uploadUrl|workspace/.test(unjoinedError) && unjoinedFetches === 0,
      JSON.stringify({ error: unjoinedError.slice(0, 120), fetches: unjoinedFetches }),
    );

    // 16i. Auth/signature failures FAIL CLOSED: no retry storm, watermark
    // intact, clear guidance.
    const badSecretConfig = collectorConfigSchema.parse({
      ...historyConfig,
      uploadSigningSecret: "wrong-secret-wrong-secret-000000",
    });
    const failClosedLogs: string[] = [];
    const failClosed = await runWorkspaceHistoryUpload(badSecretConfig, {
      ledgerPath: historyLedgerPath,
      statePath: path.join(historyHome, "fail-closed-state.json"),
      batchSize: 200,
      pageSize: 500,
      concurrency: 1,
      delayMs: 0,
      maxAttemptsPerBatch: 5,
      log: (line) => failClosedLogs.push(line),
    });
    check(
      "history_auth_failure_fails_closed",
      !failClosed.ok &&
        failClosed.acceptedEvents === 0 &&
        /401/.test(failClosed.reason ?? "") &&
        /signature|credentials/i.test(failClosed.reason ?? "") &&
        !failClosedLogs.some((line) => line.includes("workspace_backfill_retry")),
      JSON.stringify({ reason: (failClosed.reason ?? "").slice(0, 140), accepted: failClosed.acceptedEvents }),
    );


    // 16j. Project attribution parity (issue 0036). Forward path: the
    // ledger's repo columns become event.projectKey on the wire — pure
    // mapper first, then the loopback bodies of the runs above.
    const linkageBase = aiInteractionEventSchema.parse({
      id: deterministicEventId(["linkage-proof", 1]),
      source: "codex",
      eventType: "assistant_response",
      observedAt: "2026-03-01T00:00:00.000Z",
    });
    const linked = attachRepoLinkage(linkageBase, "sha256:repoX", "sha256:branchY");
    const preLabeled = attachRepoLinkage(
      { ...linkageBase, projectKey: "sha256:already" },
      "sha256:repoX",
      "sha256:branchY",
    );
    const linkedIds = new Set<string>();
    for (const body of historyBodies) {
      try {
        const parsed = JSON.parse(body) as { events?: Array<{ event: { id: string; projectKey?: string; metadata?: Record<string, unknown> } }> };
        for (const entry of parsed.events ?? []) {
          if (entry.event.projectKey === HISTORY_REPO_HASH && entry.event.metadata?.branchHash === HISTORY_BRANCH_HASH) {
            linkedIds.add(entry.event.id);
          }
        }
      } catch {
        // bad-signature fixtures and repair bodies parse differently; skip
      }
    }
    check(
      "forward_path_sends_repo_linkage_as_project_key",
      linked.projectKey === "sha256:repoX" &&
        linked.metadata.branchHash === "sha256:branchY" &&
        aiInteractionEventSchema.safeParse(linked).success &&
        preLabeled.projectKey === "sha256:already" &&
        attachRepoLinkage(linkageBase, null).projectKey === undefined &&
        linkedIds.size === 100,
      JSON.stringify({ linkedEventIdsOnWire: linkedIds.size, neverOverwrites: preLabeled.projectKey }),
    );

    // 16k. Repair rows ride the SAME deterministic id mapping as uploads.
    const repairRows = buildAttributionRepairRows([
      { id: "2b866c49-6f64-4ac1-9d6b-d9ce0aa64166", repoHash: "sha256:r1" },
      { id: "self_test_1749670000000", repoHash: "sha256:r2" },
      { id: "ignored", repoHash: null },
      { id: "", repoHash: "sha256:r3" },
    ]);
    check(
      "attribution_repair_rows_share_upload_id_mapping",
      repairRows.length === 2 &&
        repairRows[0].id === "2b866c49-6f64-4ac1-9d6b-d9ce0aa64166" &&
        repairRows[1].id === ensureUuidEventId("self_test_1749670000000").id &&
        repairRows[1].projectKey === "sha256:r2",
      JSON.stringify(repairRows),
    );

    // 16l. Repair e2e: a stub workspace whose rows are projectKey-null (the
    // production state this lane exists for) — run fills exactly the linked
    // rows; a second run reports updated 0. Fill-only is enforced stub-side
    // the same way the cloud does it.
    const repairStore = new Map<string, { projectKey: string | null }>();
    for (let index = 0; index < 100; index += 1) {
      repairStore.set(deterministicEventId(["history-e2e", index]), { projectKey: null });
    }
    let repairSignatureFailures = 0;
    const repairServer = http.createServer((request, response) => {
      let body = "";
      request.on("data", (chunk) => (body += chunk));
      request.on("end", () => {
        const timestamp = String(request.headers["x-plimsoll-upload-timestamp"] ?? "");
        const signature = String(request.headers["x-plimsoll-upload-signature"] ?? "");
        const expected = `sha256=${crypto.createHmac("sha256", HISTORY_SECRET).update(`${timestamp}.${body}`).digest("hex")}`;
        if (!timestamp || signature !== expected) {
          repairSignatureFailures += 1;
          response.writeHead(401, { "content-type": "application/json" });
          response.end(JSON.stringify({ error: "bad_upload_signature" }));
          return;
        }
        const batch = JSON.parse(body) as { kind?: string; rows: Array<{ id: string; projectKey: string }> };
        let matched = 0;
        let updated = 0;
        for (const row of batch.rows) {
          const existing = repairStore.get(row.id);
          if (!existing) continue;
          matched += 1;
          if (existing.projectKey === null) {
            existing.projectKey = row.projectKey;
            updated += 1;
          }
        }
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, accepted: 0, matched, updated, kind: batch.kind }));
      });
    });
    await new Promise<void>((resolve) => repairServer.listen(0, "127.0.0.1", () => resolve()));
    const repairPort = (repairServer.address() as AddressInfo).port;
    const repairConfig = collectorConfigSchema.parse({
      ...historyConfig,
      uploadUrl: `http://127.0.0.1:${repairPort}/api/work-intelligence/ingest`,
    });
    const repairLogs: string[] = [];
    const repair1 = await runAttributionRepair(repairConfig, {
      ledgerPath: historyLedgerPath,
      batchSize: 40,
      concurrency: 2,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: (line) => repairLogs.push(line),
    });
    const repair2 = await runAttributionRepair(repairConfig, {
      ledgerPath: historyLedgerPath,
      batchSize: 40,
      concurrency: 2,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: (line) => repairLogs.push(line),
    });
    const filledCount = [...repairStore.values()].filter((row) => row.projectKey === HISTORY_REPO_HASH).length;
    check(
      "attribution_repair_fills_once_then_settles",
      repair1.ok &&
        repair1.rowsWithRepoHash === 100 &&
        repair1.sentRows === 100 &&
        repair1.matchedRows === 100 &&
        repair1.updatedRows === 100 &&
        repair2.ok &&
        repair2.sentRows === 100 &&
        repair2.matchedRows === 100 &&
        repair2.updatedRows === 0 &&
        filledCount === 100 &&
        repairSignatureFailures === 0 &&
        repairLogs.every((line) => !line.includes(HISTORY_INSTALL_KEY)),
      JSON.stringify({
        run1: { matched: repair1.matchedRows, updated: repair1.updatedRows },
        run2: { matched: repair2.matchedRows, updated: repair2.updatedRows },
        filled: filledCount,
      }),
    );
    await new Promise<void>((resolve) => repairServer.close(() => resolve()));

    // 16m. Repo labels: slug parsing, label-over-priority precedence, raw
    // URLs never on the wire, and the loopback push.
    const slugCases =
      JSON.stringify(parseRepoSlug("github.com/cryptojym/plimsoll")) ===
        JSON.stringify({ provider: "github", owner: "cryptojym", name: "plimsoll" }) &&
      JSON.stringify(parseRepoSlug("https://github.com/CryptoJym/Plimsoll.git")) ===
        JSON.stringify({ provider: "github", owner: "cryptojym", name: "plimsoll" }) &&
      JSON.stringify(parseRepoSlug("git@github.com:o/n.git")) ===
        JSON.stringify({ provider: "github", owner: "o", name: "n" }) &&
      parseRepoSlug("gitlab.com/team/repo")?.provider === "gitlab" &&
      parseRepoSlug("just-a-name")?.provider === "local_git" &&
      parseRepoSlug("") === null;
    const labelCandidates = buildRepoLabelCandidates(
      [{ repoHash: "sha256:hash1", label: "github.com/cryptojym/plimsoll" }],
      [
        { repoHash: "sha256:hash1", url: "https://github.com/wrong/should-lose" },
        { repoHash: "sha256:hash2", url: "https://github.com/cryptojym/derived-only" },
      ],
    );
    const labelPreview = renderRepoLabelPreview(labelCandidates.candidates, 0);
    const labelBodies: string[] = [];
    let labelPath = "";
    const labelFetch: typeof fetch = async (input, init) => {
      labelPath = new URL(String(input)).pathname;
      labelBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ ok: true, created: 2, updated: 0 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };
    const pushedLabels = await pushRepoLabels(historyConfig, labelCandidates.candidates, {
      fetchImpl: labelFetch,
      log: () => undefined,
    });
    check(
      "repo_labels_disclose_slugs_never_urls",
      slugCases &&
        labelCandidates.candidates.length === 2 &&
        labelCandidates.candidates.find((c) => c.remoteUrlHash === "sha256:hash1")?.name === "plimsoll" &&
        labelCandidates.candidates.find((c) => c.remoteUrlHash === "sha256:hash1")?.source === "repo_label" &&
        labelCandidates.candidates.find((c) => c.remoteUrlHash === "sha256:hash2")?.source ===
          "derived_from_priority_url" &&
        labelPreview.includes("derived from priority URL") &&
        labelPreview.includes("Never sent: raw URLs") &&
        labelPath === "/api/work-intelligence/repo-labels" &&
        pushedLabels.pushed === 2 &&
        labelBodies.every((body) => !body.includes("://")) &&
        labelBodies.every((body) => !body.includes("should-lose")),
      JSON.stringify({ candidates: labelCandidates.candidates.length, path: labelPath, urlLeak: labelBodies.some((b) => b.includes("://")) }),
    );

    await new Promise<void>((resolve) => historyServer.close(() => resolve()));
  }

  // 17. Session sync (issue 0038 / cloud Phase D1): the ledger's stitched
  // sessions become REAL hosted rows — deterministic join-able ids, snapshot
  // totals that reconcile to the ledger, the grow-only idempotent push, and
  // the daemon's touched-sessions refresh.
  {
    // 17a. Session-id rule: anything Postgres' uuid column accepts passes
    // through verbatim-lowercased — claude v4 ids live in events.session_id
    // and codex v7 ids in metadata.externalSessionId, so the session row id
    // must equal those exact values for the join to work. Non-uuid ids derive
    // the SAME uuid every run, in a namespace distinct from event ids.
    const v4Session = "2B866C49-6F64-4AC1-9D6B-D9CE0AA64166";
    const v7Session = "019dbcc6-d26c-7c82-84cb-a211da747e46";
    const junkOnce = ensureUuidSessionId("session-junk-0042");
    const junkTwice = ensureUuidSessionId("session-junk-0042");
    check(
      "session_ids_idempotent_uuid_mapping_joins_event_forms",
      ensureUuidSessionId(v4Session).id === v4Session.toLowerCase() &&
        !ensureUuidSessionId(v4Session).derived &&
        ensureUuidSessionId(v7Session).id === v7Session &&
        !ensureUuidSessionId(v7Session).derived &&
        junkOnce.derived &&
        junkOnce.id === junkTwice.id &&
        junkOnce.id !== ensureUuidEventId("session-junk-0042").id &&
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(junkOnce.id),
      JSON.stringify({
        v7Passthrough: ensureUuidSessionId(v7Session).id === v7Session,
        derivedStable: junkOnce.id === junkTwice.id,
        eventNamespaceDistinct: junkOnce.id !== ensureUuidEventId("session-junk-0042").id,
      }),
    );

    // Fixture ledger: three healthy sessions (codex v7 with two repos —
    // dominant pair must win; claude v4; junk-id), two poisoned sessions
    // (unknown source, timezone-less timestamp) that must SKIP with reasons,
    // and sessionless events that must not invent sessions.
    const sessionHome = path.join(tempDir, "session-home");
    fs.mkdirSync(sessionHome, { recursive: true });
    const sessionLedgerPath = path.join(sessionHome, "session-ledger.sqlite");
    const SESSION_REPO_A = "sha256:sessrepoaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SESSION_REPO_B = "sha256:sessrepobbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SESSION_BRANCH_B = "sha256:sessbranchbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const sessionSeed = new LocalEventBuffer(sessionLedgerPath);
    const seedSessionEvent = (input: {
      index: number;
      sessionId?: string;
      source?: "codex" | "claude_code";
      observedAt: string;
      costUsd?: number;
      repoHash?: string;
      branchHash?: string;
    }) =>
      sessionSeed.append(
        aiInteractionEventSchema.parse({
          id: deterministicEventId(["session-proof", input.index]),
          ...(input.sessionId ? { sessionId: input.sessionId } : {}),
          actorId: "sha256:sessionproofaccount0000000000000000000001",
          source: input.source ?? "codex",
          eventType: "assistant_response",
          observedAt: input.observedAt,
          model: "proof-model",
          inputTokens: 10,
          outputTokens: 5,
          cacheReadTokens: 100,
          cacheCreationTokens: 7,
          ...(input.costUsd !== undefined ? { costUsd: input.costUsd } : {}),
          ...(input.repoHash
            ? { metadata: { git: { remoteUrlHash: input.repoHash, branchHash: input.branchHash } } }
            : {}),
        }),
        [],
      );
    const junkSessionRaw = "session-junk-0042";
    let seedIndex = 0;
    // v7 codex session: 10 events on repo A, 20 on repo B (B must dominate),
    // 5 priced.
    for (let i = 0; i < 30; i += 1) {
      seedSessionEvent({
        index: (seedIndex += 1),
        sessionId: v7Session,
        observedAt: new Date(Date.UTC(2026, 4, 1, 0, 0, i)).toISOString(),
        ...(i < 5 ? { costUsd: 0.02 } : {}),
        ...(i < 10
          ? { repoHash: SESSION_REPO_A }
          : { repoHash: SESSION_REPO_B, branchHash: SESSION_BRANCH_B }),
      });
    }
    // v4 claude session: 4 events, unpriced, no repo linkage.
    for (let i = 0; i < 4; i += 1) {
      seedSessionEvent({
        index: (seedIndex += 1),
        sessionId: v4Session.toLowerCase(),
        source: "claude_code",
        observedAt: new Date(Date.UTC(2026, 4, 2, 0, 0, i)).toISOString(),
      });
    }
    // junk-id session: 2 events.
    for (let i = 0; i < 2; i += 1) {
      seedSessionEvent({
        index: (seedIndex += 1),
        sessionId: junkSessionRaw,
        observedAt: new Date(Date.UTC(2026, 4, 3, 0, 0, i)).toISOString(),
      });
    }
    // Sessionless events: never become sessions.
    for (let i = 0; i < 3; i += 1) {
      seedSessionEvent({
        index: (seedIndex += 1),
        observedAt: new Date(Date.UTC(2026, 4, 4, 0, 0, i)).toISOString(),
      });
    }
    // Poisoned rows the strict wire schema must refuse, itemized by reason.
    sessionSeed.database
      .prepare(
        `insert into buffered_events (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json, created_at, session_id)
         values ('session-bad-source', 'mystery_tool', 'assistant_response', 'metadata', '2026-05-05T00:00:00.000Z', '{}', '[]', datetime('now'), 'mystery-session-1')`,
      )
      .run();
    sessionSeed.database
      .prepare(
        `insert into buffered_events (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json, created_at, session_id)
         values ('session-bad-time', 'codex', 'assistant_response', 'metadata', '2026-05-05 00:00:00', '{}', '[]', datetime('now'), 'badtime-session-1')`,
      )
      .run();

    // 17b. Snapshots reconcile to the ledger exactly; --until scoping is the
    // idempotency horizon; dominant (repo, branch) pair wins attribution.
    const untilT1 = new Date(Date.now() + 1000).toISOString();
    const snapshotsT1 = collectSessionSnapshots(sessionSeed.database, { until: untilT1 });
    const v7Snap = snapshotsT1.find((row) => row.sessionId === v7Session);
    const v4Snap = snapshotsT1.find((row) => row.sessionId === v4Session.toLowerCase());
    const junkSnap = snapshotsT1.find((row) => row.sessionId === junkSessionRaw);
    check(
      "session_snapshots_reconcile_to_ledger",
      snapshotsT1.length === 5 &&
        v7Snap?.events === 30 &&
        v7Snap.inputTokens === 300 &&
        v7Snap.outputTokens === 150 &&
        v7Snap.cacheReadTokens === 3000 &&
        v7Snap.cacheCreationTokens === 210 &&
        v7Snap.pricedEvents === 5 &&
        Math.abs(v7Snap.costUsd - 0.1) < 1e-9 &&
        v7Snap.repoHash === SESSION_REPO_B &&
        v7Snap.branchHash === SESSION_BRANCH_B &&
        v7Snap.startedAt === "2026-05-01T00:00:00.000Z" &&
        v7Snap.endedAt === "2026-05-01T00:00:29.000Z" &&
        v7Snap.accountHash === "sha256:sessionproofaccount0000000000000000000001" &&
        v4Snap?.events === 4 &&
        v4Snap.pricedEvents === 0 &&
        v4Snap.repoHash === null &&
        junkSnap?.events === 2 &&
        !snapshotsT1.some((row) => row.sessionId === null),
      JSON.stringify({
        sessions: snapshotsT1.map((row) => `${row.sessionId}:${row.events}`),
        v7Dominant: `${v7Snap?.repoHash}@${v7Snap?.branchHash}`,
      }),
    );

    // 17c. Wire rows obey the strict contract; poisoned sessions skip with
    // reasons; honest totals (unpriced stays unpriced); junk ids carry their
    // raw value in metadata.externalSessionId so events still join.
    const wireRows = snapshotsT1.map((snapshot) => buildSessionSyncRow(snapshot));
    const okRows = wireRows.flatMap((row) => (row.ok ? [row] : []));
    const skipReasons = wireRows
      .flatMap((row) => (row.ok ? [] : [row.reason]))
      .sort()
      .join(",");
    const junkWire = okRows.find((row) => row.row.session.metadata.externalSessionId === junkSessionRaw);
    const wireBatchParses = aiWorkSessionSyncBatchSchema.safeParse({
      kind: "session_sync",
      installKey: "pli_sessionproofkey0000000000000001",
      sessions: okRows.map((row) => row.row),
    }).success;
    const v7Wire = okRows.find((row) => row.row.session.id === v7Session);
    check(
      "session_rows_obey_contract_and_skip_with_reasons",
      okRows.length === 3 &&
        skipReasons === "schema_invalid,source_invalid" &&
        wireBatchParses &&
        v7Wire?.row.session.projectKey === SESSION_REPO_B &&
        v7Wire.row.session.metadata.branchHash === SESSION_BRANCH_B &&
        v7Wire.row.session.metadata.externalActorId ===
          "sha256:sessionproofaccount0000000000000000000001" &&
        v7Wire.row.session.metadata.externalSessionId === undefined &&
        v7Wire.row.totals.costUsd > 0 &&
        junkWire !== undefined &&
        junkWire.idDerived &&
        okRows.find((row) => row.row.session.id === v4Session.toLowerCase())?.row.totals
          .pricedEvents === 0,
      JSON.stringify({ eligible: okRows.length, skipped: skipReasons, batchParses: wireBatchParses }),
    );

    // 17d. End-to-end against a stub workspace implementing the cloud's
    // grow-only upsert (insert / update / skippedStale with the
    // ended_at + totals.events guard): full push inserts, identical re-run
    // inserts nothing and changes nothing, growth updates in place, a STALE
    // replay can never regress the held row, and the daemon-path subset only
    // touches its sessions. Signatures verified exactly like the cloud.
    const SESSION_INSTALL_KEY = "pli_sessionproofkey0000000000000001";
    const SESSION_SECRET = "session-proof-signing-secret-0123456789";
    const sessionStore = new Map<string, AiWorkSessionSyncRow>();
    let sessionSignatureFailures = 0;
    let sessionMaxBatch = 0;
    const sessionServer = http.createServer((request, response) => {
      let body = "";
      request.on("data", (chunk) => (body += chunk));
      request.on("end", () => {
        const timestamp = String(request.headers["x-plimsoll-upload-timestamp"] ?? "");
        const signature = String(request.headers["x-plimsoll-upload-signature"] ?? "");
        const expected = `sha256=${crypto.createHmac("sha256", SESSION_SECRET).update(`${timestamp}.${body}`).digest("hex")}`;
        if (!timestamp || signature !== expected) {
          sessionSignatureFailures += 1;
          response.writeHead(401, { "content-type": "application/json" });
          response.end(JSON.stringify({ error: "bad_upload_signature" }));
          return;
        }
        const batch = aiWorkSessionSyncBatchSchema.parse(JSON.parse(body));
        sessionMaxBatch = Math.max(sessionMaxBatch, batch.sessions.length);
        let inserted = 0;
        let updated = 0;
        for (const row of batch.sessions) {
          const held = sessionStore.get(row.session.id);
          if (!held) {
            sessionStore.set(row.session.id, row);
            inserted += 1;
            continue;
          }
          const heldEnd = Date.parse(held.session.endedAt);
          const incomingEnd = Date.parse(row.session.endedAt);
          const grows =
            incomingEnd > heldEnd ||
            (incomingEnd === heldEnd && row.totals.events >= held.totals.events);
          if (grows) {
            sessionStore.set(row.session.id, row);
            updated += 1;
          }
        }
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            ok: true,
            accepted: batch.sessions.length,
            inserted,
            updated,
            installKey: SESSION_INSTALL_KEY,
          }),
        );
      });
    });
    await new Promise<void>((resolve) => sessionServer.listen(0, "127.0.0.1", () => resolve()));
    const sessionPort = (sessionServer.address() as AddressInfo).port;
    const sessionConfig = collectorConfigSchema.parse({
      uploadUrl: `http://127.0.0.1:${sessionPort}/api/work-intelligence/ingest`,
      installKey: SESSION_INSTALL_KEY,
      uploadSigningSecret: SESSION_SECRET,
      tenantId: "44444444-5555-4666-8777-888888888888",
    });
    const sessionLogs: string[] = [];
    const sessionLog = (line: string) => sessionLogs.push(line);

    const sessionRun1 = await runSessionSync(sessionConfig, {
      until: untilT1,
      ledgerPath: sessionLedgerPath,
      batchSize: 2, // forces multiple batches over 3 sessions
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: sessionLog,
    });
    const storeAfterRun1 = JSON.stringify([...sessionStore.entries()].sort());
    const sessionRun2 = await runSessionSync(sessionConfig, {
      until: untilT1,
      ledgerPath: sessionLedgerPath,
      batchSize: 2,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: sessionLog,
    });
    const storeAfterRun2 = JSON.stringify([...sessionStore.entries()].sort());
    // batchSize: 2 over 3 sessions must have split batches (chunking under
    // the contract); later runs use the default size, so snapshot the max now.
    const maxBatchThroughRun2 = sessionMaxBatch;

    // Growth: one more v7 event lands after T1; the refreshed snapshot must
    // update in place (events 31, later endedAt).
    seedSessionEvent({
      index: (seedIndex += 1),
      sessionId: v7Session,
      observedAt: "2026-05-01T00:01:00.000Z",
    });
    const untilT2 = new Date(Date.now() + 1000).toISOString();
    const sessionRun3 = await runSessionSync(sessionConfig, {
      until: untilT2,
      ledgerPath: sessionLedgerPath,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: sessionLog,
    });
    const v7AfterGrowth = sessionStore.get(v7Session);

    // Stale replay: re-running over the OLD horizon recomputes the 30-event
    // snapshot — the stub (like the cloud) must refuse the regression.
    const sessionRun4 = await runSessionSync(sessionConfig, {
      until: untilT1,
      ledgerPath: sessionLedgerPath,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: sessionLog,
    });
    const v7AfterStaleReplay = sessionStore.get(v7Session);

    // Daemon path: only the touched session crosses, read through the LIVE
    // buffer handle (no second file open).
    const requestsBeforeSubset = sessionLogs.length;
    const sessionRunSubset = await runSessionSync(sessionConfig, {
      sessionIds: [v4Session.toLowerCase()],
      ledgerDb: sessionSeed.database,
      delayMs: 0,
      maxAttemptsPerBatch: 2,
      log: sessionLog,
    });
    void requestsBeforeSubset;

    // Dry run: zero network, zero store movement.
    const storeBeforeDry = JSON.stringify([...sessionStore.entries()].sort());
    const sessionDry = await runSessionSync(sessionConfig, {
      until: untilT1,
      ledgerPath: sessionLedgerPath,
      dryRun: true,
      delayMs: 0,
      log: sessionLog,
    });
    const storeAfterDry = JSON.stringify([...sessionStore.entries()].sort());
    sessionSeed.close();
    await new Promise<void>((resolve) => sessionServer.close(() => resolve()));

    check(
      "session_sync_e2e_grow_only_idempotent",
      sessionRun1.ok &&
        sessionRun1.ledgerSessions === 5 &&
        sessionRun1.eligibleSessions === 3 &&
        sessionRun1.skippedSessions === 2 &&
        sessionRun1.acceptedSessions === 3 &&
        sessionRun1.insertedSessions === 3 &&
        sessionRun1.derivedIds === 1 &&
        maxBatchThroughRun2 <= 2 &&
        sessionStore.size === 3 &&
        sessionRun2.ok &&
        sessionRun2.insertedSessions === 0 &&
        storeAfterRun1 === storeAfterRun2 &&
        sessionRun3.ok &&
        sessionRun3.insertedSessions === 0 &&
        (sessionRun3.updatedSessions ?? 0) >= 1 &&
        v7AfterGrowth?.totals.events === 31 &&
        v7AfterGrowth.session.endedAt === "2026-05-01T00:01:00.000Z" &&
        sessionRun4.ok &&
        v7AfterStaleReplay?.totals.events === 31 &&
        v7AfterStaleReplay.session.endedAt === "2026-05-01T00:01:00.000Z" &&
        sessionRunSubset.ok &&
        sessionRunSubset.ledgerSessions === 1 &&
        sessionRunSubset.sentSessions === 1 &&
        sessionDry.ok &&
        sessionDry.dryRun &&
        storeBeforeDry === storeAfterDry &&
        sessionSignatureFailures === 0 &&
        sessionLogs.every((line) => !line.includes(SESSION_INSTALL_KEY)),
      JSON.stringify({
        run1: { inserted: sessionRun1.insertedSessions, skipped: sessionRun1.skippedSessions },
        run2Inserted: sessionRun2.insertedSessions,
        growth: { events: v7AfterGrowth?.totals.events, endedAt: v7AfterGrowth?.session.endedAt },
        staleReplayHeld: v7AfterStaleReplay?.totals.events === 31,
        subsetSent: sessionRunSubset.sentSessions,
      }),
    );

    // 17e. The daemon refreshes touched sessions after each event sync —
    // pure id extraction, and the wiring stays isolated from the event
    // backoff (a session-push failure must never look like a sync failure).
    const touchedIds = sessionIdsFromBatches([
      {
        tenantId: "t",
        installKey: "k",
        appVersion: "0.1.0",
        events: [
          { event: aiInteractionEventSchema.parse({ id: deterministicEventId(["touch", 1]), sessionId: v7Session, source: "codex", eventType: "assistant_response", observedAt: "2026-05-01T00:00:00.000Z" }), suppressedFields: [] },
          { event: aiInteractionEventSchema.parse({ id: deterministicEventId(["touch", 2]), source: "codex", eventType: "assistant_response", observedAt: "2026-05-01T00:00:01.000Z" }), suppressedFields: [] },
        ],
      },
      null,
      {
        tenantId: "t",
        installKey: "k",
        appVersion: "0.1.0",
        events: [
          { event: aiInteractionEventSchema.parse({ id: deterministicEventId(["touch", 3]), sessionId: v7Session, source: "codex", eventType: "assistant_response", observedAt: "2026-05-01T00:00:02.000Z" }), suppressedFields: [] },
          { event: aiInteractionEventSchema.parse({ id: deterministicEventId(["touch", 4]), sessionId: junkSessionRaw, source: "claude_code", eventType: "assistant_response", observedAt: "2026-05-01T00:00:03.000Z" }), suppressedFields: [] },
        ],
      },
    ]);
    const cliSource = fs.readFileSync(
      path.join(process.cwd(), "packages/collector-cli/src/cli.ts"),
      "utf8",
    );
    check(
      "daemon_sync_refreshes_touched_sessions_isolated",
      JSON.stringify(touchedIds) === JSON.stringify([v7Session, junkSessionRaw]) &&
        sessionIdsFromBatches([null]).length === 0 &&
        cliSource.includes("sessionIdsFromBatches(uploadedBatches)") &&
        cliSource.includes("ledgerDb: buffer.database") &&
        cliSource.includes("pendingSessionIds") &&
        /catch[\s\S]{0,200}session_sync_failed/.test(cliSource) &&
        cliSource.includes('flag("--sessions")'),
      JSON.stringify({ touchedIds, cliWired: cliSource.includes("ledgerDb: buffer.database") }),
    );
  }

  // 18. Outcomes sync (issue 0038 / cloud Phase D2): the locally-computed
  // session↔PR join crosses to the workspace's github-outcomes route with
  // deterministic ids, the report's exact join + rework-window semantics,
  // and nothing beyond what the local join already derives.
  {
    const d2Until = "2026-05-20T00:00:00.000Z";
    const d2Owner = "Acme";
    const d2Repo = "Widgets";
    const d2RepoHash = remoteLinkageHash(`https://github.com/${d2Owner}/${d2Repo}.git`)!;
    const otherRepoHash = remoteLinkageHash("https://github.com/acme/other.git")!;
    const loginBranchHash = branchLinkageHash("feature/login")!;

    const d2SessionA = "a1b2c3d4-0000-4000-8000-000000000001";
    const d2SessionB = "codex-session-raw"; // non-uuid → deterministic derive
    const d2SessionC = "c3c3c3c3-0000-4000-8000-000000000003";
    const d2SessionD = "d4d4d4d4-0000-4000-8000-000000000004";
    const d2SessionE = "e5e5e5e5-0000-4000-8000-000000000005";

    const d2Ledger = new Database(":memory:");
    d2Ledger.exec(
      `create table buffered_events (
         session_id text, observed_at text, created_at text,
         repo_hash text, branch_hash text, head_sha text
       )`,
    );
    const d2Insert = d2Ledger.prepare(
      "insert into buffered_events values (@sessionId, @observedAt, @createdAt, @repoHash, @branchHash, @headSha)",
    );
    const d2Row = (sessionId: string, fields: Partial<Record<"repoHash" | "branchHash" | "headSha", string>>, createdAt = "2026-05-10T00:00:00.000Z") =>
      d2Insert.run({
        sessionId,
        observedAt: "2026-05-10T00:00:00.000Z",
        createdAt,
        repoHash: null,
        branchHash: null,
        headSha: null,
        ...fields,
      });
    // A: 3 events, repo + login branch (joins #7 and #9 via branch_hash).
    d2Row(d2SessionA, { repoHash: d2RepoHash, branchHash: loginBranchHash });
    d2Row(d2SessionA, { repoHash: d2RepoHash, branchHash: loginBranchHash });
    d2Row(d2SessionA, { repoHash: d2RepoHash });
    // B: 5 events, head shas hitting #8 (head_sha) and #10 (merge_sha).
    for (let i = 0; i < 3; i += 1) d2Row(d2SessionB, { repoHash: d2RepoHash, headSha: "ffff7770000000000000" });
    d2Row(d2SessionB, { repoHash: d2RepoHash, headSha: "99887766aabb00000000" });
    d2Row(d2SessionB, { repoHash: d2RepoHash });
    // C: same branch hash but ANOTHER repo — the repo scope must exclude it.
    d2Row(d2SessionC, { repoHash: otherRepoHash, branchHash: loginBranchHash });
    // D: no linkage at all. E: only event lands after the watermark.
    d2Row(d2SessionD, {});
    d2Row(d2SessionE, { repoHash: d2RepoHash, branchHash: loginBranchHash }, "2026-05-21T00:00:00.000Z");

    const d2Pulls = [
      { number: 7, state: "open", merged_at: null, head: { ref: "feature/login", sha: "aaaa1110000000000000" }, merge_commit_sha: null, updated_at: "2026-05-15T00:00:00.000Z" },
      { number: 8, state: "closed", merged_at: "2026-05-10T00:00:00.000Z", head: { ref: "fix/payments", sha: "ffff7770000000000000" }, merge_commit_sha: "1234567890ab00000000", updated_at: "2026-05-10T00:00:00.000Z" },
      { number: 9, state: "closed", merged_at: "2026-05-08T00:00:00.000Z", head: { ref: "feature/login", sha: "bbbb2220000000000000" }, merge_commit_sha: "55556666777700000000", updated_at: "2026-05-12T00:00:00.000Z" },
      { number: 10, state: "closed", merged_at: "2026-05-01T00:00:00.000Z", head: { ref: "chore/deps", sha: "cccc3330000000000000" }, merge_commit_sha: "99887766aabb00000000", updated_at: "2026-05-18T00:00:00.000Z" },
      { number: 11, state: "closed", merged_at: "2026-03-01T00:00:00.000Z", head: { ref: "old/stale", sha: "dddd4440000000000000" }, merge_commit_sha: "eeee5550000000000000", updated_at: "2026-04-01T00:00:00.000Z" },
    ];
    const d2CheckRuns: Record<string, unknown> = {
      aaaa1110000000000000: { total_count: 0, check_runs: [] },
      ffff7770000000000000: { total_count: 2, check_runs: [{ status: "completed", conclusion: "success" }, { status: "completed", conclusion: "success" }] },
      bbbb2220000000000000: { total_count: 2, check_runs: [{ status: "completed", conclusion: "failure" }, { status: "completed", conclusion: "success" }] },
      cccc3330000000000000: { total_count: 1, check_runs: [{ status: "completed", conclusion: "success" }] },
    };
    // Revert names #8's merge sha; reopen on #9 is inside the 14d window,
    // reopen on #10 (merged 05-01, reopened 05-18) is outside it.
    const d2Commits = [
      { sha: "deadbeefcafe00000000", commit: { message: "Revert payment change\n\nThis reverts commit 1234567." } },
    ];
    const d2IssueEvents: Record<string, unknown> = {
      "8": [],
      "9": [{ event: "reopened", created_at: "2026-05-12T00:00:00.000Z" }],
      "10": [{ event: "reopened", created_at: "2026-05-18T00:00:00.000Z" }],
    };

    const D2_SECRET = "d2-proof-signing-secret-0123456789";
    const D2_INSTALL_KEY = "d2-proof-install-key";
    const d2PostedBodies: string[] = [];
    let d2SignatureFailures = 0;
    const d2Fetch: typeof fetch = async (input, init) => {
      const url = String(input);
      const json = (payload: unknown) =>
        new Response(JSON.stringify(payload), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("api.github.com")) {
        if (url.includes("/pulls?")) return json(d2Pulls);
        const checkMatch = url.match(/commits\/([0-9a-f]+)\/check-runs/);
        if (checkMatch) return json(d2CheckRuns[checkMatch[1]] ?? { total_count: 0, check_runs: [] });
        if (url.includes("/commits?since=")) return json(d2Commits);
        const eventsMatch = url.match(/issues\/(\d+)\/events/);
        if (eventsMatch) return json(d2IssueEvents[eventsMatch[1]] ?? []);
        return json([]);
      }
      // The workspace route: verify the HMAC like the cloud does, then echo
      // honest counters parsed from the actual body.
      const body = String(init?.body ?? "");
      const headers = new Headers(init?.headers as HeadersInit);
      const timestamp = headers.get("x-plimsoll-upload-timestamp") ?? "";
      const expected = `sha256=${crypto.createHmac("sha256", D2_SECRET).update(`${timestamp}.${body}`).digest("hex")}`;
      if (headers.get("x-plimsoll-upload-signature") !== expected || headers.get("x-plimsoll-install-key") !== D2_INSTALL_KEY) {
        d2SignatureFailures += 1;
        return new Response(JSON.stringify({ error: "bad_upload_signature" }), { status: 401 });
      }
      d2PostedBodies.push(body);
      const parsed = JSON.parse(body) as { artifacts: unknown[]; outcomes: unknown[] };
      return json({
        ok: true,
        acceptedArtifacts: parsed.artifacts.length,
        acceptedOutcomes: parsed.outcomes.length,
        detachedActorRefs: 0,
        detachedSessionRefs: 0,
      });
    };

    const d2Config = collectorConfigSchema.parse({
      uploadUrl: "https://workspace.example/api/work-intelligence/ingest",
      installKey: D2_INSTALL_KEY,
      uploadSigningSecret: D2_SECRET,
      tenantId: "44444444-5555-4666-8777-888888888888",
    });
    const d2Logs: string[] = [];
    const d2Log = (line: string) => d2Logs.push(line);

    // 18a. Join parity + watermark + repo scope (pure halves).
    const d2Since = new Date(Date.parse(d2Until) - 30 * 24 * 60 * 60 * 1000).toISOString();
    const d2Sessions = collectSessionLinks(d2Ledger, { since: d2Since, until: d2Until });
    const d2PullSummaries: PullOutcome[] = d2Pulls
      .filter((pull) => pull.updated_at >= d2Since)
      .map((pull) => ({
        number: pull.number,
        state: pull.state,
        merged: Boolean(pull.merged_at),
        mergedAt: pull.merged_at ?? undefined,
        branchHash: branchLinkageHash(pull.head.ref),
        headSha: pull.head.sha,
        mergeCommitSha: pull.merge_commit_sha ?? undefined,
        updatedAt: pull.updated_at,
        checks: "unknown",
        checksFetched: false,
      }));
    const d2Joins = joinSessionsToPulls(d2Sessions, d2PullSummaries, d2RepoHash);
    const joinKey = (join: { pull: number; sessionId: string; via: string }) => `${join.pull}:${join.sessionId}:${join.via}`;
    const d2JoinSet = new Set(d2Joins.map(joinKey));
    check(
      "outcomes_join_parity_watermark_and_repo_scope",
      d2Sessions.length === 4 &&
        !d2Sessions.some((row) => row.sessionId === d2SessionE) &&
        d2JoinSet.size === 4 &&
        d2JoinSet.has(`7:${d2SessionA}:branch_hash`) &&
        d2JoinSet.has(`9:${d2SessionA}:branch_hash`) &&
        d2JoinSet.has(`8:${d2SessionB}:head_sha`) &&
        d2JoinSet.has(`10:${d2SessionB}:merge_sha`) &&
        !d2Joins.some((join) => join.sessionId === d2SessionC || join.sessionId === d2SessionD),
      JSON.stringify({ sessions: d2Sessions.length, joins: [...d2JoinSet].sort() }),
    );

    // 18b. Rework-window parity with the report's own yield-v2 exclusions.
    const d2Signals = [
      { pull: 8, kind: "revert" as const, evidence: "revert deadbeefc", at: "2026-05-10T00:00:00.000Z" },
      { pull: 9, kind: "reopen" as const, evidence: "reopened 2026-05-12", at: "2026-05-12T00:00:00.000Z" },
      { pull: 10, kind: "reopen" as const, evidence: "reopened 2026-05-18", at: "2026-05-18T00:00:00.000Z" },
    ];
    const d2Eligible = [
      { pull: 8, mergedAt: "2026-05-10T00:00:00.000Z" },
      { pull: 9, mergedAt: "2026-05-08T00:00:00.000Z" },
      { pull: 10, mergedAt: "2026-05-01T00:00:00.000Z" },
    ];
    const windowed = reworkSignalsInWindow(d2Eligible, d2Signals, 14);
    const v2 = validatedDeliveryYieldV2(d2Eligible, d2Signals, 14);
    check(
      "outcomes_rework_window_parity_with_yield_v2",
      JSON.stringify([...windowed.keys()].sort()) === JSON.stringify(v2.excluded.map((entry) => entry.pull).sort()) &&
        windowed.has(8) &&
        windowed.has(9) &&
        !windowed.has(10) &&
        v2.numerator === 1,
      JSON.stringify({ windowedPulls: [...windowed.keys()].sort(), v2Excluded: v2.excluded }),
    );

    // 18c. Deterministic ids, schema shape, session linkage, privacy gate.
    const d2BuildInput = {
      tenantId: d2Config.tenantId,
      owner: d2Owner,
      repo: d2Repo,
      pulls: d2PullSummaries,
      joins: d2Joins,
      signals: d2Signals,
      reworkWindowDays: 14,
    };
    const push1 = buildOutcomePush(d2BuildInput);
    const push2 = buildOutcomePush(d2BuildInput);
    const artifact8 = push1.batch?.artifacts.find((artifact) => artifact.externalId.endsWith("/pull/8"));
    const artifact7 = push1.batch?.artifacts.find((artifact) => artifact.externalId.endsWith("/pull/7"));
    const derivedB = ensureUuidSessionId(d2SessionB);
    check(
      "outcomes_push_deterministic_ids_shape_and_linkage",
      push1.batch !== null &&
        JSON.stringify(push1.batch) === JSON.stringify(push2.batch) &&
        push1.batch.repository.remoteUrlHash === d2RepoHash &&
        push1.batch.repository.owner === "acme" &&
        push1.batch.repository.name === "widgets" &&
        artifact7?.id === "artifact:github.com/acme/widgets/pull/7" &&
        artifact7.sessionId === d2SessionA &&
        artifact8?.sessionId === derivedB.id &&
        derivedB.derived &&
        (artifact8.metadata as { branchHash?: string }).branchHash === branchLinkageHash("fix/payments") &&
        push1.batch.artifacts.every(
          (artifact) => findForbiddenRawContentFields(artifact.metadata ?? {}).length === 0,
        ) &&
        push1.batch.outcomes.every(
          (outcome) => findForbiddenRawContentFields(outcome.metadata ?? {}).length === 0,
        ) &&
        push1.batch.outcomes.some((outcome) => outcome.id === "outcome:github.com/acme/widgets/pull/8:reverted"),
      JSON.stringify({
        deterministic: JSON.stringify(push1.batch) === JSON.stringify(push2.batch),
        artifacts: push1.artifacts,
        outcomes: push1.outcomes,
        artifact8Session: artifact8?.sessionId,
      }),
    );

    // 18d. End-to-end: signed push, honest counters, statuses/outcomes per
    // PR, dry-run sends nothing, and a re-run posts a byte-identical batch.
    const d2Run1 = await runOutcomesSync(d2Config, {
      repository: `${d2Owner}/${d2Repo}`,
      until: d2Until,
      ledgerDb: d2Ledger,
      fetchImpl: d2Fetch,
      log: d2Log,
    });
    const d2Run2 = await runOutcomesSync(d2Config, {
      repository: `${d2Owner}/${d2Repo}`,
      until: d2Until,
      ledgerDb: d2Ledger,
      fetchImpl: d2Fetch,
      log: d2Log,
    });
    const postsBeforeDry = d2PostedBodies.length;
    const d2Dry = await runOutcomesSync(d2Config, {
      repository: `${d2Owner}/${d2Repo}`,
      until: d2Until,
      ledgerDb: d2Ledger,
      fetchImpl: d2Fetch,
      dryRun: true,
      log: d2Log,
    });
    const run1Batch = JSON.parse(d2PostedBodies[0] ?? "{}") as {
      artifacts: Array<{ externalId: string; status: string }>;
      outcomes: Array<{ id: string }>;
    };
    const statusByPull = new Map(
      run1Batch.artifacts.map((artifact) => [artifact.externalId.split("/pull/")[1], artifact.status]),
    );
    check(
      "outcomes_sync_e2e_signed_idempotent_dry_run",
      d2Run1.ok &&
        d2Run1.pullsExamined === 4 &&
        d2Run1.pullsJoined === 4 &&
        d2Run1.artifactsSent === 4 &&
        d2Run1.artifactsAccepted === 4 &&
        d2Run1.outcomesSent === 9 &&
        d2Run1.outcomesAccepted === 9 &&
        d2Run1.detachedSessionRefs === 0 &&
        statusByPull.get("7") === "created" &&
        statusByPull.get("8") === "reverted" &&
        statusByPull.get("9") === "reopened" &&
        statusByPull.get("10") === "merged" &&
        d2Run2.ok &&
        d2PostedBodies.length === 2 &&
        d2PostedBodies[0] === d2PostedBodies[1] &&
        d2Dry.ok &&
        d2Dry.dryRun &&
        d2PostedBodies.length === postsBeforeDry &&
        d2Dry.artifactsAccepted === null &&
        d2SignatureFailures === 0 &&
        d2Logs.every((line) => !line.includes(D2_INSTALL_KEY)) &&
        d2Run1.auditTable.includes("#8"),
      JSON.stringify({
        run1: {
          artifactsAccepted: d2Run1.artifactsAccepted,
          outcomesAccepted: d2Run1.outcomesAccepted,
          statuses: [...statusByPull.entries()].sort(),
        },
        idempotent: d2PostedBodies[0] === d2PostedBodies[1],
      }),
    );
    d2Ledger.close();

    // 18e. CLI + disclosure wiring: the command exists, requires the
    // explicit repo disclosure, and the module never ships raw PR titles —
    // the only rework evidence is sha/url shaped.
    const outcomesSource = fs.readFileSync(
      path.join(process.cwd(), "packages/collector-cli/src/outcomes-sync.ts"),
      "utf8",
    );
    const cliSourceD2 = fs.readFileSync(
      path.join(process.cwd(), "packages/collector-cli/src/cli.ts"),
      "utf8",
    );
    check(
      "outcomes_cli_disclosure_and_no_raw_content_wired",
      cliSourceD2.includes('command === "sync-outcomes"') &&
        cliSourceD2.includes("runOutcomesSync(config") &&
        /sync-outcomes requires --repository/.test(cliSourceD2) &&
        outcomesSource.includes('"/api/work-intelligence/github-outcomes"') &&
        outcomesSource.includes("findForbiddenRawContentFields") &&
        !/pull\.title|\btitle\b\s*:/.test(outcomesSource) &&
        outcomesSource.includes("ensureUuidSessionId"),
      "sync-outcomes command wired with required --repository; outcomes module derives the C8 route path, runs the forbidden-content gate, links sessions via the D1 id mapping, and never holds PR titles",
    );
  }

  fs.rmSync(tempDir, { recursive: true, force: true });

  const passed = checks.every((entry) => entry.passed);
  const generatedAt = new Date(SYSTEM_DATE_NOW()).toISOString();
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

if (process.env.PLIMSOLL_PROOF_CLOCK_CASE === "1") {
  const restoreProofDateNow = installProofDateNow();
  try {
    console.log(JSON.stringify(proofClockWindowFixture()));
  } finally {
    restoreProofDateNow();
  }
} else {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
