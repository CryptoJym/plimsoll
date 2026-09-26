import assert from "node:assert/strict";
import fs from "node:fs";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";

import { BudgetSampler } from "../packages/collector-cli/src/budget-sampler";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  advanceCaptureFrontier, applyCaptureCoverage, beginCaptureCoverage,
  finishCaptureCoverage, hasCompleteCaptureCoverage,
} from "../packages/collector-cli/src/capture-frontier";
import { rootCursorKey, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { runSessionSync } from "../packages/collector-cli/src/session-sync";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

// The two tailers share one ledger with the budget sampler and summary sync.
// Each walk is suspended in a growing folder while a saved partial file
// disappears; its seen partials must also enter the revisit queue.
const temp = fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir());
const home = fs.mkdtempSync(path.join(temp, "integration-042-"));
const ledger = path.join(home, "ledger.sqlite");
const tenant = "00000000-0000-4000-8000-000000000742";
const sid = "00000000-0000-4000-8000-000000000741";
const buffer = new LocalEventBuffer(ledger, {
  workspaceId: tenant, delivery: { enabled: true },
  enrollmentNow: () => new Date(Date.now() - 10 * 86_400_000),
});
const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
const codexDir = path.join(home, "codex");
const claudeDir = path.join(home, "claude");
fs.mkdirSync(codexDir); fs.mkdirSync(claudeDir);
const roots: CaptureRoot[] = [
  { source: "codex", directory: codexDir, rootId: "codex", profileId: "codex", installationEpochId: epoch },
  { source: "claude_code", directory: claudeDir, rootId: "claude", profileId: "claude", installationEpochId: epoch },
];
const inspected = new Map<string, Set<string>>([
  ["codex", new Set()], ["claude_code", new Set()],
]);
const io = { ...DEFAULT_JSONL_TAILER_IO, lstat: (file: string) => {
  inspected.get(file.startsWith(codexDir) ? "codex" : "claude_code")!.add(file);
  return fs.lstatSync(file);
} };
const rollout = new RolloutTailer(buffer, undefined, () => [], io, roots.slice(0, 1));
const transcript = new TranscriptTailer(buffer, undefined, io, roots.slice(1));
const maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, undefined,
  { captureCoverageIntervalMs: 0, captureCoverageTurnMs: 250 });
const sampler = new BudgetSampler(buffer.database, ledger, 60_000,
  () => buffer.budgetAttemptedTotal());
const checks: string[] = [];
function check(name: string, condition: unknown) {
  assert.ok(condition, name);
  checks.push(name);
}
const key = (file: string) => jsonlScanStateKey(rootCursorKey(roots, file));

function appendPairedCodexUsage() {
  const at = Date.now() - 30_000;
  const nano = (ms: number) => String(BigInt(ms) * 1_000_000n);
  const attr = (key: string, value: string | number) => ({ key,
    value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } });
  const resource = { attributes: [attr("service.name", "codex-app-server")] };
  const fixtures = [
    { resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{ timeUnixNano: nano(at),
      attributes: [attr("event.name", "codex.sse_event"), attr("event.kind", "response.completed"),
        attr("input_token_count", "40"), attr("output_token_count", "5"),
        attr("cached_token_count", 9), attr("event.timestamp", new Date(at).toISOString()),
        attr("conversation.id", sid), attr("model", "gpt-5.1-codex-max"),
        attr("user.account_id", "integration-synthetic-account")],
    }] }] }] },
    { resourceSpans: [{ resource, scopeSpans: [{ spans: [{
      traceId: "1".padStart(32, "0"), spanId: "1".padStart(16, "0"), name: "handle_responses",
      kind: 1, startTimeUnixNano: nano(at - 1_500), endTimeUnixNano: nano(at + 20),
      attributes: [attr("gen_ai.usage.input_tokens", 40),
        attr("gen_ai.usage.output_tokens", 5),
        attr("gen_ai.usage.cache_read.input_tokens", 9)],
    }] }] }] },
  ];
  for (const fixture of fixtures) {
    const parsed = explodeOtlpPayload(fixture, { source: "codex" });
    check("codex_pair_fixture_parsed", parsed.events.length === 1 && parsed.parseFailures === 0);
    const row = parsed.events[0]!;
    check("codex_pair_fixture_appended", buffer.append(row.event, row.suppressedFields));
  }
  const rows = buffer.database.prepare(`select count(*) as total,
    sum(case when input_tokens is not null then 1 else 0 end) as eligible,
    sum(case when usage_duplicate_reason = 'codex_sse_event_span' then 1 else 0 end) as marked
    from buffered_events where source = 'codex' and
      json_extract(payload_json, '$.metadata.otelEventName') in ('codex.sse_event','handle_responses')`)
    .get() as { total: number; eligible: number; marked: number };
  check("codex_pair_on_shared_ledger_counts_once", rows.total === 2 && rows.eligible === 1 && rows.marked === 1);
  check("budget_counts_both_successful_raw_inserts", buffer.budgetAttemptedTotal() === 2);
}

async function walkSource(source: "codex" | "claude_code") {
  const directory = source === "codex"
    ? path.join(codexDir, "2026", "09", "25")
    : path.join(claudeDir, "project");
  fs.mkdirSync(directory, { recursive: true });
  const files = Array.from({ length: 800 }, (_, i) => path.join(directory,
    source === "codex" ? `rollout-${String(i).padStart(4, "0")}.jsonl`
      : `session-${String(i).padStart(4, "0")}.jsonl`));
  const kind = source === "codex" ? "codex-rollout-v2" : "claude-transcript-v3";
  const insert = buffer.database.prepare(`insert into rollout_scan_state
    (file,size,scanned_at,committed_offset,deferred_bytes,work_remaining,parser_kind)
    values (?,3,?,0,0,0,?)`);
  buffer.database.transaction(() => {
    for (const file of files) {
      fs.writeFileSync(file, "{}\n");
      insert.run(key(file), new Date().toISOString(), kind);
    }
  })();
  const startedAt = new Date().toISOString();
  const coverage = beginCaptureCoverage(buffer.database, source, startedAt);
  assert.ok(coverage);
  const tailer = source === "codex" ? rollout : transcript;
  const walk = tailer.coverageWalk();
  const batches: Array<{ key: string; fullyRead: boolean }> = [];
  const step = () => walk.step(1, (rows) => {
    batches.push(...rows.map(row => ({ key: row.key, fullyRead: row.fullyRead })));
    applyCaptureCoverage(buffer.database, coverage, rows);
  }, () => 0, 80);
  let turns = 0;
  while (inspected.get(source)!.size === 0 && !walk.done && ++turns < 40) step();
  const seen = inspected.get(source)!;
  check(`${source}_walk_suspends_with_checked_and_unseen_files`,
    seen.size > 0 && seen.size < files.length && !walk.done);
  const hot = [...seen][0]!;
  const vanished = files.find(file => !seen.has(file))!;
  fs.unlinkSync(vanished);
  fs.appendFileSync(hot, "{}\n");
  const newFile = path.join(directory, source === "codex" ? "rollout-grown.jsonl" : "session-grown.jsonl");
  fs.writeFileSync(newFile, "{}\n");
  while (!walk.done && ++turns < 200) step();
  check(`${source}_growing_folder_walk_completes`, walk.done && walk.complete);
  if (walk.complete) finishCaptureCoverage(buffer.database, coverage);
  const lost = buffer.database.prepare(`select 1 from capture_uncovered_files
    where source=? and file_key=?`).get(source, key(vanished));
  check(`${source}_unseen_partial_deletion_is_durable_gap`,
    !!lost && batches.some(row => row.key === key(vanished) && !row.fullyRead));
  const revisit = (tailer as unknown as { revisit: { next(limit: number): string[] } }).revisit.next(1024);
  check(`${source}_revisit_survives_growing_folder_cursor`, revisit.includes(hot));
  walk.close();
  const nextWalk = tailer.coverageWalk();
  let nextTurns = 0;
  while (!nextWalk.done && ++nextTurns < 200) {
    nextWalk.step(1, () => {}, () => 0, 80);
  }
  check(`${source}_next_walk_sees_grown_file`,
    nextWalk.done && nextWalk.complete && seen.has(newFile));
  nextWalk.close();
  return { source, turns, nextTurns, inspected: seen.size, grewFileSeen: seen.has(newFile) };
}

async function sync() {
  const config = collectorConfigSchema.parse({ uploadUrl: "http://127.0.0.1:1/ingest",
    tenantId: tenant, installKey: "integration-042", uploadSigningSecret: "synthetic-secret-only" });
  const sent: unknown[] = [];
  const result = await runSessionSync(config, { ledgerDb: buffer.database, incremental: true,
    sessionIds: [sid], until: new Date(Date.now() + 60_000).toISOString(),
    developmentLoopbackUrl: true, delayMs: 0, maxAttemptsPerBatch: 1, log: () => {},
    fetchImpl: async (_input, init) => {
      const body = String(init?.body ?? "");
      sent.push(JSON.parse(body));
      return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey)),
        { status: 200, headers: { "content-type": "application/json" } });
    } });
  check("summary_sync_sends_on_shared_ledger", result.ok && result.sentSessions === 1 && sent.length === 1);
}

async function statusOnSharedLedger() {
  let clockReads = 0;
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    requestBudgetNow: () => { clockReads += 1; return performance.now(); },
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const response = await fetch(`http://127.0.0.1:${port}/status`, {
      headers: { connection: "close" }, signal: AbortSignal.timeout(5_000),
    });
    await response.json();
    check("http_budget_clock_reads_shared_pairing_and_coverage_ledger",
      response.status === 200 && clockReads > 0 &&
      hasCompleteCaptureCoverage(buffer.database, "codex") &&
      hasCompleteCaptureCoverage(buffer.database, "claude_code"));
  } finally {
    if (server.listening) {
      server.closeAllConnections();
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  }
}

async function main() {
try {
  for (const source of ["codex", "claude_code"] as const) {
    advanceCaptureFrontier(buffer.database, source, { complete: true, files: [] },
      new Date(Date.now() - 120_000).toISOString());
    check(`${source}_prior_complete_frontier`, hasCompleteCaptureCoverage(buffer.database, source));
  }
  const at = new Date(Date.now() - 60_000).toISOString();
  buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,
     created_at,session_id,input_tokens,output_tokens,workspace_id,privacy_generation)
    values (?,?,?,'metadata',?,'{}','[]',?,?,2,1,?,?)`)
    .run("00000000-0000-4000-8000-000000000743", "codex", "assistant_response",
      at, at, sid, tenant, "integration-042");
  await sync();
  appendPairedCodexUsage();
  await sync();
  const observations = [];
  for (const source of ["codex", "claude_code"] as const) observations.push(await walkSource(source));
  await sampler.sample();
  await new Promise(resolve => setTimeout(resolve, 2));
  let maintenanceTurns = 0;
  do {
    (maintenance as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();
    await sampler.sample();
    maintenanceTurns++;
  } while ((maintenance as unknown as { coverageWalks: unknown }).coverageWalks && maintenanceTurns < 30);
  check("maintenance_coverage_completes_while_budget_samples", maintenanceTurns < 30 &&
    hasCompleteCaptureCoverage(buffer.database, "codex") &&
    hasCompleteCaptureCoverage(buffer.database, "claude_code"));
  check("budget_samples_persist_on_coverage_ledger",
    (buffer.database.prepare("select count(*) as n from budget_samples").get() as { n: number }).n >= 2 &&
    sampler.status().latest !== null);
  check("budget_sample_includes_paired_raw_inserts",
    (sampler.status().latest?.attemptedRowsDelta ?? 0) >= 2 ||
    (buffer.database.prepare(`select coalesce(sum(json_extract(sample_json,'$.attemptedRowsDelta')),0) as n
      from budget_samples`).get() as { n: number }).n >= 2);
  const before = (buffer.database.prepare(`select activity_revision as n from session_sync_summary_activity
    where session_id=?`).get(sid) as { n: number } | undefined)?.n ?? 0;
  buffer.database.prepare("update buffered_events set input_tokens=3 where session_id=?").run(sid);
  const after = (buffer.database.prepare(`select activity_revision as n from session_sync_summary_activity
    where session_id=?`).get(sid) as { n: number } | undefined)?.n ?? 0;
  check("summary_activity_revision_survives_capture_and_budget", after > before);
  await statusOnSharedLedger();
  check("http_status_preserves_summary_revision", (buffer.database.prepare(`select activity_revision as n
    from session_sync_summary_activity where session_id=?`).get(sid) as { n: number }).n === after);
  await sync();
  console.log(JSON.stringify({ proof: "integration-042-crosscheck", checks, observations, maintenanceTurns,
    passed: true }, null, 2));
} finally {
  sampler.stop();
  maintenance.close();
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
}

main().catch(error => { console.error(error); process.exitCode = 1; });
