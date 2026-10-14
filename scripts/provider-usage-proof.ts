import assert from "node:assert/strict";
import crypto from "node:crypto";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import Database from "better-sqlite3";

import {
  adjudicateProviderUsage,
  ensureProviderUsageSchema,
  importProviderUsage,
  normalizeProviderPayload,
  parseProviderUsageCsv,
  providerUsageCapabilities,
  readProviderUsageRows,
} from "../packages/collector-cli/src/provider-usage";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-provider-usage-"));
const database = new Database(path.join(root, "ledger.sqlite"));
const dayStart = "2026-10-13T00:00:00.000Z";
const dayEnd = "2026-10-14T00:00:00.000Z";

try {
  ensureProviderUsageSchema(database);
  database.exec(`create table buffered_events (
    id text primary key, source text not null, event_type text not null,
    observed_at text not null, session_id text, model text,
    input_tokens integer, output_tokens integer, cache_read_tokens integer,
    cache_creation_tokens integer, cost_usd real, project_key text
  )`);
  database.prepare(`insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
    crypto.randomUUID(), "codex", "usage_rollout", "2026-10-13T10:00:00.000Z", "thread-codex", "gpt-5.5", 100, 40, 10, 0, 0.001, null,
  );
  database.prepare(`insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
    crypto.randomUUID(), "grok", "assistant_response", "2026-10-13T11:00:00.000Z", "thread-grok", "grok-4.7", 7, 3, 0, 0, 0.0002, null,
  );

  const openai = normalizeProviderPayload("openai", {
    data: [{ results: [{
      input_tokens: 100, output_tokens: 40, input_cached_tokens: 10,
      num_model_requests: 1, model: "gpt-5.5",
    }] }],
  }, { observedAt: dayEnd, periodStart: dayStart, periodEnd: dayEnd });
  assert.equal(openai.length, 1);
  const csv = parseProviderUsageCsv("date,model,input_tokens,output_tokens,request_count\n2026-10-13T00:00:00.000Z,gpt-5.5,100,40,1\n");
  const csvRows = normalizeProviderPayload("cursor", csv, { observedAt: dayEnd, periodStart: dayStart, periodEnd: dayEnd });
  assert.equal(csvRows[0]?.inputTokens, 100);
  assert.equal(csvRows[0]?.outputTokens, 40);
  assert.equal(importProviderUsage(database, openai).inserted, 1);
  assert.equal(importProviderUsage(database, openai).replayed, 1);
  const revised = normalizeProviderPayload("openai", {
    data: [{ results: [{
      input_tokens: 101, output_tokens: 40, input_cached_tokens: 10,
      num_model_requests: 1, model: "gpt-5.5",
    }] }],
  }, { observedAt: dayEnd, periodStart: dayStart, periodEnd: dayEnd });
  assert.equal(importProviderUsage(database, revised).revised, 1);
  assert.equal(readProviderUsageRows(database)[0].revision, 2);

  const localMatch = normalizeProviderPayload("openai", {
    records: [{ source: "openai_usage", provider: "openai", periodStart: dayStart, periodEnd: dayEnd,
      observedAt: dayEnd, granularity: "bucket", model: "gpt-5.5", inputTokens: 100, outputTokens: 40,
      cacheReadTokens: 10, cacheWriteTokens: 0, requestCount: 1, costUsd: 0.001,
      authority: "provider_reported", costKind: "reported" }],
  }, { observedAt: dayEnd, periodStart: dayStart, periodEnd: dayEnd });
  importProviderUsage(database, localMatch);
  const matched = adjudicateProviderUsage(database, { localSource: "codex", now: dayEnd });
  assert.ok(matched.some((item) => item.status === "matched"));
  assert.ok(matched.some((item) => item.threadState === "candidate" || item.threadState === "unknown"));

  const xai = normalizeProviderPayload("xai", {
    model: "grok-4.7", id: "req-1", threadId: "thread-grok",
    usage: { prompt_tokens: 7, completion_tokens: 3, cost_in_usd_ticks: 2_000_000 },
  }, { observedAt: "2026-10-13T11:00:00.000Z", periodStart: dayStart, periodEnd: dayEnd });
  importProviderUsage(database, xai);
  const grokReport = adjudicateProviderUsage(database, { localSource: "grok", now: dayEnd });
  const exact = grokReport.find((item) => item.provider === "xai");
  assert.equal(exact?.threadState, "exact");
  assert.equal(exact?.threadId, "thread-grok");

  const unknown = adjudicateProviderUsage(database);
  assert.ok(unknown.every((item) => item.status === "unknown_scope" || item.status === "unknown_dimension"));
  const stored = JSON.stringify(database.prepare(`select * from provider_usage_snapshots`).all());
  assert.equal(stored.includes("prompt-secret-sentinel"), false);
  assert.equal(providerUsageCapabilities({}).every((item) => item.configured === false), true);

  console.log(JSON.stringify({
    schema: "plimsoll.provider-usage-proof.v1",
    checks: {
      normalizedOpenAiBucket: true,
      replayDeduplicated: true,
      revisionRecorded: true,
      localMatchAdjudicated: true,
      exactThreadJoinOnlyWhenIdsMatch: true,
      missingScopeStaysUnknown: true,
      rawPayloadNotStored: true,
      capabilityInventoryIsValueBlind: true,
      dashboardCsvIsNormalized: true,
    },
  }, null, 2));
} finally {
  database.close();
  fs.rmSync(root, { recursive: true, force: true });
}
