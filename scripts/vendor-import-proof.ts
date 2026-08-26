#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { dashboardSummary } from "../packages/collector-cli/src/dashboard-api";
import { buildIngestBatch } from "../packages/collector-cli/src/upload";
import { importVendorCsv } from "../packages/collector-cli/src/vendor-import";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const NOW = new Date("2026-05-09T12:00:00.000Z");
const originalDateNow = Date.now;
Date.now = () => NOW.getTime();

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function settle(buffer: LocalEventBuffer) {
  for (let index = 0; index < 100; index += 1) {
    const state = buffer.projection.status();
    if (state.ready && !state.dirty && Object.values(state.backlog).every((value) => value === 0)) {
      return;
    }
    buffer.projection.runMaintenance(NOW);
  }
  throw new Error(`projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}

function localEvent() {
  return aiInteractionEventSchema.parse({
    id: "00000000-0000-4000-8000-000000000001",
    source: "claude_code",
    eventType: "assistant_response",
    observedAt: "2026-05-08T10:00:00.000Z",
    sessionId: "local-session",
    inputTokens: 100,
    outputTokens: 10,
    costUsd: 5,
    actorId: "sha256:local-account",
    metadata: { git: { remoteUrlHash: "sha256:" + "a".repeat(64) } },
  });
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-vendor-import-proof-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
    workspaceId: "local-workspace",
    deviceId: "proof-device",
  });
  const config = collectorConfigSchema.parse({
    tenantId: "local-workspace",
    installKey: "proof-install",
    deviceId: "proof-device",
  });
  const privateSentinel = "PRIVATE_VENDOR_WORKSPACE_LABEL";
  const secretSentinel = "sk-private-vendor-export";
  const csv = [
    `Date,Input Tokens,Output Tokens,Cache Read Input Tokens,Cache Creation Input Tokens,Cost (USD),Workspace,API Key`,
    `2020-01-01,1,2,3,4,1.25,${privateSentinel},${secretSentinel}`,
    `2020-01-01,4,5,6,7,2.75,${privateSentinel},${secretSentinel}`,
    `2026-05-07,10,20,30,40,3.00,${privateSentinel},${secretSentinel}`,
    `2026-05-08,999,999,999,999,99.00,${privateSentinel},${secretSentinel}`,
    `2026-05-09,888,888,888,888,88.00,${privateSentinel},${secretSentinel}`,
  ].join("\n");

  try {
    buffer.setAccountLabel("sha256:local-account", privateSentinel);
    buffer.recordRepoLabel("sha256:" + "a".repeat(64), privateSentinel);
    buffer.append(localEvent());
    const first = importVendorCsv(buffer, "anthropic", csv);
    check("anthropic_csv_import_is_vendor_reported", first.importedDays === 2, first);
    check("captured_overlap_day_wins", first.skippedOverlapDays === 1, first);
    check("post_floor_day_is_not_imported", first.skippedLocalFloorDays === 1, first);
    check("pre_floor_day_is_imported", first.importedDaysByDay.includes("2026-05-07"), first);

    const rows = buffer.database.prepare(
      `select source,session_id as sessionId,observed_at as observedAt,
         input_tokens as inputTokens,output_tokens as outputTokens,
         cache_read_tokens as cacheReadTokens,cache_creation_tokens as cacheCreationTokens,
         cost_usd as costUsd,payload_json as payloadJson
       from buffered_events order by observed_at`,
    ).all() as Array<Record<string, unknown>>;
    const vendorRows = rows.filter((row) => row.source === "vendor_import");
    check("vendor_rows_have_daily_grain_and_no_fake_sessions",
      vendorRows.length === 2 && vendorRows.every((row) => row.sessionId === null),
      { vendorRows: vendorRows.length, sessionIds: vendorRows.map((row) => row.sessionId) });
    const january = vendorRows.find((row) => row.observedAt === "2020-01-01T00:00:00.000Z");
    check("duplicate_csv_days_aggregate_once",
      january?.inputTokens === 5 && january?.outputTokens === 7 && january?.costUsd === 4,
      { january });

    settle(buffer);
    const all = buffer.projection.readSnapshot(1825);
    assert.equal(all.kind, "ready", JSON.stringify(all));
    const summary = all.kind === "ready" ? all.snapshot.summary as Record<string, unknown> : {};
    const totals = summary.totals as Record<string, unknown>;
    check("all_window_reaches_earliest_vendor_day",
      all.kind === "ready" && all.snapshot.window.since === "2020-01-01T00:00:00.000Z",
      { since: all.kind === "ready" ? all.snapshot.window.since : null });
    check("overlap_day_is_not_double_counted",
      totals.costUsd === 12 && totals.inputTokens === 115 && totals.outputTokens === 37,
      { costUsd: totals.costUsd, inputTokens: totals.inputTokens, outputTokens: totals.outputTokens });
    const daily = (summary.daily ?? []) as Array<Record<string, unknown>>;
    const vendorDay = daily.find((row) => row.day === "2020-01-01");
    check("daily_provenance_is_explicit",
      vendorDay?.vendorReported === true && vendorDay?.vendorCostUsd === 4 && vendorDay?.vendorTokens === 12,
      { vendorDay });
    const raw = dashboardSummary(buffer.database, 1825);
    const rawTotals = raw.totals as Record<string, unknown>;
    const rawVendorDay = (raw.daily as Array<Record<string, unknown>>).find((row) => row.day === "2020-01-01");
    check("raw_dashboard_api_uses_extended_all_window",
      rawTotals.costUsd === 12 && rawVendorDay?.vendorEvents === 1 && rawVendorDay?.vendorCostUsd === 4,
      { since: raw.since, totals: rawTotals, rawVendorDay });

    const second = importVendorCsv(buffer, "anthropic", csv);
    check("reimport_is_idempotent", second.deduplicatedDays === 2 && second.importedDays === 0, second);

    const batch = buildIngestBatch(config, buffer).batch;
    const wireText = JSON.stringify(batch);
    check("upload_omits_local_labels_and_raw_identifiers",
      !wireText.includes(privateSentinel) && !wireText.includes(secretSentinel) &&
        batch?.events.some((entry) => entry.event.source === "vendor_import" &&
          entry.event.sessionId === undefined && entry.event.metadata.usageSource === "anthropic"),
      { bytes: Buffer.byteLength(wireText), events: batch?.events.length ?? 0 });
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
    Date.now = originalDateNow;
  }

  console.log(JSON.stringify({ proof: "vendor-import", passed: true, checks }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
