import crypto from "node:crypto";
import { gzipSync } from "node:zlib";
import type Database from "better-sqlite3";

import { LOCAL_TENANT_ID } from "../../shared/src/index";
import type { CollectorConfig } from "./config";
import { authenticatedJsonPost, MAX_POST_BYTES, validatedTransportUrl } from "./http-transport";
import { aggregateToolStatsWeek, countsOnlyToolStats, foldToolStatsVersions, selectToolStatsSessions,
  utcWeekStart, type UploadWeeklyToolStats } from "./weekly-tool-stats";

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
    .map(([key, item]) => `${JSON.stringify(key)}:${stableJson(item)}`).join(",")}}`;
  return JSON.stringify(value);
}

function digest(value: unknown): string {
  return crypto.createHash("sha256").update(stableJson(value)).digest("hex");
}

function reportBody(input: { tenantId: string; deviceId: string; reportSequence: number;
  observedAt: string; toolStats: UploadWeeklyToolStats; compressed?: boolean }): string {
  const toolStats = input.compressed
    ? { encoding: "gzip-base64", data: gzipSync(JSON.stringify(input.toolStats), { level: 9 }).toString("base64") }
    : input.toolStats;
  return JSON.stringify({ schema: "fleet-device-report/v1", kind: "tool_stats",
    tenantId: input.tenantId, deviceId: input.deviceId, reportSequence: input.reportSequence,
    observedAt: input.observedAt, toolStats });
}

type Pending = { weekStart: string; reportSequence: number; digest: string; bodyJson: string; delivered: number };
type Receipt = { schema?: unknown; deviceId?: unknown; reportSequence?: unknown;
  disposition?: unknown; nextReportSequence?: unknown; toolStatsDigest?: unknown;
  storedToolStatsReport?: unknown };

/** One frozen report per completed week; a lost response retries identical bytes and sequence. */
export async function uploadCompletedToolStatsWeek(
  config: CollectorConfig,
  db: Database.Database,
  options: { now?: () => Date; fetchImpl?: typeof fetch; maxReportBytes?: number } = {},
): Promise<"not_joined" | "not_due" | "accepted" | "retry" | "conflict"> {
  if (!config.uploadUrl || !config.cloudDeviceId || !config.deviceId ||
    config.tenantId === LOCAL_TENANT_ID || config.installKey === "local-dev" ||
    config.policy.dataMode !== "metadata") return "not_joined";
  const now = (options.now ?? (() => new Date()))();
  const maxReportBytes = options.maxReportBytes === undefined || !Number.isFinite(options.maxReportBytes)
    ? MAX_POST_BYTES :
    Math.min(MAX_POST_BYTES, Math.max(512, Math.floor(options.maxReportBytes)));
  const currentWeek = utcWeekStart(now);
  db.prepare(`insert or ignore into weekly_tool_stats_control(workspace_id,device_id,first_week) values(?,?,?)`)
    .run(config.tenantId, config.deviceId, currentWeek);
  const control = db.prepare(`select first_week as firstWeek from weekly_tool_stats_control
    where workspace_id=? and device_id=?`).get(config.tenantId, config.deviceId) as { firstWeek: string };
  let pending = db.prepare(`select week_start as weekStart, report_sequence as reportSequence,
    digest, body_json as bodyJson, delivered from weekly_tool_stats_uploads
    where workspace_id=? and device_id=? and delivered=0 order by week_start limit 1`)
    .get(config.tenantId, config.deviceId) as Pending | undefined;
  if (!pending) {
    const last = db.prepare(`select max(week_start) as weekStart, max(report_sequence) as reportSequence
      from weekly_tool_stats_uploads where workspace_id=? and device_id=?`).get(config.tenantId, config.deviceId) as
      { weekStart: string | null; reportSequence: number | null };
    const weekStart = last.weekStart ? new Date(`${last.weekStart}T00:00:00.000Z`) :
      new Date(`${control.firstWeek}T00:00:00.000Z`);
    if (last.weekStart) weekStart.setUTCDate(weekStart.getUTCDate() + 7);
    if (weekStart.toISOString().slice(0, 10) >= currentWeek) return "not_due";
    const date = weekStart.toISOString().slice(0, 10);
    let toolStats = selectToolStatsSessions(aggregateToolStatsWeek(db,
      { workspaceId: config.tenantId, deviceId: config.deviceId, weekStart: date }));
    const reportSequence = (last.reportSequence ?? 0) + 1;
    const identity = { tenantId: config.tenantId, deviceId: config.cloudDeviceId,
      reportSequence, observedAt: now.toISOString() };
    let bodyJson = reportBody({ ...identity, toolStats });
    if (Buffer.byteLength(bodyJson) > maxReportBytes) {
      toolStats = countsOnlyToolStats(toolStats);
      bodyJson = reportBody({ ...identity, toolStats });
    }
    if (Buffer.byteLength(bodyJson) > maxReportBytes) {
      bodyJson = reportBody({ ...identity, toolStats, compressed: true });
    }
    if (Buffer.byteLength(bodyJson) > maxReportBytes) {
      toolStats = foldToolStatsVersions(toolStats);
      bodyJson = reportBody({ ...identity, toolStats });
    }
    if (Buffer.byteLength(bodyJson) > maxReportBytes) {
      bodyJson = reportBody({ ...identity, toolStats, compressed: true });
    }
    if (Buffer.byteLength(bodyJson) > maxReportBytes) throw new Error("tool_stats_report_too_large");
    const reportDigest = digest(toolStats);
    db.prepare(`insert into weekly_tool_stats_uploads
      (workspace_id,device_id,week_start,report_sequence,digest,body_json) values(?,?,?,?,?,?)`)
      .run(config.tenantId, config.deviceId, date, reportSequence, reportDigest, bodyJson);
    pending = { weekStart: date, reportSequence, digest: reportDigest, bodyJson, delivered: 0 };
  }
  const reportUrl = new URL("/api/work-intelligence/fleet/report", validatedTransportUrl(config.uploadUrl, "Upload URL"));
  const response = await authenticatedJsonPost({ url: reportUrl.href, body: pending.bodyJson,
    installKey: config.installKey, ingestKey: config.ingestKey, signingSecret: config.uploadSigningSecret,
    timeoutMs: config.delivery.requestTimeoutSeconds * 1000, fetchImpl: options.fetchImpl });
  const receipt = response.body as Receipt | null;
  if (!response.ok || !receipt || receipt.schema !== "fleet-device-report-receipt/v1" ||
    receipt.deviceId !== config.cloudDeviceId || receipt.reportSequence !== pending.reportSequence) return "retry";
  // The old cloud can echo the proposed digest on a replay without having
  // stored this week. Its next sequence also cannot distinguish a lost
  // response at N from a fresh ledger whose new week reused N. The additive
  // stored-row identity is the only safe replay acknowledgement or rebase
  // signal. Until the cloud supplies it, keep an ambiguous report pending.
  const hasStoredSignal = Object.prototype.hasOwnProperty.call(receipt, "storedToolStatsReport");
  const rawStored = receipt.storedToolStatsReport;
  const stored = rawStored && typeof rawStored === "object"
    ? rawStored as { weekStart?: unknown; reportSequence?: unknown; digest?: unknown } : null;
  const validStored = stored && stored.weekStart === pending.weekStart &&
    typeof stored.reportSequence === "number" && Number.isSafeInteger(stored.reportSequence) &&
    stored.reportSequence > 0 && typeof stored.digest === "string";
  if (hasStoredSignal && rawStored !== null && !validStored) return "retry";
  if (validStored && stored.digest !== pending.digest) return "conflict";
  const storedReplay = validStored && receipt.toolStatsDigest === stored.digest &&
    (receipt.disposition === "accepted" || receipt.disposition === "replay_ignored" ||
      receipt.disposition === "sequence_gap_ignored" || receipt.disposition === "future_skew_quarantined");
  const legacyAccepted = !hasStoredSignal && receipt.disposition === "accepted" &&
    receipt.toolStatsDigest === pending.digest;
  if (storedReplay || legacyAccepted) {
    db.prepare(`update weekly_tool_stats_uploads set delivered=1 where workspace_id=? and device_id=? and week_start=?
      and report_sequence=? and digest=?`).run(config.tenantId, config.deviceId, pending.weekStart,
        pending.reportSequence, pending.digest);
    return "accepted";
  }
  if (!hasStoredSignal || rawStored !== null || receipt.disposition === "accepted") return "retry";
  if (typeof receipt.toolStatsDigest === "string" && receipt.toolStatsDigest !== pending.digest) return "conflict";
  if ((receipt.disposition === "sequence_gap_ignored" || receipt.disposition === "replay_ignored" ||
    receipt.disposition === "future_skew_quarantined") &&
    typeof receipt.nextReportSequence === "number" &&
    Number.isSafeInteger(receipt.nextReportSequence) && receipt.nextReportSequence > 0 &&
    receipt.nextReportSequence !== pending.reportSequence) {
    const body = JSON.parse(pending.bodyJson) as Record<string, unknown>;
    body.reportSequence = receipt.nextReportSequence;
    body.observedAt = now.toISOString();
    db.prepare(`update weekly_tool_stats_uploads set report_sequence=?, body_json=?
      where workspace_id=? and device_id=? and week_start=? and delivered=0`)
      .run(receipt.nextReportSequence, JSON.stringify(body), config.tenantId, config.deviceId, pending.weekStart);
  }
  return "retry";
}
