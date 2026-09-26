/**
 * Real-shaped Codex OTLP pairing proof, adapted from the independent round-one review fixture.
 *
 * Shapes follow Codex rust-v0.156.0 source (see the round-one review's
 * codex-trace-key.md):
 *  - log:  codex.sse_event / response.completed, input_token_count, output_token_count,
 *          cached_token_count, conversation.id, user.account_id,
 *          NO trace context (emitted from an uninstrumented tokio::spawn).
 *  - span: handle_responses, gen_ai.usage.*, trace/span ids, NO conversation id.
 * A "traced" variant gives the log the span's traceId to exercise the trace path.
 *
 * Uploads are simulated with the outbox's own lease/acknowledge on an injected clock; "cloud" is
 * the set of acknowledged envelopes keyed by delivery id (the cloud dedupes by source event id).
 * A failed expectation exits nonzero, so CI detects a semantic regression.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { runCodexReconciliationMaintenance } from "../packages/collector-cli/src/codex-reconciliation";
import {
  buildCodexUsagePairingIndexes,
  codexUsagePairingProgress,
  codexUsagePairingStatus,
} from "../packages/collector-cli/src/codex-usage-pairing";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "rv-pairing-"));
const T0 = Date.parse("2026-09-25T18:00:00.000Z");
const UNTIL = new Date(Date.now() + 60_000).toISOString();
const WORKSPACE = "b2b2b2b2-2222-4222-8222-222222222222";
const SESSION_A = "019e9100-0000-7000-8000-00000000000a";
const SESSION_B = "019e9100-0000-7000-8000-00000000000b";

function attr(key: string, value: string | number) {
  return { key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } };
}
const nano = (ms: number) => String(BigInt(Math.round(ms)) * 1_000_000n);
const hex = (n: number, width: number) => n.toString(16).padStart(width, "0");
const RESOURCE = { attributes: [attr("service.name", "codex-app-server"), attr("service.version", "0.156.0")] };

type Resp = { input: number; output: number; cache: number; at: number; session?: string; trace?: number;
  account?: boolean };

function logEvent(r: Resp, traced = false) {
  const attributes = [
    attr("event.name", "codex.sse_event"), attr("event.kind", "response.completed"),
    attr("input_token_count", String(r.input)), attr("output_token_count", String(r.output)),
    attr("cached_token_count", r.cache), attr("tool_token_count", String(r.input + r.output)),
    attr("event.timestamp", new Date(r.at).toISOString()),
    attr("conversation.id", r.session ?? SESSION_A), attr("app.version", "0.156.0"),
    attr("originator", "codex_vscode"), attr("model", "gpt-5.1-codex-max"), attr("slug", "gpt-5.1-codex-max"),
    ...(r.account === false ? [] : [attr("user.account_id", "synthetic-account")]),
  ];
  const record = { timeUnixNano: nano(r.at), ...(traced ? { traceId: hex(r.trace ?? 1, 32) } : {}), attributes };
  return explode({ resourceLogs: [{ resource: RESOURCE, scopeLogs: [{ logRecords: [record] }] }] });
}

let spanSeq = 1;
function spanEvent(r: Resp) {
  const record = {
    traceId: hex(r.trace ?? 1, 32), spanId: hex(spanSeq++, 16), parentSpanId: hex(9_999, 16),
    name: "handle_responses", kind: 1,
    // The usage span starts when the turn loop waits for response.completed and
    // ends just after it records usage (codex-rs/core/src/session/turn.rs:2533-2572).
    startTimeUnixNano: nano(r.at - 1_500), endTimeUnixNano: nano(r.at + 20),
    attributes: [
      attr("gen_ai.usage.input_tokens", r.input), attr("gen_ai.usage.cache_read.input_tokens", r.cache),
      attr("gen_ai.usage.output_tokens", r.output), attr("codex.usage.total_tokens", r.input + r.output),
    ],
  };
  return explode({ resourceSpans: [{ resource: RESOURCE, scopeSpans: [{ spans: [record] }] }] });
}

/** A session-bearing, token-free Codex log (e.g. codex.sse_event for response.output_item.done). */
function contextEvent(session: string, at: number) {
  const record = { timeUnixNano: nano(at), attributes: [
    attr("event.name", "codex.sse_event"), attr("event.kind", "response.output_item.done"),
    attr("duration_ms", "3"), attr("conversation.id", session), attr("model", "gpt-5.1-codex-max"),
    attr("user.account_id", "synthetic-account"),
  ] };
  return explode({ resourceLogs: [{ resource: RESOURCE, scopeLogs: [{ logRecords: [record] }] }] });
}

function explode(envelope: unknown) {
  const out = explodeOtlpPayload(envelope, { source: "codex" });
  if (out.events.length !== 1 || out.parseFailures !== 0) throw new Error("fixture_not_admitted");
  return out.events[0]!;
}

class Harness {
  vnow = T0;
  buffer: LocalEventBuffer;
  cloud = new Map<string, { eventId: string; shape: string; account: boolean; input: number; output: number;
    cache: number }>();
  uploads: Array<{ at: string; ids: string[] }> = [];
  file: string;
  constructor(readonly name: string) {
    this.file = path.join(root, `${name}.sqlite`);
    this.buffer = this.open();
  }
  open() {
    return new LocalEventBuffer(this.file, {
      workspaceId: WORKSPACE, enrollmentNow: () => new Date(T0 - 3_600_000),
      delivery: { enabled: true, now: () => new Date(this.vnow) },
    });
  }
  restart() { this.buffer.close(); this.buffer = this.open(); }
  at(offsetMs: number) { this.vnow = T0 + offsetMs; return this; }
  append(entry: ReturnType<typeof explode>) {
    const ok = this.buffer.append(entry.event, entry.suppressedFields);
    if (!ok) throw new Error(`append refused ${entry.event.id}`);
    return entry.event.id;
  }
  maintain(passes = 3) {
    let stitched = 0;
    for (let i = 0; i < passes; i += 1) stitched += runCodexReconciliationMaintenance(this.buffer.database).stitched;
    return stitched;
  }
  /** One upload cycle: lease what is eligible at vnow, then acknowledge (or hold) it. */
  upload(options: { ack?: boolean } = {}) {
    const lease = this.buffer.delivery.lease({ now: new Date(this.vnow) });
    const ids = lease.items.map((item) => item.deliveryId);
    this.uploads.push({ at: `+${(this.vnow - T0) / 1000}s`, ids: ids.map((id) => this.shapeOf(id)) });
    for (const item of lease.items) {
      const event = item.envelope.event as unknown as Record<string, any>;
      this.cloud.set(item.deliveryId, { eventId: item.deliveryId, shape: this.shapeOf(item.deliveryId),
        account: Boolean(event.actorId), input: event.inputTokens ?? 0, output: event.outputTokens ?? 0,
        cache: event.cacheReadTokens ?? 0 });
    }
    if (options.ack !== false && ids.length) this.buffer.delivery.acknowledge(lease.leaseId, ids, new Date(this.vnow));
    return lease;
  }
  shapeOf(id: string) {
    const row = this.buffer.database.prepare(`select payload_json as p from buffered_events where id = ?`).get(id) as
      { p: string } | undefined;
    if (!row) return `missing:${id.slice(0, 8)}`;
    const name = (JSON.parse(row.p) as { metadata?: { otelEventName?: string } }).metadata?.otelEventName;
    const usage = (JSON.parse(row.p) as { inputTokens?: number }).inputTokens !== undefined;
    return name === "codex.sse_event" ? (usage ? "log" : "ctx") : name === "handle_responses" ? "span" : String(name);
  }
  observe() {
    const db = this.buffer.database;
    const eligible = terminalPrivacyEligibilitySql(db, "e");
    const local = db.prepare(`select count(*) as usageRows,
        coalesce(sum(e.input_tokens),0) as input, coalesce(sum(e.output_tokens),0) as output,
        coalesce(sum(e.cache_read_tokens),0) as cache,
        sum(case when e.usage_duplicate_reason is not null then 1 else 0 end) as markedDuplicates
      from buffered_events e where e.source = 'codex' and e.input_tokens is not null and ${eligible}`).get() as
      { usageRows: number; input: number; output: number; cache: number };
    const marked = db.prepare(`select count(*) as n from buffered_events where usage_duplicate_reason is not null`)
      .get() as { n: number };
    for (let i = 0; i < 40; i += 1) {
      this.buffer.projection.runMaintenance(new Date());
      const pending = db.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as { n: number };
      if (pending.n === 0) break;
    }
    const dash = db.prepare(`select events, token_events as tokenEvents, input_tokens as input,
        output_tokens as output from dashboard_lifetime_totals where singleton = 1`).get() as
      { events: number; tokenEvents: number; input: number; output: number };
    const factCache = db.prepare(`select coalesce(sum(cache_read_tokens),0) as cache, count(*) as usageFacts
        from dashboard_event_facts where input_tokens is not null`).get() as Record<string, number>;
    const sessions: Record<string, unknown> = {};
    for (const snapshot of collectSessionSnapshots(db, { until: UNTIL })) {
      const s = snapshot as unknown as Record<string, number | string>;
      sessions[String(s.sessionId).slice(-1)] = { events: s.events, input: s.inputTokens, output: s.outputTokens,
        cache: s.cacheReadTokens };
    }
    // Only usage envelopes count as cloud usage rows; token-free context logs upload on their own.
    const cloud = [...this.cloud.values()].filter((c) => c.input > 0 || c.output > 0);
    return {
      local: { ...local, markedDuplicates: marked.n },
      dashboard: { ...dash, usageFacts: factCache.usageFacts, cache: factCache.cache },
      sessionSnapshots: sessions,
      uploads: this.uploads,
      cloud: { rows: cloud.length, shapes: cloud.map((c) => `${c.shape}${c.account ? "+acct" : ""}`),
        input: cloud.reduce((a, c) => a + c.input, 0), output: cloud.reduce((a, c) => a + c.output, 0),
        cache: cloud.reduce((a, c) => a + c.cache, 0) },
    };
  }
  async incrementalSummary(session: string) {
    const db = this.buffer.database;
    ensureSessionSummarySchema(db);
    const result = await updateSessionSummary(db, session, UNTIL, { maxRows: 1_000, maxMs: 5_000,
      read: async <T>(queries: Array<{ sql: string; params: Record<string, unknown> }>) =>
        queries.flatMap((query) => db.prepare(query.sql).all(query.params)) as T[] });
    const s = result.snapshot as unknown as Record<string, number> | undefined;
    return s ? { events: s.events, input: s.inputTokens, output: s.outputTokens, cache: s.cacheReadTokens } : null;
  }
  close() { this.buffer.close(); }
}

const R = (over: Partial<Resp> = {}): Resp => ({ input: 24_261, output: 148, cache: 11_776, at: T0, ...over });
const checks: string[] = [];
function check(name: string, condition: unknown, detail: unknown = {}) {
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
  checks.push(name);
}
const once = (o: ReturnType<Harness["observe"]>, count = 1) =>
  o.local.usageRows === count && o.local.input === 24_261 * count &&
  o.dashboard.tokenEvents === count && o.dashboard.input === 24_261 * count;

async function main() {
  {
    const h = new Harness("indexes-explicit-upgrade");
    try {
      h.buffer.close();
      const db = new Database(h.file);
      db.exec(`drop index idx_codex_usage_span_backfill;
        drop index idx_codex_usage_span_match;
        drop index idx_codex_usage_log_match`);
      db.close();
      h.buffer = h.open();
      const missing = codexUsagePairingStatus(h.buffer.database);
      check("ordinary_open_does_not_build_pairing_indexes",
        !missing.enabled && missing.missingIndexes.length === 3, missing);
      h.append(logEvent(R())); h.append(spanEvent(R()));
      check("missing_indexes_leave_both_shapes_eligible",
        h.observe().local.usageRows === 2 && h.observe().local.markedDuplicates === 0);
      h.buffer.close();
      const upgrade = new Database(h.file);
      const timings = buildCodexUsagePairingIndexes(upgrade);
      check("explicit_upgrade_builds_all_pairing_indexes",
        timings.length === 3 && codexUsagePairingStatus(upgrade).enabled, timings);
      upgrade.close();
      h.buffer = h.open();
      const later = R({ input: 28_000, output: 200, cache: 12_000, at: T0 + 120_000 });
      h.append(logEvent(later)); h.append(spanEvent(later));
      check("pairing_resumes_after_explicit_upgrade",
        h.observe().local.usageRows === 3 && h.observe().local.markedDuplicates === 1);
    } finally { h.close(); }
  }

  for (const order of ["log-first", "span-first"] as const) {
    const h = new Harness(order);
    try {
      const r = R();
      const log = logEvent(r);
      const span = spanEvent(r);
      h.at(200); h.append(order === "log-first" ? log : span);
      h.at(1_200); h.append(order === "log-first" ? span : log);
      const beforeMaintenance = h.observe();
      check(`${order}_pairs_at_ingest_without_trace`, once(beforeMaintenance) &&
        beforeMaintenance.local.markedDuplicates === 1 && beforeMaintenance.local.cache === r.cache,
        beforeMaintenance);
      const stitched = h.maintain();
      check(`${order}_does_not_need_session_stitch`, stitched === 0, { stitched });
      h.at(30_000); check(`${order}_holds_first_upload`, h.upload().items.length === 0);
      h.at(61_500); h.upload();
      const afterUpload = h.observe();
      check(`${order}_uploads_one_account_log_with_cache`, afterUpload.cloud.rows === 1 &&
        afterUpload.cloud.shapes[0] === "log+acct" && afterUpload.cloud.cache === r.cache,
        afterUpload.cloud);
      const summary = await h.incrementalSummary(SESSION_A);
      check(`${order}_session_summary_counts_once`, summary?.input === r.input && summary?.events === 1,
        { summary, sessions: h.buffer.database.prepare(`select id, session_id as sessionId,
          event_type as eventType from buffered_events`).all() });
    } finally { h.close(); }
  }

  for (const kind of ["log", "span"] as const) {
    const h = new Harness(`lone-${kind}`);
    try {
      h.at(200); h.append(kind === "log" ? logEvent(R()) : spanEvent(R()));
      const local = h.observe();
      check(`lone_${kind}_counts_once`, once(local) && (kind !== "log" || local.local.cache === R().cache),
        local);
      h.at(30_000); check(`lone_${kind}_held`, h.upload().items.length === 0);
      h.at(61_500); h.upload();
      check(`lone_${kind}_uploads_once`, h.observe().cloud.rows === 1);
    } finally { h.close(); }
  }

  {
    const h = new Harness("api-key-login");
    try {
      h.at(200); h.append(logEvent(R({ account: false })));
      h.at(1_200); h.append(spanEvent(R({ account: false })));
      const o = h.observe();
      check("accountless_log_pairs_by_shape", once(o) && o.local.markedDuplicates === 1, o);
    } finally { h.close(); }
  }

  {
    const h = new Harness("equal-count-cluster");
    try {
      const a = R(); const b = R({ at: T0 + 12_000 });
      const logA = logEvent(a); const logB = logEvent(b);
      const spanA = spanEvent(a); const spanB = spanEvent(b);
      h.at(200); h.append(logA);
      h.at(12_200); h.append(logB);
      h.at(12_400); h.append(spanA); h.append(spanB);
      const o = h.observe();
      check("equal_count_cluster_pairs_in_time_order", once(o, 2) &&
        o.local.markedDuplicates === 2 &&
        (h.buffer.database.prepare(`select usage_paired_event_id as paired
          from buffered_events where id = ?`).get(logA.event.id) as { paired: string }).paired === spanA.event.id &&
        (h.buffer.database.prepare(`select usage_paired_event_id as paired
          from buffered_events where id = ?`).get(logB.event.id) as { paired: string }).paired === spanB.event.id,
        o);
      h.at(80_000); h.upload();
      check("equal_count_cluster_uploads_two_logs", h.observe().cloud.rows === 2);
    } finally { h.close(); }
  }

  {
    const h = new Harness("uneven-count-cluster");
    try {
      h.at(200); h.append(logEvent(R()));
      h.at(12_200); h.append(logEvent(R({ at: T0 + 12_000 })));
      h.at(12_400); h.append(spanEvent(R()));
      const o = h.observe();
      check("uneven_count_cluster_refuses_merge", o.local.markedDuplicates === 0 &&
        o.local.usageRows === 3, o);
    } finally { h.close(); }
  }

  {
    const h = new Harness("concurrent-sessions");
    try {
      h.at(100); h.append(contextEvent(SESSION_B, T0 - 1_490));
      h.at(200); h.append(logEvent(R()));
      h.at(1_200); h.append(spanEvent(R()));
      const before = h.observe();
      const stitched = h.maintain();
      const after = h.observe();
      check("concurrent_sessions_pair_before_wrong_stitch", once(before) &&
        stitched === 0 && once(after), { before, stitched, after });
    } finally { h.close(); }
  }

  {
    const h = new Harness("trace-beats-guessed-session");
    try {
      h.at(100); h.append(contextEvent(SESSION_B, T0 - 1_490));
      h.at(200); h.append(spanEvent(R()));
      h.at(20_000); check("span_stitches_to_other_session_before_log", h.maintain() > 0);
      h.at(21_000); h.append(logEvent(R(), true));
      const o = h.observe();
      check("trace_match_beats_guessed_session", once(o) &&
        o.local.markedDuplicates === 1 &&
        (o.sessionSnapshots.a as { input: number } | undefined)?.input === R().input,
        o);
    } finally { h.close(); }
  }

  {
    const h = new Harness("eligibility");
    try {
      h.at(200); h.append(logEvent(R())); h.append(spanEvent(R()));
      h.buffer.database.prepare(`update buffered_events set input_tokens = 24261,
        output_tokens = 148 where usage_duplicate_reason = 'codex_sse_event_span'`).run();
      const o = h.observe();
      check("duplicate_reason_excludes_restored_tokens", once(o), o);
      const summary = await h.incrementalSummary(SESSION_A);
      check("duplicate_reason_excludes_session_summary", summary?.input === R().input, summary);
    } finally { h.close(); }
  }

  {
    const h = new Harness("upgrade-backfill");
    try {
      const log = logEvent(R());
      const span = spanEvent(R({ at: T0 + 45_000 }));
      h.at(200); h.append(log);
      h.at(45_200); h.append(span);
      check("upgrade_seed_has_two_legacy_usage_rows", h.observe().local.usageRows === 2);
      h.buffer.database.prepare(`update buffered_events set observed_at = ? where id = ?`)
        .run(new Date(T0 - 1_500).toISOString(), span.event.id);
      h.buffer.database.exec(`delete from codex_reconciliation_pending;
        delete from codex_reconciliation_candidates;
        delete from codex_reconciliation_windows;
        update codex_usage_pairing_control set cursor_rowid = 0,
          target_rowid = (select max(rowid) from buffered_events), complete = 0`);
      for (let i = 0; i < 12 && h.observe().local.markedDuplicates === 0; i += 1) h.maintain();
      const o = h.observe();
      check("upgrade_backfill_marks_historical_pair", once(o) &&
        o.local.markedDuplicates === 1, o);
    } finally { h.close(); }
  }

  {
    const h = new Harness("stitch-second-chance");
    try {
      const log = logEvent(R());
      const span = spanEvent(R({ at: T0 + 45_000 }));
      h.at(200); h.append(log);
      h.at(45_200); h.append(span);
      check("stitch_seed_is_unpaired", h.observe().local.usageRows === 2);
      h.buffer.database.prepare(`update buffered_events set observed_at = ? where id = ?`)
        .run(new Date(T0 - 1_500).toISOString(), span.event.id);
      const stitched = h.maintain();
      const o = h.observe();
      check("pairing_after_stitch_is_second_chance", stitched > 0 && once(o) &&
        o.local.markedDuplicates === 1, { stitched, o });
    } finally { h.close(); }
  }

  {
    const h = new Harness("selective-backfill");
    try {
      const db = h.buffer.database;
      const filler = db.prepare(`insert into buffered_events
        (id, source, event_type, data_mode, observed_at, payload_json, created_at)
        values (?, 'claude_code', 'tool_use', 'metadata', ?, '{}', ?)`);
      const accountlessLog = db.prepare(`insert into buffered_events
        (id, source, event_type, data_mode, observed_at, payload_json, created_at,
         input_tokens, output_tokens)
        values (?, 'codex', 'assistant_response', 'metadata', ?,
          '{"metadata":{"otelEventName":"codex.sse_event"}}', ?, 40000, 900)`);
      const old = "2026-01-01T00:00:00.000Z";
      db.transaction(() => {
        for (let i = 0; i < 500; i += 1) filler.run(`old-filler-${i}`, old, old);
        for (let i = 0; i < 500; i += 1) accountlessLog.run(`accountless-log-${i}`,
          new Date(Date.parse("2026-09-21T00:00:00.000Z") + i * 1_000).toISOString(), old);
      })();
      const r = R();
      const log = logEvent(r).event;
      const span = spanEvent(r).event;
      h.at(200); h.append({ event: log, suppressedFields: [] });
      db.prepare(`insert into buffered_events
        (id, source, event_type, data_mode, observed_at, payload_json, created_at,
         input_tokens, output_tokens, cache_read_tokens, workspace_id)
        values (?, 'codex', 'assistant_response', 'metadata', ?, ?, ?, ?, ?, ?, ?)`)
        .run(span.id, span.observedAt, JSON.stringify(span), old, span.inputTokens,
          span.outputTokens, span.cacheReadTokens, WORKSPACE);
      db.exec(`delete from codex_reconciliation_pending;
        delete from codex_reconciliation_candidates;
        delete from codex_reconciliation_windows;
        update codex_usage_pairing_control set cursor_rowid = 0,
          target_rowid = (select max(rowid) from buffered_events), complete = 0`);
      const before = codexUsagePairingProgress(db);
      check("pending_historical_span_enables_repair_cadence",
        before.pending === true && before.units === 0, before);
      h.maintain(1);
      const duplicate = db.prepare(`select usage_duplicate_reason as reason
        from buffered_events where id = ?`).get(span.id) as { reason: string | null };
      const progress = db.prepare(`select visited, paired from codex_usage_pairing_control`).get() as
        { visited: number; paired: number };
      const cadence = codexUsagePairingProgress(db);
      check("selective_backfill_reaches_recent_span_in_one_slice",
        duplicate.reason === "codex_sse_event_span" && progress.paired === 1 &&
        progress.visited <= 1, { duplicate, progress });
      check("pairing_progress_can_drive_fast_repair_cadence",
        cadence.units === progress.visited && cadence.pending === false, cadence);
    } finally { h.close(); }
  }

  {
    const h = new Harness("other-service-hold");
    try {
      const entry = logEvent(R());
      h.at(200); h.append({ ...entry, event: { ...entry.event, metadata: {
        ...entry.event.metadata, serviceName: "alternate-codex-frontend",
      } } });
      h.at(30_000);
      check("codex_usage_shape_hold_does_not_depend_on_service_name",
        h.upload().items.length === 0);
    } finally { h.close(); }
  }

  {
    const h = new Harness("late-span-first");
    try {
      h.at(200); h.append(spanEvent(R()));
      h.at(61_500); const lease = h.upload({ ack: false });
      check("late_pair_span_first_leases_before_counterpart", lease.items.length === 1);
      h.at(62_000); h.append(logEvent(R()));
      h.buffer.delivery.acknowledge(lease.leaseId,
        lease.items.map((item) => item.deliveryId), new Date(h.vnow));
      const receipt = h.buffer.database.prepare(
        `select terminal_state as state, reason from upload_receipts where delivery_id = ?`,
      ).get(lease.items[0]!.deliveryId) as { state: string; reason: string } | undefined;
      check("accepted_in_flight_span_has_remote_ack_receipt",
        receipt?.state === "acknowledged" && receipt.reason === "remote_acknowledged", receipt);
      h.at(123_000); h.upload();
      const o = h.observe();
      check("late_pair_local_once_but_cloud_has_two_without_retraction",
        once(o) && o.cloud.rows === 2, o);
    } finally { h.close(); }
  }

  {
    const h = new Harness("late-log-first");
    try {
      h.at(200); h.append(logEvent(R()));
      h.at(61_500); h.upload();
      h.at(90_000); h.append(spanEvent(R()));
      h.at(151_000); h.upload();
      const o = h.observe();
      check("late_span_after_log_upload_keeps_one_cloud_row_with_cache",
        once(o) && o.local.cache === R().cache && o.cloud.rows === 1 && o.cloud.cache === R().cache,
        o);
    } finally { h.close(); }
  }

  console.log(JSON.stringify({ status: "pass", proof: "codex-usage-pairing", checks: checks.length,
    names: checks }, null, 2));
}

main().catch((error) => { console.error(error); process.exitCode = 1; })
  .finally(() => fs.rmSync(root, { recursive: true, force: true }));
