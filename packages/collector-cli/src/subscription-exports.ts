import crypto from "node:crypto";
import type Database from "better-sqlite3";

export type CursorSubscriptionUsage = {
  observedAt: string | null;
  threadId: string | null;
  kind: string;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheWriteTokens: number | null;
  totalTokens: number | null;
  onDemandUsageUsd: number | null;
};

/** RFC 4180 cells; no source values are interpolated into SQL or commands. */
export function readCsvRows(csv: string): string[][] {
  if (Buffer.byteLength(csv) > 32 * 1024 * 1024) throw new Error("subscription_export_too_large");
  const rows: string[][] = [];
  let row: string[] = [], cell = "", quoted = false;
  for (let index = 0; index < csv.length; index += 1) {
    const c = csv[index];
    if (quoted) {
      if (c === '"' && csv[index + 1] === '"') { cell += '"'; index += 1; }
      else if (c === '"') quoted = false;
      else cell += c;
    } else if (c === '"') quoted = true;
    else if (c === ",") { row.push(cell); cell = ""; }
    else if (c === "\r" || c === "\n") {
      if (c === "\r" && csv[index + 1] === "\n") index += 1;
      row.push(cell);
      if (row.some((value) => value.trim())) rows.push(row);
      row = []; cell = "";
    } else cell += c;
  }
  if (quoted) throw new Error("subscription_export_unclosed_quote");
  row.push(cell);
  if (row.some((value) => value.trim())) rows.push(row);
  if (rows.length > 100_001) throw new Error("subscription_export_row_limit");
  return rows;
}

function counter(value: string | undefined): number | null {
  if (!value || !/^\d+$/.test(value.trim())) return null;
  const parsed = Number(value.trim());
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function text(value: string | undefined): string | null {
  return value && value !== "null" && value.length <= 256 && !/[\u0000-\u001f]/.test(value) ? value : null;
}

export function parseCursorSubscriptionCsv(csv: string): CursorSubscriptionUsage[] {
  const [header, ...rows] = readCsvRows(csv.replace(/^\uFEFF/, ""));
  const required = ["Date", "Kind", "Model", "Input (w/ Cache Write)", "Input (w/o Cache Write)", "Cache Read", "Output Tokens", "Total Tokens", "Cost"];
  if (!header || required.some((name) => !header.includes(name))) throw new Error("cursor_subscription_export_columns_unknown");
  return rows.map((cells) => {
    if (cells.length !== header.length) throw new Error("cursor_subscription_export_row_invalid");
    const row = Object.fromEntries(header.map((name, index) => [name, cells[index].trim()]));
    const date = row.Date;
    const observedAt = /^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:\d\d)$/.test(date) && Number.isFinite(Date.parse(date)) ? new Date(date).toISOString() : null;
    const kind = text(row.Kind) ?? "UNKNOWN";
    const cost = /^\d+(?:\.\d+)?$/.test(row.Cost) ? Number(row.Cost) : null;
    return {
      observedAt, threadId: text(row["Cloud Agent ID"]), kind, model: text(row.Model),
      inputTokens: counter(row["Input (w/o Cache Write)"]),
      cacheWriteTokens: counter(row["Input (w/ Cache Write)"]),
      cacheReadTokens: counter(row["Cache Read"]), outputTokens: counter(row["Output Tokens"]),
      totalTokens: counter(row["Total Tokens"]),
      onDemandUsageUsd: kind.toLowerCase() === "on-demand" && cost !== null && Number.isFinite(cost) ? cost : null,
    };
  });
}

export function ensureSubscriptionExportSchema(db: Database.Database): void {
  db.exec(`create table if not exists subscription_dashboard_exports (
    export_id text primary key, provider text not null, account_ref text not null,
    imported_at text not null, period_start text, period_end text, record_count integer not null,
    source_digest text not null, scope_state text not null
  );
  create table if not exists subscription_dashboard_events (
    export_id text not null, ordinal integer not null, observed_at text,
    thread_id text, kind text not null, model text, input_tokens integer,
    output_tokens integer, cache_read_tokens integer, cache_write_tokens integer,
    total_tokens integer, on_demand_usage_usd real,
    primary key(export_id,ordinal)
  );`);
}

/** Each export is a control snapshot. Overlapping snapshots are never added. */
export function importCursorSubscriptionExport(
  db: Database.Database,
  csv: string,
  options: { accountRef: string; periodStart?: string; periodEnd?: string; now?: string },
) {
  if (!/^[a-zA-Z0-9:_-]{1,128}$/.test(options.accountRef)) throw new Error("subscription_account_ref_invalid");
  const rows = parseCursorSubscriptionCsv(csv);
  const sourceDigest = `sha256:${crypto.createHash("sha256").update(csv).digest("hex")}`;
  const exportId = crypto.createHash("sha256").update(JSON.stringify(["cursor", options.accountRef, sourceDigest])).digest("hex");
  const now = options.now ?? new Date().toISOString();
  ensureSubscriptionExportSchema(db);
  const periodStart = options.periodStart ? new Date(options.periodStart).toISOString() : null;
  const periodEnd = options.periodEnd ? new Date(options.periodEnd).toISOString() : null;
  if (periodStart && periodEnd && periodStart >= periodEnd) throw new Error("subscription_export_period_invalid");
  if (rows.some((row) => row.observedAt !== null &&
    (periodStart !== null && row.observedAt < periodStart || periodEnd !== null && row.observedAt >= periodEnd))) {
    throw new Error("subscription_export_period_out_of_scope");
  }
  if (db.prepare("select 1 from subscription_dashboard_exports where export_id=?").get(exportId)) {
    return { exportId, provider: "cursor", records: rows.length, inserted: 0, replayed: rows.length };
  }
  const insert = db.prepare(`insert into subscription_dashboard_events values (?,?,?,?,?,?,?,?,?,?,?,?)`);
  db.transaction(() => {
    db.prepare("insert into subscription_dashboard_exports values (?,?,?,?,?,?,?,?,?)").run(exportId, "cursor", options.accountRef, now, periodStart, periodEnd, rows.length, sourceDigest, periodStart && periodEnd ? "declared" : "UNKNOWN");
    rows.forEach((row, ordinal) => insert.run(exportId, ordinal, row.observedAt, row.threadId, row.kind, row.model,
      row.inputTokens, row.outputTokens, row.cacheReadTokens, row.cacheWriteTokens, row.totalTokens, row.onDemandUsageUsd));
  })();
  return { exportId, provider: "cursor", records: rows.length, inserted: rows.length, replayed: 0 };
}

export function latestSubscriptionExportSummaries(db: Database.Database) {
  ensureSubscriptionExportSchema(db);
  const exports = db.prepare(`select e.* from subscription_dashboard_exports e
    where e.export_id=(select n.export_id from subscription_dashboard_exports n where n.provider=e.provider and n.account_ref=e.account_ref order by n.imported_at desc,n.rowid desc limit 1)`).all() as Array<Record<string, unknown>>;
  return exports.map((row) => {
    const totals = db.prepare(`select count(*) as records, count(total_tokens) as rowsWithTotalTokens,
      sum(total_tokens) as observedTotalTokens, sum(input_tokens) as observedInputTokens,
      sum(output_tokens) as observedOutputTokens, sum(cache_read_tokens) as observedCacheReadTokens,
      sum(cache_write_tokens) as observedCacheWriteTokens, sum(on_demand_usage_usd) as reportedOnDemandUsageUsd,
      sum(case when thread_id is not null then 1 else 0 end) as rowsWithThreadId,
      min(observed_at) as firstObservedAt,max(observed_at) as lastObservedAt
      from subscription_dashboard_events where export_id=?`).get(row.export_id) as {
        records:number;rowsWithTotalTokens:number;observedTotalTokens:number|null;
        observedInputTokens:number|null;observedOutputTokens:number|null;observedCacheReadTokens:number|null;
        observedCacheWriteTokens:number|null;reportedOnDemandUsageUsd:number|null;rowsWithThreadId:number;
        firstObservedAt:string|null;lastObservedAt:string|null;
      };
    return { provider: row.provider, accountRef: row.account_ref, exportId: row.export_id,
      importedAt: row.imported_at, periodStart: row.period_start, periodEnd: row.period_end, scopeState: row.scope_state,
      ...totals, completeness: "UNKNOWN", settledBankChargeUsd: null,
      note: "Latest export only; overlapping exports are not summed. Included usage is not a cash charge; unreported counters and thread joins remain UNKNOWN." };
  });
}

/** Cloud-agent IDs are provider-export chat IDs. They do not identify local
 * Cursor conversations. Keep this control snapshot outside native-event sums. */
export function latestSubscriptionExportThreadLinks(db: Database.Database) {
  ensureSubscriptionExportSchema(db);
  return db.prepare(`select e.provider,e.account_ref as accountRef,e.export_id as exportId,
    e.source_digest as sourceDigest,e.imported_at as importedAt,d.thread_id as threadId,
    count(*) as observedRows,count(d.total_tokens) as rowsWithTotalTokens,
    sum(d.input_tokens) as observedInputTokens,sum(d.output_tokens) as observedOutputTokens,
    sum(d.cache_read_tokens) as observedCacheReadTokens,sum(d.cache_write_tokens) as observedCacheWriteTokens,
    sum(d.total_tokens) as observedTotalTokens,max(d.observed_at) as lastObservedAt
    from subscription_dashboard_exports e join subscription_dashboard_events d on d.export_id=e.export_id
    where d.thread_id is not null and e.export_id=(select n.export_id from subscription_dashboard_exports n
      where n.provider=e.provider and n.account_ref=e.account_ref order by n.imported_at desc,n.rowid desc limit 1)
    group by e.export_id,d.thread_id order by lastObservedAt desc limit 1000`).all() as Array<{
      provider:string;accountRef:string;exportId:string;sourceDigest:string;importedAt:string;threadId:string;
      observedRows:number;rowsWithTotalTokens:number;observedInputTokens:number|null;observedOutputTokens:number|null;
      observedCacheReadTokens:number|null;observedCacheWriteTokens:number|null;observedTotalTokens:number|null;lastObservedAt:string|null;
    }>;
}
