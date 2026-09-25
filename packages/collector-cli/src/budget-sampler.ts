import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import { promisify } from "node:util";
import type Database from "better-sqlite3";

import { DAEMON_SESSION_SYNC_STATE_KEY, readLedgerOffThread } from "./session-sync";

const execFileAsync = promisify(execFile);
export const BUDGET_SAMPLE_VERSION = 2;
export const BUDGET_RING_LIMIT = 1_440;
export const BUDGET_DBSTAT_MAX_BYTES = 2 * 1024 ** 3;
const DEAD_AGE_MAX_ROWS = 1_000;
const DAY_MS = 86_400_000;

export type BudgetProcess = {
  pid: number;
  role: "daemon" | "maintenance" | "enrichment" | "other_child";
  rssBytes: number;
  cpuTotalMs: number;
  cpuDeltaMs: number | null;
  cpuCores: number | null;
};

export type BudgetSample = {
  version: typeof BUDGET_SAMPLE_VERSION;
  atMs: number;
  dbBytes: number | null;
  walBytes: number | null;
  shmBytes: number | null;
  /** Change in WAL file length, never a measure of physical writes. */
  walFileGrowthBytes: number | null;
  /** Node ru_oublock: filesystem output operations for this process, all files. */
  fsWriteOperationsDelta: number | null;
  fsWriteOperationsPerMinute: number | null;
  fsWriteTotal: number;
  processes: BudgetProcess[];
  outboxPending: number | null;
  outboxDead: number | null;
  outboxOldestPendingAgeSeconds: number | null;
  outboxOldestDeadAgeSeconds: number | null;
  summaryLagSeconds: number | null;
  samplerElapsedMs: number;
  /** Unavailable: process CPU across an async interval includes unrelated work. */
  samplerCpuMicros: null;
  samplerMainThreadMs: number;
  attemptedRowsDelta: number;
  unavailable: string[];
};

type DailyRow = { day: string; version: number; attemptedRows: number; dbstatStatus: string; tablePagesJson: string | null };

/** Additive, constant-size schema. Admission totals are flushed once per sample. */
export function ensureBudgetSchema(db: Database.Database): void {
  db.exec(`
    create table if not exists budget_samples (
      id integer primary key, at_ms integer not null, version integer not null,
      sample_json text not null
    );
    create index if not exists idx_budget_samples_at_ms on budget_samples(at_ms);
    create table if not exists budget_daily (
      day text primary key, version integer not null default 1, attempted_rows integer not null default 0,
      dbstat_status text not null default 'pending', table_pages_json text
    ) without rowid;
    create table if not exists budget_control (
      singleton integer primary key check(singleton=1), started_day text not null
    );
    insert or ignore into budget_control(singleton,started_day) values(1,strftime('%Y-%m-%d','now'));
  `);
}

function timeMs(text: string): number | null {
  const [dayPart, clock] = text.includes("-") ? text.split("-", 2) : ["0", text];
  const parts = clock?.split(":").map(Number) ?? [];
  if (!parts.length || parts.some((part) => !Number.isFinite(part))) return null;
  let seconds = 0;
  for (const part of parts) seconds = seconds * 60 + part;
  return Math.round((Number(dayPart) * 86_400 + seconds) * 1_000);
}

export type BudgetChildPid = { pid: number; role: "maintenance" | "enrichment" };

/** The daemon uses Node counters. ps runs only when a tracked child exists. */
export async function budgetProcessTree(
  atMs: number,
  previous?: BudgetSample,
  childPids: BudgetChildPid[] = [],
): Promise<{ processes: BudgetProcess[]; unavailable: string[] }> {
  const cpu = process.cpuUsage();
  const cpuTotalMs = (cpu.user + cpu.system) / 1_000;
  const prior = previous?.processes.find((entry) => entry.pid === process.pid && entry.role === "daemon");
  const elapsedMs = previous ? atMs - previous.atMs : 0;
  const daemonDelta = prior && cpuTotalMs >= prior.cpuTotalMs ? cpuTotalMs - prior.cpuTotalMs : null;
  const processes: BudgetProcess[] = [{ pid: process.pid, role: "daemon",
    rssBytes: process.memoryUsage.rss(), cpuTotalMs, cpuDeltaMs: daemonDelta,
    cpuCores: daemonDelta !== null && elapsedMs > 0 ? daemonDelta / elapsedMs : null }];
  const unavailable: string[] = [];
  if (!childPids.length) return { processes, unavailable };
  if (process.platform !== "darwin" && process.platform !== "linux") {
    return { processes, unavailable: ["child_process_accounting_platform_unsupported"] };
  }
  const ps = process.platform === "darwin" ? "/bin/ps" : "/usr/bin/ps";
  let output: string;
  try {
    output = (await execFileAsync(ps, ["-o", "pid=", "-o", "ppid=", "-o", "rss=", "-o", "time=", "-p",
      childPids.map((child) => child.pid).join(",")], {
      timeout: 1_000, maxBuffer: 16_384,
    })).stdout;
  } catch {
    return { processes, unavailable: [...unavailable, "child_process_accounting_failed"] };
  }
  for (const line of output.split("\n")) {
    const match = line.match(/^\s*(\d+)\s+(\d+)\s+(\d+)\s+([0-9:.\-]+)\s*$/);
    if (!match) continue;
    const currentPid = Number(match[1]);
    const tracked = childPids.find((entry) => entry.pid === currentPid);
    if (!tracked || Number(match[2]) !== process.pid) continue;
    const childCpuTotalMs = timeMs(match[4]!);
    if (childCpuTotalMs === null) continue;
    const childPrior = previous?.processes.find((entry) => entry.pid === currentPid && entry.role === tracked.role);
    const cpuDeltaMs = childPrior && childCpuTotalMs >= childPrior.cpuTotalMs
      ? childCpuTotalMs - childPrior.cpuTotalMs : null;
    processes.push({ pid: currentPid, role: tracked.role, rssBytes: Number(match[3]) * 1024,
      cpuTotalMs: childCpuTotalMs, cpuDeltaMs,
      cpuCores: cpuDeltaMs !== null && elapsedMs > 0 ? cpuDeltaMs / elapsedMs : null });
  }
  if (processes.length - 1 < childPids.length) unavailable.push("tracked_child_not_observed");
  return { processes, unavailable };
}

async function fileBytes(file: string): Promise<number | null> {
  try { return (await fs.stat(file)).size; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT" ? 0 : null; }
}

function ageSeconds(value: string | null | undefined, nowMs: number): number | null {
  if (!value) return null;
  const at = Date.parse(value);
  return Number.isFinite(at) ? Math.max(0, Math.floor((nowMs - at) / 1_000)) : null;
}

/** These reads are one control row, one indexed latest event, and at most 1,000 dead receipts. */
export function budgetLedgerSnapshot(db: Database.Database, nowMs: number) {
  const unavailable: string[] = [];
  const control = db.prepare(`select active_pending as pending, active_retry as retry,
    active_in_flight as inFlight, active_oldest_created_at as oldestPending,
    receipt_dead as dead from upload_control where singleton=1`).get() as {
      pending: number; retry: number; inFlight: number; oldestPending: string | null; dead: number;
    } | undefined;
  let oldestDead: string | null = null;
  if (control && control.dead <= DEAD_AGE_MAX_ROWS && control.dead > 0) {
    oldestDead = (db.prepare(`select min(terminal_at) as at from upload_receipts
      indexed by idx_upload_receipts_state where terminal_state='dead'`).get() as { at: string | null }).at;
  } else if (control && control.dead > DEAD_AGE_MAX_ROWS) unavailable.push("dead_age_scan_cap");
  const latest = db.prepare(`select created_at as at from buffered_events
    order by created_at desc limit 1`).get() as { at: string } | undefined;
  const state = db.prepare(`select value from maintenance_state where key=?`).get(
    DAEMON_SESSION_SYNC_STATE_KEY,
  ) as { value: string } | undefined;
  let summaryThrough: string | null = null;
  try { summaryThrough = state ? (JSON.parse(state.value) as { lastSuccessfulUntil?: string | null }).lastSuccessfulUntil ?? null : null; }
  catch { unavailable.push("summary_watermark_invalid"); }
  if (latest && !summaryThrough) unavailable.push("summary_watermark_missing");
  const lag = latest && summaryThrough ? Math.max(0, Math.floor((Date.parse(latest.at) - Date.parse(summaryThrough)) / 1_000)) : null;
  return {
    outboxPending: control ? control.pending + control.retry + control.inFlight : null,
    outboxDead: control?.dead ?? null,
    outboxOldestPendingAgeSeconds: ageSeconds(control?.oldestPending, nowMs),
    outboxOldestDeadAgeSeconds: ageSeconds(oldestDead, nowMs),
    summaryLagSeconds: Number.isFinite(lag) ? lag : null,
    unavailable,
  };
}

export async function collectBudgetSample(db: Database.Database, ledgerPath: string, options: {
  nowMs?: number; previous?: BudgetSample; childPids?: BudgetChildPid[];
} = {}): Promise<BudgetSample> {
  const started = performance.now();
  const nowMs = options.nowMs ?? Date.now();
  const fsWrites = process.resourceUsage().fsWrite;
  const [dbBytes, walBytes, shmBytes, tree] = await Promise.all([
    fileBytes(ledgerPath), fileBytes(`${ledgerPath}-wal`), fileBytes(`${ledgerPath}-shm`),
    budgetProcessTree(nowMs, options.previous, options.childPids),
  ]);
  const ledgerStarted = performance.now();
  const ledger = budgetLedgerSnapshot(db, nowMs);
  const ledgerMs = performance.now() - ledgerStarted;
  // A zero ru_oublock has proved uninformative after fsync on macOS; do not
  // show a zero write rate as if it were a verified observation.
  const fsWriteCounterValid = fsWrites > 0 || (options.previous?.fsWriteTotal ?? 0) > 0;
  const fsWriteOperationsDelta = fsWriteCounterValid && options.previous?.fsWriteTotal != null
    ? Math.max(0, fsWrites - options.previous.fsWriteTotal) : null;
  const elapsedMinutes = options.previous ? (nowMs - options.previous.atMs) / 60_000 : 0;
  return {
    version: BUDGET_SAMPLE_VERSION, atMs: nowMs, dbBytes, walBytes, shmBytes,
    walFileGrowthBytes: walBytes === null || options.previous?.walBytes == null ? null
      : walBytes >= options.previous.walBytes ? walBytes - options.previous.walBytes : walBytes,
    fsWriteTotal: fsWrites, fsWriteOperationsDelta,
    fsWriteOperationsPerMinute: fsWriteOperationsDelta !== null && elapsedMinutes > 0
      ? fsWriteOperationsDelta / elapsedMinutes : null,
    processes: tree.processes,
    ...ledger,
    samplerElapsedMs: Math.round((performance.now() - started) * 100) / 100,
    samplerCpuMicros: null,
    samplerMainThreadMs: Math.round(ledgerMs * 100) / 100,
    attemptedRowsDelta: 0,
    unavailable: [...tree.unavailable, ...ledger.unavailable,
      "physical_write_bytes_unavailable_fswrite_counts_process_output_operations",
      ...(fsWriteCounterValid ? [] : ["fswrite_counter_zero_unverified"]),
      "sampler_cpu_attribution_unavailable_concurrent_process_work",
      "thread_pool_accounted_in_process_not_separable",
      "other_process_admissions_and_counted_gaps_unavailable"],
  };
}

/** One row per minute. Deletes use the integer primary key and never scan the ledger. */
export function recordBudgetSample(db: Database.Database, sample: BudgetSample): void {
  db.transaction(() => {
    const inserted = db.prepare(`insert into budget_samples(at_ms,version,sample_json) values(?,?,?)`)
      .run(sample.atMs, sample.version, JSON.stringify(sample));
    db.prepare(`insert into budget_daily(day,attempted_rows) values(?,?)
      on conflict(day) do update set attempted_rows=attempted_rows+excluded.attempted_rows
      where excluded.attempted_rows>0`).run(
      new Date(sample.atMs).toISOString().slice(0, 10), sample.attemptedRowsDelta,
    );
    db.prepare(`delete from budget_samples where id <= ? or at_ms < ?`)
      .run(Number(inserted.lastInsertRowid) - BUDGET_RING_LIMIT, sample.atMs - DAY_MS);
    db.prepare(`delete from budget_daily where day < ?`).run(
      new Date(sample.atMs - 7 * DAY_MS).toISOString().slice(0, 10),
    );
  })();
}

const METRICS = ["dbBytes", "walBytes", "shmBytes", "walFileGrowthBytes",
  "fsWriteOperationsDelta", "fsWriteOperationsPerMinute", "rssBytes", "cpuCores",
  "outboxPending", "outboxDead", "outboxOldestPendingAgeSeconds", "outboxOldestDeadAgeSeconds",
  "summaryLagSeconds", "samplerElapsedMs", "samplerCpuMicros", "samplerMainThreadMs"] as const;

function percentile(values: number[], fraction: number): number | null {
  if (!values.length) return null;
  values.sort((a, b) => a - b);
  return values[Math.ceil(values.length * fraction) - 1] ?? null;
}

function sortedPosition(values: number[], value: number): number {
  let low = 0, high = values.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if (values[middle]! < value) low = middle + 1;
    else high = middle;
  }
  return low;
}

function sampleMetric(sample: BudgetSample, metric: typeof METRICS[number]): number | null {
  if (metric === "rssBytes") return sample.processes.length
    ? sample.processes.reduce((sum, entry) => sum + entry.rssBytes, 0) : null;
  if (metric === "cpuCores") {
    const deltas = sample.processes.map((entry) => entry.cpuCores).filter((value): value is number => value !== null);
    return deltas.length ? deltas.reduce((sum, value) => sum + value, 0) : null;
  }
  return sample[metric] ?? null;
}

const TARGETS = {
  light: { ledgerBytes: 0.75 * 1e9, rssBytes: 350e6, cpuCores: 0.10, writeBytesPerMinute: 1 * 1024 ** 2, summaryLagSeconds: 300 },
  busy: { ledgerBytes: 4e9, rssBytes: 512e6, cpuCores: 0.25, writeBytesPerMinute: 4 * 1024 ** 2, summaryLagSeconds: 300 },
  studio0_scale: { ledgerBytes: 16e9, rssBytes: 1024e6, cpuCores: 1.0, writeBytesPerMinute: 16 * 1024 ** 2, summaryLagSeconds: 900 },
} as const;

function attemptedClass(db: Database.Database, nowMs: number) {
  const day = new Date(nowMs).toISOString().slice(0, 10);
  const firstDay = new Date(nowMs - 7 * DAY_MS).toISOString().slice(0, 10);
  const rows = db.prepare(`select day, attempted_rows as attemptedRows from budget_daily
    where day >= ? and day < ? order by day`).all(firstDay, day) as Array<{ day: string; attemptedRows: number }>;
  const attemptedRows = rows.reduce((sum, row) => sum + row.attemptedRows, 0);
  const days = rows.length;
  const rate = days ? attemptedRows / days : null;
  const started = db.prepare(`select started_day as day from budget_control where singleton=1`)
    .get() as { day: string } | undefined;
  const completeDays = started ? Math.floor((Date.parse(`${day}T00:00:00.000Z`) -
    Date.parse(`${started.day}T00:00:00.000Z`)) / DAY_MS) - 1 : 0;
  const hostClass: keyof typeof TARGETS | null = rate === null ? null
    : rate < 30_000 ? "light" : rate <= 150_000 ? "busy" : "studio0_scale";
  return { hostClass, attemptedRowsPerDay: rate, observedDays: days,
    provisional: days < 7 || completeDays < 7,
    rateBasis: "lower_bound_local_admitted_rows_only; counted_gaps_and_other_writers_unavailable" };
}

/** CLI read; the daemon calls this only after a sample, and /status uses its cached return. */
function budgetSamples(db: Database.Database, nowMs = Date.now()): BudgetSample[] {
  if (!db.prepare(`select 1 from sqlite_master where type='table' and name='budget_samples'`).get()) return [];
  return (db.prepare(`select sample_json as json from budget_samples where at_ms >= ?
    order by id desc limit ?`).all(nowMs - DAY_MS, BUDGET_RING_LIMIT) as Array<{ json: string }>)
    .reverse()
    .map((row) => JSON.parse(row.json) as BudgetSample);
}

export function budgetStatus(db: Database.Database, nowMs = Date.now()) {
  if (!db.prepare(`select 1 from sqlite_master where type='table' and name='budget_samples'`).get()) {
    return { mode: "advisory" as const, latest: null, p50: null, p95: null,
      hostClass: null, attemptedRowsPerDay: null, observedDays: 0, provisional: true,
      targets: null, targetStatus: "hypothesis" as const,
      writeRateStatus: "uncalibrated_physical_bytes_unavailable" as const,
      unavailable: ["sampler_not_started"] };
  }
  const samples = budgetSamples(db, nowMs).reverse();
  const summary = (fraction: number) => samples.length
    ? Object.fromEntries(METRICS.map((metric) => [metric, percentile(samples
      .map((sample) => sampleMetric(sample, metric)).filter((value): value is number => value !== null), fraction)]))
    : null;
  const host = attemptedClass(db, nowMs);
  return { mode: "advisory" as const, latest: samples[0] ?? null,
    p50: summary(0.5), p95: summary(0.95),
    ...host, targets: host.hostClass ? TARGETS[host.hostClass] : null,
    targetStatus: "hypothesis" as const,
    writeRateStatus: "uncalibrated_physical_bytes_unavailable" as const,
    unavailable: samples[0]?.unavailable ?? ["no_sample_yet"] };
}

export function budgetCsv(db: Database.Database, nowMs = Date.now()): string {
  if (!db.prepare(`select 1 from sqlite_master where type='table' and name='budget_samples'`).get()) return "";
  const samples = budgetSamples(db, nowMs);
  const daily = budgetDailyRows(db, nowMs);
  const host = attemptedClass(db, nowMs);
  const header = ["record_type", "at", ...METRICS, "attemptedRowsDelta", "processes_json",
    "unavailable_json", "hostClass", "attemptedRowsPerDay", "provisional",
    "rateBasis", "day", "dailyAttemptedRows", "dbstatStatus", "tablePagesJson"];
  const csv = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
  return [header.join(","), ...samples.map((sample) => ["sample", new Date(sample.atMs).toISOString(),
    ...METRICS.map((metric) => sampleMetric(sample, metric)), sample.attemptedRowsDelta,
    JSON.stringify(sample.processes), JSON.stringify(sample.unavailable),
    ...Array(8).fill(null)].map(csv).join(",")),
    ["class", null, ...Array(METRICS.length + 3).fill(null), host.hostClass,
      host.attemptedRowsPerDay, host.provisional, host.rateBasis,
      ...Array(4).fill(null)].map(csv).join(","),
    ...daily.map((row) => ["daily", null, ...Array(METRICS.length + 7).fill(null),
      row.day, row.attemptedRows, row.dbstatStatus, row.tablePagesJson].map(csv).join(","))
  ].join("\n") + "\n";
}

export function budgetExport(db: Database.Database, nowMs = Date.now()) {
  const hasSamples = Boolean(db.prepare(`select 1 from sqlite_master where type='table' and name='budget_samples'`).get());
  const hasControl = Boolean(db.prepare(`select 1 from sqlite_master where type='table' and name='budget_control'`).get());
  const control = hasControl ? db.prepare(`select started_day as startedDay from budget_control where singleton=1`)
    .get() as { startedDay: string } | undefined : undefined;
  const samples = hasSamples ? budgetSamples(db, nowMs) : [];
  return { schema: "plimsoll-budget-export/v1", mode: "advisory" as const,
    startedDay: control?.startedDay ?? null, samples, daily: budgetDailyRows(db, nowMs) };
}

/** dbstat reads every page. The stat size gate runs before the off-thread query. */
export async function recordDailyTableSizes(db: Database.Database, ledgerPath: string, day: string,
  scan?: () => Promise<Array<{ name: string; pages: number }>>): Promise<string> {
  const sizes = await Promise.all([fileBytes(ledgerPath), fileBytes(`${ledgerPath}-wal`), fileBytes(`${ledgerPath}-shm`)]);
  const total = sizes.every((value) => value !== null)
    ? sizes.reduce<number>((sum, value) => sum + (value ?? 0), 0) : null;
  let status = "unavailable: stat";
  let pages: Record<string, number> | null = null;
  if (total !== null && total > BUDGET_DBSTAT_MAX_BYTES) status = "skipped: size";
  else if (total !== null) {
    try {
      const rows = scan ? await scan() : await readLedgerOffThread<{ name: string; pages: number }>(db, [{
        sql: `select coalesce(m.tbl_name, s.name) as name, count(*) as pages
          from dbstat s left join sqlite_master m on m.name=s.name
          group by coalesce(m.tbl_name, s.name) order by name`, params: {}, maxMs: 30_000,
      }]);
      pages = Object.fromEntries(rows.map((row) => [row.name, row.pages]));
      status = "measured";
    } catch { status = "unavailable: dbstat"; }
  }
  db.prepare(`insert into budget_daily(day,attempted_rows,dbstat_status,table_pages_json)
    values(?,0,?,?) on conflict(day) do update set dbstat_status=excluded.dbstat_status,
    table_pages_json=excluded.table_pages_json`).run(day, status, pages ? JSON.stringify(pages) : null);
  return status;
}

export function budgetDailyRows(db: Database.Database, nowMs = Date.now()): DailyRow[] {
  if (!db.prepare(`select 1 from sqlite_master where type='table' and name='budget_daily'`).get()) return [];
  return db.prepare(`select day, version, attempted_rows as attemptedRows, dbstat_status as dbstatStatus,
    table_pages_json as tablePagesJson from budget_daily where day >= ? order by day`).all(
      new Date(nowMs - 7 * DAY_MS).toISOString().slice(0, 10)) as DailyRow[];
}

export class BudgetSampler {
  private timer: NodeJS.Timeout | null = null;
  private stopped = false;
  private previous: BudgetSample | undefined;
  private cache: ReturnType<BudgetSampler["cachedStatus"]> & { lastMainThreadMs: number | null };
  private readonly history: BudgetSample[];
  private readonly sorted = Object.fromEntries(METRICS.map((metric) => [metric, [] as number[]])) as
    Record<typeof METRICS[number], number[]>;
  private dailyInFlight = false;
  private lastDailyDay: string | null = null;
  private inFlight = false;
  private lastAttemptedTotal: number;

  constructor(private readonly db: Database.Database, private readonly ledgerPath: string,
    private readonly intervalMs = 60_000, private readonly attemptedTotal: () => number = () => 0,
    private readonly childPids: () => BudgetChildPid[] = () => []) {
    ensureBudgetSchema(db);
    this.lastAttemptedTotal = attemptedTotal();
    this.history = budgetSamples(db);
    for (const sample of this.history) this.index(sample, true);
    this.cache = { ...this.cachedStatus(Date.now()), lastMainThreadMs: null };
    this.previous = this.cache.latest ?? undefined;
    const today = new Date().toISOString().slice(0, 10);
    if (budgetDailyRows(db).some((row) => row.day === today && row.dbstatStatus !== "pending")) {
      this.lastDailyDay = today;
    }
  }

  private index(sample: BudgetSample, add: boolean) {
    for (const metric of METRICS) {
      const value = sampleMetric(sample, metric);
      if (value === null) continue;
      const values = this.sorted[metric];
      const position = sortedPosition(values, value);
      if (add) values.splice(position, 0, value);
      else if (values[position] === value) values.splice(position, 1);
    }
  }

  private cachedStatus(nowMs: number) {
    const latest = this.history.at(-1) ?? null;
    const host = attemptedClass(this.db, nowMs);
    const summary = (fraction: number) => latest ? Object.fromEntries(METRICS.map((metric) => {
      const values = this.sorted[metric];
      return [metric, values[Math.ceil(values.length * fraction) - 1] ?? null];
    })) as Record<typeof METRICS[number], number | null> : null;
    return { mode: "advisory" as const, latest, p50: summary(0.5), p95: summary(0.95),
      ...host, targets: host.hostClass ? TARGETS[host.hostClass] : null,
      targetStatus: "hypothesis" as const,
      writeRateStatus: "uncalibrated_physical_bytes_unavailable" as const,
      unavailable: latest?.unavailable ?? ["no_sample_yet"] };
  }

  status() { return this.cache; }

  start() {
    if (this.timer || this.stopped) return;
    this.timer = setInterval(() => { void this.sample(); }, this.intervalMs);
    this.timer.unref();
    setImmediate(() => { void this.sample(); });
  }

  stop() { this.stopped = true; if (this.timer) clearInterval(this.timer); this.timer = null; }

  async sample() {
    if (this.stopped || this.inFlight) return;
    this.inFlight = true;
    try {
      const attemptedTotal = this.attemptedTotal();
      const sample = await collectBudgetSample(this.db, this.ledgerPath,
        { previous: this.previous, childPids: this.childPids() });
      if (this.stopped) return;
      sample.attemptedRowsDelta = Math.max(0, attemptedTotal - this.lastAttemptedTotal);
      const writeStarted = performance.now();
      recordBudgetSample(this.db, sample);
      this.lastAttemptedTotal = attemptedTotal;
      const writeMs = performance.now() - writeStarted;
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (this.stopped) return;
      const indexStarted = performance.now();
      this.previous = sample;
      this.history.push(sample);
      this.index(sample, true);
      while (this.history.length > BUDGET_RING_LIMIT ||
        this.history[0]!.atMs < sample.atMs - DAY_MS) this.index(this.history.shift()!, false);
      const day = new Date(sample.atMs).toISOString().slice(0, 10);
      if (!this.dailyInFlight && this.lastDailyDay !== day) {
        this.dailyInFlight = true;
        void recordDailyTableSizes(this.db, this.ledgerPath, day)
          .then(() => { this.lastDailyDay = day; })
          .catch(() => undefined).finally(() => { this.dailyInFlight = false; });
      }
      const indexMs = performance.now() - indexStarted;
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (this.stopped) return;
      const statusStarted = performance.now();
      const status = this.cachedStatus(sample.atMs);
      const statusMs = performance.now() - statusStarted;
      this.cache = { ...status,
        lastMainThreadMs: Math.round(Math.max(sample.samplerMainThreadMs, writeMs, indexMs, statusMs) * 100) / 100 };
    } catch {
      // Observation may be skipped under a writer lock; capture must proceed.
      this.cache = { ...this.cache, unavailable: [...this.cache.unavailable, "sampler_tick_failed"] };
    } finally { this.inFlight = false; }
  }
}
