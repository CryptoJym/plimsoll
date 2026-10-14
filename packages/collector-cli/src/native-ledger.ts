import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import { capturedAccountHash, NATIVE_PROVIDER_ACCOUNT_DERIVATION, type NativeAccountDerivation } from "./native-account-links";

export type NativeProviderAccountEvidence = "native047_metadata_user_account_id" | "native047_metadata_user_account_uuid";

export type NativeLedgerEvent = {
  rowid: number;
  id: string;
  source: string;
  eventType: string | null;
  observedAt: string | null;
  threadId: string | null;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheWriteTokens: number | null;
  referenceCostUsd: number | null;
  costKind: string | null;
  projectKey: string | null;
  projectEvidence?: "explicit_project_key" | "repository_hash" | null;
  repositoryKey?: string | null;
  accountHash: string | null;
  providerAccountHash?: string | null;
  providerAccountDerivation?: NativeAccountDerivation | null;
  providerAccountEvidence?: NativeProviderAccountEvidence | null;
  usageAuthority: "live" | "tailer" | "unknown";
  authorityAccepted: boolean | null;
};

export type NativeLedgerReading = {
  state: "observed" | "unknown";
  coverage: "recent_window" | "incremental" | "unknown";
  events: NativeLedgerEvent[];
  maximumRowid: number | null;
  throughRowid: number | null;
  hasMore: boolean;
  generation: string | null;
  note: string;
  rawRowCount?: number;
};

const reader = String.raw`
const Database = require(process.argv[1]);
const file = process.argv[2];
const after = process.argv[3] === '' ? null : Number(process.argv[3]);
const limit = Number(process.argv[4]);
const db = new Database(file, { readonly: true, fileMustExist: true, timeout: 500 });
try {
  db.exec('begin');
  const columns = new Set(db.prepare('pragma table_info(buffered_events)').all().map(c => c.name));
  if (!columns.has('id') || !columns.has('source')) throw new Error('ledger_schema_unavailable');
  const names = { id:'id', source:'source', event_type:'eventType', observed_at:'observedAt',
    session_id:'threadId', model:'model', input_tokens:'inputTokens', output_tokens:'outputTokens',
    cache_read_tokens:'cacheReadTokens', cache_creation_tokens:'cacheWriteTokens',
    cost_usd:'referenceCostUsd', cost_kind:'costKind', account_hash:'accountHash' };
  const fields = Object.entries(names).map(([c,a]) => (columns.has(c) ? c : 'null') + ' as ' + a);
  const hasTable = name => Boolean(db.prepare("select 1 from sqlite_master where type='table' and name=?").get(name));
  const hasAuthority = hasTable('session_usage_authority') && columns.has('session_id');
  const eventType = columns.has('event_type') ? 'event_type' : 'null';
  const accountEligibility = hasAuthority ? "case (select authority from session_usage_authority a where a.source=buffered_events.source and a.session_id=buffered_events.session_id) " +
    "when 'live' then coalesce(" + eventType + " not in ('usage_rollout','usage_transcript'),1) " +
    "when 'tailer' then coalesce(" + eventType + " in ('usage_rollout','usage_transcript'),0) else 1 end" : '1';
  // Extract exactly two scalar fields in SQLite, inside the selected raw rowid
  // window and privacy predicate. No payload, body or arbitrary JSON leaves
  // this worker, including on malformed/missing/unprotected metadata.
  fields.push((columns.has('payload_json') ? "case when (" + accountEligibility + ") and json_valid(payload_json) then case source " +
    "when 'codex' then case when json_type(payload_json,'$.metadata.\"user.account_id\"')='text' " +
      "then json_extract(payload_json,'$.metadata.\"user.account_id\"') end " +
    "when 'claude_code' then case when json_type(payload_json,'$.metadata.\"user.account_uuid\"')='text' " +
      "then json_extract(payload_json,'$.metadata.\"user.account_uuid\"') end end end" : 'null') + ' as protectedProviderAccount');
  fields.push((columns.has('project_key') ? 'project_key' : 'null') + ' as explicitProjectKey');
  fields.push((columns.has('repo_hash') ? 'repo_hash' : 'null') + ' as repositoryKey');
  // Mirror terminalPrivacyEligibilitySql, checked against that authoritative
  // predicate in native-subscription-proof. This worker never reads bodies.
  const privacy = [];
  if (columns.has('data_mode')) privacy.push("buffered_events.data_mode <> 'evidence'");
  if (columns.has('privacy_disposition')) privacy.push('buffered_events.privacy_disposition is null');
  if (columns.has('privacy_generation')) privacy.push('buffered_events.privacy_generation is not null');
  if (columns.has('usage_duplicate_reason')) privacy.push('buffered_events.usage_duplicate_reason is null');
  if (hasTable('upload_receipts')) privacy.push("not exists (select 1 from upload_receipts p where p.delivery_id=buffered_events.id and p.reason in ('local_evidence_quarantined','local_privacy_violation'))");
  const outbox = hasTable('upload_outbox') ? new Set(db.prepare('pragma table_info(upload_outbox)').all().map(c=>c.name)) : new Set();
  if (columns.has('privacy_generation') && outbox.has('raw_id') && outbox.has('raw_created_at') && outbox.has('raw_generation')) {
    privacy.push('not exists (select 1 from upload_outbox p where p.raw_rowid=buffered_events.rowid and (p.raw_id is null or p.raw_created_at is null or p.raw_generation is null or buffered_events.privacy_generation is null or p.raw_id is not buffered_events.id or p.raw_created_at is not buffered_events.created_at or p.raw_generation is not buffered_events.privacy_generation))');
  }
  fields.push(hasAuthority ? '(select authority from session_usage_authority a where a.source=buffered_events.source and a.session_id=buffered_events.session_id) as usageAuthority' : 'null as usageAuthority');
  const maximumRowid = db.prepare('select max(rowid) as value from buffered_events').get().value || 0;
  const reset = after !== null && after > maximumRowid;
  const baseline = after === null || reset;
  const rawRows = db.prepare('select rowid from buffered_events ' +
    (baseline ? 'order by rowid desc limit ?' : 'where rowid > ? order by rowid limit ?'))
    .all(...(baseline ? [limit] : [after, limit + 1]));
  const hasMore = !baseline && rawRows.length > limit;
  const selected = rawRows.slice(0,limit);
  if (baseline) selected.reverse();
  // Force the rowid range. A privacy-status index can otherwise make SQLite
  // inspect millions of rows before applying this small rowid window.
  const events = selected.length ? db.prepare('select rowid,' + fields.join(',') + ' from buffered_events not indexed where rowid >= ? and rowid <= ?' +
    (privacy.length ? ' and ' + privacy.join(' and ') : '') + ' order by rowid').all(selected[0].rowid, selected[selected.length-1].rowid) : [];
  for (const event of events) {
    if (typeof event.accountHash !== 'string' || !/^(?:sha256:[a-f0-9]{16}|(?:sha256:)?[a-f0-9]{64})$/i.test(event.accountHash)) event.accountHash = null;
    const accepted = event.usageAuthority === 'live' ? !['usage_rollout','usage_transcript'].includes(event.eventType) :
      event.usageAuthority === 'tailer' ? ['usage_rollout','usage_transcript'].includes(event.eventType) : true;
    event.providerAccountHash = accepted && typeof event.protectedProviderAccount === 'string' &&
      /^sha256:[a-f0-9]{16}$/i.test(event.protectedProviderAccount) ? event.protectedProviderAccount.toLowerCase() : null;
    delete event.protectedProviderAccount;
  }
  db.exec('commit');
  process.stdout.write(JSON.stringify({ events, maximumRowid,
    throughRowid: selected.length ? selected[selected.length-1].rowid : after || 0,
    coverage: baseline ? 'recent_window' : 'incremental', hasMore, reset, rawRowCount: selected.length }));
} finally { db.close(); }
`;

function safeString(value: unknown, max = 256): string | null {
  return typeof value === "string" && value.length > 0 && value.length <= max && !/[\u0000-\u001f]/.test(value) ? value : null;
}

function counter(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

/** A hard process deadline bounds SQLite opening, lock waits, and execution. */
export function readNativeLedger(
  file: string,
  options: { afterRowid?: number | null; limit?: number; timeoutMs?: number } = {},
): NativeLedgerReading {
  const unavailable = (note: string): NativeLedgerReading => ({
    state: "unknown", coverage: "unknown", events: [], maximumRowid: null,
    throughRowid: null, hasMore: false, generation: null, note,
  });
  const limit = options.limit ?? 2_000;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 50_000) throw new Error("native_ledger_limit_invalid");
  const after = options.afterRowid ?? null;
  if (after !== null && (!Number.isSafeInteger(after) || after < 0)) throw new Error("native_ledger_cursor_invalid");
  const timeoutMs = options.timeoutMs ?? 4_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) throw new Error("native_ledger_timeout_invalid");
  let generation: string;
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) return unavailable("Collector ledger is absent or not a regular file; totals are UNKNOWN.");
    generation = `${stat.dev}:${stat.ino}:${stat.birthtimeMs}`;
  } catch { return unavailable("Collector ledger is absent; totals are UNKNOWN."); }
  const require = createRequire(import.meta.url);
  const result = spawnSync(process.execPath, ["-e", reader, require.resolve("better-sqlite3"), file, after === null ? "" : String(after), String(limit)], {
    encoding: "utf8", timeout: timeoutMs, maxBuffer: 32 * 1024 * 1024,
    killSignal: "SIGKILL", windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error || result.status !== 0) {
    const errorCode = result.error ? (result.error as NodeJS.ErrnoException).code ?? "worker_error" :
      /SQLITE_[A-Z_]+/.exec(result.stderr)?.[0] ?? "worker_failed";
    return unavailable(`Bounded collector read failed (${errorCode}); totals are UNKNOWN.`);
  }
  try {
    const raw = JSON.parse(result.stdout) as { events: Record<string, unknown>[]; maximumRowid: number; throughRowid: number; coverage: "recent_window" | "incremental"; hasMore: boolean; reset: boolean; rawRowCount: number };
    const events: NativeLedgerEvent[] = [];
    for (const row of raw.events) {
      const id = safeString(row.id);
      const source = safeString(row.source, 64);
      const rowid = counter(row.rowid);
      if (!id || !source || rowid === null || !["codex","claude_code","grok","unknown"].includes(source)) continue;
      const projectHash = (value: unknown) => typeof value === "string" && /^sha256:[a-f0-9]{64}$/i.test(value) ? value.toLowerCase() : null;
      const explicitProjectKey = projectHash(row.explicitProjectKey);
      const repositoryKey = projectHash(row.repositoryKey);
      const providerAccountHash = typeof row.providerAccountHash === "string" && /^sha256:[a-f0-9]{16}$/.test(row.providerAccountHash) ? row.providerAccountHash : null;
      events.push({
        rowid, id, source, eventType: safeString(row.eventType), observedAt: safeString(row.observedAt),
        threadId: safeString(row.threadId), model: safeString(row.model),
        inputTokens: counter(row.inputTokens), outputTokens: counter(row.outputTokens),
        cacheReadTokens: counter(row.cacheReadTokens), cacheWriteTokens: counter(row.cacheWriteTokens),
        referenceCostUsd: typeof row.referenceCostUsd === "number" && Number.isFinite(row.referenceCostUsd) && row.referenceCostUsd >= 0 ? row.referenceCostUsd : null,
        costKind: safeString(row.costKind, 32),
        projectKey: explicitProjectKey ?? repositoryKey,
        projectEvidence: explicitProjectKey ? "explicit_project_key" : repositoryKey ? "repository_hash" : null,
        repositoryKey,
        accountHash: capturedAccountHash(row.accountHash) ? row.accountHash as string : null,
        providerAccountHash,
        providerAccountDerivation: providerAccountHash ? NATIVE_PROVIDER_ACCOUNT_DERIVATION : null,
        providerAccountEvidence: providerAccountHash ? source === "codex" ? "native047_metadata_user_account_id" : "native047_metadata_user_account_uuid" : null,
        usageAuthority: row.usageAuthority === "live" || row.usageAuthority === "tailer" ? row.usageAuthority : "unknown",
        authorityAccepted: row.usageAuthority === "live" ? !["usage_rollout", "usage_transcript"].includes(String(row.eventType)) :
          row.usageAuthority === "tailer" ? ["usage_rollout", "usage_transcript"].includes(String(row.eventType)) : null,
      });
    }
    return { state: "observed", coverage: raw.coverage, events, maximumRowid: raw.maximumRowid,
      throughRowid: raw.throughRowid, hasMore: raw.hasMore, generation,
      rawRowCount: raw.rawRowCount,
      note: raw.coverage === "recent_window" ? "Bounded recent events only; earlier history and fleet completeness are UNKNOWN." : "Incremental events since the saved local cursor; earlier history remains UNKNOWN." };
  } catch { return unavailable("Collector read returned an unsupported result; totals are UNKNOWN."); }
}
