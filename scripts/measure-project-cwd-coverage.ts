#!/usr/bin/env node
/** Compare each event's own cwd key with the collector's upload rule on a ledger copy. */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";

import { SessionAttributionBatch } from "../packages/collector-cli/src/session-attribution";
import { estimateCostUsd, priceForModel, type AiInteractionEvent } from "../packages/shared/src/index";

const ledgerArg = process.argv[process.argv.indexOf("--ledger") + 1];
if (!process.argv.includes("--confirm-copy") || !ledgerArg || !path.isAbsolute(ledgerArg)) {
  throw new Error("pass --ledger /absolute/lane/measurement/copy.sqlite --confirm-copy");
}
const permitted = path.resolve(process.cwd(), "../../measurement");
const ledger = path.resolve(ledgerArg);
if (!ledger.startsWith(`${permitted}${path.sep}`) || !fs.lstatSync(ledger).isFile() ||
    fs.lstatSync(ledger).isSymbolicLink()) throw new Error("ledger must be a regular lane measurement copy");

const db = new Database(ledger, { readonly: true, fileMustExist: true });
const scanDb = new Database(ledger, { readonly: true, fileMustExist: true });
const HASH = /^sha256:[a-f0-9]{64}$/i;
const key = (value: unknown) => typeof value === "string" && HASH.test(value)
  ? value.toLowerCase() : null;
const sessionHash = (value: string) => `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
const round = (value: number) => Number(value.toFixed(6));

function listPrice(row: LedgerRow) {
  const model = row.model?.toLowerCase() ?? "";
  const pricedModel = priceForModel(model) ? model :
    model.startsWith("gpt-") ? "gpt-5.5" :
      model.startsWith("claude-opus") ? "claude-opus-5" :
        model.startsWith("claude-sonnet") ? "claude-sonnet-5" :
          model.startsWith("claude-fable") ? "claude-fable-5" :
            model.startsWith("claude-haiku") ? "claude-haiku-4-5" : "";
  if (!pricedModel) return { usd: 0, kind: "unpriced" };
  return {
    usd: estimateCostUsd({ model: pricedModel,
      inputTokens: row.inputTokens ?? undefined,
      outputTokens: row.outputTokens ?? undefined,
      cacheReadTokens: row.cacheReadTokens ?? undefined,
      cacheCreationTokens: row.cacheCreationTokens ?? undefined,
    })?.costUsd ?? 0,
    kind: pricedModel === model ? "catalog" : "family_proxy",
  };
}

type LedgerRow = {
  id: string; sessionId: string | null; observedAt: string; payloadJson: string;
  repoHash: string | null; branchHash: string | null; model: string | null;
  inputTokens: number | null; outputTokens: number | null;
  cacheReadTokens: number | null; cacheCreationTokens: number | null;
};
type Count = { rows: number; usd: number };
const empty = (): Count => ({ rows: 0, usd: 0 });
const add = (cell: Count, usd: number) => { cell.rows += 1; cell.usd += usd; };
const totals = empty(), eventCwdOnly = empty(), uploadRule = empty();
const bases: Record<string, Count> = {};
const pricing: Record<string, Count> = {};
const bySource: Record<string, { total: Count; eventCwdOnly: Count; uploadRule: Count }> = {};
const sessions = new Map<string, { start: number; after: number; keys: Set<string> }>();
const query = scanDb.prepare(`select id, session_id as sessionId, observed_at as observedAt,
    payload_json as payloadJson, repo_hash as repoHash, branch_hash as branchHash,
    model, input_tokens as inputTokens, output_tokens as outputTokens,
    cache_read_tokens as cacheReadTokens, cache_creation_tokens as cacheCreationTokens
  from buffered_events
  where data_mode <> 'evidence' and privacy_disposition is null
    and usage_duplicate_reason is null
    and (event_type in ('usage_rollout','usage_transcript','usage_live')
      or input_tokens is not null or output_tokens is not null
      or cache_read_tokens is not null or cache_creation_tokens is not null
      or cost_usd is not null)
  order by rowid`);

function measure(rows: LedgerRow[]) {
  if (!rows.length) return;
  const events = rows.map((row) => JSON.parse(row.payloadJson) as AiInteractionEvent);
  const batch = new SessionAttributionBatch(db, rows.map((row, index) =>
    ({ event: events[index]!, repoHash: row.repoHash })));
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index]!, event = events[index]!;
    const { usd, kind } = listPrice(row);
    if (usd <= 0) continue;
    add(totals, usd);
    add(pricing[kind] ??= empty(), usd);
    const source = bySource[event.source] ??= { total: empty(), eventCwdOnly: empty(), uploadRule: empty() };
    add(source.total, usd);
    const startKey = key(event.projectKey) ?? key(row.repoHash);
    if (startKey) { add(eventCwdOnly, usd); add(source.eventCwdOnly, usd); }
    const result = batch.attribute(event, { repoHash: row.repoHash, branchHash: row.branchHash });
    add(bases[result.basis] ??= empty(), usd);
    const resultKey = key(result.event.projectKey);
    if (resultKey) { add(uploadRule, usd); add(source.uploadRule, usd); }
    if (row.sessionId) {
      const hashed = sessionHash(row.sessionId);
      const session = sessions.get(hashed) ?? { start: 0, after: 0, keys: new Set<string>() };
      if (startKey) session.start += usd;
      if (resultKey) { session.after += usd; session.keys.add(resultKey); }
      sessions.set(hashed, session);
    }
  }
}

try {
  let rows: LedgerRow[] = [];
  for (const row of query.iterate() as IterableIterator<LedgerRow>) {
    rows.push(row);
    if (rows.length === 500) { measure(rows); rows = []; }
  }
  measure(rows);
} finally { scanDb.close(); db.close(); }

const share = (cell: Count) => ({ rows: cell.rows, listUsd: round(cell.usd),
  valueSharePct: round(100 * cell.usd / totals.usd) });
const examples = [...sessions].filter(([, row]) => row.start === 0 && row.after > 0)
  .sort((a, b) => b[1].after - a[1].after).slice(0, 5)
  .map(([id, row]) => ({ sessionHash: id, projectKeys: [...row.keys].sort(),
    newlyAttributedListUsd: round(row.after) }));
console.log(JSON.stringify({ schema: "project-cwd-coverage/v1", pricedUsage: share(totals),
  eventCwdOnly: share(eventCwdOnly), collectorUploadRule: share(uploadRule),
  bySource: Object.fromEntries(Object.entries(bySource).map(([name, row]) => [name, {
    total: share(row.total), eventCwdOnly: share(row.eventCwdOnly), collectorUploadRule: share(row.uploadRule),
  }])),
  bases: Object.fromEntries(Object.entries(bases).map(([name, cell]) => [name, share(cell)])),
  pricing: Object.fromEntries(Object.entries(pricing).map(([name, cell]) => [name, share(cell)])),
  examples,
}, null, 2));
