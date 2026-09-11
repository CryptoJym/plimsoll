import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { runPendingEventLinkFillStage } from "../../packages/collector-cli/src/maintenance-stage-primitives";
import crypto from "node:crypto";

const [ledgerPath, holdMsText] = process.argv.slice(2);
if (!ledgerPath || !holdMsText) {
  throw new Error("usage: tsx hold-maintenance-fill-writer.ts LEDGER HOLD_MS");
}
const holdMs = Number(holdMsText);
const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 900 });
const database = buffer.database;
const contextId = `repoctx:v1:${"3".repeat(64)}`;
const repoHash = `sha256:${"4".repeat(64)}`;
const eventId = crypto.randomUUID();
const now = new Date().toISOString();
database.prepare(
  `insert into buffered_events
     (id, source, event_type, data_mode, observed_at, payload_json,
      suppressed_fields_json, created_at)
   values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?)`,
).run(eventId, now, now);
database.prepare(
  `insert into repo_context_results
     (context_id, repo_hash, branch_hash, head_sha, resolved_at, resolver_version, accepted_at)
   values (?, ?, null, null, ?, 'sync-storage-proof-v1', ?)
   on conflict(context_id) do nothing`,
).run(contextId, repoHash, now, now);
database.prepare(
  `insert into repo_context_event_links
     (event_id, context_id, fill_pending, context_conflict, suppression_cleaned)
   values (?, ?, 1, 0, 0)`,
).run(eventId, contextId);

const originalPrepare = database.prepare.bind(database);
let injected = false;
(database as any).prepare = (sql: string) => {
  const statement = originalPrepare(sql);
  if (
    !injected &&
    sql.includes("from repo_context_event_links l indexed by") &&
    sql.includes("order by l.event_id limit")
  ) {
    const all = statement.all.bind(statement);
    (statement as any).all = (...args: any[]) => {
      injected = true;
      if (!database.inTransaction) throw new Error("fill stage did not claim writer before read");
      process.stdout.write("READY\n");
      const until = performance.now() + holdMs;
      while (performance.now() < until) { /* deterministic synchronous stage work */ }
      return all(...args);
    };
  }
  return statement;
};

try {
  const result = runPendingEventLinkFillStage(database, { remainingMs: 30_000, batchSize: 256 });
  if (!injected) throw new Error("fill-stage writer seam not reached");
  process.stdout.write(`RELEASED ${JSON.stringify(result)}\n`);
} finally {
  (database as any).prepare = originalPrepare;
  buffer.close();
}
