import assert from "node:assert/strict";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { ensureSessionSummarySchema, updateSessionSummary,
  type SessionReadQuery } from "../packages/collector-cli/src/session-summary";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  buildSessionSyncRow, collectSessionSnapshots, commitDaemonSessionSyncFailure,
  commitDaemonSessionSyncSuccess, loadDaemonSessionSyncState, planDaemonSessionSync,
  runSessionSync, saveDaemonSessionSyncState,
} from "../packages/collector-cli/src/session-sync";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const tenantId = "00000000-0000-4000-8000-000000000106";
const installKey = "summary-repair-proof-install";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey,
  uploadSigningSecret: "summary-repair-proof-secret",
});
export const sessionId = "00000000-0000-4000-8000-000000000106";
export const eventId = (index: number) => `00000000-0000-4000-8000-${String(index + 106_000_000).padStart(12, "0")}`;
export const initialUntil = "2026-09-25T00:00:00.000Z";

export function fixture(name: string) {
  return new LocalEventBuffer(path.join(process.env.PLIMSOLL_PROOF_ROOT!, name), { workspaceId: tenantId });
}

export function directRead(buffer: LocalEventBuffer) {
  return async <T,>(queries: SessionReadQuery[]): Promise<T[]> =>
    queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]);
}

export async function drainSummary(buffer: LocalEventBuffer, until: string, maxPasses: number) {
  ensureSessionSummarySchema(buffer.database);
  let rowsRead = 0;
  let fullRecomputes = 0;
  for (let pass = 1; pass <= maxPasses; pass += 1) {
    const result = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead(buffer) });
    rowsRead += result.rowsRead;
    fullRecomputes += Number(result.fullRecompute);
    if (result.complete) return { result, passes: pass, rowsRead, fullRecomputes };
  }
  throw new Error(`summary did not drain in ${maxPasses} passes; rowsRead=${rowsRead}; fullRecomputes=${fullRecomputes}`);
}

export function appendRows(buffer: LocalEventBuffer, count: number, future?: (index: number) => string | null) {
  const insert = buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
     cost_usd, workspace_id, privacy_generation, usage_duplicate_reason)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, 2, 3,
      0.25, ?, ?, ?)`);
  for (let start = 0; start < count; start += 256) {
    buffer.database.transaction(() => {
      for (let index = start; index < Math.min(start + 256, count); index += 1) {
        const id = eventId(index);
        insert.run(id, new Date(Date.parse("2026-09-20T00:00:00.000Z") + index).toISOString(),
          future?.(index) ?? "2026-09-20T00:00:00.000Z", sessionId, tenantId,
          `generation-${id}`, index % 17 === 0 ? "sse_usage_kept_copy" : null);
      }
    }).immediate();
  }
}

export function appendBackdatedRow(buffer: LocalEventBuffer, index: number) {
  const id = eventId(index);
  assert.equal(buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
     cost_usd, workspace_id, privacy_generation)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, 2, 3,
      0.25, ?, ?)`).run(id, "2026-09-19T23:59:59.000Z", "2026-09-20T00:00:00.000Z",
    sessionId, tenantId, `generation-${id}`).changes, 1);
}

export function expectedWire(buffer: LocalEventBuffer, until: string) {
  const expected = collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until })[0];
  assert.ok(expected);
  const normalized = buildSessionSyncRow(expected);
  assert.equal(normalized.ok, true);
  return normalized.row;
}

export function scheduleCycle(buffer: LocalEventBuffer, until: string) {
  const prior = loadDaemonSessionSyncState(buffer.database);
  const plan = planDaemonSessionSync({ db: buffer.database, state: prior, uploadedBatches: [], until });
  assert.equal(plan.skip, false, "fixture must schedule the session");
  saveDaemonSessionSyncState(buffer.database, plan.state);
  return plan;
}

export async function daemonCycle(buffer: LocalEventBuffer, until: string,
  scheduled?: ReturnType<typeof scheduleCycle>) {
  const plan = scheduled ?? scheduleCycle(buffer, until);
  const sent: Array<{ session: { id: string }; [key: string]: unknown }> = [];
  const fetchImpl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const raw = String(init?.body ?? "");
    sent.push(...(JSON.parse(raw).sessions ?? []));
    return new Response(JSON.stringify({
      ...acceptedFixtureDelivery(raw, installKey), inserted: sent.length, updated: 0, skippedStale: 0,
    }), { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  const result = await runSessionSync(config, {
    sessionIds: [sessionId],
    until: plan.until, ledgerDb: buffer.database, incremental: true,
    // The fixture measures summary repair and daemon horizon behavior. Keep
    // its reads deterministic; the separate live-churn proof exercises the
    // off-thread reader and records its interruption rate.
    proofSummaryHooks: { read: directRead(buffer) },
    fetchImpl, sleep: async () => undefined, delayMs: 0, maxAttemptsPerBatch: 1,
    log: () => undefined,
  });
  const next = result.ok && result.summaryComplete
    ? commitDaemonSessionSyncSuccess(plan.state, plan.until, result.rejectedSessionIds)
    : commitDaemonSessionSyncFailure(plan.state,
      plan.sessionIds === undefined ? undefined : [...plan.sessionIds, ...result.pendingSummarySessionIds]);
  saveDaemonSessionSyncState(buffer.database, next);
  return { result, sent, state: loadDaemonSessionSyncState(buffer.database) };
}
