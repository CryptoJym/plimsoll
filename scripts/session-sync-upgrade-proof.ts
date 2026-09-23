import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import Database from "better-sqlite3";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  commitDaemonSessionSyncSuccess,
  emptyDaemonSessionSyncState,
  loadDaemonSessionSyncState,
  planDaemonSessionSync,
  runSessionSync,
  saveDaemonSessionSyncState,
} from "../packages/collector-cli/src/session-sync";
import {
  deliveryItemId,
  deliveryAcknowledgement,
  deliveryExpectation,
} from "../packages/collector-cli/src/delivery-ack";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

/**
 * Upgrade proof for eco-6hoxj.163.15.
 *
 * The fixture is written by the v0.7.4 LocalEventBuffer and then reopened by
 * the current collector code. The loopback response mirrors the cloud bundle's
 * session-sync route: tenant/device guards reject one already-held foreign row,
 * while inserted/updated/skippedStale are calculated over the complete request.
 *
 * EXPECT defaults to `green` for a working tree. Set EXPECT=red on the pinned
 * base to record the pre-fix failure without weakening the assertion.
 */

const oldRoot = process.env.PLIMSOLL_V074_ROOT ?? path.resolve(process.cwd(), "../plimsoll-v074");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-session-sync-upgrade-"));
const ledgerPath = path.join(root, "buffer.sqlite");
const currentTenant = "00000000-0000-4000-8000-000000000701";
const previousTenant = "00000000-0000-4000-8000-000000000702";
const deviceInstallId = "00000000-0000-4000-8000-000000000703";
const oldDeviceInstallId = deviceInstallId;
const installKey = "session-sync-upgrade-proof-install";
const until = "2026-09-23T23:59:59.000Z";
const expectedSourceCounts = { claude_code: 209, codex: 108 };
const conflictSessionId = "00000000-0000-4000-8000-000000000799";

type Receipt = {
  schema: "plimsoll.collector-session-sync-upgrade-proof/v1";
  expected: "red" | "green";
  passed: boolean;
  fixture: {
    ledgerPath: string;
    binding: { currentWorkspaceId: string; previousWorkspaceId: string | null };
    sessionCount: number;
    sourceCounts: Record<string, number>;
    conflictSessionId: string;
  };
  wire: {
    sentSessions: number;
    acceptedSessions: number;
    rejectedSessions: number;
    insertedSessions: number | null;
    updatedSessions: number | null;
  };
  result: { ok: boolean; reason: string | null };
  state: { caughtUp: boolean; pendingSessionIds: string[]; blockedSessionIds?: string[] };
  exactFailure?: string;
};

function uuid(n: number) {
  return `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
}

async function createV074Ledger() {
  const legacyBufferModule = await import(
    pathToFileURL(path.join(oldRoot, "packages/collector-cli/src/buffer.ts")).href,
  );
  const legacyShared = await import(
    pathToFileURL(path.join(oldRoot, "packages/shared/src/index.ts")).href,
  );
  const LegacyBuffer = legacyBufferModule.LocalEventBuffer as typeof import("../packages/collector-cli/src/buffer").LocalEventBuffer;
  const legacyEventSchema = legacyShared.aiInteractionEventSchema as typeof aiInteractionEventSchema;
  const buffer = new LegacyBuffer(ledgerPath, {
    workspaceId: previousTenant,
    deviceId: oldDeviceInstallId,
    enrollmentNow: () => new Date("2026-09-07T20:59:00.000Z"),
  });
  try {
    buffer.transitionWorkspace(previousTenant, currentTenant, deviceInstallId, uuid(705));
    let eventNumber = 1;
    const sourceCounts: Record<string, number> = {};
    for (const [source, count] of Object.entries(expectedSourceCounts)) {
      sourceCounts[source] = count;
      for (let index = 0; index < count; index += 1) {
        const sessionId = source === "claude_code"
          ? uuid(1_000 + index)
          : index === 0 ? conflictSessionId : uuid(2_000 + index);
        const event = legacyEventSchema.parse({
          id: uuid(10_000 + eventNumber),
          sessionId,
          source,
          eventType: "assistant_response",
          observedAt: `2026-09-23T${String(10 + (eventNumber % 10)).padStart(2, "0")}:00:00.000Z`,
          inputTokens: 3,
          outputTokens: 1,
        });
        assert.equal(Boolean(buffer.append(event)), true, `legacy event ${eventNumber} was not admitted`);
        eventNumber += 1;
      }
    }
  } finally {
    buffer.close();
  }

  const db = new Database(ledgerPath, { readonly: true });
  try {
    const binding = db.prepare(`
      select current_workspace_id as currentWorkspaceId,
             previous_workspace_id as previousWorkspaceId
      from collector_workspace_binding where singleton = 1
    `).get() as { currentWorkspaceId: string; previousWorkspaceId: string | null };
    const sourceRows = db.prepare(`
      select source, count(distinct session_id) as sessions
      from buffered_events where session_id is not null group by source order by source
    `).all() as Array<{ source: string; sessions: number }>;
    const sessionCount = (db.prepare(`select count(distinct session_id) as n from buffered_events`).get() as { n: number }).n;
    assert.deepEqual(binding, { currentWorkspaceId: currentTenant, previousWorkspaceId: previousTenant });
    assert.equal(sessionCount, 317);
    assert.deepEqual(Object.fromEntries(sourceRows.map((row) => [row.source, row.sessions])), expectedSourceCounts);
    return { binding, sessionCount, sourceCounts: expectedSourceCounts };
  } finally {
    db.close();
  }
}

function cloudRoute(fetchState: {
  requests: Array<{ expected: ReturnType<typeof deliveryExpectation>; body: Record<string, unknown> }>;
}) {
  return (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const rawBody = String(init?.body ?? "");
    const body = JSON.parse(rawBody) as Record<string, unknown> & {
      sessions: Array<{ session: { id: string } }>;
    };
    const expected = deliveryExpectation(rawBody, installKey);
    const acceptedIds = body.sessions
      .filter((row) => row.session.id.toLowerCase() !== conflictSessionId)
      .map((row) => deliveryItemId("session", row.session.id));
    const inserted = acceptedIds.length;
    const skippedStale = body.sessions.length - inserted;
    const ack = deliveryAcknowledgement(expected, acceptedIds);
    fetchState.requests.push({ expected, body });
    return new Response(JSON.stringify({
      ok: true,
      accepted: acceptedIds.length,
      inserted,
      updated: 0,
      skippedStale,
      ack,
    }), { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
}

async function main() {
  const fixture = await createV074Ledger();
  const db = new Database(ledgerPath);
  const config = collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1:1/ingest",
    tenantId: currentTenant,
    installKey,
    deviceId: deviceInstallId,
    uploadSigningSecret: "session-sync-upgrade-proof-secret",
  });
  const requests: Array<{ expected: ReturnType<typeof deliveryExpectation>; body: Record<string, unknown> }> = [];
  const result = await runSessionSync(config, {
    until,
    ledgerDb: db,
    fetchImpl: cloudRoute({ requests }),
    sleep: async () => undefined,
    delayMs: 0,
    maxAttemptsPerBatch: 1,
    batchSize: 500,
    concurrency: 1,
    log: () => undefined,
  });
  const rejectedSessionIds =
    (result as typeof result & { rejectedSessionIds?: string[] }).rejectedSessionIds ?? [];
  const baseState = emptyDaemonSessionSyncState();
  const stateBefore = planDaemonSessionSync({ db, state: baseState, uploadedBatches: [], until });
  saveDaemonSessionSyncState(db, stateBefore.state);
  const stateAfter = result.ok
    ? commitDaemonSessionSyncSuccess(stateBefore.state, until, rejectedSessionIds)
    : stateBefore.state;
  saveDaemonSessionSyncState(db, stateAfter);
  const state = loadDaemonSessionSyncState(db);
  const expected = (process.env.EXPECT ?? "green") === "red" ? "red" : "green";
  const exactFailure = result.reason ?? undefined;
  const green = result.ok && result.ledgerSessions === 317 && result.eligibleSessions === 317 &&
    result.sentSessions === 317 && result.acceptedSessions === 316 &&
    rejectedSessionIds.length === 1 && rejectedSessionIds[0] === conflictSessionId &&
    result.insertedSessions === 316 && result.updatedSessions === 0 &&
    state.caughtUp && state.pendingSessionIds.length === 0 &&
    state.blockedSessionIds?.includes(conflictSessionId) === true &&
    requests.length === 1 && requests[0]!.body.kind === "session_sync";
  const red = !result.ok && result.reason?.includes("invalid_acknowledgement") === true &&
    result.sentSessions === 0 && result.acceptedSessions === 0 && requests.length === 1;
  const passed = expected === "green" ? green : red;
  const receipt: Receipt = {
    schema: "plimsoll.collector-session-sync-upgrade-proof/v1",
    expected,
    passed,
    fixture: { ledgerPath, ...fixture, conflictSessionId },
    wire: {
      sentSessions: result.sentSessions,
      acceptedSessions: result.acceptedSessions,
      rejectedSessions: rejectedSessionIds.length,
      insertedSessions: result.insertedSessions,
      updatedSessions: result.updatedSessions,
    },
    result: { ok: result.ok, reason: result.reason },
    state: {
      caughtUp: state.caughtUp,
      pendingSessionIds: state.pendingSessionIds,
      blockedSessionIds: state.blockedSessionIds,
    },
    ...(exactFailure ? { exactFailure } : {}),
  };
  console.log(JSON.stringify(receipt, null, 2));
  db.close();
  fs.rmSync(root, { recursive: true, force: true });
  if (!passed) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
