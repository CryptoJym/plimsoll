import { nativeCodexFixture } from "./lib/native-codex-fixture";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { runInNewContext } from "node:vm";
import Database from "better-sqlite3";
import ts from "typescript";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { SyncStorageRetryController } from "../packages/collector-cli/src/sqlite-contention";
import {
  emptyDaemonSessionSyncState,
  loadDaemonSessionSyncState,
  saveDaemonSessionSyncState,
  saveDaemonSessionSyncStateWithRetry,
} from "../packages/collector-cli/src/session-sync";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

// Run the daemon's actual carry closure with controlled local SQLite writers.
// The only substituted dependency is sleep: its milliseconds are accounted for
// without making the proof spend 31 real seconds on every red/mutation run.
function daemonCarrySource(): string {
  const source = fs.readFileSync(
    new URL("../packages/collector-cli/src/cli.ts", import.meta.url), "utf8",
  );
  const file = ts.createSourceFile("cli.ts", source, ts.ScriptTarget.Latest, true);
  let initializer: ts.Expression | undefined;
  const visit = (node: ts.Node) => {
    if (ts.isVariableDeclaration(node) &&
        ts.isIdentifier(node.name) && node.name.text === "persistSessionCarry") {
      assert.equal(initializer, undefined, "one daemon carry closure is expected");
      initializer = node.initializer;
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  assert.ok(initializer && ts.isArrowFunction(initializer), "daemon carry closure is missing");
  return initializer.getText(file);
}

const tenantId = "00000000-0000-4000-8000-000000000119";
const installKey = "session-carry-budget-proof";
const sessionIds = [1, 2, 3].map((n) =>
  `aaaaaaaa-aaaa-4aaa-8aaa-${String(n).padStart(12, "0")}`);
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId,
  installKey,
  delivery: { requestTimeoutSeconds: 1 },
});

async function main() {
  const fixture = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "session-carry-budget-"));
  const ledgerPath = path.join(fixture, "ledger.sqlite");
  const buffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: tenantId,
    databaseBusyTimeoutMs: 0,
    delivery: { enabled: true, limits: config.delivery },
  });
  let writer: Database.Database | undefined;
  try {
    buffer.database.pragma("busy_timeout = 0");
    saveDaemonSessionSyncState(buffer.database, emptyDaemonSessionSyncState());
    const event = aiInteractionEventSchema.parse({
      id: "00000000-0000-4000-8000-000000000119",
      sessionId: sessionIds[0],
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: new Date().toISOString(),
      actionClass: "other",
      inputTokens: 1,
      outputTokens: 1,
      ...nativeCodexFixture("session-carry-119"),
    });
    const admission = buffer.append(event, undefined, { integrityReceipt: true });
    assert.equal(admission.appended, true, JSON.stringify(admission));

    let totalWaitMs = 0;
    let phaseWaitMs = 0;
    let releaseAtMs: number | null = null;
    const sleep = async (milliseconds: number) => {
      totalWaitMs += milliseconds;
      phaseWaitMs += milliseconds;
      if (releaseAtMs !== null && phaseWaitMs >= releaseAtMs && writer?.inTransaction) {
        writer.exec("commit");
      }
    };
    class VirtualRetry extends SyncStorageRetryController {
      constructor() { super({ sleep }); }
    }
    const storageRetry = new VirtualRetry();
    let fetchCalls = 0;
    const upload = await uploadBufferedEvents(config, buffer, {
      includeLegacyRemainingUnuploaded: false,
      storageRetry,
      fetchImpl: (async (_request, init) => {
        fetchCalls += 1;
        const body = String(init?.body ?? "");
        return new Response(JSON.stringify(acceptedFixtureDelivery(body, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
    });
    assert.equal(upload.uploadedEvents, 1);
    assert.equal(fetchCalls, 1);

    writer = new Database(ledgerPath, { fileMustExist: true, timeout: 0 });
    const context: Record<string, unknown> = {
      buffer,
      sessionSyncState: emptyDaemonSessionSyncState(),
      pendingSessionIds: [],
      summaryCatchUp: false,
      storageRetry,
      SyncStorageRetryController: VirtualRetry,
      saveDaemonSessionSyncStateWithRetry,
      console: { warn: (_line: string) => undefined },
    };
    const persistSessionCarry = runInNewContext(
      `(${daemonCarrySource()})`, context,
    ) as () => Promise<boolean>;

    const started = performance.now();
    for (let index = 0; index < sessionIds.length; index += 1) {
      writer.exec("begin immediate");
      phaseWaitMs = 0;
      releaseAtMs = 9_975;
      context.pendingSessionIds = sessionIds.slice(0, index + 1);
      assert.equal(await persistSessionCarry(), true, `carry ${index + 1} must commit`);
      assert.equal(phaseWaitMs, 9_975, `carry ${index + 1} must encounter contention`);
      assert.equal(writer.inTransaction, false);
    }
    // The fourth busy write consumes only the pass's remaining retry budget.
    // It attempts the already durable state, so every pending ID must survive.
    writer.exec("begin immediate");
    phaseWaitMs = 0;
    releaseAtMs = null;
    context.pendingSessionIds = [...sessionIds];
    assert.equal(await persistSessionCarry(), false);
    assert.equal(context.summaryCatchUp, true);
    const elapsedMs = performance.now() - started;
    console.log(JSON.stringify({
      proof: "session_carry_retry_budget",
      totalLocalWaitMs: totalWaitMs,
      passBudgetMs: 31_000,
      successfulCarryWrites: 3,
      fetchCalls,
      elapsedMs: Number(elapsedMs.toFixed(1)),
    }));
    assert.ok(totalWaitMs <= 31_000, `one sync pass waited ${totalWaitMs} ms locally`);
    assert.ok(elapsedMs < 31_000, `proof wall wait exceeded the pass limit: ${elapsedMs} ms`);
    assert.equal(fetchCalls, 1, "storage retry must not replay the network send");

    writer.exec("rollback");
    writer.close();
    writer = undefined;
    buffer.close();
    const reopened = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
    try {
      assert.deepEqual(loadDaemonSessionSyncState(reopened.database).pendingSessionIds, sessionIds);
    } finally {
      reopened.close();
    }
    console.log("PASS: shared pass budget, durable pending IDs, one network send");
  } finally {
    if (writer?.inTransaction) writer.exec("rollback");
    writer?.close();
    if (buffer.database.open) buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
