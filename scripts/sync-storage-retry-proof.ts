import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  LOCAL_HTTP_LIMITS,
  asHttpBoundaryRejection,
} from "../packages/collector-cli/src/http-boundary";
import {
  SQLITE_SYNC_RETRY_LIMITS,
  SyncStorageBusyError,
  SyncStorageRetryController,
  isSqliteContentionError,
} from "../packages/collector-cli/src/sqlite-contention";
import { DeliveryUploadError, uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { PLIMSOLL_VERSION } from "../packages/collector-cli/src/version";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-sync-storage-proof-"));
const holdMs = 180;
let ledgerIndex = 0;
let eventIndex = 0;
const checks: Array<{ name: string; detail?: unknown }> = [];
const cfg = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId: "00000000-0000-4000-8000-000000000001",
  installKey: "sync-storage-retry-proof",
  syncIntervalSeconds: 300,
  delivery: {
    maxOldestAgeDays: 3650,
    maxBackoffSeconds: 30,
    requestTimeoutSeconds: 1,
    maxProbesPerCycle: 8,
  },
});

function busy(code: string | number = "SQLITE_BUSY") {
  return Object.assign(new Error("database is locked"), { code });
}

function target(now?: () => Date) {
  return new LocalEventBuffer(path.join(root, `ledger-${++ledgerIndex}.sqlite`), {
    databaseBusyTimeoutMs: 0,
    workspaceId: cfg.tenantId,
    delivery: {
      enabled: true,
      limits: cfg.delivery,
      ...(now ? { now } : {}),
    },
  });
}

function event() {
  eventIndex += 1;
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(eventIndex).padStart(12, "0")}`,
    sessionId: `00000000-0000-4000-8001-${String(eventIndex).padStart(12, "0")}`,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: new Date(Date.UTC(2026, 8, 11, 14, 0, eventIndex)).toISOString(),
    actionClass: "other",
    inputTokens: 1,
    outputTokens: 1,
    metadata: { proof: "sync_storage_retry" },
  });
}

function controller(budgetMs = 1_000) {
  return new SyncStorageRetryController({ budgetMs, sleep: async () => undefined });
}

function injectBusy(targetObject: any, method: string, busyCall = 1) {
  const original = targetObject[method].bind(targetObject);
  let calls = 0;
  targetObject[method] = (...args: unknown[]) => {
    calls += 1;
    if (calls === busyCall) throw busy("SQLITE_BUSY_SNAPSHOT");
    return original(...args);
  };
  return () => calls;
}

function response(status: number, body: Record<string, unknown> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function fetchRecorder(statuses: number[], bodies: string[]) {
  let call = 0;
  return acknowledgingFetch(async (_input, init) => {
    const body = String(init?.body ?? "");
    bodies.push(body);
    const count = (JSON.parse(body) as { events: unknown[] }).events.length;
    const status = statuses[Math.min(call, statuses.length - 1)]!;
    call += 1;
    return response(status, status >= 200 && status < 300 ? { accepted: count } : {});
  });
}

async function check(name: string, run: () => Promise<unknown> | unknown) {
  await run();
  checks.push({ name });
}

function contractHash() {
  return `sha256:${crypto
    .createHash("sha256")
    .update(JSON.stringify([cfg.uploadUrl, cfg.tenantId, cfg.installKey, PLIMSOLL_VERSION]))
    .digest("hex")}`;
}

async function postSuccessBoundary(method: string, owner: "buffer" | "delivery" = "delivery") {
  const buffer = target();
  try {
    buffer.append(event());
    const bodies: string[] = [];
    const calls = injectBusy(owner === "buffer" ? buffer : buffer.delivery, method);
    const result = await uploadBufferedEvents(cfg, buffer, {
      includeLegacyRemainingUnuploaded: false,
      storageRetry: controller(),
      fetchImpl: fetchRecorder([200], bodies),
    });
    assert.equal(result.uploadedEvents, 1);
    assert.equal(calls(), 2, `${method} must retry only its local operation`);
    assert.equal(bodies.length, 1, `${method} must not replay HTTP`);
  } finally {
    buffer.close();
  }
}

async function postFailureBoundary(method: "retry" | "openCircuit", status: number) {
  const buffer = target();
  try {
    buffer.append(event());
    const bodies: string[] = [];
    const calls = injectBusy(buffer.delivery, method);
    await assert.rejects(
      uploadBufferedEvents(cfg, buffer, {
        includeLegacyRemainingUnuploaded: false,
        storageRetry: controller(),
        fetchImpl: fetchRecorder([status], bodies),
      }),
      DeliveryUploadError,
    );
    assert.equal(calls(), 2, `${method} must retry only its local operation`);
    assert.equal(bodies.length, 1, `${method} must not replay HTTP`);
  } finally {
    buffer.close();
  }
}

async function startWriter(ledgerPath: string) {
  const child = spawn(
    path.resolve("node_modules/.bin/tsx"),
    [path.resolve("scripts/fixtures/hold-maintenance-fill-writer.ts"), ledgerPath, String(holdMs)],
    { cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"] },
  );
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
  child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
  const deadline = Date.now() + 5_000;
  while (!stdout.includes("READY\n")) {
    if (child.exitCode !== null) throw new Error(`writer exited ${child.exitCode}: ${stderr}`);
    if (Date.now() >= deadline) throw new Error(`writer readiness timeout: ${stderr}`);
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
  return child;
}

async function waitForExit(child: ChildProcessWithoutNullStreams) {
  if (child.exitCode !== null) return child.exitCode;
  return new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", resolve);
  });
}

async function main() {
  await check("shared_predicate_includes_busy_locked_and_extended_codes", () => {
    for (const code of ["SQLITE_BUSY", "SQLITE_BUSY_SNAPSHOT", "SQLITE_LOCKED", "SQLITE_LOCKED_SHAREDCACHE", 5, 6, 517, 262]) {
      assert.equal(isSqliteContentionError(busy(code)), true, String(code));
    }
    for (const code of ["SQLITE_CONSTRAINT", "SQLITE_ERROR", 19, 1]) {
      assert.equal(isSqliteContentionError(busy(code)), false, String(code));
    }
    assert.equal(
      asHttpBoundaryRejection(busy("SQLITE_LOCKED_SHAREDCACHE")).reason,
      "storage_busy_retry",
    );
    assert.equal(LOCAL_HTTP_LIMITS.storageBusyRetryBudgetMs, 750);
    assert.equal(LOCAL_HTTP_LIMITS.requestDeadlineMs, 1_500);
    assert.ok(
      LOCAL_HTTP_LIMITS.storageBusyRetryBudgetMs < LOCAL_HTTP_LIMITS.requestDeadlineMs,
    );
  });

  await check("one_31000ms_wait_budget_exhausts_as_distinct_storage_busy", async () => {
    assert.deepEqual(SQLITE_SYNC_RETRY_LIMITS, {
      budgetMs: 31_000,
      initialDelayMs: 25,
      maxDelayMs: 100,
    });
    const retry = new SyncStorageRetryController({ sleep: async () => undefined });
    await assert.rejects(
      retry.run(() => { throw busy("SQLITE_BUSY_SNAPSHOT"); }),
      (error: unknown) => {
        assert.ok(error instanceof SyncStorageBusyError);
        assert.equal(error.waitMs, 31_000);
        assert.ok(error.retries > 0);
        return true;
      },
    );
  });

  await check("wait_budget_is_cumulative_across_atomic_operations", async () => {
    const retry = controller(50);
    let firstAttempts = 0;
    assert.equal(await retry.run(() => {
      firstAttempts += 1;
      if (firstAttempts === 1) throw busy();
      return "committed";
    }), "committed");
    await assert.rejects(
      retry.run(() => { throw busy("SQLITE_LOCKED_SHAREDCACHE"); }),
      (error: unknown) =>
        error instanceof SyncStorageBusyError && error.waitMs === 50 && error.retries === 2,
    );
  });

  await check("manual_upload_remains_fail_fast", async () => {
    const buffer = target();
    try {
      buffer.append(event());
      const calls = injectBusy(buffer, "useWorkspace");
      await assert.rejects(uploadBufferedEvents(cfg, buffer), isSqliteContentionError);
      assert.equal(calls(), 1);
    } finally {
      buffer.close();
    }
  });

  for (const [owner, method] of [
    ["buffer", "useWorkspace"],
    ["delivery", "configure"],
    ["delivery", "migrateLegacy"],
    ["delivery", "settleProvenValidationCandidates"],
    ["delivery", "lease"],
    ["delivery", "revalidateLeaseItems"],
    ["delivery", "acknowledge"],
    ["delivery", "clearCircuit"],
    ["delivery", "growValidationLeaseRows"],
  ] as const) {
    await check(`busy_${method}_retries_without_http_replay`, () => postSuccessBoundary(method, owner));
  }

  await check("busy_retry_settlement_does_not_replay_http", () => postFailureBoundary("retry", 500));
  await check("busy_circuit_write_does_not_replay_http", () => postFailureBoundary("openCircuit", 401));

  await check("busy_validation_candidate_write_does_not_replay_http", async () => {
    const buffer = target();
    try {
      buffer.append(event());
      const bodies: string[] = [];
      const calls = injectBusy(buffer.delivery, "markValidationCandidate");
      await assert.rejects(
        uploadBufferedEvents(cfg, buffer, {
          includeLegacyRemainingUnuploaded: false,
          storageRetry: controller(),
          maxProbes: 1,
          fetchImpl: fetchRecorder([400], bodies),
        }),
        DeliveryUploadError,
      );
      assert.equal(calls(), 2);
      assert.equal(bodies.length, 1);
    } finally {
      buffer.close();
    }
  });

  await check("busy_validation_bound_write_does_not_replay_http", async () => {
    const buffer = target();
    try {
      buffer.append(event());
      buffer.append(event());
      const bodies: string[] = [];
      const calls = injectBusy(buffer.delivery, "boundValidationLeaseRows");
      await assert.rejects(
        uploadBufferedEvents(cfg, buffer, {
          includeLegacyRemainingUnuploaded: false,
          storageRetry: controller(),
          maxProbes: 1,
          fetchImpl: fetchRecorder([400], bodies),
        }),
        DeliveryUploadError,
      );
      assert.equal(calls(), 2);
      assert.equal(bodies.length, 1);
    } finally {
      buffer.close();
    }
  });

  await check("busy_dead_letter_retries_without_replaying_any_probe", async () => {
    const buffer = target();
    try {
      buffer.append(event());
      buffer.append(event());
      const bodies: string[] = [];
      const calls = injectBusy(buffer.delivery, "deadLetterRemote");
      const result = await uploadBufferedEvents(cfg, buffer, {
        includeLegacyRemainingUnuploaded: false,
        storageRetry: controller(),
        maxProbes: 3,
        fetchImpl: fetchRecorder([400, 400, 200], bodies),
      });
      assert.equal(result.uploadedEvents, 1);
      assert.equal(calls(), 2);
      assert.equal(bodies.length, 3);
      assert.equal(new Set(bodies).size, 3, "every poison-isolation request body is sent once");
    } finally {
      buffer.close();
    }
  });

  for (const [method, busyCall] of [
    ["refreshValidationWitness", 1],
    ["settleProvenValidationCandidates", 2],
  ] as const) {
    await check(`busy_${method}_after_witness_http_does_not_replay`, async () => {
      const witnessAt = new Date("2100-09-11T14:30:00.000Z");
      const buffer = target(() => new Date(witnessAt));
      try {
        buffer.append(event());
        buffer.delivery.migrateLegacy({ now: witnessAt });
        const lease = buffer.delivery.lease({ now: witnessAt, leaseId: `witness-${method}` });
        assert.equal(lease.items.length, 1);
        buffer.delivery.refreshValidationWitness(contractHash(), lease.items[0]!, witnessAt);
        buffer.delivery.markValidationCandidate(
          lease.leaseId,
          lease.items[0]!.deliveryId,
          contractHash(),
          new Date(witnessAt.getTime() + 1),
        );
        const calls = injectBusy(buffer.delivery, method, busyCall);
        const bodies: string[] = [];
        await uploadBufferedEvents(cfg, buffer, {
          includeLegacyRemainingUnuploaded: false,
          storageRetry: controller(),
          now: () => new Date(witnessAt.getTime() + 2),
          fetchImpl: fetchRecorder([200], bodies),
        });
        assert.equal(calls(), busyCall + 1);
        assert.equal(bodies.length, 1);
      } finally {
        buffer.close();
      }
    });
  }

  await check("real_180ms_fill_writer_is_retried_and_acknowledged_within_interval", async () => {
    const ledgerPath = path.join(root, "real-contention.sqlite");
    const buffer = new LocalEventBuffer(ledgerPath, {
      databaseBusyTimeoutMs: 0,
      workspaceId: cfg.tenantId,
      delivery: { enabled: true, limits: cfg.delivery },
    });
    try {
      buffer.append(event());
      const bodies: string[] = [];
      const redWriter = await startWriter(ledgerPath);
      const redStarted = performance.now();
      let redError: unknown;
      try {
        await uploadBufferedEvents(cfg, buffer, {
          includeLegacyRemainingUnuploaded: false,
          fetchImpl: fetchRecorder([200], bodies),
        });
      } catch (error) {
        redError = error;
      }
      assert.equal(isSqliteContentionError(redError), true);
      assert.ok(performance.now() - redStarted < holdMs);
      assert.equal(await waitForExit(redWriter), 0);

      const greenWriter = await startWriter(ledgerPath);
      const storageRetry = new SyncStorageRetryController();
      const greenStarted = performance.now();
      const result = await uploadBufferedEvents(cfg, buffer, {
        includeLegacyRemainingUnuploaded: false,
        storageRetry,
        fetchImpl: fetchRecorder([200], bodies),
      });
      const elapsedMs = performance.now() - greenStarted;
      assert.equal(await waitForExit(greenWriter), 0);
      assert.equal(result.uploadedEvents, 1);
      assert.equal(buffer.delivery.status().remainingDelivery, 0);
      assert.ok(storageRetry.receipt().retries > 0);
      assert.ok(elapsedMs >= holdMs - 25 && elapsedMs < cfg.syncIntervalSeconds * 1_000);
      assert.equal(bodies.length, 1, "the failed pre-storage attempt reaches no HTTP endpoint");
      checks.push({
        name: "real_contention_timing",
        detail: { holdMs, elapsedMs: Number(elapsedMs.toFixed(3)), ...storageRetry.receipt() },
      });
    } finally {
      buffer.close();
    }
  });

  await check("daemon_classifies_storage_busy_without_failure_backoff", () => {
    const source = fs.readFileSync(
      new URL("../packages/collector-cli/src/cli.ts", import.meta.url),
      "utf8",
    );
    const start = source.indexOf("    const runSync = async () => {");
    const end = source.indexOf("\n    // First boot records", start);
    assert.notEqual(start, -1);
    assert.notEqual(end, -1);
    const runSync = source.slice(start, end);
    assert.match(runSync, /new SyncStorageRetryController\(\)/);
    assert.match(runSync, /storageRetry/);
    const busyBranch = runSync.indexOf("error instanceof SyncStorageBusyError");
    const failureIncrement = runSync.indexOf("syncFailureStreak += 1");
    assert.ok(busyBranch >= 0 && busyBranch < failureIncrement);
    assert.match(runSync.slice(busyBranch, failureIncrement), /warning:\s*"sync_storage_busy"/);
    assert.match(runSync.slice(busyBranch, failureIncrement), /syncSkipUntil\s*=\s*0/);
    assert.doesNotMatch(runSync.slice(busyBranch, failureIncrement), /syncFailureStreak\s*[+]?=/);
    assert.match(runSync.slice(failureIncrement), /warning:\s*"sync_failed"/);
    assert.match(runSync.slice(failureIncrement), /2 \*\* Math\.min\(syncFailureStreak, 4\)/);
  });

  console.log(JSON.stringify({
    proof: "sync_storage_retry",
    passed: true,
    checks: checks.length,
    details: checks.filter((entry) => entry.detail),
    liveStateTouched: false,
    providerNetworkCalled: false,
  }, null, 2));
}

void main().finally(() => fs.rmSync(root, { recursive: true, force: true }));
