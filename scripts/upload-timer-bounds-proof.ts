import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { PLIMSOLL_VERSION } from "../packages/collector-cli/src/version";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";

type Check = { name: string; passed: boolean; detail?: string };

const checks: Check[] = [];
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-upload-timer-bounds-"));
let ledgerIndex = 0;
let eventIndex = 0;

const cfg = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId: "00000000-0000-4000-8000-000000000001",
  installKey: "upload-timer-bounds-proof",
  delivery: {
    maxOldestAgeDays: 3650,
    maxBackoffSeconds: 30,
    requestTimeoutSeconds: 1,
    maxProbesPerCycle: 1,
  },
});

function buffer() {
  return new LocalEventBuffer(path.join(root, `ledger-${++ledgerIndex}.sqlite`), {
    workspaceId: cfg.tenantId,
    delivery: { enabled: true, limits: cfg.delivery },
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
    observedAt: new Date(Date.UTC(2026, 8, 9, 4, 0, eventIndex)).toISOString(),
    actionClass: "other",
    inputTokens: 1,
    outputTokens: 1,
    metadata: { proof: "upload_timer_bounds" },
  });
}

function response(status: number, body: Record<string, unknown> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function positiveFetch(onCall?: () => void): typeof fetch {
  return acknowledgingFetch(async (_input, init) => {
    onCall?.();
    const body = JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] };
    return response(200, { accepted: body.events?.length ?? 0 });
  });
}

function rejectLegacyStats(target: LocalEventBuffer) {
  Object.defineProperty(target, "stats", {
    configurable: true,
    value: () => {
      throw new Error("legacy_raw_stats_called");
    },
  });
}

async function check(name: string, run: () => void | Promise<void>) {
  try {
    await run();
    checks.push({ name, passed: true });
  } catch (error) {
    checks.push({
      name,
      passed: false,
      detail: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
    });
  }
}

async function main() {
  await check("default_manual_result_preserves_exact_numeric_raw_count", async () => {
    const target = buffer();
    try {
      target.append(event());
      target.delivery.openCircuit("auth_blocked", new Date("2026-09-09T04:01:00.000Z"));
      let httpCalls = 0;
      const result = await uploadBufferedEvents(cfg, target, {
        fetchImpl: async () => {
          httpCalls += 1;
          return response(500);
        },
        now: () => new Date("2026-09-09T04:01:01.000Z"),
      });
      assert.equal(result.remainingUnuploaded, 1);
      assert.equal(typeof result.remainingUnuploaded, "number");
      assert.equal(result.remainingDelivery, 1);
      assert.equal(httpCalls, 0);
    } finally {
      target.close();
    }
  });

  await check("bounded_empty_lease_skips_raw_stats_and_http", async () => {
    const target = buffer();
    try {
      rejectLegacyStats(target);
      let httpCalls = 0;
      const result = await uploadBufferedEvents(cfg, target, {
        includeLegacyRemainingUnuploaded: false,
        fetchImpl: async () => {
          httpCalls += 1;
          return response(500);
        },
        now: () => new Date("2026-09-09T04:02:00.000Z"),
      });
      assert.equal(result.remainingUnuploaded, null);
      assert.equal(result.remainingDelivery, 0);
      assert.equal(result.uploadedEvents, 0);
      assert.equal(httpCalls, 0);
    } finally {
      target.close();
    }
  });

  await check("bounded_normal_ack_skips_raw_stats", async () => {
    const target = buffer();
    try {
      target.append(event());
      rejectLegacyStats(target);
      const now = new Date(Date.now() + 60_000);
      let httpCalls = 0;
      const result = await uploadBufferedEvents(cfg, target, {
        includeLegacyRemainingUnuploaded: false,
        fetchImpl: positiveFetch(() => {
          httpCalls += 1;
        }),
        now: () => now,
      });
      assert.equal(result.remainingUnuploaded, null);
      assert.equal(result.remainingDelivery, target.delivery.status(now).remainingDelivery);
      assert.equal(result.uploadedEvents, 1);
      assert.equal(result.markedUploaded, 1);
      assert.equal(httpCalls, 1);
    } finally {
      target.close();
    }
  });

  await check("bounded_witness_return_skips_raw_stats", async () => {
    const target = buffer();
    try {
      target.append(event());
      const witnessBase = Date.now() + 60_000;
      const witnessedAt = new Date(witnessBase);
      const failedAt = new Date(witnessBase + 1_000);
      target.delivery.migrateLegacy({ now: witnessedAt });
      const lease = target.delivery.lease({
        maxRows: 1,
        now: witnessedAt,
        leaseId: "upload-timer-bounds-witness",
      });
      assert.equal(lease.items.length, 1, "fixture lease must expose one row");
      const contractHash = `sha256:${crypto
        .createHash("sha256")
        .update(JSON.stringify([cfg.uploadUrl, cfg.tenantId, cfg.installKey, PLIMSOLL_VERSION]))
        .digest("hex")}`;
      assert.equal(
        target.delivery.refreshValidationWitness(contractHash, lease.items[0], witnessedAt),
        1,
        "fixture witness must persist",
      );
      assert.equal(
        target.delivery.markValidationCandidate(
          lease.leaseId,
          lease.items[0].deliveryId,
          contractHash,
          failedAt,
        ),
        1,
        "fixture validation candidate must persist",
      );
      const durable = target.database
        .prepare(
          `select
             (select count(*) from upload_validation_witness) as witnesses,
             (select count(*) from upload_validation_candidates) as candidates`,
        )
        .get() as { witnesses: number; candidates: number };
      assert.deepEqual(durable, { witnesses: 1, candidates: 1 });

      rejectLegacyStats(target);
      let httpCalls = 0;
      const result = await uploadBufferedEvents(cfg, target, {
        includeLegacyRemainingUnuploaded: false,
        fetchImpl: positiveFetch(() => {
          httpCalls += 1;
        }),
        limit: 1,
        maxProbes: 1,
        now: () => new Date(witnessBase + 2_000),
      });
      assert.equal(result.remainingUnuploaded, null);
      assert.equal(result.remainingDelivery, 0);
      assert.equal(result.uploadedEvents, 0);
      assert.ok("rootLeaseEvents" in result.delivery);
      assert.equal(result.delivery.rootLeaseEvents, 0);
      assert.equal(result.delivery.attempts, 1);
      assert.equal(httpCalls, 1);
    } finally {
      target.close();
    }
  });

  await check("daemon_run_sync_passes_bounded_result_and_storage_retry_options", () => {
    const source = fs.readFileSync(
      new URL("../packages/collector-cli/src/cli.ts", import.meta.url),
      "utf8",
    );
    const start = source.indexOf("    const runSync = async () => {");
    const end = source.indexOf("\n    // First boot records", start);
    assert.notEqual(start, -1);
    assert.notEqual(end, -1);
    const runSync = source.slice(start, end);
    assert.match(
      runSync,
      /await uploadBufferedEvents\(\s*config,\s*buffer,\s*\{\s*includeLegacyRemainingUnuploaded:\s*false,\s*storageRetry,\s*\}\s*\)/,
    );
    const manualStart = source.indexOf('  if (command === "upload") {');
    const manualEnd = source.indexOf('  if (command === "upload-history") {', manualStart);
    assert.notEqual(manualStart, -1);
    assert.notEqual(manualEnd, -1);
    assert.doesNotMatch(
      source.slice(manualStart, manualEnd),
      /includeLegacyRemainingUnuploaded|storageRetry/,
    );
  });

  const failed = checks.filter((entry) => !entry.passed);
  console.log(
    JSON.stringify(
      {
        schema: "plimsoll.upload-timer-bounds-proof/v1",
        status: failed.length === 0 ? "pass" : "fail",
        checks,
        liveStateTouched: false,
        providerNetworkCalled: false,
      },
      null,
      2,
    ),
  );
  if (failed.length > 0) process.exitCode = 1;
}

void main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });
