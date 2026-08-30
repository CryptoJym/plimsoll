#!/usr/bin/env node
/**
 * Issue #196 proof: oversized compressed ingest rejections + frozen summary.
 *
 * Part A proves the HTTP boundary accepts storm-sized compressed bodies
 * (wire ceiling now equals the decoded ceiling) while every pre-existing
 * bomb/size guard still rejects. Part B proves the served snapshot advances
 * with ingestion even when the quiescent publication gate stays blocked,
 * without republishing inside the staleness bound and while failing open to
 * the cached copy when the writer is contended.
 *
 * Uses only a temporary Plimsoll home, SQLite ledger, and ephemeral loopback
 * port. Run: pnpm proof:ingest-liveness
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import zlib from "node:zlib";

import Database from "better-sqlite3";

import { guardProofCompletion } from "./lib/proof-completion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  HttpBoundaryRejection,
  LOCAL_HTTP_LIMITS,
  createRequestBudget,
  readBoundedRequestBody,
} from "../packages/collector-cli/src/http-boundary";
import { SNAPSHOT_MAX_STALENESS_MS } from "../packages/collector-cli/src/dashboard-projection";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const OLD_WIRE_CAP_BYTES = 256 * 1024;
// Tolerate running against pre-fix sources (the exported bound is absent).
const STALENESS_BOUND_MS = (SNAPSHOT_MAX_STALENESS_MS as number | undefined) ?? 15 * 60_000;
const checks: Array<{ name: string; passed: boolean; detail: unknown }> = [];
function check(name: string, condition: unknown, detail: unknown = {}) {
  const passed = Boolean(condition);
  checks.push({ name, passed, detail });
  console.log(`${passed ? "PASS" : "FAIL"} ${name} ${JSON.stringify(detail)}`);
}

type HttpResult = { status: number; body: Record<string, unknown> };
function request(
  port: number,
  route: string,
  body: Buffer | string,
  headers: Record<string, string> = {},
): Promise<HttpResult> {
    const bodyBuffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
    let responded = false;
    let settled = false;
    return new Promise((resolve, reject) => {
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method: "POST",
        headers: {
          connection: "close",
          "content-type": "application/json",
          "content-length": String(bodyBuffer.length),
          ...headers,
        },
      },
      (response) => {
        // Early rejections respond while the oversized upload is mid-flight;
        // stop feeding the socket so the response always completes cleanly.
        responded = true;
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          settled = true;
          let parsed: Record<string, unknown> = {};
          try {
            parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          } catch {}
          resolve({ status: response.statusCode ?? 0, body: parsed });
        });
      },
    );
    client.setTimeout(30_000, () => {
      if (!settled) client.destroy(new Error("ProofClientTimeout"));
    });
    // Once the server has responded (early 413s), a torn-down upload socket
    // is expected and must not fail the proof before the response completes.
    client.on("error", (error) => {
      if (!settled && !responded) reject(error);
    });
    const chunkSize = 64 * 1024;
    let offset = 0;
    const pump = () => {
      // An early-rejected multi-hundred-KB upload can lose its socket between
      // ticks; every socket touch here is best-effort so a pre-fix rejection
      // surfaces as a recorded failed check instead of a crashed proof.
      while (!responded && offset < bodyBuffer.length) {
        const chunk = bodyBuffer.subarray(offset, offset + chunkSize);
        offset += chunk.length;
        let writable = true;
        try {
          writable = client.write(chunk);
        } catch {
          return;
        }
        if (!writable) {
          client.once("drain", () => {
            try {
              if (!client.writableEnded && !client.destroyed) pump();
            } catch {}
          });
          return;
        }
      }
      try {
        if (!client.writableEnded && !client.destroyed) client.end();
      } catch {}
    };
    pump();
  });
}

function otlpLogBatch(recordCount: number, paddingBytes: number, perRecordRandom: boolean) {
  const sharedPadding = "x".repeat(paddingBytes);
  return JSON.stringify({
    resourceLogs: [{
      scopeLogs: [{
        logRecords: Array.from({ length: recordCount }, (_, index) => ({
          timeUnixNano: String(1_760_000_000_000_000_000n + BigInt(index)),
          attributes: [
            { key: "cwd", value: { stringValue: `/INGEST_LIVENESS_PRIVATE_CWD/${index}` } },
            { key: "gen_ai.usage.input_tokens", value: { intValue: "3" } },
            { key: "gen_ai.usage.output_tokens", value: { intValue: "5" } },
            {
              key: "proof.padding",
              value: {
                stringValue: (perRecordRandom
                  ? crypto.randomBytes(paddingBytes).toString("base64")
                  : sharedPadding) + index,
              },
            },
          ],
        })),
      }],
    }],
  });
}

let eventSequence = 1;

/** Drive the streaming-overflow branch of readBoundedRequestBody directly. */
async function readBoundedStreamOverflowRejection() {
  const { EventEmitter } = await import("node:events");
  const fakeRequest = new EventEmitter() as unknown as import("node:http").IncomingMessage;
  (fakeRequest as unknown as { headers: Record<string, string> }).headers = {};
  (fakeRequest as unknown as { resume: () => void }).resume = () => {};
  const budget = createRequestBudget();
  // Attach rejection handling before driving data so the boundary rejection
  // never lands in an unhandled window.
  const outcome: Promise<{ reason: string; status: number }> = readBoundedRequestBody(fakeRequest, budget).then(
    () => ({ reason: "no_rejection_raised", status: 0 }),
    (error: unknown) => {
      if (error instanceof HttpBoundaryRejection) return { reason: error.reason, status: error.status };
      throw error;
    },
  );
  const payload = Buffer.alloc(64 * 1024, "x");
  const totalChunks = Math.ceil((LOCAL_HTTP_LIMITS.compressedBodyBytes * 2) / payload.length);
  for (let index = 0; index < totalChunks; index += 1) {
    fakeRequest.emit("data", payload);
    await new Promise((resolve) => setImmediate(resolve));
  }
  return await outcome;
}

function liveEvent(observedAt: string, sessionId: string) {
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(eventSequence++).padStart(12, "0")}`,
    tenantId: "local",
    source: "codex" as const,
    dataMode: "metadata",
    eventType: "assistant_response" as const,
    observedAt,
    sessionId,
    actionClass: "other",
    inputTokens: 11,
    outputTokens: 7,
  });
}

function controlRow(db: LocalEventBuffer["database"]) {
  return db.prepare(
    `select generation,dirty,ready,last_success_at as lastSuccessAt,
      snapshot_builds as snapshotBuilds from dashboard_projection_control where singleton=1`,
  ).get() as {
    generation: number; dirty: number; ready: number;
    lastSuccessAt: string | null; snapshotBuilds: number;
  };
}

function settle(buffer: LocalEventBuffer, maxSlices = 200) {
  for (let slice = 0; slice < maxSlices; slice += 1) {
    const state = buffer.projection.status();
    if (
      state.ready && !state.dirty && state.backfill.complete &&
      state.backfill.parityComplete &&
      Object.values(state.backlog).every((value) => value === 0)
    ) return;
    buffer.projection.runMaintenance(new Date());
  }
  throw new Error(`projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}

// Refuses two silent-green failure modes: an early event-loop drain and a
// hang that never exits. See scripts/lib/proof-completion.ts.
const guard = guardProofCompletion({ countChecks: () => checks.length });

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-ingest-liveness-"));
  const previousHome = process.env.PLIMSOLL_HOME;
  process.env.PLIMSOLL_HOME = tempDir;
  const buffer = new LocalEventBuffer(path.join(tempDir, "ledger.sqlite"), {
    // Mirror the production listener: fail fast on writer contention instead
    // of inheriting better-sqlite3's five-second busy wait.
    databaseBusyTimeoutMs: 0,
  });
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;

    // ---------- Part A: boundary ----------
    // A1. Storm-sized gzip batch: wire bytes between the old 256 KiB cap and
    // the new cap, decoded well under the unchanged decoded ceiling. This is
    // the exact production rejection shape (compressed_body_too_large).
    const stormBody = zlib.gzipSync(otlpLogBatch(LOCAL_HTTP_LIMITS.otlpRecords, 4_608, true));
    check(
      "storm_batch_wire_size_between_old_and_new_cap",
      stormBody.length > OLD_WIRE_CAP_BYTES &&
        stormBody.length <= LOCAL_HTTP_LIMITS.compressedBodyBytes,
      { compressedBytes: stormBody.length, oldCap: OLD_WIRE_CAP_BYTES, newCap: LOCAL_HTTP_LIMITS.compressedBodyBytes },
    );
    const stormAccepted = await request(port, "/v1/logs", stormBody, {
      "x-plimsoll-source": "codex",
      "content-encoding": "gzip",
    });
    const stormFacts = buffer.database.prepare(
      `select count(*) as count, coalesce(sum(input_tokens),0) as inputTokens
       from buffered_events`,
    ).get() as { count: number; inputTokens: number };
    check(
      "storm_sized_gzip_batch_ingests_end_to_end",
      stormAccepted.status === 202 && stormAccepted.body.accepted === true &&
        stormAccepted.body.events === LOCAL_HTTP_LIMITS.otlpRecords &&
        stormFacts.count === LOCAL_HTTP_LIMITS.otlpRecords &&
        stormFacts.inputTokens === LOCAL_HTTP_LIMITS.otlpRecords * 3,
      { status: stormAccepted.status, body: stormAccepted.body, facts: stormFacts },
    );

    // A2. The wire ceiling itself is still enforced. Two deterministic paths:
    // (a) declared content-length above the cap is rejected before the upload
    // completes — the dominant production shape; (b) a body with no declared
    // length that streams past the cap mid-flight fails at the boundary.
    const overWire = await request(
      port,
      "/v1/logs",
      "",
      {
        "x-plimsoll-source": "codex",
        "content-length": String(LOCAL_HTTP_LIMITS.compressedBodyBytes + 1),
      },
    );
    check(
      "declared_oversize_wire_cap_still_rejects",
      overWire.status === 413 && overWire.body.reason === "compressed_body_too_large" &&
        overWire.body.error === "collector_request_rejected",
      { status: overWire.status, reason: overWire.body.reason },
    );
    const streamingRejection = await readBoundedStreamOverflowRejection();
    check(
      "streamed_oversize_wire_cap_rejects_mid_flight",
      streamingRejection.reason === "compressed_body_too_large" && streamingRejection.status === 413,
      streamingRejection,
    );

    // A3. Decompression-bomb guard unchanged (decoded ceiling).
    const bomb = zlib.gzipSync(JSON.stringify({
      resourceLogs: [{
        scopeLogs: [{
          logRecords: [{
            timeUnixNano: "1760000000000000000",
            attributes: [
              { key: "cwd", value: { stringValue: "/BOMB_CWD" } },
              { key: "proof.padding", value: { stringValue: "x".repeat(LOCAL_HTTP_LIMITS.decodedBodyBytes + 1) } },
            ],
          }],
        }],
      }],
    }));
    const bombResult = await request(port, "/v1/logs", bomb, {
      "x-plimsoll-source": "codex",
      "content-encoding": "gzip",
    });
    check(
      "decoded_ceiling_still_rejects_bombs",
      bombResult.status === 413 && bombResult.body.reason === "decoded_body_too_large",
      { status: bombResult.status, reason: bombResult.body.reason },
    );

    // A4. Compression-ratio guard unchanged (adversarial: decoded fits the
    // ceiling but expands >32x — a bomb that must not slip past the raised
    // wire cap).
    const highRatioBody = zlib.gzipSync("0".repeat(64 * 1024));
    assert.ok(highRatioBody.length < LOCAL_HTTP_LIMITS.compressedBodyBytes);
    const highRatio = await request(port, "/v1/logs", highRatioBody, {
      "x-plimsoll-source": "codex",
      "content-encoding": "gzip",
    });
    check(
      "compression_ratio_guard_survives_raised_wire_cap",
      highRatio.status === 413 && highRatio.body.reason === "compression_ratio_too_large",
      { status: highRatio.status, reason: highRatio.body.reason, compressed: highRatioBody.length },
    );

    // A5. Identity bodies between old and new caps were rejected before under
    // the same misleading reason; they must now flow.
    const identityBody = otlpLogBatch(96, 3_200, false);
    assert.ok(identityBody.length > OLD_WIRE_CAP_BYTES);
    const identityAccepted = await request(port, "/v1/logs", identityBody, {
      "x-plimsoll-source": "claude_code",
    });
    const afterIdentity = buffer.database.prepare(
      `select count(*) as count from buffered_events where source='claude_code'`,
    ).get() as { count: number };
    check(
      "identity_body_between_old_and_new_cap_accepted",
      identityAccepted.status === 202 && identityAccepted.body.accepted === true &&
        identityAccepted.body.events === 96 && afterIdentity.count === 96,
      { status: identityAccepted.status, reason: identityAccepted.body.reason, claudeCodeEvents: afterIdentity.count },
    );

    // ---------- Part B: summary liveness ----------
    settle(buffer);
    const settledControl = controlRow(buffer.database);
    assert.ok(settledControl.ready && !settledControl.dirty && settledControl.lastSuccessAt);

    // B1. Ingestion continues; background publication does not run.
    const firstNewObservedAt = new Date().toISOString();
    const appended = buffer.append(liveEvent(firstNewObservedAt, "11111111-2222-4333-8444-555555555555"));
    assert.ok(appended);
    const wedged = controlRow(buffer.database);
    check(
      "ingestion_marks_projection_dirty",
      wedged.dirty === 1,
      { dirty: wedged.dirty },
    );

    // B2. Adversarial gating: inside the freshness bound the read must stay
    // cache-only — no eager rebuild per request.
    const readsBefore = controlRow(buffer.database).snapshotBuilds;
    const cachedRead = buffer.projection.readSnapshot(30, []);
    const cachedSummary = cachedRead.kind === "ready" ? cachedRead.snapshot.summary : null;
    const readsAfterCached = controlRow(buffer.database).snapshotBuilds;
    check(
      "fresh_reads_stay_cache_only_inside_bound",
      cachedRead.kind === "ready" && cachedSummary !== null &&
        (cachedSummary as { totals?: { newest?: unknown } }).totals?.newest !== firstNewObservedAt &&
        readsAfterCached === readsBefore,
      {
        kind: cachedRead.kind,
        servedNewest: cachedSummary ? (cachedSummary as { totals?: { newest?: unknown } }).totals?.newest : null,
        expectedNewest: firstNewObservedAt,
        buildsBefore: readsBefore,
        buildsAfter: readsAfterCached,
      },
    );

    // B3. Past the staleness bound the same dirty read must republish and
    // serve the event that landed during the freeze.
    buffer.database.prepare(
      `update dashboard_projection_control set last_success_at=? where singleton=1`,
    ).run(new Date(Date.now() - STALENESS_BOUND_MS - 60_000).toISOString());
    const staleRead = buffer.projection.readSnapshot(30, []);
    const staleSummary = staleRead.kind === "ready" ? staleRead.snapshot.summary : null;
    const afterForce = controlRow(buffer.database);
    check(
      "stale_dirty_read_republishes_and_advances_newest",
      staleRead.kind === "ready" && staleSummary !== null &&
        (staleSummary as { totals?: { newest?: unknown } }).totals?.newest === firstNewObservedAt &&
        afterForce.generation === settledControl.generation + 1 &&
        afterForce.snapshotBuilds === readsAfterCached + 1 &&
        afterForce.dirty === 0,
      {
        kind: staleRead.kind,
        servedNewest: staleSummary ? (staleSummary as { totals?: { newest?: unknown } }).totals?.newest : null,
        expectedNewest: firstNewObservedAt,
        generation: afterForce.generation,
        previousGeneration: settledControl.generation,
        dirty: afterForce.dirty,
        buildsAfter: afterForce.snapshotBuilds,
        buildsExpected: readsAfterCached + 1,
      },
    );

    // B4. Production freeze mode: a permanently open backlog (sustained
    // ingestion churn keeps dirty sessions queued) plus fresh staleness must
    // STILL advance the surface — force bypasses only the backlog gate.
    buffer.append(liveEvent(new Date().toISOString(), "22222222-3333-4333-8444-666666666666"));
    buffer.database.prepare(
      `insert into dashboard_dirty_sessions (days,session_hash,reason,queued_at,revision,restart_revision)
       values (30,?,'proof_wedge',?,1,1)
       on conflict(days,session_hash) do update set revision=revision+1`,
    ).run("b".repeat(64), new Date().toISOString());
    const backlogBeforeStaleRead = buffer.projection.status().backlog.dirtySessions;
    buffer.database.prepare(
      `update dashboard_projection_control set last_success_at=? where singleton=1`,
    ).run(new Date(Date.now() - STALENESS_BOUND_MS - 60_000).toISOString());
    const secondNewObservedAt = (buffer.database.prepare(
      `select max(observed_at) as m from buffered_events`,
    ).get() as { m: string }).m;
    const wedgedRead = buffer.projection.readSnapshot(30, []);
    const wedgedSummary = wedgedRead.kind === "ready" ? wedgedRead.snapshot.summary : null;
    check(
      "open_backlog_no_longer_freezes_summary_past_bound",
      backlogBeforeStaleRead >= 1 && wedgedRead.kind === "ready" && wedgedSummary !== null &&
        (wedgedSummary as { totals?: { newest?: unknown } }).totals?.newest === secondNewObservedAt,
      {
        dirtySessionBacklogAtRead: backlogBeforeStaleRead,
        kind: wedgedRead.kind,
        servedNewestRaw: JSON.stringify((wedgedSummary as { totals?: { newest?: unknown } }).totals?.newest),
        expectedNewestRaw: JSON.stringify(secondNewObservedAt),
      },
    );

    // B5. Adversarial writer contention: forced republish fails open to the
    // cached copy instead of erroring the read or corrupting state. The
    // stale/dirty control state is written BEFORE the exclusive lock is taken.
    buffer.database.prepare(
      `update dashboard_projection_control set dirty=1,last_success_at=? where singleton=1`,
    ).run(new Date(Date.now() - STALENESS_BOUND_MS - 60_000).toISOString());
    const blocker = new Database(path.join(tempDir, "ledger.sqlite"));
    blocker.pragma("journal_mode = WAL");
    // BEGIN IMMEDIATE takes the writer lock while WAL readers proceed —
    // exactly the maintenance-child/listener overlap this lane targets.
    blocker.exec("begin immediate");
    const generationBeforeContention = controlRow(buffer.database).generation;
    let contendedRead: ReturnType<LocalEventBuffer["projection"]["readSnapshot"]>;
    try {
      contendedRead = buffer.projection.readSnapshot(30, []);
      const contendedControl = controlRow(buffer.database);
      check(
        "contended_writer_fails_open_to_cached_copy",
        contendedRead.kind === "ready" &&
          contendedControl.generation === generationBeforeContention,
        {
          kind: contendedRead.kind,
          generation: contendedControl.generation,
          generationBefore: generationBeforeContention,
        },
      );
    } finally {
      blocker.exec("commit");
      blocker.close();
    }

    // B6. HTTP end-to-end: /api/summary serves the advanced generation.
    settle(buffer);
    const finalGeneration = controlRow(buffer.database).generation;
    const summaryResponse = await new Promise<{ status: number; generation: string | string[] | undefined }>((resolve, reject) => {
      http.get(
        { host: "127.0.0.1", port, path: "/api/summary?days=30", headers: { connection: "close" } },
        (response) => {
          response.resume();
          response.on("end", () => resolve({
            status: response.statusCode ?? 0,
            generation: response.headers["x-plimsoll-projection-generation"],
          }));
        },
      ).on("error", reject);
    });
    check(
      "http_summary_serves_advanced_generation",
      summaryResponse.status === 200 &&
        String(summaryResponse.generation) === String(finalGeneration),
      { status: summaryResponse.status, headerGeneration: summaryResponse.generation, ledgerGeneration: finalGeneration },
    );
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    if (previousHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = previousHome;
    fs.rmSync(tempDir, { recursive: true, force: true });
  }

  const failed = checks.filter((result) => !result.passed);
  console.log(JSON.stringify({
    checks: checks.length,
    passed: checks.length - failed.length,
    failed: failed.length,
  }));
  if (failed.length > 0) process.exitCode = 1;
}

main().then(() => guard.complete()).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
