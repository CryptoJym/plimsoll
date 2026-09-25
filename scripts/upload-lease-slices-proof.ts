/**
 * eco-6hoxj.163.24 (round 2): an upload batch claimed and acknowledged in
 * 125-row slices keeps the delivery guarantees of the single lease it replaced.
 *
 * Scenarios from the independent review's fault harness (slice-faults.mts):
 *   S1 the wall clock jumps past the 120 s lease between two lease slices
 *      (sleep, a clock step, a long stall): the batch must upload once, not
 *      re-claim its own rows and refuse itself as a remote contract failure;
 *   S2 a failure between acknowledge slices: settled rows stay settled, the
 *      rest upload once after their lease expires;
 *   S3 a failure between lease slices: the claimed slice waits one lease,
 *      nothing is lost or duplicated;
 *   S4 the request byte budget holds across slices;
 *   S5 a first slice of locally dead rows still leads to the live rows.
 *
 *   pnpm proof:upload-lease-slices
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import type Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const root = process.env.TMPDIR;
assert.ok(root && fs.realpathSync(root) === root && root.startsWith(process.env.HOME + path.sep),
  "run under the CI layout: TMPDIR inside the synthetic HOME");
const dir = fs.mkdtempSync(path.join(root, "upload-lease-slices-"));
const installKey = "upload-lease-slices-install";
// The upload clock runs an hour after the events, and the events an hour
// ahead of the real clock: a ledger refuses events observed before it was
// enrolled (when it opened).
const baseMs = Date.now() + 2 * 3_600_000;
let ledgerNumber = 0;
let eventNumber = 1;

type Check = { name: string; passed: boolean; detail?: unknown };
const checks: Check[] = [];

async function check(name: string, run: () => unknown | Promise<unknown>) {
  try {
    const detail = await run();
    checks.push({ name, passed: true, ...(detail === undefined ? {} : { detail }) });
  } catch (error) {
    checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
  const last = checks.at(-1)!;
  console.log(JSON.stringify({ check: name, passed: last.passed, ...(last.passed ? {} : { detail: last.detail }) }));
}

const config = (delivery: Record<string, number> = {}) => collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId: "00000000-0000-4000-8000-000000000c01", installKey,
  delivery: { maxOldestAgeDays: 3650, maxBackoffSeconds: 30, requestTimeoutSeconds: 5, ...delivery },
});

function event(pad = 0) {
  const n = eventNumber++;
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`,
    sessionId: `00000000-0000-4000-8001-${String(n % 7).padStart(12, "0")}`,
    source: "codex", dataMode: "metadata", eventType: "otel_span", actionClass: "other",
    observedAt: new Date(baseMs - 3_600_000 + n * 10).toISOString(),
    metadata: pad ? { otelEventName: "codex.sse_event", note: "n".repeat(pad) } : { otelEventName: "codex.sse_event" },
  });
}

function ledger(cfg = config(), file = path.join(dir, `ledger-${++ledgerNumber}.sqlite`)) {
  return { cfg, file, buffer: new LocalEventBuffer(file, { workspaceId: cfg.tenantId, delivery: { enabled: true, limits: cfg.delivery } }) };
}

/** An acknowledging cloud that records each request's item ids and size. */
function cloud(requests: string[][], bytes: number[] = []): typeof fetch {
  return (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const body = String(init?.body ?? "");
    bytes.push(Buffer.byteLength(body));
    const parsed = JSON.parse(body) as { events?: Array<{ event?: { id?: string } }> };
    requests.push((parsed.events ?? []).map((entry) => entry.event?.id ?? ""));
    return new Response(JSON.stringify(acceptedFixtureDelivery(body, installKey)), {
      status: 200, headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
}

const outbox = (db: Database.Database) => Object.fromEntries((db.prepare(
  `select state, count(*) as n from upload_outbox group by state`).all() as Array<{ state: string; n: number }>)
  .map((row) => [row.state, row.n]));
const acknowledged = (db: Database.Database) => db.prepare(
  `select count(*) as n, count(distinct delivery_id) as distinctIds from upload_receipts where reason = 'remote_acknowledged'`,
).get() as { n: number; distinctIds: number };
const uploadedRaw = (db: Database.Database) => (db.prepare(
  `select count(*) as n from buffered_events where uploaded_at is not null`).get() as { n: number }).n;
const circuit = (db: Database.Database) => (db.prepare(
  `select circuit_kind as kind from upload_control where singleton = 1`).get() as { kind: string }).kind;
const duplicatesWithinRequests = (requests: string[][]) =>
  requests.reduce((sum, ids) => sum + ids.length - new Set(ids).size, 0);

async function main() {
  await check("s1_clock_jump_between_lease_slices_uploads_the_batch_once", async () => {
    const { cfg, buffer } = ledger();
    try {
      for (let index = 0; index < 125; index += 1) buffer.append(event());
      let clock = baseMs;
      const lease = buffer.delivery.lease.bind(buffer.delivery);
      let leaseCalls = 0;
      buffer.delivery.lease = (options) => {
        const result = lease(options);
        leaseCalls += 1;
        if (leaseCalls === 1) clock += 130_000;
        return result;
      };
      const requests: string[][] = [];
      const result = await uploadBufferedEvents(cfg, buffer, {
        fetchImpl: cloud(requests), now: () => new Date(clock), includeLegacyRemainingUnuploaded: false,
      });
      assert.equal(result.uploadedEvents, 125, "the batch did not upload");
      assert.equal(requests.length, 1);
      assert.equal(new Set(requests[0]).size, 125);
      assert.equal(duplicatesWithinRequests(requests), 0);
      assert.equal(circuit(buffer.database), "none", "a local re-claim opened a host-wide circuit");
      assert.deepEqual(outbox(buffer.database), {});
      assert.equal(acknowledged(buffer.database).distinctIds, 125);
      return { leaseCalls, uploadedEvents: result.uploadedEvents };
    } finally {
      buffer.close();
    }
  });

  await check("s2_failure_between_acknowledge_slices_settles_everything_once", async () => {
    const { cfg, buffer, file } = ledger();
    for (let index = 0; index < 400; index += 1) buffer.append(event());
    const acknowledge = buffer.delivery.acknowledge.bind(buffer.delivery);
    let ackCalls = 0;
    buffer.delivery.acknowledge = ((...args: Parameters<typeof acknowledge>) => {
      ackCalls += 1;
      if (ackCalls === 2) throw new Error("injected_failure_between_ack_slices");
      return acknowledge(...args);
    }) as typeof acknowledge;
    const requests: string[][] = [];
    await assert.rejects(uploadBufferedEvents(cfg, buffer, {
      fetchImpl: cloud(requests), now: () => new Date(baseMs), includeLegacyRemainingUnuploaded: false,
    }), /injected_failure_between_ack_slices/);
    assert.equal(acknowledged(buffer.database).n, 125, "the first acknowledge slice did not stay settled");
    assert.equal(outbox(buffer.database).in_flight, 275);
    buffer.close();
    const reopened = ledger(cfg, file).buffer;
    try {
      const beforeExpiry = await uploadBufferedEvents(cfg, reopened, {
        fetchImpl: cloud(requests), now: () => new Date(baseMs + 60_000), includeLegacyRemainingUnuploaded: false,
      });
      const afterExpiry = await uploadBufferedEvents(cfg, reopened, {
        fetchImpl: cloud(requests), now: () => new Date(baseMs + 121_000), includeLegacyRemainingUnuploaded: false,
      });
      assert.equal(beforeExpiry.uploadedEvents, 0, "leased rows were re-sent before their lease expired");
      assert.equal(afterExpiry.uploadedEvents, 275);
      assert.deepEqual(acknowledged(reopened.database), { n: 400, distinctIds: 400 });
      assert.equal(uploadedRaw(reopened.database), 400);
      assert.deepEqual(outbox(reopened.database), {});
      assert.equal(duplicatesWithinRequests(requests), 0);
      return { requests: requests.length, items: requests.flat().length };
    } finally {
      reopened.close();
    }
  });

  await check("s3_failure_between_lease_slices_loses_and_duplicates_nothing", async () => {
    const { cfg, buffer } = ledger();
    try {
      for (let index = 0; index < 300; index += 1) buffer.append(event());
      const lease = buffer.delivery.lease.bind(buffer.delivery);
      let leaseCalls = 0;
      buffer.delivery.lease = (options) => {
        leaseCalls += 1;
        if (leaseCalls === 2) throw new Error("injected_failure_between_lease_slices");
        return lease(options);
      };
      const requests: string[][] = [];
      await assert.rejects(uploadBufferedEvents(cfg, buffer, {
        fetchImpl: cloud(requests), now: () => new Date(baseMs), includeLegacyRemainingUnuploaded: false,
      }), /injected_failure_between_lease_slices/);
      buffer.delivery.lease = lease;
      assert.equal(outbox(buffer.database).in_flight, 125, "the claimed slice is not held by its lease");
      const immediate = await uploadBufferedEvents(cfg, buffer, {
        fetchImpl: cloud(requests), now: () => new Date(baseMs + 1_000), includeLegacyRemainingUnuploaded: false,
      });
      const expired = await uploadBufferedEvents(cfg, buffer, {
        fetchImpl: cloud(requests), now: () => new Date(baseMs + 121_000), includeLegacyRemainingUnuploaded: false,
      });
      assert.equal(immediate.uploadedEvents, 175);
      assert.equal(expired.uploadedEvents, 125);
      assert.deepEqual(acknowledged(buffer.database), { n: 300, distinctIds: 300 });
      assert.deepEqual(outbox(buffer.database), {});
      assert.equal(new Set(requests.flat()).size, requests.flat().length, "an event was sent twice");
      return { requests: requests.length };
    } finally {
      buffer.close();
    }
  });

  await check("s4_request_byte_budget_holds_across_slices", async () => {
    const { cfg, buffer } = ledger(config({ maxItemBytes: 64 * 1024 }));
    try {
      for (let index = 0; index < 500; index += 1) buffer.append(event(5_000));
      const requests: string[][] = [];
      const bytes: number[] = [];
      let uploaded = 0;
      for (let cycle = 0; cycle < 10; cycle += 1) {
        const result = await uploadBufferedEvents(cfg, buffer, {
          fetchImpl: cloud(requests, bytes), now: () => new Date(baseMs), includeLegacyRemainingUnuploaded: false,
          maxBytes: 400_000,
        });
        uploaded += result.uploadedEvents;
        if (result.uploadedEvents === 0) break;
      }
      assert.ok(bytes.every((size) => size <= 400_000), `a request exceeded its budget: ${Math.max(...bytes)}`);
      assert.equal(uploaded, 500);
      assert.equal(requests.flat().length, 500);
      assert.equal(new Set(requests.flat()).size, 500);
      return { requests: requests.length, largestRequestBytes: Math.max(...bytes) };
    } finally {
      buffer.close();
    }
  });

  await check("s5_a_dead_first_slice_still_leads_to_the_live_rows", async () => {
    const { cfg, buffer } = ledger();
    try {
      for (let index = 0; index < 130; index += 1) buffer.append(event());
      const db = buffer.database;
      const firstRaw = (db.prepare(`select raw_rowid as rowid from upload_outbox
        order by created_at, delivery_id limit 125`).all() as Array<{ rowid: number }>).map((row) => row.rowid);
      const remove = db.prepare(`delete from buffered_events where rowid = ?`);
      for (const rowid of firstRaw) remove.run(rowid);
      const requests: string[][] = [];
      const result = await uploadBufferedEvents(cfg, buffer, {
        fetchImpl: cloud(requests), now: () => new Date(baseMs), includeLegacyRemainingUnuploaded: false,
      });
      assert.equal(result.uploadedEvents, 5);
      assert.equal(result.delivery.deadLetters, 125);
      assert.deepEqual(outbox(db), {});
      return { uploadedEvents: result.uploadedEvents, deadLetters: result.delivery.deadLetters };
    } finally {
      buffer.close();
    }
  });

  fs.rmSync(dir, { recursive: true, force: true });
  const failed = checks.filter((entry) => !entry.passed);
  console.log(JSON.stringify({
    proof: "upload-lease-slices",
    bead: "eco-6hoxj.163.24",
    passed: failed.length === 0,
    checks: checks.length,
    failures: failed.map((entry) => ({ name: entry.name, detail: entry.detail })),
  }, null, 2));
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
