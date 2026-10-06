import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { withReader } from "./lib/legacy-reader";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("codex-post-upgrade-migration", 8);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "post-upgrade-migration-"));
const AT = Date.now() - 600_000;
const NOW = Date.now() + 300_000;
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const config = collectorConfigSchema.parse({ tenantId: uuid(999), deviceId: "post-upgrade-fixture",
  uploadUrl: "http://127.0.0.1:49777/ingest", installKey: "post-upgrade-fixture-key" });
const options = { workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(AT - 3_600_000),
  delivery: { enabled: false, now: () => new Date(NOW) } };
const leaseAt = new Date(NOW + 121_001);
const attr = (key: string, value: string | number) => ({ key, value: typeof value === "number"
  ? { intValue: String(value) } : { stringValue: value } });
function native(n: number, input: number, output?: number, model: string | null = "gpt-6.1-sol") {
  const attributes = [attr("session.id", uuid(900 + n)), attr("request_id", `response-${n}`),
    attr("gen_ai.usage.input_tokens", input), ...(output === undefined ? [] : [attr("gen_ai.usage.output_tokens", output)]),
    ...(model === null ? [] : [attr("gen_ai.request.model", model)])];
  const traceId = n.toString(16).padStart(32, "d");
  const resource = { attributes: [attr("service.name", "codex-cli")] };
  const payload = model === null
    ? { resourceSpans: [{ resource, scopeSpans: [{ spans: [{ traceId, spanId: n.toString(16).padStart(16, "a"),
      name: "handle_responses", kind: 1, startTimeUnixNano: String(BigInt(AT + n * 1000) * 1_000_000n),
      endTimeUnixNano: String(BigInt(AT + n * 1000 + 20) * 1_000_000n), attributes }] }] }] }
    : { resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{ traceId,
      timeUnixNano: String(BigInt(AT + n * 1000) * 1_000_000n),
      attributes: [attr("event.name", "codex.sse_event"), ...attributes] }] }] }] };
  const events = explodeOtlpPayload(payload, { source: "codex", resolveGit: false }).events;
  assert.equal(events.length, 1);
  return aiInteractionEventSchema.parse({ ...events[0]!.event, id: uuid(n) });
}
function bare(n: number) {
  return aiInteractionEventSchema.parse({ id: uuid(n), sessionId: uuid(800 + n), source: "codex", dataMode: "metadata",
    eventType: "assistant_response", observedAt: new Date(AT + n * 1000).toISOString(), model: "gpt-6.1-sol",
    inputTokens: 19, outputTokens: 2, metadata: {} });
}
function shapes(): AiInteractionEvent[] {
  return [native(100, 23, 3), native(101, 0, 0), native(102, 19, 2, null), native(103, 11),
    aiInteractionEventSchema.parse({ ...bare(104), source: "claude_code", model: "claude-sonnet-4-5",
      inputTokens: 7, outputTokens: 1 }), native(105, 31, 4), native(106, 5, 1)];
}
const rawSnapshot = (b: any, boundary: number) => JSON.stringify(b.database.prepare(
  "select rowid as raw_rowid,* from buffered_events where rowid<=? order by rowid").all(boundary));
const count = (b: any, table: string) => b.database.prepare(`select count(*) as n from ${table}`).get().n;
function historicalControl(b: any) {
  return b.database.prepare("select migration_cursor_rowid as cursor,migration_complete as complete from upload_control where singleton=1").get();
}
function newControl(b: any) {
  return b.database.prepare("select * from upload_new_row_migration where singleton=1").get();
}
function heldHistory(b: any, before: string, boundary: number, cursor = 0) {
  assert.equal(rawSnapshot(b, boundary), before, "every historical raw column must stay byte-identical");
  assert.equal(historicalControl(b).cursor, cursor, "new work cannot advance historical migration");
  const control = newControl(b);
  assert.equal(control.boundary_rowid, boundary);
  assert(control.cursor_rowid >= boundary, "new-row cursor cannot enter historical rows");
}
function seed(Old: any, file: string, complete = false, oldEvent: AiInteractionEvent = bare(1), nullReceipt = false) {
  const b = new Old(file, { ...options, delivery: { ...options.delivery, now: () => new Date(AT + 5000) } });
  try {
    assert(b.append(oldEvent));
    b.database.prepare(`update upload_control set migration_complete=?,migration_cursor_rowid=?,
      privacy_migration_version=1 where singleton=1`).run(complete ? 1 : 0, complete ? 1 : 0);
    if (nullReceipt) {
      const created = b.database.prepare("select created_at as n from buffered_events where id=?").get(oldEvent.id).n;
      b.database.prepare(`insert into upload_receipts(delivery_id,terminal_state,reason,status_class,
        attempt_count,created_at,terminal_at) values (?,'dead','remote_validation_rejected','remote_validation',1,?,?)`)
        .run(oldEvent.id, created, created);
    }
    return rawSnapshot(b, 1);
  } finally { b.close(); }
}
// One small ledger clone at a time. Copy after the real head appended its new
// rows so random raw generations, timestamps and install identity are identical.
// The actual pre-guard reader must then lease every item field unchanged.
async function baseline(b: LocalEventBuffer, Base: any, newIds: Set<string>) {
  const copy = path.join(root, "baseline.sqlite");
  assert(!fs.existsSync(copy));
  const free = fs.statfsSync(root);
  if (free.bavail * free.bsize < 20 * 1024 ** 3) console.warn("fixture clone disk notice: less than 20 GiB free");
  await b.database.backup(copy);
  const old = new Base(copy, { ...options, delivery: { ...options.delivery, enabled: true } });
  try {
    for (let turn = 0; turn < 20; turn++) {
      const result = old.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 32, maxWriterMs: 1000 });
      if (result.complete) break;
      assert(turn < 19, "pre-guard fixture migration must finish");
    }
    return old.delivery.lease({ now: leaseAt }).items.filter((item: any) => newIds.has(item.rawId));
  } finally {
    old.close();
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(copy + suffix, { force: true });
  }
}
async function noPendingUpload(b: LocalEventBuffer) {
  let calls = 0;
  await uploadBufferedEvents(config, b, { now: () => leaseAt, developmentLoopbackUrl: true,
    includeLegacyRemainingUnuploaded: false, fetchImpl: async () => { calls++; throw new Error("unexpected duplicate transport"); } });
  assert.equal(calls, 0);
}
async function directed(Old: any, Base: any, file: string, complete = false) {
  const before = seed(Old, file, complete);
  const b: any = new LocalEventBuffer(file, options);
  try {
    assert.equal(rawSnapshot(b, 1), before, "first head open must not mutate history");
    assert(b.append(native(100, 23, 3)));
    const expected = await baseline(b, Base, new Set([uuid(100)]));
    assert.equal(expected.length, 1, "cd94758e leases the genuinely new response");
    b.delivery.configure({ enabled: true });
    const migration = b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 32, maxWriterMs: 1000 });
    const lease = b.delivery.lease({ now: leaseAt });
    assert.deepEqual(lease.items, expected, "new lease must match cd94758e field by field");
    assert.equal(lease.items[0]!.envelope.event.model, "gpt-6.1-sol");
    assert.equal(lease.items[0]!.envelope.event.inputTokens, 23);
    assert.equal(lease.items[0]!.envelope.event.outputTokens, 3);
    if (!complete) {
      assert.equal(migration.complete, false, "historical hold cannot claim completed migration");
      assert.equal(migration.paused, "historical_migration_requires_opt_in");
      heldHistory(b, before, 1);
    }
    assert.equal(b.delivery.acknowledge(lease.leaseId, [uuid(100)], leaseAt).acknowledged, 1);
    assert.notEqual(b.database.prepare("select uploaded_at from buffered_events where id=?").get(uuid(100))!.uploaded_at, null);
    await noPendingUpload(b);
    for (let turn = 0; turn < 5; turn++) {
      b.projection.runMaintenance(leaseAt, { maxActiveMs: 100 });
      assert.equal(rawSnapshot(b, 1), before);
    }
    if (!complete) heldHistory(b, before, 1);
  } finally { b.close(); }
}
async function bounded(Old: any, Base: any, file: string) {
  const before = seed(Old, file);
  let b: any = new LocalEventBuffer(file, options);
  const events = shapes(), ids = new Set(events.map(e => e.id));
  try {
    for (const event of events) assert(b.append(event));
    const expected = await baseline(b, Base, ids);
    assert.equal(expected.length, events.length, "baseline covers named, zero, unknown, partial and Claude shapes");
    b.delivery.configure({ enabled: true });
    let previous = 1;
    for (let turn = 0; turn < 5; turn++) {
      const result = b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2, maxWriterMs: 1000 });
      assert(result.visited > 0 || count(b, "upload_outbox") === events.length,
        "a held history must not starve fresh backlog");
      assert(result.visited <= 2, "each pass is row bounded");
      heldHistory(b, before, 1);
      const cursor = newControl(b).cursor_rowid;
      assert(cursor >= previous && cursor <= previous + 2, "durable cursor advances by at most this slice");
      previous = cursor;
      b.close(); b = new LocalEventBuffer(file, { ...options, delivery: { ...options.delivery, enabled: true } });
      assert.equal(newControl(b).cursor_rowid, previous, "restart must retain new-row progress");
      heldHistory(b, before, 1);
    }
    assert.equal(count(b, "upload_outbox"), events.length);
    assert.equal(newControl(b).cursor_rowid, 1 + events.length);
    assert.throws(() => b.database.prepare("update upload_new_row_migration set cursor_rowid=0").run(),
      /immutable|boundary|CHECK/, "the cursor cannot cross the pre-upgrade boundary");
    const expectedEnvelopes = expected.map((item: any) => item.envelope).sort((a: any, z: any) => a.event.id.localeCompare(z.event.id));
    const received: any[] = [];
    let requests = 0;
    const uploaded = await uploadBufferedEvents(config, b, { now: () => leaseAt, developmentLoopbackUrl: true,
      includeLegacyRemainingUnuploaded: false, fetchImpl: async (_url, init) => {
        requests++;
        const body = JSON.parse(String(init?.body));
        received.push(...body.events);
        return new Response(JSON.stringify(acceptedFixtureDelivery(String(init?.body), config.installKey)),
          { status: 200, headers: { "content-type": "application/json" } });
      } });
    assert.equal(requests, 1, "fresh backlog uploads once");
    assert.equal(uploaded.uploadedEvents, events.length);
    assert.deepEqual(received.sort((a, z) => a.event.id.localeCompare(z.event.id)), expectedEnvelopes);
    assert.equal(new Set(received.map(e => e.event.id)).size, events.length, "no double-counted delivery IDs");
    assert.equal(count(b, "upload_receipts"), events.length);
    assert.equal(count(b, "upload_outbox"), 0);
    await noPendingUpload(b);
    for (let turn = 0; turn < 5; turn++) {
      b.projection.runMaintenance(leaseAt, { maxActiveMs: 100 });
      heldHistory(b, before, 1);
    }
    // Retention can remove all already-processed fresh raws. A new append must
    // still allocate above this durable cursor, even while delivery is off.
    const lastProcessed = newControl(b).cursor_rowid;
    b.database.prepare("delete from buffered_events where rowid>1").run();
    b.delivery.configure({enabled:false});
    assert(b.append(native(200, 13, 2)));
    const appended = b.database.prepare("select rowid as n from buffered_events where id=?").get(uuid(200));
    assert(appended.n>lastProcessed,"retention cannot hide a fresh row behind the durable cursor");
    const afterRetention = await baseline(b, Base, new Set([uuid(200)]));
    b.delivery.configure({enabled:true});
    b.delivery.migrateLegacy({now:new Date(NOW),maxRows:2,maxWriterMs:1000});
    assert.deepEqual(b.delivery.lease({now:leaseAt}).items,afterRetention);
    heldHistory(b,before,1);
  } finally { b.close(); }
}
async function main() {
  const outcomes: Array<{ name: string; passed: boolean; error?: string }> = [];
  async function check(name: string, body: (file: string) => Promise<void>) {
    try { await body(path.join(root, name + ".sqlite")); outcomes.push({ name, passed: true }); }
    catch (error) { outcomes.push({ name, passed: false, error: error instanceof Error ? error.stack : String(error) }); }
    completion.check(name, outcomes.at(-1)!.passed);
  }
  try {
    await withReader("cd94758e3d369f3b980473d914299521013e0402", async base => {
      await withReader("71d6ff27f0d39aa31d188c9bcc31d37bf188c384", async old51 => {
        await withReader("a60590559403cace3db7cbbda49812c9e3dbfe62", async old47 => {
          await check("released-0751-disabled-reenabled-SSE-matches-pre-guard-lease", file => directed(old51.Buffer, base.Buffer, file));
          await check("released-0747-disabled-reenabled-SSE-matches-pre-guard-lease", file => directed(old47.Buffer, base.Buffer, file));
          await check("released-0751-bounded-restarts-known-zero-unknown-partial-Claude", file => bounded(old51.Buffer, base.Buffer, file));
          await check("released-0747-bounded-restarts-known-zero-unknown-partial-Claude", file => bounded(old47.Buffer, base.Buffer, file));
          await check("new-byte-budget-pause-preserves-cursor-and-resumes", async file => {
            const before = seed(old51.Buffer, file); const b: any = new LocalEventBuffer(file, options);
            try {
              assert(b.append(native(100, 23, 3)));
              const expected = await baseline(b, base.Buffer, new Set([uuid(100)]));
              b.delivery.configure({ enabled: true });
              const small = b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2, maxBytes: 1, maxWriterMs: 1000 });
              assert.equal(count(b, "upload_outbox"), 0);
              assert.equal(newControl(b).cursor_rowid, 1, "slice budget cannot skip an unqueued row");
              assert.equal(small.newRows?.paused, "slice_budget_too_small");
              heldHistory(b, before, 1);
              b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2, maxWriterMs: 1000 });
              assert.deepEqual(b.delivery.lease({ now: leaseAt }).items, expected);
              heldHistory(b, before, 1);
            } finally { b.close(); }
          });
          await check("complete-migration-preserves-every-new-lease-field", async file => {
            for (const [release, Old] of [["0751", old51.Buffer], ["0747", old47.Buffer]] as const) {
              const world = file + release;
              const before = seed(Old, world, true); const b: any = new LocalEventBuffer(world, options);
              try {
                const events = shapes(), ids = new Set(events.map(e => e.id));
                for (const event of events) assert(b.append(event));
                const expected = await baseline(b, base.Buffer, ids);
                b.delivery.configure({ enabled: true });
                b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 32, maxWriterMs: 1000 });
                const actual = b.delivery.lease({ now: leaseAt }).items;
                assert.deepEqual(actual, expected, "STOP: complete-migration new delivery fields changed");
                assert.equal(rawSnapshot(b, 1), before);
                assert.equal(newControl(b).cursor_rowid, 1, "common path does not consume the held-history cursor");
              } finally { b.close(); }
            }
          });
          await check("historical-NULL-lineage-hold-does-not-starve-unrelated-new-row", async file => {
            const before = seed(old51.Buffer, file, true, bare(1), true);
            const b: any = new LocalEventBuffer(file, options);
            try {
              const receipts = JSON.stringify(b.database.prepare("select * from upload_receipts").all());
              assert(b.append(native(100, 23, 3)));
              const expected = await baseline(b, base.Buffer, new Set([uuid(100)]));
              b.delivery.configure({ enabled: true });
              b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2, maxWriterMs: 1000 });
              assert.deepEqual(b.delivery.lease({ now: leaseAt }).items, expected);
              assert.equal(JSON.stringify(b.database.prepare("select * from upload_receipts").all()), receipts);
              heldHistory(b, before, 1, 1);
              assert.equal(b.delivery.status().migration.pausedReason, "historical_receipt_recovery_requires_opt_in");
            } finally { b.close(); }
          });
          await check("explicit-fixture-history-drain-once-without-new-row-double-queue", async file => {
            const before = seed(old51.Buffer, file, false, native(1, 19, 2));
            let b: any = new LocalEventBuffer(file, options);
            let newEnvelope: string;
            try {
              assert(b.append(native(100, 23, 3))); b.delivery.configure({ enabled: true });
              b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2, maxWriterMs: 1000 });
              assert.equal(count(b, "upload_outbox"), 1);
              heldHistory(b, before, 1);
              newEnvelope = b.database.prepare("select base_envelope_json as n from upload_outbox where raw_id=?").get(uuid(100)).n;
            } finally { b.close(); }
            // Explicit, fixture-only operator opt-in. This actual pre-guard
            // reader models a FUTURE separately reviewed apply command; the
            // product ships no override, env flag or historical apply entry.
            b = new base.Buffer(file, { ...options, delivery: { ...options.delivery, enabled: true } });
            try {
              const dryRunIds = b.database.prepare("select id from buffered_events where rowid<=1 and uploaded_at is null order by rowid").all();
              assert.deepEqual(dryRunIds, [{ id: uuid(1) }]);
              let added = 0;
              for (let turn = 0; turn < 10; turn++) {
                const result = b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2, maxWriterMs: 1000 });
                added += result.enqueued;
                if (result.complete) break;
                assert(turn < 9);
              }
              assert.equal(added, 1, "opt-in queues only the historical row");
              assert.equal(count(b, "upload_outbox"), 2);
              assert.equal(b.database.prepare("select count(*) as n from upload_outbox where raw_id=?").get(uuid(100)).n, 1);
              assert.equal(b.database.prepare("select base_envelope_json as n from upload_outbox where raw_id=?").get(uuid(100)).n, newEnvelope!);
              const lease = b.delivery.lease({ now: leaseAt });
              assert.equal(lease.items.length, 2);
              assert.equal(new Set(lease.items.map((i: any) => i.deliveryId)).size, 2);
              assert.equal(b.delivery.acknowledge(lease.leaseId, lease.items.map((i: any) => i.deliveryId), leaseAt).acknowledged, 2);
              assert.equal(b.delivery.migrateLegacy({ now: new Date(NOW) }).enqueued, 0);
              assert.equal(b.delivery.lease({ now: leaseAt }).items.length, 0, "history drains once");
            } finally { b.close(); }
            b = new LocalEventBuffer(file, { ...options, delivery: { ...options.delivery, enabled: true } });
            try {
              const afterExplicitOptIn = rawSnapshot(b, 1);
              assert.equal(newControl(b).cursor_rowid, 2);
              for (let turn = 0; turn < 5; turn++) {
                b.delivery.migrateLegacy({ now: new Date(NOW), maxRows: 2 });
                b.projection.runMaintenance(leaseAt, { maxActiveMs: 100 });
                assert.equal(rawSnapshot(b, 1), afterExplicitOptIn);
                assert.equal(count(b, "upload_outbox"), 0, "head reopen must not requeue drained new rows");
              }
              assert.equal(count(b, "upload_receipts"), 2);
            } finally { b.close(); }
          });
        });
      });
    });
    console.log(JSON.stringify({ proof: "codex-post-upgrade-migration", reference: "cd94758e", outcomes }));
    completion.complete();
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
