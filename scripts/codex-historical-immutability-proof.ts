import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { ensureUuidEventId } from "../packages/collector-cli/src/upload-history";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { withReader } from "./lib/legacy-reader";
import { createProofCompletion } from "./lib/proof-completion";

const redAdditions = process.argv.includes("--red-additions");
const redRestamp = process.argv.includes("--red-restamp");
const redReplay = process.argv.includes("--red-replay");
const selected = new Set(["reviewer-native-old-observation-and-ACK-owner", "historical-pending-native-ACK-does-not-restamp-raw",
  "late-new-SSE-preserves-historical-span", "released-0747-boundary-holds-through-restart"]);
const completion = createProofCompletion("codex-historical-immutability", redReplay ? 1 : redRestamp ? 1 : redAdditions ? 4 : 10);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "historical-immutability-"));
const AT = Date.now() - 600_000;
const NOW = Date.now() + 121_001;
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const config = collectorConfigSchema.parse({ tenantId: uuid(999), deviceId: "historical-fixture",
  uploadUrl: "http://127.0.0.1:49777/ingest", installKey: "historical-fixture-key" });
const options = { workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(AT - 3_600_000), delivery: { enabled: false, now: () => new Date(NOW) } };
const seedOptions = {...options,delivery:{...options.delivery,now:()=>new Date(AT+5_000)}};
const attr = (key: string, value: string | number) => ({ key, value: typeof value === "number"
  ? { intValue: String(value) } : { stringValue: value } });
function sse(n: number, input: number, output: number, request = "historical-response") {
  const events = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [attr("service.name", "codex-cli")] },
    scopeLogs: [{ logRecords: [{ traceId: "e".repeat(32), timeUnixNano: String(BigInt(AT + n * 1000) * 1_000_000n),
      attributes: [attr("event.name", "codex.sse_event"), attr("session.id", uuid(990)), attr("request_id", request),
        attr("gen_ai.request.model", "gpt-6.1-sol"), attr("gen_ai.usage.input_tokens", input),
        attr("gen_ai.usage.output_tokens", output)] }] }] }] }, { source: "codex", resolveGit: false }).events;
  assert.equal(events.length, 1);
  return aiInteractionEventSchema.parse({ ...events[0]!.event, id: uuid(n) });
}
function bare(n: number, id = uuid(n)) {
  return aiInteractionEventSchema.parse({ id, sessionId: uuid(800 + n), source: "codex", dataMode: "metadata",
    eventType: "assistant_response", observedAt: new Date(AT + n * 1000).toISOString(), model: "gpt-6.1-sol",
    inputTokens: 19, outputTokens: 2, metadata: {} });
}
const snapshot = (b: any) => JSON.stringify(b.database.prepare("select rowid as raw_rowid,* from buffered_events order by rowid").all());
const count = (b: any, table: string) => b.database.prepare(`select count(*) as n from ${table}`).get().n;
function completeMigration(b: any) {
  b.database.prepare(`update upload_control set migration_complete=1,privacy_migration_version=1,
    migration_cursor_rowid=(select coalesce(max(rowid),0) from buffered_events) where singleton=1`).run();
}
const open = (file: string) => new LocalEventBuffer(file, { ...options,
  delivery: { enabled: true, now: options.delivery.now } });
async function advance(b: LocalEventBuffer, before: string, allowSend = false) {
  const now = options.delivery.now();
  const assertNewCursor = () => {
    const row = b.database.prepare(`select boundary_rowid as boundary,cursor_rowid as cursor
      from upload_new_row_migration where singleton=1`).get() as {boundary:number;cursor:number};
    const boundary = b.database.prepare(`select historical_high_water_rowid as n
      from collector_historical_raw_boundary where singleton=1`).get() as {n:number};
    assert.equal(row.boundary,boundary.n);
    assert(row.cursor>=boundary.n,"new-row cursor must never enter immutable history");
  };
  assertNewCursor();
  assert.equal(snapshot(b), before, "head open must preserve every historical raw column");
  b.delivery.migrateLegacy({ now, maxRows: 32, maxWriterMs: 100 });
  assertNewCursor();
  assert.equal(snapshot(b), before, "migration must preserve historical raw bytes");
  b.delivery.lease({ now });
  assertNewCursor();
  assert.equal(snapshot(b), before, "lease must preserve historical raw bytes");
  let calls = 0;
  const result = await uploadBufferedEvents(config, b, { now: () => now,
    developmentLoopbackUrl: true, includeLegacyRemainingUnuploaded: false,
    fetchImpl: async (_url, init) => {
      calls++;
      assert(allowSend, "orphan history must not acquire transport");
      return new Response(JSON.stringify(acceptedFixtureDelivery(String(init?.body), config.installKey)),
        { status: 200, headers: { "content-type": "application/json" } });
    } });
  assert.equal(snapshot(b), before, "upload must preserve historical raw bytes");
  assertNewCursor();
  for (let turn = 0; turn < 5; turn++) {
    b.projection.runMaintenance(now, { maxActiveMs: 100 });
    assertNewCursor();
    assert.equal(snapshot(b), before, `maintenance turn ${turn + 1} must preserve historical raw bytes`);
  }
  return { calls, result };
}
async function main() {
  const outcomes: Array<{ name: string; passed: boolean; error?: string }> = [];
  try {
    async function check(name: string, body: (file: string) => Promise<void>) {
        if (redReplay && name !== "historical-replay-preserves-terminal-custody") return;
        if (redRestamp && name !== "historical-restamp-preserves-raw-and-envelope") return;
        if (redAdditions && !selected.has(name)) return;
        try { await body(path.join(root, name + ".sqlite")); outcomes.push({ name, passed: true }); }
        catch (error) { outcomes.push({ name, passed: false, error: error instanceof Error ? error.stack : String(error) }); }
        completion.check(name, outcomes.at(-1)!.passed);
    }
    await withReader("71d6ff27f0d39aa31d188c9bcc31d37bf188c384", async old => {
      await check("reviewer-native-old-observation-and-ACK-owner", async file => {
        let b: any = new old.Buffer(file, seedOptions); let before: string;
        try {
          assert.equal(b.append(sse(1, 19, 2)), true); completeMigration(b);
          b.delivery.configure({ enabled: true }); assert.equal(b.append(sse(2, 38, 4)), true);
          const lease = b.delivery.lease({ now: options.delivery.now() }); assert.equal(lease.items.length, 1);
          assert.equal(b.delivery.acknowledge(lease.leaseId, [lease.items[0].deliveryId], options.delivery.now()).acknowledged, 1);
          before = snapshot(b);
        } finally { b.close(); }
        b = open(file);
        try {
          await advance(b, before!); assert.equal(count(b, "upload_outbox"), 0);
          assert.equal(b.database.prepare("select input_tokens as n from dashboard_lifetime_totals where singleton=1").get().n, 38,
            "derived projection must count the paid response once");
        } finally { b.close(); }
      });
      await check("reviewer-bare-orphans-remain-unqueued-on-replay", async file => {
        let b: any = new old.Buffer(file, seedOptions); let before: string;
        try { for (let n = 1; n <= 3; n++) assert.equal(b.append(bare(n)), true); completeMigration(b); before = snapshot(b); }
        finally { b.close(); }
        b = open(file);
        try {
          await advance(b, before!);
          for (let n = 1; n <= 3; n++) assert.equal(b.append(bare(n)), false);
          assert.equal(count(b, "upload_outbox"), 0, "repeated ID cannot opt historical raw into repair");
          assert.equal(snapshot(b), before!);
        } finally { b.close(); }
      });
      await check("incomplete-historical-migration-holds-before-admission", async file => {
        let b: any = new old.Buffer(file, seedOptions); let before: string;
        try {
          for (let n = 1; n <= 3; n++) assert.equal(b.append(bare(n)), true);
          b.database.prepare("update upload_control set migration_complete=0,migration_cursor_rowid=0,privacy_migration_version=1 where singleton=1").run();
          before = snapshot(b);
        } finally { b.close(); }
        b = open(file);
        try {
          await advance(b, before!); assert.equal(count(b, "upload_outbox"), 0);
          assert.equal(b.delivery.status().migration.pausedReason, "historical_migration_requires_opt_in");
          assert.equal(b.delivery.status().migration.cursorRowid, 0);
          const fresh = sse(100, 23, 3, "new-response"); assert.equal(b.append(fresh), true);
          const lease = b.delivery.lease({ now: new Date(NOW + 121_001) }); assert.equal(lease.items.length, 1);
          assert.equal(lease.items[0].envelope.event.model, fresh.model);
          assert.equal(lease.items[0].envelope.event.inputTokens, 23); assert.equal(lease.items[0].envelope.event.outputTokens, 3);
          b.delivery.acknowledge(lease.leaseId, [lease.items[0].deliveryId], new Date(NOW + 121_001));
          assert.notEqual(b.database.prepare("select uploaded_at from buffered_events where id=?").get(fresh.id).uploaded_at, null);
        } finally { b.close(); }
      });
      await check("NULL-lineage-recovery-holds-before-constructor", async file => {
        let b: any = new old.Buffer(file, seedOptions); let before: string, receipts: string;
        const literal = "historical-recovery", normalized = ensureUuidEventId(literal).id;
        try {
          assert.equal(b.append(bare(1, literal)), true); assert.equal(b.append(bare(2, normalized)), true);
          const stamp = new Date(AT).toISOString();
          b.database.prepare("update buffered_events set created_at=?").run(stamp); completeMigration(b);
          b.database.prepare(`insert into upload_receipts(delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
            values (?,'dead','remote_validation_rejected','remote_validation',1,?,?)`).run(normalized, stamp, stamp);
          before = snapshot(b); receipts = JSON.stringify(b.database.prepare("select * from upload_receipts").all());
        } finally { b.close(); }
        b = open(file);
        try {
          assert.equal(count(b, "upload_outbox"), 0, "constructor recovery must hold before enqueueing");
          await advance(b, before!);
          assert.equal(JSON.stringify(b.database.prepare("select * from upload_receipts").all()), receipts!);
          assert.equal(b.delivery.status().migration.pausedReason, "historical_receipt_recovery_requires_opt_in");
        } finally { b.close(); }
      });
      await check("historical-pending-native-ACK-does-not-restamp-raw", async file => {
        let b: any = new old.Buffer(file, { ...seedOptions, delivery: { ...seedOptions.delivery, enabled: true } }); let before: string;
        try {
          assert.equal(b.append(sse(1, 19, 2)), true); completeMigration(b); before = snapshot(b);
        } finally { b.close(); }
        b = open(file);
        try {
          assert.equal(snapshot(b), before!); const lease = b.delivery.lease({ now: options.delivery.now() });
          assert.equal(lease.items.length, 1); assert.equal(lease.items[0].envelope.event.model, "gpt-6.1-sol");
          assert.equal(lease.items[0].envelope.event.inputTokens, 19); assert.equal(lease.items[0].envelope.event.outputTokens, 2);
          assert.equal(b.delivery.acknowledge(lease.leaseId, [lease.items[0].deliveryId], options.delivery.now()).acknowledged, 1);
          await advance(b, before!); assert.equal(count(b, "upload_outbox"), 0); assert.equal(count(b, "upload_receipts"), 1);
        } finally { b.close(); }
      });
      await check("durable-boundary-preserves-history-after-restart", async file => {
        let b: any = new old.Buffer(file, seedOptions); let before: string;
        try { assert.equal(b.append(bare(1)), true); completeMigration(b); before = snapshot(b); }
        finally { b.close(); }
        b = open(file);
        try { await advance(b, before!); }
        finally { b.close(); }
        b = open(file);
        try {
          assert.equal(b.append(bare(1)), false); assert.equal(count(b, "upload_outbox"), 0);
          assert.equal(snapshot(b), before!);
          const boundary = b.database.prepare("select historical_high_water_rowid as n from collector_historical_raw_boundary where singleton=1").get().n;
          assert.equal(boundary, 1);
          assert.equal(b.append(sse(100, 19, 2, "fresh-after-restart")), true);
          const row = b.database.prepare("select rowid as n from buffered_events where id=?").get(uuid(100)); assert(row.n > boundary);
          const lease = b.delivery.lease({ now: new Date(NOW + 121_001) }); assert.equal(lease.items.length, 1);
          assert.equal(lease.items[0].envelope.event.model, "gpt-6.1-sol"); assert.equal(lease.items[0].envelope.event.inputTokens, 19);
        } finally { b.close(); }
      });
      await check("historical-replay-preserves-terminal-custody", async file => {
        const terminal = (b:any,id:string) => {
          b.database.prepare(`delete from upload_outbox where delivery_id=?`).run(id);
          b.database.prepare(`insert into upload_receipts(delivery_id,terminal_state,reason,status_class,
            attempt_count,created_at,terminal_at,raw_rowid,raw_id,raw_created_at,raw_generation)
            select id,'dead','remote_validation_rejected','remote_validation',1,created_at,created_at,
              rowid,id,created_at,privacy_generation from buffered_events where id=?`).run(id);
        };
        let b:any=new old.Buffer(file,{...seedOptions,delivery:{...seedOptions.delivery,enabled:true}});
        let before:string,receipts:string,replays:string;
        try{assert.equal(b.append(sse(1,19,2)),true);terminal(b,uuid(1));completeMigration(b);
          before=snapshot(b);receipts=JSON.stringify(b.database.prepare("select * from upload_receipts").all());
          replays=JSON.stringify(b.database.prepare("select * from upload_replays").all());}
        finally{b.close();}
        b=open(file);try{
          for(const dryRun of [true,false]){
            const replay=b.delivery.replayDeadLetters({reason:"remote_validation_rejected",dryRun});
            assert.equal(replay.requeued,0,"a historical dry run cannot promise a forbidden repair");
            assert.equal(replay.skipped.historicalHeld,1);
            assert.equal(snapshot(b),before!);assert.equal(count(b,"upload_outbox"),0);
            assert.equal(JSON.stringify(b.database.prepare("select * from upload_receipts").all()),receipts!);
            assert.equal(JSON.stringify(b.database.prepare("select * from upload_replays").all()),replays!);
          }
          // An old held candidate must not change a genuinely new replay's model/counters.
          assert.equal(b.append(sse(100,23,3,"new-replay-response")),true);terminal(b,uuid(100));
          const fresh=b.delivery.replayDeadLetters({reason:"remote_validation_rejected"});
          assert.equal(fresh.requeued,1);assert.equal(fresh.skipped.historicalHeld,1);
          const lease=b.delivery.lease({now:new Date(NOW+121_001)});assert.equal(lease.items.length,1);
          assert.equal(lease.items[0].envelope.event.model,"gpt-6.1-sol");
          assert.equal(lease.items[0].envelope.event.inputTokens,23);assert.equal(lease.items[0].envelope.event.outputTokens,3);
          assert.equal(JSON.stringify(b.database.prepare("select rowid as raw_rowid,* from buffered_events where id=?").all(uuid(1))),before!);
        }finally{b.close();}
      });
      await check("historical-restamp-preserves-raw-and-envelope", async file => {
        let b:any=new old.Buffer(file,{...seedOptions,delivery:{...seedOptions.delivery,enabled:true}});let before:string,queue:string;
        try{assert.equal(b.append(sse(1,19,2)),true);completeMigration(b);before=snapshot(b);
          queue=JSON.stringify(b.database.prepare("select * from upload_outbox order by delivery_id").all());}
        finally{b.close();}
        b=open(file);try{
          const raw=JSON.parse(b.database.prepare("select payload_json from buffered_events where id=?").get(uuid(1)).payload_json);
          const corrected={...raw,inputTokens:31,metadata:{...raw.metadata,"gen_ai.usage.input_tokens":"31"}};
          assert.equal(b.delivery.restampUnsentRaw(uuid(1),JSON.stringify(corrected)),false,"historical correction requires reviewed opt-in");
          assert.equal(snapshot(b),before!);assert.equal(JSON.stringify(b.database.prepare("select * from upload_outbox order by delivery_id").all()),queue!);
          // Preserve today's correction behavior for a genuinely new raw row.
          assert.equal(b.append(sse(100,23,3,"new-restamp-response")),true);
          const fresh=JSON.parse(b.database.prepare("select payload_json from buffered_events where id=?").get(uuid(100)).payload_json);
          assert.equal(b.delivery.restampUnsentRaw(uuid(100),JSON.stringify({...fresh,inputTokens:29,
            metadata:{...fresh.metadata,"gen_ai.usage.input_tokens":"29"}})),true);
          assert.equal(b.database.prepare("select input_tokens,payload_json from buffered_events where id=?").get(uuid(100)).input_tokens,23,
            "new restamp keeps the existing promoted-column contract");
          assert.equal(JSON.parse(b.database.prepare("select payload_json from buffered_events where id=?").get(uuid(100)).payload_json).inputTokens,29);
        }finally{b.close();}
      });
      await check("late-new-SSE-preserves-historical-span", async file => {
        let b: any = new old.Buffer(file,seedOptions); let before: string;
        const span=explodeOtlpPayload({resourceSpans:[{resource:{attributes:[attr("service.name","codex-cli")]},scopeSpans:[{spans:[{
          traceId:"e".repeat(32),spanId:"a".repeat(16),name:"handle_responses",kind:1,
          startTimeUnixNano:String(BigInt(AT+500)*1_000_000n),endTimeUnixNano:String(BigInt(AT+2020)*1_000_000n),
          attributes:[attr("session.id",uuid(990)),attr("request_id","historical-response"),
            attr("gen_ai.usage.input_tokens",19),attr("gen_ai.usage.output_tokens",2)]}]}]}]},
          {source:"codex",resolveGit:false}).events[0]!.event;
        span.id=uuid(1);
        try { assert.equal(b.append(span),true);completeMigration(b);before=snapshot(b); }
        finally {b.close();}
        b=open(file);
        try {
          assert.equal(b.append(sse(2,19,2)),true);
          const archived=JSON.stringify(b.database.prepare("select rowid as raw_rowid,* from buffered_events where id=?").all(uuid(1)));
          assert.equal(archived,before!,"a new exact twin cannot rewrite its historical peer");
          const lease=b.delivery.lease({now:new Date(NOW+121_001)});assert.equal(lease.items.length,1);
          assert.equal(lease.items[0].envelope.event.model,"gpt-6.1-sol");
          assert.equal(lease.items[0].envelope.event.inputTokens,19);
          assert.equal(lease.items[0].envelope.event.outputTokens,2);
          for(let turn=0;turn<5;turn++)b.projection.runMaintenance(new Date(NOW+121_001),{maxActiveMs:100});
          assert.equal(JSON.stringify(b.database.prepare("select rowid as raw_rowid,* from buffered_events where id=?").all(uuid(1))),before!);
        } finally {b.close();}
      });
    });
    await withReader("a60590559403cace3db7cbbda49812c9e3dbfe62",async old=>{
      await check("released-0747-boundary-holds-through-restart",async file=>{
        let b:any=new old.Buffer(file,seedOptions);let before:string;
        try{for(let n=1;n<=3;n++)assert.equal(b.append(bare(n)),true);completeMigration(b);before=snapshot(b);}
        finally{b.close();}
        for(let restart=0;restart<2;restart++){
          b=open(file);try{
            await advance(b,before!);for(let n=1;n<=3;n++)assert.equal(b.append(bare(n)),false);
            assert.equal(count(b,"upload_outbox"),0,".47 history stays unqueued without an opt-in");
            assert.equal(snapshot(b),before!);
          }finally{b.close();}
        }
      });
    });
    console.log(JSON.stringify({ proof: "codex-historical-immutability", outcomes }));
    completion.complete();
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
