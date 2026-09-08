import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import net, { type AddressInfo } from "node:net";
import path from "node:path";
import { Worker } from "node:worker_threads";
import { buildSync } from "esbuild";
import Database from "better-sqlite3";
import { createProofCompletion } from "./lib/proof-completion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { loadOrCreateLocalIngestAuth, LOCAL_INGEST_AUTH_FILE } from "../packages/collector-cli/src/local-auth";
import { provisionLiveProducer, authenticateLiveProducer, LIVE_BINDINGS_FILE, disableLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import { liveUsageDiagnostics, ingestLiveUsage } from "../packages/collector-cli/src/codex-live-usage-ledger";
import { canonicalJson, parseLivePacket, liveSha256, liveEventId, LIVE_COUNTERS, type LiveUsagePacket } from "../packages/collector-cli/src/codex-live-usage-protocol";
import { readLiveUsageObservation, type AiInteractionEvent } from "../packages/shared/src/index";

const completion = createProofCompletion("codex-live-usage");
const goldens = JSON.parse(fs.readFileSync(new URL("./fixtures/codex-live-usage-golden-r4.json", import.meta.url), "utf8")).vectors;
const baseline = goldens[0].packet as LiveUsagePacket;
const positive = goldens[1].packet as LiveUsagePacket;
const epochStart = "2026-09-08T08:00:00.000Z";
const workspaceId = "33333333-3333-7333-8333-333333333333", deviceId = "44444444-4444-7444-8444-444444444444";
let fixtureNo = 0;
type Result = { status: number; text: string; body: any };
function request(port: number, body: string | Buffer, headers: Record<string, string | string[] | undefined> = {}, route = "/hooks/codex", method = "POST"): Promise<Result> {
  return new Promise((resolve, reject) => {
    const bytes = Buffer.from(body);
    const req = http.request({ host: "127.0.0.1", port, path: route, method, headers: {
      connection: "close", "content-type": "application/json", "content-length": String(bytes.length), ...headers,
    } }, res => {
      const chunks: Buffer[] = []; res.on("data", c => chunks.push(c)); res.on("end", () => {
        const text = Buffer.concat(chunks).toString(); let body: any; try { body = JSON.parse(text); } catch { body = null; }
        resolve({ status: res.statusCode!, text, body });
      });
    });
    req.setTimeout(5000, () => req.destroy(new Error("test_http_timeout"))); req.on("error", reject); req.end(bytes);
  });
}
async function fixture() {
  const home = path.join(process.env.PLIMSOLL_PROOF_ROOT!, `live-${++fixtureNo}`);
  fs.mkdirSync(home, { mode: 0o700 });
  const file = path.join(home, "ledger.sqlite");
  const options = { workspaceId, deviceId, enrollmentNow: () => new Date(epochStart), databaseBusyTimeoutMs: 100,
    delivery: { enabled: true } };
  let buffer = new LocalEventBuffer(file, options);
  const root = { rootId: "synthetic-root", profileId: "synthetic-profile", source: "codex" as const,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId!, directory: path.join(home, "empty-source"),
    account: { actorHash: "sha256:" + "a".repeat(64), validFrom: epochStart, validUntil: null as string | null, evidenceRef: "account-evidence" },
    dispatch: [{ sessionId: baseline.threadId, workItemId: "work-1", projectKey: "sha256:" + "b".repeat(64),
      companyRef: "company-1", attemptId: "attempt-1", parentAttemptId: null, acceptedOutcomeId: null,
      validFrom: epochStart, validUntil: null as string | null, evidenceRef: "work-evidence" }] };
  fs.mkdirSync(root.directory, { mode: 0o700 });
  const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId, captureRoots: [root] });
  const ordinary = loadOrCreateLocalIngestAuth(home);
  const localAuthBefore = fs.readFileSync(path.join(home, LOCAL_INGEST_AUTH_FILE));
  const enroll = (producerId = baseline.producerId, credentialId = baseline.credentialId) => {
    const result = provisionLiveProducer({ home, buffer, config, producerId, credentialId, captureRootId: root.rootId, enrolledAt: epochStart });
    assert.equal(fs.statSync(result.credentialFile).mode & 0o777, 0o600);
    assert.deepEqual(fs.readFileSync(path.join(home, LOCAL_INGEST_AUTH_FILE)), localAuthBefore);
    return fs.readFileSync(result.credentialFile, "utf8");
  };
  let token = enroll();
  let server = createCollectorServer(config, buffer, { localAuth: ordinary, liveProducerHome: home, perSourceRequestLimit: 10000 });
  let port = 0;
  const start = async () => { await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve)); port = (server.address() as AddressInfo).port; };
  await start();
  const headers = (producerId = baseline.producerId, value = token) => ({ "x-plimsoll-producer-id": producerId, "x-plimsoll-token": value });
  const send = (packet: unknown, extra: Record<string, string | string[]> = {}) => request(port, canonicalJson(packet), { ...headers(), ...extra });
  const count = (table: string) => (buffer.database.prepare(`select count(*) n from ${table}`).get() as {n:number}).n;
  const checkpoint = () => { const row = buffer.database.prepare("select checkpoint_json from codex_live_attachments where checkpoint_json is not null limit 1").get() as any; return row ? JSON.parse(row.checkpoint_json) : null; };
  const events = () => buffer.database.prepare("select payload_json from buffered_events order by rowid").all().map((r: any) => JSON.parse(r.payload_json));
  return { home, file, config, ordinary, options, headers, send, count, checkpoint, events, enroll,
    get port() { return port; }, get buffer() { return buffer; }, get token() { return token; },
    requestStarted() {return new Promise<void>(resolve=>server.once("request",()=>resolve()));},
    rotate() { token = enroll(baseline.producerId, "synthetic-credential-2"); },
    async restart() { await new Promise<void>(r => server.close(() => r())); buffer.close(); buffer = new LocalEventBuffer(file, options);
      server = createCollectorServer(config, buffer, { localAuth: ordinary, liveProducerHome: home, perSourceRequestLimit: 10000 }); await start(); },
    async close() { await new Promise<void>(r => server.close(() => r())); buffer.close(); fs.rmSync(home, {recursive:true,force:true}); } };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;
async function test(name: string, run: (f: Fixture) => Promise<void>) {
  const f = await fixture(); try { await run(f); completion.check(name); console.log("PASS " + name); } finally { await f.close(); }
}
const clone = <T>(p:T):T => structuredClone(p);
function legacyEvent(id: string, eventType: AiInteractionEvent["eventType"] = "assistant_response", sessionId = baseline.threadId): AiInteractionEvent {
  return {id,source:"codex" as const,dataMode:"metadata" as const,eventType,sessionId,tenantId:workspaceId,
    observedAt:baseline.capturedAt,intent:"unknown" as const,actionClass:"other" as const,inputTokens:1,outputTokens:1,metadata:{}};
}
function rawLegacy(f:Fixture, id:string, eventType:string, input:number|null=1, cache:number|null=null) {
  f.buffer.database.prepare(`insert into buffered_events
    (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id,input_tokens,cache_read_tokens)
    values(?,'codex',?,'metadata',?,'{}',?,?,?,?)`).run(id,eventType,baseline.capturedAt,baseline.capturedAt,baseline.threadId,input,cache);
}
async function race(f:Fixture, actions:Array<{packet?:unknown;token?:string;event?:unknown}>) {
  const root=process.env.PLIMSOLL_PROOF_ROOT!, entry=path.join(root,"race-worker.cjs");
  if(!fs.existsSync(entry)) {
    fs.symlinkSync(path.resolve("node_modules"),path.join(root,"node_modules"),"dir");
    buildSync({entryPoints:[path.resolve("scripts/codex-live-usage-race-worker.ts")],outfile:entry,bundle:true,platform:"node",format:"cjs",packages:"external",logLevel:"silent"});
  }
  const barrier=new SharedArrayBuffer(4), workers:Worker[]=[], results:Promise<any>[]=[];
  try {
    for(const action of actions) {
      let ready!:()=>void; const start=new Promise<void>(r=>{ready=r;});
      const worker=new Worker(entry,{workerData:{...action,barrier,file:f.file,home:f.home,config:f.config,epochStart},resourceLimits:{maxOldGenerationSizeMb:256}});
      workers.push(worker);
      results.push(new Promise((resolve,reject)=>{worker.on("message",message=>{if(message.ready) ready(); else resolve(message.result);});worker.on("error",reject);worker.on("exit",code=>{if(code) reject(new Error("race_worker_exit"));});}));
      await start;
    }
    Atomics.store(new Int32Array(barrier),0,1); Atomics.notify(new Int32Array(barrier),0);
    return await Promise.all(results);
  } finally {await Promise.all(workers.map(w=>w.terminate()));}
}

async function main() {
// Golden branches each begin with a new database: no artificial control collisions.
for (let i = 0; i < goldens.length; i++) {
  const g = goldens[i];
  await test("golden_" + g.name, async f => {
    for (const v of goldens) { assert.equal(liveSha256(v.canonicalUtf8), v.sha256); assert.equal(canonicalJson(parseLivePacket(Buffer.from(v.canonicalUtf8))), v.canonicalUtf8); }
    const predecessors = i === 0 || i === 8 ? 0 : i === 1 || i === 6 || i === 7 ? 1 : i === 2 ? 2 : 3;
    if (i === 7) f.config.captureRoots![0].account!.validUntil = g.context.bindingA.validUntil;
    for (let n = 0; n < predecessors; n++) assert.equal((await f.send(goldens[n].packet)).text, goldens[n].receiptCanonicalUtf8);
    if (i === 7) {
      f.config.captureRoots![0].account = { ...f.config.captureRoots![0].account!, actorHash: "sha256:"+"c".repeat(64), ...g.context.bindingB };
      f.config.captureRoots![0].dispatch![0].validFrom = g.context.bindingB.validFrom;
    }
    if (i === 8) f.rotate();
    const r = await f.send(g.packet);
    assert.equal(r.text, g.receiptCanonicalUtf8); assert.equal(liveSha256(r.text), g.receiptSha256);
    if (g.receipt.committed) {
      const replay = await f.send(g.packet); assert.deepEqual(replay.body, {...g.receipt,replayed:true});
    }
    if (g.expectedEventId) {
      const e = f.events()[0]; assert.equal(e.id, g.expectedEventId);
      assert.deepEqual([e.inputTokens,e.cacheReadTokens,e.cacheCreationTokens,e.outputTokens,e.metadata.liveReasoningOutputTokens,e.metadata.liveTotalTokens], LIVE_COUNTERS.map(k=>g.expectedDelta[k]));
      assert.equal(e.eventType,"usage_live"); assert.equal(e.source,"codex"); assert.equal(e.dataMode,"metadata");
      assert.equal(e.sessionId,baseline.threadId); assert.equal(e.actor,undefined); assert.equal(e.model,undefined); assert.equal(e.costUsd,undefined);
      assert.equal(e.metadata.captureProfileId,"synthetic-profile"); assert.equal(e.metadata.profileId,undefined);
      assert.ok(readLiveUsageObservation(e.metadata,e.observedAt)); assert.equal(e.metadata.liveFinanceEligibility,"unqualified_observer");
      assert.equal(f.count("upload_outbox"),1);
      if (i===7) { assert.equal(e.metadata.liveAttributionState,"unresolved"); assert.equal(e.metadata.captureAccountHash,undefined); assert.equal(e.metadata.workItemId,undefined); }
    }
    if (i===0) { assert.equal(f.count("buffered_events"),0); assert.equal(f.count("upload_outbox"),0); assert.equal(f.count("codex_live_pins"),1); }
  });
}

await test("raw_header_audience_cardinality_target_and_browser_controls", async f => {
  const b=canonicalJson(baseline), h=f.headers();
  for (const route of ["/hooks/codex?x=1","/hooks/claude_code","/v1/logs","/status","/healthz","/hooks/codex/"]) assert.ok((await request(f.port,b,h,route)).status>=400,route);
  assert.ok((await request(f.port,b,h,"/hooks/codex","GET")).status>=400);
  for (const extra of [
    {"x-plimsoll-producer-id":[baseline.producerId,baseline.producerId]}, {"x-plimsoll-token":[f.token,f.token]},
    {"content-type":["application/json","application/json"]}, {"x-plimsoll-source":["codex","codex"]},
    {"x-plimsoll-source":"claude_code"}, {"origin":"http://localhost"}, {"sec-fetch-site":"cross-site"},
    {"content-type":"application/json; charset=utf-8"}, {"content-encoding":"gzip"},
    {"x-plimsoll-token":f.ordinary.codexProducer}, {"x-plimsoll-token":f.ordinary.managementRead},
    {"x-plimsoll-producer-id":"wrong"},
  ]) assert.ok((await request(f.port,b,{...h,...extra})).status>=400);
  assert.ok((await request(f.port,b,{"x-plimsoll-token":f.token})).status>=400);
  assert.ok((await request(f.port,b,{"x-plimsoll-token":f.token},"/v1/logs")).status>=400);
  assert.ok((await request(f.port,"",{"x-plimsoll-token":f.token},"/status","GET")).status>=400);
  assert.equal((await request(f.port,"",{},"/healthz","GET")).status,200);
  assert.equal(f.count("codex_live_receipts"),0); assert.equal(f.count("buffered_events"),0);
});

await test("closed_canonical_4k_body_grammar", async f => {
  const b=canonicalJson(baseline), bads: Array<string|Buffer> = ["", "null", "[]", b+"\n", " "+b,
    b.replace('"kind":"usage"','"kind":"usage","kind":"usage"'), b.replace('"kind"','"k\\u0069nd"'),
    b.replace('"inputTokens":0','"inputTokens":-0'), b.replace('"inputTokens":0','"inputTokens":0.0'),
    b.replace('"inputTokens":0','"inputTokens":9007199254740992'), b.replace('"inputTokens":0','"inputTokens":null'),
    b.replace('"schema":"codex.app-server.usage.v1"','"schema":"codex.app-server.usage.v99"'),
    canonicalJson({...baseline, extra:"PRIVATE_BODY_SENTINEL"}), canonicalJson({...baseline,total:{...baseline.total,extra:1}}),
    Buffer.from([0xff]), "x".repeat(4097)];
  for (const body of bads) { const r=await request(f.port,body,f.headers()); assert.ok(r.status>=400); assert.ok(!r.text.includes("PRIVATE_BODY_SENTINEL")); }
  assert.equal(f.count("codex_live_receipts"),0); assert.equal(f.count("codex_live_pins"),0);
});

await test("rotation_echo_revocation_and_current_source_binding_before_dedupe", async f => {
  assert.equal((await f.send(baseline)).body.disposition,"baseline_only");
  const old=f.token, before=f.buffer.database.prepare("select * from codex_live_receipts").all();
  for (const mutate of [
    () => {f.config.captureRoots![0].source="claude_code";}, () => {f.config.captureRoots![0].rootId="other";},
    () => {f.config.captureRoots![0].profileId="other";}, () => {f.config.captureRoots![0].installationEpochId="other";},
    () => {f.config.captureRoots![0].directory+="-other";}, () => {f.config.tenantId="other";}, () => {f.config.deviceId="other";},
  ]) { const saved=clone(f.config); mutate(); const r=await f.send(baseline); assert.ok(r.status===401||r.status===403); assert.equal(r.body?.packetDigest,undefined); Object.assign(f.config,saved); }
  f.rotate(); assert.equal((await request(f.port,canonicalJson(baseline),f.headers(baseline.producerId,old))).status,401);
  assert.equal((await f.send(baseline)).body.disposition,"enrollment_rejected");
  const fresh={...baseline,credentialId:"synthetic-credential-2",attachmentId:"d".repeat(64)};
  assert.equal((await f.send(fresh)).body.disposition,"baseline_only"); assert.equal(f.count("codex_live_pins"),1);
  assert.deepEqual(f.buffer.database.prepare("select * from codex_live_receipts where scope_digest=?").all((before[0] as any).scope_digest),before);
  disableLiveProducer(f.home,f.buffer,baseline.producerId); assert.equal((await f.send(fresh)).status,401);
  assert.equal(f.count("codex_live_pins"),1);
});

await test("gaps_resets_collisions_hold_checkpoint_and_preserve_original_receipts", async f => {
  await f.send(baseline); await f.send(positive); const before=f.checkpoint();
  const collision={...positive,total:{...positive.total,inputTokens:101}};
  assert.equal((await f.send(collision)).body.disposition,"collision"); assert.deepEqual(f.checkpoint(),before);
  assert.equal((await f.send(positive)).body.disposition,"stored"); assert.equal((await f.send(positive)).body.replayed,true);
  const next={...positive,observationSeq:3,previousDigest:liveSha256(canonicalJson(positive)),capturedAt:"2026-09-08T09:00:10.000Z"};
  assert.equal((await f.send(next)).body.disposition,"gap"); assert.deepEqual(f.checkpoint(),before);
  assert.equal(f.count("buffered_events"),1);
});

await test("receipt_failure_rolls_back_event_checkpoint_outbox_and_baseline_pin", async f => {
  f.buffer.database.exec("create trigger fail_receipt before insert on codex_live_receipts begin select raise(abort,'synthetic'); end");
  assert.equal((await f.send(baseline)).body.disposition,"retryable");
  for (const table of ["codex_live_pins","session_usage_authority","codex_live_attachments","codex_live_packet_keys","codex_live_receipts","buffered_events","upload_outbox"]) assert.equal(f.count(table),0,table);
  f.buffer.database.exec("drop trigger fail_receipt"); await f.send(baseline); const before=f.checkpoint();
  f.buffer.database.exec("create trigger fail_receipt before insert on codex_live_receipts begin select raise(abort,'synthetic'); end");
  const r=await f.send(positive); assert.equal(r.status,503); assert.equal(r.body.committed,false); assert.deepEqual(f.checkpoint(),before);
  assert.equal(f.count("buffered_events"),0); assert.equal(f.count("upload_outbox"),0); assert.equal(f.count("codex_live_receipts"),1);
  f.buffer.database.exec("drop trigger fail_receipt"); assert.equal((await f.send(positive)).body.disposition,"stored");
});

await test("restart_raw_retention_and_immutable_receipts_pins", async f => {
  await f.send(baseline); await f.send(positive);
  const before=f.buffer.database.prepare("select * from codex_live_receipts order by packet_key").all(), pin=f.buffer.database.prepare("select * from codex_live_pins").all();
  await f.restart(); assert.equal((await f.send(positive)).body.replayed,true);
  f.buffer.prune(1,{now:new Date("2030-01-01T00:00:00.000Z")});
  assert.equal(f.count("buffered_events"),0); assert.equal((await f.send(positive)).body.replayed,true); assert.equal(f.count("buffered_events"),0);
  assert.deepEqual(f.buffer.database.prepare("select * from codex_live_receipts order by packet_key").all(),before);
  assert.deepEqual(f.buffer.database.prepare("select * from codex_live_pins").all(),pin);
  for (const table of ["codex_live_pins","codex_live_receipts","codex_live_packet_keys"]) {
    assert.throws(()=>f.buffer.database.exec(`delete from ${table}`),/immutable/);
  }
  assert.throws(()=>f.buffer.database.exec("update codex_live_pins set producer_id='other'"),/immutable/);
});

for(const mode of ["live","tailer","cache","metric"]) await test("old_ledger_"+mode+"_prevents_claim_after_upgrade",async f=>{
  if(mode==="metric") f.buffer.appendMetricSample({id:"old-metric",source:"codex",metricName:"codex.token.usage",observedAt:baseline.capturedAt,sessionId:baseline.threadId,value:1,attrs:{},suppressedFields:[]});
  else rawLegacy(f,"old-row",mode==="tailer"?"usage_rollout":"assistant_response",mode==="cache"?null:1,mode==="cache"?2:null);
  await f.restart();
  assert.equal((await f.send(baseline)).body.disposition,"authority_conflict"); assert.equal(f.count("codex_live_pins"),0);
});
await test("legacy_live_claim_without_pin_is_never_upgraded",async f=>{
  f.buffer.database.prepare("insert into session_usage_authority values('codex',?,'live',?)").run(baseline.threadId,epochStart);
  assert.equal((await f.send(baseline)).body.disposition,"authority_conflict"); assert.equal(f.count("codex_live_pins"),0);
});
await test("separate_connection_competing_producer_first_baselines",async f=>{
  const otherToken=f.enroll("other-producer","other-credential"), other={...baseline,producerId:"other-producer",credentialId:"other-credential",attachmentId:"a".repeat(64)};
  let receipts=await race(f,[{packet:baseline,token:f.token},{packet:other,token:otherToken}]);
  for(let i=0;i<receipts.length;i++) if(receipts[i].disposition==="retryable") receipts[i]=i===0?(await f.send(baseline)).body:(await request(f.port,canonicalJson(other),f.headers("other-producer",otherToken))).body;
  assert.deepEqual(receipts.map(r=>r.disposition).sort(),["authority_conflict","baseline_only"]);
  assert.equal(f.count("codex_live_pins"),1); assert.equal(f.count("codex_live_attachments"),2); assert.equal(f.count("buffered_events"),0);
});
await test("separate_connection_same_producer_fresh_attachment_baselines",async f=>{
  const next={...baseline,attachmentId:"c".repeat(64)};
  const receipts=await race(f,[{packet:baseline,token:f.token},{packet:next,token:f.token}]);
  for(let i=0;i<receipts.length;i++) if(receipts[i].disposition==="retryable") receipts[i]=(await f.send(i===0?baseline:next)).body;
  assert.ok(receipts.every(r=>r.disposition==="baseline_only")); assert.equal(f.count("codex_live_pins"),1); assert.equal(f.count("buffered_events"),0);
});
await test("separate_connection_tailer_first_writer_race",async f=>{
  const results=await race(f,[{packet:baseline,token:f.token},{event:legacyEvent("race-tailer","usage_rollout")}]);
  const receipt=results[0].disposition==="retryable"?(await f.send(baseline)).body:results[0];
  if(results[1].busy) results[1]=f.buffer.append(legacyEvent("race-tailer","usage_rollout"),[],{integrityReceipt:true});
  if(receipt.disposition==="baseline_only") {assert.equal(results[1].appended,false);assert.equal(f.count("buffered_events"),0);}
  else {assert.equal(receipt.disposition,"authority_conflict");assert.equal(results[1].appended,true);assert.equal(f.count("codex_live_pins"),0);}
});

await test("ordinary_hook_OTLP_and_second_adapter_cannot_assert_internal_capability",async f=>{
  const ordinaryHeaders={"x-plimsoll-token":f.ordinary.codexProducer}, otlpHeaders={...ordinaryHeaders,"x-plimsoll-source":"codex"};
  for(const claimed of [{...baseline},{metadata:{liveTotalTokens:3}},{metadata:{sourceVersion:"codex.app-server.usage.v1"}},{event_type:"usage_live"}])
    assert.equal((await request(f.port,canonicalJson(claimed),ordinaryHeaders)).status,403);
  const attributes=[{key:"liveTotalTokens",value:{intValue:"3"}}];
  assert.equal((await request(f.port,canonicalJson({resourceLogs:[{scopeLogs:[{logRecords:[{attributes}]}]}]}),otlpHeaders,"/v1/logs")).status,403);
  await f.send(baseline);
  const tokenHook={id:"pinned-hook",event_type:"assistant_response",session_id:baseline.threadId,timestamp:baseline.capturedAt,input_tokens:12,output_tokens:2};
  await request(f.port,canonicalJson(tokenHook),ordinaryHeaders); assert.equal(f.count("buffered_events"),0);
  const at=String(BigInt(Date.parse(baseline.capturedAt))*1000000n), attrs=[
    {key:"session.id",value:{stringValue:baseline.threadId}},{key:"gen_ai.usage.input_tokens",value:{intValue:"9"}}];
  await request(f.port,canonicalJson({resourceLogs:[{scopeLogs:[{logRecords:[{timeUnixNano:at,attributes:attrs}]}]}]}),otlpHeaders,"/v1/logs");
  assert.equal(f.count("buffered_events"),0);
  await request(f.port,canonicalJson({resourceMetrics:[{scopeMetrics:[{metrics:[{name:"codex.token.usage",sum:{dataPoints:[{timeUnixNano:at,asInt:"9",attributes:attrs}]}}]}]}]}),otlpHeaders,"/v1/metrics");
  assert.equal(f.count("metric_samples"),0);
  assert.equal(f.buffer.append(legacyEvent("second-adapter")),false);
  const generated:AiInteractionEvent={...legacyEvent("forgery"),eventType:"usage_live",metadata:{liveTotalTokens:1}};
  assert.equal(f.buffer.append(generated),false);
  const work:AiInteractionEvent={...legacyEvent("ordinary-work"),inputTokens:undefined,outputTokens:undefined,eventType:"tool_use"};
  assert.equal(f.buffer.append(work),true);
  const unrelated={...tokenHook,id:"unrelated-hook",session_id:"other-session"};
  assert.equal((await request(f.port,canonicalJson(unrelated),ordinaryHeaders)).status,202); assert.equal(f.count("buffered_events"),2);
});

for(const key of LIVE_COUNTERS) await test("reset_"+key+"_never_adopts_rejected_counters",async f=>{
  await f.send(baseline);await f.send(positive);const before=f.checkpoint();
  const reset={...positive,observationSeq:3,previousDigest:liveSha256(canonicalJson(positive)),total:{...positive.total,[key]:positive.total[key]-1}};
  const result=await f.send(reset);assert.equal(result.body.disposition,"counter_reset");assert.equal(result.body.committedObservationSeq,2);assert.deepEqual(f.checkpoint(),before);
  assert.deepEqual((await f.send(reset)).body,{...result.body,replayed:true});assert.equal(f.count("buffered_events"),1);
});
for(const [name,change] of Object.entries({sequence:{observationSeq:4},digest:{previousDigest:"f".repeat(64)},time:{capturedAt:"2026-09-08T08:59:59.000Z"},late:{observationSeq:1}}))
  await test("gap_"+name+"_holds_trusted_checkpoint",async f=>{
    await f.send(baseline);await f.send(positive);const before=f.checkpoint();
    const p={...positive,observationSeq:3,previousDigest:liveSha256(canonicalJson(positive)),...change};
    const result=await f.send(p);assert.equal(result.body.disposition,name==="late"?"collision":"gap");assert.equal(result.body.committedObservationSeq,2);assert.deepEqual(f.checkpoint(),before);
    const late={...positive,observationSeq:3,previousDigest:liveSha256(canonicalJson(positive)),capturedAt:"2026-09-08T09:00:06.000Z"};
    assert.equal((await f.send(late)).body.disposition,name==="sequence"||name==="late"?"gap":"collision");assert.deepEqual(f.checkpoint(),before);
  });
await test("event_id_collision_commits_only_refusal",async f=>{
  await f.send(baseline);const before=f.checkpoint();rawLegacy(f,liveEventId(positive),"assistant_response");
  const result=await f.send(positive);assert.equal(result.body.disposition,"collision");assert.deepEqual(f.checkpoint(),before);
  assert.equal(f.count("buffered_events"),1);assert.equal(f.count("upload_outbox"),0);
});

for(const mode of ["same","account-missing","work-missing","account-changed","work-changed","interior-overlap","zero-time"]) await test("whole_interval_"+mode,async f=>{
  const r=f.config.captureRoots![0];
  if(mode==="account-missing") delete r.account;
  if(mode==="work-missing") r.dispatch=[];
  await f.send(baseline);
  if(mode==="account-changed") r.account!.evidenceRef="new-account-evidence";
  if(mode==="work-changed") r.dispatch![0].evidenceRef="new-work-evidence";
  if(mode==="interior-overlap") r.dispatch!.push({...r.dispatch![0],workItemId:"overlap",validFrom:"2026-09-08T09:00:01.000Z",validUntil:"2026-09-08T09:00:02.000Z"});
  const packet=mode==="zero-time"?{...positive,capturedAt:baseline.capturedAt}:positive;
  assert.equal((await f.send(packet)).body.disposition,"stored");const e=f.events()[0],m=e.metadata;
  assert.equal(m.liveAttributionState,mode==="same"?"qualified":"unresolved");
  assert.equal(Boolean(m.captureAccountHash),!["account-missing","account-changed","zero-time"].includes(mode));
  assert.equal(Boolean(m.workItemId),!["work-missing","work-changed","interior-overlap","zero-time"].includes(mode));
  assert.equal(e.projectKey,undefined);assert.equal(e.costUsd,undefined);assert.equal(m.liveFinanceEligibility,"unqualified_observer");
  assert.equal(f.checkpoint().seq,2);
});
await test("bounded_metadata_only_diagnostics_and_private_hash_registry",async f=>{
  await f.send(baseline);await f.send(positive);await f.send(goldens[4].packet);
  const registry=fs.readFileSync(path.join(f.home,LIVE_BINDINGS_FILE),"utf8"), diagnostics=canonicalJson(liveUsageDiagnostics(f.buffer.database));
  assert.ok(!registry.includes(f.token)); assert.equal(fs.statSync(path.join(f.home,LIVE_BINDINGS_FILE)).mode&0o777,0o600);
  assert.ok(diagnostics.length<2048);assert.ok(!diagnostics.includes(f.token));assert.ok(!diagnostics.includes(baseline.threadId));assert.ok(!diagnostics.includes("inputTokens"));
  const checkpoint=canonicalJson(f.checkpoint());assert.ok(!checkpoint.includes(f.home));assert.ok(checkpoint.length<=8192);
  assert.equal((f.buffer.database.pragma("foreign_key_check") as unknown[]).length,0);
});
await test("credential_rotation_during_body_read_is_terminal_before_replay",async f=>{
  await f.send(baseline);const bytes=Buffer.from(canonicalJson(baseline)), started=f.requestStarted();
  const result=new Promise<Result>((resolve,reject)=>{
    const req=http.request({host:"127.0.0.1",port:f.port,path:"/hooks/codex",method:"POST",headers:{...f.headers(),"content-type":"application/json","content-length":bytes.length,connection:"close"}},res=>{
      const chunks:Buffer[]=[];res.on("data",c=>chunks.push(c));res.on("end",()=>{const text=Buffer.concat(chunks).toString();resolve({status:res.statusCode!,text,body:JSON.parse(text)});});
    });
    req.on("error",reject);req.setTimeout(5000,()=>req.destroy(new Error("rotation_request_timeout")));
    req.write(bytes.subarray(0,20));
    void started.then(()=>{f.rotate();req.end(bytes.subarray(20));}).catch(reject);
  });
  const r=await result;assert.ok(r.status===401||r.status===403);assert.equal(r.body.packetDigest,undefined);assert.equal(f.count("codex_live_receipts"),1);
});
await test("body_producer_echo_and_fabricated_auth_context_never_select_scope",async f=>{
  await f.send(baseline);const changed={...baseline,producerId:"other"};
  const r=await f.send(changed);assert.equal(r.body.disposition,"enrollment_rejected");assert.equal(r.body.producerId,"other");assert.equal(r.body.committedObservationSeq,null);
  const auth=authenticateLiveProducer(f.home,f.buffer,f.config,baseline.producerId,f.token);
  assert.throws(()=>ingestLiveUsage(f.buffer,positive,liveSha256(canonicalJson(positive)),()=>({...auth})),/producer_token_invalid/);
  assert.throws(()=>{auth.context.producerId="other";},TypeError);assert.equal(f.count("codex_live_receipts"),1);
});
for(const category of ["cachedInputTokens","reasoningOutputTokens"] as const) await test("invalid_marginal_category_"+category,async f=>{
  await f.send(baseline);await f.send(positive);const before=f.checkpoint();
  const invalid={...positive,observationSeq:3,previousDigest:liveSha256(canonicalJson(positive)),total:{...positive.total,[category]:positive.total[category]+1}};
  assert.equal((await f.send(invalid)).body.disposition,"counter_reset");assert.deepEqual(f.checkpoint(),before);
});
await test("gap_receipt_failure_retains_prior_checkpoint_and_retries_exactly",async f=>{
  await f.send(baseline);const before=f.checkpoint();f.buffer.database.exec("create trigger fail_gap before insert on codex_live_receipts begin select raise(abort,'synthetic'); end");
  const p=goldens[6].packet;assert.equal((await f.send(p)).body.disposition,"retryable");assert.deepEqual(f.checkpoint(),before);
  assert.equal((f.buffer.database.prepare("select held_reason from codex_live_attachments").get() as any).held_reason,null);
  f.buffer.database.exec("drop trigger fail_gap");assert.equal((await f.send(p)).text,goldens[6].receiptCanonicalUtf8);
  assert.equal((await f.send(p)).body.replayed,true);
});
await test("actual_http_parser_rejects_duplicate_content_length",async f=>{
  const body=canonicalJson(baseline);
  const reply=await new Promise<string>((resolve,reject)=>{
    const socket=net.connect(f.port,"127.0.0.1",()=>socket.end(`POST /hooks/codex HTTP/1.1\r\nHost: 127.0.0.1:${f.port}\r\nConnection: close\r\nContent-Type: application/json\r\nX-Plimsoll-Producer-Id: ${baseline.producerId}\r\nX-Plimsoll-Token: ${f.token}\r\nContent-Length: ${body.length}\r\nContent-Length: ${body.length}\r\n\r\n${body}`));
    let data="";socket.on("data",c=>{data+=c.toString();});socket.on("end",()=>resolve(data));socket.on("error",reject);socket.setTimeout(5000,()=>socket.destroy(new Error("raw_parser_timeout")));
  });assert.match(reply,/^HTTP\/1.1 400/);assert.equal(f.count("codex_live_receipts"),0);
});
await test("sqlite_writer_contention_is_retryable_without_false_ack",async f=>{
  const other=new Database(f.file);other.exec("begin immediate");
  try {
    const start=performance.now(),r=await f.send(baseline);assert.equal(r.status,503);assert.equal(r.body.disposition,"retryable");assert.equal(r.body.committed,false);
    assert.ok(performance.now()-start<1500);assert.equal(f.count("codex_live_receipts"),0);assert.equal(f.count("codex_live_pins"),0);
    assert.equal((await request(f.port,"",{},"/healthz","GET")).status,200);
  } finally {other.exec("rollback");other.close();}
  assert.equal((await f.send(baseline)).body.disposition,"baseline_only");
});
await test("live_body_deadline_is_bounded_and_leaves_no_ledger_state",async f=>{
  const result=await new Promise<Result>((resolve,reject)=>{
    const req=http.request({host:"127.0.0.1",port:f.port,path:"/hooks/codex",method:"POST",headers:{...f.headers(),"content-type":"application/json","content-length":"100",connection:"close"}},res=>{
      const chunks:Buffer[]=[];res.on("data",c=>chunks.push(c));res.on("end",()=>{const text=Buffer.concat(chunks).toString();resolve({status:res.statusCode!,text,body:JSON.parse(text)});req.destroy();});
    });req.on("error",reject);req.setTimeout(4000,()=>req.destroy(new Error("deadline_not_enforced")));req.write("{");
  });assert.equal(result.status,408);assert.equal(f.count("codex_live_receipts"),0);assert.equal(f.count("codex_live_pins"),0);
});
completion.complete();
}
void main().catch(error => { console.error(error); process.exitCode=1; });
