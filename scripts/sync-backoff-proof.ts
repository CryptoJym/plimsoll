import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { useFixtureRoot } from "./lib/fixture-root";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { uploadBufferedEvents, DeliveryUploadError } from "../packages/collector-cli/src/upload";
import { postJson, TransportError } from "../packages/collector-cli/src/http-transport";
import { SyncBackoff } from "../packages/collector-cli/src/sync-backoff";
import { retryAfterMilliseconds } from "../packages/collector-cli/src/retry-after";
import { createCollectorServer } from "../packages/collector-cli/src/server";

const fixture = useFixtureRoot(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-sync-backoff-")));
const checks: Array<{name: string; passed: boolean; detail?: string}> = [];
// The injected delivery clock must follow real SQLite insertion timestamps.
const t0 = Math.ceil(Date.now() / 1_000) * 1_000 + 60_000, interval = 300_000;
let sequence = 0, database = 0;
const config = collectorConfigSchema.parse({ uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId: "00000000-0000-4000-8000-000000000001", installKey: "sync-backoff-fixture",
  delivery: { maxOldestAgeDays: 3650, maxBackoffSeconds: 30, requestTimeoutSeconds: 1, maxProbesPerCycle: 16 },
});
function open(file = path.join(fixture.root, `ledger-${++database}.sqlite`)) {
  return { file, buffer: new LocalEventBuffer(file, { workspaceId: config.tenantId, delivery: { enabled: true, limits: config.delivery } }) };
}
function append(buffer: LocalEventBuffer) {
  const event = aiInteractionEventSchema.parse({ id: `00000000-0000-4000-8000-${String(++sequence).padStart(12,"0")}`,
    source: "codex", dataMode: "metadata", eventType: "assistant_response", observedAt: new Date(t0 - 60_000).toISOString(), inputTokens: 1, outputTokens: 1, metadata: {} });
  buffer.append(event); return event.id;
}
const json = (status: number, retry?: string) => new Response(JSON.stringify(status === 200 ? { accepted: 1 } : {}), { status,
  headers: { "content-type": "application/json", ...(retry ? { "retry-after": retry } : {}) } });
async function check(name: string, fn: () => unknown | Promise<unknown>) {
  try { await fn(); checks.push({ name, passed: true }); }
  catch (error) { checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) }); }
}
async function main() {
  await check("partial_cycle_success_resets_prior_outage_streak", () => {
    const state = new SyncBackoff(interval); state.arm(t0);
    const error = new DeliveryUploadError("remote_transient", "network", 0, "ECONNRESET");
    for (let i = 0; i < 3; i++) state.failure(error, 0, t0);
    const result = state.failure(error, 500, t0);
    assert.equal(result.failureStreak, 0); assert.equal(result.backoffMs, 0);
    assert.equal(state.ready(t0), true); assert.equal(state.status(false,t0).nextAttemptAt, new Date(t0+interval).toISOString());
    assert.equal(state.status(false,t0).lastError?.code,"ECONNRESET");
  });
  await check("complete_remote_outage_still_escalates_and_caps", () => {
    const state = new SyncBackoff(interval); const error = new DeliveryUploadError("remote_transient", "remote_transient");
    for (const delay of [600_000,1_200_000,2_400_000,3_600_000,3_600_000]) assert.equal(state.failure(error,0,t0).backoffMs,delay);
    assert.equal(state.ready(t0+3_599_999),false); assert.equal(state.ready(t0+3_600_000),true);
  });
  await check("local_storage_pressure_never_inherits_remote_exponential_pause", () => {
    const state=new SyncBackoff(interval);state.failure(new Error("PRIVATE_FAILURE"),0,t0);
    const result=state.failure(Object.assign(new Error("PRIVATE_BUSY"),{code:"SQLITE_BUSY"}),0,t0);
    assert.equal(result.failureStreak,0);assert.equal(result.backoffMs,0);assert.equal(result.error.code,"local_storage_busy");
  });
  await check("server_retry_after_survives_partial_success", () => {
    const state=new SyncBackoff(interval);state.arm(t0);
    const result=state.failure(new DeliveryUploadError("remote_transient","remote_transient",900_000),500,t0);
    assert.equal(result.failureStreak,0);assert.equal(result.backoffMs,900_000);
    assert.equal(state.ready(t0+899_999),false);assert.equal(state.ready(t0+900_000),true);
  });
  await check("partial_result_can_carry_server_cooldown_without_throwing", () => {
    const state=new SyncBackoff(interval);state.arm(t0);state.success(1,900_000,t0);
    assert.equal(state.ready(t0+interval),false);assert.equal(state.status(false,t0).notBefore,new Date(t0+900_000).toISOString());
  });
  await check("network_timeout_during_maintenance_pressure_keeps_regular_cadence", () => {
    const state=new SyncBackoff(interval);
    const result=state.failure(new DeliveryUploadError("remote_transient","network",0,"ETIMEDOUT"),0,t0,true);
    assert.equal(result.failureStreak,0);assert.equal(result.backoffMs,0);
    assert.equal(result.localPressure,true);assert.equal(result.error.code,"ETIMEDOUT");
  });
  await check("real_server_refusal_is_not_dismissed_by_local_pressure", () => {
    const state=new SyncBackoff(interval);
    const result=state.failure(new DeliveryUploadError("remote_transient","remote_transient",900_000),0,t0,true);
    assert.equal(result.failureStreak,1);assert.equal(result.backoffMs,900_000);assert.equal(result.localPressure,false);
  });
  await check("status_has_no_raw_exception_message", () => {
    const state=new SyncBackoff(interval);state.failure(new Error("PRIVATE_URL_TOKEN_FIXTURE"),0,t0);
    assert.ok(!JSON.stringify(state.status(false,t0)).includes("PRIVATE_URL"));assert.equal(state.status(false,t0).lastError?.code,"unclassified");
    assert.equal(state.status(true,t0).nextAttemptAt,null);
  });
  for(const [header,expected] of [["5",5000],["0",0],["900",900000],[new Date(t0+60000).toUTCString(),60000],[new Date(t0-10000).toUTCString(),0],["-1",0],["1.5",0],["garbage",0],["999999999999999999999999999999",0],[null,0]] as const) {
    await check(`retry_after_${String(header)}`,()=>assert.equal(retryAfterMilliseconds(header,t0),expected));
  }
  await check("network_code_is_preserved_without_private_error_text",async()=>{
    await assert.rejects(postJson({url:config.uploadUrl!,body:"{}",fetchImpl:(async()=>{throw Object.assign(new Error("PRIVATE_NETWORK_LOCATION"),{cause:{code:"ECONNRESET"}});}) as typeof fetch}),error=>{
      assert.ok(error instanceof TransportError);assert.equal(error.code,"network_error");assert.equal(error.networkCode,"ECONNRESET");assert.ok(!JSON.stringify(error).includes("PRIVATE_NETWORK_LOCATION"));return true;
    });
  });
  await check("real_upload_progress_then_failure_retries_at_regular_cadence",async()=>{
    const {buffer}=open();let now=t0;let calls=0;const state=new SyncBackoff(interval);state.arm(t0);
    try {
      for(let cycle=0;cycle<3;cycle++) {
        append(buffer);append(buffer);
        const fetchImpl=acknowledgingFetch(async()=>{calls++;if(calls%3===2)throw Object.assign(new Error("PRIVATE_NETWORK"),{cause:{code:"ECONNRESET"}});return json(200);});
        const opts={limit:1,fetchImpl,now:()=>new Date(now),includeLegacyRemainingUnuploaded:false};
        const first=await uploadBufferedEvents(config,buffer,opts);assert.equal(first.uploadedEvents,1,JSON.stringify({uploaded:first.uploadedEvents,response:first.response,delivery:first.delivery}));
        await assert.rejects(uploadBufferedEvents(config,buffer,opts),error=>{
          assert.ok(error instanceof DeliveryUploadError);assert.equal(error.networkCode,"ECONNRESET");
          assert.equal(state.failure(error,first.uploadedEvents,now).backoffMs,0);return true;
        });
        now+=interval;assert.equal(state.ready(now),true);
        const recovered=await uploadBufferedEvents(config,buffer,opts);assert.equal(recovered.uploadedEvents,1);
      }
      assert.equal(buffer.delivery.status(new Date(now)).receipts.dead,0);
    } finally {buffer.close();}
  });
  await check("server_cooldown_is_durable_across_buffer_reopen",async()=>{
    let {buffer,file}=open();let now=t0,calls=0;append(buffer);
    const fetchImpl=acknowledgingFetch(async()=>{calls++;return calls===1?json(503,"900"):json(200);});
    const opts={limit:1,fetchImpl,now:()=>new Date(now),includeLegacyRemainingUnuploaded:false};
    try {
      await assert.rejects(uploadBufferedEvents(config,buffer,opts),error=>{assert.ok(error instanceof DeliveryUploadError);assert.equal(error.retryAfterMs,900000);return true;});
      buffer.close();buffer=open(file).buffer;
      now+=interval;const early=await uploadBufferedEvents(config,buffer,opts);assert.equal(early.uploadedEvents,0);assert.equal(calls,1);
      now=t0+900000;const accepted=await uploadBufferedEvents(config,buffer,opts);assert.equal(accepted.uploadedEvents,1);assert.equal(calls,2);
    }finally{buffer.close();}
  });
  await check("partial_single_lease_keeps_acknowledged_sibling_and_server_floor",async()=>{
    const {buffer}=open();let now=t0,calls=0;append(buffer);append(buffer);
    const fetchImpl=acknowledgingFetch(async()=>{calls++;return calls===1?json(400):calls===2?json(200):json(503,"900");});
    try {
      const result=await uploadBufferedEvents(config,buffer,{limit:2,fetchImpl,now:()=>new Date(now),includeLegacyRemainingUnuploaded:false});
      assert.equal(result.uploadedEvents,1);assert.ok("retryAfterMs" in result.delivery);assert.equal(result.delivery.retryAfterMs,900000);
      now+=interval;const before=calls;const early=await uploadBufferedEvents(config,buffer,{limit:2,fetchImpl,now:()=>new Date(now),includeLegacyRemainingUnuploaded:false});
      assert.equal(early.uploadedEvents,0);assert.equal(calls,before);
    }finally{buffer.close();}
  });
  await check("real_http_503_retry_after_blocks_reopened_ledger_until_due",async()=>{
    let {buffer,file}=open();let now=t0,calls=0;append(buffer);
    const adapter=acknowledgingFetch(async()=>{calls++;return calls===1?json(503,"900"):json(200);});
    const upstream=http.createServer((request,response)=>{
      const chunks:Buffer[]=[];
      request.on("data",chunk=>chunks.push(Buffer.from(chunk)));
      request.on("end",()=>{void (async()=>{
        const reply=await adapter("http://127.0.0.1/fixture",{body:Buffer.concat(chunks).toString("utf8")});
        response.writeHead(reply.status,Object.fromEntries(reply.headers));response.end(await reply.text());
      })().catch(()=>{response.writeHead(500,{"content-type":"application/json"});response.end("{}");});});
    });
    try{
      await new Promise<void>(resolve=>upstream.listen(0,"127.0.0.1",resolve));
      const address=upstream.address() as AddressInfo;
      const socketConfig={...config,uploadUrl:`http://127.0.0.1:${address.port}/ingest`};
      const opts={limit:1,now:()=>new Date(now),includeLegacyRemainingUnuploaded:false};
      await assert.rejects(uploadBufferedEvents(socketConfig,buffer,opts),error=>{
        assert.ok(error instanceof DeliveryUploadError);assert.equal(error.retryAfterMs,900_000);return true;
      });
      buffer.close();buffer=open(file).buffer;now+=interval;
      assert.equal((await uploadBufferedEvents(socketConfig,buffer,opts)).uploadedEvents,0);assert.equal(calls,1);
      now=t0+900_000;assert.equal((await uploadBufferedEvents(socketConfig,buffer,opts)).uploadedEvents,1);assert.equal(calls,2);
    }finally{upstream.closeAllConnections();await new Promise<void>(resolve=>upstream.close(()=>resolve()));buffer.close();}
  });
  await check("session_followups_are_carried_during_server_cooldown",()=>{
    const source=fs.readFileSync(path.resolve("packages/collector-cli/src/cli.ts"),"utf8");
    const guard="if (serverRetryAfterMs > 0) { carrySessions(); return; }";
    assert.ok(source.includes(guard));
    assert.ok(source.indexOf(guard)<source.indexOf("const touchedSessionIds ="));
  });
  await check("native_runSync_uses_actual_progress_and_cache_only_status",()=>{
    const source=fs.readFileSync(path.resolve("packages/collector-cli/src/cli.ts"),"utf8");
    assert.match(source,/syncBackoff\.failure\(error, uploaded, Date\.now\(\), maintenanceBoundary\.status\(\)\.state === "circuit_open"\)/);assert.match(source,/let uploaded = 0;[\s\S]*?try \{[\s\S]*?uploaded \+= result\.uploadedEvents/);
    assert.match(source,/syncStatus: \(\) => syncBackoff\.status\(syncInFlight\)/);
    assert.ok(!source.includes("syncFailureStreak += 1"));
  });
  await check("status_endpoint_exposes_in_memory_scheduler_without_a_ledger_read",async()=>{
    const {buffer}=open(),state=new SyncBackoff(interval);state.arm(t0);state.failure(new DeliveryUploadError("remote_transient","network",0,"ETIMEDOUT"),0,t0);
    const server=createCollectorServer(config,buffer,{syncStatus:()=>state.status(false,t0)});
    try{
      await new Promise<void>(resolve=>server.listen(0,"127.0.0.1",resolve));
      const address=server.address() as AddressInfo;const response=await fetch(`http://127.0.0.1:${address.port}/status`);
      assert.equal(response.status,200);const body=await response.json() as {sync:ReturnType<SyncBackoff["status"]>};
      assert.equal(body.sync.failureStreak,1);assert.equal(body.sync.lastError?.code,"ETIMEDOUT");
    }finally{await new Promise<void>(resolve=>server.close(()=>resolve()));buffer.close();}
  });
  for(const result of checks)console.log(`${result.passed?"PASS":"FAIL"} ${result.name}${result.detail?" "+result.detail:""}`);
  console.log(JSON.stringify({proof:"sync_backoff",checks:checks.length,passed:checks.filter(x=>x.passed).length,failed:checks.filter(x=>!x.passed).length}));
  if(checks.some(x=>!x.passed))process.exitCode=1;
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>{fixture.restore();fs.rmSync(fixture.root,{recursive:true,force:true});});
