import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { authenticatedJsonPost, postJson, TransportError } from "../packages/collector-cli/src/http-transport";
import { deliveryAcknowledgement, deliveryExpectation, validateDeliveryAcknowledgement } from "../packages/collector-cli/src/delivery-ack";
import { postDelivery } from "../packages/collector-cli/src/delivery-post";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-delivery-proof-"));
process.env.HOME = path.join(root, "home");
process.env.PLIMSOLL_HOME = path.join(root, "state");
fs.mkdirSync(process.env.HOME); fs.mkdirSync(process.env.PLIMSOLL_HOME);
const checks: { name: string; passed: boolean; detail?: string }[] = [];
async function check(name: string, action: () => Promise<void> | void) {
  try { await action(); checks.push({ name, passed: true }); }
  catch (error) { checks.push({ name, passed: false, detail: String(error) }); }
}
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const tenantId = uuid(1), installKey = "fixture-install", ingestKey = "fixture-key";
const rawBody = JSON.stringify({tenantId, installKey, events:[{event:{id:uuid(10)}},{event:{id:uuid(11)}}]});
const expected = deliveryExpectation(rawBody, installKey);
const valid = () => ({ ok:true, accepted:2, inserted:2, ack:deliveryAcknowledgement(expected, expected.itemIds) });
const fake = (body: unknown, status = 200) => (async()=>new Response(typeof body === "string" ? body : JSON.stringify(body),{status})) as typeof fetch;
const options = {url:"http://127.0.0.1:1/ingest",body:rawBody,installKey,ingestKey};
async function listen(server: http.Server) { await new Promise<void>(r=>server.listen(0,"127.0.0.1",r)); return `http://127.0.0.1:${(server.address() as {port:number}).port}`; }
async function close(server:http.Server) {server.closeAllConnections();await new Promise<void>(r=>server.close(()=>r()));}

async function main() {
  const { postHistoryBatch, runWorkspaceHistoryUpload } = await import("../packages/collector-cli/src/upload-history");
  const { LocalEventBuffer } = await import("../packages/collector-cli/src/buffer");
  const { collectorConfigSchema } = await import("../packages/collector-cli/src/config");
  const { uploadBufferedEvents } = await import("../packages/collector-cli/src/upload");
  const { aiInteractionEventSchema } = await import("../packages/shared/src/index");
  for (const url of ["http://example.com/ingest","http://localhost.example/ingest","http://x.localhost/ingest","https://user:password@example.com/ingest","file:///tmp/ingest"]) {
    await check(`reject_origin_${checks.length}`, async()=>{let called=false;await assert.rejects(authenticatedJsonPost({...options,url,fetchImpl:(async()=>{called=true;return new Response('{}');}) as typeof fetch}), TransportError);assert.equal(called,false);});
  }
  await check("redirect_no_key_or_body", async()=>{
    let received=0;
    const target=http.createServer((req,res)=>{received++;req.resume();res.end('{}');});const targetUrl=await listen(target);
    const origin=http.createServer((req,res)=>{req.resume();res.writeHead(307,{location:targetUrl});res.end();});
    try {
      const url=await listen(origin);
      await assert.rejects(postHistoryBatch({...options,url,fetchImpl:fetch,sleep:async()=>{},maxAttempts:1,log:()=>{}}),/redirect_rejected/);
      await assert.rejects(postDelivery({...options,url}),/redirect_rejected/);
      assert.equal(received,0);
    } finally {await close(origin);await close(target);}
  });
  await check("headers_do_not_end_deadline_and_next_cycle_runs", async()=>{
    const server=http.createServer((req,res)=>{req.resume();res.writeHead(200,{'content-type':'application/json'});res.flushHeaders();res.write('{');});
    try {const start=Date.now();await assert.rejects(postDelivery({...options,url:await listen(server),timeoutMs:80}),/deadline_exceeded/);assert.ok(Date.now()-start<1500);await postDelivery({...options,fetchImpl:fake(valid())});}finally{await close(server);}
  });
  await check("abort_ignoring_fetch_is_bounded",async()=>{await assert.rejects(postJson({...options,timeoutMs:30,fetchImpl:(()=>new Promise(()=>{})) as typeof fetch}),/deadline_exceeded/);});
  await check("abort_ignoring_body_is_bounded",async()=>{let cancelled=false;await assert.rejects(postJson({...options,timeoutMs:30,fetchImpl:(async()=>new Response(new ReadableStream({cancel(){cancelled=true;}}))) as typeof fetch}),/deadline_exceeded/);assert.equal(cancelled,true);});
  await check("declared_response_bytes_are_bounded",async()=>{await assert.rejects(postJson({...options,maxResponseBytes:100,fetchImpl:(async()=>new Response('{}',{headers:{'content-length':'101'}})) as typeof fetch}),/response_too_large/);});
  await check("streaming_response_bytes_are_bounded",async()=>{const server=http.createServer((req,res)=>{req.resume();res.writeHead(200);res.write('x'.repeat(256));res.end();});try{await assert.rejects(postJson({...options,url:await listen(server),maxResponseBytes:100}),/response_too_large/);}finally{await close(server);}});
  await check("request_bytes_are_bounded_before_fetch",async()=>{let called=false;await assert.rejects(postJson({...options,maxRequestBytes:1,fetchImpl:(async()=>{called=true;return new Response('{}');}) as typeof fetch}),/request_too_large/);assert.equal(called,false);});
  const invalid: [string,unknown][] = [
    ['missing',{}],['zero',{accepted:0}],['malformed','{'],['wrong_version',{...valid(),ack:{...valid().ack,version:2}}],
    ['partial',{...valid(),ack:{...valid().ack,acceptedIds:expected.itemIds.slice(0,1)}}],
    ['duplicate_identity',{...valid(),ack:{...valid().ack,acceptedIds:[expected.itemIds[0],expected.itemIds[0]]}}],
    ['foreign_identity',{...valid(),ack:{...valid().ack,acceptedIds:[expected.itemIds[0],'foreign']}}],
    ['rejected',{...valid(),ack:{...valid().ack,rejectedIds:[expected.itemIds[0]]}}],
    ['wrong_request',{...valid(),ack:{...valid().ack,requestDigest:'sha256:wrong'}}],
    ['wrong_scope',{...valid(),ack:{...valid().ack,scopeDigest:'sha256:wrong'}}],
    ['wrong_kind',{...valid(),ack:{...valid().ack,kind:'sessions'}}],
    ['contradictory_count',{...valid(),accepted:0}],['contradictory_ok',{...valid(),ok:false}],
    ['contradictory_error',{...valid(),error:'fixture-secret'}],['negative_inserted',{...valid(),inserted:-1}],
    ['fractional_inserted',{...valid(),inserted:0.5}],['excess_inserted',{...valid(),inserted:3}],
  ];
  for (const [name,body] of invalid) await check(`history_rejects_${name}`, async()=>{
    await assert.rejects(postHistoryBatch({...options,fetchImpl:fake(body),sleep:async()=>{},maxAttempts:1,log:()=>{}}));
  });
  for (const [name,body] of invalid) await check(`outbox_retains_${name}`, async()=>{
    const cfg=collectorConfigSchema.parse({tenantId,installKey,uploadUrl:options.url,delivery:{requestTimeoutSeconds:1}});
    const buffer=new LocalEventBuffer(path.join(root,`${name}.sqlite`),{workspaceId:tenantId,delivery:{enabled:true,limits:cfg.delivery}});
    try {
      buffer.append(aiInteractionEventSchema.parse({id:uuid(20),source:'codex',eventType:'assistant_response',observedAt:new Date().toISOString(),inputTokens:7}));
      await assert.rejects(uploadBufferedEvents(cfg,buffer,{fetchImpl:(async(_url,init)=>{
        if (typeof body !== "object" || body === null || !("ack" in body)) return new Response(typeof body === 'string'?body:JSON.stringify(body));
        const exp=deliveryExpectation(String(init?.body),installKey);
        const base=valid();const broken=structuredClone(body) as typeof base;
        const ack={...deliveryAcknowledgement(exp,exp.itemIds)};
        for (const key of Object.keys(broken.ack) as (keyof typeof ack)[]) {
          if(JSON.stringify(broken.ack[key])!==JSON.stringify(base.ack[key])) (ack as Record<string,unknown>)[key]=broken.ack[key];
        }
        const result={...broken,accepted:broken.accepted===2?1:broken.accepted,inserted:broken.inserted===2?1:broken.inserted,ack};
        return new Response(JSON.stringify(result));
      }) as typeof fetch}));
      assert.equal(buffer.delivery.status().remainingDelivery,1);assert.equal(buffer.stats().unuploadedCount,1);
      assert.equal((buffer.database.prepare('select count(*) as n from upload_outbox').get() as {n:number}).n,1);
    } finally {buffer.close();}
  });
  await check('symbolic_refusal_and_network_diagnostics',async()=>{
    const log:string[]=[];
    for(const fetchImpl of [fake({error:'fixture-private-error'},401),(async()=>{throw new Error('fixture-private-network');}) as typeof fetch]){
      try{await postHistoryBatch({...options,fetchImpl,sleep:async()=>{},maxAttempts:2,log:s=>log.push(s)});}catch(error){log.push(String(error));}
    }
    assert.equal(log.join('').includes('fixture-private'),false);
  });
  await check('all_delivery_kinds_bound_to_content_and_scope',()=>{
    const batches=[JSON.parse(rawBody),{tenantId,installKey,kind:'session_sync',sessions:[{session:{id:uuid(40)},totals:{events:1}}]},
      {tenantId,installKey,kind:'attribution_repair',rows:[{id:uuid(10),projectKey:'sha256:fixture'}]},
      {tenantId,artifacts:[{id:'artifact-1'}],outcomes:[{id:'outcome-1'}]}];
    for(const batch of batches){const body=JSON.stringify(batch), exp=deliveryExpectation(body,installKey);const response={ack:deliveryAcknowledgement(exp,exp.itemIds)};validateDeliveryAcknowledgement(response,exp);assert.throws(()=>validateDeliveryAcknowledgement(response,deliveryExpectation(body+' ',installKey)));assert.throws(()=>validateDeliveryAcknowledgement(response,{...exp,scopeDigest:'foreign'}));}
  });
  await check('session_sync_retains_source_on_missing_ack',async()=>{
    const { runSessionSync } = await import("../packages/collector-cli/src/session-sync");
    const cfg=collectorConfigSchema.parse({tenantId,installKey,uploadUrl:options.url});
    const buffer=new LocalEventBuffer(path.join(root,'sessions.sqlite'),{workspaceId:tenantId});
    try {
      buffer.append(aiInteractionEventSchema.parse({id:uuid(80),sessionId:uuid(81),source:'codex',eventType:'assistant_response',observedAt:new Date().toISOString(),inputTokens:7}));
      const result=await runSessionSync(cfg,{ledgerDb:buffer.database,fetchImpl:fake({}),sleep:async()=>{},delayMs:0,maxAttemptsPerBatch:1,log:()=>{}});
      assert.equal(result.ok,false);assert.equal(result.acceptedSessions,0);assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as {n:number}).n,1);
    }finally{buffer.close();}
  });
  for (const mode of ['valid','missing','contradictory','network'] as const) await check(`outcomes_sync_${mode}`,async()=>{
    const { runOutcomesSync } = await import("../packages/collector-cli/src/outcomes-sync");
    const { remoteLinkageHash } = await import("../packages/shared/src/index");
    const cfg=collectorConfigSchema.parse({tenantId,installKey,uploadUrl:options.url});
    const buffer=new LocalEventBuffer(path.join(root,`outcomes-${mode}.sqlite`),{workspaceId:tenantId});
    const now=new Date().toISOString(), sha='a'.repeat(40);const logs:string[]=[];
    try {
      buffer.append(aiInteractionEventSchema.parse({id:uuid(90),sessionId:uuid(91),source:'codex',eventType:'assistant_response',observedAt:now,inputTokens:7}));
      buffer.database.prepare('update buffered_events set repo_hash = ?, head_sha = ?').run(remoteLinkageHash('https://github.com/fixture/outcomes.git'),sha);
      const fetchImpl:typeof fetch=async(input,init)=>{
        const url=new URL(String(input));
        if(url.hostname==='api.github.com'){
          const data=url.pathname.endsWith('/pulls')?[{number:1,state:'closed',merged_at:now,updated_at:now,merge_commit_sha:'b'.repeat(40),head:{sha,ref:'main'}}]:url.pathname.endsWith('/check-runs')?{check_runs:[]}:[];
          return new Response(JSON.stringify(data));
        }
        if(mode==='network')throw new Error('fixture-private-network');
        if(mode==='missing')return new Response('{}');
        const raw=String(init?.body),payload=JSON.parse(raw),exp=deliveryExpectation(raw,installKey);
        return new Response(JSON.stringify({ok:true,acceptedArtifacts:mode==='contradictory'?0:payload.artifacts.length,acceptedOutcomes:payload.outcomes.length,detachedActorRefs:0,detachedSessionRefs:0,ack:deliveryAcknowledgement(exp,exp.itemIds)}));
      };
      const result=await runOutcomesSync(cfg,{repository:'fixture/outcomes',ledgerDb:buffer.database,fetchImpl,log:line=>logs.push(line)});
      assert.ok(result.artifactsSent>0);assert.equal(result.ok,mode==='valid');assert.equal(logs.join('').includes('fixture-private-network'),false);
      if(mode!=='valid'){assert.equal(result.artifactsAccepted,null);assert.equal(result.outcomesAccepted,null);}
    }finally{buffer.close();}
  });
  await check('history_watermark_retained_on_missing_ack',async()=>{
    const ledger=path.join(root,'history.sqlite');const buffer=new LocalEventBuffer(ledger,{workspaceId:tenantId});
    buffer.append(aiInteractionEventSchema.parse({id:uuid(70),source:'codex',eventType:'assistant_response',observedAt:new Date().toISOString(),inputTokens:7}));buffer.close();
    const statePath=path.join(root,'history-state.json');const cfg=collectorConfigSchema.parse({tenantId,installKey,uploadUrl:options.url});
    const result=await runWorkspaceHistoryUpload(cfg,{ledgerPath:ledger,statePath,fetchImpl:fake({}),sleep:async()=>{},delayMs:0,maxAttemptsPerBatch:1,log:()=>{}});
    assert.equal(result.ok,false);const state=JSON.parse(fs.readFileSync(statePath,'utf8'));assert.equal(state.watermark,null);
  });
}
let completed=false;
const watchdog=setTimeout(()=>{console.error('delivery proof did not complete');process.exit(1);},45_000);
main().then(()=>{completed=true;}).catch(error=>{checks.push({name:'proof_execution',passed:false,detail:String(error)});}).finally(()=>{
  clearTimeout(watchdog);
  const receipt={schema:'plimsoll.delivery-transport-proof/v1',complete:completed,passed:checks.filter(c=>c.passed).length,failed:checks.filter(c=>!c.passed).length,checks};
  console.log(JSON.stringify(receipt,null,2));if(!completed||receipt.failed)process.exitCode=1;
  fs.rmSync(root,{recursive:true,force:true});
});
