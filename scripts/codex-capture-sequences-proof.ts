import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { buildIngestBatch } from "../packages/collector-cli/src/upload";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline, sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
import { planCaptureHistory, applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot, withReader } from "./lib/legacy-reader";

/** One session, one economic response, two independent diagnostic signals.
 * The oracle knows the PRODUCER facts and first frozen wire observations. It
 * never calls capture, pairing or authority helpers to calculate expectations.
 * A lease is a potentially accepted request; retransmission of the same ID is
 * harmless, whereas two named IDs for the one response are a duplicate.
 * A terminal rejection parks usage for explicit replay, rather than losing it.
 */
const OPS = ["sse-valid", "sse-invalid", "sse-conflicting", "trace-sol", "trace-astra", "span",
  "rollout-turn", "rollout-no-turn", "neighbour-turn", "history-turn", "history-no-turn",
  "lease", "ack", "expire-retry", "retry", "remote-terminal", "replay", "restamp", "gap-seal",
  "stateless-build", "reopen", "rollback-047", "rollback-048", "upgrade-047", "upgrade-048"] as const;
type Op = typeof OPS[number];
type Version = "head" | "0.7.47" | "0.7.48";
type Frozen = { id: string; rawId: string; bytes: string; event: any; named: boolean; gap: boolean };
const MODEL = "gpt-6.1-sol", OTHER = "gpt-6-astra";
const SESSION = "22222222-2222-4222-8222-222222222222";
const WORKSPACE = "11111111-1111-4111-8111-111111111111";
const AT = Date.now() - 900_000;
const root = proofTempRoot("capture-sequences");
const fixtureDirectory = path.join(process.cwd(),"scripts/fixtures/codex-capture-sequences");
const seeds = Number(process.argv.find(arg=>arg.startsWith("--seeds="))?.slice(8) ?? 200);
assert.ok(Number.isSafeInteger(seeds) && seeds >= 200 && seeds <= 2000,"pin at least 200 bounded seeds");
const caseFile = process.argv.find(arg=>arg.startsWith("--case="))?.slice(7);
const directed = caseFile ? [{file:path.basename(caseFile),...JSON.parse(fs.readFileSync(caseFile,"utf8"))}] : fs.existsSync(fixtureDirectory) ? fs.readdirSync(fixtureDirectory).filter(f=>f.endsWith(".json"))
  .sort().map(file=>({file,...JSON.parse(fs.readFileSync(path.join(fixtureDirectory,file),"utf8"))})) : [];
const completion = createProofCompletion("codex-capture-sequences",(caseFile?0:seeds)+directed.length);
let Old047: any, Old048: any, runIndex = 0;
const coverage = new Map<Op,number>(OPS.map(op=>[op,0]));
let steps = 0, rollbackLeases = 0, terminalReplays = 0, historyImports = 0, historyRefusals = 0, pairingChecks = 0;

function random(seed: number) {
  let value = (seed+1) >>> 0;
  return () => { value ^= value<<13; value ^= value>>>17; value ^= value<<5; return (value>>>0)/4294967296; };
}
function sequence(seed: number): Op[] {
  const rand=random(seed); const result=[...OPS];
  for(let i=result.length-1;i>0;i--){const j=Math.floor(rand()*(i+1));[result[i],result[j]]=[result[j]!,result[i]!];}
  // Further draws revisit already-frozen, ACKed and terminal states.
  for(let i=0;i<16;i++) result.push(OPS[Math.floor(rand()*OPS.length)]!);
  return result;
}
const attribute = (key: string,value: string|number) => ({key,value:typeof value==="number"?{intValue:String(value)}:{stringValue:value}});
function log(model: string,trace: string,at: number,input?: number,output?: number,contradict=false) {
  return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attribute("service.name","codex-app-server")]},
    scopeLogs:[{logRecords:[{timeUnixNano:String(BigInt(at)*1000000n),traceId:trace,
      attributes:[attribute("event.name","codex.sse_event"),attribute("conversation.id",SESSION),attribute("model",model),
        ...(input===undefined?[]:[attribute("input_token_count",input)]),
        ...(output===undefined?[]:[attribute("output_token_count",output)]),
        ...(contradict?[attribute("gen_ai.request.model",OTHER)]:[])]}]}]}]},
    {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
}
function responseSpan(trace: string) {
  return explodeOtlpPayload({resourceSpans:[{resource:{attributes:[attribute("service.name","codex-app-server")]},
    scopeSpans:[{spans:[{name:"handle_responses",traceId:trace,spanId:"1".repeat(16),
      startTimeUnixNano:String(BigInt(AT+5000)*1000000n),endTimeUnixNano:String(BigInt(AT+6000)*1000000n),
      attributes:[attribute("conversation.id",SESSION),attribute("gen_ai.usage.input_tokens",19),
        attribute("gen_ai.usage.output_tokens",2)]}]}]}]},{source:"codex",transportPath:"/v1/traces"}).events[0]!.event;
}
function financial(e: any) { return [e.inputTokens,e.outputTokens,e.cacheReadTokens,e.cacheCreationTokens,e.costUsd].some(v=>v!==undefined); }
function gap(e: any) { return e.metadata?.usageSource==="capture_gap" || e.metadata?.captureGap===true; }
function unit(e: any) { return e.inputTokens===19 && e.outputTokens===2; }
function hasTable(db: any,name: string) { return Boolean(db.prepare("select 1 from sqlite_master where type='table' and name=?").get(name)); }
function invariant(ok: unknown,tag: string,detail: unknown) { assert.ok(ok,tag+": "+JSON.stringify(detail)); }

class World {
  dir = path.join(root,String(++runIndex)); file = path.join(this.dir,"ledger.sqlite");
  sessions = path.join(this.dir,"sessions"); rolloutFile = "";
  trace = "a".repeat(32); gapTrace="c".repeat(32);
  now = new Date(AT+2000); b: any; version: Version="head";
  frozen = new Map<string,Frozen>(); gapRaw = new Set<string>();
  nativeKinds = new Map<string,"trace"|"turn"|"invalid">();
  nativeModels = new Set<string>();
  // The first complete file response establishes its counter/turn knowledge.
  // Later context-only sightings have no new marginal counters to deliver.
  fileWritten = false; fileHasTurn = false; nativeObserved = false;
  currentLease: any = {leaseId:"no-lease",items:[]};
  rootIdentity: any;
  constructor() { fs.mkdirSync(this.dir,{recursive:true});this.open("head"); }
  opts() { return {workspaceId:WORKSPACE,deviceId:"sequence-device",enrollmentNow:()=>new Date(AT-10_000_000),
    delivery:{enabled:true,now:()=>this.now}}; }
  open(version: Version) {
    this.b?.close(); const Reader=version==="head"?LocalEventBuffer:version==="0.7.47"?Old047:Old048;
    this.b=new Reader(this.file,this.opts());this.version=version;
  }
  head() { if(this.version!=="head") this.open("head"); }
  advance(ms=123_000) { this.now=new Date(this.now.getTime()+ms); }
  observe(lease: any,transportLease=true) {
    for(const item of lease.items) {
      const e=item.envelope.event, previous=this.frozen.get(item.deliveryId);
      if(previous) invariant(item.envelopeJson===previous.bytes,"I2_BYTES",{id:item.deliveryId,version:this.version});
      if(gap(e)) {
        invariant(!financial(e)&&e.model===undefined,"I1_GAP_COUNTERS",e);
        this.gapRaw.add(item.rawId);
      } else if(financial(e)) {
        invariant(typeof e.model==="string"&&!!e.model,"I1_MODEL_MISSING",e);
        if(!previous && unit(e)) {
          const nativeTurn=e.metadata.usageSource==="rollout" && this.fileHasTurn;
          invariant(nativeTurn || (this.nativeModels.size===1 && this.nativeModels.has(e.model)),"I1_NATIVE_FACTS",e);
        }
      }
      if(this.gapRaw.has(item.rawId)) invariant(gap(e)||!financial(e),"I3_GAP_REGAINS_USAGE",e);
      if(!previous) this.frozen.set(item.deliveryId,{id:item.deliveryId,rawId:item.rawId,bytes:item.envelopeJson,event:e,
        named:!gap(e)&&unit(e),gap:gap(e)});
    }
    if(transportLease)this.currentLease=lease;
  }
  lease(expire=true) { if(expire)this.advance();const result=this.b.delivery.lease({now:this.now});this.observe(result);return result; }
  async nativeFile(withTurn: boolean) {
    this.head();
    if(!this.fileWritten) {
      const day=path.join(this.sessions,...new Date(AT).toISOString().slice(0,10).split("-"));fs.mkdirSync(day,{recursive:true});
      this.rolloutFile=path.join(day,`rollout-sequence-${SESSION}.jsonl`);
      const row=(at:number,type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+at).toISOString(),type,payload});
      fs.writeFileSync(this.rolloutFile,[row(5000,"session_meta",{id:SESSION}),
        row(5000,"turn_context",{model:MODEL,...(withTurn?{turn_id:"unit-turn"}:{})}),
        row(5000,"event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:0,output_tokens:0,cached_input_tokens:0}}}),
        row(6000,"event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:19,output_tokens:2,cached_input_tokens:0}}})].join("\n")+"\n");
      this.fileWritten=true;this.fileHasTurn=withTurn;
    }
  }
  async tail(withTurn: boolean) {
    await this.nativeFile(withTurn);
    const t=new RolloutTailer(this.b,this.sessions,()=>[]);
    try { const result=await t.scan({scope:"full",now:this.now});
      invariant(result.parseErrors===0,"PRODUCER_PARSE",result);
      if(this.fileHasTurn) this.nativeObserved=true;
      pairingChecks++;
    } finally {t.close();}
  }
  async history(withTurn: boolean) {
    await this.nativeFile(withTurn);
    if(this.now.getTime()<AT+64_000)this.now=new Date(AT+64_000);
    if(!this.rootIdentity) {
      const db=this.b.database;
      this.rootIdentity={...deriveCaptureRootIdentity("sequence","codex",this.sessions),directory:this.sessions,source:"codex",
        installationEpochId:this.b.workspaceBinding().currentInstallationEpochId};
      const start=beginAutomaticCaptureBaseline(db,"codex",{startedAt:new Date(AT-2000).toISOString(),filesDiscovered:0});
      completeAutomaticCaptureBaseline(db,"codex",{runId:start.latestRun!.runId,completedAt:new Date(AT-1000).toISOString()});
      const stat=fs.statSync(this.rolloutFile,{bigint:true});
      sealCaptureBaselineGenerations(db,"codex",[{path:this.rolloutFile,device:stat.dev,inode:stat.ino,size:stat.size,birthtimeNs:stat.birthtimeNs}],this.now.toISOString());
    }
    const financialState=()=>JSON.stringify([
      this.b.database.prepare("select id,payload_json,input_tokens,output_tokens,usage_duplicate_reason from buffered_events order by id").all(),
      this.b.database.prepare("select * from upload_outbox order by delivery_id").all(),
      this.b.database.prepare("select * from upload_receipts order by delivery_id").all(),
    ]);
    const before=financialState();
    try {
      const plan=await planCaptureHistory(this.b.database,this.rootIdentity);
      invariant(plan.missingRows<=1,"HISTORY_PREVIEW",plan);
      const receipt=await applyCaptureHistory(this.b,this.rootIdentity);
      invariant(receipt.importedRows<=1,"HISTORY_IMPORT",receipt);historyImports+=receipt.importedRows;
    } catch(error) {
      // A paired native row can retain its original payload while its ledger
      // counters are suppressed. The public importer refuses that mismatch.
      // Refusal is an executed operation, and must preserve financial state.
      if(String(error)!=="Error: capture_history_refused:existing_event_conflict")throw error;
      invariant(financialState()===before,"HISTORY_REFUSAL_CHANGED_USAGE",String(error));historyRefusals++;
    }
    if(this.fileHasTurn)this.nativeObserved=true;
  }
  async operate(op: Op) {
    if(["sse-valid","sse-invalid","sse-conflicting","trace-sol","trace-astra","span","rollout-turn","rollout-no-turn",
      "neighbour-turn","history-turn","history-no-turn","gap-seal","stateless-build","upgrade-047","upgrade-048"].includes(op)) this.head();
    switch(op) {
      case "sse-valid": {
        const e=log(MODEL,this.trace,AT+6000,19,2);this.nativeKinds.set(e.id,"trace");
        const accepted=this.b.append(e);if(accepted)this.nativeModels.add(MODEL);break;
      }
      case "sse-invalid": case "sse-conflicting": {
        // Two explicit model aliases are permanently contradictory native
        // facts, even after a gap ACK. Counters describe a diagnostic signal.
        const e=log(MODEL,op==="sse-invalid"?this.gapTrace:this.trace,AT+7000,7,1,true);
        this.nativeKinds.set(e.id,"invalid");const accepted=this.b.append(e);
        if(accepted&&op==="sse-conflicting"){this.nativeModels.add(MODEL);this.nativeModels.add(OTHER);}break;
      }
      case "trace-sol": case "trace-astra": {
        // The world has one Sol response. The Astra operation contradicts
        // that SAME trace; an Astra-only trace and a Sol rollout would be
        // different eligible responses, not a proven duplicate by counts.
        if(op==="trace-astra") {
          if(this.b.append(log(MODEL,this.trace,AT+8000)))this.nativeModels.add(MODEL);
        }
        const model=op==="trace-sol"?MODEL:OTHER;const e=log(model,this.trace,AT+(op==="trace-sol"?8000:9000));
        if(this.b.append(e))this.nativeModels.add(model);break;
      }
      case "span": { const e=responseSpan(this.trace);this.nativeKinds.set(e.id,"trace");this.b.append(e);break; }
      case "rollout-turn": await this.tail(true);break;
      case "rollout-no-turn": await this.tail(false);break;
      case "history-turn": await this.history(true);break;
      case "history-no-turn": await this.history(false);break;
      case "neighbour-turn": {
        await this.nativeFile(true);
        fs.appendFileSync(this.rolloutFile,JSON.stringify({timestamp:new Date(AT+10000).toISOString(),type:"turn_context",
          payload:{turn_id:"neighbour-turn",model:OTHER}})+"\n");await this.tail(this.fileHasTurn);break;
      }
      case "lease": this.lease(false);break;
      case "expire-retry": this.lease();break;
      case "retry": {
        const lease=this.currentLease;
        // Real retry API also executes when all IDs are stale (an assertion
        // of refusal), rather than skipping an empty operation.
        this.b.delivery.retry(lease.leaseId,lease.items,"remote_transient",this.now);
        this.advance(10_000);this.lease(false);break;
      }
      case "ack": {
        const lease=this.currentLease;
        this.b.delivery.acknowledge(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),this.now);break;
      }
      case "remote-terminal": {
        const lease=this.currentLease;
        this.b.delivery.deadLetterRemote(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),this.now);break;
      }
      case "replay": {
        const result=this.b.delivery.replayDeadLetters({reason:"remote_validation_rejected",now:this.now});
        terminalReplays+=result.requeued;this.advance();this.lease(false);break;
      }
      case "restamp": {
        const rows=this.b.database.prepare("select id,payload_json as payload from buffered_events where input_tokens is not null order by rowid").all();
        for(const row of rows) {
          const value=JSON.parse(row.payload),wasFrozen=[...this.frozen.values()].some(f=>f.rawId===row.id);
          const changed=this.b.delivery.restampUnsentRaw(row.id,JSON.stringify({...value,metadata:{...value.metadata,
            workItemId:"44444444-4444-4444-8444-444444444444"}}));
          invariant(!wasFrozen || !changed,"I2_RESTAMP",{id:row.id,version:this.version});
        }
        if(!rows.length) invariant(this.b.delivery.restampUnsentRaw("no-such-row","{}")===false,"RESTAMP_REFUSAL",op);
        break;
      }
      case "gap-seal": {
        const bad=log(MODEL,this.gapTrace,AT+7000,7,1,true);this.nativeKinds.set(bad.id,"invalid");this.b.append(bad);
        this.lease();break;
      }
      case "stateless-build": {
        const config=collectorConfigSchema.parse({tenantId:WORKSPACE,deviceId:"sequence-device",installKey:"sequence-fixture-key"});
        const result=buildIngestBatch(config,this.b,{now:()=>this.now});
        invariant(!result.batch || result.batch.events.length===result.rows.length,"SNAPSHOT_ROWS",result.rows.length);
        if(result.batch)this.observe({items:result.batch.events.map((envelope,index)=>({
          deliveryId:envelope.event.id,rawId:result.rows[index]!.id,envelope,envelopeJson:JSON.stringify(envelope),
        }))},false);
        break;
      }
      case "reopen": this.open(this.version);break;
      case "rollback-047": case "rollback-048": {
        // Compatibility is reading head-written decisions. The old release
        // is not asked to enforce a capture rule it never implemented.
        this.head();this.lease();this.open(op==="rollback-047"?"0.7.47":"0.7.48");this.lease();rollbackLeases++;break;
      }
      case "upgrade-047": case "upgrade-048": {
        this.open(op==="upgrade-047"?"0.7.47":"0.7.48");
        // Actual old public producer, then upgrade before its unsealed usage
        // is sent. This qualifies by explicit native attributes, never a guess.
        const e=log(MODEL,this.trace,AT+6000,19,2);this.nativeKinds.set(e.id,"trace");
        if(this.b.append(e))this.nativeModels.add(MODEL);this.open("head");break;
      }
    }
    coverage.set(op,coverage.get(op)!+1);steps++;
  }
  assertStep() {
    const db=this.b.database;
    const active=db.prepare("select delivery_id as id,raw_id as rawId,sealed_envelope_json as bytes from upload_outbox").all();
    const replay=hasTable(db,"upload_replays") && (db.pragma("table_info(upload_replays)") as any[]).some(c=>c.name==="frozen_envelope_json")
      ? db.prepare("select delivery_id as id,raw_id as rawId,frozen_envelope_json as bytes from upload_replays where frozen_envelope_json is not null").all():[];
    const receipts=db.prepare("select delivery_id as id,terminal_state as state,reason from upload_receipts").all();
    const captures=hasTable(db,"codex_named_captures")
      ? db.prepare("select delivery_id as id,envelope_json as bytes from codex_named_captures").all():[];
    // A native reader may freeze coverage before the first transport lease.
    // Observe that accounting boundary as it occurs, not retrospectively
    // after later contradictory facts arrive.
    for(const row of active)if(row.bytes&&!this.frozen.has(row.id)) {
      const e=JSON.parse(row.bytes).event;
      if(financial(e)&&unit(e))invariant(this.fileHasTurn&&e.metadata.usageSource==="rollout" ||
        this.nativeModels.size===1&&this.nativeModels.has(e.model),"I1_NATIVE_FACTS_AT_FREEZE",e);
      this.frozen.set(row.id,{id:row.id,rawId:row.rawId,bytes:row.bytes,event:e,named:!gap(e)&&unit(e),gap:gap(e)});
      if(gap(e))this.gapRaw.add(row.rawId);
    }
    for(const row of [...active,...replay]) if(row.bytes) {
      const e=JSON.parse(row.bytes).event;
      invariant(!gap(e)||(!financial(e)&&e.model===undefined),"I1_SEALED_GAP",e);
      invariant(gap(e)||!financial(e)||typeof e.model==="string","I1_SEALED_MODEL",e);
      invariant(!this.gapRaw.has(row.rawId)||gap(e)||!financial(e),"I3_SEALED_REGAINS_USAGE",e);
      const frozen=this.frozen.get(row.id);
      if(frozen)invariant(row.bytes===frozen.bytes,"I2_STORED_BYTES",{id:row.id,version:this.version});
    }
    const named=[...this.frozen.values()].filter(f=>f.named);
    invariant(named.length<=1,"NO_DUPLICATE_USAGE",named.map(f=>({id:f.id,event:f.event})));
    for(const frozen of named) {
      const a=active.find((r:any)=>r.id===frozen.id),r=replay.find((r:any)=>r.id===frozen.id),receipt=receipts.find((r:any)=>r.id===frozen.id);
      const parked=receipt?.state==="dead"&&receipt.reason.startsWith("remote_")&&
        captures.some((c:any)=>c.id===frozen.id&&c.bytes===frozen.bytes);
      invariant(a || r?.bytes===frozen.bytes || receipt?.state==="acknowledged" || parked,"I2_NAMED_LOST",{id:frozen.id,version:this.version,receipts});
    }
    if(this.nativeObserved && this.fileHasTurn && !named.length) {
      // A known native file may be waiting behind the grace hold or a
      // provisional live stream, but it must have a retained eligible owner.
      const raw=db.prepare("select id,event_type as type,usage_duplicate_reason as duplicate from buffered_events where source='codex' and (input_tokens=19 and output_tokens=2)").all();
      const candidates=raw.filter((r:any)=>!r.duplicate&&!this.gapRaw.has(r.id)&&
        (r.type==="usage_rollout" || (this.nativeModels.size===1&&this.nativeModels.has(MODEL))));
      invariant(candidates.some((c:any)=>active.some((a:any)=>a.rawId===c.id)||replay.some((r:any)=>r.rawId===c.id)),
        "KNOWN_NATIVE_LOST",{version:this.version,raw,active,replay,models:[...this.nativeModels]});
    }
    // Outbox/receipt state must never describe one ID as active and terminal.
    invariant(active.every((a:any)=>!receipts.some((r:any)=>r.id===a.id)),"STATE_ACTIVE_TERMINAL",active);
  }
  async settle() {
    this.head();this.b.delivery.replayDeadLetters({reason:"remote_validation_rejected",now:this.now});
    if(this.fileWritten)await this.tail(this.fileHasTurn);
    // One pass retires unsafe legacy envelopes; its distinct gap is a second
    // real queued delivery. Drain it as well, without changing any source.
    this.lease();this.assertStep();this.lease();this.assertStep();
    if(this.nativeObserved&&this.fileHasTurn)
      invariant([...this.frozen.values()].filter(f=>f.named).length===1,"KNOWN_NATIVE_FINAL",{models:[...this.nativeModels]});
  }
  close() { this.b?.close();fs.rmSync(this.dir,{recursive:true,force:true}); }
}

async function run(operations: Op[],settle=true) {
  const world=new World();
  let index=-1;
  try {
    for(index=0;index<operations.length;index++) { await world.operate(operations[index]!);world.assertStep(); }
    if(settle)await world.settle();
    return {passed:true as const,steps:operations.length,named:[...world.frozen.values()].filter(f=>f.named).length};
  } catch(error) {
    // Persist enough producer/state evidence to diagnose an accounting error
    // after this disposable world is removed. This is observation, not an
    // oracle shortcut: expected accounting still comes from producer facts.
    const db=world.b.database;
    const snapshot={version:world.version,now:world.now.toISOString(),nativeModels:[...world.nativeModels],
      fileHasTurn:world.fileHasTurn,nativeObserved:world.nativeObserved,
      raw:db.prepare("select * from buffered_events order by rowid").all(),
      outbox:db.prepare("select * from upload_outbox order by delivery_id").all(),
      receipts:db.prepare("select * from upload_receipts order by delivery_id").all(),
      frozen:[...world.frozen.values()]};
    return {passed:false as const,index,error:String(error),snapshot};
  }
  finally { world.close(); }
}
function failureTag(error: string) { return error.match(/(?:I[123]_[A-Z_]+|NO_DUPLICATE_USAGE|KNOWN_NATIVE_[A-Z_]+|STATE_ACTIVE_TERMINAL)/)?.[0]; }
async function shrink(operations: Op[],tag: string) {
  let result=[...operations], attempts=0;
  // Deterministic deletion shrink; every attempt uses a fresh real world.
  for(let width=Math.max(1,Math.floor(result.length/2));width>=1;width=Math.floor(width/2)) {
    for(let index=0;index<result.length&&attempts<100;) {
      const candidate=[...result.slice(0,index),...result.slice(index+width)];attempts++;
      const outcome=await run(candidate);
      if(!outcome.passed&&failureTag(outcome.error)===tag) result=candidate;
      else index+=width;
    }
    if(width===1)break;
  }
  return {operations:result,attempts};
}
async function main() {
  await withReader("a60590559403cace3db7cbbda49812c9e3dbfe62",async({Buffer:B047})=>
    withReader("34d58bcd90865679e09fcbd1ee1703de5effda97",async({Buffer:B048})=>{
      Old047=B047;Old048=B048;
      for(const fixture of directed) {
        const result=await run(fixture.operations);
        invariant(result.passed,"DIRECTED_SEQUENCE",{fixture:fixture.file,result});completion.check(fixture.file);
      }
      for(let seed=0;seed<(caseFile?0:seeds);seed++) {
        const operations=sequence(seed),result=await run(operations);
        if(!result.passed) {
          const tag=failureTag(result.error);
          const evidence=path.join(process.cwd(),"evidence/codex-capture-sequences");fs.mkdirSync(evidence,{recursive:true});
          const file=path.join(evidence,`seed-${seed}-${tag??"harness"}.json`);
          // Save the full counterexample BEFORE shrinking. A proof timeout
          // during reduction must not erase the original failing sequence.
          fs.writeFileSync(file,JSON.stringify({seed,requestedSeeds:seeds,...result,operations,attempts:0},null,2)+"\n");
          const reduced=tag?await shrink(operations,tag):{operations,attempts:0};
          const counterexample={seed,requestedSeeds:seeds,...result,...reduced};
          fs.writeFileSync(file,JSON.stringify(counterexample,null,2)+"\n");
          console.error(JSON.stringify({counterexample,file},null,2));throw new Error(result.error);
        }
        completion.check(`seed-${seed}`);
      }
    }));
  if(!caseFile)invariant(OPS.every(op=>coverage.get(op)!>0),"OPERATION_COVERAGE",Object.fromEntries(coverage));
  console.log(JSON.stringify({proof:"codex-capture-sequences",seeds,directed:directed.length,steps,
    operations:Object.fromEntries(coverage),rollbackLeases,terminalReplays,historyImports,historyRefusals,pairingChecks,
    invariants:["I1 native model or tokenless gap","I2 named ID/bytes final","I3 gap never regains counters",
      "one potentially accepted named owner","known native response retained and eventually named"]},null,2));
  completion.complete();
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>fs.rmSync(root,{recursive:true,force:true}));
