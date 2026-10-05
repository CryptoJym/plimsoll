import assert from "node:assert/strict";
import fs from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { buildIngestBatch } from "../packages/collector-cli/src/upload";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline, sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
import { planCaptureHistory, applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { createProofCompletion } from "./lib/proof-completion";
import { proofProcessIdentity } from "./lib/proof-process-identity";
import { proofTempRoot, withReader } from "./lib/legacy-reader";

/** Multiple economic responses, cumulative turns and independent diagnostics.
 * The oracle knows the PRODUCER facts and first frozen wire observations. It
 * never calls capture, pairing or authority helpers to calculate expectations.
 * A lease is a potentially accepted request; retransmission of the same ID is
 * harmless, whereas two named IDs for the one response are a duplicate.
 * A terminal rejection parks usage for explicit replay, rather than losing it.
 */
const OPS = ["sse-valid", "sse-invalid", "sse-conflicting", "trace-sol", "trace-astra", "span",
  "rollout-turn", "rollout-no-turn", "neighbour-turn", "history-turn", "history-no-turn",
  "lease", "ack", "expire-retry", "retry", "remote-terminal", "replay", "restamp", "gap-seal",
  "stateless-build", "reopen", "rollback-047", "rollback-048", "upgrade-047", "upgrade-048", "sse-zero", "sse-partial", "sse-cache-cost",
  "second-turn", "third-turn", "history-multiple", "cache-only-turn", "sse-partial-twin", "sse-zero-complement-twin",
  "skew-partial-request", "skew-complete-request", "skew-partial-turn", "skew-complete-turn",
  "skew-growing", "skew-smaller", "skew-native", "skew-native-update", "request-only-partial", "request-only-complete", "request-only-span", "alias-request", "alias-native", "alias-bridge", "alias-chain", "call-request-input", "call-request-output", "call-complete",
  "old-acked-047-input-output", "old-acked-047-output-input", "old-acked-048-input-output", "old-acked-048-output-input", "old-acked-pair-047", "old-acked-pair-048"] as const;
type Op = typeof OPS[number] | "skew-series" | "alias-long-chain";
type Version = "head" | "0.7.47" | "0.7.48";
const FIELDS = ["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens","costUsd"] as const;
type Field = typeof FIELDS[number];
type Producer = {event:any;response:string;kind:"trace"|"turn"|"invalid"};
type Frozen = { id: string; rawId: string; bytes: string; event: any; named: boolean; gap: boolean };
const MODEL = "gpt-6.1-sol", OTHER = "gpt-6-astra";
const identityQueries=proofProcessIdentity();
const LEGACY_SESSION="44444444-4444-4444-8444-444444444444";
const ALIAS_SESSION="33333333-3333-4333-8333-333333333333";
const SESSION = "22222222-2222-4222-8222-222222222222";
const WORKSPACE = "11111111-1111-4111-8111-111111111111";
const AT = Date.now() - 900_000;
const root = proofTempRoot("capture-sequences");
const fixtureDirectory = path.join(process.cwd(),"scripts/fixtures/codex-capture-sequences");
const seeds = Number(process.argv.find(arg=>arg.startsWith("--seeds="))?.slice(8) ?? 200);
assert.ok(Number.isSafeInteger(seeds) && seeds >= 200 && seeds <= 2000,"pin at least 200 bounded seeds");
const partitionArg=process.argv.find(arg=>arg.startsWith("--partition="))?.slice(12);
const partition=partitionArg?partitionArg.split(":").map(Number):undefined;
if(partition)assert.ok(partition.length===2&&partition.every(Number.isSafeInteger)&&partition[0]!>=0&&
  partition[1]!>partition[0]!&&partition[1]!<=seeds,"bounded internal seed partition");
const workerSummary=process.argv.find(arg=>arg.startsWith("--worker-summary="))?.slice(17);
if(partition)assert.ok(workerSummary&&path.resolve(workerSummary).startsWith(process.env.PLIMSOLL_PROOF_ROOT!+path.sep),"worker summary is private");
const caseFile = process.argv.find(arg=>arg.startsWith("--case="))?.slice(7);
const directed = caseFile ? [{file:path.basename(caseFile),...JSON.parse(fs.readFileSync(caseFile,"utf8"))}] : fs.existsSync(fixtureDirectory) ? fs.readdirSync(fixtureDirectory).filter(f=>f.endsWith(".json"))
  .sort().map(file=>({file,...JSON.parse(fs.readFileSync(path.join(fixtureDirectory,file),"utf8"))})) : [];
const orderGroups: Op[][]=[
  ["skew-partial-request","skew-native","skew-complete-request"],
  ["skew-partial-turn","skew-native","skew-complete-turn"],
  ["skew-partial-request","skew-native","skew-complete-turn"],
  ["skew-partial-turn","skew-native","skew-complete-request"],
  ["skew-partial-request","skew-native","skew-growing"],
  ["skew-smaller","skew-native","skew-growing"],
  ["request-only-partial","request-only-span","request-only-complete"],
  ["skew-partial-request","skew-native-update","skew-complete-request"],
  ["alias-request","alias-native","alias-bridge"],
  ["call-request-input","call-request-output","call-complete"],
];
const completion = createProofCompletion(partition?"codex-capture-sequences-partition":"codex-capture-sequences",
  partition?partition[1]!-partition[0]!:(caseFile?0:seeds+orderGroups.length*6)+directed.length);
let Old047: any, Old048: any, runIndex = 0;
const coverage = new Map<Op,number>([...OPS,"skew-series" as const,"alias-long-chain" as const].map(op=>[op,0]));
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
function log(model: string,trace: string,at: number,input?: number,output?: number,contradict=false,cache?: number,
  identity?: {turn?:string;request?:string;call?:string},session=SESSION) {
  return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attribute("service.name","codex-app-server")]},
    scopeLogs:[{logRecords:[{timeUnixNano:String(BigInt(at)*1000000n),traceId:trace,
      attributes:[attribute("event.name","codex.sse_event"),attribute("conversation.id",session),attribute("model",model),
        ...(input===undefined?[]:[attribute("input_token_count",input)]),
        ...(output===undefined?[]:[attribute("output_token_count",output)]),
        ...(cache===undefined?[]:[attribute("cached_token_count",cache)]),
        ...(identity?.turn?[attribute("turn.id",identity.turn)]:[]),
        ...(identity?.request?[attribute("request_id",identity.request)]:[]),
        ...(identity?.call?[attribute("call_id",identity.call)]:[]),
        ...(contradict?[attribute("gen_ai.request.model",OTHER)]:[])]}]}]}]},
    {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
}
function responseSpan(trace: string,identity?: {turn?:string;request?:string},at=6000,session=SESSION) {
  return explodeOtlpPayload({resourceSpans:[{resource:{attributes:[attribute("service.name","codex-app-server")]},
    scopeSpans:[{spans:[{name:"handle_responses",traceId:trace,spanId:"1".repeat(16),
      startTimeUnixNano:String(BigInt(AT+at-1000)*1000000n),endTimeUnixNano:String(BigInt(AT+at)*1000000n),
      attributes:[attribute("conversation.id",session),attribute("gen_ai.usage.input_tokens",19),
        ...(identity?.turn?[attribute("turn.id",identity.turn)]:[]),
        ...(identity?.request?[attribute("request_id",identity.request)]:[]),
        attribute("gen_ai.usage.output_tokens",2)]}]}]}]},{source:"codex",transportPath:"/v1/traces"}).events[0]!.event;
}
function financial(e: any) { return [e.inputTokens,e.outputTokens,e.cacheReadTokens,e.cacheCreationTokens,e.costUsd].some(v=>v!==undefined); }
function gap(e: any) { return e.metadata?.usageSource==="capture_gap" || e.metadata?.captureGap===true; }
function hasTable(db: any,name: string) { return Boolean(db.prepare("select 1 from sqlite_master where type='table' and name=?").get(name)); }
function invariant(ok: unknown,tag: string,detail: unknown) { assert.ok(ok,tag+": "+JSON.stringify(detail)); }

class World {
  dir = path.join(root,String(++runIndex)); file = path.join(this.dir,"ledger.sqlite");
  sessions = path.join(this.dir,"sessions"); rolloutFile = "";
  trace = "a".repeat(32); gapTrace="c".repeat(32);
  now = new Date(AT+2000); b: any; version: Version="head";
  frozen = new Map<string,Frozen>(); gapRaw = new Set<string>();
  producers = new Map<string,Producer>();
  traceModels = new Map<string,Set<string>>();
  expected = new Map<string,Partial<Record<Field,number>>>();
  knownNative = new Map<string,Partial<Record<Field,number>>>();
  nativeTurns = 1; historyTurns = 0;
  nativeUpdateWritten=false;
  nativeModels = new Set<string>();
  // The first complete file response establishes its counter/turn knowledge.
  // Later context-only sightings have no new marginal counters to deliver.
  fileWritten = false; fileHasTurn = false; nativeObserved = false;
  oldAcknowledged = new Map<string,string>();
  aliasWritten=false;
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
  register(e:any,response:string,kind:Producer["kind"]) {
    this.producers.set(e.id,{event:e,response,kind});
    const amounts=this.expected.get(response)??{};
    // Fixture native counters are deltas. Convert their producer facts into
    // this response's own amounts before taking maxima with SSE/span reports.
    const native=this.nativeAmounts(response);
    for(const k of FIELDS) {
      if(e[k]!==undefined)amounts[k]=Math.max(amounts[k]??0,e[k]);
      if(native[k]!==undefined)amounts[k]=Math.max(amounts[k]??0,native[k]!);
    }
    this.expected.set(response,amounts);
  }
  nativeAmounts(response:string,prefix=Infinity) {
    const producers=[...this.producers.values()].filter(p=>p.response===response&&p.kind==="turn"&&
      (p.event.metadata.counterOrdinal??Number(p.response.slice(9)))<=prefix);
    return Object.fromEntries(FIELDS.filter(k=>producers.some(p=>p.event[k]!==undefined))
      .map(k=>[k,producers.reduce((n,p)=>n+(p.event[k]??0),0)])) as Partial<Record<Field,number>>;
  }
  fact(trace:string,model:string) {
    const models=this.traceModels.get(trace)??new Set<string>();models.add(model);this.traceModels.set(trace,models);
  }
  eligible(p:Producer,e:any) {
    if(p.kind==="invalid")return false;
    if(p.kind==="turn")return e.model===p.event.model;
    const models=this.traceModels.get(p.event.metadata.traceId);
    const sessions=new Set([...this.producers.values()].filter(other=>
      other.event.metadata.traceId===p.event.metadata.traceId).map(other=>other.event.sessionId).filter(Boolean));
    return models?.size===1&&models.has(e.model)&&sessions.size<=1;
  }
  nativeProvenance(e:any,rawId:string) {
    const p=this.producers.get(rawId);
    invariant(p&&this.eligible(p,e),"I1_NATIVE_FACTS",{rawId,event:e,producer:p});
    for(const k of FIELDS)if(e[k]!==undefined) {
      invariant(p!.event[k]!==undefined&&e[k]>=0&&e[k]<=p!.event[k],"I1_NATIVE_FINANCIAL_FIELD",{field:k,event:e,producer:p});
    }
  }
  sums(response:string) {
    const result:Partial<Record<Field,number>>={};
    for(const f of this.frozen.values())if(f.named&&this.producers.get(f.rawId)?.response===response)
      for(const k of FIELDS)if(f.event[k]!==undefined)result[k]=(result[k]??0)+f.event[k];
    return result;
  }

  observe(lease: any,transportLease=true) {
    for(const item of lease.items) {
      const e=item.envelope.event, previous=this.frozen.get(item.deliveryId);
      if(previous) invariant(item.envelopeJson===previous.bytes,"I2_BYTES",{id:item.deliveryId,version:this.version});
      if(gap(e)) {
        invariant(!financial(e)&&e.model===undefined,"I1_GAP_COUNTERS",e);
        this.gapRaw.add(item.rawId);
      } else if(financial(e)) {
        invariant(typeof e.model==="string"&&!!e.model,"I1_MODEL_MISSING",e);
        if(!previous)this.nativeProvenance(e,item.rawId);
      }
      if(this.gapRaw.has(item.rawId)) invariant(gap(e)||!financial(e),"I3_GAP_REGAINS_USAGE",e);
      if(!previous) this.frozen.set(item.deliveryId,{id:item.deliveryId,rawId:item.rawId,bytes:item.envelopeJson,event:e,
        named:!gap(e)&&financial(e),gap:gap(e)});
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
      this.register({id:deterministicEventId(["codex-rollout",SESSION,"1"]),model:MODEL,
        inputTokens:19,outputTokens:2,cacheReadTokens:0,metadata:{usageSource:"rollout"}},"response-1",withTurn?"turn":"invalid");
    }
  }
  async tail(withTurn: boolean) {
    await this.nativeFile(withTurn);
    const t=new RolloutTailer(this.b,this.sessions,()=>[]);
    try { const result=await t.scan({scope:"full",now:this.now});
      invariant(result.parseErrors===0,"PRODUCER_PARSE",result);
      this.nativeObserved=true;
      for(const p of this.producers.values())if(p.kind==="turn")
        this.knownNative.set(p.response,this.nativeAmounts(p.response));

      pairingChecks++;
    } finally {t.close();}
  }
  async grow(target:number) {
    await this.nativeFile(true);
    const input=[0,19,32,45,45,64],output=[0,2,5,8,8,10],cache=[0,0,5,7,10,10],times=[0,6000,11000,16000,18000,21000];
    for(let i=this.nativeTurns+1;i<=target;i++) {
      const row=(type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+times[i]!).toISOString(),type,payload});
      fs.appendFileSync(this.rolloutFile,[row("turn_context",{turn_id:i===5?"skew-turn":`unit-turn-${i}`,model:MODEL}),
        row("event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:input[i]!,
          output_tokens:output[i]!,cached_input_tokens:cache[i]!}}})].join("\n")+"\n");
      this.register({id:deterministicEventId(["codex-rollout",SESSION,String(i)]),model:MODEL,
        inputTokens:input[i]!-input[i-1]!,outputTokens:output[i]!-output[i-1]!,cacheReadTokens:cache[i]!-cache[i-1]!,
        metadata:{usageSource:"rollout",counterOrdinal:i}},`response-${i}`,"turn");this.nativeTurns=i;
    }
  }
  async history(withTurn: boolean) {
    await this.nativeFile(withTurn);
    if(this.now.getTime()<AT+64_000)this.now=new Date(AT+64_000);
    if(!this.rootIdentity) {
      const db=this.b.database;this.historyTurns=this.nativeTurns;
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
      invariant(plan.missingRows<=this.nativeTurns,"HISTORY_PREVIEW",plan);
      const receipt=await applyCaptureHistory(this.b,this.rootIdentity);
      invariant(receipt.importedRows<=this.nativeTurns,"HISTORY_IMPORT",receipt);historyImports+=receipt.importedRows;
    } catch(error) {
      // A paired native row can retain its original payload while its ledger
      // counters are suppressed. The public importer refuses that mismatch.
      // Refusal is an executed operation, and must preserve financial state.
      if(String(error)!=="Error: capture_history_refused:existing_event_conflict")throw error;
      invariant(financialState()===before,"HISTORY_REFUSAL_CHANGED_USAGE",String(error));historyRefusals++;
    }
    // A fenced history snapshot may exclude counters appended later. Its
    // oracle marks only responses inside the native prefix sealed at creation.
    for(const p of this.producers.values())if(p.kind==="turn"&&Number(p.response.slice(9))<=this.historyTurns)
      this.knownNative.set(p.response,this.nativeAmounts(p.response,this.historyTurns));

  }
  async aliasObservation(requestId:string,turnId?:string,at=59000) {
    const e=log(MODEL,"8".repeat(32),AT+at,29,7,false,3,{request:requestId,turn:turnId},ALIAS_SESSION);
    e.cacheCreationTokens=5;e.costUsd=.125;e.costKind="reported";
    Object.assign(e.metadata,{"gen_ai.usage.cache_creation_input_tokens":5,cost_usd:.125});
    // Producer facts designate one physical response independently of the
    // collector's alias query. Each new alias is explicitly linked on input.
    this.register(e,"alias-response","trace");this.fact("8".repeat(32),MODEL);this.b.append(e);
  }
  async aliasNative() {
    this.head();const sessions=path.join(this.dir,"alias-sessions"),day=path.join(sessions,...new Date(AT).toISOString().slice(0,10).split("-"));
    fs.mkdirSync(day,{recursive:true});
    if(!this.aliasWritten) {
      const row=(at:number,type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+at).toISOString(),type,payload});
      const count=(i:number,o:number,c:number)=>({type:"token_count",info:{total_token_usage:{input_tokens:i,output_tokens:o,cached_input_tokens:c}}});
      fs.writeFileSync(path.join(day,`rollout-alias-${ALIAS_SESSION}.jsonl`),[
        row(60000,"session_meta",{id:ALIAS_SESSION}),row(60000,"turn_context",{turn_id:"alias-turn-0",model:MODEL}),
        row(60000,"event_msg",count(0,0,0)),row(61000,"event_msg",count(29,7,3))].join("\n")+"\n");
      this.register({id:deterministicEventId(["codex-rollout",ALIAS_SESSION,"1"]),model:MODEL,
        inputTokens:29,outputTokens:7,cacheReadTokens:3,metadata:{usageSource:"rollout",counterOrdinal:1}},"alias-response","turn");
      this.aliasWritten=true;
    }
    const tailer=new RolloutTailer(this.b,sessions,()=>[]);
    try {const scan=await tailer.scan({scope:"full",now:this.now});invariant(scan.parseErrors===0,"ALIAS_FILE_PARSE",scan);}
    finally {tailer.close();}
    this.knownNative.set("alias-response",this.nativeAmounts("alias-response"));
  }
  async oldAcked(op:Op) {
    this.head();this.lease();
    const version=op.includes("047")?"0.7.47":"0.7.48";
    this.open(version);
    const requestId=op,trace=(op.includes("047")?"6":"7").repeat(32);
    const partials=[log(MODEL,trace,AT+70000,19,undefined,false,undefined,{request:requestId},LEGACY_SESSION),
      log(MODEL,trace,AT+71000,undefined,2,false,undefined,{request:requestId},LEGACY_SESSION)];
    if(op.endsWith("output-input"))partials.reverse();
    for(const e of partials) {
      this.register(e,op,"trace");this.fact(trace,MODEL);
      const appended=this.b.append(e);
      invariant(appended||Boolean(this.b.database.prepare("select 1 from buffered_events where id=? and uploaded_at is not null").get(e.id)),
        "OLD_NATIVE_PRODUCER_ADMISSION",{op,id:e.id});
      this.lease();this.b.delivery.acknowledge(this.currentLease.leaseId,this.currentLease.items.map((i:any)=>i.deliveryId),this.now);
      const raw=this.b.database.prepare("select input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_usd,usage_duplicate_reason from buffered_events where id=?").get(e.id);
      invariant(raw&&[...this.frozen.values()].some(f=>f.rawId===e.id&&f.named),"OLD_NATIVE_ACK_REQUIRED",{op,event:e});
      this.oldAcknowledged.set(e.id,JSON.stringify(raw));
    }
    this.head();
    const complete=log(MODEL,trace,AT+69000,19,2,false,undefined,{request:requestId},LEGACY_SESSION);
    this.register(complete,op,"trace");this.b.append(complete);
  }
  async oldAckedPair(op:Op) {
    this.head();this.lease();this.open(op.endsWith("047")?"0.7.47":"0.7.48");
    const trace=(op.endsWith("047")?"4":"5").repeat(32),identity={request:op};
    const e=log(MODEL,trace,AT+89000,19,2,false,undefined,identity,LEGACY_SESSION);
    this.register(e,op,"trace");this.fact(trace,MODEL);
      const appended=this.b.append(e);
      invariant(appended||Boolean(this.b.database.prepare("select 1 from buffered_events where id=? and uploaded_at is not null").get(e.id)),
        "OLD_NATIVE_PRODUCER_ADMISSION",{op,id:e.id});this.lease();
    this.b.delivery.acknowledge(this.currentLease.leaseId,this.currentLease.items.map((i:any)=>i.deliveryId),this.now);
    invariant([...this.frozen.values()].some(f=>f.rawId===e.id&&f.named),"OLD_NATIVE_ACK_REQUIRED",{op,event:e});
    this.oldAcknowledged.set(e.id,JSON.stringify(this.b.database.prepare("select input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_usd,usage_duplicate_reason from buffered_events where id=?").get(e.id)));
    this.head();const twin=responseSpan(trace,identity,90000,LEGACY_SESSION);
    twin.cacheReadTokens=3;twin.cacheCreationTokens=5;twin.costUsd=.125;twin.costKind="reported";
    Object.assign(twin.metadata,{"gen_ai.usage.cache_read_tokens":3,"gen_ai.usage.cache_creation_input_tokens":5,cost_usd:.125});
    this.register(twin,op,"trace");this.b.append(twin);
  }
  async operate(op: Op) {
    if(["sse-valid","sse-invalid","sse-conflicting","trace-sol","trace-astra","span","rollout-turn","rollout-no-turn",
      "neighbour-turn","history-turn","history-no-turn","gap-seal","stateless-build","upgrade-047","upgrade-048"].includes(op)) this.head();
    if(["sse-zero","sse-partial","sse-cache-cost","second-turn","third-turn","history-multiple","cache-only-turn","sse-partial-twin","sse-zero-complement-twin"].includes(op))this.head();
    if(op.startsWith("skew-")||op.startsWith("request-only-"))this.head();
    if(op.startsWith("call-"))this.head();
    switch(op) {
      case "call-request-input": case "call-request-output": case "call-complete": {
        const input=op!=="call-request-output",output=op!=="call-request-input";
        const identity={call:"shared-call",request:op==="call-complete"?undefined:op};
        const e=log(MODEL,"0123456789abcdef".repeat(2),AT+(input?56000:57000),input?19:undefined,output?2:undefined,
          false,input?0:undefined,identity,"55555555-5555-4555-8555-555555555555");
        if(op==="call-complete") {
          e.cacheCreationTokens=3;e.costUsd=.125;e.costKind="reported";
          Object.assign(e.metadata,{"gen_ai.usage.cache_creation_input_tokens":3,cost_usd:.125});
        }
        this.register(e,"call-response","trace");this.fact("0123456789abcdef".repeat(2),MODEL);
        this.b.append(e);break;
      }
      case "old-acked-pair-047": case "old-acked-pair-048": await this.oldAckedPair(op);break;
      case "alias-request": this.head();await this.aliasObservation("alias-request-0");break;
      case "alias-bridge": this.head();await this.aliasObservation("alias-request-0","alias-turn-0",62000);break;
      case "alias-native": await this.aliasNative();break;
      case "alias-chain": case "alias-long-chain": {
        this.head();await this.aliasObservation("alias-request-0","alias-turn-0",62000);
        // Sixty-four covered bridges, alternating key namespaces. Freeze/ACK
        // and restart between links, then query only the final request alias.
        const length=op==="alias-long-chain"?32:8;
        for(let n=1;n<=length;n++) {
          await this.aliasObservation(`alias-request-${n-1}`,`alias-turn-${n}`,62000+n*2);
          this.lease();this.b.delivery.acknowledge(this.currentLease.leaseId,this.currentLease.items.map((i:any)=>i.deliveryId),this.now);this.assertStep();
          await this.aliasObservation(`alias-request-${n}`,`alias-turn-${n}`,62001+n*2);
          this.lease();this.b.delivery.acknowledge(this.currentLease.leaseId,this.currentLease.items.map((i:any)=>i.deliveryId),this.now);this.assertStep();
          if(n%8===0)this.open("head");
        }
        await this.aliasObservation(`alias-request-${length}`,undefined,58000);break;
      }
      case "old-acked-047-input-output": case "old-acked-047-output-input":
      case "old-acked-048-input-output": case "old-acked-048-output-input": await this.oldAcked(op);break;
      case "skew-series": {
        for(let i=1;i<=24;i++){
          const e=log(MODEL,"b".repeat(32),AT+18000+i*7,19+i,2,false,3,{turn:"skew-turn",request:"skew-request"});
          this.register(e,"response-5","trace");this.fact("b".repeat(32),MODEL);this.b.append(e);
          this.lease();this.b.delivery.acknowledge(this.currentLease.leaseId,this.currentLease.items.map((item:any)=>item.deliveryId),this.now);
          this.assertStep();
        }
        break;
      }
      case "skew-native": await this.grow(5);await this.tail(this.fileHasTurn);break;
      case "skew-native-update": {
        await this.grow(5);
        if(!this.nativeUpdateWritten) {
          const row=(type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+21400).toISOString(),type,payload});
          fs.appendFileSync(this.rolloutFile,[row("turn_context",{turn_id:"skew-turn",model:MODEL}),
            row("event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:68,output_tokens:11,cached_input_tokens:17}}})].join("\n")+"\n");
          this.register({id:deterministicEventId(["codex-rollout",SESSION,"6"]),model:MODEL,inputTokens:4,outputTokens:1,
            cacheReadTokens:7,metadata:{usageSource:"rollout",counterOrdinal:6}},"response-5","turn");
          this.nativeTurns=6;this.nativeUpdateWritten=true;
        }
        await this.tail(this.fileHasTurn);break;
      }
      case "skew-partial-request": case "skew-complete-request": case "skew-partial-turn": case "skew-complete-turn":
      case "skew-growing": case "skew-smaller": {
        const partial=op.includes("partial"),growing=op==="skew-growing",smaller=op==="skew-smaller";
        // Native completion is 21000. Complete reports arrive with EARLIER
        // clocks; the turn-only observations have a different drift again.
        const at=growing?19800:smaller?22100:op.endsWith("turn")?partial?21300:20200:partial?21000:20500;
        const e=log(MODEL,"b".repeat(32),AT+at,smaller?17:growing?23:19,partial?undefined:smaller?1:growing?3:2,false,
          partial?undefined:smaller?0:growing?7:3,{turn:"skew-turn",...(!op.endsWith("turn")?{request:"skew-request"}:{})});
        if(!partial) {
          e.cacheCreationTokens=smaller?0:growing?5:2;e.costUsd=smaller?0:growing?.125:.1;e.costKind="reported";
          Object.assign(e.metadata,{"gen_ai.usage.cache_creation_input_tokens":e.cacheCreationTokens,cost_usd:e.costUsd});
        }
        this.register(e,"response-5","trace");this.fact("b".repeat(32),MODEL);this.b.append(e);break;
      }
      case "request-only-partial": case "request-only-complete": case "request-only-span": {
        const trace="9".repeat(32),identity={request:"request-without-turn"};
        const e=op==="request-only-span"?responseSpan(trace,identity,48000):
          log(MODEL,trace,AT+(op==="request-only-partial"?45000:43000),19,op==="request-only-partial"?undefined:2,false,
            op==="request-only-partial"?undefined:3,identity);
        if(op==="request-only-complete") {
          e.cacheCreationTokens=2;e.costUsd=.1;e.costKind="reported";
          Object.assign(e.metadata,{"gen_ai.usage.cache_creation_input_tokens":2,cost_usd:.1});
        }
        this.register(e,"request-only","trace");
        if(op!=="request-only-span")this.fact(trace,MODEL);
        this.b.append(e);break;
      }
      case "sse-zero-complement-twin": {
        const e=log(MODEL,this.trace,AT+6000,19,2,false,0);this.register(e,"response-1","trace");
        if(this.b.append(e)){this.nativeModels.add(MODEL);this.fact(this.trace,MODEL);}break;
      }
      case "sse-partial-twin": {
        const e=log(MODEL,this.trace,AT+6000,19);this.register(e,"response-1","trace");
        if(this.b.append(e)){this.nativeModels.add(MODEL);this.fact(this.trace,MODEL);}break;
      }
      case "sse-valid": {
        const e=log(MODEL,this.trace,AT+6000,19,2);this.register(e,"response-1","trace");
        const accepted=this.b.append(e);if(accepted){this.nativeModels.add(MODEL);this.fact(this.trace,MODEL);}break;
      }
      case "sse-invalid": case "sse-conflicting": {
        // Two explicit model aliases are permanently contradictory native
        // facts, even after a gap ACK. Counters describe a diagnostic signal.
        const e=log(MODEL,op==="sse-invalid"?this.gapTrace:this.trace,AT+7000,7,1,true);
        this.register(e,op==="sse-invalid"?"diagnostic-7-1":"diagnostic-conflict","invalid");const accepted=this.b.append(e);
        if(accepted&&op==="sse-conflicting"){this.nativeModels.add(MODEL);this.nativeModels.add(OTHER);this.fact(this.trace,MODEL);this.fact(this.trace,OTHER);}break;
      }
      case "trace-sol": case "trace-astra": {
        // The world has one Sol response. The Astra operation contradicts
        // that SAME trace; an Astra-only trace and a Sol rollout would be
        // different eligible responses, not a proven duplicate by counts.
        if(op==="trace-astra") {
          if(this.b.append(log(MODEL,this.trace,AT+8000))){this.nativeModels.add(MODEL);this.fact(this.trace,MODEL);}
        }
        const model=op==="trace-sol"?MODEL:OTHER;const e=log(model,this.trace,AT+(op==="trace-sol"?8000:9000));
        if(this.b.append(e)){this.nativeModels.add(model);this.fact(this.trace,model);}break;
      }
      case "span": { const e=responseSpan(this.trace);this.register(e,"response-1","trace");this.b.append(e);break; }
      case "rollout-turn": await this.tail(true);break;
      case "rollout-no-turn": await this.tail(false);break;
      case "history-turn": await this.history(true);break;
      case "history-no-turn": await this.history(false);break;
      case "sse-zero": case "sse-partial": case "sse-cache-cost": {
        const shape=op==="sse-zero"?{input:0,output:0,trace:"d",at:21000}:op==="sse-partial"?
          {input:11,output:undefined,trace:"e",at:31000}:{input:23,output:4,trace:"f",at:41000};
        const e=log(MODEL,shape.trace.repeat(32),AT+shape.at,shape.input,shape.output);
        if(op==="sse-zero") {
          e.cacheReadTokens=0;e.cacheCreationTokens=0;e.costUsd=0;e.costKind="reported";
          Object.assign(e.metadata,{cached_token_count:0,"gen_ai.usage.cache_creation_input_tokens":0,cost_usd:0});
        }
        if(op==="sse-partial"){e.costUsd=.031;e.costKind="reported";e.metadata.cost_usd=.031;}
        if(op==="sse-cache-cost") {
          e.cacheReadTokens=9;e.cacheCreationTokens=5;e.costUsd=0.123;e.costKind="reported";
          Object.assign(e.metadata,{"gen_ai.usage.cache_read_tokens":9,"gen_ai.usage.cache_creation_input_tokens":5,cost_usd:0.123});
        }
        this.register(e,op,"trace");this.fact(e.metadata.traceId as string,MODEL);this.b.append(e);break;
      }
      case "second-turn": case "third-turn": case "history-multiple": case "cache-only-turn": {
        const target=op==="second-turn"?2:op==="cache-only-turn"?4:3;
        await this.grow(target);
        if(op==="history-multiple")await this.history(this.fileHasTurn);else await this.tail(this.fileHasTurn);break;
      }
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
        const bad=log(MODEL,this.gapTrace,AT+7000,7,1,true);this.register(bad,"diagnostic-7-1","invalid");this.b.append(bad);
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
        const e=log(MODEL,this.trace,AT+6000,19,2);this.register(e,"response-1","trace");
        if(this.b.append(e)){this.nativeModels.add(MODEL);this.fact(this.trace,MODEL);}this.open("head");break;
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
      if(financial(e)&&!gap(e))this.nativeProvenance(e,row.rawId);
      this.frozen.set(row.id,{id:row.id,rawId:row.rawId,bytes:row.bytes,event:e,named:!gap(e)&&financial(e),gap:gap(e)});
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
    for(const [id,fields] of this.oldAcknowledged) {
      const raw=db.prepare("select input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_usd,usage_duplicate_reason from buffered_events where id=?").get(id);
      invariant(JSON.stringify(raw)===fields,"I2_OLD_ACK_FINANCE",{id,before:fields,after:raw});
    }
    const named=[...this.frozen.values()].filter(f=>f.named);
    for(const [response,expected] of this.expected) {
      const sum=this.sums(response);
      for(const k of FIELDS)invariant((sum[k]??0)<=(expected[k]??0)+1e-12,"NO_DUPLICATE_USAGE",
        {response,field:k,expected:expected[k]??0,observed:sum[k]??0,named:named.filter(f=>this.producers.get(f.rawId)?.response===response)});
    }
    for(const frozen of named) {
      const a=active.find((r:any)=>r.id===frozen.id),r=replay.find((r:any)=>r.id===frozen.id),receipt=receipts.find((r:any)=>r.id===frozen.id);
      const parked=receipt?.state==="dead"&&receipt.reason.startsWith("remote_")&&
        captures.some((c:any)=>c.id===frozen.id&&c.bytes===frozen.bytes);
      invariant(a || r?.bytes===frozen.bytes || receipt?.state==="acknowledged" || parked,"I2_NAMED_LOST",{id:frozen.id,version:this.version,receipts});
    }
    for(const [response,expected] of this.knownNative) {
      const retained=this.sums(response);
      for(const p of this.producers.values())if(p.response===response&&p.kind==="turn"&&
        ![...this.frozen.values()].some(f=>f.rawId===p.event.id)&&!this.gapRaw.has(p.event.id)) {
        const raw=db.prepare("select input_tokens as inputTokens,output_tokens as outputTokens,cache_read_tokens as cacheReadTokens,cache_creation_tokens as cacheCreationTokens,cost_usd as costUsd,usage_duplicate_reason as duplicate from buffered_events where id=?").get(p.event.id);
        if(raw&&!raw.duplicate&&(active.some((a:any)=>a.rawId===p.event.id)||replay.some((r:any)=>r.rawId===p.event.id)))
          for(const k of FIELDS)retained[k]=(retained[k]??0)+(raw[k]??0);
      }
      for(const k of FIELDS)invariant((retained[k]??0)>=(expected[k]??0),"KNOWN_NATIVE_LOST",
        {response,field:k,retained,expected,version:this.version});
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
    // Every eligible native producer, including zero/partial/cache/cost SSE,
    // must retain all its fields. Persisted gaps remain diagnostic forever.
    for(const p of this.producers.values())if(!this.gapRaw.has(p.event.id)&&this.eligible(p,p.event))
      for(const k of FIELDS)if(p.event[k]!==undefined) {
        invariant([...this.frozen.values()].some(f=>f.named&&this.producers.get(f.rawId)?.response===p.response&&
          f.event[k]!==undefined),"KNOWN_NATIVE_FIELD_MISSING",{response:p.response,field:k,expected:p.event[k]});
        invariant((this.sums(p.response)[k]??0)>=p.event[k],"KNOWN_NATIVE_FINAL",
          {response:p.response,field:k,expected:p.event[k],sum:this.sums(p.response)});
      }
    for(const [response,native] of this.knownNative)for(const k of FIELDS) {
      const expected=Math.max(native[k]??0,this.expected.get(response)?.[k]??0);
      invariant(Math.abs((this.sums(response)[k]??0)-expected)<1e-12,"KNOWN_NATIVE_FINAL",
        {response,field:k,expected,sum:this.sums(response)});
    }
    // These two isolated response alphabets have explicit native model facts
    // and no model/account contradictions. A mistakenly gapped complete SSE
    // must not excuse lost complementary fields. A span gapped before its
    // first model log remains diagnostic and is not used to raise this floor.
    for(const response of ["response-5","request-only","call-response"]) {
      const required=this.nativeAmounts(response);
      for(const p of this.producers.values())if(p.response===response&&p.kind==="trace"&&
        p.event.metadata.otelEventName==="codex.sse_event"&&this.eligible(p,p.event))
        for(const k of FIELDS)if(p.event[k]!==undefined)required[k]=Math.max(required[k]??0,p.event[k]);
      for(const k of FIELDS)if(required[k]!==undefined)invariant(this.sums(response)[k]!==undefined&&
        Math.abs(this.sums(response)[k]!-required[k]!)<1e-12,"KNOWN_NATIVE_RESPONSE_MAXIMUM",
        {response,field:k,required,sums:this.sums(response)});
    }
  }
  close() { this.b?.close();fs.rmSync(this.dir,{recursive:true,force:true}); }
}

async function run(operations: Op[],settle=true) {
  const world=new World();
  let index=-1;
  try {
    for(index=0;index<operations.length;index++) { await world.operate(operations[index]!);world.assertStep(); }
    if(settle)await world.settle();
    const totals=Object.fromEntries([...world.expected.keys()].map(response=>[response,world.sums(response)]));
    return {passed:true as const,steps:operations.length,named:[...world.frozen.values()].filter(f=>f.named).length,totals,
      expected:Object.fromEntries(world.expected)};
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
      frozen:[...world.frozen.values()],
      accounting:hasTable(db,"codex_response_coverage")?db.prepare("select * from codex_response_coverage").all():[],
      decisions:hasTable(db,"codex_capture_decisions")?db.prepare("select raw_id,reason from codex_capture_decisions").all():[]};
    return {passed:false as const,index,error:String(error),snapshot};
  }
  finally { world.close(); }
}
function failureTag(error: string) { return error.match(/(?:I[123]_[A-Z_]+|NO_DUPLICATE_USAGE|KNOWN_NATIVE_[A-Z_]+|STATE_ACTIVE_TERMINAL|ORDER_[A-Z_]+)/)?.[0]; }
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
function* permutations(ops:Op[]):Generator<Op[]> {
  if(!ops.length){yield [];return;}
  for(let i=0;i<ops.length;i++)for(const rest of permutations([...ops.slice(0,i),...ops.slice(i+1)]))yield [ops[i]!,...rest];
}
async function saveFailure(name:string,operations:Op[],result:Extract<Awaited<ReturnType<typeof run>>,{passed:false}>) {
  const tag=failureTag(result.error),directory=path.join(process.cwd(),"evidence/codex-capture-sequences");fs.mkdirSync(directory,{recursive:true});
  const file=path.join(directory,`${name}-${tag??"harness"}.json`);
  fs.writeFileSync(file,JSON.stringify({...result,originalOperations:operations,operations,attempts:0},null,2)+"\n");
  const reduced=tag?await shrink(operations,tag):{operations,attempts:0};
  fs.writeFileSync(file,JSON.stringify({...result,originalOperations:operations,...reduced},null,2)+"\n");
  console.error(JSON.stringify({file,tag,...reduced}));
}
function completionSeedNames(from:number,to:number) {return Array.from({length:to-from},(_,n)=>`seed-${from+n}`);}
async function parallelSeeds() {
  const split=Math.ceil(seeds/2),ranges=[[0,split],[split,seeds]] as const;
  const children: ReturnType<typeof spawn>[]=[];
  try {
    const summaries=await Promise.all(ranges.map(async([from,to])=>{
      const directory=path.join(process.env.PLIMSOLL_PROOF_ROOT!,`seed-worker-${from}`);fs.mkdirSync(directory,{mode:0o700});
      const summary=path.join(directory,"summary.json"),receipt=path.join(directory,"completion.json");
      const child=spawn(process.execPath,["--import",path.join(process.cwd(),"node_modules/tsx/dist/loader.mjs"),
        path.join(process.cwd(),"scripts/codex-capture-sequences-proof.ts"),`--seeds=${seeds}`,`--partition=${from}:${to}`,`--worker-summary=${summary}`],
        {cwd:process.cwd(),env:{...process.env,PLIMSOLL_PROOF_RECEIPT:receipt},stdio:["ignore","ignore","inherit"]});
      children.push(child);
      await new Promise<void>((resolve,reject)=>{
        child.once("error",reject);child.once("exit",(code,signal)=>code===0?resolve():reject(new Error(`seed partition ${from}:${to} exit ${code}/${signal}`)));
      });
      const actual=JSON.parse(fs.readFileSync(receipt,"utf8")),expected=completionSeedNames(from,to);
      invariant(actual.completed===true&&actual.status==="passed"&&actual.runId===process.env.PLIMSOLL_PROOF_RUN_ID&&
        actual.expectedChecks===to-from&&actual.counts.passed===to-from&&actual.counts.failed===0&&
        JSON.stringify(actual.checks)===JSON.stringify(expected.map(name=>({name,passed:true}))),"SEED_PARTITION_RECEIPT",actual);
      const result=JSON.parse(fs.readFileSync(summary,"utf8"));
      invariant(result.from===from&&result.to===to&&JSON.stringify(result.seeds)===JSON.stringify(expected),"SEED_PARTITION_RANGE",result);
      return result;
    }));
    const all=summaries.flatMap(s=>s.seeds);
    invariant(JSON.stringify(all)===JSON.stringify(completionSeedNames(0,seeds))&&new Set(all).size===seeds,"EVERY_PINNED_SEED_ONCE",all);
    for(const summary of summaries) {
      for(const name of summary.seeds)completion.check(name);
      steps+=summary.steps;rollbackLeases+=summary.rollbackLeases;terminalReplays+=summary.terminalReplays;
      historyImports+=summary.historyImports;historyRefusals+=summary.historyRefusals;pairingChecks+=summary.pairingChecks;
      for(const [op,count] of Object.entries(summary.operations))coverage.set(op as Op,coverage.get(op as Op)!+(count as number));
    }
    console.log(JSON.stringify({seedPartitions:summaries.map(s=>({from:s.from,to:s.to,steps:s.steps,processIdentity:s.processIdentity})),everySeedOnce:true}));
  } finally {
    // On any partition failure, terminate only owned fixture children and await
    // their exits before the disposable proof root can be removed.
    for(const child of children)if(child.exitCode===null&&child.signalCode===null) {
      const done=new Promise<void>(resolve=>child.once("exit",()=>resolve()));child.kill("SIGTERM");await done;
    }
  }
}
async function main() {
  await withReader("a60590559403cace3db7cbbda49812c9e3dbfe62",async({Buffer:B047})=>
    withReader("34d58bcd90865679e09fcbd1ee1703de5effda97",async({Buffer:B048})=>{
      Old047=B047;Old048=B048;
      for(const fixture of partition?[]:directed) {
        const result=await run(fixture.operations);
        if(!result.passed)await saveFailure(fixture.file,fixture.operations,result);
        invariant(result.passed,"DIRECTED_SEQUENCE",{fixture:fixture.file,result});completion.check(fixture.file);
      }
      if(!caseFile&&!partition)for(let group=0;group<orderGroups.length;group++) {
        let canonical:Record<string,number>|undefined;
        const observations=orderGroups[group]!,response=observations[0]!.startsWith("call-")?"call-response":observations[0]!.startsWith("alias-")?"alias-response":observations[0]!.startsWith("request-only-")?"request-only":"response-5";
        let permutation=0;
        for(const order of permutations(observations)) {
          // ACK after EACH observation so different arrival orders cannot
          // be repaired by rewriting a delivery that already left.
          const operations=order.flatMap(op=>[op,"expire-retry","ack"] as Op[]),result=await run(operations);
          if(!result.passed)await saveFailure(`order-${group}-${permutation}`,operations,result);
          invariant(result.passed,"ORDER_EXECUTION",result);
          const totals=result.passed?result.totals[response]!:{},expected=result.passed?result.expected[response]!:{ };
          for(const k of FIELDS)invariant(expected[k]!==undefined&&totals[k]!==undefined&&Math.abs(totals[k]!-expected[k]!)<1e-12,
            "ORDER_FIELD_MAXIMUM",{group,order,field:k,totals,expected});
          if(canonical)for(const k of FIELDS)invariant(Math.abs(totals[k]!-canonical[k]!)<1e-12,"ORDER_INVARIANCE",{order,totals,canonical});
          canonical=totals;completion.check(`order-${group}-${permutation++}`);
        }
      }
      for(let seed=partition?.[0]??0;seed<(partition?.[1]??0);seed++) {
        const operations=sequence(seed),result=await run(operations);
        if(!result.passed) {
          const tag=failureTag(result.error);
          const evidence=path.join(process.cwd(),"evidence/codex-capture-sequences");fs.mkdirSync(evidence,{recursive:true});
          const file=path.join(evidence,`seed-${seed}-${tag??"harness"}.json`);
          // Save the full counterexample BEFORE shrinking. A proof timeout
          // during reduction must not erase the original failing sequence.
          fs.writeFileSync(file,JSON.stringify({seed,requestedSeeds:seeds,...result,originalOperations:operations,operations,attempts:0},null,2)+"\n");
          const reduced=tag?await shrink(operations,tag):{operations,attempts:0};
          const counterexample={seed,requestedSeeds:seeds,...result,originalOperations:operations,...reduced};
          fs.writeFileSync(file,JSON.stringify(counterexample,null,2)+"\n");
          console.error(JSON.stringify({counterexample,file},null,2));throw new Error(result.error);
        }
        completion.check(`seed-${seed}`);
      }
    }));
  if(!caseFile&&!partition)await parallelSeeds();
  if(partition) {
    const summary={from:partition[0],to:partition[1],seeds:completionSeedNames(partition[0]!,partition[1]!),
      steps,operations:Object.fromEntries(coverage),rollbackLeases,terminalReplays,historyImports,historyRefusals,pairingChecks,
      processIdentity:identityQueries.stats()};
    fs.writeFileSync(workerSummary!,JSON.stringify(summary)+"\n");
  }
  if(!caseFile)invariant(OPS.every(op=>coverage.get(op)!>0),"OPERATION_COVERAGE",Object.fromEntries(coverage));
  console.log(JSON.stringify({proof:"codex-capture-sequences",seeds,directed:directed.length,steps,
    orderChecks:caseFile?0:orderGroups.length*6,
    operations:Object.fromEntries(coverage),rollbackLeases,terminalReplays,historyImports,historyRefusals,pairingChecks,processIdentity:identityQueries.stats(),
    invariants:["I1 native model or tokenless gap","I2 named ID/bytes final","I3 gap never regains counters",
      "financial totals bounded per response and field","every known native response retained and eventually named",
      "arrival permutations converge to every field maximum with ACKs between observations"]},null,2));
  completion.complete();
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>{identityQueries.restore();fs.rmSync(root,{recursive:true,force:true});});
