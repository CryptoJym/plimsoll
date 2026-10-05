/** Qualification probe built from the exact immutable bridge source. It is
 * not an installed previous version and cannot enable bridge adoption. */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { readCollectorConfig, collectorConfigPath } from "../../packages/collector-cli/src/config";
import { currentDispatchCaptureRoots, captureRootDigest, dispatchBindingSchema,
  dispatchBindingForSession, rootEventMetadata } from "../../packages/collector-cli/src/capture-root-inventory";
import { dispatchHistoryPressure } from "../../packages/collector-cli/src/dispatch-binding-index";
import { assertPrivateStateDirectory } from "../../packages/collector-cli/src/collector-state-io";
import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
const hash = (bytes: string | Buffer) => crypto.createHash("sha256").update(bytes).digest("hex");
const SOURCE = "7f53dd8e1b08b369d95b7471692bb0d83adefd92";
type Event = { source: "codex" | "claude_code"; sessionId: string; observedAt: string; rootId?: string };
export function readRollbackSnapshot(input: { profile: Buffer; image: Buffer; scratchRoot: string;
  events?: Event[]; namedUsage?: { ledger: Buffer; now: string; workspaceId: string; deviceId: string; sealedSha256: string } }) {
  const { profile, image } = input;
  if (!Buffer.isBuffer(profile) || !Buffer.isBuffer(image) || profile.length > 32*1024*1024 ||
      image.length > 32*1024*1024 || input.events && input.events.length > 1024)
    throw new Error("rollback_reader_input_bound");
  assertPrivateStateDirectory(input.scratchRoot);
  const home = fs.mkdtempSync(path.join(input.scratchRoot, "bridge-reader-")), previous = process.env.PLIMSOLL_HOME;
  const imageSha256=hash(image);
  try {
    process.env.PLIMSOLL_HOME=home;
    const directory=path.join(home,"dispatch-binding-history");fs.mkdirSync(directory,{mode:0o700});
    fs.writeFileSync(path.join(directory,imageSha256+".sqlite"),image,{mode:0o600,flag:"wx"});
    fs.writeFileSync(collectorConfigPath(),profile,{mode:0o600,flag:"wx"});
    // Exactly one actual full-profile schema pass and one actual dynamic-root
    // consumer pass. Avoid the former third pass/copy, without replacing either
    // real consumer with a mock or a weaker schema.
    const opened=readCollectorConfig();if(opened.status!=="valid")throw new Error("rollback_reader_profile_invalid");
    const configured=opened.config.captureRoots??[], roots=currentDispatchCaptureRoots();
    if(roots.length!==configured.length)throw new Error("rollback_reader_missing_roots");
    const refs=roots.flatMap(root=>root.dispatchHistory?[root.dispatchHistory]:[]);
    if(!refs.length || refs.some(ref=>ref.sha256!==imageSha256))throw new Error("rollback_reader_image_mismatch");
    const pressure=dispatchHistoryPressure(roots,dispatchBindingSchema.parse);
    if(pressure.state!=="known")throw new Error("rollback_reader_unknown_history:"+pressure.reason);
    // The real history consumer has already validated every pinned row, custody,
    // schema, terminal marker, generation, duplicate and byte bound. Reuse those
    // immutable row strings for inventory; do not parse/serialize them again.
    const db=new Database(image,{readonly:true});
    try {
      const rows=db.prepare("select root_digest as rootDigest,binding from history_bindings order by root_digest,session_id,attempt_id,from_ms limit 65537").all() as Array<{rootDigest:string;binding:string}>;
      if(rows.length!==refs[0].totalRows || rows.length>65536)throw new Error("rollback_reader_row_bound");
      const terminalRows=db.prepare("select proof from history_terminals order by proof_id limit 4097").all() as Array<{proof:string}>;
      if(terminalRows.length!==refs[0].terminalRows || terminalRows.length>4096)throw new Error("rollback_reader_terminal_bound");
      const byRoot=new Map<string,string[]>();for(const row of rows){const list=byRoot.get(row.rootDigest)??[];list.push(row.binding);byRoot.set(row.rootDigest,list);}
      const inventory=roots.map(root=>{
        const {dispatch,dispatchHistory,...metadata}=root,rootDigest=captureRootDigest(root),historical=byRoot.get(rootDigest)??[];
        const bindings=[...(dispatch??[]).map(binding=>JSON.stringify(binding)),...historical].sort();
        return {rootDigest,rootMetadataSha256:hash(JSON.stringify(metadata)),inventorySha256:hash(JSON.stringify(bindings)),hot:dispatch?.length??0,historical:historical.length,total:bindings.length};
      }).sort((a,b)=>a.rootDigest.localeCompare(b.rootDigest));
      const result={protocol:"plimsoll.dispatch-history-bridge-reader/v2",sourceCommit:SOURCE,collectorVersion:"0.7.48",
        profileSha256:hash(profile),imageSha256,generation:refs[0].generation,roots:inventory,
        terminalSha256:hash(JSON.stringify(terminalRows.map(row=>row.proof).sort())),inventorySha256:hash(JSON.stringify(inventory)),nodeAbi:process.versions.modules};
      return {...result,...(input.events?{events:input.events.map(event=>{
        const binding=dispatchBindingForSession(event.source,event.sessionId,event.observedAt,roots);
        const root=event.rootId?roots.find(value=>value.rootId===event.rootId):undefined;
        return {binding,metadata:root?rootEventMetadata(root,"bridge-rollback-fixture",event.observedAt,event.sessionId):null};
      })}:{}),...(input.namedUsage?{namedUsage:readNamedUsage(input.namedUsage,home)}:{})};
    } finally {db.close();}
  } finally {process.env.PLIMSOLL_HOME=previous;fs.rmSync(home,{recursive:true,force:true});}
}
function readNamedUsage(input: NonNullable<Parameters<typeof readRollbackSnapshot>[0]["namedUsage"]>,home:string) {
  if(!Buffer.isBuffer(input.ledger)||input.ledger.length>32*1024*1024||!Number.isFinite(Date.parse(input.now)))throw new Error("rollback_reader_named_usage_bound");
  const file=path.join(home,"named-usage.sqlite");fs.writeFileSync(file,input.ledger,{mode:0o600,flag:"wx"});
  const buffer=new LocalEventBuffer(file,{workspaceId:input.workspaceId,deviceId:input.deviceId,delivery:{enabled:true,now:()=>new Date(input.now)}});
  try {
    const before=buffer.database.prepare("select delivery_id as id,sealed_envelope_json as sealed from upload_outbox order by delivery_id").all();
    if(before.length!==2||hash(JSON.stringify(before))!==input.sealedSha256)throw new Error("rollback_reader_named_usage_seal_mismatch");
    const lease=buffer.delivery.lease({now:new Date(input.now)});
    if(lease.locallyDead!==0||lease.items.length!==2)throw new Error("rollback_reader_named_usage_dropped");
    const named=lease.items.find(item=>item.envelope.event.model==="gpt-6.1-sol"),gap=lease.items.find(item=>!item.envelope.event.model);
    if(named?.envelope.event.inputTokens!==19||named.envelope.event.outputTokens!==2||gap?.envelope.event.inputTokens!==undefined)
      throw new Error("rollback_reader_named_usage_incompatible");
    const after=buffer.database.prepare("select delivery_id as id,sealed_envelope_json as sealed from upload_outbox order by delivery_id").all();
    if(hash(JSON.stringify(after))!==input.sealedSha256)throw new Error("rollback_reader_named_usage_seal_changed");
    return {sealedSha256:input.sealedSha256,items:2,locallyDead:0,leaseExpiryRespected:true};
  } finally {buffer.close();}
}
