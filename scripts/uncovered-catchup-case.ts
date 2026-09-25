import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { captureFrontier } from "../packages/collector-cli/src/capture-frontier";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { rootCursorKey, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { CaptureRevisitQueue } from "../packages/collector-cli/src/capture-revisit-queue";

const base = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()), "uncovered-catchup-"));
const now = new Date();
let stamp = now.toISOString();
const today = stamp.slice(0, 10).split("-");
const old = new Date(now.getTime() - 15 * 86400000).toISOString().slice(0, 10).split("-");
const id = (digit: string) => `${digit.repeat(8)}-${digit.repeat(4)}-4${digit.repeat(3)}-8${digit.repeat(3)}-${digit.repeat(12)}`;
const codex = (session: string, counts: number[], padding = 0) => [
  { type: "session_meta", timestamp: stamp, payload: { id: session } },
  { type: "turn_context", timestamp: stamp, payload: { model: "gpt-6-sol" } },
  ...counts.map((n) => ({ type: "event_msg", timestamp: stamp, payload: { type: "token_count", info: { total_token_usage: {
    input_tokens: n, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0,
  } } }, padding: "p".repeat(padding) })),
];
const claude = (session: string, n: number) => ({ type: "assistant", sessionId: session, timestamp: stamp,
  message: { id: `${session}-${n}`, model: "claude-opus-5", usage: { input_tokens: n, output_tokens: 1 } } });
const encode = (rows: unknown[]) => rows.map((row) => JSON.stringify(row)).join("\n") + "\n";
const write = (file: string, rows: unknown[]) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, encode(rows)); };
const codexDir = path.join(base, "codex");
const claudeDir = path.join(base, "claude");
fs.mkdirSync(claudeDir, { recursive: true });
const codexFile = (parts: string[], name: string) => path.join(codexDir, ...parts, `rollout-${name}.jsonl`);
const claudeFile = (name: string) => path.join(claudeDir, "project", `${name}.jsonl`);
const beforeEnrollment = codexFile(today, id("1"));
write(beforeEnrollment, codex(id("1"), [0]));
const boundaryFragment = codexFile(today, id("b"));
fs.writeFileSync(boundaryFragment, `{"type":"event_msg","timestamp":"${stamp}","payload":{"type":"user_message","message":"before`);
const ledger = path.join(base, "ledger.sqlite");
const options = { workspaceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", delivery: { enabled: true } };
let buffer = new LocalEventBuffer(ledger, options);
const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
const roots: CaptureRoot[] = [
  { source: "codex", directory: codexDir, rootId: "codex", profileId: "codex", installationEpochId: epoch },
  { source: "claude_code", directory: claudeDir, rootId: "claude", profileId: "claude", installationEpochId: epoch },
];
let rollout: RolloutTailer;
let transcript: TranscriptTailer;
let maintenance: CollectorMaintenance;
let activeTurn: { writeLockBodyMs: number; writeLockEnvelopeMs: number } | null = null;
const costTurns: Array<{cpuMs:number;wallMs:number;bytesRead:number;writeLockBodyMs:number;writeLockEnvelopeMs:number}> = [];
const threadCpuMs = () => {
  const use = (process as { threadCpuUsage?: () => NodeJS.CpuUsage }).threadCpuUsage?.() ?? process.cpuUsage();
  return (use.user + use.system) / 1000;
};
const open = () => {
  const transaction = buffer.transactionWithRepoContextHandoffs.bind(buffer);
  buffer.transactionWithRepoContextHandoffs = ((work: () => unknown) => {
    const started = performance.now();
    try { return transaction(() => {
      const bodyStarted=performance.now();
      try {return work();}
      finally {if(activeTurn) activeTurn.writeLockBodyMs+=performance.now()-bodyStarted;}
    }); }
    finally { if (activeTurn) activeTurn.writeLockEnvelopeMs += performance.now() - started; }
  }) as typeof buffer.transactionWithRepoContextHandoffs;
  rollout = new RolloutTailer(buffer, undefined, () => [], DEFAULT_JSONL_TAILER_IO, roots.slice(0, 1));
  transcript = new TranscriptTailer(buffer, undefined, DEFAULT_JSONL_TAILER_IO, roots.slice(1));
  maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, undefined,
    { captureCoverageIntervalMs: 0, captureCoverageTurnMs: 250 });
};
open();
const restart = () => { maintenance.close(); buffer.close(); buffer = new LocalEventBuffer(ledger, options); open(); };
const cursor = (file: string) => buffer.database.prepare(`select committed_offset as offset, deferred_bytes as deferred,
  work_remaining as remaining, unresolved_kind as unresolved from rollout_scan_state where file = ?`)
  .get(jsonlScanStateKey(rootCursorKey(roots, file))) as { offset: number; deferred: number; remaining: number; unresolved: string | null } | undefined;
const total = (session: string) => buffer.database.prepare(`select count(*) as n, coalesce(sum(input_tokens),0) as tokens
  from buffered_events where session_id = ?`).get(session) as { n: number; tokens: number };
const run = async (turns: number) => { const results = []; for (let i = 0; i < turns; i++) {
  const started = performance.now(), cpu = threadCpuMs();
  activeTurn = {writeLockBodyMs:0,writeLockEnvelopeMs:0};
  const result = await maintenance.runRecent();
  const sample = {cpuMs:threadCpuMs()-cpu,wallMs:performance.now()-started,
    bytesRead:result.rollout.bytesRead+result.transcript.bytesRead,
    writeLockBodyMs:activeTurn.writeLockBodyMs,writeLockEnvelopeMs:activeTurn.writeLockEnvelopeMs};
  activeTurn = null;
  if (sample.bytesRead > 0) costTurns.push(sample);
  results.push(result);
} return results; };
const percentile = (values:number[], fraction:number) => [...values].sort((a,b)=>a-b)[Math.max(0,Math.ceil(values.length*fraction)-1)] ?? 0;
const costSummary = () => {
  const field = (key:keyof typeof costTurns[number]) => ({p95:percentile(costTurns.map(x=>x[key]),.95),
    max:Math.max(0,...costTurns.map(x=>x[key]))});
  return {activeTurns:costTurns.length,cpuMs:field("cpuMs"),wallMs:field("wallMs"),
    bytesRead:field("bytesRead"),writeLockBodyMs:field("writeLockBodyMs"),
    writeLockEnvelopeMs:field("writeLockEnvelopeMs"),
    totalBytesRead:costTurns.reduce((n,x)=>n+x.bytesRead,0),
    totalActiveWallMs:costTurns.reduce((n,x)=>n+x.wallMs,0)};
};
const intakeBody = (session:string, sequence:number) => JSON.stringify({resourceLogs:[{resource:{attributes:[
  {key:"service.name",value:{stringValue:"codex_cli_rs"}}]},scopeLogs:[{logRecords:[{
    timeUnixNano:String(BigInt(Date.now())*1000000n+BigInt(sequence)),attributes:[
      {key:"event.name",value:{stringValue:"codex.sse_event"}},
      {key:"conversation.id",value:{stringValue:session}},
      {key:"event.kind",value:{stringValue:"response.completed"}},
      {key:"event.sequence",value:{intValue:String(sequence)}},
      {key:"input_token_count",value:{intValue:"1"}},
      {key:"output_token_count",value:{intValue:"1"}},
    ]}]}]}]});
const postIntake = (socketPath:string, session:string, sequence:number) => new Promise<{status:number;ms:number}>((resolve,reject)=>{
  const body=intakeBody(session,sequence), started=performance.now();
  const request=http.request({socketPath,path:"/v1/logs",method:"POST",headers:{
    "content-type":"application/json","content-length":Buffer.byteLength(body),"x-plimsoll-source":"codex",
  }},response=>{response.resume();response.on("end",()=>resolve({status:response.statusCode??0,ms:performance.now()-started}));});
  request.on("error",reject);request.end(body);
});
const checks: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
const check = (name: string, passed: boolean, detail?: unknown) => checks.push({ name, passed, detail });
export async function runUncoveredCatchupCase() {
let server: ReturnType<typeof createCollectorServer> | undefined;
try {
  const queue = new CaptureRevisitQueue(2);
  queue.offer("a"); queue.offer("b"); queue.next(1); queue.offer("c");
  check("bounded revisit queue admits new files after service", JSON.stringify(queue.next(2)) === JSON.stringify(["b","c"]));
  for (let i = 0; i < 30 && captureBaselineStatus(buffer.database).status !== "complete"; i++) await run(1);
  check("baseline completes", captureBaselineStatus(buffer.database).status === "complete");
  stamp = new Date().toISOString();

  const neverId = id("2"), never = codexFile(old, "never-started");
  write(never, codex(neverId, [0, 5, 9]));
  const claudeId = id("3"), oldClaude = claudeFile("old-claude");
  write(oldClaude, [claude(claudeId, 7)]);
  const aged = new Date(now.getTime() - 7 * 86400000);
  fs.utimesSync(oldClaude, aged, aged);
  await run(12);
  check("no_tailer_row Codex old day", cursor(never)?.offset === fs.statSync(never).size && total(neverId).tokens === 9,
    { cursor: cursor(never), total: total(neverId) });
  check("no_tailer_row Claude old mtime", cursor(oldClaude)?.offset === fs.statSync(oldClaude).size && total(claudeId).n === 1,
    { cursor: cursor(oldClaude), total: total(claudeId) });

  fs.appendFileSync(beforeEnrollment, encode(codex(id("1"), [3, 8]).slice(2)));
  fs.appendFileSync(boundaryFragment, `after"}}\n${encode(codex(id("b"), [0,3]).slice(2))}`);
  await run(12);
  check("pre-enrollment generation reads post-enrollment append", cursor(beforeEnrollment)?.offset === fs.statSync(beforeEnrollment).size && total(id("1")).tokens === 5,
    { cursor: cursor(beforeEnrollment), total: total(id("1")) });
  check("mid-record enrollment boundary is skipped with known loss", cursor(boundaryFragment)?.offset === fs.statSync(boundaryFragment).size &&
    total(id("b")).tokens === 3 && Boolean(buffer.database.prepare(`select 1 from sqlite_master where type='table' and name='capture_record_losses'`).get()) &&
    Boolean(buffer.database.prepare(`select 1 from capture_record_losses where reason='enrollment_boundary_fragment' and usage_possible=1`).get()),
    {cursor:cursor(boundaryFragment),total:total(id("b"))});

  const stuckId = id("4"), stuck = codexFile(old, "stuck");
  write(stuck, codex(stuckId, [0, ...Array.from({ length: 140 }, (_, i) => i + 1)], 6500));
  let gate = 0;
  await rollout.scan({ scope: "full", now, onProgress: (progress) => progress.stage !== "jsonl_validation" || ++gate <= 1 });
  const first = cursor(stuck);
  await run(30);
  check("work_remaining old file finishes", Boolean(first?.remaining) && cursor(stuck)?.offset === fs.statSync(stuck).size,
    { first, final: cursor(stuck) });

  const behindId = id("5"), behind = codexFile(old, "behind");
  write(behind, codex(behindId, [0, 2]));
  await rollout.scan({ scope: "full", now });
  const behindBefore = cursor(behind)?.offset;
  fs.appendFileSync(behind, encode(codex(behindId, [6]).slice(2)));
  await run(12);
  check("behind old day append finishes", Boolean(behindBefore) && cursor(behind)?.offset === fs.statSync(behind).size && total(behindId).tokens === 6,
    { before: behindBefore, final: cursor(behind), total: total(behindId) });

  const largeId = id("6"), large = codexFile(old, "oversized");
  write(large, [...codex(largeId, [0]), { type: "event_msg", timestamp: stamp,
    payload: { type: "user_message", rate_limits: { plan_type: "q".repeat(5000) },
      message: "x".repeat(17 * 1024 * 1024) } }, ...codex(largeId, [4]).slice(2)]);
  const socketPath=path.join(base,"collector.sock");
  const config=collectorConfigSchema.parse({uploadUrl:"http://127.0.0.1:1/ingest",tenantId:options.workspaceId,
    installKey:"fixture-install",deviceId:"fixture-device",managed:true});
  server=createCollectorServer(config,buffer);
  await new Promise<void>((resolve)=>server!.listen(socketPath,resolve));
  const intakeSession=id("a"), intake: Array<{status:number;ms:number}>=[];
  let intakeDone=false;
  const intakePump=(async()=>{for(let sequence=1;sequence<=40&&!intakeDone;sequence++){
    intake.push(await postIntake(socketPath,intakeSession,sequence));
    await new Promise<void>(resolve=>setTimeout(resolve,2));
  }})();
  await run(700);
  intakeDone=true;
  await intakePump;
  check("daemon intake p95 under 300 ms during catch-up", intake.length>=10 &&
    intake.every(x=>x.status===202) && percentile(intake.map(x=>x.ms),.95)<300,
    {count:intake.length,statuses:[...new Set(intake.map(x=>x.status))],p95Ms:percentile(intake.map(x=>x.ms),.95),
      maxMs:Math.max(0,...intake.map(x=>x.ms))});
  check("oversized non-usage record skips and later usage arrives", cursor(large)?.offset === fs.statSync(large).size && total(largeId).tokens === 4,
    { cursor: cursor(large), total: total(largeId) });
  const hasLossTable = Boolean(buffer.database.prepare(`select 1 from sqlite_master where type='table' and name='capture_record_losses'`).get());
  const nonUsageLoss = hasLossTable ? buffer.database.prepare(`select kind, usage_possible as possible, skipped_bytes as bytes
    from capture_record_losses where source='codex' and usage_possible=0`).get() as {kind:string;possible:number;bytes:number}|undefined : undefined;
  const uncoveredLarge = buffer.database.prepare(`select 1 from capture_uncovered_files where file_key=? limit 1`)
    .get(jsonlScanStateKey(rootCursorKey(roots,large)));
  check("oversized non-usage is counted but coverage closes", nonUsageLoss?.kind === "codex_non_usage" &&
    nonUsageLoss.bytes > 16 * 1024 * 1024 && !uncoveredLarge, {nonUsageLoss,uncoveredLarge:!!uncoveredLarge});
  const lossColumns=(buffer.database.prepare(`pragma table_info(capture_record_losses)`).all() as Array<{name:string}>).map(x=>x.name).sort();
  check("loss receipts store counts and classification only",JSON.stringify(lossColumns)===JSON.stringify([
    "detected_at","identity_key","installation_epoch_id","kind","reason","skipped_bytes","source","usage_possible",
  ]),{columns:lossColumns});

  const lossId = id("8"), usageLoss = codexFile(old, "oversized-usage");
  write(usageLoss, [...codex(lossId, [0]), { type: "event_msg", timestamp: stamp,
    payload: { type: "token_count", rate_limits: { plan_type: "q".repeat(5000) },
      info: { total_token_usage: {input_tokens:2,cached_input_tokens:0,output_tokens:0,reasoning_output_tokens:0} } },
    padding: "x".repeat(17 * 1024 * 1024) }, ...codex(lossId, [4,6]).slice(2)]);
  await run(700);
  const usageLossRow = hasLossTable ? buffer.database.prepare(`select kind, usage_possible as possible, skipped_bytes as bytes
    from capture_record_losses where source='codex' and usage_possible=1 and skipped_bytes > ?`)
    .get(16 * 1024 * 1024) as {kind:string;possible:number;bytes:number}|undefined : undefined;
  const uncoveredUsageLoss=buffer.database.prepare(`select 1 from capture_uncovered_files where file_key=? limit 1`)
    .get(jsonlScanStateKey(rootCursorKey(roots,usageLoss)));
  check("oversized usage is a known loss; later marginal is safe", cursor(usageLoss)?.offset === fs.statSync(usageLoss).size &&
    total(lossId).tokens === 2 && usageLossRow?.kind === "codex_token_count" && usageLossRow.possible === 1 &&
    usageLossRow.bytes > 16 * 1024 * 1024 && !uncoveredUsageLoss && Boolean(captureFrontier(buffer.database)?.gaps.length),
    {cursor:cursor(usageLoss),total:total(lossId),usageLossRow,uncovered:!!uncoveredUsageLoss,
      gaps:captureFrontier(buffer.database)?.gaps.length});

  const claudeLossId = id("9"), claudeLoss = claudeFile("oversized-usage");
  const skippedClaude = claude(claudeLossId, 11) as ReturnType<typeof claude> & {padding?: string};
  skippedClaude.message.model = "q".repeat(5000);
  skippedClaude.padding = "x".repeat(17 * 1024 * 1024);
  write(claudeLoss, [skippedClaude, claude(claudeLossId, 7)]);
  await run(700);
  const claudeLossRow = hasLossTable ? buffer.database.prepare(`select kind, usage_possible as possible, skipped_bytes as bytes
    from capture_record_losses where source='claude_code' and usage_possible=1`).get() as {kind:string;possible:number;bytes:number}|undefined : undefined;
  const uncoveredClaudeLoss=buffer.database.prepare(`select 1 from capture_uncovered_files where file_key=? limit 1`)
    .get(jsonlScanStateKey(rootCursorKey(roots,claudeLoss)));
  check("oversized Claude assistant is known loss and later usage arrives", cursor(claudeLoss)?.offset === fs.statSync(claudeLoss).size &&
    total(claudeLossId).tokens === 7 && claudeLossRow?.kind === "claude_assistant" && claudeLossRow.bytes > 16 * 1024 * 1024 &&
    !uncoveredClaudeLoss,
    {cursor:cursor(claudeLoss),total:total(claudeLossId),claudeLossRow,uncovered:!!uncoveredClaudeLoss});

  const rewriteId = id("7"), rewrite = codexFile(old, "rewrite");
  const original = codex(rewriteId, [0, 2]);
  (original[0]!.payload as Record<string, unknown>).ignored = "a";
  write(rewrite, original);
  await rollout.scan({ scope: "full", now });
  const before = total(rewriteId);
  // Same-inode, same-size rewrite keeps event identities, then a new append.
  const sameSize = codex(rewriteId, [0, 2]);
  (sameSize[0]!.payload as Record<string, unknown>).ignored = "b";
  write(rewrite, sameSize);
  restart();
  await run(12);
  const afterRewrite = cursor(rewrite);
  fs.appendFileSync(rewrite, encode(codex(rewriteId, [5]).slice(2)));
  await run(12);
  check("same-inode rewrite completes without duplicate usage", cursor(rewrite)?.offset === fs.statSync(rewrite).size &&
    afterRewrite?.offset === fs.statSync(rewrite).size - Buffer.byteLength(encode(codex(rewriteId, [5]).slice(2))) &&
    total(rewriteId).tokens === 5 && total(rewriteId).n === before.n + 1,
    { cursor: cursor(rewrite), afterRewrite, before, final: total(rewriteId) });

  const transcriptRewriteId=id("d"), transcriptRewrite=claudeFile("rewrite-claude");
  const originalTranscript=[claude(transcriptRewriteId,2),claude(transcriptRewriteId,3)];
  (originalTranscript[0] as Record<string,unknown>).ignored="a";
  write(transcriptRewrite,originalTranscript);
  await transcript.scan({scope:"full",now});
  const transcriptBefore=total(transcriptRewriteId);
  const revisedTranscript=[claude(transcriptRewriteId,2),claude(transcriptRewriteId,3)];
  (revisedTranscript[0] as Record<string,unknown>).ignored="b";
  write(transcriptRewrite,revisedTranscript);
  restart();
  await run(12);
  const transcriptAfterRewrite=cursor(transcriptRewrite);
  fs.appendFileSync(transcriptRewrite,encode([claude(transcriptRewriteId,7)]));
  await run(12);
  check("Claude rewrite replays once and later usage arrives",transcriptAfterRewrite?.offset===fs.statSync(transcriptRewrite).size-
    Buffer.byteLength(encode([claude(transcriptRewriteId,7)])) && cursor(transcriptRewrite)?.offset===fs.statSync(transcriptRewrite).size &&
    total(transcriptRewriteId).n===transcriptBefore.n+1 && total(transcriptRewriteId).tokens===transcriptBefore.tokens+7,
    {cursor:cursor(transcriptRewrite),afterRewrite:transcriptAfterRewrite,before:transcriptBefore,final:total(transcriptRewriteId)});

  if (process.env.PLIMSOLL_LARGE_CATCHUP === "1") {
    // Earlier rewrite cases reopen the ledger. Serve this intake probe from
    // that current buffer, not the first server's now-closed fixture buffer.
    await new Promise<void>(resolve => server!.close(() => resolve()));
    server=createCollectorServer(config,buffer);
    const largeSocketPath=path.join(base,"collector-large.sock");
    await new Promise<void>(resolve => server!.listen(largeSocketPath,resolve));
    const hugeId=id("c"), huge=codexFile(old,"four-hundred-megabytes");
    write(huge,codex(hugeId,[0]));
    const fd=fs.openSync(huge,"a"), chunk=Buffer.alloc(1024*1024,0x78);
    try {
      for(let section=1;section<=4;section++) {
        fs.writeSync(fd,`{"type":"event_msg","timestamp":"${stamp}","payload":{"type":"user_message","rate_limits":{"plan_type":"${"q".repeat(5000)}"},"message":"`);
        for(let block=0;block<101;block++) fs.writeSync(fd,chunk);
        fs.writeSync(fd,`"}}\n${encode(codex(hugeId,[section*100]).slice(2))}`);
      }
    } finally {fs.closeSync(fd);}
    const fileHash=crypto.createHash("sha256").update(huge).digest("hex");
    let previous=0,advanced=0,setupTurns=0,activeTurns=0;
    const hugeIntake: Array<{status:number;ms:number}>=[];
    let hugeIntakeDone=false;
    const hugePump=(async()=>{for(let sequence=1;sequence<=40&&!hugeIntakeDone;sequence++){
      hugeIntake.push(await postIntake(largeSocketPath,id("e"),sequence));
      await new Promise<void>(resolve=>setTimeout(resolve,2));
    }})();
    for(let turn=0;turn<1500&&cursor(huge)?.offset!==fs.statSync(huge).size;turn++) {
      const [result]=await run(1);
      const state=buffer.database.prepare(`select envelope_json as json from jsonl_continuations
        where provider='codex' and file_key=?`).get(fileHash) as {json:string}|undefined;
      const progress=Math.max(cursor(huge)?.offset??0,state?JSON.parse(state.json).prefix?.end??0:0);
      if(result.rollout.bytesRead>0&&progress>0) {
        activeTurns++;
        if(progress>previous) advanced++;
        else setupTurns++;
      }
      previous=Math.max(previous,progress);
    }
    hugeIntakeDone=true;
    await hugePump;
    await run(2);
    const hugeUncovered=buffer.database.prepare(`select 1 from capture_uncovered_files where file_key=? limit 1`)
      .get(jsonlScanStateKey(rootCursorKey(roots,huge)));
    check("404 MiB rollout advances durably with usage across the file",cursor(huge)?.offset===fs.statSync(huge).size &&
      total(hugeId).tokens===400 && advanced>=100 && setupTurns<=8 && !hugeUncovered,
      {fileBytes:fs.statSync(huge).size,cursor:cursor(huge),total:total(hugeId),activeTurns,advanced,setupTurns,
        uncovered:!!hugeUncovered});
    check("daemon intake stays below 300 ms during 404 MiB catch-up",hugeIntake.length>=10 &&
      hugeIntake.every(x=>x.status===202) && percentile(hugeIntake.map(x=>x.ms),.95)<300,
      {count:hugeIntake.length,p95Ms:percentile(hugeIntake.map(x=>x.ms),.95),
        maxMs:Math.max(0,...hugeIntake.map(x=>x.ms))});
  }

  const passed = checks.every((entry) => entry.passed);
  console.log(JSON.stringify({ schema: "plimsoll.uncovered-catchup-proof.v1", syntheticOnly: true,
    cost:costSummary(),checks,passed }, null, 2));
  if (!passed) process.exitCode = 1;
  return passed;
} finally {
  if (server) await new Promise<void>(resolve=>server!.close(()=>resolve()));
  maintenance.close(); buffer.close(); fs.rmSync(base, { recursive: true, force: true });
}
}
if (process.argv[1] && fs.realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runUncoveredCatchupCase().catch((error) => { console.error(error); process.exitCode = 1; });
}
