import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("codex-legacy-model-evidence", 1);
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { LocalEventBuffer as NewBuffer } from '../packages/collector-cli/src/buffer';
import { aiInteractionEventSchema } from '../packages/shared/src/index';

const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-legacy-guess-'));
const file=path.join(root,'ledger.sqlite');
const workspace='11111111-1111-4111-8111-111111111111';
const targetId='00000000-0000-4000-8000-000000000401';
const peerId='00000000-0000-4000-8000-000000000402';
const targetSession='22222222-2222-4222-8222-222222222222';
const foreignSession='33333333-3333-4333-8333-333333333333';
const at=Date.now()-300000;
let now=new Date(at+2000);
const opts={workspaceId:workspace,deviceId:'review-device',enrollmentNow:()=>new Date(at-1000),
  delivery:{enabled:true,now:()=>now}};
async function main() {
  let old:{ close: () => void; database: any; append: (event: any) => boolean }|undefined,
    newer:NewBuffer|undefined;
  let oldRoot: string | undefined;
  try {
  // Keep the reproducer self-contained in CI while still exercising the
  // actual 0.7.48 reader and reconciliation implementation. The checkout has
  // the pinned commit in its history; make a disposable worktree for it and
  // share only the already-installed native dependencies.
  oldRoot=fs.mkdtempSync(path.join(os.tmpdir(),'plimsoll-0748-reader-'));
  fs.rmSync(oldRoot,{recursive:true,force:true});
  execFileSync('git',['worktree','add','--detach','--quiet',oldRoot,
    '34d58bcd90865679e09fcbd1ee1703de5effda97'],{cwd:process.cwd(),stdio:'ignore'});
  const installed=path.join(process.cwd(),'node_modules');
  if (fs.existsSync(installed)) fs.symlinkSync(installed,path.join(oldRoot,'node_modules'),'dir');
  const oldModules=await import(pathToFileURL(path.join(oldRoot,'packages/collector-cli/src/buffer.ts')).href);
  const oldReconciliation=await import(pathToFileURL(path.join(oldRoot,'packages/collector-cli/src/codex-reconciliation.ts')).href);
  old=new oldModules.LocalEventBuffer(file,opts);
  const target=aiInteractionEventSchema.parse({id:targetId,sessionId:targetSession,
    source:'codex',dataMode:'metadata',eventType:'assistant_response',
    observedAt:new Date(at).toISOString(),inputTokens:19,outputTokens:2,
    metadata:{otelEventName:'handle_responses',traceId:'a'.repeat(32)},
  });
  assert.equal(old.append(aiInteractionEventSchema.parse({id:peerId,sessionId:foreignSession,
    source:'codex',dataMode:'metadata',eventType:'otel_span',
    observedAt:new Date(at+1000).toISOString(),model:'gpt-6-astra',
    metadata:{otelEventName:'thread/read',traceId:'b'.repeat(32)},
  })),true);
  assert.equal(old.append(target),true);
  const maintenance=oldReconciliation.runCodexReconciliationMaintenance(old.database,{legacyRowLimit:100,legacyChunkLimit:100,
    contextWindowLimit:100,contextRowLimit:100,candidateLimit:100,freshCandidateLimit:100,timeLimitMs:1000});
  const oldRaw=JSON.parse((old.database.prepare('select payload_json as payload from buffered_events where id=?').get(targetId) as {payload:string}).payload);
  assert.equal(oldRaw.model,'gpt-6-astra','base main really guesses a model from the other session and trace');
  assert.equal(oldRaw.sessionId,targetSession);
  old.close();old=undefined;
  now=new Date(at+123000);
  newer=new NewBuffer(file,opts);
  const leased=newer.delivery.lease({now});
  assert.equal(leased.locallyDead,0);
  const sent=leased.items.find(item=>item.rawId===targetId);
  assert.ok(sent);
  const passed=sent.envelope.event.model===undefined && sent.envelope.event.inputTokens===undefined;
  console.log(JSON.stringify({case:'base-main-guessed-model-on-upgrade',passed,
    base:'34d58bcd90865679e09fcbd1ee1703de5effda97',maintenance,
    originalWithoutModel:target,oldRaw,headDelivery:sent.envelope,
    expected:'foreign trace/session guess must not become billable model evidence'},null,2));
  completion.check("legacy_proximity_model_becomes_tokenless_gap", passed);
  if(!passed)process.exitCode=1;
  else completion.complete();
  } finally {
    old?.close();newer?.close();fs.rmSync(root,{recursive:true,force:true});
    if(oldRoot) {
      try { execFileSync('git',['worktree','remove','--force',oldRoot],{cwd:process.cwd(),stdio:'ignore'}); } catch {}
      fs.rmSync(oldRoot,{recursive:true,force:true});
    }
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
