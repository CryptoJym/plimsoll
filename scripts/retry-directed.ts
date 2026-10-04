import assert from 'node:assert/strict';
import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("codex-capture-retry", 1);
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { aiInteractionEventSchema } from '../packages/shared/src/index';

const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-retry-'));
const workspace='11111111-1111-4111-8111-111111111111';
const trace='c'.repeat(32);
const targetId='00000000-0000-4000-8000-000000000101';
const evidenceId='00000000-0000-4000-8000-000000000102';
const at=Date.now()-300000;
let now=new Date(at+2000);
const b=new LocalEventBuffer(path.join(root,'ledger.sqlite'),{
  workspaceId:workspace,deviceId:'review-device',enrollmentNow:()=>new Date(at-1000),
  delivery:{enabled:true,now:()=>now},
});
try {
  const target=aiInteractionEventSchema.parse({
    id:targetId,source:'codex',sessionId:workspace,dataMode:'metadata',
    eventType:'assistant_response',observedAt:new Date(at).toISOString(),
    inputTokens:19,outputTokens:2,metadata:{traceId:trace,otelEventName:'handle_responses'},
  });
  assert.equal(b.append(target),true);
  const queued=b.database.prepare('select base_envelope_json as body from upload_outbox where delivery_id=?').get(targetId) as {body:string};
  const old=JSON.parse(queued.body);
  delete old.event.metadata.installationEpochId;
  const frozen=JSON.stringify(old);
  // A previous release sent this exact token-bearing body; the remote accepted
  // its ID and the ACK was lost. Simulate only that local retry state.
  b.database.prepare("update upload_outbox set sealed_envelope_json=?,sealed_bytes=?,attempt_count=1,state='retry' where delivery_id=?")
    .run(frozen,Buffer.byteLength(frozen),targetId);
  const rawBefore=(b.database.prepare('select payload_json as body from buffered_events where id=?').get(targetId) as {body:string}).body;
  now=new Date(at+63000);
  const retire=b.delivery.lease({now});
  assert.equal(retire.items.length,0);
  assert.equal(retire.locallyDead,1);
  const gapBase=b.database.prepare('select delivery_id as id,base_envelope_json as body from upload_outbox').get() as {id:string;body:string};
  assert.notEqual(gapBase.id,targetId);
  assert.equal(JSON.parse(gapBase.body).event.metadata.usageSource,'capture_gap');
  assert.equal(JSON.parse(gapBase.body).event.inputTokens,undefined);
  // Model evidence arrives after retirement and before the replacement's
  // first lease. It has no usage of its own, so cannot be a count duplicate.
  assert.equal(b.append(aiInteractionEventSchema.parse({
    id:evidenceId,source:'codex',sessionId:workspace,dataMode:'metadata',
    eventType:'otel_span',observedAt:new Date(at+1000).toISOString(),model:'gpt-6.1-sol',
    metadata:{traceId:trace,otelEventName:'codex.sse_event'},
  })),true);
  const second=b.delivery.lease({now});
  assert.equal(second.locallyDead,0);
  const replacement=second.items.find(item=>item.deliveryId===gapBase.id);
  assert.ok(replacement,'replacement reaches the actual outbox lease path');
  const rawAfter=(b.database.prepare('select payload_json as body from buffered_events where id=?').get(targetId) as {body:string}).body;
  assert.equal(rawAfter,rawBefore);
  const duplicate=(replacement.envelope.event.inputTokens??0)+(replacement.envelope.event.outputTokens??0);
  console.log(JSON.stringify({
    case:'legacy-sealed-gap-with-late-trace-evidence',
    expected:'the distinct replacement stays tokenless after evidence arrives',
    passed:duplicate===0,
    originalRemoteAccepted:{id:targetId,inputTokens:19,outputTokens:2},
    retired:retire.locallyDead,gapBase:JSON.parse(gapBase.body),
    replacement:replacement.envelope,rawUnchanged:rawAfter===rawBefore,
    remoteTotalsIfBothAccepted:{inputTokens:19+(replacement.envelope.event.inputTokens??0),
      outputTokens:2+(replacement.envelope.event.outputTokens??0)},
  },null,2));
  completion.check("capture_gap_replacement_stays_tokenless_after_late_evidence", duplicate===0);
  if(duplicate!==0) process.exitCode=1;
  else completion.complete();
} finally { b.close();fs.rmSync(root,{recursive:true,force:true}); }
