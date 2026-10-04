import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { RolloutTailer } from '../packages/collector-cli/src/rollout-tailer';
import { collectorConfigSchema } from '../packages/collector-cli/src/config';
import { uploadBufferedEvents } from '../packages/collector-cli/src/upload';
import { sealOutboundEnvelope } from '../packages/collector-cli/src/outbound-envelope';
import { codexPlanLimitWindows } from '../packages/collector-cli/src/plan-limit-observation';
import { acceptedFixtureDelivery } from './lib/delivery-fixture';

async function main() {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-a03-'));
  const codexHome=path.join(root,'.codex');
  const sessions=path.join(codexHome,'sessions');
  const day=path.join(sessions,'2026','09','29');
  fs.mkdirSync(day,{recursive:true});
  fs.writeFileSync(path.join(codexHome,'auth.json'),JSON.stringify({tokens:{account_id:'review-account'},last_refresh:'2026-09-29T00:00:00Z'}));
  const tenant='11111111-1111-4111-8111-111111111111';
  const cfg=collectorConfigSchema.parse({tenantId:tenant,deviceId:'review-device',
    installKey:'review-fixture-install',uploadUrl:'http://127.0.0.1:49849/ingest'});
  const b=new LocalEventBuffer(path.join(root,'ledger.sqlite'),{workspaceId:tenant,deviceId:cfg.deviceId,
    enrollmentNow:()=>new Date('2026-09-29T00:00:00Z'),
    delivery:{enabled:true,now:()=>new Date('2026-09-29T09:02:00Z')},
  });
  const tailer=new RolloutTailer(b,sessions,()=>[]);
  const line=(timestamp:string,type:string,payload:object)=>JSON.stringify({timestamp,type,payload});
  const session='019e9999-1111-7222-8333-444444444444';
  try {
    await tailer.scan({scope:'full',now:new Date('2026-09-29T08:00:00Z')});
    fs.writeFileSync(path.join(day,`rollout-review-${session}.jsonl`),[
      line('2026-09-29T09:00:00Z','session_meta',{id:session}),
      line('2026-09-29T09:00:01Z','event_msg',{type:'token_count',info:{total_token_usage:{input_tokens:0,output_tokens:0,cached_input_tokens:0}}}),
      line('2026-09-29T09:00:02Z','turn_context',{turn_id:'review-turn',model:'gpt-6-sol'}),
      line('2026-09-29T09:00:03Z','event_msg',{type:'token_count',
        info:{total_token_usage:{input_tokens:10,output_tokens:2,cached_input_tokens:0}},
        rate_limits:{primary:{used_percent:40,window_minutes:525601,resets_at:'2026-09-29T14:00:00Z'}}}),
    ].join('\n')+'\n');
    await tailer.scan({scope:'full',now:new Date('2026-09-29T09:00:05Z')});
    const raw=(b.database.prepare('select payload_json as payload from buffered_events').all() as {payload:string}[]).map(r=>JSON.parse(r.payload));
    assert.equal(raw.filter(e=>e.eventType==='plan_limit_observation').length,0);
    assert.ok(raw.some(e=>e.eventType==='usage_rollout'&&e.inputTokens===10&&e.outputTokens===2));
    const requests:any[]=[];
    const result=await uploadBufferedEvents(cfg,b,{
      now:()=>new Date('2026-09-29T09:03:00Z'),
      fetchImpl:async(input,init)=>{
        assert.ok(String(input).startsWith('http://127.0.0.1:49849/'));
        const body=String(init?.body);
        requests.push(JSON.parse(body));
        return new Response(JSON.stringify(acceptedFixtureDelivery(body,cfg.installKey!)),{
          status:200,headers:{'content-type':'application/json'},
        });
      },
    });
    assert.equal(result.uploadedEvents,1);
    assert.equal(requests.length,1);
    assert.equal(requests[0].events.length,1);
    assert.equal(requests[0].events[0].event.inputTokens,10);
    assert.equal(requests[0].events[0].event.eventType,'usage_rollout');
    const keyless=sealOutboundEnvelope({event:{
      id:'00000000-0000-4000-8000-000000000301',source:'codex',dataMode:'metadata',
      eventType:'plan_limit_observation',observedAt:'2026-09-29T09:00:03Z',metadata:{
        planLimitSource:'codex_rollout',planLimitWindow:'five_hour',planLimitWindowMinutes:300,
        planLimitUsedPercent:40,planLimitResetsAt:'2026-09-29T14:00:00Z',
      },
    },suppressedFields:[]});
    assert.ok(keyless.ok,'valid legacy keyless reading remains sealable');
    assert.equal(codexPlanLimitWindows({primary:{used_percent:40,window_minutes:525600,resets_at:'2026-09-29T14:00:00Z'}}).length,1);
    console.log(JSON.stringify({case:'A03-actual-rollout-usage-upload',passed:true,
      rawReadings:0,uploadedEvents:result.uploadedEvents,requests:requests.length,
      usage:requests[0].events[0],validKeylessSealable:keyless.ok,maxWindowAccepted:true},null,2));
  } finally {tailer.close();b.close();fs.rmSync(root,{recursive:true,force:true});}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
