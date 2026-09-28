/** An authenticated ingest acknowledgement changes the durable capability. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { readActivitySummaryAdvertisement } from "../packages/collector-cli/src/lean/activity-summary-capability";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(),"b22-ad-ack-"));
const tenantId = "00000000-0000-4000-8000-000000000081";
const installKey = "pli_b22_advertisement_fixture_install";
const cloudDeviceId = "00000000-0000-4000-8000-000000000082";
const config = collectorConfigSchema.parse({
  uploadUrl:"http://127.0.0.1:49771/api/work-intelligence/ingest",
  tenantId, installKey, delivery:{maxOldestAgeDays:3650},
});
const buffer = new LocalEventBuffer(path.join(root,"ledger.sqlite"),{
  workspaceId:tenantId,delivery:{enabled:true,limits:{maxOldestAgeDays:3650}},
});
let seq = 0;
let advertisement: number | undefined;
const fetchImpl: typeof fetch = async (_input, init) => {
  const rawBody = String(init?.body ?? "");
  if (JSON.parse(rawBody).kind === "activity_summary_v2") {
    const claim = JSON.parse(new Headers(init?.headers).get("x-plimsoll-capture") ?? "null");
    return new Response(JSON.stringify({ok:true,receipts:[],
      capture:{status:"advanced",cursor:claim.cursor,state:"pending",acceptedThrough:null},
      actorBindingVersion:7,deviceId:cloudDeviceId}),
    {status:200,headers:{"content-type":"application/json"}});
  }
  const expected = deliveryExpectation(rawBody,installKey);
  const body = {ok:true,accepted:expected.itemIds.length,
    ack:deliveryAcknowledgement(expected,expected.itemIds),
    ...(advertisement === undefined ? {} : {activitySummaryContractVersion:advertisement}),
    actorBindingVersion:7,deviceId:cloudDeviceId};
  return new Response(JSON.stringify(body),{status:200,headers:{"content-type":"application/json"}});
};
const append = () => {
  seq++;
  buffer.append(aiInteractionEventSchema.parse({
    id:`00000000-0000-4000-8000-${String(seq).padStart(12,"0")}`,
    sessionId:`00000000-0000-4000-8000-${String(seq+100).padStart(12,"0")}`,
    source:"codex",dataMode:"metadata",eventType:"assistant_response",
    observedAt:new Date().toISOString(),actionClass:"other",inputTokens:1,outputTokens:1,
  }));
};
async function main() {
try {
  append();
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome:path.join(root,"spools")});
  assert.equal(readActivitySummaryAdvertisement(buffer.database,installKey).enabled,false);
  advertisement=2;
  append();
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome:path.join(root,"spools")});
  assert.equal(readActivitySummaryAdvertisement(buffer.database,installKey).enabled,true);
  buffer.close();
  const reopened = new LocalEventBuffer(path.join(root,"ledger.sqlite"),{
    workspaceId:tenantId,delivery:{enabled:true,limits:{maxOldestAgeDays:3650}},
  });
  try {
    assert.equal(readActivitySummaryAdvertisement(reopened.database,installKey).enabled,true);
    console.log("PASS authenticated_ingest_ack_enables_v2_durably");
    advertisement=undefined;
    seq++;
    reopened.append(aiInteractionEventSchema.parse({
      id:`00000000-0000-4000-8000-${String(seq).padStart(12,"0")}`,
      sessionId:`00000000-0000-4000-8000-${String(seq+100).padStart(12,"0")}`,
      source:"codex",dataMode:"metadata",eventType:"assistant_response",
      observedAt:new Date().toISOString(),actionClass:"other",inputTokens:1,outputTokens:1,
    }));
    await uploadBufferedEvents(config,reopened,{fetchImpl,spoolHome:path.join(root,"spools")});
    assert.equal(readActivitySummaryAdvertisement(reopened.database,installKey).enabled,false);
    console.log("PASS next_authenticated_ack_without_advertisement_withdraws_v2");
  } finally { reopened.close(); }
} finally {
  try {buffer.close();} catch {}
  fs.rmSync(root,{recursive:true,force:true});
}
}
main().catch((error) => { console.error(error); process.exitCode=1; });
