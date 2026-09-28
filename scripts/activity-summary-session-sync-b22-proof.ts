/** Authenticated session-sync replies are persisted after the upload leases settle. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { readActivitySummaryAdvertisement } from "../packages/collector-cli/src/lean/activity-summary-capability";
import { runSessionSync } from "../packages/collector-cli/src/session-sync";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(), "b22-sync-ad-"));
const tenantId = "00000000-0000-4000-8000-000000000091";
const installKey = "pli_b22_session_sync_advertisement";
const file = path.join(root, "ledger.sqlite");
const config = collectorConfigSchema.parse({
  tenantId, installKey, uploadUrl:"https://b22-fixture.example/api/work-intelligence/ingest",
});
const buffer = new LocalEventBuffer(file);

async function main() {
  try {
    for (let index=1; index<=2; index++) {
      assert.equal(buffer.append(aiInteractionEventSchema.parse({
        id:`00000000-0000-4000-8000-${String(index).padStart(12,"0")}`,
        sessionId:`00000000-0000-4000-8000-${String(index+100).padStart(12,"0")}`,
        actorId:"sha256:sessionproofaccount0000000000000000000001",
        source:"codex",dataMode:"metadata",eventType:"assistant_response",
        observedAt:"2026-09-20T00:00:00.000Z",actionClass:"other",
        inputTokens:1,outputTokens:1,
      })),true);
    }
    let responses = 0;
    const fetchImpl: typeof fetch = async (_input, init) => {
      responses++;
      const expected = deliveryExpectation(String(init?.body ?? ""),installKey);
      return new Response(JSON.stringify({ok:true,accepted:expected.itemIds.length,
        ack:deliveryAcknowledgement(expected,expected.itemIds),
        actorBindingVersion:3,deviceId:"00000000-0000-4000-8000-000000000092",
        ...(responses===1 ? {activitySummaryContractVersion:2} : {}),
      }),{status:200,headers:{"content-type":"application/json"}});
    };
    const result = await runSessionSync(config,{ledgerPath:file,fetchImpl,
      until:"2026-09-30T00:00:00.000Z",batchSize:1,concurrency:2,
      delayMs:0,maxAttemptsPerBatch:1,log:()=>{}});
    assert.equal(result.ok,true,result.reason ?? "session sync failed");
    assert.equal(result.acceptedSessions,2,JSON.stringify({result,responses}));
    assert.equal(responses,2);
    const advertisement = readActivitySummaryAdvertisement(buffer.database,installKey);
    assert.equal(advertisement.enabled,false);
    assert.ok(advertisement.lastV2AdvertisedAtMs !== null);
    console.log("PASS session_sync_persists_latest_authenticated_reply_after_lease_path");
  } finally {
    buffer.close();
    fs.rmSync(root,{recursive:true,force:true});
  }
}
main().catch((error)=>{console.error(error);process.exitCode=1;});
