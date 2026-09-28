/** The later authenticated handshake acknowledgement governs a join grant. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { collectorBufferPath } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { performJoin } from "../packages/collector-cli/src/join";
import { readActivitySummaryAdvertisement } from "../packages/collector-cli/src/lean/activity-summary-capability";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(),"b22-ad-join-"));
const tenantId = "00000000-0000-4000-8000-000000000091";
const deviceId = "00000000-0000-4000-8000-000000000092";
const installKey = "pli_b22_join_advertisement_install";
async function scenario(name: string, grantAd: boolean, ackAd: boolean) {
  const homeDir = path.join(root,name);
  fs.mkdirSync(homeDir,{recursive:true});
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    if (url.pathname.endsWith("/join")) return new Response(JSON.stringify({
      ok:true,tenantId,deviceId,installKey,
      uploadUrl:"https://b22-fixture.example/api/work-intelligence/ingest",
      actorBindingVersion:3,
      ...(grantAd ? {activitySummaryContractVersion:2} : {}),
    }),{status:201,headers:{"content-type":"application/json"}});
    const body = String(init?.body ?? "");
    const expected = deliveryExpectation(body,installKey);
    return new Response(JSON.stringify({ok:true,accepted:expected.itemIds.length,
      ack:deliveryAcknowledgement(expected,expected.itemIds),deviceId,actorBindingVersion:3,
      ...(ackAd ? {activitySummaryContractVersion:2} : {}),
    }),{status:200,headers:{"content-type":"application/json"}});
  };
  const result = await performJoin({target:"pljt_b22_fixture_token",baseUrl:"https://b22-fixture.example",
    homeDir,fetchImpl,temporaryRoot:root,reassign:true});
  assert.equal(result.joined,true,result.joined ? "" : result.reason);
  const db = new Database(collectorBufferPath(homeDir),{readonly:true,fileMustExist:true});
  try { assert.equal(readActivitySummaryAdvertisement(db,installKey).enabled,ackAd); }
  finally { db.close(); }
  console.log(`PASS ${name}`);
}
async function main() {
  try {
    await scenario("later_handshake_withdraws_grant_v2",true,false);
    await scenario("later_handshake_enables_v2",false,true);
  } finally { fs.rmSync(root,{recursive:true,force:true}); }
}
main().catch((error)=>{console.error(error);process.exitCode=1;});
