import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { observeActivitySummaryAdvertisement, readActivitySummaryAdvertisement } from
  "../packages/collector-cli/src/lean/activity-summary-capability";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(), "b22-capability-"));
const file = path.join(root, "ledger.sqlite");
const key = "install-test-1";
let buffer = new LocalEventBuffer(file);
try {
  const db = buffer.database;
  assert.equal(readActivitySummaryAdvertisement(db, key).enabled, false);
  observeActivitySummaryAdvertisement(db, key, { activitySummaryContractVersion:2,
    actorBindingVersion:4, deviceId:"00000000-0000-4000-8000-000000000001" }, 1000);
  assert.deepEqual(readActivitySummaryAdvertisement(db, key), {enabled:true,
    version:2,lastResponseAtMs:1000,lastV2AdvertisedAtMs:1000,
    actorBindingVersion:4,actorBindingInstallHeard:"00000000-0000-4000-8000-000000000001"});
  buffer.close();
  buffer = new LocalEventBuffer(file);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,key).enabled,true);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,"other-install").enabled,false);
  console.log("PASS v2_advertisement_durable_and_install_scoped");

  observeActivitySummaryAdvertisement(buffer.database, key,
    {activitySummaryContractVersion:1}, 2000);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,key).enabled,false);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,key).lastV2AdvertisedAtMs,1000);
  observeActivitySummaryAdvertisement(buffer.database, key, {}, 3000);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,key).version,0);
  console.log("PASS lower_or_absent_advertisement_withdraws_immediately");
} finally {
  buffer.close();
  fs.rmSync(root,{recursive:true,force:true});
}
