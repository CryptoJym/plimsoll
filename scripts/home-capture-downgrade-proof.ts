import assert from "node:assert/strict";
import { runCaptureDowngradeFixtures } from "./lib/capture-downgrade-fixture";

runCaptureDowngradeFixtures().then(observations => {
  assert.equal(observations.length, 4);
  console.log(JSON.stringify({ proof: "home-capture-downgrade", status: "PASS", checks: 4, observations }));
}).catch(error => { console.error(error); process.exitCode = 1; });
