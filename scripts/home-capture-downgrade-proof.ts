import assert from "node:assert/strict";
import { runCaptureDowngradeFixtures } from "./lib/capture-downgrade-fixture";
import { runR2DirectoryCaptureFixture } from "./lib/r2-directory-capture-fixture";

runCaptureDowngradeFixtures().then(async observations => {
  assert.equal(observations.length, 4);
  const r2DirectoryUpgrade = await runR2DirectoryCaptureFixture();
  assert.equal(r2DirectoryUpgrade.observations.length, 3);
  console.log(JSON.stringify({ proof: "home-capture-downgrade", status: "PASS", checks: 5, observations, r2DirectoryUpgrade }));
}).catch(error => { console.error(error); process.exitCode = 1; });
