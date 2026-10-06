import assert from "node:assert/strict";
import { runCaptureDowngradeFixtures, runCaptureDowngrade047Fixtures } from "./lib/capture-downgrade-fixture";
import { runR2DirectoryCaptureFixture } from "./lib/r2-directory-capture-fixture";

runCaptureDowngradeFixtures().then(async observations => {
  assert.equal(observations.length, 4);
  const r2DirectoryUpgrade = await runR2DirectoryCaptureFixture();
  assert.equal(r2DirectoryUpgrade.observations.length, 3);
  const released047 = await runCaptureDowngrade047Fixtures();
  assert.equal(released047.length, 2);
  console.log(JSON.stringify({ proof: "home-capture-downgrade", status: "PASS", checks: 7, observations, r2DirectoryUpgrade, released047 }));
}).catch(error => { console.error(error); process.exitCode = 1; });
