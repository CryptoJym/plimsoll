#!/usr/bin/env node
import assert from "node:assert/strict";
import Database from "better-sqlite3";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  disabledRepoContextDrainReceipt,
  ensureRepoContextReplaySchema,
  readLatestRepoContextDrainReceipt,
  writeRepoContextDrainReceipt,
} from "../packages/collector-cli/src/repo-context-replay-state";

const config = collectorConfigSchema.parse({});
assert.deepEqual(config.repoContextDrain, {
  enabled: false,
  scanSliceMs: 50,
  maxContextsPerRun: 64,
  maxDistinctCwdsPerRun: 8,
  expireEnabled: false,
  expireAfterCompletePasses: 2,
  expireLinksPerRun: 256,
});
assert.equal(collectorConfigSchema.safeParse({
  repoContextDrain: { scanSliceMs: 51 },
}).success, false);
assert.equal(collectorConfigSchema.safeParse({
  repoContextDrain: { maxContextsPerRun: 65 },
}).success, false);
assert.equal(collectorConfigSchema.safeParse({
  repoContextDrain: { maxDistinctCwdsPerRun: 9 },
}).success, false);
assert.equal(collectorConfigSchema.safeParse({
  repoContextDrain: { expireAfterCompletePasses: 1 },
}).success, false);
assert.equal(collectorConfigSchema.safeParse({
  repoContextDrain: { expireLinksPerRun: 257 },
}).success, false);

const database = new Database(":memory:");
ensureRepoContextReplaySchema(database);
ensureRepoContextReplaySchema(database);
const disabled = disabledRepoContextDrainReceipt();
writeRepoContextDrainReceipt(database, disabled);
assert.deepEqual(readLatestRepoContextDrainReceipt(database), disabled);
assert.equal(disabled.status, "disabled");
assert.equal(disabled.rowsInspected, 0);
assert.equal(disabled.candidateContexts, 0);
assert.equal(disabled.distinctCwdGroups, 0);
assert.equal(disabled.successfulContexts, 0);
assert.equal(disabled.expiredLinks, 0);
assert.equal(disabled.reResolvedExpiredLinks, 0);
database.close();

console.log(JSON.stringify({ proof: "repo_context_drain", checks: 14, passed: 14 }));
