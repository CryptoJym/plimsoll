#!/usr/bin/env node
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  disabledRepoContextDrainReceipt,
  ensureRepoContextReplaySchema,
  readLatestRepoContextDrainReceipt,
  writeRepoContextDrainReceipt,
} from "../packages/collector-cli/src/repo-context-replay-state";
import {
  discoverRepoContextReplaySources,
  readRepoContextReplaySlice,
  repoContextReplayFileIdentity,
} from "../packages/collector-cli/src/repo-context-replay";

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

const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-repo-context-drain-")));
const codexRoot = path.join(root, "codex");
const claudeRoot = path.join(root, "claude");
fs.mkdirSync(codexRoot, { recursive: true });
fs.mkdirSync(claudeRoot, { recursive: true });
const codexSession = "019e3000-0000-7000-8000-000000000001";
const claudeSession = "019e3000-0000-7000-8000-000000000002";
const codexFile = path.join(codexRoot, `rollout-${codexSession}.jsonl`);
const claudeFile = path.join(claudeRoot, `${claudeSession}.jsonl`);
const codexCwd = path.join(root, "repo-a");
const claudeCwd = path.join(root, "repo-b");
fs.writeFileSync(codexFile, `${JSON.stringify({
  timestamp: "2026-09-11T00:00:00.000Z",
  type: "session_meta",
  payload: { id: codexSession, cwd: codexCwd },
})}\n${JSON.stringify({ type: "response_item", payload: { text: "private" } })}\n`);
fs.writeFileSync(claudeFile, `${JSON.stringify({
  timestamp: "2026-09-11T00:00:00.000Z",
  type: "assistant",
  sessionId: claudeSession,
  cwd: claudeCwd,
  message: { id: "message-1", model: "claude", usage: { input_tokens: 1 } },
})}\n`);

const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
const roots = [
  { rootId: "codex-root", profileId: "codex-profile", installationEpochId: "epoch-a",
    source: "codex" as const, directory: codexRoot },
  { rootId: "claude-root", profileId: "claude-profile", installationEpochId: "epoch-b",
    source: "claude_code" as const, directory: claudeRoot },
];
const discovery = discoverRepoContextReplaySources(roots, { deadlineMs: 50 });
assert.equal(discovery.complete, true);
assert.equal(discovery.sources.length, 2);
const codexSource = discovery.sources.find((source) => source.source === "codex")!;
const claudeSource = discovery.sources.find((source) => source.source === "claude_code")!;
const codexSlice = readRepoContextReplaySlice(buffer, codexSource, null, { maxContexts: 64 });
const claudeSlice = readRepoContextReplaySlice(buffer, claudeSource, null, { maxContexts: 64 });
assert.equal(codexSlice.candidates.length, 1);
assert.equal(claudeSlice.candidates.length, 1);
assert.equal(codexSlice.candidates[0]!.request.contextId,
  buffer.repoContextOccurrenceRequest("codex", [
    "codex-rollout", repoContextReplayFileIdentity(codexFile), codexSession, "session_meta", "0",
  ].join(":"), codexCwd)!.contextId);
assert.equal(claudeSlice.candidates[0]!.request.contextId,
  buffer.repoContextOccurrenceRequest("claude_code", [
    "claude-transcript", claudeSession,
    crypto.createHash("sha256").update("message-1").digest("hex"),
  ].join(":"), claudeCwd)!.contextId);
assert.equal((buffer.database.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n, 0,
  "replay parser must not append usage");
assert.equal(codexSlice.complete, true);
assert.equal(claudeSlice.complete, true);
buffer.close();
fs.rmSync(root, { recursive: true, force: true });

console.log(JSON.stringify({ proof: "repo_context_drain", checks: 23, passed: 23 }));
