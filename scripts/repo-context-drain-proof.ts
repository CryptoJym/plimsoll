#!/usr/bin/env node
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { runRepoContextDrainStage } from "../packages/collector-cli/src/repo-context-drain";
import { remainingRepoContextDrainLookupMs } from "../packages/collector-cli/src/maintenance-worker";
import {
  ensureRepoContextLinkDispositionSchema,
  REPO_CONTEXT_EXPIRY_SELECTION_SQL,
  reResolveExpiredRepoContextLinks,
  rollbackRepoContextLinkDispositionMigration,
} from "../packages/collector-cli/src/repo-context-link-dispositions";
import {
  attachRepoContextSidecar,
  REPO_CONTEXT_RESOLVER_VERSION,
} from "../packages/collector-cli/src/repo-context";
import {
  disabledRepoContextDrainReceipt,
  beginRepoContextReplayPass,
  ensureRepoContextReplaySchema,
  recordRepoContextReplayAttempt,
  readLatestRepoContextDrainReceipt,
  repoContextReplayCursor,
  writeRepoContextDrainReceipt,
} from "../packages/collector-cli/src/repo-context-replay-state";
import {
  discoverRepoContextReplaySources,
  readRepoContextReplaySlice,
  REPO_CONTEXT_REPLAY_MAX_RECORD_BYTES,
  repoContextReplayFileIdentity,
} from "../packages/collector-cli/src/repo-context-replay";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

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
assert.equal(remainingRepoContextDrainLookupMs({
  gitBudgetMs: 10_000,
  remainingJobMs: 29_000,
  childFreshElapsedMs: 2_500,
  parentFreshElapsedMs: 7_000,
}), 500, "slow child and parent fresh lookups must share the drain's git allowance");
assert.equal(remainingRepoContextDrainLookupMs({
  gitBudgetMs: 10_000,
  remainingJobMs: 300,
  childFreshElapsedMs: 2_500,
  parentFreshElapsedMs: 7_000,
}), 300, "remaining child deadline is an independent tighter cap");

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

const legacyReplayDatabase = new Database(":memory:");
legacyReplayDatabase.exec(`
  create table repo_context_replay_attempts (
    source_key text not null,
    source_digest text not null,
    position text not null,
    context_id text not null,
    pass_id text not null,
    outcome text not null check (outcome in (
      'started', 'not_pending', 'success', 'boundary_unavailable',
      'resolution_failed', 'worker_crash', 'replay_exhausted'
    )),
    attempted_at text not null,
    primary key (source_key, source_digest, position, context_id, pass_id)
  ) without rowid;
  insert into repo_context_replay_attempts values (
    '${"a".repeat(64)}', '${"b".repeat(64)}', '0:-1',
    'repoctx:v1:${"c".repeat(64)}', 'legacy-pass', 'success',
    '2026-09-11T00:00:00.000Z'
  );
`);
ensureRepoContextReplaySchema(legacyReplayDatabase);
recordRepoContextReplayAttempt(legacyReplayDatabase, {
  sourceKey: "d".repeat(64),
  sourceDigest: "e".repeat(64),
  position: "1:-1",
  contextId: `record:v1:${"f".repeat(64)}`,
  passId: "oversized-pass",
  outcome: "record_too_large",
});
assert.equal((legacyReplayDatabase.prepare(
  `select count(*) as n from repo_context_replay_attempts where outcome = 'success'`,
).get() as { n: number }).n, 1, "attempt migration must preserve existing receipts");
assert.equal((legacyReplayDatabase.prepare(
  `select count(*) as n from repo_context_replay_attempts where outcome = 'record_too_large'`,
).get() as { n: number }).n, 1);
ensureRepoContextReplaySchema(legacyReplayDatabase);
assert.equal((legacyReplayDatabase.prepare(
  `select count(*) as n from repo_context_replay_attempts`,
).get() as { n: number }).n, 2, "attempt outcome migration must remain idempotent");
legacyReplayDatabase.close();

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

const oversizedRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-replay-oversized-")));
const oversizedSession = "019e3000-0000-7000-8000-000000000006";
const oversizedFile = path.join(oversizedRoot, `rollout-${oversizedSession}.jsonl`);
const oversizedCwd = path.join(oversizedRoot, "repo");
const oversizedRecord = JSON.stringify({ type: "response_item", payload: "x".repeat(70 * 1024) });
fs.writeFileSync(oversizedFile, `${oversizedRecord}\n${JSON.stringify({
  type: "session_meta", payload: { id: oversizedSession, cwd: oversizedCwd },
})}\n`);
const oversizedDiscovery = discoverRepoContextReplaySources([{
  rootId: "oversized-root",
  profileId: "oversized-profile",
  installationEpochId: "oversized-epoch",
  source: "codex",
  directory: oversizedRoot,
}], { deadlineMs: 50 });
const oversizedSlice = readRepoContextReplaySlice(
  buffer, oversizedDiscovery.sources[0]!, null, { maxContexts: 64 },
);
assert.equal(oversizedSlice.complete, true, "a 70KiB record must not pin the cursor");
assert.equal(oversizedSlice.rowsInspected, 2);
assert.equal(oversizedSlice.candidates.length, 1);
assert.notEqual(oversizedSlice.nextPosition, "0:-1");
fs.rmSync(oversizedRoot, { recursive: true, force: true });

const generationRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-replay-generation-")));
const generationFile = path.join(
  generationRoot, "rollout-019e3000-0000-7000-8000-000000000010.jsonl",
);
fs.writeFileSync(generationFile, `${JSON.stringify({
  type: "session_meta",
  payload: { id: "019e3000-0000-7000-8000-000000000010", cwd: path.join(generationRoot, "old") },
})}\n`);
const generationRoots = [{
  rootId: "generation-root",
  profileId: "generation-profile",
  installationEpochId: "generation-epoch",
  source: "codex" as const,
  directory: generationRoot,
}];
const oldGeneration = discoverRepoContextReplaySources(generationRoots, { deadlineMs: 50 }).sources[0]!;
fs.renameSync(generationFile, `${generationFile}.replaced`);
fs.writeFileSync(generationFile, `${JSON.stringify({
  type: "session_meta",
  payload: { id: "019e3000-0000-7000-8000-000000000010", cwd: path.join(generationRoot, "new") },
})}\n`);
const staleGenerationSlice = readRepoContextReplaySlice(buffer, oldGeneration, null, { maxContexts: 64 });
assert.equal((staleGenerationSlice as unknown as { generationChanged: boolean }).generationChanged, true);
assert.equal(staleGenerationSlice.rowsInspected, 0);
assert.equal(staleGenerationSlice.candidates.length, 0);
assert.equal(staleGenerationSlice.nextPosition, "0:-1");
const staleGenerationAtOldEof = readRepoContextReplaySlice(
  buffer, oldGeneration, `${oldGeneration.size}:-1`, { maxContexts: 64 },
);
assert.equal((staleGenerationAtOldEof as unknown as { generationChanged: boolean }).generationChanged, true,
  "generation revalidation must precede even an apparent end-of-file cursor");
assert.equal(staleGenerationAtOldEof.complete, false);
const newGeneration = discoverRepoContextReplaySources(generationRoots, { deadlineMs: 50 }).sources[0]!;
assert.notEqual(newGeneration.sourceDigest, oldGeneration.sourceDigest);
const newGenerationSlice = readRepoContextReplaySlice(buffer, newGeneration, null, { maxContexts: 64 });
assert.equal((newGenerationSlice as unknown as { generationChanged: boolean }).generationChanged, false);
assert.equal(newGenerationSlice.candidates.length, 1,
  "replacement bytes must be processed only under the new generation namespace");
fs.rmSync(generationRoot, { recursive: true, force: true });

const terminalOversizedRoot = fs.realpathSync(fs.mkdtempSync(
  path.join(os.tmpdir(), "plimsoll-replay-terminal-oversized-"),
));
const terminalOversizedSession = "019e3000-0000-7000-8000-000000000007";
const terminalOversizedFile = path.join(
  terminalOversizedRoot, `rollout-${terminalOversizedSession}.jsonl`,
);
const terminalOversizedCwd = path.join(terminalOversizedRoot, "repo");
fs.writeFileSync(terminalOversizedFile, `${JSON.stringify({
  type: "response_item",
  payload: "x".repeat(REPO_CONTEXT_REPLAY_MAX_RECORD_BYTES + 16 * 1024),
})}\n${JSON.stringify({
  type: "session_meta",
  payload: { id: terminalOversizedSession, cwd: terminalOversizedCwd },
})}\n`);
const terminalOversizedBuffer = new LocalEventBuffer(path.join(terminalOversizedRoot, "ledger.sqlite"));
const terminalOversizedIdentity = repoContextReplayFileIdentity(terminalOversizedFile);
const terminalOversizedOccurrence = [
  "codex-rollout", terminalOversizedIdentity, terminalOversizedSession, "session_meta", "0",
].join(":");
const terminalOversizedEvent = aiInteractionEventSchema.parse({
  id: "terminal-oversized-event",
  tenantId: "local",
  source: "codex",
  dataMode: "metadata",
  eventType: "tool_use",
  observedAt: "2026-09-11T00:30:00.000Z",
  sessionId: terminalOversizedSession,
  actionClass: "shell",
  metadata: {},
});
assert.equal(attachRepoContextSidecar(
  terminalOversizedEvent, terminalOversizedOccurrence, terminalOversizedCwd,
), true);
assert.equal(terminalOversizedBuffer.append(terminalOversizedEvent), true);
terminalOversizedBuffer.database.prepare(`delete from repo_context_handoffs`).run();
const terminalOversizedRoots = [{
  rootId: "terminal-oversized-root",
  profileId: "terminal-oversized-profile",
  installationEpochId: "terminal-oversized-epoch",
  source: "codex" as const,
  directory: terminalOversizedRoot,
}];
const terminalOversizedConfig = collectorConfigSchema.parse({
  repoContextDrain: { enabled: true },
}).repoContextDrain;
const terminalOversizedFirst = runRepoContextDrainStage(terminalOversizedBuffer, {
  config: terminalOversizedConfig,
  captureRoots: terminalOversizedRoots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
});
assert.equal((terminalOversizedFirst as unknown as { oversizedSourceRecords: number }).oversizedSourceRecords, 1);
assert.equal((terminalOversizedBuffer.database.prepare(
  `select count(*) as n from repo_context_replay_attempts where outcome = 'record_too_large'`,
).get() as { n: number }).n, 1, "oversized source record needs a terminal receipt");
const terminalOversizedSecond = runRepoContextDrainStage(terminalOversizedBuffer, {
  config: terminalOversizedConfig,
  captureRoots: terminalOversizedRoots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
  resolve: (request) => ({
    contextId: request.contextId,
    repoHash: `sha256:${"7".repeat(64)}`,
    branchHash: null,
    headSha: null,
    resolvedAt: new Date().toISOString(),
    resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
  }),
});
assert.equal(terminalOversizedSecond.successfulContexts, 1);
assert.equal((terminalOversizedBuffer.database.prepare(
  `select count(*) as n from repo_context_replay_passes where status = 'complete'`,
).get() as { n: number }).n, 1, "terminal oversized record must not prevent pass completion");
terminalOversizedBuffer.close();
fs.rmSync(terminalOversizedRoot, { recursive: true, force: true });

const unavailableRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-replay-unavailable-")));
const unavailableBuffer = new LocalEventBuffer(path.join(unavailableRoot, "ledger.sqlite"));
const unavailableCwd = path.join(unavailableRoot, "repo");
const unavailableOccurrence = "missing-authorized-source";
const unavailableEvent = aiInteractionEventSchema.parse({
  id: "unavailable-source-event",
  tenantId: "local",
  source: "codex",
  dataMode: "metadata",
  eventType: "tool_use",
  observedAt: "2026-09-11T00:40:00.000Z",
  sessionId: "019e3000-0000-7000-8000-000000000008",
  actionClass: "shell",
  metadata: {},
});
assert.equal(attachRepoContextSidecar(unavailableEvent, unavailableOccurrence, unavailableCwd), true);
assert.equal(unavailableBuffer.append(unavailableEvent), true);
unavailableBuffer.database.prepare(`delete from repo_context_handoffs`).run();
const unavailableConfig = collectorConfigSchema.parse({
  repoContextDrain: { enabled: true, expireEnabled: true },
}).repoContextDrain;
const unavailableDiscovery = discoverRepoContextReplaySources([{
  rootId: "missing-root",
  profileId: "missing-profile",
  installationEpochId: "missing-epoch",
  source: "codex",
  directory: path.join(unavailableRoot, "does-not-exist"),
}], { deadlineMs: 50, now: () => 0 });
assert.equal(unavailableDiscovery.complete, false);
assert.equal(unavailableDiscovery.unavailableRoots, 1);
assert.equal(unavailableDiscovery.unavailableEntries, 0);
let unavailableReceipt = disabledRepoContextDrainReceipt();
for (let run = 0; run < 2; run += 1) {
  unavailableReceipt = runRepoContextDrainStage(unavailableBuffer, {
    config: unavailableConfig,
    captureRoots: [{
      rootId: "missing-root",
      profileId: "missing-profile",
      installationEpochId: "missing-epoch",
      source: "codex",
      directory: path.join(unavailableRoot, "does-not-exist"),
    }],
    captureElapsedMs: 0,
    freshContextsUsed: 0,
    freshDeferred: 0,
    remainingJobMs: 29_000,
    remainingLookupMs: 10_000,
    now: () => 0,
  });
}
assert.equal((unavailableReceipt as unknown as { unavailableSourceRoots: number }).unavailableSourceRoots, 1);
assert.equal((unavailableReceipt as unknown as { unavailableSourceEntries: number }).unavailableSourceEntries, 0);
assert.equal(unavailableReceipt.unresolvedContexts.source_unavailable, 0,
  "root inventory failures are not unresolved-context outcomes");
assert.equal(unavailableReceipt.expiredLinks, 0);
assert.equal((unavailableBuffer.database.prepare(
  `select count(*) as n from repo_context_replay_passes where status = 'complete'`,
).get() as { n: number }).n, 0, "an unavailable root must make the inventory pass ineligible");
assert.equal((unavailableBuffer.database.prepare(
  `select fill_pending as fillPending from repo_context_event_links where event_id = ?`,
).get(unavailableEvent.id) as { fillPending: number }).fillPending, 1);
unavailableBuffer.close();
fs.rmSync(unavailableRoot, { recursive: true, force: true });

const groupedSession = "019e3000-0000-7000-8000-000000000003";
const groupedFile = path.join(codexRoot, `rollout-${groupedSession}.jsonl`);
const groupedCwd = path.join(root, "repo-grouped");
const groupedLines = [
  JSON.stringify({ type: "session_meta", payload: { id: groupedSession } }),
  ...Array.from({ length: 64 }, () => JSON.stringify({
    type: "turn_context", payload: { cwd: groupedCwd },
  })),
];
fs.writeFileSync(groupedFile, `${groupedLines.join("\n")}\n`);
const groupedIdentity = repoContextReplayFileIdentity(groupedFile);
for (let index = 1; index <= 64; index += 1) {
  const occurrence = [
    "codex-rollout", groupedIdentity, groupedSession, "turn_context", String(index),
  ].join(":");
  const event = aiInteractionEventSchema.parse({
    id: `grouped-event-${String(index).padStart(2, "0")}`,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: "tool_use",
    observedAt: `2026-09-11T00:${String(index % 60).padStart(2, "0")}:00.000Z`,
    sessionId: groupedSession,
    actionClass: "shell",
    metadata: {},
  });
  assert.equal(attachRepoContextSidecar(event, occurrence, groupedCwd), true);
  assert.equal(buffer.append(event), true);
}
const missingContextIds: string[] = [];
for (let index = 0; index < 3; index += 1) {
  const occurrence = `missing-source-${index}`;
  const event = aiInteractionEventSchema.parse({
    id: `missing-event-${index}`,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: "tool_use",
    observedAt: `2026-09-11T02:0${index}:00.000Z`,
    sessionId: groupedSession,
    actionClass: "shell",
    metadata: {},
  });
  assert.equal(attachRepoContextSidecar(event, occurrence, groupedCwd), true);
  assert.equal(buffer.append(event), true);
  missingContextIds.push(buffer.repoContextOccurrenceRequest("codex", occurrence, groupedCwd)!.contextId);
}
buffer.database.prepare(`delete from repo_context_handoffs`).run();
let lookups = 0;
const applyBatchSizes: number[] = [];
const applyRepoContextResults = buffer.applyRepoContextResults.bind(buffer);
buffer.applyRepoContextResults = (results) => {
  applyBatchSizes.push(results.length);
  return applyRepoContextResults(results);
};
const enabledConfig = collectorConfigSchema.parse({
  repoContextDrain: { enabled: true },
}).repoContextDrain;
const drain = runRepoContextDrainStage(buffer, {
  config: enabledConfig,
  captureRoots: roots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
  resolve: (request) => {
    lookups += 1;
    return {
      contextId: request.contextId,
      repoHash: `sha256:${"a".repeat(64)}`,
      branchHash: `sha256:${"b".repeat(64)}`,
      headSha: "c".repeat(40),
      resolvedAt: new Date().toISOString(),
      resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
    };
  },
});
assert.equal(drain.candidateContexts, 64);
assert.equal(drain.distinctCwdGroups, 1);
assert.equal(drain.successfulContexts, 64);
assert.equal(lookups, 1, "one canonical cwd must cause one git lookup");
assert.equal((buffer.database.prepare(
  `select count(*) as n from repo_context_event_links where fill_pending = 0`,
).get() as { n: number }).n, 64);
assert.equal((buffer.database.prepare(`select count(*) as n from repo_context_results`).get() as { n: number }).n, 64);
assert.ok(Math.max(...applyBatchSizes) <= 8, "fan-out commits must retain the existing eight-result bound");
assert.ok(drain.elapsedMs <= 200);

const expiringConfig = collectorConfigSchema.parse({
  repoContextDrain: { enabled: true, expireEnabled: true },
}).repoContextDrain;
let expiryPass = disabledRepoContextDrainReceipt();
for (let run = 0; run < 10 && expiryPass.expiredLinks === 0; run += 1) {
  expiryPass = runRepoContextDrainStage(buffer, {
    config: expiringConfig,
    captureRoots: roots,
    captureElapsedMs: 0,
    freshContextsUsed: 0,
    freshDeferred: 0,
    remainingJobMs: 29_000,
    remainingLookupMs: 10_000,
    now: () => 0,
    resolve: () => { throw new Error("resolved rows must not be looked up again"); },
  });
}
assert.equal(expiryPass.expiredLinks, 3, "expiry requires two complete replay passes");
assert.ok((buffer.database.prepare(
  `select count(*) as n from repo_context_replay_passes where status = 'complete'`,
).get() as { n: number }).n >= 2, "expiry must follow at least two completed passes");
const dispositions = buffer.database.prepare(
  `select event_id as eventId, reason, expired_at as expiredAt, re_resolved_at as reResolvedAt
   from repo_context_link_dispositions order by event_id`,
).all() as Array<{ eventId: string; reason: string; expiredAt: string | null; reResolvedAt: string | null }>;
assert.equal(dispositions.length, 3);
assert.ok(dispositions.every((row) => row.reason === "source_unavailable" && row.expiredAt && !row.reResolvedAt));
assert.equal((buffer.database.prepare(
  `select count(*) as n from repo_context_event_links where event_id like 'missing-event-%' and fill_pending = 1`,
).get() as { n: number }).n, 0);

const restored = {
  contextId: missingContextIds[0]!,
  repoHash: `sha256:${"d".repeat(64)}`,
  branchHash: `sha256:${"e".repeat(64)}`,
  headSha: "f".repeat(40),
  resolvedAt: new Date().toISOString(),
  resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
};
buffer.database.prepare(
  `insert into repo_context_inflight (context_id, started_at, owner) values (?, ?, 'child')`,
).run(restored.contextId, new Date().toISOString());
buffer.applyRepoContextResults([restored]);
assert.equal(reResolveExpiredRepoContextLinks(buffer.database, restored), 1);
const reversed = buffer.database.prepare(
  `select expired_at as expiredAt, re_resolved_at as reResolvedAt
   from repo_context_link_dispositions where context_id = ?`,
).get(restored.contextId) as { expiredAt: string | null; reResolvedAt: string | null };
assert.ok(reversed.expiredAt, "original expiry evidence must be retained");
assert.ok(reversed.reResolvedAt, "re-resolution must clear the operational expiry state");
assert.equal((buffer.database.prepare(
  `select fill_pending as fillPending from repo_context_event_links where context_id = ?`,
).get(restored.contextId) as { fillPending: number }).fillPending, 0,
"re-resolution must not reopen the monotonic 0-to-1 link transition");

ensureRepoContextLinkDispositionSchema(buffer.database);
ensureRepoContextLinkDispositionSchema(buffer.database);
rollbackRepoContextLinkDispositionMigration(buffer.database);
assert.equal((buffer.database.prepare(
  `select count(*) as n from sqlite_master where type = 'table' and name = 'repo_context_link_dispositions'`,
).get() as { n: number }).n, 0);
ensureRepoContextLinkDispositionSchema(buffer.database);
const defaultOff = runRepoContextDrainStage(buffer, {
  config: config.repoContextDrain,
  captureRoots: roots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
});
assert.equal(defaultOff.status, "disabled");
assert.equal(defaultOff.rowsInspected, 0);

const overflowBefore = (buffer.database.prepare(
  `select coalesce((select dropped_count from repo_context_unknown_counters
    where reason = 'queue_overflow'), 0) as n`,
).get() as { n: number }).n;
const yielded = runRepoContextDrainStage(buffer, {
  config: enabledConfig,
  captureRoots: roots,
  captureElapsedMs: 0,
  freshContextsUsed: 8,
  freshDeferred: 1,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
});
assert.equal(yielded.status, "yielded_fresh");
assert.equal(yielded.candidateContexts, 0);
assert.equal((buffer.database.prepare(
  `select coalesce((select dropped_count from repo_context_unknown_counters
    where reason = 'queue_overflow'), 0) as n`,
).get() as { n: number }).n, overflowBefore);

const workerCrashEvent = aiInteractionEventSchema.parse({
  id: "worker-crash-event",
  tenantId: "local",
  source: "codex",
  dataMode: "metadata",
  eventType: "tool_use",
  observedAt: "2026-09-11T02:30:00.000Z",
  sessionId: groupedSession,
  actionClass: "shell",
  metadata: {},
});
assert.equal(attachRepoContextSidecar(workerCrashEvent, "worker-crash-missing", groupedCwd), true);
assert.equal(buffer.append(workerCrashEvent), true);
buffer.database.prepare(`delete from repo_context_handoffs`).run();
const workerCrashContext = buffer.repoContextOccurrenceRequest(
  "codex", "worker-crash-missing", groupedCwd,
)!.contextId;
recordRepoContextReplayAttempt(buffer.database, {
  sourceKey: "0".repeat(64),
  sourceDigest: "1".repeat(64),
  position: "0:-1",
  contextId: workerCrashContext,
  passId: "worker-crash-pass",
  outcome: "started",
});
const recoveredCrash = runRepoContextDrainStage(buffer, {
  config: enabledConfig,
  captureRoots: roots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
  resolve: () => { throw new Error("no unresolved source candidate is expected"); },
});
assert.equal(recoveredCrash.unresolvedContexts.worker_crash, 1);
assert.equal((buffer.database.prepare(
  `select reason from repo_context_link_dispositions where event_id = 'worker-crash-event'`,
).get() as { reason: string }).reason, "worker_crash");
assert.equal((buffer.database.prepare(
  `select outcome from repo_context_replay_attempts where context_id = ? and pass_id = 'worker-crash-pass'`,
).get(workerCrashContext) as { outcome: string }).outcome, "worker_crash");

const crashSession = "019e3000-0000-7000-8000-000000000004";
const crashFile = path.join(codexRoot, `rollout-${crashSession}.jsonl`);
const crashCwd = path.join(root, "repo-crash-safe");
fs.writeFileSync(crashFile, `${JSON.stringify({
  type: "session_meta", payload: { id: crashSession, cwd: crashCwd },
})}\n`);
const crashIdentity = repoContextReplayFileIdentity(crashFile);
const crashOccurrence = [
  "codex-rollout", crashIdentity, crashSession, "session_meta", "0",
].join(":");
const crashEvent = aiInteractionEventSchema.parse({
  id: "crash-safe-event",
  tenantId: "local",
  source: "codex",
  dataMode: "metadata",
  eventType: "tool_use",
  observedAt: "2026-09-11T03:00:00.000Z",
  sessionId: crashSession,
  actionClass: "shell",
  metadata: {},
});
assert.equal(attachRepoContextSidecar(crashEvent, crashOccurrence, crashCwd), true);
assert.equal(buffer.append(crashEvent), true);
buffer.database.prepare(`delete from repo_context_handoffs`).run();
const crashDiscovery = discoverRepoContextReplaySources(roots, { deadlineMs: 50 });
const crashInventoryDigest = crypto.createHash("sha256").update(JSON.stringify(
  crashDiscovery.sources.map((source) => [source.sourceKey, source.sourceDigest]),
)).digest("hex");
const crashPass = beginRepoContextReplayPass(buffer.database, crashInventoryDigest);
const crashSource = crashDiscovery.sources.find((source) => source.file === crashFile)!;
const crashSlice = readRepoContextReplaySlice(buffer, crashSource, null, { maxContexts: 64 });
const crashCandidate = crashSlice.candidates[0]!;
const crashResult = {
  contextId: crashCandidate.request.contextId,
  repoHash: `sha256:${"1".repeat(64)}`,
  branchHash: `sha256:${"2".repeat(64)}`,
  headSha: "3".repeat(40),
  resolvedAt: new Date().toISOString(),
  resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
};
buffer.database.prepare(
  `insert into repo_context_inflight (context_id, started_at, owner) values (?, ?, 'child')`,
).run(crashResult.contextId, new Date().toISOString());
buffer.applyRepoContextResults([crashResult]);
recordRepoContextReplayAttempt(buffer.database, {
  sourceKey: crashCandidate.sourceKey,
  sourceDigest: crashCandidate.sourceDigest,
  position: crashCandidate.position,
  contextId: crashCandidate.request.contextId,
  passId: crashPass.passId,
  outcome: "success",
});
assert.equal(repoContextReplayCursor(buffer.database, {
  sourceKey: crashSource.sourceKey,
  sourceDigest: crashSource.sourceDigest,
  passId: crashPass.passId,
}), null, "simulated crash occurs before cursor advance");
runRepoContextDrainStage(buffer, {
  config: enabledConfig,
  captureRoots: roots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
  resolve: () => { throw new Error("durable result replay must not repeat git lookup"); },
});
assert.equal(repoContextReplayCursor(buffer.database, {
  sourceKey: crashSource.sourceKey,
  sourceDigest: crashSource.sourceDigest,
  passId: crashPass.passId,
})?.status, "complete");
assert.equal((buffer.database.prepare(
  `select count(*) as n from repo_context_results where context_id = ?`,
).get(crashResult.contextId) as { n: number }).n, 1);
assert.equal((buffer.database.prepare(
  `select outcome from repo_context_replay_attempts where context_id = ? and pass_id = ?`,
).get(crashResult.contextId, crashPass.passId) as { outcome: string }).outcome, "success");
assert.equal(
  buffer.database.serialize().includes(Buffer.from(root)),
  false,
  "replay and disposition state must not persist a raw source path or cwd",
);
buffer.close();
fs.rmSync(root, { recursive: true, force: true });

const cwdCapRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-repo-context-cwd-cap-")));
const cwdCapSourceRoot = path.join(cwdCapRoot, "codex");
fs.mkdirSync(cwdCapSourceRoot, { recursive: true });
const cwdCapSession = "019e3000-0000-7000-8000-000000000005";
const cwdCapSourceFile = path.join(cwdCapSourceRoot, `rollout-${cwdCapSession}.jsonl`);
const cwdCapLines = [
  JSON.stringify({ type: "session_meta", payload: { id: cwdCapSession } }),
  ...Array.from({ length: 9 }, (_, index) => JSON.stringify({
    type: "turn_context", payload: { cwd: path.join(cwdCapRoot, `repo-${index}`) },
  })),
];
fs.writeFileSync(cwdCapSourceFile, `${cwdCapLines.join("\n")}\n`);
const cwdCapIdentity = repoContextReplayFileIdentity(cwdCapSourceFile);
const cwdCapBuffer = new LocalEventBuffer(path.join(cwdCapRoot, "ledger.sqlite"));
for (let index = 1; index <= 9; index += 1) {
  const cwd = path.join(cwdCapRoot, `repo-${index - 1}`);
  const occurrence = [
    "codex-rollout", cwdCapIdentity, cwdCapSession, "turn_context", String(index),
  ].join(":");
  const event = aiInteractionEventSchema.parse({
    id: `cwd-cap-event-${index}`,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: "tool_use",
    observedAt: `2026-09-11T03:${String(index).padStart(2, "0")}:00.000Z`,
    sessionId: cwdCapSession,
    actionClass: "shell",
    metadata: {},
  });
  assert.equal(attachRepoContextSidecar(event, occurrence, cwd), true);
  assert.equal(cwdCapBuffer.append(event), true);
}
cwdCapBuffer.database.prepare(`delete from repo_context_handoffs`).run();
const cwdCapRoots = [{
  rootId: "cwd-cap-root",
  profileId: "cwd-cap-profile",
  installationEpochId: "cwd-cap-epoch",
  source: "codex" as const,
  directory: cwdCapSourceRoot,
}];
let cwdCapLookups = 0;
const resolveCwdCap = (request: { contextId: string }) => {
  cwdCapLookups += 1;
  return {
    contextId: request.contextId,
    repoHash: `sha256:${"4".repeat(64)}`,
    branchHash: `sha256:${"5".repeat(64)}`,
    headSha: "6".repeat(40),
    resolvedAt: new Date().toISOString(),
    resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
  };
};
const overflowBeforeCwdCap = (cwdCapBuffer.database.prepare(
  `select coalesce((select dropped_count from repo_context_unknown_counters
    where reason = 'queue_overflow'), 0) as n`,
).get() as { n: number }).n;
const firstCwdCap = runRepoContextDrainStage(cwdCapBuffer, {
  config: enabledConfig,
  captureRoots: cwdCapRoots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
  resolve: resolveCwdCap,
});
assert.equal(firstCwdCap.candidateContexts, 8);
assert.equal(firstCwdCap.distinctCwdGroups, 8);
assert.equal(firstCwdCap.cwdBudgetExhausted, true);
assert.equal(cwdCapLookups, 8);
assert.equal((cwdCapBuffer.database.prepare(`select count(*) as n from repo_context_results`).get() as { n: number }).n, 8);
const secondCwdCap = runRepoContextDrainStage(cwdCapBuffer, {
  config: enabledConfig,
  captureRoots: cwdCapRoots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  now: () => 0,
  resolve: resolveCwdCap,
});
assert.equal(secondCwdCap.candidateContexts, 1);
assert.equal(secondCwdCap.distinctCwdGroups, 1);
assert.equal(cwdCapLookups, 9);
assert.equal((cwdCapBuffer.database.prepare(`select count(*) as n from repo_context_results`).get() as { n: number }).n, 9);
assert.equal((cwdCapBuffer.database.prepare(
  `select coalesce((select dropped_count from repo_context_unknown_counters
    where reason = 'queue_overflow'), 0) as n`,
).get() as { n: number }).n, overflowBeforeCwdCap,
"cwd-cap deferral must stay cursor-backed rather than becoming UNKNOWN");
cwdCapBuffer.close();
fs.rmSync(cwdCapRoot, { recursive: true, force: true });

const budgetRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-repo-context-budget-")));
const budgetBuffer = new LocalEventBuffer(path.join(budgetRoot, "ledger.sqlite"));
const insertEvent = budgetBuffer.database.prepare(
  `insert into buffered_events
     (id, source, event_type, data_mode, observed_at, payload_json, created_at)
   values (?, 'codex', 'tool_use', 'metadata', ?, '{}', ?)`,
);
const insertLink = budgetBuffer.database.prepare(
  `insert into repo_context_event_links
     (event_id, context_id, fill_pending, context_conflict, suppression_cleaned)
   values (?, ?, 1, 0, 0)`,
);
const budgetAt = "2026-09-11T04:00:00.000Z";
budgetBuffer.database.transaction(() => {
  for (let index = 0; index < 196_000; index += 1) {
    const eventId = `budget-${index.toString(16).padStart(8, "0")}`;
    const contextId = `repoctx:v1:${index.toString(16).padStart(64, "0")}`;
    insertEvent.run(eventId, budgetAt, budgetAt);
    insertLink.run(eventId, contextId);
  }
})();
const budgetSourceRoot = path.join(budgetRoot, "codex");
fs.mkdirSync(budgetSourceRoot, { recursive: true });
fs.writeFileSync(path.join(
  budgetSourceRoot, "rollout-019e3000-0000-7000-8000-000000000009.jsonl",
), "");
const budgetRoots = [{
  rootId: "budget-root",
  profileId: "budget-profile",
  installationEpochId: "budget-epoch",
  source: "codex" as const,
  directory: budgetSourceRoot,
}];
const expiryPlan = budgetBuffer.database.prepare(
  `explain query plan ${REPO_CONTEXT_EXPIRY_SELECTION_SQL}`,
).all(256) as Array<{ detail: string }>;
assert.ok(expiryPlan.every((row) => !/\bSCAN l\b/.test(row.detail)),
  `expiry selection must not scan the links table: ${JSON.stringify(expiryPlan)}`);
const budgetExpiryConfig = collectorConfigSchema.parse({
  repoContextDrain: { enabled: true, expireEnabled: true },
}).repoContextDrain;
runRepoContextDrainStage(budgetBuffer, {
  config: budgetExpiryConfig,
  captureRoots: budgetRoots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
});
const budgetStarted = performance.now();
const budgetReceipt = runRepoContextDrainStage(budgetBuffer, {
  config: budgetExpiryConfig,
  captureRoots: budgetRoots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
});
const budgetFixtureMs = performance.now() - budgetStarted;
assert.equal(budgetReceipt.expiredLinks, 256);
assert.ok(budgetReceipt.elapsedMs <= 200, `receipt exceeded 200ms: ${budgetReceipt.elapsedMs}`);
assert.ok(budgetFixtureMs <= 200, `196k-link drain slice exceeded 200ms: ${budgetFixtureMs}`);
assert.ok(budgetFixtureMs < 29_000);
budgetBuffer.close();
fs.rmSync(budgetRoot, { recursive: true, force: true });

console.log(JSON.stringify({
  proof: "repo_context_drain",
  checks: 111,
  passed: 111,
  oversizedRecord: {
    hardCapBytes: REPO_CONTEXT_REPLAY_MAX_RECORD_BYTES,
    grownRecordBytes: Buffer.byteLength(oversizedRecord),
    terminalReceipts: 1,
  },
  sourceGenerationReplacement: { staleRowsInspected: 0, freshCandidates: 1 },
  expiryFixture: {
    links: 196_000,
    expireEnabled: true,
    replaySources: 1,
    expiredLinks: budgetReceipt.expiredLinks,
    elapsedMs: Math.round(budgetFixtureMs * 100) / 100,
    plan: expiryPlan.map((row) => row.detail),
  },
}));
