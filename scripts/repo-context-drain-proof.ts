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
import {
  ensureRepoContextLinkDispositionSchema,
  reResolveExpiredRepoContextLinks,
  rollbackRepoContextLinkDispositionMigration,
} from "../packages/collector-cli/src/repo-context-link-dispositions";
import {
  attachRepoContextSidecar,
  REPO_CONTEXT_RESOLVER_VERSION,
} from "../packages/collector-cli/src/repo-context";
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
assert.ok(drain.elapsedMs <= 200);

const expiringConfig = collectorConfigSchema.parse({
  repoContextDrain: { enabled: true, expireEnabled: true },
}).repoContextDrain;
const expiryPass = runRepoContextDrainStage(buffer, {
  config: expiringConfig,
  captureRoots: roots,
  captureElapsedMs: 0,
  freshContextsUsed: 0,
  freshDeferred: 0,
  remainingJobMs: 29_000,
  remainingLookupMs: 10_000,
  resolve: () => { throw new Error("resolved rows must not be looked up again"); },
});
assert.equal(expiryPass.expiredLinks, 3, "expiry requires two complete replay passes");
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
buffer.close();
fs.rmSync(root, { recursive: true, force: true });

console.log(JSON.stringify({ proof: "repo_context_drain", checks: 43, passed: 43 }));
