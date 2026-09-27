import assert from "node:assert/strict";
import Database from "better-sqlite3";
import {
  buildOutcomePush,
  collectUsageEventLinks,
  projectWorkUsage,
  runOutcomesSync,
} from "../packages/collector-cli/src/outcomes-sync";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { branchLinkageHash, remoteLinkageHash } from "../packages/shared/src/linkage";
import { allocateEvents, collectAllocationEvents } from "./event-allocation";

assert.equal(typeof collectUsageEventLinks, "function");
assert.equal(typeof projectWorkUsage, "function");

const repoHash = remoteLinkageHash("https://github.com/owner/repo.git")!;
const branchHash = branchLinkageHash("feature/b")!;
const sessionId = "64dc762b-1870-4d39-81fa-0dc0a463e604";
const runA = "c9b875c8-e875-4f95-94b2-34b0d54c44ec";
const runB = "a73f9e9f-9890-4b74-a863-68ed68ddc897";
const workA = "beads:eco-6hoxj.163.104";
const workB = "beads:eco-6hoxj.163.105";
const db = new Database(":memory:");
db.exec(`create table buffered_events (
  id text primary key, session_id text, event_type text, observed_at text, created_at text,
  data_mode text, payload_json text, repo_hash text, branch_hash text, head_sha text,
  privacy_disposition text, usage_duplicate_reason text, privacy_generation integer
)`);
const add = db.prepare(`insert into buffered_events values (@id,@sessionId,'assistant_response',
  '2026-09-26T17:00:00.000Z','2026-09-26T17:00:00.000Z','metadata',@payload,@repoHash,@branchHash,null,
  @privacyDisposition,null,1)`);
function event(id: string, workItemId?: string, attemptId?: string, privacyDisposition: string | null = null) {
  add.run({ id, sessionId, payload: JSON.stringify({ inputTokens: 10, metadata: {
    ...(workItemId ? { workItemId, attemptId, workEvidenceRef: `dispatch:${id}` } : {}),
  } }), repoHash, branchHash, privacyDisposition });
}
event("event-a", workA, runA);
event("event-b", workB, runB);
event("event-unbound");
event("event-suppressed", workA, runA, "local_privacy_violation");
db.exec(`alter table buffered_events add column input_tokens integer;
  alter table buffered_events add column output_tokens integer;
  alter table buffered_events add column cache_read_tokens integer;
  alter table buffered_events add column cache_creation_tokens integer;
  alter table buffered_events add column cost_usd real;
  update buffered_events set input_tokens=10, output_tokens=2`);

const links = collectUsageEventLinks(db, {
  since: "2026-09-26T00:00:00.000Z", until: "2026-09-27T00:00:00.000Z",
});
assert.deepEqual(links.map((row) => row.eventId), ["event-a", "event-b", "event-unbound"]);
const pulls = [12, 13].map((number) => ({
  number, state: "closed", merged: true, updatedAt: "2026-09-26T18:00:00.000Z",
  checks: "passed" as const, checksFetched: true,
  branchHash: number === 13 ? branchHash : `sha256:${"c".repeat(64)}`,
}));
const artifacts = [
  { workItemId: workA, artifactRef: "github:owner/repo/pull/12", evidenceRef: "receipt:a" },
  { workItemId: workB, artifactRef: "github:owner/repo/pull/13", evidenceRef: "receipt:b" },
];
const project = (accepted = artifacts, explicitJoinEnabled = true) =>
  projectWorkUsage(links, pulls, repoHash, "owner/repo", accepted, { explicitJoinEnabled });
const joined = project();
assert.deepEqual(joined.map((row) => [row.eventId, row.workItemId, row.runId, row.pull, row.via]), [
  ["event-a", workA, runA, 12, "work_id"],
  ["event-b", workB, runB, 13, "work_id"],
  ["event-unbound", null, null, 13, "inferred_git"],
]);
assert.equal(new Set(joined.map((row) => row.eventId)).size, 3);
const push = buildOutcomePush({
  tenantId: "63f4c837-f137-40b9-8495-91dc8f20cd39", owner: "owner", repo: "repo", pulls,
  joins: joined.filter((row) => row.pull !== null && row.sessionId !== null).map((row) => ({
    pull: row.pull!, sessionId: row.sessionId!, via: row.via as "work_id" | "inferred_git",
    events: 1, eventId: row.eventId, workItemId: row.workItemId,
  })), signals: [], reworkWindowDays: 14,
});
assert.deepEqual(push.batch?.artifacts.find((artifact) => artifact.externalId.endsWith("/pull/12"))?.metadata.linkedWorkItemIds,
  [workA]);
assert.equal(project(artifacts.slice(0, 1))[1]?.pull, null);
const rollback = project(artifacts, false);
assert.ok(rollback.every((row) => row.workItemId === null && row.via === "inferred_git"));
assert.equal(rollback.length, 3);
const allocationEvents = collectAllocationEvents(db, "2026-09-26T00:00:00.000Z");
assert.deepEqual(allocationEvents.map((row) => row.eventId), ["event-a", "event-b", "event-unbound"]);
const candidates = [12, 13].map((number) => ({ pull: number, repoHash,
  branchHash: number === 13 ? branchHash : `sha256:${"c".repeat(64)}`,
  headSha: null, createdAt: "2026-09-25T00:00:00.000Z",
  updatedAt: "2026-09-26T19:00:00.000Z" }));
const allocation = allocateEvents(allocationEvents, candidates,
  { repository: "owner/repo", workArtifacts: artifacts });
assert.deepEqual(allocation.receipts.map((row) => [row.eventId, row.workItemId, row.pull, row.via]), [
  ["event-a", workA, 12, "work_id"],
  ["event-b", workB, 13, "work_id"],
  ["event-unbound", null, 13, "inferred_git"],
]);
assert.deepEqual(allocation.workRows.map((row) => [row.workItemId, row.events, row.inputTokens]), [
  [null, 1, 10], [workA, 1, 10], [workB, 1, 10],
]);
assert.equal(allocation.coverage.reconciliation.exact, true);
const withoutEvidence = allocateEvents(allocationEvents, candidates,
  { repository: "owner/repo", workArtifacts: artifacts.slice(0, 1) });
assert.equal(withoutEvidence.receipts[1]?.pull, null);
assert.equal(withoutEvidence.receipts[1]?.workItemId, workB);
const allocationRollback = allocateEvents(allocationEvents, candidates,
  { repository: "owner/repo", workArtifacts: artifacts, explicitJoinEnabled: false });
assert.ok(allocationRollback.receipts.every((row) => row.workItemId === null && row.via === "inferred_git"));

async function operational() {
  const fakeFetch: typeof fetch = async (input) => {
    const url = String(input);
    const body = url.includes("/pulls?") ? [12, 13].map((number) => ({
      number, state: "closed", merged_at: "2026-09-26T18:00:00.000Z",
      updated_at: "2026-09-26T18:00:00.000Z",
      head: { ref: number === 13 ? "feature/b" : "feature/a" },
    })) : [];
    return new Response(JSON.stringify(body), { status: 200 });
  };
  const config = collectorConfigSchema.parse({ uploadUrl: "http://127.0.0.1:1/ingest",
    tenantId: "63f4c837-f137-40b9-8495-91dc8f20cd39", installKey: "fixture-install" });
  const base = { repository: "owner/repo", ledgerDb: db, githubToken: "fixture-token",
    fetchImpl: fakeFetch, until: "2026-09-27T00:00:00.000Z", dryRun: true,
    log: () => {} };
  const explicit = await runOutcomesSync(config, { ...base, workArtifacts: artifacts });
  assert.equal(explicit.pullsJoined, 2);
  assert.ok(explicit.auditTable.includes("work_id"));
  const noArtifact = await runOutcomesSync(config, { ...base, workArtifacts: [] });
  assert.equal(noArtifact.pullsJoined, 1);
  assert.ok(noArtifact.auditTable.includes("inferred_git"));
  const disabled = await runOutcomesSync(config,
    { ...base, workArtifacts: artifacts, explicitJoinEnabled: false });
  assert.equal(disabled.pullsJoined, 1);
  assert.ok(disabled.auditTable.includes("inferred_git"));
  db.close();
  console.log("work-linkage proof: 15 passed");
}
operational().catch((error) => { console.error(error); process.exitCode = 1; });
