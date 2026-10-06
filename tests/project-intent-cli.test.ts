import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { appendProjectIntentReceipt, selectedProjectIntent, type ProjectIntentReceipt } from "../packages/shared/src/project-intent";
import { canonicalIdentity, intentDigest } from "../packages/collector-cli/src/project-intent-identity";
import { ProjectIntentProducer, type IntentLaunch, type IntentSession } from "../packages/collector-cli/src/project-intent-producer";
import { ensureUuidSessionId } from "../packages/collector-cli/src/session-sync";
import { createIntentFixture, fixtureIntentAck, INTENT_FIXTURE_PROJECTS } from "../scripts/lib/project-intent-fixture";

const nativeId = () => crypto.randomUUID();
const golden = JSON.parse(fs.readFileSync(new URL("../packages/shared/fixtures/project-intent-v1.json", import.meta.url), "utf8"));

test("producer derives every P02 golden linkage/evidence digest and rejects malformed Unicode", () => {
  for (const vector of Object.values(golden.derivationVectors) as Array<{ kind: string; normalizedParts: Array<string | number | null>; expected: string }>)
    assert.equal(intentDigest(vector.kind, vector.normalizedParts), vector.expected);
  assert.equal(canonicalIdentity("Caf\u0065\u0301"), "Café");
  assert.throws(() => canonicalIdentity("broken\ud800"), /invalid_identity/);
});

test("pending native/root binding uses the existing session UUID, never cwd or observed repo as project", async () => {
  const f = await createIntentFixture();
  try {
    const choices = await f.p.choices();
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: choices.projects[0], observedRepoKey: `sha256:${"c".repeat(64)}` });
    assert.equal(launch.localState, "awaiting_native_binding");
    assert.equal(launch.receiptDraft.sessionId, null); assert.equal(f.p.store.ids("sessions").length, 0);
    const id = nativeId(); const row = f.p.bind(launch.launchId, id)!;
    assert.equal(row.sessionId, id); assert.equal(row.projectKey, choices.projects[0].projectKey);
    assert.notEqual(row.observedRepoKey, row.projectKey);
    assert.equal(row.sourceRootKey, intentDigest("source-root", [f.config.cloudDeviceId!, "codex", fs.realpathSync(f.sourceRoot)]));
    const missingPath = path.join(f.root, "missing");
    const missing = f.p.declare({ source: "codex", sourceRoot: missingPath, project: null });
    const waitingId = nativeId();
    assert.equal(f.p.bind(missing.launchId, waitingId), null);
    assert.equal(f.p.store.read<IntentLaunch>("launches", missing.launchId)!.localState, "awaiting_native_binding");
    fs.mkdirSync(missingPath);
    assert.equal(f.p.bind(missing.launchId, waitingId)!.sessionId, waitingId);
    const opaqueId = "opaque-Caf\u0065\u0301";
    const unicodeRoot = path.join(f.root, "state-Caf\u0065\u0301");
    fs.mkdirSync(unicodeRoot);
    const unicodeLaunch = f.p.declare({ source: "codex", sourceRoot: unicodeRoot, project: null });
    const unicodeReceipt = f.p.bind(unicodeLaunch.launchId, opaqueId)!;
    assert.equal(unicodeReceipt.sessionId, ensureUuidSessionId(opaqueId).id);
    assert.notEqual(unicodeReceipt.sessionId, ensureUuidSessionId(canonicalIdentity(opaqueId)).id);
    assert.equal(unicodeReceipt.nativeSessionKey, intentDigest("native-session", [f.config.cloudDeviceId!, "codex", canonicalIdentity(opaqueId)]));
    assert.equal(f.p.bind(unicodeLaunch.launchId, opaqueId)!.receiptId, unicodeReceipt.receiptId);
  } finally { await f.close(); }
});

test("offline queue survives producer restart and replays the identical receipt with a fresh signature", async () => {
  const f = await createIntentFixture();
  try {
    const choices = await f.p.choices();
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: choices.projects[0] });
    const receipt = f.p.bind(launch.launchId, nativeId())!;
    const offline = new ProjectIntentProducer(f.config, { directory: path.join(f.root, "collector"), fetchImpl: async () => { throw new Error("OFFLINE_PRIVATE_TEXT"); } });
    const queued = await offline.sendSession(receipt.sessionId);
    assert.equal(queued.queued, 1); assert.equal(queued.delivered, 0);
    assert.equal((await offline.choices(true)).cached, true);
    const restored = new ProjectIntentProducer(f.config, { directory: path.join(f.root, "collector") });
    const result = await restored.replay(); assert.equal(result.delivered, 1); assert.equal(result.queued, 0);
    const post = f.calls.find(call => call.method === "POST")!;
    assert.deepEqual(JSON.parse(post.body).receipt, receipt);
    assert.equal(restored.store.read<IntentSession>("sessions", receipt.sessionId)!.observedRevision, 1);
  } finally { await f.close(); }
});

test("ambiguous accepted delivery replays one ID/time/evidence and never guesses an ACK", async () => {
  const f = await createIntentFixture();
  try {
    const choice = (await f.p.choices()).projects[0];
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: choice });
    const receipt = f.p.bind(launch.launchId, nativeId())!;
    let lost = true, tick = 0;
    const p = new ProjectIntentProducer(f.config, { directory: path.join(f.root, "collector"), now: () => new Date(Date.now() + tick++ * 1000),
      fetchImpl: async (url, init) => { const response = await fetch(url, init); if (lost) { lost = false; throw new Error("lost_response"); } return response; } });
    const first = await p.sendSession(receipt.sessionId); assert.equal(first.queued, 1);
    assert.equal(p.store.read<IntentSession>("sessions", receipt.sessionId)!.observedRevision, 0);
    f.setPost(body => ({ status: 202, body: fixtureIntentAck(body.receipt, 3, true) }));
    assert.equal((await p.sendSession(receipt.sessionId)).queued, 0);
    const posts = f.calls.filter(call => call.method === "POST");
    assert.equal(posts.length, 2); assert.equal(posts[0].body, posts[1].body);
    assert.notEqual(posts[0].headers["x-plimsoll-upload-signature"], posts[1].headers["x-plimsoll-upload-signature"]);
    assert.equal(p.store.read<IntentSession>("sessions", receipt.sessionId)!.observedRevision, 3);
  } finally { await f.close(); }
});

test("409 recovery changes only expectedRevision, is bounded, and rejects a mismatched stale response", async () => {
  const f = await createIntentFixture();
  try {
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: (await f.p.choices()).projects[0] });
    const row = f.p.bind(launch.launchId, nativeId())!; let calls = 0;
    f.setPost(body => ++calls === 1 ? { status: 409, body: { error: "intent_revision_stale", receiptId: body.receipt.receiptId, sessionId: row.sessionId, revision: 4 } }
      : { status: 202, body: fixtureIntentAck(body.receipt, 5) });
    assert.equal((await f.p.sendSession(row.sessionId)).queued, 0);
    const posts = f.calls.filter(call => call.method === "POST").map(call => JSON.parse(call.body));
    assert.deepEqual(posts[0].receipt, posts[1].receipt); assert.equal(posts[0].expectedRevision, 0); assert.equal(posts[1].expectedRevision, 4);
    const next = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
    const second = f.p.bind(next.launchId, nativeId())!;
    f.setPost(body => ({ status: 409, body: { error: "intent_revision_stale", receiptId: body.receipt.receiptId, sessionId: nativeId(), revision: 99 } }));
    assert.equal((await f.p.sendSession(second.sessionId)).queued, 1);
    assert.equal(f.p.store.read<IntentSession>("sessions", second.sessionId)!.observedRevision, 0);
    const boundedLaunch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
    const bounded = f.p.bind(boundedLaunch.launchId, nativeId())!;
    f.setPost(body => ({ status: 409, body: { error: "intent_revision_stale", receiptId: body.receipt.receiptId, sessionId: body.receipt.sessionId, revision: 6 } }));
    const before = f.calls.length;
    assert.equal((await f.p.sendSession(bounded.sessionId)).queued, 1);
    assert.equal(f.calls.length - before, 3);
  } finally { await f.close(); }
});

test("invalid ACK retains a queue; Unknown is a durable review item; late ACK cannot regress revision", async () => {
  const f = await createIntentFixture();
  try {
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
    const row = f.p.bind(launch.launchId, nativeId())!;
    f.setPost(body => ({ status: 202, body: { ...fixtureIntentAck(body.receipt), sessionId: nativeId() } }));
    assert.equal((await f.p.sendSession(row.sessionId)).queued, 1);
    f.p.store.mutate<IntentSession, void>("sessions", row.sessionId, state => { state!.observedRevision = 9; return { state: state!, result: undefined }; });
    f.setPost(body => ({ status: 202, body: fixtureIntentAck(body.receipt) }));
    assert.equal((await f.p.sendSession(row.sessionId)).queued, 0);
    const stored = f.p.store.read<IntentSession>("sessions", row.sessionId)!;
    assert.equal(stored.observedRevision, 9); assert.equal(stored.receipts[0].review, "needs_project");
    assert.equal(stored.receipts[0].ack!.acknowledged, true);
    const projectLaunch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: (await f.p.choices()).projects[0] });
    const projectReceipt = f.p.bind(projectLaunch.launchId, nativeId())!;
    f.setPost(body => {
      const ack = fixtureIntentAck(body.receipt);
      return { status: 202, body: { ...ack, project: { ...ack.project, company: null, companyReason: "company_mapping_missing" } } };
    });
    assert.equal((await f.p.sendSession(projectReceipt.sessionId)).queued, 0);
    assert.equal(f.p.store.read<IntentSession>("sessions", projectReceipt.sessionId)!.receipts[0].review, "company_mapping_missing");
  } finally { await f.close(); }
});

test("revoked install never falls back to stale choices or prunes refused receipts", async () => {
  const f = await createIntentFixture();
  try {
    const project = (await f.p.choices()).projects[0];
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project });
    const row = f.p.bind(launch.launchId, nativeId())!;
    f.setPost(() => ({ status: 403, body: { error: "device_revoked" } }));
    const result = await f.p.sendSession(row.sessionId); assert.equal(result.reason, "device_revoked"); assert.equal(result.queued, 1);
    assert.throws(() => f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null }), /device_revoked/);
    f.setGetStatus(403); await assert.rejects(() => f.p.choices(true), /device_revoked/);
    assert.equal(f.p.store.read<IntentSession>("sessions", row.sessionId)!.receipts[0].delivered, false);
    const cli = await f.run(["launch", "codex", "--project", project.projectKey]);
    assert.equal(cli.code, 2); assert.ok(!cli.stdout.includes("FIXTURE:"));
  } finally { await f.close(); }
});

test("repeat/resume/compact keep one receipt; root/account rotation keeps epoch and has persisted lineage", async () => {
  const f = await createIntentFixture();
  try {
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: (await f.p.choices()).projects[0] });
    const id = nativeId(), first = f.p.bind(launch.launchId, id)!;
    assert.deepEqual(f.p.bind(launch.launchId, id, { continuation: true }), first);
    const rotated = f.p.bind(launch.launchId, id, { continuation: true, sourceRoot: path.join(f.root, "codex-rotated"), principal: "acct_fixture" })!;
    assert.equal(rotated.sessionId, first.sessionId); assert.equal(rotated.sessionEpochKey, first.sessionEpochKey);
    assert.equal(rotated.rootAttemptId, first.rootAttemptId); assert.equal(rotated.parentAttemptId, first.attemptId);
    assert.notEqual(rotated.attemptId, first.attemptId);
    assert.equal(f.p.bind(launch.launchId, id, { continuation: true })!.receiptId, rotated.receiptId);
    const reused = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
    assert.throws(() => f.p.bind(reused.launchId, id), /native_id_reused_requires_fresh_ledger_binding/);
    assert.equal(f.p.store.read<IntentLaunch>("launches", reused.launchId)!.localState, "awaiting_native_binding");
    const cleared = f.p.bind(launch.launchId, nativeId())!;
    assert.notEqual(cleared.sessionId, first.sessionId); assert.notEqual(cleared.sessionEpochKey, first.sessionEpochKey);
    assert.equal(cleared.attemptId, cleared.rootAttemptId); assert.equal(cleared.parentAttemptId, null);
    assert.deepEqual(f.p.store.read<IntentSession>("sessions", id)!.receipts[0].receipt, first);
    appendProjectIntentReceipt([first], rotated);
  } finally { await f.close(); }
});

test("two simultaneous non-repo/home Claude helpers preserve cwd and different projects, drop stale env and dedupe hooks", async () => {
  const f = await createIntentFixture();
  try {
    const [a, b] = await Promise.all([
      f.run(["launch", "claude", "--project", INTENT_FIXTURE_PROJECTS[0].projectKey], { cwd: path.join(f.root, "home"), env: {
        FIXTURE_NATIVE_SESSION: nativeId(), PLIMSOLL_PROJECT_KEY: INTENT_FIXTURE_PROJECTS[1].projectKey, PLIMSOLL_FOLDER_PROJECT: "stale", PLIMSOLL_INTENT_LAUNCH_ID: nativeId() } }),
      f.run(["launch", "claude", "--project", INTENT_FIXTURE_PROJECTS[1].projectKey], { env: { FIXTURE_NATIVE_SESSION: nativeId() } }),
    ]);
    assert.equal(a.code, 0, a.stdout + a.stderr); assert.equal(b.code, 0, b.stdout + b.stderr);
    const read = (text: string) => JSON.parse(text.split("\n").find(line => line.startsWith("FIXTURE:"))!.slice(8));
    assert.equal(read(a.stdout).cwd, path.join(f.root, "home")); assert.equal(read(b.stdout).cwd, path.join(f.root, "non-repo"));
    assert.equal(read(a.stdout).staleProject, null); assert.equal(read(a.stdout).defaultProject, null);
    const sessions = f.p.store.ids("sessions").map(id => f.p.store.read<IntentSession>("sessions", id)!);
    assert.equal(sessions.length, 2); assert.ok(sessions.every(session => session.receipts.length === 1));
    assert.deepEqual(new Set(sessions.map(session => session.receipts[0].receipt.projectKey)), new Set(INTENT_FIXTURE_PROJECTS.map(project => project.projectKey)));
    assert.equal(f.calls.filter(call => call.method === "POST").length, 0);
    for (const session of sessions) assert.equal(session.receipts[0].receipt.source, "claude_code");
  } finally { await f.close(); }
});

test("Claude clear with a different native ID binds a fresh incarnation within one launch", async () => {
  const f = await createIntentFixture();
  try {
    const firstId = nativeId(), clearedId = nativeId();
    const result = await f.run(["launch", "claude", "--project", INTENT_FIXTURE_PROJECTS[0].projectKey, "--", "--session-id", firstId], {
      env: { FIXTURE_NATIVE_SESSION: firstId, FIXTURE_CLEAR_NATIVE_SESSION: clearedId },
    });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    const launchIds = f.p.store.ids("launches"); assert.equal(launchIds.length, 1);
    assert.equal(f.p.store.read<IntentLaunch>("launches", launchIds[0])!.bindings.length, 2);
    const first = f.p.store.read<IntentSession>("sessions", firstId)!, cleared = f.p.store.read<IntentSession>("sessions", clearedId)!;
    assert.equal(first.receipts.length, 1); assert.equal(cleared.receipts.length, 1);
    assert.equal(first.receipts[0].receipt.effectiveUntil, null);
    assert.notEqual(first.receipts[0].receipt.sessionEpochKey, cleared.receipts[0].receipt.sessionEpochKey);
    assert.notEqual(first.receipts[0].receipt.rootAttemptId, cleared.receipts[0].receipt.rootAttemptId);
    assert.equal(cleared.receipts[0].receipt.parentAttemptId, null);
    assert.equal(cleared.receipts[0].receipt.projectKey, first.receipts[0].receipt.projectKey);
  } finally { await f.close(); }
});

test("trusted folder suggestion is unconfirmed, expires on helper exit, rejects wrong folder and changed registry", async () => {
  const f = await createIntentFixture();
  try {
    assert.equal((await f.run(["intent", "folder-default", "--project", INTENT_FIXTURE_PROJECTS[0].projectKey])).code, 0);
    const launch = await f.run(["launch", "claude", "--use-folder-default"], { env: { FIXTURE_NATIVE_SESSION: nativeId() } });
    assert.equal(launch.code, 0, launch.stdout + launch.stderr); assert.match(launch.stderr, /Default \(unconfirmed\)/);
    const session = f.p.store.read<IntentSession>("sessions", f.p.store.ids("sessions")[0])!;
    assert.equal(session.receipts.length, 2); assert.equal(session.receipts[0].receipt.basis, "trusted_folder_default");
    assert.equal(session.receipts[0].receipt.effectiveUntil, null);
    assert.equal(selectedProjectIntent(session.receipts.map(item => item.receipt), session.receipts[1].receipt.effectiveFrom)!.projectKey, null);
    assert.equal((await f.run(["launch", "codex", "--use-folder-default"], { cwd: path.join(f.root, "other") })).code, 2);
    f.setProjects(INTENT_FIXTURE_PROJECTS.map(project => ({ ...project, projectRegistryRevision: 2 })));
    assert.equal((await f.run(["launch", "codex", "--use-folder-default"])).code, 2);
  } finally { await f.close(); }
});

test("direct no-choice and stale hook env remain Needs a project; Codex receipt-only bind preserves provider exit", async () => {
  const f = await createIntentFixture();
  try {
    const launch = await f.run(["launch", "codex", "--needs-project"], { env: { PLIMSOLL_PROJECT_KEY: INTENT_FIXTURE_PROJECTS[0].projectKey, FIXTURE_EXIT_CODE: "7" } });
    assert.equal(launch.code, 7); assert.match(launch.stderr, /Needs a project/);
    const id = f.p.store.ids("launches")[0];
    const pending = f.p.store.read<IntentLaunch>("launches", id)!;
    assert.equal(pending.receiptDraft.projectKey, null); assert.equal(pending.localState, "awaiting_native_binding");
    const hook = await f.run(["intent", "hook", "--source", "codex"], { env: { PLIMSOLL_INTENT_LAUNCH_ID: id },
      input: JSON.stringify({ hook_event_name: "SessionStart", source: "startup", session_id: nativeId() }) });
    assert.equal(hook.code, 0); assert.match(hook.stdout, /Needs a project/); assert.equal(f.p.store.ids("sessions").length, 0);
    const bind = await f.run(["intent", "bind", "--launch-id", id, "--native-session", nativeId(), "--queue-only"]);
    assert.equal(bind.code, 4); assert.equal(f.p.store.ids("sessions").length, 1);
  } finally { await f.close(); }
});

test("CLI producer return codes cover choices, pending binding, queued delivery and value-blind invalid input", async () => {
  const f = await createIntentFixture();
  try {
    assert.equal((await f.run(["intent", "choices"])).code, 0);
    const declare = await f.run(["intent", "declare", "--source", "codex", "--source-root", f.sourceRoot, "--project", INTENT_FIXTURE_PROJECTS[0].projectKey, "--basis", "routed_launch"]);
    assert.equal(declare.code, 3);
    const id = JSON.parse(declare.stdout).launchId;
    const bind = await f.run(["intent", "bind", "--launch-id", id, "--native-session", nativeId()]); assert.equal(bind.code, 0, bind.stdout);
    const invalid = await f.run(["intent", "declare", "--secret", "PRIVATE_SENTINEL"]);
    assert.equal(invalid.code, 2); assert.ok(!invalid.stdout.includes("PRIVATE_SENTINEL"));
  } finally { await f.close(); }
});

test("cross-process delivery lease serializes a session and reports a busy queue as unknown", async () => {
  const f = await createIntentFixture();
  try {
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
    const row = f.p.bind(launch.launchId, nativeId())!;
    const second = new ProjectIntentProducer(f.config, { directory: path.join(f.root, "collector") });
    const results = await Promise.all([f.p.sendSession(row.sessionId), second.sendSession(row.sessionId)]);
    assert.equal(results[0].delivered, 1); assert.equal(results[1].reason, "intent_delivery_busy"); assert.equal(results[1].queued, null);
    assert.equal(results[0].retainedRefusals, 0); assert.equal(results[1].retainedRefusals, null);
    assert.equal(f.calls.filter(call => call.method === "POST").length, 1);
  } finally { await f.close(); }
});

test("registry-stale preserves refused facts and explicit renewal creates a distinct receipt, never overwrites one", async () => {
  const f = await createIntentFixture();
  try {
    const choice = (await f.p.choices()).projects[0];
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: choice });
    const id = nativeId(), old = f.p.bind(launch.launchId, id)!;
    f.setProjects(INTENT_FIXTURE_PROJECTS.map(project => ({ ...project, projectRegistryRevision: 2 })));
    f.setPost(() => ({ status: 409, body: { error: "project_registry_stale" } }));
    const refused = await f.p.sendSession(id); assert.equal(refused.reason, "project_registry_stale");
    const observed = f.p.store.read<IntentSession>("sessions", id)!;
    assert.equal(observed.observedRevision, 0); assert.deepEqual(observed.receipts[0].receipt, old);
    const callsBefore = f.calls.length;
    await f.p.sendSession(id); assert.equal(f.calls.length, callsBefore, "permanent refusal must not loop on upload cycles");
    const fresh = (await f.p.choices()).projects[0];
    const renewal = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: fresh });
    const updated = f.p.bind(renewal.launchId, id, { continuation: true })!;
    assert.notEqual(updated.receiptId, old.receiptId); assert.notEqual(updated.evidenceRef, old.evidenceRef);
    assert.equal(updated.projectRegistryRevision, 2);
    f.setPost(body => ({ status: 202, body: fixtureIntentAck(body.receipt) }));
    const result = await f.p.sendSession(id); assert.equal(result.delivered, 1);
    const state = f.p.store.read<IntentSession>("sessions", id)!;
    assert.deepEqual(state.receipts[0].receipt, old); assert.equal(state.receipts[0].supersededBy, updated.receiptId);
    assert.equal(state.receipts[0].delivered, false); assert.equal(state.receipts[1].delivered, true);
  } finally { await f.close(); }
});

test("bounded replay reports a known zero, partial coverage and unexamined rows after revocation", async () => {
  const f = await createIntentFixture();
  try {
    const empty = await f.p.replay();
    assert.equal(empty.queued, 0); assert.equal(empty.queueCoverage, "complete"); assert.equal(empty.unexaminedSessions, 0);
    assert.equal(empty.retainedRefusals, 0);
    for (let count = 0; count < 2; count++) {
      const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
      f.p.bind(launch.launchId, nativeId());
    }
    const bounded = await f.p.replay(1, 1);
    assert.equal(bounded.delivered, 1); assert.equal(bounded.queueCoverage, "partial"); assert.equal(bounded.unexaminedSessions, 1);
    assert.equal(bounded.retainedRefusals, 0, "count covers only examined sessions, alongside partial coverage");
    f.setPost(() => ({ status: 403, body: { error: "device_revoked" } }));
    const revoked = await f.p.replay();
    assert.equal(revoked.queued, 1); assert.equal(revoked.queueCoverage, "partial"); assert.equal(revoked.unexaminedSessions, 1);
    assert.deepEqual(revoked.reasons, ["device_revoked"]);
  } finally { await f.close(); }
});

test("lost durable session binding stays pending and exhausted stale recovery does not restart automatically", async () => {
  const f = await createIntentFixture();
  try {
    const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
    const id = nativeId(), row = f.p.bind(launch.launchId, id)!;
    f.setPost(body => ({ status: 409, body: { error: "intent_revision_stale", receiptId: body.receipt.receiptId, sessionId: id, revision: 5 } }));
    const stale = await f.p.sendSession(id); assert.equal(stale.reason, "intent_revision_retry_exhausted");
    const calls = f.calls.length;
    await f.p.sendSession(id); assert.equal(f.calls.length, calls);
    fs.rmSync(path.join(f.p.store.root, "sessions", `${row.sessionId}.json`));
    assert.throws(() => f.p.bind(launch.launchId, id), /intent_continuity_unproved/);
    assert.equal(f.p.store.read<IntentSession>("sessions", id), null);
  } finally { await f.close(); }
});

// Ported from check-r1/checks/nested-start-repro.ts: the parent is still active.
test("P04-R1-01 direct startup under an active parent never borrows its project", async () => {
  const f = await createIntentFixture();
  try {
    const first = nativeId(), unrelated = nativeId();
    fs.writeFileSync(path.join(f.root, "bin", "claude"), `#!${process.execPath}\n${String.raw`
const cp = require('node:child_process');
const args = process.argv.slice(2);
const hook = JSON.parse(args[args.indexOf('--settings') + 1]).hooks.SessionStart[0].hooks[0].command;
const input = id => JSON.stringify({hook_event_name:'SessionStart', source:'startup', session_id:id});
const early = cp.spawnSync('/bin/sh', ['-c', hook], {input:input(process.env.UNRELATED_ID), env:process.env, encoding:'utf8'});
const primary = cp.spawnSync('/bin/sh', ['-c', hook], {input:input(process.env.FIRST_ID), env:process.env, encoding:'utf8'});
if (primary.status !== 0) process.exit(91);
const direct = cp.spawnSync(process.execPath, ['-e',
 "const cp=require('node:child_process');const r=cp.spawnSync('/bin/sh',['-c',process.env.INHERITED_HOOK_COMMAND],{input:process.env.UNRELATED_HOOK_INPUT,env:process.env,encoding:'utf8'});process.stdout.write(r.stdout);process.exit(r.status);"],
 {env:{...process.env, INHERITED_HOOK_COMMAND:hook, UNRELATED_HOOK_INPUT:input(process.env.UNRELATED_ID)}, encoding:'utf8'});
// Also exercise the environment-only shipped hook operation from the intended provider.
const environmentHook = hook.replace(/ '--launch-id' '[0-9a-f-]+'$/, '');
const other = cp.spawnSync('/bin/sh', ['-c', environmentHook], {input:input(process.env.UNRELATED_ID), env:process.env, encoding:'utf8'});
const childClear = cp.spawnSync(process.execPath, ['-e',
 "const cp=require('node:child_process');const r=cp.spawnSync('/bin/sh',['-c',process.env.INHERITED_HOOK_COMMAND],{input:process.env.UNRELATED_HOOK_INPUT,env:process.env,encoding:'utf8'});process.stdout.write(r.stdout);process.exit(r.status);"],
 {env:{...process.env, INHERITED_HOOK_COMMAND:hook, UNRELATED_HOOK_INPUT:JSON.stringify({hook_event_name:'SessionStart',source:'clear',session_id:process.env.UNRELATED_ID})}, encoding:'utf8'});
console.log('ACTIVE_PARENT:'+JSON.stringify({primary:primary.status, direct:direct.status, directOutput:direct.stdout,
 environment:other.status, environmentOutput:other.stdout, early:early.status, earlyOutput:early.stdout,
 childClear:childClear.status, childClearOutput:childClear.stdout}));
process.exit(early.status || direct.status || other.status || childClear.status);
`}`, { mode: 0o700 });
    const launched = await f.run(["launch", "claude", "--project", INTENT_FIXTURE_PROJECTS[0].projectKey,
      "--", "--session-id", first], { env: { FIRST_ID: first, UNRELATED_ID: unrelated } });
    assert.equal(launched.code, 0, launched.stdout + launched.stderr);
    const evidence = JSON.parse(launched.stdout.split("\n").find(line => line.startsWith("ACTIVE_PARENT:"))!.slice(14));
    assert.equal(evidence.primary, 0); assert.equal(evidence.direct, 0); assert.equal(evidence.environment, 0);
    assert.equal(evidence.early, 0); assert.match(evidence.earlyOutput, /Needs a project/);
    assert.equal(evidence.childClear, 0); assert.match(evidence.childClearOutput, /Needs a project/);
    assert.match(evidence.directOutput, /Needs a project/);
    assert.match(evidence.environmentOutput, /Needs a project/);
    assert.equal(f.p.store.ids("launches").length, 1);
    const session = f.p.store.read<IntentSession>("sessions", first)!;
    assert.equal(session.receipts.length, 1); assert.equal(session.receipts[0].receipt.projectKey, INTENT_FIXTURE_PROJECTS[0].projectKey);
    assert.equal(f.p.store.read<IntentSession>("sessions", unrelated), null);
    assert.equal(f.p.store.ids("sessions").length, 1);
  } finally { await f.close(); }
});

test("an unknown Claude resume target cannot claim the first native hook ID", async () => {
  const f = await createIntentFixture();
  try {
    const result = await f.run(["launch", "claude", "--project", INTENT_FIXTURE_PROJECTS[0].projectKey,
      "--", "--resume"], { env: { FIXTURE_NATIVE_SESSION: nativeId() } });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    assert.equal(result.stdout.match(/Needs a project/g)?.length, 4, "no lifecycle hook may claim an unknown initial ID");
    const launches = f.p.store.ids("launches");
    assert.equal(launches.length, 1); assert.equal(f.p.store.ids("sessions").length, 0);
    const launch = f.p.store.read<IntentLaunch>("launches", launches[0])!;
    assert.equal(launch.localState, "awaiting_native_binding"); assert.deepEqual(launch.bindings, []);
  } finally { await f.close(); }
});

// Ported from check-r1/checks/recovery-edge-repro.ts; ACK only after unchanged P02 admission.
test("P04-R1-02 registry renewal plus root/account rotation drains a valid first attempt", async () => {
  for (const rotation of ["both", "root", "account"]) {
    const f = await createIntentFixture();
    try {
      const project = (await f.p.choices()).projects[0];
      const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project });
      const native = nativeId(), refused = f.p.bind(launch.launchId, native)!;
      f.setProjects(INTENT_FIXTURE_PROJECTS.map(p => ({ ...p, projectRegistryRevision: 2 })));
      f.setPost(() => ({ status: 409, body: { error: "project_registry_stale" } }));
      assert.equal((await f.p.sendSession(native)).reason, "project_registry_stale");
      const renewed = f.p.declare({ source: "codex", project: (await f.p.choices()).projects[0],
        sourceRoot: rotation === "account" ? f.sourceRoot : path.join(f.root, "codex-rotated"),
        principal: rotation === "root" ? undefined : "rotated-authoritative-principal" });
      const next = f.p.bind(renewed.launchId, native, { continuation: true })!;
      let admitted = false;
      f.setPost(body => {
        try { appendProjectIntentReceipt([], body.receipt); }
        catch { return { status: 409, body: { error: "attempt_lineage_invalid" } }; }
        admitted = true;
        return { status: 202, body: fixtureIntentAck(body.receipt) };
      });
      const before = f.calls.filter(c => c.method === "POST").length;
      const delivery = await f.p.sendSession(native);
      assert.equal(delivery.delivered, 1, JSON.stringify({ rotation, delivery }));
      assert.equal(f.calls.filter(c => c.method === "POST").length, before + 1);
      assert.equal(admitted, true); assert.equal(delivery.queued, 0);
      assert.equal(delivery.retainedRefusals, 1);
      assert.equal(next.sessionId, refused.sessionId); assert.equal(next.nativeSessionKey, refused.nativeSessionKey);
      assert.equal(next.sessionEpochKey, refused.sessionEpochKey);
      assert.equal(next.rootAttemptId, next.attemptId); assert.equal(next.parentAttemptId, null);
      assert.notEqual(next.attemptId, refused.attemptId); assert.notEqual(next.evidenceRef, refused.evidenceRef);
      const state = f.p.store.read<IntentSession>("sessions", native)!;
      assert.deepEqual(state.receipts[0].receipt, refused);
      assert.equal(state.receipts[0].supersededBy, next.receiptId); assert.equal(state.receipts[0].delivered, false);
      assert.equal(state.receipts[1].delivered, true);
    } finally { await f.close(); }
  }
});

test("registry renewal anchors rotation to admitted history, and ambiguity never becomes a fresh root", async () => {
  const f = await createIntentFixture();
  try {
    const project = (await f.p.choices()).projects[0];
    const firstLaunch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project });
    const native = nativeId(), first = f.p.bind(firstLaunch.launchId, native)!;
    let admitted: ProjectIntentReceipt[] = [];
    const admit = (body: { receipt: ProjectIntentReceipt; expectedRevision: number }) => {
      assert.equal(body.expectedRevision, admitted.length);
      try { admitted = appendProjectIntentReceipt(admitted, body.receipt).receipts; }
      catch { return { status: 409, body: { error: "attempt_lineage_invalid" } }; }
      return { status: 202, body: fixtureIntentAck(body.receipt, admitted.length) };
    };
    f.setPost(admit);
    assert.equal((await f.p.sendSession(native)).delivered, 1);
    const rejectedLaunch = f.p.declare({ source: "codex", sourceRoot: path.join(f.root, "codex-rotated"), project, principal: "rejected-principal" });
    const rejected = f.p.bind(rejectedLaunch.launchId, native, { continuation: true })!;
    f.setProjects(INTENT_FIXTURE_PROJECTS.map(p => ({ ...p, projectRegistryRevision: 2 })));
    f.setPost(() => ({ status: 409, body: { error: "project_registry_stale" } }));
    assert.equal((await f.p.sendSession(native)).reason, "project_registry_stale");
    const renewed = f.p.declare({ source: "codex", sourceRoot: path.join(f.root, "codex-rotated"),
      project: (await f.p.choices()).projects[0], principal: "renewed-principal" });
    const next = f.p.bind(renewed.launchId, native, { continuation: true })!;
    assert.equal(next.rootAttemptId, first.rootAttemptId); assert.equal(next.parentAttemptId, first.attemptId);
    assert.notEqual(next.parentAttemptId, rejected.attemptId);
    f.setPost(admit);
    const delivered = await f.p.sendSession(native);
    assert.equal(delivered.delivered, 1); assert.equal(delivered.queued, 0); assert.equal(delivered.retainedRefusals, 1);
    assert.equal(admitted.length, 2);
    const state = f.p.store.read<IntentSession>("sessions", native)!;
    assert.deepEqual(state.receipts[0].receipt, first); assert.deepEqual(state.receipts[1].receipt, rejected);
    assert.equal(state.receipts[1].supersededBy, next.receiptId);
  } finally { await f.close(); }
  const g = await createIntentFixture();
  try {
    const project = (await g.p.choices()).projects[0];
    const launch = g.p.declare({ source: "codex", sourceRoot: g.sourceRoot, project });
    const native = nativeId(), ambiguous = g.p.bind(launch.launchId, native)!;
    g.setPost(body => ({ status: 202, body: { ...fixtureIntentAck(body.receipt), sessionId: nativeId() } }));
    assert.equal((await g.p.sendSession(native)).reason, "invalid_intent_ack");
    const renewed = g.p.declare({ source: "codex", sourceRoot: path.join(g.root, "codex-rotated"), project, principal: "changed-principal" });
    const next = g.p.bind(renewed.launchId, native, { continuation: true })!;
    assert.equal(next.rootAttemptId, ambiguous.rootAttemptId); assert.equal(next.parentAttemptId, ambiguous.attemptId);
    const state = g.p.store.read<IntentSession>("sessions", native)!;
    assert.equal(state.receipts[0].supersededBy, null); assert.deepEqual(state.receipts[0].receipt, ambiguous);
    assert.doesNotThrow(() => appendProjectIntentReceipt([ambiguous], next));
  } finally { await g.close(); }
});
