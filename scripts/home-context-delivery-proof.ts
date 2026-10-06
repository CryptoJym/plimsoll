import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { attachRepoContextSidecar, REPO_CONTEXT_CAPTURE_POLICY_GENERATION, resolveRepoContextRequests } from "../packages/collector-cli/src/repo-context";
import { buildIngestBatch, attachRepoLinkage } from "../packages/collector-cli/src/upload";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { prepareHistoryEvent, sealHistoryEvent, normalizeHistoryEvent } from "../packages/collector-cli/src/upload-history";
import { applyProjectAttribution, SessionAttributionBatch } from "../packages/collector-cli/src/session-attribution";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { aiInteractionEventSchema, branchLinkageHash, remoteLinkageHash } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "independent-null-delivery-"));
const originalHome = os.homedir;
const sha = (text: string) => createHash("sha256").update(text).digest("hex");
let buffer: LocalEventBuffer | undefined;
let tailer: RolloutTailer | undefined;
const write = (file: string, content: string) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, content, { mode: 0o600 });
};
async function main() {
  try {
    const home = path.join(root, "home");
    const chat = path.join(home, "Documents/chat");
    fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
    const remote = "https://github.com/CryptoJym/plimsoll.git";
    write(path.join(home, ".git/HEAD"), "ref: refs/heads/main\n");
    write(path.join(home, ".git/refs/heads/main"), "a".repeat(40) + "\n");
    write(path.join(home, ".git/config"), `[remote "origin"]\n url = ${remote}\n`);
    os.homedir = () => home;
    const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
    let fakeNow = baseMs;
    const config = collectorConfigSchema.parse({ uploadUrl: "http://127.0.0.1:1/ingest", installKey: "fixture-install" });
    const options = { workspaceId: config.tenantId, enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(fakeNow) } };
    const sessionId = "019f8000-0000-7000-8000-000000000465";
    const oldId = "00000000-0000-4000-8000-000000000466";
    const ledger = path.join(root, "fixture.sqlite");
    buffer = new LocalEventBuffer(ledger, options);
    // Seed one previously captured home-derived row; only later new captures
    // exercise the changed resolver, through the actual head tailer.
    assert.equal(buffer.append(aiInteractionEventSchema.parse({
      id: oldId, source: "codex", eventType: "tool_use", dataMode: "metadata",
      sessionId, observedAt: new Date(baseMs).toISOString(), model: "gpt-6.1-sol",
      metadata: { git: {
        remoteUrlHash: remoteLinkageHash(remote), branchHash: branchLinkageHash("main"), headSha: "a".repeat(40),
      } },
    })), true);
    assert.equal(buffer.delivery.lease({ now: new Date(baseMs) }).items.length, 1);
    const oldRow = () => JSON.stringify(buffer!.database.prepare("select * from buffered_events where id = ?").get(oldId));
    const oldSeal = () => (buffer!.database.prepare("select sealed_envelope_json as seal from upload_outbox where delivery_id = ?")
      .get(oldId) as { seal: string }).seal;
    const before = { row: oldRow(), seal: oldSeal() };
    buffer.close(); buffer = new LocalEventBuffer(ledger, options);
    const sessions = path.join(root, "sessions");
    const rollout = path.join(sessions, "2026/10/05", `rollout-proof-${sessionId}.jsonl`);
    fakeNow = baseMs + 120_000;
    const count = (input: number, output: number) => ({ timestamp: new Date(fakeNow).toISOString(),
      type: "event_msg", payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: input, cached_input_tokens: 0, output_tokens: output,
        reasoning_output_tokens: 0, total_tokens: input + output,
      } } } });
    write(rollout, [
      { timestamp: new Date(fakeNow).toISOString(), type: "session_meta", payload: { id: sessionId, cwd: chat } },
      { timestamp: new Date(fakeNow).toISOString(), type: "turn_context", payload: { model: "gpt-6.1-sol", cwd: chat } },
      count(0, 0),
      count(19, 2),
    ].map(item => JSON.stringify(item)).join("\n") + "\n");
    tailer = new RolloutTailer(buffer, sessions, () => [], { ...DEFAULT_JSONL_TAILER_IO, now: () => fakeNow });
    assert.equal((await tailer.scan({ scope: "full", now: new Date(fakeNow) })).eventsAppended, 1);
    const requests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
    assert.equal(requests.length, 1);
    const results = resolveRepoContextRequests(requests);
    assert.deepEqual([results[0]?.repoHash, results[0]?.branchHash, results[0]?.headSha], [null, null, null]);
    const receipt = buffer.applyRepoContextResults(results);
    assert.equal(receipt.unknownResults, 1);
    assert.equal(receipt.rowsFilled, 0);
    const raw = buffer.database.prepare(`select id, repo_hash as repo, branch_hash as branch, head_sha as head,
      input_tokens as input, output_tokens as output from buffered_events where id <> ?`).get(oldId) as {
        id: string; repo: string | null; branch: string | null; head: string | null; input: number; output: number;
      };
    assert.deepEqual({ repo: raw.repo, branch: raw.branch, head: raw.head }, { repo: null, branch: null, head: null });
    const lease = buffer.delivery.lease({ now: new Date(fakeNow + 2_000) });
    assert.equal(lease.locallyDead, 0);
    const newDelivery = lease.items.find(item => item.deliveryId === raw.id)!;
    assert.ok(newDelivery);
    assert.equal(newDelivery.envelope.event.projectKey, undefined);
    assert.equal(newDelivery.envelope.event.metadata.projectBasis, "unallocated");
    assert.equal(newDelivery.envelope.event.metadata.git, undefined);
    assert.equal(newDelivery.envelope.event.metadata.branchHash, undefined);
    assert.equal(newDelivery.envelope.event.metadata.headSha, undefined);
    assert.equal(newDelivery.envelope.event.metadata.repoContextPolicyGeneration, undefined);
    assert.equal(newDelivery.envelope.suppressedFields.includes("repoContextPolicyGeneration"), false);
    assert.equal(newDelivery.envelope.event.inputTokens, 19);
    assert.equal(newDelivery.envelope.event.outputTokens, 2);
    // Every attribution reader sees the same actual raw GEN2/null receipt,
    // with the earlier home-tagged tool still present in this session.
    const captured = buffer.listUnuploaded({ maxRows: 100 }).find(row => row.id === raw.id)!;
    assert.ok(captured);
    assert.equal(captured.payload.metadata.repoContextPolicyGeneration, REPO_CONTEXT_CAPTURE_POLICY_GENERATION);
    const batchAttribution = new SessionAttributionBatch(buffer.database,
      [{ event: captured.payload, repoHash: captured.repoHash }]);
    const historyInput = { payloadJson: JSON.stringify(captured.payload),
      suppressedFieldsJson: JSON.stringify(captured.suppressedFields),
      repoHash: captured.repoHash, branchHash: captured.branchHash };
    const prepared = prepareHistoryEvent(historyInput);
    assert.ok(prepared.ok);
    const history = sealHistoryEvent(prepared, { ...historyInput, attribution: batchAttribution });
    assert.ok(history.ok);
    const historyWithoutBatch = normalizeHistoryEvent(historyInput);
    assert.ok(historyWithoutBatch.ok);
    const noMark = buildIngestBatch(config, buffer).batch!.events.find(item => item.event.id === raw.id)!;
    assert.ok(noMark);
    const direct = sealOutboundEnvelope({ event: batchAttribution.attribute(captured.payload, {
      repoHash: captured.repoHash, branchHash: captured.branchHash }).event, suppressedFields: [] });
    assert.ok(direct.ok);
    const own = sealOutboundEnvelope({ event: applyProjectAttribution(captured.payload, {
      repoHash: captured.repoHash, branchHash: captured.branchHash }).event, suppressedFields: [] });
    assert.ok(own.ok);
    const attached = sealOutboundEnvelope({ event: attachRepoLinkage(captured.payload, captured.repoHash,
      captured.branchHash), suppressedFields: [] });
    assert.ok(attached.ok);
    const routes = { outbox: newDelivery.envelope, noMark, history: history.envelope,
      historyWithoutBatch: historyWithoutBatch.envelope, batchAttribution: direct.envelope,
      directAttribution: own.envelope, attachRepoLinkage: attached.envelope };
    console.log(JSON.stringify({ sealRoutes: Object.fromEntries(Object.entries(routes).map(([name, envelope]) =>
      [name, { project: envelope.event.projectKey ?? null, basis: envelope.event.metadata.projectBasis,
        input: envelope.event.inputTokens, output: envelope.event.outputTokens }])) }));
    for (const [name, envelope] of Object.entries(routes)) {
      assert.equal(envelope.event.projectKey, undefined, `${name} honours the capture-time exclusion`);
      assert.equal(envelope.event.metadata.projectBasis, "unallocated", name);
      assert.equal(envelope.event.metadata.git, undefined, name);
      assert.equal(envelope.event.metadata.branchHash, undefined, name);
      assert.equal(envelope.event.metadata.headSha, undefined, name);
      assert.equal(envelope.event.inputTokens, 19, name); assert.equal(envelope.event.outputTokens, 2, name);
      assert.ok(!JSON.stringify(envelope).includes("repoContextPolicyGeneration"), name);
      assert.ok(!JSON.stringify(envelope).includes(chat), name);
    }
    // An existing generation-2 exclusion stays excluded after this policy
    // advances. The older raw receipt is never rewritten to generation 3.
    const legacyReceipt = { ...captured.payload, metadata: { ...captured.payload.metadata, repoContextPolicyGeneration: 2 } };
    const legacyAttribution = sealOutboundEnvelope({ event: batchAttribution.attribute(legacyReceipt, {
      repoHash: null }).event, suppressedFields: [] });
    assert.ok(legacyAttribution.ok);
    const legacyHistoryInput = { ...historyInput, payloadJson: JSON.stringify(legacyReceipt) };
    const legacyPrepared = prepareHistoryEvent(legacyHistoryInput);
    assert.ok(legacyPrepared.ok);
    const legacyHistory = sealHistoryEvent(legacyPrepared, { ...legacyHistoryInput, attribution: batchAttribution });
    assert.ok(legacyHistory.ok);
    for (const envelope of [legacyAttribution.envelope, legacyHistory.envelope]) {
      assert.equal(envelope.event.projectKey, undefined);
      assert.equal(envelope.event.metadata.projectBasis, "unallocated");
      assert.ok(!JSON.stringify(envelope).includes("repoContextPolicyGeneration"));
    }
    assert.equal(legacyReceipt.metadata.repoContextPolicyGeneration, 2);
    // An explicit dispatch/session assignment remains stronger than the
    // filesystem exclusion; a real repository is checked later below.
    const explicit = applyProjectAttribution({ ...captured.payload,
      projectKey: remoteLinkageHash("https://example.invalid/explicit/owner.git") }, { repoHash: null }).event;
    const explicitSeal = sealOutboundEnvelope({ event: explicit, suppressedFields: [] });
    assert.ok(explicitSeal.ok);
    assert.equal(explicitSeal.envelope.event.projectKey, explicit.projectKey);
    assert.equal(explicitSeal.envelope.event.metadata.projectBasis, "explicit");
    assert.deepEqual({ row: oldRow(), seal: oldSeal() }, before);
    const insideWindow = { freshHomeLookupReturnsNull: true, rawRepoBranchHeadNull: true,
      actualProjectKey: newDelivery.envelope.event.projectKey, actualBasis: newDelivery.envelope.event.metadata.projectBasis,
      inputTokens: 19, outputTokens: 2, archivedRowAndSealByteIdentical: true, oldRowSha256: sha(before.row) };
    fakeNow += 60_000;
    // OTLP uses a separate live-authority session; mixing it with this
    // rollout-authority session would correctly reject its paid event.
    const inlineSession = "019f8000-0000-7000-8000-000000000467";
    const inlineOldId = "00000000-0000-4000-8000-000000000468";
    assert.equal(buffer.append(aiInteractionEventSchema.parse({ id: inlineOldId,
      source: "codex", eventType: "tool_use", dataMode: "metadata", sessionId: inlineSession,
      observedAt: new Date(baseMs).toISOString(), metadata: { git: {
        remoteUrlHash: remoteLinkageHash(remote), branchHash: branchLinkageHash("main"), headSha: "a".repeat(40),
      } } })), true);
    assert.ok(buffer.delivery.lease({ now: new Date(fakeNow) }).items.some(item => item.deliveryId === inlineOldId));
    const inlineSnapshot = () => ({
      row: JSON.stringify(buffer!.database.prepare("select * from buffered_events where id=?").get(inlineOldId)),
      seal: (buffer!.database.prepare("select sealed_envelope_json as seal from upload_outbox where delivery_id=?")
        .get(inlineOldId) as { seal: string }).seal,
    });
    const inlineBefore = inlineSnapshot();
    const inline = explodeOtlpPayload({ resourceLogs: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "codex_exec" } }] },
      scopeLogs: [{ logRecords: [{ timeUnixNano: String(BigInt(fakeNow) * 1_000_000n), attributes: [
        { key: "cwd", value: { stringValue: chat } },
        { key: "session.id", value: { stringValue: inlineSession } },
        { key: "gen_ai.usage.input_tokens", value: { intValue: "19" } },
        { key: "gen_ai.usage.output_tokens", value: { intValue: "2" } },
      ] }] }],
    }] }, { source: "codex", resolveGit: true });
    assert.equal(inline.events.length, 1);
    assert.equal(buffer.appendMany(inline.events, inline.metricSamples, inline.admissionDrops).deduplicatedCount, 0);
    const inlineRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
    assert.equal(inlineRequests.length, 1);
    assert.equal(buffer.applyRepoContextResults(resolveRepoContextRequests(inlineRequests)).rowsFilled, 0);
    const inlineDelivery = buffer.delivery.lease({ now: new Date(fakeNow) }).items
      .find(item => item.deliveryId === inline.events[0]!.event.id)!;
    assert.ok(inlineDelivery);
    assert.equal(inlineDelivery.envelope.event.projectKey, undefined);
    assert.equal(inlineDelivery.envelope.event.metadata.projectBasis, "unallocated");
    assert.equal(inlineDelivery.envelope.event.metadata.git, undefined);
    assert.equal(inlineDelivery.envelope.event.inputTokens, 19);
    assert.equal(inlineDelivery.envelope.event.outputTokens, 2);
    assert.ok(!JSON.stringify(inlineDelivery.envelope).includes(chat));
    assert.ok(!JSON.stringify(inlineDelivery.envelope).includes("repoContextPolicyGeneration"));
    assert.deepEqual({ row: oldRow(), seal: oldSeal() }, before);
    assert.deepEqual(inlineSnapshot(), inlineBefore);
    fakeNow = baseMs + 7 * 3_600_000;
    fs.appendFileSync(rollout, JSON.stringify(count(38, 4)) + "\n");
    assert.equal((await tailer.scan({ scope: "full", now: new Date(fakeNow) })).eventsAppended, 1);
    const beyond = buffer.database.prepare(`select id, repo_hash as repo, branch_hash as branch, head_sha as head,
      input_tokens as input, output_tokens as output from buffered_events order by observed_at desc limit 1`).get() as {
        id: string; repo: string | null; branch: string | null; head: string | null; input: number; output: number;
      };
    assert.equal(beyond.repo, null);
    const beyondLease = buffer.delivery.lease({ now: new Date(fakeNow) });
    const beyondDelivery = beyondLease.items.find(item => item.deliveryId === beyond.id)!;
    assert.ok(beyondDelivery);
    assert.equal(beyondDelivery.envelope.event.projectKey, undefined);
    assert.equal(beyondDelivery.envelope.event.metadata.projectBasis, "unallocated");
    assert.equal(beyondDelivery.envelope.event.inputTokens, 19);
    assert.equal(beyondDelivery.envelope.event.outputTokens, 2);
    // A real checkout with the very same Plimsoll remote remains attributable.
    // The policy excludes a context, never a remote hash.
    const real = path.join(home, "projects/real-plimsoll");
    write(path.join(real, ".git/HEAD"), "ref: refs/heads/main\n");
    write(path.join(real, ".git/refs/heads/main"), "b".repeat(40) + "\n");
    write(path.join(real, ".git/config"), `[remote "origin"]\n url = ${remote}\n`);
    fakeNow += 60_000;
    fs.appendFileSync(rollout, JSON.stringify({ timestamp: new Date(fakeNow).toISOString(),
      type: "turn_context", payload: { model: "gpt-6.1-sol", cwd: real } }) + "\n" + JSON.stringify(count(57, 6)) + "\n");
    assert.equal((await tailer.scan({ scope: "full", now: new Date(fakeNow) })).eventsAppended, 1);
    const realRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
    assert.equal(realRequests.length, 1);
    assert.equal(buffer.applyRepoContextResults(resolveRepoContextRequests(realRequests)).rowsFilled, 1);
    const realRaw = buffer.database.prepare("select id from buffered_events order by observed_at desc limit 1").get() as { id: string };
    const realDelivery = buffer.delivery.lease({ now: new Date(fakeNow) }).items.find(item => item.deliveryId === realRaw.id)!;
    assert.ok(realDelivery);
    assert.equal(realDelivery.envelope.event.projectKey, remoteLinkageHash(remote));
    assert.equal(realDelivery.envelope.event.metadata.projectBasis, "repo_context");
    assert.equal(realDelivery.envelope.event.inputTokens, 19);
    assert.equal(realDelivery.envelope.event.outputTokens, 2);
    fakeNow += 121_001;
    const positive = aiInteractionEventSchema.parse({ id: "00000000-0000-4000-8000-000000000478",
      sessionId: "00000000-0000-4000-8000-000000000479", source: "codex", dataMode: "metadata",
      eventType: "assistant_response", observedAt: new Date(fakeNow).toISOString(), model: "gpt-6.1-sol",
      inputTokens: 19, outputTokens: 2, metadata: { git: { remoteUrlHash: remoteLinkageHash(remote),
        branchHash: branchLinkageHash("main"), headSha: "b".repeat(40) } } });
    assert.equal(attachRepoContextSidecar(positive, "real-inline-positive", real), true);
    assert.equal(buffer.append(positive), true);
    const positiveRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
    assert.equal(positiveRequests.length, 1);
    buffer.applyRepoContextResults(resolveRepoContextRequests(positiveRequests));
    const positiveRaw = buffer.listUnuploaded({ maxRows: 100 }).find(row => row.id === positive.id)!;
    assert.equal(positiveRaw.payload.metadata.repoContextPolicyGeneration, REPO_CONTEXT_CAPTURE_POLICY_GENERATION);
    const positiveDelivery = buffer.delivery.lease({ now: new Date(fakeNow) }).items
      .find(item => item.deliveryId === positive.id)!.envelope;
    const positiveNoMark = buildIngestBatch(config, buffer).batch!.events.find(item => item.event.id === positive.id)!;
    const positiveHistory = normalizeHistoryEvent({ payloadJson: JSON.stringify(positiveRaw.payload),
      suppressedFieldsJson: "[]", repoHash: positiveRaw.repoHash, branchHash: positiveRaw.branchHash });
    assert.ok(positiveHistory.ok);
    for (const envelope of [positiveDelivery, positiveNoMark, positiveHistory.envelope]) {
      assert.equal(envelope.event.projectKey, remoteLinkageHash(remote));
      assert.equal((envelope.event.metadata.git as { remoteUrlHash: string }).remoteUrlHash, remoteLinkageHash(remote));
      assert.equal((envelope.event.metadata.git as { branchHash: string }).branchHash, branchLinkageHash("main"));
      assert.equal((envelope.event.metadata.git as { headSha: string }).headSha, "b".repeat(40));
      assert.equal(envelope.event.inputTokens, 19); assert.equal(envelope.event.outputTokens, 2);
      assert.ok(!JSON.stringify(envelope).includes("repoContextPolicyGeneration"));
    }
    assert.deepEqual({ row: oldRow(), seal: oldSeal() }, before);
    console.log(JSON.stringify({ proof: "independent-fresh-home-delivery", status: "PASS",
      insideWindow, beyondWindow: { elapsedMs: 7 * 3_600_000, projectAbsent: true, inputTokens: 19, outputTokens: 2 },
      sessionInheritanceWindowMs: 6 * 3_600_000, savedActiveContextWasNotReused: true,
      inlineNullOtlpEnvelopeUnlinked: true,
      captureReceiptHonouredByAllAttributionRoutes: Object.keys(routes), explicitAssignmentPreserved: true,
      legacyGenerationTwoExclusionPreserved: true,
      realPlimsollRepoPreserved: true, positiveReceiptKeepsAllGitFieldsOnEveryUploadPath: true }, null, 2));
  } finally { tailer?.close(); buffer?.close(); os.homedir = originalHome; fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
