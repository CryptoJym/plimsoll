import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { resolveRepoContextRequests } from "../packages/collector-cli/src/repo-context";
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
    const options = { delivery: { enabled: true, now: () => new Date(fakeNow) } };
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
    console.log(JSON.stringify({ proof: "independent-fresh-home-delivery", status: "PASS",
      insideWindow, beyondWindow: { elapsedMs: 7 * 3_600_000, projectAbsent: true, inputTokens: 19, outputTokens: 2 },
      sessionInheritanceWindowMs: 6 * 3_600_000, savedActiveContextWasNotReused: true,
      inlineNullOtlpEnvelopeUnlinked: true,
      realPlimsollRepoPreserved: true }, null, 2));
  } finally { tailer?.close(); buffer?.close(); os.homedir = originalHome; fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error instanceof Error ? error.message : "delivery witness failed"); process.exitCode = 1; });
