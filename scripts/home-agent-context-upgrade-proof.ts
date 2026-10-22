import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { GrokUsageTailer, GROK_USAGE_LIMITS } from "../packages/collector-cli/src/grok-usage-tailer";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { REPO_CONTEXT_RESOLVER_VERSION,
  resolveRepoContextRequests, type RepoContextRequest } from "../packages/collector-cli/src/repo-context";
import { remoteLinkageHash } from "../packages/shared/src/index";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { grokUsageDocument, type FixtureTurn } from "./lib/grok-usage-fixture";

const kind = process.argv[2];
assert.ok(kind === "claude" || kind === "grok");
const release = "71d6ff27f0d39aa31d188c9bcc31d37bf188c384";
const root = fs.mkdtempSync(path.join(os.tmpdir(), `home-${kind}-upgrade-`));
const oldTree = path.join(root, "released-0751");
const originalHome = os.homedir;
let registered = false;
let buffer: LocalEventBuffer | undefined;
let tailer: TranscriptTailer | GrokUsageTailer | undefined;
const write = (file: string, value: string) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, value, { mode: 0o600 });
};
async function main() {
  try {
    execFileSync("git", ["worktree", "add", "--quiet", "--detach", oldTree, release]);
    registered = true;
    fs.symlinkSync(path.resolve("node_modules"), path.join(oldTree, "node_modules"), "dir");
    const oldBuffer = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/buffer.ts")).href);
    const oldClaude = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/transcript-tailer.ts")).href);
    const oldGrok = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/grok-usage-tailer.ts")).href);
    const oldContext = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/repo-context.ts")).href);
    const home = path.join(root, "home");
    const chat = path.join(home, "Documents/chat");
    const real = path.join(home, "projects/real-plimsoll");
    fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
    const remote = "https://github.com/CryptoJym/plimsoll.git";
    for (const directory of [home, real]) {
      write(path.join(directory, ".git/HEAD"), "ref: refs/heads/main\n");
      write(path.join(directory, ".git/refs/heads/main"), "a".repeat(40) + "\n");
      write(path.join(directory, ".git/config"), `[remote "origin"]\n url = ${remote}\n`);
    }
    os.homedir = () => home;
    const session = "019f8000-0000-7000-8000-000000000466";
    const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
    let now = baseMs;
    const options = { enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(now) } };
    const ledger = path.join(root, "upgrade.sqlite");
    const projects = path.join(root, "claude-projects");
    const transcript = path.join(projects, "fixture-project", session + ".jsonl");
    const grokHome = path.join(root, "grok");
    const turns: FixtureTurn[] = [];
    const record = (index: number, cwd?: string) => {
      if (kind === "claude") {
        const line = JSON.stringify({ type: "assistant", sessionId: session, timestamp: new Date(now).toISOString(),
          ...(cwd ? { cwd } : {}), message: { id: "growing-message", model: "claude-sonnet-4-20250514",
            usage: { input_tokens: 19 * index, output_tokens: 2 * index,
              cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } } }) + "\n";
        if (index === 1) write(transcript, line); else fs.appendFileSync(transcript, line);
      } else {
        turns.push({ turnNumber: index, endedAt: new Date(now).toISOString(), models: [{ model: "grok-4.7-build",
          input: 19, output: 2, cachedRead: 0, cacheCreation: 0, reasoning: 0, modelCalls: 1, costTicks: 10 }] });
        write(path.join(grokHome, "sessions", encodeURIComponent(cwd ?? chat), session, "usage.json"),
          JSON.stringify(grokUsageDocument({ sessionId: session, updatedAt: new Date(now).toISOString(), shape: "modern", turns })));
      }
    };
    const makeTailer = (legacy: boolean): TranscriptTailer | GrokUsageTailer => kind === "claude"
      ? new (legacy ? oldClaude.TranscriptTailer : TranscriptTailer)(buffer, projects, { ...DEFAULT_JSONL_TAILER_IO, now: () => now })
      : new (legacy ? oldGrok.GrokUsageTailer : GrokUsageTailer)(buffer, grokHome, GROK_USAGE_LIMITS, () => now);
    const scan = () => kind === "claude"
      ? (tailer as TranscriptTailer).scan({ scope: "full", now: new Date(now) })
      : (tailer as GrokUsageTailer).scan({ now: new Date(now), budget: new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 10_000 }) });
    record(1, chat);
    buffer = new oldBuffer.LocalEventBuffer(ledger, options);
    tailer = makeTailer(true);
    assert.equal((await scan()).eventsAppended, 1);
    const oldRequests = buffer!.beginRepoContextResolution(buffer!.takeRepoContextBatch());
    assert.equal(oldRequests.length, 1);
    const oldId = oldRequests[0]!.contextId;
    const oldResults = oldContext.resolveRepoContextRequests(oldRequests);
    assert.equal(oldResults[0].repoHash, remoteLinkageHash(remote));
    assert.equal(buffer!.applyRepoContextResults(oldResults).rowsFilled, 1);
    const oldEventId = (buffer!.database.prepare("select id from buffered_events").get() as { id: string }).id;
    assert.equal(buffer!.delivery.lease({ now: new Date(now) }).items.length, 1);
    const snapshot = () => ({
      row: JSON.stringify(buffer!.database.prepare("select * from buffered_events where id=?").get(oldEventId)),
      seal: (buffer!.database.prepare("select sealed_envelope_json as seal from upload_outbox where delivery_id=?")
        .get(oldEventId) as { seal: string }).seal,
      result: JSON.stringify(buffer!.database.prepare("select * from repo_context_results where context_id=?").get(oldId)),
    });
    const archived = snapshot();
    const claudeCursor = () => buffer!.database.prepare(
      "select committed_offset as offset, parser_state_json as state from rollout_scan_state",
    ).get() as { offset: number; state: string };
    let priorOffset = kind === "claude" ? claudeCursor().offset : 0;
    if (kind === "claude") assert.equal(priorOffset, fs.statSync(transcript).size);
    tailer!.close(); buffer!.close();
    buffer = new LocalEventBuffer(ledger, options); tailer = makeTailer(false);
    for (const [i, offset] of [60_000, 86_400_000, 172_800_000].entries()) {
      now = baseMs + offset;
      record(i + 2); // Claude has no new cwd; Grok re-observes the same group.
      assert.equal((await scan()).eventsAppended, 1);
      const requests: RepoContextRequest[] = buffer!.beginRepoContextResolution(buffer!.takeRepoContextBatch());
      if (kind === "grok") assert.equal(requests.length, 1);
      else assert.equal(requests.length, 0);
      if (requests.length) {
        assert.notEqual(requests[0]!.contextId, oldId);
        const results = resolveRepoContextRequests(requests);
        assert.equal(results[0]!.repoHash, null);
        assert.equal(results[0]!.resolverVersion, REPO_CONTEXT_RESOLVER_VERSION);
        assert.equal(buffer!.applyRepoContextResults(results).rowsFilled, 0);
      }
      const latest = buffer!.database.prepare("select id, repo_hash as repo, branch_hash as branch, head_sha as head, model, input_tokens as input, output_tokens as output from buffered_events order by observed_at desc limit 1")
        .get() as { id: string; repo: string | null; branch: string | null; head: string | null;
          model: string; input: number; output: number };
      assert.equal(latest.repo, null); assert.equal(latest.input, 19); assert.equal(latest.output, 2);
      assert.equal(latest.branch, null); assert.equal(latest.head, null);
      assert.equal(latest.model, kind === "claude" ? "claude-sonnet-4-20250514" : "grok-4.7-build");
      if (kind === "claude") {
        const checkpoint = claudeCursor();
        assert.ok(checkpoint.offset > priorOffset);
        assert.equal(checkpoint.offset, fs.statSync(transcript).size);
        const state = JSON.parse(checkpoint.state) as { repoContextPolicyGeneration: number;
          usageRevisions: Array<{ messageId: string; input: number; output: number; repoContextId: string;
            repoContextPolicyGeneration: number }> };
        assert.equal(state.repoContextPolicyGeneration, undefined);
        assert.ok(oldClaude.validateTranscriptParserState(state));
        assert.equal(state.usageRevisions.length, 1);
        assert.equal(state.usageRevisions[0]!.repoContextPolicyGeneration, undefined);
        assert.equal(buffer!.repoContextHasCurrentCapturePolicy(state.usageRevisions[0]!.repoContextId, [latest.id]), true);
        assert.equal(state.usageRevisions[0]!.messageId, "growing-message");
        assert.equal(state.usageRevisions[0]!.input, 19 * (i + 2));
        assert.equal(state.usageRevisions[0]!.output, 2 * (i + 2));
        assert.notEqual(state.usageRevisions[0]!.repoContextId, oldId);
        assert.equal(latest.id, deterministicEventId(["claude-transcript-revision", session,
          "growing-message", String(19 * (i + 2)), "0", "0", String(2 * (i + 2))]));
        priorOffset = checkpoint.offset;
      }
      const delivery: ReturnType<LocalEventBuffer["delivery"]["lease"]>["items"][number] =
        buffer!.delivery.lease({ now: new Date(now) }).items.find(item => item.deliveryId === latest.id)!;
      assert.ok(delivery);
      assert.equal(delivery.envelope.event.projectKey, undefined);
      assert.equal(delivery.envelope.event.metadata.git, undefined);
      assert.equal(delivery.envelope.event.metadata.branchHash, undefined);
      assert.equal(delivery.envelope.event.metadata.headSha, undefined);
      assert.equal(delivery.envelope.event.metadata.repoContextPolicyGeneration, undefined);
      assert.ok(!JSON.stringify(delivery.envelope).includes("repoContextPolicyGeneration"));
      assert.equal(delivery.envelope.event.inputTokens, 19); assert.equal(delivery.envelope.event.outputTokens, 2);
      assert.deepEqual(snapshot(), archived);
      tailer!.close(); buffer!.close();
      buffer = new LocalEventBuffer(ledger, options); tailer = makeTailer(false);
    }
    now += 60_000; record(5, chat);
    assert.equal((await scan()).eventsAppended, 1);
    const homeRequests = buffer!.beginRepoContextResolution(buffer!.takeRepoContextBatch());
    assert.equal(homeRequests.length, 1);
    assert.equal(buffer!.applyRepoContextResults(resolveRepoContextRequests(homeRequests)).rowsFilled, 0);
    const homeRow = buffer!.database.prepare("select id from buffered_events order by observed_at desc limit 1").get() as { id: string };
    assert.equal(buffer!.delivery.lease({ now: new Date(now) }).items.find(item => item.deliveryId === homeRow.id)!.envelope.event.projectKey, undefined);
    now += 60_000; record(6, real);
    assert.equal((await scan()).eventsAppended, 1);
    const realRequests = buffer!.beginRepoContextResolution(buffer!.takeRepoContextBatch());
    assert.equal(realRequests.length, 1);
    const realResults = resolveRepoContextRequests(realRequests);
    assert.equal(realResults[0]!.resolverVersion, REPO_CONTEXT_RESOLVER_VERSION);
    assert.equal(buffer!.applyRepoContextResults(realResults).rowsFilled, 1);
    assert.equal(buffer!.repoContextHasCurrentCapturePolicy(realRequests[0]!.contextId), true);
    assert.equal(buffer!.repoContextHasCurrentCapturePolicy(oldId), false);
    const realRow = buffer!.database.prepare("select id from buffered_events order by observed_at desc limit 1").get() as { id: string };
    const realDelivery = buffer!.delivery.lease({ now: new Date(now) }).items.find(item => item.deliveryId === realRow.id)!;
    assert.ok(realDelivery); assert.equal(realDelivery.envelope.event.projectKey, remoteLinkageHash(remote));
    assert.equal(realDelivery.envelope.event.inputTokens, 19); assert.equal(realDelivery.envelope.event.outputTokens, 2);
    assert.deepEqual(snapshot(), archived);
    console.log(JSON.stringify({ proof: `home-${kind}-saved-context-upgrade`, status: "PASS", releasedWriter: release,
      continuedAtMs: [60_000, 86_400_000, 172_800_000], newHomeEnvelopesUnlinked: true,
      earlierRawSealAndResultByteIdentical: true, realPlimsollRetained: true, marginalInput: 19, marginalOutput: 2 }));
  } finally {
    tailer?.close(); buffer?.close(); os.homedir = originalHome;
    if (registered) execFileSync("git", ["worktree", "remove", "--force", oldTree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error instanceof Error ? error.message : "agent upgrade proof failed"); process.exitCode = 1; });
