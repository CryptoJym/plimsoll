import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { resolveRepoContextRequests, type RepoContextRequest } from "../packages/collector-cli/src/repo-context";
import { remoteLinkageHash } from "../packages/shared/src/index";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";

const released = "71d6ff27f0d39aa31d188c9bcc31d37bf188c384";
const root = fs.mkdtempSync(path.join(os.tmpdir(), "independent-tailer-"));
const oldTree = path.join(root, "released-0751");
const oldHome = os.homedir;
let registered = false;
let buffer: LocalEventBuffer | undefined;
let tailer: RolloutTailer | undefined;
const sha = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const write = (file: string, content: string) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, content, { mode: 0o600 });
};
async function main() {
  try {
    execFileSync("git", ["worktree", "add", "--quiet", "--detach", oldTree, released]);
    registered = true;
    fs.symlinkSync(path.resolve("node_modules"), path.join(oldTree, "node_modules"), "dir");
    const previousBuffer = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/buffer.ts")).href);
    const previousTailer = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/rollout-tailer.ts")).href);
    const previousContext = await import(pathToFileURL(path.join(oldTree, "packages/collector-cli/src/repo-context.ts")).href);
    assert.equal(JSON.parse(fs.readFileSync(path.join(oldTree, "packages/collector-cli/package.json"), "utf8")).version, "0.7.51");
    const home = path.join(root, "home");
    const chat = path.join(home, "Documents/chat");
    fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
    write(path.join(home, ".git/HEAD"), "ref: refs/heads/main\n");
    write(path.join(home, ".git/refs/heads/main"), "a".repeat(40) + "\n");
    const remote = "https://github.com/CryptoJym/plimsoll.git";
    write(path.join(home, ".git/config"), `[remote "origin"]\n url = ${remote}\n`);
    os.homedir = () => home;
    const sessions = path.join(root, "codex-sessions");
    const sessionId = "019f8000-0000-7000-8000-000000000464";
    const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
    let fakeNow = baseMs;
    const io = { ...DEFAULT_JSONL_TAILER_IO, now: () => fakeNow };
    const rollout = path.join(sessions, "2026/10/05", `rollout-proof-${sessionId}.jsonl`);
    const count = (input: number, output: number) => ({ timestamp: new Date(fakeNow).toISOString(),
      type: "event_msg", payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: input, cached_input_tokens: 0, output_tokens: output,
        reasoning_output_tokens: 0, total_tokens: input + output,
      } } } });
    write(rollout, [
      { timestamp: new Date(fakeNow).toISOString(), type: "session_meta", payload: { id: sessionId, cwd: chat } },
      { timestamp: new Date(fakeNow).toISOString(), type: "turn_context", payload: { model: "gpt-6.1-sol", cwd: chat } },
      count(19, 2),
    ].map(line => JSON.stringify(line)).join("\n") + "\n");
    const ledger = path.join(root, "upgrade.sqlite");
    const options = { enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(fakeNow) } };
    buffer = new previousBuffer.LocalEventBuffer(ledger, options);
    tailer = new previousTailer.RolloutTailer(buffer, sessions, () => [], io);
    const oldScan = await tailer!.scan({ scope: "full", now: new Date(fakeNow) });
    assert.equal(oldScan.eventsAppended, 1);
    const oldRequests = buffer!.beginRepoContextResolution(buffer!.takeRepoContextBatch());
    assert.equal(oldRequests.length, 1);
    const oldResult = previousContext.resolveRepoContextRequests(oldRequests);
    assert.equal(oldResult[0].repoHash, remoteLinkageHash(remote));
    assert.equal(buffer!.applyRepoContextResults(oldResult).rowsFilled, 1);
    const oldId = (buffer!.database.prepare("select id from buffered_events").get() as { id: string }).id;
    const oldLease = buffer!.delivery.lease({ now: new Date(fakeNow) });
    assert.equal(oldLease.items.length, 1);
    const snapshot = () => ({
      row: JSON.stringify(buffer!.database.prepare("select * from buffered_events where id = ?").get(oldId)),
      outbox: JSON.stringify(buffer!.database.prepare("select * from upload_outbox where delivery_id = ?").get(oldId)),
      results: JSON.stringify(buffer!.database.prepare("select * from repo_context_results order by context_id").all()),
    });
    const before = snapshot();
    const cursor = () => buffer!.database.prepare("select committed_offset as offset, parser_state_json as state from rollout_scan_state").get() as
      { offset: number; state: string };
    const previousState = JSON.parse(cursor().state) as { activeRepoContextId: string; tokenCountIndex: number;
      previous: Record<string, number>; model: string };
    let priorOffset = cursor().offset;
    assert.equal(priorOffset, fs.statSync(rollout).size);
    tailer!.close(); buffer!.close(); tailer = undefined; buffer = undefined;
    buffer = new LocalEventBuffer(ledger, options);
    tailer = new RolloutTailer(buffer, sessions, () => [], io);
    const continuations: Array<Record<string, unknown>> = [];
    for (const [index, offsetMs] of [60_000, 86_400_000, 172_800_000].entries()) {
      fakeNow = baseMs + offsetMs;
      fs.appendFileSync(rollout, JSON.stringify(count(19 * (index + 2), 2 * (index + 2))) + "\n");
      const scan = await tailer.scan({ scope: "full", now: new Date(fakeNow) });
      assert.equal(scan.eventsAppended, 1);
      const newRequests: RepoContextRequest[] = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
      assert.equal(newRequests.length, 0);
      const rows = buffer.database.prepare(`select id, repo_hash as repo, branch_hash as branch, head_sha as head,
        input_tokens as input, output_tokens as output from buffered_events order by observed_at`).all() as Array<{
          id: string; repo: string | null; branch: string | null; head: string | null; input: number; output: number;
        }>;
      const latest = rows.at(-1)!;
      const checkpoint = cursor();
      const parser = JSON.parse(checkpoint.state) as { repoContextPolicyGeneration: number; activeRepoContextId: string;
        tokenCountIndex: number; previous: Record<string, number>; model: string };
      assert.ok(checkpoint.offset > priorOffset);
      assert.equal(checkpoint.offset, fs.statSync(rollout).size);
      assert.equal(parser.repoContextPolicyGeneration, undefined);
      assert.ok(previousTailer.validateRolloutParserState(parser));
      assert.equal(buffer.repoContextHasCurrentCapturePolicy(parser.activeRepoContextId, [latest.id]), true);
      assert.notEqual(parser.activeRepoContextId, previousState.activeRepoContextId);
      assert.equal(parser.tokenCountIndex, previousState.tokenCountIndex + index + 1);
      assert.equal(parser.model, previousState.model);
      assert.deepEqual(parser.previous, { input: 19 * (index + 2), cachedInput: 0, output: 2 * (index + 2), reasoningOutput: 0 });
      assert.equal(latest.id, deterministicEventId(["codex-rollout", sessionId, String(previousState.tokenCountIndex + index + 1)]));
      priorOffset = checkpoint.offset;
      assert.equal(latest.repo, null);
      assert.equal(latest.branch, null);
      assert.equal(latest.head, null);
      assert.equal(latest.input, 19);
      assert.equal(latest.output, 2);
      assert.deepEqual(snapshot(), before);
      continuations.push({ offsetMs, newUsageRows: scan.eventsAppended, resolverRequests: newRequests.length,
        homeLinkageAbsent: true, inputTokens: 19, outputTokens: 2 });
      // Reopen again to prove a further restart does not end the reuse.
      tailer.close(); buffer.close();
      buffer = new LocalEventBuffer(ledger, options);
      tailer = new RolloutTailer(buffer, sessions, () => [], io);
    }
    fakeNow += 60_000;
    fs.appendFileSync(rollout, JSON.stringify({ timestamp: new Date(fakeNow).toISOString(),
      type: "turn_context", payload: { model: "gpt-6.1-sol", cwd: chat } }) + "\n" +
      JSON.stringify(count(95, 10)) + "\n");
    const freshScan = await tailer.scan({ scope: "full", now: new Date(fakeNow) });
    assert.equal(freshScan.eventsAppended, 1);
    const freshRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
    assert.equal(freshRequests.length, 1);
    const freshResults = resolveRepoContextRequests(freshRequests);
    assert.equal(freshResults[0]?.repoHash, null);
    assert.equal(buffer.applyRepoContextResults(freshResults).rowsFilled, 0);
    assert.deepEqual(snapshot(), before);
    const last = buffer.database.prepare(`select repo_hash as repo, branch_hash as branch, head_sha as head,
      input_tokens as input, output_tokens as output from buffered_events order by observed_at desc limit 1`).get();
    assert.deepEqual(last, { repo: null, branch: null, head: null, input: 19, output: 2 });
    const leased = buffer.delivery.lease({ now: new Date(fakeNow) });
    assert.equal(leased.locallyDead, 0);
    assert.equal(leased.items.length, 5);
    const projects = leased.items.map(item => item.envelope.event.projectKey);
    assert.equal(projects.filter(p => p === remoteLinkageHash(remote)).length, 1);
    assert.equal(projects.filter(p => p === undefined).length, 4);
    for (const delivery of leased.items.filter(item => item.deliveryId !== oldId)) {
      assert.equal(delivery.envelope.event.projectKey, undefined);
      assert.equal(delivery.envelope.event.metadata.branchHash, undefined);
      assert.equal(delivery.envelope.event.metadata.headSha, undefined);
      assert.equal(delivery.envelope.event.metadata.git, undefined);
      assert.equal(delivery.envelope.event.inputTokens, 19);
      assert.equal(delivery.envelope.event.outputTokens, 2);
      assert.equal(delivery.envelope.event.metadata.repoContextPolicyGeneration, undefined);
    }
    console.log(JSON.stringify({ proof: "independent-real-codex-tailer-upgrade", status: "PASS",
      releasedBaseline: { commit: released, version: "0.7.51",
        tailerSha256: sha(fs.readFileSync(path.join(oldTree, "packages/collector-cli/src/rollout-tailer.ts"))) },
      continuations, capturedRowAndSealedEnvelopeAndSavedResultsByteIdentical: true,
      newTurnWithCwd: { freshLookup: true, linkageAbsent: true, inputTokens: 19, outputTokens: 2 },
      leased: { total: 5, homeProject: 1, noProject: 4, locallyDead: 0 },
      legacyBindingMigratedAtFirstNewCapture: true, observationSpanMs: 172_800_000 }, null, 2));
  } finally {
    tailer?.close(); buffer?.close(); os.homedir = oldHome;
    if (registered) execFileSync("git", ["worktree", "remove", "--force", oldTree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error instanceof Error ? error.message : "tailer fixture failed"); process.exitCode = 1; });
