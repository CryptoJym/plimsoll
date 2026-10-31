import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";
import { GrokUsageTailer, GROK_USAGE_LIMITS } from "../../packages/collector-cli/src/grok-usage-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { CaptureWorkBudget, AUTOMATIC_CAPTURE_LIMITS } from "../../packages/collector-cli/src/capture-work-budget";
import { REPO_CONTEXT_CAPTURE_POLICY_GENERATION, resolveRepoContextRequests } from "../../packages/collector-cli/src/repo-context";
import { remoteLinkageHash } from "../../packages/shared/src/index";
import { grokUsageDocument, type FixtureTurn } from "./grok-usage-fixture";

/** Invalidate r2's wrong positive results as well as the released v1 results.
 * Grok repeats an identical occurrence, exercising the HMAC generation too. */
export async function runR2DirectoryCaptureFixture() {
  const currentRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "r2-directory-upgrade-"));
  const tree = path.join(root, "r2-reader");
  const oldHome = os.homedir;
  let registered = false;
  const beforeCommit = "22461f390851b8dcbde4d0134f8dc09d98eefa0d";
  try {
    execFileSync("git", ["worktree", "add", "--quiet", "--detach", tree, beforeCommit]);
    registered = true;
    fs.symlinkSync(path.join(currentRoot, "node_modules"), path.join(tree, "node_modules"), "dir");
    const load = (file: string) => import(pathToFileURL(path.join(tree, "packages/collector-cli/src", file + ".ts")).href);
    const previousBuffer = await load("buffer") as typeof import("../../packages/collector-cli/src/buffer");
    const previousCodex = await load("rollout-tailer") as typeof import("../../packages/collector-cli/src/rollout-tailer");
    const previousClaude = await load("transcript-tailer") as typeof import("../../packages/collector-cli/src/transcript-tailer");
    const previousGrok = await load("grok-usage-tailer") as typeof import("../../packages/collector-cli/src/grok-usage-tailer");
    const previousContext = await load("repo-context") as typeof import("../../packages/collector-cli/src/repo-context");
    assert.equal(previousContext.REPO_CONTEXT_CAPTURE_POLICY_GENERATION, 2);
    assert.ok(REPO_CONTEXT_CAPTURE_POLICY_GENERATION > 2);
    const home = path.join(root, "home");
    fs.mkdirSync(home, { mode: 0o700 });
    os.homedir = () => home;
    const git = (directory: string, args: string[]) => execFileSync("git", ["-C", directory, ...args],
      { stdio: "pipe", timeout: 10_000 });
    git(home, ["init", "-q"]);
    git(home, ["config", "user.name", "James Brady"]);
    git(home, ["config", "user.email", "131711520+CryptoJym@users.noreply.github.com"]);
    git(home, ["commit", "-q", "--allow-empty", "-m", "fixture"]);
    const remote = "https://github.com/CryptoJym/plimsoll.git";
    git(home, ["remote", "add", "origin", remote]);
    const checkout = path.join(home, "directory-worktree");
    git(home, ["worktree", "add", "--quiet", "--detach", checkout, "HEAD"]);
    const dotGit = path.join(checkout, ".git");
    const admin = fs.readFileSync(dotGit, "utf8").trim().replace(/^gitdir: /, "");
    fs.unlinkSync(dotGit); fs.renameSync(admin, dotGit);
    fs.writeFileSync(path.join(dotGit, "commondir"), path.join(home, ".git") + "\n");
    fs.copyFileSync(path.join(home, ".git/config"), path.join(dotGit, "config"));
    assert.equal(fs.realpathSync(git(checkout, ["rev-parse", "--git-common-dir"]).toString().trim()),
      fs.realpathSync(path.join(home, ".git")));
    const observations = [];
    const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
    for (const kind of ["claude", "codex", "grok"] as const) {
      let now = baseMs;
      const session = "019f8000-0000-7000-8000-000000000480";
      const directory = path.join(root, kind);
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      const sessions = path.join(directory, "sessions");
      const file = kind === "claude" ? path.join(sessions, "project", session + ".jsonl")
        : path.join(sessions, "2026/10/05", `rollout-proof-${session}.jsonl`);
      const grokHome = path.join(directory, "grok");
      const usageFile = path.join(grokHome, "sessions", encodeURIComponent(checkout), session, "usage.json");
      const turns: FixtureTurn[] = [];
      const record = (count: number, freshCwd: boolean) => {
        if (kind === "grok") {
          turns.push({ turnNumber: count, endedAt: new Date(now).toISOString(), models: [{ model: "grok-4.7-build",
            input: 19, output: 2, cachedRead: 0, cacheCreation: 0, reasoning: 0, modelCalls: 1, costTicks: 10 }] });
          fs.mkdirSync(path.dirname(usageFile), { recursive: true, mode: 0o700 });
          fs.writeFileSync(usageFile, JSON.stringify(grokUsageDocument({ sessionId: session,
            updatedAt: new Date(now).toISOString(), shape: "modern", turns })));
          return;
        }
        fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
        const countLine = { type: "event_msg", timestamp: new Date(now).toISOString(), payload: { type: "token_count",
          info: { total_token_usage: { input_tokens: count * 19, output_tokens: count * 2,
            cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: count * 21 } } } };
        const lines = kind === "claude" ? [{ type: "assistant", sessionId: session, timestamp: new Date(now).toISOString(),
          ...(freshCwd ? { cwd: checkout } : {}), message: { id: "growing-message", model: "claude-sonnet-4-20250514",
            usage: { input_tokens: count * 19, output_tokens: count * 2, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } } }]
          : [...(count === 1 ? [{ type: "session_meta", timestamp: new Date(now).toISOString(),
            payload: { id: session, cwd: checkout } }, { ...countLine, payload: { type: "token_count", info: {
              total_token_usage: { input_tokens: 0, output_tokens: 0, cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: 0 } } } }] : []),
          ...(freshCwd ? [{ type: "turn_context", timestamp: new Date(now).toISOString(), payload: { model: "gpt-6.1-sol", cwd: checkout } }] : []), countLine];
        fs.appendFileSync(file, lines.map(line => JSON.stringify(line)).join("\n") + "\n");
      };
      const options = { enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(now) } };
      const ledger = path.join(directory, "ledger.sqlite");
      let buffer = new previousBuffer.LocalEventBuffer(ledger, options);
      const makeTailer = (legacy: boolean) => kind === "claude"
        ? new (legacy ? previousClaude.TranscriptTailer : TranscriptTailer)(buffer, sessions, { ...DEFAULT_JSONL_TAILER_IO, now: () => now })
        : kind === "codex" ? new (legacy ? previousCodex.RolloutTailer : RolloutTailer)(buffer, sessions, () => [], { ...DEFAULT_JSONL_TAILER_IO, now: () => now })
        : new (legacy ? previousGrok.GrokUsageTailer : GrokUsageTailer)(buffer, grokHome, GROK_USAGE_LIMITS, () => now);
      let tailer = makeTailer(true);
      const scan = () => kind === "grok" ? (tailer as GrokUsageTailer).scan({ now: new Date(now),
        budget: new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 10_000 }) })
        : (tailer as RolloutTailer | TranscriptTailer).scan({ scope: "full", now: new Date(now) });
      try {
        record(1, true);
        assert.equal((await scan()).eventsAppended, 1);
        const oldRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
        assert.equal(oldRequests.length, 1);
        const oldContextId = oldRequests[0]!.contextId;
        const oldResults = previousContext.resolveRepoContextRequests(oldRequests);
        assert.equal(oldResults[0]!.repoHash, remoteLinkageHash(remote));
        assert.equal(buffer.applyRepoContextResults(oldResults).rowsFilled, 1);
        const oldId = (buffer.database.prepare("select id from buffered_events").get() as { id: string }).id;
        assert.equal(buffer.delivery.lease({ now: new Date(now) }).items[0]!.envelope.event.projectKey, remoteLinkageHash(remote));
        const snapshot = () => ({ row: JSON.stringify(buffer.database.prepare("select * from buffered_events where id=?").get(oldId)),
          result: JSON.stringify(buffer.database.prepare("select * from repo_context_results where context_id=?").get(oldContextId)),
          seal: (buffer.database.prepare("select sealed_envelope_json as sealed from upload_outbox where delivery_id=?").get(oldId) as { sealed: string }).sealed });
        const original = snapshot();
        tailer.close(); buffer.close(); buffer = new LocalEventBuffer(ledger, options); tailer = makeTailer(false);
        for (const count of [2, 3]) {
          now = baseMs + (count - 1) * 121_001;
          record(count, count === 3); // Resume without cwd first, then re-observe it.
          const scanned = await scan();
          assert.equal(scanned.eventsAppended, 1);
          const requests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
          if (kind === "grok" || count === 3) {
            assert.equal(requests.length, 1);
            assert.notEqual(requests[0]!.contextId, oldContextId);
            assert.equal(buffer.applyRepoContextResults(resolveRepoContextRequests(requests)).rowsFilled, 0);
          } else assert.equal(requests.length, 0);
          const row = buffer.database.prepare(`select id, repo_hash as repo, branch_hash as branch, head_sha as head,
            input_tokens as input, output_tokens as output from buffered_events order by observed_at desc limit 1`).get() as
            { id: string; repo: string | null; branch: string | null; head: string | null; input: number; output: number };
          assert.deepEqual({ repo: row.repo, branch: row.branch, head: row.head }, { repo: null, branch: null, head: null });
          const delivery = buffer.delivery.lease({ now: new Date(now) }).items.find(item => item.deliveryId === row.id)!.envelope;
          assert.equal(delivery.event.projectKey, undefined); assert.equal(delivery.event.metadata.projectBasis, "unallocated");
          assert.equal(delivery.event.inputTokens, 19); assert.equal(delivery.event.outputTokens, 2);
          assert.ok(!JSON.stringify(delivery).includes("repoContextPolicyGeneration"));
          assert.deepEqual(snapshot(), original);
        }
        observations.push({ kind, oldPolicyGeneration: 2, newPolicyGeneration: REPO_CONTEXT_CAPTURE_POLICY_GENERATION,
          newRows: 2, inputPerRow: 19, outputPerRow: 2, oldRowSealAndResultUnchanged: true,
          resumedAndFreshOccurrenceEnvelopesUnallocated: true });
      } finally { tailer.close(); buffer.close(); }
    }
    return { beforeCommit, nativeDirectoryWorktree: true, observations };
  } finally {
    os.homedir = oldHome;
    if (registered) execFileSync("git", ["worktree", "remove", "--force", tree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}
