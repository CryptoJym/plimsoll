#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { resolveGitContext } from "../packages/collector-cli/src/git-context";
import { readBoundedRegularFile } from "../packages/collector-cli/src/safe-file-read";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { branchLinkageHash, remoteLinkageHash } from "../packages/shared/src/index";

const HEAD_SHA = "918424fd85571dc1368400ab06ca7540f44127e1";
const WORKTREE_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PACKED_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const REMOTE = "https://github.com/CryptoJym/plimsoll.git";
const REMOTE_LABEL = "github.com/cryptojym/plimsoll";
const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function write(file: string, content: string, mode = 0o600) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, content, { mode });
}

function makeFifo(file: string) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const made = spawnSync("mkfifo", [file], { stdio: "ignore" });
  assert.equal(made.status, 0, "mkfifo fixture creation failed");
}

function normalRepo(root: string, name: string, branch = "main", sha = HEAD_SHA) {
  const repo = path.join(root, name);
  const git = path.join(repo, ".git");
  write(path.join(git, "HEAD"), `ref: refs/heads/${branch}\n`);
  write(path.join(git, "refs", "heads", branch), `${sha}\n`);
  write(path.join(git, "config"), `[remote "origin"]\n\turl = ${REMOTE}\n`);
  return { repo, git };
}

function withHardDeadline<T>(promise: Promise<T>, milliseconds: number, reason: string) {
  let timer: NodeJS.Timeout | undefined;
  return Promise.race([
    promise,
    new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(reason)), milliseconds);
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

async function closeChild(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
  child.kill("SIGKILL");
  await withHardDeadline(closed, 2_000, "blocked child did not terminate");
}

async function proveOldReadPrimitiveBlocks(fifo: string) {
  const child = spawn(
    process.execPath,
    [
      "-e",
      'const fs=require("node:fs"); if(process.send)process.send("ready"); fs.readFileSync(process.argv[1],"utf8"); if(process.send)process.send("returned");',
      fifo,
    ],
    { stdio: ["ignore", "ignore", "ignore", "ipc"] },
  );
  let returned = false;
  child.on("message", (message) => {
    if (message === "returned") returned = true;
  });
  try {
    await withHardDeadline(
      new Promise<void>((resolve, reject) => {
        child.on("message", (message) => {
          if (message === "ready") resolve();
        });
        child.once("close", () => reject(new Error("read child exited before FIFO open")));
      }),
      2_000,
      "read child did not become ready",
    );
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
    check(
      "real_fifo_blocks_the_previous_readfilesync_primitive",
      child.exitCode === null && child.signalCode === null && !returned,
      { realFifo: true, observationMs: 250, blocked: child.exitCode === null && !returned },
    );
  } finally {
    await closeChild(child);
  }
}

function proveValidRepositories(root: string) {
  const normal = normalRepo(root, "normal");
  const normalContext = resolveGitContext(normal.repo);
  check(
    "normal_repository_keeps_remote_branch_and_head_attribution",
    normalContext?.remoteUrlHash === remoteLinkageHash(REMOTE) &&
      normalContext?.remoteLabel === REMOTE_LABEL &&
      normalContext?.branchHash === branchLinkageHash("main") &&
      normalContext?.headSha === HEAD_SHA &&
      !normalContext?.isWorktree,
    {
      remote: normalContext?.remoteUrlHash === remoteLinkageHash(REMOTE),
      branch: normalContext?.branchHash === branchLinkageHash("main"),
      head: normalContext?.headSha === HEAD_SHA,
    },
  );

  const packed = normalRepo(root, "packed", "packed-proof", PACKED_SHA);
  fs.unlinkSync(path.join(packed.git, "refs", "heads", "packed-proof"));
  write(path.join(packed.git, "packed-refs"), `${PACKED_SHA} refs/heads/packed-proof\n`);
  const packedContext = resolveGitContext(packed.repo);
  check(
    "packed_reference_keeps_branch_and_head_attribution",
    packedContext?.branchHash === branchLinkageHash("packed-proof") &&
      packedContext?.headSha === PACKED_SHA,
    {
      branch: packedContext?.branchHash === branchLinkageHash("packed-proof"),
      head: packedContext?.headSha === PACKED_SHA,
    },
  );

  const commonGit = path.join(root, "common", ".git");
  const worktreeGit = path.join(commonGit, "worktrees", "bounded-proof");
  const worktree = path.join(root, "worktree");
  fs.mkdirSync(worktree, { recursive: true, mode: 0o700 });
  write(path.join(worktree, ".git"), `gitdir: ${worktreeGit}\n`);
  write(path.join(worktreeGit, "commondir"), "../..\n");
  write(path.join(worktreeGit, "HEAD"), "ref: refs/heads/worktree-proof\n");
  write(path.join(commonGit, "refs", "heads", "worktree-proof"), `${WORKTREE_SHA}\n`);
  write(path.join(commonGit, "config"), `[remote "origin"]\n\turl = ${REMOTE}\n`);
  const worktreeContext = resolveGitContext(worktree);
  check(
    "linked_worktree_keeps_common_config_and_ref_attribution",
    worktreeContext?.isWorktree === true &&
      worktreeContext.remoteUrlHash === remoteLinkageHash(REMOTE) &&
      worktreeContext.branchHash === branchLinkageHash("worktree-proof") &&
      worktreeContext.headSha === WORKTREE_SHA,
    {
      worktree: worktreeContext?.isWorktree === true,
      remote: worktreeContext?.remoteUrlHash === remoteLinkageHash(REMOTE),
      branch: worktreeContext?.branchHash === branchLinkageHash("worktree-proof"),
      head: worktreeContext?.headSha === WORKTREE_SHA,
    },
  );

  const groupWritable = normalRepo(root, "group-writable");
  fs.chmodSync(path.join(groupWritable.git, "config"), 0o660);
  const groupWritableContext = resolveGitContext(groupWritable.repo);
  check(
    "group_shared_regular_metadata_keeps_attribution",
    groupWritableContext?.remoteUrlHash === remoteLinkageHash(REMOTE) &&
      groupWritableContext?.headSha === HEAD_SHA,
    {
      remote: groupWritableContext?.remoteUrlHash === remoteLinkageHash(REMOTE),
      head: groupWritableContext?.headSha === HEAD_SHA,
    },
  );

  const hardlinked = normalRepo(root, "hardlinked-config");
  const sharedConfig = path.join(root, "shared-regular-config");
  write(sharedConfig, `[remote "origin"]\n\turl = ${REMOTE}\n`);
  fs.unlinkSync(path.join(hardlinked.git, "config"));
  fs.linkSync(sharedConfig, path.join(hardlinked.git, "config"));
  const hardlinkedContext = resolveGitContext(hardlinked.repo);
  check(
    "stable_regular_hardlink_keeps_attribution",
    fs.statSync(sharedConfig).nlink === 2 &&
      hardlinkedContext?.remoteUrlHash === remoteLinkageHash(REMOTE) &&
      hardlinkedContext?.headSha === HEAD_SHA,
    {
      linkCount: fs.statSync(sharedConfig).nlink,
      remote: hardlinkedContext?.remoteUrlHash === remoteLinkageHash(REMOTE),
      head: hardlinkedContext?.headSha === HEAD_SHA,
    },
  );
}

function expectUnsafeContext(name: string, repo: string, startedAt: number) {
  const context = resolveGitContext(repo);
  const elapsedMs = performance.now() - startedAt;
  check(name, context === undefined && elapsedMs < 250, {
    failedClosed: context === undefined,
    latencyBudgetMs: 250,
    withinBudget: elapsedMs < 250,
  });
}

async function proveUnsafeMetadata(root: string) {
  const fifoConfig = normalRepo(root, "fifo-config");
  fs.unlinkSync(path.join(fifoConfig.git, "config"));
  makeFifo(path.join(fifoConfig.git, "config"));
  await proveOldReadPrimitiveBlocks(path.join(fifoConfig.git, "config"));
  expectUnsafeContext(
    "fifo_config_fails_closed_without_blocking",
    fifoConfig.repo,
    performance.now(),
  );
  const repeatStartedAt = performance.now();
  for (let index = 0; index < 1_000; index += 1) resolveGitContext(fifoConfig.repo);
  const repeatElapsedMs = performance.now() - repeatStartedAt;
  check(
    "unsafe_cwd_is_negatively_cached_without_busy_loop",
    repeatElapsedMs < 100,
    { calls: 1_000, latencyBudgetMs: 100, withinBudget: repeatElapsedMs < 100 },
  );

  const fifoHead = normalRepo(root, "fifo-head");
  fs.unlinkSync(path.join(fifoHead.git, "HEAD"));
  makeFifo(path.join(fifoHead.git, "HEAD"));
  expectUnsafeContext("fifo_head_fails_closed_without_blocking", fifoHead.repo, performance.now());

  const fifoRef = normalRepo(root, "fifo-ref");
  fs.unlinkSync(path.join(fifoRef.git, "refs", "heads", "main"));
  makeFifo(path.join(fifoRef.git, "refs", "heads", "main"));
  expectUnsafeContext("fifo_direct_ref_fails_closed_without_blocking", fifoRef.repo, performance.now());

  const fifoPacked = normalRepo(root, "fifo-packed", "packed-fifo");
  fs.unlinkSync(path.join(fifoPacked.git, "refs", "heads", "packed-fifo"));
  makeFifo(path.join(fifoPacked.git, "packed-refs"));
  expectUnsafeContext(
    "fifo_packed_refs_fails_closed_without_blocking",
    fifoPacked.repo,
    performance.now(),
  );

  const fifoPointer = path.join(root, "fifo-pointer");
  fs.mkdirSync(fifoPointer, { recursive: true, mode: 0o700 });
  makeFifo(path.join(fifoPointer, ".git"));
  expectUnsafeContext(
    "fifo_worktree_pointer_fails_closed_without_blocking",
    fifoPointer,
    performance.now(),
  );

  const symlink = normalRepo(root, "symlink-config");
  const externalConfig = path.join(root, "external-config");
  write(externalConfig, `[remote "origin"]\n\turl = ${REMOTE}\n`);
  fs.unlinkSync(path.join(symlink.git, "config"));
  fs.symlinkSync(externalConfig, path.join(symlink.git, "config"));
  expectUnsafeContext("symlink_config_fails_closed", symlink.repo, performance.now());

  const oversized = normalRepo(root, "oversized-config");
  write(path.join(oversized.git, "config"), "x".repeat(256 * 1024 + 1));
  expectUnsafeContext("oversized_config_fails_closed", oversized.repo, performance.now());

  const writable = normalRepo(root, "world-writable-config");
  fs.chmodSync(path.join(writable.git, "config"), 0o666);
  expectUnsafeContext("world_writable_config_fails_closed", writable.repo, performance.now());

  const unavailable = path.join(root, "unavailable", "child");
  fs.mkdirSync(unavailable, { recursive: true, mode: 0o700 });
  fs.rmSync(path.join(root, "unavailable"), { recursive: true, force: true });
  expectUnsafeContext("unavailable_parent_returns_without_blocking", unavailable, performance.now());

  const socketPath = path.join(root, "metadata.socket");
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  try {
    const socketRead = readBoundedRegularFile(socketPath, 1024);
    check(
      "unix_socket_is_rejected_as_nonregular_before_read",
      socketRead.kind === "unsafe",
      { failedClosed: socketRead.kind === "unsafe" },
    );
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

function proveReplacementAndBounds(root: string) {
  const racedToFifo = path.join(root, "raced-to-fifo");
  write(racedToFifo, "safe\n");
  const fifoStartedAt = performance.now();
  const racedFifoRead = readBoundedRegularFile(racedToFifo, 1024, {
    afterPreflight: () => {
      fs.unlinkSync(racedToFifo);
      makeFifo(racedToFifo);
    },
  });
  const fifoElapsedMs = performance.now() - fifoStartedAt;
  check(
    "regular_to_fifo_open_race_is_nonblocking_and_fails_closed",
    racedFifoRead.kind === "unsafe" && fifoElapsedMs < 250,
    {
      failedClosed: racedFifoRead.kind === "unsafe",
      latencyBudgetMs: 250,
      withinBudget: fifoElapsedMs < 250,
    },
  );

  const replacedPath = path.join(root, "replaced-after-open");
  const heldPath = path.join(root, "opened-generation");
  write(replacedPath, "original\n");
  const replacedRead = readBoundedRegularFile(replacedPath, 1024, {
    afterOpen: () => {
      fs.renameSync(replacedPath, heldPath);
      write(replacedPath, "replacement\n");
    },
  });
  check(
    "path_replacement_after_descriptor_open_fails_identity_check",
    replacedRead.kind === "unsafe",
    { failedClosed: replacedRead.kind === "unsafe" },
  );

  const mutatedPath = path.join(root, "mutated-during-read");
  write(mutatedPath, "a".repeat(32 * 1024));
  const mutatedRead = readBoundedRegularFile(mutatedPath, 64 * 1024, {
    afterFirstChunk: () => write(mutatedPath, "b".repeat(32 * 1024)),
  });
  check(
    "same_inode_mutation_during_read_fails_descriptor_identity_check",
    mutatedRead.kind === "unsafe",
    { failedClosed: mutatedRead.kind === "unsafe" },
  );

  const growingPath = path.join(root, "growing-past-bound");
  write(growingPath, "c".repeat(1024));
  const growingRead = readBoundedRegularFile(growingPath, 1024, {
    afterOpen: () => fs.appendFileSync(growingPath, "x"),
  });
  check(
    "limit_plus_one_chunked_read_rejects_growth_past_bound",
    growingRead.kind === "unsafe",
    { failedClosed: growingRead.kind === "unsafe", limitBytes: 1024 },
  );
}

function percentile(values: number[], quantile: number) {
  const ordered = [...values].sort((left, right) => left - right);
  return ordered[Math.min(ordered.length - 1, Math.floor(ordered.length * quantile))] ?? Infinity;
}

async function proveMaintenanceTailerLatency(root: string) {
  const maintenanceRoot = path.join(root, "maintenance");
  const codexRoot = path.join(maintenanceRoot, "codex");
  const day = new Date();
  const [year, month, date] = day.toISOString().slice(0, 10).split("-");
  const rolloutDir = path.join(codexRoot, year!, month!, date!);
  const cwdFixtures: string[] = [];
  for (let index = 0; index < 192; index += 1) {
    const fixture = normalRepo(maintenanceRoot, `unsafe-${String(index).padStart(3, "0")}`);
    fs.unlinkSync(path.join(fixture.git, "config"));
    makeFifo(path.join(fixture.git, "config"));
    cwdFixtures.push(fixture.repo);
  }
  const sessionId = "019f8000-0000-7000-8000-000000000147";
  const lines = [
    JSON.stringify({
      timestamp: day.toISOString(),
      type: "session_meta",
      payload: { id: sessionId, cwd: cwdFixtures[0] },
    }),
    ...cwdFixtures.map((cwd) => JSON.stringify({
      timestamp: day.toISOString(),
      type: "turn_context",
      payload: { model: "gpt-5.5", cwd },
    })),
    JSON.stringify({
      timestamp: day.toISOString(),
      type: "event_msg",
      payload: {
        type: "token_count",
        info: {
          total_token_usage: {
            input_tokens: 10,
            cached_input_tokens: 2,
            output_tokens: 3,
            reasoning_output_tokens: 0,
            total_tokens: 13,
          },
        },
      },
    }),
  ];
  write(
    path.join(rolloutDir, `rollout-proof-${sessionId}.jsonl`),
    `${lines.join("\n")}\n`,
  );

  const buffer = new LocalEventBuffer(path.join(maintenanceRoot, "ledger.sqlite"));
  const tailer = new RolloutTailer(buffer, codexRoot, () => []);
  const heartbeatDelays: number[] = [];
  let expectedAt = performance.now() + 5;
  const heartbeat = setInterval(() => {
    const now = performance.now();
    heartbeatDelays.push(Math.max(0, now - expectedAt));
    expectedAt = now + 5;
  }, 5);
  try {
    const startedAt = performance.now();
    const result = await tailer.scan({ scope: "full", now: day });
    const elapsedMs = performance.now() - startedAt;
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    const p95Ms = percentile(heartbeatDelays, 0.95);
    const maxMs = Math.max(...heartbeatDelays);
    const payloads = buffer.database
      .prepare(`select payload_json as payload from buffered_events`)
      .all() as Array<{ payload: string }>;
    const serialized = JSON.stringify({ result, payloads });
    check(
      "production_rollout_tailer_keeps_heartbeat_bounded_across_unsafe_recorded_cwds",
      result.eventsAppended === 1 &&
        result.cooperativeYields >= 2 &&
        p95Ms < 100 &&
        maxMs < 250 &&
        elapsedMs < 1_000 &&
        !serialized.includes(root) &&
        !serialized.includes(REMOTE),
      {
        unsafeCwds: cwdFixtures.length,
        eventsAppended: result.eventsAppended,
        cooperativeYields: result.cooperativeYields,
        p95BudgetMs: 100,
        p95WithinBudget: p95Ms < 100,
        maxBudgetMs: 250,
        maxWithinBudget: maxMs < 250,
        scanBudgetMs: 1_000,
        scanWithinBudget: elapsedMs < 1_000,
        pathAndContentFree: !serialized.includes(root) && !serialized.includes(REMOTE),
      },
    );
  } finally {
    clearInterval(heartbeat);
    tailer.close();
    buffer.close();
  }
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-git-context-proof-"));
  try {
    proveValidRepositories(root);
    await proveUnsafeMetadata(root);
    proveReplacementAndBounds(root);
    await proveMaintenanceTailerLatency(root);
    const serializedChecks = JSON.stringify(checks);
    check(
      "proof_receipt_contains_no_fixture_path_or_metadata_content",
      !serializedChecks.includes(root) && !serializedChecks.includes(REMOTE),
      { pathAndContentFree: !serializedChecks.includes(root) && !serializedChecks.includes(REMOTE) },
    );
    console.log(JSON.stringify({ status: "passed", checks }, null, 2));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : "git context proof failed");
  process.exitCode = 1;
});
