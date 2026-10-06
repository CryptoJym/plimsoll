#!/usr/bin/env node

import assert from "node:assert/strict";
import { execFileSync, spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  gitContextCacheNonDigestKeyCountForProof,
  gitContextCacheSizeForProof,
  resolveGitContext,
  resolveGitContextUncached,
} from "../packages/collector-cli/src/git-context";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import {
  attachRepoContextId,
  attachRepoContextSidecar,
  resolveRepoContextRequests,
} from "../packages/collector-cli/src/repo-context";
import { readBoundedRegularFile } from "../packages/collector-cli/src/safe-file-read";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { homeCodexCaptureBinding, homeCodexNativeRequest } from "./lib/home-codex-capture-fixture";
import {
  aiInteractionEventSchema,
  branchLinkageHash,
  remoteLinkageHash,
  type GitLinkageContext,
} from "../packages/shared/src/index";

const patchableFs = fs as { statSync: typeof fs.statSync; lstatSync: typeof fs.lstatSync };

const HEAD_SHA = "918424fd85571dc1368400ab06ca7540f44127e1";
const WORKTREE_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PACKED_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const REMOTE = "https://github.com/CryptoJym/plimsoll.git";
const REMOTE_LABEL = "github.com/cryptojym/plimsoll";
const REAL_REMOTE = "https://github.com/Proof-Owner/Real-Project.git";
const NESTED_REMOTE = "https://github.com/Proof-Owner/Nested-Project.git";
const LEGACY_COMMIT = "71d6ff27f0d39aa31d188c9bcc31d37bf188c384";
const LEGACY_RESOLVER_SHA256 = "90455ab531d63287c9716c485f709856648170a9c9bfecc0bdcf6c5612943c5f";
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

function normalRepo(root: string, name: string, branch = "main", sha = HEAD_SHA, remote = REMOTE) {
  const repo = path.join(root, name);
  const git = path.join(repo, ".git");
  write(path.join(git, "HEAD"), `ref: refs/heads/${branch}\n`);
  write(path.join(git, "refs", "heads", branch), `${sha}\n`);
  write(path.join(git, "config"), `[remote "origin"]\n\turl = ${remote}\n`);
  return { repo, git };
}

function withHome<T>(home: string, run: () => T): T {
  const originalHome = os.homedir;
  os.homedir = () => home;
  try { return run(); } finally { os.homedir = originalHome; }
}

function fullLinkage(context: GitLinkageContext | undefined, branch = "main", sha = HEAD_SHA, remote = REMOTE) {
  return context !== undefined && context.remoteUrlHash === remoteLinkageHash(remote) &&
    context.branchHash === branchLinkageHash(branch) && context.headSha === sha;
}

function proveHomeBoundaries(root: string) {
  const parent = normalRepo(root, "home-boundary-parent");
  const home = normalRepo(parent.repo, "home");
  withHome(home.repo, () => {
    check("home_git_itself_sends_no_remote_branch_or_head",
      resolveGitContext(home.repo) === undefined && resolveGitContextUncached(home.repo) === undefined,
      { cachedAndUncached: true, linkageAbsent: true });
    for (const depth of [1, 4, 30]) {
      const chat = path.join(home.repo, ...Array.from({ length: depth }, (_, i) => `chat-${i}`));
      fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
      check(`home_only_chat_at_depth_${depth}_sends_no_linkage`,
        resolveGitContext(chat) === undefined && resolveGitContextUncached(chat) === undefined,
        { depth, cachedAndUncached: true, linkageAbsent: true });
    }

    const ordinary = normalRepo(home.repo, "ordinary", "main", HEAD_SHA, REAL_REMOTE);
    const inner = path.join(ordinary.repo, "src", "deep");
    fs.mkdirSync(inner, { recursive: true, mode: 0o700 });
    check("real_repository_below_git_home_keeps_own_linkage",
      fullLinkage(resolveGitContext(inner), "main", HEAD_SHA, REAL_REMOTE),
      { remote: true, branch: true, head: true, distinctFromHomeRemote: true });
    const nested = normalRepo(ordinary.repo, "nested", "nested", PACKED_SHA, NESTED_REMOTE);
    check("nested_repository_below_git_home_uses_nearest_repository",
      fullLinkage(resolveGitContext(nested.repo), "nested", PACKED_SHA, NESTED_REMOTE),
      { nearestBranchAndHead: true, distinctFromParentAndHomeRemotes: true });
    const twoLevels = normalRepo(home.repo, "projects/real", "two-levels", WORKTREE_SHA, REAL_REMOTE);
    check("repository_two_levels_below_git_home_keeps_linkage",
      fullLinkage(resolveGitContext(twoLevels.repo), "two-levels", WORKTREE_SHA, REAL_REMOTE),
      { levels: 2, distinctFromHomeRemote: true });

    const worktree = path.join(home.repo, "lanes", "worktree");
    const worktreeGit = path.join(ordinary.git, "worktrees", "home-proof");
    write(path.join(worktree, ".git"), `gitdir: ${path.relative(worktree, worktreeGit)}\n`);
    write(path.join(worktreeGit, "commondir"), "../..\n");
    write(path.join(worktreeGit, "HEAD"), "ref: refs/heads/home-worktree\n");
    write(path.join(ordinary.git, "refs", "heads", "home-worktree"), `${WORKTREE_SHA}\n`);
    const worktreeContext = resolveGitContext(worktree);
    check("worktree_below_git_home_keeps_pointer_common_config_and_ref",
      worktreeContext?.isWorktree && fullLinkage(worktreeContext, "home-worktree", WORKTREE_SHA, REAL_REMOTE),
      { relativePointer: true, commonConfigAndRef: true, distinctFromHomeRemote: true });

    const alias = path.join(root, "home-alias");
    fs.symlinkSync(home.repo, alias, "dir");
    check("symlinked_cwd_at_home_and_below_home_cannot_bypass_boundary",
      resolveGitContext(alias) === undefined &&
      resolveGitContext(path.join(alias, "chat-0")) === undefined,
      { homeAndChild: true });
    withHome(alias, () => {
      check("symlinked_running_user_home_resolves_to_same_boundary",
        resolveGitContext(home.repo) === undefined &&
        fullLinkage(resolveGitContext(ordinary.repo), "main", HEAD_SHA, REAL_REMOTE),
        { homeExcluded: true, realRepositoryLinked: true });
    });
    const caseAlias = path.join(parent.repo, "HOME");
    if (fs.existsSync(caseAlias)) {
      check("case_alias_of_home_and_home_only_chat_sends_no_linkage",
        resolveGitContext(caseAlias) === undefined &&
        resolveGitContextUncached(path.join(caseAlias, "chat-0")) === undefined &&
        withHome(caseAlias, () => resolveGitContextUncached(home.repo)) === undefined &&
        fullLinkage(resolveGitContext(path.join(caseAlias, "ordinary")), "main", HEAD_SHA, REAL_REMOTE),
        { caseInsensitiveVolume: true, directoryIdentityUsed: true, realRepositoryLinked: true });
    } else {
      check("case_sensitive_volume_has_no_alias_of_home",
        !fs.existsSync(caseAlias), { caseInsensitiveVolume: false });
    }

    // Remove home's .git to prove that discovery cannot continue into the
    // parent repository. Observe both forbidden metadata paths directly.
    fs.renameSync(home.git, path.join(home.repo, "saved-git"));
    const originalLstat = fs.lstatSync;
    let forbiddenReads = 0;
    patchableFs.lstatSync = ((file: fs.PathLike, options?: fs.StatOptions) => {
      if (String(file) === home.git || String(file) === parent.git) forbiddenReads += 1;
      return originalLstat(file, options);
    }) as typeof fs.lstatSync;
    try {
      check("walk_stops_before_home_metadata_and_never_reaches_parent_repository",
        resolveGitContextUncached(path.join(home.repo, "chat-0")) === undefined && forbiddenReads === 0,
        { forbiddenMetadataReads: forbiddenReads });
    } finally { patchableFs.lstatSync = originalLstat; }
    write(home.git, `gitdir: ${parent.git}\n`);
    check("home_git_file_pointer_is_excluded_before_following_it",
      resolveGitContextUncached(home.repo) === undefined &&
      resolveGitContextUncached(path.join(home.repo, "chat-0")) === undefined,
      { homeAndChild: true, pointerNotFollowed: true });
    fs.unlinkSync(home.git);
    fs.renameSync(path.join(home.repo, "saved-git"), home.git);

    // Changing the running-user home must also change the cache identity.
    check("cache_is_scoped_to_resolved_running_user_home",
      fullLinkage(resolveGitContext(ordinary.repo), "main", HEAD_SHA, REAL_REMOTE) &&
      withHome(ordinary.repo, () => resolveGitContext(ordinary.repo)) === undefined &&
      fullLinkage(resolveGitContext(ordinary.repo), "main", HEAD_SHA, REAL_REMOTE), { homesSeparated: true });
  });

  for (const kind of ["missing", "relative", "file", "throws"] as const) {
    const homeFile = path.join(root, "home-is-file");
    write(homeFile, "fixture\n");
    const originalHome = os.homedir;
    os.homedir = () => {
      if (kind === "throws") throw new Error("home resolution unavailable");
      return kind === "missing" ? path.join(root, "missing-home")
        : kind === "relative" ? "relative-home" : homeFile;
    };
    try {
      check(`unresolved_${kind}_home_fails_closed_even_for_real_repository`,
        resolveGitContext(parent.repo) === undefined && resolveGitContextUncached(parent.repo) === undefined,
        { cachedAndUncached: true, linkageAbsent: true });
    } finally { os.homedir = originalHome; }
  }
}

function proveDiskRootAndOtherVolume(root: string) {
  const fakeGit = normalRepo(root, "root-git-metadata");
  const child = path.join(root, "root-only-child", "deeper");
  fs.mkdirSync(child, { recursive: true, mode: 0o700 });
  const diskRoot = path.parse(fs.realpathSync(root)).root;
  const originalLstat = fs.lstatSync;
  let rootGitReads = 0;
  patchableFs.lstatSync = ((file: fs.PathLike, options?: fs.StatOptions) => {
    if (String(file) === path.join(diskRoot, ".git")) {
      rootGitReads += 1;
      return originalLstat(fakeGit.git, options);
    }
    if (path.basename(String(file)) === ".git") {
      throw Object.assign(new Error("fixture missing"), { code: "ENOENT" });
    }
    return originalLstat(file, options);
  }) as typeof fs.lstatSync;
  try {
    withHome(fakeGit.repo, () => {
      check("system_root_git_is_never_read_at_root_or_from_descendants",
        resolveGitContextUncached(diskRoot) === undefined &&
        resolveGitContextUncached(child) === undefined && rootGitReads === 0,
        { rootAndDescendant: true, syntheticRootGit: true, rootGitReads });
    });
  } finally { patchableFs.lstatSync = originalLstat; }

  // A mounted-volume namespace is injected onto real fixture metadata; no
  // file is created outside the disposable proof root.
  const volumeRepo = normalRepo(root, "volume-metadata", "main", HEAD_SHA, REAL_REMOTE);
  const mountedCwd = path.join(diskRoot, "Volumes", "git-context-proof", "project");
  const originalRealpath = fs.realpathSync;
  const originalOpen = fs.openSync;
  const originalStat = fs.statSync;
  const translate = (file: fs.PathLike) => {
    const value = String(file);
    return value === mountedCwd || value.startsWith(`${mountedCwd}${path.sep}`)
      ? path.join(volumeRepo.repo, path.relative(mountedCwd, value)) : file;
  };
  fs.realpathSync = Object.assign(((file: fs.PathLike) =>
    String(file) === mountedCwd ? mountedCwd : originalRealpath(translate(file))) as typeof fs.realpathSync,
  { native: originalRealpath.native });
  patchableFs.lstatSync = ((file: fs.PathLike, options?: fs.StatOptions) =>
    originalLstat(translate(file), options)) as typeof fs.lstatSync;
  patchableFs.statSync = ((file: fs.PathLike, options?: fs.StatOptions) =>
    originalStat(translate(file), options)) as typeof fs.statSync;
  fs.openSync = ((file: fs.PathLike, flags: string | number, mode?: fs.Mode) =>
    originalOpen(translate(file), flags, mode)) as typeof fs.openSync;
  try {
    check("repository_on_another_volume_keeps_linkage",
      fullLinkage(resolveGitContextUncached(mountedCwd), "main", HEAD_SHA, REAL_REMOTE),
      { simulatedMountedVolume: true, realBoundedMetadataReads: true });
  } finally {
    fs.realpathSync = originalRealpath;
    patchableFs.lstatSync = originalLstat;
    patchableFs.statSync = originalStat;
    fs.openSync = originalOpen;
  }
}

function proveCacheAndPrivacy(root: string) {
  const home = normalRepo(root, "cache-home");
  const repo = normalRepo(home.repo, "real");
  const empty = path.join(home.repo, "new-repo");
  fs.mkdirSync(empty, { mode: 0o700 });
  const originalNow = Date.now;
  const originalLog = console.log;
  const originalWarn = console.warn;
  const originalError = console.error;
  const messages: unknown[][] = [];
  const uncachedSizeBefore = gitContextCacheSizeForProof();
  withHome(home.repo, () => {
    resolveGitContextUncached(repo.repo);
    resolveGitContextUncached(empty);
  });
  check("uncached_real_and_home_only_lookups_retain_no_cache_entry",
    gitContextCacheSizeForProof() === uncachedSizeBefore, { cacheGrowth: 0 });
  let now = 1_000_000;
  Date.now = () => now;
  console.log = console.warn = console.error = (...args: unknown[]) => { messages.push(args); };
  try {
    withHome(home.repo, () => {
      const first = resolveGitContext(repo.repo);
      const absent = resolveGitContext(empty);
      write(path.join(repo.git, "refs", "heads", "main"), `${WORKTREE_SHA}\n`);
      normalRepo(home.repo, "new-repo", "new", PACKED_SHA);
      now += 29_999;
      check("thirty_second_cache_keeps_positive_and_negative_results_until_expiry",
        fullLinkage(first) && absent === undefined && fullLinkage(resolveGitContext(repo.repo)) &&
        resolveGitContext(empty) === undefined, { ttlMs: 30_000, elapsedMs: 29_999 });
      now += 1;
      check("thirty_second_cache_refreshes_positive_and_negative_results_at_expiry",
        fullLinkage(resolveGitContext(repo.repo), "main", WORKTREE_SHA) &&
        fullLinkage(resolveGitContext(empty), "new", PACKED_SHA), { ttlMs: 30_000, elapsedMs: 30_000 });
    });
  } finally {
    Date.now = originalNow;
    console.log = originalLog;
    console.warn = originalWarn;
    console.error = originalError;
  }
  check("compatibility_cache_retains_only_opaque_keys_and_resolver_logs_no_raw_paths",
    gitContextCacheNonDigestKeyCountForProof() === 0 && messages.length === 0,
    { nonOpaqueCacheKeys: gitContextCacheNonDigestKeyCountForProof(), logMessages: messages.length });
}

async function proveUpgradeSpanningCapture(root: string) {
  const resolverPath = "packages/collector-cli/src/git-context.ts";
  const bufferPath = "packages/collector-cli/src/buffer.ts";
  const legacySource = execFileSync("git", ["show", `${LEGACY_COMMIT}:${resolverPath}`],
    { encoding: "utf8", maxBuffer: 1024 * 1024 });
  const hash = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
  const legacyTree = path.join(root, "released-0.7.51");
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", legacyTree, LEGACY_COMMIT]);
  try {
    fs.symlinkSync(path.resolve("node_modules"), path.join(legacyTree, "node_modules"), "dir");
    check("upgrade_fixture_uses_pinned_0751_resolver_and_released_collector",
      hash(legacySource) === LEGACY_RESOLVER_SHA256 &&
      hash(fs.readFileSync(path.join(legacyTree, resolverPath))) === LEGACY_RESOLVER_SHA256 &&
      JSON.parse(fs.readFileSync(path.join(legacyTree, "packages/collector-cli/package.json"), "utf8")).version === "0.7.51",
      { legacyCommit: LEGACY_COMMIT, resolverSha256: LEGACY_RESOLVER_SHA256, legacyVersion: "0.7.51" });
    const legacy = await import(pathToFileURL(path.join(legacyTree, resolverPath)).href) as {
      resolveGitContext: typeof resolveGitContext;
      resolveGitContextUncached: typeof resolveGitContextUncached;
    };
    const previous = await import(pathToFileURL(path.join(legacyTree, bufferPath)).href) as {
      LocalEventBuffer: typeof LocalEventBuffer;
    };
    const previousContexts = await import(pathToFileURL(
      path.join(legacyTree, "packages/collector-cli/src/repo-context.ts"),
    ).href) as { attachRepoContextSidecar: typeof attachRepoContextSidecar; REPO_CONTEXT_RESOLVER_VERSION: string };
    const home = normalRepo(root, "studio0-upgrade-home");
    const chat = path.join(home.repo, "Documents", "Codex", "2026-10-05", "chat");
    fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
    const real = normalRepo(home.repo, "projects/real", "real-project", PACKED_SHA, REAL_REMOTE);
    const originalHome = os.homedir;
    os.homedir = () => home.repo;
    const now = new Date("2026-10-05T12:00:00.000Z");
    const options = {
      // buffer.ts:815,1092,2732,2750 supplies the actual native boundary.
      ...homeCodexCaptureBinding,
      enrollmentNow: () => now, delivery: { enabled: true, now: () => now },
    };
    const ledger = path.join(root, "home-upgrade.sqlite");
    let buffer: LocalEventBuffer | undefined;
    try {
      const oldContext = legacy.resolveGitContext(chat);
      assert.ok(oldContext);
      check("studio0_old_rule_primes_home_linked_cache_while_real_repo_has_own_linkage",
        fullLinkage(oldContext) &&
        fullLinkage(legacy.resolveGitContext(real.repo), "real-project", PACKED_SHA, REAL_REMOTE),
        { oldChatLinkedToHome: true, realRepositoryLinkedSeparately: true, distinctProjectRemotes: true });
      const event = (index: number, context?: GitLinkageContext) => aiInteractionEventSchema.parse({
        id: deterministicEventId(["home-upgrade", String(index)]),
        source: "codex", dataMode: "metadata", eventType: "assistant_response",
        observedAt: now.toISOString(), sessionId: "00000000-0000-4000-8000-000000000913",
        actorId: "sha256:0123456789abcdef", model: "gpt-6.1-sol", inputTokens: 19, outputTokens: 2,
        metadata: {
          // otlp.ts:570-635 produces the request facts through the real normalizer.
          ...homeCodexNativeRequest(deterministicEventId(["home-upgrade", String(index)]),
            "00000000-0000-4000-8000-000000000913", now.toISOString()),
          ...(context ? { git: {
            remoteUrlHash: context.remoteUrlHash, branchHash: context.branchHash, headSha: context.headSha,
          } } : {}),
        },
      });
      buffer = new previous.LocalEventBuffer(ledger, options);
      const acked = event(0, oldContext);
      assert.equal(buffer.append(acked), true);
      const firstLease = buffer.delivery.lease({ now });
      assert.deepEqual(firstLease.items.map(item => item.deliveryId), [acked.id]);
      assert.equal(buffer.delivery.acknowledge(firstLease.leaseId!, [acked.id], now).acknowledged, 1);

      const saved = event(1);
      assert.equal(previousContexts.attachRepoContextSidecar(saved, "legacy-home-context", chat), true);
      assert.equal(buffer.append(saved), true);
      const savedRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
      assert.equal(savedRequests.length, 1);
      const savedContextId = savedRequests[0]!.contextId;
      assert.equal(buffer.applyRepoContextResults([{
        contextId: savedContextId, repoHash: oldContext.remoteUrlHash!,
        branchHash: oldContext.branchHash!, headSha: oldContext.headSha!,
        resolvedAt: now.toISOString(), resolverVersion: previousContexts.REPO_CONTEXT_RESOLVER_VERSION,
      }]).resultsInserted, 1);
      const sealed = event(2, oldContext);
      assert.equal(buffer.append(sealed), true);
      const oldLease = buffer.delivery.lease({ now });
      assert.deepEqual(oldLease.items.map(item => item.deliveryId).sort(), [saved.id, sealed.id].sort());
      const unsealed = event(3, oldContext);
      assert.equal(buffer.append(unsealed), true);
      const pending = event(4);
      assert.equal(previousContexts.attachRepoContextSidecar(pending, "queued-before-upgrade", chat), true);
      assert.equal(buffer.append(pending), true);
      const preUpgradeDeferred = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
      assert.equal(preUpgradeDeferred.length, 1);
      const oldIds = [acked.id, saved.id, sealed.id, unsealed.id, pending.id].sort();
      const snapshot = () => ({
        rows: JSON.stringify(buffer!.database.prepare(
          "select * from buffered_events where id in (?,?,?,?,?) order by id",
        ).all(...oldIds)),
        outbox: JSON.stringify(buffer!.database.prepare(
          "select * from upload_outbox where delivery_id in (?,?,?,?,?) order by delivery_id",
        ).all(...oldIds)),
        receipts: JSON.stringify(buffer!.database.prepare(
          "select * from upload_receipts order by delivery_id",
        ).all()),
        savedResults: JSON.stringify(buffer!.database.prepare(
          "select * from repo_context_results order by context_id",
        ).all()),
      });
      const before = snapshot();
      buffer.close();
      buffer = new LocalEventBuffer(ledger, options); // collector restart at upgrade
      check("upgrade_restart_discards_old_thirty_second_cache_without_waiting_for_expiry",
        resolveGitContext(chat) === undefined && fullLinkage(legacy.resolveGitContext(chat)),
        { legacyCacheStillLinked: true, upgradedCacheHasNoLinkage: true, ttlMs: 30_000 });

      const fresh = event(5, resolveGitContext(chat));
      assert.equal(buffer.append(fresh), true);
      const reused = event(6);
      assert.equal(attachRepoContextId(reused, savedContextId), true);
      assert.equal(buffer.append(reused), true);
      const newOccurrence = event(7);
      assert.equal(attachRepoContextSidecar(newOccurrence, "fresh-after-upgrade", chat), true);
      assert.equal(buffer.append(newOccurrence), true);
      const newRequests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
      const results = resolveRepoContextRequests([...preUpgradeDeferred, ...newRequests]);
      const applied = buffer.applyRepoContextResults(results);
      const after = snapshot();
      check("upgrade_keeps_all_five_earlier_captured_rows_byte_identical",
        after.rows === before.rows, { capturedRows: 5, includesAckedSealedUnsealedAndPending: true });
      check("upgrade_keeps_earlier_sealed_and_unsealed_outbox_rows_byte_identical",
        after.outbox === before.outbox, { frozenEnvelopes: 2, pendingUnsealedRows: 2 });
      check("upgrade_keeps_acked_receipt_and_saved_linkage_result_byte_identical",
        after.receipts === before.receipts && after.savedResults === before.savedResults,
        { ackedReceipts: 1, savedLinkageResults: 1 });
      check("upgrade_deferred_home_lookups_return_null_without_filling_earlier_rows",
        results.length === 2 && results.every(result =>
          result.repoHash === null && result.branchHash === null && result.headSha === null) &&
        applied.unknownResults === 2 && applied.rowsFilled === 0,
        { queuedBeforeUpgrade: 1, queuedAfterUpgrade: 1, unknownResults: applied.unknownResults, rowsFilled: applied.rowsFilled });
      const read = buffer.database.prepare(
        "select repo_hash as repo, branch_hash as branch, head_sha as head, input_tokens as input, output_tokens as output from buffered_events where id = ?",
      );
      const noLinkage = { repo: null, branch: null, head: null, input: 19, output: 2 };
      check("upgrade_new_home_resolutions_keep_usage_and_omit_all_linkage",
        JSON.stringify(read.get(fresh.id)) === JSON.stringify(noLinkage) &&
        JSON.stringify(read.get(newOccurrence.id)) === JSON.stringify(noLinkage),
        { freshLookups: 2, inputTokensEach: 19, outputTokensEach: 2 });
      check("upgrade_later_event_reusing_legacy_context_keeps_usage_without_old_linkage",
        JSON.stringify(read.get(reused.id)) === JSON.stringify(noLinkage) &&
        JSON.stringify(read.get(newOccurrence.id)) === JSON.stringify(noLinkage),
        { reusedLegacyContextNotLinked: true, freshContextNotLinked: true });
      const retained = JSON.stringify({
        payloads: buffer.database.prepare("select payload_json from buffered_events").all(),
        results, receipt: applied, queue: buffer.repoContextQueueStatus(),
      });
      check("upgrade_payloads_results_and_queue_receipts_retain_no_raw_working_path",
        !retained.includes(home.repo) && !retained.includes(chat) && !retained.includes(root),
        { rawWorkingPathsAbsent: true });
    } finally {
      buffer?.close();
      os.homedir = originalHome;
    }
  } finally {
    execFileSync("git", ["worktree", "remove", "--force", legacyTree], { stdio: "ignore" });
  }
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
  // Keep the Unix-socket fixture below macOS's sockaddr_un path limit even
  // when CI places TMPDIR beneath a deeply nested disposable home.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "git-ctx-"));
  try {
    proveValidRepositories(root);
    proveHomeBoundaries(root);
    proveDiskRootAndOtherVolume(root);
    proveCacheAndPrivacy(root);
    await proveUpgradeSpanningCapture(root);
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
