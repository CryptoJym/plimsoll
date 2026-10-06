import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { resolveGitContext, resolveGitContextUncached, gitContextCacheSizeForProof,
  gitContextCacheNonDigestKeyCountForProof } from "../packages/collector-cli/src/git-context";
import { attachRepoContextSidecar, resolveRepoContextRequests } from "../packages/collector-cli/src/repo-context";
import { aiInteractionEventSchema, remoteLinkageHash } from "../packages/shared/src/index";

const patchableFs = fs as { statSync: typeof fs.statSync; lstatSync: typeof fs.lstatSync };

const root = fs.mkdtempSync(path.join(os.tmpdir(), "independent-home-"));
const originalHome = os.homedir;
const originalEnvHome = process.env.HOME;
const observations: Record<string, unknown> = {};
const remote = "https://github.com/CryptoJym/plimsoll.git";
const write = (file: string, value: string) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, value, { mode: 0o600 });
};
const makeRepo = (directory: string, url: string) => {
  write(path.join(directory, ".git/HEAD"), "ref: refs/heads/main\n");
  write(path.join(directory, ".git/refs/heads/main"), "a".repeat(40) + "\n");
  write(path.join(directory, ".git/config"), `[remote "origin"]\n url = ${url}\n`);
};
const git = (directory: string, args: string[]) => execFileSync("git", ["-C", directory, ...args],
  { stdio: "pipe", timeout: 10_000, env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" } });
let buffer: LocalEventBuffer | undefined;
try {
  const home = path.join(root, "running-home");
  fs.mkdirSync(home, { mode: 0o700 });
  process.env.HOME = home;
  os.homedir = () => home;
  git(home, ["init", "-q"]);
  git(home, ["config", "user.name", "Review Fixture"]);
  git(home, ["config", "user.email", "review@example.invalid"]);
  git(home, ["commit", "-q", "--allow-empty", "-m", "fixture"]);
  git(home, ["remote", "add", "origin", remote]);
  const lane = path.join(home, "lanes/home-worktree");
  git(home, ["worktree", "add", "--quiet", "--detach", lane, "HEAD"]);
  assert.equal(resolveGitContext(home), undefined);
  assert.equal(resolveGitContextUncached(home), undefined);
  const cached = resolveGitContext(lane);
  const uncached = resolveGitContextUncached(lane);
  assert.equal(cached, undefined);
  assert.equal(uncached, undefined);
  observations.homeWorktree = { desired: "no-linkage", actual: "no-linkage",
    cachedAndUncached: true, realGitWorktree: true, allLinkageAbsent: true };
  const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
  buffer = new LocalEventBuffer(path.join(root, "worktree.sqlite"), {
    enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(baseMs) },
  });
  const oldId = "00000000-0000-4000-8000-000000000460";
  assert.equal(buffer.append(aiInteractionEventSchema.parse({ id: oldId, source: "codex", dataMode: "metadata",
    eventType: "tool_use", sessionId: "00000000-0000-4000-8000-000000000462",
    observedAt: new Date(baseMs).toISOString(), metadata: { git: {
      remoteUrlHash: remoteLinkageHash(remote), headSha: "a".repeat(40),
    } } })), true);
  assert.equal(buffer.delivery.lease({ now: new Date(baseMs) }).items.length, 1);
  const archived = () => ({ row: JSON.stringify(buffer!.database.prepare("select * from buffered_events where id=?").get(oldId)),
    seal: (buffer!.database.prepare("select sealed_envelope_json as seal from upload_outbox where delivery_id=?")
      .get(oldId) as { seal: string }).seal });
  const before = archived();
  const event = aiInteractionEventSchema.parse({
    id: "00000000-0000-4000-8000-000000000461", source: "codex", dataMode: "metadata",
    eventType: "assistant_response", sessionId: "00000000-0000-4000-8000-000000000462",
    observedAt: new Date(baseMs + 120_000).toISOString(), model: "gpt-6.1-sol", inputTokens: 19, outputTokens: 2,
  });
  assert.equal(attachRepoContextSidecar(event, "new-home-worktree-event", lane), true);
  assert.equal(buffer.append(event), true);
  const requests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
  assert.equal(requests.length, 1);
  const resolved = resolveRepoContextRequests(requests);
  assert.equal(resolved[0]?.repoHash, null);
  assert.equal(buffer.applyRepoContextResults(resolved).rowsFilled, 0);
  const lease = buffer.delivery.lease({ now: new Date(baseMs + 122_000) });
  assert.equal(lease.items.length, 2);
  const delivery = lease.items.find(item => item.deliveryId === event.id)!;
  assert.ok(delivery);
  assert.equal(delivery.envelope.event.projectKey, undefined);
  assert.equal(delivery.envelope.event.metadata.git, undefined);
  assert.equal(delivery.envelope.event.metadata.branchHash, undefined);
  assert.equal(delivery.envelope.event.metadata.headSha, undefined);
  assert.equal(delivery.envelope.event.inputTokens, 19);
  assert.equal(delivery.envelope.event.outputTokens, 2);
  assert.deepEqual(archived(), before);
  observations.homeWorktreeDelivery = { newEventHasNoHomeProject: true,
    inputTokens: delivery.envelope.event.inputTokens,
    outputTokens: delivery.envelope.event.outputTokens, locallyDead: lease.locallyDead,
    earlierRowAndSealByteIdentical: true };
  buffer.close(); buffer = undefined;

  const alternate = path.join(root, "environment-home");
  fs.mkdirSync(alternate, { mode: 0o700 });
  const chat = path.join(home, "Documents/chat");
  fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
  process.env.HOME = alternate;
  assert.equal(resolveGitContextUncached(chat), undefined);
  observations.osHomeDisagreesWithEnv = { osHomeAuthoritative: true, homeOnlyLinkageAbsent: true };
  os.homedir = originalHome;
  assert.equal(os.homedir(), alternate);
  assert.equal(resolveGitContextUncached(chat)?.remoteUrlHash, remoteLinkageHash(remote));
  observations.environmentOverridesNativeOsHome = { osHomeEqualsOverriddenHOME: true,
    priorHomeIsTreatedAsOutsideSelectedHome: true, priorHomeRemoteStillResolved: true,
    specification: "boundary is os.homedir, not passwd home" };
  os.homedir = () => home;

  const otherUserHome = path.join(root, "studio5");
  makeRepo(otherUserHome, remote);
  const otherProject = path.join(otherUserHome, "projects/real");
  makeRepo(otherProject, "https://example.invalid/owner/real.git");
  os.homedir = () => otherUserHome;
  assert.equal(resolveGitContext(otherUserHome), undefined);
  assert.equal(resolveGitContextUncached(otherProject)?.remoteUrlHash,
    remoteLinkageHash("https://example.invalid/owner/real.git"));
  observations.anotherUser = { simulatedOsHomeReturn: true, homeExcluded: true, nestedProjectPreserved: true };
  os.homedir = () => home;

  const outside = path.join(root, "outside-project");
  makeRepo(outside, "https://example.invalid/owner/outside.git");
  assert.equal(resolveGitContextUncached(outside)?.remoteUrlHash,
    remoteLinkageHash("https://example.invalid/owner/outside.git"));
  assert.equal(resolveGitContextUncached("/"), undefined);
  observations.outsideHome = { sameVolumeRealRepositoryPreserved: true, systemRootCwdUnlinked: true };

  fs.chmodSync(home, 0o000);
  try {
    const homeContentsReadable = (() => { try { fs.readdirSync(home); return true; } catch { return false; } })();
    assert.equal(homeContentsReadable, false);
    assert.equal(resolveGitContext(home), undefined);
    assert.equal(resolveGitContextUncached(home), undefined);
    assert.equal(resolveGitContextUncached(outside), undefined);
    observations.unreadableHome = { actualChmod000: true, contentsReadable: false,
      directoryIdentityStillResolvable: true, homeRejected: true, unverifiableRepositoryOwnershipFailsClosed: true };
  } finally { fs.chmodSync(home, 0o700); }
  const deniedParent = path.join(root, "denied-parent");
  const deniedHome = path.join(deniedParent, "home");
  fs.mkdirSync(deniedHome, { recursive: true, mode: 0o700 });
  fs.chmodSync(deniedParent, 0o000);
  os.homedir = () => deniedHome;
  try {
    assert.equal(resolveGitContext(outside), undefined);
    assert.equal(resolveGitContextUncached(outside), undefined);
    observations.unresolvableUnreadableHome = { actualAncestorChmod000: true, bothEntrypointsFailClosed: true };
  } finally { fs.chmodSync(deniedParent, 0o700); os.homedir = () => home; }

  const homeGit = path.join(home, ".git");
  const worktreeGit = fs.readFileSync(path.join(lane, ".git"), "utf8").trim().replace(/^gitdir: /, "");
  const metadataAlias = path.join(root, "home-git-alias");
  fs.symlinkSync(homeGit, metadataAlias, "dir");
  for (const [name, pointer] of [
    ["symlink", path.join(metadataAlias, "worktrees", path.basename(worktreeGit))],
    ["case", worktreeGit.replace(`${path.sep}.git${path.sep}`, `${path.sep}.GIT${path.sep}`)],
    ["direct", homeGit],
  ]) {
    if (!fs.existsSync(pointer)) continue; // Case alias alternative on a case-sensitive host.
    const checkout = path.join(root, `pointer-${name}`);
    write(path.join(checkout, ".git"), `gitdir: ${pointer}\n`);
    assert.equal(resolveGitContext(checkout), undefined);
    assert.equal(resolveGitContextUncached(checkout), undefined);
  }
  const backing = path.join(root, "home-repository-metadata");
  fs.renameSync(homeGit, backing);
  write(homeGit, `gitdir: ${backing}\n`);
  const pointerGit = path.join(backing, "worktrees", "home-file-pointer");
  write(path.join(pointerGit, "HEAD"), "a".repeat(40) + "\n");
  write(path.join(pointerGit, "commondir"), "../..\n");
  const pointerCheckout = path.join(home, "file-pointer-worktree");
  write(path.join(pointerCheckout, ".git"), `gitdir: ${pointerGit}\n`);
  assert.equal(resolveGitContext(pointerCheckout), undefined);
  assert.equal(resolveGitContextUncached(pointerCheckout), undefined);
  observations.pointerOwnership = { symlinkAlias: true, directGitdir: true,
    homeGitFileCommonDirectory: true, realGitWorktreeRetainedInRoundOneProof: true };

  // System-root ownership is injected; never create or read a real /.git.
  const systemMetadata = path.join(root, "system-root-repository");
  makeRepo(path.join(root, "system-root-fixture"), remote);
  fs.renameSync(path.join(root, "system-root-fixture/.git"), systemMetadata);
  const systemPointer = path.join(root, "system-root-worktree");
  write(path.join(systemPointer, ".git"), `gitdir: ${systemMetadata}\n`);
  const originalStat = fs.statSync;
  const originalRealpath = fs.realpathSync;
  const originalOpen = fs.openSync;
  let forbiddenMetadataReads = 0;
  patchableFs.statSync = ((file: fs.PathLike, options?: fs.StatOptions) =>
    originalStat(String(file) === "/.git" ? systemMetadata : file, options)) as typeof fs.statSync;
  fs.realpathSync = ((file: fs.PathLike, options?: fs.ObjectEncodingOptions) =>
    originalRealpath(String(file) === "/.git" ? systemMetadata : file, options)) as typeof fs.realpathSync;
  fs.openSync = ((file: fs.PathLike, flags: string | number, mode?: fs.Mode) => {
    if ([path.join(systemMetadata, "HEAD"), path.join(systemMetadata, "config")].includes(String(file))) {
      forbiddenMetadataReads += 1;
    }
    return originalOpen(file, flags, mode);
  }) as typeof fs.openSync;
  try {
    assert.equal(resolveGitContext(systemPointer), undefined);
    assert.equal(resolveGitContextUncached(systemPointer), undefined);
    assert.equal(forbiddenMetadataReads, 0);
  } finally { patchableFs.statSync = originalStat; fs.realpathSync = originalRealpath; fs.openSync = originalOpen; }
  observations.systemRootOwnership = { injectedMetadataDirectory: true, forbiddenHeadAndConfigReads: 0 };

  const privateProject = path.join(home, "PRIVATE_PATH_SENTINEL-project");
  makeRepo(privateProject, "https://example.invalid/owner/privacy.git");
  const oldSet = Map.prototype.set;
  const observedKeys: unknown[] = [];
  Map.prototype.set = function (key: unknown, value: unknown) {
    if (typeof value === "object" && value !== null &&
      typeof (value as { at?: unknown }).at === "number" && "context" in value) observedKeys.push(key);
    return oldSet.call(this, key, value);
  };
  let context;
  try { context = resolveGitContext(privateProject); }
  finally { Map.prototype.set = oldSet; }
  assert.equal(observedKeys.length, 1);
  assert.equal(typeof observedKeys[0], "string");
  assert.match(observedKeys[0] as string, /^[a-f0-9]{64}$/);
  assert.equal(JSON.stringify({ observedKeys, context }).includes(root), false);
  assert.equal(gitContextCacheNonDigestKeyCountForProof(), 0);
  assert.ok(gitContextCacheSizeForProof() > 0);
  observations.cachePrivacy = { actualMapSetObserved: true, populatedCache: true,
    rawPathRetainedAsKey: false, rawPathInContext: false, opaqueKey: true };
  console.log(JSON.stringify({ proof: "independent-home-boundaries", status: "PASS",
    observations, changedRepositorySource: false }, null, 2));
} finally {
  buffer?.close();
  os.homedir = originalHome;
  if (originalEnvHome === undefined) delete process.env.HOME;
  else process.env.HOME = originalEnvHome;
  fs.rmSync(root, { recursive: true, force: true });
}
