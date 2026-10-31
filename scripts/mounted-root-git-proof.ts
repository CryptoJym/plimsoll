import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { resolveGitContext, resolveGitContextUncached } from "../packages/collector-cli/src/git-context";
import { remoteLinkageHash } from "../packages/shared/src/index";

const patchableFs = fs as { statSync: typeof fs.statSync; lstatSync: typeof fs.lstatSync };

const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "independent-volume-"));
const originalHome = os.homedir;
const originalStat = fs.statSync;
const originalLstat = fs.lstatSync;
try {
  const home = path.join(fixture, "home");
  const mountRoot = path.join(fixture, "mounted-volume");
  const child = path.join(mountRoot, "home-only-chat");
  fs.mkdirSync(home, { mode: 0o700 });
  fs.mkdirSync(path.join(mountRoot, ".git/refs/heads"), { recursive: true, mode: 0o700 });
  fs.mkdirSync(child, { mode: 0o700 });
  const remote = "https://github.com/CryptoJym/plimsoll.git";
  fs.writeFileSync(path.join(mountRoot, ".git/HEAD"), "ref: refs/heads/main\n");
  fs.writeFileSync(path.join(mountRoot, ".git/refs/heads/main"), "a".repeat(40) + "\n");
  fs.writeFileSync(path.join(mountRoot, ".git/config"), `[remote "origin"]\n url = ${remote}\n`);
  const nativeDev = originalStat(fixture, { bigint: true }).dev;
  const mountedDev = nativeDev + 1n;
  os.homedir = () => home;
  patchableFs.statSync = ((file: fs.PathLike, options?: fs.StatOptions) => {
    const stat = originalStat(file, options);
    if (!stat) return stat;
    const name = String(file);
    if (stat.isDirectory() && (name === mountRoot || name.startsWith(mountRoot + path.sep))) {
      stat.dev = typeof stat.dev === "bigint" ? mountedDev : Number(mountedDev);
    }
    return stat;
  }) as typeof fs.statSync;
  let rootMetadataReads = 0;
  patchableFs.lstatSync = ((file: fs.PathLike, options?: fs.StatOptions) => {
    if (String(file) === path.join(mountRoot, ".git")) rootMetadataReads += 1;
    return originalLstat(file, options);
  }) as typeof fs.lstatSync;
  assert.notEqual(fs.statSync(mountRoot, { bigint: true }).dev,
    fs.statSync(path.dirname(mountRoot), { bigint: true }).dev);
  const atRoot = resolveGitContextUncached(mountRoot);
  const atChild = resolveGitContextUncached(child);
  const cachedChild = resolveGitContext(child);
  for (const context of [atRoot, atChild, cachedChild]) {
    assert.equal(context?.remoteUrlHash, remoteLinkageHash(remote));
  }
  assert.equal(rootMetadataReads, 3);
  console.log(JSON.stringify({ proof: "independent-mounted-filesystem-root", status: "PASS",
    filesystemDeviceBoundaryInjected: true, actualSecondVolumeNotUsed: true, realMetadataReads: true,
    rootDiffersFromParentDevice: true, atRootLinked: true, rootOnlyDescendantLinked: true,
    cachedAndUncachedAffected: true, rootGitMetadataReads: rootMetadataReads,
    expectedRootGitMetadataReads: 3, mountedRootIntentionallyAttributable: true }, null, 2));
} finally {
  patchableFs.statSync = originalStat; patchableFs.lstatSync = originalLstat; os.homedir = originalHome;
  fs.rmSync(fixture, { recursive: true, force: true });
}
