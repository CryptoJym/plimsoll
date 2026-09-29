/** A crash while upgrading legacy lifecycle state must remain recoverable. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { FilesystemLifecycleAdapter, type ManagedLifecyclePaths } from
  "../../packages/collector-cli/src/lifecycle-filesystem";
import type { RuntimeArtifact } from "../../packages/collector-cli/src/lifecycle";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-r6-lifecycle-crash-"));
const paths: ManagedLifecyclePaths = {
  ownershipRoot: root,
  lifecycleRoot: path.join(root, "private", "lifecycle"),
  artifactSourceRoot: path.join(root, "artifacts"),
  collectorConfig: path.join(root, "private", "collector.config.json"),
  database: path.join(root, "private", "work-ledger.sqlite"),
  serviceManifest: path.join(root, "Library", "LaunchAgents", "com.plimsoll.collector.plist"),
  ownedToolFragments: [], history: [],
  statusSummary: path.join(root, "private", "status-summary.json"),
};
let servingVersion: string | null = null;
const service = {
  async activate(input: { version: string }) { servingVersion = input.version; },
  async restore(input: { version: string | null }) { servingVersion = input.version; },
  async remove() { servingVersion = null; },
  async readiness(version: string) { return { ready: servingVersion === version,
    runtimeVersion: servingVersion, serviceReady: servingVersion === version,
    configCompatible: true, databaseCompatible: true }; },
  async supportSnapshot() { return {} as any; },
};
const database = { async snapshot() { return false; }, async restore() {} };
const adapter = new FilesystemLifecycleAdapter(paths, service, database);
function artifact(version: string): RuntimeArtifact {
  const sourcePath = path.join(paths.artifactSourceRoot, `${version}.mjs`);
  fs.mkdirSync(path.dirname(sourcePath), { recursive: true, mode: 0o700 });
  fs.writeFileSync(sourcePath, `// ${version}\n`, { mode: 0o600 });
  return { version, platform: "darwin", architecture: "arm64", nodeMajor: 22,
    sourcePath, sha256: `sha256:${createHash("sha256").update(fs.readFileSync(sourcePath))
      .digest("hex")}` };
}
async function main() {
try {
  const v1 = artifact("0.7.44");
  const v2 = artifact("0.7.45");
  await adapter.stage(v1);
  await adapter.switchTo(v1);
  const statePath = path.join(paths.lifecycleRoot, "state.json");
  const installationPath = path.join(paths.lifecycleRoot, "installation.json");
  const oldState = JSON.parse(fs.readFileSync(statePath, "utf8")) as Record<string, unknown>;
  delete oldState.installId;
  fs.writeFileSync(statePath, `${JSON.stringify(oldState)}\n`, { mode: 0o600 });
  fs.rmSync(installationPath);
  await adapter.acquireLock("upgrade-crash");
  await adapter.snapshot("upgrade-crash");
  await adapter.stage(v2);

  const rename = fs.renameSync;
  let interrupted = false;
  fs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    rename(from, to);
    if (String(to) === installationPath) {
      interrupted = true;
      throw new Error("fixture crash after installation identity publication");
    }
  }) as typeof fs.renameSync;
  try { await adapter.switchTo(v2); }
  catch (error) {
    assert.match(String(error), /fixture crash/);
  } finally { fs.renameSync = rename; }
  assert.ok(interrupted);
  const stateAfter = JSON.parse(fs.readFileSync(statePath, "utf8")) as Record<string, unknown>;
  const installationAfter = JSON.parse(fs.readFileSync(installationPath, "utf8")) as
    Record<string, unknown>;
  let retryError: string | null = null;
  try { await adapter.switchTo(v2); }
  catch (error) { retryError = error instanceof Error ? error.message : String(error); }
  let restoreError: string | null = null;
  try { await adapter.restore("upgrade-crash", "upgrade-crash"); }
  catch (error) { restoreError = error instanceof Error ? error.message : String(error); }
  console.log(JSON.stringify({ interrupted, servingVersion,
    oldStateHasInstallId: typeof stateAfter.installId === "string",
    newIdentityHasInstallId: typeof installationAfter.installId === "string",
    retryError, restoreError }));
  assert.equal(retryError ?? restoreError, null,
    "a crash between identity and state publication stranded a legacy update");
} finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
