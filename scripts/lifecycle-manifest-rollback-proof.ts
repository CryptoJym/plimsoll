import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { composeLifecycleAdapter, LaunchAgentManifestLifecycleService } from "../packages/collector-cli/src/lifecycle-adapters";
import { LifecycleManager, type RuntimeArtifact } from "../packages/collector-cli/src/lifecycle";
import { collectorBufferPath, collectorConfigPath } from "../packages/collector-cli/src/config";
import { installLaunchAgent, launchAgentPlistPath, renderLaunchAgentPlist, uninstallLaunchAgent } from "../packages/collector-cli/src/launch-agent";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("lifecycle-manifest-rollback", 6);
const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "plimsoll-manifest-rollback-"));
const priorHome = process.env.PLIMSOLL_HOME;
const checks: Array<{ name: string; passed: boolean }> = [];

async function scenario(kind: "legacy" | "legacy-adaptive" | "managed" | "absent") {
  const home = path.join(root, kind);
  fs.mkdirSync(home, { mode: 0o700 });
  process.env.PLIMSOLL_HOME = path.join(home, ".plimsoll");
  fs.mkdirSync(process.env.PLIMSOLL_HOME, { mode: 0o700 });
  const lifecycleRoot = path.join(process.env.PLIMSOLL_HOME, "lifecycle");
  const config = collectorConfigPath(home);
  fs.writeFileSync(config, "{}\n", { mode: 0o600 });
  const ledger = new Database(collectorBufferPath(home));
  ledger.pragma("journal_mode = WAL");
  ledger.exec("create table retained(value text); insert into retained values('fixture-evidence')");
  ledger.close();
  const service = new LaunchAgentManifestLifecycleService({ homeDir: home, lifecycleRoot });
  const options = { homeDir: home, lifecycleRoot, artifactSourceRoot: home };
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(home, `fixture-${version}.mjs`);
    fs.writeFileSync(sourcePath, `// Inert fixture ${version}; never executed.\n`);
    return { version, sourcePath, platform: "darwin", architecture: process.arch as "arm64" | "x64",
      nodeMajor: Number(process.versions.node.split(".")[0]),
      sha256: `sha256:${createHash("sha256").update(fs.readFileSync(sourcePath)).digest("hex")}` };
  };
  if (kind === "managed") {
    await new LifecycleManager(composeLifecycleAdapter(options)).update({ operationId: "prior", artifact: artifact("1.0.0-fixture") });
    // A prior runtime may use a different absolute Node executable. Its exact
    // manifest, including that choice, must survive rollback.
    installLaunchAgent({ homeDir: home, repoRoot: home, workingDirectory: path.join(lifecycleRoot, "versions", "1.0.0-fixture", `darwin-${process.arch}`, "bin"),
      programArguments: [path.join(home, "prior-runtime", "node"), path.join(lifecycleRoot, "versions", "1.0.0-fixture", `darwin-${process.arch}`, "bin", "plimsoll.mjs"), "start"] });
  } else if (kind === "legacy" || kind === "legacy-adaptive") {
    installLaunchAgent({ homeDir: home, repoRoot: home, pnpmPath: path.join(home, "prior-runtime", "pnpm") });
    if (kind === "legacy-adaptive") {
      const manifest = launchAgentPlistPath(home);
      fs.writeFileSync(manifest, fs.readFileSync(manifest, "utf8").replace("<key>Label</key>", "<key>ProcessType</key><string>Adaptive</string><key>Label</key>"));
    }
  }
  const manifest = launchAgentPlistPath(home);
  const before = fs.existsSync(manifest) ? fs.readFileSync(manifest) : null;
  const failingService = {
    activate: service.activate.bind(service), restore: service.restore.bind(service), remove: service.remove.bind(service),
    supportSnapshot: service.supportSnapshot.bind(service),
    readiness: async () => ({ ready: false, runtimeVersion: "2.0.0-fixture", serviceReady: false,
      configCompatible: true, databaseCompatible: true, reason: "service_unready" as const }),
  };
  const adapter = composeLifecycleAdapter({ ...options, service: failingService });
  await assert.rejects(new LifecycleManager(adapter).update({ operationId: "failed-update", artifact: artifact("2.0.0-fixture") }), /readiness failed/);
  const after = fs.existsSync(manifest) ? fs.readFileSync(manifest) : null;
  assert.equal(before === null ? after === null : after?.equals(before), true, "prior manifest bytes or absence must be restored");
  assert.equal(fs.readFileSync(config, "utf8"), "{}\n");
  const restored = new Database(collectorBufferPath(home), { readonly: true });
  try { assert.deepEqual(restored.prepare("select value from retained").all(), [{ value: "fixture-evidence" }]); }
  finally { restored.close(); }
  assert.equal(await adapter.readJournal(), null);
  const receipt = JSON.parse(fs.readFileSync(path.join(lifecycleRoot, "receipts", "failed-update-update.json"), "utf8"));
  assert.equal(receipt.status, "rolled_back");
}

async function main() {
  try {
    for (const kind of ["legacy", "legacy-adaptive", "managed", "absent"] as const) {
      const name = `${kind}_rollback_preserves_exact_manifest_config_and_ledger`;
      try { await scenario(kind); checks.push({ name, passed: true }); }
      catch (error) { console.error(JSON.stringify({ name, error: error instanceof Error ? error.message.slice(0, 300) : "fixture_failed" })); checks.push({ name, passed: false }); }
      completion.check(name, checks.at(-1)!.passed);
    }
    for (const kind of ["foreign-process-type", "unexpected-key"] as const) {
      const name = `${kind}_is_rejected_without_manifest_mutation`;
      try {
        const home = path.join(root, kind);
        fs.mkdirSync(home, { mode: 0o700 });
        const options = { homeDir: home, repoRoot: home, pnpmPath: path.join(home, "runtime", "pnpm") };
        const manifest = launchAgentPlistPath(home);
        fs.mkdirSync(path.dirname(manifest), { recursive: true, mode: 0o700 });
        const extra = kind === "foreign-process-type"
          ? "<key>ProcessType</key><string>Background</string>"
          : "<key>ProcessType</key><string>Adaptive</string><key>OperatorExtra</key><true/>";
        const before = renderLaunchAgentPlist(options).replace("<key>Label</key>", `${extra}<key>Label</key>`);
        fs.writeFileSync(manifest, before, { mode: 0o600 });
        const code = kind === "foreign-process-type" ? "PLIST_PROCESS_TYPE_UNEXPECTED" : "PLIST_KEYS_UNEXPECTED";
        assert.throws(() => installLaunchAgent(options), { code });
        assert.throws(() => uninstallLaunchAgent({ homeDir: home }), { code });
        assert.equal(fs.readFileSync(manifest, "utf8"), before);
        checks.push({ name, passed: true });
      } catch (error) {
        console.error(JSON.stringify({ name, error: error instanceof Error ? error.message.slice(0, 300) : "fixture_failed" }));
        checks.push({ name, passed: false });
      }
      completion.check(name, checks.at(-1)!.passed);
    }
  } finally {
    if (priorHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = priorHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
  console.log(JSON.stringify({ checks, liveStateTouched: false, serviceManagerCalled: false }));
  completion.complete();
}
main().catch(() => { process.exitCode = 1; });
