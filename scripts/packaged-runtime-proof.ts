/**
 * Issue #155 proof: packaged runtime provenance, exact direct-Node
 * LaunchAgent ProgramArguments, and truthful memory-probe status.
 *
 * Tier-1 (exit-blocking): builder/verifier contract, self-containment,
 * exact packaged ProgramArguments end to end against the real transaction
 * code, adversarial refusal of every nonconforming argument shape, and the
 * development-chain regression guard.
 *
 * Tier-2 (recorded, non-blocking): whether the REAL approved source builds
 * today, and the resident-memory probe of the packaged daemon. On a tree
 * where `packages/collector-cli/src/cli.ts` cannot bundle, these are
 * reported as failed/not_run with verbatim errors instead of being skipped
 * silently. Every executed check runs against real files; nothing here
 * touches a real LaunchAgent, the operator home, or the network.
 */
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import {
  buildRuntime,
  scanBundleSpecifiers,
  verifyRuntime,
} from "./build-packaged-runtime";
import {
  LAUNCH_AGENT_LABEL,
  LaunchAgentTransactionError,
  installLaunchAgent,
  inspectLaunchAgentManifest,
  launchAgentsDir,
  readLaunchAgentProgramArguments,
  uninstallLaunchAgent,
} from "../packages/collector-cli/src/launch-agent";

type Check = {
  name: string;
  status: "passed" | "failed" | "not_run";
  detail?: unknown;
};

const checks: Check[] = [];
const repoRoot = path.resolve(import.meta.dirname, "..");
const tsxCli = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
const builderScript = path.join(repoRoot, "scripts", "build-packaged-runtime.ts");
const evidenceDir = path.join(repoRoot, "evidence");
const receiptPath = path.join(evidenceDir, "packaged-runtime-proof.json");

function check(name: string, condition: unknown, detail?: unknown): void {
  const passed = Boolean(condition);
  checks.push({ name, status: passed ? "passed" : "failed", ...(passed ? {} : { detail }) });
  if (!passed) throw new Error(`${name}: ${JSON.stringify(detail ?? null)}`);
}

function observe(name: string, detail: unknown): void {
  checks.push({ name, status: "not_run", detail });
}

function sandbox(label: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-p155-${label}-`));
}

function makeFixtureDist(root: string) {
  const dist = path.join(root, "dist");
  fs.mkdirSync(dist, { recursive: true });
  fs.writeFileSync(
    path.join(root, "fixture-package.json"),
    `${JSON.stringify({
      name: "@plimsoll/runtime-fixture",
      version: "9.9.9-fixture",
      engines: { node: ">=20 <25" },
    }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(root, "dashboard.html"),
    "<!doctype html><title>fixture</title>",
  );
  fs.writeFileSync(
    path.join(root, "entry.ts"),
    `console.log(JSON.stringify({ ok: true, runtime: "fixture" }));\n`,
  );
  return {
    dist,
    entry: path.join(root, "entry.ts"),
    packageManifest: path.join(root, "fixture-package.json"),
  };
}

const FIXTURE_COMMIT = "a".repeat(40);

/**
 * Deterministic classification of one `ps` row for the wrapper-free
 * process-tree rule: a plimsoll chain process whose command line involves
 * pnpm or tsx as an executable basename or as a resolved package directory
 * (`node_modules/tsx/dist/cli.mjs`) is a development wrapper residue.
 * Longer names that merely share a prefix (tsx-utils) are direct_node.
 */
export function classifyProcessCommand(command: string): "direct_node" | "wrapper_residue" {
  const tokens = command.split(/\s+/).filter(Boolean);
  const wrapperToken = tokens.some((token) => {
    const segments = token.split("/");
    return segments.includes("pnpm") || segments.includes("tsx");
  });
  return wrapperToken ? "wrapper_residue" : "direct_node";
}

export function parsePsOutput(
  output: string,
): Array<{ pid: number; rssKb: number; command: string }> {
  return output.split("\n").flatMap((line) => {
    const trimmed = line.trim();
    if (!trimmed) return [];
    const match = trimmed.match(/^(\d+)\s+(\d+)\s+(.+)$/);
    if (!match) return [];
    return [{ pid: Number(match[1]), rssKb: Number(match[2]), command: match[3]! }];
  });
}

async function main() {
  const ambientHome = process.env.PLIMSOLL_HOME;

  try {
    // ------------------------------------------------------------------
    // Builder contract on hermetic fixtures (authoritative behavior comes
    // from running the real builder binary as a subprocess).
    // ------------------------------------------------------------------
    const brokenRoot = sandbox("broken");
    const brokenDist = path.join(brokenRoot, "dist");
    fs.mkdirSync(brokenDist, { recursive: true });
    fs.writeFileSync(
      path.join(brokenRoot, "broken-entry.ts"),
      `import { gone } from "./module-that-does-not-exist";\nconsole.log(gone);\n`,
    );
    const brokenRun = spawnSync(
      process.execPath,
      [tsxCli, builderScript,
        "--repo-root", brokenRoot,
        "--entry", path.join(brokenRoot, "broken-entry.ts"),
        "--dist", brokenDist,
        "--package-manifest", path.join(brokenRoot, "missing-package.json"),
        "--source-commit", FIXTURE_COMMIT],
      { encoding: "utf8" },
    );
    check(
      "builder_fail_closed_on_unresolvable_entry",
      brokenRun.status !== 0 &&
        !fs.existsSync(path.join(brokenDist, "runtime-manifest.json")),
      { status: brokenRun.status, stderr: (brokenRun.stderr ?? "").slice(0, 2000) },
    );

    const fixtureRoot = sandbox("fixture");
    const fixture = makeFixtureDist(fixtureRoot);
    const manifest = await buildRuntime({
      repoRoot: repoRoot,
      entry: fixture.entry,
      dist: fixture.dist,
      packageManifestPath: fixture.packageManifest,
      sourceCommitOverride: FIXTURE_COMMIT,
    });
    const fixtureArtifact = path.join(fixture.dist, "cli.mjs");
    check("builder_emits_complete_provenance_manifest", true, {
      sha256: manifest.artifact.sha256,
      bytes: manifest.artifact.bytes,
      version: manifest.package.version,
      engines: manifest.nodeCompatibility.engines,
      buildTarget: manifest.nodeCompatibility.buildTarget,
      bundler: manifest.provenance.bundler,
      commit: manifest.source.commit,
      companions: manifest.companions,
    });
    check(
      "provenance_digest_is_lowercase_sha256_hex",
      /^[0-9a-f]{64}$/.test(manifest.artifact.sha256),
      manifest.artifact.sha256,
    );
    const fixtureManifestPath = path.join(fixture.dist, "runtime-manifest.json");
    const verifyOk = verifyRuntime(fixtureManifestPath);
    check("verifier_accepts_intact_artifact_and_manifest", verifyOk.ok, verifyOk);
    const boot = spawnSync(process.execPath, [fixtureArtifact], { encoding: "utf8" });
    const bootsClean = (() => {
      try {
        return boot.status === 0 && JSON.parse(boot.stdout).ok === true;
      } catch {
        return false;
      }
    })();
    check("built_fixture_runs_under_direct_node_only", bootsClean, {
      status: boot.status,
      stderr: (boot.stderr ?? "").slice(0, 500),
    });

    const tamperedDist = path.join(sandbox("tamper"), "dist");
    fs.cpSync(fixture.dist, tamperedDist, { recursive: true });
    fs.appendFileSync(path.join(tamperedDist, "cli.mjs"), "\n// tampered\n");
    const tampered = verifyRuntime(path.join(tamperedDist, "runtime-manifest.json"));
    check(
      "verifier_rejects_tampered_artifact_digest",
      !tampered.ok && tampered.code === "digest_mismatch",
      tampered,
    );

    const companionName = (manifest.companions as Array<{ name: string }>)[0]?.name;
    if (companionName) {
      const companionTamperedDist = path.join(sandbox("tamper2"), "dist");
      fs.cpSync(fixture.dist, companionTamperedDist, { recursive: true });
      fs.appendFileSync(path.join(companionTamperedDist, companionName), "\n");
      const companionTampered = verifyRuntime(
        path.join(companionTamperedDist, "runtime-manifest.json"),
      );
      check(
        "verifier_rejects_tampered_companion_digest",
        !companionTampered.ok && companionTampered.code === "digest_mismatch",
        companionTampered,
      );
    }

    const incompleteManifestPath = path.join(sandbox("incomplete"), "m.json");
    const incomplete = JSON.parse(fs.readFileSync(fixtureManifestPath, "utf8"));
    delete incomplete.provenance.bundler.version;
    fs.writeFileSync(incompleteManifestPath, JSON.stringify(incomplete, null, 2));
    const incompleteResult = verifyRuntime(incompleteManifestPath);
    check(
      "verifier_rejects_incomplete_manifest",
      !incompleteResult.ok && incompleteResult.code === "manifest_invalid",
      incompleteResult,
    );

    check(
      "self_containment_scan_flags_foreign_bare_import",
      (() => {
        const violations = scanBundleSpecifiers(
          `import { x } from "react";\nimport fs from "node:fs";\nimport Database from "better-sqlite3";\nconst m = import("./lazy-chunk");\n`,
          ["better-sqlite3"],
        );
        return violations.length === 1 &&
          violations[0]!.specifier === "react" &&
          violations[0]!.kind === "static";
      })(),
      "only foreign bare specifiers are violations",
    );

    check(
      "process_tree_classifier_adversarial",
      (() => {
        const rows = parsePsOutput([
          "  501  81100 /opt/homebrew/bin/pnpm --dir /repo collector start",
          "  502  62493 node /repo/node_modules/tsx/dist/cli.mjs src/cli.ts start",
          "  503 119808 /Users/x/.nvm/versions/v22/bin/node /abs/dist/cli.mjs start",
          "  504 123904 /Users/x/.nvm/versions/v22/bin/node /abs/dist/cli.mjs",
          "  505   4096 node /repo/node_modules/tsx-utils/bin.js serve",
          "  506   2048 sh -c echo pnpm-store-is-fine",
        ].join("\n"));
        const byPid = new Map(rows.map((row) => [row.pid, row]));
        return (
          byPid.get(501)!.rssKb === 81100 &&
          classifyProcessCommand(byPid.get(501)!.command) === "wrapper_residue" &&
          classifyProcessCommand(byPid.get(502)!.command) === "wrapper_residue" &&
          classifyProcessCommand(byPid.get(503)!.command) === "direct_node" &&
          classifyProcessCommand(byPid.get(504)!.command) === "direct_node" &&
          classifyProcessCommand(byPid.get(505)!.command) === "direct_node" &&
          classifyProcessCommand(byPid.get(506)!.command) === "direct_node"
        );
      })(),
      "pnpm/tsx basenames flagged; superstring tokens not flagged",
    );

    // ------------------------------------------------------------------
    // Real approved source: buildability is recorded, never faked.
    // ------------------------------------------------------------------
    const realDist = path.join(repoRoot, "packages", "collector-cli", "dist");
    const realManifestPath = path.join(realDist, "runtime-manifest.json");
    const realBuild = spawnSync(process.execPath, [tsxCli, builderScript], {
      cwd: repoRoot,
      encoding: "utf8",
    });
    if (realBuild.status === 0) {
      const verified = verifyRuntime(realManifestPath);
      check("real_source_builds_and_verifies", verified.ok, verified);
    } else {
      observe("real_source_build_unavailable_on_this_tree", {
        exitStatus: realBuild.status,
        stderr: (realBuild.stderr ?? "").slice(0, 4000),
      });
      check(
        "real_source_failure_leaves_no_runtime_manifest",
        !fs.existsSync(realManifestPath),
        realManifestPath,
      );
    }

    // ------------------------------------------------------------------
    // LaunchAgent packaged runtime: exact ProgramArguments, adversarial
    // refusals, regression guard. Library-level against the real
    // transaction code, fully sandboxed.
    // ------------------------------------------------------------------
    const collectorSandboxHome = sandbox("collector-home");
    fs.chmodSync(collectorSandboxHome, 0o700);
    process.env.PLIMSOLL_HOME = collectorSandboxHome;

    const packagedHome = sandbox("agent-packaged");
    const nodeAbsolute = fs.realpathSync(process.execPath);
    const installOptions = {
      homeDir: packagedHome,
      repoRoot: packagedHome,
      programArguments: [nodeAbsolute, fixtureArtifact, "start"],
      workingDirectory: path.dirname(fixtureArtifact),
    };
    const installed = installLaunchAgent(installOptions);
    check("packaged_install_succeeds_in_sandbox", installed.receipt.status === "installed", {
      status: installed.receipt.status,
      manifestDigest: installed.receipt.manifestDigest,
    });
    const observedArgs = readLaunchAgentProgramArguments({ homeDir: packagedHome });
    check(
      "program_arguments_exactly_node_entry_start",
      JSON.stringify(observedArgs.programArguments) ===
        JSON.stringify([nodeAbsolute, fixtureArtifact, "start"]),
      observedArgs.programArguments,
    );
    check(
      "working_directory_is_entry_directory",
      observedArgs.workingDirectory === path.dirname(fixtureArtifact),
      observedArgs.workingDirectory,
    );
    check(
      "node_executable_recorded_absolute_and_real",
      path.isAbsolute(observedArgs.programArguments[0]!) &&
        fs.realpathSync(observedArgs.programArguments[0]!) === nodeAbsolute,
      observedArgs.programArguments[0],
    );
    const validDigest = inspectLaunchAgentManifest({ homeDir: packagedHome }).manifestDigest;
    const rerun = installLaunchAgent(installOptions);
    check(
      "identical_install_is_exact_noop_with_stable_digest",
      rerun.receipt.status === "unchanged" && rerun.receipt.manifestDigest === validDigest,
      { status: rerun.receipt.status, digest: rerun.receipt.manifestDigest },
    );

    const plistText = fs.readFileSync(
      path.join(launchAgentsDir(packagedHome), `${LAUNCH_AGENT_LABEL}.plist`),
      "utf8",
    );
    const propagatedPathEntries =
      plistText.match(/<key>PATH<\/key>\s*<string>([^<]+)<\/string>/)?.[1]?.split(path.delimiter) ??
      [];
    const envKeyCount = (
      plistText.match(/<key>(PATH|PLIMSOLL_COLLECTOR_DATA_MODE|PLIMSOLL_HOME)<\/key>/g) ?? []
    ).length;
    check(
      "propagated_path_covers_node_directory",
      propagatedPathEntries.includes(path.dirname(nodeAbsolute)),
      propagatedPathEntries.slice(0, 4),
    );
    check(
      "environment_keys_match_propagated_home_branch",
      envKeyCount === 3,
      { envKeyCount, plimsollHomeSet: true },
    );

    const adversarialCases: Array<{
      name: string;
      options: Record<string, unknown>;
      expectedCode: string;
    }> = [
      {
        name: "refuses_two_argument_form",
        options: { programArguments: [nodeAbsolute, fixtureArtifact] },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_extra_fourth_argument",
        options: {
          programArguments: [nodeAbsolute, fixtureArtifact, "start", "--flag"],
          workingDirectory: path.dirname(fixtureArtifact),
        },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_non_start_verb",
        options: {
          programArguments: [nodeAbsolute, fixtureArtifact, "status"],
          workingDirectory: path.dirname(fixtureArtifact),
        },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_relative_node_executable",
        options: {
          programArguments: ["node", fixtureArtifact, "start"],
          workingDirectory: path.dirname(fixtureArtifact),
        },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_relative_entry",
        options: {
          programArguments: [nodeAbsolute, "dist/cli.mjs", "start"],
          workingDirectory: fixture.dist,
        },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_non_script_entry_extension",
        options: {
          programArguments: [nodeAbsolute, path.join(fixture.dist, "cli.bin"), "start"],
          workingDirectory: fixture.dist,
        },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_working_directory_mismatch",
        options: {
          programArguments: [nodeAbsolute, fixtureArtifact, "start"],
          workingDirectory: os.tmpdir(),
        },
        expectedCode: "PLIST_ARGUMENTS_UNOWNED",
      },
      {
        name: "refuses_foreign_label",
        options: {
          label: "com.evil.collector",
          programArguments: [nodeAbsolute, fixtureArtifact, "start"],
          workingDirectory: path.dirname(fixtureArtifact),
        },
        expectedCode: "LABEL_NOT_ALLOWLISTED",
      },
    ];
    for (const adversarial of adversarialCases) {
      let refused: LaunchAgentTransactionError | null = null;
      try {
        installLaunchAgent({
          homeDir: packagedHome,
          repoRoot: packagedHome,
          ...adversarial.options,
        } as never);
      } catch (error) {
        if (error instanceof LaunchAgentTransactionError) refused = error;
        else throw error;
      }
      const digestAfter = inspectLaunchAgentManifest({ homeDir: packagedHome }).manifestDigest;
      check(
        `launch_agent_${adversarial.name}`,
        refused !== null && refused.code === adversarial.expectedCode && digestAfter === validDigest,
        { code: refused?.code ?? "no_error", digestUnchanged: digestAfter === validDigest },
      );
    }

    // Development-chain regression guard: the explicit five-token pnpm form
    // remains owned so source development keeps working; #155 makes the
    // PERMANENT agent direct-Node without touching the dev path.
    const devHome = sandbox("agent-dev");
    const dummyBin = path.join(devHome, "bin");
    fs.mkdirSync(dummyBin, { recursive: true });
    const dummyPnpm = path.join(dummyBin, "pnpm");
    fs.writeFileSync(dummyPnpm, "#!/bin/sh\nexit 0\n", { mode: 0o755 });
    const devInstalled = installLaunchAgent({
      homeDir: devHome,
      repoRoot: devHome,
      pnpmPath: dummyPnpm,
    });
    const devArgs = readLaunchAgentProgramArguments({ homeDir: devHome });
    check(
      "development_chain_still_owned_for_source_work",
      devInstalled.receipt.status === "installed" &&
        devArgs.programArguments.length === 5 &&
        path.basename(devArgs.programArguments[0]!) === "pnpm" &&
        devArgs.programArguments[3] === "collector" &&
        devArgs.programArguments[4] === "start",
      devArgs.programArguments,
    );
    const devUninstall = uninstallLaunchAgent({ homeDir: devHome });
    check("sandbox_dev_manifest_removed_after_guard", devUninstall.receipt.status === "removed", {
      status: devUninstall.receipt.status,
    });

    // Default-home branch: with no propagated PLIMSOLL_HOME the manifest
    // carries exactly the two base environment keys.
    delete process.env.PLIMSOLL_HOME;
    const defaultEnvHome = sandbox("agent-default-env");
    const defaultInstalled = installLaunchAgent({
      homeDir: defaultEnvHome,
      repoRoot: defaultEnvHome,
      programArguments: [nodeAbsolute, fixtureArtifact, "start"],
      workingDirectory: path.dirname(fixtureArtifact),
    });
    const defaultEnvText = fs.readFileSync(
      path.join(launchAgentsDir(defaultEnvHome), `${LAUNCH_AGENT_LABEL}.plist`),
      "utf8",
    );
    const defaultKeyCount = (
      defaultEnvText.match(/<key>(PATH|PLIMSOLL_COLLECTOR_DATA_MODE|PLIMSOLL_HOME)<\/key>/g) ?? []
    ).length;
    check(
      "default_home_install_succeeds",
      defaultInstalled.receipt.status === "installed",
      defaultInstalled.receipt.status,
    );
    check("default_home_has_two_environment_keys_only", defaultKeyCount === 2, defaultKeyCount);

    // ------------------------------------------------------------------
    // Resident-memory probe of the packaged daemon: only when the real
    // bundle exists and boots. Otherwise recorded as not_run with reason.
    // ------------------------------------------------------------------
    const realArtifact = path.join(realDist, "cli.mjs");
    if (!fs.existsSync(realArtifact)) {
      observe("packaged_memory_probe_not_run", {
        reason: "real_source_bundle_unbuildable",
        detail:
          "packages/collector-cli/src/cli.ts does not bundle on this tree; see real_source_build receipt",
      });
    } else {
      await measurePackagedDaemon(realArtifact);
    }
  } finally {
    if (ambientHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = ambientHome;
  }

  const summary = {
    schema: "plimsoll.packaged-runtime-proof.v1",
    host: { platform: `${process.platform}-${process.arch}`, node: process.versions.node },
    checks,
    summary: {
      total: checks.length,
      passed: checks.filter((entry) => entry.status === "passed").length,
      failed: checks.filter((entry) => entry.status === "failed").map((entry) => entry.name),
      notRun: checks.filter((entry) => entry.status === "not_run").map((entry) => entry.name),
    },
  };
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(receiptPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify(summary.summary));
  if (summary.summary.failed.length > 0) process.exitCode = 1;
}

async function measurePackagedDaemon(artifact: string): Promise<void> {
  const dist = path.dirname(artifact);
  const port = await (async () => {
    const server = http.createServer();
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    const chosen = address && typeof address === "object" ? address.port : 0;
    await new Promise<void>((resolve) => server.close(() => resolve()));
    return chosen;
  })();
  const collectorHome = sandbox("memory-home");
  fs.chmodSync(collectorHome, 0o700);
  fs.writeFileSync(
    path.join(collectorHome, "collector.config.json"),
    `${JSON.stringify({ port }, null, 2)}\n`,
  );
  const daemon = spawn(process.execPath, [artifact, "start"], {
    env: {
      PATH: "/usr/bin:/bin",
      PLIMSOLL_HOME: collectorHome,
      PLIMSOLL_COLLECTOR_DATA_MODE: "metadata",
    },
    stdio: ["ignore", "ignore", "pipe"],
  });
  const startedAt = Date.now();
  let ready = false;
  while (Date.now() - startedAt < 15_000) {
    if (daemon.exitCode !== null) break;
    ready = await probeEndpoint(port);
    if (ready) break;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  if (!ready) {
    daemon.kill("SIGTERM");
    observe("packaged_memory_probe_not_run", { reason: "daemon_never_became_ready", port });
    return;
  }
  const samples: Array<{ atMs: number; rows: ReturnType<typeof parsePsOutput> }> = [];
  while (Date.now() - startedAt < 20_000) {
    const ps = spawnSync("/bin/ps", ["-axo", "pid=", "rss=", "command="], { encoding: "utf8" });
    samples.push({
      atMs: Date.now() - startedAt,
      rows: parsePsOutput(ps.stdout ?? "").filter((row) => row.command.includes(artifact)),
    });
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  daemon.kill("SIGTERM");
  const exited = await new Promise<boolean>((resolve) => {
    if (daemon.exitCode !== null) {
      resolve(true);
      return;
    }
    const timer = setTimeout(() => resolve(false), 5_000);
    timer.unref();
    daemon.once("exit", () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
  const allRows = samples.flatMap((sample) => sample.rows);
  const wrapperRows = allRows.filter(
    (row) => classifyProcessCommand(row.command) === "wrapper_residue",
  );
  const collectorRows = allRows.filter((row) => row.command.endsWith(" start"));
  const childRows = allRows.filter(
    (row) =>
      classifyProcessCommand(row.command) === "direct_node" && !row.command.endsWith(" start"),
  );
  const peakRssMiB = (rows: typeof allRows) =>
    Number((rows.reduce((peak, row) => Math.max(peak, row.rssKb), 0) / 1024).toFixed(1));
  const postExit = spawnSync("/bin/ps", ["-axo", "pid=", "rss=", "command="], {
    encoding: "utf8",
  });
  const orphans = parsePsOutput(postExit.stdout ?? "").filter((row) =>
    row.command.includes(artifact),
  );
  check("packaged_daemon_shuts_down_within_deadline", exited, { pid: daemon.pid });
  check("packaged_chain_contains_zero_wrapper_processes", wrapperRows.length === 0, wrapperRows);
  check(
    "packaged_chain_observed_collector_process",
    collectorRows.length > 0,
    { samples: samples.length },
  );
  check("packaged_shutdown_leaves_no_orphan_processes", orphans.length === 0, orphans);
  const manifestVerification = verifyRuntime(path.join(dist, "runtime-manifest.json"));
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(
    path.join(evidenceDir, "packaged-runtime-memory.json"),
    `${JSON.stringify({
      schema: "plimsoll.packaged-runtime-memory.v1",
      artifactSha256: manifestVerification.ok ? manifestVerification.sha256 : null,
      observationWindowMs: 20_000,
      samples: samples.length,
      collectorPeakRssMiB: peakRssMiB(collectorRows),
      maintenanceChildPeakRssMiB: childRows.length > 0 ? peakRssMiB(childRows) : null,
      maintenanceChildObserved: childRows.length > 0,
      wrapperProcessesObserved: wrapperRows.length,
      shutdownWithinDeadline: exited,
      orphanProcessesAfterShutdown: orphans.length,
      method:
        "ps -axo pid=,rss=,command= sampled every 500ms for 20s; rows filtered by built-artifact path; RSS is resident set including shared pages",
    }, null, 2)}\n`,
  );
  check("packaged_memory_receipt_written", fs.existsSync(path.join(evidenceDir, "packaged-runtime-memory.json")), {});
}

function probeEndpoint(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const request = http.get(
      { host: "127.0.0.1", port, path: "/api/summary", timeout: 1_000 },
      (response) => {
        response.resume();
        resolve(response.statusCode === 200);
      },
    );
    request.once("error", () => resolve(false));
    request.once("timeout", () => {
      request.destroy();
      resolve(false);
    });
  });
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? `${error.message}` : String(error));
  process.exit(1);
});
