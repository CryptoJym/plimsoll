/**
 * Issue #135 proof: collector home end-to-end isolated canary state.
 *
 * Proves that one canonical resolver validates an absolute, user-owned,
 * private, non-symlink collector home; that every derived path binds that
 * exact home; that the LaunchAgent boundary propagates it through an
 * allowlisted environment field with path-free identity hashes in receipts
 * and status/doctor comparisons; and that alternate homes cannot enable two
 * simultaneous collectors on one machine-global port.
 *
 * Every fixture runs under fresh temp homes. The proof never touches the
 * operator's collector directory or LaunchAgent, uses no credentials, and
 * talks only to 127.0.0.1.
 */
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import {
  CollectorHomeError,
  collectorHomeIdentityHash,
  defaultCollectorHome,
  resolveCollectorHome,
} from "../packages/collector-cli/src/collector-home";
import {
  collectorBufferPath,
  collectorConfigPath,
  collectorLogPath,
} from "../packages/collector-cli/src/config";
import { readLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import {
  FilesystemLifecycleAdapter,
  type LifecycleServiceAdapter,
} from "../packages/collector-cli/src/lifecycle-filesystem";

type Check = { name: string; passed: boolean; detail: Record<string, unknown> };

const startedAt = Date.now();
const repoRoot = process.cwd();
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-collector-home-proof-"));
const checks: Check[] = [];
const cliSource = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");

function record(name: string, passed: boolean, detail: Record<string, unknown> = {}) {
  checks.push({ name, passed, detail });
  console.log(`${passed ? "PASS" : "FAIL"} ${name}`);
  if (!passed) console.log(`  detail ${JSON.stringify(detail).slice(0, 600)}`);
}

function privateDir(parent: string, name: string) {
  const dir = fs.mkdtempSync(path.join(parent, name));
  fs.chmodSync(dir, 0o700);
  return dir;
}

function cleanEnv(extra: NodeJS.ProcessEnv = {}) {
  const env = { ...process.env };
  // Strip the ambient variable BEFORE applying fixture overrides so a
  // missing fixture value can never silently select the operator's home.
  delete env.PLIMSOLL_HOME;
  const merged = { ...env, ...extra };
  if (!("PLIMSOLL_HOME" in extra || "HOME" in extra)) {
    throw new Error("proof bug: every CLI fixture must pin PLIMSOLL_HOME or HOME explicitly");
  }
  return merged;
}

function runCli(args: string[], extraEnv: NodeJS.ProcessEnv) {
  return spawnSync(process.execPath, ["--import", "tsx", cliSource, ...args], {
    cwd: repoRoot,
    encoding: "utf8",
    env: cleanEnv(extraEnv),
    timeout: 60_000,
  });
}

function reserveLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = http.createServer();
    server.once("listening", () => {
      const address = server.address() as AddressInfo;
      server.close(() => resolve(address.port));
    });
    server.once("error", reject);
    server.listen(0, "127.0.0.1");
  });
}

function fetchStatus(
  port: number,
  managementToken?: string,
): Promise<Record<string, unknown> | null> {
  return new Promise((resolve) => {
    // Issue 0056 (#104): an enforcing daemon gates status behind the
    // provisioned management credential, which this proof reads from the
    // daemon's own isolated home.
    const headers = managementToken
      ? { "x-plimsoll-token": managementToken }
      : {};
    const request = http.get(
      `http://127.0.0.1:${port}/status`,
      { timeout: 3_000, headers },
      (response) => {
      let body = "";
      response.on("data", (chunk) => {
        body += chunk;
      });
      response.on("end", () => {
        try {
          resolve(JSON.parse(body) as Record<string, unknown>);
        } catch {
          resolve(null);
        }
      });
    });
    request.on("error", () => resolve(null));
    request.on("timeout", () => {
      request.destroy();
      resolve(null);
    });
  });
}

function waitFor(condition: () => boolean, timeoutMs: number, stepMs = 100): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve) => {
    const tick = () => {
      if (condition()) {
        resolve(true);
        return;
      }
      if (Date.now() > deadline) {
        resolve(false);
        return;
      }
      setTimeout(tick, stepMs);
    };
    tick();
  });
}

async function main() {
  // ---------------------------------------------------------------------------
  // 1. Canonical resolver: validation rules.
  // ---------------------------------------------------------------------------
  const sandbox = privateDir(root, "resolver-sandbox");
  const previousHome = process.env.PLIMSOLL_HOME;

  const validHome = privateDir(sandbox, "valid-home");
  process.env.PLIMSOLL_HOME = validHome;
  try {
    const valid = resolveCollectorHome();
    record(
      "resolver_accepts_private_owned_absolute_home",
      valid.home === path.resolve(validHome) && valid.source === "env",
      valid,
    );
    record(
      "identity_hash_is_path_free_and_stable",
      /^sha256:[0-9a-f]{64}$/.test(collectorHomeIdentityHash(valid.home)) &&
        collectorHomeIdentityHash(validHome) === collectorHomeIdentityHash(path.join(validHome, "trailing", "..")),
      { hash: collectorHomeIdentityHash(valid.home) },
    );

    const absentHome = path.join(sandbox, "not-created-yet");
    process.env.PLIMSOLL_HOME = absentHome;
    record(
      "resolver_allows_absent_home_for_first_run",
      resolveCollectorHome().home === absentHome,
      { absentHome },
    );
  } finally {
    if (previousHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = previousHome;
  }

  const defaultResolved = resolveCollectorHome({
    env: {},
    homeDir: "/some/operator-home",
  });
  record(
    "resolver_default_home_unchanged_without_env",
    defaultResolved.source === "default" &&
      defaultResolved.home === defaultCollectorHome("/some/operator-home"),
    defaultResolved,
  );

  const currentUid = typeof process.getuid === "function" ? process.getuid() : 501;
  const refusals: Array<{ name: string; setup: () => NodeJS.ProcessEnv; uid?: number }> = [
    {
      name: "resolver_refuses_relative_home",
      setup: () => ({ PLIMSOLL_HOME: "relative-canary-home" }),
    },
    {
      name: "resolver_refuses_symlinked_home",
      setup: () => {
        const real = privateDir(sandbox, "symlink-target");
        const link = path.join(sandbox, "canary-link");
        fs.symlinkSync(real, link);
        return { PLIMSOLL_HOME: link };
      },
    },
    {
      name: "resolver_refuses_group_accessible_home",
      setup: () => {
        const loose = privateDir(sandbox, "loose-home");
        fs.chmodSync(loose, 0o755);
        return { PLIMSOLL_HOME: loose };
      },
    },
    {
      name: "resolver_refuses_foreign_owned_home",
      setup: () => ({ PLIMSOLL_HOME: privateDir(sandbox, "foreign-home") }),
      uid: currentUid + 1,
    },
  ];
  for (const refusal of refusals) {
    const env = refusal.setup();
    let threw: CollectorHomeError | null = null;
    try {
      resolveCollectorHome({ env, uid: refusal.uid });
    } catch (error) {
      if (error instanceof CollectorHomeError) threw = error;
    }
    record(refusal.name, threw !== null, {
      code: threw?.code ?? String(threw),
      requested: env.PLIMSOLL_HOME,
    });
  }

  // ---------------------------------------------------------------------------
  // 2. Every derived path binds the resolved home.
  // ---------------------------------------------------------------------------
  const derivedHome = privateDir(sandbox, "derived-home");
  process.env.PLIMSOLL_HOME = derivedHome;
  try {
    const bufferPath = collectorBufferPath();
    const derivedPaths = {
      config: collectorConfigPath(),
      ledger: bufferPath,
      ledgerWal: `${bufferPath}-wal`,
      ledgerShm: `${bufferPath}-shm`,
      pid: collectorLogPath("collector.pid"),
      outLog: collectorLogPath("collector.out.log"),
      errLog: collectorLogPath("collector.err.log"),
      historyWatermark: collectorLogPath("workspace-backfill-state.json"),
      outcomeTimeline: collectorLogPath("outcome-timeline-v1.sqlite"),
      startLock: `${collectorLogPath("collector.pid")}.start.lock`,
    };
    record(
      "all_primary_paths_derive_from_resolved_home",
      Object.values(derivedPaths).every((candidate) => candidate.startsWith(path.resolve(derivedHome))),
      derivedPaths,
    );
    const lifecyclePaths = {
      lifecycleRoot: path.join(derivedHome, "lifecycle"),
      ownershipRoot: derivedHome,
      artifactSourceRoot: path.join(derivedHome, "artifacts"),
      collectorConfig: collectorConfigPath(),
      database: bufferPath,
      serviceManifest: path.join(derivedHome, "lifecycle", "service-manifest.json"),
      ownedToolFragments: [],
      history: [path.join(derivedHome, "history")],
    };
    const stubService = {} as unknown as LifecycleServiceAdapter;
    const stubDatabase = {
      async snapshot() {
        return false;
      },
      async restore(): Promise<void> {},
    };
    const lifecycleAdapter = new FilesystemLifecycleAdapter(lifecyclePaths, stubService, stubDatabase);
    const lockAcquired = await lifecycleAdapter.acquireLock("proof-operation");
    const lockExists = fs.existsSync(path.join(lifecyclePaths.lifecycleRoot, "operation.lock"));
    await lifecycleAdapter.releaseLock("proof-operation");
    record(
      "lifecycle_root_and_operation_lock_bind_same_home",
      lockAcquired && lockExists,
      { lock: path.join(lifecyclePaths.lifecycleRoot, "operation.lock") },
    );
    void lifecycleAdapter;
  } finally {
    if (previousHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = previousHome;
  }

  // ---------------------------------------------------------------------------
  // 3. Doctor binds the validated home identity end to end.
  // ---------------------------------------------------------------------------
  const doctorSandboxHome = privateDir(sandbox, "doctor-home");
  const doctorPlimsollHome = privateDir(sandbox, "doctor-plimsoll");
  const doctor = runCli(["doctor", "--read-only", "--json"], {
    HOME: doctorSandboxHome,
    PLIMSOLL_HOME: doctorPlimsollHome,
  });
  let doctorReceipt: Record<string, unknown> | null = null;
  try {
    doctorReceipt = JSON.parse(doctor.stdout) as Record<string, unknown>;
  } catch {
    doctorReceipt = null;
  }
  const doctorRuntime = doctorReceipt?.runtime as Record<string, unknown> | undefined;
  const doctorHomeSection = doctorRuntime?.home as Record<string, unknown> | undefined;
  record(
    "doctor_reports_validated_home_identity_hash",
    Boolean(doctorReceipt) &&
      doctorHomeSection?.identityHash === collectorHomeIdentityHash(doctorPlimsollHome) &&
      doctorHomeSection?.custom === true &&
      doctorHomeSection?.source === "env",
    { exitCode: doctor.status, identityHash: doctorHomeSection?.identityHash ?? null },
  );
  record(
    "doctor_launchagent_section_carries_home_identity_comparison",
    Boolean(doctorReceipt) &&
      (doctorReceipt!.launchAgent as Record<string, unknown> | null)?.homeIdentity !== undefined,
    { keys: Object.keys(doctorReceipt?.launchAgent as Record<string, unknown> ?? {}) },
  );

  // ---------------------------------------------------------------------------
  // 4. Adversarial: invalid custom homes refuse closed, never fall back.
  // ---------------------------------------------------------------------------
  const untouchedDefaultParent = privateDir(sandbox, "adversarial-home-parent");
  const untouchedDefaultHome = path.join(untouchedDefaultParent, "home");
  fs.mkdirSync(untouchedDefaultHome, { recursive: false });

  const symlinkReal = privateDir(sandbox, "symlink-adversary-real");
  const symlinkPath = path.join(sandbox, "canary-symlink");
  fs.symlinkSync(symlinkReal, symlinkPath);
  const refusedSymlink = runCli(["status"], {
    HOME: untouchedDefaultHome,
    PLIMSOLL_HOME: symlinkPath,
  });
  record(
    "adversarial_symlink_home_refused_with_verbatim_code",
    refusedSymlink.status !== 0 &&
      (refusedSymlink.stderr ?? "").includes("collector_home_home_is_symlink"),
    { exitCode: refusedSymlink.status, stderr: refusedSymlink.stderr?.slice(0, 200) },
  );
  record(
    "adversarial_symlink_refusal_never_falls_back_to_default_home",
    !fs.existsSync(defaultCollectorHome(untouchedDefaultHome)),
    { defaultHome: defaultCollectorHome(untouchedDefaultHome) },
  );

  const refusedRelative = runCli(["status"], {
    HOME: untouchedDefaultHome,
    PLIMSOLL_HOME: "relative-canary-home",
  });
  const relativeArtifact = fs.existsSync(path.join(repoRoot, "relative-canary-home"));
  record(
    "adversarial_relative_home_refused_and_created_nothing",
    refusedRelative.status !== 0 &&
      (refusedRelative.stderr ?? "").includes("collector_home_home_not_absolute") &&
      !relativeArtifact,
    { exitCode: refusedRelative.status, createdInCwd: relativeArtifact },
  );

  const looseHome = privateDir(sandbox, "loose-adversary-home");
  fs.chmodSync(looseHome, 0o755);
  const refusedLoose = runCli(["status"], {
    HOME: untouchedDefaultHome,
    PLIMSOLL_HOME: looseHome,
  });
  record(
    "adversarial_group_accessible_home_refused_before_ledger_open",
    refusedLoose.status !== 0 &&
      (refusedLoose.stderr ?? "").includes("collector_home_home_not_private") &&
      !fs.existsSync(path.join(looseHome, "work-ledger.sqlite")),
    { exitCode: refusedLoose.status, stderr: refusedLoose.stderr?.slice(0, 200) },
  );

  // ---------------------------------------------------------------------------
  // 5. LaunchAgent boundary propagates the validated home.
  // ---------------------------------------------------------------------------
  const fakeBin = privateDir(sandbox, "fake-bin");
  fs.writeFileSync(path.join(fakeBin, "pnpm"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
  const laSandboxHome = privateDir(sandbox, "la-home");
  const laCustomHome = privateDir(sandbox, "la-custom-plimsoll");
  const laOtherHome = privateDir(sandbox, "la-other-plimsoll");

  const dryInstall = runCli(
    ["install-launch-agent", "--dev", "--repo-root", repoRoot, "--pnpm", path.join(fakeBin, "pnpm"), "--dry-run"],
    { HOME: laSandboxHome, PLIMSOLL_HOME: laCustomHome },
  );
  let dryReceipt: Record<string, unknown> | null = null;
  try {
    dryReceipt = JSON.parse(dryInstall.stdout) as Record<string, unknown>;
  } catch {
    dryReceipt = null;
  }
  record(
    "install_receipt_propagates_custom_home_through_allowlisted_env_field",
    Boolean(dryReceipt) &&
      Array.isArray(dryReceipt!.environmentKeys) &&
      (dryReceipt!.environmentKeys as string[]).join(",") ===
        ["PATH", "PLIMSOLL_COLLECTOR_DATA_MODE", "PLIMSOLL_HOME"].join(",") &&
      dryReceipt!.homeIdentityHash === collectorHomeIdentityHash(laCustomHome),
    { environmentKeys: dryReceipt?.environmentKeys, homeIdentityHash: dryReceipt?.homeIdentityHash },
  );

  const realInstall = runCli(
    ["install-launch-agent", "--dev", "--repo-root", repoRoot, "--pnpm", path.join(fakeBin, "pnpm")],
    { HOME: laSandboxHome, PLIMSOLL_HOME: laCustomHome },
  );
  const plistPath = path.join(
    laSandboxHome,
    "Library",
    "LaunchAgents",
    "com.plimsoll.collector.plist",
  );
  let installedPlist: string | null = null;
  try {
    installedPlist = fs.readFileSync(plistPath, "utf8");
  } catch {
    installedPlist = null;
  }
  const plutil = installedPlist
    ? spawnSync("/usr/bin/plutil", ["-convert", "json", "-o", "-", "--", plistPath], {
        encoding: "utf8",
      })
    : null;
  let parsedPlist: Record<string, unknown> | null = null;
  try {
    parsedPlist = JSON.parse(plutil?.stdout ?? "null") as Record<string, unknown>;
  } catch {
    parsedPlist = null;
  }
  const plistEnvironment = parsedPlist?.EnvironmentVariables as Record<string, unknown> | undefined;
  record(
    "installed_manifest_writes_validated_home_into_environment_variables",
    typeof installedPlist === "string" &&
      installedPlist.includes("<key>PLIMSOLL_HOME</key>") &&
      plistEnvironment?.PLIMSOLL_HOME === laCustomHome &&
      realInstall.status === 0,
    { plistPath, exitCode: realInstall.status, observed: plistEnvironment?.PLIMSOLL_HOME ?? null },
  );

  const defaultInstallHome = privateDir(sandbox, "la-default-home");
  const defaultInstall = runCli(
    ["install-launch-agent", "--dev", "--repo-root", repoRoot, "--pnpm", path.join(fakeBin, "pnpm"), "--dry-run"],
    { HOME: defaultInstallHome },
  );
  let defaultDryReceipt: Record<string, unknown> | null = null;
  try {
    defaultDryReceipt = JSON.parse(defaultInstall.stdout) as Record<string, unknown>;
  } catch {
    defaultDryReceipt = null;
  }
  record(
    "default_home_install_stays_byte_compatible_no_propagation",
    Boolean(defaultDryReceipt) &&
      (defaultDryReceipt!.environmentKeys as string[]).length === 2 &&
      defaultDryReceipt!.homeIdentityHash === null,
    { environmentKeys: defaultDryReceipt?.environmentKeys, homeIdentityHash: defaultDryReceipt?.homeIdentityHash },
  );

  // The audit scenario: operator inspects the DEFAULT home while the manifest
  // points launchd at an alternate home. Doctor must call it conflicted.
  const auditScenarioHome = privateDir(sandbox, "audit-home");
  const auditPlistSource = typeof installedPlist === "string"
    ? installedPlist.replaceAll(laCustomHome, laOtherHome)
    : null;
  const auditLaunchAgents = path.join(auditScenarioHome, "Library", "LaunchAgents");
  fs.mkdirSync(auditLaunchAgents, { recursive: true, mode: 0o700 });
  const auditPlistPath = path.join(auditLaunchAgents, "com.plimsoll.collector.plist");
  if (auditPlistSource) fs.writeFileSync(auditPlistPath, auditPlistSource, { mode: 0o600 });
  const auditDoctor = runCli(["doctor", "--read-only", "--json"], { HOME: auditScenarioHome });
  let auditReceipt: Record<string, unknown> | null = null;
  try {
    auditReceipt = JSON.parse(auditDoctor.stdout) as Record<string, unknown>;
  } catch {
    auditReceipt = null;
  }
  const auditLaunchAgent = auditReceipt?.launchAgent as Record<string, unknown> | undefined;
  const auditHomeIdentity = auditLaunchAgent?.homeIdentity as Record<string, unknown> | undefined;
  record(
    "doctor_flags_manifest_pointing_at_alternate_home_while_operator_inspects_default",
    Boolean(auditReceipt) &&
      auditLaunchAgent?.status === "conflicted" &&
      auditHomeIdentity?.ok === false &&
      auditHomeIdentity?.observedHash === collectorHomeIdentityHash(laOtherHome),
    {
      status: auditLaunchAgent?.status ?? null,
      ok: auditHomeIdentity?.ok ?? null,
      observedMatchesOther: auditHomeIdentity?.observedHash === collectorHomeIdentityHash(laOtherHome),
    },
  );
}

// ---------------------------------------------------------------------------
// 6. Machine-global single-owner port across alternate homes (live daemons).
// ---------------------------------------------------------------------------
async function portOwnershipProof() {
  const sandbox = privateDir(root, "port-sandbox");
  const homeA = privateDir(sandbox, "home-a");
  const homeB = privateDir(sandbox, "home-b");
  // Hard guard: live daemons may only ever run inside the lane temp root.
  if (!homeA.startsWith(root) || !homeB.startsWith(root)) {
    throw new Error("proof bug: daemon homes escaped the temp root; refusing to spawn");
  }
  const port = await reserveLoopbackPort();
  for (const home of [homeA, homeB]) {
    fs.writeFileSync(
      path.join(home, "collector.config.json"),
      `${JSON.stringify({ port }, null, 2)}\n`,
      { mode: 0o600 },
    );
  }
  const envFor = (home: string) => cleanEnv({ PLIMSOLL_HOME: home });

  const childA = spawn(process.execPath, ["--import", "tsx", cliSource, "start"], {
    cwd: repoRoot,
    env: envFor(homeA),
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdoutA = "";
  childA.stdout?.on("data", (chunk) => {
    stdoutA += chunk;
  });
  let stderrA = "";
  childA.stderr?.on("data", (chunk) => {
    stderrA += chunk;
  });
  const activeSeen = await waitFor(() => stdoutA.includes('"active"'), 45_000);
  record("alternate_home_collector_starts_against_own_ledger", activeSeen, {
    home: homeA,
    port,
    stdoutHead: stdoutA.slice(0, 200),
  });

  if (activeSeen) {
    const daemonCredential = readLocalIngestAuth(homeA);
    const statusBody = await fetchStatus(port, daemonCredential?.managementRead);
    record(
      "daemon_attests_exact_home_identity_over_status_endpoint",
      statusBody?.homeIdentityHash === collectorHomeIdentityHash(homeA),
      { reported: statusBody?.homeIdentityHash ?? null },
    );

    const challengerB = spawnSync(process.execPath, ["--import", "tsx", cliSource, "start"], {
      cwd: repoRoot,
      encoding: "utf8",
      env: envFor(homeB),
      timeout: 90_000,
    });
    record(
      "second_alternate_home_cannot_bind_the_machine_global_port",
      challengerB.status === 1 && (challengerB.stderr ?? "").includes("port_in_use"),
      { exitCode: challengerB.status, stderrHead: challengerB.stderr?.slice(0, 200) },
    );
    record(
      "challenger_leaves_no_orphan_pid_ownership_in_its_home",
      !fs.existsSync(path.join(homeB, "collector.pid")),
      { home: homeB },
    );

    const doctorWhileRunning = runCli(["doctor", "--read-only", "--json"], { PLIMSOLL_HOME: homeA });
    let runningReceipt: Record<string, unknown> | null = null;
    try {
      runningReceipt = JSON.parse(doctorWhileRunning.stdout) as Record<string, unknown>;
    } catch {
      runningReceipt = null;
    }
    const runningRuntime = runningReceipt?.runtime as Record<string, unknown> | undefined;
    const runningHome = runningRuntime?.home as Record<string, unknown> | undefined;
    record(
      "doctor_confirms_live_daemon_matches_expected_home_identity",
      runningReceipt !== null && runningHome?.daemonHomeMatches === true,
      { daemonHomeMatches: runningHome?.daemonHomeMatches ?? null },
    );
  }

  childA.kill("SIGTERM");
  const exitedCleanly = await waitFor(() => childA.exitCode !== null || childA.signalCode !== null, 20_000);
  record("first_owner_shuts_down_cleanly_after_proof", exitedCleanly && childA.exitCode === 0, {
    exitCode: childA.exitCode,
    signal: childA.signalCode,
    stderrTail: stderrA.slice(-300),
  });
  const pidGone = await waitFor(() => !fs.existsSync(path.join(homeA, "collector.pid")), 5_000);
  record("stopped_owner_removes_its_pid_file_from_its_home", pidGone, { home: homeA });
}

async function run() {
  try {
    await main();
    await portOwnershipProof();
  } finally {
  const durationMs = Date.now() - startedAt;
  const passed = checks.every((check) => check.passed);
  console.log(
    JSON.stringify({
      issue: 135,
      proof: "collector-home-canary-isolation",
      passed,
      checks: checks.length,
      failures: checks.filter((check) => !check.passed).map((check) => check.name),
      durationMs,
      tempRoot: root,
    }),
  );
  fs.rmSync(root, { recursive: true, force: true });
  if (!passed) process.exitCode = 1;
}
}

run().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
  fs.rmSync(root, { recursive: true, force: true });
});
