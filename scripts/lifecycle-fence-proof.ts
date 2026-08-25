/**
 * Issue #158 proof: one fenced lock for every LaunchAgent mutation.
 *
 * Everything runs under temporary sandbox roots with injected service and
 * database adapters. No launchctl, network, browser, installed config, live
 * ledger, or provider is touched. The only subprocesses are three sibling
 * fence-race children allocating revisions from one shared authority root.
 *
 * Run: pnpm proof:lifecycle-fence
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

delete process.env.PLIMSOLL_HOME;

import {
  LifecycleInterruption,
  LifecycleManager,
  type LifecycleReadiness,
  type RuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import {
  LifecycleMutationAuthority,
} from "../packages/collector-cli/src/lifecycle-authority";
import {
  FilesystemLifecycleAdapter,
  type LifecycleDatabaseAdapter,
  type LifecycleServiceAdapter,
  type ManagedLifecyclePaths,
} from "../packages/collector-cli/src/lifecycle-filesystem";
import {
  installLaunchAgent,
  inspectLaunchAgentManifest,
  uninstallLaunchAgent,
} from "../packages/collector-cli/src/launch-agent";

type Check = { name: string; passed: boolean; detail: unknown };
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  const row = { name, passed: Boolean(condition), detail };
  checks.push(row);
  console.log(`${row.passed ? "PASS" : "FAIL"} ${name}`);
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function digest(file: string): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}`;
}

function writeExecutable(target: string, body: string, mode = 0o700) {
  fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
  fs.writeFileSync(target, body, { mode });
}

const NODE_MAJOR = Number(process.versions.node.split(".")[0]);

type Fixture = ReturnType<typeof fixture>;

function fixture(name: string) {
  const ownershipRoot = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-fence-${name}-`));
  const paths: ManagedLifecyclePaths = {
    ownershipRoot,
    lifecycleRoot: path.join(ownershipRoot, "private", "lifecycle"),
    artifactSourceRoot: path.join(ownershipRoot, "artifacts"),
    collectorConfig: path.join(ownershipRoot, "private", "collector.config.json"),
    database: path.join(ownershipRoot, "private", "work-ledger.sqlite"),
    serviceManifest: path.join(ownershipRoot, "Library", "LaunchAgents", "com.plimsoll.collector.plist"),
    ownedToolFragments: [],
    history: [],
  };
  fs.mkdirSync(path.dirname(paths.database), { recursive: true, mode: 0o700 });
  fs.writeFileSync(paths.database, "ledger-v1\n", { mode: 0o600 });
  let runtimeVersion: string | null = null;
  let restoreCount = 0;
  let activateDelayMs = 0;
  let snapshotDelayMs = 0;
  const service: LifecycleServiceAdapter = {
    async activate(input) {
      if (activateDelayMs > 0) await sleep(activateDelayMs);
      runtimeVersion = input.version;
    },
    async restore(input) {
      restoreCount += 1;
      runtimeVersion = input.version;
    },
    async remove() {
      runtimeVersion = null;
    },
    async readiness(expectedVersion): Promise<LifecycleReadiness> {
      const ready = runtimeVersion === expectedVersion;
      return {
        ready,
        runtimeVersion,
        serviceReady: ready,
        configCompatible: ready,
        databaseCompatible: ready,
        reason: ready ? "ready" : "runtime_mismatch",
      };
    },
    async supportSnapshot() {
      throw new Error("support snapshot is not exercised by this proof");
    },
  };
  const database: LifecycleDatabaseAdapter = {
    async snapshot({ destination }) {
      if (snapshotDelayMs > 0) await sleep(snapshotDelayMs);
      fs.copyFileSync(paths.database, destination);
      return true;
    },
    async restore({ source, destination }) {
      fs.copyFileSync(source, destination);
    },
  };
  const artifactSource = path.join(paths.artifactSourceRoot, "bundle.mjs");
  writeExecutable(artifactSource, "console.log('runtime');\n");
  const artifact: RuntimeArtifact = {
    version: "9.9.9",
    platform: "darwin",
    architecture: process.arch === "arm64" ? "arm64" : "x64",
    nodeMajor: NODE_MAJOR,
    sha256: digest(artifactSource),
    sourcePath: artifactSource,
  };
  const counters = {
    get runtimeVersion() { return runtimeVersion; },
    get restoreCount() { return restoreCount; },
  };
  const delays = {
    set activate(ms: number) { activateDelayMs = ms; },
    set snapshot(ms: number) { snapshotDelayMs = ms; },
  };
  const compose = (authority?: LifecycleMutationAuthority) =>
    new FilesystemLifecycleAdapter(paths, service, database, authority);
  return { paths, compose, artifact, counters, delays };
}

async function main() {
  // ---- Authority primitives ------------------------------------------------
  const authRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fence-auth-"));
  const authority = new LifecycleMutationAuthority(path.join(authRoot, "authority"));

  const first = authority.acquire();
  check("first_acquire_returns_revision_one",
    first.kind === "acquired" && first.lease.revision === 1,
    first.kind === "acquired" ? first.lease.revision : first);

  const second = authority.acquire();
  check("second_acquire_is_literal_busy_with_holder_revision_and_deadline",
    second.kind === "busy" &&
    second.reason === "held_by_current_owner" &&
    second.currentRevision === 1 &&
    second.busyUntilMs === (first.kind === "acquired" ? first.lease.expiresAtMs : -1),
    second);

  const observedHeld = authority.observe();
  check("observe_reports_held_while_lease_is_current",
    observedHeld.kind === "held" && observedHeld.currentRevision === 1, observedHeld);

  check("authority_directories_are_private_0700",
    (() => {
      const authorityDir = path.join(authRoot, "authority");
      const leasesDir = path.join(authorityDir, "leases");
      return (fs.lstatSync(authorityDir).mode & 0o7777) === 0o700 &&
        (fs.lstatSync(leasesDir).mode & 0o7777) === 0o700;
    })(), null);
  check("lease_records_are_private_0600_regular_files",
    (() => {
      const leases = path.join(authRoot, "authority", "leases");
      return fs.readdirSync(leases).every((name) => {
        const stat = fs.lstatSync(path.join(leases, name));
        return stat.isFile() && !stat.isSymbolicLink() && (stat.mode & 0o7777) === 0o600;
      });
    })(), null);

  const releaseOne = first.kind === "acquired" ? first.lease.release() : "missing";
  check("owner_release_marks_released", releaseOne === "released", releaseOne);

  const staleAssertAfterRelease = first.kind === "acquired" ? first.lease.assertCurrent() : null;
  check("released_lease_revalidates_as_released",
    staleAssertAfterRelease !== null && !staleAssertAfterRelease.ok &&
    staleAssertAfterRelease.reason === "released",
    staleAssertAfterRelease);

  const observedFree = authority.observe();
  check("observe_reports_free_after_release", observedFree.kind === "free", observedFree);

  const third = authority.acquire();
  check("reacquire_advances_monotonic_revision",
    third.kind === "acquired" && third.lease.revision === 2,
    third.kind === "acquired" ? third.lease.revision : third);
  if (third.kind === "acquired") third.lease.release();

  let previousRevision = 2;
  let strictlyIncreasing = true;
  for (let index = 0; index < 5; index += 1) {
    const acquisition = authority.acquire();
    if (acquisition.kind !== "acquired") { strictlyIncreasing = false; break; }
    strictlyIncreasing &&= acquisition.lease.revision > previousRevision;
    previousRevision = acquisition.lease.revision;
    acquisition.lease.release();
  }
  check("sequential_acquisitions_strictly_increase_revisions",
    strictlyIncreasing, previousRevision);

  check("busy_and_observation_literals_are_path_free",
    !JSON.stringify([second, observedHeld, observedFree]).includes(authRoot), null);

  // ---- Expiry and supersession --------------------------------------------
  const expiryRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fence-expiry-"));
  const expiryAuthority = new LifecycleMutationAuthority(path.join(expiryRoot, "authority"));
  const slow = expiryAuthority.acquire({ leaseMs: 60 });
  check("short_lease_acquired", slow.kind === "acquired", slow.kind);
  await sleep(140);

  const successor = expiryAuthority.acquire({ leaseMs: 220 });
  check("expired_owner_is_superseded_by_a_higher_revision",
    successor.kind === "acquired" && successor.lease.revision === 2,
    successor.kind === "acquired" ? successor.lease.revision : successor.kind);

  const slowRevalidation = slow.kind === "acquired" ? slow.lease.assertCurrent() : null;
  check("slow_owner_revalidates_as_superseded",
    slowRevalidation !== null && !slowRevalidation.ok && slowRevalidation.reason === "superseded",
    slowRevalidation);

  const staleReleaseOutcome = slow.kind === "acquired" ? slow.lease.release() : "missing";
  const successorStillCurrent =
    successor.kind === "acquired" ? successor.lease.assertCurrent() : null;
  check("stale_release_cannot_affect_the_successor",
    successorStillCurrent !== null && successorStillCurrent.ok && successorStillCurrent.revision === 2,
    { staleReleaseOutcome, successorStillCurrent });

  // The successor's short lease is left unreleased on purpose: once it
  // expires, the domain must be free again without any release call.
  await sleep(300);
  check("expired_unreleased_lease_leaves_domain_free",
    expiryAuthority.observe().kind === "free", expiryAuthority.observe());
  const afterExpiry = expiryAuthority.acquire();
  check("acquire_after_expiry_allocates_the_next_revision",
    afterExpiry.kind === "acquired" && afterExpiry.lease.revision === 3,
    afterExpiry.kind === "acquired" ? afterExpiry.lease.revision : afterExpiry);
  if (afterExpiry.kind === "acquired") afterExpiry.lease.release();

  // ---- Ambiguity authorizes nothing ---------------------------------------
  const ambiguityScenarios = [
    {
      name: "unexpected_entry",
      plant: (leases: string) => fs.writeFileSync(path.join(leases, "junk.txt"), "x", { mode: 0o600 }),
    },
    {
      name: "record_malformed",
      plant: (leases: string) =>
        fs.writeFileSync(path.join(leases, `${"9".repeat(20)}.json`), "{broken", { mode: 0o600 }),
    },
    {
      name: "record_unreadable_symlink",
      plant: (leases: string) => {
        const outside = path.join(path.dirname(leases), "outside-target.json");
        fs.writeFileSync(outside, "{}\n", { mode: 0o600 });
        fs.symlinkSync(outside, path.join(leases, `${"8".repeat(20)}.json`));
      },
    },
  ] as const;
  for (const scenario of ambiguityScenarios) {
    const scenarioRoot = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-fence-amb-${scenario.name}-`));
    const scenarioAuthority = new LifecycleMutationAuthority(path.join(scenarioRoot, "authority"));
    const seed = scenarioAuthority.acquire({ leaseMs: 5_000 });
    if (seed.kind !== "acquired") throw new Error(`seed acquire failed for ${scenario.name}`);
    seed.lease.release();
    scenario.plant(path.join(scenarioRoot, "authority", "leases"));
    const beforeEntries = fs.readdirSync(path.join(scenarioRoot, "authority", "leases")).sort().join("|");
    const acquireOutcome = scenarioAuthority.acquire();
    const observeOutcome = scenarioAuthority.observe();
    const afterEntries = fs.readdirSync(path.join(scenarioRoot, "authority", "leases")).sort().join("|");
    check(`ambiguous_${scenario.name}_fails_closed`,
      acquireOutcome.kind === "ambiguous" && observeOutcome.kind === "ambiguous",
      { acquire: acquireOutcome, observe: observeOutcome });
    check(`ambiguous_${scenario.name}_performs_no_destructive_cleanup`,
      afterEntries === beforeEntries, { before: beforeEntries, after: afterEntries });
  }

  // ---- Cross-process allocation race --------------------------------------
  const raceRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fence-race-"));
  const raceAuthorityRoot = path.join(raceRoot, "authority");
  const childScript = path.resolve("scripts/fixtures/lifecycle-fence-child.ts");
  const children = [1, 2, 3].map((id) => spawnSync(process.execPath, [
    "--import", "tsx", childScript, raceAuthorityRoot, "8",
  ], {
    encoding: "utf8",
    timeout: 120_000,
    env: { ...process.env, FENCE_CHILD_ID: String(id) },
  }));
  const allocatedRevisions: number[] = [];
  for (const [index, child] of children.entries()) {
    check(`fence_race_child_${index + 1}_exits_zero`, child.status === 0,
      { status: child.status, stderr: child.stderr.slice(-500) });
    for (const line of child.stdout.trim().split("\n").filter(Boolean)) {
      const row = JSON.parse(line) as { kind: string; revision?: number };
      if (row.kind === "acquired" && typeof row.revision === "number") allocatedRevisions.push(row.revision);
    }
  }
  allocatedRevisions.sort((left, right) => left - right);
  const expectedRevisions = Array.from({ length: 24 }, (_, index) => index + 1);
  check("concurrent_processes_allocate_unique_contiguous_monotonic_revisions",
    allocatedRevisions.length === 24 &&
    allocatedRevisions.every((revision, index) => revision === expectedRevisions[index]),
    { allocated: allocatedRevisions.length, unique: new Set(allocatedRevisions).size });

  // ---- Adapter lock domain unification ------------------------------------
  {
    const fx = fixture("adapter");
    const sharedAuthority = new LifecycleMutationAuthority(
      path.join(fx.paths.lifecycleRoot, "authority"), { defaultLeaseMs: 60_000 });
    const adapterA = fx.compose(sharedAuthority);
    const adapterB = fx.compose(sharedAuthority);
    check("adapter_a_acquires_shared_mutation_lock", await adapterA.acquireLock("op-a"), null);
    check("adapter_b_refused_while_a_holds_the_same_domain",
      !(await adapterB.acquireLock("op-b")), null);
    await adapterA.releaseLock("op-a");
    check("adapter_b_acquires_after_release", await adapterB.acquireLock("op-b"), null);
    let staleReleaseThrew: unknown = null;
    try {
      // A no longer holds anything; this must stay inert.
      await adapterA.releaseLock("op-a");
      await adapterA.assertFence?.("op-a").catch(() => undefined);
    } catch (error) {
      staleReleaseThrew = error;
    }
    check("stale_foreign_release_is_inert", staleReleaseThrew === null, staleReleaseThrew);
    let fenceBError: unknown = null;
    try {
      await adapterB.assertFence?.("op-b");
    } catch (error) {
      fenceBError = error;
    }
    check("b_lease_still_current_after_foreign_stale_release", fenceBError === null, String(fenceBError));
  }

  // ---- Manager: busy refusal across the unified domain ---------------------
  {
    const fx = fixture("manager-busy");
    const sharedAuthority = new LifecycleMutationAuthority(
      path.join(fx.paths.lifecycleRoot, "authority"), { defaultLeaseMs: 60_000 });
    const holder = sharedAuthority.acquire();
    check("external_holder_acquires_first", holder.kind === "acquired", holder.kind);
    let refusal = "";
    try {
      await new LifecycleManager(fx.compose(sharedAuthority), {})
        .update({ operationId: "busy-op", artifact: fx.artifact });
    } catch (error) {
      refusal = error instanceof Error ? error.message : String(error);
    }
    check("update_refused_while_mutation_lease_is_busy",
      /another lifecycle operation owns the lock/.test(refusal), refusal);
    check("refused_update_touched_no_runtime", fx.counters.runtimeVersion === null, fx.counters.runtimeVersion);
    if (holder.kind === "acquired") holder.lease.release();
  }

  // ---- Manager: expired fence interrupts without auto-rollback -------------
  {
    const fx = fixture("expiry");
    fx.delays.snapshot = 260;
    const shortLease = new LifecycleMutationAuthority(
      path.join(fx.paths.lifecycleRoot, "authority"), { defaultLeaseMs: 120 });
    let interruption: unknown = null;
    try {
      await new LifecycleManager(fx.compose(shortLease), {})
        .update({ operationId: "expiring-op", artifact: fx.artifact });
    } catch (error) {
      interruption = error;
    }
    check("expired_fence_interrupts_update_with_lifecycle_interruption",
      interruption instanceof LifecycleInterruption, String(interruption));
    check("interrupted_operation_never_switched_and_never_restored",
      fx.counters.runtimeVersion === null && fx.counters.restoreCount === 0,
      { runtimeVersion: fx.counters.runtimeVersion, restores: fx.counters.restoreCount });

    const freshLongLease = new LifecycleMutationAuthority(
      path.join(fx.paths.lifecycleRoot, "authority"), { defaultLeaseMs: 60_000 });
    const receipt = await new LifecycleManager(fx.compose(freshLongLease), {})
      .update({ operationId: "expiring-op", artifact: fx.artifact });
    check("reopen_resumes_interrupted_operation_to_completed",
      receipt.status === "completed" && receipt.toVersion === fx.artifact.version,
      receipt.status);
  }

  // ---- Manager: superseded owner cannot roll back a successor's world ------
  {
    const fx = fixture("supersede");
    fx.delays.snapshot = 300;
    const sharedAuthority = new LifecycleMutationAuthority(
      path.join(fx.paths.lifecycleRoot, "authority"), { defaultLeaseMs: 150 });
    const updateSettled = new Promise<unknown>((resolve) => {
      new LifecycleManager(fx.compose(sharedAuthority), {})
        .update({ operationId: "superseded-op", artifact: fx.artifact })
        .then(resolve, resolve);
    });
    await sleep(400);
    const takeover = sharedAuthority.acquire({ leaseMs: 60_000 });
    check("successor_overtook_the_expired_stale_owner",
      takeover.kind === "acquired", takeover.kind);
    const outcome = await updateSettled;
    check("superseded_owner_fails_closed_with_interruption",
      outcome instanceof LifecycleInterruption, String(outcome));
    check("superseded_owner_ran_no_automatic_rollback",
      fx.counters.restoreCount === 0, fx.counters.restoreCount);
    const journal = await fx.compose(sharedAuthority).readJournal();
    check("superseded_owner_journal_is_still_resumable",
      journal !== null && journal.operationId === "superseded-op" && journal.phase === "snapshotted",
      journal);
    if (takeover.kind === "acquired") takeover.lease.release();

    const resumed = await new LifecycleManager(fx.compose(sharedAuthority), {})
      .update({ operationId: "superseded-op", artifact: fx.artifact });
    check("resumed_operation_completes_under_the_current_owner",
      resumed.status === "completed", resumed.status);
  }

  // ---- Install/uninstall fencing ------------------------------------------
  {
    const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fence-install-"));
    const home = path.join(sandbox, "home");
    fs.mkdirSync(home, { mode: 0o700 });
    const syntheticPnpm = path.join(sandbox, "runtime", "pnpm");
    writeExecutable(syntheticPnpm, "#!/bin/sh\nexit 0\n");
    const installAuthority = new LifecycleMutationAuthority(
      path.join(sandbox, "plimsoll-home", "lifecycle-authority"));

    const foreign = installAuthority.acquire({ leaseMs: 60_000 });
    check("foreign_mutation_lease_held_before_install", foreign.kind === "acquired", foreign.kind);

    let installBusyCode = "";
    try {
      installLaunchAgent({
        homeDir: home,
        repoRoot: path.join(sandbox, "repo-a"),
        pnpmPath: syntheticPnpm,
        mutationAuthority: installAuthority,
      });
    } catch (error) {
      installBusyCode = error instanceof Error ? error.message : String(error);
    }
    check("install_refuses_with_literal_busy_code",
      installBusyCode.endsWith("LAUNCH_AGENT_LIFECYCLE_FENCE_BUSY"), installBusyCode);
    check("refused_install_left_no_manifest_behind",
      !fs.existsSync(path.join(home, "Library", "LaunchAgents", "com.plimsoll.collector.plist")), null);

    const preview = installLaunchAgent({
      homeDir: home, repoRoot: path.join(sandbox, "repo-a"), pnpmPath: syntheticPnpm,
      dryRun: true, mutationAuthority: installAuthority,
    });
    check("preview_install_never_acquires_the_lease",
      preview.receipt.status === "preview", preview.receipt.status);

    let inspectThrew: unknown = null;
    try {
      const inspected = inspectLaunchAgentManifest({ homeDir: home });
      check("read_only_manifest_inspection_works_without_the_lease",
        inspected.ok === false && inspected.status === "missing", inspected);
    } catch (error) {
      inspectThrew = error;
    }
    check("read_only_inspection_does_not_touch_the_fence", inspectThrew === null, String(inspectThrew));

    if (foreign.kind === "acquired") foreign.lease.release();

    const installed = installLaunchAgent({
      homeDir: home, repoRoot: path.join(sandbox, "repo-a"), pnpmPath: syntheticPnpm,
      mutationAuthority: installAuthority,
    });
    check("install_succeeds_once_the_lease_is_free",
      installed.receipt.status === "installed", installed.receipt.status);

    const blocker = installAuthority.acquire({ leaseMs: 60_000 });
    check("blocking_lease_held_for_uninstall", blocker.kind === "acquired", blocker.kind);
    let uninstallBusyCode = "";
    try {
      uninstallLaunchAgent({ homeDir: home, mutationAuthority: installAuthority });
    } catch (error) {
      uninstallBusyCode = error instanceof Error ? error.message : String(error);
    }
    check("uninstall_refuses_with_literal_busy_code",
      uninstallBusyCode.endsWith("LAUNCH_AGENT_LIFECYCLE_FENCE_BUSY"), uninstallBusyCode);
    check("refused_uninstall_left_the_manifest_in_place",
      fs.existsSync(installed.plistPath), null);
    const uninstallPreview = uninstallLaunchAgent({
      homeDir: home, dryRun: true, mutationAuthority: installAuthority,
    });
    check("uninstall_preview_never_acquires_the_lease",
      uninstallPreview.receipt.status === "preview", uninstallPreview.receipt.status);
    if (blocker.kind === "acquired") blocker.lease.release();

    const removed = uninstallLaunchAgent({ homeDir: home, mutationAuthority: installAuthority });
    check("uninstall_removes_exactly_after_regaining_the_fence",
      removed.receipt.status === "removed" && !fs.existsSync(installed.plistPath),
      removed.receipt.status);
  }

  console.log(JSON.stringify({
    proof: "lifecycle-fence",
    checks: checks.length,
    passed: checks.filter((row) => row.passed).length,
    failed: checks.filter((row) => !row.passed).map((row) => row.name),
    liveStateTouched: false,
  }));
}

main().then(() => {
  process.exit(checks.every((row) => row.passed) ? 0 : 1);
}).catch((error: unknown) => {
  console.error(String(error));
  process.exit(1);
});
