#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";

import {
  createResourceSandbox,
  removeResourceSandbox,
  runDuplicateStartSingleOwnerContract,
} from "./scenarios";
import { writeResourceReceiptAtomically } from "./index";

type PrivacyFixture = {
  schemaVersion: number;
  prefixLength: number;
  sentinels: Record<string, string>;
};

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const finalizationProofPath = fileURLToPath(import.meta.url);
const parentChangeFixturePath = path.join(
  repoRoot,
  "scripts",
  "resource-proof",
  "fixtures",
  "receipt-parent-change-fixture.mjs",
);
let finalizationStage = "startup";

function within(parent: string, candidate: string) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

type BoundedChildOutcome = {
  status: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  outputExceeded: boolean;
  forcedKill: boolean;
  reapTimedOut: boolean;
};

function spawnBoundedJsonChild(
  args: string[],
  input: string,
  env: NodeJS.ProcessEnv,
) {
  const child = spawn(process.execPath, args, {
    cwd: repoRoot,
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let timedOut = false;
  let outputExceeded = false;
  let forcedKill = false;
  let reapTimedOut = false;
  let settled = false;
  let completionTimeout: NodeJS.Timeout | undefined;
  let killTimeout: NodeJS.Timeout | undefined;
  let reapTimeout: NodeJS.Timeout | undefined;
  let resolveOutcome!: (outcome: BoundedChildOutcome) => void;
  const completion = new Promise<BoundedChildOutcome>((resolve) => {
    resolveOutcome = resolve;
  });
  const finish = (status: number | null, signal: NodeJS.Signals | null) => {
    if (settled) return;
    settled = true;
    if (completionTimeout) clearTimeout(completionTimeout);
    if (killTimeout) clearTimeout(killTimeout);
    if (reapTimeout) clearTimeout(reapTimeout);
    resolveOutcome({
      status,
      signal,
      stdout,
      stderr,
      timedOut,
      outputExceeded,
      forcedKill,
      reapTimedOut,
    });
  };
  const beginTermination = () => {
    if (settled || killTimeout || reapTimeout) return;
    child.kill("SIGTERM");
    killTimeout = setTimeout(() => {
      if (settled) return;
      forcedKill = true;
      child.kill("SIGKILL");
      reapTimeout = setTimeout(() => {
        if (settled) return;
        reapTimedOut = true;
        child.unref();
        finish(null, "SIGKILL");
      }, 3_000);
    }, 3_000);
  };
  child.once("error", () => {
    // `close` still supplies the bounded symbolic outcome; never emit raw errors.
  });
  child.once("close", (status, signal) => {
    finish(status, signal);
  });
  const append = (current: string, chunk: Buffer) => {
    const next = current + chunk.toString("utf8");
    if (Buffer.byteLength(next) > 64 * 1024) {
      outputExceeded = true;
      beginTermination();
      return current;
    }
    return next;
  };
  child.stdout.on("data", (chunk: Buffer) => {
    stdout = append(stdout, chunk);
  });
  child.stderr.on("data", (chunk: Buffer) => {
    stderr = append(stderr, chunk);
  });
  child.stdin.end(input);
  completionTimeout = setTimeout(() => {
    timedOut = true;
    beginTermination();
  }, 40_000);
  return {
    completion,
    terminateAndReap: async () => {
      beginTermination();
      return completion;
    },
  };
}

function parseChildReceipt(output: string) {
  try {
    const parsed = JSON.parse(output) as Record<string, unknown>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

async function waitForFixtureBarrier(candidate: string) {
  const deadline = Date.now() + 10_000;
  for (;;) {
    try {
      const stat = fs.lstatSync(candidate);
      if (!stat.isFile() || stat.isSymbolicLink()) {
        throw new Error("FixtureBarrierInvalid");
      }
      return;
    } catch (error) {
      if (error instanceof Error && error.message === "FixtureBarrierInvalid") {
        throw error;
      }
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw new Error("FixtureBarrierUnavailable");
      }
    }
    if (Date.now() >= deadline) throw new Error("FixtureBarrierTimeout");
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

function digestTree(directory: string) {
  const hash = createHash("sha256");
  const visit = (current: string) => {
    for (const entry of fs
      .readdirSync(current, { withFileTypes: true })
      .sort((left, right) => left.name.localeCompare(right.name))) {
      const full = path.join(current, entry.name);
      const relative = path.relative(directory, full);
      const stat = fs.lstatSync(full);
      hash.update(`${entry.isDirectory() ? "d" : "f"}\0${relative}\0${stat.mode & 0o777}\0`);
      if (entry.isDirectory()) visit(full);
      else hash.update(fs.readFileSync(full));
    }
  };
  visit(directory);
  return hash.digest("hex");
}

function errorName(work: () => void) {
  try {
    work();
    return "none";
  } catch (error) {
    return error instanceof Error ? error.name : "UnknownError";
  }
}

function runReceiptWriteAdversaries(tempRoot: string) {
  const fixtureRoot = path.join(tempRoot, "receipt-writer");
  fs.mkdirSync(fixtureRoot, { mode: 0o700 });
  const serialized = '{"schema":"receipt-adversary","complete":true}\n';

  finalizationStage = "receipt_write_symlink";
  const externalTarget = path.join(fixtureRoot, "external-target");
  const symlinkDestination = path.join(fixtureRoot, "symlink-receipt.json");
  fs.writeFileSync(externalTarget, "external-must-not-change\n", { mode: 0o600 });
  fs.symlinkSync(externalTarget, symlinkDestination);
  writeResourceReceiptAtomically(symlinkDestination, serialized);
  const symlinkDestinationStat = fs.lstatSync(symlinkDestination);

  finalizationStage = "receipt_write_nonregular";
  const nonregularDestination = path.join(fixtureRoot, "nonregular-receipt.json");
  fs.mkdirSync(nonregularDestination, { mode: 0o700 });
  const nonregularFailure = errorName(() =>
    writeResourceReceiptAtomically(nonregularDestination, serialized),
  );

  finalizationStage = "receipt_write_partial";
  const partialDestination = path.join(fixtureRoot, "partial-receipt.json");
  fs.writeFileSync(partialDestination, "previous-complete-receipt\n", { mode: 0o600 });
  const partialBefore = fs.readFileSync(partialDestination, "utf8");
  const partialFailure = errorName(() =>
    writeResourceReceiptAtomically(partialDestination, serialized, {
      injectFailureAfterBytes: 7,
    }),
  );
  const partialResidue = fs
    .readdirSync(fixtureRoot)
    .filter((name) => name.startsWith(".partial-receipt.json.") && name.endsWith(".tmp"));

  finalizationStage = "receipt_write_stable_temp_alias";
  const temporaryAlias = path.resolve(os.tmpdir());
  const canonicalTemporaryRoot = fs.realpathSync.native(temporaryAlias);
  let stableTemporaryRootAliasNormalized = true;
  if (
    temporaryAlias !== canonicalTemporaryRoot &&
    within(canonicalTemporaryRoot, tempRoot)
  ) {
    const aliasedTempRoot = path.join(
      temporaryAlias,
      path.relative(canonicalTemporaryRoot, tempRoot),
    );
    const canonicalAliasParent = path.join(tempRoot, "stable-temp-alias");
    const aliasedReceipt = path.join(
      aliasedTempRoot,
      "stable-temp-alias",
      "receipt.json",
    );
    const canonicalAliasedReceipt = path.join(
      canonicalAliasParent,
      "receipt.json",
    );
    fs.mkdirSync(canonicalAliasParent, { mode: 0o700 });
    writeResourceReceiptAtomically(aliasedReceipt, serialized);
    const aliasedReceiptStat = fs.lstatSync(canonicalAliasedReceipt);
    stableTemporaryRootAliasNormalized =
      aliasedReceiptStat.isFile() &&
      !aliasedReceiptStat.isSymbolicLink() &&
      (aliasedReceiptStat.mode & 0o777) === 0o600 &&
      fs.readFileSync(canonicalAliasedReceipt, "utf8") === serialized;
  }

  finalizationStage = "receipt_write_ancestor_alias";
  const externalAncestorRoot = path.join(tempRoot, "ancestor-external");
  const externalNested = path.join(externalAncestorRoot, "nested");
  const externalReceipt = path.join(externalNested, "receipt.json");
  fs.mkdirSync(externalNested, { recursive: true, mode: 0o700 });
  fs.writeFileSync(externalReceipt, "external-ancestor-target\n", { mode: 0o600 });
  const ancestorAlias = path.join(fixtureRoot, "ancestor-alias");
  fs.symlinkSync(externalAncestorRoot, ancestorAlias);
  const escapedReceipt = path.join(ancestorAlias, "nested", "receipt.json");
  const ancestorFailure = errorName(() =>
    writeResourceReceiptAtomically(escapedReceipt, serialized),
  );
  const externalTempResidue = fs
    .readdirSync(externalNested)
    .filter((name) => name.startsWith(".receipt.json.") && name.endsWith(".tmp"));

  finalizationStage = "receipt_write_parent_change";
  const raceParent = path.join(tempRoot, "receipt-race-parent");
  const raceDisplacedParent = path.join(tempRoot, "receipt-race-displaced");
  const raceReceipt = path.join(raceParent, "receipt.json");
  fs.mkdirSync(raceParent, { mode: 0o700 });
  fs.writeFileSync(raceReceipt, "original-parent-prior-receipt\n", { mode: 0o600 });
  const raceFailure = errorName(() =>
    writeResourceReceiptAtomically(raceReceipt, serialized, {
      injectParentReplacementAfterTempWrite: {
        displacedParent: raceDisplacedParent,
        replacementReceipt: "replacement-parent-prior-receipt\n",
      },
    }),
  );
  finalizationStage = /^ResourceReceipt[A-Za-z]+$/.test(raceFailure)
    ? `receipt_write_parent_change_${raceFailure}`
    : "receipt_write_parent_change_unknown";
  const raceResidue = [raceParent, raceDisplacedParent].flatMap((directory) =>
    fs.existsSync(directory)
      ? fs
          .readdirSync(directory)
          .filter((name) => /^\.receipt\.json\..*\.(tmp|backup|failed)$/.test(name))
      : ["missing-directory"],
  );

  finalizationStage = "receipt_write_assertions";
  return {
    symlinkNeutralizedWithoutFollowing:
      fs.readFileSync(externalTarget, "utf8") === "external-must-not-change\n" &&
      symlinkDestinationStat.isFile() &&
      !symlinkDestinationStat.isSymbolicLink() &&
      (symlinkDestinationStat.mode & 0o777) === 0o600 &&
      fs.readFileSync(symlinkDestination, "utf8") === serialized,
    nonregularDestinationRejected:
      nonregularFailure === "ResourceReceiptDestinationInvalid" &&
      fs.lstatSync(nonregularDestination).isDirectory(),
    partialWriteFailurePreservesPriorReceipt:
      partialFailure === "ResourceReceiptInjectedWriteFailure" &&
      fs.readFileSync(partialDestination, "utf8") === partialBefore &&
      (fs.statSync(partialDestination).mode & 0o777) === 0o600 &&
      partialResidue.length === 0,
    stableTemporaryRootAliasNormalized,
    ancestorSymlinkEscapeRejected:
      ancestorFailure === "ResourceReceiptAncestorInvalid" &&
      fs.lstatSync(ancestorAlias).isSymbolicLink() &&
      fs.readFileSync(externalReceipt, "utf8") === "external-ancestor-target\n" &&
      externalTempResidue.length === 0,
    parentReplacementRaceFailsWithoutOverwrite:
      raceFailure === "ResourceReceiptAncestorChanged" &&
      fs.existsSync(raceDisplacedParent) &&
      fs.existsSync(raceReceipt) &&
      fs.readFileSync(path.join(raceDisplacedParent, "receipt.json"), "utf8") ===
        "original-parent-prior-receipt\n" &&
      fs.readFileSync(raceReceipt, "utf8") ===
        "replacement-parent-prior-receipt\n" &&
      raceResidue.length === 0 &&
      !raceFailure.includes(tempRoot),
  };
}

async function runSeparateProcessParentChangeProof(tempRoot: string) {
  const fixtureRoot = path.join(tempRoot, "separate-process-parent-change");
  const receiptParent = path.join(fixtureRoot, "receipt-parent");
  const displacedParent = path.join(fixtureRoot, "receipt-parent-displaced");
  const barrierDirectory = path.join(fixtureRoot, "barrier");
  const isolatedHome = path.join(fixtureRoot, "home");
  const receiptPath = path.join(receiptParent, "receipt.json");
  const boundarySentinel = path.join(tempRoot, "separate-process-boundary-sentinel");
  const priorReceipt = "separate-process-prior-receipt\n";
  const replacementReceipt = "separate-process-replacement-receipt\n";
  const attemptedReceipt = '{"schema":"separate-process-proof","complete":true}\n';

  fs.mkdirSync(fixtureRoot, { mode: 0o700 });
  fs.mkdirSync(receiptParent, { mode: 0o700 });
  fs.mkdirSync(barrierDirectory, { mode: 0o700 });
  fs.mkdirSync(isolatedHome, { mode: 0o700 });
  fs.writeFileSync(receiptPath, priorReceipt, { mode: 0o600 });
  fs.writeFileSync(boundarySentinel, "outside-fixture-must-not-change\n", {
    mode: 0o600,
  });
  const boundaryBefore = fs.readFileSync(boundarySentinel);

  const childEnvironment: NodeJS.ProcessEnv = {
    HOME: isolatedHome,
    USERPROFILE: isolatedHome,
    TMPDIR: fixtureRoot,
    TMP: fixtureRoot,
    TEMP: fixtureRoot,
    TZ: "UTC",
    LANG: "C",
    LC_ALL: "C",
  };
  const racerRequest = JSON.stringify({
    fixtureRoot,
    parent: receiptParent,
    displacedParent,
    barrierDirectory,
    finalName: "receipt.json",
    replacementReceipt,
  });
  const racer = spawnBoundedJsonChild(
    [parentChangeFixturePath],
    racerRequest,
    childEnvironment,
  );
  let racerOutcome: BoundedChildOutcome | undefined;
  try {
    await waitForFixtureBarrier(path.join(barrierDirectory, "racer-ready"));
    const writerFailure = errorName(() =>
      writeResourceReceiptAtomically(receiptPath, attemptedReceipt, {
        testBarrierAfterTempWrite: { directory: barrierDirectory },
      }),
    );
    const parentAbsentAfterWriterReturn = !fs.existsSync(receiptParent);
    fs.writeFileSync(path.join(barrierDirectory, "writer-done"), "done\n", {
      flag: "wx",
      mode: 0o600,
    });
    racerOutcome = await racer.completion;
    const racerReceipt = parseChildReceipt(racerOutcome.stdout);
    const receiptResidue = [receiptParent, displacedParent].flatMap(
      (directory) =>
        fs.existsSync(directory)
          ? fs
              .readdirSync(directory)
              .filter((name) =>
                /^\.receipt\.json\..*\.(tmp|backup|failed)$/.test(name),
              )
          : ["missing-directory"],
    );
    const childOutput = [racerOutcome.stdout, racerOutcome.stderr].join("\n");

    const checks = {
      separateProcessMissingParentFailsClosed:
        writerFailure === "ResourceReceiptAncestorChanged" &&
        racerOutcome.status === 0 &&
        racerOutcome.signal === null &&
        racerReceipt.passed === true,
      separateProcessObservationDidNotRecreateParent:
        parentAbsentAfterWriterReturn &&
        racerReceipt.parentAbsentBeforeReplacement === true &&
        racerReceipt.replacementInstalled === true,
      separateProcessPriorAndReplacementReceiptsPreserved:
        fs.readFileSync(path.join(displacedParent, "receipt.json"), "utf8") ===
          priorReceipt &&
        fs.readFileSync(receiptPath, "utf8") === replacementReceipt,
      separateProcessReceiptResidueRemoved: receiptResidue.length === 0,
      separateProcessConfiguredPathsWithinFixtureAndSentinelUnchanged:
        within(fixtureRoot, receiptParent) &&
        within(fixtureRoot, displacedParent) &&
        within(fixtureRoot, barrierDirectory) &&
        Buffer.compare(boundaryBefore, fs.readFileSync(boundarySentinel)) === 0,
      separateProcessChildrenBoundedAndPathFree:
        !racerOutcome.timedOut &&
        !racerOutcome.outputExceeded &&
        !racerOutcome.forcedKill &&
        !racerOutcome.reapTimedOut &&
        racerOutcome.stderr.length === 0 &&
        !childOutput.includes(tempRoot) &&
        !childOutput.includes(os.homedir()),
      separateProcessScrubbedHomeUnchanged:
        fs.readdirSync(isolatedHome).length === 0,
    };
    return {
      checks,
      measurements: {
        writerFailure,
        parentAbsentAfterWriterReturn,
        racerExitCode: racerOutcome.status,
        racerTimedOut: racerOutcome.timedOut,
        racerForcedKill: racerOutcome.forcedKill,
        racerReapTimedOut: racerOutcome.reapTimedOut,
        racerReportedParentAbsent:
          racerReceipt.parentAbsentBeforeReplacement === true,
        racerReportedReplacementInstalled:
          racerReceipt.replacementInstalled === true,
        receiptResidueEntries: receiptResidue.length,
        isolatedHomeEntries: fs.readdirSync(isolatedHome).length,
      },
    };
  } finally {
    if (!racerOutcome) {
      await racer.terminateAndReap();
    }
  }
}

async function separateProcessParentChangeProofMain() {
  const canonicalTempRoot = fs.realpathSync.native(os.tmpdir());
  const tempRoot = fs.mkdtempSync(
    path.join(canonicalTempRoot, "plimsoll-parent-change-proof-"),
  );
  try {
    const result = await runSeparateProcessParentChangeProof(tempRoot);
    const passed = Object.values(result.checks).every(Boolean);
    process.stdout.write(`${JSON.stringify({ passed, ...result })}\n`);
    if (!passed) process.exitCode = 1;
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

async function runBuildFailureAdversaries(tempRoot: string) {
  const sandbox = await createResourceSandbox();
  try {
    const observedFailureStates: Array<{
      fixtureManifestUnchanged: boolean;
      homeEntryCountBefore: number;
      homeEntryCountAfter: number;
    }> = [];
    const runObservedFailure = async (
      options: Parameters<typeof runDuplicateStartSingleOwnerContract>[1],
    ) => {
      const manifestBefore = digestTree(sandbox.root);
      const homeEntryCountBefore = fs.readdirSync(sandbox.home).length;
      const receipt = await runDuplicateStartSingleOwnerContract(sandbox, options);
      const manifestAfter = digestTree(sandbox.root);
      const homeEntryCountAfter = fs.readdirSync(sandbox.home).length;
      observedFailureStates.push({
        fixtureManifestUnchanged: manifestBefore === manifestAfter,
        homeEntryCountBefore,
        homeEntryCountAfter,
      });
      return receipt;
    };
    const missingBuilder = path.join(tempRoot, "missing-builder.mjs");
    const missing = await runObservedFailure({
      builderPath: missingBuilder,
      expectedBuilderPath: missingBuilder,
      expectedBuilderSha256: "0".repeat(64),
    });

    const repositoryBuilder = path.join(
      repoRoot,
      "node_modules",
      "esbuild",
      "bin",
      "esbuild",
    );
    const tamperedBuilder = path.join(tempRoot, "tampered-builder.mjs");
    const trustedBytes = fs.readFileSync(repositoryBuilder);
    const trustedDigest = createHash("sha256").update(trustedBytes).digest("hex");
    fs.writeFileSync(tamperedBuilder, Buffer.concat([trustedBytes, Buffer.from("\n// tampered\n")]));
    const tampered = await runObservedFailure({
      builderPath: tamperedBuilder,
      expectedBuilderPath: tamperedBuilder,
      expectedBuilderSha256: trustedDigest,
    });

    const builderRequire = createRequire(fs.realpathSync(repositoryBuilder));
    const resolvedNativeBinary = builderRequire.resolve(
      `@esbuild/${process.platform}-${process.arch}/bin/esbuild`,
    );
    const trustedNativeBytes = fs.readFileSync(resolvedNativeBinary);
    const trustedNativeDigest = createHash("sha256")
      .update(trustedNativeBytes)
      .digest("hex");
    const tamperedNativeBinary = path.join(tempRoot, "tampered-native-esbuild");
    fs.writeFileSync(
      tamperedNativeBinary,
      Buffer.concat([trustedNativeBytes, Buffer.from("tampered")]),
      { mode: 0o700 },
    );
    const tamperedNative = await runObservedFailure({
      nativeBinaryPath: tamperedNativeBinary,
      expectedNativeBinaryPath: tamperedNativeBinary,
      expectedNativeBinarySha256: trustedNativeDigest,
    });

    const atomicOutput = path.join(tempRoot, "atomic-dist");
    fs.mkdirSync(atomicOutput, { mode: 0o700 });
    fs.writeFileSync(path.join(atomicOutput, "cli.mjs"), "old-cli\n", { mode: 0o700 });
    fs.writeFileSync(path.join(atomicOutput, "dashboard.html"), "old-dashboard\n", {
      mode: 0o600,
    });
    const atomicBefore = digestTree(atomicOutput);
    const publicationFailure = await runObservedFailure({
      outputDirectory: atomicOutput,
      injectPublicationFailureAfterBackup: true,
    });
    const atomicAfter = digestTree(atomicOutput);
    const publicationResidue = fs
      .readdirSync(tempRoot)
      .filter((name) => /^\.atomic-dist\.(stage|backup|failed)-/.test(name));

    const symbolic = JSON.stringify({
      missing,
      tampered,
      tamperedNative,
      publicationFailure,
    });
    const noServiceStarted = [
      missing,
      tampered,
      tamperedNative,
      publicationFailure,
    ].every(
      (receipt) =>
        receipt.status === "fail" &&
        receipt.measurements?.lifecycleStage === "build" &&
        receipt.measurements?.candidatesRaced === 0 &&
        receipt.counters.listenersCreated === 0,
    );
    return {
      missingBuilderFailedBeforeService:
        noServiceStarted &&
        missing.measurements?.errorClass === "PackagedCollectorBuilderUnavailable",
      tamperedBuilderFailedBeforeService:
        noServiceStarted &&
        tampered.measurements?.errorClass ===
          "PackagedCollectorBuilderIntegrityInvalid",
      tamperedNativeBinaryFailedBeforeService:
        noServiceStarted &&
        tamperedNative.measurements?.errorClass ===
          "PackagedCollectorNativeBinaryIntegrityInvalid",
      secondStepPublicationFailureRolledBack:
        noServiceStarted &&
        publicationFailure.measurements?.errorClass ===
          "PackagedCollectorPublicationInjectedFailure" &&
        atomicBefore === atomicAfter &&
        publicationResidue.length === 0,
      buildFailuresLeaveFixtureManifestUnchanged:
        observedFailureStates.length === 4 &&
        observedFailureStates.every((state) => state.fixtureManifestUnchanged),
      buildFailuresLeaveScrubbedHomeUnchanged:
        observedFailureStates.length === 4 &&
        observedFailureStates.every(
          (state) =>
            state.homeEntryCountBefore === 0 &&
            state.homeEntryCountAfter === 0,
        ),
      failuresAreSymbolicAndPathFree:
        !symbolic.includes(tempRoot) &&
        !symbolic.includes(sandbox.root) &&
        !symbolic.includes(os.homedir()),
    };
  } finally {
    await removeResourceSandbox(sandbox);
  }
}

async function main() {
  const canonicalTempRoot = fs.realpathSync.native(os.tmpdir());
  const tempRoot = fs.mkdtempSync(
    path.join(canonicalTempRoot, "plimsoll-finalization-proof-"),
  );
  try {
    finalizationStage = "build_failure_checks";
    const buildFailureChecks = await runBuildFailureAdversaries(tempRoot);
    finalizationStage = "receipt_write_checks";
    const receiptWriteChecks = runReceiptWriteAdversaries(tempRoot);
    finalizationStage = "concurrency_integrity_check";
    const separateProcessProof =
      await runSeparateProcessParentChangeProof(tempRoot);
    const receiptPath = path.join(tempRoot, "resource-receipt.json");
    const linkedReceiptTarget = path.join(tempRoot, "linked-receipt-target");
    const resourceChildHome = path.join(tempRoot, "resource-child-home");
    fs.mkdirSync(resourceChildHome, { mode: 0o700 });
    fs.writeFileSync(linkedReceiptTarget, "linked-target-must-not-change\n", {
      mode: 0o600,
    });
    fs.symlinkSync(linkedReceiptTarget, receiptPath);
    const fixture = JSON.parse(
      fs.readFileSync(
        path.join(
          repoRoot,
          "scripts",
          "resource-proof",
          "fixtures",
          "metadata-privacy-sentinels.json",
        ),
        "utf8",
      ),
    ) as PrivacyFixture;
    if (fixture.schemaVersion !== 1) throw new Error("FixtureInvalid");
    const values = Object.values(fixture.sentinels);
    const terms = [
      ...values,
      ...values.map((value) => value.slice(0, fixture.prefixLength)),
      os.homedir(),
    ];
    finalizationStage = "integrated_resource_child";
    const run = spawnSync(
      process.execPath,
      [
        path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs"),
        path.join(repoRoot, "scripts", "resource-proof", "index.ts"),
        "--require-integrated",
        "--receipt",
        receiptPath,
      ],
      {
        cwd: repoRoot,
        env: {
          HOME: resourceChildHome,
          USERPROFILE: resourceChildHome,
          TMPDIR: tempRoot,
          TMP: tempRoot,
          TEMP: tempRoot,
          TZ: "UTC",
          LANG: "C",
          LC_ALL: "C",
          PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
          PLIMSOLL_RESOURCE_PROOF_TEST_FINAL_PRIVACY_FAILURE: "1",
        },
        encoding: "utf8",
        timeout: 120_000,
        maxBuffer: 8 * 1024 * 1024,
      },
    );
    const stdout = run.stdout ?? "";
    const stderr = run.stderr ?? "";
    const persisted = fs.readFileSync(receiptPath, "utf8");
    const parsed = JSON.parse(stdout) as {
      overall?: string;
      gateReady?: boolean;
      summary?: { failed?: number; requiredIncomplete?: number };
      scenarios?: Array<{
        id?: string;
        status?: string;
        measurements?: Record<string, unknown>;
      }>;
    };
    const privacy = parsed.scenarios?.find(
      (scenario) => scenario.id === "metadata_privacy_sentinels",
    );
    const ownership = parsed.scenarios?.find(
      (scenario) => scenario.id === "duplicate_start_single_owner",
    );
    const failedScenarios =
      parsed.scenarios?.filter((scenario) => scenario.status === "fail") ?? [];
    const leakCount = terms.filter(
      (term) =>
        term &&
        (stdout.includes(term) || stderr.includes(term) || persisted.includes(term)),
    ).length;
    const mode = fs.statSync(receiptPath).mode & 0o777;
    const finalReceiptStat = fs.lstatSync(receiptPath);
    const finalReceiptTempResidue = fs
      .readdirSync(tempRoot)
      .filter((name) => name.startsWith(".resource-receipt.json.") && name.endsWith(".tmp"));
    finalizationStage = "final_receipt_checks";
    const checks = {
      subprocessExitedNonzero: run.status === 1,
      finalizedReceiptFailed:
        parsed.overall === "fail" &&
        parsed.gateReady === false &&
        parsed.summary?.failed === 1 &&
        parsed.summary?.requiredIncomplete === 1 &&
        privacy?.status === "fail",
      postSummaryPrivacyFailureRecorded:
        privacy?.measurements?.finalReceiptPrivacyLeaks === 1 &&
        privacy?.measurements?.finalReceiptPrivacyScanPassed === false,
      exactlyOneInjectedFailure:
        failedScenarios.length === 1 &&
        failedScenarios[0]?.id === "metadata_privacy_sentinels",
      hermeticPackagedBuild:
        ownership?.status === "pass" &&
        ownership.measurements?.buildUsedExactNodeExecutable === true &&
        ownership.measurements?.buildUsedExactRepositoryBuilder === true &&
        ownership.measurements?.nativeBinaryDigestMatchedExpected === true &&
        ownership.measurements?.packagedOutputInsideTemporaryFixture === true &&
        ownership.measurements?.packagedOutputFixtureManifestExact === true &&
        ownership.measurements?.buildHomeEntryCountBefore === 0 &&
        ownership.measurements?.buildHomeEntryCountAfter === 0 &&
        ownership.measurements?.buildHomeUnchanged === true &&
        ownership.measurements?.buildPathWasUnset === true &&
        ownership.measurements?.buildInvocationExecutableBasename === "node" &&
        typeof ownership.measurements?.buildInvocationArgumentCount === "number" &&
        ownership.measurements.buildInvocationArgumentCount > 0 &&
        ownership.measurements?.buildInvocationArgumentZeroMatchedValidatedBuilder ===
          true,
      exactPackagedArtifacts:
        ownership?.measurements?.packagedCliExactPath === true &&
        ownership.measurements?.packagedCliExecutable === true &&
        ownership.measurements?.packagedDashboardExactPath === true &&
        ownership.measurements?.packagedDashboardMode === 0o644 &&
        ownership.measurements?.packagedDashboardCopiedExactly === true,
      duplicateStartStopCleanup:
        ownership?.measurements?.stoppedThroughCli === true &&
        ownership.measurements?.pidRecordRemoved === true &&
        ownership.measurements?.startLockReleased === true &&
        ownership.measurements?.activeOwners === 1 &&
        ownership.measurements?.alreadyRunningCandidates === 1,
      ...buildFailureChecks,
      ...receiptWriteChecks,
      ...separateProcessProof.checks,
      integratedSymlinkReceiptNeutralized:
        fs.readFileSync(linkedReceiptTarget, "utf8") ===
          "linked-target-must-not-change\n" &&
        finalReceiptStat.isFile() &&
        !finalReceiptStat.isSymbolicLink() &&
        finalReceiptTempResidue.length === 0,
      privateTermsNotEmitted: leakCount === 0,
      receiptOverwriteExact: stdout === persisted,
      receiptModeOwnerOnly: mode === 0o600,
      childStderrEmpty: stderr.length === 0,
      resourceProofChildHomeUnchanged:
        fs.readdirSync(resourceChildHome).length === 0,
    };
    const passed = Object.values(checks).every(Boolean);
    process.stdout.write(
      `${JSON.stringify({
        passed,
        checks,
        privateTermCount: terms.length,
        privateTermLeaks: leakCount,
        childExitCode: run.status,
        receiptMode: mode,
      })}\n`,
    );
    if (!passed) process.exitCode = 1;
    finalizationStage = "complete";
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

const invokedAsScript = Boolean(
  process.argv[1] &&
    path.resolve(process.argv[1]) === path.resolve(finalizationProofPath),
);
if (
  invokedAsScript &&
  process.argv[2] === "--receipt-parent-change-only"
) {
  separateProcessParentChangeProofMain().catch(() => {
    process.stdout.write(
      `${JSON.stringify({ passed: false, failedSafely: true })}\n`,
    );
    process.exitCode = 1;
  });
} else if (invokedAsScript) {
  main().catch((error) => {
    const errorName = error instanceof Error ? error.name : "";
    const symbolicError = /^[A-Za-z][A-Za-z0-9]+$/.test(errorName)
      ? errorName
      : "FinalizationProofFailed";
    process.stdout.write(
      `${JSON.stringify({
        passed: false,
        failedSafely: true,
        stage: finalizationStage,
        error: symbolicError,
      })}\n`,
    );
    process.exitCode = 1;
  });
}
