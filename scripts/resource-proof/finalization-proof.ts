#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
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

  const externalTarget = path.join(fixtureRoot, "external-target");
  const symlinkDestination = path.join(fixtureRoot, "symlink-receipt.json");
  fs.writeFileSync(externalTarget, "external-must-not-change\n", { mode: 0o600 });
  fs.symlinkSync(externalTarget, symlinkDestination);
  writeResourceReceiptAtomically(symlinkDestination, serialized);
  const symlinkDestinationStat = fs.lstatSync(symlinkDestination);

  const nonregularDestination = path.join(fixtureRoot, "nonregular-receipt.json");
  fs.mkdirSync(nonregularDestination, { mode: 0o700 });
  const nonregularFailure = errorName(() =>
    writeResourceReceiptAtomically(nonregularDestination, serialized),
  );

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
  const raceResidue = [raceParent, raceDisplacedParent].flatMap((directory) =>
    fs
      .readdirSync(directory)
      .filter((name) => /^\.receipt\.json\..*\.(tmp|backup|failed)$/.test(name)),
  );

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
    ancestorSymlinkEscapeRejected:
      ancestorFailure === "ResourceReceiptAncestorInvalid" &&
      fs.lstatSync(ancestorAlias).isSymbolicLink() &&
      fs.readFileSync(externalReceipt, "utf8") === "external-ancestor-target\n" &&
      externalTempResidue.length === 0,
    parentReplacementRaceFailsWithoutOverwrite:
      raceFailure === "ResourceReceiptAncestorChanged" &&
      fs.readFileSync(path.join(raceDisplacedParent, "receipt.json"), "utf8") ===
        "original-parent-prior-receipt\n" &&
      fs.readFileSync(raceReceipt, "utf8") ===
        "replacement-parent-prior-receipt\n" &&
      raceResidue.length === 0 &&
      !raceFailure.includes(tempRoot),
  };
}

async function runBuildFailureAdversaries(tempRoot: string) {
  const sandbox = await createResourceSandbox();
  try {
    const missingBuilder = path.join(tempRoot, "missing-builder.mjs");
    const missing = await runDuplicateStartSingleOwnerContract(sandbox, {
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
    const tampered = await runDuplicateStartSingleOwnerContract(sandbox, {
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
    const tamperedNative = await runDuplicateStartSingleOwnerContract(sandbox, {
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
    const publicationFailure = await runDuplicateStartSingleOwnerContract(sandbox, {
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
    const buildFailureChecks = await runBuildFailureAdversaries(tempRoot);
    const receiptWriteChecks = runReceiptWriteAdversaries(tempRoot);
    const receiptPath = path.join(tempRoot, "resource-receipt.json");
    const linkedReceiptTarget = path.join(tempRoot, "linked-receipt-target");
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
          HOME: os.homedir(),
          USERPROFILE: os.homedir(),
          TMPDIR: tempRoot,
          TMP: tempRoot,
          TEMP: tempRoot,
          TZ: "UTC",
          LANG: "C",
          LC_ALL: "C",
          PATH: process.env.PATH,
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
        ownership.measurements?.nativeBinaryValidatedBeforeBuild === true &&
        ownership.measurements?.publicationFailureAtomic === true &&
        ownership.measurements?.buildHomeWasEmpty === true &&
        ownership.measurements?.buildPathWasUnset === true &&
        ownership.measurements?.packageManagerInvocations === 0,
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
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

main().catch(() => {
  process.stdout.write(`${JSON.stringify({ passed: false, failedSafely: true })}\n`);
  process.exitCode = 1;
});
