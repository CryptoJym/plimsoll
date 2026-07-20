#!/usr/bin/env node

import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  createResourceSandbox,
  loadUnwiredIntegrationScenarios,
  removeResourceSandbox,
  runArchitectureContract,
  runBoundedCodexReconciliationContract,
  runChildEnvironmentContract,
  runEmptyLedgerContract,
  runExistingSignalFidelityProof,
  runIsolationContract,
  runNoChangeConstantWorkContract,
  runPortReservationContract,
  runDuplicateStartSingleOwnerContract,
  runPoisonContinuationContract,
  runDashboardProjectionBudgetContract,
  runIntegratedCaptureProjectionOutboxContract,
  runMaintenanceRegressionContract,
  runMetadataPrivacySentinelsContract,
  runLearningFactPrivacyAndResourceContract,
  resourceReceiptPrivacyLeakCount,
} from "./scenarios";
import { runBoundedCaptureContract } from "./bounded-capture";
import {
  RESOURCE_PROOF_SCHEMA,
  type ResourceProofReceipt,
  type ScenarioReceipt,
} from "./types";

function within(parent: string, candidate: string) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function optionValue(name: string) {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function printHelp() {
  console.log(`Plimsoll resource-proof scaffold

Usage:
  pnpm exec tsx scripts/resource-proof/index.ts [options]

Options:
  --receipt <path>       Write the JSON receipt to a caller-selected path.
  --run-existing-proof   Run scripts/signal-fidelity-proof.ts in the isolated environment.
  --require-integrated   Exit non-zero while any required scenario is fail/not_wired/skipped.
  --help                 Show this help.

The default harness runs every required architecture, isolation, ownership,
maintenance, reconciliation, projection, delivery, integrated capture, and
metadata-privacy scenario. Pass --require-integrated for the release gate.`);
}

function summarize(scenarios: ScenarioReceipt[]) {
  const count = (status: ScenarioReceipt["status"]) =>
    scenarios.filter((scenario) => scenario.status === status).length;
  return {
    passed: count("pass"),
    failed: count("fail"),
    notWired: count("not_wired"),
    skipped: count("skipped"),
    requiredIncomplete: scenarios.filter(
      (scenario) => scenario.required && scenario.status !== "pass",
    ).length,
  };
}

export type ResourceReceiptWriteOptions = {
  injectFailureAfterBytes?: number;
  injectParentReplacementAfterTempWrite?: {
    displacedParent: string;
    replacementReceipt: string;
  };
};

class ResourceReceiptFinalizeError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = code;
  }
}

function receiptFinalizeFailure(code: string): never {
  throw new ResourceReceiptFinalizeError(code);
}

type ReceiptAncestor = {
  path: string;
  device: number;
  inode: number;
};

function noFollowReceiptAncestors(parent: string) {
  const parsed = path.parse(parent);
  const relative = path.relative(parsed.root, parent);
  const components = relative ? relative.split(path.sep).filter(Boolean) : [];
  const ancestors: ReceiptAncestor[] = [];
  let current = parsed.root;
  for (const component of ["", ...components]) {
    if (component) current = path.join(current, component);
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(current);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || !component) {
        receiptFinalizeFailure("ResourceReceiptAncestorUnavailable");
      }
      try {
        fs.mkdirSync(current, { mode: 0o700 });
        stat = fs.lstatSync(current);
      } catch {
        receiptFinalizeFailure("ResourceReceiptAncestorUnavailable");
      }
    }
    if (!stat.isDirectory() || stat.isSymbolicLink()) {
      receiptFinalizeFailure("ResourceReceiptAncestorInvalid");
    }
    ancestors.push({ path: current, device: stat.dev, inode: stat.ino });
  }
  try {
    if (fs.realpathSync.native(parent) !== parent) {
      receiptFinalizeFailure("ResourceReceiptAncestorInvalid");
    }
  } catch (error) {
    if (error instanceof ResourceReceiptFinalizeError) throw error;
    receiptFinalizeFailure("ResourceReceiptAncestorUnavailable");
  }
  return ancestors;
}

function sameReceiptAncestorAuthority(
  expected: readonly ReceiptAncestor[],
  observed: readonly ReceiptAncestor[],
) {
  return (
    expected.length === observed.length &&
    expected.every(
      (entry, index) =>
        entry.path === observed[index]?.path &&
        entry.device === observed[index]?.device &&
        entry.inode === observed[index]?.inode,
    )
  );
}

export function validateResourceReceiptDestination(receiptPath: string) {
  const resolved = path.resolve(receiptPath);
  const parent = path.dirname(resolved);
  let ancestors: ReceiptAncestor[];
  try {
    ancestors = noFollowReceiptAncestors(parent);
    try {
      const destinationStat = fs.lstatSync(resolved);
      if (!destinationStat.isFile() && !destinationStat.isSymbolicLink()) {
        receiptFinalizeFailure("ResourceReceiptDestinationInvalid");
      }
    } catch (error) {
      if (error instanceof ResourceReceiptFinalizeError) throw error;
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        receiptFinalizeFailure("ResourceReceiptDestinationUnavailable");
      }
    }
  } catch (error) {
    if (error instanceof ResourceReceiptFinalizeError) throw error;
    receiptFinalizeFailure("ResourceReceiptAncestorUnavailable");
  }
  return { resolved, parent, ancestors };
}

function assertSimpleReceiptName(name: string) {
  if (!name || name === "." || name === ".." || path.basename(name) !== name) {
    receiptFinalizeFailure("ResourceReceiptNameInvalid");
  }
}

function anchoredParentStillAuthoritative(
  absoluteParent: string,
  expectedDevice: number,
  expectedInode: number,
) {
  const authority = noFollowReceiptAncestors(absoluteParent).at(-1);
  return Boolean(
    authority &&
      authority.device === expectedDevice &&
      authority.inode === expectedInode,
  );
}

function anchoredEntryExists(name: string) {
  try {
    fs.lstatSync(name);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    receiptFinalizeFailure("ResourceReceiptDestinationUnavailable");
  }
}

function writeResourceReceiptFromAnchoredCwd(args: {
  absoluteParent: string;
  finalName: string;
  tempName: string;
  expectedDevice: number;
  expectedInode: number;
  serialized: string;
  injectFailureAfterBytes?: number;
  injectParentReplacementAfterTempWrite?: {
    displacedParent: string;
    replacementReceipt: string;
  };
}) {
  assertSimpleReceiptName(args.finalName);
  assertSimpleReceiptName(args.tempName);
  const anchoredParent = fs.statSync(".");
  if (
    !anchoredParent.isDirectory() ||
    anchoredParent.dev !== args.expectedDevice ||
    anchoredParent.ino !== args.expectedInode ||
    !anchoredParentStillAuthoritative(
      args.absoluteParent,
      args.expectedDevice,
      args.expectedInode,
    )
  ) {
    receiptFinalizeFailure("ResourceReceiptAncestorChanged");
  }

  try {
    const destination = fs.lstatSync(args.finalName);
    if (!destination.isFile() && !destination.isSymbolicLink()) {
      receiptFinalizeFailure("ResourceReceiptDestinationInvalid");
    }
  } catch (error) {
    if (error instanceof ResourceReceiptFinalizeError) throw error;
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      receiptFinalizeFailure("ResourceReceiptDestinationUnavailable");
    }
  }

  const bytes = Buffer.from(args.serialized, "utf8");
  const injectAfter = args.injectFailureAfterBytes;
  if (
    injectAfter !== undefined &&
    (!Number.isSafeInteger(injectAfter) || injectAfter < 0 || injectAfter >= bytes.length)
  ) {
    receiptFinalizeFailure("ResourceReceiptInjectionInvalid");
  }

  const backupName = `.${args.finalName}.${process.pid}.${randomUUID()}.backup`;
  const failedName = `.${args.finalName}.${process.pid}.${randomUUID()}.failed`;
  let fd: number | undefined;
  let backupMoved = false;
  let published = false;
  try {
    fd = fs.openSync(
      args.tempName,
      fs.constants.O_WRONLY |
        fs.constants.O_CREAT |
        fs.constants.O_EXCL |
        (fs.constants.O_NOFOLLOW ?? 0),
      0o600,
    );
    const opened = fs.fstatSync(fd);
    const openedPath = fs.lstatSync(args.tempName);
    if (
      !opened.isFile() ||
      openedPath.isSymbolicLink() ||
      opened.dev !== openedPath.dev ||
      opened.ino !== openedPath.ino
    ) {
      receiptFinalizeFailure("ResourceReceiptTempInvalid");
    }
    fs.fchmodSync(fd, 0o600);
    let offset = 0;
    while (offset < bytes.length) {
      if (injectAfter !== undefined && offset >= injectAfter) {
        receiptFinalizeFailure("ResourceReceiptInjectedWriteFailure");
      }
      const remainingBeforeInjection =
        injectAfter === undefined ? bytes.length - offset : injectAfter - offset;
      const length = Math.min(bytes.length - offset, remainingBeforeInjection);
      if (length <= 0) receiptFinalizeFailure("ResourceReceiptInjectedWriteFailure");
      const written = fs.writeSync(fd, bytes, offset, length, null);
      if (written <= 0) receiptFinalizeFailure("ResourceReceiptWriteFailed");
      offset += written;
    }
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;

    const replacement = args.injectParentReplacementAfterTempWrite;
    if (replacement) {
      if (
        path.dirname(path.resolve(replacement.displacedParent)) !==
          path.dirname(path.resolve(args.absoluteParent)) ||
        path.resolve(replacement.displacedParent) === path.resolve(args.absoluteParent)
      ) {
        receiptFinalizeFailure("ResourceReceiptInjectionInvalid");
      }
      fs.renameSync(args.absoluteParent, replacement.displacedParent);
      fs.mkdirSync(args.absoluteParent, { mode: 0o700 });
      fs.writeFileSync(
        path.join(args.absoluteParent, args.finalName),
        replacement.replacementReceipt,
        { mode: 0o600 },
      );
    }

    if (
      !anchoredParentStillAuthoritative(
        args.absoluteParent,
        args.expectedDevice,
        args.expectedInode,
      )
    ) {
      receiptFinalizeFailure("ResourceReceiptAncestorChanged");
    }
    if (anchoredEntryExists(args.finalName)) {
      fs.renameSync(args.finalName, backupName);
      backupMoved = true;
    }
    fs.renameSync(args.tempName, args.finalName);
    published = true;

    if (
      !anchoredParentStillAuthoritative(
        args.absoluteParent,
        args.expectedDevice,
        args.expectedInode,
      )
    ) {
      receiptFinalizeFailure("ResourceReceiptAncestorChanged");
    }
    const finalized = fs.lstatSync(args.finalName);
    if (
      !finalized.isFile() ||
      finalized.isSymbolicLink() ||
      (finalized.mode & 0o777) !== 0o600 ||
      finalized.size !== bytes.length
    ) {
      receiptFinalizeFailure("ResourceReceiptFinalStateInvalid");
    }
    const directoryFd = fs.openSync(
      ".",
      fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0),
    );
    try {
      fs.fsyncSync(directoryFd);
    } finally {
      fs.closeSync(directoryFd);
    }
    if (backupMoved) {
      fs.unlinkSync(backupName);
      backupMoved = false;
    }
    return { bytesWritten: bytes.length, mode: finalized.mode & 0o777 };
  } catch (error) {
    if (fd !== undefined) {
      try {
        fs.closeSync(fd);
      } catch {
        // Preserve the original path-free finalization failure.
      }
    }
    try {
      if (published && anchoredEntryExists(args.finalName)) {
        fs.renameSync(args.finalName, failedName);
        published = false;
      }
      if (backupMoved && anchoredEntryExists(backupName)) {
        fs.renameSync(backupName, args.finalName);
        backupMoved = false;
      }
      for (const disposable of [args.tempName, failedName, backupName]) {
        try {
          fs.unlinkSync(disposable);
        } catch (cleanupError) {
          if ((cleanupError as NodeJS.ErrnoException).code !== "ENOENT") {
            receiptFinalizeFailure("ResourceReceiptRollbackFailed");
          }
        }
      }
    } catch (rollbackError) {
      if (rollbackError instanceof ResourceReceiptFinalizeError) throw rollbackError;
      receiptFinalizeFailure("ResourceReceiptRollbackFailed");
    }
    if (error instanceof ResourceReceiptFinalizeError) throw error;
    receiptFinalizeFailure("ResourceReceiptAtomicWriteFailed");
  }
}

export function writeResourceReceiptAtomically(
  receiptPath: string,
  serialized: string,
  options: ResourceReceiptWriteOptions = {},
) {
  const initialAuthority = validateResourceReceiptDestination(receiptPath);
  const confirmedAuthority = validateResourceReceiptDestination(receiptPath);
  if (
    !sameReceiptAncestorAuthority(
      initialAuthority.ancestors,
      confirmedAuthority.ancestors,
    )
  ) {
    receiptFinalizeFailure("ResourceReceiptAncestorChanged");
  }
  const expectedParent = initialAuthority.ancestors.at(-1);
  if (!expectedParent) receiptFinalizeFailure("ResourceReceiptAncestorUnavailable");
  const finalName = path.basename(initialAuthority.resolved);
  const tempName = `.${finalName}.${process.pid}.${randomUUID()}.tmp`;
  const modulePath = fileURLToPath(import.meta.url);
  const repoRoot = path.resolve(path.dirname(modulePath), "../..");
  const tsxCli = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
  const run = spawnSync(
    process.execPath,
    [
      tsxCli,
      modulePath,
      "--atomic-receipt-child",
    ],
    {
      cwd: initialAuthority.parent,
      env: {
        HOME: initialAuthority.parent,
        USERPROFILE: initialAuthority.parent,
        TMPDIR: initialAuthority.parent,
        TMP: initialAuthority.parent,
        TEMP: initialAuthority.parent,
        TZ: "UTC",
        LANG: "C",
        LC_ALL: "C",
      },
      input: JSON.stringify({
        absoluteParent: initialAuthority.parent,
        finalName,
        tempName,
        expectedDevice: expectedParent.device,
        expectedInode: expectedParent.inode,
        serialized,
        injectFailureAfterBytes: options.injectFailureAfterBytes,
        injectParentReplacementAfterTempWrite:
          options.injectParentReplacementAfterTempWrite,
      }),
      encoding: "utf8",
      timeout: 30_000,
      maxBuffer: 1024 * 1024,
    },
  );
  let result: { passed?: boolean; bytesWritten?: number; mode?: number; error?: string } = {};
  try {
    result = JSON.parse(run.stdout || "{}") as typeof result;
  } catch {
    receiptFinalizeFailure("ResourceReceiptChildProtocolInvalid");
  }
  if (
    run.status !== 0 ||
    run.error ||
    result.passed !== true ||
    result.bytesWritten !== Buffer.byteLength(serialized) ||
    result.mode !== 0o600
  ) {
    const symbolicError =
      typeof result.error === "string" && /^ResourceReceipt[A-Za-z]+$/.test(result.error)
        ? result.error
        : "ResourceReceiptChildFailed";
    receiptFinalizeFailure(symbolicError);
  }
  return { bytesWritten: result.bytesWritten, mode: result.mode };
}

function atomicReceiptChildMain() {
  const input = fs.readFileSync(0, "utf8");
  if (Buffer.byteLength(input) > 1024 * 1024) {
    receiptFinalizeFailure("ResourceReceiptChildInputInvalid");
  }
  let request: {
    absoluteParent?: unknown;
    finalName?: unknown;
    tempName?: unknown;
    expectedDevice?: unknown;
    expectedInode?: unknown;
    serialized?: unknown;
    injectFailureAfterBytes?: unknown;
    injectParentReplacementAfterTempWrite?: unknown;
  } = {};
  try {
    request = JSON.parse(input) as typeof request;
  } catch {
    receiptFinalizeFailure("ResourceReceiptChildInputInvalid");
  }
  if (
    typeof request.absoluteParent !== "string" ||
    typeof request.finalName !== "string" ||
    typeof request.tempName !== "string" ||
    typeof request.serialized !== "string" ||
    !Number.isSafeInteger(request.expectedDevice) ||
    !Number.isSafeInteger(request.expectedInode) ||
    (request.injectFailureAfterBytes !== undefined &&
      !Number.isSafeInteger(request.injectFailureAfterBytes)) ||
    (request.injectParentReplacementAfterTempWrite !== undefined &&
      (typeof request.injectParentReplacementAfterTempWrite !== "object" ||
        request.injectParentReplacementAfterTempWrite === null ||
        typeof (request.injectParentReplacementAfterTempWrite as Record<string, unknown>)
          .displacedParent !== "string" ||
        typeof (request.injectParentReplacementAfterTempWrite as Record<string, unknown>)
          .replacementReceipt !== "string"))
  ) {
    receiptFinalizeFailure("ResourceReceiptChildArgumentsInvalid");
  }
  const result = writeResourceReceiptFromAnchoredCwd({
    absoluteParent: request.absoluteParent,
    finalName: request.finalName,
    tempName: request.tempName,
    expectedDevice: request.expectedDevice as number,
    expectedInode: request.expectedInode as number,
    serialized: request.serialized,
    injectFailureAfterBytes: request.injectFailureAfterBytes as number | undefined,
    injectParentReplacementAfterTempWrite:
      request.injectParentReplacementAfterTempWrite as
        | { displacedParent: string; replacementReceipt: string }
        | undefined,
  });
  process.stdout.write(`${JSON.stringify({ passed: true, ...result })}\n`);
}

async function main() {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printHelp();
    return;
  }
  const receiptPath = optionValue("--receipt");
  if (process.argv.includes("--receipt") && !receiptPath) {
    throw new Error("--receipt requires a path");
  }
  const requireIntegrated = process.argv.includes("--require-integrated");
  const runExistingProof = process.argv.includes("--run-existing-proof");
  const operatorHome = os.homedir();
  if (receiptPath) {
    const liveCollectorHome = path.join(
      operatorHome,
      "Library",
      "Application Support",
      "Plimsoll",
    );
    if (within(liveCollectorHome, receiptPath)) {
      throw new Error("--receipt must not point inside the operator's live Plimsoll directory");
    }
    validateResourceReceiptDestination(receiptPath);
  }
  const sandbox = await createResourceSandbox();
  const scenarios: ScenarioReceipt[] = [];

  try {
    scenarios.push(runIsolationContract(sandbox, operatorHome));
    const portScenario = await runPortReservationContract(sandbox);
    scenarios.push(portScenario);
    const environmentScenario = runChildEnvironmentContract(sandbox);
    scenarios.push(environmentScenario);
    scenarios.push(runArchitectureContract());
    scenarios.push(runEmptyLedgerContract(sandbox));
    scenarios.push(runExistingSignalFidelityProof(sandbox, runExistingProof));
    scenarios.push(runMaintenanceRegressionContract(sandbox));
    scenarios.push(await runNoChangeConstantWorkContract(sandbox));
    scenarios.push(await runBoundedCaptureContract(sandbox));
    scenarios.push(runBoundedCodexReconciliationContract(sandbox));
    scenarios.push(await runDuplicateStartSingleOwnerContract(sandbox));
    scenarios.push(await runPoisonContinuationContract(sandbox));
    scenarios.push(await runDashboardProjectionBudgetContract(sandbox));
    scenarios.push(runIntegratedCaptureProjectionOutboxContract(sandbox, operatorHome));
    scenarios.push(runMetadataPrivacySentinelsContract(sandbox, operatorHome));
    scenarios.push(runLearningFactPrivacyAndResourceContract(sandbox, operatorHome));
    scenarios.push(
      ...loadUnwiredIntegrationScenarios(
        new Set([
          "no_change_constant_work",
          "bounded_generation_capture",
          "duplicate_start_single_owner",
          "poison_continuation",
          "bounded_codex_reconciliation",
          "dashboard_projection_budget",
          "integrated_capture_projection_outbox",
          "metadata_privacy_sentinels",
          "learning_fact_privacy_and_resource_bounds",
        ]),
      ),
    );

    const summary = summarize(scenarios);
    const gateReady = summary.failed === 0 && summary.requiredIncomplete === 0;
    const receipt: ResourceProofReceipt = {
      schema: RESOURCE_PROOF_SCHEMA,
      generatedAt: new Date().toISOString(),
      overall: summary.failed > 0 ? "fail" : gateReady ? "pass" : "scaffold_ready",
      gateReady,
      requireIntegrated,
      environment: {
        isolation: "temporary-home-db-and-session-roots",
        loopbackPort:
          portScenario.status === "pass" ? "held-and-challenged" : "unverified",
        providerNetwork: "not-configured",
        credentials:
          environmentScenario.status === "pass" ? "scrubbed-by-allowlist" : "unverified",
        liveStateTouched: false,
        node: process.version,
        platform: process.platform,
      },
      summary,
      scenarios,
    };
    let serialized = `${JSON.stringify(receipt, null, 2)}\n`;
    // Test-only seam: append one already-classified private term to the local
    // scan input, never to the receipt, so finalized exit state can be proved
    // without emitting the term.
    const privacyScanInput =
      process.env.PLIMSOLL_RESOURCE_PROOF_TEST_FINAL_PRIVACY_FAILURE === "1"
        ? `${serialized}${operatorHome}`
        : serialized;
    const receiptPrivacyLeaks = resourceReceiptPrivacyLeakCount(privacyScanInput, operatorHome);
    const privacyScenario = scenarios.find(
      (scenario) => scenario.id === "metadata_privacy_sentinels",
    );
    if (!privacyScenario) throw new Error("MetadataPrivacyScenarioMissing");
    privacyScenario.measurements = {
      ...privacyScenario.measurements,
      finalReceiptPrivacyLeaks: receiptPrivacyLeaks,
      finalReceiptPrivacyScanPassed: receiptPrivacyLeaks === 0,
    };
    if (receiptPrivacyLeaks > 0) {
      privacyScenario.status = "fail";
      privacyScenario.detail =
        "The final resource receipt contained a private-term leak; receipt content is omitted.";
    }
    receipt.summary = summarize(scenarios);
    receipt.gateReady = receipt.summary.failed === 0 && receipt.summary.requiredIncomplete === 0;
    receipt.overall = receipt.summary.failed > 0 ? "fail" : receipt.gateReady ? "pass" : "scaffold_ready";
    serialized = `${JSON.stringify(receipt, null, 2)}\n`;
    if (resourceReceiptPrivacyLeakCount(serialized, operatorHome) > 0) {
      throw new Error("ResourceReceiptPrivacyViolation");
    }
    if (receiptPath) {
      writeResourceReceiptAtomically(receiptPath, serialized);
    }
    process.stdout.write(serialized);

    if (receipt.summary.failed > 0 || (requireIntegrated && !receipt.gateReady)) {
      process.exitCode = 1;
    }
  } finally {
    await removeResourceSandbox(sandbox);
  }
}

const invokedAsScript = Boolean(
  process.argv[1] &&
    path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url)),
);
if (invokedAsScript) {
  if (process.argv[2] === "--atomic-receipt-child") {
    try {
      atomicReceiptChildMain();
    } catch (error) {
      const errorClass =
        error instanceof ResourceReceiptFinalizeError
          ? error.name
          : "ResourceReceiptChildFailed";
      process.stdout.write(
        `${JSON.stringify({ passed: false, error: errorClass })}\n`,
      );
      process.exitCode = 1;
    }
  } else {
    main().catch((error) => {
      const errorClass = error instanceof Error ? error.name : "UnknownError";
      process.stderr.write(
        `${JSON.stringify({ schema: RESOURCE_PROOF_SCHEMA, overall: "fail", error: errorClass })}\n`,
      );
      process.exitCode = 1;
    });
  }
}
