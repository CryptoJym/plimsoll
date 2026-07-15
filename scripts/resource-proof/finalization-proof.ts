#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

type PrivacyFixture = {
  schemaVersion: number;
  prefixLength: number;
  sentinels: Record<string, string>;
};

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function main() {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-finalization-proof-"));
  try {
    const receiptPath = path.join(tempRoot, "resource-receipt.json");
    fs.writeFileSync(receiptPath, "stale\n", { mode: 0o644 });
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
    const leakCount = terms.filter(
      (term) =>
        term &&
        (stdout.includes(term) || stderr.includes(term) || persisted.includes(term)),
    ).length;
    const mode = fs.statSync(receiptPath).mode & 0o777;
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

try {
  main();
} catch {
  process.stdout.write(`${JSON.stringify({ passed: false, failedSafely: true })}\n`);
  process.exitCode = 1;
}
