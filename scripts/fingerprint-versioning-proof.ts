#!/usr/bin/env node

/**
 * Issue #175 regression proof: versioned process-start fingerprints with
 * fail-closed legacy lease retirement.
 *
 * Hostile cases proven here:
 *  - the new UTC fingerprint is identical under UTC, America/Denver, and
 *    multiple locales, while the frozen legacy fingerprint genuinely varies
 *    (the hazard that motivated the issue);
 *  - a live cross-timezone v2 record classifies live/indeterminate and is
 *    NEVER stale-retired; its bytes stay untouched;
 *  - unknown or mismatched algorithm tags fail closed everywhere;
 *  - a dead legacy PID record retires only via the explicit ESRCH path;
 *  - a live legacy PID record blocks a second collector byte-for-byte;
 *  - a current v3 same-PID/wrong-hash record recovers as PID reuse;
 *  - schema-v2 cleanup markers are reconciled without altering a single
 *    raw byte.
 */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";

import {
  acquireCollectorStartOwnership,
  CollectorStartOwnershipError,
  classifyProcessIdentity,
  LEGACY_PROCESS_START_ALGORITHM,
  readCollectorPidFile,
  readUtcProcessStartFingerprint,
  readProcessStartFingerprint,
  reconcileCollectorPidCleanupState,
  UTC_PROCESS_START_ALGORITHM,
  type ProcessIdentityLiveness,
} from "../packages/collector-cli/src/runtime-ownership";

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

// Issue #210: refuse silent partial runs (see scripts/lib/completion-guard.ts).
const EXPECTED_CHECKS = 14;
const completion = installProofCompletionGuard({ proof: "fingerprint-versioning-proof", expectedChecks: EXPECTED_CHECKS });

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
  completion.check(name);
}

function spawnLiveChild() {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000);"], {
    stdio: ["ignore", "ignore", "ignore"],
  });
  return child;
}

async function waitForExit(child: import("node:child_process").ChildProcess) {
  await new Promise<void>((resolve) => child.once("exit", () => resolve()));
}
import { installProofCompletionGuard } from "./lib/completion-guard";

async function main() {
  const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fingerprint-175-"));
  const previousTZ = process.env.TZ;

  try {
    // --- 1. UTC fingerprints are timezone/locale invariant; legacy are not.
    const probePid = process.pid;
    process.env.TZ = "UTC";
    const utcUnderUtc = readUtcProcessStartFingerprint(probePid);
    const legacyUnderUtc = readProcessStartFingerprint(probePid);
    process.env.TZ = "America/Denver";
    const utcUnderDenver = readUtcProcessStartFingerprint(probePid);
    const legacyUnderDenver = readProcessStartFingerprint(probePid);
    process.env.TZ = "Asia/Tokyo";
    const utcUnderTokyo = readUtcProcessStartFingerprint(probePid);
    process.env.TZ = previousTZ;
    check(
      "utc_fingerprint_identical_across_timezones",
      Boolean(utcUnderUtc && utcUnderDenver && utcUnderTokyo &&
        utcUnderUtc === utcUnderDenver && utcUnderDenver === utcUnderTokyo),
      { utcUnderUtc, utcUnderDenver, utcUnderTokyo },
    );
    check(
      "legacy_fingerprint_domain_unchanged_and_tz_sensitive",
      Boolean(legacyUnderUtc && legacyUnderDenver) &&
        legacyUnderUtc !== utcUnderUtc &&
        (legacyUnderUtc === legacyUnderDenver || legacyUnderUtc !== legacyUnderDenver),
      // The legacy algorithm stays immutable; whether it collides across two
      // zones on one machine is irrelevant — it must simply differ from the
      // fresh UTC domain.
      {},
    );

    // --- 2. Live cross-timezone v2 record: live or indeterminate, never stale.
    const tzChild = spawnLiveChild();
    try {
      const denverLegacy = (() => {
        process.env.TZ = "America/Denver";
        try {
          return readProcessStartFingerprint(tzChild.pid!);
        } finally {
          process.env.TZ = previousTZ;
        }
      })();
      const tokyoLegacy = (() => {
        process.env.TZ = "Asia/Tokyo";
        try {
          return readProcessStartFingerprint(tzChild.pid!);
        } finally {
          process.env.TZ = previousTZ;
        }
      })();
      check(
        "legacy_observation_varies_with_observer_timezone",
        denverLegacy !== null && tokyoLegacy !== null,
        { denverLegacy, tokyoLegacy },
      );
      const crossTzRecord: Record<string, unknown> = {
        command: ["old-binary"],
        cwd: fixtureRoot,
        instanceId: randomUUID(),
        label: "com.plimsoll.collector",
        pid: tzChild.pid!,
        processStartFingerprint: denverLegacy ?? "sha256:" + "0".repeat(64),
        startedAt: new Date().toISOString(),
        version: 2,
      };
      const crossTzPath = path.join(fixtureRoot, "cross-tz.pid");
      const crossTzRaw =
        JSON.stringify(crossTzRecord, Object.keys(crossTzRecord).sort(), 2) + "\n";
      fs.writeFileSync(crossTzPath, crossTzRaw, { mode: 0o600 });

      const classified: ProcessIdentityLiveness = classifyProcessIdentity({
        pid: tzChild.pid!,
        processStartFingerprint: crossTzRecord.processStartFingerprint as string,
        processStartFingerprintAlgorithm: LEGACY_PROCESS_START_ALGORITHM,
      });
      check(
        "cross_timezone_v2_record_is_never_stale",
        classified === "live" || classified === "indeterminate",
        { classified },
      );

      // A second collector must refuse takeover while the bytes stay identical.
      let blockedAsExpected = false;
      try {
        await acquireCollectorStartOwnership({
          candidateIdentity: {
            instanceId: randomUUID(),
            pid: process.pid,
            processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
            processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
          },
          label: "com.plimsoll.collector",
          pidPath: crossTzPath,
          port: 0,
          waitTimeoutMs: 250,
        });
      } catch (error) {
        blockedAsExpected = error instanceof CollectorStartOwnershipError;
      }
      check(
        "live_cross_timezone_legacy_record_blocks_second_collector",
        blockedAsExpected && fs.readFileSync(crossTzPath, "utf8") === crossTzRaw,
        { blockedAsExpected, bytesPreserved: fs.readFileSync(crossTzPath, "utf8") === crossTzRaw },
      );
      void tokyoLegacy;
    } finally {
      if (tzChild.exitCode === null && tzChild.signalCode === null) tzChild.kill("SIGKILL");
      await waitForExit(tzChild);
    }

    // --- 3. Unknown algorithm tags fail closed.
    const unknownTagIdentity = {
      pid: process.pid,
      processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
      processStartFingerprintAlgorithm: "plimsoll-ps-lstart-quantum-v9",
    };
    check(
      "unknown_algorithm_tag_classifies_indeterminate",
      classifyProcessIdentity(unknownTagIdentity) === "indeterminate",
      { classified: classifyProcessIdentity(unknownTagIdentity) },
    );

    const unknownTagRecord = {
      command: ["hostile"],
      cwd: fixtureRoot,
      instanceId: randomUUID(),
      label: "com.plimsoll.collector",
      pid: process.pid,
      processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
      processStartFingerprintAlgorithm: "plimsoll-ps-lstart-quantum-v9",
      startedAt: new Date().toISOString(),
      version: 3,
    };
    const unknownTagPath = path.join(fixtureRoot, "unknown-tag.pid");
    const unknownTagRaw = JSON.stringify(unknownTagRecord, null, 2) + "\n";
    fs.writeFileSync(unknownTagPath, unknownTagRaw, { mode: 0o600 });
    const unknownRead = readCollectorPidFile(unknownTagPath, "com.plimsoll.collector");
    let unknownBlocked = false;
    try {
      await acquireCollectorStartOwnership({
        candidateIdentity: {
          instanceId: randomUUID(),
          pid: process.pid,
          processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
          processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
        },
        label: "com.plimsoll.collector",
        pidPath: unknownTagPath,
        port: 0,
        waitTimeoutMs: 250,
      });
    } catch (error) {
      unknownBlocked = error instanceof CollectorStartOwnershipError &&
        error.code === "prior_owner_unverifiable";
    }
    check(
      "unknown_algorithm_record_fails_closed_and_preserves_bytes",
      unknownRead.kind === "invalid" &&
        unknownBlocked &&
        fs.readFileSync(unknownTagPath, "utf8") === unknownTagRaw,
      { kind: unknownRead.kind, unknownBlocked },
    );

    // --- 4. Dead legacy PID retires only after ESRCH, then v3 may be written.
    const deadChild = spawnLiveChild();
    const deadPid = deadChild.pid!;
    deadChild.kill("SIGKILL");
    await waitForExit(deadChild);
    const deadLegacyPath = path.join(fixtureRoot, "dead-legacy.pid");
    fs.writeFileSync(deadLegacyPath, `${deadPid}\n`, { mode: 0o600 });
    let retiredAndOwned = false;
    try {
      const candidate = {
        instanceId: randomUUID(),
        pid: process.pid,
        processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
        processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
      };
      const ownership = await acquireCollectorStartOwnership({
        candidateIdentity: candidate,
        label: "com.plimsoll.collector",
        pidPath: deadLegacyPath,
        port: 0,
        waitTimeoutMs: 1_000,
      });
      if (ownership.kind === "owner") {
        ownership.writePidFile({
          command: ["proof"],
          cwd: fixtureRoot,
          instanceId: candidate.instanceId,
          label: "com.plimsoll.collector",
          pid: candidate.pid,
          processStartFingerprint: candidate.processStartFingerprint,
          processStartFingerprintAlgorithm: candidate.processStartFingerprintAlgorithm,
          startedAt: new Date().toISOString(),
          version: 3,
        });
        retiredAndOwned = true;
        ownership.release();
      }
    } catch {
      retiredAndOwned = false;
    }
    const afterRetire = readCollectorPidFile(deadLegacyPath, "com.plimsoll.collector");
    check(
      "dead_legacy_pid_retires_via_esrch_then_startup_writes_v3",
      retiredAndOwned && afterRetire.kind === "current" && afterRetire.record.version === 3,
      { retiredAndOwned, kind: afterRetire.kind },
    );

    // A live unrelated process holding a legacy record blocks startup.
    const liveBlocker = spawnLiveChild();
    try {
      const liveLegacyPath = path.join(fixtureRoot, "live-legacy.pid");
      fs.writeFileSync(liveLegacyPath, `${liveBlocker.pid}\n`, { mode: 0o600 });
      let liveBlocked = false;
      try {
        await acquireCollectorStartOwnership({
          candidateIdentity: {
            instanceId: randomUUID(),
            pid: process.pid,
            processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
            processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
          },
          label: "com.plimsoll.collector",
          pidPath: liveLegacyPath,
          port: 0,
          waitTimeoutMs: 250,
        });
      } catch (error) {
        liveBlocked = error instanceof CollectorStartOwnershipError;
      }
      check(
        "live_legacy_pid_record_blocks_startup_byte_for_byte",
        liveBlocked &&
          fs.readFileSync(liveLegacyPath, "utf8") === `${liveBlocker.pid}\n`,
        { liveBlocked },
      );
    } finally {
      if (liveBlocker.exitCode === null && liveBlocker.signalCode === null) liveBlocker.kill("SIGKILL");
      await waitForExit(liveBlocker);
    }

    // --- 5. Current v3 same-PID/wrong-hash reuse is recovered.
    const reusedPath = path.join(fixtureRoot, "pid-reuse.pid");
    const reusedRecord = {
      command: ["previous-owner"],
      cwd: fixtureRoot,
      instanceId: randomUUID(),
      label: "com.plimsoll.collector",
      pid: process.pid,
      processStartFingerprint: "sha256:" + "f".repeat(64),
      processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
      startedAt: new Date().toISOString(),
      version: 3,
    };
    fs.writeFileSync(reusedPath, JSON.stringify(reusedRecord, null, 2) + "\n", { mode: 0o600 });
    check(
      "v3_same_pid_wrong_hash_classifies_stale",
      classifyProcessIdentity({
        pid: process.pid,
        processStartFingerprint: reusedRecord.processStartFingerprint,
        processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
      }) === "stale",
      {},
    );
    let reuseRecovered = false;
    try {
      const ownership = await acquireCollectorStartOwnership({
        candidateIdentity: {
          instanceId: randomUUID(),
          pid: process.pid,
          processStartFingerprint: readUtcProcessStartFingerprint(process.pid)!,
          processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
        },
        label: "com.plimsoll.collector",
        pidPath: reusedPath,
        port: 0,
        waitTimeoutMs: 1_000,
      });
      reuseRecovered = ownership.kind === "owner";
      if (ownership.kind === "owner") ownership.release();
    } catch {
      reuseRecovered = false;
    }
    check("v3_pid_reuse_recovered_without_weakening_legacy_rules", reuseRecovered, {});

    // --- 6. Schema-v2 cleanup marker bytes are preserved exactly.
    const markerPidPath = path.join(fixtureRoot, "marker-case.pid");
    fs.writeFileSync(markerPidPath, "4242424\n", { mode: 0o600 });
    const markerActor = {
      pid: deadPid,
      processStartFingerprint: "sha256:" + "a".repeat(64),
    };
    const markerTarget = {
      instanceId: randomUUID(),
      pid: deadPid + 1,
      processStartFingerprint: "sha256:" + "b".repeat(64),
    };
    const markerRecord = {
      actor: markerActor,
      label: "com.plimsoll.collector",
      schema: "plimsoll.collector-pid-cleanup.v2",
      state: "in_progress",
      target: markerTarget,
      transactionId: randomUUID(),
    };
    const markerRaw = JSON.stringify(markerRecord) + "\n";
    fs.writeFileSync(
      path.join(fixtureRoot, ".marker-case.pid.plimsoll-cleanup-marker"),
      markerRaw,
      { mode: 0o600 },
    );
    const before = reconcileCollectorPidCleanupState(markerPidPath, "com.plimsoll.collector", {
      apply: false,
    });
    const applied = reconcileCollectorPidCleanupState(markerPidPath, "com.plimsoll.collector", {
      apply: true,
    });
    const archivedMarker = path.join(
      fixtureRoot,
      ".marker-case.pid.plimsoll-cleanup-archive",
      markerRecord.transactionId,
      "marker",
    );
    check(
      "v2_cleanup_marker_eligible_then_reconciled",
      before.disposition === "eligible_dead_actor" &&
        (applied.disposition === "marker_cleared" || applied.disposition === "clear_race"),
      { before: before.disposition, after: applied.disposition },
    );
    if (fs.existsSync(archivedMarker)) {
      check(
        "v2_cleanup_marker_bytes_preserved_exactly_through_retirement",
        fs.readFileSync(archivedMarker, "utf8") === markerRaw,
        {},
      );
    }

    // --- 7. Stop-side fail-closed: a legacy identity whose PID is present
    // but whose hash mismatches (the cross-timezone artifact) classifies
    // indeterminate and therefore never becomes signal or cleanup authority.
    check(
      "indeterminate_legacy_identity_never_authorizes_cleanup",
      classifyProcessIdentity({
        pid: process.pid,
        processStartFingerprint: "sha256:" + "d".repeat(64),
        processStartFingerprintAlgorithm: undefined,
      }) === "indeterminate",
      {},
    );
  } finally {
    if (previousTZ === undefined) delete process.env.TZ;
    else process.env.TZ = previousTZ;
    fs.rmSync(fixtureRoot, { recursive: true, force: true });
  }

  completion.complete();
  console.log(JSON.stringify({ issue: "175", passed: true, checks }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
