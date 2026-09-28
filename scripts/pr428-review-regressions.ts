/** Independent PR #428 privacy and machine-label regression witnesses. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  captureRootsDeriveFrom,
  deriveCaptureRootIdentity,
  discoverCaptureRootCandidates,
  resolveCaptureRootMachineLabel,
} from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-review-"));
const failures: string[] = [];
function check(name: string, actual: unknown, expected: unknown) {
  try {
    assert.deepEqual(actual, expected);
    console.log(`PASS ${name}`);
  } catch {
    failures.push(name);
    console.log(`FAIL ${name}: actual=${JSON.stringify(actual)} expected=${JSON.stringify(expected)}`);
  }
}

try {
  const home = path.join(root, "home");
  const unrelated = path.join(home, "Documents", "private-sessions");
  const linked = path.join(home, ".clientai/studio/borg/conductors/primary/profile/sessions");
  fs.mkdirSync(unrelated, { recursive: true });
  fs.mkdirSync(path.dirname(linked), { recursive: true });
  fs.symlinkSync(unrelated, linked);
  const symlinkCandidates = discoverCaptureRootCandidates(home);
  check("studio_symlink_does_not_enroll_unrelated_physical_folder",
    symlinkCandidates.some((entry) => entry.shape === "studio_codex_conductor" && entry.directory === unrelated), false);

  const otherHome = path.join(root, "other-user-home", "sessions");
  const outsideLink = path.join(home, ".clientai/studio/borg/conductors/other/profile/sessions");
  fs.mkdirSync(otherHome, { recursive: true });
  fs.mkdirSync(path.dirname(outsideLink), { recursive: true });
  fs.symlinkSync(otherHome, outsideLink);
  check("studio_symlink_outside_operator_home_is_rejected",
    discoverCaptureRootCandidates(home).some((entry) => entry.directory === otherHome), false);

  const other = path.join(home, ".clientai/studio/borg/conductors/not-an-agent/profile/sessions");
  fs.mkdirSync(other, { recursive: true });
  fs.writeFileSync(path.join(path.dirname(other), "config.toml"), 'model = "gpt-6-sol"\n');
  fs.writeFileSync(path.join(other, "rollout-2026-09-27T00-00-00-12345678-1234-4234-8234-123456789abc.jsonl"),
    '{}\n');
  const nameOnlyCandidates = discoverCaptureRootCandidates(home);
  check("studio_name_only_folder_is_preview_only",
    nameOnlyCandidates.find((entry) => entry.shape === "studio_codex_conductor" && entry.directory === other)?.autoEnroll,
    false);

  const machine = "fleet-label-not-the-hostname";
  const directory = path.join(home, ".codex/sessions");
  const identity = deriveCaptureRootIdentity(machine, "codex", directory);
  const roots = [{ ...identity, source: "codex" as const, directory,
    installationEpochId: randomUUID() }];
  const hostname = os.hostname();
  const candidates = [...new Set([hostname, hostname.split(".")[0] ?? "", hostname.toLowerCase(),
    (hostname.split(".")[0] ?? "").toLowerCase()])].filter(Boolean);
  check("nonhostname_label_cannot_be_guessed_from_hostname",
    resolveCaptureRootMachineLabel(roots, candidates), null);
  const persisted = collectorConfigSchema.parse({ captureRoots: roots, enrollmentMachineLabel: machine });
  check("join_can_recover_existing_nonhostname_machine_label",
    resolveCaptureRootMachineLabel(roots, [persisted.enrollmentMachineLabel ?? ""]) === machine &&
      captureRootsDeriveFrom(roots, persisted.enrollmentMachineLabel ?? ""), true);
  // The packaged join proof checks local event and hosted unique-event totals
  // across the first-contact replay; this unit witness never fakes an append.
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(JSON.stringify({ proof: "pr428-review-regressions", failures }));
if (failures.length) process.exitCode = 1;
