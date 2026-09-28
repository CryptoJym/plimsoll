/** Independent PR #428 regression witnesses. Expected to fail until the PR is fixed. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  deriveCaptureRootIdentity,
  discoverCaptureRootCandidates,
  resolveCaptureRootMachineLabel,
} from "../packages/collector-cli/src/capture-root-inventory";
import { dashboardSummary } from "../packages/collector-cli/src/dashboard-api";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

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
  const nameOnlyCandidates = discoverCaptureRootCandidates(home);
  check("studio_name_only_folder_is_not_assumed_to_be_agent_sessions",
    nameOnlyCandidates.some((entry) => entry.shape === "studio_codex_conductor" && entry.directory === other), false);

  const machine = "fleet-label-not-the-hostname";
  const directory = path.join(home, ".codex/sessions");
  const identity = deriveCaptureRootIdentity(machine, "codex", directory);
  const roots = [{ ...identity, source: "codex" as const, directory,
    installationEpochId: randomUUID() }];
  const hostname = os.hostname();
  const candidates = [...new Set([hostname, hostname.split(".")[0] ?? "", hostname.toLowerCase(),
    (hostname.split(".")[0] ?? "").toLowerCase()])].filter(Boolean);
  check("join_can_recover_existing_nonhostname_machine_label",
    resolveCaptureRootMachineLabel(roots, candidates) !== null, true);

  const buffer = new LocalEventBuffer(path.join(root, "probe.sqlite"));
  try {
    const before = dashboardSummary(buffer.database).totals.events;
    const appended = buffer.append(aiInteractionEventSchema.parse({
      id: `join-setup-${randomUUID()}`, tenantId: "753a5a4f-c092-484b-b15e-0cfab3de4550",
      source: "codex", dataMode: "metadata", eventType: "unknown",
      observedAt: new Date().toISOString(), metadata: { collectorSetupProbe: true },
    }));
    assert.equal(appended, true);
    const after = dashboardSummary(buffer.database).totals.events;
    check("setup_probe_does_not_inflate_local_dashboard_event_total", after, before);
  } finally { buffer.close(); }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(JSON.stringify({ proof: "pr428-review-regressions", failures }));
if (failures.length) process.exitCode = 1;
