import { createHash } from "node:crypto";

export type MaintenanceSource = "codex" | "claude_code";
export type MaintenanceProgressStage =
  | "source_scan"
  | "discovery_directory"
  | "discovery_read"
  | "candidate_metadata"
  | "jsonl_open"
  | "jsonl_validation"
  | "git_context";

export type MaintenanceProgress = {
  source: MaintenanceSource;
  stage: MaintenanceProgressStage;
  candidateHash: string | null;
};

/** One-way local identity; raw paths never cross the child boundary. */
export function maintenanceCandidateHash(value: string) {
  return "sha256:" + createHash("sha256")
    .update("plimsoll-maintenance-candidate-v1\0" + value)
    .digest("hex");
}
