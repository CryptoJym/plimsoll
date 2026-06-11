import crypto from "node:crypto";

import Database from "better-sqlite3";

import { collectorBufferPath } from "./config";
import type { CollectorConfig } from "./config";
import {
  workRepoLabelsBatchSchema,
  type WorkRepoLabel,
} from "../../shared/src/index";

/**
 * push-repo-labels (issue 0036): give the workspace human names for the repo
 * hashes its events carry. Labels are DELIBERATE owner disclosures — unlike
 * telemetry (hashes only), a repo display name is meaning the owner chooses
 * to share. So the command:
 *   - previews EXACTLY what will cross the wire before sending (doctor-style),
 *   - sends only derived slugs (owner/name) — the schema refuses anything
 *     containing "://", so raw remote URLs cannot ride along by accident,
 *   - never sends branch names, paths, or credentials.
 *
 * Sources: repo_labels (collector-recorded `github.com/owner/name` slugs) is
 * authoritative; priority_repos URLs are the fallback for hashes that never
 * got a label — those rows are marked derived-from-url in the preview.
 */

export type RepoSlugParts = {
  provider: "github" | "gitlab" | "local_git" | "unknown";
  owner?: string;
  name: string;
};

/**
 * Parse a recorded label slug ("github.com/owner/name") or a remote URL
 * ("https://github.com/owner/name.git", "git@github.com:owner/name.git")
 * into provider/owner/name. Never returns anything containing "://".
 */
export function parseRepoSlug(raw: string): RepoSlugParts | null {
  let value = raw.trim().toLowerCase();
  if (!value) return null;
  value = value.replace(/^[a-z+]+:\/\//, "");
  value = value.replace(/^git@([^:]+):/, "$1/");
  value = value.replace(/\.git$/, "").replace(/\/+$/, "");
  const segments = value.split("/").filter(Boolean);
  if (segments.length === 0) return null;

  const host = segments[0].includes(".") ? segments[0] : null;
  const pathSegments = host ? segments.slice(1) : segments;
  if (pathSegments.length === 0) return null;

  const provider: RepoSlugParts["provider"] =
    host === "github.com" ? "github" : host === "gitlab.com" ? "gitlab" : host ? "unknown" : "local_git";
  const name = pathSegments[pathSegments.length - 1];
  const owner = pathSegments.length > 1 ? pathSegments[pathSegments.length - 2] : undefined;
  if (!name) return null;
  return { provider, ...(owner ? { owner } : {}), name };
}

export type RepoLabelCandidate = WorkRepoLabel & {
  /** Where the name came from — labels are recorded slugs; derived rows were
   * parsed out of a priority-repo URL because no label existed. */
  source: "repo_label" | "derived_from_priority_url";
};

/** Pure: ledger label/priority rows → wire rows, labels winning per hash. */
export function buildRepoLabelCandidates(
  labels: Array<{ repoHash: string; label: string }>,
  priorities: Array<{ repoHash: string; url: string }>,
): { candidates: RepoLabelCandidate[]; skippedUnparseable: number } {
  const byHash = new Map<string, RepoLabelCandidate>();
  let skippedUnparseable = 0;

  for (const priority of priorities) {
    const parts = parseRepoSlug(priority.url);
    if (!parts) {
      skippedUnparseable += 1;
      continue;
    }
    byHash.set(priority.repoHash, {
      remoteUrlHash: priority.repoHash,
      name: parts.name,
      ...(parts.owner ? { owner: parts.owner } : {}),
      provider: parts.provider,
      source: "derived_from_priority_url",
    });
  }

  for (const label of labels) {
    const parts = parseRepoSlug(label.label);
    if (!parts) {
      skippedUnparseable += 1;
      continue;
    }
    byHash.set(label.repoHash, {
      remoteUrlHash: label.repoHash,
      name: parts.name,
      ...(parts.owner ? { owner: parts.owner } : {}),
      provider: parts.provider,
      source: "repo_label",
    });
  }

  return {
    candidates: [...byHash.values()].sort((a, b) =>
      `${a.owner ?? ""}/${a.name}`.localeCompare(`${b.owner ?? ""}/${b.name}`),
    ),
    skippedUnparseable,
  };
}

export function renderRepoLabelPreview(
  candidates: RepoLabelCandidate[],
  skippedUnparseable: number,
): string {
  const lines = [
    `This will disclose ${candidates.length} repo display name(s) to the workspace.`,
    "Exactly these fields cross the wire per repo: remoteUrlHash (already in your telemetry), provider, owner, name.",
    "Never sent: raw URLs, branch names, file paths, credentials.",
    "",
  ];
  const header = ["hash", "provider", "owner", "name", "source"];
  const rows = candidates.map((candidate) => [
    `${candidate.remoteUrlHash.slice(0, 18)}…`,
    candidate.provider,
    candidate.owner ?? "—",
    candidate.name,
    candidate.source === "repo_label" ? "recorded label" : "derived from priority URL",
  ]);
  const widths = header.map((_, column) =>
    Math.max(header[column].length, ...rows.map((row) => row[column].length)),
  );
  const renderRow = (row: string[]) =>
    row.map((value, column) => value.padEnd(widths[column])).join("  ").trimEnd();
  lines.push(renderRow(header));
  lines.push(widths.map((width) => "-".repeat(width)).join("  "));
  for (const row of rows) lines.push(renderRow(row));
  if (skippedUnparseable > 0) {
    lines.push("");
    lines.push(`skipped (could not parse a slug): ${skippedUnparseable}`);
  }
  return lines.join("\n");
}

/** Read the ledger's label tables (read-only) and build the preview. */
export function prepareRepoLabelsPush(options: { ledgerPath?: string } = {}): {
  candidates: RepoLabelCandidate[];
  skippedUnparseable: number;
  preview: string;
} {
  const ledgerPath = options.ledgerPath ?? collectorBufferPath();
  let ledger: Database.Database;
  try {
    ledger = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  } catch (error) {
    throw new Error(
      `No readable local ledger at ${ledgerPath} (${error instanceof Error ? error.message : String(error)}) — no labels to push.`,
    );
  }
  try {
    const labels = ledger
      .prepare(`select repo_hash as repoHash, label from repo_labels order by repo_hash`)
      .all() as Array<{ repoHash: string; label: string }>;
    const priorities = ledger
      .prepare(`select repo_hash as repoHash, url from priority_repos order by repo_hash`)
      .all() as Array<{ repoHash: string; url: string }>;
    const { candidates, skippedUnparseable } = buildRepoLabelCandidates(labels, priorities);
    return {
      candidates,
      skippedUnparseable,
      preview: renderRepoLabelPreview(candidates, skippedUnparseable),
    };
  } finally {
    ledger.close();
  }
}

const LABELS_PATH = "/api/work-intelligence/repo-labels";
const MAX_LABELS_PER_BATCH = 200;

/**
 * POST the prepared rows to the workspace labels route — same machine-path
 * auth as event uploads (install key + HMAC over `${ts}.${body}`); the
 * response is summarized, never echoed (it contains the install key).
 */
export async function pushRepoLabels(
  config: CollectorConfig,
  candidates: RepoLabelCandidate[],
  options: {
    url?: string;
    appVersion?: string;
    fetchImpl?: typeof fetch;
    log?: (line: string) => void;
  } = {},
): Promise<{ pushed: number; created: number; updated: number; batches: number }> {
  const log = options.log ?? ((line: string) => console.log(line));
  const fetchImpl = options.fetchImpl ?? fetch;

  const baseUrl = options.url ?? config.uploadUrl;
  if (!baseUrl) {
    throw new Error(
      "This machine has not joined a workspace (no uploadUrl in collector.config.json). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry push-repo-labels.',
    );
  }
  if ((!config.installKey || config.installKey === "local-dev") && !config.ingestKey) {
    throw new Error(
      "No workspace install credentials found (installKey is missing/local-dev and there is no ingestKey). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry push-repo-labels.',
    );
  }
  if (candidates.length === 0) {
    return { pushed: 0, created: 0, updated: 0, batches: 0 };
  }

  // The labels route lives next to the ingest route; derive it so a custom
  // --url pointing at the ingest endpoint still lands on the right path.
  const url = new URL(baseUrl);
  url.pathname = LABELS_PATH;

  let pushed = 0;
  let created = 0;
  let updated = 0;
  let batches = 0;
  for (let start = 0; start < candidates.length; start += MAX_LABELS_PER_BATCH) {
    const slice = candidates.slice(start, start + MAX_LABELS_PER_BATCH);
    const body = JSON.stringify(
      workRepoLabelsBatchSchema.parse({
        tenantId: config.tenantId,
        installKey: config.installKey,
        appVersion: options.appVersion ?? "0.1.0",
        repositories: slice.map(({ source: _source, ...wire }) => wire),
      }),
    );
    const headers: Record<string, string> = {
      "content-type": "application/json",
      "x-plimsoll-install-key": config.installKey,
    };
    if (config.ingestKey) headers["x-plimsoll-ingest-key"] = config.ingestKey;
    if (config.uploadSigningSecret) {
      const timestamp = new Date().toISOString();
      const digest = crypto
        .createHmac("sha256", config.uploadSigningSecret)
        .update(`${timestamp}.${body}`)
        .digest("hex");
      headers["x-plimsoll-upload-timestamp"] = timestamp;
      headers["x-plimsoll-upload-signature"] = `sha256=${digest}`;
    }

    const response = await fetchImpl(url.toString(), { method: "POST", headers, body });
    const responseBody = (await response.json().catch(() => ({}))) as {
      error?: unknown;
      created?: unknown;
      updated?: unknown;
    };
    if (!response.ok) {
      const errorCode = typeof responseBody.error === "string" ? responseBody.error : "unknown_error";
      throw new Error(
        `Workspace refused the repo-labels batch with HTTP ${response.status} (${errorCode}). ` +
          "Nothing further was sent.",
      );
    }
    pushed += slice.length;
    created += typeof responseBody.created === "number" ? responseBody.created : 0;
    updated += typeof responseBody.updated === "number" ? responseBody.updated : 0;
    batches += 1;
    log(
      JSON.stringify({
        status: "repo_labels_progress",
        pushed,
        created,
        updated,
        batches,
      }),
    );
  }

  return { pushed, created, updated, batches };
}
