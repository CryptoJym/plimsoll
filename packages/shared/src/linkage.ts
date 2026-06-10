import crypto from "node:crypto";

/**
 * Linkage keys join local AI sessions to GitHub outcomes without exposing
 * identifying values: both sides hash the same normalized inputs, so equality
 * survives while the raw branch/remote strings never leave the machine.
 * Commit shas stay plain — they are already public on the GitHub side.
 */
export function linkageHash(value: string | null | undefined) {
  if (!value) return undefined;
  return `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
}

/**
 * Canonicalize a git remote to `github.com/<owner>/<repo>` (lowercase, no
 * protocol, no credentials, no .git suffix) so ssh/https/owner-case variants
 * of the same repository hash identically. Returns undefined for unparseable
 * or local-only remotes.
 */
export function normalizeGitRemote(url: string | null | undefined) {
  if (!url) return undefined;
  let value = url.trim();
  if (!value) return undefined;

  // ssh scp-like form: git@host:owner/repo(.git)
  const scpMatch = value.match(/^[\w.-]+@([\w.-]+):(.+)$/);
  if (scpMatch) {
    value = `${scpMatch[1]}/${scpMatch[2]}`;
  } else {
    value = value
      .replace(/^[a-z+]+:\/\//i, "") // protocol
      .replace(/^[^/@]+@/, ""); // credentials
  }

  value = value
    .replace(/:\d+\//, "/") // port
    .replace(/\.git\/?$/, "")
    .replace(/\/+$/, "")
    .toLowerCase();

  const segments = value.split("/").filter(Boolean);
  if (segments.length < 3 || !segments[0].includes(".")) return undefined;
  return segments.slice(0, 3).join("/");
}

export function remoteLinkageHash(url: string | null | undefined) {
  return linkageHash(normalizeGitRemote(url));
}

export function branchLinkageHash(ref: string | null | undefined) {
  if (!ref) return undefined;
  const branch = ref.replace(/^refs\/heads\//, "").trim();
  return branch ? linkageHash(branch) : undefined;
}

export type GitLinkageContext = {
  remoteUrlHash?: string;
  branchHash?: string;
  headSha?: string;
  isWorktree?: boolean;
};
