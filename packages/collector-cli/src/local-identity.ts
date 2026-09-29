import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { providerAccountKey } from "../../shared/src/policy";

/**
 * Local account identity (issue 0028). AI-tool accounts are tied to emails;
 * the emails and account ids are readable from each tool's LOCAL config:
 *   - Claude Code: ~/.claude.json → oauthAccount.emailAddress
 *   - Codex: ~/.codex/auth.json → email, and the id_token's
 *     https://api.openai.com/auth claims (chatgpt_account_id, plan type,
 *     last_refresh for the honest-attribution window)
 *
 * Everything read here is LOCAL-ONLY material: raw emails/ids stay local;
 * provider account keys use the same protected OTLP value hash as wire rows.
 */

export type LocalIdentity = {
  source: "claude_code" | "codex";
  email?: string;
  /** Provider account key shared by local labels, OTLP and file-derived rows. */
  actorHash?: string;
  /** codex only: chatgpt_plan_type (e.g. "pro") for plan-leverage suggestions. */
  planType?: string;
  /** codex only: ISO time of the current login's last refresh. Sessions that
   * started at/after this instant provably ran under this identity; earlier
   * sessions stay unattributed (under-attribute, never mis-attribute). */
  validFrom?: string;
};

export type LocalIdentityPaths = {
  claudeConfigPath?: string | null;
  codexAuthPath?: string | null;
};

/** Reads a Codex home's auth file once per observed mtime, retaining only its key. */
export class CodexAccountKeyCache {
  private readonly entries = new Map<string, { mtimeMs: number | null; key?: string }>();

  fromSessionsDir(sessionsDir: string): string | undefined {
    const file = path.join(path.dirname(sessionsDir), "auth.json");
    let mtimeMs: number | null = null;
    try {
      const stat = fs.lstatSync(file);
      if (stat.isFile() && !stat.isSymbolicLink()) mtimeMs = stat.mtimeMs;
    } catch { /* Missing or unreadable auth stays unattributed. */ }
    const old = this.entries.get(file);
    if (old?.mtimeMs === mtimeMs) return old.key;
    let key: string | undefined;
    if (mtimeMs !== null) {
      const auth = readJson(file);
      const tokens = auth?.tokens;
      const id = tokens && typeof tokens === "object" && !Array.isArray(tokens)
        ? (tokens as Record<string, unknown>).account_id : undefined;
      if (typeof id === "string" && id.length > 0) key = providerAccountKey(id);
    }
    this.entries.set(file, { mtimeMs, key });
    return key;
  }
}

/** Claude config roots use .claude.json; the default profile keeps it beside .claude. */
export class ClaudeAccountKeyCache {
  private readonly entries = new Map<string, { mtimeMs: number | null; key?: string }>();

  fromProjectsDir(projectsDir: string): string | undefined {
    const configDir = path.dirname(projectsDir);
    const explicitDir = process.env.CLAUDE_CONFIG_DIR && path.resolve(process.env.CLAUDE_CONFIG_DIR) === path.resolve(configDir);
    const file = !explicitDir && path.resolve(configDir) === path.join(os.homedir(), ".claude")
      ? path.join(os.homedir(), ".claude.json") : path.join(configDir, ".claude.json");
    let mtimeMs: number | null = null;
    try {
      const stat = fs.lstatSync(file);
      if (stat.isFile() && !stat.isSymbolicLink()) mtimeMs = stat.mtimeMs;
    } catch { /* Missing or unreadable account stays unattributed. */ }
    const old = this.entries.get(file);
    if (old?.mtimeMs === mtimeMs) return old.key;
    let key: string | undefined;
    if (mtimeMs !== null) {
      const config = readJson(file);
      const oauth = config?.oauthAccount;
      const id = oauth && typeof oauth === "object" && !Array.isArray(oauth)
        ? (oauth as Record<string, unknown>).accountUuid : undefined;
      if (typeof id === "string" && id.length > 0) key = providerAccountKey(id);
    }
    this.entries.set(file, { mtimeMs, key });
    return key;
  }
}

function readJson(file: string): Record<string, unknown> | undefined {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

function jwtClaims(token: unknown): Record<string, unknown> | undefined {
  if (typeof token !== "string") return undefined;
  const parts = token.split(".");
  if (parts.length < 2) return undefined;
  try {
    const payload = Buffer.from(parts[1], "base64url").toString("utf8");
    return JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

export function readLocalIdentities(paths: LocalIdentityPaths = {}): LocalIdentity[] {
  const identities: LocalIdentity[] = [];

  const claude = paths.claudeConfigPath === null ? undefined : readJson(paths.claudeConfigPath ??
    (process.env.CLAUDE_CONFIG_DIR ? path.join(process.env.CLAUDE_CONFIG_DIR, ".claude.json") : path.join(os.homedir(), ".claude.json")));
  const oauth = (claude?.oauthAccount ?? {}) as Record<string, unknown>;
  const claudeId = typeof oauth.accountUuid === "string" ? oauth.accountUuid : undefined;
  if (claudeId || typeof oauth.emailAddress === "string" && oauth.emailAddress.includes("@")) {
    identities.push({ source: "claude_code", email: typeof oauth.emailAddress === "string" ? oauth.emailAddress : undefined,
      actorHash: claudeId ? providerAccountKey(claudeId) : undefined });
  }

  const auth = paths.codexAuthPath === null ? undefined : readJson(paths.codexAuthPath ?? path.join(os.homedir(), ".codex", "auth.json"));
  if (auth) {
    const tokens = (auth.tokens ?? {}) as Record<string, unknown>;
    const claims = jwtClaims(tokens.id_token) ?? {};
    const apiAuth = (claims["https://api.openai.com/auth"] ?? {}) as Record<string, unknown>;
    const accountId = typeof tokens.account_id === "string" ? tokens.account_id :
      typeof apiAuth.chatgpt_account_id === "string" ? apiAuth.chatgpt_account_id : undefined;
    const email = typeof auth.email === "string" && auth.email.includes("@") ? auth.email : undefined;
    if (accountId || email) {
      identities.push({
        source: "codex",
        email,
        actorHash: accountId ? providerAccountKey(accountId) : undefined,
        planType: typeof apiAuth.chatgpt_plan_type === "string" ? apiAuth.chatgpt_plan_type : undefined,
        validFrom:
          typeof auth.last_refresh === "string" && !Number.isNaN(Date.parse(auth.last_refresh))
            ? new Date(Date.parse(auth.last_refresh)).toISOString()
            : undefined,
      });
    }
  }

  return identities;
}

/** Explicit enrollment inventory only. A missing configured file is not default-profile identity. */
export function readProfileIdentities(profiles: Array<{
  rootId: string; profileId: string; paths: LocalIdentityPaths;
}>): Array<{ rootId: string; profileId: string; identities: LocalIdentity[]; state: "observed" | "unavailable" }> {
  if (profiles.length > 64 || new Set(profiles.map(profile => profile.rootId)).size !== profiles.length) {
    throw new Error("invalid_identity_inventory");
  }
  return profiles.map(profile => {
    const identities = readLocalIdentities({
      claudeConfigPath: profile.paths.claudeConfigPath ?? null,
      codexAuthPath: profile.paths.codexAuthPath ?? null,
    });
    return { rootId: profile.rootId, profileId: profile.profileId, identities,
      state: identities.length ? "observed" as const : "unavailable" as const };
  });
}
