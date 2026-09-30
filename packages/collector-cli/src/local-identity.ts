import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import type Database from "better-sqlite3";

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

export type AccountKeyObservation = { key?: string; signature: string | null };

/** A provider-home history contains keys only, never source account ids or paths. */
export class AccountBindingHistory {
  private readonly homeDigests = new Map<string, string>();
  private readonly windows = new Map<string, Array<{ firstSeenMs: number; untilMs: number | null; accountKey: string }>>();

  constructor(private readonly db: Database.Database, private readonly source: "codex" | "claude_code") {
    db.exec(`create table if not exists account_binding_windows (
      source text not null, home_digest text not null, first_seen_ms integer not null,
      last_seen_ms integer not null, until_ms integer, account_key text not null,
      file_signature text,
      primary key (source, home_digest, first_seen_ms)
    )`);
  }

  private homeDigest(home: string): string {
    const resolved = path.resolve(home);
    let digest = this.homeDigests.get(resolved);
    if (!digest) {
      digest = crypto.createHash("sha256").update(resolved).digest("hex");
      this.homeDigests.set(resolved, digest);
    }
    return digest;
  }

  /** A changed auth file closes the old window at the last confirmed sighting.
   * The unobserved gap cannot be assigned even if the key later returns. */
  observe(home: string, account: AccountKeyObservation, seenAtMs: number): void {
    if (!Number.isSafeInteger(seenAtMs)) return;
    const homeDigest = this.homeDigest(home);
    const current = this.db.prepare(`select first_seen_ms as firstSeenMs, last_seen_ms as lastSeenMs,
      account_key as accountKey, file_signature as fileSignature
      from account_binding_windows where source=? and home_digest=? and until_ms is null
      order by first_seen_ms desc limit 1`).get(this.source, homeDigest) as
      { firstSeenMs: number; lastSeenMs: number; accountKey: string; fileSignature: string | null } | undefined;
    if (current && current.accountKey === account.key && current.fileSignature === account.signature) {
      if (seenAtMs > current.lastSeenMs) this.db.prepare(`update account_binding_windows set last_seen_ms=?
        where source=? and home_digest=? and first_seen_ms=?`)
        .run(seenAtMs, this.source, homeDigest, current.firstSeenMs);
      return;
    }
    if (current) {
      const until = current.lastSeenMs + 1;
      this.db.prepare(`update account_binding_windows set until_ms=?
        where source=? and home_digest=? and first_seen_ms=?`)
        .run(until, this.source, homeDigest, current.firstSeenMs);
    }
    if (account.key && /^sha256:[a-f0-9]{16}$/.test(account.key)) {
      const start = Math.max(seenAtMs, current ? current.lastSeenMs + 1 : seenAtMs);
      this.db.prepare(`insert into account_binding_windows
        (source,home_digest,first_seen_ms,last_seen_ms,until_ms,account_key,file_signature)
        values (?,?,?,?,null,?,?)
        on conflict(source,home_digest,first_seen_ms) do update set
          last_seen_ms=excluded.last_seen_ms,until_ms=null,account_key=excluded.account_key,
          file_signature=excluded.file_signature`)
        .run(this.source, homeDigest, start, start, account.key, account.signature);
    }
    this.windows.delete(homeDigest);
  }

  keyAt(home: string, eventAt: string | undefined, knownAtMs: number): string | undefined {
    if (!eventAt) return undefined;
    const at = Date.parse(eventAt);
    if (!Number.isFinite(at) || at > knownAtMs) return undefined;
    const digest = this.homeDigest(home);
    let windows = this.windows.get(digest);
    if (!windows) {
      windows = this.db.prepare(`select first_seen_ms as firstSeenMs, until_ms as untilMs,
        account_key as accountKey from account_binding_windows where source=? and home_digest=?
        order by first_seen_ms`).all(this.source, digest) as
        Array<{ firstSeenMs: number; untilMs: number | null; accountKey: string }>;
      this.windows.set(digest, windows);
    }
    for (let index = windows.length - 1; index >= 0; index -= 1) {
      const window = windows[index]!;
      if (at >= window.firstSeenMs && (window.untilMs === null || at < window.untilMs)) return window.accountKey;
    }
    return undefined;
  }
}

/** Reads a Codex home's auth file once per stat signature, retaining only its key. */
export class CodexAccountKeyCache {
  private readonly entries = new Map<string, { signature: string | null; observation: AccountKeyObservation;
    missingParentMtimeNs?: string }>();

  fromSessionsDir(sessionsDir: string): string | undefined {
    return this.observationFromSessionsDir(sessionsDir).key;
  }

  observationFromSessionsDir(sessionsDir: string): AccountKeyObservation {
    const file = path.join(path.dirname(sessionsDir), "auth.json");
    const old = this.entries.get(file);
    if (old?.signature === null && old.missingParentMtimeNs !== undefined) {
      try {
        const parent = fs.lstatSync(path.dirname(file), { bigint: true });
        if (parent.isDirectory() && String(parent.mtimeNs) === old.missingParentMtimeNs) return old.observation;
      } catch { /* Parent changed or disappeared: retry the auth path. */ }
    }
    let signature: string | null = null;
    try {
      const stat = fs.lstatSync(file, { bigint: true });
      if (stat.isFile() && !stat.isSymbolicLink()) {
        signature = crypto.createHash("sha256")
          .update(`${stat.dev}:${stat.ino}:${stat.mtimeNs}:${stat.ctimeNs}:${stat.size}`).digest("hex");
      }
    } catch { /* Missing or unreadable auth stays unattributed. */ }
    if (signature !== null && old?.signature === signature) return old.observation;
    let key: string | undefined;
    if (signature !== null) {
      const auth = readJson(file);
      const tokens = auth?.tokens;
      const id = tokens && typeof tokens === "object" && !Array.isArray(tokens)
        ? (tokens as Record<string, unknown>).account_id : undefined;
      if (typeof id === "string" && id.length > 0) key = providerAccountKey(id);
    }
    const observation = { key, signature };
    let missingParentMtimeNs: string | undefined;
    if (signature === null) {
      try {
        const parent = fs.lstatSync(path.dirname(file), { bigint: true });
        if (parent.isDirectory()) missingParentMtimeNs = String(parent.mtimeNs);
      } catch { /* Missing parent stays unattributed. */ }
    }
    this.entries.set(file, { signature, observation, missingParentMtimeNs });
    return observation;
  }
}

/** Claude config roots use .claude.json; the default profile keeps it beside .claude. */
export class ClaudeAccountKeyCache {
  private readonly entries = new Map<string, { signature: string | null; observation: AccountKeyObservation;
    missingParentMtimeNs?: string }>();

  fromProjectsDir(projectsDir: string): string | undefined {
    return this.observationFromProjectsDir(projectsDir).key;
  }

  observationFromProjectsDir(projectsDir: string): AccountKeyObservation {
    const configDir = path.dirname(projectsDir);
    const explicitDir = process.env.CLAUDE_CONFIG_DIR && path.resolve(process.env.CLAUDE_CONFIG_DIR) === path.resolve(configDir);
    const file = !explicitDir && path.resolve(configDir) === path.join(os.homedir(), ".claude")
      ? path.join(os.homedir(), ".claude.json") : path.join(configDir, ".claude.json");
    const old = this.entries.get(file);
    if (old?.signature === null && old.missingParentMtimeNs !== undefined) {
      try {
        const parent = fs.lstatSync(path.dirname(file), { bigint: true });
        if (parent.isDirectory() && String(parent.mtimeNs) === old.missingParentMtimeNs) return old.observation;
      } catch { /* Parent changed or disappeared: retry the account path. */ }
    }
    let signature: string | null = null;
    try {
      const stat = fs.lstatSync(file, { bigint: true });
      if (stat.isFile() && !stat.isSymbolicLink()) {
        signature = crypto.createHash("sha256")
          .update(`${stat.dev}:${stat.ino}:${stat.mtimeNs}:${stat.ctimeNs}:${stat.size}`).digest("hex");
      }
    } catch { /* Missing or unreadable account stays unattributed. */ }
    if (signature !== null && old?.signature === signature) return old.observation;
    let key: string | undefined;
    if (signature !== null) {
      const config = readJson(file);
      const oauth = config?.oauthAccount;
      const id = oauth && typeof oauth === "object" && !Array.isArray(oauth)
        ? (oauth as Record<string, unknown>).accountUuid : undefined;
      if (typeof id === "string" && id.length > 0) key = providerAccountKey(id);
    }
    const observation = { key, signature };
    let missingParentMtimeNs: string | undefined;
    if (signature === null) {
      try {
        const parent = fs.lstatSync(path.dirname(file), { bigint: true });
        if (parent.isDirectory()) missingParentMtimeNs = String(parent.mtimeNs);
      } catch { /* Missing parent stays unattributed. */ }
    }
    this.entries.set(file, { signature, observation, missingParentMtimeNs });
    return observation;
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
