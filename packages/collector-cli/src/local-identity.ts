import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { hashProtectedValue } from "../../shared/src/policy";

/**
 * Local account identity (issue 0028). AI-tool accounts are tied to emails;
 * the emails and account ids are readable from each tool's LOCAL config:
 *   - Claude Code: ~/.claude.json → oauthAccount.emailAddress
 *   - Codex: ~/.codex/auth.json → email, and the id_token's
 *     https://api.openai.com/auth claims (chatgpt_account_id, plan type,
 *     last_refresh for the honest-attribution window)
 *
 * Everything read here is LOCAL-ONLY material: raw emails/ids go into
 * account_labels (never uploaded, proof-enforced); only the deterministic
 * hash of the codex account id is stamped onto events. Wire telemetry
 * identities do NOT derive from these values (verified 2026-06-10 — no hash
 * chain matches), so nothing here is ever auto-attached to telemetry-derived
 * hashes; humans link identities via merge/labels.
 */

export type LocalIdentity = {
  source: "claude_code" | "codex";
  email?: string;
  /** codex only: stable identity hash for rollout-derived events. */
  actorHash?: string;
  /** codex only: chatgpt_plan_type (e.g. "pro") for plan-leverage suggestions. */
  planType?: string;
  /** codex only: ISO time of the current login's last refresh. Sessions that
   * started at/after this instant provably ran under this identity; earlier
   * sessions stay unattributed (under-attribute, never mis-attribute). */
  validFrom?: string;
};

export type LocalIdentityPaths = {
  claudeConfigPath?: string;
  codexAuthPath?: string;
};

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

  const claude = readJson(paths.claudeConfigPath ?? path.join(os.homedir(), ".claude.json"));
  const oauth = (claude?.oauthAccount ?? {}) as Record<string, unknown>;
  if (typeof oauth.emailAddress === "string" && oauth.emailAddress.includes("@")) {
    identities.push({ source: "claude_code", email: oauth.emailAddress });
  }

  const auth = readJson(paths.codexAuthPath ?? path.join(os.homedir(), ".codex", "auth.json"));
  if (auth) {
    const tokens = (auth.tokens ?? {}) as Record<string, unknown>;
    const claims = jwtClaims(tokens.id_token) ?? {};
    const apiAuth = (claims["https://api.openai.com/auth"] ?? {}) as Record<string, unknown>;
    const accountId = typeof apiAuth.chatgpt_account_id === "string" ? apiAuth.chatgpt_account_id : undefined;
    const email = typeof auth.email === "string" && auth.email.includes("@") ? auth.email : undefined;
    if (accountId || email) {
      identities.push({
        source: "codex",
        email,
        actorHash: accountId ? hashProtectedValue(accountId) : undefined,
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
