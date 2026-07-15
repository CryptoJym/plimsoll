import { isForbiddenRawContentFieldName } from "./schemas";

const SESSION_ID_KEYS = [
  "sessionId",
  "session_id",
  "conversation.id",
  "conversation_id",
  "thread_id",
  "session.id",
  "gen_ai.session.id",
] as const;

const MODEL_KEYS = ["model", "slug", "gen_ai.request.model", "gen_ai.response.model"] as const;

const INPUT_TOKEN_KEYS = [
  "inputTokens",
  "input_tokens",
  "gen_ai.usage.input_tokens",
  "llm.usage.prompt_tokens",
] as const;

const OUTPUT_TOKEN_KEYS = [
  "outputTokens",
  "output_tokens",
  "gen_ai.usage.output_tokens",
  "llm.usage.completion_tokens",
] as const;

const CACHE_READ_TOKEN_KEYS = [
  "cacheReadTokens",
  "cache_read_tokens",
  "gen_ai.usage.cache_read_tokens",
  "gen_ai.usage.cache_read.input_tokens",
  "gen_ai.usage.cached_tokens",
] as const;

const CACHE_CREATION_TOKEN_KEYS = [
  "cacheCreationTokens",
  "cache_creation_tokens",
  "cache_creation_input_tokens",
  "cache_creation.input_tokens",
  "gen_ai.usage.cache_creation_input_tokens",
  "gen_ai.usage.cache_creation.input_tokens",
] as const;

const COST_KEYS = [
  "costUsd",
  "cost_usd",
  "estimated_cost_usd",
  "gen_ai.usage.cost_usd",
  "plimsoll.estimated_cost_usd",
  "cfo_one.estimated_cost_usd",
] as const;

const ACTOR_ID_KEYS = [
  "actorId",
  "actor_id",
  "user.id",
  "user.account_id",
  "user.account_uuid",
  "user_id",
  "userId",
  "user.email",
] as const;

export const usageFieldKeys = {
  actorId: ACTOR_ID_KEYS,
  cacheReadTokens: CACHE_READ_TOKEN_KEYS,
  cacheCreationTokens: CACHE_CREATION_TOKEN_KEYS,
  costUsd: COST_KEYS,
  inputTokens: INPUT_TOKEN_KEYS,
  model: MODEL_KEYS,
  outputTokens: OUTPUT_TOKEN_KEYS,
  sessionId: SESSION_ID_KEYS,
} as const;

export type AnalyticalScalarKind = "token_count" | "nonnegative_number" | "status_code" | "boolean";

const APPROVED_ANALYTICAL_SCALARS = new Map<string, AnalyticalScalarKind>([
  ...[
    ...INPUT_TOKEN_KEYS,
    ...OUTPUT_TOKEN_KEYS,
    ...CACHE_READ_TOKEN_KEYS,
    ...CACHE_CREATION_TOKEN_KEYS,
  ].map((key) => [key, "token_count"] as const),
  ...COST_KEYS.map((key) => [key, "nonnegative_number"] as const),
  ["duration_ms", "nonnegative_number"],
  ["event.sequence", "token_count"],
  ["http.response.status_code", "status_code"],
  ["otelStatusCode", "status_code"],
  ["status.code", "status_code"],
  ["turnIndex", "token_count"],
  ["costEstimated", "boolean"],
  ["error", "boolean"],
  ["failed", "boolean"],
  ["otelExplicitAction", "boolean"],
  ["otelHasError", "boolean"],
  ["otelHasException", "boolean"],
  ["repoStitched", "boolean"],
  ["success", "boolean"],
]);

function keyWords(value: string) {
  return value
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean)
    .map((word) => word.replace(/[0-9]+$/g, ""))
    .filter(Boolean);
}

const PRIVATE_SEMANTIC_WORDS = new Set([
  "access",
  "auth",
  "authentication",
  "authorization",
  "bearer",
  "cookie",
  "cookies",
  "credential",
  "credentials",
  "email",
  "oauth",
  "password",
  "private",
  "secret",
  "secrets",
  "signing",
  "ssh",
  "token",
  "tokens",
]);

const RAW_OR_PATH_SEMANTIC_WORDS = new Set([
  "args",
  "argument",
  "arguments",
  "body",
  "command",
  "content",
  "cwd",
  "directory",
  "dir",
  "file",
  "filepath",
  "filename",
  "folder",
  "home",
  "message",
  "output",
  "patch",
  "path",
  "prompt",
  "pwd",
  "query",
  "response",
  "sql",
  "stack",
  "stacktrace",
  "statement",
  "uri",
  "url",
  "workdir",
  "working",
  "worktree",
]);

/** Private key concepts are genericized in receipts so attacker-controlled credential names do not echo. */
export function hasPrivateMetadataKeyConcept(key: string) {
  const words = keyWords(key);
  if (words.some((word) => PRIVATE_SEMANTIC_WORDS.has(word))) return true;
  const collapsed = words.join("");
  return [
    "accesskey",
    "apikey",
    "clientsecret",
    "privatekey",
    "signingkey",
    "sshkey",
  ].some((concept) => collapsed.includes(concept));
}

export function approvedAnalyticalScalarKind(key: string) {
  return APPROVED_ANALYTICAL_SCALARS.get(key);
}

/**
 * Classify the key before looking at its value type. Exact analytical keys
 * win over generic words such as `token`; every other private/raw/path key is
 * sensitive, including camel-case, separator, case, and numeric-suffix forms.
 */
export function isSensitiveMetadataSemanticKey(key: string) {
  if (approvedAnalyticalScalarKind(key)) return false;
  if (isForbiddenRawContentFieldName(key) || hasPrivateMetadataKeyConcept(key)) return true;
  return keyWords(key).some((word) => RAW_OR_PATH_SEMANTIC_WORDS.has(word));
}

function finiteNonnegative(value: unknown) {
  if (typeof value === "number") return Number.isFinite(value) && value >= 0;
  return (
    typeof value === "string" &&
    /^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/.test(value) &&
    Number.isFinite(Number(value))
  );
}

function nonnegativeSafeInteger(value: unknown) {
  if (!finiteNonnegative(value)) return false;
  return Number.isSafeInteger(Number(value));
}

/** One value validator is shared by OTLP capture and outbound sealing. */
export function isApprovedAnalyticalScalarAttribute(key: string, value: unknown) {
  const kind = approvedAnalyticalScalarKind(key);
  if (!kind) return false;
  if (kind === "boolean") {
    return typeof value === "boolean" || value === "true" || value === "false";
  }
  if (kind === "token_count") return nonnegativeSafeInteger(value);
  if (kind === "status_code") {
    if (typeof value === "string" && /^(?:OK|ERROR|UNSET)$/i.test(value)) return true;
    return nonnegativeSafeInteger(value) && Number(value) <= 999;
  }
  return finiteNonnegative(value);
}
