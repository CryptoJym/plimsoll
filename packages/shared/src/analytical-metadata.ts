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

export type AnalyticalScalarKind =
  | "token_count"
  | "nonnegative_number"
  | "status_code"
  | "boolean";

export type MetadataStringKind =
  | "signal"
  | "model"
  | "component"
  | "identifier"
  | "classification"
  | "version"
  | "trace"
  | "linkage"
  | "commit_sha"
  | "transport_path"
  | "http_method"
  | "timestamp";

export type OtlpAttributeSurface = "record" | "resource" | "scope";

export type MetadataKeyDisposition =
  | {
      valueKind: "analytical_scalar";
      scalarKind: AnalyticalScalarKind;
      otlpSurfaces: readonly OtlpAttributeSurface[];
      outbound: true;
    }
  | {
      valueKind: "string";
      stringKind: MetadataStringKind;
      otlpSurfaces: readonly OtlpAttributeSurface[];
      outbound: true;
    };

const RECORD_SURFACE = ["record"] as const;
const RESOURCE_SURFACE = ["resource"] as const;
const NO_OTLP_SURFACE = [] as const;

const RECORD_ANALYTICAL_SCALARS = new Map<string, AnalyticalScalarKind>([
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
  ["status.code", "status_code"],
  ["error", "boolean"],
  ["failed", "boolean"],
  ["success", "boolean"],
]);

const GENERATED_ANALYTICAL_SCALARS = new Map<string, AnalyticalScalarKind>([
  ["otelStatusCode", "status_code"],
  ["turnIndex", "token_count"],
  ["costEstimated", "boolean"],
  ["otelExplicitAction", "boolean"],
  ["otelHasError", "boolean"],
  ["otelHasException", "boolean"],
  ["repoStitched", "boolean"],
]);

// OTLP semantic-convention keys use their documented lowercase spelling.
// Camel-case entries below are explicit legacy producer aliases, not a
// case-insensitive contract: only the literal map key receives admission.
const RECORD_STRING_KEYS: Array<readonly [string, MetadataStringKind]> = [
  ...ACTOR_ID_KEYS.filter((key) => key !== "user.email").map(
    (key) => [key, "identifier"] as const,
  ),
  ...MODEL_KEYS.map((key) => [key, "model"] as const),
  ...SESSION_ID_KEYS.map((key) => [key, "identifier"] as const),
  ["event.name", "signal"],
  ["event.timestamp", "timestamp"],
  ["timestamp", "timestamp"],
  ["tool_name", "component"],
  ["toolName", "component"],
  ["tool", "component"],
  ["name", "component"],
  ["gen_ai.tool.name", "component"],
  ["plimsoll.action_class", "classification"],
  ["cfo_one.action_class", "classification"],
  ["action_class", "classification"],
  ["mcp_server", "component"],
  ["request_id", "identifier"],
  ["call_id", "identifier"],
  ["gen_ai.response.id", "identifier"],
  ["plimsoll.project", "linkage"],
  ["cfo_one.project", "linkage"],
  ["project_key", "linkage"],
  ["project", "linkage"],
  ["projectKey", "linkage"],
  ["plimsoll.customer", "identifier"],
  ["cfo_one.customer", "identifier"],
  ["customer_key", "identifier"],
  ["customer", "identifier"],
  ["customerKey", "identifier"],
  ["plimsoll.workflow", "identifier"],
  ["cfo_one.workflow", "identifier"],
  ["workflow_key", "identifier"],
  ["workflow", "identifier"],
  ["workflowKey", "identifier"],
  ["tenantId", "identifier"],
  ["tenant_id", "identifier"],
  ["observedAt", "timestamp"],
  ["observed_at", "timestamp"],
  ["time", "timestamp"],
  ["actionClass", "classification"],
  ["decision", "classification"],
  ["eventType", "classification"],
  ["event_type", "classification"],
  ["hook_event_name", "classification"],
  ["type", "classification"],
  ["error.type", "component"],
  ["exception.type", "component"],
  ["http.request.method", "http_method"],
  ["rpc.system", "component"],
  ["rpc.service", "component"],
  ["rpc.method", "component"],
  ["db.system", "component"],
  ["db.operation.name", "component"],
  ["remoteUrlHash", "linkage"],
  ["branchHash", "linkage"],
  ["repoHash", "linkage"],
  ["headSha", "commit_sha"],
  ["transport_path", "transport_path"],
];

const RESOURCE_STRING_KEYS: Array<readonly [string, MetadataStringKind]> = [
  ["service.name", "component"],
  ["service.version", "version"],
];

const GENERATED_STRING_KEYS: Array<readonly [string, MetadataStringKind]> = [
  ["otelEventName", "signal"],
  ["gen_ai.system", "component"],
  ["serviceName", "component"],
  ["toolClassDetail", "component"],
  ["originator", "classification"],
  ["otelOriginalActionClass", "classification"],
  ["planType", "classification"],
  ["stitched", "classification"],
  ["usageSource", "classification"],
  ["cliVersion", "version"],
  ["serviceVersion", "version"],
  ["spanId", "trace"],
  ["traceId", "trace"],
];

const METADATA_KEY_DISPOSITIONS = new Map<string, MetadataKeyDisposition>([
  ...[...RECORD_ANALYTICAL_SCALARS].map(
    ([key, scalarKind]) =>
      [
        key,
        { valueKind: "analytical_scalar", scalarKind, otlpSurfaces: RECORD_SURFACE, outbound: true },
      ] as const,
  ),
  ...[...GENERATED_ANALYTICAL_SCALARS].map(
    ([key, scalarKind]) =>
      [
        key,
        { valueKind: "analytical_scalar", scalarKind, otlpSurfaces: NO_OTLP_SURFACE, outbound: true },
      ] as const,
  ),
  ...RECORD_STRING_KEYS.map(
    ([key, stringKind]) =>
      [key, { valueKind: "string", stringKind, otlpSurfaces: RECORD_SURFACE, outbound: true }] as const,
  ),
  ...RESOURCE_STRING_KEYS.map(
    ([key, stringKind]) =>
      [key, { valueKind: "string", stringKind, otlpSurfaces: RESOURCE_SURFACE, outbound: true }] as const,
  ),
  ...GENERATED_STRING_KEYS.map(
    ([key, stringKind]) =>
      [key, { valueKind: "string", stringKind, otlpSurfaces: NO_OTLP_SURFACE, outbound: true }] as const,
  ),
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

const APPROVED_KEY_FINGERPRINTS = new Set(
  [...METADATA_KEY_DISPOSITIONS.keys()].map((key) => keyWords(key).join("")),
);

export function metadataKeyDisposition(key: string) {
  return METADATA_KEY_DISPOSITIONS.get(key);
}

export function isMetadataKeyVariantOfApprovedKey(key: string) {
  return !metadataKeyDisposition(key) && APPROVED_KEY_FINGERPRINTS.has(keyWords(key).join(""));
}

export function isDispositionAllowedOnOtlpSurface(
  disposition: MetadataKeyDisposition,
  surface: OtlpAttributeSurface,
) {
  return disposition.otlpSurfaces.includes(surface);
}

export function approvedAnalyticalScalarKind(key: string) {
  const disposition = metadataKeyDisposition(key);
  return disposition?.valueKind === "analytical_scalar" ? disposition.scalarKind : undefined;
}

/**
 * Classify the key before looking at its value type. Exact analytical keys
 * win over generic words such as `token`; every other private/raw/path key is
 * sensitive, including camel-case, separator, case, and numeric-suffix forms.
 */
export function isSensitiveMetadataSemanticKey(key: string) {
  if (metadataKeyDisposition(key)) return false;
  if (isMetadataKeyVariantOfApprovedKey(key)) return true;
  if (isForbiddenRawContentFieldName(key) || hasPrivateMetadataKeyConcept(key)) return true;
  return keyWords(key).some((word) => RAW_OR_PATH_SEMANTIC_WORDS.has(word));
}

const SAFE_IDENTIFIER = /^[a-zA-Z0-9][a-zA-Z0-9_.:+-]{0,159}$/;
const SAFE_COMPONENT_NAME = /^[a-zA-Z0-9][a-zA-Z0-9_.:+-]{0,199}$/;
const SAFE_CLASSIFICATION = /^[a-zA-Z0-9][a-zA-Z0-9_.:+-]{0,95}$/;
const SAFE_VERSION = /^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,63}$/;
const CANONICAL_LINKAGE = /^sha256:([a-f0-9]{64})$/i;
const COMMIT_SHA = /^[a-f0-9]{40}(?:[a-f0-9]{24})?$/i;
const EMAIL = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const SECRET_PREFIX =
  /(?:^|[^a-z0-9])(?:sk_live|sk_test|sk-|ghp[a-z0-9_-]*|github_pat[a-z0-9_-]*|xox[a-z0-9_-]*)/i;
const AUTH_SCHEME = /(?:^|[^a-z0-9])(?:bearer|basic)(?:\s|:|$)/i;
const JWT =
  /(?:^|[^a-z0-9_-])eyJ[a-z0-9_-]{6,}\.[a-z0-9_-]{6,}\.[a-z0-9_-]{6,}(?:$|[^a-z0-9_-])/i;
const PEM_PRIVATE_KEY = /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----/i;
const APPROVED_SLASH_SIGNAL_NAMES = new Set([
  "persist/rollout/items",
  "remotecontrol/enable",
  "thread/goal/get",
  "thread/list",
  "thread/read",
  "thread/resume",
]);

export function hasUnsafeMetadataString(
  value: unknown,
  options: { allowSlash?: boolean; allowSemanticLabel?: boolean } = {},
) {
  if (typeof value !== "string") return true;
  const candidate = value.trim();
  return (
    candidate.length === 0 ||
    !/^[\x20-\x7e]+$/.test(candidate) ||
    EMAIL.test(candidate) ||
    SECRET_PREFIX.test(candidate) ||
    AUTH_SCHEME.test(candidate) ||
    JWT.test(candidate) ||
    PEM_PRIVATE_KEY.test(candidate) ||
    (options.allowSemanticLabel
      ? keyWords(candidate).some((word) =>
          [
            "bearer",
            "cookie",
            "credential",
            "email",
            "oauth",
            "password",
            "private",
            "secret",
            "signing",
            "ssh",
          ].includes(word),
        )
      : hasPrivateMetadataKeyConcept(candidate)) ||
    /(?:^|[/.])\.\.(?:[/.]|$)/.test(candidate) ||
    /%(?:2e|2f|5c)/i.test(candidate) ||
    /(?:file|https?):\/\//i.test(candidate) ||
    /^www\./i.test(candidate) ||
    candidate.includes("\\") ||
    (!options.allowSlash && candidate.includes("/"))
  );
}

function safeStringByPattern(
  value: unknown,
  pattern: RegExp,
  maxLength: number,
  options: { normalizeSpaces?: boolean; allowSemanticLabel?: boolean } = {},
) {
  if (typeof value !== "string") return null;
  const candidate = value.trim();
  if (
    candidate.length > maxLength ||
    hasUnsafeMetadataString(candidate, { allowSemanticLabel: options.allowSemanticLabel })
  ) {
    return null;
  }
  const normalized = options.normalizeSpaces ? candidate.replace(/ +/g, "_") : candidate;
  return pattern.test(normalized) ? normalized : null;
}

/** Exact-key string validation shared by OTLP capture and outbound sealing. */
export function safeMetadataStringAttribute(key: string, value: unknown) {
  const disposition = metadataKeyDisposition(key);
  if (!disposition || disposition.valueKind !== "string") return null;
  const kind = disposition.stringKind;
  if (kind === "signal") {
    const lowCardinality = safeStringByPattern(value, SAFE_COMPONENT_NAME, 160, {
      normalizeSpaces: true,
      allowSemanticLabel: true,
    });
    if (lowCardinality) return lowCardinality;
    if (typeof value !== "string") return null;
    const candidate = value.trim();
    const canonicalSignal = candidate.toLowerCase();
    return !hasUnsafeMetadataString(candidate, { allowSlash: true, allowSemanticLabel: true }) &&
      APPROVED_SLASH_SIGNAL_NAMES.has(canonicalSignal)
      ? canonicalSignal
      : null;
  }
  if (kind === "model" || kind === "component") {
    return safeStringByPattern(value, SAFE_COMPONENT_NAME, 200, { normalizeSpaces: true });
  }
  if (kind === "identifier") return safeStringByPattern(value, SAFE_IDENTIFIER, 160);
  if (kind === "classification") {
    return safeStringByPattern(value, SAFE_CLASSIFICATION, 96, { normalizeSpaces: true });
  }
  if (kind === "version") return safeStringByPattern(value, SAFE_VERSION, 64);
  if (kind === "linkage") {
    if (typeof value !== "string" || hasUnsafeMetadataString(value)) return null;
    const match = value.trim().match(CANONICAL_LINKAGE);
    return match ? `sha256:${match[1].toLowerCase()}` : null;
  }
  if (kind === "commit_sha") {
    if (typeof value !== "string" || hasUnsafeMetadataString(value)) return null;
    const candidate = value.trim();
    return COMMIT_SHA.test(candidate) ? candidate.toLowerCase() : null;
  }
  if (kind === "transport_path") {
    return typeof value === "string" && ["/v1/logs", "/v1/traces", "/v1/metrics"].includes(value)
      ? value
      : null;
  }
  if (kind === "trace") {
    if (typeof value !== "string" || hasUnsafeMetadataString(value)) return null;
    const candidate = value.trim();
    return /^(?:[a-f0-9]{16}|[a-f0-9]{32})$/i.test(candidate)
      ? candidate.toLowerCase()
      : null;
  }
  if (kind === "http_method") {
    if (typeof value !== "string" || hasUnsafeMetadataString(value)) return null;
    const candidate = value.trim().toUpperCase();
    return /^[A-Z]{3,12}$/.test(candidate) ? candidate : null;
  }
  if (kind === "timestamp") {
    if (typeof value !== "string" || hasUnsafeMetadataString(value)) return null;
    const candidate = value.trim();
    return candidate.length <= 80 && !Number.isNaN(Date.parse(candidate)) ? candidate : null;
  }
  return null;
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

export function validatedMetadataAttribute(key: string, value: unknown) {
  const disposition = metadataKeyDisposition(key);
  if (!disposition) return { accepted: false as const };
  if (disposition.valueKind === "analytical_scalar") {
    return isApprovedAnalyticalScalarAttribute(key, value)
      ? { accepted: true as const, value }
      : { accepted: false as const };
  }
  const stringValue = safeMetadataStringAttribute(key, value);
  return stringValue === null
    ? { accepted: false as const }
    : { accepted: true as const, value: stringValue };
}

export function admittedMetadataAttributes(
  input: Record<string, unknown>,
  surface: OtlpAttributeSurface = "record",
) {
  const attributes: Record<string, unknown> = {};
  const rejectedKeys: string[] = [];
  for (const [key, value] of Object.entries(input)) {
    const disposition = metadataKeyDisposition(key);
    if (!disposition || !isDispositionAllowedOnOtlpSurface(disposition, surface)) {
      rejectedKeys.push(key);
      continue;
    }
    const validated = validatedMetadataAttribute(key, value);
    if (!validated.accepted) {
      rejectedKeys.push(key);
    } else {
      attributes[key] = validated.value;
    }
  }
  return { attributes, rejectedKeys };
}
