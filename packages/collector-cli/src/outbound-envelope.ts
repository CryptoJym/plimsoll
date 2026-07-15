import {
  aiWorkIngestEventSchema,
  aiWorkSessionSyncRowSchema,
  isForbiddenRawContentFieldName,
  type AiInteractionEvent,
  type AiWorkIngestEvent,
  type AiWorkSessionSyncRow,
} from "../../shared/src/index";

const CANONICAL_LINKAGE = /^sha256:([a-f0-9]{64})$/i;
const COMMIT_SHA = /^[a-f0-9]{40}(?:[a-f0-9]{24})?$/i;
const SAFE_SUPPRESSED_FIELD = /^[a-zA-Z0-9_.:-]{1,96}$/;
const SAFE_IDENTIFIER = /^[a-zA-Z0-9][a-zA-Z0-9_.:+-]{0,159}$/;
const SAFE_COMPONENT_NAME = /^[a-zA-Z0-9][a-zA-Z0-9_.:+-]{0,199}$/;
const SAFE_CLASSIFICATION = /^[a-zA-Z0-9][a-zA-Z0-9_.:+-]{0,95}$/;
const SAFE_VERSION = /^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,63}$/;
const EMAIL = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const SECRET_PREFIX = /(?:^|[^a-z0-9])(?:sk_live|sk_test|sk-|ghp[a-z0-9_-]*|github_pat[a-z0-9_-]*|xox[a-z0-9_-]*)/i;
const AUTH_SCHEME = /(?:^|[^a-z0-9])(?:bearer|basic)(?:\s|:|$)/i;
const JWT = /(?:^|[^a-z0-9_-])eyJ[a-z0-9_-]{6,}\.[a-z0-9_-]{6,}\.[a-z0-9_-]{6,}(?:$|[^a-z0-9_-])/i;
const PEM_PRIVATE_KEY = /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----/i;
const APPROVED_SLASH_SIGNAL_NAMES = new Set([
  "persist/rollout/items",
  "remotecontrol/enable",
  "thread/goal/get",
  "thread/list",
  "thread/read",
  "thread/resume",
]);
const SLASH_SIGNAL_KEYS = new Set(["event.name", "otelEventName"]);

const SIGNAL_STRING_KEYS = new Set([
  "event.name",
  "otelEventName",
]);

const MODEL_STRING_KEYS = new Set([
  "gen_ai.request.model",
  "model",
]);

const COMPONENT_STRING_KEYS = new Set([
  "db.operation.name",
  "db.system",
  "error.type",
  "exception.type",
  "gen_ai.system",
  "gen_ai.tool.name",
  "mcp_server",
  "rpc.method",
  "rpc.service",
  "rpc.system",
  "serviceName",
  "tool",
  "toolClassDetail",
  "toolName",
  "tool_name",
]);

const OPAQUE_ID_STRING_KEYS = new Set([
  "call_id",
  "gen_ai.response.id",
  "request_id",
]);

const CLASSIFICATION_STRING_KEYS = new Set([
  "action_class",
  "cfo_one.action_class",
  "decision",
  "originator",
  "otelOriginalActionClass",
  "planType",
  "plimsoll.action_class",
  "status.code",
  "stitched",
  "type",
  "usageSource",
]);

const VERSION_STRING_KEYS = new Set(["cliVersion", "serviceVersion"]);
const TRACE_STRING_KEYS = new Set(["spanId", "traceId"]);

const SAFE_BOOLEAN_KEYS = new Set([
  "costEstimated",
  "error",
  "failed",
  "otelExplicitAction",
  "otelHasError",
  "otelHasException",
  "repoStitched",
  "success",
]);

const SAFE_NUMBER_KEYS = new Set([
  "duration_ms",
  "event.sequence",
  "http.response.status_code",
  "otelStatusCode",
  "turnIndex",
]);

const OMIT_LOCAL_ONLY_KEYS = new Set([
  "externalEventId",
  "external_event_id",
  "cwd",
  "rolloutFile",
  "transcriptFile",
]);

const SENSITIVE_WORD = /^(?:access|api|args|argument|arguments|auth|authentication|authorization|bearer|body|command|content|cookie|cookies|credential|credentials|cwd|directory|dir|email|file|filename|folder|home|key|message|oauth|output|password|path|private|prompt|pwd|query|response|secret|signing|sql|ssh|stack|statement|token|tokens|uri|url|workdir|working|worktree)$/;

type MetadataOutcome =
  | { ok: true; metadata: Record<string, unknown>; omittedFields: string[] }
  | { ok: false };

export type OutboundEnvelopeOutcome =
  | { ok: true; envelope: AiWorkIngestEvent }
  | { ok: false; reason: "schema" | "privacy" };

export type OutboundSessionRowOutcome =
  | { ok: true; row: AiWorkSessionSyncRow }
  | { ok: false; reason: "schema" | "privacy" };

export function canonicalLinkage(value: string | null | undefined) {
  if (!value) return null;
  const match = value.trim().match(CANONICAL_LINKAGE);
  return match ? `sha256:${match[1].toLowerCase()}` : null;
}

function keyWords(key: string) {
  return key
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
}

function safeInteger(value: unknown) {
  const numeric =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^(?:0|[1-9][0-9]*)$/.test(value)
        ? Number(value)
        : Number.NaN;
  return Number.isSafeInteger(numeric) && numeric >= 0;
}

function safeFiniteNumber(value: unknown) {
  if (typeof value === "number") return Number.isFinite(value) && value >= 0;
  return typeof value === "string" &&
    /^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/.test(value) &&
    Number.isFinite(Number(value));
}

function safeBoolean(value: unknown) {
  return typeof value === "boolean" || value === "true" || value === "false";
}

function safeNumericTokenCounter(key: string, value: unknown) {
  const words = keyWords(key);
  if (!safeInteger(value) || !words.some((word) => /^tokens?$/.test(word))) return false;
  if (words.some((word) => SENSITIVE_WORD.test(word) && !/^(?:output|tokens?)$/.test(word))) {
    return false;
  }
  const collapsed = words.join("");
  return (
    /(?:input|output)tokens?$/.test(collapsed) ||
    /cache[a-z0-9]*tokens?$/.test(collapsed) ||
    /reasoning[a-z0-9]*tokens?$/.test(collapsed)
  );
}

function hasSecretValueConcept(value: string) {
  const words = keyWords(value);
  if (words.some((word) => ["credential", "credentials", "secret", "secrets", "password", "token", "tokens"].includes(word))) {
    return true;
  }
  const collapsed = words.join("");
  return collapsed.includes("apikey") || collapsed.includes("privatekey");
}

/**
 * Content-independent outbound value gate. Keys are not enough: a credential
 * can be placed under an otherwise approved field such as serviceName. This
 * check therefore runs before every field-specific string validator.
 */
export function hasUnsafeOutboundString(value: unknown, options: { allowSlash?: boolean } = {}) {
  if (typeof value !== "string") return true;
  const candidate = value.trim();
  if (
    candidate.length === 0 ||
    !/^[\x20-\x7e]+$/.test(candidate) ||
    EMAIL.test(candidate) ||
    SECRET_PREFIX.test(candidate) ||
    AUTH_SCHEME.test(candidate) ||
    JWT.test(candidate) ||
    PEM_PRIVATE_KEY.test(candidate) ||
    hasSecretValueConcept(candidate) ||
    /(?:^|[/.])\.\.(?:[/.]|$)/.test(candidate) ||
    /%(?:2e|2f|5c)/i.test(candidate) ||
    /(?:file|https?):\/\//i.test(candidate) ||
    /^www\./i.test(candidate) ||
    candidate.includes("\\") ||
    (!options.allowSlash && candidate.includes("/"))
  ) {
    return true;
  }
  return false;
}

function safeByPattern(
  value: unknown,
  pattern: RegExp,
  maxLength: number,
  options: { normalizeSpaces?: boolean } = {},
) {
  if (typeof value !== "string") return null;
  const candidate = value.trim();
  if (candidate.length > maxLength || hasUnsafeOutboundString(candidate)) return null;
  // A space is legitimate in bounded, typed low-cardinality telemetry (for
  // example an originator label). Normalize only literal ASCII spaces after
  // the unsafe-value gate; field-specific alphabets still reject all other
  // punctuation and high-cardinality material.
  const normalized = options.normalizeSpaces ? candidate.replace(/ +/g, "_") : candidate;
  return pattern.test(normalized) ? normalized : null;
}

export function safeOutboundIdentifier(value: unknown) {
  return safeByPattern(value, SAFE_IDENTIFIER, 160);
}

function safeComponentName(value: unknown) {
  return safeByPattern(value, SAFE_COMPONENT_NAME, 200, { normalizeSpaces: true });
}

function safeClassification(value: unknown) {
  return safeByPattern(value, SAFE_CLASSIFICATION, 96, { normalizeSpaces: true });
}

function safeVersion(value: unknown) {
  return safeByPattern(value, SAFE_VERSION, 64);
}

export function canonicalCommitSha(value: unknown) {
  if (typeof value !== "string" || hasUnsafeOutboundString(value)) return null;
  const candidate = value.trim();
  return COMMIT_SHA.test(candidate) ? candidate.toLowerCase() : null;
}

function safeSignalName(key: string, value: unknown) {
  const lowCardinality = safeByPattern(value, SAFE_COMPONENT_NAME, 160, { normalizeSpaces: true });
  if (lowCardinality) return lowCardinality;
  if (typeof value !== "string" || !SLASH_SIGNAL_KEYS.has(key)) return null;
  const candidate = value.trim();
  return !hasUnsafeOutboundString(candidate, { allowSlash: true }) && APPROVED_SLASH_SIGNAL_NAMES.has(candidate)
    ? candidate
    : null;
}

function safeMetadataString(key: string, value: unknown) {
  if (SIGNAL_STRING_KEYS.has(key)) return safeSignalName(key, value);
  if (MODEL_STRING_KEYS.has(key)) return safeComponentName(value);
  if (COMPONENT_STRING_KEYS.has(key)) return safeComponentName(value);
  if (OPAQUE_ID_STRING_KEYS.has(key)) return safeOutboundIdentifier(value);
  if (CLASSIFICATION_STRING_KEYS.has(key)) return safeClassification(value);
  if (VERSION_STRING_KEYS.has(key)) return safeVersion(value);
  if (TRACE_STRING_KEYS.has(key)) {
    if (typeof value !== "string" || hasUnsafeOutboundString(value)) return null;
    const candidate = value.trim();
    return /^(?:[a-f0-9]{16}|[a-f0-9]{32})$/i.test(candidate) ? candidate.toLowerCase() : null;
  }
  if (key === "http.request.method") {
    if (typeof value !== "string" || hasUnsafeOutboundString(value)) return null;
    const candidate = value.trim().toUpperCase();
    return /^[A-Z]{3,12}$/.test(candidate) ? candidate : null;
  }
  return null;
}

function sanitizeMetadata(input: Record<string, unknown>): MetadataOutcome {
  const metadata: Record<string, unknown> = {};
  const omittedFields: string[] = [];
  const recordOmission = (key: string) => {
    if (SAFE_SUPPRESSED_FIELD.test(key) && !hasSecretValueConcept(key)) omittedFields.push(key);
  };

  for (const [key, value] of Object.entries(input)) {
    if (OMIT_LOCAL_ONLY_KEYS.has(key) || isForbiddenRawContentFieldName(key)) {
      recordOmission(key);
      continue;
    }
    if (!/^[\x20-\x7e]+$/.test(key)) continue;
    if (value === null || value === undefined) continue;

    if (key === "git") {
      if (!value || typeof value !== "object" || Array.isArray(value)) return { ok: false };
      const source = value as Record<string, unknown>;
      if (Object.keys(source).some((nestedKey) => !["remoteUrlHash", "branchHash", "headSha"].includes(nestedKey))) {
        return { ok: false };
      }
      const git: Record<string, string> = {};
      for (const linkageKey of ["remoteUrlHash", "branchHash"] as const) {
        if (source[linkageKey] === undefined) continue;
        const canonical = typeof source[linkageKey] === "string"
          ? canonicalLinkage(source[linkageKey])
          : null;
        if (!canonical) return { ok: false };
        git[linkageKey] = canonical;
      }
      if (source.headSha !== undefined) {
        const headSha = canonicalCommitSha(source.headSha);
        if (!headSha) return { ok: false };
        git.headSha = headSha;
      }
      if (Object.keys(git).length > 0) metadata.git = git;
      continue;
    }

    if (["remoteUrlHash", "branchHash", "repoHash"].includes(key)) {
      const canonical = typeof value === "string" ? canonicalLinkage(value) : null;
      if (!canonical) return { ok: false };
      metadata[key] = canonical;
      continue;
    }
    if (key === "headSha") {
      const headSha = canonicalCommitSha(value);
      if (!headSha) return { ok: false };
      metadata[key] = headSha;
      continue;
    }
    if (key === "transport_path") {
      if (typeof value !== "string" || !["/v1/logs", "/v1/traces", "/v1/metrics"].includes(value)) {
        return { ok: false };
      }
      metadata[key] = value;
      continue;
    }
    if (safeNumericTokenCounter(key, value)) {
      metadata[key] = value;
      continue;
    }
    if (SAFE_NUMBER_KEYS.has(key)) {
      if (!safeFiniteNumber(value)) return { ok: false };
      metadata[key] = value;
      continue;
    }
    if (SAFE_BOOLEAN_KEYS.has(key)) {
      if (!safeBoolean(value)) return { ok: false };
      metadata[key] = value;
      continue;
    }
    if (
      SIGNAL_STRING_KEYS.has(key) ||
      MODEL_STRING_KEYS.has(key) ||
      COMPONENT_STRING_KEYS.has(key) ||
      OPAQUE_ID_STRING_KEYS.has(key) ||
      CLASSIFICATION_STRING_KEYS.has(key) ||
      VERSION_STRING_KEYS.has(key) ||
      TRACE_STRING_KEYS.has(key) ||
      key === "http.request.method"
    ) {
      const safe = safeMetadataString(key, value);
      if (!safe) return { ok: false };
      metadata[key] = safe;
      continue;
    }
    if (key === "otelSignalNames") {
      if (!Array.isArray(value) || value.length > 64) return { ok: false };
      const names = value.map((name) => safeSignalName("otelEventName", name));
      if (names.some((name) => name === null)) return { ok: false };
      metadata[key] = names;
      continue;
    }
    if (key === "otelSignalTimestamps") {
      if (
        !Array.isArray(value) ||
        value.length > 64 ||
        value.some((entry) => typeof entry !== "string" || Number.isNaN(Date.parse(entry)))
      ) {
        return { ok: false };
      }
      metadata[key] = value;
      continue;
    }
    if (key === "otelAttributes") {
      if (!value || typeof value !== "object" || Array.isArray(value)) return { ok: false };
      const nested = sanitizeMetadata(value as Record<string, unknown>);
      if (!nested.ok) return nested;
      if (Object.keys(nested.metadata).length > 0) metadata[key] = nested.metadata;
      omittedFields.push(...nested.omittedFields);
      continue;
    }
    // Unknown metadata is local-only by default, including legacy raw fields
    // and sensitive-looking keys. Values never cross; bounded ASCII field
    // names remain in suppression receipts for auditability.
    recordOmission(key);
  }

  return { ok: true, metadata, omittedFields: [...new Set(omittedFields)] };
}

function safeTopLevelIdentifier(value: string | undefined) {
  if (value === undefined) return { ok: true as const, value: undefined };
  const safe = safeOutboundIdentifier(value);
  return safe ? { ok: true as const, value: safe } : { ok: false as const };
}

function safeTopLevelProjectKey(value: string | undefined) {
  if (value === undefined) return { ok: true as const, value: undefined };
  const safe = canonicalLinkage(value);
  return safe ? { ok: true as const, value: safe } : { ok: false as const };
}

function safeTopLevelModel(value: string | undefined) {
  if (value === undefined) return { ok: true as const, value: undefined };
  const safe = safeComponentName(value);
  return safe ? { ok: true as const, value: safe } : { ok: false as const };
}

export function sealOutboundEvent(event: AiInteractionEvent) {
  const metadata = sanitizeMetadata(event.metadata);
  if (!metadata.ok) return { ok: false as const, reason: "privacy" as const };
  const id = safeTopLevelIdentifier(event.id);
  const sessionId = safeTopLevelIdentifier(event.sessionId);
  const tenantId = safeTopLevelIdentifier(event.tenantId);
  // Actor identifiers are privacy-preserving local aliases today, including
  // legacy truncated `sha256:` aliases. They are identifiers, not repo
  // linkage, so require the bounded character contract without silently
  // upgrading them to the 256-bit linkage namespace.
  const actorId = safeTopLevelIdentifier(event.actorId);
  const projectKey = safeTopLevelProjectKey(event.projectKey);
  const customerKey = safeTopLevelIdentifier(event.customerKey);
  const workflowKey = safeTopLevelIdentifier(event.workflowKey);
  const model = safeTopLevelModel(event.model);
  if (
    !id.ok ||
    !sessionId.ok ||
    !tenantId.ok ||
    !actorId.ok ||
    !projectKey.ok ||
    !customerKey.ok ||
    !workflowKey.ok ||
    !model.ok
  ) {
    return { ok: false as const, reason: "privacy" as const };
  }
  return {
    ok: true as const,
    event: {
      ...event,
      id: id.value!,
      ...(sessionId.value === undefined ? {} : { sessionId: sessionId.value }),
      ...(tenantId.value === undefined ? {} : { tenantId: tenantId.value }),
      ...(actorId.value === undefined ? {} : { actorId: actorId.value }),
      ...(projectKey.value === undefined ? {} : { projectKey: projectKey.value }),
      ...(customerKey.value === undefined ? {} : { customerKey: customerKey.value }),
      ...(workflowKey.value === undefined ? {} : { workflowKey: workflowKey.value }),
      ...(model.value === undefined ? {} : { model: model.value }),
      metadata: metadata.metadata,
    },
    omittedFields: metadata.omittedFields,
  };
}

export function sealOutboundEnvelope(input: unknown): OutboundEnvelopeOutcome {
  const parsed = aiWorkIngestEventSchema.safeParse(input);
  if (!parsed.success) return { ok: false, reason: "schema" };
  const sealed = sealOutboundEvent(parsed.data.event);
  if (!sealed.ok) return sealed;
  const envelope = aiWorkIngestEventSchema.safeParse({
    event: sealed.event,
    suppressedFields: [...new Set(
      [...parsed.data.suppressedFields, ...sealed.omittedFields]
        .map((field) => field.trim())
        .filter((field) => SAFE_SUPPRESSED_FIELD.test(field) && !hasSecretValueConcept(field)),
    )],
  });
  return envelope.success
    ? { ok: true, envelope: envelope.data }
    : { ok: false, reason: "schema" };
}

/** Session snapshots use the same outbound boundary as events. Only typed
 * counters plus canonical linkage and a privacy-safe actor alias may cross. */
export function sealOutboundSessionRow(input: unknown): OutboundSessionRowOutcome {
  const parsed = aiWorkSessionSyncRowSchema.safeParse(input);
  if (!parsed.success) return { ok: false, reason: "schema" };
  const { session } = parsed.data;
  const id = safeOutboundIdentifier(session.id);
  const tenantId = session.tenantId === undefined ? undefined : safeOutboundIdentifier(session.tenantId);
  const actorId = session.actorId === undefined ? undefined : safeOutboundIdentifier(session.actorId);
  const deviceInstallId = session.deviceInstallId === undefined
    ? undefined
    : safeOutboundIdentifier(session.deviceInstallId);
  const projectKey = session.projectKey === undefined ? undefined : canonicalLinkage(session.projectKey);
  const customerKey = session.customerKey === undefined ? undefined : safeOutboundIdentifier(session.customerKey);
  const workflowKey = session.workflowKey === undefined ? undefined : safeOutboundIdentifier(session.workflowKey);
  const repositoryId = session.repositoryId === undefined ? undefined : canonicalLinkage(session.repositoryId);
  const repoPathHash = session.repoPathHash === undefined ? undefined : canonicalLinkage(session.repoPathHash);
  if (
    !id ||
    (session.tenantId !== undefined && !tenantId) ||
    (session.actorId !== undefined && !actorId) ||
    (session.deviceInstallId !== undefined && !deviceInstallId) ||
    (session.projectKey !== undefined && !projectKey) ||
    (session.customerKey !== undefined && !customerKey) ||
    (session.workflowKey !== undefined && !workflowKey) ||
    (session.repositoryId !== undefined && !repositoryId) ||
    (session.repoPathHash !== undefined && !repoPathHash) ||
    session.branch !== undefined
  ) {
    return { ok: false, reason: "privacy" };
  }

  const metadataSource = session.metadata as Record<string, unknown>;
  const metadata: Record<string, string> = {};
  if (metadataSource.branchHash !== undefined) {
    const branchHash = typeof metadataSource.branchHash === "string"
      ? canonicalLinkage(metadataSource.branchHash)
      : null;
    if (!branchHash) return { ok: false, reason: "privacy" };
    metadata.branchHash = branchHash;
  }
  if (metadataSource.externalActorId !== undefined) {
    const externalActorId = safeOutboundIdentifier(metadataSource.externalActorId);
    if (!externalActorId) return { ok: false, reason: "privacy" };
    metadata.externalActorId = externalActorId;
  }

  const sealed = aiWorkSessionSyncRowSchema.safeParse({
    ...parsed.data,
    session: {
      ...session,
      id,
      ...(tenantId === undefined ? {} : { tenantId }),
      ...(actorId === undefined ? {} : { actorId }),
      ...(deviceInstallId === undefined ? {} : { deviceInstallId }),
      ...(repositoryId === undefined ? {} : { repositoryId }),
      ...(repoPathHash === undefined ? {} : { repoPathHash }),
      ...(projectKey === undefined ? {} : { projectKey }),
      ...(customerKey === undefined ? {} : { customerKey }),
      ...(workflowKey === undefined ? {} : { workflowKey }),
      metadata,
    },
  });
  return sealed.success
    ? { ok: true, row: sealed.data }
    : { ok: false, reason: "schema" };
}
