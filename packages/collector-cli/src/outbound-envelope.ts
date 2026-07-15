import {
  aiWorkIngestEventSchema,
  findForbiddenRawContentFields,
  type AiInteractionEvent,
  type AiWorkIngestEvent,
} from "../../shared/src/index";

const CANONICAL_LINKAGE = /^sha256:([a-f0-9]{64})$/i;
const COMMIT_SHA = /^[a-f0-9]{40}(?:[a-f0-9]{24})?$/i;
const SAFE_SUPPRESSED_FIELD = /^[a-zA-Z0-9_.:-]{1,96}$/;
const SAFE_LOW_CARDINALITY = /^[a-zA-Z0-9_.:+-]{1,160}$/;
const EMAIL = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const APPROVED_SLASH_SIGNAL_NAMES = new Set([
  "persist/rollout/items",
  "remotecontrol/enable",
  "thread/goal/get",
  "thread/list",
  "thread/read",
  "thread/resume",
]);
const SLASH_SIGNAL_KEYS = new Set(["event.name", "otelEventName"]);

const SAFE_STRING_KEYS = new Set([
  "action_class",
  "call_id",
  "cfo_one.action_class",
  "cliVersion",
  "db.operation.name",
  "db.system",
  "decision",
  "error.type",
  "event.name",
  "exception.type",
  "gen_ai.request.model",
  "gen_ai.response.id",
  "gen_ai.system",
  "gen_ai.tool.name",
  "http.request.method",
  "mcp_server",
  "model",
  "originator",
  "otelEventName",
  "otelOriginalActionClass",
  "planType",
  "plimsoll.action_class",
  "request_id",
  "rpc.method",
  "rpc.service",
  "rpc.system",
  "serviceName",
  "serviceVersion",
  "spanId",
  "status.code",
  "stitched",
  "tool",
  "toolClassDetail",
  "toolName",
  "tool_name",
  "traceId",
  "type",
  "usageSource",
]);

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
  | { ok: true; metadata: Record<string, unknown> }
  | { ok: false };

export type OutboundEnvelopeOutcome =
  | { ok: true; envelope: AiWorkIngestEvent }
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

function hasSensitiveConcept(key: string) {
  return keyWords(key).some((word) => SENSITIVE_WORD.test(word));
}

function safeLowCardinality(value: unknown) {
  if (typeof value !== "string") return null;
  const candidate = value.trim();
  if (
    !SAFE_LOW_CARDINALITY.test(candidate) ||
    EMAIL.test(candidate) ||
    /^(?:\/|~\/|\.{1,2}\/|file:\/\/|https?:\/\/|[a-zA-Z]:[\\/]|\\\\)/.test(candidate) ||
    candidate.includes("\\")
  ) {
    return null;
  }
  return candidate;
}

function safeSignalName(key: string, value: unknown) {
  const lowCardinality = safeLowCardinality(value);
  if (lowCardinality) return lowCardinality;
  if (typeof value !== "string" || !SLASH_SIGNAL_KEYS.has(key)) return null;
  const candidate = value.trim();
  return APPROVED_SLASH_SIGNAL_NAMES.has(candidate) ? candidate : null;
}

function sanitizeMetadata(input: Record<string, unknown>): MetadataOutcome {
  if (findForbiddenRawContentFields(input).length > 0) return { ok: false };
  const metadata: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(input)) {
    if (OMIT_LOCAL_ONLY_KEYS.has(key)) continue;
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
        if (typeof source.headSha !== "string" || !COMMIT_SHA.test(source.headSha.trim())) {
          return { ok: false };
        }
        git.headSha = source.headSha.trim().toLowerCase();
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
      if (typeof value !== "string" || !COMMIT_SHA.test(value.trim())) return { ok: false };
      metadata[key] = value.trim().toLowerCase();
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
    if (SAFE_STRING_KEYS.has(key)) {
      const safe = safeSignalName(key, value);
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
      continue;
    }
    if (hasSensitiveConcept(key)) return { ok: false };
    // Unknown metadata is local-only by default. This also defeats Unicode
    // homoglyphs without pretending they normalize to a trusted concept.
  }

  return { ok: true, metadata };
}

function safeTopLevelKey(value: string | undefined, linkage = false) {
  if (value === undefined) return { ok: true as const, value: undefined };
  if (linkage && value.trim().toLowerCase().startsWith("sha256:")) {
    const canonical = canonicalLinkage(value);
    return canonical ? { ok: true as const, value: canonical } : { ok: false as const };
  }
  const safe = safeLowCardinality(value);
  return safe ? { ok: true as const, value: safe } : { ok: false as const };
}

export function sealOutboundEvent(event: AiInteractionEvent) {
  const metadata = sanitizeMetadata(event.metadata);
  if (!metadata.ok) return { ok: false as const, reason: "privacy" as const };
  const id = safeTopLevelKey(event.id);
  const sessionId = safeTopLevelKey(event.sessionId);
  const tenantId = safeTopLevelKey(event.tenantId);
  // Actor identifiers are privacy-preserving local aliases today, including
  // legacy truncated `sha256:` aliases. They are identifiers, not repo
  // linkage, so require the bounded character contract without silently
  // upgrading them to the 256-bit linkage namespace.
  const actorId = safeTopLevelKey(event.actorId);
  const projectKey = safeTopLevelKey(event.projectKey, true);
  const customerKey = safeTopLevelKey(event.customerKey);
  const workflowKey = safeTopLevelKey(event.workflowKey);
  const model = safeTopLevelKey(event.model);
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
      parsed.data.suppressedFields
        .map((field) => field.trim())
        .filter((field) => SAFE_SUPPRESSED_FIELD.test(field)),
    )],
  });
  return envelope.success
    ? { ok: true, envelope: envelope.data }
    : { ok: false, reason: "schema" };
}
