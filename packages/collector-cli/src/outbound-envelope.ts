import {
  aiWorkIngestEventSchema,
  aiWorkSessionSyncRowSchema,
  canonicalSuppressionReceipt,
  canonicalizeSuppressionReceipts,
  hasUnsafeMetadataString,
  isForbiddenRawContentFieldName,
  isSensitiveMetadataSemanticKey,
  metadataKeyDisposition,
  safeMetadataStringAttribute,
  validatedMetadataAttribute,
  type AiInteractionEvent,
  type AiWorkIngestEvent,
  type AiWorkSessionSyncRow,
} from "../../shared/src/index";

const OMIT_LOCAL_ONLY_KEYS = new Set([
  "externalEventId",
  "external_event_id",
  "cwd",
  "rolloutFile",
  "transcriptFile",
]);

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
  const validated = validatedMetadataAttribute("projectKey", value);
  return validated.accepted && typeof validated.value === "string" ? validated.value : null;
}

/**
 * Content-independent outbound value gate. Keys are not enough: a credential
 * can be placed under an otherwise approved field such as serviceName. This
 * check therefore runs before every field-specific string validator.
 */
export function hasUnsafeOutboundString(value: unknown, options: { allowSlash?: boolean } = {}) {
  return hasUnsafeMetadataString(value, options);
}

export function safeOutboundIdentifier(value: unknown) {
  return safeMetadataStringAttribute("request_id", value);
}

function safeComponentName(value: unknown) {
  return safeMetadataStringAttribute("serviceName", value);
}

export function canonicalCommitSha(value: unknown) {
  const validated = validatedMetadataAttribute("headSha", value);
  return validated.accepted && typeof validated.value === "string" ? validated.value : null;
}

function safeSignalName(key: string, value: unknown) {
  return safeMetadataStringAttribute(key, value);
}

function sanitizeMetadata(input: Record<string, unknown>): MetadataOutcome {
  const metadata: Record<string, unknown> = {};
  const omittedFields: string[] = [];
  const recordOmission = (key: string) => {
    omittedFields.push(canonicalSuppressionReceipt(key));
  };

  for (const [key, value] of Object.entries(input)) {
    if (OMIT_LOCAL_ONLY_KEYS.has(key) || isForbiddenRawContentFieldName(key)) {
      recordOmission(key);
      continue;
    }
    if (!/^[\x20-\x7e]+$/.test(key)) {
      recordOmission(key);
      continue;
    }
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
      const validated = validatedMetadataAttribute(key, value);
      if (!validated.accepted) return { ok: false };
      metadata[key] = validated.value;
      continue;
    }
    const disposition = metadataKeyDisposition(key);
    if (disposition?.outbound) {
      const validated = validatedMetadataAttribute(key, value);
      if (!validated.accepted) return { ok: false };
      metadata[key] = validated.value;
      continue;
    }
    if (isSensitiveMetadataSemanticKey(key)) {
      recordOmission(key);
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

  return { ok: true, metadata, omittedFields: canonicalizeSuppressionReceipts(omittedFields) };
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
  // Evidence-marked rows belong in a separately reviewed encrypted vault.
  // That vault is not implemented, so no ordinary upload surface may seal
  // them even when their current typed fields happen to look harmless.
  if (parsed.data.event.dataMode === "evidence") {
    return { ok: false, reason: "privacy" };
  }
  const sealed = sealOutboundEvent(parsed.data.event);
  if (!sealed.ok) return sealed;
  const envelope = aiWorkIngestEventSchema.safeParse({
    event: sealed.event,
    suppressedFields: canonicalizeSuppressionReceipts([
      ...parsed.data.suppressedFields,
      ...sealed.omittedFields,
    ]),
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
