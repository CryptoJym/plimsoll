import crypto from "node:crypto";

import {
  LOCAL_TENANT_ID,
  type DataMode,
  type PolicyConfig,
  findForbiddenRawContentFields,
  isForbiddenRawContentFieldName,
  policyConfigSchema,
} from "./schemas";

export const DEFAULT_POLICY: PolicyConfig = policyConfigSchema.parse({
  id: "default-policy",
  tenantId: LOCAL_TENANT_ID,
  dataMode: "metadata",
  version: "2026-05-17.metadata-v1",
  benchmarkContribution: "disabled",
  employeeSelfViewEnabled: true,
  managerDrilldownEnabled: false,
  minimumCohortSize: 5,
  evidence: {
    enabled: false,
    allowedCategories: [],
    rbacScopes: [],
  },
  updatedAt: "2026-05-17T00:00:00.000Z",
});

export type PolicyEvaluation = {
  allowed: boolean;
  dataMode: DataMode;
  reasons: string[];
  suppressedFields: string[];
};

export type SanitizedForPolicy<T> = {
  evaluation: PolicyEvaluation;
  value: T;
};

const REMOVE_FIELD = Symbol("remove-field");

export const protectedMetadataFieldNames = [
  "account_id",
  "account_email",
  "account_uuid",
  "actor_email",
  "actor_id",
  "cwd",
  "current_working_directory",
  "email",
  "email_address",
  "file_path",
  "full_path",
  "organization_id",
  "org_id",
  "owner_email",
  "project_path",
  "repo_path",
  "repository_url",
  "transcript_path",
  "workdir",
  "working_directory",
  "workspace_path",
  "user.account_id",
  "user.account_uuid",
  "user.email",
  "user.id",
  "user_email",
  "user_id",
  "username",
] as const;

const protectedMetadataFields = new Set(
  protectedMetadataFieldNames.map((field) => normalizeFieldName(field)),
);

function normalizeFieldName(field: string) {
  return field.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}

export function isProtectedMetadataFieldName(field: string) {
  return protectedMetadataFields.has(normalizeFieldName(field));
}

function hashProtectedValue(value: unknown) {
  const serialized =
    typeof value === "string" || typeof value === "number" || typeof value === "boolean"
      ? String(value)
      : JSON.stringify(value);
  return `sha256:${crypto.createHash("sha256").update(serialized ?? "").digest("hex").slice(0, 16)}`;
}

function protectedScalar(value: unknown) {
  return hashProtectedValue(value);
}

function protectedOtelValue(value: unknown) {
  return {
    stringValue: hashProtectedValue(value),
  };
}

function otelAttributeKey(value: Record<string, unknown>) {
  return typeof value.key === "string" && "value" in value ? value.key : undefined;
}

function sanitizeRoutineMetadata(
  value: unknown,
  suppressed: string[],
  path = "",
): unknown | typeof REMOVE_FIELD {
  if (!value || typeof value !== "object") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.flatMap((item, index) => {
      const sanitized = sanitizeRoutineMetadata(item, suppressed, `${path}[${index}]`);
      return sanitized === REMOVE_FIELD ? [] : [sanitized];
    });
  }

  const semanticKey = otelAttributeKey(value as Record<string, unknown>);
  if (semanticKey) {
    const currentPath = path ? `${path}.${semanticKey}` : semanticKey;
    if (isForbiddenRawContentFieldName(semanticKey)) {
      suppressed.push(currentPath);
      return REMOVE_FIELD;
    }

    if (isProtectedMetadataFieldName(semanticKey)) {
      suppressed.push(currentPath);
      return {
        ...(value as Record<string, unknown>),
        value: protectedOtelValue((value as Record<string, unknown>).value),
      };
    }
  }

  const next: Record<string, unknown> = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    const currentPath = path ? `${path}.${key}` : key;
    if (isForbiddenRawContentFieldName(key)) {
      suppressed.push(currentPath);
      continue;
    }

    if (isProtectedMetadataFieldName(key)) {
      suppressed.push(currentPath);
      next[key] = protectedScalar(nestedValue);
      continue;
    }

    const sanitized = sanitizeRoutineMetadata(nestedValue, suppressed, currentPath);
    if (sanitized !== REMOVE_FIELD) {
      next[key] = sanitized;
    }
  }

  return next;
}

export function evaluatePolicyInput(
  input: unknown,
  policy: PolicyConfig = DEFAULT_POLICY,
): PolicyEvaluation {
  const suppressedFields = findForbiddenRawContentFields(input);
  const reasons: string[] = [];

  if (policy.dataMode !== "evidence" && suppressedFields.length > 0) {
    reasons.push(
      `Suppressed ${suppressedFields.length} raw-content field(s) because ${policy.dataMode} mode forbids raw prompt/output/tool content.`,
    );
  }

  if (policy.dataMode === "evidence") {
    const parsedPolicy = policyConfigSchema.safeParse(policy);
    if (!parsedPolicy.success) {
      reasons.push("Evidence mode policy is incomplete.");
      return {
        allowed: false,
        dataMode: policy.dataMode,
        reasons,
        suppressedFields,
      };
    }
  }

  return {
    allowed: true,
    dataMode: policy.dataMode,
    reasons,
    suppressedFields: policy.dataMode === "evidence" ? [] : suppressedFields,
  };
}

export function sanitizeForPolicy<T>(
  input: T,
  policy: PolicyConfig = DEFAULT_POLICY,
): SanitizedForPolicy<T> {
  const evaluation = evaluatePolicyInput(input, policy);

  if (policy.dataMode === "evidence") {
    return { evaluation, value: input };
  }

  const suppressedFields: string[] = [];
  const value = sanitizeRoutineMetadata(input, suppressedFields) as T;

  return {
    evaluation: {
      ...evaluation,
      suppressedFields,
    },
    value,
  };
}
