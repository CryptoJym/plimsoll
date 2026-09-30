import crypto from "node:crypto";

import {
  LOCAL_TENANT_ID,
  type DataMode,
  type PolicyConfig,
  findForbiddenRawContentFields,
  policyConfigSchema,
} from "./schemas";
import {
  canonicalizeSuppressionReceipts,
  isSafeSuppressionSourceKey,
  suppressionReceiptForAttributeKey,
} from "./suppression-receipt";
import {
  admittedMetadataAttributes,
  isSensitiveMetadataSemanticKey,
} from "./analytical-metadata";

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
  "workspace_root",
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

const PROTECTED_METADATA_HASH = /^sha256:[a-f0-9]{16}$/;

/**
 * Fail-closed admission for ordinary hook metadata. Exact analytical fields
 * use the shared validators; protected path/identity fields survive only as
 * hashes already produced by `sanitizeForPolicy`. Every unknown key drops.
 */
export function admittedHookMetadata(input: Record<string, unknown>, receivedAtMs = Date.now()) {
  const validated = admittedMetadataAttributes(input, "record", receivedAtMs);
  const attributes: Record<string, unknown> = { ...validated.attributes };
  const rejectedKeys: string[] = [];
  for (const [key, value] of Object.entries(input)) {
    if (Object.hasOwn(attributes, key)) continue;
    if (
      isProtectedMetadataFieldName(key) &&
      typeof value === "string" &&
      PROTECTED_METADATA_HASH.test(value)
    ) {
      attributes[key] = value;
      continue;
    }
    rejectedKeys.push(key);
  }
  return { attributes, rejectedKeys };
}

export function hashProtectedValue(value: unknown) {
  const serialized =
    typeof value === "string" || typeof value === "number" || typeof value === "boolean"
      ? String(value)
      : JSON.stringify(value);
  return `sha256:${crypto.createHash("sha256").update(serialized ?? "").digest("hex").slice(0, 16)}`;
}

/** The OTLP account attribute hashes its serialized AnyValue, not its raw id. */
export function providerAccountKey(rawId: string): string {
  return hashProtectedValue({ stringValue: rawId });
}

function accountIdFromOtelValue(value: unknown): string | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const id = (value as Record<string, unknown>).stringValue;
  return typeof id === "string" ? id : undefined;
}

function isProviderAccountField(key: string): boolean {
  return ["accountid", "accountuuid", "useraccountid", "useraccountuuid"].includes(normalizeFieldName(key));
}

function protectedScalar(value: unknown, key: string) {
  return isProviderAccountField(key) && typeof value === "string"
    ? providerAccountKey(value)
    : hashProtectedValue(value);
}

function protectedOtelValue(value: unknown, key: string) {
  const accountId = isProviderAccountField(key) ? accountIdFromOtelValue(value) : undefined;
  return {
    stringValue: accountId !== undefined ? providerAccountKey(accountId) : hashProtectedValue(value),
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
    if (!isSafeSuppressionSourceKey(semanticKey)) {
      suppressed.push(suppressionReceiptForAttributeKey(semanticKey));
      return REMOVE_FIELD;
    }
    if (isSensitiveMetadataSemanticKey(semanticKey)) {
      suppressed.push(suppressionReceiptForAttributeKey(semanticKey));
      return REMOVE_FIELD;
    }

    if (isProtectedMetadataFieldName(semanticKey)) {
      suppressed.push(suppressionReceiptForAttributeKey(semanticKey));
      return {
        ...(value as Record<string, unknown>),
        value: protectedOtelValue((value as Record<string, unknown>).value, semanticKey),
      };
    }
  }

  const next: Record<string, unknown> = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    const currentPath = path ? `${path}.${key}` : key;
    if (!isSafeSuppressionSourceKey(key)) {
      suppressed.push(currentPath);
      continue;
    }
    if (isSensitiveMetadataSemanticKey(key)) {
      suppressed.push(currentPath);
      continue;
    }

    if (isProtectedMetadataFieldName(key)) {
      suppressed.push(currentPath);
      next[key] = protectedScalar(nestedValue, key);
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
  const detectedSuppressedFields = findForbiddenRawContentFields(input);
  const suppressedFields = canonicalizeSuppressionReceipts(detectedSuppressedFields);
  const reasons: string[] = [];

  if (policy.dataMode !== "evidence" && detectedSuppressedFields.length > 0) {
    reasons.push(
      `Suppressed ${detectedSuppressedFields.length} raw-content field(s) because ${policy.dataMode} mode forbids raw prompt/output/tool content.`,
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
      suppressedFields: canonicalizeSuppressionReceipts(suppressedFields),
    },
    value,
  };
}
