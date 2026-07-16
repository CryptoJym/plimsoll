export type HookAuthorityField =
  | "source"
  | "tenant"
  | "dataMode"
  | "transport"
  | "gitLinkage"
  | "action"
  | "eventId"
  | "eventType"
  | "observedAt";

type HookAuthorityRule = {
  aliases: readonly string[];
  owner: "transport" | "config" | "resolver" | "validated_hook";
  receipt: string;
};

/**
 * Hook fields with authority or derived meaning never enter routine metadata.
 * Only literal aliases may be selected as validated hook inputs. Case,
 * separator, camel/snake/dot and control-suffixed lookalikes are stripped and
 * audited, but never gain authority through fingerprint matching.
 */
export const HOOK_AUTHORITY_CONTRACT: Record<HookAuthorityField, HookAuthorityRule> = {
  source: {
    aliases: ["source", "provider", "originator"],
    owner: "transport",
    receipt: "hook.authority.source",
  },
  tenant: {
    aliases: ["tenantId", "tenant_id", "tenant.id"],
    owner: "config",
    receipt: "hook.authority.tenant",
  },
  dataMode: {
    aliases: ["dataMode", "data_mode"],
    owner: "config",
    receipt: "hook.authority.data_mode",
  },
  transport: {
    aliases: ["transport_path", "transportPath"],
    owner: "transport",
    receipt: "hook.authority.transport",
  },
  gitLinkage: {
    aliases: [
      "git",
      "remoteUrlHash",
      "remote_url_hash",
      "remote.url_hash",
      "repoHash",
      "repo_hash",
      "repo.hash",
      "branchHash",
      "branch_hash",
      "branch.hash",
      "headSha",
      "head_sha",
      "head.sha",
      "commitSha",
      "commit_sha",
      "commit.sha",
    ],
    owner: "resolver",
    receipt: "hook.authority.git_linkage",
  },
  action: {
    aliases: [
      "actionClass",
      "action_class",
      "plimsoll.action_class",
      "cfo_one.action_class",
    ],
    owner: "validated_hook",
    receipt: "hook.authority.action",
  },
  eventId: {
    aliases: ["id", "eventId", "event_id"],
    owner: "validated_hook",
    receipt: "hook.authority.event_id",
  },
  eventType: {
    aliases: ["eventType", "event_type", "hook_event_name", "type"],
    owner: "validated_hook",
    receipt: "hook.authority.event_type",
  },
  observedAt: {
    aliases: ["observedAt", "observed_at", "event.timestamp", "timestamp", "time"],
    owner: "validated_hook",
    receipt: "hook.authority.observed_at",
  },
};

export type HookAuthorityEntry = {
  field: HookAuthorityField;
  key: string;
  value: unknown;
  exact: boolean;
};

export type HookAuthorityPartition = {
  metadata: Record<string, unknown>;
  supplied: Record<HookAuthorityField, HookAuthorityEntry[]>;
};

function authorityKeyFingerprint(key: string) {
  return key
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

const exactAliases = new Map<string, HookAuthorityField>();
const variantFingerprints = new Map<string, HookAuthorityField>();
for (const [field, rule] of Object.entries(HOOK_AUTHORITY_CONTRACT) as Array<
  [HookAuthorityField, HookAuthorityRule]
>) {
  for (const alias of rule.aliases) {
    exactAliases.set(alias, field);
    const fingerprint = authorityKeyFingerprint(alias);
    const existing = variantFingerprints.get(fingerprint);
    if (existing && existing !== field) {
      throw new Error(`HookAuthorityFingerprintCollision:${existing}:${field}`);
    }
    variantFingerprints.set(fingerprint, field);
  }
}

function emptySupplied(): Record<HookAuthorityField, HookAuthorityEntry[]> {
  return {
    source: [],
    tenant: [],
    dataMode: [],
    transport: [],
    gitLinkage: [],
    action: [],
    eventId: [],
    eventType: [],
    observedAt: [],
  };
}

export function hookAuthorityField(key: string) {
  const exactField = exactAliases.get(key);
  if (exactField) return { field: exactField, exact: true as const };
  const variantField = variantFingerprints.get(authorityKeyFingerprint(key));
  return variantField ? { field: variantField, exact: false as const } : undefined;
}

export function partitionHookAuthority(input: Record<string, unknown>): HookAuthorityPartition {
  const metadata: Record<string, unknown> = {};
  const supplied = emptySupplied();
  for (const [key, value] of Object.entries(input)) {
    const match = hookAuthorityField(key);
    if (!match) {
      metadata[key] = value;
      continue;
    }
    supplied[match.field].push({ ...match, key, value });
  }
  return { metadata, supplied };
}

export function hookAuthorityEntries(
  partitions: readonly HookAuthorityPartition[],
  field: HookAuthorityField,
) {
  return partitions.flatMap((partition) => partition.supplied[field]);
}

export function hookAuthorityReceipt(field: HookAuthorityField) {
  return HOOK_AUTHORITY_CONTRACT[field].receipt;
}

export function selectValidatedHookAuthority<T>(
  partitions: readonly HookAuthorityPartition[],
  field: HookAuthorityField,
  validate: (value: unknown, key: string) => T | undefined,
) {
  const entries = hookAuthorityEntries(partitions, field);
  let selected: HookAuthorityEntry | undefined;
  let value: T | undefined;
  for (const partition of partitions) {
    for (const alias of HOOK_AUTHORITY_CONTRACT[field].aliases) {
      const entry = partition.supplied[field].find(
        (candidate) => candidate.exact && candidate.key === alias,
      );
      if (!entry) continue;
      const validated = validate(entry.value, entry.key);
      if (validated !== undefined) {
        selected = entry;
        value = validated;
        break;
      }
    }
    if (selected) break;
  }
  return {
    value,
    receiptRequired: entries.some((entry) => entry !== selected),
  };
}

export function ignoredHookAuthorityReceipt(
  partitions: readonly HookAuthorityPartition[],
  field: HookAuthorityField,
) {
  return hookAuthorityEntries(partitions, field).length > 0
    ? hookAuthorityReceipt(field)
    : undefined;
}
