import { collectorSecretConfigSchema } from "./config";

/** Diagnostic captures are not rollback configs. Never inspect or report a secret value. */
export function redactLifecycleCapture(input: unknown) {
  const secrets = new Set(Object.keys(collectorSecretConfigSchema.shape));
  const fields = new Set<string>();
  const walk = (value: unknown, depth: number): unknown => {
    if (depth > 64) throw new Error("capture JSON nesting exceeds the redaction bound");
    if (Array.isArray(value)) return value.map((item) => walk(item, depth + 1));
    if (!value || typeof value !== "object") return value;
    return Object.fromEntries(Object.entries(value).map(([key, child]) => {
      if (secrets.has(key)) { fields.add(key); return [key, "[REDACTED]"]; }
      return [key, walk(child, depth + 1)];
    }));
  };
  return { config: walk(input, 0), fields: [...fields].sort() };
}
