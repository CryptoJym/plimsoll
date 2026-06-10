import fs from "node:fs";

/**
 * Config APPLY mode (issue 0003): idempotent, surgical merges of Plimsoll's
 * telemetry settings into the user's existing tool configs. Never clobbers:
 * unknown keys/hooks/sections are preserved byte-for-byte where possible, a
 * timestamped backup is written before any change, and a second run reports
 * no-op. Callers render the change list and own the confirm step.
 */

export type ApplyResult = {
  path: string;
  changed: boolean;
  changes: string[];
  backupPath?: string;
  /** Set when an existing conflicting config blocks a safe merge. */
  conflict?: string;
};

function backup(file: string): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupPath = `${file}.plimsoll-backup-${stamp}`;
  fs.copyFileSync(file, backupPath);
  return backupPath;
}

/** Merge generated env + hooks into Claude Code settings.json. */
export function applyClaudeSettings(
  file: string,
  generated: { env: Record<string, string>; hooks?: Record<string, unknown[]> },
  options: { dryRun?: boolean } = {},
): ApplyResult {
  const exists = fs.existsSync(file);
  const current = exists
    ? (JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>)
    : {};
  const changes: string[] = [];

  const env = { ...((current.env as Record<string, string> | undefined) ?? {}) };
  for (const [key, value] of Object.entries(generated.env)) {
    if (env[key] !== value) {
      changes.push(`env.${key} ${key in env ? `"${env[key]}" → ` : "+ "}"${value}"`);
      env[key] = value;
    }
  }

  const hooks = { ...((current.hooks as Record<string, unknown[]> | undefined) ?? {}) };
  for (const [event, entries] of Object.entries(generated.hooks ?? {})) {
    const existing = Array.isArray(hooks[event]) ? [...(hooks[event] as unknown[])] : [];
    const has = (url: string) =>
      existing.some((entry) =>
        JSON.stringify(entry).includes(`"url":${JSON.stringify(url)}`),
      );
    for (const entry of entries) {
      const url = JSON.stringify(entry).match(/"url":("([^"]+)")/)?.[2];
      if (url && has(url)) continue;
      existing.push(entry);
      changes.push(`hooks.${event} + plimsoll http hook`);
    }
    hooks[event] = existing;
  }

  if (changes.length === 0) {
    return { path: file, changed: false, changes: [] };
  }
  if (options.dryRun) {
    return { path: file, changed: true, changes };
  }
  const next = { ...current, env, hooks };
  const backupPath = exists ? backup(file) : undefined;
  fs.writeFileSync(file, `${JSON.stringify(next, null, 2)}\n`);
  return { path: file, changed: true, changes, backupPath };
}

/** Append the [otel] sections to codex config.toml when absent. */
export function applyCodexConfig(
  file: string,
  generatedToml: string,
  options: { dryRun?: boolean } = {},
): ApplyResult {
  const exists = fs.existsSync(file);
  const current = exists ? fs.readFileSync(file, "utf8") : "";
  const endpointMatch = generatedToml.match(/endpoint\s*=\s*"([^"]+)"/);
  const endpoint = endpointMatch?.[1] ?? "";

  if (endpoint && current.includes(endpoint)) {
    return { path: file, changed: false, changes: [] };
  }
  if (current.includes("[otel")) {
    return {
      path: file,
      changed: false,
      changes: [],
      conflict:
        "config.toml already has an [otel] section pointing elsewhere — not touching it. " +
        "Remove or repoint it manually, then re-run.",
    };
  }
  const changes = ["+ [otel] exporter sections (logs + traces → local collector)"];
  if (options.dryRun) {
    return { path: file, changed: true, changes };
  }
  const backupPath = exists ? backup(file) : undefined;
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n\n" : current.length > 0 ? "\n" : "";
  fs.writeFileSync(file, `${current}${separator}${generatedToml.trimEnd()}\n`);
  return { path: file, changed: true, changes, backupPath };
}
