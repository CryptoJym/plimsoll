import type { DataMode } from "../../shared/src/index";

const DEFAULT_PORT = 48271;

export type ToolConfigOptions = {
  repoRoot: string;
  port?: number;
  dataMode?: DataMode;
  confirmEvidence?: boolean;
  pnpmCommand?: string;
};

function assertEvidenceConfirmed(options: ToolConfigOptions) {
  if (options.dataMode === "evidence" && !options.confirmEvidence) {
    throw new Error(
      "Evidence-mode config generation requires explicit confirmation because it can collect raw content.",
    );
  }
}

function shellQuote(value: string) {
  if (/^[a-zA-Z0-9_./:@=-]+$/.test(value)) {
    return value;
  }

  return `'${value.replace(/'/g, "'\\''")}'`;
}

function tomlString(value: string) {
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function hookForwardCommand(options: ToolConfigOptions, source: "claude-code" | "codex") {
  // curl into the local receiver keeps per-event overhead at ~10ms; spawning
  // pnpm/node per hook event costs 1-2s per tool call across the whole fleet.
  return `curl -s --max-time 2 -X POST -H 'Content-Type: application/json' --data-binary @- http://127.0.0.1:${port(options)}/hooks/${source} || true`;
}

function port(options: ToolConfigOptions) {
  return options.port ?? DEFAULT_PORT;
}

export function generateClaudeCodeSettings(options: ToolConfigOptions) {
  assertEvidenceConfirmed(options);

  const hookUrl = `http://127.0.0.1:${port(options)}/hooks/claude-code`;

  return {
    env: {
      CLAUDE_CODE_ENABLE_TELEMETRY: "1",
      OTEL_LOGS_EXPORTER: "otlp",
      OTEL_METRICS_EXPORTER: "otlp",
      OTEL_EXPORTER_OTLP_PROTOCOL: "http/json",
      OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port(options)}`,
      OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${port(options)}/v1/logs`,
      OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: `http://127.0.0.1:${port(options)}/v1/metrics`,
      OTEL_EXPORTER_OTLP_HEADERS: "x-plimsoll-source=claude_code",
      OTEL_LOG_USER_PROMPTS: options.dataMode === "evidence" ? "1" : "0",
      OTEL_LOG_TOOL_DETAILS: options.dataMode === "evidence" ? "1" : "0",
      OTEL_LOG_TOOL_CONTENT: options.dataMode === "evidence" ? "1" : "0",
      OTEL_LOG_RAW_API_BODIES: "0",
    },
    hooks: {
      UserPromptSubmit: [
        {
          hooks: [
            {
              type: "http",
              url: hookUrl,
              timeout: 5,
            },
          ],
        },
      ],
      PostToolUse: [
        {
          matcher: ".*",
          hooks: [
            {
              type: "http",
              url: hookUrl,
              timeout: 5,
            },
          ],
        },
      ],
      Stop: [
        {
          hooks: [
            {
              type: "http",
              url: hookUrl,
              timeout: 5,
            },
          ],
        },
      ],
    },
  };
}

export function generateCodexConfigToml(options: ToolConfigOptions) {
  assertEvidenceConfirmed(options);

  const command = hookForwardCommand(options, "codex");
  const basePort = port(options);
  const exporterTable = (signalPath: string) => [
    `endpoint = ${tomlString(`http://127.0.0.1:${basePort}${signalPath}`)}`,
    'protocol = "json"',
    'headers = { "x-plimsoll-source" = "codex" }',
  ];

  return [
    "# Plimsoll metadata-mode config.",
    "# Generated from official Codex config/hooks surfaces; raw user prompts stay disabled by default.",
    "# Token usage is recorded on Codex trace spans (gen_ai.usage.*), so the trace exporter",
    "# must stay enabled for token/cost attribution. Metrics carry tool/api counters.",
    "",
    "[otel]",
    'environment = "plimsoll-local"',
    `log_user_prompt = ${options.dataMode === "evidence" ? "true" : "false"}`,
    "",
    '[otel.exporter."otlp-http"]',
    ...exporterTable("/v1/logs"),
    "",
    '[otel.trace_exporter."otlp-http"]',
    ...exporterTable("/v1/traces"),
    "",
    '[otel.metrics_exporter."otlp-http"]',
    ...exporterTable("/v1/metrics"),
    "",
    "[features]",
    "hooks = true",
    "",
    "[hooks]",
    "",
    "[[hooks.UserPromptSubmit]]",
    "[[hooks.UserPromptSubmit.hooks]]",
    'type = "command"',
    `command = ${tomlString(command)}`,
    "timeout = 5",
    'statusMessage = "Recording AI work metadata"',
    "",
    "[[hooks.PostToolUse]]",
    'matcher = ".*"',
    "[[hooks.PostToolUse.hooks]]",
    'type = "command"',
    `command = ${tomlString(command)}`,
    "timeout = 5",
    'statusMessage = "Recording AI tool metadata"',
    "",
    "[[hooks.Stop]]",
    "[[hooks.Stop.hooks]]",
    'type = "command"',
    `command = ${tomlString(command)}`,
    "timeout = 5",
    'statusMessage = "Recording AI session metadata"',
    "",
  ].join("\n");
}

export function generateSetupInstructions(options: ToolConfigOptions) {
  assertEvidenceConfirmed(options);

  return {
    dataMode: options.dataMode ?? "metadata",
    claudeCodeSettingsPath: "~/.claude/settings.json or project .claude/settings.json",
    codexConfigPath:
      "~/.codex/config.toml, project .codex/config.toml, or managed requirements.toml with managed_dir",
    collectorStartCommand: `${shellQuote(options.pnpmCommand ?? "pnpm")} --dir ${shellQuote(options.repoRoot)} collector start`,
    collectorDoctorCommand: `${shellQuote(options.pnpmCommand ?? "pnpm")} --dir ${shellQuote(options.repoRoot)} collector doctor`,
    privacyDefaults: {
      rawPrompts: options.dataMode === "evidence",
      rawOutputs: options.dataMode === "evidence",
      screenshots: false,
      keystrokes: false,
      clipboardBody: false,
      browserHistory: false,
    },
  };
}
