# Telemetry adapters

Plimsoll adapters accept provider telemetry at the local collector boundary,
normalize it into `AiInteractionEvent`, and apply the metadata-only privacy
policy before a ledger write. Provider-specific field names belong in the
shared usage and metadata admission maps; the OTLP exploder remains shared.

## Gemini CLI

The Gemini CLI source id is `gemini_cli`. Its native OpenTelemetry exporter
uses the following current settings keys (verified against the [Gemini CLI
telemetry documentation](https://github.com/google-gemini/gemini-cli/blob/main/docs/cli/telemetry.md)
and [configuration reference](https://github.com/google-gemini/gemini-cli/blob/main/docs/reference/configuration.md)):

| Setting | Environment override | Plimsoll value | Purpose |
|---|---|---|---|
| `telemetry.enabled` | `GEMINI_TELEMETRY_ENABLED` | `true` | Enable telemetry. |
| `telemetry.target` | `GEMINI_TELEMETRY_TARGET` | `"local"` | Select the local OTLP target. |
| `telemetry.otlpEndpoint` | `GEMINI_TELEMETRY_OTLP_ENDPOINT` | `http://127.0.0.1:48271/gemini` | Collector endpoint prefix. |
| `telemetry.otlpProtocol` | `GEMINI_TELEMETRY_OTLP_PROTOCOL` | `"http"` | Use the HTTP OTLP exporter. |
| `telemetry.useCollector` | `GEMINI_TELEMETRY_USE_COLLECTOR` | `true` | Keep export on the local collector path. |
| `telemetry.traces` | `GEMINI_TELEMETRY_TRACES_ENABLED` | `false` | Avoid enabling trace payloads by default. |
| `telemetry.logPrompts` | `GEMINI_TELEMETRY_LOG_PROMPTS` | `false` | Keep prompt bodies disabled. |

Generate the settings with:

```sh
pnpm collector generate-config gemini-cli
```

The generated endpoint is source-qualified so the Gemini exporter can append
`/v1/logs`, `/v1/metrics`, and `/v1/traces` to it. The collector routes those
paths to the Gemini source and canonicalizes the stored transport receipt to
the standard OTLP path. When local ingest auth is provisioned, the generator
adds the source-bound Plimsoll token to the endpoint query; do not share the
generated settings outside the local machine.

The adapter uses the documented settings JSON and the corresponding
`GEMINI_TELEMETRY_*` environment overrides; it does not rely on undocumented
CLI switches. The checked-in JSON settings are the worked example because they
preserve the complete local endpoint and privacy configuration in one file.

### Normalization map

Gemini's documented OTLP attributes map as follows:

| Gemini attribute | Ledger field |
|---|---|
| `session.id` | `sessionId` |
| `model` | `model` |
| `input_token_count` | `inputTokens` |
| `output_token_count` | `outputTokens` |
| `cached_content_token_count` | `cacheReadTokens` |
| `thoughts_token_count`, `tool_token_count` | output-token fallback when no `output_token_count` is present |
| `function_name` | `toolName` and action classification |
| `status_code` | admitted status metadata |

Gemini's API response, tool-call, and session attributes share the normal OTLP
record path. A response with `function_name: "run_shell_command"` is classified
as a shell action. `tool_type` and `mcp_server_name` are admitted as typed
metadata when present.

### Raw-content boundary

The following Gemini fields are forbidden before a local ledger write:

`function_args`, `request_text`, `response_text`,
`gen_ai.input.messages`, `gen_ai.output.messages`, and
`gen_ai.system_instructions`.

The adapter also inherits the common forbidden field list. The generator keeps
`logPrompts` and `traces` disabled, and metadata sanitization removes raw
attributes even if a producer sends them. The executable proof uses raw-value
sentinels and verifies they do not reach the event or SQLite ledger:

```sh
pnpm proof:gemini-adapter
```

The documented fixture is
`packages/shared/fixtures/otel-inputs/gemini-cli-api-response.json`.
