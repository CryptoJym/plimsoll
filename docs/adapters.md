# Adapter contract

An adapter is a capture-and-normalization boundary. It must provide exactly
four things:

1. **Source id** — one stable `ToolSource` value. Cursor uses `cursor`.
2. **Transport** — a bounded producer path. Cursor command hooks POST JSON to
   `http://127.0.0.1:48271/hooks/cursor`; an OTLP bridge may replay JSON into
   `/v1/logs` or `/v1/metrics` with `x-plimsoll-source: cursor`.
3. **Session key** — the source identifier that joins events from one session.
   Cursor hooks use `conversation_id`; Cursor OTLP uses
   `cursor.conversation.id`. Both normalize to `sessionId`.
4. **Raw-content field names** — an explicit denylist for source fields that
   may contain prompts, tool arguments, command bodies, results, file content,
   or local paths. The names must be covered by the shared sentinel proof.

## Cursor

Cursor exposes two useful local surfaces:

- Agent hooks receive JSON on stdin. The relevant lifecycle and tool events are
  `sessionStart`, `sessionEnd`, `preToolUse`, `postToolUse`,
  `postToolUseFailure`, `beforeShellExecution`, `afterShellExecution`,
  `beforeMCPExecution`, `afterMCPExecution`, `beforeReadFile`, `afterFileEdit`,
  `beforeSubmitPrompt`, `subagentStart`, `subagentStop`, `afterAgentResponse`,
  `afterAgentThought`, and `stop`.
- Enterprise OpenTelemetry Export sends Cursor telemetry to an OTLP/HTTP
  endpoint. Its documented resource is `service.name=cursor`, and its logs use
  `cursor.conversation.id`; token request attributes include
  `cursor.api.request.input_tokens`, `output_tokens`, `cache_read_tokens`, and
  `cache_creation_tokens`. The current local receiver's JSON OTLP surface is
  suitable for a small bridge/replayer; Cursor's direct Enterprise export is
  binary protobuf and is therefore not claimed as direct local ingestion here.

The local hook transport is metadata-only. A hook command forwards stdin and
never copies the body into arguments or logs:

```json
{
  "version": 1,
  "hooks": {
    "sessionStart": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "sessionEnd": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "preToolUse": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "postToolUse": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "postToolUseFailure": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "beforeShellExecution": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "afterShellExecution": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "beforeMCPExecution": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "afterMCPExecution": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "beforeReadFile": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "afterFileEdit": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "beforeSubmitPrompt": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }],
    "stop": [{ "command": "curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: $PLIMSOLL_CURSOR_TOKEN' --data-binary @- http://127.0.0.1:48271/hooks/cursor" }]
  }
}
```

Set `PLIMSOLL_CURSOR_TOKEN` in the Cursor hook process environment from the
private `cursorProducer` entry in the local Plimsoll auth file. Do not paste a
token into a checked-in project hook. When the local receiver is run without
its authenticated daemon boundary, the source-bound token header may be
omitted; the `/hooks/cursor` path still fixes the source id.

Cursor hook input fields that are never retained as metadata include:
`command`, `output`, `tool_input`, `tool_output`, `result`, `result_json`,
`task`, `text`, `user_message`, `agent_message`, `additional_context`,
`updated_input`, `error_message`, `edits`, `file_path`, `cwd`,
`working_directory`, `user_email`, `transcript_path`, and `workspace_roots`,
plus their `cursor_*` namespaced aliases. The shared sanitizer records only
bounded suppression receipts.

The adapter does not alter outcome linkage. The receiver may later enrich a
captured event with its workdir/repository context using the same tool-agnostic
path already used by the other live sources.

Reference surfaces: [Cursor Hooks](https://cursor.com/docs/hooks) and [Cursor
OpenTelemetry Export Wire Reference](https://cursor.com/docs/enterprise/opentelemetry-export/wire).
