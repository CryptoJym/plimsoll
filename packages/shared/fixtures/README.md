# AI Work Intelligence Shared Fixtures

These fixtures support the shared schema contract for issue `#5`.

| Folder | Contents |
|---|---|
| `valid/` | Valid shared schema objects and backend ingest batches |
| `policies/` | Valid policy fixtures for metadata, event_detail, and evidence modes |
| `adapter-inputs/` | Raw pre-suppression collector input used to prove adapter redaction |
| `hook-inputs/` | Claude Code and Codex hook receiver fixtures for UserPromptSubmit, Stop, tool action, unknown, and malformed JSON replay |
| `otel-inputs/` | OTLP-compatible Claude Code and Codex telemetry fixtures for LLM request, session, tool action, token/cost mapping, and raw-content suppression |
| `claude-code-analytics/` | Paginated Claude Code Analytics Admin API fixtures for aggregate daily user metrics, missing model breakdowns, and raw-sentinel suppression proof |
| `invalid/` | Negative examples that must fail schema validation or backend raw-content storage gates |

Run the fixture proof with:

```bash
pnpm work-intelligence:schema-proof -- --write
```

The proof validates the shared Zod schemas, collector adapter suppression, backend ingest fixtures, evidence-mode policy requirements, fixture policies for each data mode, and metadata-mode rejection of raw prompt/output/body fields.
