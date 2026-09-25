-- The pinned <UR> usage-record predicate (docs/lean/ARCHITECTURE.md §3.1, §10; CONTRACTS.md C8). Verbatim from the inline text of
-- plimsoll-cloud src/lib/economics/loader.ts (unchanged since 067a8a4; re-read at 4954995). The pin is the sha256 of this text
-- after whitespace is collapsed and case folded (tests/contracts/lean/usage-record-pin.contract.ts). The same file and pin live
-- in plimsoll-cloud tests/contracts/lean/fixtures/. Lane 2 re-pins when it exports USAGE_RECORD_PREDICATE_SQL.
(
  event_type IN ('usage_rollout','usage_transcript','usage_live')
  OR input_tokens IS NOT NULL OR output_tokens IS NOT NULL
  OR cache_read_tokens IS NOT NULL OR cache_creation_tokens IS NOT NULL
  OR cost_usd IS NOT NULL
)
