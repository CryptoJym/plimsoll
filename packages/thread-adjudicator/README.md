# Project attribution shadow pilot

Compare the installed classifier with cached decisions for actual user turns.
One request contains the relevant threads; adjacent turns assigned to the same
project need one segment answer. This command
reads sanitized JSON and writes private local reports; it never collects native
histories, changes production assignments, or changes financial records.

The existing strong model and effort settings come from `--baseline-source`.
The catalogue accepts stable keys, labels, descriptions and aliases. Only an
explicit `Project: label-or-alias` header or a revisioned owner correction is a
deterministic assignment; other evidence goes to the model. Count-only changes
reuse decisions. Semantic catalogue changes reconsider cached decisions.

The pilot sends each relevant thread's complete ordered requests once, with
whitespace normalized, turn IDs and nonempty paths. It omits usage, timestamps,
metadata, project keys and empty catalogue fields. Exact consecutive repeats
share one unit while retaining every actual turn ID. Opening and preceding
requests remain available for terse continuations, including cached turns and
owner/alias assignments marked read-only `context`. No request is clipped or
summarized. All pending pilot units share one model request; the installed
baseline retains its original 20-thread batches. Very large inputs can exceed
provider limits; such a failure stays incomplete, without truncation or retry.

The wire response is `{"results":[{"start":"0","end":"31","label":"Plimsoll",
"confidence":92,"reason":"Requested work benefits this project"}]}`. IDs refer
to ordered units in this request, with inclusive endpoints. Local validation
requires exact coverage in order, rejecting unknown endpoints, gaps, overlaps,
cross-thread ranges and ranges including read-only context. It checks integer
confidence bounds locally; the SDK schema has no numeric minimum/maximum.
Each validated range expands into the existing per-unit decisions and cache;
allocation still uses actual turn IDs. Fingerprints include the ordered prefix
of request evidence and owner corrections, so changing opening context also
reconsiders later continuations. Appending a different request classifies it;
appending an exact consecutive repeat reuses its decision. The storage version
is unchanged; old fingerprint entries miss once, then reuse the new decisions.

## Run

Use the existing adjudicator's Python environment. Run the existing privacy
masker and pre-ship scan on **all** input strings before setting `sanitized: true`.
The fixture contains synthetic data only.

```sh
python packages/thread-adjudicator/pilot.py \
  --input /absolute/path/sanitized.json \
  --state-dir /absolute/path/private-shadow-state \
  --baseline-source /absolute/path/adjudicate.py
```

The default provider is `none`: it makes no network calls. Add
`--provider anthropic` for measured calls and supply `ANTHROPIC_API_KEY` from the
existing secret helper **inside a process**, never in a shell argument or file.
The existing `anthropic` SDK is the only optional dependency. Reuse the same
state directory for subsequent passes. There is no new scheduled job.

`fixtures/sample.json` documents the input shape. Supply complete ordered user
turns with their original timestamps; sampled summaries cannot establish exact
boundaries. Optional `baseline_brief` retains the existing extractor's summary
for comparison. Usage input means fresh input, with cache read/write separate.
Per-turn usage must cover every turn and conserve the thread totals; otherwise
the report explicitly labels its equal-turn allocation as estimated. Parent
totals must exclude child usage. Owner corrections survive later omission;
only a newer revision explicitly releases one.

Inspect `provider_requests`, `provider_usage`, `usage_complete`, `errors`,
cache decisions, segment evidence, and conservation. Input bytes and scripted
fixture responses are not measured model savings. Best guesses and unknowns
remain distinct. Completely unchanged inputs should cause zero calls in both
the baseline and pilot; count-only changes are the useful comparison.

## Check and undo

```sh
PLIMSOLL_BASELINE_SOURCE=/absolute/path/adjudicate.py \
  python packages/thread-adjudicator/check.py
```

The check uses synthetic answers, not live credentials. The reader's normal
`proof:lean-reader` command also runs its advisor accounting check. Stop invoking
the shadow command to undo its use; there are no production assignments to
restore. Keep runtime state outside the repository, or in ignored `.runtime/`.

## Pilot measurement, 2026-11-10

On seven selected local chats (57 user requests, 80 catalogue projects), using
the existing Opus 5.5/max settings and actual provider usage counters:

| Pass | Installed baseline tokens | Compact pilot tokens |
| --- | ---: | ---: |
| Initial classification | 20,006 | 31,646 |
| Unchanged input | 0 | 0 |
| Activity-count changes only | 19,678 | 0 |

The initial pass plus one count-only update used 20.3% fewer tokens, although
initial classification used 58.2% more. This is a selected sample, not a fleet
savings forecast or a provider bill. The pilot returned nine project segments;
six explicit request-label checks passed versus four for the whole-chat baseline.
These are not independent owner labels. Five chats had native turn usage; two
needed estimated within-turn allocations. All four usage categories conserved.
The final cold comparison made one provider call; subsequent passes made none.
Production assignment writes remain off. Maintenance savings are not established:
this adds 520 implementation lines, using the existing SDK with no new service.
