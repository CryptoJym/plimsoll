# Rule models (Python) behind the pending contract tests

These in-memory models let an implementer replay a lean-Plimsoll rule before writing code. Run them from the repository root with the passing rule listed here:

| Fixture | Passing `--rule` |
| --- | --- |
| `b1_day_target_receipt.py` | `r6` |
| `b1_membership_edges.py` | `r5` |
| `b22_false_complete.py` | `r7` |
| `b2_epoch_day_keys.py` | `r5` |
| `b3_durable_target_refs.py` | `r5` |
| `b4_actor_at_capture.py` | `r5` |
| `b4_offline_rebind.py` | `r14` |
| `b5_lexical_boundary.py` | `r5` |
| `b5_non_iso_day_facts.py` | `r6` |
| `b6_unknown_volume_gates.py` | `r5` |
| `s1b_runway_geometry.py` | `r6` |
| `s1b_runway_host_bound.py` | `r9` |
| `sf1_ladder_rollback_parity.py` | `r5` |
| `sf5_unresolved_file_open_gap.py` | `r5` |
| `sf_abort_rebuild_bound.py` | `r6` |
| `sf_no_cache_columns_guard.py` | `r6` |

For example, B6's corrected unknown-volume gate is the `r5` branch:

```sh
python3 tests/contracts/lean/fixtures/b6_unknown_volume_gates.py --rule r5
```

Earlier rules intentionally reproduce red cases. Each fixture has its own rule branches; a later rule is not automatically supported by an earlier fixture. `_common.py` is the shared helper. `b22_false_complete.py` reads `docs/lean/` at the repository root by default. `host_cardinality_census.py` is B1's read-only census tool (run on a `VACUUM INTO` copy only), and `usage_record_predicate.sql` is the pinned `<UR>` text. CI runs the TypeScript contracts one directory up, not these Python models.
