#!/usr/bin/env python3
"""Host cardinality census for the S1b runway bound (B0 contract C2; run by B1 on a light host's ledger COPY only).

Counts, read-only, the cardinalities that size the lean tables a host's conversion will add, so G_host can be
bounded per host from that host's own row mix instead of Studio1's (review-r6 blocker 2). Usage: python3
host_cardinality_census.py <ledger-copy.sqlite> [--json out.json]. Never run it on a live ledger or on Studio0.

Round 8 (B0 round 2, CONTRACTS.md C2 rule 1): the census is taken AFTER the catch-up, at most a day before the S1b
decision, re-taken before S3, and the VACUUM INTO copy it runs on is made only when free space >= ledger bytes + reserve
(the copy is deleted afterwards). It also counts `segment_proxy`, the sessions split at 7 days (a session spanning d days
contributes ceil(d / 7) segments), which replaces the plan's 1.2 x sessions geometry when it is larger; the terminal-pause
splits it does not count are covered by the 1.25 factor. The UR predicate below is the pinned <UR> text
(tests/contracts/lean/fixtures/usage_record_predicate.sql; the pin test compares them after whitespace and case folding).
"""
import json, sqlite3, sys, time
path = sys.argv[1]
out = sys.argv[sys.argv.index("--json") + 1] if "--json" in sys.argv else None
db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
UR = ("(event_type in ('usage_rollout','usage_transcript','usage_live') or input_tokens is not null or output_tokens is not null"
      " or cache_read_tokens is not null or cache_creation_tokens is not null or cost_usd is not null)")
DAY = "substr(observed_at,1,10)"      # the canonical-shape day; the shape census below says whether any row needs the code pass
HOUR = "substr(observed_at,1,13)"
t0 = time.time()
q = lambda sql: db.execute(sql).fetchone()[0]
r = {"ledger_copy": path, "sqlite": sqlite3.sqlite_version}
r["raw_rows"] = q("select count(*) from buffered_events")
r["usage_rows"] = q(f"select count(*) from buffered_events where {UR}")
r["activity_rows"] = r["raw_rows"] - r["usage_rows"]
r["sessions"] = q("select count(distinct session_id) from buffered_events where session_id is not null")
r["sessions_with_usage"] = q(f"select count(distinct session_id) from buffered_events where session_id is not null and {UR}")
r["session_days"] = q(f"select count(*) from (select distinct session_id, {DAY} from buffered_events where session_id is not null)")
r["session_day_dims"] = q(f"select count(*) from (select distinct session_id, {DAY}, coalesce(source,''), coalesce(repo_hash,''), coalesce(branch_hash,''), coalesce(head_sha,''), coalesce(account_hash,''), coalesce(machine,'') from buffered_events where session_id is not null)")
r["days"] = q(f"select count(distinct {DAY}) from buffered_events")
r["epochs"] = q("select count(distinct coalesce(installation_epoch_id,'')) from buffered_events")
r["sources"] = q("select count(distinct source) from buffered_events")
r["day_targets"] = q(f"select count(*) from (select distinct coalesce(installation_epoch_id,''), source, {DAY} from buffered_events)")
r["model_day_rows"] = q(f"select count(*) from (select distinct coalesce(installation_epoch_id,''), {DAY}, source, coalesce(model,'') from buffered_events where {UR})")
r["activity_day_rows"] = q(f"select count(*) from (select distinct coalesce(installation_epoch_id,''), {DAY}, source, event_type, coalesce(action_class,''), coalesce(machine,'') from buffered_events)")
r["sessionless_rows"] = q("select count(*) from buffered_events where session_id is null")
r["rollup_buckets"] = q(f"select count(*) from (select distinct {HOUR}, source, coalesce(installation_epoch_id,''), coalesce(model,''), event_type, coalesce(machine,'') from buffered_events where session_id is null)")
r["segment_proxy"] = q(f"select coalesce(sum(1 + cast((julianday(last_day) - julianday(first_day)) / 7 as integer)), 0) from (select session_id, min({DAY}) as first_day, max({DAY}) as last_day from buffered_events where session_id is not null group by session_id)")   # sessions split at 7 days (round 8)
r["turn_rows_upper_bound"] = q(f"select count(*) from (select distinct session_id, coalesce(json_extract(payload_json,'$.metadata.turnIndex'),rowid) from buffered_events where session_id is not null and {UR})")
r["payload_bytes"] = q("select coalesce(sum(length(payload_json)),0) from buffered_events")
try:
    r["raw_table_bytes_incl_indexes"] = q("select sum(pgsize) from dbstat where name='buffered_events' or name like 'idx_events_%' or name like 'sqlite_autoindex_buffered_events%'")
    r["ledger_bytes_dbstat"] = q("select sum(pgsize) from dbstat")
except sqlite3.OperationalError as exc:
    r["raw_table_bytes_incl_indexes"] = None; r["dbstat_error"] = str(exc)
r["page_size"] = q("pragma page_size"); r["page_count"] = q("pragma page_count"); r["freelist_count"] = q("pragma freelist_count")
r["shape_census"] = dict(db.execute("""select case
  when observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[01][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z'
    or observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T2[0-3]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z' then 'canonical'
  when observed_at not glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*' then 'non_iso'
  when strftime('%Y-%m-%d', observed_at) is null then 'unparsed_by_sqlite'
  when substr(observed_at,1,10) <> strftime('%Y-%m-%d', observed_at) then 'day_move'
  when observed_at < substr(observed_at,1,10)||'T00:00:00.000Z' then 'before_own_midnight'
  else 'examine' end as c, count(*) from buffered_events group by 1""").fetchall())
r["per_1000_usage"] = {k: round(1000 * r[k] / r["usage_rows"], 1) for k in ("sessions", "session_days", "session_day_dims", "model_day_rows", "activity_day_rows", "rollup_buckets", "segment_proxy", "turn_rows_upper_bound", "activity_rows")} if r["usage_rows"] else {}
r["elapsed_s"] = round(time.time() - t0, 1)
print(json.dumps(r, indent=2))
if out: open(out, "w").write(json.dumps(r, indent=2) + "\n")
