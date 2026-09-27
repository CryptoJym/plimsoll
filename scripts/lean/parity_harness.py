#!/usr/bin/env python3
"""B9a read-only census, V8 fixture check and SQL/original-snapshot comparison.

The ledger is attached with mode=ro&immutable=1 to an in-memory SQLite
connection. All expanded compact rows and oracle tables live in TEMP memory.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile


WINDOWS = (30, 90, 182, 365, 1825)
DAY_MS = 86_400_000
CANONICAL = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z$")
ISO_START = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}")
HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
MODEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
HERE = Path(__file__).resolve().parent


def v8_parse(values: list[str], node: str) -> tuple[str, dict[str, int | None]]:
    unique = list(dict.fromkeys(values))
    response = subprocess.run(
        [node, str(HERE / "date-parse.cjs")], input=json.dumps(unique),
        text=True, capture_output=True, check=True, timeout=180,
    )
    result = json.loads(response.stdout)
    if len(result["parsed"]) != len(unique):
        raise ValueError("V8 parser returned the wrong number of rows")
    return result["node"], dict(zip(unique, result["parsed"]))


def connect(ledger: Path) -> sqlite3.Connection:
    if not ledger.is_file():
        raise FileNotFoundError(ledger)
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("attach database ? as ledger", (ledger.resolve().as_uri() + "?mode=ro&immutable=1",))
    return db


def fixture_module(path: Path, rule: str) -> tuple[dict, str]:
    # Execute the supplied fixture unchanged. Its Checks.finish exits, so
    # capture the module dictionary after that exit and require its verdict.
    old_argv, old_path = sys.argv, sys.path[:]
    sys.argv = [str(path), "--rule", rule]
    sys.path.insert(0, str(path.parent))
    namespace = {"__file__": str(path), "__name__": "__main__"}
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            try:
                exec(compile(path.read_text(), str(path), "exec"), namespace)
            except SystemExit as done:
                if done.code not in (0, None):
                    raise AssertionError(f"fixture failed: {path.name}\n{out.getvalue()}")
    finally:
        sys.argv, sys.path = old_argv, old_path
    return namespace, out.getvalue()


def fixture_check(fixture_dir: Path, node: str, logs: list[Path]) -> dict:
    b5, b5_log = fixture_module(fixture_dir / "b5_lexical_boundary.py", "r5")
    non_iso, non_iso_log = fixture_module(fixture_dir / "b5_non_iso_day_facts.py", "r6")
    strings = list(dict.fromkeys([*b5["valid"], *(r[1] for r in non_iso["ROWS"])]))
    runtime, parsed = v8_parse(strings, node)
    mismatches = []
    for value in b5["valid"]:
        expected = b5["date_parse_ms"](value)
        if parsed[value] != expected:
            mismatches.append({"value": value, "fixture": expected, "runtime": parsed[value]})
    for _, value, *_ in non_iso["ROWS"]:
        expected = non_iso["date_parse_ms"](value)
        if parsed[value] != expected:
            mismatches.append({"value": value, "fixture": expected, "runtime": parsed[value]})
    boundary_mismatches = []
    for value in b5["valid"]:
        expected = b5["differing_cutoffs"](value)
        actual = [day.isoformat() for day in b5["CUTOFFS"]
                  if (value >= b5["cutoff_str"](day)) !=
                  (parsed[value] is not None and parsed[value] >= b5["cutoff_ms"](day))]
        if expected != actual:
            boundary_mismatches.append({"value": value, "fixture": expected, "runtime": actual})
    log_differences = {}
    for path in logs:
        differences = []
        for line in path.read_text().splitlines():
            match = re.match(r'^"(.*?)"\s+schema_ok=\w+\s+ms=(\S+)', line)
            if not match:
                continue
            value, logged = match.groups()
            actual = v8_parse([value], node)[1][value] if value not in parsed else parsed[value]
            expected = None if logged == "NaN" else int(logged)
            if actual != expected:
                differences.append({"value": value, "logged": expected, "runtime": actual})
        log_differences[path.name] = differences
    return {
        "status": "PASS" if not mismatches and not boundary_mismatches and all(not d for d in log_differences.values()) else "FAIL",
        "node": runtime, "binary": node, "fixture_strings": len(strings),
        "non_iso_rows_checked": [row[1] for row in non_iso["ROWS"]],
        "non_iso_mismatches": [row for row in mismatches if row["value"] in {r[1] for r in non_iso["ROWS"]}],
        "b5_fixture_verdict": b5_log.splitlines()[-1],
        "non_iso_fixture_verdict": non_iso_log.splitlines()[-1],
        "parse_mismatches": mismatches, "boundary_mismatches": boundary_mismatches,
        "log_differences": log_differences,
    }


def census(ledger: Path, node: str) -> dict:
    with connect(ledger) as db:
        rows = list(db.execute((HERE / "census.sql").read_text()))
        runtime, parsed = v8_parse([r[2] for r in rows], node)
        classes = {name: 0 for name in (
            "canonical_T_3frac_Z", "day_move", "before_own_midnight", "non_iso",
            "unparsed_by_sqlite", "examine", "parse_reject", "noncanonical_union",
        )}
        shape_by_source: dict[str, dict[str, int]] = {}
        examples: dict[str, list[dict]] = {name: [] for name in classes}
        for raw_rowid, source, value, sql_shape, sqlite_day in rows:
            ms = parsed[value]
            source_shapes = shape_by_source.setdefault(source, {})
            source_shapes[sql_shape] = source_shapes.get(sql_shape, 0) + 1
            if sql_shape == "canonical_T_3frac_Z":
                classes["canonical_T_3frac_Z"] += 1
            else:
                classes["noncanonical_union"] += 1
                if sql_shape == "examine":
                    classes["examine"] += 1
            if sqlite_day is None:
                classes["unparsed_by_sqlite"] += 1
            if ms is None:
                classes["parse_reject"] += 1
                if len(examples["parse_reject"]) < 8:
                    examples["parse_reject"].append({"rowid": raw_rowid, "value": value})
                continue
            utc_day = datetime.fromtimestamp(ms / 1000, timezone.utc).date().isoformat()
            non_iso = not bool(ISO_START.match(value))
            flags = {
                "day_move": not non_iso and value[:10] != utc_day,
                "before_own_midnight": not non_iso and value < value[:10] + "T00:00:00.000Z",
                "non_iso": non_iso,
            }
            for name, active in flags.items():
                if active:
                    classes[name] += 1
                    if len(examples[name]) < 8:
                        examples[name].append({"rowid": raw_rowid, "value": value, "utc_day": utc_day})
        # The shape partition and the three overlapping predicates are both
        # reported. A row may count in more than one predicate class.
        return {"status": "PASS" if not classes["parse_reject"] else "FAIL",
                "node": runtime, "rows": len(rows), "classes": classes,
                "shape_by_source": shape_by_source, "examples": examples}


def safe_hash(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.strip().lower()
    return lowered if HASH.fullmatch(lowered) else "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def canonical_linkage(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.strip().lower()
    return lowered if HASH.fullmatch(lowered) else None


def safe_model(value: str | None) -> str | None:
    if not value:
        return None
    return value if MODEL.fullmatch(value) else "sha256:" + hashlib.sha256(("model:" + value).encode()).hexdigest()


def cost_nanos(value: float | None) -> int | None:
    if value is None:
        return None
    # JS Math.round(x), including its rule at negative half units.
    import math
    return math.floor(value * 1_000_000_000 + 0.5)


def prepare_oracle(db: sqlite3.Connection, ledger: Path, node: str, frozen: datetime) -> dict:
    db.execute("create temp table oracle_compact_items(segment_id integer,item_index integer,raw_rowid integer,observed_at text,source text,event_type text,action_class text)")
    expanded = []
    for segment in db.execute("select segment_id,event_count,payload_gzip from ledger.dashboard_compact_segments"):
        items = json.loads(gzip.decompress(segment[2]))
        if len(items) != segment[1]:
            raise AssertionError(f"compact segment {segment[0]} has {len(items)} rows, expected {segment[1]}")
        expanded.extend((segment[0], index, item["rawRowid"], item["observedAt"],
                         item["source"], item["eventType"], item.get("actionClass"))
                        for index, item in enumerate(items))
    db.executemany("insert into oracle_compact_items values (?,?,?,?,?,?,?)", expanded)
    values = [r[0] for r in db.execute("select observed_at from ledger.buffered_events")]
    values.extend(r[3] for r in expanded)
    runtime, parsed = v8_parse(values, node)
    aliases = {safe_hash(r[0]): safe_hash(r[1]) for r in db.execute(
        "select alias_hash,canonical_hash from ledger.account_aliases")}
    db.create_function("parse_ms", 1, lambda value: parsed.get(value), deterministic=True)
    db.create_function("safe_hash", 1, safe_hash, deterministic=True)
    db.create_function("canonical_linkage", 1, canonical_linkage, deterministic=True)
    db.create_function("safe_model", 1, safe_model, deterministic=True)
    db.create_function("cost_nanos", 1, cost_nanos, deterministic=True)
    db.create_function("account_alias", 1, lambda value: aliases.get(value, value), deterministic=True)
    db.execute("create temp table oracle_clock(days integer,cutoff_ms integer,cutoff_iso text,frozen_ms integer)")
    frozen_ms = int(frozen.timestamp() * 1000)
    db.executemany("insert into oracle_clock values (?,?,?,?)", [
        (days, frozen_ms - days * DAY_MS,
         (frozen - timedelta(days=days)).isoformat(timespec="milliseconds").replace("+00:00", "Z"), frozen_ms)
        for days in WINDOWS
    ])
    db.executescript((HERE / "oracle.sql").read_text())
    return {"node": runtime, "raw_rows": len(values) - len(expanded),
            "compact_items": len(expanded), "oracle_rows": db.execute("select count(*) from oracle_rows").fetchone()[0],
            "epoch_keys": [r[0] for r in db.execute("select distinct epoch_key from oracle_rows order by 1")],
            "rejects": [dict(r) for r in db.execute(
                "select event_id,observed_at from oracle_rows where observed_ms is null limit 30")]}


def rows_as_dict(db: sqlite3.Connection, sql: str, params=()) -> list[dict]:
    return [dict(row) for row in db.execute(sql, params)]


def group_compare(label: str, expected: dict, actual: dict, out: list[dict]) -> None:
    for key in sorted(set(expected) | set(actual)):
        old, new = expected.get(key), actual.get(key)
        equal = old == new or (isinstance(old, float) and isinstance(new, float) and abs(old - new) < 1e-9)
        out.append({"group": label, "key": key, "old": old, "oracle": new,
                    "status": "PASS" if equal else "FAIL"})


def fixed_four(value: float) -> float:
    """Match Number(value.toFixed(4)) for the snapshot's repository tail."""
    return float(Decimal.from_float(value).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def displayed_repos(repo_rows: list[sqlite3.Row], published: dict) -> dict:
    def key(row: sqlite3.Row) -> str:
        return row["repo_hash"] or "__unlinked__"

    def value(row: sqlite3.Row) -> dict:
        return {"repoHash": row["repo_hash"], "label": None, "sessions": row["sessions"],
                "branchRefs": row["branch_refs"], "inputTokens": row["input_tokens"],
                "outputTokens": row["output_tokens"], "costUsd": row["cost_nanos"] / 1e9}

    if len(repo_rows) <= 12:
        return {key(row): value(row) for row in repo_rows}

    # The producer orders only by costNanos. At the 11th-place boundary it
    # declares no secondary tie-break. Use its published head identities only
    # when they form a valid top-11 set; every compared value remains oracle-
    # derived. An invalid head falls back to the independent query order and
    # produces ordinary value differences.
    cutoff = repo_rows[10]["cost_nanos"]
    mandatory = {key(row) for row in repo_rows if row["cost_nanos"] > cutoff}
    eligible = {key(row) for row in repo_rows if row["cost_nanos"] >= cutoff}
    published_head = set(published) - {"__tail__"}
    if ("__tail__" in published and len(published_head) == 11
            and mandatory <= published_head <= eligible):
        head_keys = published_head
    else:
        head_keys = {key(row) for row in repo_rows[:11]}
    head = {key(row): value(row) for row in repo_rows if key(row) in head_keys}
    tail = [value(row) for row in repo_rows if key(row) not in head_keys]
    head["__tail__"] = {
        "repoHash": "__tail__", "label": f"({len(tail)} more repositories)",
        "sessions": sum(row["sessions"] for row in tail), "branchRefs": 0,
        "inputTokens": sum(row["inputTokens"] for row in tail),
        "outputTokens": sum(row["outputTokens"] for row in tail),
        "costUsd": fixed_four(sum(row["costUsd"] for row in tail)),
    }
    return head


def query_manifest(parity_log: Path | None, fixture_log: Path | None, census_log: Path | None) -> dict:
    queries = json.loads((HERE / "proof-queries.json").read_text())
    by_id = {row["id"]: row for row in queries}
    if len(queries) != 34 or len(by_id) != len(queries):
        raise AssertionError("PROOF §4 query list must contain 34 unique rows")
    for row in queries:
        row["status"] = "NOT RUN"
        row["evidence"] = None
    if parity_log:
        parity = json.loads(parity_log.read_text())
        by_id["dashboard_five"].update(status=parity["status"], evidence=str(parity_log))
    if fixture_log:
        fixture = json.loads(fixture_log.read_text())
        by_id["boundary_timestamps"].update(status=fixture["status"], evidence=str(fixture_log))
        non_iso = fixture["non_iso_fixture_verdict"].endswith("GREEN") and not fixture["non_iso_mismatches"]
        by_id["non_iso_day_facts"].update(status="PASS" if non_iso else "FAIL", evidence=str(fixture_log))
    return {"schema": "lean-proof-query-skeleton/v1", "fixed_query_count": len(queries),
            "studio0_baseline_rule": "On Studio0's copy, use this independent raw-row and compact-segment SQL oracle as the S0 baseline. Its published schema-2 snapshots are stale while repairs remain queued. A converged schema-2 snapshot on the copy may be kept as a second reference; S3 certification compares schema 3 with the oracle.",
            "queries": queries,
            "census_evidence": str(census_log) if census_log else None}


def oracle_selftest(node: str, scratch_dir: Path) -> dict:
    # The Studio5 copy is all canonical. Exercise SQL itself against the B5
    # boundary families and an expired compact item without changing that copy.
    with tempfile.TemporaryDirectory(prefix="lean-b9a-", dir=scratch_dir) as root:
        ledger = Path(root) / "fixture.sqlite"
        db = sqlite3.connect(ledger)
        db.executescript("""
          create table buffered_events(
            id text,installation_epoch_id text,source text,event_type text,observed_at text,
            session_id text,action_class text,model text,input_tokens integer,output_tokens integer,
            cache_read_tokens integer,cache_creation_tokens integer,cost_usd real,repo_hash text,
            branch_hash text,machine text,account_hash text,data_mode text,privacy_disposition text,
            usage_duplicate_reason text,privacy_generation text,created_at text);
          create table upload_receipts(delivery_id text,reason text);
          create table upload_outbox(raw_rowid integer,raw_id text,raw_created_at text,raw_generation text);
          create table account_aliases(alias_hash text,canonical_hash text);
          create table priority_repos(repo_hash text);
          create table dashboard_compact_segments(segment_id integer,event_count integer,payload_gzip blob);
          create table dashboard_compact_cancellations(raw_rowid integer,observed_at text,source text,event_type text,action_key text);
        """)
        times = [
            ("c1", "2026-09-25T10:00:00.000Z", 50, 5),
            ("c2", "2026-09-26T00:00:00+00:00", 40, 4),
            ("r1", "Sat, 26 Sep 2026 00:00:00 GMT+00:00", 100, 10),
            ("r3", "1 Sep 2026 00:00:00 +00:00", 7, 1),
            ("d1", "2026-09-25T23:30:00-02:00", 20, 2),
        ]
        db.executemany("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            (event_id, "epochA", "codex", "usage_rollout", observed, None, None, "gpt-5", i, o,
             None, None, None, None, None, None, None, "metadata", None, None, "gen", observed)
            for event_id, observed, i, o in times
        ])
        compact = [{"rawRowid": 999, "observedAt": "Sat, 26 Sep 2026 00:00:00 GMT+00:00",
                    "source": "codex", "eventType": "tool_use", "actionClass": "other", "windowMask": 63}]
        db.execute("insert into dashboard_compact_segments values (?,?,?)",
                   (1, len(compact), gzip.compress(json.dumps(compact).encode())))
        db.commit()
        db.close()
        with connect(ledger) as memory:
            setup = prepare_oracle(memory, ledger, node, datetime(2026, 10, 26, tzinfo=timezone.utc))
            checks = {
                "all_accepted_rows_retained": setup["oracle_rows"] == 6 and not setup["rejects"],
                "epoch_keys_explicit": set(setup["epoch_keys"]) == {"epochA", "__compact_unknown_epoch__"},
                "non_iso_folded_to_utc_day": memory.execute(
                    "select utc_day from oracle_rows where event_id='r1'").fetchone()[0] == "2026-09-26",
                "day_move_uses_parsed_instant": memory.execute(
                    "select utc_day from oracle_rows where event_id='d1'").fetchone()[0] == "2026-09-26",
                "day_aligned_30_day_window": memory.execute(
                    "select count(*) from oracle_window_rows where days=30").fetchone()[0] == 4,
                "per_cutoff_zero_offset_flip": memory.execute(
                    "select lexical_in,instant_in from oracle_boundary where days=30 and event_id='c2'").fetchone()[:] == (0, 1),
                "per_cutoff_non_iso_flip": memory.execute(
                    "select lexical_in,instant_in from oracle_boundary where days=90 and event_id='r3'").fetchone()[:] == (0, 1),
                "compact_item_used_without_raw": memory.execute(
                    "select count(*) from oracle_rows where origin='compact'").fetchone()[0] == 1,
            }
            return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks, "setup": setup}


def comparison(db: sqlite3.Connection, frozen: datetime) -> dict:
    # Values are compared by stable (group,key,field), not array display order.
    values = {}
    for r in db.execute("select * from oracle_values"):
        values.setdefault((r["days"], r["group_name"], r["group_key"]), {})[r["metric"]] = r["value"]
    snapshots = {r["days"]: json.loads(r["payload_json"]) for r in db.execute(
        "select days,payload_json from ledger.dashboard_snapshots")}
    frozen_iso = frozen.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    result = {"frozen": frozen_iso, "windows": {}, "status": "PASS"}
    for days in WINDOWS:
        snap = snapshots[days]
        rows = []
        total = values.get((days, "totals", "all"), {})
        totals = snap["summary"]["totals"]
        compare_fields = ("events", "tokenEvents", "inputTokens", "outputTokens",
                          "cacheReadTokens", "cacheCreationTokens", "sessions", "sessionsWithTokens")
        group_compare("totals", {field: totals[field] for field in compare_fields},
                      {field: total.get(field, 0) for field in compare_fields}, rows)
        group_compare("totals", {"costUsd": totals["costUsd"]},
                      {"costUsd": total.get("costNanos", 0) / 1e9}, rows)
        span = db.execute("select min(observed_at),max(observed_at) from oracle_window_rows where days=?", (days,)).fetchone()
        group_compare("totals", {"oldest": totals["oldest"], "newest": totals["newest"]},
                      {"oldest": span[0], "newest": span[1]}, rows)
        old_groups = {r["source"]: r for r in snap["summary"]["bySource"]}
        oracle_keys = {key for d, group, key in values if d == days and group == "bySource"}
        for source in sorted(set(old_groups) | oracle_keys):
            old = old_groups.get(source, {})
            new = values.get((days, "bySource", source), {})
            fields = ("events", "sessions", "sessionsWithTokens", "inputTokens", "outputTokens")
            group_compare(f"bySource/{source}", {field: old.get(field) for field in fields},
                          {field: new.get(field, 0) for field in fields}, rows)
            group_compare(f"bySource/{source}", {"costUsd": old.get("costUsd")},
                          {"costUsd": new.get("costNanos", 0) / 1e9}, rows)
        old_daily = {r["day"]: r for r in snap["summary"]["daily"]}
        oracle_days = {key for d, group, key in values if d == days and group == "daily"}
        for day in sorted(set(old_daily) | oracle_days):
            old = old_daily.get(day, {})
            new = values.get((days, "daily", day), {})
            if new.get("tokens", 0) == 0 and new.get("costNanos", 0) == 0 and not old:
                continue
            group_compare(f"daily/{day}", {"tokens": old.get("tokens"), "costUsd": old.get("costUsd")},
                          {"tokens": new.get("tokens", 0), "costUsd": new.get("costNanos", 0) / 1e9}, rows)
        old_models = {r["model"]: r for r in snap["summary"]["byModel"]}
        model_values = {key: value for (d, group, key), value in values.items() if d == days and group == "byModel"}
        top_models = sorted(model_values, key=lambda key: (-model_values[key].get("costNanos", 0),
                                                            -model_values[key].get("inputTokens", 0), key))[:12]
        for model in sorted(set(old_models) | set(top_models)):
            old, new = old_models.get(model, {}), model_values.get(model, {}) if model in top_models else {}
            fields = ("calls", "unpricedCalls", "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens")
            group_compare(f"byModel/{model}", {field: old.get(field) for field in fields},
                          {field: new.get(field) for field in fields}, rows)
            group_compare(f"byModel/{model}", {"costUsd": old.get("costUsd")},
                          {"costUsd": new.get("costNanos", 0) / 1e9 if new else None}, rows)
        old_actions = {r["actionClass"]: r["n"] for r in snap["summary"]["actionMix"]}
        new_actions = {key: value["events"] for (d, group, key), value in values.items()
                       if d == days and group == "actionMix"}
        group_compare("actionMix", old_actions, new_actions, rows)
        old_sessions = {(r["sessionId"], r["source"]): r for r in snap["sessions"]}
        oracle_sessions = {}
        for r in db.execute("select * from oracle_top_sessions where days=? and display_rank<=60", (days,)):
            oracle_sessions[(r["session_hash"], r["source"])] = {
                "sessionId": r["session_hash"], "source": r["source"],
                "startedAt": r["started_at"], "endedAt": r["ended_at"],
                "events": r["events"], "inputTokens": r["input_tokens"],
                "outputTokens": r["output_tokens"], "cacheReadTokens": r["cache_read_tokens"],
                "costUsd": r["cost_nanos"] / 1e9, "branchHash": r["branch_hash"],
                "repoCount": r["repo_count"], "repoHash": r["dominant_repo_hash"], "repoLabel": None,
            }
        for key in sorted(set(old_sessions) | set(oracle_sessions)):
            group_compare(f"sessions/{key[0]}/{key[1]}", old_sessions.get(key, {}),
                          oracle_sessions.get(key, {}), rows)
        old_repos = {r["repoHash"] or "__unlinked__": r for r in snap["repos"]}
        repo_rows = list(db.execute("select * from oracle_repos where days=? order by cost_nanos desc", (days,)))
        oracle_repos = displayed_repos(repo_rows, old_repos)
        for key in sorted(set(old_repos) | set(oracle_repos)):
            group_compare(f"repos/{key}", old_repos.get(key, {}), oracle_repos.get(key, {}), rows)
        old_accounts = {r["accountHash"] or "__unlinked__": r for r in snap["accounts"]["accounts"]}
        machines = {}
        for r in db.execute("select days,account_hash,machine_hash from oracle_account_machines where days=?", (days,)):
            machines.setdefault(r["account_hash"] or "__unlinked__", []).append(r["machine_hash"])
        oracle_accounts = {}
        for r in db.execute("select * from oracle_accounts where days=?", (days,)):
            key = r["account_hash"] or "__unlinked__"
            oracle_accounts[key] = {
                "accountHash": r["account_hash"], "machines": sorted(machines.get(key, [])),
                "sessions": r["sessions"], "priorityUsd": r["priority_nanos"] / 1e9,
                "otherUsd": r["other_nanos"] / 1e9, "unlinkedUsd": r["unlinked_nanos"] / 1e9,
                "totalUsd": r["cost_nanos"] / 1e9, "claudeUsd": r["claude_nanos"] / 1e9,
                "codexUsd": r["codex_nanos"] / 1e9, "inputTokens": r["input_tokens"],
                "outputTokens": r["output_tokens"], "label": None, "email": None,
                "subscription": None,
            }
        for key in sorted(set(old_accounts) | set(oracle_accounts)):
            old = dict(old_accounts.get(key, {}))
            if "machines" in old:
                old["machines"] = sorted(old["machines"])
            group_compare(f"accounts/{key}", old, oracle_accounts.get(key, {}), rows)
        account_buckets = {
            "priorityUsd": round(sum(r["priorityUsd"] for r in oracle_accounts.values()), 4),
            "otherUsd": round(sum(r["otherUsd"] for r in oracle_accounts.values()), 4),
            "unlinkedUsd": round(sum(r["unlinkedUsd"] for r in oracle_accounts.values()), 4),
        }
        group_compare("accounts/buckets", snap["accounts"]["buckets"], account_buckets, rows)
        priority_count = db.execute("select count(distinct canonical_linkage(repo_hash)) from ledger.priority_repos").fetchone()[0]
        group_compare("accounts", {"priorityRepoCount": snap["accounts"]["priorityRepoCount"]},
                      {"priorityRepoCount": priority_count}, rows)
        lifetime = db.execute("""
          select count(*) as count,min(observed_at) as oldestCreatedAt,max(observed_at) as newestCreatedAt,
            coalesce(sum(input_tokens is not null or output_tokens is not null),0) as tokenAttributedEvents,
            coalesce(sum(input_tokens),0) as totalInputTokens,
            coalesce(sum(output_tokens),0) as totalOutputTokens,
            coalesce(sum(cost_nanos),0)/1000000000.0 as totalCostUsd
          from oracle_rows
        """).fetchone()
        lifetime = dict(lifetime)
        lifetime["unuploadedCount"] = None
        lifetime["metricSampleCount"] = db.execute("select count(*) from ledger.metric_samples").fetchone()[0]
        group_compare("status/stats", snap["status"]["stats"], lifetime, rows)
        target = (frozen - timedelta(days=days)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        suppressed = db.execute("""
          select count(*) from (
            select source,session_id from ledger.buffered_events
            where observed_at>=? and session_id is not null
              and event_type in ('usage_rollout','usage_transcript')
              and (input_tokens is not null or output_tokens is not null or
                cache_read_tokens is not null or cache_creation_tokens is not null or cost_usd is not null)
            intersect
            select source,session_id from ledger.buffered_events
            where session_id is not null and event_type not in ('usage_rollout','usage_transcript')
              and (input_tokens is not null or output_tokens is not null or
                cache_read_tokens is not null or cache_creation_tokens is not null or cost_usd is not null)
          )
        """, (target,)).fetchone()[0]
        group_compare("summary/usageAuthority",
                      {"backfillSessionsSuppressed": snap["summary"]["usageAuthority"]["backfillSessionsSuppressed"]},
                      {"backfillSessionsSuppressed": suppressed}, rows)
        # The old snapshot is an actual product artifact. At a later midnight,
        # prove that moving its published cutoff to T changes no old-membership
        # rows; only then is its group payload a valid old-path reference at T.
        published = snap["window"]["since"]
        moved = db.execute("select count(*) from oracle_rows where (observed_at>=?)<>(observed_at>=?)",
                           (published, target)).fetchone()[0]
        after_frozen = db.execute("select count(*) from oracle_rows where observed_ms>=?",
                                  (int(frozen.timestamp() * 1000),)).fetchone()[0]
        failures = sum(r["status"] == "FAIL" for r in rows)
        result["windows"][str(days)] = {"published_since": published, "target_since": target,
            "old_cutoff_membership_changes": moved, "rows_at_or_after_frozen": after_frozen,
            "compared": len(rows), "failures": failures, "values": rows}
        if failures or moved or after_frozen:
            result["status"] = "FAIL"
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=("fixture", "census", "parity", "queries", "selftest"))
    p.add_argument("--ledger", type=Path)
    p.add_argument("--node", required=True)
    p.add_argument("--fixture-dir", type=Path)
    p.add_argument("--node-log", type=Path, action="append", default=[])
    p.add_argument("--parity-log", type=Path)
    p.add_argument("--fixture-log", type=Path)
    p.add_argument("--census-log", type=Path)
    p.add_argument("--scratch-dir", type=Path)
    p.add_argument("--frozen", default="2026-09-28T00:00:00.000Z")
    args = p.parse_args()
    if args.command == "selftest":
        if not args.scratch_dir:
            p.error("selftest requires --scratch-dir")
        output = oracle_selftest(args.node, args.scratch_dir)
    elif args.command == "queries":
        output = query_manifest(args.parity_log, args.fixture_log, args.census_log)
    elif args.command == "fixture":
        if not args.fixture_dir:
            p.error("fixture requires --fixture-dir")
        output = fixture_check(args.fixture_dir, args.node, args.node_log)
    elif args.command == "census":
        if not args.ledger:
            p.error("census requires --ledger")
        output = census(args.ledger, args.node)
    else:
        if not args.ledger:
            p.error("parity requires --ledger")
        frozen = datetime.fromisoformat(args.frozen.replace("Z", "+00:00"))
        if frozen.utcoffset() != timedelta(0) or (frozen.hour, frozen.minute, frozen.second, frozen.microsecond) != (0, 0, 0, 0):
            p.error("parity requires a UTC-midnight --frozen instant")
        with connect(args.ledger) as db:
            setup = prepare_oracle(db, args.ledger, args.node, frozen)
            output = {"setup": setup, **comparison(db, frozen)}
            output["boundary"] = rows_as_dict(db, "select w.days,count(b.event_id) as noncanonical_rows,coalesce(sum(b.lexical_in<>b.instant_in),0) as flips,coalesce(sum(b.day_move),0) as day_moves,coalesce(sum(b.non_iso),0) as non_iso_rows from oracle_clock w left join oracle_boundary b on b.days=w.days group by w.days order by w.days")
            output["projection_exceptions"] = rows_as_dict(db,
                "select b.rowid as raw_rowid,b.event_type,b.usage_duplicate_reason from ledger.buffered_events b join ledger.dashboard_event_facts f on f.raw_rowid=b.rowid where b.usage_duplicate_reason is not null order by b.rowid")
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output.get("status", "PASS") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
