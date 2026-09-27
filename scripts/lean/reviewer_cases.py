#!/usr/bin/env python3
"""Disposable schema-2 presentation fixtures for PR 413's two review cases.

The old snapshot's session key and repository tail shape come from
dashboard-projection.ts:1038-1055,3742-3777 at ce091870. Every ledger created
here is disposable and is reopened by the oracle as ro+immutable.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile


FROZEN = datetime(2026, 9, 28, tzinfo=timezone.utc)
WINDOWS = (30, 90, 182, 365, 1825)


def digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def raw_rows(case: str) -> list[dict]:
    if case == "epoch_collision":
        return [
            dict(id="epoch-a", epoch="epoch-a", session="same-session", repo=digest("repo"),
                 account=digest("account"), observed="2026-09-27T12:00:00.000Z",
                 input=4, output=1, cost=4.0),
            dict(id="epoch-b", epoch="epoch-b", session="same-session", repo=digest("repo"),
                 account=digest("account"), observed="2026-09-27T12:01:00.000Z",
                 input=6, output=2, cost=6.0),
        ]
    costs = list(range(1, 14))
    if case == "repo_tie":
        # Repositories 2 and 3 tie across the eleventh-place boundary.
        costs[2] = 2
    else:
        # Both tail costs have fractional cents; the producer rounds their sum.
        costs[0], costs[1] = 0.00006, 2.00006
    return [dict(id=f"repo-{i}", epoch="one-epoch", session=f"session-{i}",
                 repo=digest(f"repo-{i}"), account=None,
                 observed=f"2026-09-27T12:00:{i:02d}.000Z",
                 input=i, output=2 * i, cost=float(costs[i - 1]))
            for i in range(1, 14)]


def producer_repos(db: sqlite3.Connection, rows: list[dict]) -> list[dict]:
    """Execute the producer's group/order query, then its 11+tail projection."""
    db.execute("drop table if exists temp.repo_session")
    db.execute("create temp table repo_session(repo_key text,repo_hash text,session_hash text,input_tokens integer,output_tokens integer,cost_nanos integer)")
    grouped = {}
    for row in rows:
        key = (row["repo"], digest(row["session"]))
        entry = grouped.setdefault(key, [0, 0, 0])
        entry[0] += row["input"]
        entry[1] += row["output"]
        entry[2] += int(row["cost"] * 1_000_000_000 + 0.5)
    db.executemany("insert into repo_session values (?,?,?,?,?,?)",
                   [(repo, repo, session, *values) for (repo, session), values in grouped.items()])
    ordered = db.execute("""
        select repo_key as repoKey,repo_hash as repoHash,count(*) as sessions,
          sum(input_tokens) as inputTokens,sum(output_tokens) as outputTokens,
          sum(cost_nanos) as costNanos
        from repo_session group by repo_key,repo_hash order by costNanos desc
    """).fetchall()
    if len(rows) > 12 and rows[1]["cost"] == rows[2]["cost"]:
        # ORDER BY has no secondary key. Either order across this tie is valid;
        # exercise the choice opposite this SQLite execution's default.
        assert ordered[10][5] == ordered[11][5]
        ordered[10], ordered[11] = ordered[11], ordered[10]
    repos = [dict(repoHash=r[1], label=None, sessions=r[2], branchRefs=0,
                  inputTokens=r[3], outputTokens=r[4], costUsd=r[5] / 1e9)
             for r in ordered]
    if len(repos) > 12:
        tail = repos[11:]
        repos = repos[:11] + [dict(repoHash="__tail__", label=f"({len(tail)} more repositories)",
                                   sessions=sum(r["sessions"] for r in tail), branchRefs=0,
                                   inputTokens=sum(r["inputTokens"] for r in tail),
                                   outputTokens=sum(r["outputTokens"] for r in tail),
                                   costUsd=round(sum(r["costUsd"] for r in tail), 4))]
    return repos


def snapshot(db: sqlite3.Connection, rows: list[dict], days: int) -> dict:
    input_tokens = sum(row["input"] for row in rows)
    output_tokens = sum(row["output"] for row in rows)
    cost = sum(row["cost"] for row in rows)
    sessions = {}
    for row in rows:
        sessions.setdefault(row["session"], []).append(row)
    session_rows = []
    for session, members in sessions.items():
        session_rows.append(dict(sessionId=digest(session), source="codex",
                                 startedAt=min(r["observed"] for r in members),
                                 endedAt=max(r["observed"] for r in members),
                                 events=len(members), inputTokens=sum(r["input"] for r in members),
                                 outputTokens=sum(r["output"] for r in members),
                                 cacheReadTokens=0, costUsd=sum(r["cost"] for r in members),
                                 branchHash=None, repoCount=1, repoHash=members[0]["repo"],
                                 repoLabel=None))
    session_rows.sort(key=lambda r: (-r["costUsd"], -r["events"]))
    account = rows[0]["account"]
    account_row = dict(accountHash=account, machines=[], sessions=len(sessions),
                       priorityUsd=0, otherUsd=cost, unlinkedUsd=0, totalUsd=cost,
                       claudeUsd=0, codexUsd=cost, inputTokens=input_tokens,
                       outputTokens=output_tokens, label=None, email=None, subscription=None)
    oldest = min(row["observed"] for row in rows)
    newest = max(row["observed"] for row in rows)
    since = (FROZEN - timedelta(days=days)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return {
        "window": {"since": since},
        "summary": {
            "totals": dict(events=len(rows), tokenEvents=len(rows), inputTokens=input_tokens,
                           outputTokens=output_tokens, cacheReadTokens=0, cacheCreationTokens=0,
                           sessions=len(sessions), sessionsWithTokens=len(sessions),
                           costUsd=cost, oldest=oldest, newest=newest),
            "bySource": [dict(source="codex", events=len(rows), sessions=len(sessions),
                              sessionsWithTokens=len(sessions), inputTokens=input_tokens,
                              outputTokens=output_tokens, costUsd=cost)],
            "daily": [dict(day="2026-09-27", tokens=input_tokens + output_tokens, costUsd=cost)],
            "byModel": [], "actionMix": [],
            "usageAuthority": {"backfillSessionsSuppressed": 0},
        },
        "sessions": session_rows,
        "repos": producer_repos(db, rows),
        "accounts": {"accounts": [account_row],
                     "buckets": {"priorityUsd": 0, "otherUsd": round(cost, 4), "unlinkedUsd": 0},
                     "priorityRepoCount": 0},
        "status": {"stats": dict(count=len(rows), oldestCreatedAt=oldest, newestCreatedAt=newest,
                                 tokenAttributedEvents=len(rows), totalInputTokens=input_tokens,
                                 totalOutputTokens=output_tokens, totalCostUsd=cost,
                                 unuploadedCount=None, metricSampleCount=0)},
    }


def make_ledger(path: Path, case: str) -> dict:
    rows = raw_rows(case)
    db = sqlite3.connect(path)
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
        create table dashboard_snapshots(days integer,payload_json text);
        create table metric_samples(id integer);
    """)
    db.executemany("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        (r["id"], r["epoch"], "codex", "usage_rollout", r["observed"], r["session"],
         None, None, r["input"], r["output"], None, None, r["cost"], r["repo"],
         None, None, r["account"], "metadata", None, None, "generation", r["observed"])
        for r in rows
    ])
    snapshots = [(days, json.dumps(snapshot(db, rows, days))) for days in WINDOWS]
    db.executemany("insert into dashboard_snapshots values (?,?)", snapshots)
    db.commit()
    db.close()
    return {"raw_rows": len(rows), "epoch_count": len({r["epoch"] for r in rows}),
            "expected_session_count": len({r["session"] for r in rows}),
            "expected_repo_keys": [r["repoHash"] for r in json.loads(snapshots[0][1])["repos"]]}


def load_harness(path: Path):
    spec = importlib.util.spec_from_file_location("parity_harness_under_review", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def run_case(case: str, harness, node: str, scratch_dir: Path) -> dict:
    with tempfile.TemporaryDirectory(prefix="lean-pr413-", dir=scratch_dir) as directory:
        ledger = Path(directory) / "fixture.sqlite"
        setup = make_ledger(ledger, case)
        with harness.connect(ledger) as db:
            oracle = harness.prepare_oracle(db, ledger, node, FROZEN)
            top = [dict(r) for r in db.execute(
                "select epoch_key,session_hash,source,events,input_tokens,cost_nanos from oracle_top_sessions where days=30")]
            try:
                result = harness.comparison(db, FROZEN)
            except AssertionError as error:
                return {"case": case, "status": "FAIL", "exception": str(error),
                        "fixture": setup, "oracle": oracle, "top_sessions": top}
            failures = {days: [row for row in window["values"] if row["status"] == "FAIL"]
                        for days, window in result["windows"].items()}
            return {"case": case, "status": result["status"], "fixture": setup,
                    "oracle": oracle, "top_sessions": top,
                    "compared": {days: w["compared"] for days, w in result["windows"].items()},
                    "failures": failures}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--scratch-dir", type=Path, required=True)
    parser.add_argument("--case", choices=("repo_tail", "repo_tie", "epoch_collision", "all"), default="all")
    args = parser.parse_args()
    harness = load_harness(args.harness.resolve())
    names = ("repo_tail", "repo_tie", "epoch_collision") if args.case == "all" else (args.case,)
    results = [run_case(name, harness, args.node, args.scratch_dir) for name in names]
    print(json.dumps({"schema": "lean-pr413-reviewer-cases/v1", "harness": str(args.harness.resolve()),
                      "results": results, "status": "PASS" if all(r["status"] == "PASS" for r in results) else "FAIL"},
                     indent=2, sort_keys=True))
    return 0 if all(r["status"] == "PASS" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
