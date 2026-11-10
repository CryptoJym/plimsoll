#!/usr/bin/env python3
"""One focused synthetic pass; scripted responses exercise plumbing, not accuracy."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
import pilot as p

ROOT = Path(__file__).resolve().parents[2]
BASELINE = Path(os.environ.get("PLIMSOLL_BASELINE_SOURCE", str(Path.home() / ".claude/skills/plimsoll-thread-adjudicator/scripts/adjudicate.py")))
spec = importlib.util.spec_from_file_location("baseline", BASELINE)
baseline = importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)
data = json.loads((Path(__file__).parent / "fixtures/sample.json").read_text())
state = {"version": p.VERSION, "decisions": {}, "baseline": {}, "owners": {}}
calls = []
gold = {"a1": "Plimsoll", "m1": "Plimsoll", "m2": "Second String", "c1": "Second String", "u1": "Unknown", "o1": "Plimsoll"}
base_gold = {"single": "Plimsoll", "mixed": "Second String", "alias": "Plimsoll", "owner": "Plimsoll", "child": "Second String", "uncertain": "Unknown"}


def replay(params, items, original):
    assert params["model"] == "claude-opus-5-5" and params["output_config"]["effort"] == "max"
    assert p.GUARD in params["system"][0]["text"]
    calls.append((original, len(items)))
    rows, previous = [], None
    for item in items:
        label = base_gold[item["id"]] if original else gold[item["turn_ids"][0]]
        row = {"label": label, "confidence": 20 if label == "Unknown" else 92,
               "reason": "Scripted fixture; no model accuracy claim"}
        if original:
            rows.append({**row, "id": item["id"], "suggest": "", "suggest_parent": ""})
        else:
            if previous == (item["thread_id"], label, int(item["wire_id"]) - 1):
                rows[-1]["end"] = item["wire_id"]
            else:
                rows.append({**row, "start": item["wire_id"], "end": item["wire_id"]})
            previous = (item["thread_id"], label, int(item["wire_id"]))
    return {"results": rows}, None, "scripted-not-a-provider"


def compare(sample):
    return p.compare(sample, state, baseline, replay, "replay")


first = compare(data)
assert calls == [(True, 6), (False, 5)]
assert first["baseline"]["metrics"]["provider_usage"] is None
assert first["pilot"]["metrics"]["deterministic_decisions"] == 2
threads = first["pilot"]["threads"]
assert len(threads["single"]) == 1 and threads["single"][0]["allocation_basis"] == "measured_per_turn"
assert [s["label"] for s in threads["mixed"]] == ["Plimsoll", "Second String"]
assert threads["mixed"][0]["end_time_exclusive"] == "2026-11-09T11:05:00Z"
assert all(s["allocation_basis"].startswith("estimated") for s in threads["mixed"])
assert threads["alias"][0]["label"] == "Plimsoll" and threads["owner"][0]["label"] == "Second String"
assert threads["uncertain"][0]["status"] == "unknown"
assert first["pilot"]["families"]["mixed"]["exclusive_thread_ids"] == ["mixed", "child"]
assert all(c["pass"] for c in first["pilot"]["conservation"])
calls.clear(); warm = compare(data)
assert not calls and warm["pilot"]["metrics"]["cache_hits"] == 5
changed = copy.deepcopy(data)
changed["threads"][0]["baseline_brief"] = {"turns": 900, "paths": ["/fixture/plimsoll (900)"]}
changed["threads"][0]["turns"][0]["paths"] = ["/fixture/plimsoll (900)"]
del changed["threads"][3]["owner"]
calls.clear(); counts = compare(changed)
assert calls == [(True, 1)] and counts["pilot"]["threads"]["owner"][0]["status"] == "owner"
# Appending the same purpose adds a real ordered turn, conserving totals with no call.
appended = copy.deepcopy(changed)
appended["threads"][0]["turns"].append({**appended["threads"][0]["turns"][-1], "id": "a3", "index": 3, "timestamp": "2026-11-09T10:02:00Z"})
for k in p.TOKENS:
    appended["threads"][0]["totals"][k] += appended["threads"][0]["turns"][-1]["usage"][k]
calls.clear(); extra_turn = compare(appended)
assert not calls and extra_turn["pilot"]["threads"]["single"][0]["turn_ids"] == ["a1", "a2", "a3"]
changed["catalog"][0]["description"] += "; expanded accounting scope"
calls.clear(); recatalog = compare(changed)
assert calls == [(False, 5)]  # even the previously confident decisions reconsidered
catalog, _ = p.validate(data, {})
items = [{"id": f"unit{i}", "wire_id": str(i), "thread_id": "thread", "turn_ids": [f"turn{i}"]} for i in range(3)]
good = {"start": "0", "end": "2", "label": "Plimsoll", "confidence": 90, "reason": "Evidence"}
for rows in ([{**good, "end": "alien"}], [good, good], [], [{**good, "confidence": 101}],
             [{**good, "confidence": -1}], [{**good, "confidence": True}], [{**good, "percentage": -1}],
             [{**good, "label": "Invented"}], [{**good, "start": "1"}], [{**good, "start": "2", "end": "0"}],
             [{**good, "end": "1"}, {**good, "start": "1"}],
             [{**good, "start": "2"}, {**good, "end": "1"}]):
    try:
        p.checked({"results": rows}, items, catalog)
        raise AssertionError("invalid response accepted")
    except p.PilotError:
        pass
for blocked in (items[::2], [items[0], items[1], {**items[2], "thread_id": "other"}]):
    try:
        p.checked({"results": [good]}, blocked, catalog)
        raise AssertionError("range crossed context/owner or thread")
    except p.PilotError:
        pass
guesses = p.checked({"results": [{**good, "confidence": 60}]}, items, catalog)
assert all(r["status"] == "best_guess" and r["evidence"] == [f"turn{i}"] for i, r in enumerate(guesses.values()))
assert '"minimum"' not in p.packed(p.schema(catalog)) and '"maximum"' not in p.packed(p.schema(catalog))
for alter in (lambda d: d["threads"][0].update(turns_complete=False),
              lambda d: d["threads"][0].update(totals_scope="inclusive"),
              lambda d: d["threads"][0]["turns"][0]["usage"].update(input_tokens=999)):
    invalid = copy.deepcopy(data); alter(invalid)
    try:
        p.validate(invalid, {})
        raise AssertionError("invalid conservation or boundaries accepted")
    except p.PilotError:
        pass
with tempfile.TemporaryDirectory(prefix="plimsoll-shadow-check-") as tmp:
    state_file = Path(tmp) / "state.json"; p.atomic_json(state_file, state)
    assert state_file.stat().st_mode & 0o777 == 0o600
    assert "previous_request" not in state_file.read_text()
    cli = subprocess.run([sys.executable, str(Path(__file__).with_name("pilot.py")), "--input", str(Path(__file__).parent / "fixtures/sample.json"), "--state-dir", tmp, "--baseline-source", str(BASELINE)], capture_output=True, text=True)
    assert cli.returncode == 0, cli.stderr
    assert json.loads(cli.stdout)["pilot"]["provider_requests"] == 0
changed["threads"][3]["owner"] = {"project_key": None, "revision": 2, "reason": "Release the correction"}
released = compare(changed)
assert released["pilot"]["threads"]["owner"][0]["status"] == "confident"
run = subprocess.run([os.environ.get("NODE", "node"), str(ROOT / "packages/lean-reader/pilot-check.mjs")], capture_output=True, text=True)
assert run.returncode == 0, run.stderr
reader = json.loads(run.stdout)
receipt = {"status": "PASS", "provider_calls": 0, "classification": "scripted responses; accuracy not checked",
           "cold": {k: first[k]["metrics"] for k in ("baseline", "pilot")},
           "unchanged_second_pass": {k: warm[k]["metrics"] for k in ("baseline", "pilot")},
           "count_only_change": {k: counts[k]["metrics"] for k in ("baseline", "pilot")},
           "relevant_catalogue_change": recatalog["pilot"]["metrics"],
           "reader": reader}
print(json.dumps(receipt, indent=2))
