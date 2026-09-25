#!/usr/bin/env python3
"""Capture honesty (round 6), ported with repository-relative paths (B0, review-r6 `shouldFix[3]`).

ARCHITECTURE.md §3.6 requires an unresolved or never-read file's gap to be OPEN-ENDED (`interval_basis = 'epoch_open'`, `ended_at_ms`
null); round 5's BEADS.md B22 and PROOF.md §8 item 1 still prescribed `epoch_to_last_write`, which reproduces the false-complete
case (a period after the file's last write called complete while an event stamped inside it is unparsed). This script is the
DOCUMENT half: it reads BEADS.md, PROOF.md and ARCHITECTURE.md, extracts the interval basis they prescribe, and replays the case
under it. The BEHAVIOURAL half lives beside sf5_unresolved_file_open_gap.py as repository tests (collector
tests/contracts/lean/capture-gaps.contract.ts; cloud tests/contracts/lean/capture-coverage.contract.test.ts).

Paths: `--docs DIR` names the document directory. Without it the script looks, in order, for the directory that holds the three
documents beside this fixture set (`<fixtures>/../`, the packet layout) and for `docs/lean/` at the enclosing repository root
(the layout of the ported copy in plimsoll `tests/contracts/lean/fixtures/`). Round-5 documents are not shipped with the packet;
the runner recreates their two prescriptions in a stub directory for the red run (out/checks/b22-r5-docs/).
"""
import argparse, re, sys
from pathlib import Path

def find_docs():
    here = Path(__file__).resolve()
    candidates = [here.parents[1]]
    for parent in here.parents:
        if (parent / "package.json").exists(): candidates.append(parent / "docs" / "lean")
    for d in candidates:
        if all((d / f).exists() for f in ("BEADS.md", "PROOF.md", "ARCHITECTURE.md")): return d
    return None

p = argparse.ArgumentParser()
p.add_argument("--rule", choices=("r4", "r5", "r6", "r7"), required=True)
p.add_argument("--docs", type=Path, default=None, help="directory holding BEADS.md, PROOF.md and ARCHITECTURE.md (default: repository-relative)")
args = p.parse_args()
sys.argv = [sys.argv[0], "--rule", args.rule]                 # _common.rule_arg() re-parses argv
from _common import Checks, rule_arg
rule = rule_arg()
c = Checks("b22_false_complete", rule)
DOCS = args.docs.resolve() if args.docs else find_docs()
if DOCS is None or not all((DOCS / f).exists() for f in ("BEADS.md", "PROOF.md", "ARCHITECTURE.md")):
    c.expect(False, "the documents were found relative to the repository or the packet (pass --docs DIR otherwise)", f"docs={DOCS}")
    c.finish()
beads = (DOCS / "BEADS.md").read_text(); proof = (DOCS / "PROOF.md").read_text(); arch = (DOCS / "ARCHITECTURE.md").read_text()
b22 = next((l for l in beads.splitlines() if l.startswith("| **B22**")), "")
sec8 = proof.split("\n## 8.", 1)[1].split("\n## ", 1)[0] if "\n## 8." in proof else ""
item1 = next((l for l in sec8.splitlines() if l.strip().startswith("1.")), "")

def prescribed(text):
    if re.search(r"epoch[_-]to[_-]last[_-]write", text): return "epoch_to_last_write"
    if "epoch_open" in text: return "epoch_open"
    return "unspecified"
pb, pp = prescribed(b22), prescribed(item1)
print(f"    documents read from {DOCS}: B22 row prescribes {pb}; PROOF §8 item 1 prescribes {pp}")
c.expect(pb == "epoch_open", "BEADS.md B22 prescribes the open-ended interval (epoch_open) for unresolved files", pb)
c.expect(pp == "epoch_open" and "null" in item1, "PROOF.md §8 item 1 accepts only epoch_open with a null ended_at_ms", f"{pp}; 'null' in item 1: {'null' in item1}")
c.expect("epoch_open" in arch and "ended_at_ms = null" in arch.replace("`", "").replace("**", ""), "ARCHITECTURE.md §3.6 specifies epoch_open with a null end")
c.expect(pb == pp == "epoch_open", "the bead, the acceptance drill and the architecture agree on one interval basis", f"B22={pb} PROOF§8={pp}")

# replay the false-complete case under the basis the documents prescribe (minutes-as-integers: 10_00 = 10:00)
EPOCH_START, LAST_WRITE, FIRST_PARSE, STAMPED = 0, 10_00, 11_00, 10_05
basis = "epoch_to_last_write" if "epoch_to_last_write" in (pb, pp) else pb
gap = {"started_at": EPOCH_START, "ended_at": LAST_WRITE if basis == "epoch_to_last_write" else None, "resolved": False}
def overlaps(g, start, end):
    if g["resolved"]: return False
    return g["started_at"] < end and (g["ended_at"] if g["ended_at"] is not None else float("inf")) >= start
def complete(gaps, start, end, through): return through >= end and not any(overlaps(g, start, end) for g in gaps)
period = (10_01, 11_00)
c.expect(not complete([gap], *period, through=11_00), "under the prescribed basis a period after the last write is NOT complete while the file is unparsed",
         f"basis={basis} gap={gap} complete={complete([gap], *period, through=11_00)}")
c.expect(gap["ended_at"] is None or STAMPED <= gap["ended_at"], "the event stamped 10:05 (after the 10:00 last write; the clamp keeps it, normalizer.ts:203-214) lies inside the declared gap", f"gap end={gap['ended_at']}")
gap["resolved"] = True
c.expect(complete([gap], *period, through=11_00), "after the file is parsed to its end the gap resolves and the period can be complete")
if rule in ("r6", "r7"):
    c.expect("b22_false_complete.py" in sec8 and "b22_false_complete.py" in b22, "PROOF §8 and the B22 acceptance run this fixture at the B22 integration gate")
c.finish()
