"""Shared helpers for the round-5, round-6, round-7, round-8 and round-9 (B0, eco-6hoxj.164.4, rounds 1-3) fixtures.

Every fixture is a standalone script: `python3 <fixture>.py --rule r4|r5|r6|r7|r8|r9`.
It builds the minimal in-memory SQLite (or plain Python) state for the reviewer's counterexample, applies the rule of the
named round, and asserts the outcome the plan promises. Round-5 fixtures are red under `--rule r4` and green under
`--rule r5`; round-6 fixtures are red under `--rule r5` and green under `--rule r6`; the round-7 (B0 round 1) fixtures were
red under `--rule r6` and green under `--rule r7`; the round-8 (B0 round 2) versions of the same two fixtures are red under
`--rule r6` and `--rule r7` and green under `--rule r8`; the round-9 (B0 round 3) versions of the same two fixtures are red under
`--rule r6`, `r7` and `r8` and green under `--rule r9` (exit 1 = RED, exit 0 = GREEN).
No collector, ledger, network or hosted service is touched.
"""
from __future__ import annotations
import argparse, hashlib, sys

MOD = 1 << 128

def H(event_id: str) -> int:
    """First 16 bytes of sha256(lower(trim(id))) as a 128-bit integer (ARCHITECTURE.md §2.2 'Digests')."""
    return int.from_bytes(hashlib.sha256(event_id.strip().lower().encode()).digest()[:16], "big")

def sum_digest(ids) -> int:
    return sum(H(i) for i in ids) % MOD

def hexd(v: int) -> str:
    return f"{v:032x}"

def digest16(payload: str) -> bytes:
    return hashlib.sha256(payload.encode()).digest()[:16]

def rule_arg() -> str:
    p = argparse.ArgumentParser()
    p.add_argument("--rule", choices=("r4", "r5", "r6", "r7", "r8", "r9"), required=True, help="which round's rule to apply")
    return p.parse_args().rule

class Checks:
    """Collects named assertions; prints one line each; exits 1 if any failed."""
    def __init__(self, fixture: str, rule: str):
        self.fixture, self.rule, self.rows = fixture, rule, []
        print(f"== {fixture} under the {rule} rule ==")
    def expect(self, ok: bool, name: str, detail: str = "") -> bool:
        self.rows.append((bool(ok), name))
        print(f"  [{'ok  ' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
        return bool(ok)
    def finish(self) -> None:
        failed = [n for ok, n in self.rows if not ok]
        print(f"{self.fixture} {self.rule}: {len(self.rows) - len(failed)}/{len(self.rows)} checks passed; verdict={'GREEN' if not failed else 'RED'}")
        sys.exit(0 if not failed else 1)
