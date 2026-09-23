#!/usr/bin/env python3
"""Fleet liveness/status reader after eco-6hoxj.154.

Unauthenticated HTTP is GET /healthz only ({ok: true}). Full collector status
is the credentialed CLI (`plimsoll status`), never a raw GET of the management
route. This file must not open that management route and must not print tokens.

Sweep-boundary capture-health reads (eco-6hoxj.155): a single
`0 this sweep, N this tick` sample is a transient. CHECK only if it persists
across two reads at least 10 s apart (mirrors the existing 25 s second-read
rule, with a shorter window because this flicker is one cadence).
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import shutil
import subprocess
import sys

DEFAULT_PORT = 48271
HEALTHZ_PATH = "/healthz"
LIVENESS_TIMEOUT_S = 3
STATUS_TIMEOUT_S = 15
# eco-6hoxj.155: leftover tick beside a new-sweep zero. Shorter than the
# existing 25 s second-read rule because this lie is one cadence wide.
SWEEP_BOUNDARY_TRANSIENT_MIN_S = 10
ZERO_SWEEP_LEFTOVER_TICK = re.compile(
    r"(?<![0-9])0 entr\(ies\) this sweep, ([1-9][0-9]*) this tick"
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PLIMSOLL_PORT", DEFAULT_PORT)),
        help=f"Collector loopback port (default {DEFAULT_PORT} or PLIMSOLL_PORT)",
    )
    parser.add_argument(
        "--liveness-only",
        action="store_true",
        help="Probe GET /healthz only; do not run plimsoll status",
    )
    parser.add_argument(
        "--plimsoll-bin",
        default=os.environ.get("PLIMSOLL_BIN", ""),
        help="Credentialed CLI path (default: PLIMSOLL_BIN or plimsoll on PATH)",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Prove the sweep-boundary transient rule; do not contact the collector",
    )
    return parser.parse_args(argv)


def _as_nonneg_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer():
        number = int(value)
        return number if number >= 0 else None
    return None


def incoherent_sweep_tick(scan: object, reason: object = None) -> bool:
    """True when a receipt reports 0 this sweep beside a leftover this-tick count."""
    if isinstance(scan, dict):
        sweep = _as_nonneg_int(scan.get("entriesThisSweep"))
        tick = _as_nonneg_int(scan.get("entriesThisTick"))
        if sweep == 0 and tick is not None and tick > 0:
            return True
    if isinstance(reason, str) and ZERO_SWEEP_LEFTOVER_TICK.search(reason):
        return True
    return False


def classify_sweep_tick_boundary(
    first_scan: object,
    second_scan: object,
    elapsed_s: float,
    first_reason: object = None,
    second_reason: object = None,
) -> str:
    """Classify a pair of capture-health reads.

    `ok` — first read is coherent.
    `transient` — first read is sweep=0,tick>0 but it did not persist across
    two reads at least 10 s apart.
    `check` — the lie persisted across two reads at least 10 s apart.
    """
    if not incoherent_sweep_tick(first_scan, first_reason):
        return "ok"
    if second_scan is None and first_reason is None:
        return "transient"
    if elapsed_s < SWEEP_BOUNDARY_TRANSIENT_MIN_S:
        return "transient"
    if incoherent_sweep_tick(second_scan, second_reason):
        return "check"
    return "transient"


def run_self_test() -> int:
    lie = {"entriesThisSweep": 0, "entriesThisTick": 33, "converging": True}
    healthy = {"entriesThisSweep": 49, "entriesThisTick": 33, "converging": True}
    progressed = {"entriesThisSweep": 82, "entriesThisTick": 33, "converging": True}
    reason_lie = (
        "local activity scan still sweeping — 0/6 capture root(s) enumerated, "
        "0 entr(ies) this sweep, 33 this tick"
    )
    reason_ok = (
        "local activity scan still sweeping — 3/6 capture root(s) enumerated, "
        "49 entr(ies) this sweep, 33 this tick"
    )
    reason_digit = (
        "local activity scan still sweeping — 3/6 capture root(s) enumerated, "
        "20 entr(ies) this sweep, 33 this tick"
    )
    cases = [
        ("single_lie_is_transient", classify_sweep_tick_boundary(lie, None, 0) == "transient"),
        ("too_soon_second_read_is_transient", classify_sweep_tick_boundary(lie, lie, 9.9) == "transient"),
        ("persisted_lie_is_check", classify_sweep_tick_boundary(lie, lie, 10) == "check"),
        ("boundary_then_progress_is_transient", classify_sweep_tick_boundary(lie, healthy, 10) == "transient"),
        ("healthy_is_ok", classify_sweep_tick_boundary(healthy, progressed, 10) == "ok"),
        ("reason_lie_is_incoherent", incoherent_sweep_tick(None, reason_lie) is True),
        ("reason_ok_is_coherent", incoherent_sweep_tick(None, reason_ok) is False),
        ("digit_boundary_is_coherent", incoherent_sweep_tick(None, reason_digit) is False),
        ("min_window_is_10s", SWEEP_BOUNDARY_TRANSIENT_MIN_S == 10),
    ]
    failed = [name for name, ok in cases if not ok]
    if failed:
        print(f"self_test_failed:{','.join(failed)}", file=sys.stderr)
        return 1
    json.dump(
        {
            "schema": "plimsoll.native-status-read.self-test.v1",
            "status": "pass",
            "checks": [name for name, _ in cases],
        },
        sys.stdout,
        separators=(",", ":"),
    )
    sys.stdout.write("\n")
    return 0


def read_liveness(port: int) -> dict[str, object]:
    # HTTP/1.0 + Connection: close matches the collector (keepAliveTimeout=0).
    # urllib HTTP/1.1 keep-alive waits out the timeout against that listener.
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=LIVENESS_TIMEOUT_S)
    try:
        connection._http_vsn = 10
        connection._http_vsn_str = "HTTP/1.0"
        connection.request(
            "GET",
            HEALTHZ_PATH,
            headers={
                "Host": f"127.0.0.1:{port}",
                "Accept": "application/json",
                "Connection": "close",
            },
        )
        response = connection.getresponse()
        raw = response.read()
        status = int(response.status)
    except Exception as error:
        raise SystemExit(f"liveness_unreachable:{type(error).__name__}")
    finally:
        connection.close()
    if status != 200:
        raise SystemExit(f"liveness_http_{status}")
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SystemExit("liveness_not_json")
    if not isinstance(body, dict) or body.get("ok") is not True:
        raise SystemExit("liveness_not_ok")
    # Echo only the allowlisted liveness field. Extra keys, if a future daemon
    # added them, stay off stdout so this reader cannot widen the leak-gate.
    return {"ok": True, "port": port}


def run_status_cli(plimsoll_bin: str) -> dict[str, object]:
    binary = plimsoll_bin.strip() or shutil.which("plimsoll")
    if not binary:
        raise SystemExit("status_cli_not_found")
    try:
        completed = subprocess.run(
            [binary, "status"],
            check=False,
            capture_output=True,
            text=True,
            timeout=STATUS_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit("status_cli_timeout")
    except OSError as error:
        raise SystemExit(f"status_cli_exec:{type(error).__name__}")
    if completed.returncode != 0:
        raise SystemExit(f"status_cli_exit_{completed.returncode}")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        raise SystemExit("status_cli_not_json")
    if not isinstance(payload, dict):
        raise SystemExit("status_cli_not_object")
    return payload


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.self_test:
        return run_self_test()
    if args.port < 1024 or args.port > 65535:
        print("invalid_port", file=sys.stderr)
        return 2
    liveness = read_liveness(args.port)
    if args.liveness_only:
        json.dump(liveness, sys.stdout, separators=(",", ":"))
        sys.stdout.write("\n")
        return 0
    status = run_status_cli(args.plimsoll_bin)
    json.dump({"liveness": liveness, "status": status}, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
