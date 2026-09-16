#!/usr/bin/env python3
"""Fleet liveness/status reader after eco-6hoxj.154.

Unauthenticated HTTP is GET /healthz only ({ok: true}). Full collector status
is the credentialed CLI (`plimsoll status`), never a raw GET of the management
route. This file must not open that management route and must not print tokens.
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import shutil
import subprocess
import sys

DEFAULT_PORT = 48271
HEALTHZ_PATH = "/healthz"
LIVENESS_TIMEOUT_S = 3
STATUS_TIMEOUT_S = 15


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
    return parser.parse_args(argv)


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
