#!/usr/bin/env python3
"""Give a fixture join CLI an actual TTY without putting its token in argv."""

import os
import pty
import select
import subprocess
import sys
import time


def main():
    token = sys.stdin.buffer.readline().rstrip(b"\r\n")
    if not token or len(sys.argv) < 2:
        raise SystemExit(64)
    master, slave = pty.openpty()
    child = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave)
    os.close(slave)
    transcript = bytearray()
    sent = False
    deadline = time.monotonic() + 85
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.2)
            if ready:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                transcript.extend(chunk)
                if not sent and b"Join token (input hidden): " in transcript:
                    os.write(master, token + b"\r")
                    sent = True
            if child.poll() is not None and not ready:
                break
        if child.poll() is None:
            try:
                child.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                sys.stderr.write("join token prompt did not finish within 85 seconds\n")
                raise SystemExit(124)
        if not sent:
            sys.stderr.write("join token prompt was never shown\n")
            raise SystemExit(65)
        sys.stdout.buffer.write(transcript)
        raise SystemExit(child.returncode)
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)
        os.close(master)


if __name__ == "__main__":
    main()
