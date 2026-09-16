#!/usr/bin/env python3
"""ADR 595: wait on the workstation for this attempt's completion datagram.

usage: adr595_wait_completion.py --port P --token T --deadline-unix-ms D [--bind ADDR]

Listens for UDP datagrams; a datagram whose record carries `token=T` ends the wait with exit 0
and the record on stdout. Datagrams with any other token (a stale marker, a probe, another
attempt) are logged to stderr and ignored. At the deadline the wait ends with exit 1 and
nothing on stdout: a missing signal never authorizes a read-out, it triggers the recovery
procedure, which is recorded separately.
"""
import argparse
import socket
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--deadline-unix-ms", type=float, required=True)
    parser.add_argument("--bind", default="0.0.0.0")
    args = parser.parse_args()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.bind, args.port))
    needle = f"token={args.token} "
    while True:
        remaining = args.deadline_unix_ms / 1000.0 - time.time()
        if remaining <= 0:
            print(f"deadline reached without a datagram for token {args.token}", file=sys.stderr)
            return 1
        sock.settimeout(min(remaining, 1.0))
        try:
            data, sender = sock.recvfrom(4096)
        except socket.timeout:
            continue
        record = data.decode("utf-8", "replace").strip()
        if needle in record + " ":
            print(f"{record} received_unix_ms={int(time.time() * 1000)} sender={sender[0]}")
            return 0
        print(f"ignored datagram from {sender[0]}: {record[:120]}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
