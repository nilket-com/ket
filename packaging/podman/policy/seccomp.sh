#!/usr/bin/env bash
# Derive Nilket's seccomp profile from the Linux host's Podman baseline.
set -euo pipefail

if (($# != 2)); then
	printf 'usage: %s BASELINE OUTPUT\n' "$0" >&2
	exit 2
fi
baseline=$1
output=$2

jq -e '
	.defaultAction != "SCMP_ACT_ALLOW" and
	(.syscalls | type == "array") and
	([.syscalls[].names[] | select(. == "io_uring_setup" or . == "io_uring_enter" or . == "io_uring_register")] | length == 0)
' "$baseline" >/dev/null || {
	printf 'seccomp: baseline is not an allowlist or already mentions io_uring; review it first\n' >&2
	exit 1
}

jq -S '.syscalls += [{"names": ["io_uring_enter", "io_uring_register", "io_uring_setup"], "action": "SCMP_ACT_ALLOW"}]' "$baseline" > "$output"
