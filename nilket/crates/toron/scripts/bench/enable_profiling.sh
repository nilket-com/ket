#!/usr/bin/env bash
#
# enable_profiling.sh — bootstrap profiler permissions on a dev machine
# so `samply` / `perf` / `gdb` attach can capture flamegraphs during the
# bench harness in run_runtime_filter_perft.sh. Closes the explicit
# blocker recorded in ADR 005 and ADR 006 bench artifacts.
#
# Linux-only. Defaults target values that match common dev-machine perf
# profiling needs:
#   /proc/sys/kernel/perf_event_paranoid -> 1  (allow user-space events)
#   /proc/sys/kernel/yama/ptrace_scope   -> 0  (allow same-user ptrace)
#
# Modes:
#   --apply (default): record prior state, apply target values via sysctl.
#   --revert: restore prior state from the recorded state file.
#   --verify: print current state and run a samply smoke probe.
#
# State is recorded under TORON_PERFT_OUT_DIR (default:
# target/toron-perft/) as profiling-state.txt so revert is mechanical.
#
# These settings are NOT for production. They reduce security boundaries
# to enable developer profiling. Use --revert (or reboot, the changes
# don't persist without --persist) when finished.

set -euo pipefail

PERF_PARANOID_PATH="/proc/sys/kernel/perf_event_paranoid"
PTRACE_SCOPE_PATH="/proc/sys/kernel/yama/ptrace_scope"

TARGET_PERF_PARANOID="${TORON_PERFT_PERF_PARANOID:-1}"
TARGET_PTRACE_SCOPE="${TORON_PERFT_PTRACE_SCOPE:-0}"

OUT_DIR="${TORON_PERFT_OUT_DIR:-target/toron-perft}"
STATE_FILE="${OUT_DIR}/profiling-state.txt"

MODE="apply"

usage() {
	cat <<'USAGE'
Usage:
  enable_profiling.sh [--apply | --revert | --verify]
                      [--out-dir <path>]

Modes:
  --apply   (default) Record current values, apply dev profiling targets.
  --revert  Restore prior values from the recorded state file.
  --verify  Print current values and run a samply smoke probe.

Environment:
  TORON_PERFT_OUT_DIR        Base dir for state file (default: target/toron-perft).
  TORON_PERFT_PERF_PARANOID  Target perf_event_paranoid value (default: 1).
  TORON_PERFT_PTRACE_SCOPE   Target ptrace_scope value (default: 0).

Notes:
  - Requires sudo for sysctl writes when current uid is not root.
  - Changes are runtime-only and do not survive reboot.
  - Treats already-at-target as a no-op (idempotent).
USAGE
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--apply) MODE="apply"; shift ;;
		--revert) MODE="revert"; shift ;;
		--verify) MODE="verify"; shift ;;
		--out-dir) OUT_DIR="$2"; STATE_FILE="${OUT_DIR}/profiling-state.txt"; shift 2 ;;
		-h|--help) usage; exit 0 ;;
		*) echo "unknown argument: $1" >&2; usage; exit 2 ;;
	esac
done

require_linux() {
	if [[ "$(uname -s)" != "Linux" ]]; then
		echo "enable_profiling.sh: Linux only (detected $(uname -s))." >&2
		echo "  perf_event_paranoid and yama.ptrace_scope are Linux kernel knobs." >&2
		exit 1
	fi
}

read_sysctl() {
	# read_sysctl <path>
	if [[ -r "$1" ]]; then
		cat "$1"
	else
		echo "missing"
	fi
}

write_sysctl() {
	# write_sysctl <path> <value>
	local path="$1" value="$2"
	if [[ "$(id -u)" -eq 0 ]]; then
		echo "$value" > "$path"
	else
		echo "$value" | sudo tee "$path" > /dev/null
	fi
}

show_current() {
	local perf ptrace
	perf="$(read_sysctl "$PERF_PARANOID_PATH")"
	ptrace="$(read_sysctl "$PTRACE_SCOPE_PATH")"
	echo "  perf_event_paranoid = $perf"
	echo "  yama.ptrace_scope   = $ptrace"
}

verify_samply() {
	if ! command -v samply >/dev/null 2>&1; then
		echo "samply not installed (cargo install samply); skipping probe."
		return 1
	fi
	local probe_out
	probe_out="$(mktemp -t toron-perft-samply-probe.XXXXXX.json.gz)"
	if samply record -s -o "$probe_out" true 2>/dev/null; then
		echo "samply probe: OK ($probe_out)"
		rm -f "$probe_out"
		return 0
	else
		echo "samply probe: FAILED (kernel still gating; check current values above)"
		rm -f "$probe_out" 2>/dev/null || true
		return 1
	fi
}

mode_verify() {
	require_linux
	echo "Current profiling-permission state:"
	show_current
	echo
	echo "Target values for dev profiling:"
	echo "  perf_event_paranoid = $TARGET_PERF_PARANOID"
	echo "  yama.ptrace_scope   = $TARGET_PTRACE_SCOPE"
	echo
	echo "samply smoke probe:"
	verify_samply || true
}

mode_apply() {
	require_linux

	local current_perf current_ptrace
	current_perf="$(read_sysctl "$PERF_PARANOID_PATH")"
	current_ptrace="$(read_sysctl "$PTRACE_SCOPE_PATH")"

	echo "Current:"
	show_current
	echo "Target:"
	echo "  perf_event_paranoid = $TARGET_PERF_PARANOID"
	echo "  yama.ptrace_scope   = $TARGET_PTRACE_SCOPE"

	if [[ "$current_perf" == "$TARGET_PERF_PARANOID" ]] \
		&& [[ "$current_ptrace" == "$TARGET_PTRACE_SCOPE" ]]; then
		echo
		echo "Already at target values; no changes applied."
		echo "Running samply smoke probe to confirm:"
		verify_samply || true
		return 0
	fi

	mkdir -p "$OUT_DIR"

	if [[ ! -f "$STATE_FILE" ]]; then
		cat > "$STATE_FILE" <<EOF
# Recorded by enable_profiling.sh --apply on $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Restore via: enable_profiling.sh --revert
perf_event_paranoid_prior=$current_perf
ptrace_scope_prior=$current_ptrace
EOF
		echo
		echo "Recorded prior state at $STATE_FILE"
	else
		echo
		echo "State file already present at $STATE_FILE; not overwriting."
		echo "  (Use --revert first if you want to re-record from a fresh baseline.)"
	fi

	echo "Applying via sysctl (sudo may prompt)..."
	if [[ "$current_perf" != "$TARGET_PERF_PARANOID" ]]; then
		write_sysctl "$PERF_PARANOID_PATH" "$TARGET_PERF_PARANOID"
	fi
	if [[ "$current_ptrace" != "$TARGET_PTRACE_SCOPE" ]]; then
		write_sysctl "$PTRACE_SCOPE_PATH" "$TARGET_PTRACE_SCOPE"
	fi

	echo
	echo "Applied. New state:"
	show_current
	echo
	echo "samply smoke probe:"
	verify_samply || true
}

mode_revert() {
	require_linux

	if [[ ! -f "$STATE_FILE" ]]; then
		echo "No state file at $STATE_FILE; nothing to revert." >&2
		exit 1
	fi

	# shellcheck disable=SC1090
	source "$STATE_FILE"

	echo "Reverting using state from $STATE_FILE:"
	echo "  perf_event_paranoid -> ${perf_event_paranoid_prior:-unknown}"
	echo "  yama.ptrace_scope   -> ${ptrace_scope_prior:-unknown}"

	if [[ -n "${perf_event_paranoid_prior:-}" ]] && [[ "$perf_event_paranoid_prior" != "missing" ]]; then
		write_sysctl "$PERF_PARANOID_PATH" "$perf_event_paranoid_prior"
	fi
	if [[ -n "${ptrace_scope_prior:-}" ]] && [[ "$ptrace_scope_prior" != "missing" ]]; then
		write_sysctl "$PTRACE_SCOPE_PATH" "$ptrace_scope_prior"
	fi

	rm -f "$STATE_FILE"

	echo
	echo "Reverted. Current state:"
	show_current
}

case "$MODE" in
	apply) mode_apply ;;
	revert) mode_revert ;;
	verify) mode_verify ;;
esac
