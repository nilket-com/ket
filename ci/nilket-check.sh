#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ADR 0164: the lane carries its own environment. Nothing below may depend on
# the operator's shell profile, runner script, or Git configuration.

# The supervisor's shutdown tests drive /bin/sh fixtures with SIGINT. A
# non-interactive shell sets SIGINT and SIGQUIT to ignored for every `&` job,
# and an ignored-on-entry signal cannot be trapped by any child shell. bash
# cannot reset a disposition it inherited ignored, so re-execute through perl,
# which can, exactly once.
signal_ignored() {
	local mask
	mask="$(awk '/^SigIgn:/ { print $2 }' /proc/self/status 2>/dev/null || true)"
	[[ -n "$mask" ]] || return 1
	# SIGINT is signal 2 (bit 1), SIGQUIT is signal 3 (bit 2).
	(( (16#$mask & 0x6) != 0 ))
}
if signal_ignored; then
	if [[ "${NILKET_CHECK_SIGNALS_RESET:-0}" == "1" ]]; then
		echo "ci/nilket-check.sh: SIGINT/SIGQUIT are still ignored after reset; refusing" >&2
		exit 2
	fi
	command -v perl >/dev/null || {
		echo "ci/nilket-check.sh: SIGINT/SIGQUIT are ignored and perl is unavailable to reset them" >&2
		exit 2
	}
	export NILKET_CHECK_SIGNALS_RESET=1
	exec perl -e '$SIG{INT} = "DEFAULT"; $SIG{QUIT} = "DEFAULT"; exec @ARGV or die "exec: $!"' \
		-- bash "${BASH_SOURCE[0]}" "$@"
fi

# The debug test profile overflows the default thread stack in the binder and
# planner tests. This is the value nilket/scripts/cargo-cgroup uses.
export RUST_MIN_STACK="${RUST_MIN_STACK:-67108864}"

# Temporary-repository tests must not see the operator's Git configuration:
# signing, hooks, templates, and default-branch settings all change what a
# `git commit` in a fixture does. Identity is fixed so commits are possible
# on a machine with none configured.
GIT_ISOLATION_DIR="$(mktemp -d)"
trap 'rm -rf "$GIT_ISOLATION_DIR"' EXIT
: > "$GIT_ISOLATION_DIR/gitconfig"
export GIT_CONFIG_GLOBAL="$GIT_ISOLATION_DIR/gitconfig"
export GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME="nilket-check" GIT_AUTHOR_EMAIL="nilket-check@example.invalid"
export GIT_COMMITTER_NAME="nilket-check" GIT_COMMITTER_EMAIL="nilket-check@example.invalid"

cd "$ROOT_DIR/nilket"

run() {
	printf '\n==> nilket: %s\n' "$*"
	"$@"
}

run cargo fmt --check
run cargo check --workspace --all-targets
# Nilket contains concurrency-sensitive saturation fixtures whose contract is
# deterministic in isolation. Keep the product lane reproducible instead of
# making unrelated harnesses race for one process-global test environment.
RUST_TEST_THREADS=1 run cargo test --workspace --no-fail-fast
