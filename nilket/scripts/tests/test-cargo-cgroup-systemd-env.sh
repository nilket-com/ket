#!/usr/bin/env bash
# Run: bash nilket/scripts/tests/test-cargo-cgroup-systemd-env.sh
# No root or real systemd is needed.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
wrapper="$script_dir/../cargo-cgroup"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT

mkdir -p "$tmp/bin" "$tmp/home" "$tmp/cargo home" "$tmp/rustup home" "$tmp/target dir" "$tmp/tmp dir"
# Fake systemd keeps its state in $tmp/state: the slice MemoryMax and the
# active cargo-cgroup units, so the never-lower rule can be exercised.
mkdir -p "$tmp/state"
: > "$tmp/state/active"
cat > "$tmp/bin/systemctl" <<'SH'
#!/usr/bin/env bash
state=__STATE__
all="$*"
log() { printf '%s\n' "$*" >> "${SYSTEMCTL_LOG:-/dev/null}"; }
case "$*" in
	"--user show-environment") exit 0 ;;
	"--user show -p DefaultLimitNOFILE --value") printf '%s\n' "${FAKE_DEFAULT_NOFILE:-524288}" ;;
	"--user show -p MemoryMax --value cargo-cgroup.slice") cat "$state/slice_max" 2>/dev/null || echo infinity ;;
	"--user list-units --plain --no-legend --state=active,activating,deactivating cargo-cgroup-*.service")
		[[ -z "${FAKE_LIST_FAIL:-}" ]] || exit 1
		sed 's/$/ loaded active running/' "$state/active" ;;
	"--user show -p Result --value "*) printf '%s\n' "${FAKE_RESULT:-success}" ;;
	"--user reset-failed "*) log "$*" ;;
	"--user show -p ActiveState --value "*)
		grep -qx -- "${all##* }" "$state/active" && echo active || echo inactive ;;
	"--user set-property --runtime cargo-cgroup.slice MemoryMax="*)
		log "$*"; printf '%s\n' "${all##*=}" > "$state/slice_max" ;;
	"--user kill "*) log "$*" ;;
	"--user stop "*) log "$*"; grep -vx -- "${all##* }" "$state/active" > "$state/active.new" || true; mv "$state/active.new" "$state/active" ;;
	*) exit 1 ;;
esac
SH
cat > "$tmp/bin/systemd-run" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=__STATE__
printf '%s\0' "$@" > "$SYSTEMD_ARGS_FILE"
unit=""
for arg in "$@"; do [[ "$arg" != --unit=* ]] || unit=${arg#--unit=}; done
printf '%s\n' "$unit" >> "$state/active"
deregister() { grep -vx -- "$unit" "$state/active" > "$state/active.$$" || true; mv "$state/active.$$" "$state/active"; }
printf '%s\n' "$$" > "$state/client.pid"
if [[ -n "${FAKE_SLEEP:-}" ]]; then
	sleep "$FAKE_SLEEP" &
	sleeper=$!
	printf '%s\n' "$sleeper" > "$state/sleep.pid"
	trap 'kill "$sleeper" 2>/dev/null; deregister; exit 143' TERM
	wait "$sleeper" || true
	deregister
	exit 0
fi
scope_env=()
while (($#)); do
	case "$1" in
		--setenv=*)
			name=${1#--setenv=}
			[[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]
			[[ -v "$name" ]]
			scope_env+=("$name=${!name}")
			shift
			;;
		--*) shift ;;
		*) break ;;
	esac
done
rc=0
/usr/bin/env -i PATH=/usr/bin:/bin "${scope_env[@]}" "$@" || rc=$?
deregister
exit "$rc"
SH
sed -i "s|__STATE__|'$tmp/state'|" "$tmp/bin/systemctl" "$tmp/bin/systemd-run"
cat > "$tmp/bin/cargo" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
{
	printf 'TORON_ADR628_DIR=%s\n' "${TORON_ADR628_DIR-<unset>}"
	printf 'TORON_X=%s\n' "${TORON_X-<unset>}"
	printf 'WENDELON_Y=%s\n' "${WENDELON_Y-<unset>}"
	printf 'FOO=%s\n' "${FOO-<unset>}"
	printf 'RUST_TEST_THREADS=%s\n' "${RUST_TEST_THREADS-<unset>}"
	printf 'CARGO_BUILD_JOBS=%s\n' "${CARGO_BUILD_JOBS-<unset>}"
	printf 'RUST_MIN_STACK=%s\n' "${RUST_MIN_STACK-<unset>}"
	printf 'CARGO_TERM_PROGRESS_WHEN=%s\n' "${CARGO_TERM_PROGRESS_WHEN-<unset>}"
} > "$CARGO_HOME/probe.txt"
printf 'fake cargo %s\n' "$*"
SH
cat > "$tmp/bin/taskset" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == -c ]]
shift 2
exec "$@"
SH
cat > "$tmp/bin/ionice" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == -c && "$2" == 3 ]]
shift 2
exec "$@"
SH
chmod 0755 "$tmp/bin/systemctl" "$tmp/bin/systemd-run" "$tmp/bin/cargo" "$tmp/bin/taskset" "$tmp/bin/ionice"

scope_value="$tmp/evidence path=value with spaces"
env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" \
	PATH="$tmp/bin:/usr/bin:/bin" \
	HOME="$tmp/home" \
	CARGO_HOME="$tmp/cargo home" \
	RUSTUP_TOOLCHAIN=1.95.0 \
	CARGO_TARGET_DIR="$tmp/target dir" \
	CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu \
	CARGO_ENCODED_RUSTFLAGS='-C\x1fopt-level=1' \
	RUSTFLAGS='-C opt-level=1' \
	CARGO_NET_OFFLINE=true \
	RUSTC=/tmp/fake-rustc \
	RUSTDOC=/tmp/fake-rustdoc \
	RUSTC_WRAPPER=/tmp/fake-rustc-wrapper \
	RUSTC_WORKSPACE_WRAPPER=/tmp/fake-workspace-wrapper \
	TMPDIR="$tmp/tmp dir" \
	RUST_LOG=trace \
	RUST_BACKTRACE=1 \
	TORON_ADR628_DIR="$scope_value" \
	TORON_X='toron value=two' \
	WENDELON_Y='wendelon value=three' \
	FOO=unrelated \
	RUST_TEST_THREADS=8 \
	CARGO_CGROUP_BACKEND=systemd \
	CARGO_CGROUP_ALLOW_AFFINITY_ONLY=0 \
	CARGO_CGROUP_JOBS=6 \
	CARGO_CGROUP_RUST_TEST_THREADS=7 \
	CARGO_CGROUP_MEMTOTAL_KB=67108864 \
	FAKE_DEFAULT_NOFILE=4096 \
	SYSTEMCTL_LOG="$tmp/systemctl.log" \
	SYSTEMD_ARGS_FILE="$tmp/systemd-args.bin" \
	"$wrapper" --version

probe="$tmp/cargo home/probe.txt"
[[ -f "$probe" ]]
grep -Fx "TORON_ADR628_DIR=$scope_value" "$probe" >/dev/null
grep -Fx 'TORON_X=toron value=two' "$probe" >/dev/null
grep -Fx 'WENDELON_Y=wendelon value=three' "$probe" >/dev/null
grep -Fx 'FOO=<unset>' "$probe" >/dev/null
grep -Fx 'RUST_TEST_THREADS=7' "$probe" >/dev/null
grep -Fx 'CARGO_BUILD_JOBS=6' "$probe" >/dev/null
grep -Fx 'RUST_MIN_STACK=67108864' "$probe" >/dev/null
grep -Fx 'CARGO_TERM_PROGRESS_WHEN=never' "$probe" >/dev/null

mapfile -d '' -t args < "$tmp/systemd-args.bin"
has_arg() {
	local expected=$1 arg
	for arg in "${args[@]}"; do [[ "$arg" == "$expected" ]] && return 0; done
	return 1
}
has_arg --setenv=TORON_ADR628_DIR
has_arg --setenv=TORON_X
has_arg --setenv=WENDELON_Y
for name in PATH HOME CARGO_HOME RUSTUP_TOOLCHAIN CARGO_TARGET_DIR CARGO_BUILD_TARGET CARGO_ENCODED_RUSTFLAGS RUSTFLAGS CARGO_NET_OFFLINE RUSTC RUSTDOC RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER TMPDIR RUST_LOG RUST_BACKTRACE; do
	has_arg "--setenv=$name"
done
for name in FOO RUST_TEST_THREADS CARGO_BUILD_JOBS RUST_MIN_STACK CARGO_TERM_PROGRESS_WHEN CARGO_CGROUP_BACKEND CARGO_CGROUP_ALLOW_AFFINITY_ONLY CARGO_CGROUP_JOBS CARGO_CGROUP_RUST_TEST_THREADS RUSTUP_HOME; do
	! has_arg "--setenv=$name"
done
has_arg --nice=19
has_arg --property=CPUQuota=1200%
# 64 GiB host: slice = min(96G, 60% of MemTotal); invocation = min(32G, slice).
slice_bytes=$((67108864 * 1024 / 100 * 60))
has_arg --property=MemoryHigh=$((28 << 30))
has_arg --property=MemoryMax=$((32 << 30))
has_arg --property=TasksMax=16384
has_arg --property=IOWeight=10
# nofile = min(caller hard limit, user manager DefaultLimitNOFILE=4096).
has_arg "--property=LimitNOFILE=$(( $(ulimit -Hn) < 4096 ? $(ulimit -Hn) : 4096 ))"
has_arg --slice=cargo-cgroup.slice
has_arg --property=RuntimeMaxSec=86400
grep -Fx -- "--user set-property --runtime cargo-cgroup.slice MemoryMax=${slice_bytes}" "$tmp/systemctl.log" >/dev/null
unit_arg=$(printf '%s\n' "${args[@]}" | grep -E '^--unit=cargo-cgroup-[0-9]+-[0-9]+\.service$')
[[ -n "$unit_arg" ]]
has_arg --user
has_arg --same-dir
has_arg --wait
! has_arg --collect
has_arg --quiet
has_arg --pipe

command_tail=(taskset -c 0-11 ionice -c 3 env CARGO_BUILD_JOBS=6 RUST_TEST_THREADS=7 RUST_MIN_STACK=67108864 CARGO_TERM_PROGRESS_WHEN=never cargo --version)
matched=0
for ((start = 0; start + ${#command_tail[@]} <= ${#args[@]}; start++)); do
	matched=1
	for ((offset = 0; offset < ${#command_tail[@]}; offset++)); do
		if [[ "${args[start + offset]}" != "${command_tail[offset]}" ]]; then
			matched=0
			break
		fi
	done
	if ((matched)); then break; fi
done
[[ "$matched" -eq 1 ]]

# Default test threads: one per CPU of the cpuset (ADR 0694), counting each CPU
# once. An ambient RUST_TEST_THREADS must not leak through.
default_case() {
	local cpus=$1 want=$2
	rm -f "$tmp/cargo home/probe.txt"
	env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" \
		PATH="$tmp/bin:/usr/bin:/bin" \
		HOME="$tmp/home" \
		CARGO_HOME="$tmp/cargo home" \
		RUST_TEST_THREADS=99 \
		CARGO_CGROUP_BACKEND=systemd \
		${cpus:+CARGO_CGROUP_CPUS="$cpus"} \
		SYSTEMD_ARGS_FILE="$tmp/systemd-args-default.bin" \
		"$wrapper" --version >/dev/null
	local threads
	threads=$(sed -n 's/^RUST_TEST_THREADS=//p' "$tmp/cargo home/probe.txt")
	[[ "$threads" == "$want" ]] || {
		echo "FAIL: cpuset '${cpus}' defaulted to ${threads} test threads, want ${want}" >&2
		exit 1
	}
}
default_case '' 12
default_case 0-7,16-23 16
default_case 3 1
default_case 0-3,2-5 6
# Negative control: a cpuset the wrapper cannot count is refused, not defaulted.
for bad in x 5-2 0,,1; do
	if env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_HOME="$tmp/cargo home" \
		CARGO_CGROUP_BACKEND=systemd CARGO_CGROUP_CPUS="$bad" \
		SYSTEMD_ARGS_FILE="$tmp/systemd-args-bad.bin" "$wrapper" --version >/dev/null 2>&1; then
		echo "FAIL: uncountable CARGO_CGROUP_CPUS=$bad was accepted" >&2
		exit 1
	fi
done
# Negative control: a non-numeric override is refused, not passed through.
if env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_HOME="$tmp/cargo home" \
	CARGO_CGROUP_BACKEND=systemd CARGO_CGROUP_RUST_TEST_THREADS=all \
	SYSTEMD_ARGS_FILE="$tmp/systemd-args-bad.bin" "$wrapper" --version >/dev/null 2>&1; then
	echo "FAIL: non-numeric CARGO_CGROUP_RUST_TEST_THREADS was accepted" >&2
	exit 1
fi

# Host-aware memory: on a 32 GiB host the slice (60%) caps the invocation too.
memory_case() {
	local memtotal_kb=$1 want_slice=$2 want_max=$3 want_high=$4
	shift 4
	: > "$tmp/systemctl-mem.log"
	env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_HOME="$tmp/cargo home" \
		CARGO_CGROUP_BACKEND=systemd CARGO_CGROUP_MEMTOTAL_KB="$memtotal_kb" "$@" \
		SYSTEMCTL_LOG="$tmp/systemctl-mem.log" SYSTEMD_ARGS_FILE="$tmp/systemd-args-mem.bin" \
		"$wrapper" --version >/dev/null
	mapfile -d '' -t args < "$tmp/systemd-args-mem.bin"
	grep -Fx -- "--user set-property --runtime cargo-cgroup.slice MemoryMax=${want_slice}" "$tmp/systemctl-mem.log" >/dev/null
	has_arg "--property=MemoryMax=${want_max}"
	has_arg "--property=MemoryHigh=${want_high}"
}
small=$((33554432 * 1024 / 100 * 60))
memory_case 33554432 "$small" "$small" "$small"
# bigfoot ladder: 80% of 123 GiB clears 96G, so three 32G lanes fit the slice.
memory_case 128974848 $((96 << 30)) $((32 << 30)) $((28 << 30)) CARGO_CGROUP_SLICE_MEMORY_PERCENT=80
# Negative control: a malformed size is refused.
if env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_CGROUP_BACKEND=systemd \
	CARGO_CGROUP_MEMORY_MAX=lots SYSTEMD_ARGS_FILE="$tmp/x.bin" "$wrapper" --version >/dev/null 2>&1; then
	echo "FAIL: malformed CARGO_CGROUP_MEMORY_MAX was accepted" >&2
	exit 1
fi

# A signal (timeout(1) sends TERM) stops the unit: a killed systemd-run client
# would otherwise leave its unit running.
: > "$tmp/systemctl-sig.log"
env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_HOME="$tmp/cargo home" \
	CARGO_CGROUP_BACKEND=systemd FAKE_SLEEP=30 SYSTEMCTL_LOG="$tmp/systemctl-sig.log" \
	SYSTEMD_ARGS_FILE="$tmp/systemd-args-sig.bin" "$wrapper" --version &
wrapper_pid=$!
for _ in $(seq 50); do [[ -s "$tmp/systemd-args-sig.bin" ]] && break; sleep 0.1; done
kill -TERM "$wrapper_pid"
sig_rc=0
wait "$wrapper_pid" || sig_rc=$?
[[ "$sig_rc" -eq 143 ]]
grep -Eq -- '^--user kill --signal=SIGKILL cargo-cgroup-[0-9]+-[0-9]+\.service$' "$tmp/systemctl-sig.log"
grep -Eq -- '^--user stop cargo-cgroup-[0-9]+-[0-9]+\.service$' "$tmp/systemctl-sig.log"
grep -Eq -- '^--user reset-failed cargo-cgroup-[0-9]+-[0-9]+\.service$' "$tmp/systemctl-sig.log"
# The systemd-run client and its child are gone once the wrapper returns.
! kill -0 "$(cat "$tmp/state/client.pid")" 2>/dev/null
! kill -0 "$(cat "$tmp/state/sleep.pid")" 2>/dev/null

# The slice limit is never lowered under a running session.
session() {
	local memtotal_kb=$1 log=$2
	shift 2
	env -i CARGO_CGROUP_RUNTIME_DIR="$tmp" PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_HOME="$tmp/cargo home" \
		CARGO_CGROUP_BACKEND=systemd CARGO_CGROUP_MEMTOTAL_KB="$memtotal_kb" "$@" \
		SYSTEMCTL_LOG="$log" SYSTEMD_ARGS_FILE="$tmp/systemd-args-session.bin" \
		"$wrapper" --version >/dev/null
}
# The running wrapper's PID is in its unit name (cargo-cgroup-<pid>-<n>);
# signal the wrapper itself, so it stops its unit and nothing outlives it.
stop_running_session() {
	local pid
	pid=$(sed -nE '1s/^cargo-cgroup-([0-9]+)-.*/\1/p' "$tmp/state/active")
	[[ -n "$pid" ]] && kill -TERM "$pid"
	wait "$1" || true
	! kill -0 "$(cat "$tmp/state/sleep.pid")" 2>/dev/null
}
wait_active() { for _ in $(seq 50); do [[ -s "$tmp/state/active" ]] && return 0; sleep 0.1; done; return 1; }
big_kb=128974848 small_kb=33554432
big=$((96 << 30)) small=$((small_kb * 1024 / 100 * 60))
# A big session runs; a small session starting meanwhile leaves the limit alone.
: > "$tmp/state/active"; : > "$tmp/n1-big.log"; : > "$tmp/n1-small.log"
session "$big_kb" "$tmp/n1-big.log" CARGO_CGROUP_SLICE_MEMORY_PERCENT=80 FAKE_SLEEP=30 &
big_pid=$!
wait_active
[[ "$(cat "$tmp/state/slice_max")" == "$big" ]]
session "$small_kb" "$tmp/n1-small.log"
[[ "$(cat "$tmp/state/slice_max")" == "$big" ]]
! grep -q set-property "$tmp/n1-small.log"
stop_running_session "$big_pid"
# A small session runs; a big session starting meanwhile raises the limit.
: > "$tmp/state/active"; : > "$tmp/n1-raise.log"
session "$small_kb" /dev/null FAKE_SLEEP=30 &
small_pid=$!
wait_active
[[ "$(cat "$tmp/state/slice_max")" == "$small" ]]
session "$big_kb" "$tmp/n1-raise.log" CARGO_CGROUP_SLICE_MEMORY_PERCENT=80
[[ "$(cat "$tmp/state/slice_max")" == "$big" ]]
grep -Fxq -- "--user set-property --runtime cargo-cgroup.slice MemoryMax=${big}" "$tmp/n1-raise.log"
stop_running_session "$small_pid"
# With nothing running, a session may set a lower limit.
: > "$tmp/state/active"
session "$small_kb" /dev/null
[[ "$(cat "$tmp/state/slice_max")" == "$small" ]]
# A failed unit listing counts as active: the limit is not lowered.
: > "$tmp/state/active"; : > "$tmp/n1-listfail.log"
printf '%s\n' "$big" > "$tmp/state/slice_max"
session "$small_kb" "$tmp/n1-listfail.log" FAKE_LIST_FAIL=1
[[ "$(cat "$tmp/state/slice_max")" == "$big" ]]
! grep -q set-property "$tmp/n1-listfail.log"
# An unbounded slice while units run is refused, not run under.
printf 'cargo-cgroup-1-1.service\n' > "$tmp/state/active"
printf 'infinity\n' > "$tmp/state/slice_max"
inf_rc=0
session "$small_kb" /dev/null 2>"$tmp/n1-inf.err" || inf_rc=$?
[[ "$inf_rc" -eq 70 ]]
grep -q 'refusing to start unbounded' "$tmp/n1-inf.err"
: > "$tmp/state/active"
# No user runtime directory, no lock: refused.
nodir_rc=0
env -i PATH="$tmp/bin:/usr/bin:/bin" HOME="$tmp/home" CARGO_CGROUP_BACKEND=systemd \
	CARGO_CGROUP_RUNTIME_DIR="$tmp/absent" SYSTEMD_ARGS_FILE="$tmp/x.bin" "$wrapper" --version \
	>/dev/null 2>&1 || nodir_rc=$?
[[ "$nodir_rc" -eq 70 ]]
printf 'PASS: the slice limit is never lowered under a running session\n'

# A unit stopped by RuntimeMaxSec is reported on stderr with rc 124.
rt_rc=0
session "$small_kb" /dev/null FAKE_RESULT=timeout 2>"$tmp/rt.err" || rt_rc=$?
[[ "$rt_rc" -eq 124 ]]
grep -q 'was stopped by RuntimeMaxSec=86400s' "$tmp/rt.err"
printf 'PASS: a RuntimeMaxSec stop is reported with rc 124\n'

printf 'PASS: host-aware slice and invocation memory, nofile minimum, signal stops the unit\n'
printf 'PASS: default test threads follow the cpuset, and the override and cpuset are validated\n'
printf 'PASS: systemd environment allowlist, prefix forwarding, exclusions, values, and resource argv\n'
