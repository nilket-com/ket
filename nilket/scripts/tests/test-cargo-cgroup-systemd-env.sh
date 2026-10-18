#!/usr/bin/env bash
# Run: bash nilket/scripts/tests/test-cargo-cgroup-systemd-env.sh
# No root or real systemd is needed.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
wrapper="$script_dir/../cargo-cgroup"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT

mkdir -p "$tmp/bin" "$tmp/home" "$tmp/cargo home" "$tmp/rustup home" "$tmp/target dir" "$tmp/tmp dir"
cat > "$tmp/bin/systemctl" <<'SH'
#!/usr/bin/env bash
[[ "$*" == "--user show-environment" ]]
SH
cat > "$tmp/bin/systemd-run" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\0' "$@" > "$SYSTEMD_ARGS_FILE"
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
exec /usr/bin/env -i PATH=/usr/bin:/bin "${scope_env[@]}" "$@"
SH
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
env -i \
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
	CARGO_CGROUP_RUST_TEST_THREADS=1 \
	SYSTEMD_ARGS_FILE="$tmp/systemd-args.bin" \
	"$wrapper" --version

probe="$tmp/cargo home/probe.txt"
[[ -f "$probe" ]]
grep -Fx "TORON_ADR628_DIR=$scope_value" "$probe" >/dev/null
grep -Fx 'TORON_X=toron value=two' "$probe" >/dev/null
grep -Fx 'WENDELON_Y=wendelon value=three' "$probe" >/dev/null
grep -Fx 'FOO=<unset>' "$probe" >/dev/null
grep -Fx 'RUST_TEST_THREADS=1' "$probe" >/dev/null
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
has_arg --property=MemoryHigh=12G
has_arg --property=MemoryMax=16G
has_arg --property=TasksMax=512
has_arg --property=IOWeight=10
has_arg --user
has_arg --same-dir
has_arg --wait
has_arg --collect
has_arg --quiet
has_arg --pipe

command_tail=(taskset -c 0-11 ionice -c 3 env CARGO_BUILD_JOBS=6 RUST_TEST_THREADS=1 RUST_MIN_STACK=67108864 CARGO_TERM_PROGRESS_WHEN=never cargo --version)
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

printf 'PASS: systemd environment allowlist, prefix forwarding, exclusions, values, and resource argv\n'
