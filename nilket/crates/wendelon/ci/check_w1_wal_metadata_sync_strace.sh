#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v strace >/dev/null 2>&1; then
	echo "strace is required for the W1 metadata-sync check." >&2
	exit 1
fi

exe="$(
	cargo test --lib blocking_file_ops_metadata_sync_trace_fixture --no-run --message-format=json \
		| sed -n 's/.*"executable":"\([^"]*\)".*/\1/p' \
		| tail -n 1
)"

if [[ -z "${exe}" || ! -x "${exe}" ]]; then
	echo "failed to locate the metadata-sync trace fixture test binary" >&2
	exit 1
fi

trace_dir="$(mktemp -d)"
trap 'rm -rf "$trace_dir"' EXIT

strace \
	-f \
	-o "$trace_dir/trace" \
	-e trace=fsync,fdatasync,rename,renameat,renameat2,ftruncate \
	"$exe" \
	consensus::raft_storage::recovery_tests::blocking_file_ops_metadata_sync_trace_fixture \
	--exact \
	--nocapture \
	--test-threads=1

rename_count="$(grep -hEc 'rename(at|at2)?\(' "$trace_dir"/trace* || true)"
fdatasync_count="$(grep -hEc 'fdatasync\(' "$trace_dir"/trace* || true)"
fsync_count="$(grep -hEc 'fsync\(' "$trace_dir"/trace* || true)"
ftruncate_count="$(grep -hEc 'ftruncate\(' "$trace_dir"/trace* || true)"

if (( rename_count < 2 )); then
	echo "expected at least 2 rename syscalls for WAL segment create/rotation, saw $rename_count" >&2
	exit 1
fi
if (( fdatasync_count < 2 )); then
	echo "expected at least 2 fdatasync syscalls for WAL durability barriers, saw $fdatasync_count" >&2
	exit 1
fi
if (( fsync_count < 3 )); then
	echo "expected at least 3 fsync syscalls for directory/truncation metadata syncs, saw $fsync_count" >&2
	exit 1
fi
if (( ftruncate_count < 1 )); then
	echo "expected at least 1 ftruncate syscall for WAL truncation, saw $ftruncate_count" >&2
	exit 1
fi

echo "W1 strace metadata-sync check passed: rename=$rename_count fdatasync=$fdatasync_count fsync=$fsync_count ftruncate=$ftruncate_count"
