#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/baryon-huge.XXXXXX")"
FIXTURE="$FIXTURE_DIR/sparse-200-gib.bin"
LOGICAL_BYTES=$((200 * 1024 * 1024 * 1024))
ALLOCATED_LIMIT=$((16 * 1024 * 1024))

cleanup() {
	rm -rf "$FIXTURE_DIR"
}
trap cleanup EXIT INT TERM

if [[ "${1:-}" == "--full-scan" && "${BARYON_CI_HUGE:-0}" != "1" ]]; then
	echo "Refusing full-scan variant without BARYON_CI_HUGE=1" >&2
	exit 2
fi

if [[ "$(uname -s)" != "Linux" ]]; then
	echo "huge sparse integration test skipped: Linux is required"
	exit 0
fi

truncate -s "$LOGICAL_BYTES" "$FIXTURE"
printf 'HEAD' | dd of="$FIXTURE" bs=1 seek=0 conv=notrunc status=none
printf 'PRE4' | dd of="$FIXTURE" bs=1 seek=$((4 * 1024 * 1024 * 1024 - 4)) conv=notrunc status=none
printf 'POST' | dd of="$FIXTURE" bs=1 seek=$((4 * 1024 * 1024 * 1024)) conv=notrunc status=none
printf 'MIDDLE' | dd of="$FIXTURE" bs=1 seek=$((100 * 1024 * 1024 * 1024)) conv=notrunc status=none
printf 'TAILMARK' | dd of="$FIXTURE" bs=1 seek=$((LOGICAL_BYTES - 8)) conv=notrunc status=none

logical_bytes="$(stat -c '%s' "$FIXTURE")"
allocated_bytes="$(( $(stat -c '%b' "$FIXTURE") * 512 ))"
echo "logical bytes:   $logical_bytes"
echo "allocated bytes: $allocated_bytes"

if [[ "$logical_bytes" -ne "$LOGICAL_BYTES" ]]; then
	echo "sparse fixture has the wrong logical length" >&2
	exit 1
fi
if [[ "$allocated_bytes" -gt "$ALLOCATED_LIMIT" ]]; then
	echo "filesystem did not preserve sparsity; refusing to launch Baryon tests" >&2
	exit 1
fi

cd "$ROOT_DIR"
export BARYON_SPARSE_FIXTURE="$FIXTURE"
timeout 120s cargo test --locked -p libaryon \
	sparse_200_gib -- --test-threads=1

allocated_after="$(( $(stat -c '%b' "$FIXTURE") * 512 ))"
echo "allocated bytes after test: $allocated_after"
if [[ "$allocated_after" -gt "$ALLOCATED_LIMIT" ]]; then
	echo "sparse test exceeded its allocation budget" >&2
	exit 1
fi
