#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGETS=(
	"x86_64-pc-windows-msvc"
	"x86_64-apple-darwin"
	"aarch64-apple-darwin"
)

ensure_targets() {
	if [[ "${BARYON_CI_SKIP_TARGET_INSTALL:-0}" == "1" ]]; then
		return
	fi

	for target in "${TARGETS[@]}"; do
		if ! rustup target list --installed | grep -Fxq "$target"; then
			rustup target add "$target"
		fi
	done
}

run() {
	printf '\n==> %s\n' "$*"
	"$@"
}

ensure_targets

run cargo fmt --check
run cargo test --locked --lib -- --test-threads=1
run env BARYON_SVP_BACKEND=blocking cargo test --locked --lib -- --test-threads=1
run cargo test --locked --test architecture_test

for target in "${TARGETS[@]}"; do
	run cargo check --locked --target "$target"
done
