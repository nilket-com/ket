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
run packaging/ubuntu/test-nilket-local-user-service.sh
run scripts/cargo-ctrl --locked
run env BARYON_SVP_BACKEND=blocking scripts/cargo-ctrl --locked
run cargo test --locked --test architecture_test
run cargo check --locked -p polariton --no-default-features
run cargo test --locked -p polariton --no-default-features --bin polariton

for target in "${TARGETS[@]}"; do
	run cargo check --locked --target "$target"
	run cargo check --locked -p polariton --no-default-features --target "$target"
done
