#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run() {
	printf '\n==> %s\n' "$*"
	"$@"
}

run cargo fmt --check
run packaging/ubuntu/test-nilket-local-user-service.sh
run scripts/cargo-ctrl --locked
run env BARYON_SVP_BACKEND=blocking scripts/cargo-ctrl --locked
run cargo test --locked --test architecture_test
run cargo check --locked -p polariton --no-default-features
run cargo test --locked -p polariton --no-default-features --bin polariton
