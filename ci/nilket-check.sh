#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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
