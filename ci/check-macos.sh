#!/usr/bin/env bash
set -euo pipefail

# Native macOS gate (ADR 0170). Linux cannot cross-check the desktop crates:
# `ring`, `aws-lc-sys`, `psm`, and `stacker` compile C for their target.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ "$(uname -s)" != "Darwin" ]]; then
	echo "ci/check-macos.sh runs on macOS; use ci/check.sh on Linux" >&2
	exit 2
fi

run() {
	printf '\n==> %s\n' "$*"
	"$@"
}

run cargo fmt --check
# Per-package runs include each crate's architecture test. A workspace-wide
# run would also build Nilket's Linux-only member crates.
run cargo test --locked -p libaryon -p baryon -p ket
# Polariton's default features: local Toron supervision compiles out here.
run cargo test --locked -p polariton
run cargo check --locked -p polariton --no-default-features
