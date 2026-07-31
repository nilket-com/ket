#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
	printf 'usage: %s {ket|nilket|cross|all|changed [BASE]}\n' "$0" >&2
	exit 2
}

run_ket() {
	if [[ "${PRODUCT_CI_DRY_RUN:-0}" == "1" ]]; then
		printf 'selected: ket\n'
		return
	fi
	"$ROOT_DIR/ci/check.sh"
}

run_nilket() {
	if [[ "${PRODUCT_CI_DRY_RUN:-0}" == "1" ]]; then
		printf 'selected: nilket\n'
		return
	fi
	"$ROOT_DIR/ci/nilket-check.sh"
}

run_cross() {
	if [[ "${PRODUCT_CI_DRY_RUN:-0}" == "1" ]]; then
		printf 'selected: cross\n'
		return
	fi
	"$ROOT_DIR/ci/cross-product-check.sh"
}

run_changed() {
	local base="${1:-origin/main}"
	local ket=0
	local nilket=0
	local cross=0
	local path
	local paths=()

	mapfile -t paths < <(git diff --name-only "$base"...HEAD)
	for path in "${paths[@]}"; do
		case "$path" in
			nilket/*)
				nilket=1
				;;
			ci/nilket-check.sh)
				nilket=1
				;;
			shared/* | plans/product/* | tests/support/manifest_dependency_guard.rs | ci/product-check.sh | ci/cross-product-check.sh)
				ket=1
				nilket=1
				cross=1
				;;
			*)
				ket=1
				;;
		esac
	done

	if ((ket && nilket)); then
		cross=1
	fi
	if ((!ket && !nilket)); then
		printf 'No changed product paths relative to %s.\n' "$base"
		return
	fi
	((ket)) && run_ket
	((nilket)) && run_nilket
	((cross)) && run_cross
}

case "${1:-all}" in
	ket)
		run_ket
		;;
	nilket)
		run_nilket
		;;
	cross)
		run_cross
		;;
	all)
		run_ket
		run_nilket
		run_cross
		;;
	changed)
		run_changed "${2:-origin/main}"
		;;
	*)
		usage
		;;
esac
