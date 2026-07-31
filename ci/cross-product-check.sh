#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run() {
	printf '\n==> product: %s\n' "$*"
	"$@"
}

printf '\n==> product: ket cargo metadata\n'
cargo metadata --no-deps --format-version 1 >/dev/null
printf '\n==> product: nilket cargo metadata\n'
cargo metadata --manifest-path nilket/Cargo.toml --no-deps --format-version 1 >/dev/null
run cargo test -p libaryon --test architecture_test adr_0005
run cargo test -p libaryon --test architecture_test product_0001
run cargo test -p baryon --test architecture_test adr_0005
run cargo test -p polariton --test architecture_test

# PRODUCT-0001 establishes this as the cross-product job. The resumed Toron
# dogfood pair extends it with the durable launcher/client/UI acceptance path;
# until that implementation exists, dependency and workspace contracts are the
# complete executable cross-product surface.
