#!/usr/bin/env bash
# Reproducible dependency census for the licensing inventory.
# Writes, next to this script:
#   <workspace>_packages.jsonl   every package cargo metadata resolves for the
#                                workspace lockfile: name, version, source,
#                                declared license, license file
#   <product>_<target>.txt       the normal (non-dev, non-build) dependency
#                                closure of one binary for one target, one row
#                                per package-and-feature-set, as
#                                `name version (source) | license | features`;
#                                a package compiled with two feature sets has
#                                two rows, so rows exceed distinct packages
# This is the resolver's view. It is not proof of what a built artifact
# contains: build scripts can generate code or link native libraries, and
# assets sit outside Cargo's graph. Verify shipped artifacts separately.
# Requires cargo, jq. Run from the ket root through nilket/scripts/cargo-cgroup.
set -Eeuo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
cargo="$root/nilket/scripts/cargo-cgroup"
target=${CENSUS_TARGET:-x86_64-unknown-linux-gnu}

packages() { # workspace-dir label
	"$cargo" metadata --manifest-path "$1/Cargo.toml" --format-version 1 --locked \
	| jq -c ".packages[] | select(.source != null) | {name, version, source, license: (.license // null), license_file: (.license_file // null)}" \
	| sort > "$here/$2_packages.jsonl"
}
closure() { # workspace-dir product
	( cd "$1" && "$cargo" tree --locked -p "$2" -e normal --target "$target" --prefix none \
		--format "{p} | {l} | {f}" ) | sed "s|$root|<ket>|g" | sort -u > "$here/$2_$target.txt"
}

packages "$root" root
packages "$root/nilket" nilket
for p in polariton baryon; do closure "$root" "$p"; done
for p in toron wendelon nilket; do closure "$root/nilket" "$p"; done
{
	echo "census generated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "root Cargo.lock blob $(git -C "$root" hash-object Cargo.lock)"
	echo "nilket Cargo.lock blob $(git -C "$root" hash-object nilket/Cargo.lock)"
	echo "target $target"
	echo "rustc $(rustc --version)"
	for f in "$here"/*_packages.jsonl; do
		printf "%s %s packages\n" "$(basename "$f")" "$(wc -l < "$f")"
	done
	for f in "$here"/*_"$target".txt; do
		printf "%s %s rows, %s distinct name-version pairs\n" "$(basename "$f")" "$(wc -l < "$f")" "$(cut -d"|" -f1 "$f" | sed "s/ (.*//" | sort -u | wc -l)"
	done
} > "$here/MANIFEST.txt"
