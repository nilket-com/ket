#!/usr/bin/env bash
# Build the two arms of the §8 measurement campaign (design r2, Codex r1/r2,
# harness ruling r1 item 5):
#
#   base = 9e7e3ad1 plus the shared measurement-harness overlay
#   fix  = the pinned integrated fix commit (FIX_REF) plus the same overlay
#
# Provenance: the overlay is frozen ONCE per invocation (copied out of REPO
# into a campaign-owned directory and hashed); both arms are overlaid from
# that frozen copy, never from the live repository. Before building, the
# overlaid files are verified against the frozen hashes and the whole input
# tree is hashed; after building, both are verified again and the binary is
# copied to a frozen location with its hash. Only the fix arm gets
# RUSTFLAGS="--cfg s7_fix" (declared via check-cfg in toron-tpch's Cargo.toml).
#
# Usage: S7_BUILD_MODE=campaign|smoke build-arms.sh base|fix|both
#   campaign: requires PROFILE=release, a clean overlay in git (no
#             ALLOW_DIRTY_OVERLAY), FIX_REF a full 40-hex commit id, and
#             REVIEWED_OVERLAY_SHA256 equal to the frozen overlay's aggregate.
#             Its records satisfy run.sh's CAMPAIGN preconditions.
#   smoke:    anything goes (PROFILE=dev, dirty overlay, FIX_REF=HEAD); its
#             records are marked mode=smoke and never satisfy them.
#   REPO      repository root (default: this script's repository)
#   PROFILE   release | dev (default release)
#
# Records: $CAMPAIGN/build/<arm>.json (read by run.sh) and a copy in
# ~/work/reviews-logs/s7-measure-<date>/build/.
set -euo pipefail

BASE_REF=9e7e3ad122f9ca6a3042bd12845f89ab9c1426ce
arms=${1:?usage: S7_BUILD_MODE=campaign|smoke build-arms.sh base|fix|both}
MODE=${S7_BUILD_MODE:?S7_BUILD_MODE must be campaign or smoke}
here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(git -C "$here" rev-parse --show-toplevel)}
PROFILE=${PROFILE:-release}
CAMPAIGN=${CAMPAIGN:-$HOME/work/s7-measure-campaign}
DATE=${S7_DATE:-$(date -u +%Y%m%d)}
LOGS=${S7_LOGS:-$HOME/work/reviews-logs/s7-measure-$DATE}
refuse() { echo "build-arms.sh: refused: $*" >&2; exit 2; }
mkdir -p "$CAMPAIGN/build" "$LOGS/build"

# The overlay: the harness source, toron-tpch's manifest (feature, bin,
# optional wendelon-client dependency, check-cfg), the frozen manifest and
# the launchers. The lockfile gets one line (toron-tpch -> wendelon-client),
# applied as an edit.
overlay_files=(
	nilket/crates/toron-tpch/Cargo.toml
	nilket/crates/toron-tpch/src/bin/s7_measure/common.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/compare.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/drive.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/drive/tests.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/evaluate.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/evaluate/tests.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/main.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/manifest.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/node.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/pki.rs
	nilket/crates/toron-tpch/src/bin/s7_measure/prom.rs
	nilket/scripts/cargo-cgroup
	nilket/scripts/s7-measure/build-arms.sh
	nilket/scripts/s7-measure/campaign.sh
	nilket/scripts/s7-measure/manifest.json
	nilket/scripts/s7-measure/run.sh
	nilket/scripts/s7-measure/test-run.sh
)

overlay_dirty=$(git -C "$REPO" status --porcelain -- "${overlay_files[@]}" nilket/Cargo.lock | wc -l)
case $MODE in
campaign)
	[ "$PROFILE" = release ] || refuse "campaign builds are PROFILE=release"
	[ "${ALLOW_DIRTY_OVERLAY:-0}" != 1 ] || refuse "campaign builds never allow a dirty overlay"
	[ "$overlay_dirty" -eq 0 ] || refuse "overlay has $overlay_dirty uncommitted files"
	[ -n "${REVIEWED_OVERLAY_SHA256:-}" ] || refuse "REVIEWED_OVERLAY_SHA256 is required"
	;;
smoke) ;;
*) refuse "S7_BUILD_MODE must be campaign or smoke" ;;
esac

# Freeze the overlay once: copy, hash, and from here on read only the copy.
stage=$(mktemp -d "$CAMPAIGN/overlay-stage.XXXXXX")
for f in "${overlay_files[@]}"; do
	mkdir -p "$(dirname "$stage/$f")"
	cp "$REPO/$f" "$stage/$f"
done
(cd "$stage" && sha256sum "${overlay_files[@]}") > "$stage.sha256"
overlay_aggregate=$(sha256sum "$stage.sha256" | cut -d' ' -f1)
FROZEN=$CAMPAIGN/overlay/$overlay_aggregate
if [ -d "$FROZEN" ]; then
	rm -rf "$stage"
	(cd "$FROZEN" && sha256sum -c --quiet "$FROZEN.sha256") || refuse "frozen overlay $FROZEN was altered"
	cmp -s "$stage.sha256" "$FROZEN.sha256" || refuse "frozen overlay manifest mismatch"
	rm "$stage.sha256"
else
	mkdir -p "$CAMPAIGN/overlay"
	mv "$stage" "$FROZEN"
	mv "$stage.sha256" "$FROZEN.sha256"
	chmod -R a-w "$FROZEN"
fi
overlay_manifest_sha=$(sha256sum "$FROZEN/nilket/scripts/s7-measure/manifest.json" | cut -d' ' -f1)
echo "overlay frozen: $FROZEN (aggregate $overlay_aggregate)"
if [ "$MODE" = campaign ] && [ "$overlay_aggregate" != "$REVIEWED_OVERLAY_SHA256" ]; then
	refuse "frozen overlay $overlay_aggregate != reviewed $REVIEWED_OVERLAY_SHA256"
fi

# The one lockfile line the optional dependency adds; idempotent.
lock_overlay() {
	python3 -I - "$1" <<'PY'
import sys
path = sys.argv[1]
s = open(path).read()
i = s.index('name = "toron-tpch"')
j = s.index(']\n', i)
if '"wendelon-client"' not in s[i:j]:
    old = ' "wendelon",\n'
    k = s.index(old, i)
    assert k < j, "toron-tpch lock entry has no wendelon dependency"
    s = s[:k + len(old)] + ' "wendelon-client",\n' + s[k + len(old):]
    open(path, 'w').write(s)
PY
}

tree_sha() {
	(cd "$1" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -d' ' -f1)
}

build_arm() {
	local arm=$1 ref src target rustflags
	case $arm in
	base) ref=$BASE_REF rustflags="" ;;
	fix)
		ref=${FIX_REF:?FIX_REF must pin the integrated fix commit}
		if [ "$MODE" = campaign ] && ! [[ $ref =~ ^[0-9a-f]{40}$ ]]; then
			refuse "campaign FIX_REF must be a full commit id, not $ref"
		fi
		rustflags="--cfg s7_fix"
		;;
	esac
	ref=$(git -C "$REPO" rev-parse --verify "$ref^{commit}")
	[ "$arm" != base ] || [ "$ref" = "$BASE_REF" ] || refuse "base resolved to $ref"
	src=$CAMPAIGN/$arm-src
	target=$HOME/work/target-s7-meas-$arm
	echo "== $arm ($MODE): $ref -> $src (target $target, profile $PROFILE)"

	rm -rf "$src"
	mkdir -p "$src"
	git -C "$REPO" archive --format=tar "$ref" > "$CAMPAIGN/$arm-src.tar"
	local archive_sha
	archive_sha=$(sha256sum "$CAMPAIGN/$arm-src.tar" | cut -d' ' -f1)
	tar -C "$src" -xf "$CAMPAIGN/$arm-src.tar"
	rm "$CAMPAIGN/$arm-src.tar"

	# Overlay from the frozen copy; the manifest may only gain lines.
	local manifest_diff
	manifest_diff=$(diff "$src/nilket/crates/toron-tpch/Cargo.toml" "$FROZEN/nilket/crates/toron-tpch/Cargo.toml" || true)
	if printf '%s\n' "$manifest_diff" | grep -q '^<'; then
		printf '%s\n' "$manifest_diff" >&2
		refuse "$arm: toron-tpch/Cargo.toml overlay would remove lines from the archive"
	fi
	for f in "${overlay_files[@]}"; do
		mkdir -p "$(dirname "$src/$f")"
		cp "$FROZEN/$f" "$src/$f"
		chmod u+w "$src/$f"
	done
	(cd "$src" && sha256sum -c --quiet "$FROZEN.sha256") || refuse "$arm: overlaid sources differ from the frozen overlay"
	local lock_before lock_diff
	lock_before=$(mktemp)
	cp "$src/nilket/Cargo.lock" "$lock_before"
	lock_overlay "$src/nilket/Cargo.lock"
	lock_diff=$(diff "$lock_before" "$src/nilket/Cargo.lock" || true)
	rm "$lock_before"
	local inputs_before
	inputs_before=$(tree_sha "$src")

	mkdir -p "$target" "/dev/shm/target-s7-meas-$arm-tmp"
	ln -sfn "/dev/shm/target-s7-meas-$arm-tmp" "$target/tmp"
	# One wrapper for both arms: the frozen copy, never the live checkout.
	local wrapper=$FROZEN/nilket/scripts/cargo-cgroup
	local cmd=(env CARGO_TARGET_DIR="$target" TMPDIR="$target/tmp" RUSTFLAGS="$rustflags"
		"$wrapper" build --locked --profile "$PROFILE" -p toron-tpch --features s7-measure --bin s7-measure)
	echo "   ${cmd[*]}"
	(cd "$src/nilket" && "${cmd[@]}") 2>&1 | tee "$LOGS/build/$arm-build.log"
	local warnings
	warnings=$(grep -c '^warning' "$LOGS/build/$arm-build.log" || true)

	# The inputs that were built are the inputs that were verified.
	(cd "$src" && sha256sum -c --quiet "$FROZEN.sha256") || refuse "$arm: overlay changed during the build"
	[ "$(tree_sha "$src")" = "$inputs_before" ] || refuse "$arm: input tree changed during the build"

	local outdir=$PROFILE
	[ "$PROFILE" = dev ] && outdir=debug
	(cd "$FROZEN" && sha256sum -c --quiet "$FROZEN.sha256") || refuse "$arm: the frozen overlay changed during its build"
	local bin=$target/$outdir/s7-measure
	local frozen_bin=$CAMPAIGN/bin/$MODE-$arm/s7-measure
	mkdir -p "$(dirname "$frozen_bin")"
	rm -f "$frozen_bin"
	cp "$bin" "$frozen_bin"
	chmod a-w "$frozen_bin"
	local bin_sha
	bin_sha=$(sha256sum "$frozen_bin" | cut -d' ' -f1)
	[ "$bin_sha" = "$(sha256sum "$bin" | cut -d' ' -f1)" ] || refuse "$arm: frozen binary copy differs"
	local toolchain
	toolchain=$(cd "$src/nilket" && rustc -vV && cargo -V)

	local rec=$CAMPAIGN/build/$arm.json
	[ "$MODE" = campaign ] || rec=$CAMPAIGN/build/smoke-$arm.json
	jq -n \
		--arg arm "$arm" --arg mode "$MODE" --arg ref "$ref" --arg archive_sha "$archive_sha" \
		--arg description "$ref plus the shared measurement-harness overlay" \
		--arg profile "$PROFILE" --arg rustflags "$rustflags" \
		--arg check_cfg 'toron-tpch [lints.rust] unexpected_cfgs check-cfg = ["cfg(s7_fix)"]' \
		--arg command "(cd $src/nilket && ${cmd[*]})" \
		--arg wrapper_sha "$(sha256sum "$wrapper" | cut -d' ' -f1)" \
		--arg frozen "$FROZEN" --arg aggregate "$overlay_aggregate" \
		--arg overlay "$(cat "$FROZEN.sha256")" --argjson overlay_dirty "$overlay_dirty" \
		--arg manifest_sha "$overlay_manifest_sha" --arg inputs "$inputs_before" \
		--arg manifest_diff "$manifest_diff" --arg lock_diff "$lock_diff" \
		--arg lock_sha "$(sha256sum "$src/nilket/Cargo.lock" | cut -d' ' -f1)" \
		--arg toolchain "$toolchain" \
		--arg binary "$bin" --arg frozen_bin "$frozen_bin" --arg binary_sha "$bin_sha" \
		--argjson warnings "$warnings" \
		'{arm: $arm, mode: $mode, ref: $ref, description: $description,
		  archive_sha256: $archive_sha, profile: $profile, rustflags: $rustflags,
		  check_cfg: $check_cfg, command: $command, wrapper_sha256: $wrapper_sha,
		  frozen_overlay: $frozen, overlay_aggregate_sha256: $aggregate,
		  overlay_sha256sums: $overlay, overlay_uncommitted_files: $overlay_dirty,
		  overlay_manifest_sha256: $manifest_sha, input_tree_sha256: $inputs,
		  toron_tpch_manifest_overlay_diff: $manifest_diff, lockfile_overlay_diff: $lock_diff,
		  lockfile_sha256: $lock_sha, toolchain: $toolchain,
		  built_binary: $binary, frozen_binary: $frozen_bin, binary_sha256: $binary_sha,
		  build_warnings: $warnings}' > "$rec"
	cp "$rec" "$LOGS/build/"
	echo "   record: $rec (warnings: $warnings, binary $bin_sha)"
}

case $arms in
base | fix) build_arm "$arms" ;;
both) build_arm base; build_arm fix ;;
*) refuse "usage: S7_BUILD_MODE=campaign|smoke build-arms.sh base|fix|both" ;;
esac
