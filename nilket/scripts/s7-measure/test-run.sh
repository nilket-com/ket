#!/usr/bin/env bash
# Script-level controls for run.sh and build-arms.sh (harness ruling r1,
# items 1, 2 and 5), with a fake s7-measure binary so no node or cargo build
# is involved. The fake evaluator always reports PASS: each run.sh control
# proves that a process or collection failure still makes the block INVALID
# (exit 4), that the shutdown and driver deadlines hold, and that a CAMPAIGN
# label is refused without campaign build records; the build-arms.sh controls
# prove that campaign preconditions are refused before anything is built.
#
# Usage: test-run.sh    (exits nonzero on the first failed control)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/s7-test-run.XXXXXX")
trap 'chmod -R u+w "$work"; rm -rf "$work"' EXIT

cat > "$work/fake-s7" <<'FAKE'
#!/usr/bin/env bash
cmd=$1; shift
block=
while [ $# -gt 0 ]; do case $1 in --block-dir) block=$2; shift 2 ;; *) shift 2 ;; esac; done
case $cmd in
deadline) echo 3 ;;
node)
	if [ "$FAKE" = node-ignores-term ]; then trap '' TERM; else trap 'exit 0' TERM; fi
	while :; do sleep 0.1; done ;;
drive)
	case $FAKE in
	drive-hangs) trap '' TERM; while :; do sleep 0.1; done ;;
	drive-fails) exit 1 ;;
	*) exit 0 ;;
	esac ;;
evaluate)
	id=$(jq -r .block_id "$block/block-id.json")
	[ "$FAKE" = verdict-unbound ] && id=someone-else
	jq -n --arg id "$id" '{verdict: "PASS", block_id: $id}' > "$block/verdict.json"
	[ "$FAKE" = evaluate-fails ] && exit 1
	exit 0 ;;
esac
FAKE
chmod +x "$work/fake-s7"
jq '.label = "TEST" | .node_shutdown_timeout_secs = 2' "$here/manifest.json" > "$work/manifest.json"

failures=0
n=0
check() { # name fake expected-exit expected-verdict max-seconds [label-manifest]
	local name=$1 fake=$2 want_rc=$3 want_verdict=$4 max=$5 manifest=${6:-$work/manifest.json}
	n=$((n + 1))
	local t0 rc=0 got
	t0=$(date +%s)
	FAKE=$fake S7_BIN=$work/fake-s7 S7_MANIFEST=$manifest CAMPAIGN=$work/campaign \
		S7_LOGS=$work/logs S7_DATE=test "$here/run.sh" fix "$n" > "$work/out-$n.log" 2>&1 || rc=$?
	local took=$(($(date +%s) - t0))
	got=$(jq -r .verdict "$work"/campaign/blocks/test-b$n-fix*/block-record.json 2>/dev/null || echo none)
	if [ "$rc" = "$want_rc" ] && [ "$got" = "$want_verdict" ] && [ "$took" -le "$max" ]; then
		echo "ok   $name (exit $rc, verdict $got, ${took}s)"
	else
		echo "FAIL $name: exit $rc (want $want_rc), verdict $got (want $want_verdict), ${took}s (max $max)"
		sed 's/^/     /' "$work/out-$n.log"
		failures=$((failures + 1))
	fi
}

check "clean block passes through" clean 0 PASS 10
check "node ignoring SIGTERM is killed at the shutdown deadline: INVALID" node-ignores-term 4 INVALID 10
check "driver past its deadline is stopped: INVALID" drive-hangs 4 INVALID 45
check "driver failure after a valid verdict: INVALID" drive-fails 4 INVALID 10
check "evaluator failure: INVALID" evaluate-fails 4 INVALID 10
check "verdict bound to another block: INVALID" verdict-unbound 4 INVALID 10
jq '.label = "CAMPAIGN"' "$work/manifest.json" > "$work/campaign-manifest.json"
check "CAMPAIGN without campaign build records is refused" clean 2 none 5 "$work/campaign-manifest.json"
grep -q "refused: CAMPAIGN needs both build records" "$work/out-$n.log" \
	|| { echo "FAIL campaign refusal reason"; failures=$((failures + 1)); }

# build-arms.sh campaign preconditions (refused before anything is built).
refusal() { # name expected-message env...
	local name=$1 want=$2 rc=0
	shift 2
	n=$((n + 1))
	env CAMPAIGN="$work/campaign" S7_LOGS="$work/logs" "$@" "$here/build-arms.sh" base \
		> "$work/out-$n.log" 2>&1 || rc=$?
	if [ "$rc" = 2 ] && grep -q "$want" "$work/out-$n.log"; then
		echo "ok   $name (exit 2)"
	else
		echo "FAIL $name: exit $rc"; sed 's/^/     /' "$work/out-$n.log"
		failures=$((failures + 1))
	fi
}
refusal "a build without a declared mode is refused" "S7_BUILD_MODE must be" S7_BUILD_MODE=bogus
refusal "a campaign build must be release" "PROFILE=release" S7_BUILD_MODE=campaign PROFILE=dev
refusal "a campaign build never allows a dirty overlay" "never allow a dirty overlay" \
	S7_BUILD_MODE=campaign ALLOW_DIRTY_OVERLAY=1
refusal "a campaign build needs the reviewed overlay hash" "REVIEWED_OVERLAY_SHA256\\|uncommitted" \
	S7_BUILD_MODE=campaign

# A live launcher that is not the frozen one is refused, by both launchers.
n=$((n + 1))
mkdir -p "$work/frozen/nilket/scripts/s7-measure" "$work/lc/build"
for f in run.sh campaign.sh; do echo "# not this launcher" > "$work/frozen/nilket/scripts/s7-measure/$f"; done
for arm in base fix; do jq -n --arg o "$work/frozen" '{frozen_overlay: $o, frozen_binary: "/bin/true"}' > "$work/lc/build/$arm.json"; done
rc=0
env FAKE=clean CAMPAIGN="$work/lc" S7_BIN=/bin/true S7_MANIFEST="$work/campaign-manifest.json" S7_LOGS="$work/logs" S7_DATE=test \
	"$here/run.sh" fix "$n" > "$work/out-$n.log" 2>&1 || rc=$?
if [ "$rc" = 2 ] && grep -q "not the frozen launcher" "$work/out-$n.log"; then echo "ok   run.sh refuses an unfrozen launcher"
else echo "FAIL run.sh launcher check: exit $rc"; sed 's/^/     /' "$work/out-$n.log"; failures=$((failures + 1)); fi
n=$((n + 1)); rc=0
env CAMPAIGN="$work/lc" S7_LOGS="$work/logs" S7_DATE=test "$here/campaign.sh" > "$work/out-$n.log" 2>&1 || rc=$?
if [ "$rc" = 2 ] && grep -q "not the frozen launcher" "$work/out-$n.log"; then echo "ok   campaign.sh refuses an unfrozen launcher"
else echo "FAIL campaign.sh launcher check: exit $rc"; sed 's/^/     /' "$work/out-$n.log"; failures=$((failures + 1)); fi

# The build runs the frozen wrapper, whatever happens to the live one: a fake repository and a
# fake wrapper that tampers with the live copy from inside the build, with no cargo involved.
n=$((n + 1))
fake=$work/fake-repo; mkdir -p "$fake/nilket/scripts/s7-measure" "$fake/nilket/crates/toron-tpch/src/bin/s7_measure/drive" "$fake/nilket/crates/toron-tpch/src/bin/s7_measure/evaluate"
git -C "$fake" init -q; git -C "$fake" config user.email t@t; git -C "$fake" config user.name t; git -C "$fake" config commit.gpgsign false
printf '[package]\nname = "toron-tpch"\n' > "$fake/nilket/crates/toron-tpch/Cargo.toml"
printf '[[package]]\nname = "toron-tpch"\ndependencies = [\n "wendelon",\n]\n' > "$fake/nilket/Cargo.lock"
for f in common compare drive drive/tests evaluate evaluate/tests main manifest node pki prom; do echo "// $f" > "$fake/nilket/crates/toron-tpch/src/bin/s7_measure/$f.rs"; done
cp "$here"/build-arms.sh "$here"/campaign.sh "$here"/manifest.json "$here"/run.sh "$here"/test-run.sh "$fake/nilket/scripts/s7-measure/"
cat > "$fake/nilket/scripts/cargo-cgroup" <<WRAP
#!/usr/bin/env bash
echo "\$0" >> "$work/wrapper-ran"
echo "# tampered" >> "$fake/nilket/scripts/cargo-cgroup"
mkdir -p "\$CARGO_TARGET_DIR/release"; printf '#!/bin/true\n' > "\$CARGO_TARGET_DIR/release/s7-measure"; chmod +x "\$CARGO_TARGET_DIR/release/s7-measure"
WRAP
chmod +x "$fake/nilket/scripts/cargo-cgroup"; git -C "$fake" add -A; git -C "$fake" commit -qm fake
want=$(sha256sum "$fake/nilket/scripts/cargo-cgroup" | cut -d' ' -f1); rc=0
env RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}" CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}" HOME="$work/fake-home" CAMPAIGN="$work/fc" S7_LOGS="$work/logs" S7_DATE=test S7_BUILD_MODE=smoke FIX_REF=HEAD REPO="$fake" \
	bash "$fake/nilket/scripts/s7-measure/build-arms.sh" fix > "$work/out-$n.log" 2>&1 || rc=$?
ran=$(head -1 "$work/wrapper-ran" 2>/dev/null || true); got=$(jq -r .wrapper_sha256 "$work/fc/build/smoke-fix.json" 2>/dev/null || true)
if [ "$rc" = 0 ] && [[ "$ran" == "$work/fc/overlay/"* ]] && [ "$got" = "$want" ] && [ "$(sha256sum "$fake/nilket/scripts/cargo-cgroup" | cut -d' ' -f1)" != "$want" ]; then
	echo "ok   the build runs and records the frozen wrapper, not the tampered live one"
else echo "FAIL frozen wrapper: exit $rc ran=$ran got=$got want=$want"; sed 's/^/     /' "$work/out-$n.log" | tail -8; failures=$((failures + 1)); fi

echo "$((n - failures))/$n launcher controls passed"
[ "$failures" = 0 ]
