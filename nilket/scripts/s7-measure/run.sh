#!/usr/bin/env bash
# Run one independent §8 measurement block: a fresh campaign-owned block
# directory (store, L2, WAL, PKI) with a unique block id, a fresh node child,
# its own warm-up and measured interval, evaluation, and a sealed record.
#
# Usage: run.sh base|fix <block-index>
#   S7_MANIFEST  the manifest (default: manifest.json beside this script)
#   S7_BIN       the arm's binary. CAMPAIGN-labelled manifests ignore any
#                override and require the frozen build of build-arms.sh;
#                other labels (SMOKE, TEST) may use any binary and are never
#                evidence.
#   S7_KEEP_STORE=1  keep store/ and l2/ after sealing (default: removed)
#
# The block's verdict is the evaluator's, made INVALID by any process or
# collection failure: a nonzero driver, node or evaluator exit, a node that
# had to be killed, a driver past its deadline, or missing records.
#
# Exit: 0 = fix PASS, or base evaluated (its failures are preserved in the
# record); 3 = fix FAIL; 4 = INVALID; 2 = refused to start. The campaign
# STOPs on any nonzero exit.
set -euo pipefail

arm=${1:?usage: run.sh base|fix <block-index>}
idx=${2:?usage: run.sh base|fix <block-index>}
case $arm in base | fix) ;; *) echo "arm must be base or fix" >&2; exit 2 ;; esac
here=$(cd "$(dirname "$0")" && pwd)
MANIFEST=${S7_MANIFEST:-$here/manifest.json}
DATE=${S7_DATE:-$(date -u +%Y%m%d)}
CAMPAIGN=${CAMPAIGN:-$HOME/work/s7-measure-campaign}
LOGS=${S7_LOGS:-$HOME/work/reviews-logs/s7-measure-$DATE}
label=$(jq -er .label "$MANIFEST")
manifest_sha=$(sha256sum "$MANIFEST" | cut -d' ' -f1)
refuse() { echo "run.sh: refused: $*" >&2; exit 2; }

if [ "$label" = CAMPAIGN ]; then
	# Evidence path: only the frozen, reviewed, release build of the fix arm,
	# with the manifest it was built with. Fix-only by the operator's decision
	# of 2026-11-06, so a base block is no evidence of anything here.
	[ "$arm" = fix ] || refuse "CAMPAIGN is fix-only; base evidence is historical"
	rec=$CAMPAIGN/build/$arm.json
	[ -f "$rec" ] || refuse "CAMPAIGN needs the fix build record in $CAMPAIGN/build"
	# The launcher running now is the frozen, reviewed one.
	cmp -s "$here/run.sh" "$(jq -er .frozen_overlay "$rec")/nilket/scripts/s7-measure/run.sh" \
		|| refuse "this run.sh is not the frozen launcher"
	BIN=$(jq -er .frozen_binary "$rec")
	[ -z "${S7_BIN:-}" ] || [ "$S7_BIN" = "$BIN" ] || refuse "CAMPAIGN ignores S7_BIN; the frozen binary is $BIN"
	jq -e '.mode == "campaign" and .profile == "release" and .overlay_uncommitted_files == 0
		and (.ref | test("^[0-9a-f]{40}$")) and .build_warnings == 0' "$rec" > /dev/null \
		|| refuse "$rec is not a clean campaign release build of a pinned commit"
	[ "$(sha256sum "$BIN" | cut -d' ' -f1)" = "$(jq -er .binary_sha256 "$rec")" ] \
		|| refuse "binary $BIN does not match its build record"
	[ "$manifest_sha" = "$(jq -er .overlay_manifest_sha256 "$rec")" ] \
		|| refuse "manifest $MANIFEST is not the one frozen into the build"
	[ "$(jq -er .arm "$rec")" = "$arm" ] || refuse "build record arm mismatch"
else
	BIN=${S7_BIN:-$HOME/work/target-s7-meas-$arm/release/s7-measure}
fi

block_name=$DATE-b$idx-$arm$([ "$label" = CAMPAIGN ] || echo "-$label")
BLOCK=$CAMPAIGN/blocks/$block_name
SEAL=$LOGS/$block_name
[ -x "$BIN" ] || refuse "no binary at $BIN"
[ -e "$BLOCK" ] && refuse "block dir $BLOCK exists; blocks are never reused"
[ -e "$SEAL" ] && refuse "seal dir $SEAL exists"
mkdir -p "$BLOCK/tmp" "$SEAL"
cp "$MANIFEST" "$BLOCK/manifest.json"
block_id=$block_name-$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
jq -n --arg id "$block_id" '{block_id: $id}' > "$BLOCK/block-id.json"
export TMPDIR=$BLOCK/tmp
t_start=$(date -u +%FT%TZ)
shutdown_timeout=$(jq -er .node_shutdown_timeout_secs "$BLOCK/manifest.json")
drive_deadline=$("$BIN" deadline --manifest "$BLOCK/manifest.json")

stat_start() { awk '{ sub(/^.*\) /, ""); print $20 }' "/proc/$1/stat"; }

"$BIN" node --manifest "$BLOCK/manifest.json" --block-dir "$BLOCK" > "$BLOCK/node.log" 2>&1 &
node_pid=$!
node_start=$(stat_start "$node_pid")
echo "block $block_name: node pid $node_pid start $node_start (driver deadline ${drive_deadline}s)"

drive_rc=0
timeout -s TERM -k 30 "$drive_deadline" \
	"$BIN" drive --manifest "$BLOCK/manifest.json" --block-dir "$BLOCK" --node-pid "$node_pid" \
	> "$BLOCK/drive.log" 2>&1 || drive_rc=$?

# Orderly stop of exactly the process we started (PID and start time),
# bounded; a node that has to be killed makes the block INVALID.
node_stop=exited-before-sigterm
if [ -e "/proc/$node_pid" ] && [ "$(stat_start "$node_pid")" = "$node_start" ]; then
	kill -TERM "$node_pid"
	if timeout "$shutdown_timeout" tail --pid="$node_pid" -f /dev/null; then
		node_stop=sigterm
	else
		kill -KILL "$node_pid" 2>/dev/null || true
		node_stop=sigkill-after-${shutdown_timeout}s
	fi
fi
node_rc=0
wait "$node_pid" || node_rc=$?

jq -n --arg id "$block_id" --arg sha "$manifest_sha" \
	--argjson drive_rc "$drive_rc" --argjson node_rc "$node_rc" --arg stop "$node_stop" \
	--argjson pid "$node_pid" --argjson start "$node_start" --argjson deadline "$drive_deadline" \
	'{block_id: $id, manifest_sha256: $sha, drive_exit_code: $drive_rc,
	  drive_timed_out: ($drive_rc == 124 or $drive_rc == 137), driver_deadline_secs: $deadline,
	  node_exit_code: $node_rc, node_stop: $stop, node_pid: $pid, node_pid_start_time: $start}' \
	> "$BLOCK/process.json"

eval_rc=0
"$BIN" evaluate --manifest "$BLOCK/manifest.json" --block-dir "$BLOCK" --arm "$arm" \
	> "$BLOCK/evaluate.log" 2>&1 || eval_rc=$?
verdict=$(jq -er .verdict "$BLOCK/verdict.json" 2>/dev/null || echo MISSING)
# The block verdict: the evaluator's, unless any process or collection step
# failed (the evaluator also checks these; this is the launcher's own gate).
block_verdict=$verdict
reasons=()
[ "$drive_rc" = 0 ] || reasons+=("driver exit $drive_rc")
[ "$node_rc" = 0 ] || reasons+=("node exit $node_rc")
[ "$node_stop" = sigterm ] || reasons+=("node stop $node_stop")
[ "$eval_rc" = 0 ] || reasons+=("evaluator exit $eval_rc")
[ "$verdict" != MISSING ] || reasons+=("no verdict")
if [ "$(jq -r '.block_id // ""' "$BLOCK/verdict.json" 2>/dev/null)" != "$block_id" ]; then
	reasons+=("verdict not bound to this block")
fi
[ "${#reasons[@]}" = 0 ] || block_verdict=INVALID

jq -n --arg arm "$arm" --arg idx "$idx" --arg block "$block_name" --arg id "$block_id" \
	--arg label "$label" --arg t_start "$t_start" --arg t_end "$(date -u +%FT%TZ)" \
	--arg bin "$BIN" --arg bin_sha "$(sha256sum "$BIN" | cut -d' ' -f1)" \
	--arg manifest_sha "$manifest_sha" \
	--arg verdict "$verdict" --arg block_verdict "$block_verdict" \
	--arg reasons "$(printf '%s\n' "${reasons[@]+"${reasons[@]}"}")" \
	--argjson eval_rc "$eval_rc" --arg host "$(hostname)" --arg nproc "$(nproc)" \
	--arg cgroup "$(cat /proc/self/cgroup)" --arg ulimits "$(ulimit -a)" \
	--slurpfile process "$BLOCK/process.json" \
	--arg build_record "$CAMPAIGN/build/$arm.json" \
	'{arm: $arm, block_index: ($idx | tonumber), block: $block, block_id: $id, label: $label,
	  evidence: ($label == "CAMPAIGN"), t_start: $t_start, t_end: $t_end,
	  binary: $bin, binary_sha256: $bin_sha, manifest_sha256: $manifest_sha,
	  build_record: $build_record, process: $process[0], evaluate_exit_code: $eval_rc,
	  evaluator_verdict: $verdict, verdict: $block_verdict,
	  launcher_invalid_reasons: ($reasons | split("\n") | map(select(. != ""))),
	  host: $host, nproc: ($nproc | tonumber), cgroup: $cgroup, ulimits: $ulimits}' \
	> "$BLOCK/block-record.json"

# Seal: the record files (never the store, L2 or keys) with their SHA-256s.
for f in block-record.json block-id.json process.json manifest.json effective-config.json \
	node-ready.json observations.jsonl driver.json driver-batches.jsonl verdict.json \
	node.log drive.log evaluate.log; do
	[ -e "$BLOCK/$f" ] && cp "$BLOCK/$f" "$SEAL/$f"
done
[ -f "$CAMPAIGN/build/$arm.json" ] && cp "$CAMPAIGN/build/$arm.json" "$SEAL/build-$arm.json"
(cd "$SEAL" && sha256sum -- * > SHA256SUMS)
(cd "$LOGS" && sha256sum "$block_name/SHA256SUMS" >> SEALS)
echo "block $block_name: verdict $block_verdict (evaluator $verdict${reasons[*]:+; ${reasons[*]}}); sealed in $SEAL"

# Cleanup after sealing: only this block's campaign-owned data.
if [ "${S7_KEEP_STORE:-0}" != 1 ]; then
	rm -rf "$BLOCK/store" "$BLOCK/l2" "$BLOCK/tmp"
fi

case $arm:$block_verdict in
fix:PASS | base:NO_FAILURE_ON_PRESENT_FIELDS | base:FAIL) exit 0 ;;
fix:FAIL) exit 3 ;;
*) exit 4 ;;
esac
