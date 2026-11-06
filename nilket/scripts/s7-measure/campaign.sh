#!/usr/bin/env bash
# The §8 measurement campaign, fix-only by the operator's decision of 2026-11-06:
# the three fix blocks of the frozen order B F F B B F, because the pinned base
# wedges and could never complete a block. No A/B claim follows from this, and
# the historical base evidence is reported separately. Each block is its own
# fresh node, store, warm-up and measured interval (run.sh). STOP on the first
# nonzero block exit (fix FAIL, any INVALID or a harness failure), on a block
# that is not evidence, or when a block is not comparable with the ones before
# it (dataset, schema, manifest, effective config). No threshold is relaxed and
# no block is retried.
#
# Evidence only: the frozen CAMPAIGN manifest and a campaign-mode release build
# of the fix arm (build-arms.sh; run.sh enforces the preconditions).
# Do not run without the reviewed source, the pinned fix commit and the
# operator's go.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
CAMPAIGN=${CAMPAIGN:-$HOME/work/s7-measure-campaign}
DATE=${S7_DATE:-$(date -u +%Y%m%d)}
LOGS=${S7_LOGS:-$HOME/work/reviews-logs/s7-measure-$DATE}
[ -z "${S7_MANIFEST:-}" ] || { echo "campaign.sh runs only the frozen manifest" >&2; exit 2; }
[ -z "${S7_BIN:-}" ] || { echo "campaign.sh runs only the frozen binaries" >&2; exit 2; }
[ "$(jq -r .label "$here/manifest.json")" = CAMPAIGN ] || { echo "manifest is not CAMPAIGN" >&2; exit 2; }
fix_bin=$(jq -er .frozen_binary "$CAMPAIGN/build/fix.json")
# The launchers running now are the frozen, reviewed ones.
frozen=$(jq -er .frozen_overlay "$CAMPAIGN/build/fix.json")/nilket/scripts/s7-measure
for launcher in campaign.sh run.sh; do
	cmp -s "$here/$launcher" "$frozen/$launcher" || { echo "$launcher is not the frozen launcher" >&2; exit 2; }
done
export S7_DATE=$DATE S7_LOGS=$LOGS CAMPAIGN

order=(fix fix fix)
sealed=()
for i in "${!order[@]}"; do
	arm=${order[$i]}
	n=$((i + 1))
	echo "=== block $n/${#order[@]}: $arm"
	if ! "$here/run.sh" "$arm" "$n"; then
		echo "STOP: block $n ($arm) did not pass; see its sealed record" >&2
		exit 1
	fi
	seal=$LOGS/$DATE-b$n-$arm
	jq -e '.evidence == true and .label == "CAMPAIGN"' "$seal/block-record.json" > /dev/null \
		|| { echo "STOP: block $n is not evidence" >&2; exit 1; }
	sealed+=("$seal")
	if ! "$fix_bin" compare --blocks "$(IFS=,; echo "${sealed[*]}")" > "$LOGS/compare-b$n.json"; then
		echo "STOP: block $n is not comparable with the earlier blocks; see $LOGS/compare-b$n.json" >&2
		exit 1
	fi
done
echo "campaign complete: ${#order[@]} fix blocks sealed and comparable; no A/B claim"
