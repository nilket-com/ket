#!/usr/bin/env bash
# Compare the Rune port of verify_nilket_graft against the Python original.
#
#   RNX=/path/to/rnx ./compare.sh
#
# The real inputs to this migration were not kept, so every case is built by
# build.sh and then mutated. The build is deterministic, which is what lets a
# diagnostic naming a commit be recorded.
set -u
cd "$(dirname "$0")" || exit 2
here=$(pwd)
migration=$(cd ../.. && pwd)
py="$migration/verify_nilket_graft.py"
rn="$migration/verify_nilket_graft.rn"
rnx=${RNX:-rnx}
limit=${TIMEOUT:-180}

work=$(mktemp -d "${TMPDIR:-/tmp}/graft-compare.XXXXXX") || exit 2
trap 'rm -rf "$work"' EXIT INT TERM
failures=0

# A fresh lineage for a case to mutate.
build() {
	local into=$1
	"$here/build.sh" "$into" >/dev/null || {
		echo "the fixture would not build"
		exit 2
	}
}

# `case_ NAME TREE ARGS...` requires identical standard output and exit code,
# and the port's standard error to match expected/NAME.err. The two runtimes
# word their diagnostics differently: the original raises and the port says
# what differs, so only the port's is recorded.
case_() {
	local name=$1
	local tree=$2
	shift 2
	timeout -s KILL "$limit" python3 "$py" "$@" >"$work/py.out" 2>"$work/py.err"
	local py_code=$?
	timeout -s KILL "$limit" "$rnx" run "$rn" "$@" >"$work/rn.out" 2>"$work/rn.err"
	local rn_code=$?
	# A killed invocation is a failure, never a result to compare: two sides
	# that both time out would otherwise look like agreement.
	for code in "$py_code" "$rn_code"; do
		if [ "$code" = 124 ] || [ "$code" = 137 ]; then
			failures=$((failures + 1))
			printf '%-16s TIMED OUT after %ss (exit %s/%s)\n' \
				"$name" "$limit" "$py_code" "$rn_code"
			return
		fi
	done
	local out="same"
	cmp -s "$work/py.out" "$work/rn.out" || { out="DIFFERENT"; failures=$((failures + 1)); }
	[ "$py_code" = "$rn_code" ] || failures=$((failures + 1))
	local err="as recorded"
	sed -e "s|$tree/||g" "$work/rn.err" >"$work/rn.norm"
	if [ "${RECORD:-}" = 1 ]; then
		mkdir -p expected
		cp "$work/rn.norm" "expected/$name.err"
		err="recorded"
	elif [ ! -f "expected/$name.err" ]; then
		err="NO EXPECTATION RECORDED"
		failures=$((failures + 1))
	elif ! cmp -s "$work/rn.norm" "expected/$name.err"; then
		err="UNEXPECTED"
		failures=$((failures + 1))
	fi
	printf '%-16s exit %s/%s  stdout %-10s stderr %s\n' \
		"$name" "$py_code" "$rn_code" "$out" "$err"
}

good="$work/good"
build "$good"
case_ verified "$good" "$good/source" "$good/target" "$good/commit_map"
case_ wrong-arguments "$good" "$good/source" "$good/target"

# A message the rewrite did not preserve.
changed="$work/changed"
build "$changed"
tip=$(git -C "$changed/target" rev-parse refs/heads/main)
git -C "$changed/target" cat-file commit "$tip" |
	sed 's/a body line for 3/a body line for X/' |
	git -C "$changed/target" hash-object -t commit -w --stdin >"$work/new"
new=$(cat "$work/new")
git -C "$changed/target" update-ref refs/heads/main "$new"
awk -v o="$tip" -v n="$new" '{ if ($2 == o) print $1, n; else print }' \
	"$changed/commit_map" >"$work/cm" && mv "$work/cm" "$changed/commit_map"
case_ message-changed "$changed" "$changed/source" "$changed/target" "$changed/commit_map"

# A wrapper tree under the wrong name.
renamed="$work/renamed"
build "$renamed"
tip=$(git -C "$renamed/target" rev-parse refs/heads/main)
old_tree=$(git -C "$renamed/target" rev-parse "$tip^{tree}")
inner=$(git -C "$renamed/target" cat-file -p "$old_tree" | awk '{print $3}')
wrapped=$(printf '040000 tree %s\telsewhere\n' "$inner" | git -C "$renamed/target" mktree)
git -C "$renamed/target" cat-file commit "$tip" |
	sed "s/^tree .*/tree $wrapped/" |
	git -C "$renamed/target" hash-object -t commit -w --stdin >"$work/new"
new=$(cat "$work/new")
git -C "$renamed/target" update-ref refs/heads/main "$new"
awk -v o="$tip" -v n="$new" '{ if ($2 == o) print $1, n; else print }' \
	"$renamed/commit_map" >"$work/cm" && mv "$work/cm" "$renamed/commit_map"
case_ tree-renamed "$renamed" "$renamed/source" "$renamed/target" "$renamed/commit_map"

# The same map written with tabs, and with carriage returns at the ends of its
# lines. The original splits on any whitespace and strips a carriage return,
# so both say what the plain one says.
tabbed="$work/tabbed"
build "$tabbed"
sed 's/ /\t/g' "$tabbed/commit_map" >"$work/cm" && mv "$work/cm" "$tabbed/commit_map"
case_ map-tabs "$tabbed" "$tabbed/source" "$tabbed/target" "$tabbed/commit_map"

returns="$work/returns"
build "$returns"
sed 's/$/\r/' "$returns/commit_map" >"$work/cm" && mv "$work/cm" "$returns/commit_map"
case_ map-crlf "$returns" "$returns/source" "$returns/target" "$returns/commit_map"

# A rewritten commit that carries a signature, which a rewrite invalidates.
resigned="$work/resigned"
build "$resigned"
tip=$(git -C "$resigned/target" rev-parse refs/heads/main)
git -C "$resigned/target" cat-file commit "$tip" |
	sed '/^committer /a gpgsig -----BEGIN PGP SIGNATURE-----\n \n not a real signature\n -----END PGP SIGNATURE-----' |
	git -C "$resigned/target" hash-object -t commit -w --stdin >"$work/new"
new=$(cat "$work/new")
git -C "$resigned/target" update-ref refs/heads/main "$new"
awk -v o="$tip" -v n="$new" '{ if ($2 == o) print $1, n; else print }' \
	"$resigned/commit_map" >"$work/cm" && mv "$work/cm" "$resigned/commit_map"
case_ target-signed "$resigned" "$resigned/source" "$resigned/target" "$resigned/commit_map"

# A commit the map does not carry.
short="$work/short"
build "$short"
head -n -1 "$short/commit_map" >"$work/cm" && mv "$work/cm" "$short/commit_map"
case_ map-short "$short" "$short/source" "$short/target" "$short/commit_map"

# The real artifacts, as far as they go. The commit map this migration
# produced is kept, and its target is this repository; the frozen source is
# not. So the original has not been runnable on real inputs since the freeze,
# and what can still be checked is that both refuse the same way.
real_map="$migration/0001_nilket_commit_map.tsv"
ket=$(cd "$migration/../../.." && pwd)
if [ ! -f "$real_map" ]; then
	failures=$((failures + 1))
	echo "real-inputs      THE COMMIT MAP IS GONE, so this case proves nothing"
else
	case_ real-inputs "$work" "$work/frozen-source-that-is-gone" "$ket" "$real_map"
fi

if [ "$failures" -gt 0 ]; then
	echo "$failures check(s) failed"
	exit 1
fi
echo "standard output and exit codes agree, and every diagnostic is as recorded"
