#!/usr/bin/env bash
# Compare the Rune port of check_blake3_confinement against the Bash original.
# Run it from anywhere:
#
#   RNX=/path/to/rnx ./compare.sh
#
# Both implementations run the same `grep` over the same tree, so unlike the
# ported Python scripts these two must agree on standard error as well: every
# line either of them writes there is its own, not a runtime's diagnostic.
set -u
cd "$(dirname "$0")" || exit 2
here=$(pwd)
crate=$(cd ../../.. && pwd)
sh_check="$crate/ci/check_blake3_confinement.sh"
rn_check="$crate/ci/check_blake3_confinement.rn"
rnx=${RNX:-rnx}
limit=${TIMEOUT:-120}

work=$(mktemp -d "${TMPDIR:-/tmp}/blake3-compare.XXXXXX") || exit 2
trap 'rm -rf "$work"' EXIT INT TERM

failures=0

# `case_ NAME DIRECTORY` runs both from that directory and requires standard
# output, standard error, and the exit code all to agree.
case_() {
	local name=$1
	local where=$2
	# A tree that is not there would make both sides fail alike and look like
	# agreement, so it is a failure of the harness rather than a case.
	if [ ! -d "$where" ]; then
		failures=$((failures + 1))
		printf '%-12s MISSING TREE %s\n' "$name" "$where"
		return
	fi
	(cd "$where" && timeout -s KILL "$limit" bash "$sh_check") \
		>"$work/sh.out" 2>"$work/sh.err"
	local sh_code=$?
	(cd "$where" && timeout -s KILL "$limit" "$rnx" run "$rn_check") \
		>"$work/rn.out" 2>"$work/rn.err"
	local rn_code=$?

	for code in "$sh_code" "$rn_code"; do
		if [ "$code" = 124 ] || [ "$code" = 137 ]; then
			failures=$((failures + 1))
			printf '%-12s TIMED OUT after %ss (exit %s/%s)\n' \
				"$name" "$limit" "$sh_code" "$rn_code"
			return
		fi
	done

	local out="same"
	cmp -s "$work/sh.out" "$work/rn.out" || { out="DIFFERENT"; failures=$((failures + 1)); }
	local err="same"
	cmp -s "$work/sh.err" "$work/rn.err" || { err="DIFFERENT"; failures=$((failures + 1)); }
	if [ "$sh_code" != "$rn_code" ]; then
		failures=$((failures + 1))
	fi
	printf '%-12s exit %s/%s  stdout %-10s stderr %s\n' \
		"$name" "$sh_code" "$rn_code" "$out" "$err"
}

case_ violating "$here/violating"
case_ clean "$here/clean"
case_ empty "$here/empty"
case_ nosrc "$here/nosrc"
# The crate as it actually is, which no fixture can stand in for.
case_ live "$crate"

# The original has no capture limit, no timeout, and no interrupt handling, so
# for these there is nothing to compare. What matters is that the port REFUSES
# rather than reporting a clean check: a violation it never saw looks exactly
# like a tree that has none.
#
# `refuses NAME EXPECTED-FRAGMENT` runs the port in a prepared tree and
# requires a nonzero status, the fragment on standard error, and no claim of
# having passed.
refuses() {
	local name=$1
	local fragment=$2
	local check=$3
	local where=$4
	(cd "$where" && timeout -s KILL "$limit" "$rnx" run "$check") \
		>"$work/rn.out" 2>"$work/rn.err"
	local code=$?
	local why=""
	[ "$code" = 0 ] && why="reported success"
	grep -q "check passed" "$work/rn.out" && why="$why; claimed to have passed"
	grep -q "$fragment" "$work/rn.err" || why="$why; did not say why"
	if [ -n "$why" ]; then
		failures=$((failures + 1))
		printf '%-12s REFUSAL MISSING exit %s%s\n' "$name" "$code" "$why"
	else
		printf '%-12s refused, exit %s\n' "$name" "$code"
	fi
}

# A capture larger than rnx holds, with the violation past the limit. Both
# lines are in one file, so grep emits them in file order and the case does
# not depend on how a directory is walked. The original reports the violation
# and exits 1; the port must not report a clean tree.
truncated_tree="$work/truncated/src"
mkdir -p "$truncated_tree"
{
	printf 'let q = crate::hash::blake3::content("'
	head -c 2621440 /dev/zero | tr '\0' 'x'
	printf '");\nlet v = blake3::hash(p);\n'
} >"$truncated_tree/late.rs"
(cd "$work/truncated" && bash "$sh_check") >"$work/sh.out" 2>"$work/sh.err"
if [ "$?" = 0 ] || ! grep -q "src/late.rs" "$work/sh.err"; then
	failures=$((failures + 1))
	echo "truncated    THE ORIGINAL DID NOT REPORT THE VIOLATION, so this case proves nothing"
fi
refuses truncated "more output than rnx captures" "$rn_check" "$work/truncated"

# A named pipe nothing writes to, so grep blocks. A copy of the port with a
# short timeout drives the case; it differs from the port by that one
# constant and by nothing else.
blocked_tree="$work/blocked/src"
mkdir -p "$blocked_tree"
mkfifo "$blocked_tree/blocked"
echo 'let ok = 1;' >"$blocked_tree/plain.rs"
sed 's/^const CAPTURE_TIMEOUT_MS = 60000;$/const CAPTURE_TIMEOUT_MS = 400;/' \
	"$rn_check" >"$work/impatient.rn"
if ! grep -q "CAPTURE_TIMEOUT_MS = 400;" "$work/impatient.rn"; then
	failures=$((failures + 1))
	echo "timed-out    THE TIMEOUT CONSTANT MOVED, so this case tests nothing"
fi
refuses timed-out "did not finish within" "$work/impatient.rn" "$work/blocked"

# The same blocked tree, with the ordinary timeout, interrupted while grep is
# running.
(cd "$work/blocked" && timeout -s KILL "$limit" "$rnx" run "$rn_check") \
	>"$work/rn.out" 2>"$work/rn.err" &
interrupted=$!
sleep 1
kill -INT "$interrupted" 2>/dev/null
wait "$interrupted"
cancel_code=$?
if [ "$cancel_code" = 0 ] || ! grep -q "interrupted while grep" "$work/rn.err"; then
	failures=$((failures + 1))
	printf '%-12s REFUSAL MISSING exit %s: %s\n' "cancelled" "$cancel_code" \
		"$(head -1 "$work/rn.err")"
else
	printf '%-12s refused, exit %s\n' "cancelled" "$cancel_code"
fi

if [ "$failures" -gt 0 ]; then
	echo "$failures check(s) failed"
	exit 1
fi
echo "the five comparisons agree, and an incomplete capture is refused three ways"
