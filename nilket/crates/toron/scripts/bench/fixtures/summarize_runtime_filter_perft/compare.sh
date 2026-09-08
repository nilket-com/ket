#!/usr/bin/env bash
# Compare the Rune port of summarize_runtime_filter_perft against the Python
# original on every case that has one. Run it from anywhere:
#
#   RNX=/path/to/rnx ./compare.sh
#
# Standard output and the exit code must match. Standard error is checked
# against a per-case expectation in expected/, because the two runtimes word
# their diagnostics differently and a column that only reports is a column
# that cannot fail.
set -u
cd "$(dirname "$0")" || exit 2
here=$(pwd)
bench=$(cd ../.. && pwd)
py="$bench/summarize_runtime_filter_perft.py"
rn="$bench/summarize_runtime_filter_perft.rn"
rnx=${RNX:-rnx}
limit=${TIMEOUT:-60}

work=$(mktemp -d "${TMPDIR:-/tmp}/summarize-compare.XXXXXX") || exit 2
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

failures=0
stale=0

# Paths reach a diagnostic as they were given on the command line, and this
# script always runs from the fixture directory, so they are already
# relative. Strip any absolute prefix anyway, so an expectation recorded here
# holds from another checkout.
normalize() {
	sed -e "s|$here/||g" -e "s|$bench/||g" -e "s|$work/||g" "$1"
}

# `case_ NAME [--known-exit-2] ARGS...` runs both implementations under a
# timeout and checks four things:
#
#   - standard output is identical
#   - the exit code is identical, unless the case is marked --known-exit-2
#   - the port's standard error equals expected/NAME.err
#   - where the original succeeds, neither wrote to standard error
#
# --known-exit-2 covers a usage error: argparse exits 2 for one and the Rune
# runner can only exit 1, because a script signals failure by returning an
# error and every error exits 1. Such a case still has to agree on standard
# output, still has to fail in both, and still has to produce its recorded
# diagnostic. If it ever starts agreeing on the code the script says so, so
# the note gets removed.
case_() {
	local name=$1
	shift
	local known=0
	if [ "${1:-}" = "--known-exit-2" ]; then
		known=1
		shift
	fi
	# --from-stdin FILE pipes the file in and passes no path, which is how
	# both implementations are asked to read standard input.
	local from=""
	if [ "${1:-}" = "--from-stdin" ]; then
		from=$2
		shift 2
	fi
	local note=""

	if [ -n "$from" ]; then
		timeout -s KILL "$limit" python3 "$py" "$@" \
			<"$from" >"$work/py.out" 2>"$work/py.err"
	else
		timeout -s KILL "$limit" python3 "$py" "$@" \
			>"$work/py.out" 2>"$work/py.err"
	fi
	local py_code=$?
	if [ -n "$from" ]; then
		timeout -s KILL "$limit" "$rnx" run "$rn" "$@" \
			<"$from" >"$work/rn.out" 2>"$work/rn.err"
	else
		timeout -s KILL "$limit" "$rnx" run "$rn" "$@" \
			>"$work/rn.out" 2>"$work/rn.err"
	fi
	local rn_code=$?

	# A killed invocation is a failure, never a comparison.
	local timedout=0
	for code in "$py_code" "$rn_code"; do
		if [ "$code" = 124 ] || [ "$code" = 137 ]; then
			timedout=1
		fi
	done
	if [ "$timedout" = 1 ]; then
		failures=$((failures + 1))
		printf '%-20s TIMED OUT after %ss (exit %s/%s)\n' \
			"$name" "$limit" "$py_code" "$rn_code"
		return
	fi

	local out="same"
	if ! cmp -s "$work/py.out" "$work/rn.out"; then
		out="DIFFERENT"
		failures=$((failures + 1))
	fi

	local code="$py_code/$rn_code"
	if [ "$known" = 1 ]; then
		if [ "$py_code" = 2 ] && [ "$rn_code" = 1 ]; then
			code="2/1 known"
		else
			code="$py_code/$rn_code UNRECORDED"
			stale=$((stale + 1))
		fi
	elif [ "$py_code" != "$rn_code" ]; then
		failures=$((failures + 1))
	fi

	# The port's diagnostic must be exactly the one recorded for this case.
	local err="as recorded"
	local expected="expected/$name.err"
	normalize "$work/rn.err" >"$work/rn.norm"
	if [ ! -f "$expected" ]; then
		err="NO EXPECTATION RECORDED"
		failures=$((failures + 1))
	elif ! cmp -s "$work/rn.norm" "$expected"; then
		err="UNEXPECTED"
		failures=$((failures + 1))
		note=$(diff "$expected" "$work/rn.norm" | head -6)
	fi

	# Where the original succeeds, neither implementation may say anything.
	if [ "$py_code" = 0 ]; then
		if [ -s "$work/py.err" ]; then
			err="$err; original wrote to stderr on success"
			failures=$((failures + 1))
		fi
		if [ -s "$work/rn.err" ]; then
			err="$err; port wrote to stderr on success"
			failures=$((failures + 1))
		fi
	fi

	printf '%-20s exit %-16s stdout %-10s stderr %s\n' \
		"$name" "$code" "$out" "$err"
	if [ -n "$note" ]; then
		printf '%s\n' "$note" | sed 's/^/                     /'
	fi
}

case_ mixed rows.log
case_ complete rows_complete.log
case_ digest-required rows_complete.log --require-digest-equality
case_ digest-mismatch rows_digest_mismatch.log --require-digest-equality
case_ probe-floor rows_complete.log --min-probe-reduction-pct 80
case_ every-floor rows_complete.log --min-probe-reduction-pct 5 \
	--min-shuffle-reduction-pct 5 --min-cluster-reduction-pct 90
case_ inline-value rows_complete.log --min-probe-reduction-pct=80
case_ two-paths rows_complete.log rows_complete.log
case_ piped-rows --from-stdin rows.log
case_ piped-complete --from-stdin rows_complete.log
case_ piped-empty --from-stdin empty.log
case_ no-rows empty.log
case_ missing-file does_not_exist.log
case_ not-numeric rows_not_numeric.log
case_ missing-field rows_missing_field.log
case_ unknown-flag --known-exit-2 rows_complete.log --nope
case_ flag-without-value --known-exit-2 rows_complete.log --min-probe-reduction-pct
case_ value-not-a-number --known-exit-2 rows_complete.log --min-probe-reduction-pct abc
case_ value-on-a-switch --known-exit-2 rows_complete.log --require-digest-equality=true

if [ "$stale" -gt 0 ]; then
	echo "$stale case(s) no longer match their recorded exit codes; update the notes"
	exit 1
fi
if [ "$failures" -gt 0 ]; then
	echo "$failures check(s) failed"
	exit 1
fi
echo "standard output, exit codes, and diagnostics are all as recorded"
