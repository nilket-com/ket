#!/usr/bin/env python3
"""Classify perf cpu-clock samples inside the grouped fold's row loop (ADR 508 step 2).

Usage:
    scripts/classify_fold_perf.py <cell> <key-prep-lines> <update-lines> <run.data>...
    scripts/classify_fold_perf.py q20 2165-2168 2173-2226 target/tpch/adr508/perf/q20_*.data

Each `<run>.data` must have a sibling `<run>_node.json` — the node-attribution profile of the same
run — because the exact `row_lookup_update` stage is the denominator, not the classified total.

**Period-weighted, not sample-counted.** `perf record -F <hz>` is adaptive: it retunes the period to
hold the rate, so samples are not equal slices of time. Each sample's `period` is its CPU nanoseconds,
so classes are summed by period and divided by the exact wall-time parent. What is left is genuine
shortfall — off-CPU time in the stage, plus work the symbol table cannot attribute — and it is
reported rather than absorbed.

**Leaf (symbol, source line), no call graph.** `fold_bound_grouped_aggregate_batch` is inlined into
its async caller, so neither dwarf nor LBR ever puts it on a stack. The leaf symbol is what
disambiguates lines shared between phases: `GroupValue`'s derive line resolves under `find_bucket_in`
(its `Hash`/`Eq`, the probe) and separately under `quicksort` (its `Ord`, which is finalize).

Classes with zero classified samples are reported as zero with a rule-of-three 95% upper bound, never
as a measured 0.0%.
"""

import json
import re
import subprocess
import sys
from collections import Counter

# Predeclared symbol rules, checked in this order. Matched as substrings of the demangled symbol.
NOT_ROW_LOOP = (
	"finalize_bound_grouped_aggregate",
	"quicksort",
	"stable_file_hash",
)
LOOKUP_GROWTH = (
	"group_table::find_bucket_in",
	"group_table::find_single_utf8_bucket_in",
	"entry_or_init",
	"lookup_entry",
	"lookup_single_utf8",
	"grow_table",
	"GroupKeyStorage",
	"key_resident_bytes",
	"BoundGroupedAggregateGroups",
	"BoundGroupedAggregateGroupState",
)
STATE_UPDATE = (
	"add_sum_value",
	"add_avg_value",
	"update_min_state",
	"update_max_state",
	"numeric_value",
	"init_bound_states",
)
KEY_PREPARATION = (
	"GroupArrayReader",
	"group_array_reader",
)

FOLD_BODY = "fold_bound_grouped_aggregate_batch"
AGGREGATE_SRC = "crates/toron/src/exec/aggregate.rs"
CLASSES = ("lookup_growth", "state_update", "key_preparation", "fold_body_unattributed")

# With zero observed events, the 95% upper bound on the expected count is 3 (rule of three).
RULE_OF_THREE = 3.0


def classify(symbol, srcline, key_prep_lines, update_lines):
	for needle in NOT_ROW_LOOP:
		if needle in symbol:
			return "other"
	for needle in LOOKUP_GROWTH:
		if needle in symbol:
			return "lookup_growth"
	for needle in STATE_UPDATE:
		if needle in symbol:
			return "state_update"
	for needle in KEY_PREPARATION:
		if needle in symbol:
			return "key_preparation"
	if FOLD_BODY in symbol:
		match = re.search(re.escape(AGGREGATE_SRC) + r":(\d+)", srcline)
		if match:
			line = int(match.group(1))
			if line in key_prep_lines:
				return "key_preparation"
			if line in update_lines:
				return "state_update"
		return "fold_body_unattributed"
	return "other"


def samples(path):
	"""Yield (period_nanos, symbol, srcline). perf emits the period and symbol on one line and the
	source line on the next; a sample whose IP resolves to no symbol has no source line."""
	raw = subprocess.run(
		["perf", "script", "-i", path, "-F", "ip,sym,srcline,period", "--full-source-path"],
		capture_output=True,
		text=True,
	).stdout
	lines = [line for line in raw.splitlines() if line.strip()]
	index = 0
	while index < len(lines):
		parts = lines[index].strip().split(None, 2)
		if len(parts) == 3 and parts[0].isdigit() and index + 1 < len(lines):
			following = lines[index + 1].strip()
			if ":" in following and not following.split()[0].isdigit():
				yield int(parts[0]), parts[2], following
				index += 2
				continue
		index += 1


def row_loop_micros(node_path):
	profile = json.load(open(node_path))
	for invocation in profile["aggregate_invocations"]:
		stages = invocation.get("stages")
		if isinstance(stages, dict) and stages.get("vocabulary"):
			for stage in stages["stages"]:
				if stage["stage"] == "row_lookup_update":
					return stage["micros"]
	raise SystemExit(f"no reconciled row_lookup_update stage in {node_path}")


def main():
	cell, key_range, update_range = sys.argv[1], sys.argv[2], sys.argv[3]
	runs = sys.argv[4:]
	key_lo, key_hi = (int(value) for value in key_range.split("-"))
	update_lo, update_hi = (int(value) for value in update_range.split("-"))
	key_prep_lines = set(range(key_lo, key_hi + 1))
	update_lines = set(range(update_lo, update_hi + 1))

	per_run = []
	for data_path in runs:
		parent_micros = row_loop_micros(data_path.replace(".data", "_node.json"))
		nanos = Counter()
		counts = Counter()
		for period, symbol, srcline in samples(data_path):
			name = classify(symbol, srcline, key_prep_lines, update_lines)
			nanos[name] += period
			counts[name] += 1
		parent_nanos = parent_micros * 1000.0
		attributed = sum(nanos[name] for name in CLASSES)
		per_run.append(
			{
				"parent_nanos": parent_nanos,
				"nanos": nanos,
				"counts": counts,
				"shortfall_nanos": parent_nanos - attributed,
			}
		)

	print(f"===== {cell}  ({len(per_run)} runs, period-weighted cpu-clock)")
	print(f"{'run':>4} {'row_loop ms':>11} {'lookup':>8} {'state':>8} {'keyprep':>8} {'foldunk':>8} {'shortfall':>10}")
	for index, run in enumerate(per_run):
		cells = "".join(f" {100 * run['nanos'][name] / run['parent_nanos']:7.2f}%" for name in CLASSES)
		print(
			f"{index + 1:>4} {run['parent_nanos'] / 1e6:>11.1f}{cells}"
			f" {100 * run['shortfall_nanos'] / run['parent_nanos']:9.2f}%"
		)

	print("  share of the exact row loop, mean and cross-run range:")
	for name in CLASSES + ("shortfall",):
		key = "shortfall_nanos" if name == "shortfall" else None
		shares = [
			100 * (run[key] if key else run["nanos"][name]) / run["parent_nanos"] for run in per_run
		]
		mean = sum(shares) / len(shares)
		observed = sum(run["counts"][name] for run in per_run) if not key else None
		if observed == 0:
			# Zero classified samples is not a measured zero. Bound it: three events is the 95% upper
			# limit on what could have been observed, priced at this run's mean period.
			total_periods = sum(sum(run["nanos"].values()) for run in per_run)
			total_samples = sum(sum(run["counts"].values()) for run in per_run)
			mean_period = total_periods / max(total_samples, 1)
			parent_total = sum(run["parent_nanos"] for run in per_run)
			bound = 100 * RULE_OF_THREE * mean_period / parent_total
			print(f"    {name:24} 0 classified samples (95% upper bound {bound:.3f}%)")
		else:
			print(f"    {name:24} {mean:6.2f}%   range {min(shares):5.2f}% .. {max(shares):5.2f}%")

	total = sum(sum(run["counts"][name] for name in CLASSES) for run in per_run)
	print(f"  total row-loop samples across runs: {total:,}")


if __name__ == "__main__":
	main()
