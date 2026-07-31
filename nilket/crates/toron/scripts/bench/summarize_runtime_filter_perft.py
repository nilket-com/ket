#!/usr/bin/env python3
"""Summarize Toron runtime-filter bench rows.

The distributed_inner_equi_join bench emits one CSV-like line per case, prefixed
with adr004_distributed_inner_equi_join. This script extracts those rows and
computes the deltas ADR 006/007 discussions keep using: probe-row rejection,
shuffle-byte rejection, bytes-movement cluster time, wall time, digest parity,
and implied Bloom false-positive rate.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROW_PREFIX = "adr004_distributed_inner_equi_join,"
GROUP_KEYS = (
    "quick",
    "strategy",
    "key",
    "workers",
    "build_rows",
    "probe_rows",
    "probe_key_domain",
)
BYTES_CLUSTER_FIELDS = (
    "batch_send_ms",
    "shuffle_pull_decode_ms",
    "output_publish_ms",
    "batch_encode_ms",
)


@dataclass(frozen=True)
class BenchRow:
    values: dict[str, str]

    def text(self, key: str, default: str = "") -> str:
        return self.values.get(key, default)

    def number(self, key: str) -> float:
        try:
            return float(self.values[key])
        except KeyError as exc:
            raise ValueError(f"bench row missing required field {key}") from exc
        except ValueError as exc:
            value = self.values.get(key, "")
            raise ValueError(f"bench row field {key} is not numeric: {value}") from exc

    def integer(self, key: str) -> int:
        return int(self.number(key))

    def mode(self) -> tuple[str, str]:
        return (self.text("runtime_filter"), self.text("wendelon_runtime_filter"))

    def group_key(self) -> tuple[str, ...]:
        return tuple(self.text(key) for key in GROUP_KEYS)

    def bytes_cluster_ms(self) -> float:
        return sum(self.number(field) for field in BYTES_CLUSTER_FIELDS)


def parse_row(line: str) -> BenchRow | None:
    line = line.strip()
    if not line.startswith(ROW_PREFIX):
        return None

    values: dict[str, str] = {}
    for field in line.split(",")[1:]:
        if "=" not in field:
            continue
        key, value = field.split("=", 1)
        values[key] = value

    return BenchRow(values)


def iter_input_lines(paths: list[Path]) -> Iterable[str]:
    if not paths:
        yield from sys.stdin
        return

    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            yield from handle


def pct_reduction(before: float, after: float) -> float | None:
    if before == 0.0:
        return None
    return (1.0 - (after / before)) * 100.0


def fmt_number(value: float | int) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if abs(value) >= 1000:
        return f"{value:,.3f}"
    return f"{value:.3f}"


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}%"


def summarize_group(
    group_key: tuple[str, ...],
    rows: list[BenchRow],
    require_digest_equality: bool,
    min_probe_reduction_pct: float | None,
    min_shuffle_reduction_pct: float | None,
    min_cluster_reduction_pct: float | None,
    failures: list[str],
) -> list[str]:
    lines: list[str] = []
    group_label = ", ".join(
        f"{key}={value}" for key, value in zip(GROUP_KEYS, group_key, strict=True)
    )
    rows_by_mode = {row.mode(): row for row in rows}
    digests = {row.text("digest") for row in rows if row.text("digest")}
    digest_status = "pass" if len(digests) == 1 else "fail"

    if require_digest_equality and digest_status != "pass":
        failures.append(f"{group_label}: digest mismatch across modes")

    lines.append(f"## {group_label}")
    lines.append("")
    lines.append(f"- Digest equality: {digest_status}")
    lines.append("")
    lines.append(
        "| runtime filter | Wendelon filter | shuffle bytes | probe rows | bytes cluster ms | wall ms | digest prefix |"
    )
    lines.append(
        "| --- | --- | ---: | ---: | ---: | ---: | --- |"
    )

    for mode in (
        ("enabled", "enabled"),
        ("enabled", "disabled"),
        ("disabled", "enabled"),
        ("disabled", "disabled"),
    ):
        row = rows_by_mode.get(mode)
        if row is None:
            continue
        digest = row.text("digest")
        lines.append(
            "| {runtime} | {wendelon} | {shuffle} | {probe} | {cluster} | {wall} | {digest} |".format(
                runtime=mode[0],
                wendelon=mode[1],
                shuffle=fmt_number(row.integer("shuffle_bytes_published")),
                probe=fmt_number(row.integer("telemetry_probe_rows")),
                cluster=fmt_number(row.bytes_cluster_ms()),
                wall=fmt_number(row.number("wall_ms")),
                digest=digest[:12],
            )
        )

    baseline = rows_by_mode.get(("disabled", "disabled"))
    filtered = rows_by_mode.get(("enabled", "enabled"))

    if baseline is None or filtered is None:
        failures.append(
            f"{group_label}: missing disabled/disabled baseline or enabled/enabled filtered row"
        )
        lines.append("")
        lines.append("- Delta summary: unavailable")
        lines.append("")
        return lines

    probe_reduction = pct_reduction(
        baseline.number("telemetry_probe_rows"), filtered.number("telemetry_probe_rows")
    )
    shuffle_reduction = pct_reduction(
        baseline.number("shuffle_bytes_published"),
        filtered.number("shuffle_bytes_published"),
    )
    cluster_reduction = pct_reduction(
        baseline.bytes_cluster_ms(), filtered.bytes_cluster_ms()
    )
    wall_reduction = pct_reduction(baseline.number("wall_ms"), filtered.number("wall_ms"))

    true_matches = filtered.integer("matched_rows")
    filtered_probe_rows = filtered.integer("telemetry_probe_rows")
    baseline_probe_rows = baseline.integer("telemetry_probe_rows")
    implied_false_positives = max(0, filtered_probe_rows - true_matches)
    baseline_non_matches = max(0, baseline_probe_rows - true_matches)
    implied_fp_rate = (
        implied_false_positives / baseline_non_matches * 100.0
        if baseline_non_matches
        else None
    )

    lines.append("")
    lines.append(
        "- Filtered vs baseline: probe rows {probe}, shuffle bytes {shuffle}, "
        "bytes cluster {cluster}, wall {wall}.".format(
            probe=fmt_pct(probe_reduction),
            shuffle=fmt_pct(shuffle_reduction),
            cluster=fmt_pct(cluster_reduction),
            wall=fmt_pct(wall_reduction),
        )
    )
    lines.append(
        "- Implied Bloom false positives: {fp} / {non_matches} ({rate}).".format(
            fp=fmt_number(implied_false_positives),
            non_matches=fmt_number(baseline_non_matches),
            rate=fmt_pct(implied_fp_rate),
        )
    )
    lines.append("")

    if (
        min_probe_reduction_pct is not None
        and probe_reduction is not None
        and probe_reduction < min_probe_reduction_pct
    ):
        failures.append(
            f"{group_label}: probe-row reduction {probe_reduction:.1f}% "
            f"below {min_probe_reduction_pct:.1f}%"
        )
    if (
        min_shuffle_reduction_pct is not None
        and shuffle_reduction is not None
        and shuffle_reduction < min_shuffle_reduction_pct
    ):
        failures.append(
            f"{group_label}: shuffle-byte reduction {shuffle_reduction:.1f}% "
            f"below {min_shuffle_reduction_pct:.1f}%"
        )
    if (
        min_cluster_reduction_pct is not None
        and cluster_reduction is not None
        and cluster_reduction < min_cluster_reduction_pct
    ):
        failures.append(
            f"{group_label}: bytes-cluster reduction {cluster_reduction:.1f}% "
            f"below {min_cluster_reduction_pct:.1f}%"
        )

    return lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize runtime-filter distributed_inner_equi_join bench rows."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="raw bench logs or extracted row files; reads stdin when omitted",
    )
    parser.add_argument(
        "--require-digest-equality",
        action="store_true",
        help="exit nonzero if result digests differ across runtime-filter modes",
    )
    parser.add_argument(
        "--min-probe-reduction-pct",
        type=float,
        default=None,
        help="exit nonzero if enabled/enabled probe rows do not fall by this much",
    )
    parser.add_argument(
        "--min-shuffle-reduction-pct",
        type=float,
        default=None,
        help="exit nonzero if enabled/enabled shuffle bytes do not fall by this much",
    )
    parser.add_argument(
        "--min-cluster-reduction-pct",
        type=float,
        default=None,
        help="exit nonzero if enabled/enabled bytes-cluster time does not fall by this much",
    )
    args = parser.parse_args()

    rows = [row for row in (parse_row(line) for line in iter_input_lines(args.paths)) if row]
    if not rows:
        print("no adr004_distributed_inner_equi_join rows found", file=sys.stderr)
        return 1

    groups: dict[tuple[str, ...], list[BenchRow]] = defaultdict(list)
    for row in rows:
        groups[row.group_key()].append(row)

    failures: list[str] = []
    output: list[str] = ["# Runtime Filter Perft Summary", ""]
    for group_key in sorted(groups):
        output.extend(
            summarize_group(
                group_key,
                groups[group_key],
                args.require_digest_equality,
                args.min_probe_reduction_pct,
                args.min_shuffle_reduction_pct,
                args.min_cluster_reduction_pct,
                failures,
            )
        )

    if failures:
        output.append("## Validation Failures")
        output.append("")
        output.extend(f"- {failure}" for failure in failures)
        output.append("")

    print("\n".join(output))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
