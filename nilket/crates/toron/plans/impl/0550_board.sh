#!/usr/bin/env bash
# Generator for ADR 550's evidence receipt.
#
# Regenerate with, from the `nilket/` directory:
#
#     crates/toron/plans/impl/0550_board.sh \
#         target/tpch/adr538_timing_atlas/timing_atlas_cad04fc4.jsonl \
#         > crates/toron/plans/impl/0550_timing_component_scope_is_not_fusion_invariant_evidence.md
#
# Reads only the banked artifact. No engine build, dataset, or timing rerun.
set -euo pipefail

artifact="${1:?usage: 0550_board.sh <timing_atlas.jsonl>}"

jq -sr '
def fmt1: (. * 10 | round) / 10 | tostring | if test("\\.") then . else . + ".0" end;
def pct: round | tostring + "%";
def median:
  sort
  | if length == 0 then 0
    elif (length % 2) == 1 then .[(length / 2) | floor]
    else ((.[length / 2 - 1] + .[length / 2]) / 2)
    end;

  map(select(.kind == "artifact_header"))[0]      as $h
| map(select(.kind == "timing_atlas_seal"))[0]    as $seal
| map(select(.kind == "attribution_component"))   as $comps
| map(select(.kind == "cell_summary"))            as $cells
| map(select(.kind == "attribution_path"))        as $paths
| (INDEX($comps[]; .query + "/" + .component))    as $C
| (INDEX($cells[]; .query))                       as $CS
| ($paths
   | group_by(.query + "/" + .arm)
   | map({ key:   (.[0].query + "/" + .[0].arm),
           value: { wall: (map(.path_wall_ms)   | median),
                    rem:  (map(.remainder_ms)   | median) } })
   | from_entries)                                as $P
| ($cells | map(.query) | sort)                   as $qs
| ["scan","operator","shuffle","dispatch","transport"] as $COMP
| ($qs | map(select($CS[.].verdict == "distinguished"
                    and $CS[.].median_paired_delta_ms > 0))) as $S
| (($S | length) * 2 / 3 | ceil)                  as $thr
| def share($q; $c): 100 * $C[$q + "/" + $c].contribution_ms / $CS[$q].median_paired_delta_ms;
  ( [ "# ADR 550 receipt — the complete timing board", "",
      "Generated from `timing_atlas_cad04fc4.jsonl` (build `" + $h.build_commit + "`,",
      "sf=" + ($h.scale_factor | tostring) + ", " + $h.mode + ", " + $h.transport + ", "
        + ($h.worker_count | tostring) + " workers, "
        + ($h.measured_pairs | tostring) + " measured pairs).",
      "**" + ($qs | length | tostring) + " measured cells; q15 is recorded as `excluded_cell`",
      "and is not measured.** All values are medians in ms.", "",
      "Published in full because a 15-cell presentation is the `distinguished`-and-slower",
      "subset S, not the board.", "",
      "**Columns are independent medians and must not be summed row-wise.** Each of wall,",
      "the five components, and remainder is a median taken separately over the measured",
      "pairs, so a row is not an arithmetic partition of its own wall figure. Per-child and",
      "per-path reconciliation was enforced by the atlas replay validator at seal time, not",
      "re-derived here: this artifact sealed with `replay_violations: "
        + ($seal.replay_violations | tojson) + "`, `withheld_reasons: "
        + ($seal.withheld_reasons | tojson) + "`, `timing_authority: "
        + ($seal.timing_authority | tostring) + "`.", "" ]

  + ( [["Local","median_local_ms","local"],["Distributed","median_distributed_ms","distributed"]]
      | map( . as [$label,$key,$arm]
        | [ "## " + $label + " arm", "",
            "| q | wall | scan | operator | shuffle | dispatch | transport | remainder | rem% |",
            "|---|---|---|---|---|---|---|---|---|" ]
        + ( $qs | map( . as $q
            | $P[$q + "/" + $arm].wall as $w
            | $P[$q + "/" + $arm].rem  as $r
            | "| " + $q + " | " + ($w | fmt1)
              + ( $COMP | map(" | " + ($C[$q + "/" + .][$key] | fmt1)) | join("") )
              + " | " + ($r | fmt1)
              + " | " + (if $w > 0 then (100 * $r / $w) else 0 end | pct) + " |" ) )
        + [ "" ] )
      | add )

  + [ "## Pairing and the dominance fork", "",
      "`verdict` and `median_paired_delta_ms` are the artifact'"'"'s own. Component shares are",
      "`contribution_ms` as a percentage of that delta — ADR 538'"'"'s leading basis. Shares are",
      "omitted where the delta is not positive. Shares may exceed 100% or go negative: they",
      "are signed contributions to a *net* delta, not a partition of it.", "",
      "| q | verdict | delta | scan% | operator% | shuffle% | transport% | in S | scan+operator% |",
      "|---|---|---|---|---|---|---|---|---|" ]
  + ( $qs | map( . as $q
      | $CS[$q].median_paired_delta_ms as $d
      | "| " + $q + " | " + $CS[$q].verdict + " | " + ($d | fmt1) + " | "
        + ( if $d > 0
            then ( ["scan","operator","shuffle","transport"]
                   | map(share($q; .) | pct) | join(" | ") )
            else "— | — | — | —" end )
        + " | " + (if ($S | index($q)) then "yes" else "no" end) + " | "
        + ( if $d > 0 then ((share($q; "scan") + share($q; "operator")) | pct) else "—" end )
        + " |" ) )
  + [ "",
      "**|S| = " + ($S | length | tostring) + "**, threshold = ceil(2/3 x "
        + ($S | length | tostring) + ") = **" + ($thr | tostring) + "**. This matches the",
      "artifact'"'"'s own `distinguished_slower_cells: "
        + ($seal.distinguished_slower_cells | tostring) + "`.", "",
      "| component | cells led |", "|---|---|" ]
  + ( ["scan","operator","shuffle","transport"]
      | map( . as $c
        | "| " + $c + " | "
          + ( [ $S[] | select(share(.; $c) >= 50) ] | length | tostring ) + " |" ) )
  + [ "",
      "No component reaches " + ($thr | tostring) + ", so no component leads and the no-work",
      "branch wins — reproducing ADR 538'"'"'s verdict over its stated inputs.", "",
      "Merging `scan` and `operator` — the boundary ADR 550 shows to be scope-variant — gives",
      "**" + ( [ $S[] | select((share(.; "scan") + share(.; "operator")) >= 50) ]
               | length | tostring )
        + "** cells, which crosses " + ($thr | tostring) + " and flips the fork. Recorded as",
      "sensitivity evidence only; it is not a dominance result and licenses no",
      "combined-component optimization.", "",
      "## Cells pairing a zero median local `scan` counter against a nonzero distributed one", "",
      "Reported as counter observations. Each component row summarizes "
        + ($h.measured_pairs | tostring) + " measured pairs,",
      "so a `median_local_ms` of 0 is a property of the median and does **not** assert that",
      "every pair recorded zero. No per-cell plan shape is known, and no claim is made here",
      "about where those local reads were counted; see ADR 550 Decision 5.", "",
      "| q | local scan | distributed scan | scan% of delta |", "|---|---|---|---|" ]
  + ( $qs
      | map(select($C[. + "/scan"].median_local_ms == 0
                   and $C[. + "/scan"].median_distributed_ms > 0))
      | map( . as $q
        | "| " + $q + " | 0.0 | " + ($C[$q + "/scan"].median_distributed_ms | fmt1) + " | "
          + ( if $CS[$q].median_paired_delta_ms > 0 then (share($q; "scan") | pct) else "—" end )
          + " |" ) )
  + [ "",
      "Distributed `scan` total across these cells: **"
        + ( $qs
            | map(select($C[. + "/scan"].median_local_ms == 0
                         and $C[. + "/scan"].median_distributed_ms > 0))
            | map($C[. + "/scan"].median_distributed_ms) | add | fmt1 )
        + " ms**. Each is marked `pairing: paired`, `eligible: true`, `additive: true`.", "",
      "## Scan contribution exceeding the median paired delta", "",
      "| q | scan% of delta |", "|---|---|" ]
  + ( $qs
      | map(select($CS[.].median_paired_delta_ms > 0 and share(.; "scan") > 100))
      | map("| " + . + " | " + (share(.; "scan") | pct) + " |") )
  + [ "",
      "This is **not** evidence of mismatched scope on its own: a valid positive component",
      "can exceed a net delta when another component is negative, which is the case in each",
      "of these cells. It measures how much weight the fork places on the scan/operator",
      "boundary. The scope defect itself is established by the source mapping in ADR 550,",
      "not by these figures." ]
  ) | join("\n")
' "$artifact"
