# ADR 156: Heavy-Hitter Top-K Optimizer Fact Producer

**Status:** Promoted. **Genuine 3-of-3 (no residual divergence).** A **Toron-sequence
cross-ring optimizer-fact producer + activation** (the 142/144/147 pattern): Wendelon
maintains an opt-in per-column heavy-hitter payload and publishes exact per-key
frequencies through the table-optimizer-facts path; **Toron only maps** them into ADR
155's existing consumer carrier, replacing the dark `Vec::new()`. This makes 155's
dark heavy-hitter consumer **live**, closing `154 measured miss → 155 consumer → 156
producer` into a production fix. The binder/chooser/replay from 155 are unchanged.

## Placement (resolved 3-of-3, grounded)

**Toron-sequence ADR 156, not Wendelon relocation.** `142_wendelon_ndv_optimizer_fact_producer.md`,
`144_…`, `147_…` — the Wendelon NDV/range/float producers — all live in the Toron
`plans/adr/` sequence: cross-ring optimizer-fact producers that activate a Toron CBO
consumer are interior optimizer-fact substrate homed in the Toron aggregator. (Code lands
in `crates/wendelon/src`; "Toron-sequence" is council placement.) Codex retracted his
iter-0 relocate position on this grounding; Gemini accepted. *(Corrects the erroneous
"156 is Wendelon-home" note in the 155 roadmap.)*

## Decisions

**D1 — Cross-ring Toron ADR; Toron is activation-only.** Wendelon owns the producer +
schema-owned fact encoding; Toron owns only the storage-fact mapping into 155's carrier
(`ColumnHeavyHitterFact`, `DescriptorOptimizerFacts.column_heavy_hitters`, the
`heavy_hitter_equality_estimate`/`heavy_hitter_in_list_estimate` spend) + the end-to-end
activation proof. **No** new Toron selectivity math, hard bounds, `EmptyRelation`, EXPLAIN,
range/join selectivity, or Count-Min.

**D2 — Opt-in per-column stats policy (default-empty, budgeted).** Extend
`ColumnStatsPolicyV1` (`schema/table.rs:1326`, beside `bloom_filters`/`hll_distincts`/
`small_sets`) with `heavy_hitters: Vec<ColumnHeavyHitterSpecV1 { column_name, max_entries,
max_bytes }>` + `validate_heavy_hitter_stats_policy` (mirror `validate_small_set_stats_policy`
`:2663`: dup/unknown column reject, unsupported type reject, invalid caps reject, CSB budget
counts `max_bytes`). **No default-policy flip** — top-k counting is costlier than min/max;
the operator opts in skew-sensitive columns (141/142 posture).

**D3 — CSB payload, built at write/compaction; never request-time row scans.** Heavy
hitters are maintained stats: built while the Arrow batch is in hand at write, **recomputed
from survivors at L1/L2 compaction/retention rewrite**; `table_optimizer_facts` aggregates
maintained metadata only (the 142 no-active-scan posture). Extension via a **named CSB stat
flag** `COLUMN_STAT_HAS_HEAVY_HITTERS = 1 << 9` (retarget `COLUMN_STAT_RESERVED_MASK`) — old
payloads without the flag decode to `None`; the parser guard proves this was intentional,
not a reserved-bit leak (no silent format reuse).

**D4 — Local payload stores EXACT emitted-key frequencies + a `complete` flag. [load-bearing
correctness contract].** 155 spends a tracked `frequency` as **exact**, so 156 must never
publish a count it can't prove. Space-Saving/Misra-Gries is a **bounded candidate selector
only**; before writing the CSB, the producer **counts the emitted candidates exactly over
the in-hand batch**:
```
ColumnStatsHeavyHitterPayload { complete: bool, entries: Vec<{ value_key: Vec<u8>, frequency: u64 }> }
```
`complete = true` means every distinct non-null key fits both `max_entries` and `max_bytes`,
so the payload holds **every** distinct key and absence proves local frequency **0**.
`complete = false` → absence is **unknown**. The producer proves `complete` with exact
distinct tracking up to `capacity + 1` plus a serialized-size check while the batch is in
hand; it must not infer completeness from the candidate list length. Entries sorted
deterministically; dup/zero reject; `Σ frequency ≤ non_null_count`; string keys are full
canonical bytes (never min/max prefixes).

**D5 — Table-level merge is exact-or-omit (Codex's rule, 3-of-3).** For an opted-in column,
**every visible contributor must carry a payload or the column fact is omitted** (142 fence).
Candidate universe = union of emitted keys. Per candidate:
- contributor **has** the key → add its exact frequency;
- contributor **complete** and lacks the key → add **0**;
- contributor **incomplete** and lacks the key → **unknown → omit that key** from the
  emitted fact.
After exact merge, sort surviving keys by descending frequency with deterministic key
tie-breaks, then emit at most the policy's `max_entries` and within `max_bytes`; cap-dropped
keys stay untracked. If no keys survive, omit the column fact. **Absence from an incomplete
local top-k is unknown, not zero** — this is what keeps 155's `Σ frequency`/uniform-tail
honest; undercounting a key while calling it exact would re-create the 154 over-broadcast.
Omitted keys stay untracked → 155 uniform tail → an estimate-quality miss, never a
correctness one (replay backstops).

**D6 — Schema-owned wire facts; `ScalarLiteralKey` never on the wire.** Extend Wendelon
`TableOptimizerFacts` (`schema/optimizer_facts.rs:28`) with `#[serde(default)]
column_heavy_hitters: Vec<ColumnHeavyHitterFact>` plus an explicit
`TableOptimizerFactsWithoutHeavyHitters` postcard fallback before the existing
`…WithoutRowBound:74` and legacy decoders; `#[serde(default)]` documents the default but is
not the mixed-version compatibility mechanism. Re-export through `wendelon-client`:
```
ColumnHeavyHitterFact  { column_ordinal: u16, arrow_type_tag: u8, entries: Vec<ColumnHeavyHitterEntry> }
ColumnHeavyHitterEntry { value_key: Vec<u8>, frequency: u64 }   // canonical scalar bytes
```
Reuse Wendelon's optimizer-fact scalar encodings/tags. Stays behind the existing
`TABLE_OPTIMIZER_FACTS_GET` blob — no new RPC.

**D7 — Toron maps supported keys, fails open.** In `descriptor_optimizer_facts_from_storage`
(`catalog.rs:181`), replace `column_heavy_hitters: Vec::new()` with decode/map of
`(arrow_type_tag, value_key)` → `ScalarLiteralKey` families (Boolean; signed Int32/Int64;
Date32; Timestamp ms/us/ns; finite Float32/Float64 `-0.0`→`+0.0`; Utf8). Fail open by
dropping unsupported tags (unsigned/Date64/LargeUtf8/Binary deferred), malformed bytes,
non-finite floats, duplicates, zero frequencies, out-of-schema ordinals, or
`Σ frequency > row_upper_bound` when a base row count is present. 155's binder/chooser/replay
unchanged.

**D8 — Channel honesty + coverage intact.** Estimate-channel only (via 155); never
`row_upper_bound`/`byte_upper_bound`/`EmptyRelation`/scan pruning. The 142 all-contributors
posture: uncovered local rows / missing tiered stats / identity drift / a policied column
missing a payload → no heavy-hitter fact.

**D9 — Retarget inverted guards.** ADR 154's no-live-producer guard → producer exists,
Count-Min does not, path stays estimate-only. ADR 155's `descriptor_sync_emits_empty` guard →
absent storage facts still map empty; valid → non-empty; invalid → fail-open drop. New guards
live in `tests/architecture/heavy_hitter.rs`, **not** the already-over-ceiling `optimizer.rs`.

## Headline gate (155's dark headline turns live, unseeded)

`produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding`: a Wendelon descriptor
opts `region` into heavy-hitters; fully-covered skewed data (`region='hot'` freq 5000, NDV 100,
10_000 rows); `TABLE_OPTIMIZER_FACTS_GET` returns the valid entry; Toron sync maps
`column_heavy_hitters` **non-empty (no test seeding)** → the 154/155 join estimates the produced
frequency → hash up front, `replayed == false`. Fails on `main` (sync emits empty). Cross-ring
payoff: storage-produced evidence changes a Toron distributed-join decision.

## Supporting gates

- `table_optimizer_facts_round_trips_heavy_hitters_and_legacy_decodes_empty`;
- `heavy_hitter_stats_policy_default_empty_and_budgeted`;
- `heavy_hitter_stats_build_counts_emitted_keys_exactly`;
- `heavy_hitter_complete_flag_requires_exact_distinct_and_byte_fit`;
- `heavy_hitter_payload_complete_absence_counts_zero`;
- `incomplete_contributor_topk_absence_is_unknown_not_zero` (D5 — the proof rule);
- `table_heavy_hitter_fact_respects_max_entries_and_max_bytes`;
- `small_set_membership_is_not_used_as_frequency`;
- `partial_contributor_coverage_suppresses_heavy_hitter_fact`;
- `active_l1_without_stats_suppresses_heavy_hitter_fact`;
- `compaction_recomputes_heavy_hitters_from_survivors`;
- `unsupported_or_nonfinite_heavy_hitter_keys_fail_open`;
- `descriptor_sync_maps_supported_heavy_hitter_storage_facts` /
  `descriptor_sync_drops_invalid_zero_duplicate_or_over_base_heavy_hitter_fact`;
- ADR 155 seeded-consumer + projection-remap regressions green;
- architecture: `adr156_heavy_hitter_producer_is_opt_in_and_default_empty`,
  `adr156_heavy_hitter_csb_payload_is_stats_not_request_scan`,
  `adr156_heavy_hitter_frequencies_are_exact_for_emitted_keys`,
  `adr156_incomplete_absence_is_unknown_not_zero`,
  `adr156_no_count_min_or_approx_frequency_consumer`,
  `adr156_storage_wire_does_not_use_toron_scalar_literal_key`,
  `adr156_descriptor_sync_maps_heavy_hitters_fail_open`; retargeted 154/155 guards.

## Consensus trail

3-of-3, no residual divergence. **Placement** Toron-sequence (Claude's 142/144/147 grounding;
Codex retracted relocate; Gemini adopted). **The exact-or-decline merge proof (D4/D5)** is
Codex's sharpening — bounded sketches are candidate selectors, *absence from an incomplete
local payload is unknown, not zero* — crystallized as the `complete: bool` payload + the
exact-or-omit merge; adopted 3-of-3 (it keeps 155's exact-frequency spend honest across a
tiered/merged table). Opt-in policy, CSB-flag extension (`1<<9`), CSB-at-compaction, the wire
boundary, and the 142 coverage fence converged. **adr077/adr031 hygiene** stays a separate
commit (3-of-3) — 156's guards land in a fresh `heavy_hitter.rs`.

## Forward arc

`154 evidence → 155 consumer (dark) → 156 heavy-hitter producer (live) → equality/IN skew
correction live → KLL range-skew evidence → Theta join-key intersection → join selectivity →
reordering` · *(parallel: CONVERT/units)*.
