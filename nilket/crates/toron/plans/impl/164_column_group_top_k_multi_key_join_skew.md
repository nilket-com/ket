# Impl Plan — ADR 164: Column-Group Top-K Facts for Multi-Key Join Skew

Pairs with `plans/adr/164_column_group_top_k_multi_key_join_skew.md`. One cross-ring cut (156/159
pattern): Wendelon Column-Group Top-K producer (extend 156's Space-Saving to tuple keys) + Toron
multi-key-skew consumer (exact hot tuple floor over 163) + activation. **Every seam mirrors an existing
156 producer surface and 162's validated exact hot intersection shape** — the de-risking spine. The
consumer lands provably dark (Phase 0) before the producer; if the unified feat sprawls, the 158/159 split
(dark consumer 164 → producer 165) is the documented fallback and Phase 0 keeps it cheap. Anchors verified
on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Producer collector (mirror, tuple-key) | `HeavyHitterCollector`/`finish` — `wendelon/src/storage/stats.rs` (Space-Saving + exact-or-decline `complete`) |
| Producer CSB payload + budget (mirror) | `ColumnStatsHeavyHitterPayload` — stats.rs:156; opt-in select/budget — stats.rs:1045–1270 |
| Producer tiered merge (mirror) | `heavy_hitter_fact_for_column` (exact-or-decline) — `wendelon/src/storage/manager.rs:872`; `has_uncovered_local_rows_for_descriptor` coverage |
| Producer policy (mirror) | `ColumnHeavyHitterSpec` + `heavy_hitters: Vec<…>` on `ColumnStatsPolicyV1` — `schema/table.rs` |
| Wire (add beside) | `column_heavy_hitters: Vec<ColumnHeavyHitterFact>` — `wendelon/src/schema/optimizer_facts.rs:42` |
| Consumer reader (mirror) | `clean_base_column_heavy_hitter_for_join_input:4208` |
| Consumer exact-hot shape | `HotBothJoinContribution:4238`, `hot_both_join_contribution:4465`, `heavy_hitter_fact_matches_join_key_type:4511` |
| Consumer seam (163) | `multi_key_join_cardinality:4311`; keep 163 prior, then apply a raising-only exact-hot floor |
| Live-producer headline harness | `…produced_storage_…_fact` fixture + `force_seal_active_for_test`; replay-signal runner (158/159) |
| Tuple scalar encoders (reuse) | the optimizer-fact scalar encoders (158/161); `decode_range_scalar` family |
| Guards | `crates/toron/tests/architecture/join_cardinality.rs` (adr164_*) + Wendelon `tests/` group-top-k guards |

## Phase 0 — Consumer-dark first (prove the math before producing)

Land D3 reading a `ColumnGroupTopKFact` the producer does **not** yet emit (descriptor sync emits empty for
groups). Fixture-seed a group fact in the test catalog → prove the exact-hot tuple floor; **darkness gate**
`absent_column_group_top_k_falls_back_to_163_byte_identical` asserts exact equality to 163 when absent.
Proves the hard part cheaply, and isolates the consumer so a later split stays cheap.

## Phase 1 — Consumer: exact hot tuple floor (Toron)

- `clean_base_column_group_top_k_for_join_input(input, group: &[ColumnOrdinal]) ->
  Option<JoinOrderedColumnGroupTopK>`: root-`Scan` only, projection-remap each requested output ordinal,
  match a fact whose group exactly equals the requested descriptor-column set, and permute tuple
  components into requested join-key order.
- `hot_tuple_join_contribution(left: &ColumnGroupTopKFact, left_rows, left_types: &[DataType], right, right_rows,
  right_types)`: mirror `hot_both_join_contribution:4465`, key = *tuple* (`TupleKey`); validate arity == key
  count, each component tag ↔ key `DataType`, freq-sum ≤ rows, nonzero; iterate the smaller entries map;
  `Σ freq_L(t)·freq_R(t)`; checked `u128`; any problem → `None` (fall to 163).
- In `multi_key_join_cardinality:4311`: `prior = <current 163 estimate>`; if a group fact matches the join
  key set on both sides → `hb = hot_tuple_join_contribution(...)`; return `Some(max(prior, hb.rows))`.
  No fact / `H=∅` → `prior` (byte-identical to 163). If `prior == 0`, return 0 before spending grouped
  sidecars.

## Phase 2 — Producer: group Space-Saving (Wendelon, extend 156)

- `ColumnGroupTopKSpec { column_names: Vec<String>, max_entries: u16, max_bytes: u32 }` on
  `ColumnStatsPolicyV1` (mirror `ColumnHeavyHitterSpec`, `#[serde(default)]`, brand-clean).
- Group collector: at seal, tuple key = `Vec<Vec<u8>>`, one full canonical optimizer-fact scalar byte
  vector per component, paired with group `arrow_type_tags`; **skip any row with a null key component**
  (can't equi-join); insert into a `SpaceSaving<Vec<Vec<u8>>>`; `finish` sets `complete`
  (exact-or-decline). CSB encode/decode anchors the grouped payload on the lowest group ordinal's
  `ColumnStatsBlockEntry`, while the payload carries the full group identity; add to opt-in budget
  (lowest priority).
- `column_group_top_k_fact_for_group` (mirror `heavy_hitter_fact_for_column:872`): exact-or-decline tiered
  merge — exact tuple frequency for tuples in all contributors, decline otherwise; all-contributors
  coverage (else omit). Wire into `table_optimizer_facts`.

## Phase 3 — Wire + descriptor sync + activation (flip dark → live)

- `ColumnGroupTopKFact { column_ordinals: Vec<u16>, arrow_type_tags: Vec<u8>, entries:
  Vec<ColumnGroupTopKEntry> }` on the `TABLE_OPTIMIZER_FACTS_GET` response (beside
  `column_heavy_hitters`), where `ColumnGroupTopKEntry { value_keys: Vec<Vec<u8>>, frequency }`.
  Collector internals stay Wendelon-private; the wire remains schema-owned and does not mention Toron's
  `ScalarLiteralKey`.
- Descriptor sync: map storage group facts → Toron grouped facts (validate group ordinals, tags, tuple
  arity, decoded keys, duplicates, nonzero frequencies, and row-bound total; drop malformed). **Flip the Phase-0 dark emit** → live mapping — the single
  behavior-change moment.

## Phase 4 — Fixtures + gates

- Headline `live_column_group_top_k_recovers_multi_key_skew_and_hashes_without_seeding` (live producer, ≥2
  slabs, co-hot tuple → hash, no replay; 163-shape on `main` → broadcast → replay).
- `absent_column_group_top_k_falls_back_to_163_byte_identical` (darkness, exact equality — Phase 0);
- `hot_tuple_contribution_is_exact_product_of_frequencies`; `consumer_uses_exact_hot_floor_not_fake_residual`;
- `column_group_top_k_merges_across_two_slabs`; `column_group_top_k_declines_on_partial_coverage`;
- `multi_key_skew_never_lowers_163_prior`; `column_group_top_k_validates_tuple_arity_and_types`;
  `group_must_match_full_join_key_set`; `group_order_normalizes_to_join_key_order`;
- `tuple_entries_are_schema_owned_and_scalar_literal_key_stays_toron_private`; `…_spec_names_are_brand_clean`;
- `group_top_k_csb_payload_is_anchored_not_format_bumped`;
- keep 160/161/162/163 `join_cardinality` + 155/156 + 158/159 + 144/147 regressions green.

## Phase 5 — Guards

- `adr164_group_top_k_is_estimate_channel_only`; `adr164_consumer_dark_without_producer` (darkness/
  byte-identical pin); `adr164_exact_or_decline_not_approximate` (top-k, not KLL);
  `adr164_consumer_uses_exact_hot_floor_not_fake_residual`; `adr164_group_top_k_sketch_is_wendelon_private`;
- `adr164_group_top_k_wire_does_not_use_toron_scalar_literal_key`; `adr164_group_top_k_csb_payload_is_anchored_not_format_bumped`;
- retarget 163's `adr163_no_wendelon_tuple_top_k_surface` → "group top-k owned by 164, brand-clean";
- `adr136` markers unchanged. Producer guards in `crates/wendelon/tests/`.

## Verification

```sh
scripts/cargo-cgroup check -p wendelon --all-targets       # new spec/fact/wire field ⇒ E0063 sweep
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron absent_column_group_top_k_falls_back_to_163_byte_identical
scripts/cargo-cgroup test -p toron live_column_group_top_k_recovers_multi_key_skew
scripts/cargo-cgroup test -p wendelon column_group_top_k
scripts/cargo-cgroup test -p toron join_cardinality
scripts/cargo-cgroup test -p toron  --test architecture_test adr164
scripts/cargo-cgroup test -p toron  --test architecture_test adr163     # retargets green
scripts/cargo-cgroup test -p toron  --test architecture_test            # FULL binary (expect 0 failed)
scripts/cargo-cgroup test -p toron  --test architecture_test adr136     # markers unchanged for groups
git diff --check
```

## Risks

- **Scale (~159+162)** — if the unified feat sprawls, fall back to the 158/159 split (dark consumer 164 →
  producer 165); the Phase-0-dark structure keeps that split cheap (the consumer is already isolated).
- **Tuple key comparability** — L and R must decode and normalize the tuple identically (same scalar
  encoders, explicit type tags, full-group match, join-order permutation). Pinned by
  `…validates_tuple_arity_and_types` + `group_order_normalizes_to_join_key_order`.
- **Null key component** — null in any tuple component → exclude the row (can't equi-join, not a hot-tuple
  contributor). Confirm at producer insert.
- **Darkness** — absent group fact must be *exactly* 163 (not `Some(0)`/degenerate); the byte-identical
  gate is the proof.
- **Residual overclaim** — do not subtract hot tuples from 163's independence denominator until a composite
  tuple-NDV fact exists; `max(prior, exact_hot_tuple)` is pinned.
- **CSB anchoring confusion** — anchor column is only storage location; payload carries full group identity.
- **E0063 breadth** — new `ColumnStatsPolicyV1` field + wire fact hits Wendelon initializers; `--all-targets`
  both crates.

## Deferred (named)

N-column groups beyond the headline 2 (the tuple key generalizes; confirm budget); Float/Date/Timestamp
tuple components (Int64 first); composite-NDV (residual 2) and Theta key-set overlap (residual 3); the
measured residual-evidence cut (A, if wanted); multi-group-per-join; the Rune cockpit group-skew gauge.
