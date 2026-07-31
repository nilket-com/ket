# Impl Plan — ADR 155: Heavy-Hitter Top-K NDV Selectivity Consumer

Pairs with `plans/adr/155_heavy_hitter_top_k_ndv_selectivity_consumer.md`. **Toron-only
consumer cut, dark** (141 pattern): new catalog fact, the equality/`IN` spend, projection
remap, fixture-seeded gates. Live descriptor sync emits empty; the Wendelon sketch producer
is **156**. No Wendelon/wire change. All anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Canonical key (reuse) | `binder.rs:4024` `InListLiteralKey` → promote to shared `ScalarLiteralKey`; `InListLiteralCardinality` keeps Key/Null/Unsupported classification |
| Consumer injection | `equality_ndv_estimated_row_count:3998`, `in_list_ndv_estimated_row_count:4041`, `estimated_rows_for_ndv:4683` |
| Plan reader | `column_ndv_for_input:4142` (mirror) |
| Catalog facts | `TableOptimizerFacts {column_ndv…}` (`catalog.rs:119`); `DescriptorOptimizerFacts` (`:131`); `for_descriptor:137`; `descriptor_optimizer_facts_from_storage:163` (dark) |
| Scan carrier | `ExecutionPlan::Scan { column_ndv: Vec<(ColumnOrdinal,_)>, column_range: Vec<…>, .. }` (`ir/execution.rs:239`) |
| Projection remap | `optimize_for_required:6059` — Scan match `:6092`, `ordinal_remap:6098`, `column_ndv`/`column_range` rewrite `:6104–6133` |
| Init sites | `binder.rs:13859` `column_ndv: Default::default()` + every `Scan`/facts initializer |
| 154 fixture | `lib.rs` skew dim + replay-signal runner |

## Phase 1 — Promote `ScalarLiteralKey` (refactoring travels with the ADR)

Move `InListLiteralKey` from binder-private to a shared `ScalarLiteralKey`, and move the
literal-classification helper with it. Keep the tri-state classification: `Key`,
`Null`, and `Unsupported`. Re-point the `IN` dedup at that helper so null handling stays
unchanged and unsupported literals still trigger the pre-existing fallback/decline path
rather than being silently ignored. **Behavior-neutral** — 148 IN tests pin
canonicalization; don't touch `-0.0`/NaN handling. Supported = the variants;
interval/calendar-interval/non-finite/binary/LargeUtf8/unit-mismatch → `Unsupported`
(fail-open).

## Phase 2 — Catalog carrier (mirror `column_ndv`, dark)

`catalog.rs`: `ColumnHeavyHitterFact { entries: BTreeMap<ScalarLiteralKey, u64> }`;
`DescriptorColumnHeavyHitterFact { column_ordinal, fact }`;
`TableOptimizerFacts.column_heavy_hitters: BTreeMap<ColumnOrdinal, ColumnHeavyHitterFact>`;
`DescriptorOptimizerFacts.column_heavy_hitters: Vec<…>` `#[serde(default)]`;
`descriptor_column_heavy_hitters` (mirror `descriptor_column_ndv`, drop out-of-schema
ordinals); **`descriptor_optimizer_facts_from_storage` emits empty** (`:163`, dark). Update all
facts initializers (`..Default::default()`). **Validation:** drop a fact with entry
`frequency==0` or `Σ frequency > base_rows`.

## Phase 3 — Scan carrier + reader + projection remap

- `ir/execution.rs`: `Scan.column_heavy_hitters: Vec<(ColumnOrdinal, ColumnHeavyHitterFact)>`
  (beside `column_ndv:239`). **New Scan field → E0063 at every construction site → `--all-targets`.**
- `binder.rs`: populate in `scan_plan` from `binding.optimizer_facts.column_heavy_hitters`;
  add `column_heavy_hitters_for_input` (mirror `column_ndv_for_input:4142`, Scan + `Filter`).
- **`optimize_for_required` (`:6092–6133`): remap `column_heavy_hitters` via the same
  `ordinal_remap` (`:6098`) as `column_ndv`/`column_range`** — drop ordinals not in `requested`.
  Pin `heavy_hitter_fact_projection_remaps_with_scan_projection`.

## Phase 4 — Consumer spend (the heart, D5)

- `equality_ndv_estimated_row_count` (`:3998`): after the literal `value` (`:4011`),
  classify it; if `column_heavy_hitters_for_input(...)` is present and valid:
  tracked key → `Some(min(frequency, B))`; supported untracked key (NDV + tail usable) →
  `Some(tail)`; null/unsupported → existing behavior. No fact → existing
  `estimated_rows_for_ndv`.
- `in_list_ndv_estimated_row_count` (`:4041`): partition deduped `literal_keys` → tracked
  matched sum `M` + supported untracked count `miss`; compute `tail` from all fact entries
  with `T=Σ all_entry_freq`; if `miss == 0`, return `M`; if `miss > 0` and tail is usable,
  return `min(B, M + min(B−T, miss × tail))`; if `miss > 0` and tail is unavailable, fall
  back to the existing behavior. No/invalid fact → unchanged.
- `heavy_hitter_uniform_tail(entries, B, ndv) = ceil((B−T)/max(D−K,1))`, checked
  (`T=Σ all_entry_freq ≤ B` from validation; `max(…,1)` no div-zero). **Never
  `EmptyRelation`** for a 0 estimate.

## Phase 5 — Tests

- Binder tests (extend descriptor-sidecar helpers with seeded `column_heavy_hitters`):
  `hot_key_equality_uses_heavy_hitter_frequency`, `untracked_literal_uses_uniform_tail_not_global_uniform`,
  `hot_key_in_list_sums_tracked_frequencies_and_uniform_tail`,
  `missing_heavy_hitter_fact_preserves_141_148_uniform_ndv_behavior`,
  `invalid_heavy_hitter_fact_zero_freq_or_over_base_is_ignored`,
  `non_finite_or_unsupported_literal_fails_open_to_existing_behavior`,
  `heavy_hitter_estimate_stays_estimate_channel_not_hard_bound`,
  `heavy_hitter_fact_projection_remaps_with_scan_projection`. Keep 141/148 gates green.
- Headline (`lib.rs`, reuse 154's replay-signal runner): seed `{region:{'hot'→5000}}` →
  `hot_key_filter_with_heavy_hitter_fact_estimates_frequency_and_hashes` — assert bind reason is
  **not** `BroadcastEstimated`, run under 154's tight cap, `replayed == false`, digest matches
  forced-hash. `stale_low_heavy_hitter_frequency_still_replays_as_hash` if cheap.

## Phase 6 — Guards (tests/architecture/optimizer.rs, adr155_*)

- `adr155_heavy_hitter_is_estimate_only_consumer` (only `estimated_row_count`; no hard bound/elision).
- retarget `adr154_uniform_ndv_math_stays_current_behavior` — 155 wrapper allowed; uniform
  fallback still routes `estimated_rows_for_ndv`.
- `adr155_no_wendelon_heavy_hitter_producer_or_count_min` — Toron consumer allowed; forbid
  Wendelon frequency/heavy-hitter producer (`crates/wendelon/src`) + Count-Min markers both rings.
- `adr155_descriptor_sync_emits_empty_heavy_hitter_facts` — `descriptor_optimizer_facts_from_storage`
  emits empty.
- keep: no public EXPLAIN surface; `build_byte_candidate` prefers `byte_upper_bound`; replay keys
  only on `BroadcastReplication`.

## Verification

```sh
cargo fmt -p toron
scripts/cargo-cgroup check -p toron --all-targets   # MANDATORY — new Scan/facts field ⇒ E0063
scripts/cargo-cgroup test -p toron heavy_hitter
scripts/cargo-cgroup test -p toron hot_key_filter_with_heavy_hitter_fact
scripts/cargo-cgroup test -p toron --test architecture_test adr155
scripts/cargo-cgroup test -p toron --test architecture_test adr141   # ScalarLiteralKey move neutral
scripts/cargo-cgroup test -p toron --test architecture_test adr148
git diff --check
```

## Risks

- **Projection remap miss** (D4) — forgetting `column_heavy_hitters` in `optimize_for_required`
  leaves stale ordinals after pushdown → mis-spent/lost fact. Pinned by the remap gate.
- **Key-promotion regression** — the `ScalarLiteralKey` move must be behavior-neutral (148 tests).
- **E0063 breadth** — new `Scan`/facts field hits every initializer; `--all-targets`.
- **Not fail-open** — present-but-invalid/mismatched fact must fall to the existing
  uniform/decline path, never panic/mis-spend.
- **Accidentally un-dark** — sync must emit empty; producer is 156. Guard it.
- **Tail math** — `Σ freq ≤ base` (validation) + `max(D−K,1)` keep the tail non-negative / no div-zero.

## Deferred (named)

Wendelon heavy-hitter **sketch producer** + wire `value_key: Vec<u8>` encoding + ingest/
compaction maintenance + coverage (ADR 156, Wendelon-home); Count-Min (error-bound ADR);
Binary/LargeUtf8 keys; range/KLL skew; `CONVERT`/units.
