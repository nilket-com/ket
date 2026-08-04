# Impl Plan — ADR 144: Labeled Range Selectivity from Maintained Min/Max

**Migration for** `plans/adr/0144_range_selectivity_from_min_max.md`.
**Cross-ring:** Wendelon producer-exposure (additive on the 142 facts blob) + Toron
mapping/consumer. Mirrors the 142 NDV pattern.
**Non-scope:** KLL/quantile/histogram facts; UTF-8/binary/float/decimal/boolean
range; date/timestamp *consumer* decode; hard-from-range; multi-column; join
selectivity; `TableSchemaDescriptor` mutation; new RPC; binder chooser/executor
change.

## Pre-flight anchors (grounded against `main`)
| Concern | Anchor |
|---|---|
| Wendelon facts blob (add `column_range`) | `wendelon/src/schema/optimizer_facts.rs` (`TableOptimizerFacts`, `ColumnNdvFact` to mirror) |
| Always-on min/max + counts source | `wendelon/src/storage/stats.rs:159` `CanonicalStatsValues`; `:152` `CanonicalScalar{arrow_type_tag,bytes,truncated}`; `:638` `build_always_on_column_stats_block`; `COLUMN_STAT_MIN/MAX_TRUNCATED` flags |
| Range scalar tag ownership | promote/re-export the optimizer-fact tag/decode contract through `wendelon_client`; Toron must not import `wendelon::storage::stats` |
| 142 coverage fence (reuse) | the contributor inventory 142 already computes for `column_ndv` |
| Live facts have NO hard row bound | `toron/src/catalog.rs` `descriptor_optimizer_facts_from_storage` → `row_upper_bound: None` (**the inertness driver**) |
| Toron fact map (mirror NDV) | `catalog.rs:101/108` `TableOptimizerFacts`/`DescriptorOptimizerFacts`; `:156` `descriptor_column_ndv`; `Default` sites `:2973`,`:3039` |
| Scan carry | `ExecutionPlan::Scan { column_ndv, .. }` → add `column_range` |
| Equality hook to mirror | `binder.rs:3773` `filter_equality_ndv_estimated_row_count`; `:3845` `column_ndv_for_input`; `:261` `with_estimated_row_count`; `:289` `derive_estimated_byte_size` |
| Range ops | `ir.rs:517–520` `Lt/Lte/Gt/Gte` → bound `BoundBinaryOp` |
| Guard to retarget | `binder.rs:9119` `range_filter_does_not_use_ndv_selectivity` |

## Phase 1 — Wendelon producer (additive exposure)
1. Add `column_range: Vec<ColumnRangeFact>` (`#[serde(default)]`) + the
   `ColumnRangeFact{column_ordinal, arrow_type_tag, null_count, non_null_count,
   min: Option<Vec<u8>>, max: Option<Vec<u8>>}` per ADR D1.
2. Move or mirror the supported optimizer range tag constants and signed-byte decode
   contract into `schema/optimizer_facts.rs` (or a schema module it re-exports) so
   `wendelon-client` exposes the contract and storage reuses it. Do **not** make
   Toron import Wendelon storage stats internals.
3. Do not rely on `#[serde(default)]` as old-postcard-payload compatibility. Either
   record same-version facts payloads as the deployment assumption or add an explicit
   legacy decoder that maps the ADR 142 shape to empty `column_range`; pin the chosen
   behavior in a test.
4. Populate from always-on CSB stats over **fully-covered** contributors (D3):
   sum counts; `min`-of-mins/`max`-of-maxes on canonical bytes. Omit a column if
   `non_null_count>0` and any non-empty contributor lacks non-truncated min/max.
   `non_null_count==0` → `min=max=None`. Emit only int/date/timestamp tags.

## Phase 2 — Toron mapping (no Wendelon import)
`catalog.rs`: `RangeFact{arrow_type_tag, null_count, non_null_count, min:
Option<Vec<u8>>, max: Option<Vec<u8>>}`; `column_range: BTreeMap<ColumnOrdinal,
RangeFact>` on `TableOptimizerFacts`; `DescriptorColumnRangeFact` on
`DescriptorOptimizerFacts`; map `wendelon_client::ColumnRangeFact → RangeFact`
beside `descriptor_column_ndv` (`:156`). Default empty at every `column_ndv`
default site.

## Phase 3 — carry on Scan + binder consumer
1. `ExecutionPlan::Scan` gains `column_range: Vec<(ColumnOrdinal, RangeFact)>`
   (widely-constructed → `--all-targets`, [[feedback_verify_all_targets_on_new_ir_field]]).
2. `column_range_for_input` — mirror `column_ndv_for_input` (read `Scan`, recurse
   through `Filter`).
3. `filter_range_selectivity_estimated_row_count`: collect top-level-`AND`
   `Lt/Lte/Gt/Gte` atoms grouped by `Column`; **intersect** same-column bounds to a
   clipped inclusive `[low,high]` (D6); for an `Int32/Int64` decodable column
   (decode range fact bytes through the schema-owned tag/decode contract; non-integer
   tag or `non_null_count==0` handled per D5), apply the discrete formula
   `ceil(non_null_count · overlap_width / domain_width)`, `domain_width =
   max−min+1`; `min==max` all-or-zero; contradictory bounds → 0. **Estimate from
   `non_null_count`, never `row_upper_bound`** (the inertness fix).
4. Integrate (D7): the Filter bind arm (`binder.rs:~580`) takes the **`min`** of the
   equality-NDV estimate and the range estimate → `with_estimated_row_count` →
   `derive_estimated_byte_size` (unchanged). Decode/type/coverage miss → `None` →
   today's default.
5. **Never** touch `row_upper_bound`/`byte_upper_bound` (D2); provable-empty → est 0.

## Phase 4 — tests
Toron binder (fixtures via `seed_optimizer_facts` with `column_range` incl.
`non_null_count`): the headline + `range_lt_and_gt_estimate_clipped_fraction`,
`range_between_intersects_same_column_bounds`,
`range_multiple_bounds_keep_tightest_interval`,
`range_constant_column_min_equals_max_no_div_by_zero`,
`range_outside_envelope_estimates_zero_without_hard_bound`,
`range_filter_estimates_without_row_upper_bound` (inertness),
`range_utf8_and_float_facts_fail_open`,
`range_and_equality_take_single_tightest_estimate`.
Wendelon: `range_fact_with_empty_stats_policy_is_produced_from_min_max`,
`range_fact_requires_full_contributor_coverage`, `range_fact_merges_counts_min_and_max`,
`range_truncated_min_max_omits_fact`.
Architecture (`optimizer.rs`): retarget `range_filter_does_not_use_ndv_selectivity`
→ `range_filter_uses_min_max_range_selectivity_not_ndv`; add the `adr144_*` guards,
including `adr144_range_scalar_tags_are_schema_owned` and
`adr144_range_fact_wire_compatibility_is_explicit`.

## Phase 5 — verification
```sh
cargo test -p toron
cargo test -p wendelon
cargo test -p toron --test architecture_test adr144     # file stem, not `architecture`
cargo test -p toron --test architecture_test adr141 adr142   # NDV path intact
cargo check -p toron --all-targets        # Scan column_range
cargo check -p wendelon --all-targets     # TableOptimizerFacts column_range
git diff --check
```
([[feedback_toron_architecture_test_target]], [[feedback_verify_all_targets_on_new_ir_field]].)
129–143 regressions expected green; facts payload compatibility is explicit and
guarded, not assumed from `#[serde(default)]` alone.

## Risk register
- **Inertness** — pinned by `range_filter_estimates_without_row_upper_bound`
  (estimate must derive from `non_null_count`, since live `row_upper_bound` is None).
- **Canonical-byte → i64 decode** — the one non-trivial piece; scope strictly to
  `Int32`/`Int64` tags and consume only the schema-owned optimizer-fact tag/decode
  contract exposed through `wendelon-client`. Unit-test the schema helper against
  Wendelon's storage encoder inside Wendelon; Toron tests consume the client-facing
  contract and must not import `wendelon::storage::stats`.
- **D3 coverage fence** — the aggressive-risk guard (partial/truncated merge →
  too-narrow range → over-broadcast); the Wendelon partial-coverage test is live.
- **Combination (D7)** — same-base range + equality take `min`, not product.
