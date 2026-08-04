# Impl Plan — ADR 146: Hard Table-Size Optimizer Facts (Live Hard Row Upper Bounds)

**Migration for** `plans/adr/0146_hard_table_size_optimizer_facts.md`.
**Cross-ring activation** (Wendelon producer field + Toron import), mirroring
142/144. Row half only; byte half deferred. Anchors verified against `main`.

## Surfaces
| Concern | Anchor |
|---|---|
| Wendelon facts blob (add `row_upper_bound`) | `wendelon/src/schema/optimizer_facts.rs` `TableOptimizerFacts` |
| Producer (restructure) | `manager.rs:5060` `table_optimizer_facts`; `:5067` early-return on empty candidates; `:5070` `has_uncovered_local_rows`; `:5079-5097` tiered coverage → `blocks` |
| Row-count source | `ColumnStatsBlockHeader::row_count` (`stats.rs:64`, `batch.num_rows()`) |
| Toron mapper (drop `None`) | `catalog.rs::descriptor_optimizer_facts_from_storage`; `DescriptorOptimizerFacts::row_upper_bound` already exists (`:128`) |
| Known-empty normalize (unchanged) | `for_descriptor` `(None, Some(0)) => ByteCount::ZERO` |
| Consumers unlocked | `binder.rs:318` `estimate_row_source` (byte estimate anchor); `:3791` `filter_equality_ndv_estimated_row_count` (`base_rows = row_upper_bound?` — 141) |
| Elision is static-false-only (D5) | `binder.rs:9820` (est-0 ≠ EmptyRelation); `AlwaysReject` is the only EmptyRelation source |
| Guard to retarget | the ADR 142 hard-bound guard ("NDV facts don't synthesize hard bounds") |

## Phase 0 — failing gates first
- Headline `wendelon_live_row_upper_bound_enables_unseeded_small_dimension_broadcast`
  (live CSB fixture, no seed) — fails on `main`.
- `live_ndv_equality_estimate_uses_imported_row_upper_bound` — a live NDV equality
  filter estimates rows only once the row bound is imported (was inert).
- `row_zero_fact_does_not_elide_scan` — passes on `main`, must keep passing
  (guards against accidentally adding elision).

## Phase 1 — Wendelon producer
1. Add `#[serde(default)] pub row_upper_bound: Option<u64>` to `TableOptimizerFacts`.
   Add an explicit postcard fallback for the current range-capable payload without
   `row_upper_bound`, then keep the older NDV-only legacy fallback; `#[serde(default)]`
   alone is not the compatibility proof. Round-trip + current-payload +
   `legacy_facts_decode_row_bound_absent_not_zero` tests (144 pattern).
2. **Restructure `table_optimizer_facts`** so the coverage path + block load + row
   sum run **independent of** NDV/range candidate presence (relax the `:5067`
   early-return). Under the existing fence (no uncovered local rows + all tiered
   `Matching`): `row_upper_bound = checked_sum(blocks.iter().map(|b| b.header.row_count))`
   (saturate/suppress on overflow). Empty covered table → `Some(0)`. Emit a facts
   blob when `row_upper_bound.is_some()` **or** ndv/range non-empty.

## Phase 2 — Toron import
`descriptor_optimizer_facts_from_storage`: map
`row_upper_bound: facts.row_upper_bound.map(RowCount::new)` (drop hard-coded `None`);
`materialized_byte_upper_bound: None` (unchanged); identity-mismatch still drops the
fact; `for_descriptor` row-0→byte-0 unchanged.

## Phase 3 — tests
Producer (`manager.rs`, live CSB fixtures): `..._emits_row_bound_for_stat_less_table`,
`..._sums_matching_block_header_row_counts`,
`..._suppresses_row_bound_with_uncovered_local_rows` / `..._on_missing_tiered_stats`,
`..._known_empty_emits_zero_row_bound`. Schema: round-trip + legacy-decode.
Toron binder (live, unseeded): headline + `storage_optimizer_facts_map_hard_row_bound`,
`hard_row_bound_anchors_138_byte_estimate`,
`live_ndv_equality_estimate_uses_imported_row_upper_bound`,
`row_zero_fact_does_not_elide_scan`,
`materialized_byte_bound_stays_none_byte_estimated_from_rows`, identity-mismatch-drops.
Architecture: add the nine `adr146_*` guards; **retarget the ADR 142 hard-bound
guard** (NDV/range don't synthesize row bounds; only the explicit field maps; mapper
no longer hard-codes `None`).

## Phase 4 — verification
```sh
cargo test -p toron
cargo test -p wendelon
cargo test -p toron --test architecture_test adr146
cargo test -p toron --test architecture_test adr136 adr142   # consumer + retargeted fence intact
cargo check -p toron --all-targets
cargo check -p wendelon --all-targets
git diff --check
```
129–145 regressions expected green; the blob field is additive (`#[serde(default)]`).
**Watch:** the row bound now flows live — any 141/144 fixture test that assumed an
*absent* live row bound (inert equality estimate) may now see it fire; reconcile
explicitly (intended — it's the un-darkening), don't paper over.

## Risk register
- **Producer restructure (`:5067`)** — the row path must run under the *same*
  coverage fence as NDV/range; `..._suppresses_row_bound_with_uncovered_local_rows`
  is the guard.
- **Hard-bound honesty** — `row_upper_bound` must never understate live rows
  (identity + epoch + coverage; D6). Stale-low is replay-protected on the broadcast
  path, but the contract stays hard.
- **No new elision (D5)** — `row_zero_fact_does_not_elide_scan` +
  `adr146_no_scan_elision_from_row_count_fact`.
- **Byte field stays dark (D4)** — `adr146_no_materialized_byte_producer_yet` +
  `adr146_no_compressed_bytes_as_row_or_byte_bound` (the 136 trap).
- **Overflow** — checked-sum; saturate/suppress, never wrap.
```
