# Impl Plan — ADR 156: Heavy-Hitter Top-K Optimizer Fact Producer

Pairs with `plans/adr/156_heavy_hitter_top_k_optimizer_fact_producer.md`. **Cross-ring,
Toron-sequence** (142 pattern): Wendelon produces (policy + CSB payload + compaction +
manager aggregation + wire); **Toron activation is mapping-only** (155 already built the
consumer). All anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Wendelon facts | `schema/optimizer_facts.rs:28` `TableOptimizerFacts {column_ndv, column_range}` + legacy `…WithoutRowBound:74` decoders |
| Opt-in policy | `schema/table.rs:1326` `ColumnStatsPolicyV1 {bloom_filters, hll_distincts, small_sets}` + validators `:2661–2663` |
| CSB per-column payload | `storage/stats.rs:104–129` (`hll_distinct: Option<…>`, `small_set: Option<Vec<Vec<u8>>>`); CSB stat flags + `COLUMN_STAT_RESERVED_MASK` |
| Producer aggregation | `StorageManager::table_optimizer_facts` + `ndv_column_candidates` (`storage/manager.rs:592`); all-contributors coverage |
| Sketch home | `storage/sketch.rs` (HllRegisters; add Space-Saving) |
| Wire re-export | `wendelon-client` (`#[serde(default)]`) |
| Toron map (dark point) | `catalog.rs:181` `descriptor_optimizer_facts_from_storage` (`column_heavy_hitters: Vec::new()`) |
| Toron carrier+spend (155) | `catalog.rs:132/147` `ColumnHeavyHitterFact`/`descriptor_column_heavy_hitters`; `ir.rs` `ScalarLiteralKey`/`scalar_literal_key`; binder `heavy_hitter_equality_estimate`/`heavy_hitter_in_list_estimate` |
| Guards | new `crates/toron/tests/architecture/heavy_hitter.rs` + Wendelon stats tests |

## Phase 1 — Opt-in policy (schema/table.rs)

`ColumnHeavyHitterSpec { column_name, max_entries: u16, max_bytes: u32 }`; add
`heavy_hitters: Vec<…>` to `ColumnStatsPolicyV1` (`:1326`); `validate_heavy_hitter_stats_policy`
(mirror `validate_small_set_stats_policy:2663`). Default empty; no policy flip.

## Phase 2 — CSB payload + build (storage/sketch.rs, storage/stats.rs)

- `sketch.rs`: `SpaceSaving` (bounded candidate selector; `capacity = max_entries`).
- `stats.rs`: `ColumnStatsHeavyHitterPayload { complete: bool, entries: Vec<(Vec<u8>, u64)> }`;
  add `heavy_hitters: Option<…>` to the parsed entry (`:128` neighbor). Build in the stats pass:
  feed values to Space-Saving (bound) → **count emitted candidates exactly over the in-hand
  batch**; prove `complete` only when every distinct non-null key fits both `max_entries` and
  `max_bytes`, using exact distinct tracking up to `capacity + 1` plus a serialized-size check
  (never from candidate list length alone). Reject dup/zero; `Σ freq ≤ non_null_count`; keep the
  serialized payload within `max_bytes`, and mark incomplete if the byte cap drops any key.
- **CSB flag** `COLUMN_STAT_HAS_HEAVY_HITTERS = 1 << 9`; retarget `COLUMN_STAT_RESERVED_MASK`;
  old payloads (flag unset) decode `None`. Parser guard proves intentional (no reserved-bit leak).

## Phase 3 — Compaction recompute (storage/...compaction)

Recompute the heavy-hitter payload from survivors at L1/L2 rewrite (data in hand) — **do not
merge lossy sketches across tiers**; rebuild exact local payloads on the rewritten run. Pin
`compaction_recomputes_heavy_hitters_from_survivors`.

## Phase 4 — Producer aggregation (storage/manager.rs)

`heavy_hitter_column_candidates` (mirror `ndv_column_candidates:592`) gated by policy +
coverage. **Exact-or-omit merge (D5):** candidate universe = union of contributor emitted keys;
per key: present→add exact; complete+absent→add 0; **incomplete+absent→unknown→omit key**
from the emitted fact. Any contributor missing a payload / uncovered active rows → decline
(142). After exact totals are known, sort by descending frequency plus deterministic key
tie-break, retain entries within `max_entries`/`max_bytes`, and emit `ColumnHeavyHitterFact`
sorted in canonical key order for wire determinism. If no entries survive, omit the column.

## Phase 5 — Wire schema (schema/optimizer_facts.rs, wendelon-client)

`ColumnHeavyHitterFact { column_ordinal: u16, arrow_type_tag: u8, entries: Vec<{value_key:
Vec<u8>, frequency: u64}> }`; `#[serde(default)] column_heavy_hitters` on `TableOptimizerFacts`
(`:28`); add `TableOptimizerFactsWithoutHeavyHitters` as the first postcard fallback, then keep
the existing `…WithoutRowBound` and legacy decoders. Do not rely on `#[serde(default)]` alone
for old postcard payloads. Reuse the optimizer-fact scalar encoders/tags. Re-export via
`wendelon-client`. Pin
`table_optimizer_facts_round_trips_heavy_hitters_and_legacy_decodes_empty`.

## Phase 6 — Toron activation (catalog.rs — mapping only)

In `descriptor_optimizer_facts_from_storage` (`:181`) replace `Vec::new()`: per wire fact, map
`(arrow_type_tag, value_key)` → `ScalarLiteralKey` (reuse `scalar_literal_key` family rules);
drop entry on bad tag/key/non-finite/unsupported, the column fact on dup/zero/out-of-ordinal/
`Σ > row_upper_bound`; build `ColumnHeavyHitterFact { entries: BTreeMap<ScalarLiteralKey,u64> }`.
**No binder/chooser/replay change** (155 owns the spend).

## Phase 7 — Guards (new tests/architecture/heavy_hitter.rs) + retargets

`adr156_*`: opt-in/budgeted; CSB-payload-is-stats-not-scan; frequencies-exact-for-emitted;
complete-flag-requires-exact-distinct-and-byte-fit; incomplete-absence-unknown-not-zero; table-fact-respects
`max_entries`/`max_bytes`; no-Count-Min/approx-consumer; **no `ScalarLiteralKey` on the
Wendelon wire**; descriptor-sync-maps-fail-open. Retarget 154's no-producer guard (producer
exists, Count-Min absent, estimate-only) + 155's `adr155_descriptor_sync_emits_empty_heavy_hitter_facts`
(absent→empty, present→mapped, invalid→drop). Land in `heavy_hitter.rs`, **not** `optimizer.rs`
(over the adr077 ceiling).

## Phase 8 — Tests + headline

- Wendelon: build-counts-exactly; `complete` requires exact distinct proof and byte fit; `complete`-absence-zero;
  incomplete-absence-unknown; table fact cap respected; `small_set`-not-frequency;
  partial-coverage-declines; active-L1-declines; unsupported-fail-open.
- **Headline** (cross-ring integration, real manager/client seam, **no MockCatalog seeding**):
  `produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding` — opt-in region, skewed
  covered data, fetch facts, Toron sync maps non-empty, the 154/155 join hashes up front,
  `replayed == false`.
- 155 regressions green (seeded consumer math, projection remap).

## Verification

```sh
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p wendelon heavy_hitter
scripts/cargo-cgroup test -p wendelon table_optimizer_facts
scripts/cargo-cgroup test -p toron heavy_hitter
scripts/cargo-cgroup test -p toron produced_heavy_hitter_fact_makes_hot_key_filter
scripts/cargo-cgroup test -p toron --test architecture_test adr156
scripts/cargo-cgroup test -p toron --test architecture_test adr154   # retargets green
scripts/cargo-cgroup test -p toron --test architecture_test adr155   # retargets green
git diff --check
```

## Risks

- **Undercount-as-exact (D5)** — the cardinal risk: treating absent-from-incomplete as zero
  corrupts 155's exact spend → undercount → over-broadcast. Merge MUST omit unless every
  contributor proves the candidate (`incomplete_..._is_unknown_not_zero`).
- **`small_set` misuse** — distinct-values, no counts; never a frequency source.
- **CSB flag leak** — `1<<9` must retarget `COLUMN_STAT_RESERVED_MASK` + a guard; old payloads
  decode `None`; no silent reserved-bit reuse.
- **Wire-version** — explicit without-heavy-hitters fallback + older legacy decoders;
  mixed-version → empty, never panic.
- **Toron enum leak** — only `(arrow_type_tag, value_key)` crosses; `ScalarLiteralKey` internal.
- **Cost** — opt-in/budgeted (`max_entries`/`max_bytes`); no default flip.

## Deferred (named)

Count-Min / error-bounded frequencies (own ADR + consumer contract); cross-tier lossy sketch
merge (v1 recomputes at compaction); multi-column/correlated heavy hitters; range/KLL skew;
unsigned/Date64/LargeUtf8/Binary keys; adr077/adr031 hygiene (separate commit + review-checklist
fix: run the full `architecture_test` binary).
