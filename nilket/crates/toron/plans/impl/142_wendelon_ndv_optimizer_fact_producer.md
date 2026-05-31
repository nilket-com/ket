# Impl Plan — ADR 142: Wendelon NDV Optimizer Fact Producer (cross-ring)

**Migration for** `plans/adr/142_wendelon_ndv_optimizer_fact_producer.md`.
**Cross-ring:** Wendelon-ring producer + control RPC + Toron-ring fail-open sync wiring. (If split
at promotion: Wendelon producer subcut + thin Toron sync-wiring subcut; same contract.)
**Non-scope:** stats default-policy flip; SQL pushdown; quantile/range stats; hard-from-exact;
binder/chooser/replay/executor change; `TableSchemaDescriptor` mutation; Toron-side stats cache or
background scanner; appending facts to descriptor get/list responses.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| 141 consumer contract to satisfy | `DescriptorOptimizerFacts.column_ndv: Vec<DescriptorColumnNdvFact>`; `DistinctCountFact{Exact(u64),Approx{estimate,rel_err_ppm:u32}}`; `for_descriptor` normalizes + identity-gates (`catalog.rs`) |
| Wendelon per-contributor distinct-count + raw payloads | `distinct_count_for_column_*` (boundary accessor) + raw HLL register / small-set payloads `wendelon/src/storage/stats.rs` |
| HLL register merge (table-level) | `HllRegisters::merge` (merge registers, estimate from merged — NOT merge scalars) |
| opt-in policy (do NOT flip) | `ColumnStatsPolicyV1` default empty |
| coverage inventory | active tail + sealed L1 + tiered runs for the descriptor identity (D3) |
| descriptor get/list wire (unchanged) | `TABLE_DESCRIPTOR_GET_RESP` / `..._LIST_RESP` length-checked descriptor-only |
| new control opcodes | `TABLE_OPTIMIZER_FACTS_GET_REQ/RESP` (next free, e.g. `0x46/0x47`) |
| Toron sync seam | `WendelonDescriptorRegistry` + `sync_descriptor_for_tenant_with_facts`; live descriptor sync currently calls the descriptor-only path in `catalog.rs::resolve_table_sync_owner` |
| freshness | `schema_generation` + `physical_descriptor_fingerprint` identity (136 pattern) |

## Phases
0. **Failing gate first:** `rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed` — the
   141 headline over a **live Wendelon-produced** NDV (multi-contributor, HLL on `region`), no Toron
   seed — **fails on `main`** (live sync emits no facts). `facts_fetch_failure_syncs_descriptor_without_facts`
   + `table_without_stats… inert` green before + after.
1. **(Wendelon) Producer fact type (D1):** `TableOptimizerFactsV1 { table_object_id, schema_generation,
   physical_descriptor_fingerprint, producer_epoch, column_ndv: Vec<ColumnNdvFactV1> }`;
   `ColumnNdvFactV1 { column_ordinal, distinct: NdvFactV1 }`; `NdvFactV1 = Exact{count} | Approx{estimate, rel_err_ppm}`.
   No Toron import.
2. **(Wendelon) Coverage inventory + merge (D3/D4):** for the descriptor identity, enumerate **all
   visible contributors (active tail + sealed L1 + tiered runs)**. Per column: if **any** visible
   contributor lacks compatible stats / drifts / has incompatible HLL precision → omit. Else: union
   complete small-sets → `Exact{union_len}` (only if union stays bounded+complete); else merge **raw
   HLL registers** via `HllRegisters::merge`, estimate from the merged registers → `Approx{estimate,
   rel_err_ppm = ceil(std_err*1_000_000) clamp u32::MAX}`; else omit. **No scan of active data at sync
   time** — absent maintained stats → unknown.
3. **(Wendelon) Control RPC (D2):** `TABLE_OPTIMIZER_FACTS_GET_REQ/RESP` (separate from descriptor
   get/list; those wire contracts unchanged). Request: tenant + namespace/table + descriptor identity.
   Response: length-delimited `TableOptimizerFactsV1` blob (or empty). Server-side absence/unsupported
   → empty, not error.
4. **(Toron) Fail-open sync wiring (D5):** after the descriptor fetch, the descriptor-sync client
   issues the optional facts fetch for that exact identity; on success map `TableOptimizerFactsV1` →
   `DescriptorOptimizerFacts.column_ndv` (`NdvFactV1` → `DistinctCountFact`) and call
   `sync_descriptor_for_tenant_with_facts` (`for_descriptor` re-validates identity); on any
   failure/unsupported/empty → the descriptor-only path. **No Toron-side stats cache, no background
   scanner.** Replace the descriptor-only sync call in `resolve_table_sync_owner` with a
   facts-aware path.
5. **(Toron) Consumer untouched:** no change to `scan_plan` NDV carry, the binder recognizer, or
   `derive_estimated_byte_size` — 142 only makes real facts arrive.
6. **Guards + behavior tests** (the `adr142_*` set across both rings + the gate list); re-run
   138/139/140/141 fences.
7. **Verify:** `cargo test -p toron` · `cargo test -p wendelon` · `--test architecture_test -- adr142`
   (+ adr138..141) · `cargo check -p toron --all-targets` · `cargo check -p wendelon --all-targets` ·
   `git diff --check`.

## Producer contract (the written spec)
```
for column c at descriptor identity D:
  contributors = ALL visible contributors (active tail + sealed L1 + tiered) for D
  if any visible contributor for c lacks compatible stats / drifts / precision-mismatch: omit   // D3
  elif every contributor has a complete small-set and ∪ stays bounded+complete: Exact{|∪|}
  elif every contributor has compatible HLL: Approx{ estimate = merged_hll.estimate(),
                                                     rel_err_ppm = ceil(std_err*1e6) clamp u32::MAX }
  else: omit
expose: TableOptimizerFactsV1 via TABLE_OPTIMIZER_FACTS_GET (separate, fail-open); keyed by D
Toron: descriptor get → optional facts get(D) → map → sync_descriptor_for_tenant_with_facts | descriptor-only
```
Opt-in stats only (no default flip). Estimate-channel only. No active-data scan at sync.

## Risk register
- **Wrong-direction risk framing** (corrected): aggressive broadcast = under-estimated rows from an
  *inflated* NDV (HLL inflation/skew/stale/hot literal), bounded by 139/140 replay. A partial-subset
  NDV *under*-counts → *over*-estimates rows → conservative. So all-contributors-or-absent is a
  **fact-honesty** fence (`adr142_ndv_requires_full_contributor_coverage`), not over-broadcast
  prevention.
- **Merging estimates instead of registers** (statistically wrong): merge raw registers, estimate
  from merged; `adr142_hll_merge_uses_registers_not_estimate_scalars`.
- **Active/sealed-L1 not covered** → would mislabel a partial fact as table-level: include them in
  the inventory; absent maintained stats → unknown; no sync-time scan; `active_l1_without_stats_makes_ndv_unknown`.
- **Facts fetch as an availability dependency:** advisory + fail-open; `adr142_fact_fetch_is_fail_open`
  + `facts_fetch_failure_syncs_descriptor_without_facts`.
- **Hard-bound leak:** producer/mapper emit only into `column_ndv` (estimate channel);
  `produced_ndv_never_writes_hard_bound` + `adr142_no_hard_bound_writes`.
- **std_err f64 across the boundary** (breaks 141 `Eq`): quantize to `rel_err_ppm`.
- **Default-policy flip / descriptor mutation / Toron stats cache / descriptor-wire change:** guarded
  (`adr142_default_stats_policy_stays_empty`, `..._not_mutated...`, `..._no_toron_side_wendelon_stats_cache`,
  `..._descriptor_get_list_wire_contracts_unchanged`, `..._optimizer_facts_use_separate_msg_type`).

## Acceptance
The 141 filtered-dim headline broadcasts over a live Wendelon-produced NDV with no Toron seed (fails
on `main`); complete small-set → `Exact`, compatible HLL → `Approx{rel_err_ppm}` via raw register
merge; partial / active-L1-without-stats / no-policy / stale-identity → no fact (141 inert); facts
fetch is a separate fail-open RPC (descriptor get/list unchanged); no default-policy flip, no
descriptor mutation, no hard-bound write, no Toron-side stats cache; the 141 consumer is unchanged;
`adr142_*` guards (both rings) + 138–141 fences green.
