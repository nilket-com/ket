# ADR 142: Wendelon NDV Optimizer Fact Producer

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini) on the cross-ring shape.** The
activation cut for ADR 141: **Wendelon produces** table-level per-column NDV optimizer facts from
its already-maintained opt-in HLL/small-set stats, and **Toron fetches them during descriptor
sync** and maps them into `DescriptorOptimizerFacts.column_ndv` — making the built-but-dark
129→141 CBO consumer live for covered opt-in tables without changing the 141 consumer.
Inert/absent when stats don't cover the whole table; never a hard bound; no stats-default flip.

**Placement:** one **cross-ring ADR** — the producer is Wendelon-ring (`crates/wendelon`), the
sync wiring + boundary mapping is Toron-ring (`crates/toron`). Promoted as ADR 142 in this
sequence per council consensus; if strict ring numbering is later wanted, the natural split is a
Wendelon producer subcut + a thin Toron sync-wiring subcut — the contract is identical
([[feedback_adr_placement_interior_exterior]], the 136-T/136-W precedent).
**Category:** Cross-ring — Wendelon optimizer-stat producer + Toron descriptor-sync consumer wiring.
**Predecessors:** 141 (the consumer contract this satisfies), 136-W (descriptor-adjacent sidecar +
sync pattern), [[feedback_cross_ring_admin_orchestration_boundary]] (Wendelon owns the production;
Toron caches no Wendelon stats state).
**Migration:** `plans/impl/142_wendelon_ndv_optimizer_fact_producer.md`.

## Doctrine — activate the dark CBO spine with already-maintained stats, honestly labeled
> 129→141 built Toron's CBO consumer, but it is production-inert: live descriptor sync emits no
> facts. 142 has **Wendelon merge its already-maintained per-column distinct-count stats into a
> table-level NDV** and expose it in a descriptor-adjacent optimizer-fact blob that Toron fetches
> and maps into the channel 141 already consumes. NDV stays **labeled-approximate** (Exact
> small-set / Approx HLL), feeds only 141's estimate channel (never a hard bound), and is emitted
> **only with full live-table contributor coverage** — a partial subset is not a table-level fact.

## Risk vocabulary (grounded — corrects the iter-1 framing)
141 estimates `ceil(base_rows / max(1, ndv))`. So the **aggressive (over-broadcast) risk is an
under-estimated row count, from an *inflated* NDV** (HLL over-estimation, skew, stale stats, or a
literal hotter than the uniform assumption) — bounded by 139/140 replay. A **partial-subset merge
*under*-counts NDV → *over*-estimates rows → conservative (safe)**. So the all-contributors-or-absent
rule (D3) is about **fact honesty and a stable optimizer contract** — a partial subset must not be
labeled a table-level NDV — **not** about preventing over-broadcast. (This corrects Claude's iter-1
and Gemini's iter-2, which both framed the coverage fence as "partial merge → optimistic → erroneous
broadcast"; the direction is reversed. The rule is unchanged; the rationale is honesty.)

## Decisions
**D1 — Wendelon owns the producer fact type (no Toron import).** A Wendelon-owned,
descriptor-adjacent payload:
```rust
TableOptimizerFacts { table_object_id, schema_generation, physical_descriptor_fingerprint,
                        producer_epoch, column_ndv: Vec<ColumnNdvFact> }
ColumnNdvFact { column_ordinal: u16, distinct: NdvFact }
NdvFact = Exact { count: u64 } | Approx { estimate: u64, rel_err_ppm: u32 }
```
Wendelon does not import Toron optimizer types; **Toron maps** this into
`DescriptorOptimizerFacts.column_ndv` and lets `for_descriptor` perform the final identity gate.
`Exact` means exact **for the covered Wendelon stats domain** — it still maps into 141's *estimate*
channel only; not a hard claim that a literal is present/absent or uniformly distributed.

**D2 — A separate, fail-open optimizer-facts fetch (not folded into descriptor get).** Descriptor
get/list frames stay descriptor-only and length-checked. Add a descriptor-adjacent facts RPC (next
free control opcodes, e.g. `TABLE_OPTIMIZER_FACTS_GET_REQ/RESP = 0x46/0x47`); the request carries
tenant + namespace/table + the descriptor identity (schema generation + fingerprint), the response
a length-delimited facts blob. **Fail-open:** fetch failure / unsupported server / stale identity /
no covered columns all degrade to "no facts" → descriptor-only sync. The endpoint is **advisory** —
never a planning availability dependency.

**D3 — Emit per-column NDV only with full live contributor coverage.** A table-level NDV requires
**every visible contributor for the descriptor** — including the **active tail and local sealed
(L1)**, not only tiered/LSM runs — to carry compatible stats for that column. Omit the column when:
any visible contributor lacks compatible stats; descriptor-identity/stats-locator validation drifts;
HLL precision/register shape differs across contributors; the column has no opt-in HLL/small-set;
or a small-set union can't stay bounded-and-complete and complete HLL coverage is unavailable.
**Do not scan active data during descriptor sync to manufacture a fact** — if active/sealed-L1 lack
a cheap maintained stats sidecar in this cut, the covered answer is unknown. Sparse rollout is the
right trade: unknown preserves behavior; a partial fact is a contract the producer can't defend.

**D4 — Merge raw stats payloads, not `DistinctCount` scalars.** For a covered column: (1) if every
contributor has a complete small-set and the union stays bounded → `Exact { count: union_len }`;
(2) else if every contributor has compatible HLL → **merge the raw registers (`HllRegisters::merge`)
and estimate from the merged registers** → `Approx { estimate, rel_err_ppm }`; (3) else omit.
Merging per-contributor *estimates* is statistically wrong — merge registers. Quantize at the
boundary: `rel_err_ppm = ceil(std_err * 1_000_000)` (clamp `u32::MAX`) — 141 chose integer error to
keep catalog facts `Eq`.

**D5 — Toron sync consumes facts; planner logic unchanged.** After a descriptor fetch, Toron's
descriptor-sync client performs the optional facts fetch for that exact identity; on success it maps
into `DescriptorOptimizerFacts` and calls `sync_descriptor_for_tenant_with_facts`, on failure the
descriptor-only path. **No Toron-side Wendelon stats cache, no background stats scanner.** No binder
/ chooser / replay / executor / SQL change — the only planner-visible difference is that 141's
existing `column_ndv` consumer now sees facts from live sync.

**D6 — No default-policy or hard-channel changes.** 142 does not flip `ColumnStatsPolicyV1::default()`
(stays empty), mutate `TableSchemaDescriptor`, write `row_upper_bound`/`byte_upper_bound`, cache
Wendelon stats in Toron, or add SQL pushdown / range selectivity / Theta sketches.

## Honesty fences
- Labeled-approximate, never hard (141 estimate channel; no hard-bound writes).
- Full-coverage-or-absent (D3) — fact honesty; a partial subset isn't a table-level fact.
- Opt-in stats only; no default-policy flip (D6).
- Advisory + fail-open (D2) — never a planning availability dependency.
- No `TableSchemaDescriptor` mutation; freshness by descriptor identity (136 pattern).

## Behavior gates (Rune-first per [[rune_is_primary_interface]])
- `rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed` — **headline/activation**: the
  141 filtered-dim broadcast fires from a **live Wendelon-produced** NDV (multi-contributor table,
  HLL on `region`), with **no Toron test seed**. Fails on `main`. (Rune + direct-IR.)
- `complete_small_set_produces_exact_ndv_fact` / `compatible_hll_contributors_produce_approx_ndv_fact`
  — the two label paths; register-merge + boundary quantization exercised.
- `partial_contributor_coverage_emits_no_ndv_fact` — a missing active/sealed/tiered contributor omits
  the column (D3).
- `active_l1_without_stats_makes_ndv_unknown` — active rows without maintained stats prevent a
  table-level fact (no sync-time scan).
- `descriptor_identity_mismatch_drops_fact` — stale facts ignored.
- `facts_fetch_failure_syncs_descriptor_without_facts` — fail-open.
- `produced_ndv_never_writes_hard_bound` — end-to-end channel honesty.
- Regressions: 138/139/140/141 fences green; the 141 consumer unchanged.

## Architecture guards (span both rings)
- `adr142_wendelon_optimizer_facts_have_no_toron_dependency`;
  `adr142_table_schema_descriptor_not_mutated_for_optimizer_facts`.
- `adr142_descriptor_get_list_wire_contracts_unchanged`; `adr142_optimizer_facts_use_separate_msg_type`.
- `adr142_default_stats_policy_stays_empty`; `adr142_ndv_requires_full_contributor_coverage`.
- `adr142_hll_merge_uses_registers_not_estimate_scalars`; `adr142_fact_fetch_is_fail_open`.
- `adr142_no_toron_side_wendelon_stats_cache`; `adr142_141_consumer_unchanged`;
  `adr142_no_hard_bound_writes`.
- `adr142_no_sql_pushdown_or_range_selectivity`; `adr142_theta_sketches_roadmap_only`.

## Non-goals
Stats default-policy flip; SQL join-filter pushdown (the named 143 candidate that unlocks SQL's
form of 141's headline); quantile/range selectivity (second stats producer); hard-from-exact
(freshness/coverage doctrine); multi-column / join selectivity; Theta-sketch join-key intersection
(roadmap-only); 140's A1.

## Council history
iter-0: 3-of-3 picked the producer — it activates the inert 141 consumer (Claude's iter-0 headline:
the spine is production-dark pending the Wendelon producers). iter-1: Claude drafted the cross-ring
dual + flagged placement. iter-2 (Codex, final): corrected the **risk direction** (aggressive
broadcast = inflated-NDV under-estimated rows, not partial-NDV undercount — which is conservative;
corrects Claude iter-1 + Gemini iter-2), the **sync wording** (separate fail-open facts RPC, not
"one RPC"), and the **merge** (raw HLL registers, not DistinctCount scalars); pinned **active+sealed-L1
coverage with no sync-time scan** and the **Wendelon-owned fact type**. Promoted on Codex's grounded
version; Gemini's reversed coverage-fence rationale surfaced and corrected (the rule is unchanged).
[[feedback_concession_cycle_ground_not_headcount]], [[feedback_cross_ring_admin_orchestration_boundary]].

## Follow-ons
SQL join-filter pushdown (143 candidate — unlocks SQL's 141 headline); a stats default-policy
decision (storage-cost product call, to widen coverage); quantile/range selectivity (second stats
producer); hard-from-exact (freshness/coverage doctrine); then the forward arc — **HLL is for
single-column NDV, not intersections; Theta sketches are the likely Wendelon family for future
join-key intersection facts** → `NDV equality selectivity (141) → 142 producer → quantile/range →
Theta-sketch join-key intersection → join selectivity → join reordering`.

## Promotion criteria
The 141 filtered-dim headline broadcasts over a **live Wendelon-produced** NDV with no Toron seed
(fails on `main`); complete small-set → `Exact`, compatible HLL → `Approx{rel_err_ppm}` via raw
register merge; partial / active-L1-without-stats / no-policy / stale-identity → no fact (141 inert);
the facts fetch is a **separate fail-open RPC** (descriptor get/list contracts unchanged); no
default-policy flip, no `TableSchemaDescriptor` mutation, no hard-bound write, no Toron-side stats
cache; the 141 consumer is unchanged; the `adr142_*` guards (both rings) + 138–141 fences green;
verification gates green: `cargo test -p toron`; `cargo test -p wendelon`;
`cargo test -p toron --test architecture_test -- adr142`; `cargo check -p toron --all-targets`;
`cargo check -p wendelon --all-targets`; `git diff --check`.
