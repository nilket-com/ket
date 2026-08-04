# ADR 138: Estimate-Tolerant Per-Row Materialized-Size Model

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini) + operator on B′.** ADR 137
left a row-bounded relation with `row_upper_bound` set but `byte_upper_bound` untouched, so
it still hashed: the chooser is byte-only and nothing converted rows to bytes. 138 lets
Toron derive a **conservative over-count estimate** of a relation's materialized size from
its row cap + schema, in a **separate, clearly-labeled channel**, so a *string-bearing tiny
dimension* finally broadcasts — while the hard `byte_upper_bound` contract stays exactly as
129/136/137 defined it.

**Category:** Toron interior — optimizer IQ (binder-local).
**Predecessors:** 137 (row cap, spent here), 136 (the hard `byte_upper_bound` contract this
protects + descriptor row facts as an estimate input), 129 (do-no-harm inference doctrine —
the residual is measured against it), 134 (EXPLAIN / reason receipts — extended to label
estimated inference), 132/130 (the byte chooser).
**Migration:** `plans/impl/0138_estimate_tolerant_per_row_materialized_size.md`.

## Doctrine — Toron estimates; only the storage ring issues hard byte facts
> The runtime caps a broadcast build against the **encoded Arrow IPC payload byte length**
> (`PartitionPayload::size_bytes() = bytes.len()`, the `StreamWriter` output;
> `partition_store.rs:155`), metering **actual** bytes and failing with a typed
> `BudgetExhausted` — never wrong rows. Toron, at plan time, cannot *prove* a bound on that
> encoded unit (IPC schema/message framing is not in hand, and for tiny relations it can
> exceed the data). So a Toron-computed `rows × width` is honestly an **estimate**, not a
> hard bound — and it belongs in its own field, never in `byte_upper_bound`. Hard byte facts
> come only from the storage ring (136/136-W), which holds the actual materialized bytes.
> This is [[logical_identity_receipt_doctrine]]: enforce (broadcast) only with a receipt that
> declares its own provenance — hard *inferred* vs *estimated*.

## Why this is safe enough to ship before the runtime fallback (the grounded reframe)
The operator's steer — *a rough estimate beats no estimate; "always hash when no hard bound"
is /dev/urandom* — is grounded-correct against the runtime:
- Undercount is **not** a correctness risk: broadcast replicates the build correctly
  regardless of size; being wrong is purely a **resource** concern. "Undercount is a
  correctness bug" (129/136 framing) is **mis-stated for the distributed path** — it is an
  *availability* concern, caught gracefully.
- The engine already tolerates an imprecise planner: `default_auto_broadcast_cap_ceiling() =
  broadcast_build_bytes_per_task / 4` (`binder.rs:4079`). The **hard ceiling is `cap/4`
  (16 MiB)**; the **shipped default threshold is lower (1 MiB)** (`binder.rs:69`), so default
  headroom vs the 64 MiB runtime cap is ~64×. That margin exists *so the planner can be
  wrong*.
- **Residual, named not hidden:** there is no runtime broadcast→hash fallback (strategy is
  concrete by binding, 129). So an estimate that undercounts past the headroom on a
  *genuinely large* input fails the query (`BudgetExhausted`) where hash would have
  succeeded — a bounded **availability** regression, not wrong rows. Retired by **ADR 139**.

## Decisions

**D1 — One new estimate channel; all Toron-derived sizes are estimates.**
`PlanOptimizerFacts` gains `estimated_byte_size: Option<ByteCount>` beside the **untouched**
hard `byte_upper_bound`. 138 **never writes `byte_upper_bound`** (or
`materialized_byte_upper_bound`) — that channel stays exclusively producer-fed. A
fully-fixed-width schema yields a *tight* estimate; a string/binary-bearing schema a *looser*
one; both live only in `estimated_byte_size`. (This demotes iter-3's "fixed-width = hard
exact" after Codex grounded that Toron cannot prove the encoded-IPC byte unit — fixed-width
is an exact-*ish* estimate of in-memory size, still only an estimate of the metered unit.)

**D2 — The estimate is over-count biased, validated against the runtime unit.**
`estimated_byte_size = row_upper_bound × Σ per_row_estimate(col) + overhead`. Fixed-width
columns contribute their exact Arrow width; `Utf8`/`Binary` a conservative default payload +
offset (4B) + validity allowance; `LargeUtf8`/`LargeBinary` a larger default + 8B offset;
**any nested/dictionary/unsupported column ⇒ the whole estimate is `None`** (one unsupported
column disables it). Plus 64-byte buffer padding + per-column validity + a fixed
RecordBatch/IPC-framing allowance so even a 1-row relation over-counts the **encoded** payload
— the honesty gate proves `estimated_byte_size ≥ encode_record_batch(batch).len()` for
representative schemas at `R ∈ {1, 8, 1000}`. Bias must be **upward**; saturating arithmetic.

**D3 — Chooser: hard wins, estimate fills absence, distinct receipt.** Shape stays
byte-driven (137's chooser-byte-only invariant preserved — it reads byte-typed fields only,
never `row_upper_bound`):
- hard `byte_upper_bound ≤ threshold` → broadcast, reason `broadcast_inferred` (unchanged);
- else `estimated_byte_size ≤ threshold` → broadcast, new reason **`broadcast_estimated`**
  (carries estimated bytes + threshold);
- else hash. If both exist the **hard bound is authoritative**; the estimate only fills its
  absence, never overrides it.

**D4 — Receipts/EXPLAIN stay honest (134).** New `JoinStrategyDecisionReason::BroadcastEstimated`
→ label `"broadcast_estimated"`, flowing through the existing
`join_strategy_decision_reason_label` into EXPLAIN. An estimate-driven broadcast is never
shown as a hard materialized bound. Reason types stay crate-private / off-wire (134 guards).

**D5 — Derivation sites = where a hard row cap meets a single-base schema.** Estimate is
computed at `from_table` (descriptor `row_upper_bound` present, single-base) and re-computed
after 137's `with_pk_row_cap` (row → 1), and **carried** through 137's row-local preserve
helpers (Filter preserves; column-subset Project preserves; expr/dup Project drops it, like
byte). Binder-local; no new fact unit beyond the one field, no `ExecutionPlan`/wire/
fragmenter/scheduler/Wendelon change. **Default threshold unchanged** (don't spend the
headroom this ADR leans on).

## `Take` stays out (grounded, dropped from the cut)
`LogicalPlan::Take` returns `PlanOptimizerFacts::take(..) = composite_with_row(..)` —
`PlanOrigin::Composite` (`binder.rs:243/540`) — and `choose_auto_join_strategy` reads build
bytes **only** when `origin == SingleBaseRelation` (`binder.rs:3949/3957`). So a `LIMIT n`
relation cannot broadcast under the current chooser regardless of any byte fact; making it
broadcast requires preserving single-base lineage through `Take` or a chooser exception —
**both retarget ADR 129's "Take carried, not scheduled on" decision**, outside this estimate
cut. Dropped (correcting an earlier "fold 116" framing that assumed Take preserved
single-base lineage); a guard pins that Take stays `Composite` and hashes. Named follow-on.

## Scope fence
In: the `estimated_byte_size` channel; the over-count estimator (fixed exact-ish + var-width
conservative defaults); derivation at `from_table` + post-PK-cap with carry-through; the
`broadcast_estimated` receipt. Out (correctly): writing `byte_upper_bound` from any Toron
derivation; `Take`/`LIMIT` broadcast (needs a 129-retargeting lineage decision); the runtime
broadcast→hash fallback (→ 139); a length-carrying refinement type-system feature (would make
var-width a *hard* bound); raising the default threshold; multi-column/histogram selectivity;
join reordering; 136-W.

## Behavior gates (Rune-first per [[rune_is_primary_interface]])
- `string_bearing_tiny_dim_infers_broadcast_by_estimate` — **headline (Rune)**: `fact ⋈ dim`
  where `dim` has `Utf8` columns and a **hard row source** (descriptor sidecar
  `row_upper_bound`, *not* a non-PK filter) → estimated size under threshold → broadcast with
  reason `broadcast_estimated`. Fails on `main` *and* under any fixed-width-only framing. SQL
  twin.
- `pk_filtered_string_dim_broadcasts_by_estimate` — a single-column-PK equality (137 row=1) on
  a string-bearing table derives the estimate from the proven cap and broadcasts (the row
  source is the PK cap, not selectivity).
- `estimated_size_over_counts_encoded_payload` — **honesty gate**: `estimated_byte_size ≥
  encode_record_batch(batch).len()` for representative schemas (multi-column, nullable) at
  `R ∈ {1,8,1000}` — proven against the **encoded IPC unit the runtime caps**.
- `estimated_broadcast_receipt_is_labeled_estimated` — EXPLAIN shows `broadcast_estimated`,
  distinct from `broadcast_inferred`.
- `hard_byte_bound_still_wins_over_estimate` — a 136 hard `byte_upper_bound` keeps its
  `broadcast_inferred` path; the estimate doesn't override it.
- `estimate_above_effective_threshold_hashes` — an over-counted dim above 1 MiB hashes.
- `take_limited_dim_still_hashes` — `LIMIT n` stays `Composite` and hashes (lineage unchanged).
- `broadcast_estimate_over_cap_fails_budget_not_wrong_rows` — documents the residual via the
  existing over-cap runtime test (no invented success).
- Regressions: 129–137 green, incl. the two 137 honesty guards (un-retargeted).

## Architecture guards
- `adr138_byte_upper_bound_not_written_by_toron_derivation` — no derivation path assigns
  `byte_upper_bound`/`materialized_byte_upper_bound`; Toron-derived sizes live only in
  `estimated_byte_size`.
- `adr138_estimate_and_derivation_live_in_one_helper`.
- `adr138_estimate_over_counts_encoded_payload_unit` — the honesty gate targets
  `encode_record_batch(..).len()`, not only `get_array_memory_size()`.
- `adr138_chooser_prefers_hard_then_estimate_and_labels_estimated` — and reads byte-typed
  fields only (137 chooser-byte-only preserved; no `row_upper_bound` read).
- `adr138_take_lineage_unchanged` — `take` stays `composite_with_row`; chooser still requires
  `SingleBaseRelation`.
- `adr138_default_threshold_unchanged`.
- `adr138_no_substrate_or_wire_change`.

## Non-goals
Hard Toron-side byte facts (only the storage ring issues those); `Take`/`LIMIT` broadcast;
the runtime fallback (139); the length-refinement feature; raising the default threshold;
multi-column/range/histogram selectivity; join reordering; 136-W; any substrate/wire/
scheduler change.

## Council history
iter-0: 3-of-3 on the width model. iter-1 (Claude dual): scoped it hard, fixed-width-only,
Utf8 out. **Operator steer:** a rough estimate beats no estimate. iter-2: Claude grounded the
runtime (actual-byte metering, graceful `BudgetExhausted`, no wrong-answer risk, deliberate
`/4` tolerance band) and reframed to an over-count estimate; Codex corrected `/4` is a
*ceiling* not the 1 MiB default threshold, and **do not collapse hard bounds into estimates**
(separate labeled channel); operator voted **B′**. iter-3: Codex grounded two promotion
blockers — `Take` is `Composite` so it cannot broadcast without a 129-retargeting lineage
decision (**dropped**), and Toron cannot prove the **encoded-IPC byte unit** the runtime caps,
so fixed-width must not claim `byte_upper_bound` (**demoted to the estimate channel**, matching
Gemini iter-2). Final: all Toron-derived sizes are estimates, hard channel stays producer-only,
headline gate names a real hard row source. [[feedback_concession_cycle_ground_not_headcount]]
(conceded to grounding, not headcount), [[feedback_human_steer_catches_the_wrong_cut]],
[[feedback_ground_the_premise_before_sketching_scope]] (Take lineage + IPC unit grounded
before locking), [[logical_identity_receipt_doctrine]].

## Follow-ons
**ADR 139** — runtime broadcast-build → hash fallback (catch the build-side `BudgetExhausted`
and degrade *that join* to hash instead of failing the query; independently hardens every
existing hard-bound broadcast against a mis-set cap; the keystone that makes any estimate safe
to be wrong). Then: a length-carrying logical-type refinement (would promote var-width to a
*hard* bound); `Take`/`LIMIT` single-base lineage (129-retarget) so limited dims broadcast;
narrowing-Project estimate tightening; multi-column/histogram selectivity; join reordering;
136-W Wendelon hard-facts producer.

## Promotion criteria
A Rune (+SQL) string-bearing tiny dim infers broadcast via a labeled *estimated* receipt from
a hard row source (fails on `main` and under any fixed-width-only framing); the estimate
over-counts the **encoded** IPC payload for representative schemas at `R ∈ {1,8,1000}`; EXPLAIN
distinguishes estimated from hard; a hard 136 bound still wins; an over-threshold estimate
hashes; `Take` stays `Composite`/hashes; the over-cap residual is documented via the existing
runtime test; no Toron derivation writes `byte_upper_bound` (guarded); the estimate +
derivation live in one helper; default threshold unchanged; 137's chooser/cap honesty guards
stay green un-retargeted; the seven `adr138_*` guards + 129–137 guards green; `cargo test -p
toron`, `--test architecture_test -- adr138`, `cargo check -p toron --all-targets`, `git diff
--check` all green.
