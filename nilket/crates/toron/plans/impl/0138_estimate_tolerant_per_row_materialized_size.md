# Impl Plan — ADR 138: Estimate-Tolerant Per-Row Materialized-Size Model

**Migration for** `plans/adr/0138_estimate_tolerant_per_row_materialized_size.md`.
**Scope:** `binder.rs` (one new fact field + estimator + derivation helper + two derive
sites + carry-through in 137's helpers + chooser estimate branch + `BroadcastEstimated`
reason) + EXPLAIN label (134 surface, label fn only) + tests/guards.
**Non-scope:** writing `byte_upper_bound` from any Toron derivation; `Take`/`LIMIT` broadcast;
the runtime broadcast→hash fallback (→ 139); raising the default threshold; the
length-refinement feature; `Sort` (`:516`, inert per 137); fragmenter/scheduler/wire/
Wendelon; 136-W.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| facts type (add estimate field) | `PlanOptimizerFacts { origin, byte_upper_bound, row_upper_bound }` `binder.rs:157` |
| hard contract to preserve (never write) | `byte_upper_bound`/`materialized_byte_upper_bound` `catalog.rs:80/89` |
| chooser (byte-driven; single-base gate) | `choose_auto_join_strategy` `binder.rs:3941`; build bytes only if `origin==SingleBaseRelation` `:3949/3957`; threshold `:3942` |
| effective threshold (don't raise) | `effective_auto_broadcast_threshold` `:4073` = `min(config 1 MiB, cap/4=16 MiB)` = 1 MiB (`binder.rs:69`, `binder.rs:4079`) |
| reason enum (add variant) | `JoinStrategyDecisionReason` `:283` (`BroadcastInferred`/`HashByDefault`/`PinnedByAuthor`); label fn `:4543` → add `"broadcast_estimated"` |
| Take is Composite (out) | `PlanOptimizerFacts::take` `:243` → `composite_with_row`; arm `:540` |
| 137 row-local helpers (carry field) | `preserve_volume_non_increasing` / `preserve_through_project` / `with_pk_row_cap` (137) |
| 137 invariants (keep green) | `with_pk_row_cap` byte-untouching; chooser reads no `row_upper_bound` |
| encoded byte unit (honesty gate) | `PartitionPayload::size_bytes()=bytes.len()` `partition_store.rs:155`; `encode_record_batch` `distributed/arrow_ipc.rs:42` (StreamWriter, uncompressed) |
| from_table base facts | `PlanOptimizerFacts::from_table` `:171` (stamps `SingleBaseRelation`) |

## Phases
0. **Failing gates first:** `string_bearing_tiny_dim_infers_broadcast_by_estimate` (Rune +
   SQL) fails on `main` (no estimate channel → string dim hashes). `hard_byte_bound_still_wins`
   and `take_limited_dim_still_hashes` green before + after.
1. **Add the channel:** `PlanOptimizerFacts.estimated_byte_size: Option<ByteCount>` (default
   `None`). Thread `None`/carry through every constructor (`composite_with_*`, `from_table`,
   `empty_relation`, exchange, 137's preserve/cap helpers). **`cargo check -p toron
   --all-targets`** (new fact field touches many sites incl. benches —
   [[feedback_verify_all_targets_on_new_ir_field]]).
2. **`per_row_estimate_bytes(&DataType) -> Option<usize>`** — explicit match. Fixed-width →
   exact Arrow width; `Utf8`/`Binary` → `UTF8_DEFAULT_PAYLOAD + 4 (offset)`; `LargeUtf8`/
   `LargeBinary` → `LARGE_UTF8_DEFAULT_PAYLOAD + 8`; `Boolean → 1`; nested/dict/REE/unsupported
   → `None`. Named over-count consts; unit-test each arm + representative `None`s.
3. **`PlanOptimizerFacts::derive_estimated_byte_size(self, schema) -> Self`** (D1/D2, the one
   helper): `let Some(R)=self.row_upper_bound else {return self}`; sum
   `per_row_estimate_bytes` (any `None` ⇒ return self unchanged, estimate stays `None`);
   `est = Σ_c [pad64(R*w_c) + pad64(div_ceil(R,8))] + RECORD_BATCH_IPC_FRAMING_OVERHEAD`
   (saturating); `estimated_byte_size = Some(min(existing?, est))`. **Never touches
   `byte_upper_bound`.**
4. **Derive sites (D5):** `from_table` (after base facts, when `row_upper_bound` is `Some`) →
   `derive_estimated_byte_size(&schema)`; Filter PK path → after `with_pk_row_cap()` chain
   `.derive_estimated_byte_size(&plan.schema())` (re-derive on row→1). Carry the field through
   137's `preserve_volume_non_increasing` (returns self → carries automatically) and
   `preserve_through_project` (subset preserves; non-subset drops `estimated_byte_size`
   alongside byte). **Do not** wire `Take`.
5. **Chooser + receipt (D3/D4):** in `choose_auto_join_strategy`, per build side with
   `origin==SingleBaseRelation`: if hard `byte_upper_bound` present use it
   (`BroadcastInferred`); elif `estimated_byte_size` present and `< threshold` →
   `BroadcastEstimated { estimated_byte_size, threshold }`; else fall through to hash. Hard
   authoritative when both. Add the `BroadcastEstimated` variant + its `"broadcast_estimated"`
   label in `join_strategy_decision_reason_label`; thread into EXPLAIN render. **Do not** read
   `row_upper_bound` in the chooser; **do not** change the threshold.
6. **Guards + tests:** the seven `adr138_*` guards (esp. `…_byte_upper_bound_not_written…`,
   `…_take_lineage_unchanged`, `…_estimate_over_counts_encoded_payload_unit`) + the gate list;
   re-run 129–137 incl. the two 137 honesty guards.
7. **Verify:** `cargo test -p toron` · `--test architecture_test -- adr138` (+ adr129..137) ·
   `cargo check -p toron --all-targets` · `git diff --check`.

## Estimator & formula (the written spec)
```
hard  byte_upper_bound   : 129/136/137 contract — producer-fed only; 138 NEVER writes it
new   estimated_byte_size: over-count-biased Toron estimate; chooser uses it only when hard absent

per col c, row cap R:
  values_c   = pad64(R * per_row_estimate_bytes(c))
  validity_c = pad64(div_ceil(R, 8))
  est        = Σ_c (values_c + validity_c) + RECORD_BATCH_IPC_FRAMING_OVERHEAD
  estimated_byte_size = min(existing?, est)        # any unsupported col ⇒ estimate stays None
chooser: hard ≤ thr → broadcast_inferred ; elif est ≤ thr → broadcast_estimated ; else hash
```
All saturating. `RECORD_BATCH_IPC_FRAMING_OVERHEAD` + var-width defaults are named consts
chosen so the honesty gate (`est ≥ encode_record_batch(batch).len()`) passes with margin at
`R ∈ {1,8,1000}`, including the worst case for over-count: tiny `R` where IPC framing
dominates. Default effective threshold stays 1 MiB.

## Risk register
- **Estimate leaking into the hard field** (silent 129/136 retarget — the cardinal sin): the
  derivation only ever writes `estimated_byte_size`; `adr138_byte_upper_bound_not_written_by_toron_derivation`
  greps every Toron derivation path. Honesty check: deleting the estimate field leaves every
  129/136/137 gate green and only drops the *estimated* broadcasts.
- **Wrong byte unit** (Codex blocker 2): the honesty gate targets `encode_record_batch(..).len()`
  (the metered unit), not `get_array_memory_size()`; `RECORD_BATCH_IPC_FRAMING_OVERHEAD` covers
  schema/message framing so even `R=1` over-counts.
- **Take accidentally broadcasting** (Codex blocker 1): `Take` left `Composite`; not a derive
  site; `adr138_take_lineage_unchanged` + `take_limited_dim_still_hashes`.
- **Smuggling selectivity into the headline** (Codex minor): headline uses a descriptor
  `row_upper_bound` (or 137 PK cap), never a bare non-PK filter; assert the estimate derives
  from the hard row source.
- **Inverting a 137 guard:** chooser reads byte-typed fields only (`byte_upper_bound`,
  `estimated_byte_size`), never `row_upper_bound`; `with_pk_row_cap` body untouched →
  both 137 honesty guards stay green.
- **New fact field breaks a bench/example:** `--all-targets` in phase 1.
- **Spending the headroom:** no threshold bump; `adr138_default_threshold_unchanged`.
- **Residual availability (until 139):** `broadcast_estimate_over_cap_fails_budget_not_wrong_rows`
  documents it via the existing over-cap runtime test.
- **Sort:** leave `:516` inert (137).

## Acceptance
A Rune+SQL string-bearing tiny dim broadcasts via a labeled *estimated* receipt from a hard
row source (fails on `main`); the estimate over-counts the encoded IPC payload at
`R ∈ {1,8,1000}`; EXPLAIN distinguishes estimated vs hard; a hard 136 bound still wins; an
over-threshold estimate hashes; `Take` stays `Composite`/hashes; the over-cap residual is
documented; no Toron derivation writes `byte_upper_bound`; the estimate + derivation live in
one helper; default threshold unchanged; 137's chooser/cap honesty guards stay green
un-retargeted; seven `adr138_*` + 129–137 guards green.
