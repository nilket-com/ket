# Impl Plan — ADR 135: Broadcast-Star Recursive Probe Fragmentation

**Migration for** `plans/adr/0135_broadcast_star_recursive_probe_fragmentation.md`.
**Scope:** `binder.rs` (widen star-eligibility in the probe predicate),
`distributed/fragment.rs` (shape-gated left-deep flatten → one scan-share probe +
N build upstreams, rewriting build exchanges to `shuffle_input_scan`, and
fail-loud rejection of unsupported pinned nested probes), guards (incl. retargets),
tests.
**Non-scope:** new IR/stage/wire types, ADR 123 scheduler/worker topology (reused),
remote-live fan-in, general recursive fragmentation, Wendelon, 129–134 strategy logic.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| probe fence to widen | `broadcast_probe_is_scan_linear` `binder.rs:3960` (rejects `ExecutionPlan::Join`); `ProbeNotScanLinear` `:255`,`:3939` |
| single-boundary fragmenter (flatten target) | `fragment_broadcast_join_plan` `fragment.rs:473`; `first_join` `:839`; `replace_first_join` `:1124` |
| multi-upstream already exists (123) | `TaskSpec.inputs: Vec<TaskInputBinding>` `task.rs:134`; `Scan{share}`/`ShuffleConsumer` `:141`; scheduler/worker 123 path (`scheduler.rs:654`, `server/mod.rs:260`) |
| build-exchange→input rewrite primitive | `shuffle_input_scan(...)` (fragment.rs) |
| per-link order (130) | `JoinPhysicalPlacement::SwappedRightBuild` + restoring `Project` |
| per-link strategy (131/132) | `choose_auto_join_strategy` |
| 131 gates / fragmenter-shape guard to retarget | `right_tiny_left_deep_join_stays_hash`, `probe_containing_join_stays_hash`, `broadcast_join_fragmenter_input_shape_is_architecture_guarded` |

## Phases
0. **Failing gates first:** `three_way_broadcast_star_flattens_to_single_probe_stage`
   + `three_way_broadcast_star_digest_matches_hash_over_tcp` fail on `main` (131
   hashes the nested probe); `nested_hash_probe_still_stays_hash` green before+after.
1. **Binder eligibility (D1):** widen the probe predicate to accept a left-deep
   `Join{Broadcast, one-partition build Exchange, recursively-eligible probe}` under
   row-local Filter/Project, bottoming at a scan-linear leaf. Keep all other nested
   probes `ProbeNotScanLinear`. Add a focused `broadcast_star_probe_*` helper; pin
   with the narrow-eligibility guard. Do not re-prove table facts in this helper:
   auto links already earned facts through the existing decision seam; pinned links
   are accepted by authored shape and remain fail-loud through validation/caps/
   fragmenter errors.
2. **Fragmenter flatten (D3):** add a shape-gated recursive collector that detects
   the left-deep all-broadcast-build chain bottoming at one shared scan, allocates
   one build-publish stage per dim, rewrites each build exchange to
   `shuffle_input_scan`, and emits one scan-share probe stage with N `ShuffleConsumer`
   inputs (reuse 123). Existing two-way broadcast behavior is preserved; the nested
   flatten is gated on the exact star shape.
3. **Order spine (D4):** compose the per-link restoring `Project`s so flattened
   output == unflattened == hash (column + digest). Build the TCP digest gate here —
   the correctness crux.
4. **EXPLAIN (D6):** confirm each link still produces its own `DecisionReceipt`
   (133) so 134 renders N rows; add `star_explain_emits_one_receipt_per_link`.
5. **Guards + tests:** six `adr135_*` + the gates; **retarget** (don't delete) the
   131 nested-probe hash gates → "non-broadcast/global/limited join stays hash" and
   `broadcast_join_fragmenter_input_shape_is_architecture_guarded` → "one scan-share
   + N broadcast-build ShuffleConsumers." Re-run 123/129–134.
6. **Verify:** `cargo test -p toron` (star gates + TCP digest parity) · `--test
   architecture_test -- adr135` (+ re-run 123/129–134) · `cargo check -p toron
   --all-targets` · `git diff --check`.

## Risk register
- **Output-order drift (the big one):** flattening must compose the 130 restoring
  projections; the TCP digest+column gate is the backstop. Output-equivalent
  regrouping, never re-derivation.
- **Over-wide eligibility:** the binder predicate must admit ONLY left-deep
  already-broadcast build chains over a scan-linear probe; auto links still prove
  tiny single-base facts through the existing decision seam. `nested_hash_probe_still_stays_hash`
  + `nested_aggregate_sort_take_or_limited_probe_still_stays_hash` are backstops.
- **Same-worker-live assumption (123):** cross-worker builds fall back to sealed
  manifests (correct, slower); the TCP digest gate must run across workers to
  exercise the sealed path. Remote-live fan-in is out of scope.
- **Fragmenter regression:** two-way broadcast behavior must remain byte-equivalent;
  130/131 right-build + scan-linear gates are the backstop.
- **Scan-once:** the fact is scanned once (one scan-share input), not N times —
  `adr135_final_probe_stage_has_one_scan_share` + the headline gate.
- **Pinned silent-embed:** `pinned_broadcast_unsupported_nested_probe_rejects` pins
  fail-loud (no unfragmented nested exchange inside a consumer).
- **Guard inversion as deletion:** retarget the 131/fragmenter-shape guards
  explicitly (discipline #4).

## Acceptance
3-way left-deep broadcast star flattens to one scan-share probe + N one-partition
builds; output digest+column-identical to hash over TCP; non-star/hash/global/
limited/mixed stay hash (131 intact); pinned-unsupported rejects fail-loud; EXPLAIN
N per-link receipts; no new IR/stage/wire type; reuses 123 fan-in; six `adr135_*` +
retargeted 131/fragmenter guards + 123/129–134 guards green.
