# ADR 0724: a retention pass reads no tiered object when no retention floor can exist

Status: accepted, 2026-11-13 (Claude, Codex). Scope ruled by Codex 01a1d28e; source accepted 01a1d295; evidence and delivery check 01a1d2a1 (`reviews/adr0724-20261113/`, finding in `reviews/three-host-20261112/SOURCE-PASS.md`). Implementation lands with this plan.

## Why
- Every sweep pass on the leader runs the tiered retention step (`SlabSweeper::rewrite_tiered_slabs_for_retention_once`). It visits every tiered slab the node lists, and its per-pass limit of one counts only slabs it **rewrote**.
- Preparing one slab (`SlabManager::prepare_tiered_table_retention_rewrite`) fetches the slab's whole object from the object store and decodes it **before** it can tell that nothing is prunable.
- So a node with nothing to prune fetches every one of its listed tiered objects, whole, on every pass. That is true with retention switched off for the cluster and for every table.
- Found by reading source after ADR 0723's two failed executions, whose request traces show a steady stream of whole-object fetches. **Those requests are not attributed to this path and this ADR does not claim to fix those failures.**

## Decision
1. **A retention floor comes from exactly two places**: the cluster's window (for every primary-key table and every table that inherits it) or a table's own fixed window. `any_retention_floor_possible(cluster policy, committed table lifecycle catalog)` is true when the cluster policy has a window or any table in the catalog has a fixed window.
2. **When it is false the retention step returns before visiting any slab.** No floor exists for any record of any table, in the catalog or not, so no record can be marked for reclaim and no version can lag a floor: the step could not have rewritten, retired or pruned anything.
3. **Every other combination keeps the full pass unchanged**, including a disabled cluster policy with one table carrying its own window. The step is skipped only on what the node's configured policy and the state machine's committed catalog prove; never on a slab's own advisory metadata.
4. The catalog is read once per pass, from the same state-machine snapshot the step already read once per slab. A table window committed during a pass is seen by the next pass, as it was for the slabs already visited.

## What this does not do
- It does not bound the pass when a floor IS possible: with any window anywhere, a pass still fetches every listed tiered object that has nothing to prune. That general cost is registered separately; no cursor, per-slab fact, cache or persisted state is added here.
- It changes nothing in the table-drop rewrite, the re-wrap and plaintext-rewrite steps, upload verification, rehydration or the read path.

## What is no longer observed
- With no floor possible, a slab holding a multi-row frame whose table could not be resolved was reported each pass as "ambiguous rows retained" (the `Observe` outcome, feeding `observe_storage_table_retention_l2_prune`). No policy could have pruned those rows; that count is no longer reported while no floor is possible. It is reported again as soon as one is.
- Retention lag cannot exist without a floor, so nothing is lost there.
- **Incidental failures of the skipped work are no longer performed or reported.** Even with no floor, preparing a slab could fail: the tier-authority or unresolved-publication checks could refuse, or fetching, validating or decoding the object could fail, each counted in this step's `failed` and logged as a warning. The inactive step no longer does those things, so it no longer reports them. This clears no fence, changes no read-path validation, completes no unresolved cleanup, and is not a scrub of tiered objects; runtime reconciliation and cleanup-marker processing still run earlier in the same sweep. The per-slab exclusive rewrite claim and the refusal over an unpublished commit protect a proposed rewrite; none is prepared here, so they are not taken.

## Acceptance
- Unit: the predicate over the catalog (no table; inheriting and disabled tables; a cluster window; one table window with the cluster policy disabled), checked against `effective_table_retention` for each case.
- Sweeper, real slab manager and tier pointer, a store that counts every request, only the retention step driven so the count is that step's own: with no floor possible, three passes make **zero** requests of any kind and the old version is still readable at an old timestamp; with a cluster window the same pass fetches and prunes; with the cluster policy disabled and a table window in the catalog the pass still fetches.
- The pre-fix behaviour (a fetch per pass with no floor possible) is shown by running the zero-request test against the tree without the early return.

## Validation record
- Code tree of the implementation: `nilket` as validated at ba8784d50 (unchanged in the landed pair; only this file's status and this section were added afterwards).
- The first attempt (2d83565c8) **did not compile**: two errors in the new tests (a fixture field's type, a missing trait import). A compile failure, not a test result (bigfoot `~/work/reviews-logs/adr0724-focused-20261113T014656Z`).
- Focused, on ba8784d50 (`…/adr0724-focused-20261113T015208Z`, seal 671abd61…): format check clean; the four new tests, the existing sweeper retention tests and the retention unit tests, 18 passed, 0 failed; architecture test 206 passed.
- Control with only the early return removed (13 lines): the zero-request test failed at its first pass with one fetch and no other request, `(1, 0)` against `(0, 0)`; the cluster-window and table-window tests passed on that tree.
- Delivery check, on ba8784d50 (`…/adr0724-storage-20261113T015908Z`, seal 8cc73163…): the whole `wendelon` library `storage::` test tree at repository parallelism, 2,711 passed, 0 failed, 14 ignored. One execution. No full checkpoint gate was run for this change, by ruling; main's known DC-167 failure is unaffected and still open.
- One compiler warning in those builds is not from this change (`field node is never read`, `node/parent_decisions_cluster_tests/attempt_exhaustion.rs`).
- Limits: the counting tests use a local-directory store and one tiered slab; the table-window test shows the full pass is kept, not that a table window prunes a tiered append-only row. Nothing here measures a daemon or an object-store service, and nothing attributes ADR 0723's failed executions to this path.
