# ADR 125: Rune Broadcast Join Authoring

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** —
converged on a single Rune `broadcast_join` method dispatching through a
strategy-aware `build_join_plan` helper that replaces `build_hash_join_plan`.
No compat wrapper; old name does not survive. All three iter-3 drafts agreed
on every load-bearing decision, gate, and guard after iter-1 grounding
toppled the Gemini/Codex iter-0 "constant-memory live shuffle" framing in
favor of broadcast authoring (the conceded iter-0 topic from ADR 124).
**Category:** Toron interior — frontend authoring / Rune composition. No
substrate change.
**Predecessors:** 122 (Rune `.join` + 8 kind methods — this ADR adds the
broadcast sibling to the 9-method surface, retargets the 122 single-builder
guards); 121 (qualifier resolution — broadcast joins use the same
`RelationScope`-aware column resolution); 087–093 (logical identity flows
through authored broadcast joins via the existing 089 schema metadata
writer).
**Migration:** `plans/impl/0125_rune_broadcast_join_authoring.md`.

## Doctrine — expose execution, don't re-implement it

> Broadcast execution is **complete and verified working**:
> `fragment_broadcast_join_plan` at `fragment.rs:473-565` publishes the build
> side as `Exchange(partition_count=1)` and replicates to worker_count
> consumer tasks each reading a bare probe-side `Scan`; `binder.rs:3664-3679`
> rejects non-INNER + bad placement; `lib.rs:594
> distributed_broadcast_join_digest_matches_hash_join_over_tcp` proves
> equivalence-of-result with hash-partitioned execution; cap enforcement
> (`broadcast_build_bytes_per_task` / `…replicated_bytes_per_query`) is
> terminal. **The only missing thread is the frontend bridge** — both SQL
> `lower_join:2646` and Rune `join_with_kind:414` hard-code
> `JoinStrategy::HashPartitioned` by calling `build_hash_join_plan`, the
> single production literal site for that variant.

ADR 125 closes that gap on the Rune side only in V1. 122 just gave users
authored joins; broadcast is the natural 10th method (INNER-only). SQL hint
authoring is a deliberately separate ADR (don't invent a string preprocessor
before grounding `sqlparser`'s hint/comment preservation).

The receipt doctrine ([[logical_identity_receipt_doctrine]]) is honored:
the frontend selects a strategy hint; downstream binder + fragmenter
validators are the receipt that earns enforcement. Bad placement /
non-INNER already rejects.

## Decisions

**D1 — Single new Rune method: `df.broadcast_join(other, on)`.** Receiver
= LEFT = build = the small table. Reads `small.broadcast_join(big, on)` =
"broadcast `small` as build, probe `big`." Inner-only by API construction
(no `broadcast_left_join` etc — non-INNER broadcast is not supported by
execution today). Same `RuneExpr` predicate shape as 122; same
`expr_to_join_keys` bridge.

**D2 — Strategy-aware builder: `build_join_plan(left, right, kind, on,
strategy)`.** Rename `build_hash_join_plan` → `build_join_plan` with
explicit `strategy` parameter. **No compat wrapper for the old name.**
Internal match on `strategy`:

- `HashPartitioned` → wrap both sides in
  `Exchange(DEFAULT_JOIN_PARTITION_COUNT, keys)` (current behavior,
  bit-identical).
- `Broadcast` → require `kind == Inner` (frontend rejection mirror of
  `FragmentError::BroadcastNonInner`); wrap LEFT in `Exchange(1,
  left_keys)`; leave RIGHT bare.

The two SQL `build_hash_join_plan` callers (`sql_planner.rs:2646`,
`sql_planner.rs:3598`) become
`build_join_plan(.., JoinStrategy::HashPartitioned)`. Rune `join_with_kind`
passes `JoinStrategy::HashPartitioned` through a shared Rune join helper,
and the new Rune method passes `JoinStrategy::Broadcast` through that same
helper. **Single strategy-aware builder preserved** — the 122 economy moves
from a hash-only helper name to explicit strategy selection at the frontend
call boundary.

**D3 — Frontend non-INNER reject for broadcast.** `build_join_plan` with
`(strategy == Broadcast, kind != Inner)` returns
`FrontendError::unsupported("broadcast join is only supported for INNER
joins")` matching the existing fragmenter + binder reject messages. The
Rune method enforces by construction; the frontend-level reject is
defense-in-depth for future SQL hint authoring + manual callers.

**D4 — Build side = LEFT = receiver = small.** Documented at the method's
Rune doc-comment + a fixture (G-B) pinning the receiver semantics. The
convention matches the binder broadcast validator (`terminal_exchange(left)`
+ `partition_count == 1` at `binder.rs:3670-3675`) and the fragmenter
(`extract_exchange(*join.left)` + `partition_count != 1` reject at
`fragment.rs:484-486`). Reversed convention would silently produce
binder/fragmenter rejections; doc-comment + fixture make it readable.

**D5 — Distributed equivalence via authored path.** G-A mirrors
`distributed_broadcast_join_digest_matches_hash_join_over_tcp:594` but
constructs **both sides via Rune authoring** — `df_a.inner_join(df_b,
on)` (hash) and `df_a.broadcast_join(df_b, on)` (broadcast) — and asserts
identity-of-row-digest. Proves the authoring bridge is bit-identical to
the substrate's existing hand-rolled-IR equivalence.

**D6 — Logical-identity flow inherited.** The 089 schema metadata writer
+ 088 catalog decoder run unchanged through `derive_join_exchange_keys`'s
qualifier-resolved column expressions; `binder.rs:3664-3679`'s `Broadcast`
arm already validates over `BoundExprProgram` keys. No new identity work.
Pinned by G-C: a `CustomerId`-bearing column carries through the authored
broadcast join output schema.

**D7 — Cap exhaustion + 121 qualifier resolution preserved.** Existing
`broadcast_build_bytes_per_task` / `…replicated_bytes_per_query` cap
remains terminal (G-D regression). Existing 121 qualifier resolution
applies in `on`-clause expressions identically to 122's `.join` (G-E
regression).

**D8 — Scope boundary.** No `exec.rs`, `distributed/fragment.rs`,
`distributed/scheduler.rs`, `binder.rs` broadcast validator (consumed
unchanged), PBC, Wendelon, partition store, or SQL grammar change. No new
IR variant (`JoinStrategy::Broadcast` reused). No SQL hint surface
(deferred — `sqlparser` hint preservation grounding is the prerequisite).
No non-INNER broadcast widening (named follow-on). No cost-based
selection (named follow-on). Change surface is precisely: 1 new Rune
method + 1 helper rename/refactor + 3 production callsite updates + 1
Rune module registration line + behavior gates + architecture guards.
**122 guards retargeted in-change** (discipline #4).

**D9 — Honest non-scope: Codex's empty-partition Arrow IPC trap is not a
125 concern.** Surfaced for the next council: `seal_live` would produce
empty bytes rather than a schema-bearing encoded empty `RecordBatch` for
never-appended partitions; this is a correctness detail for the deferred
ledger-backed live shuffle sealing cut, not broadcast authoring. Named
in §"What to do next" so it's not lost.

## Behavior gates

- **`rune_authored_broadcast_join_digest_matches_hash_join_over_tcp`**
  (G-A, headline) — constructs the same equi-join query via
  `.inner_join` (hash) and `.broadcast_join` (broadcast) end-to-end
  through Rune authoring over a TCP multi-worker cluster; asserts
  identity-of-row-digest. Mirrors `lib.rs:594` but authored.
- **`rune_authored_broadcast_join_constructs_validated_broadcast_plan`**
  (G-B, receiver convention) — `small.broadcast_join(big, on)` produces
  `LogicalPlan::Join { strategy: Broadcast, kind: Inner, left:
  Exchange(input: small, partition_count: 1, key: ..), right: big (bare),
  .. }`. Pins receiver = LEFT = build = small AND the structural
  broadcast shape (RIGHT not wrapped in Exchange).
- **`rune_authored_broadcast_join_preserves_logical_identity`** (G-C) —
  the 089 round-trip through the authored path: `customer_id` identity
  carries through to the join output schema for broadcast authored over
  two identity-bearing columns.
- **`rune_authored_broadcast_join_respects_cap_terminally`** (G-D,
  regression) — authored broadcast over a build side exceeding
  `broadcast_build_bytes_per_task` cap still fails terminally with the
  existing `BroadcastReplication` budget error.
- **`broadcast_join_qualifier_resolution_matches_hash`** (G-E,
  regression) — 121 qualifier resolution in `on`-clause expressions
  behaves identically for broadcast as for hash.
- **`build_join_plan_rejects_broadcast_non_inner_at_frontend`** (G-F) —
  defensive: `build_join_plan(.., kind: LeftOuter, .., strategy:
  Broadcast)` returns `FrontendError::unsupported`.
- **`rune_inner_join_still_produces_hash_partitioned`** (G-G,
  regression) — `.inner_join` / `.join` / 7 other 122 methods still
  construct `HashPartitioned` (no accidental strategy slip).
- **`existing_distributed_broadcast_equivalence_holds`** (G-H,
  regression) — the existing hand-rolled-IR
  `distributed_broadcast_join_digest_matches_hash_join_over_tcp:594`
  still passes.
- **`existing_broadcast_cap_failure_holds`** (G-I, regression) — the
  existing
  `distributed_broadcast_join_over_cap_fails_terminally_over_tcp:606`
  still passes.

Regressions: 115–124 suites green; `join_kind_test.rs` green; ADR 122
Rune join tests green.

## Architecture guards

- **`adr125_rune_exposes_single_broadcast_join_authoring_method`** (Gᵃ¹)
  — `rune_engine.rs` registers `broadcast_join` as a single
  `RuneDataFrame` method dispatching through the shared Rune join helper
  to `build_join_plan(.., JoinStrategy::Broadcast)`. No per-join-kind
  broadcast methods (no
  `broadcast_left_join` etc).
- **`adr125_strategy_aware_join_builder_is_single_helper`** (Gᵃ²) —
  `sql_planner.rs` contains exactly one `build_join_plan` definition;
  the old name `build_hash_join_plan` does NOT survive; SQL lowering
  callsites pass `JoinStrategy::HashPartitioned`; Rune default and
  broadcast joins pass `JoinStrategy::HashPartitioned` /
  `JoinStrategy::Broadcast` into the shared Rune helper that calls
  `build_join_plan`.
- **`adr125_frontend_rejects_broadcast_non_inner`** (Gᵃ³) —
  `build_join_plan` body contains the `kind != Inner && strategy ==
  Broadcast` reject arm.
- **`adr125_broadcast_authoring_builds_left_exchange_one_and_bare_right`**
  (Gᵃ⁴) — source-scan: `build_join_plan`'s `Broadcast` arm constructs
  `Exchange { .. partition_count: 1, .. }` for LEFT AND leaves RIGHT as
  the input plan (no `Exchange` wrap). Pins build-side convention.
- **`adr125_no_executor_fragmenter_binder_pbc_wendelon_change`** (Gᵃ⁵)
  — `exec.rs`, `distributed/fragment.rs`, `binder.rs` broadcast
  validator source slice, `pbc.rs`, Wendelon source contain no
  `adr125_*` markers.
- **`adr125_no_sql_hint_authoring_yet`** (Gᵃ⁶) — `sql_planner.rs`
  `lower_join` + `:3598` callers still route
  `JoinStrategy::HashPartitioned` unconditionally; no comment/hint
  parsing path for broadcast at the SQL surface. Inverts on the named
  follow-on.

**Retargeted from 122 (discipline #4, in-change):**

- `adr122_join_builder_is_pair_shaped_and_shared` (`rune_frontend.rs:1713`)
  retargets to assert the strategy-aware shape at the new name. The
  substring `build_hash_join_plan` is removed; new assertions:
  `build_join_plan` exists, SQL callers route with explicit
  `JoinStrategy::HashPartitioned`, Rune default joins route
  `JoinStrategy::HashPartitioned` through the shared helper, and the Rune
  broadcast method routes with `JoinStrategy::Broadcast`.
- `adr122_join_builder_inserts_terminal_hash_exchanges`
  (`rune_frontend.rs:1749`) retargets to assert the
  `JoinStrategy::HashPartitioned` arm body still contains the dual
  `Exchange` wrap with `DEFAULT_JOIN_PARTITION_COUNT`.
- `adr122_no_executor_pbc_wendelon_or_scheduler_change`
  (`rune_frontend.rs:1816`) reject-set widens to include `adr125_*`
  markers (matches the 123→124 widening pattern).

**Recipe:** `cargo test -p toron` + `cargo test -p toron --test
architecture_test adr122` + `cargo test -p toron --test architecture_test
adr125` + `cargo check -p toron --all-targets`.

## Council history

- **iter-0:** Gemini led B (`partition_batches` removal framed as "true
  constant-memory streamable shuffle"); Codex agreed with B but
  sharpened to "Ledger-Backed Live Shuffle Sealing" with the
  empty-partition Arrow IPC trap (also overclaimed memory bounding);
  Claude grounded both framings against `partition_store.rs:501-515` +
  `server/mod.rs:511`, surfaced that `live.chunks` retains all encoded
  bytes through seal AND the publish path double-encodes + double-reserves
  shuffle bytes. Recommended A (Rune broadcast authoring).
- **iter-1:** Codex conceded the constant-memory overclaim and adopted
  A. Claude iter-1 locked the design with the four iter-0 forks (F1–F4)
  resolved.
- **iter-2:** All three peers wrote the ADR/impl dual, converging on
  the same shape: single Rune method, strategy-aware builder rename
  without compat wrapper, INNER-only, receiver = LEFT = build = small,
  9 gates, 6 new guards + 3 122 retargets.
- **iter-3:** Three-way verification. No load-bearing divergence on any
  decision, gate, or guard. Promotion-locked.

**Thirteenth grounding-correction in the 119→125 arc.** The pattern
[[feedback_ground_the_premise_before_sketching_scope]] this time applied
to Codex's iter-0 sub-claim ("memory bounded by ADR 124 unread backlog
plus transient per-batch work") overlaying Gemini's "true constant-memory"
framing. The grounding move catalyzed the topic convergence; without it,
all three peers would have promoted the wrong cut
([[feedback_cut_must_deliver_the_name]]).

## What to do next

- **Ledger-backed live shuffle sealing** — drop `partition_batches`
  RecordBatch retention + double publish/budget reservation in
  `execute_live_shuffle_producer`; route the publish path through
  `seal_live`'s chunk-concatenation branch. **Must include the
  empty-partition schema-bearing Arrow IPC fix** (Codex iter-0 trap:
  `seal_live` would produce empty bytes rather than an encoded empty
  Arrow IPC payload for never-appended partitions). Honest name: "drop
  RecordBatch retention from live producer," NOT "constant-memory live
  shuffle."
- **Chunk-granular sealed manifest** — true constant-memory live shuffle.
  Multi-entry-per-partition manifest OR streaming sealed-payload
  assembly. Substantial; depends on consumer-side multi-entry support.
- **Configurable `LIVE_BACKLOG_DEPTH` via `QueryBudget`** — earns a cut
  when a real workload demonstrates the constant is wrong.
- **Executor build/probe reorder** — pairs with 124 for end-to-end
  natural backpressure for joins.
- **Output-side joins-as-streamable-producers** — widen
  `streamable_unary_plan` for `Join` + restructure join executor to
  yield mid-probe.
- **Multi-producer fan-in + remote live transport** — Codex's complete
  three-substrate framing of the 123 D2 deferral.
- **SQL broadcast hint authoring** — separate ADR; depends on grounding
  `sqlparser`'s comment/hint preservation + a bind-time hint extraction
  path.
- **Non-INNER broadcast widening** — separate ADR; binder + fragmenter
  relaxation + per-kind correctness proofs.
- **Cost-based strategy selection** — needs cardinality estimates not in
  IR today (`estimated_build_hash_bytes:2871` is hash-table-size, not
  row-count). Substantial.

## Promotion criteria

All 9 behavior gates (G-A through G-I) green; G-A is the headline
distributed-equivalence observable; G-B is the receiver-convention pin;
G-C is the 089 identity-flow pin; G-D + G-H + G-I are the broadcast
substrate regression pins; 115–124 regressions green; 122 retargets
hold; Gᵃ¹–Gᵃ⁶ hold; `cargo test -p toron` + `cargo test -p toron --test
architecture_test adr122` + `cargo test -p toron --test architecture_test
adr125` + `cargo check -p toron --all-targets` green. Migration:
`plans/impl/0125_rune_broadcast_join_authoring.md`.
