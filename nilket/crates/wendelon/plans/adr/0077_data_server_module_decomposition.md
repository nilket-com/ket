# Wendelon ADR 077: Data-Server Module Decomposition

**Status:** Promoted. **Migration:** `plans/impl/0077_data_server_module_decomposition.md`.
**Category:** Wendelon storage-ring maintainability cut — drafted in the cross-ring council
(operator + Codex + Claude aligned), relocated to the Wendelon sequence at promotion (the 071/075/076
precedent). A behavior-neutral decomposition of
`crates/wendelon/src/network/data_server.rs` (30,602 lines) into a module directory, following the
ADR 176 binder-decomposition playbook exactly.

## Why this cut exists

`data_server.rs` is the largest file in the repo by 4,000 lines and the data plane's whole
request-handling surface in one file. It is unreviewable, unnavigable, and a merge-conflict
magnet. ADR 176 proved the remedy for Toron's binder: split the dominant type's inherent impl
across child modules, pin the split with architecture guards, prove zero surface change. This cut
applies that playbook to the data server and **establishes the Wendelon decomposition convention**
so `manager.rs` (26,442 lines) follows it next.

## Grounding (verified)

- `WendelonDataServer`'s inherent impl owns most of the file (2 of 31 impl blocks); ~24 `handle_*`
  async fns cluster into clean domain families; ~29 helper types are self-contained.
- **The Rust mechanism holds (operator-confirmed):** a child module of `network::data_server` may
  write `impl WendelonDataServer` and access private fields defined in `mod.rs`. Converting
  `data_server.rs` → `data_server/mod.rs` **preserves the `wendelon::network::data_server::…`
  path** verbatim — `ReplicaRouter`, `WendelonDataServer`, etc. are imported crate-wide
  (ring.rs:35, node.rs:61) and must keep working unchanged.
- The dispatch is `MsgType`-keyed routing in `handle_frame`/`handle_data_frame`.
- Wendelon's architecture guards currently live in one file,
  `crates/wendelon/tests/architecture_test.rs`, and many prior guards read
  `src/network/data_server.rs` directly. This ADR includes the source-reader retarget: add a
  `data_server_source_files()`/`data_server_source()` helper that concatenates the exact
  `network/data_server/*.rs` manifest, then move every existing data-server guard to that helper.
  It does **not** require a separate Wendelon architecture-harness decomposition.
- A product-stage `//! ## V1 scope` doc heading sits at line 19 (the milestone terminology the
  June purge retired); the byte-domain `*.v1` constants (`wendelon.table_query.span.v1` etc.) are
  protocol identifiers and stay.

## Decisions (promotion-locked)

**D1 — Behavior-neutral, inherent-impl-across-submodules.** Handlers stay methods on
`WendelonDataServer`; they move, they are not re-architected. No new types, trait objects,
dispatch-table rewrites, or behavior-shaped abstraction — that would make the move non-mechanical
and void the proof. The proof of neutrality is **the full Wendelon suite green with no
assertion/behavior rewrites** (mechanical import/module-path changes and relocating the
`#[cfg(test)]` block to `tests.rs` are acceptable, per operator), plus the surface-change guard.

**D2 — Coarse handler modules; helpers colocated with their domain (operator's granularity
vote).** No generic `helpers/` bucket. Domain ownership:
- `counter.rs` — counter handlers + `CounterReplicaNetworkTransport`, `CounterParticipantFactory`
- `table_query.rs` — table-query/query/aggregate handlers + query-execution helpers
  (`QueryProjectedExecution`, row-group fetch helpers)
- `write_replication.rs` — write/replicate/read handlers + `DurableQuorumTracker`,
  `PendingFinalizationClaims`, `WriteOutcome`
- `descriptor_migration.rs` — migrate/pending-witness handlers + `MigrationRegistry`
- `streaming.rs` — cdc/finalization/bulk stream handlers (split further only if it busts the
  ceiling)
- `replica_routing.rs` — `StaticReplicaRouter` + its `ReplicaRouter` impl (a cohesive standalone)
A genuinely cross-cutting helper stays in `mod.rs`; only those.

**D3 — Dispatch stays in `mod.rs` (the reader's map).** `WendelonDataServer` struct def,
construction, and the `MsgType` routing in `handle_frame`/`handle_data_frame` remain in `mod.rs` —
the single legible entry point that names every domain. `mod.rs` `pub use`-re-exports every type
that moved to a child module but was referenced as `data_server::Foo` (path preservation, D-grounding).

**D4 — Establish the Wendelon decomposition convention.** Add
`crates/wendelon/plans/conventions/module-decomposition.md` codifying the inherent-impl-split
pattern + the guard set, so `manager.rs` and the next monolith follow one documented playbook.

**D5 — Scope: `data_server.rs` only.** `manager.rs` (storage/LSM lifecycle — a different surface)
is the named follow-on under the same convention, not folded here. Decomposing one cleanly and
codifying D4 makes the second near-mechanical.

**D6 — The `V1 scope` heading is renamed in this cut.** `//! ## V1 scope` → `## Scope and
simplifications` (or equivalent) — the continuous-refactoring-travels-with-the-ADR rule, since the
cut rewrites the module header anyway. Byte-domain `*.v1` protocol constants untouched.

## Guards (mirror ADR 176, promotion-locked)

In the current Wendelon architecture home, `crates/wendelon/tests/architecture_test.rs`
(unless a separate architecture-harness decomposition lands first):
- `adr077_data_server_source_concatenates_submodules` — exact file manifest of
  `network/data_server/*.rs` in stable order (`mod.rs`, domain modules, `tests.rs`), plus the 176
  `*_source_concatenates_submodules` source-concatenation helper. Every prior architecture guard
  that reads "the data server source" must use this helper, not `src/network/data_server.rs`.
- `adr077_semantic_modules_exist` — each named domain module present + declared in `mod.rs`.
- `adr077_dispatch_and_struct_stay_in_root` — `WendelonDataServer` def + `MsgType` routing in
  `mod.rs`; the carved handler bodies are NOT in `mod.rs`.
- `adr077_root_is_bounded` — `mod.rs` ≤ a line cap (proposed ~5,000; tighten at impl once the
  carve lands).
- `adr077_tests_extracted` — the `#[cfg(test)]` block in `tests.rs`.
- `adr077_no_wire_or_runtime_surface_change` — dirty-tree scan: only `network/data_server/**` and
  the decomposition guard/convention files changed; **no edits to wire types, MsgType, protocol
  schema, or production crate files outside `network/data_server/**`** (the cardinal-risk guard).
- `adr077_no_v1_scope_heading` — the renamed heading stays renamed (rides the existing
  `no_new_v1_suffixed_identifiers` discipline; doc-prose variant).

## Honesty gate

There is no red-on-main *behavioral* gate — a pure move changes no behavior by design. The gate is
inverted: **the entire existing Wendelon suite passes with no assertion/behavior edits**, and the
`adr077_*` structural guards (which DO fail on main — `data_server.rs` is a single file, no
submodules exist) pass after. The decomposition guards are the "fails on main" proof; the
unchanged behavior suite is the "delivers its name without breaking anything" proof.

## Risks / honest notes

- **Enormous mechanical diff; the suite is the review.** No human reviews 30k lines of moves —
  review burden is entirely "suite unchanged + guards + the diff is moves-only." The instant a
  move "fixes" anything, it stops being mechanical and the proof collapses (D1).
- **Re-export completeness** is the subtle failure mode: a type moved to a child module without a
  `mod.rs` `pub use` breaks `data_server::Foo` call sites crate-wide — caught by `cargo check`,
  but name it as the thing to get right (D3).
- **Architecture-source retargeting** is the other mechanical trap: old guards that still read
  `src/network/data_server.rs` will fail after the file becomes a directory, or worse, miss moved
  code if a compatibility stub appears. The helper retarget is part of the cut, not cleanup.
- **Module sizing** — table-query and write/replication may each still be multi-thousand-line;
  the ceiling guard forces a further split rather than a blown cap.

## Forward arc

`077 data_server decomposition (this) → 078 manager.rs storage-lifecycle decomposition (same
convention) → the next monolith by the documented playbook`. Dividend: the counter handlers become
a legible `counter.rs`, easing the elasticity-gated counter-handoff cut when its signal fires.

## Deferred (named)

`manager.rs` decomposition; `consensus/raft_storage.rs` (9,783 lines) and `sql_planner.rs` (8,314)
if they cross a future ceiling; any behavior change to the data plane (explicitly out of scope —
this is a move-only cut).

**Carried from the V1-purge audit (this cycle):** the June purge (`refactor:` `4d25482`) covered
north-star prose + `*V1`-suffixed identifiers but missed (a) ~27 sites of **feature-stage "V1"
prose** in doc-comments/error messages across coordinator/units/lsm/topology/client/crypto/l2/
content_hash (Category A — the same retired milestone concept, in prose the identifier guard cannot
see), and (b) `TableSchemaDropRequest { V1 { … } }` — a lone `V1` enum variant with no `V2`
(Category C — a cosmetic-suffix miss). This cut renames only its own in-file `## V1 scope` heading
(D6); the repo-wide Category-A prose sweep, the Category-C variant (operator-deferred), and the
prose-`V1` guard gap are named follow-ons — a separate behavior-neutral chore, not folded into this
move-only cut. (Real format/protocol versions — `index.rs`/`crypto.rs` v1→v2 docs, byte-domain
`.v1` constants, `/v1/traces` — are correct survivors, untouched.)
