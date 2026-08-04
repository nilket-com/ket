# Impl Plan — Wendelon ADR 077: Data-Server Module Decomposition

Pairs with `plans/adr/0077_data_server_module_decomposition.md`. A behavior-neutral move-only cut: `data_server.rs` →
`data_server/{mod.rs + domain modules}`, mirroring ADR 176. The discipline that makes it safe:
**every edit is a move, an import-path fix, or a `pub use` re-export — never a logic change.** The
review is the unchanged suite + the structural guards + a moves-only diff.

## The mechanism (ADR 176, confirmed for Wendelon)

`impl WendelonDataServer { … }` blocks live in child modules of `network::data_server`; private
fields defined in `mod.rs` are visible to descendants. `data_server.rs` → `data_server/mod.rs`
preserves `wendelon::network::data_server::…`. Types that move to a child module but are referenced
externally as `data_server::Foo` get a `pub use child::Foo;` in `mod.rs`.

Wendelon's existing architecture guards are monolithic in `tests/architecture_test.rs`, and many
read `src/network/data_server.rs` directly. The implementation must add
`data_server_source_files()` / `data_server_source()` in that file and retarget all existing
data-server source reads before the full architecture binary can be green.

## Module map (operator's granularity: coarse handlers, helpers colocated)

| Module | Owns |
|---|---|
| `mod.rs` | `WendelonDataServer` struct + construction; `handle_frame`/`handle_data_frame` `MsgType` dispatch; `pub use` re-exports; only genuinely cross-cutting helpers |
| `counter.rs` | `handle_counter_{add,read,replicate,rollback}` + `CounterReplicaNetworkTransport`, `CounterParticipantFactory` |
| `table_query.rs` | `handle_{table_query,table_query_stream,table_query_cancel,query,query_stream,aggregate}` + `QueryProjectedExecution`, `RowGroupFetchAuditCounters` |
| `write_replication.rs` | `handle_{write,table_write,replicate,table_replicate,read}` + `DurableQuorumTracker`, `PendingFinalizationClaims`, `WriteOutcome` |
| `descriptor_migration.rs` | `handle_{migrate,pending_witness_pull}` + `MigrationRegistry` |
| `streaming.rs` | `handle_{cdc_subscribe_stream,stream_finalization_stream,bulk_stream}` (split → cdc/finalization/bulk only if it busts the cap) |
| `replica_routing.rs` | `StaticReplicaRouter` + its `ReplicaRouter` impl |
| `tests.rs` | the extracted `#[cfg(test)]` block |

(Exact handler→module assignment and the helper inventory are derived from the file at impl; the
table is the intent, the `cargo check` + guards are the enforcement.)

## Phases (each ends `cargo check -p wendelon` green; commit-sized steps if useful)

**Phase 0 — Scaffold + architecture source helper.** Create `data_server/mod.rs` from
`data_server.rs` unchanged (rename file → dir), then immediately add the architecture-test
`data_server_source_files()` / `data_server_source()` helper and retarget every existing direct read
of `src/network/data_server.rs` to it. Confirm `cargo check` and the full architecture binary are
green before moving any handler body. This isolates "the directory conversion works" from "the
moves are correct" without breaking prior ADR guards.

**Phase 1 — Standalone helper types first (lowest risk).** Move `replica_routing.rs`
(`StaticReplicaRouter` + impl) and the other cohesive standalone types to their domain modules;
`pub use` each externally-referenced one from `mod.rs`. `cargo check` after each — a missing
re-export surfaces immediately as an unresolved `data_server::Foo`.

**Phase 2 — Handler families (the bulk).** Per domain module, move its `handle_*` bodies into an
`impl WendelonDataServer` block + the domain's colocated helpers. Dispatch stays in `mod.rs`
calling `self.handle_x(...)` (unchanged — the method is just defined elsewhere now). One domain at
a time, `cargo check` + targeted tests between.

**Phase 3 — Tests + V1 heading.** Extract `#[cfg(test)]` → `tests.rs` (mechanical move; assertions
untouched). Rename `//! ## V1 scope` → `## Scope and simplifications` in the `mod.rs` header (D6).

**Phase 4 — Guards + convention doc.** Add the seven `adr077_*` guards to
`tests/architecture_test.rs` (Wendelon's current architecture home, unless that harness has already
been split by a separate cut) and add `plans/conventions/module-decomposition.md`. Tighten
`adr077_root_is_bounded`'s cap to the actual `mod.rs` size + headroom.

## Verification gate

```text
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup test  -p wendelon --test architecture_test adr077
scripts/cargo-cgroup test  -p wendelon --test architecture_test          # FULL binary
scripts/cargo-cgroup test  -p wendelon --lib                              # the behavior proof — zero assertion edits
# the data-plane integration suites — the real neutrality proof:
scripts/cargo-cgroup test  -p wendelon --test client_data_api
scripts/cargo-cgroup test  -p wendelon --test data_ring_wire
scripts/cargo-cgroup test  -p toron    --test wendelon_storage_execution_test   # cross-ring: Toron still drives Wendelon
# final "full suite" proof required by D1; evidence may call out any pre-existing ignored tests:
scripts/cargo-cgroup test  -p wendelon --tests
cargo fmt -p wendelon -- --check
git diff --check
```

## Review aids (for the eventual code review — a 30k-line move needs them)

- **Moves-only claim, mechanically checkable:** `git diff -M` rename detection + a per-module
  assertion that the moved handler bodies are byte-identical after excluding surrounding
  `impl`/import scaffolding. State in the evidence doc that no handler statement, branch, match
  arm, predicate, status mapping, or await ordering was edited.
- **Re-export completeness:** `cargo check` is the proof (no unresolved `data_server::` paths).
- **Architecture source continuity:** full `architecture_test` proves every prior guard now reads
  the concatenated `data_server_source()` instead of the removed flat file path.
- **The neutrality proof is the integration suites** (`client_data_api`, `data_ring_wire`,
  cross-ring `wendelon_storage_execution_test`) passing with zero assertion edits — call these out
  explicitly; the `--lib` unit suite alone doesn't exercise the wire dispatch.

## Named falsifications (for the review)

1. Edit a handler body during the move ("while I'm here"): the integration suites must catch any
   behavior change — and if they don't, the move was not actually behavior-relevant; either way
   the evidence doc must assert moves-only.
2. Move a type without re-exporting it: `cargo check` fails crate-wide (the re-export pin).
3. Leave a handler body in `mod.rs`: `adr077_dispatch_and_struct_stay_in_root` /
   `adr077_root_is_bounded` fail.
4. Touch a wire/MsgType/schema file: `adr077_no_wire_or_runtime_surface_change` fails.
5. Leave an old architecture guard reading `src/network/data_server.rs`: full
   `architecture_test` fails after the directory conversion.

## Hygiene

- No m-word; no new product-stage v1 idiom. The `V1 scope` heading is the one in-scope rename
  (D6), and `*.v1` protocol/domain constants stay untouched.
- No goldens (no encoding/wire change — pure source reorg).
- `cargo fmt` after the moves; several arch guards read formatted source.
- Cargo via `scripts/cargo-cgroup` (a 30k-line file's `cargo check` is exactly the kind of build
  the cgroup wrapper exists to bound).

## Open for the implementing session (verify-first)

1. Whether `WendelonDataServer` has derive/macro attributes that complicate child-module field
   access (the one technical unknown — Phase 0 surfaces it immediately).
2. The exact `mod.rs` line cap once Phase 2 lands (set the guard from the real number + headroom).
3. Whether `streaming.rs` needs the cdc/finalization/bulk sub-split (decide from its post-carve size).
