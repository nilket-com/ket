# Impl — Wendelon ADR 0093: Durable table-drop completion

Pairs with `plans/adr/0093_durable_drop_completion.md`. Cross-crate cut:
wendelon producer half, toron consumer half, one `impl: adr 0093` commit.
Steps are ordered so every behavior change lands behind a test that could
fail without it.

## Step 0 — grounding probes

- **P1:** pin post-finalize retry classification today: descriptor
  retained-but-unbound → `TableAlreadyDropped`
  (`network/control_server.rs`) vs fully removed → `TableMissing`. The
  step-2 outcome check must precede both.
- **P2 (gates closing the originating finding):** drive materialize into a
  name whose prior DROP half-completed (remote finalized, no local marker)
  and record whether the `ctas_sagas` path still publishes and issues a
  receipt over an unreadable relation. **The probe must reproduce the
  observed multi-run lifecycle**: the wedged DROP and the materialize
  happened in separate runs, so the probe rebuilds the Toron
  coordinator/descriptor registry against persisted Wendelon state between
  the two steps. A same-process probe returning `TableAlreadyExists` off
  warm local state has not reached the shadow path and disproves nothing.
  **Disposition is binary:** reproduce → fix in this cut or the ADR
  withdraws its receipt-integrity claim and the follow-on is scheduled;
  not reproducible under the multi-run shape → record the probe as the
  closing evidence. The probe rides the impl commit as a test either way.

## Step 1 — Wendelon: retained outcome state

**1a. The catalog.** New type beside the fence machinery in
`wendelon/src/schema/lifecycle.rs`: a drop-outcome catalog keyed
`(TenantId, TableObjectId)` →
`{ operation_id: DropOperationId, finalize_log_index: u64 }`, with `record`
and `get`. Doc comment pins the invariant: *the fence proves a drop in
flight; the outcome proves it finished — exactly one of the two exists for
a table from prepare until compaction.* `SmState` gains the field next to
`table_drop_fences` (`consensus/raft_storage.rs`).

**1b. Snapshot format v2 → v3** (ADR 0092's machinery, exercised):

- freeze the outgoing `SmState` spine as `snapshot_body.rs::v2`, following
  the `v1` module's pattern (frozen container spine, unchanged leaf types
  referenced from the live schema);
- dispatch v2 decodes through the frozen module in the snapshot decode
  dispatch; upconvert with an **empty** outcome catalog;
- bump the current snapshot format version to 3;
- add immutable byte fixtures for v2 and for the new current shape, and
  extend ADR 0092's format-fixture corpus controls to cover them.

**1c. Finalize apply — thread the log index, prevalidate, then mutate.**

- Dispatch: pass `entry_log_index` to `apply_table_drop_finalize`, as
  `TableDropPrepare` and `TableReclaimCompletion` already receive it.
- Inside the apply, in order:
  1. *Replay recognition:* retained outcome with identical
     `(operation_id, finalize_log_index)` → success, before any fence is
     required.
  2. *Prevalidation, no mutations:* fence presence; fence operation match;
     `fence_log_index` and `reclaim_completion_log_index` checks;
     **conflicting retained outcome** (same table, different operation or
     index) → error; name-unbind precondition checked via a non-mutating
     query (or the unbind verified fallible-first — whichever the
     `table_schemas` API supports without a partial write).
  3. *Infallible mutations only:* remove fence, insert outcome, unbind
     name (unless CTAS retirement), remove lifecycle entry and
     certificate.
- Contract: any validation failure leaves `SmState` byte-identical.

**Controls:** post-finalize state holds outcome and no fence (assert
both); replaying the identical finalize entry is success and leaves one
outcome; a conflicting finalize (same table, different operation) errors
**and** a full `SmState` equality check shows zero mutation; snapshot
round-trip carries outcomes; a v2-fixture decode upconverts to an empty
outcome catalog; the corpus guards fail if the v2 or current fixture
drifts.

## Step 2 — Wendelon: same-operation idempotent drop RPC

In `run_table_schema_drop`, before the descriptor lookup: consult retained
outcomes for `(tenant_id, table_object_id)` — matching `operation_id` →
`Ok(())`; different → `TableAlreadyDropped`; no outcome → existing flow
unchanged.

**Controls:** same-operation re-drop after finalize → `Ok`; different
operation id → `TableAlreadyDropped`; never-existed → `TableMissing`
unchanged.

## Step 3 — the severed-response seam and the A2 end-to-end control

**3a. Hook gating — a non-default `test-support` feature, not
`#[cfg(test)]`.** `cfg(test)` is crate-local: Toron integration tests
compile Wendelon as a normal dependency where it is false, so test-gated
hooks would be invisible to exactly the tests that need them. Instead:

- Wendelon gains a **non-default** `test-support` feature; both hooks (this
  step's sever seam, step 4's reclaim delay) and their arming APIs are
  gated `#[cfg(any(test, feature = "test-support"))]` — Wendelon's own unit
  tests keep working ungated by the feature.
- Toron gains a **non-default** forwarding feature
  `test-support = ["wendelon/test-support"]`; the A1/A2 integration tests
  are themselves gated on it, so default `cargo test -p toron` neither
  compiles the hooks nor silently skips-as-green the acceptance controls
  (the feature-enabled verification legs run them explicitly).
- **Guard that default production builds cannot arm the hooks:** the
  Wendelon architecture test asserts `test-support` is absent from
  `[features] default` in `Cargo.toml` and that every hook site sits under
  the gate (source-scan, same pattern as the endpoint census).

**3b. The seam.** A `test-support` hook in the control server's schema-drop
response path that, when armed for a target table, closes the stream after
`run_table_schema_drop` returns `Ok` and **before the response frame is
written**. The remote state machine has finalized; the caller observes
transport loss. A direct client call is not a substitute: it receives its
response and severs nothing.

**3c. Toron mapping changes:**

- `map_wendelon_admin_error_to_storage` (`toron/src/storage.rs`): split
  `TableAlreadyDropped` into a new `StorageError::TableAlreadyDropped`.
- `drop_existing_table_with_storage_admin` (`toron/src/coordinator.rs`):
  the `Ok(_) | Err(TableNotFound)` arm is untouched; the new variant falls
  through to the error return with the divergence named.
- Enumerate **all** `StorageError` match sites compile-guided across toron
  and toron-tpch; decide each new arm explicitly, no wildcards.

**Controls:**

- **A2 end-to-end (no timing anywhere):** arm the seam; (1) public Toron
  DROP → observes transport failure (`Unavailable`), remote has finalized,
  no local marker; (2) disarm; same-statement retry →
  `drop_table_storage` succeeds, `mark_finalized_drop` runs,
  `catalog.drop_table` consumes the marker; (3) materialize into the same
  name; (4) **read the rows back**. Fails under keep-alive-only by
  construction.
- Foreign-operation control: same severed setup, retry constructed with a
  different schema generation → distinct error, no marker install, no
  catalog removal.

## Step 4 — Wendelon: QUIC liveness policy (A1)

- Constants beside the transport constructors in
  `wendelon/src/network/tls.rs`: finite `max_idle_timeout`,
  `keep_alive_interval` strictly shorter, values aligned with
  `toron/src/quic.rs` (ADR 0577).
- Install a shared `Arc<TransportConfig>` at the choke points:
  `quinn_server_config_from_rustls` (covers `server_config` and
  `server_config_with_peer_roots`) and `client_config`; `wendelon-client`
  inherits through them.
- **Endpoint-census architecture guard** in
  `wendelon/tests/architecture_test.rs`: source-scan that
  `quinn::ServerConfig::`/`quinn::ClientConfig::` construction appears only
  in `network/tls.rs` (allowlisting the guard file itself, per the existing
  guards' pattern).

**Controls:**

- Compressed-timescale pair mirroring `toron/src/quic.rs::compressed_pair`
  — silence beyond idle survives with keep-alive AND dies without it (both
  arms). The fast *mechanism* control only.
- **Owned end-to-end exhibit, `#[ignore]`d for wall-clock:** a DROP whose
  reclaim genuinely exceeds 30 seconds (held via the `test-support` delay
  hook in `drive_table_drop_physical_reclaim`) completes over a live
  connection under the installed policy. Run explicitly for the evidence
  receipt; the compressed pair alone does not discharge A1's literal claim.

## Verification gate

Order: edits → `fmt` → `fmt --check` → FULL architecture tests (both
crates). The gate:

```
scripts/cargo-cgroup fmt  -p toron -p wendelon -- --check
scripts/cargo-cgroup test -p wendelon --lib
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test      # FULL
scripts/cargo-cgroup test -p wendelon --test architecture_test   # FULL
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p wendelon --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
# A1/A2 hooks compile only under the non-default feature — these legs are
# the ONLY place the sever/delay controls run; without them the gate would
# claim green while never executing the ADR's acceptance controls. At
# implementation time, narrow both to the named test target/filter —
# especially the --ignored leg, so it runs the A1 exhibit alone and not
# every ignored exhibit in the suite:
scripts/cargo-cgroup test -p toron --features test-support --test <a2_target>
scripts/cargo-cgroup test -p toron --features test-support --test <a1_target> -- --ignored <a1_exhibit_filter>
git diff --check
```

The A1 exhibit's run is recorded in the evidence receipt (which rides the
`impl:` commit). No new IR field → no `--all-targets` leg; re-check if step
3c's variant reaches toron-tpch reporting types. Re-run the gate
immediately before claiming green.

## Guard reconciliation

None inverted. Three guards added (endpoint census; snapshot
format-fixture extension; test-support absent from default builds with
every hook site under its gate). The Toron catalog marker guard keeps its
message and behavior; the mapping change is additionally pinned by the A2
end-to-end control — a behavioral net that fails if the collapse returns.
