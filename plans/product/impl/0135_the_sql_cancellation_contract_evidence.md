# PRODUCT-0135 evidence — the SQL cancellation contract

Implementation of PRODUCT-0135, following
`plans/product/impl/0135_the_sql_cancellation_contract.md`. Five commits, in
the plan's order:

```text
030b7623  test: adr 0135 capture sql dispatch baselines
40a6dd5d  impl: adr 0135 add the run generation gate
fb4f8902  impl: adr 0135 add out-of-band cancellation capabilities
9da96296  impl: adr 0135 add the cancel surface
          evidence: close sql cancellation (this commit)
```

Verification at close: `cargo test -p polariton --bin polariton` — **951
passed, 0 failed, 8 ignored**; `--test architecture_test` — **96 passed**; no
dead-code or new clippy warnings in touched files. The known pre-existing
`product_0063` parallel-run flake fired intermittently during stage 2/3
verification runs and passed in isolation each time; noted, not
re-investigated, per its recorded diagnosis.

## Gate ledger

### Stage 0 — census and baselines

- The completion-path enumeration concluded: SQL results reach the host
  through exactly two messages — `RunResult` (plain and snippet runs share
  `run_code`) and `RunMarkdownCellResult` (`run_sql_markdown_cell`) — and
  became a standing architecture census
  (`product_0135_sql_results_reach_the_host_through_exactly_two_paths`), so a
  third dispatch site fails the census rather than shipping unstamped.
- The pre-arc Code menu pair was pinned; no production behavior changed.

### Stage 1 — the run generation gate

- Both SQL paths route through one shared gated future,
  `dispatch_sql_gated` — the census was **strengthened** accordingly: exactly
  one raw dispatch site, one definition plus two call sites.
- Edge one holds by construction: the generation is captured when the task is
  built, before it first awaits the adapter lock. A task acquiring the lock
  under a stale generation returns without dispatching
  (`product_0135_stale_generation_task_never_dispatches`, counting adapter,
  zero dispatches).
- Both completion messages carry the generation; both handlers discard stale
  results before any effect — status, timer, grid size, registry, and
  notebook cell each asserted
  (`product_0135_late_results_from_a_cancelled_generation_change_nothing`).
  A cancelled named claim concludes through the existing
  `conclude_unbound`, exactly as failure does; no new registry semantics.
- `cancel_sql_generation` advances the generation first, then drains
  `pending_runs` (`product_0135_cancel_advances_generation_and_drains_the_queue`).

### Stage 2 — the capabilities

- `CancelCapability::{Engine, StopWaiting, Refuse}` with `EngineCancel` in
  `adapters/common.rs`; `AdapterState::connect` returns the capability beside
  the connection, and `AdapterState.cancellation` holds it as a sibling
  field — never behind the connection's lock.
- The accepted table realized: Toron `Engine` (client clone + dispatch-minted
  `QueryId` in a slot with its own mutex; RAII stamp asserted by
  `product_0135_query_stamp_fills_and_clears_the_shared_slot` — a finished
  dispatch can never be cancelled late); Postgres `Engine` (`cancel_token()`
  at connect, cancelling over its own connection); SQLite, MySQL, BigQuery
  `StopWaiting`; Parquet `Refuse`; scratch fallback `StopWaiting`
  (`product_0135_capabilities_match_the_accepted_table` covers the offline
  rows and the fallback).
- The local-workspace attach path — which builds its Toron adapter directly —
  carries the capability through `LocalWorkspaceAttached`, so both connect
  paths obey §2.
- The in-flight task handle is retained via `Task::abortable`; a plain handle
  drop does not abort — only cancel's explicit `abort()` does.

### Stage 3 — the surface

- `Message::CancelRun` from two affordances: a pressable status-bar segment
  rendered only while a run is in flight
  (`product_0135_cancel_affordance_is_bound_to_an_in_flight_run`), and
  `code:cancel` in the Code menu
  (`product_0135_code_menu_offers_the_run_and_cancel_actions` — the stage 0
  census pinned the pre-arc pair this stage deliberately extends).
- The handler applies the contract's order — generation advance first, then
  abort, then the engine signal task — and edge two is asserted as far as the
  host can observe it: the generation has advanced by the time the signal
  task is returned (`product_0135_cancel_outcomes_follow_the_capability`).
- Honest outcomes in the routine `status_msg` lane, `status_error` empty in
  every arm: "cancelled" (engine signal settled Ok), "stopped waiting; the
  engine may still be finishing" (StopWaiting, and Engine-signal failure),
  "cannot be cancelled; the query runs to completion" (Refusal — which
  performs no advance, no abort, no drain, each asserted), and "nothing to
  cancel" when idle.
- The statusbar model gained its smallest interactive extension:
  `Segment::Action` plus `view_with_action`; every other segment kind and
  caller is unchanged.

## Deviations and judgment calls

1. **The census tightened mid-arc by design.** Stage 0 asserted two dispatch
   sites; stage 1's shared gated future made it one definition with two
   callers, and the census now enforces that stronger shape.
2. **The plain `StatusBar::view` was removed** rather than kept beside
   `view_with_action`: its only caller became the action-mapped renderer, and
   an uncalled public renderer on a binary crate is dead surface. The
   PRODUCT-0071 chrome guard was updated to track the action-mapped call —
   same model, same single `app_status_bar` producer.
3. **Edge two's engine-side half is asserted at the host boundary.** The test
   proves the generation advanced before the signal task was returned; the
   signal's actual network departure afterward is the runtime's sequencing,
   which no unit test can observe. The handler's code order carries it.
4. **`CancelSignalSettled(Err)` reports "stopped waiting" wording**, because
   that is what is then true: the client abandoned the dispatch and the
   engine-side stop could not be confirmed. The error detail rides in the
   same routine message; it is not a `status_error`.
5. **Toron dispatch now mints its `QueryId` explicitly** (`stamp_query` →
   `execute_with_query_id(id, code, "UTC")`), preserving the previous
   `execute` semantics — same timezone, same single-statement path — while
   making the id visible to the capability.
6. **Refusal leaves the abort handle in place.** A refused cancel changes
   nothing, including the handle; the run completes and clears it, exactly as
   an uncancelled run does.

## Cross-stage guards, verified at close

- **The uncancelled path is unchanged:** full suite green at every stage; no
  status wording or result behavior moved for runs nobody cancels.
- **No signal path can wait on the adapter lock:** capabilities are built
  from a client clone, a pre-captured token, or nothing; none holds
  `Arc<RwLock<dyn DatabaseAdapter>>`.
- **Rune and plot scheduling untouched:** `rune_view_compiler.rs`,
  `rune_presentation.rs`, the Rune view scheduler, and `dispatch_rune` have
  empty diffs across the arc.
- **`crates/libaryon` untouched:** empty diff across the arc.
- **DB-01's remainder untouched:** no planning, cost-gate, provenance, or
  pushdown surface appeared, and this arc is not precedent for any of them.

## Left open, correctly

Engine-side MySQL cancellation (`KILL QUERY`, a second connection), BigQuery
jobs-API cancellation, any Parquet interruptibility (bound to polars's
synchronous execution), SQLite interrupt on a `tokio-rusqlite` upgrade that
exposes it (a one-row table amendment per the record), keybindings, and
timeouts. Each is a later record per the ADR's Rejected and Consequences.
