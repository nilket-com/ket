# Implement the SQL cancellation contract

Scope: implement PRODUCT-0135. Add the run-generation gate over every SQL
dispatch and completion path, the out-of-band cancellation capability acquired
at connect, and the Cancel surface with honest per-outcome wording. The
implementation adds no timeouts, no Rune or plot cancellation, no second MySQL
connection, no BigQuery jobs-API migration, and nothing from DB-01's
unscheduled remainder.

The work is five commits, in this order:

```text
test: adr 0135 capture sql dispatch baselines
impl: adr 0135 add the run generation gate
impl: adr 0135 add out-of-band cancellation capabilities
impl: adr 0135 add the cancel surface
evidence: close sql cancellation
```

The first commit changes no production behavior. The evidence commit records
the gate ledger and verification; it contains no implementation change.

## Halt rule

PRODUCT-0135 permits a SQL run generation with its two ordering edges, an
adapter-level cancellation capability acquired at connect and held beside the
connection, honest Cancelled / Stopped waiting / Refusal outcomes, queue
draining that covers lock-waiting tasks, host-level stale-result rejection on
every SQL completion path, and the two affordances.

Stop and return the finding to PRODUCT-0135 or a new owning record if
implementation requires any of:

- reaching a cancel signal through `AdapterState.connection` or any path that
  can wait on the adapter's `RwLock`;
- a per-query cancel handle or a change to `DatabaseAdapter::dispatch`'s
  signature;
- cancelling `dispatch_rune`, local Rune presentation execution, or the Rune
  view scheduler;
- a timeout of any kind;
- a second MySQL connection, `KILL QUERY`, or a BigQuery jobs-API migration;
- a keybinding;
- new registry semantics — a cancelled run concludes as a failed one already
  does;
- anything from DB-01's remainder: planning, cost gates, provenance display,
  density pushdown, deferred relations.

`crates/libaryon` is out of bounds entirely. If Toron's client proves
non-clonable, halt — the capability design assumed it on cited advice, and a
workaround would improvise transport semantics.

### Where work may land

- `crates/polariton/src/adapters/common.rs` — the capability types
  (`CancelCapability`, the engine-signal trait);
- `crates/polariton/src/adapters/driver.rs` — `connect` returns the
  capability beside the connection; `AdapterState` holds it as a sibling
  field, never behind the connection lock;
- `crates/polariton/src/adapters/toron/mod.rs` — dispatch-minted `QueryId`
  in a slot the capability shares through its own synchronization, plus the
  client clone;
- `crates/polariton/src/adapters/postgres.rs` — `cancel_token()` capture at
  connect;
- `crates/polariton/src/gui/state.rs` — the generation, its two ordering
  edges, the dispatch stamps, the completion-path discards, the queue drain,
  and the Cancel handler;
- `crates/polariton/src/gui/messages.rs` — `CancelRun` and generation-stamped
  completion payloads;
- `crates/polariton/src/gui/statusbar/` — one interactive segment kind, the
  smallest extension that renders a pressable Cancel beside the timer; and
- `crates/polariton/src/gui/components.rs` — the `code:cancel` menu action and
  the status-bar wiring.

## Stage 0 — census and baselines

### 0a. Enumerate every SQL completion path

The record's discard rule is only as good as this enumeration. Establish, with
citations, that SQL results reach the host through exactly two messages —
`RunResult` (plain and snippet runs share `run_code`) and
`RunMarkdownCellResult` (notebook SQL via `run_sql_markdown_cell`) — and that
no other path mutates status, grid, registry, or cell output from a SQL
dispatch. If a third path exists, the census names it and stage 1 stamps it.

### 0b. Pin the pre-arc behavior

- Both dispatch sites hold `db.write().await` for the dispatch's life; no
  `Cancel` message exists (`gui/messages.rs`).
- `finish_pending_runs` batches; `pending_runs` holds undispatched snapshot
  requests only — a run already dispatched is invisible to it.
- `ResultRegistry::begin` retires the name at dispatch; failure leaves the
  name unbound (existing test).
- The elapsed timer renders only around a run; the status bar has no
  interactive segment.

### Acceptance gates

- No production behavior changes.
- The completion-path enumeration carries citations and a stated conclusion.

Commit: `test: adr 0135 capture sql dispatch baselines`.

## Stage 1 — the run generation gate

- Application state owns the generation as a shared atomic; the current value
  is readable inside dispatch tasks without the adapter lock.
- **Edge one:** each run captures its generation when its task is built —
  before the task first awaits the adapter lock, never on acquisition.
- **Edge two:** cancel advances the generation before anything else it does.
  (The engine signal arrives in stage 2; the ordering lands now.)
- A task that acquires the adapter lock under a stale generation returns
  without dispatching.
- Both completion messages carry the run's generation; both handlers discard a
  stale result **before** status, grid, registry, or cell-output effects. A
  discarded named claim concludes as failure already does.
- Cancel clears `pending_runs`.

### Acceptance gates

- A stale-generation task provably never dispatches (test: task built, then
  generation advanced, then lock acquired — adapter records no dispatch).
- A late result from a cancelled generation changes nothing: status, grid,
  registry, and notebook cell each asserted, per completion path.
- A cancelled named claim leaves the name unbound.
- Ordinary runs are unaffected: full suite green, timings and status wording
  unchanged for the uncancelled path.

Commit: `impl: adr 0135 add the run generation gate`.

## Stage 2 — the capabilities

- `AdapterState::connect` returns the capability beside the connection;
  `AdapterState` stores it as a sibling field. The scratch fallback is
  Stopped waiting.
- Per the accepted table: Toron `Engine` (client clone + dispatch-minted
  `QueryId` in a shared slot with its own synchronization); Postgres `Engine`
  (`cancel_token()` captured at connect, cancels over its own connection);
  SQLite, MySQL, BigQuery `StopWaiting`; Parquet `Refuse`.
- The Toron adapter's dispatch writes and clears the current-query slot; the
  capability reads it without touching the adapter lock.
- The in-flight task handle (`Task::abortable`) is retained so cancel can
  abandon the client future; abandonment plus generation discard is the whole
  StopWaiting outcome.

### Acceptance gates

- No capability path can wait on the adapter `RwLock` — asserted structurally:
  the capability types hold no `Arc<RwLock<dyn DatabaseAdapter>>`.
- Each adapter's connect yields exactly the accepted table's capability.
- Toron's slot holds the id during dispatch and is empty after (unit test at
  the adapter, no live deployment).
- Postgres capability construction requires no network (token capture is
  local; signalling is not exercised in tests).

Commit: `impl: adr 0135 add out-of-band cancellation capabilities`.

## Stage 3 — the surface

- `Message::CancelRun`, dispatched from a status-bar Cancel rendered beside
  the elapsed timer only while a run is in flight, and from a `code:cancel`
  menu action under Code.
- The statusbar model gains its smallest interactive extension — one pressable
  segment kind; existing segments and callers are unchanged.
- The handler applies the record's order: advance generation → abort the
  retained task handle → clear `pending_runs` → fire the engine signal when
  the capability is `Engine` → report the outcome in the `status_msg` lane:
  Cancelled / Stopped waiting wording per the table, Refusal for Parquet with
  nothing else done.
- Cancelling when nothing runs is a no-op with routine feedback.

### Acceptance gates

- The affordance exists only while a run is in flight.
- Each outcome's wording is asserted; none uses `status_error`.
- Refusal performs no generation advance, no abort, no queue drain.
- The menu census gains `code:cancel`; `connect`/`plot`/`session` menus are
  unchanged.

Commit: `impl: adr 0135 add the cancel surface`.

## Cross-stage guards

- **The uncancelled path is sacred:** every stage re-runs the full suite; no
  status wording, timing, or result behavior changes for runs nobody cancels.
- **No signal through the adapter lock**, re-checked at every stage.
- **Rune and plot scheduling untouched:** `rune_view_scheduler`,
  `dispatch_rune`, and presentation tasks have empty diffs.
- **`crates/libaryon` untouched.**

## Evidence and review checklist

- [ ] Completion-path enumeration with citations; every path stamped.
- [ ] Stale task never dispatches; late results change nothing, per path.
- [ ] Ordering edges asserted: stamp-before-lock-await, advance-before-signal.
- [ ] Capability table realized exactly; no lock-reachable signal path.
- [ ] Outcome wording per adapter; `status_msg` lane only; Refusal inert.
- [ ] Affordance visibility bound to an in-flight run.
- [ ] Full suite + architecture green; counts reported passed/ignored
      separately. Known 0063 flake noted if seen, not re-investigated.
