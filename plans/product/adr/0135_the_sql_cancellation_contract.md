# ADR 0135: the SQL cancellation contract

Status: accepted 2026-08-19. This record gives the user a brake: a way to stop
an in-flight SQL dispatch without wedging, killing, or restarting Polariton. It
is the scoped half of deferred-ledger DB-01 that
`plans/product/review/deferred_triage.md` § Ratification — 2026-08-19
scheduled, and it takes that ratification's fence as binding: nothing here may
acquire the live-table planning surface.

It decides the user-facing verb and its honest outcomes, the out-of-band
cancellation capability, the per-adapter capability table, the run-generation
gate that covers dispatch, the queue, and every SQL completion path, and the
interface affordances. It does not decide timeouts, `dispatch_rune` or local
Rune cancellation, plot scheduling, adapter planning, dry-run cost gates,
provenance display, or density pushdown.

## Context

Polariton cannot stop a query. `run_code` dispatches through
`Task::perform(async move { db.write().await.dispatch(code).await }, …)`
(`crates/polariton/src/gui/state.rs`), holding the adapter's `RwLock` write
guard for the query's entire life. There is no `Cancel` variant in
`gui/messages.rs`. On an engine that answers 10M-row queries, one bad statement
wedges the application until the engine finishes or the process dies.

The primitives exist and are inert. `toron_client::Client::cancel(&self,
QueryId)` takes `&self` and is exercised only by an `#[ignore]`d test
(`adapters/toron/mod.rs`); `execute_with_query_id` already lets a caller mint
the id before dispatch. `tokio_postgres` ships `Client::cancel_token()`, which
cancels over its own connection by design. Neither is wired to anything.

Two mechanical facts shape every decision below.

**The adapter lock is the enemy.** The active query owns the write guard, so
any cancel path that reaches the adapter through
`AdapterState.connection` — including a method on the adapter itself — waits
behind the very query it is trying to stop. The signal must travel a path that
never touches that lock.

**Draining `pending_runs` is not draining the queue.** `finish_pending_runs`
batches tasks, so at cancel time a run can exist in three places: undispatched
in `pending_runs`, already a task awaiting the adapter write lock, or in
flight. Clearing the first set alone would let a lock-waiter start the moment
the cancelled query releases the guard — a cancel that visibly doesn't work.

One piece of the answer already exists. `ResultRegistry::begin` retires a
name's binding at dispatch, and "failure leaves the name unbound" is the
tested rule — but the registry only governs `-- @result`-annotated
publications. Unannotated results, notebook-cell results, status text, and the
grid all take effect at the host, and each of those paths must refuse a
cancelled run's late result itself.

## Decision

### 1. One verb, with honest outcomes

The user operation is **Cancel**. What it achieves depends on the adapter, and
the status line says which, in the ordinary `status_msg` lane — a cancellation
is routine interaction feedback, per PRODUCT-0133's surface rule, not an error:

- **Cancelled** — the engine was told to stop and the client stopped waiting.
- **Stopped waiting** — the client abandoned the dispatch; the engine may
  still be finishing. Worded so the user learns the engine-side truth.
- **Refusal** — the adapter cannot cancel and cannot be abandoned mid-query;
  the status says so rather than pretending.

There is no second verb. "Abandon" and "cancel" are outcomes of one gesture,
never two affordances the user must choose between.

### 2. The capability is acquired out-of-band and held beside the connection

Cancellation travels through a **capability handle acquired when the adapter
connects and retained in application state beside the connection** — never
reached through `AdapterState.connection`, whose lock the active query owns.

A capability may share per-dispatch state with its adapter — Toron's current
`QueryId`, for instance — only through its own synchronization, never the
adapter lock. The operation is adapter-level, not per-query: exactly one
dispatch executes per adapter at a time, so a per-query handle would model a
concurrency the product does not have.

Capability presence is a fact about the connection, established at connect
time. Product logic asks the capability what it can do; it never branches on
the adapter's brand (KET-0005 §3).

### 3. The capability table is honest, per engine

| adapter | outcome | mechanism |
|---|---|---|
| Toron | Cancelled | `Client::cancel` with the dispatch-minted `QueryId` |
| Postgres | Cancelled | `cancel_token()` over its own connection |
| SQLite | Stopped waiting | `tokio-rusqlite` 0.7.0 exposes no interrupt handle; abandon the client future — the worker may continue |
| MySQL | Stopped waiting | engine-side `KILL QUERY` needs a second connection — deferred |
| BigQuery | Stopped waiting | the query API surfaces no job id to cancel — deferred |
| Parquet | Refusal | polars executes synchronously with no await point; even abandonment waits |

The Parquet row is a fact this record inherits, not one it creates: a
synchronous execution on the async runtime cannot be interrupted at all, and
its blocking of the executor is a pre-existing condition recorded here rather
than repaired. Upgrading MySQL, BigQuery, or Parquet is a later record per
engine, not a quiet extension of this one.

### 4. One run generation gates dispatch, the queue, and every completion path

Application state holds a **SQL run generation**. Ordering is part of the
contract, because both edges race:

- **A run captures its generation before its task first awaits the adapter
  lock** — never after acquiring it. A task stamped on acquisition could
  capture a generation advanced while it waited and outlive the cancel aimed
  at it.
- **Cancel advances the generation before issuing the engine-side signal**, so
  by the time any guard releases, every waiting task is already stale.

Then:

- **A task that acquires the adapter lock under a stale generation returns
  without dispatching.** This is what closes the lock-waiter gap: batched
  tasks that were already waiting when the user cancelled die at the gate
  instead of starting.
- **`pending_runs` is cleared**, so nothing undispatched survives either.
- **Every SQL completion path discards a stale-generation result before any
  effect** — before status text, before the grid, before the registry, and
  explicitly including notebook-cell SQL. `ResultRegistry` protects only
  named publications; the host rejects the rest. A cancelled generation's
  late result changes nothing anywhere.

A cancelled run concludes exactly as a failed one does at the registry: the
name stays unbound, per the rule `begin` already enforces. No new registry
semantics are introduced.

### 5. Cancel means stop, not stop-one

Cancel applies to the whole intent in motion: the in-flight dispatch, the
lock-waiting tasks, and the undispatched queue. A cancel that let three queued
statements start the moment the guard released would read as a cancel that
failed. Running the remainder is a fresh user decision, expressed by running
again.

### 6. The affordances

Two, both existing surfaces:

- a **Cancel affordance in the status bar**, rendered beside the elapsed
  timer that already appears only while a run is in flight; and
- a **`code:cancel` menu action** under Code.

No keybinding in this record: Escape and Ctrl+C already carry meanings in the
editor and grid, and a chord is a keymap decision.

### 7. Scope refusals, stated as decisions

`dispatch_rune`, local Rune presentation execution, and plot scheduling are
**rejected from this record**, not deferred by omission. Each has its own
revision and scheduler discipline (PRODUCT-0003's structured failures,
PRODUCT-0133's generation-stamped views, the Rune view scheduler), and
cancelling them is a contract over those disciplines, not over
`DatabaseAdapter::dispatch`. Including them would materially enlarge the arc
the triage bounded.

Timeouts are likewise out: a timeout is policy about a query nobody is
watching; this record is the hand of a user who is.

## Rejected

- **A `cancel_in_flight()` method reached through the connection.** The active
  query owns that lock; the cancel would deadlock behind its own target.
- **Per-query cancel handles.** One dispatch executes per adapter; the
  generality has no consumer and doubles the trait surface.
- **Draining only `pending_runs`.** Lock-waiting tasks are invisible to it;
  the next query would start on guard release and the cancel would appear to
  fail.
- **Trusting `ResultRegistry` to reject late results.** It governs named
  publications only; unannotated and notebook results take effect at the host.
- **Presenting abandonment as engine-side cancellation.** The user plans
  around what the engine is doing; the wording must not lie about it.
- **A fake Parquet cancel.** Nothing can interrupt a synchronous collect;
  offering the affordance and ignoring the gesture is worse than refusing.
- **Second-connection `KILL QUERY` for MySQL, jobs-API migration for
  BigQuery.** Real upgrades, each its own record; neither blocks the brake.
- **Timeouts.** Policy, not cancellation; separate record if ever wanted.
- **Including Rune, notebooks' non-SQL cells, or plot scheduling.** Different
  disciplines; the triage's bound holds.
- **Any planning, cost-gate, provenance, or pushdown surface.** The
  ratification fence: a cancellation record must not acquire the live-table
  planning surface.

## Consequences and sequencing

**Both census questions are resolved into this record.** `tokio-rusqlite`
0.7.0 exposes no interrupt handle, so SQLite's table row is Stopped waiting —
a later crate upgrade that surfaces the interrupt is a one-row amendment, not
a redesign. Toron's capability holds a client clone plus synchronized
current-query state; the binding requirement is that the path never touches
the adapter lock, not that its own synchronization be lock-free.

**Every SQL completion path must be enumerated and stamped** — the plain run
path, the snippet path, and notebook-cell SQL — with a test per path proving a
stale generation's result changes neither status, grid, registry, nor cell
output. The enumeration is stage 0 work; a missed path is a silent
resurrection of the bug this record exists to fix.

**The status-bar affordance appears only while a run is in flight**, beside
the timer that already behaves that way, and cancellation feedback uses the
ordinary `status_msg` lane.

**DB-01's remainder stays open and unscheduled.** Deferred relations,
`db.table(...)`, adapter planning, cost gates, provenance display, and density
pushdown are untouched, and this record may not be cited as precedent for any
of them.
