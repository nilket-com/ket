# ADR 551 evidence — authoritative task-phase timer census

Every site that writes `build_micros`, `probe_micros` or `spill_micros` on the
task receipt, classified **before** any envelope is wrapped. ADR 551 requires a
checked matrix rather than a mechanical replacement of telemetry closures,
because a scope placed around a speculative bracket turns a rejected candidate
into reported operator time.

## Census scope

- **63 candidate task-phase writers**: 21 in `exec/join.rs`, 42 in
  `exec/join/streaming.rs`. (An earlier count of 43 for `streaming.rs` counted
  continuation lines of multi-line assignments, not statements.)
- **6 publication/export sites**: 3 in `server/mod.rs`, 3 in `telemetry.rs`.
  These aggregate and export already-recorded phases. They are **sinks, not
  scopes**, and are explicitly not wrapped.

The census is extracted statement-wise and validated: every assignment in both
files appears exactly once below.

## Columns

- **timer** — the wall-clock start bound, or `copied` when the written value is
  a variable or profile-derived figure rather than an interval measured here.
- **Ok(None)** — speculative rejections **between** the timer start and the
  write. Non-zero means a commit-on-drop scope would publish a rejected
  candidate: these need an explicit `discard()` on those exits.
- **err?** — fallible exits in the same window. These fail the task, so no
  receipt is published either way; they do not require discard.
- **acq** — `open_scan` calls textually inside the window. A **lower bound**:
  acquisition inside a callee is not visible to textual analysis and is resolved
  per site when wrapping.
- **nest** — other wall timers started inside the window, i.e. possible
  parent/child scope nesting.

## Wall-timer sites

| file:line | fn | phase | timer | Ok(None) | err? | acq | nest |
|---|---|---|---|---|---|---|---|
| join.rs:452 | `execute_hash_join` | Probe | `probe_started` | 0 | 1 | 0 | 1 |
| join.rs:537 | `execute_hash_join_into_global_aggregate` | Build | `build_started` | 0 | 1 | 0 | 1 |
| join.rs:567 | `execute_hash_join_into_global_aggregate` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join.rs:741 | `execute_hash_join_into_global_aggregate` | Probe | `probe_started` | 0 | 1 | 0 | 1 |
| join.rs:1893 | `try_execute_streamed_spilled_left_semi_join` | Build | `build_started` | 0 | 2 | 0 | 1 |
| join.rs:1920 | `try_execute_streamed_spilled_left_semi_join` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join.rs:2105 | `try_execute_partitioned_spilled_left_outer_join` | Build | `build_started` | 0 | 3 | 0 | 1 |
| join.rs:2150 | `try_execute_partitioned_spilled_left_outer_join` | Probe | `probe_started` | 0 | 1 | 0 | 1 |
| join.rs:2462 | `try_execute_streamed_spilled_inner_join` | Build | `build_started` | 1 | 1 | 0 | 1 |
| join.rs:2489 | `try_execute_streamed_spilled_inner_join` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join.rs:3087 | `execute_spilled_hash_join` | Build | `build_started` | 0 | 0 | 0 | 1 |
| join.rs:3125 | `execute_spilled_hash_join` | Spill | `spill_started` | 0 | 1 | 0 | 1 |
| join.rs:3148 | `execute_spilled_hash_join` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join.rs:3203 | `execute_spilled_hash_join_into_global_aggregate` | Build | `build_started` | 0 | 0 | 0 | 1 |
| join.rs:3247 | `execute_spilled_hash_join_into_global_aggregate` | Spill | `spill_started` | 0 | 1 | 0 | 1 |
| join.rs:3274 | `execute_spilled_hash_join_into_global_aggregate` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join.rs:3327 | `visit_spilled_hash_join_filtered_chunks` | Build | `build_started` | 0 | 0 | 0 | 1 |
| join.rs:3370 | `visit_spilled_hash_join_filtered_chunks` | Spill | `spill_started` | 0 | 1 | 0 | 1 |
| join.rs:3396 | `visit_spilled_hash_join_filtered_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:5897 | `try_visit_streaming_inner_join_scan_probe_chunks` | Build | `build_started` | 0 | 2 | 0 | 1 |
| join/streaming.rs:5931 | `try_visit_streaming_inner_join_scan_probe_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:6023 | `try_visit_streaming_inner_join_scan_probe_chunks` | Build | `build_started` | 1 | 5 | 0 | 2 |
| join/streaming.rs:6057 | `try_visit_streaming_inner_join_scan_probe_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:6116 | `try_visit_streaming_inner_join_scan_probe_chunks` | Build | `build_started` | 3 | 9 | 0 | 3 |
| join/streaming.rs:6158 | `try_visit_streaming_inner_join_scan_probe_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:6474 | `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` | Build | `build_started` | 0 | 7 | 0 | 1 |
| join/streaming.rs:6535 | `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:6771 | `try_visit_partitioned_streamed_inner_materialized_build_probe_chunks` | Build | `build_started` | 0 | 3 | 0 | 1 |
| join/streaming.rs:6829 | `try_visit_partitioned_streamed_inner_materialized_build_probe_chunks` | Probe | `probe_started` | 0 | 3 | 0 | 1 |
| join/streaming.rs:7014 | `try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks` | Build | `build_started` | 0 | 3 | 0 | 1 |
| join/streaming.rs:7057 | `try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:9048 | `try_visit_streaming_join0_chunks` | Build | `build_started` | 0 | 2 | 0 | 1 |
| join/streaming.rs:9091 | `try_visit_streaming_join0_chunks` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:9181 | `try_visit_streaming_join0_chunks` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:9200 | `try_visit_streaming_join0_chunks` | Probe | `probe_started` | 0 | 1 | 0 | 1 |
| join/streaming.rs:9317 | `try_streaming_left_outer_grouped_aggregate` | Build | `build_started` | 0 | 2 | 0 | 1 |
| join/streaming.rs:9391 | `try_streaming_left_outer_grouped_aggregate` | Probe | `probe_started` | 0 | 4 | 0 | 1 |
| join/streaming.rs:9456 | `try_streaming_left_outer_grouped_aggregate` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:9513 | `try_streaming_left_outer_grouped_aggregate` | Probe | `probe_started` | 0 | 2 | 0 | 1 |
| join/streaming.rs:10826 | `try_streaming_projected_left_semi_grouped_aggregate` | Build | `chain_started` | 0 | 5 | 0 | 1 |
| join/streaming.rs:10930 | `try_streaming_projected_left_semi_grouped_aggregate` | Build | `build_started` | 0 | 1 | 0 | 1 |
| join/streaming.rs:10965 | `try_streaming_projected_left_semi_grouped_aggregate` | Probe | `probe_started` | 0 | 0 | 0 | 1 |
| join/streaming.rs:11973 | `try_visit_reoriented_adaptive_derived_last_stage` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:12029 | `try_visit_reoriented_adaptive_derived_last_stage` | Probe | `probe_started` | 0 | 2 | 0 | 1 |
| join/streaming.rs:12111 | `try_visit_reoriented_adaptive_derived_last_stage_to_sink` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:12228 | `try_visit_reoriented_adaptive_derived_last_stage_to_sink` | Probe | `probe_started` | 0 | 5 | 1 | 1 |
| join/streaming.rs:12586 | `try_visit_streaming_reoriented_inner_chain_scan_probe_base` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:12776 | `try_visit_streaming_reoriented_inner_chain_scan_probe_base` | Probe | `probe_started` | 0 | 9 | 0 | 1 |
| join/streaming.rs:13659 | `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` | Spill | `intermediate_started` | 1 | 24 | 1 | 1 |
| join/streaming.rs:13881 | `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` | Build | `terminal_build_started` | 0 | 17 | 0 | 1 |
| join/streaming.rs:14708 | `try_streaming_inner_prefix_semi_anti_grouped_aggregate` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:14727 | `try_streaming_inner_prefix_semi_anti_grouped_aggregate` | Probe | `probe_started` | 0 | 1 | 0 | 1 |
| join/streaming.rs:15627 | `try_streaming_inner_prefix_semi_anti_scan_probe_base` | Build | `build_started` | 1 | 0 | 0 | 1 |
| join/streaming.rs:16270 | `try_streaming_inner_prefix_semi_anti_scan_probe_base` | Probe | `probe_started` | 1 | 41 | 0 | 10 |
| join/streaming.rs:16493 | `prepare_reoriented_inner_join_runtime_with_scan_build` | Build | `build_started` | 0 | 1 | 0 | 1 |
| join/streaming.rs:16592 | `prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline` | Build | `build_started` | 0 | 1 | 0 | 1 |
| join/streaming.rs:17584 | `try_load_resident_partitioned_streamed_inner_scan_build` | Build | `build_started` | 3 | 3 | 0 | 1 |
| join/streaming.rs:17741 | `build_partitioned_streaming_scan_inner_build_reservoir` | Build | `build_started` | 1 | 6 | 0 | 1 |
| join/streaming.rs:18363 | `probe_partitioned_streamed_inner_scan_build` | Build | `build_started` | 0 | 2 | 0 | 1 |

## Copied / profile-derived sites — not wrapped

| file:line | fn | phase |
|---|---|---|
| join.rs:427 | `execute_hash_join` | Build |
| join.rs:716 | `execute_hash_join_into_global_aggregate` | Build |
| join/streaming.rs:16650 | `prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline` | Build |

## What the census decides

**14 sites are speculative** — they carry `return Ok(None)` between the timer
start and the write, so the enclosing bracket may be rejected after the timer has
run. Under a commit-on-drop scope each would publish a rejected candidate as
operator time. Every one needs an explicit `discard()` on those exits:

`try_execute_streamed_spilled_inner_join`,
`try_visit_streaming_inner_join_scan_probe_chunks` (x2),
`try_visit_streaming_join0_chunks`,
`try_streaming_left_outer_grouped_aggregate`,
`try_visit_reoriented_adaptive_derived_last_stage`,
`try_visit_reoriented_adaptive_derived_last_stage_to_sink`,
`try_visit_streaming_reoriented_inner_chain_scan_probe_base`,
`try_streaming_reoriented_inner_chain_scan_probe_base_to_sink`,
`try_streaming_inner_prefix_semi_anti_grouped_aggregate`,
`try_streaming_inner_prefix_semi_anti_scan_probe_base` (x2),
`try_load_resident_partitioned_streamed_inner_scan_build`,
`build_partitioned_streaming_scan_inner_build_reservoir`.

The last is worth noting: it has **no** `try_` prefix and would have been missed
by a name-based sweep. The `try_` convention is a hint, not the classifier.

**3 sites write a copied or profile-derived value** rather than an interval
measured at that site. They are not wall envelopes and must not become scopes;
wrapping them would invent a bracket around arithmetic.

**Acquisition inside a bracket is under-counted here.** Only two sites call
`open_scan` textually inside their window, but a fused join acquires through
callees that this analysis cannot see. The `acq` column is a lower bound and is
resolved per site at wrapping time, not trusted from the table.

## Commit semantics this census assumes

`commit()` means **independently authoritative selected phase** — not "this
function returned successfully". A bracket whose enclosing candidate may still be
rejected must not commit merely because its own work finished: it stays
uncommitted, or is discarded when the candidate is. Internal eager publication is
an implementation fact and cannot decide evidence scope.

That rule is what makes the retained-child policy sound. A committed child under
a discarded root is retained as an independently published outermost scope only
because committing already asserted the child was itself a selected authoritative
phase; if it was not, it should never have committed.

## Status

Census complete and validated. **No envelope has been wrapped yet.** The next
step is the engine-facing fused/bare control, demonstrated red under the old
bare-node authority, before the first production scope lands.

## Census correction — early returns that are not `Ok(None)`

The first census counted only `return Ok(None)` between timer start and write.
That was **incomplete for the purpose**. A commit-on-drop scope publishes on
*every* exit, so any early return that skips the write today is a behaviour
change tomorrow — not only the speculative ones.

Re-run across the same windows:

- **14 sites** have a post-start `return Ok(None)` — speculative rejection.
- **21 sites** have a post-start early return that is **not** `Ok(None)`, such as
  the `return result;` in `execute_hash_join`, which returns successfully while
  skipping the phase write.
- **29 sites in total** have at least one post-start early return and therefore
  require an explicit disposition before any scope is placed.

The non-`Ok(None)` cases are the harder ones. Each needs a decision the census
cannot make mechanically: the work really happened, so publishing it may be more
correct than today's silence — but it is still a change to a reported figure, and
this arc exists because reported figures were trusted without that check. They
are not all the same: some return a completed result, some hand off to another
path that will time the same work again, and the second class would double-count.

**The sweep is blocked on classifying these 29.** Wrapping on the earlier
14-site matrix would have silently altered up to 21 sites' published phases,
which is precisely the failure this record was written to prevent.

## Exit-class overlap

14 `Ok(None)` sites plus 21 other-early-return sites against 29 unique sites
implies six carry both classes. Verified:

| fn | phase | line | Ok(None) | other |
|---|---|---|---|---|
| `try_visit_streaming_inner_join_scan_probe_chunks` | Build | 6023 | 1 | 3 |
| `try_visit_streaming_inner_join_scan_probe_chunks` | Build | 6116 | 3 | 4 |
| `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` | Spill | 13659 | 1 | 6 |
| `try_streaming_inner_prefix_semi_anti_scan_probe_base` | Probe | 16270 | 1 | 1 |
| `try_load_resident_partitioned_streamed_inner_scan_build` | Build | 17584 | 3 | 3 |
| `build_partitioned_streaming_scan_inner_build_reservoir` | Build | 17741 | 1 | 2 |

Breakdown: **6 both, 8 `Ok(None)`-only, 15 other-only = 29**.

The overlap is why the unit of classification has to be the **exit edge**, not
the site. A site carrying both classes needs a different disposition per branch,
and a per-site verdict would force one answer onto branches that do not share
one.

## Commit-on-drop withdrawn as the default

An implicit commit would make each of those unclassified exits authoritative
without anyone deciding it should be. Dropping an `OperatorScope` now closes its
timer and records `unresolved_operator_scope`, which withholds the receipt:

- `commit()` — explicitly publishes selected authoritative work.
- `discard()` — explicitly rejects the work, or hands it off to be timed
  elsewhere.
- drop — resolves nothing, and is a fault.

Error propagation stays safe. A failing task publishes no receipt at all, and if
an error is caught and execution continues, the unresolved fault stops that path
producing evidence. A missed branch becomes a visible veto rather than silent
inflation.

Two controls hold this: `dropping_an_unresolved_scope_vetoes_the_receipt`, and
`an_early_return_that_discards_explicitly_is_not_a_fault`, which together show
the veto is about the missing decision and not about early returns as such.

## Exit-edge classification — still to do

Each of the 29 sites' post-start exits needs, per edge: exact line and
expression; class (candidate rejection, terminal success, error, downstream
handoff); whether the legacy writer executes; whether downstream timing covers
the same work; the disposition; whether that preserves or changes behaviour; and
the control that would catch the wrong choice.

`commit()` goes at the old authoritative write boundary by default. Any
successful early return newly classified as authoritative is a **separate
semantic correction** with its own discriminating control — it must not ride
inside the mechanical flip.

## Edge-complete census

Classification expanded from explicit `return` to every control-flow edge
between a timer start and its legacy write. The result is an order of
magnitude larger than the site view: **318 edges across 48 of the 59
wall-timer sites**.

| edge class | count | drops the scope before the write? |
|---|---|---|
| `?` error propagation | 144 | yes — unwinds past the write |
| `await?` | 52 | yes — same, across an await |
| explicit `return` | 51 | yes |
| `continue` | 41 | **only if the scope is opened inside the loop** |
| `return Ok(None)` | 18 | yes — candidate rejection |
| `break` | 12 | **only if the scope is opened inside the loop** |

No `bail!` or `ensure!` edges occur in these windows.

**The loop edges are not all real exits.** A `break` or `continue` leaves the
scope alive when the timer opens outside the loop, and drops it only when the
scope is opened inside the loop body. Which of the 53 loop edges are genuine
scope exits is a structural question per site, and is not answered by this
table — counting them as exits would overstate the work exactly as counting
only `Ok(None)` understated it.

### Sites by edge count

| fn | phase | write line | edges |
|---|---|---|---|
| `try_streaming_inner_prefix_semi_anti_scan_probe_base` | Probe | 16270 | 53 |
| `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` | Spill | 13659 | 37 |
| `probe_partitioned_streamed_inner_scan_build` | Build | 18363 | 24 |
| `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` | Build | 13881 | 22 |
| `try_visit_streaming_inner_join_scan_probe_chunks` | Build | 6116 | 17 |
| `try_visit_streaming_reoriented_inner_chain_scan_probe_base` | Probe | 12776 | 15 |
| `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` | Build | 6474 | 11 |
| `try_load_resident_partitioned_streamed_inner_scan_build` | Build | 17584 | 11 |
| `build_partitioned_streaming_scan_inner_build_reservoir` | Build | 17741 | 11 |
| `try_visit_streaming_inner_join_scan_probe_chunks` | Build | 6023 | 9 |
| `try_visit_reoriented_adaptive_derived_last_stage_to_sink` | Probe | 12228 | 9 |
| `try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks` | Build | 7014 | 9 |
| `try_visit_partitioned_streamed_inner_materialized_build_probe_chunks` | Build | 6771 | 9 |
| `try_streaming_projected_left_semi_grouped_aggregate` | Build | 10826 | 7 |
| `try_visit_partitioned_streamed_inner_materialized_build_probe_chunks` | Probe | 6829 | 6 |

The 196 error edges dominate. Most terminate the task, so no receipt
publishes and the runtime veto is moot — but each still needs a stated
disposition, because an outer candidate that catches or converts the error
keeps executing, and that path would otherwise carry an unresolved scope.

### Disposition rules

- Rejected candidate or downstream handoff → `discard()`.
- The existing authoritative write boundary → `commit()` at that exact point.
- Successful terminal result with no downstream overlap → `commit()` **only**
  as a separately identified behaviour change with its own discriminating
  control.
- Error that necessarily terminates the task → explicit `discard()`, noting
  that no receipt publishes.
- **Ambiguous edge → stays unresolved and blocks the sweep.**

The unresolved-scope veto is the runtime backstop. This matrix remains the
design authority: the veto catches a missed edge at runtime, but only the
matrix records why each edge was given the disposition it has.

## Reducing the superset to boundary-crossing edges

Two counting corrections first.

**`await?` and `?` do not double-count.** The classifier tests `await?` before the
general `?` branch, and no line carries both — verified at zero.

**But 318 counts lines, not edges.** The windows contain **207** `?` operator
occurrences against 196 `?`-classified lines: eleven share a line with another
edge. The conservative superset is roughly **329 edges**, not 318.

### Region 1 — `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` (Build)

The heaviest region, and it settles a question that applies to the other 47.

- **Function**: `streaming.rs:12889`, returns `Result<Option<()>, ExecutionError>`.
- **Scope start**: `13690`, `let terminal_build_started = Instant::now();`
- **Legacy write boundary**: `13881`, inside `record_task_telemetry`.
- **Edges in window**: 22, of which 17 are `?`/`await?`.

**Its errors are not task-terminal.** The sole caller —
`try_streaming_projected_left_semi_grouped_aggregate` at `10767` — does not
propagate with `?`:

```rust
let drive_result = self
    .try_streaming_reoriented_inner_chain_scan_probe_base_to_sink(...)
    .await;
drop(sink);
if drive_result.as_ref().is_ok_and(|matched| matched.is_some()) { ... }
```

It binds the result, drops the sink, inspects the outcome and **keeps
executing**. So every `?` inside this region exits to a caller that continues,
and none of them may be documented as terminal. Under the new lifecycle each
would drop an unresolved scope and withhold the receipt — the veto firing
correctly, but a veto on a path that should have had a stated disposition.

**Disposition for this region:** the 17 error edges need an explicit `discard()`
before propagation, or a typed helper that makes it structural. Seventeen hand
edits on one function is the argument for the helper.

**The general lesson:** an error edge is not presumptively terminal. Whether it
terminates the task is a property of the **caller**, not of the `?`. Any
classification of the remaining regions that reads only the timer window will get
this wrong, and would get it wrong in the direction that silently publishes.

### Remaining

47 regions unclassified. Each needs its callers read, not just its window. The
40/0 control stays red until every region is resolved.

## Counting discipline

The ~329 figure is **candidate occurrences, not CFG edges**. `return`, `break`
and `continue` can share lines and coexist with `?` just as `?` does with itself,
and nothing here has been counted by column. The final matrix must assign stable
edge IDs of the form `file:line:column`; until then no total in this receipt is a
CFG edge count.

## Review batch — the five remaining concentrated regions

Each region's caller chain was followed to where the result is consumed,
converted, retried, or reaches the task boundary.

| region (phase, write line) | callers | how consumed |
|---|---|---|
| `try_visit_streaming_inner_join_scan_probe_chunks` (Build, 6116) | 8879, 8908, 8936, 8968 | `let Some(route) = … .await?` — propagates |
| `try_visit_streaming_reoriented_inner_chain_scan_probe_base` (Probe, 12776) | 12459, 12861 | **mixed**: 12459 `.await?` propagates; 12861 `let result = … .await;` catches |
| `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` (Build, 6474) | 5839 | `let Some((…)) = … .await?` — propagates |
| `try_load_resident_partitioned_streamed_inner_scan_build` (Build, 17584) | 17376 | `.await?` — propagates |
| `build_partitioned_streaming_scan_inner_build_reservoir` (Build, 17741) | 13206, 14231, 14958, 20323 | **mixed**: 13206 and 14231 `.await;` catch; 14958 `.await?` propagates |

### What the mixed callers do and do not change

Two regions are reached from callers that disagree — some catch the error and
continue, others propagate it. That looked like it would force a per-caller
disposition, and it does not, for a reason worth stating:

**The region-local result boundary makes the discard decision caller-independent.**
If the region returns `Err`, its legacy write did not execute, so discarding
preserves behaviour no matter what the caller then does. Caller behaviour decides
something narrower — whether a receipt could still publish on that path, and
therefore whether the unresolved backstop is ever reachable. It does not decide
the disposition.

So all five take the same shape as region 1:

- open at the legacy timer boundary;
- run the existing body into a captured result;
- resolve once from that result — `Err` and rejected outcomes `discard()`;
- `commit()` only at the old writer boundary;
- unresolved-on-drop remains the backstop, with no generic
  "error means discard" RAII default.

`Ok(Some)` is **not** treated as globally synonymous with commit. In these six
regions the old writer boundary happens to sit on the success path, but the
remaining 42 may use the same shape for handoff, and the disposition stays
region-specific.

### Status

6 of 48 edge-bearing regions classified. 42 remain, each requiring its caller
chain read rather than its window alone. The 40/0 control stays red throughout.

## Pilot 1 (propagating) — `try_load_resident_partitioned_streamed_inner_scan_build`

Region: timer `build_started` at `17474`; writer at `17582`; success return
`Ok(Some((loaded_builds, loaded_resident_bytes, total_build_rows)))` at `17590`.
Sole caller `17376` consumes with `.await?`.

### Path audit — 11 candidate occurrences reduce to 4 edits

| line | occurrence | crosses the writer boundary? | disposition |
|---|---|---|---|
| 17526, 17536 | `continue` | no — targets `for partition in group_range` at `17522`, inside the region | **internal**, no decision |
| 17477, 17486, 17569 | `?` / `await?` | yes, but returns `Err` | **terminal**: caller propagates with `.await?`, so no receipt publishes; veto unreachable |
| 17518, 17531, 17557 | `return Err(error)` | yes, returns `Err` | **terminal**, same reasoning |
| 17516, 17550, 17572 | `return Ok(None)` | yes, returns **`Ok`** | **discard()** — the caller's `.await?` succeeds with `None` and continues, so a receipt can still publish |
| 17582 | legacy writer | reaches it | **commit()** exactly there |

The decisive split is not error-versus-success but whether the caller keeps
running. The three `Ok(None)` rejections return successfully, the caller
continues, and a receipt can still publish — those are the paths that need an
explicit `discard()`. The six `Err` paths reach the task boundary through the
caller's `?`, so no receipt exists to contaminate.

**Edit set: 3 discards + 1 commit.** Eleven occurrences, four edits.

### The async block is not applicable here

The region's mutable state — `loaded_builds`, `loaded_resident_bytes`,
`total_build_rows` — is written inside the loop and **read after** the region, by
the writer and the success return. An async block would have to return that state
rather than a bare result, changing the function's shape and its drop order,
which is exactly the case the caution excludes. Placing `discard()` at the three
`Ok` exits and `commit()` at the writer needs no restructuring, no retargeted
`return`, and no drop-order change.

The async-block form remains right for regions whose result is immediately
propagated and whose state does not outlive the region — region 1 is one. The
pattern is therefore **per region tree, chosen by whether state crosses the
writer boundary**, not applied uniformly.

## Pilot 1 correction — the error paths are **not** terminal

The "terminal, no edit" verdict above rested on reading **one** caller. Traced to
the receipt boundary it does not hold:

```
region writer 17584
  → 17376  .await?                    propagates
  → probe_reoriented_inner_join_streaming_with_profile (17281)
  → 12716  .await?                    propagates
  → try_visit_streaming_reoriented_inner_chain_scan_probe_base (12478)
  → 12861  let result = … .await;     CATCHES — and keeps running
```

At `12861` the result is bound without `?`, and execution continues into
`record_join_runtime_observation`. So an `Err` raised in the pilot region can be
caught three levels up on a path that then **records telemetry and publishes a
receipt**.

Consequences, both of which invalidate earlier conclusions in this receipt:

- **The six `Err` paths need an explicit disposition after all.** Left
  unresolved, they would withhold a receipt that today publishes — turning
  "receipt without this `build_micros`" into "no receipt", which is a behaviour
  change, not a preserved one.
- **The 11-to-4 reduction does not stand.** The edit set is three `Ok(None)`
  discards, six `Err`-path dispositions, and one commit. Three of those `Err`
  edges are bare `?`, which cannot carry a statement before propagating, so this
  region needs restructuring after all — while still carrying mutable state
  across the writer boundary that makes a plain async block unsuitable.

**Both of the pilot's conclusions rested on the same unverified assumption**, and
the caller chain was the only thing that could have exposed it. No region may be
classified from its own body, nor from its immediate caller: the chain must be
followed until the result is consumed, converted, retried, or reaches the task
boundary.

Note that region 3 in the batch above — `try_visit_streaming_reoriented_inner_chain_scan_probe_base`
— is the very frame that catches here. Its "mixed consumption" was recorded as a
property of that region; it is also the reason a different region's errors are
non-terminal. Mixed consumption propagates *downward* through the call graph, and
the remaining 42 must be classified with that in mind.

## Census correction — the three "copied" sites are wall timers

All three sites classified as copied or profile-derived are in fact **wall
timers**, hoisted one line earlier into a local:

```rust
let build_micros = duration_micros(build_started);
record_task_telemetry(&telemetry, |task| {
    task.phases.build_micros = task.phases.build_micros.saturating_add(build_micros);
});
```

- `join.rs:427` — `execute_hash_join`
- `join.rs:716` — `execute_hash_join_into_global_aggregate`
- `streaming.rs:16650` — `prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline`

The extractor only recognised `duration_micros(x)` **inside** the assignment, so
hoisting it into a variable made the site look like a copied value. There are
therefore **63 wall-timer sites and zero copied ones**, not 59 and 3. The 63rd,
identified during implementation, is the hoisted `membership_micros` writer
described in the quarantine correction below.

This mattered: excluding them meant three authoritative envelopes would have
stayed unwrapped while the publication switch removed legacy authority, and their
build time would have vanished from the receipt entirely — a silent
under-report, the mirror image of the inflation the rest of this record guards
against. Both directions of error come from trusting the mechanical census
without reading the site.

The `nest` column has the same weakness: a timer whose `Instant::now()` is
hoisted or passed in is invisible to it. Each region is read at wrapping time.

## Backstop guard requirement

Every one of the **63** wall-timer sites must map to exactly one disposition
record, and the guard fails if any site lacks one. With zero copied sites, there
is no unwrapped-by-design category left in `join.rs` or `streaming.rs`; the only
excluded sites are the 6 server/telemetry sinks, which are aggregation points and
not envelopes.

## Three boundaries, not two — and pilot 1 does not satisfy the equivalence test

The authority is the triple **(timer start, duration measurement, telemetry
write)**. Treating it as start-to-write is wrong wherever the measurement is not
taken at the write.

### Pilot 1 checked against that triple

- **timer start**: `let build_started = Instant::now();`
- **duration measurement**: `duration_micros(build_started)` evaluated **inside**
  the `record_task_telemetry` closure
- **telemetry write**: the same closure
- **commit() as placed**: *after* `record_task_telemetry(...)` returns

The closure does not run until `record_task_telemetry` has acquired the mutex and
recorded `telemetry_lock_micros`. So the legacy interval ends part-way through
that call, while the committed scope ends after it returns. They differ by the
statements following the `build_micros` line inside the closure, and by the
closure/lock teardown.

**Pilot 1 therefore fails the shape-1 test.** There is no *exit* between
measurement and writer, which is what I checked, but shape 1 requires the two
boundaries to be the *same instant*, and they are not. The delta is small and
data-independent, which is exactly what would let it pass unnoticed and then sit
inside every published operator figure.

A related pre-existing fact, worth recording though not introduced here: because
the measurement happens after the lock is taken, legacy `build_micros` already
contains the lock wait that `telemetry_lock_micros` also records. Reproducing the
legacy boundary reproduces that overlap; moving the boundary would silently
change it.

### Consequence

The `finish()` / `commit()` split is required, not optional:

- `finish()` closes the interval **at the legacy duration-measurement point**,
  capturing it;
- `commit()` publishes the captured interval **at the legacy writer**;
- an exit between the two discards the finished interval.

Pilot 1 must be reworked to that shape before it is used as a template, and the
same (start, measurement, write) triple must be established for all 63 writer
sites rather than assumed to collapse to two points. Several sites may share one
start and one still-active transition when they are mutually exclusive fallback
publications.

### Census columns are now aids only

Candidate locations remain useful. Timer classification, nesting, acquisition
visibility, exit disposition, and now boundary collapse all require source
confirmation. The manifest must record all three boundaries plus the enclosing
function under stable identifiers, not line numbers alone. Exactly 63 writer
sites resolve through their lifecycle topology; the six server/telemetry sinks
stay excluded.

---

# QUARANTINE — derived columns in this receipt are VOID

Everything above that was **derived** by the extractor is withdrawn. Only one
output is trusted:

> **The set of 63 legacy phase assignments**, validated directly against both
> files: 21 in `join.rs` and 42 in `streaming.rs`.

The earlier extractor's supposedly exact set contained only 62. It omitted
`probe_micros += membership_micros` in
`try_streaming_projected_left_semi_grouped_aggregate`: `membership_micros` is a
hoisted `duration_micros(membership_started)` value, but its name does not encode
the Probe phase. Exact assignment matching against `task.phases.{build,probe,spill}_micros`
finds all 42 streaming writers. This is the sixth census defect and invalidates
the earlier 41/62 denominator; the candidate-location table above is retained as
methodology evidence, not as the authoritative set.

## Void — do not reuse

| withdrawn | why |
|---|---|
| timer-start line (`ts`) | the variable→line map is keyed by name only, never reset per file or function. `build_started` recurs in dozens of functions, so a site resolves to whatever definition was seen last — possibly in another function or another file, with per-file line numbers applied across files. Site `streaming.rs:5897` claimed `ts=5833`, a **blank line**; the real definition is `5834`. |
| `Ok(None)` / `err?` / early-return counts | computed over windows bounded by that `ts`. Wrong window, wrong counts. |
| `acq` (acquisition inside) | textual only; blind to acquisition inside callees. Already flagged as a lower bound, now also built on a wrong window. |
| `nest` | blind to hoisted or passed-in timers, and built on the wrong window. |
| boundary-shape classification (46 direct / 13 nested / 3 hoisted) | produced depths of 0 and 1 for method bodies, which is structurally impossible. Never used. |

**Every derived column has been wrong at least once, in both directions** —
inflating (`Ok(None)`-only exits, one-level caller reachability, boundary
collapse) and under-reporting (three wall timers misread as copied values). The
candidate *locations* held; nothing inferred from them did.

The corrections are retained above as methodology evidence. The numbers in them
are not inputs to any decision.

# Read-and-wrap workflow

No semantic property is derived mechanically. For each site:

1. assign a stable ID — `file / enclosing function / phase / ordinal`;
2. **read the function** to establish start, measurement, publication, and every
   boundary-crossing exit;
3. wrap immediately with `OperatorTransition`;
4. record the verified tuple and dispositions here;
5. compile after small batches, and run targeted controls per boundary shape.

Batching is a verification convenience only. It is never a source of inference:
two sites sharing a shape do not share a disposition.

# Manifest

| site ID | start | measurement | publication | pre-measurement exits | status |
|---|---|---|---|---|---|
| `streaming.rs / try_load_resident_partitioned_streamed_inner_scan_build / Build / 1` | `let build_started = Instant::now();` before the `for group` loop | `duration_micros(build_started)` inside the `record_task_telemetry` closure, after the mutex | that same closure | 9 — three `Ok(None)`, three `return Err`, three `?`; two `continue` are internal to loops inside the region | **wrapped, verified** |

62 sites remain unread. Pilot 1 is the only certified template.

## Final source guard (to add before the flip)

Keyed on the stable IDs, proving: every one of the 63 legacy writers has exactly
one manifest entry; every selected writer finishes and commits the transition
opened at its timer start; no unknown or duplicate site IDs; after the flip, zero
legacy join-phase duration writers remain; and the six aggregation sinks remain
present and excluded. A timer start may feed several mutually exclusive writer
IDs, so writer coverage is not inferred from a raw count of transition opens.

## streaming.rs hazard survey (structural, not derived)

Checked against the hazards that would need a third template. These rest only on
statement text and closure structure — both directly verifiable — not on the
voided extractor columns.

| hazard | finding |
|---|---|
| legacy assignment overwrites rather than accumulates | **absent** — all 42 accumulate from the same field |
| multiple phase writes in one telemetry closure | **one case** — the membership-first chain publishes independently measured Probe and Build intervals in one closure |
| phase write outside a telemetry closure | **absent** |
| closure runs but returns before the phase write | **absent** — no early return inside any closure |
| measurement position | **40 inside** the closure (pilot 1 template), **2 hoisted** (the certified Build shape and the newly recovered membership Probe) |

These axes did not reveal a third template. Per-site reading later did: one timer
start can feed several mutually exclusive phase writers after fallback attempts.
That alternative-publication lifecycle cannot be inferred from assignment text.

**Two hazards remain open and require per-site reading**, and are not answered
here:

- *Measurement inside a loop*, needing one transition per iteration. The timer
  start position cannot be recovered mechanically — that was the voided `ts`
  column — so it is read per site.
- *Nested closures*. The mechanical check produced only false positives, matching
  each `record_task_telemetry` call against its own closure. Recorded as
  inconclusive rather than clean.

### Site density — why lifecycle counting matters

The 42 sites live in **18 distinct functions**. Eight functions carry more than
one, and `try_streaming_left_outer_grouped_aggregate` carries four.

A guard counting textual `finish_at_measurement` calls would therefore be
meaningless: one function may legitimately need several independent transitions,
while one transition may legitimately have several mutually exclusive writer
IDs. A helper expansion could also satisfy a textual count while leaving a real
writer unwrapped. The guard must compare stable manifest IDs with stable source
markers; source scanning remains only a coverage backstop.

## Completion arithmetic — corrected

An earlier report of "2 wrapped, 38 remaining" against what was then believed to
be 41 sites did not add up. The corrected denominator is 42.
Reconciled against source:

| quantity | value | how counted |
|---|---|---|
| streaming.rs phase assignments | **42** | exact phase-field assignment match |
| `OperatorTransition::open` sites | **2** | source |
| `finish_at_measurement` calls | **2** | source |
| **remaining** | **40** | 42 − 2 |

One missing envelope was **`streaming.rs:16593`**. Its function,
`prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline`, carries
**two** sites — 16593 and 16664. Wrapping 16664 for the hoisted certification
made the *function* look handled, and the site at 16593 was neither wrapped nor
recorded.

The other was the hoisted membership Probe writer above. It disappeared because
the extractor treated a variable whose name did not repeat the phase as outside
the timing set. Both omissions reinforce that the phase-field assignment, not a
timer-variable naming convention, is the coverage backstop.

This is exactly the multi-site hazard, and it hid inside completion arithmetic
rather than inside code. Two consequences, both now binding:

- **A writer site is the manifest unit, never a function.** Each phase assignment
  gets its own entry with start, measurement, publication and pre-measurement
  exits, even when it shares a function, closure, loop or timer start with
  another. Lifecycle topology is recorded separately: mutually exclusive writer
  sites may resolve one shared transition.
- **Completion arithmetic must reconcile against source on every report**, not be
  carried forward from the previous one. The count is derived, and derived values
  in this arc have a perfect record of being wrong.

### Manifest — wrapped and verified

| site ID | shape | status |
|---|---|---|
| `streaming.rs / try_load_resident_partitioned_streamed_inner_scan_build / Build / 1` | measurement inside closure | wrapped, verified |
| `streaming.rs / prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline / Build / 2` (line 16664) | hoisted measurement | wrapped, verified |

### Manifest — explicitly outstanding

| site ID | note |
|---|---|
| `streaming.rs / prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline / Build / 1` (line 16593) | **unwrapped**; shares a function with a wrapped site. Recorded so it cannot be absorbed into its neighbour again. |

39 streaming envelopes outstanding, including 16593.

---

# AUTHORITATIVE MANIFEST

Supersedes every earlier table in this receipt. Identity is
`file / enclosing function / phase / frozen ordinal` — **never** a line number,
which shifts on every edit, and never source text, since two envelopes in
`streaming.rs` are byte-identical.

Each wrapped envelope carries a matching `ADR0551-SITE:` marker in source. The
architecture guard requires **exact set equality** between markers and the
verified IDs below, so a duplicated marker cannot mask a missing one. Markers
survive removal of the legacy writers, giving the final guard something durable
to check.

## Verified

- ADR0551-ID: `streaming/prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline/Build/1`
  - start: `let build_started = Instant::now();` after `spilled.use_compact_resident_index()`
  - measurement: **inside** the `record_task_telemetry` closure
  - publication: that closure; commit after it returns (pilot-1 template)
  - pre-measurement exits: **1** — `return Err(error)` after `spilled.cleanup()`.
    The `?` inside the preceding `async` block is **internal**: it exits the block,
    not the function, and surfaces as `build_result`.
- ADR0551-ID: `streaming/prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline/Build/2`
  - start: `let build_started = Instant::now();` after `key_arrays(&build_batch, …)`
  - measurement: **hoisted**, `let build_micros = duration_micros(build_started);`
  - publication: closure; commit **inside** it after the legacy write
  - pre-measurement exits: **2** — the `?` on `build_in_memory_join_rows`, and the
    `budget_exhausted` else-branch. These share one `let … = expr? else` statement:
    two semantic exits, one source line.
  - independence: ordinal 1's branch returns before this region is reachable, so
    the two lifecycles are mutually exclusive and no finished-child reservation
    crosses them.
- ADR0551-ID: `streaming/try_load_resident_partitioned_streamed_inner_scan_build/Build/1`
  - start: before the `for group` loop
  - measurement: **inside** the closure, after the mutex
  - publication: closure; commit after it returns (pilot-1 template)
  - pre-measurement exits: **9** — three `Ok(None)`, three `return Err`, three `?`.
    Two `continue` edges are internal to loops within the region.

- ADR0551-ID: `streaming/try_streaming_left_outer_grouped_aggregate/Build/1`
  - start: `let build_started = Instant::now();` inside the retained-column branch
  - diagnostic read: the conditional throughput-profile measurement occurs before
    the phase measurement and does not finish the transition
  - authoritative phase measurement: **inside** the `record_task_telemetry` closure
  - publication: that closure; commit after it returns (pilot-1 template)
  - pre-measurement exits: **2** — errors from
    `write_streaming_scan_inner_build_batches` and `spilled.finish`; both preserve
    the original error value
  - topology: sequential before `Probe/1`; the build transition fully resolves
    before the probe transition opens
- ADR0551-ID: `streaming/try_streaming_left_outer_grouped_aggregate/Probe/1`
  - start: `let probe_started = Instant::now();` inside the retained-column branch
  - diagnostic read: the conditional throughput-profile measurement occurs before
    the phase measurement and does not finish the transition
  - authoritative phase measurement: **inside** the `record_task_telemetry` closure
  - publication: that closure; commit after it returns (pilot-1 template)
  - pre-measurement exits: **4** — errors from decoded-page admission, scan probing,
    unmatched-row folding and empty-input aggregate folding; each preserves the
    original error value
  - topology: sequential after `Build/1`; the build transition fully resolves
    before this probe transition opens
- ADR0551-ID: `streaming/try_streaming_left_outer_grouped_aggregate/Probe/2`
  - start: `let probe_started = Instant::now();` at function top level
  - diagnostic reads: none for this timer
  - authoritative phase measurement: **inside** the `record_task_telemetry` closure
  - publication: that closure; commit after it returns (pilot-1 template)
  - pre-measurement exits: **2** — the `.await?` on
    `visit_streaming_left_outer_scan_probe_chunks`, and the `?` on
    `fold_bound_grouped_aggregate_batch`
  - topology: sequential after `Build/2`; the two never overlap, so no
    finished-child reservation crosses them
- ADR0551-ID: `streaming/try_streaming_left_outer_grouped_aggregate/Build/2`
  - start: `let build_started = Instant::now();` at function top level
  - diagnostic reads: none for this timer
  - authoritative phase measurement: **inside** the `record_task_telemetry` closure
  - publication: that closure; commit after it returns (pilot-1 template)
  - pre-measurement exits: **2** — the error from
    `build_in_memory_join_rows` and its `None` budget rejection. The earlier
    handoff named only the `Ok(None)` exit; rereading the current source before
    editing found the `?` on the same expression and preserved its exact error.
  - topology: sequential before `Probe/2`; both fully resolve before the next
    lifecycle opens
- ADR0551-ID: `streaming/try_visit_streaming_inner_join_scan_probe_chunks/Build/1`
  - shared start: one `build_started` and one transition span all three mutually
    exclusive Build writer sites in this function
  - selected route: streamed spilled scan build
  - authoritative phase measurement: **inside** its telemetry closure
  - publication: that closure; commit after it returns
  - prior route outcomes: partitioned-scan-build error discards; its selected
    handoff discards because the nested route owns its own phases; `None` falls
    through with the transition still active
- ADR0551-ID: `streaming/try_visit_streaming_inner_join_scan_probe_chunks/Build/2`
  - shared start: the same still-active transition as Build/1; no second bracket
    opens after a preceding fallback
  - selected route: streamed spilled materialized build
  - authoritative phase measurement: **inside** its telemetry closure
  - publication: that closure; commit after it returns
  - prior route outcomes: missing build batches and errors discard; a selected
    partitioned-materialized handoff discards; unselected alternatives fall
    through with the transition active
- ADR0551-ID: `streaming/try_visit_streaming_inner_join_scan_probe_chunks/Build/3`
  - shared start: the same transition after all preceding alternatives declined
  - selected route: in-memory scan probe
  - authoritative phase measurement: **inside** its telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: concat, key-array, key-plan and build-table errors plus
    memory and absent-build rejections all discard explicitly
  - topology: Build/1, Build/2 and Build/3 are alternative writer IDs for one
    lifecycle, never three overlapping transitions
- ADR0551-ID: `streaming/try_visit_streaming_inner_join_scan_probe_chunks/Probe/1`
  - start: after the selected streamed-spilled scan build publishes Build/1
  - authoritative phase measurement: **inside** its telemetry closure
  - publication: unconditional for both `Ok` and `Err` probe results, matching the
    legacy writer; commit after the closure returns
  - pre-measurement exits: none — the probe result is captured without `?` and is
    propagated only after timing, runtime observations and cleanup
  - topology: independent of Probe/2 and Probe/3 because their build routes are
    mutually exclusive
- ADR0551-ID: `streaming/try_visit_streaming_inner_join_scan_probe_chunks/Probe/2`
  - start: after the selected streamed-spilled materialized build publishes Build/2
  - authoritative phase measurement: **inside** its telemetry closure
  - publication: unconditional for both `Ok` and `Err` probe results, matching the
    legacy writer; commit after the closure returns
  - pre-measurement exits: none — the probe result is captured without `?` and is
    propagated only after timing, runtime observations and cleanup
  - topology: independent of Probe/1 and Probe/3 because their build routes are
    mutually exclusive
- ADR0551-ID: `streaming/try_visit_streaming_inner_join_scan_probe_chunks/Probe/3`
  - start: after the selected in-memory build publishes Build/3
  - authoritative phase measurement: **inside** its telemetry closure
  - publication: unconditional for both `Ok` and `Err` probe results, matching the
    legacy writer; commit after the closure returns
  - pre-measurement exits: none — the probe result is captured without `?` and is
    propagated only after timing and local phase diagnostics
  - topology: independent of Probe/1 and Probe/2 because their build routes are
    mutually exclusive
- ADR0551-ID: `streaming/try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks/Build/1`
  - lifecycle: one transition per partition group; every iteration resolves
    before the matching Probe/1 transition opens
  - start: after the group build observer opens and before scan-build writing
  - diagnostic timers: group/profile and sub-leaf timers are independent; none
    chooses the authoritative phase boundary
  - authoritative phase measurement: **inside** the per-group telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **10** — build writing, finish-timer creation, build
    finish, finish-timer completion, coalesced-index completion, four lifecycle
    counter overflows and residency recording; each explicitly discards while
    preserving the existing cleanup and error value
  - internal control flow: a missing partition build `continue`s inside the
    partition loop and does not resolve the group transition
- ADR0551-ID: `streaming/try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks/Probe/1`
  - lifecycle: one transition per partition group, opened only after that group's
    Build/1 transition commits
  - authoritative phase measurement: **inside** the per-group telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — a failed group probe discards before preserving
    the existing partition cleanup and exact error
  - topology: Build/1 and Probe/1 are sequential within each iteration; both
    resolve before the next group opens either lifecycle
- ADR0551-ID: `streaming/try_visit_partitioned_streamed_inner_materialized_build_probe_chunks/Build/1`
  - lifecycle: one transition per partition group
  - start: after the group partitions are created and before materialized build
    batches are partitioned and written
  - authoritative phase measurement: **inside** the per-group telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **6** — partition-index conversion, partition batch
    selection, build writing, spill-budget exhaustion, build finish and index
    rebuild; all explicitly discard while preserving existing cleanup and errors
  - internal control flow: empty partition batches and missing partition builds
    `continue` inside the nested loops without resolving the group transition
  - topology: fully resolves before the matching Probe/1 lifecycle opens
- ADR0551-ID: `streaming/try_visit_partitioned_streamed_inner_materialized_build_probe_chunks/Probe/1`
  - lifecycle: one transition per partition group, after that group's Build/1
  - authoritative phase measurement: **inside** the per-group telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **3** — partition-index conversion, partition batch
    selection and spilled-probe execution; each preserves the original error
  - internal control flow: empty partition batches, missing builds and empty builds
    `continue` inside the nested loops without resolving the transition
  - topology: resolves before group cleanup and before the next iteration opens
- ADR0551-ID: `streaming/try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks/Build/1`
  - lifecycle: one transition per partition group
  - start: after group partition creation and before materialized build batches
    are partitioned and written
  - authoritative phase measurement: **inside** the per-group telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **6** — partition-index conversion, partition batch
    selection, build writing, spill-budget exhaustion, build finish and index
    rebuild; each preserves the existing error and cleanup behavior
  - internal control flow: empty partition batches and missing partition builds
    `continue` inside the nested loops without resolving the transition
  - topology: resolves before its scan Probe/1 lifecycle opens
- ADR0551-ID: `streaming/try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks/Probe/1`
  - lifecycle: one transition per partition group, after that group's Build/1
  - authoritative phase measurement: **inside** the per-group telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — failed scan probing discards before preserving
    the existing partition cleanup and exact error
  - topology: resolves before group cleanup and before the next iteration opens
- ADR0551-ID: `streaming/try_visit_streaming_join0_chunks/Build/1`
  - lifecycle: first of two independent build epochs in the function
  - start: after both materialized inputs exist and before the partitioned-route
    attempt
  - authoritative phase measurement: **inside** the spilled-build telemetry closure
  - publication: that closure; commit after it returns
  - dispositions: partitioned-route error discards; a selected partitioned
    handoff discards because the nested route owns its phases; spilled-build
    creation error discards; no selected spilled build discards before the second
    in-memory build epoch opens
  - topology: publishes only for the spilled-build route; otherwise it resolves
    without crossing the later Build/2 lifecycle
- ADR0551-ID: `streaming/try_visit_streaming_join0_chunks/Probe/1`
  - start: after the spilled Build/1 writer commits
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both filtered/unfiltered and `Ok`/`Err` probe
    results, matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — both probe variants return a captured result
    that is propagated only after timing, observations and cleanup
  - topology: exists only on the spilled-build branch and never overlaps Probe/2
- ADR0551-ID: `streaming/try_visit_streaming_join0_chunks/Build/2`
  - lifecycle: second independent build epoch, reached only after Build/1
    discarded its unselected route
  - authoritative phase measurement: **inside** the in-memory telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** — build-table construction error and absent build
    rows; both discard explicitly and preserve the original outcome
  - topology: resolves before Probe/2 opens
- ADR0551-ID: `streaming/try_visit_streaming_join0_chunks/Probe/2`
  - start: after in-memory Build/2 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — in-memory probe failure discards and preserves
    the exact kernel error
  - topology: independent of spilled Probe/1 and resolves before selection output
    materialization
- ADR0551-ID: `streaming/try_streaming_scan_left_semi_grouped_aggregate/Build/1`
  - start: after the spilled build is created and immediately before its projected
    input is written
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — the captured asynchronous build failure cleans
    up the spill, discards the transition and preserves the exact error
  - internal control flow: write and finish errors leave the inner async block and
    surface through the single captured result; they are not separate function exits
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `streaming/try_streaming_scan_left_semi_grouped_aggregate/Probe/1`
  - start: after Build/1 publishes and immediately before the projected streamed
    semi-aggregate fold
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured fold results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — the result is propagated only after the phase
    write, runtime observation and aggregate finalization setup
  - topology: resolves before spill cleanup and final aggregate materialization
- ADR0551-ID: `streaming/try_visit_reoriented_adaptive_derived_last_stage/Build/1`
  - start: after the derived input and join-key plan are materialized, immediately
    before the in-memory build rows are created
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** sharing the build call — its exact kernel error
    and an absent build result each discard explicitly before preserving the
    original outcome
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `streaming/try_visit_reoriented_adaptive_derived_last_stage/Probe/1`
  - start: after Build/1 publishes and immediately before either the materialized
    or streaming probe route
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 result boundary** covering either probe route and
    any error surfaced by its visitor callback; it discards before preserving the
    exact error
  - topology: the two probe routes are alternatives under one lifecycle and the
    transition resolves before their row counts are recorded
- ADR0551-ID: `streaming/try_visit_reoriented_adaptive_derived_last_stage_to_sink/Build/1`
  - start: after the derived input and join-key plan are materialized, immediately
    before the in-memory build rows are created
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** sharing the build call — its exact kernel error
    and an absent build result each discard before preserving the original outcome
  - topology: resolves before the sink Probe/1 lifecycle opens
- ADR0551-ID: `streaming/try_visit_reoriented_adaptive_derived_last_stage_to_sink/Probe/1`
  - start: after Build/1 publishes and before the sink-driving probe async block
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured probe results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — all materialized-route, scan-route, filter,
    repartition and sink errors surface through the captured async-block result
    and are propagated only after publication
  - topology: one lifecycle covers the two alternative probe routes and resolves
    before their row counts are recorded
- ADR0551-ID: `streaming/try_visit_streaming_reoriented_inner_chain_scan_probe_base/Build/1`
  - start: at the legacy hash-build timer boundary, after base preparation and
    immediately before the in-memory build rows are created
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** sharing the build call — its exact kernel error
    and an absent build result each discard before preserving the original outcome
  - topology: resolves before the scan is opened and before Probe/1 begins
- ADR0551-ID: `streaming/try_visit_streaming_reoriented_inner_chain_scan_probe_base/Probe/1`
  - start: after the scan stream is opened, exactly at the legacy probe timer
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering the stream
    payload, scan filter, key extraction, join probe, selection materialization,
    join filter, repartition, suffix probe, suffix filter and output-visitor errors;
    it discards before preserving the exact error
  - internal control flow: stream exhaustion, row-limit exhaustion, empty filtered
    batches, empty selections and exhausted suffixes remain `break`/`continue`
    inside the captured probe loop
  - topology: only the two row counters cross the captured async boundary; the
    transition resolves before runtime observation publication
- ADR0551-ID: `streaming/try_streaming_inner_prefix_semi_anti_grouped_aggregate/Build/1`
  - lifecycle: fallback base-join build inside the function's captured execution
    result, reached only when the specialized scan-probe route declines
  - start: immediately before the in-memory base build rows are created
  - authoritative phase measurement: **inside** the second telemetry closure,
    after the first closure publishes row counters only
  - publication: that phase closure; commit after it returns
  - pre-measurement exits: **2** sharing the build call — its exact kernel error
    and an absent build result each discard before preserving the original outcome
  - topology: resolves before the fallback Probe/1 lifecycle opens
- ADR0551-ID: `streaming/try_streaming_inner_prefix_semi_anti_grouped_aggregate/Probe/1`
  - lifecycle: fallback base-join probe inside the same captured execution result
  - start: immediately after Build/1 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — the in-memory probe kernel error discards before
    preserving the exact error
  - topology: resolves before selection materialization, repartition and the
    remaining inner/semi/anti chain; their later errors do not belong to this probe
    envelope under the legacy boundary
- ADR0551-ID: `streaming/try_streaming_inner_prefix_semi_anti_scan_probe_base/Build/1`
  - start: after base preparation and immediately before the in-memory build rows
    are created
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** sharing the build call — its exact kernel error
    and an absent build result each discard before preserving the original outcome
  - topology: resolves before the scan request and probe stream are prepared
- ADR0551-ID: `streaming/try_streaming_inner_prefix_semi_anti_scan_probe_base/Probe/1`
  - start: after the scan stream is opened and immediately before the base drive
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2 result boundaries** — the captured direct/deferred
    drive result (after its existing cleanup) and the final profile flush; each
    discards before preserving the exact error
  - internal control flow: all scan, replay, suffix and fold errors converge on the
    captured drive result; stream/row-limit exhaustion and empty batches remain
    internal loop control
  - topology: resolves before the join runtime observation is published
- ADR0551-ID: `streaming/prepare_reoriented_inner_join_runtime_with_scan_build/Build/1`
  - lifecycle: the single-partition streamed-scan build route; the multi-partition
    route returns before this timer exists
  - start: after the spill writer is created and immediately before scan-build
    batches are written
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — the captured write-or-finish failure cleans up
    the spill, discards the transition and preserves the exact error
  - topology: resolves before the prepared runtime is returned
- ADR0551-ID: `streaming/build_partitioned_streaming_scan_inner_build_reservoir/Build/1`
  - start: after writer setup completes and immediately before the partitioned
    scan-build reservoir is written
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **6 result boundaries** — the main write result, the
    post-write residency phase, finish-timer creation, per-partition finish, the
    post-finish residency phase and finish-timer publication; each discards before
    preserving the existing diagnostic, cleanup and return order
  - dispositions: the uncommitted-owner budget exhaustion remains `Ok(None)`;
    every other failed boundary preserves its exact error
  - topology: the per-partition loop `continue`s for absent/empty writers are
    internal; the transition resolves before membership spill accounting and the
    reservoir return
- ADR0551-ID: `streaming/try_streaming_projected_left_semi_grouped_aggregate/Probe/1`
  - lifecycle: membership-side execution and presence-set construction, recovered
    after the original census omitted its hoisted phase value
  - start: immediately before the membership subtree executes
  - authoritative phase measurement: **hoisted before** the telemetry closure at
    `membership_micros = duration_micros(membership_started)`; the transition
    freezes immediately before that expression
  - publication: the later shared telemetry closure; commit immediately after its
    Probe write and before the independent Build measurement
  - pre-measurement exits: **8 result boundaries** — membership timer creation,
    subtree execution, membership timer finish, presence timer creation, presence
    construction, presence timer finish, absent presence and presence-profile
    publication; each discards before preserving the existing outcome
  - post-measurement rule: any chain preparation/drive/cleanup failure skips the
    legacy Probe write and therefore leaves this finished token unresolved; the
    receipt is withheld rather than publishing a value the legacy path omitted
- ADR0551-ID: `streaming/try_streaming_projected_left_semi_grouped_aggregate/Build/1`
  - lifecycle: chain runtime preparation, drive and cleanup; independent of the
    preceding membership Probe timer despite sharing its publication closure
  - start: after the membership measurement and immediately before chain runtime
    preparation
  - authoritative phase measurement: **inside** the shared telemetry closure,
    after the Probe write and Probe commit
  - publication: that Build write; commit after the closure returns
  - pre-measurement exits: **8 result boundaries** — prepare-timer creation,
    runtime preparation, prepare-timer finish, absent committed runtime,
    cleanup-timer creation, cleanup-timer finish, a declined drive and a failed
    drive; each discards before preserving the existing error
  - topology: the Probe transition is already frozen and off the nesting stack
    when this Build transition opens; the two intervals are sequential, not nested
- ADR0551-ID: `streaming/try_streaming_reoriented_inner_chain_scan_probe_base_to_sink/Spill/1`
  - start: after the base and intermediate spill writers exist and immediately
    before the base-probe/intermediate-spill drive
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **6 result boundaries** — failed base drive, declined
    base drive, intermediate finish failure, spill-byte underflow, base-probe
    profile publication and intermediate profile publication; each discards after
    the existing cleanup and before preserving the exact outcome
  - internal control flow: every scan/write/read/probe/filter error inside the
    captured base result converges on its single outer boundary
  - topology: resolves before base-runtime observation/cleanup and before the
    terminal Build/1 lifecycle opens
- ADR0551-ID: `streaming/try_streaming_reoriented_inner_chain_scan_probe_base_to_sink/Build/1`
  - start: after intermediate Spill/1 and base-runtime cleanup, immediately before
    the grouped terminal build/probe drive
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **4 result boundaries** — captured terminal drive,
    terminal-build spill profile publication, engine-spill total overflow and
    engine-spill profile publication; each discards before preserving the error
  - internal control flow: all per-group creation, write, finish, read, probe,
    filter and sink errors converge on the captured terminal result after existing
    per-group and intermediate cleanup
  - topology: resolves before terminal row counts are folded into the runtime
- ADR0551-ID: `streaming/probe_partitioned_streamed_inner_scan_build/Build/1`
  - start: after probe partitioning proves there is work and immediately before
    the grouped scan-build/probe drive
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering invalid group
    bounds/partition indices, writer creation, scan-build writing, writer finish,
    parallel-plan conversion and every parallel/sequential read/probe failure; it
    discards after the existing per-group cleanup and preserves the exact error
  - internal control flow: empty groups/builds, unavailable partitions and rejected
    parallel result slots remain `continue`/`break` inside the captured body
  - topology: only output and the four accumulated counters cross the async
    boundary; the transition resolves at the one legacy Build writer
- ADR0551-ID: `join/execute_hash_join/Build/1`
  - start: after all spill-route fallbacks decline and immediately before the
    in-memory hash build
  - authoritative phase measurement: **hoisted before** the telemetry closure at
    `build_micros = duration_micros(build_started)`; freeze at that expression
  - publication: the telemetry closure; commit immediately after the Build write
    inside it, so a skipped closure cannot publish
  - pre-measurement exits: **2** sharing the build call — its exact kernel error
    and an absent build that hands off to the spilled helper; both discard before
    preserving the existing outcome
  - topology: resolves before build profiling and Probe/1 opens
- ADR0551-ID: `join/execute_hash_join/Probe/1`
  - start: after Build/1 profiling and immediately before the in-memory probe
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — the probe kernel error discards before
    preserving the exact error
  - topology: resolves before output materialization and runtime observation
- ADR0551-ID: `join/execute_hash_join_into_global_aggregate/Build/1`
  - lifecycle: streamed-spilled build attempt, mutually exclusive with Build/2
  - start: immediately before the streamed-spilled build helper
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** — helper error and declined build; the latter
    discards before falling through to the materialized routes
  - topology: a selected build resolves before Probe/1 opens
- ADR0551-ID: `join/execute_hash_join_into_global_aggregate/Probe/1`
  - lifecycle: streamed-spilled aggregate fold after Build/1
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured fold results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — cleanup and error propagation occur after timing
- ADR0551-ID: `join/execute_hash_join_into_global_aggregate/Build/2`
  - lifecycle: in-memory fallback after the streamed-spilled and estimate-spill
    routes decline
  - authoritative phase measurement: **hoisted before** the telemetry closure;
    freeze at `build_micros = duration_micros(build_started)`
  - publication: commit immediately after the Build write inside the closure
  - pre-measurement exits: **2** — build kernel error and absent build; the latter
    discards before preserving the spilled-helper handoff
  - topology: resolves before build profiling and Probe/2 opens
- ADR0551-ID: `join/execute_hash_join_into_global_aggregate/Probe/2`
  - start: immediately before the in-memory selection probe
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — probe kernel error
  - topology: resolves before aggregate filtering/folding and runtime observation
- ADR0551-ID: `join/try_execute_streamed_spilled_left_semi_join/Build/1`
  - start: after the spilled writer is created and immediately before build batches
    are written
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering every batch
    write and the final spill finish; it discards before preserving the exact error
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `join/try_execute_streamed_spilled_left_semi_join/Probe/1`
  - start: immediately after Build/1 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured probe results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — runtime observation, cleanup and propagation all
    occur after publication
- ADR0551-ID: `join/try_execute_partitioned_spilled_left_outer_join/Build/1`
  - lifecycle: one transition per partition iteration
  - start: after that partition's spill writer is created and immediately before
    preserved build batches are selected/written
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering partition
    selection, every batch write and writer finish
  - topology: resolves before the same partition's Probe/1 opens
- ADR0551-ID: `join/try_execute_partitioned_spilled_left_outer_join/Probe/1`
  - lifecycle: one transition per partition iteration after Build/1
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — the partitioned probe/visitor error discards
    before preserving the exact error
  - topology: resolves before that partition's spill cleanup and the next iteration
- ADR0551-ID: `join/try_execute_streamed_spilled_inner_join/Build/1`
  - start: immediately before the streamed-spilled build helper
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **2** — helper error and declined route; both discard
    before preserving the exact outcome
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `join/try_execute_streamed_spilled_inner_join/Probe/1`
  - start: immediately after Build/1 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured probe results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — runtime observation, cleanup and propagation all
    occur after publication
- ADR0551-ID: `join/execute_spilled_hash_join/Build/1`
  - start: immediately before the primitive in-memory hash-map build
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering null-safe/key
    encoding and row-index conversion for every build row
  - topology: resolves before the empty-build return or Spill/1 opens
- ADR0551-ID: `join/execute_spilled_hash_join/Spill/1`
  - start: after the retained-memory guard and immediately before spill writing
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — spill-writer failure
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `join/execute_spilled_hash_join/Probe/1`
  - start: immediately after Spill/1 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured probe results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — cleanup and propagation occur after publication
- ADR0551-ID: `join/execute_spilled_hash_join_into_global_aggregate/Build/1`
  - start: immediately before the primitive in-memory hash-map build
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering null-safe/key
    encoding and row-index conversion for every build row
  - topology: resolves before the empty-build aggregate return or Spill/1 opens
- ADR0551-ID: `join/execute_spilled_hash_join_into_global_aggregate/Spill/1`
  - start: after the retained-memory guard and immediately before spill writing
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — spill-writer failure
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `join/execute_spilled_hash_join_into_global_aggregate/Probe/1`
  - start: immediately after Spill/1 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured probe/fold results,
    matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — cleanup and propagation occur after publication
- ADR0551-ID: `join/visit_spilled_hash_join_filtered_chunks/Build/1`
  - start: immediately before the primitive in-memory hash-map build
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1 captured-result boundary** covering null-safe/key
    encoding and row-index conversion for every build row
  - topology: resolves before the empty-build visitor return or Spill/1 opens
- ADR0551-ID: `join/visit_spilled_hash_join_filtered_chunks/Spill/1`
  - start: after the retained-memory guard and immediately before spill writing
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: that closure; commit after it returns
  - pre-measurement exits: **1** — spill-writer failure
  - topology: resolves before Probe/1 opens
- ADR0551-ID: `join/visit_spilled_hash_join_filtered_chunks/Probe/1`
  - start: immediately after Spill/1 publishes
  - authoritative phase measurement: **inside** the telemetry closure
  - publication: unconditional for both `Ok` and `Err` captured probe/visitor
    results, matching the legacy writer; commit after the closure returns
  - pre-measurement exits: none — cleanup and propagation occur after publication

## Completed envelope set and authority flip

The unified `JoinTiming` handle carries both the telemetry sink and scan meter
through all eight `join.rs` free-function signatures; only its
`open_operator_transition(kind)` facade constructs their scopes. The final
verified sets are:

| scope | verified | outstanding | total |
|---|---:|---:|---:|
| `streaming.rs` | 42 | 0 | **42** |
| `join.rs` | 21 | 0 | **21** |
| whole arc | 63 | 0 | **63** |

Every lifecycle keeps its stable `ADR0551-SITE` marker after the legacy writer is
removed. The architecture guard asserts exact set equality against the 63
`ADR0551-ID` entries above, rejects duplicates independently, and also asserts
that the only remaining assignments to `scan_micros`, `build_micros`,
`probe_micros` and `spill_micros` are the four executor-root ledger
publications. Counts remain a consequence of those sets, never a substitute.

Publication moved atomically at the public `Executor::execute_bound` boundary.
Recursive plan nodes call a private node executor and cannot seal. The root:

1. reconciles acquisition and phase ledgers through `ScanMeter::seal_receipt`;
2. publishes base scan and exclusive build/probe/spill exactly once;
3. checked-adds materialized/live pull time to shuffle and live decode to
   transport; and
4. records `TaskPhaseReceipt::Sealed` or the typed
   `TaskPhaseReceipt::Withheld(LedgerFaults)` veto.

The query result is not failed by an instrumentation veto, but the timing atlas
refuses both local and distributed component construction unless every consumed
receipt is `Sealed`. A withheld receipt therefore cannot masquerade as observed
zero. The fused live diagnostic remains only on the acquisition ledger and is
never copied into `TaskTelemetryPhases`.

The bare-node scan writer and all 63 legacy join phase assignments are gone.
Their timer starts that served no independent diagnostic were removed too; timer
reads retained by throughput/profile diagnostics remain diagnostic only.

## Census reconciliation: 63 is not a new envelope

The earlier 62-site denominator was the quarantined census defect described
above, not the source population. Exact matching on `origin/main` finds **42**
streaming phase assignments and **21** join assignments, for **63** total. The
omitted assignment was the hoisted `probe_micros += membership_micros` writer in
`try_streaming_projected_left_semi_grouped_aggregate`.

The apparent per-function discrepancy around the semi-grouped aggregate is a
relocation during the refactor, not two newly authoritative scopes:

| function / phase | `origin/main` | implementation | correspondence |
|---|---:|---:|---|
| `try_streaming_projected_left_semi_grouped_aggregate / Build` | 2 | 1 | chain build remains in the projected path; fallback build moved to `try_streaming_scan_left_semi_grouped_aggregate` |
| `try_streaming_projected_left_semi_grouped_aggregate / Probe` | 2 | 1 | membership probe remains in the projected path; fallback probe moved to the scan-left path |
| `try_streaming_scan_left_semi_grouped_aggregate / Build` | 0 | 1 | relocated origin fallback build writer |
| `try_streaming_scan_left_semi_grouped_aggregate / Probe` | 0 | 1 | relocated origin fallback probe writer |

The affected population is therefore 4 writers before and 4 after. The four
phase assignments in `exec/phase_receipt.rs` are the new root ledger publisher,
not envelope writers; the architecture guard requires exactly those four after
the authority flip.

## Discriminating receipt result

Before the authority flip, the engine-facing control failed deterministically on
the same scripted read and injected clock:

```text
bare route scan_micros  = 40
fused route scan_micros = 0
```

After the flip it passes at **40 / 40**. Both arms also assert a sealed typed
receipt. A separate negative control injects a clock-regression fault, observes a
successful query, and requires a typed withheld receipt carrying
`clock_regressions = 1`; no numeric phase receipt is published.

## Verification

The exact current tree passes the bounded compile and focused gates below. The
full workspace test receipt is intentionally not claimed here: `nextest` runs
distributed test binaries concurrently, while the project's canonical
`cargo-cgroup` path serializes the timing-sensitive lane. Two ADR 546 distributed
tests pass in isolation on both `origin/main` and this tree; the full nextest run
was stopped after both fixture sets parked in futex waits, so it is runner
evidence rather than a code verdict.

- `cargo check --workspace --all-targets`: exit 0; all targets compile (bench
  targets are checked, not executed).
- architecture suite: **2,129 passed, 0 failed**;
- phase-ledger controls: **38 passed, 0 failed**;
- scan-metering controls: **10 passed, 0 failed**;
- scan-open authority controls: **2 passed, 0 failed**;
- isolated ADR 546 distributed controls on both revisions: **2 / 2 passed**.

The focused authority-flip controls are green:

- bare-versus-fused receipt equality: **40 / 40**;
- withheld-fault publication control: green;
- phase-ledger controls: green;
- scan-metering controls: green;
- exact manifest/source set and zero-legacy-writer guards: green.
