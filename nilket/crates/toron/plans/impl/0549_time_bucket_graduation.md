# ADR 0549 implementation receipt — 2026-08-12

`TIME_BUCKET` binds and executes under the default `PlannerConfig`. The
experimental gate is removed, and the contract it now carries is the repaired
one, not the one that was behind the flag.

## What graduation actually cost

The gate was never the only blocker. Grounding before touching it, and then
reviewing the resulting repair, found **four** defects — all measured, none
covered by the four tests that existed:

| Defect | Effect | Fixed in |
| --- | --- | --- |
| Truncation instead of flooring | Every pre-epoch timestamp one bucket late; `-1` shared the epoch bucket with `+1` | `d3342755` |
| Rune coerced unknown intervals to a zero `Duration` | Divide-by-zero abort, reachable from the primary frontend | `d3342755` |
| Unchecked bucket product | Panic in debug, silent wrap to a positive timestamp in release, near `i64::MIN` | `c4022c4a` |
| Physical-type-only check | A named logical `Int64` — a customer id — bound as a timestamp | `c4022c4a` |

The last two were found reviewing the fix for the first two. That is the
argument for grounding before graduating rather than after: removing the gate
first would have promoted a reachable panic, a wrong answer for every timestamp
before 1970, and a silent wrap at the representable edge into a stable contract.

## The three gate examples

`TIME_BUCKET` was the worked example in every place that demonstrated the
experimental gate. All three are retargeted to `LinearRegress`, the gate's sole
remaining member:

- `tests/architecture/sql_planner_surface.rs` — the default planner refuses;
- `src/binder/tests.rs` — the refusal names the kernel;
- `src/error.rs` — `BindingError::ExperimentalKernel` is constructible.

The third mattered most and was the easiest to miss. It is a *constructibility*
test: `BindingError::ExperimentalKernel { kernel: "TimeBucket" }` compiles and
renders whether or not `TimeBucket` is gated, so it would have passed
indefinitely while documenting a refusal that no longer happens. The other two
fail loudly on graduation.

Retargeting alone would have left the graduation itself uncovered — the suite
would prove `LinearRegress` is refused and say nothing about whether the
graduated kernel works ungated. `binder_binds_time_bucket_without_the_experimental_flag`
covers that directly.

## Pushdown

The decline reason moved from `ExperimentalKernelInPredicate` to
`ExpressionShapeUnsupported("TIME_BUCKET is not PBC-eligible")`, matching `FLOOR`
beside it and the existing `AtTimeZone` / `Extract` / `DateTrunc` declines.
Behaviour is unchanged; `TIME_BUCKET` is still not PBC-eligible.
`ExperimentalKernelInPredicate` had no other consumer and is removed rather than
left as an unreachable arm.

The reclassification was deliberately **not** made ahead of graduation. While the
gate stood, the old reason was accurate; changing it early would have made the
decline less informative and implemented the record before it was approved.

## Contract now in force

`Int64` Unix milliseconds only, naked — named logical types refuse at bind.
Nulls pass through. Intervals are exactly `1m`, `1h`, `1d`, resolved through one
parser and refused at lowering by both frontends in their own idiom, with no
process panic. Buckets floor toward negative infinity; zero, oversized, and
unrepresentable results refuse with typed errors at the evaluator, which is
where a directly constructed `Expr::TimeBucket` is caught.

**Alignment is UTC epoch, not calendar.** `1d` is 86,400,000 ms from the epoch,
not a local day, and observes no DST, leap seconds, or session timezone. Correct
for a metrics histogram over a UTC millisecond column; silently wrong for a
daily aggregate in another zone. A calendar-aware bucket is a different operator.

## Acceptance coverage

Review of the first draft of this receipt found four acceptance items claimed on
controls that did not reach the stated path. Each now has one:

| Item | Was | Now |
| --- | --- | --- |
| 1 — ungated SQL and Rune execution | bound raw IR only, skipping both frontends | `time_bucket_executes_ungated_from_sql_and_rune` runs a SQL statement and a Rune script end to end under the default config |
| 2 — `Timestamp` and `Float64` refuse | named in the record, controlled nowhere | `time_bucket_refuses_timestamp_and_float_inputs` asserts a typed mismatch for both |
| 4 — `i64::MIN` at every allowed interval | only `1m` | all of `1m`, `1h`, `1d` |
| 8 — SQL/Rune interval parity | the "parity" control never invoked Rune | `rune_refuses_unsupported_time_bucket_intervals_at_lowering` drives Rune directly for all three accepted and six rejected literals |

The first and last were the same mistake in different places: asserting a claim
about a frontend without going through it. Binding raw IR proves nothing about
lowering, and a parity control that only calls SQL proves nothing about parity.

## Verification

| Gate | Result |
| --- | --- |
| `test --workspace --no-fail-fast` | `CARGO_EXIT=0`, **178/178 targets reporting**, 7,421 passed, 0 failed, 19 ignored |
| `test -p toron --test architecture_test` | 2,123 passed, 0 failed |
| `fmt --check` | clean |
| `check --workspace --all-targets` | clean |

Target-count validation is part of the receipt because summing reported failures
cannot see an aborted target: a crash prints no `test result:` line and reads as
`failed=0`.

One guard fired during this cut and was right to. Adding a duplicate positive
assertion to `tests/architecture/sql_planner_surface.rs` pushed it to 2,516
lines, past ADR 077's 2,500 ceiling. The duplicate was dropped rather than the
ceiling raised — the ungated bind is covered in-crate — leaving the module at
2,499.

## Not done

`Timestamp` input acceptance, calendar-aware bucketing, additional interval
literals, and PBC eligibility are each separate records. None is blocked by this
one.
