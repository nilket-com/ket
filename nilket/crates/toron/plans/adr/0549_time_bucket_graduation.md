# ADR 549 - `TIME_BUCKET` graduation

**Status:** Accepted and implemented 2026-08-12.

## Context

Polariton can build numeric histograms today: ADR 545 shipped `FLOOR` as a
stable scalar expression and alias-first bucketing as ordinary composition.
Time histograms cannot be built the same way, because `TIME_BUCKET` refuses at
bind time unless `allow_experimental_kernels` is set, and that flag is not
something a consumer can reasonably require.

ADR 545 declined to add a numeric analogue of `TIME_BUCKET`, on the grounds that
temporal bucketing carries duration and timestamp semantics that plain numeric
arithmetic does not. That reasoning stands. The consequence is that the temporal
path stayed where it was, gated, while the numeric path graduated — so this
record finishes the pair rather than widening it.

**The gate was not the only blocker.** A grounding pass before touching it, and
review of the resulting repair, found four defects — all measured, all fixed by
`d3342755` and `c4022c4a`:

- The evaluator truncated toward zero rather than flooring, so every pre-epoch
  timestamp landed one bucket late. With a one-second bucket, `-1500` returned
  `-1000` instead of `-2000`, and `-1` shared the epoch bucket with `+1`. This
  is the same defect ADR 545 fixed for numeric `FLOOR`.
- Rune coerced an unknown interval to a zero `Duration`, which reached the
  evaluator and divided by zero, aborting the process. SQL rejected the same
  input with a typed error. The two frontends held separate copies of the
  allowlist and had already drifted.
- The bucket product was unchecked, so timestamps near `i64::MIN` panicked in
  debug and wrapped in release.
- Only the physical type was checked, so a named logical `Int64` — a customer
  id — bound as a timestamp and produced a plausible bucket from it.

The last two were found reviewing the fix for the first two, which is the
argument for grounding before graduating rather than after. Removing the gate
first would have promoted a panic reachable from the primary frontend, a wrong
answer for every timestamp before 1970, and a silent wrap at the representable
edge into a stable contract. This record graduates the repaired behaviour and
states it.

## Decision

### 1. The input type is `Int64` milliseconds, and nothing else

`TIME_BUCKET(timestamp, interval)` accepts a **naked** `Int64` expression
carrying Unix milliseconds. Every other input type refuses at bind time with a
typed mismatch. The record does not add `Timestamp` acceptance, timezone-aware
inputs, or implicit conversion; those are a separate decision with their own
semantics.

**Named logical types over `Int64` refuse**, matching `FLOOR`. Checking only the
physical type accepted a value whose declared units are not milliseconds — a
customer id bucketed as a timestamp — and produced a plausible number from it.

### 2. Nulls pass through

A null timestamp yields null. It is not bucketed to the epoch, and null is not
treated as zero.

### 3. The interval allowlist is closed, shared, and typed

The accepted interval literals are exactly `1m`, `1h`, `1d`. Both frontends
resolve them through one parser, `ir::time_bucket_interval_from_literal`, and
both refuse anything else at lowering in their own idiom, with no process panic.

The refusal is **frontend-native lowering refusal with no process panic**, not a
single shared error type: SQL returns a `FrontendError`, Rune returns a VM panic
result. What is shared is the allowlist and the guarantee that neither frontend
defers an invalid interval to the evaluator.

One parser is part of the contract, not an implementation detail. Two copies
already drifted once, into an error on one frontend and a process abort on the
other; a shared allowlist is what makes frontend parity structural rather than
maintained by attention.

### 4. Buckets floor toward negative infinity, and nonpositive intervals refuse

The bucket start is `timestamp.div_euclid(interval_ms) * interval_ms`. Flooring,
not truncation, so a pre-epoch timestamp lands in the bucket that contains it.

An interval that is zero, or too large for `i64` milliseconds, refuses with a
typed evaluation error. That check lives at the evaluator rather than only at
lowering, because an `Expr::TimeBucket` can be constructed directly and bypass
the frontend entirely — which is exactly how the division by zero was
reachable.

**The product is checked, and an unrepresentable bucket start refuses.** Near
`i64::MIN` the mathematical bucket start falls below the representable range for
every allowed interval: a `1m` bucket of `i64::MIN` is
-9_223_372_036_854_780_000. Unchecked, that panics in debug and wraps in
release, turning a pre-epoch timestamp into a positive one — the worse of the
two failures, because it is silent.

**One evaluator serves both paths.** The unbound `Expr` evaluator carried its
own copy of this arithmetic and kept the truncating, unchecked, divide-by-zero
form after the bound path was repaired. It is `#[cfg(test)]`, so it never ran in
production, but a test-only evaluator that disagrees with the shipped one lets a
test pass against semantics the engine does not have. Both route through one
helper.

### 5. Alignment is UTC epoch, not calendar

**Buckets are fixed-width offsets from the Unix epoch in UTC.** `1d` means every
86,400,000 milliseconds from the epoch. It does **not** mean a local calendar
day, and it does not observe daylight-saving transitions, leap seconds, or a
session timezone.

For a metrics histogram over a UTC millisecond column this is what a caller
wants. For a daily aggregate in a non-UTC zone it is silently wrong — the day
boundaries will not match the viewer's midnight. Stating it here makes it the
committed contract rather than an inherited accident, and it is the thing a
calendar-aware successor would have to change.

`AT TIME ZONE` remains the separate, already-stable surface for zone
conversion; this record does not compose the two.

### 6. Storage pushdown is declined by shape, not by experiment

`TIME_BUCKET` in a predicate declines storage pushdown. Today the decline reason
is `ExperimentalKernelInPredicate`, which becomes false on graduation. It
becomes `ExpressionShapeUnsupported`, matching `FLOOR` two lines above it and
the existing `AtTimeZone` / `Extract` / `DateTrunc` declines.

Making `TIME_BUCKET` PBC-eligible is explicitly **not** in scope. The behaviour
is unchanged; only the reason a caller sees is corrected.

`TIME_BUCKET` is `ExperimentalKernelInPredicate`'s only consumer, so the variant
is **removed** with this reclassification rather than left as an unreachable
arm, and the PBC fixture is updated with it. The reclassification is deliberately
not made ahead of graduation: while the gate stands, the existing reason is
accurate.

### 7. The experimental flag remains, with one member

`allow_experimental_kernels` is not removed. After this record `LinearRegress`
is its sole remaining member.

The flag is hashed into the **CTAS intent fingerprint** (`binder.rs`), not a
general planner fingerprint.

**Three** places currently use `TimeBucket` as the gate's worked example, and
all three are retargeted to `LinearRegress`:

- `tests/architecture/sql_planner_surface.rs` — the default planner refuses;
- `src/binder/tests.rs` — the refusal names the kernel;
- `src/error.rs` — `BindingError::ExperimentalKernel` is constructible and
  renders its kernel name.

Otherwise graduation deletes the only coverage that the gate refuses anything at
all, and leaves an error-construction example naming a kernel that is no longer
gated.

## Acceptance

1. **Ungated SQL and Rune.** `TIME_BUCKET` binds and executes under the default
   `PlannerConfig`, from both frontends, with no experimental flag.
2. **Type refusal.** Every non-`Int64` input refuses at bind time with a typed
   mismatch, including `Timestamp` and `Float64`, so the exclusion is visible
   rather than assumed.
3. **Floor across the epoch.** `-1`, `-1000`, `-1001`, `-1500` floor to `-1000`,
   `-1000`, `-2000`, `-2000`; positive boundaries and interiors are unchanged.
   These fail under truncation.
4. **Extreme values refuse.** `i64::MIN` at every allowed interval returns a
   typed unrepresentable-bucket error, with no panic and no wrap.
5. **Named logical types refuse at bind**, with a typed mismatch.
6. **Both evaluators agree.** The unbound path floors, preserves nulls, and
   refuses a zero interval identically to the bound path.
7. **Null passthrough.**
8. **Interval parity.** The same rejected literals are refused at lowering by
   both SQL and Rune, each in its frontend-native form and neither panicking the
   process, and the three accepted literals succeed on both.
9. **Nonpositive interval refuses at evaluation**, including a directly
   constructed `Expr::TimeBucket`, with no panic.
10. **Pushdown decline is reclassified**, the orphaned variant removed, and a
    control asserts the reason is no longer `ExperimentalKernelInPredicate`
    while the declined-and-matches-baseline behaviour is unchanged.

11. **`LinearRegress` still refuses** without the flag, proving graduation did not
   open the gate for everything behind it.

## Guardrails

- No timezone or calendar semantics are added. A calendar-day bucket is a
  different operator with a different contract.
- No `Timestamp` input acceptance, and no implicit conversion from it.
- No new interval literals; widening the allowlist is a separate decision that
  must state its own overflow and boundary behaviour.
- `TIME_BUCKET` does not become PBC-eligible.

## Risks

- **UTC alignment will surprise someone.** A consumer computing daily buckets in
  a non-UTC zone gets boundaries offset from local midnight, with no error. The
  contract says so explicitly, which is the only mitigation this record offers;
  a calendar-aware operator is the real one.
- **The allowlist is narrow.** Three literals cover the histogram cases Polariton
  has today and nothing else. Widening it is cheap but must not be done
  implicitly at a call site.

## Forward

Closes the asymmetry ADR 545 left: numeric and temporal bucketing are both
stable, spelled differently for reasons stated in 545. A calendar-aware bucket,
`Timestamp` input, and PBC eligibility are each separate records, and none is
blocked by this one.
