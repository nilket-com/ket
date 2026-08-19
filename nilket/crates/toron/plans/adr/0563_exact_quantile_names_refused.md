# ADR 563 - Exact quantile names are refused, not aliased

**Status:** Proposed 2026-08-19.

## Context

`MEDIAN`, `PERCENTILE_CONT`, and `PERCENTILE_DISC` promise exact quantiles.
This engine computes approximate ones: `APPROX_QUANTILE` is KLL-sketch-backed,
and its name states the trade. Mapping the exact names onto it would be a
semantic lie of the kind this codebase refuses — a result wearing a precision
its computation does not have.

Computing them honestly requires buffering every value per group, and the
resource policy that requires does not exist: ADR 562 shipped `STRING_AGG` with
its unbounded buffer named and gated on state-accounting integration. Admitting
a second ungated buffer — one that grows with group cardinality on numeric
columns, the common case — would compound a risk already on the books before
its gating decision is made.

Today a user writing `MEDIAN(price)` gets the generic fallthrough error,
`unsupported expression: …` with a debug dump. That tells them nothing about
why, or what to write instead.

## Decision

The three names are **refused by name with a directive**: the error says the
name implies an exact quantile this engine does not compute, and points at
`APPROX_QUANTILE` — `(value, 0.5)` for `MEDIAN` — which states the
approximation it makes.

The refusal table lives in the aggregate registry file beside the name
resolution it modifies, and is exact-set guarded: a name leaves the table only
in review, which is precisely how exact quantiles get admitted later — a future
ADR that first decides buffering, spill, and resource accounting, then removes
the name and registers the kernel. The guard failing on removal is the
mechanism that forces that order.

The refusal hooks the one lowering fallthrough every unrecognized call reaches,
so it fires for the plain call, the `WITHIN GROUP (ORDER BY …)` grammar, and
grouped queries alike, and a genuinely unknown name still gets the generic
error — the table must never swallow a typo into a quantile lecture.

## Acceptance

- `MEDIAN(price)` fails with an error naming `APPROX_QUANTILE(value, 0.5)`.
- `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)` and the plain-call form
  both fail with the directive, as does `PERCENTILE_DISC`.
- The refusal fires identically under `GROUP BY`.
- A misspelled function name still gets the generic unsupported-expression
  error, proving the table refuses exactly its three names and nothing else.

## Guardrails

The table is an exact set in the registry file; the architecture guard asserts
its membership, so additions and removals are review decisions.

## Forward

The ADR that admits exact quantiles must decide: the buffering representation,
the spill policy when a group exceeds memory, integration with the quantile
state accounting ADR 546 established, and whether `PERCENTILE_DISC` shares the
buffer with `PERCENTILE_CONT`. It should also close ADR 562's state-accounting
gate in the same motion, since both features stand behind the same missing
policy.
