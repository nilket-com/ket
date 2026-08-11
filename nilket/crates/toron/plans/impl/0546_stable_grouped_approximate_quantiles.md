# ADR 0546 implementation receipt — 2026-08-11

`APPROX_QUANTILE(value, q)` now uses one stable bounded implementation for global and grouped
SQL and Rune execution. This receipt claims the feature implemented and code-gated. It does not
claim Tukey whiskers, outlier rows, or any box-plot surface; those remain ADR 0547 work.

## Surface and semantics

- The stable catalog accepts naked Int32, Int64, Float32 and Float64 and returns nullable
  Float64. Decimal128 and named logical types refuse at bind time; non-finite observations refuse
  during execution.
- `q` must be a finite literal in `[0,1]`. Its canonical bits remain part of planner identity, so
  authored quartile outputs do not alias.
- Values use Wendelon's order-preserving signed and floating optimizer-fact encoders. The old
  Int32-only raw-big-endian retained-value implementation is unreachable and returns a loud
  internal refusal if invoked.
- One sketch is shared per `(group, value expression)` and finalized at every requested rank.
  q1, median and q3 therefore come from one population and remain ordered, including after
  compaction on a skewed fixture.

## Confidence and admission

- Every `ExecutionContext` draws one 256-bit seed before reading data. The seed crosses the task
  carrier and reaches ordinary, grouped, and both streaming-join global aggregate paths; no
  aggregate path mints local randomness.
- At most 1,024 distinct quantile populations are admitted per execution. Fragmentation divides
  that count across terminal quantile tasks by quotient and remainder; the grants sum to 1,024
  rather than multiplying per worker.
- Count admission is independent of bytes. State 1,024 fits and state 1,025 refuses before
  construction even if memory remains.
- Memory admission charges 5,601 fixed-width retained slots plus metadata per distinct
  population. Int64/Float64 therefore charge 45,320 bytes per sketch in this implementation;
  Int32/Float32 charge 22,916. The ordinary task-memory gate may refuse before the confidence
  count cap, especially because the 64-bit payload alone consumes about 68% of the default
  64 MiB budget at the full count ceiling.

## Controls

- prior global and grouped SQL refusals now produce exact small-fixture results without the
  experimental flag;
- q validation, nulls, non-finite values, physical types, negative integers, mixed-sign floats,
  Decimal128 and logical-type refusal are covered;
- SQL and Rune agree for both global and grouped queries;
- local, two-worker and four-worker execution with the same seed produce identical schemas and
  values for global and grouped quantiles;
- a shared-state fixture proves three ranks share one allocation and remain ordered on compacted
  skewed input;
- count and byte gates refuse before construction; and
- architecture guards pin the stable catalog, single Wendelon query authority, execution seed
  seam, terminal-task allowance conservation, weighted representation, non-flattening merge, and
  the absence of a v1 constant-seeded fact producer.

## Dependency disposition

ADR 0546 consumes the implemented aggregate half of Wendelon ADR 0083. It does not wait for CSB
v2 because the aggregate path never persists `ColumnStatsKllPayload`; version-1 range facts are
instead suppressed safely while that migration remains open. ADR 0547 may consume these grouped
quartiles, but its reported lineage repair remains conditional on receiving and reproducing the
exact production artifact.

## Verification

- `scripts/cargo-cgroup test --workspace --no-fail-fast --quiet` — exit 0.
- `scripts/cargo-cgroup check --workspace --all-targets` — exit 0, zero warnings.
- `cargo fmt --all -- --check` and `git diff --check` — clean.
