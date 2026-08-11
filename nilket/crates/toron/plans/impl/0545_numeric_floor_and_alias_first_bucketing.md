# impl 0545 — numeric `FLOOR` and alias-first bucketing

Implementation receipt for `plans/adr/0545_numeric_floor_and_alias_first_bucketing.md`.
Toron now carries `FLOOR` as a stable typed scalar from SQL lowering through the logical and
bound expression programs to local and worker-process execution. Numeric histogram relations
compose through the already-supported projected-alias route; direct computed `GROUP BY` remains
refused.

## Red proof

Three fixtures were banked against the pre-implementation tree. All failed at the intended
frontend boundary with `Unsupported { message: "unsupported expression: Floor ..." }`:

- the Int32 / Int64 / Float32 / Float64 type matrix and null propagation;
- finite versus NaN and infinity evaluation;
- the alias-first sparse histogram relation with a negative bucket.

The fixtures pass after the implementation. The direct sibling
`GROUP BY FLOOR(price)` remains a named negative control.

## Expression path

`Expr::Floor(Box<Expr>)` and `BoundExprNode::Floor { input }` are closed-enum variants. SQL
lowering accepts only `CeilFloorKind::DateTimeField(NoDateTime)`; datetime-field and scale forms
do not enter the numeric path. Every compiler-forced visitor, remapper, structural hash/equality
path, aggregate-signature canonicalizer, bound-subgraph clone, ordinal/column collector, pretty
printer, and join profiler carries the variant explicitly.

Binding lives with arithmetic rather than growing the binder root. It accepts only naked
Int32, Int64, Float32, and Float64 inputs. Decimal128 and named logical types return typed binding
errors. Integer arrays retain their physical type and values. Floating arrays apply mathematical
floor, preserve nulls, and reject NaN and infinities with a typed kernel error naming `FLOOR`.

`FLOOR` remains Toron-executed. The PBC compiler declines it explicitly; no storage or Wendelon
wire surface changed. A two-worker process fixture sends Float64 values ending in `.25` through
the serialized task boundary and receives the exact floored sequence `0..15`, with both workers
selected.

## Relation composition

The live relation control projects
`FLOOR((price - 20.0) / 10.0) * 10.0 AS bin_start`, groups by `bin_start`, and returns the exact
sparse half-open buckets beginning at `[-20, -10, 0, 10]`. The negative row discriminates floor
from truncation toward zero. Projection, filter, alias ordering, and computed aggregate arguments
all exercise the same bound scalar.

The in-memory `FixtureScan` does not interpret storage-pushed predicates, so the fixture uses the
identity `price + 0.0 IS NOT NULL` to keep null filtering on Toron's evaluation rail. This changes
neither the authored relation nor production semantics; the production canonical form remains
`price IS NOT NULL` and storage executes its exact pushed predicate.

## Structural ratchets

Adding the real bound node advanced the bound-expression census from 15 to 16. The binder root
initially crossed its 6,278-line ceiling. Instead of raising the cap, the two existing exhaustive
expression walkers were carved into `binder/expression_walk.rs`; the root fell to 6,146 lines.
The ADR guards likewise moved into a new semantic `architecture/numeric_floor.rs` leaf rather
than pushing `sql_planner_surface.rs` over its 2,500-line ceiling.

The new guards pin the closed type matrix, finite-value check, non-PBC posture, SQL AST field
restriction, absence of adjacent `CEIL`/`ROUND` IR, the alias-first fixture, and the unchanged
simple-column `GROUP BY` refusal.

## Verification

- `cargo test -p toron --lib` — 1,527 passed, 0 failed.
- `cargo test -p toron --test group_by_test` — 229 passed, 0 failed.
- `cargo test -p toron --test architecture_test` — 2,117 passed, 0 failed.
- `cargo test -p toron --test distributed_process_test floor_distributed_execution_matches_the_exact_numeric_result` — passed.
- `cargo check --workspace --all-targets` — exit 0, zero warnings.
- `cargo fmt --all -- --check` and `git diff --check` — clean.

Per the current verification policy, this non-concurrent scalar cut used targeted evidence rather
than a redundant full workspace test run. Its cross-cutting closed-enum risk is covered by
workspace all-target compilation, the complete Toron library and architecture batteries, and the
real process-boundary control.
