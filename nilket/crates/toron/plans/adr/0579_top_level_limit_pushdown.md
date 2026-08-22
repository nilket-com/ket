# ADR 0579 - Top-level LIMIT pushdown through row-preserving projection

**Status:** Proposed 2026-08-22. Supplies the source bound that Wendelon ADR 0088
left missing. It changes which plans carry a scan row limit; it does not change
response framing, `LIMIT` semantics, or storage layout.

## Context

Wendelon ADR 0088 replaced one-response-frame-per-location with a 4,096-row
response quantum. That decision stands. What it removed, without replacement, was
an accidental property: when every frame held one row, a consumer that stopped
reading stopped the producer within one row of where it stopped caring.

A daemon-path A/B on the reference machine measured the cost of that loss. Same
data directory, same fixture, same client, fresh daemon per arm, binaries
checksummed distinct, one warm-up discarded and two measured passes. The only
variable is Wendelon's response target:

| rows | target 1 (pre-0088 framing) | target 4,096 (current) | ratio |
|---|---|---|---|
| 2,000 | 4.34 / 4.36 s | 8.84 / 8.78 s | 2.03x |
| 5,000 | 10.67 / 11.19 s | 17.76 / 17.66 s | 1.62x |
| 13,000 | 27.12 / 27.10 s | 26.96 / 26.83 s | 0.99x |

The shape is rounding, not latency. Producer work is
`ceil(limit / 4096) * 4096`, capped at the source: predicted 2.048x / 1.638x /
1.000x against measured 2.03x / 1.62x / 0.99x. A `LIMIT 2000` reads 4,096
locations. At 13,000 the source is exhausted before the next boundary, which is
why the effect looked like it only touched small queries.

ADR 0088 named this consequence twice — "bounds producer work by the response
quantum, not by one row", and a risk accepting a wait of up to one quantum. It
priced it as framing. Measured, it is up to a full quantum of storage reads on
every bounded query, which at roughly two milliseconds per location is most of a
statement-timeout budget spent on rows nobody asked for.

The bound the consumer used to imply is available and already plumbed. The scan
request carries `row_limit`; it is already translated into the Wendelon
table-query options; Wendelon's per-location loop already honours it and already
stops. A pass already exists — `apply_safe_row_limit_to_scan`, reached from the
`Take` arm of `optimize_for_required` — and it already admits a bare scan, a
fully-pushed filter, and a nested take.

It admits one shape too few. Bound through the real entry point, the four query
shapes resolve as:

| query | plan | scan `row_limit` |
|---|---|---|
| `SELECT * FROM sales LIMIT 2000` | `Take{Scan}` | `Some(2000)` |
| `SELECT id FROM sales LIMIT 2000` | `Take{Project{Scan}}` | `None` |
| `SELECT id, amount, region, ts FROM sales LIMIT 2000` | `Take{Project{Scan}}` | `None` |
| `SELECT id AS k FROM sales LIMIT 2000` | `Take{Project{Scan}}` | `None` |

Naming any column introduces a `Project`, the traversal stops, and the limit is
lost. `SELECT *` works today; the moment a user writes a column list it does not.
The measured exhibit is `SELECT x FROM ctrl_plain LIMIT n`, which is the second
row of that table.

This is therefore not a new mechanism. It is one missing arm in an existing
traversal, and the gap is a projection node that cannot change cardinality.

## Decision

### 1. The traversal admits row-preserving projection and alias layers

`apply_safe_row_limit_to_scan` descends through a `Project` between the `Take`
and the scan. A projection evaluates expressions per input row and emits exactly
one output row per input row: it is a rename-and-compute layer, and a row bound
above it is the same row bound below it. Cardinality-changing work lives in other
nodes — `Aggregate`, `Sort`, `Window`, `Join`, `Exchange` — and those remain
untraversed.

Admission is positive, not residual. The traversal descends into the shapes this
record names and returns false for everything else, so a node added later is
excluded until someone extends this list deliberately. A new cardinality-changing
node must not silently inherit pushdown by failing to be excluded.

### 2. The bound pushed is the bound proved

The pushed limit is the top-level `LIMIT` and nothing else. Where a nested take
already carries a limit, the smaller wins, which is existing behaviour. The
`RowIdentityProjection::None` guard is retained unchanged: a scan that appends
row identity is not admitted.

A limit of zero is not pushed by the arm this record adds. `LIMIT 0` has an
existing representation and an existing server-side handling of `Some(0)`, and
folding a degenerate bound into the same path as a real one would make one
control cover two behaviours. The `SELECT *` path's present treatment of
`LIMIT 0` is unchanged and out of scope.

### 3. Non-pushdown cases stay refused, by name, until each has its own proof

These are excluded deliberately, not incidentally, and each needs its own record
and its own control before admission:

- **`ORDER BY`** — the limit belongs to the sorted order. Pushing it to the scan
  would take an arbitrary prefix of the source and sort that. `Sort` carries its
  own `limit` and `offset` and is a separate optimization.
- **`OFFSET`** — lives only on `Sort`; `LIMIT/OFFSET` without `ORDER BY` is
  refused at the frontend, so `Take` is always a pure limit. Nothing to push, and
  no live defect here.
- **Aggregates, `DISTINCT`, joins, `Window`, `Exchange`** — output cardinality is
  not input cardinality.
- **Filters not fully pushed** — a predicate Toron still evaluates means the scan
  must supply more rows than the limit. Only `FilterDisposition::FullyPushed` is
  admitted, which is existing behaviour.

### 4. Evidence is source progress, not wall clock

The controls assert what the producer did, measured at the Wendelon boundary
against `clocks.progress_marks`, over a source larger than one response quantum.
Equality is the assertion. A strict inequality against a full scan is what ADR
0088's `LIMIT 3` control used, over a fixture smaller than one quantum, and it is
exactly why this regression shipped: it cannot distinguish stopping at three from
stopping at four thousand and ninety-six.

## Acceptance

- Binding `SELECT id FROM sales LIMIT 2000` through the public bind entry yields a
  scan carrying `row_limit = Some(2000)`. The control binds the real entry point,
  not the inner recursion — the inner one does not run this pass at all, so a
  control written against it would pass while proving nothing.
- The same holds through an alias (`SELECT id AS k ...`) and an explicit full
  column list, and continues to hold for `SELECT *`.
- Against a Wendelon source larger than one response quantum, a row limit `n`
  under the quantum bounds producer work to exactly `n` rows' worth. A progress
  mark is a location processed *or* a batch produced, not one per location, so
  the control derives the per-row constant from the full scan and asserts
  `progress_marks == marks_per_row * n` — an exact equality on producer work
  rather than the strict inequality that let this regression through. Its failure
  message names what quantum rounding would have reported.
- The daemon-path exhibit recovers: with the fixture and query shape that measured
  8.8 s at 2,000 rows and 17.7 s at 5,000, cost tracks the limit rather than the
  quantum. Reported as a measurement with its binary identified, not as a CI gate.
- `ORDER BY ... LIMIT`, an aggregate under a limit, a `DISTINCT` under a limit, a
  join under a limit, and a limit over a filter that is not fully pushed each
  leave `row_limit` unset. Each asserts the scan is unbounded, so admitting any of
  them later requires deleting a control rather than silently widening a match.
- A live projected `LIMIT n` returns exactly `n` rows, with the projected schema,
  and every row is a distinct genuine row of the source. `LIMIT` without
  `ORDER BY` promises no particular rows, so this asserts membership and
  distinctness rather than a fixed prefix — a control demanding the first `n` in
  order would be asserting a guarantee the query does not make. A pushed bound
  changes what the producer reads, never what the query returns.

## Guardrails

The traversal is a closed, positive list: scan, fully-pushed filter, nested take,
row-preserving projection. Everything else returns false. Adding a node type to
this list requires a record naming why that node cannot change cardinality.

A row limit on a scan is already a probe-linearity fence. This record widens how
often that fence appears, and does not change its meaning.

Bounded-read evidence is source progress at the Wendelon boundary. Frame counts,
batch counts, and wall clock are not proxies for it — 0088 established that frame
count and producer work are independent, and that is the property this record
depends on.

## Risks and deferrals

A pushed bound narrows the set of rows a projection expression is evaluated over.
An expression that would fault on a row past the limit no longer sees that row.
SQL does not promise evaluation beyond a limit, and a streaming consumer that
stops early already produced this, so this is a narrowing of an existing
nondeterminism rather than a new behaviour — but it is a real change and belongs
on the record.

This record does not touch ADR 0088's framing decision, the response quantum, or
storage layout. It does not make `LIMIT` push through sorts or aggregates, and it
does not address the remaining per-row read cost, which is separately measured as
mostly living outside per-location materialization and PBC.

The absolute throughput figures around this path — the roughly 480 rows per
second, the derived statement-timeout ceiling near 29,000 rows, and everything
projected from them — remain **provisional**. The client harness that produced
them is a debug build, because the Polariton test target does not currently
compile in release. The A/B above is unaffected, since both arms share that
client, but no product-level throughput claim should rest on those numbers until
the release client is repaired and the measurement rerun.

## Forward

After this lands, re-measure the daemon path at 2,000 and 5,000 rows. If cost
tracks the limit, the quantum interaction is closed and the remaining question is
the per-row cost itself. Repair the release Polariton build before treating any
absolute rate as a product constraint. `ORDER BY ... LIMIT` pushdown into a sorted
source is the natural successor and needs its own evidence that a top-k source
bound is correct under the sort, not merely faster.
