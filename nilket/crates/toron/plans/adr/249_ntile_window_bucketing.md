# ADR 249 — NTILE Window Bucketing

## Status

Proposed.

## Context

After the substrate (241), aggregate (242), frames (243/244/245), value functions (246),
stacked exchanges (247), and multiple specs (248), `NTILE(n)` is the one standard analytic
window function still fenced. It currently has no `WindowFunction` variant and falls through
`extract_window_call` to the generic unsupported-function error (pinned in
`deferred_window_forms_stay_rejected`).

`NTILE(n)` divides each ordered partition into `n` near-equal buckets and emits the 1-based
bucket index per row: the first `len % n` buckets hold one extra row. It is a row-preserving,
partition-local, frame-independent computation over partition geometry — the same shape as the
ranking functions, with an integer bucket-count argument.

### Grounding — NTILE rides the ranking machinery

`WindowFunction { RowNumber, Rank, DenseRank, Aggregate(_), Value(_) }` — NTILE is a new
variant. `execute_window` already materializes + sorts each partition and builds
`partition_start[]` / `partition_end[]` (`exec.rs:3734–3748`); the ranking arms
(`exec.rs:3804–3812`) push a precomputed Int64 column per call. NTILE is one more such arm:
for a row at 0-indexed position `i = row - partition_start[row]` in a partition of
`len = partition_end[row] - partition_start[row] + 1` rows with `k` buckets:

```
base = len / k;  rem = len % k          // first `rem` buckets hold base+1 rows, the rest hold base
bucket = if i < rem * (base + 1) { i / (base + 1) + 1 }
         else { rem + (i - rem * (base + 1)) / base + 1 }
```

When `len < k`, `base = 0` and `rem = len`, so every row falls in the first branch (`bucket = i + 1`,
one row per bucket) — the `else` branch never divides by zero.

## Decision

Add `WindowFunction::Ntile { buckets: u64 }`.

- **Lowering**: `NTILE(n)` with `n` a **positive integer literal** (reject missing / non-literal /
  non-integer / zero / negative argument, and `IGNORE NULLS` / `FILTER`). NTILE is frame-
  independent: an explicit frame is rejected, exactly as for the ranking functions.
- **Binder**: output is a non-null `Int64` column, reusing the ranking field shape
  (`Field::new(&call.output_name, DataType::Int64, false)`); no bound argument (the bucket count
  rides in the variant, so `WindowFunctionCall.args` is empty, like ranking).
- **Executor**: one `WindowFunction::Ntile { buckets }` arm computing the per-row bucket from
  partition geometry as above.

### Decision — NTILE does **not** require ORDER BY

Ranking functions do not require ORDER BY in Toron — only LAG/LEAD do (`sql_planner.rs:3972`).
NTILE's closest analog is ROW_NUMBER (a partition-geometry enumeration), which is permitted
without an ORDER BY; requiring it for NTILE alone would be an inconsistent special-case, and
standard SQL permits NTILE without window ordering. Crucially, the result is **deterministic
either way**: `execute_window` totalizes the partition sort with every column as an ascending
tiebreaker *after* the partition and order keys (`exec.rs:3687`, "so ROW_NUMBER among peers is
deterministic"), so with no ORDER BY the buckets follow that totalized order, not an arbitrary one
— identical to ROW_NUMBER. (Confirmed with the operator: keep this. Requiring ORDER BY would only
make sense as a blanket "every positional window function must name its order" policy, which is
not Toron's current shape — so we do not introduce that fence here.)

## Consequences

- The standard analytic window function set is complete (ranking, aggregate, all three frame
  units, value lookups, NTILE).
- No new node, no fragmenter change: a single OVER spec rides 241; multiple specs (one being
  NTILE) ride 248's stacked exchanges.

## Discriminators (each fails on `main` — NTILE rejects today)

- **`ntile_buckets_rows_uniformly`** — `NTILE(2)` over a 4-row partition → `[1, 1, 2, 2]`.
- **`ntile_distributes_remainder_to_first_buckets`** — `NTILE(3)` over 4 → `[1, 1, 2, 3]`; locks
  the remainder rule (would fail under naive `i / (len / k)` bucketing, which gives `[1,1,3,4]`).
- **`ntile_restarts_per_partition`** — buckets reset at each partition boundary (grp a vs grp b).
- **`ntile_more_buckets_than_rows`** — `NTILE(10)` over a 1-row partition → `[1]` (each row its
  own bucket; no division by zero).
- **`ntile_rejects_zero_or_non_literal_bucket_count`** — `NTILE(0)`, `NTILE(score)` reject.
- **`ntile_without_order_by_is_accepted`** — `NTILE(2) OVER (PARTITION BY grp)` (no ORDER BY) plans
  and buckets via the executor's totalized row order; locks the settled no-ORDER-BY decision
  (would fail under a require-ORDER-BY fence). Folded from the council (Gemini iter-2).
- **`ntile_distributed_gate_is_value_discriminating`** — `NTILE(2) OVER (PARTITION BY set_key
  ORDER BY id)` over `set_left`: the NULL partition (ids 2,4) gives id 2 → bucket 1, id 4 →
  bucket 2, which holds only once the exchange co-locates and orders both rows; a single producer
  sees one row and reaches only bucket 1 (236/238 cross-producer cardinal).

## Architecture guards

- `adr249_window_function_has_ntile_variant` — `WindowFunction::Ntile { buckets: u64 }` present;
  NTILE is not a value/aggregate variant.
- `adr249_ntile_is_partition_geometry_not_frame` — the executor arm computes from
  `partition_start`/`partition_end`; NTILE is in the frame-reject set with the ranking functions.
- `adr249_ntile_bucket_count_is_positive_integer_literal` — the lowering parses a positive integer
  literal and rejects zero / non-literal.
- `adr249_ntile_distributed_gate_is_value_discriminating` — the distributed gate uses the 236/238
  cross-producer cardinal discipline.

## Deferred

- **Named windows** (`WINDOW w AS (...)`) — frontend desugar, a different mechanism; ADR 250.
- **IGNORE / RESPECT NULLS** — value-function null-skipping mechanism.
- **EXCLUDE** frame clause — sqlparser 0.62 carries no field; external blocker.

## Forward arc

249 NTILE (this) → 250 named windows (frontend desugar) → window arc complete (IGNORE NULLS /
EXCLUDE deferred on mechanism / external grounds); ready to branch.
