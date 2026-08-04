# ADR 405 - Int64 join build/probe primitive table (q03 hash-path mechanism)

## Context

401/402 closed q03's logical-shape gap (SQL == Rune, `666e0ac028036f70`, ~373 ms). 404 - now profile-backed under
`perf_event_paranoid=1` - named the remaining lever: ~25% of q03 self-time is the hash build/probe path
(`hashbrown::rustc_entry` 10.80% + `toron::exec::join::build_in_memory_join_rows` 10.61% + `HashMap::get` 3.19%), and
404 classified it as a **representation/mechanism** problem, not another per-row cleanup inside the build loop.

The current representation (`exec/join/build_rows.rs:21`, `exec/join.rs:1137`):

```
JoinBuildRows = HashMap<JoinKey, BuildRowList, ahash::RandomState>
enum JoinKey { Int32(i32), Int64(i64), Generic(Vec<u8>) }
```

pays three structural taxes on every one of q03's 3.24M build inserts / probes: an 8-byte `i64` stored as a `JoinKey`
enum bloated to ~24-32 B by the `Generic(Vec<u8>)` variant; a general-purpose hashbrown Swiss-table; and per-key value
objects. q03's build is `JoinKeyPlan::Int64`, 3.24M rows, 829,958 unique keys, <=7 rows/key, no nulls - the ideal shape
for a specialized integer table (the packed join tables DuckDB ~40 ms and Polars ~102 ms use).

405 introduces a **specialized Int64 build/probe table**, fenced to `JoinKeyPlan::Int64` and non-null-safe joins. This
is not the `HashMap<i64, BuildRowList>` micro-cleanup 404 ruled out (that keeps the generic Swiss-table); it is a
purpose-built table that drops the enum key and hashbrown machinery.

## Decision

### The cut - fenced, behavior-neutral
- A specialized open-addressing Int64 build/probe table, selected **only** when `JoinKeyPlan::Int64` **and**
  `!null_safe_keys`. `Int32`, `Generic`, and null-safe joins keep today's `HashMap<JoinKey, BuildRowList>` untouched.
- **Preserve the probe/output contract exactly:** `lookup(key)` yields build row indices in **insertion order**; join
  output is emitted in **probe-row order**; the `matched_build_rows` bitmap semantics for outer/anti/semi are
  unchanged. The lowest-risk design **keeps `BuildRowList` as the table value** (so `matches.rows()` insertion-order is
  preserved by construction) and replaces only the key representation + table structure - not the row-list, probe loop,
  or output materialization.

### Stop-line (explicit)
If the primitive table cannot preserve current join semantics - duplicate order, probe-row output order, null routing,
outer/anti build-match bitmap, spill/`PartitionedScan` interop - **without reshaping the probe/output materialization**,
do **not** ship a partial q03-only table. Seal as a **design/diagnostic** and pivot to the radix/partitioned-table
design (the "B" mechanism) as the next ADR. A representation change that only works by special-casing q03's output path
is out of bounds.

## Acceptance
- **Fence honored:** the primitive table is selected only for `Int64` + `!null_safe_keys`; `Int32`/`Generic`/null-safe
  paths are byte-unchanged. Executor-internal only - the bound plan fingerprint stays `666e0ac028036f70` (no plan/shape
  change).
- **Behavior-neutral across join kinds:** identical output for inner/left/right/full/semi/anti; **duplicate-order and
  probe-order determinism locked by tests** (a >=2-duplicate case that would fail under LIFO/reordered chains); nulls
  never match; outer/anti unmatched-build rows preserved.
- **Memory accounting preserved or more conservative:** the primitive table must not loosen today's
  `estimated_build_hash_bytes` / join-build guard behavior. Either keep the current estimate as a conservative guard,
  or replace it with a measured table-specific estimate locked by an under/equal/over-cap test. A faster table that
  silently admits a build today's guard would spill/reject is rejected.
- **q03 sf=1 four-way before/after is primary** (`toron,rune,polars_sql,duckdb`, fresh same-session baseline): the
  profiled hash frames shrink, **Toron SQL / Polars improves beyond the runs=7 noise band**, and **SQL/Rune stays
  ~1.00x** (both use `Int64` -> both must move together). A one-path move is a red flag.
- **Controls (catch join-kind / spill / partition regressions):** q04 (join-kind coverage), q19 (spill path, ~51 MB),
  q08 (403 reoriented/`PartitionedScan`) stay green at sf=10 - no regression.
- **Full gate:** `--no-fail-fast` both crates; the join suite (unique/duplicate/null/generic-order/preallocation + the
  new order-determinism tests) green; de-branded; **no committed `.perf.data`**.
- **Or the stop-line:** a sealed design/diagnostic + a named radix/partitioned follow-on - no partial q03-only table.
- **Scope honesty:** targets the ~25% Int64 hash path; **not** a Polars-parity claim (decode ~9%, memmove ~5%,
  vectorization remain); `Int32` and radix are follow-ons.

## Guardrails
- **Fence** - `Int64` + `!null_safe_keys` only; every other path untouched (bounded blast radius).
- **Contract-preserving** - probe/output/duplicate-order/determinism preserved; the lowest-risk design keeps
  `BuildRowList` value semantics and touches only key + table.
- **Stop-line over hack** - semantics needing probe/output reshaping -> design/diagnostic + radix pivot, not a q03-only
  table.
- **Behavior-neutral core-join change** - the full join suite + engine gate is the tripwire, not q03 alone.
- **Determinism** - output stays in probe-row order; the table must not introduce any build-iteration-order dependence
  (the current random hash seed is safe only because output never iterates the build map).

## Risks
- **Duplicate order** (open-addressing/chained tables tend to reorder duplicates) -> keep `BuildRowList` (or tail-insert);
  lock with a >=2-duplicate order test that fails under LIFO.
- **Null / null-safe** -> fenced out (`!null_safe_keys`); the primitive path must still route in-array nulls as no-match
  (q03 has none - test it anyway).
- **Outer/anti/semi bitmap** -> `matched_build_rows` must receive the same build indices; test every kind.
- **Spill / `PartitionedScan` interop (403)** -> the table is the in-memory build; Phase 0 confirms whether the
  spilled/partitioned per-partition build reuses `build_in_memory_join_rows` (in-scope and fine if so), and q08/q19
  controls verify no regression.
- **Memory guard drift** -> keep the current estimate conservative or prove a table-specific estimate with guard
  decision tests at under/equal/over cap.
- **Mechanism doesn't bank** -> if the specialized table doesn't beat the noise band, name it honestly (404-style); but
  the profile says the headroom is real.
- **Core-join regression** -> full `--no-fail-fast` + join suite.

## Forward Arc
If A/Int64 lands and banks, `Int32` rides the proven table contract (a small follow-on), then radix-partitioning (B)
for larger builds and parallelism, then the >360s time cells and the decode residual. If the stop-line fires, the
radix/partitioned-table design becomes the next ADR - the signal being that the current probe/output shape can't host a
specialized table without a broader execution-model reshape. Either way, 405 is the first real execution-model mechanism
after the shape (401/402) and substrate-diagnostic (404) arc. Article-grade throughout.

## Grounding evidence (file:line)
Representation: `exec/join/build_rows.rs:21` (`JoinBuildRows`), `:8` (`BuildRowList` + inline overflow), `exec/join.rs:1137`
(`JoinKey` enum), `:1144` (`JoinKeyPlan`). Build: `exec/join.rs:1274` (`build_in_memory_join_rows`), Int64 arm ~`:1306`;
plan selection `:1159` (`from_key_arrays` - `Int64` iff a single `Int64` key column and `!null_safe_keys`). Probe/output
contract: `exec/join.rs:1357` (`probe_in_memory_join_rows`), Int64 arm `:1399`: `build_rows.get(&key)` ->
`append_join_matches_for_kind(kind, row, matches.rows(), matched_build_rows, ...)` in insertion order, output in
probe-row order, `matched_build_rows` bitmap for outer/anti. 404 profile (`plans/impl/404_..._results.md`):
`rustc_entry` 10.80% + `build_in_memory_join_rows` 10.61% + `HashMap::get` 3.19% (~24.6%); q03 Int64: 3.24M rows, 829,958
unique, <=7/key, no nulls. Post-402 q03 four-way baseline: toron 373.105 / rune 372.137 / polars 102.107 / duckdb 46.0,
SQL/Rune 1.003x, Toron/Polars 3.654x.
