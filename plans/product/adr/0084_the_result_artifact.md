# ADR 0084: the result artifact and tri-state schema knowledge

Status: proposed 2026-08-12. This record gives Polariton one producer-neutral
result type carrying schema knowledge that survives the adapter boundary, so a
property a database reported is still answerable where a contract needs it. Arrow
becomes the authoritative backing for **both Arrow producers — Toron and
BigQuery**; Polars becomes a compatibility projection rather than the public
truth type. **No mark semantics change**, and no consumer
gains or loses an admission it has today.

## Context

PRODUCT-0082 §2 had to weaken a contract for a reason that was not about box
plots. The record wanted to require a nullable `outlier` column, because ADR 547
represents "no outliers" as a null row. It could not: Polariton's adapter
converts Toron's Arrow IPC batches into a Polars `DataFrame`
(`adapters/toron/polars_compat.rs:12`), and resolution sees only
`Schema<DataType>` — a non-nullable `Float64` and a nullable one containing no
nulls are indistinguishable at the point the check would run. PRODUCT-0082
stop condition 4 named the escalation: a second contract weakened the same way
turns this from a citation into a prerequisite.

### Stage 0: the distinction survives to Polariton's own door

A live asymmetric probe returned:

```
required_value  Int32  nullable=false
optional_value  Int32  nullable=true
null_counts=[0, 0]
```

Both columns were entirely non-null, so the asymmetry could only have come from
**schema metadata** — row inference would have reported the two identically.
That is what makes the probe discriminating rather than merely positive.

The path preserves it end to end until Polariton:

| stage | site |
|---|---|
| Toron computes expression nullability | `nilket/crates/toron/src/ir/execution.rs:1151` |
| the daemon preserves `field.is_nullable()` and encodes the schema into each IPC stream | `nilket/crates/toron/src/daemon.rs:2111` |
| the public client returns those IPC bytes unchanged | `nilket/crates/toron-client/src/client.rs:49` |
| **Polariton discards it** | `crates/polariton/src/adapters/toron/polars_compat.rs:12` |

**No Toron protocol or client change is required.** The database side owes
documentation and regression protection for an IPC contract that already holds,
not an implementation. This is a viz-team repair.

### Six producers, and **two** of them already speak Arrow

Scoping found what the Toron probe could not:

| adapter | wire | today |
|---|---|---|
| Toron | **Arrow IPC** | discarded at `polars_compat.rs:12` |
| **BigQuery** | **Arrow IPC** | discarded at `bigquery.rs:203`, via the same `IpcStreamReader` |
| SQLite | rows | `sqlite_to_df` (`sqlite.rs:80`) |
| Postgres | rows | `pg_to_df` (`postgres.rs:100`) |
| MySQL | rows | `mysql_to_df` (`mysql.rs:77`) |
| Parquet | file | Polars reader |

**BigQuery is the consequential find.** An earlier draft gated "only Toron
carries Arrow", which would have made this record *discard an existing Arrow
boundary* — precisely the defect it exists to remove. BigQuery assembles schema
bytes and record batches and then throws them at `IpcStreamReader`
(`bigquery.rs:188-203`), so the metadata is present and dropped exactly as
Toron's is.

For the four row-oriented and file producers an Arrow-native truth would mean
**synthesizing** a schema, and their nullability is whatever the driver reported,
if the adapter captured it at all.

Polariton also has no `arrow` dependency today: `Cargo.toml:40` pulls `polars`
with `ipc_streaming`, so Arrow IPC is currently decoded *through* Polars.

`DataFrame` appears in 35 production files — 23 under `plot/`, 6 in `adapters/`,
3 in `gui/`, plus `rune_host.rs`, `result_registry.rs`, and `editor.rs`. The
transport begins earlier than the two obvious carriers: every adapter returns
`ExecutionResult::Rows { frame: DataFrame, .. }` (`adapters/common.rs:50`) and
Rune returns `RuneDispatchResult = Result<DataFrame, …>` (`:77`), before
`StampedResult { data: Arc<DataFrame>, … }` (`plot/spec/mod.rs:1145`) and the
registry's `Arc<DataFrame>` (`result_registry.rs:49`) ever see a result. §5 lists
the full chain.

## Decision

### 1. One `ResultArtifact` for every adapter

Every adapter produces the same type. It is **producer-neutral**, not an
Arrow-only container:

```rust
pub struct ResultArtifact {
    schema: ResultSchema,
    backing: ResultBacking,
}

enum ResultBacking {
    Arrow(ArrowBacking),         // Toron, BigQuery
    Polars(Arc<DataFrame>),      // SQLite, Postgres, MySQL, Parquet
}

struct ArrowBacking {
    schema: SchemaRef,           // the full Arrow schema, dtypes included
    ipc_streams: Vec<Vec<u8>>,   // retained for the artifact's lifetime; §1b
    batches: Vec<RecordBatch>,
}
```

**The chain carries `Arc<ResultArtifact>`, never the artifact by value.**
`ExecutionResult`, `StampedResult`, and registry snapshots are all cloned today;
value ownership would copy the retained IPC bytes and, worse, give each clone an
**independent projection cache** — the same result decoded repeatedly while every
copy believed it had memoized.

One downstream truth boundary rather than two. An artifact that existed only when
Arrow happened to be available would mean "Arrow when we have it", which is too
weak a thing to build a schema contract on.

### 1a. IPC bytes are the sole input authority

`ArrowBacking` holds two representations answering **different** truths: the
batches answer schema and nullness, while the retained streams produce the Polars
values consumers see. If a caller could supply both, they could disagree — and
batch-schema validation would not catch it, because differing *values* or
differing *validity bits* both satisfy an identical schema.

The backing is therefore constructed **only** from complete IPC streams, by
private constructors that derive `SchemaRef`, the `RecordBatch`es, and
`ResultSchema` themselves. There is no public struct literal and no constructor
accepting predecoded batches. BigQuery already assembles schema and batch
messages into a complete stream before decoding (`bigquery.rs:188-203`); it hands
that stream over rather than its parts.

Without this, "one truth boundary" would contain two independently injectable
truths, which is the failure the record is named against.

**Bytes alone cannot express §2's two outcomes**, since Toron admits Arrow
nullability as knowledge and BigQuery deliberately does not. That difference is
carried by an explicit policy argument rather than by adapter-specific
constructors, which would put the decision back where a caller could vary it:

```rust
enum ArrowNullabilityPolicy {
    Preserve,   // Toron
    Unknown,    // BigQuery, until stop condition 7's probe runs
}

fn from_arrow_ipc(streams: Vec<Vec<u8>>, policy: ArrowNullabilityPolicy) -> Result<..>;
fn from_polars(frame: Arc<DataFrame>) -> Self;   // names derived, always Unknown
```

The bytes remain the sole authority for schema, rows, and validity. The policy
decides **only** whether reported nullability is admitted as knowledge — a
narrower power than a bespoke constructor, and one visible at every call site.
`from_polars` derives field names internally and reports `Unknown` with no
argument, so a row-oriented adapter has no way to claim `Known` at all.

**An `Arrow` constructor validates every batch against the full `SchemaRef`** —
field order and dtype included, not merely names. This is why `ArrowBacking`
keeps the Arrow schema even though §1c's `ResultSchema` deliberately carries no
dtype: without it, "every batch agrees with the declared schema" would be
undefined, because the declared schema would contain nothing to disagree with. An
empty result is valid and keeps its schema knowledge with zero batches.

### 1b. The IPC bytes are retained, and that costs memory

`arrow-rs`'s `RecordBatch` and Polars' `polars_arrow::RecordBatchT` are different
representations, and **no direct conversion is proven**. Decoding the IPC to
Arrow and discarding the bytes would leave §4's projection with no mechanical
path to exist.

`ArrowBacking` therefore keeps the original IPC streams, and the cached
projection decodes them once through the existing Polars IPC reader — the path
that works today. The honest cost is stated rather than buried: **the encoded
bytes are held alongside the decoded batches, and a materialized projection is a
third representation.** The streams are **retained for the artifact's lifetime**
rather than released once the projection is cached: a conditional release makes
the backing a state machine — bytes present or not — for a memory win stop
condition 4 already governs. Pinned either way, because "may release" is not a
contract.

Stop condition 4 fires if that retention proves material, at which point a direct
Arrow-to-Polars bridge must be *proven* before it is required — this record does
not assume one exists.

### 1c. `ResultSchema` carries names and nullability, not a type vocabulary

```rust
pub struct ResultSchema {
    fields: Vec<(String, FieldNullability)>,
}
```

Arrow and Polars have different `DataType` enums, and inventing a
producer-neutral third vocabulary is a large decision this record does not need:
its purpose is nullability. **Dtype questions continue to be answered by the
projection**, exactly as consumers ask them today.

That is a deliberate narrowing, not an oversight. A future contract wanting dtype
from the truth boundary is stop condition 5, and it should choose a vocabulary on
its own evidence rather than inherit one chosen here for a different purpose.

### 2. Nullability is tri-state, and `Unknown` is a real answer

```rust
pub enum FieldNullability {
    Known(bool),
    Unknown,
}
```

The defect was never Polars as such — it was a boundary with **no way to say "I
don't know."** A lossy conversion and an honest absence of information are
different facts, and collapsing them is what let a contract be written that could
not be enforced.

Toron reports `Known`, read from the IPC schema.

**BigQuery reports `Unknown` in this cut, while keeping its Arrow backing.** Its
Stage 0 has not run — no live BigQuery was available to probe — and leaving the
choice to implementation would be the "remove or refuse" shape earlier records
had to correct: product truth decided at the keyboard. So it is pinned to the
honest answer now. It may **not** be assumed equivalent to Toron because both say
"Arrow"; that both wires carry a schema says nothing about whether BigQuery's
`is_nullable` is meaningful rather than uniformly permissive. Retaining the Arrow
backing is what makes the upgrade cheap — stop condition 7 names the probe, and
satisfying it changes a mapping rather than re-plumbing an adapter.

SQLite, Postgres, MySQL, and Parquet report `Unknown` for every field until each
proves stronger metadata on its own evidence. **No adapter fabricates a schema to satisfy the type**, which is the
whole reason §1's backing is an enum rather than Arrow everywhere.

### 3. `Unknown` does not refuse anything

This record makes nullability **available**. It does not make it **required**.

An earlier proposal had `Unknown` refuse, and it was wrong in a way worth
recording: it would have silently superseded PRODUCT-0082 §2, which deliberately
replaced a schema rule with row semantics and **explicitly accepts an all-non-null
outlier column**. Reviving blanket refusal here would overturn an accepted
decision through a substrate record that never mentioned it.

Every consumer therefore admits exactly what it admits today. A consumer that
comes to require `Known` must **earn that refusal explicitly**, in its own record,
against its own exhibit — including PRODUCT-0082, should it want its original
nullable-required contract back now that the information exists.

### 4. Polars is a projection, not the truth

`ResultArtifact` exposes a **lazily built, cached** Polars projection. For an
`Arrow` backing it is materialized on first request and memoized; for a `Polars`
backing it is the frame already held, with no conversion.

The rule that makes this worth doing: **schema *and* nullness are answered from
the backing, never from the projection.** The projection exists so the 23
`plot/` consumers and the table path keep taking `&DataFrame` unchanged; it is a
compatibility surface with a scheduled end, not a second source of truth.

### 4a. Row nullness is served from the validity buffer, not from materialized values

Schema nullability says a column *may* contain nulls. It does not say **which
rows are**, and that is the question PRODUCT-0082's sentinel actually asks: is
*this* category's `outlier` null? Answering it through the projection would
force a full Arrow-to-Polars materialization to read one bit per row, which
defeats holding Arrow at all.

An `Arrow` backing therefore exposes nullness **directly from each array's
validity**, with no per-value boxing and no conversion:

The API has **two levels**, because a uniform borrowed-`NullBuffer` accessor is
not implementable: arrow-rs uses `NullBuffer` and Polars uses
`polars_arrow::bitmap::Bitmap`, so a `Polars` backing could satisfy
`Option<&NullBuffer>` only by converting or allocating.

```rust
// Producer-neutral, uniform across both backings.
fn row_is_null(&self, field: &str, global_row: usize) -> Result<bool, SchemaError>;

// Arrow-specific raw access, for consumers that want the bitmap itself.
struct ValiditySegment<'a> {
    rows: Range<usize>,          // global row range, not just a count
    nulls: ArrowNulls<'a>,
}

enum ArrowNulls<'a> {
    AllValid,
    Physical(&'a NullBuffer),    // borrowed, zero additional copy
    Logical(NullBuffer),         // owned; computed where physical ≠ logical
}
```

`row_is_null` is the question every consumer actually asks, and it is answerable
on both backings. The segment API is Arrow-only and does not pretend otherwise.
Segments carry a **global row range** rather than a count, so batch-boundary
mapping is explicit at the call site instead of reconstructed by summing.

**An explicit `AllValid` state is required, not stylistic.** `Array::nulls()`
returns `None` for an all-valid array *even when its field is nullable* — the Stage 0 `optional_value`
column is exactly that case, `nullable=true` with zero nulls. An accessor that
promised every batch a bitmap would have to **synthesize an all-valid mask**,
allocating precisely where this section claims not to.

Three further precisions:

- **Per batch, in batch order.** A multi-batch result yields a sequence of
  segments. A single contiguous mask would require concatenation, which is the
  materialization this section exists to avoid.
- **Offset and length are preserved.** Arrow arrays can be slices, so a bare
  underlying buffer is insufficient; the segment carries the `NullBuffer` with
  its own offset intact.
- **The zero-copy promise is bounded to flat arrays.** `nulls()` is *physical*
  nullness, which for dictionary, run-end, union, and null arrays is not the same
  as logical nullness. For flat primitive columns — including the `outlier`
  column that motivated this — the physical bitmap is exact and free, and the
  segment reports `Physical`. Where the two differ the segment reports `Logical`
  with an **owned** buffer, which is correct and explicitly not zero-copy.

And the promise stated exactly: **nullness inspection is zero-*additional*-copy
after Arrow decoding. The IPC-to-Arrow decode itself is not claimed to be
zero-copy.**

One property pinned so an implementation cannot quietly regress it: **asking for
nullness must not materialize the projection** (gate 12).

### 4b. Shape is answered by the backing, not by projecting

`ResultRegistry::publish` computes provenance with `rows: data.height()` and
`cols: data.width()` (`result_registry.rs:94-95`). If the artifact answered those
by projecting, **every publication would eagerly materialize Polars** — stop
condition 3 would fire on the record's own first commit rather than guarding a
future risk.

Shape is therefore backing-native:

| backing | height | width |
|---|---|---|
| `Arrow` | checked sum of batch row counts | `SchemaRef` field count |
| `Arrow`, empty | `0` | `SchemaRef` field count |
| `Polars` | frame height | frame width |

The empty row matters: a result with zero batches still has a width, and
answering `0 × 0` would lose schema knowledge the artifact demonstrably has.

### 5. Both producer transports carry `Arc<ResultArtifact>`

Two choke points are not enough. Every adapter returns
`ExecutionResult::Rows { frame: DataFrame, .. }` (`adapters/common.rs:50`) and
Rune returns `RuneDispatchResult = Result<DataFrame, RuneDispatchFailure>`
(`:77`). If those keep carrying `DataFrame`, the artifact is projected back to
Polars **immediately** and reconstructed later — schema knowledge destroyed
before it reaches either named choke point, and the record achieves nothing.

**These are two parallel transports, not four sequential hops**, and an earlier
draft described them as one chain. `ExecutionResult::Rows` is the SQL path,
handled through `Message::RunResult`; `RuneDispatchResult` is the separate Rune
relation path through `dispatch_rune_relation_with_palettes`. **No single
artifact travels both.** They converge on the shared model rather than forming a
sequence:

| carrier | path |
|---|---|
| `ExecutionResult::Rows` (`adapters/common.rs:50`) | SQL producer transport |
| `RuneDispatchResult` (`adapters/common.rs:77`) | Rune relation producer transport |
| the registry's stored value (`result_registry.rs:49`, `publish` `:85`, `snapshot` `:120`) | shared |
| `StampedResult.data` (`plot/spec/mod.rs:1145`) | shared |

All four carry `Arc<ResultArtifact>`. Identity is preserved **within** each
path's real clone and publication hops; it is not claimed across the two, since
that would assert a journey no artifact makes.

Call sites reach the frame through the projection.

No kernel signature changes. `prepare_*` functions keep taking `&DataFrame`, and
migrating them is per-consumer surface work this record does not attempt.

### 6. Narrow arrow crates

The **authoritative** decode moves to `arrow-ipc`; the **compatibility
projection** continues to use the Polars IPC reader per §1b, so both readers
exist by design. Dependencies are `arrow-array`, `arrow-schema`, `arrow-buffer`,
and `arrow-ipc` rather than the `arrow` facade — `arrow-buffer` because §4a reads
validity bitmaps directly —
unless implementation evidence shows the facade is materially cleaner — in which
case the swap is a note in the evidence file, not a new record.

### 7. What this record does not decide

No mark semantics, no admission change, no consumer migration off `DataFrame`, no
upgrade to any adapter's metadata capture, and no removal of Polars — which
remains a computation adapter and the projection's implementation.

## Implementation sequence

**Stage 1 — the type.** `ResultArtifact`, `ResultSchema`, `FieldNullability`, the
backing enum, and the cached projection, with no producer or consumer changed.

**Stage 2 — the producers.** Toron and BigQuery decode to Arrow via `arrow-ipc`;
Toron reports `Known`, BigQuery `Unknown` per §2. SQLite, Postgres, MySQL, and
Parquet wrap their existing frame and report `Unknown`.

**Stage 3 — the chain.** `ExecutionResult::Rows`, `RuneDispatchResult`, the
registry, and `StampedResult` all carry `Arc<ResultArtifact>`; call sites reach
the frame through the projection.

## Acceptance gates

1. **The asymmetric probe survives end to end.** A Toron result with
   `required_value Int32 nullable=false` and `optional_value Int32 nullable=true`,
   **both containing no nulls**, reports `Known(false)` and `Known(true)`. The
   all-non-null fixture is what makes this discriminating: row inference reports
   the two identically, so a null-counting implementation fails while passing any
   fixture that happened to contain a null.
2. **Every non-Arrow adapter reports `Unknown` through the one path that cannot
   say otherwise.** SQLite, Postgres, MySQL, **and** Parquet each report
   `Unknown` for every field, and each is asserted to construct via
   `from_polars` — testing SQLite alone would leave three adapters free to inject
   `Known` through some other route. `Unknown` is asserted as the variant
   specifically, since an implementation defaulting to `Known(true)` is
   indistinguishable from `Unknown` unless the gate names it.
3. **Both Arrow producers keep their Arrow, and no adapter fabricates it.**
   Toron **and BigQuery** carry an `Arrow` backing; SQLite, Postgres, MySQL, and
   Parquet carry `Polars`. The BigQuery half is the discriminating one — an
   implementation that migrated only Toron would pass every other gate here while
   discarding an Arrow boundary that already exists at `bigquery.rs:203`.
4. **BigQuery reports `Unknown` while keeping Arrow.** Its artifact carries an
   `Arrow` backing **and** `Unknown` for every field, constructed via
   `from_arrow_ipc(.., ArrowNullabilityPolicy::Unknown)` while Toron passes
   `Preserve` — the same constructor, differing only in the policy argument, so
   the divergence is one visible value rather than two code paths. Both halves
   matter:
   `Known` without a probe fails, and a `Polars` backing would discard the wire
   metadata that makes the later upgrade cheap.
5. **IPC bytes are the only way in.** `ArrowBacking` cannot be built from
   predecoded batches: no public struct literal and no constructor taking a
   `Vec<RecordBatch>` exists, asserted by a trybuild fixture with banked stderr
   per PRODUCT-0049's binary-only finding, plus a should-pass twin constructing
   from complete IPC streams. Without it, batches and streams are two
   independently injectable truths that schema validation cannot reconcile.
6. **Batch agreement is validated against the full Arrow schema.** A second
   batch whose dtype differs from the declared `SchemaRef` **refuses** at
   construction, and an **empty** Arrow result — zero batches — is accepted and
   still reports its schema knowledge. The dtype case is why `ArrowBacking` keeps
   the `SchemaRef`; `ResultSchema` carries no dtype and could not detect it.
7. **Publication does not project.** An unprojected `Arrow` artifact published
   through `ResultRegistry::publish` yields provenance `rows` equal to the summed
   batch row counts and `cols` equal to the `SchemaRef` field count, **and the
   projection cache is still empty afterwards** — the second half is the
   discriminating one, since projecting would produce identical numbers. An empty
   Arrow result publishes `0` rows with its real column count, not `0 × 0`.
8. **Artifact identity survives each transport separately.** On the SQL path,
   and again on the Rune relation path, the `Arc<ResultArtifact>` is the **same
   allocation** across every real clone and publication hop that path takes,
   asserted by `Arc::ptr_eq` — so no hop clones the retained IPC bytes or forks
   the projection cache. Asserted **per path**: the transports are parallel per
   §5, and a gate spanning both would assert a journey no artifact makes.
9. **Admission is unchanged.** PRODUCT-0082's box plot still accepts an
   all-non-null `outlier` column, and no consumer refuses on `Unknown` anywhere
   in the tree. This is the gate that holds §3, and it fails an implementation
   that "helpfully" treats `Unknown` as a problem.
10. **Projection parity for both Arrow producers.** For a Toron result the
   projected `DataFrame` equals what `decode_batches` produces today, and for a
   BigQuery result it equals what `bigquery.rs:203` produces today — same column
   names, dtypes, and values — so no consumer observes the change.
11. **The projection is built once.** Two accesses to the same artifact's
   projection return the same cached frame rather than decoding twice, asserted
   by `Arc::ptr_eq` rather than by value equality, which would pass against a
   re-decode.
12. **Neither schema nor nullness comes from the projection.** `ResultSchema` is
   derived from the backing's own schema, and no nullability answer is computed
   from projected column contents. The discriminating half: a Toron result
   answers a **row-nullness** query with its projection **still unbuilt**,
   asserted by observing the cache, so an implementation that materialized to
   read a bit fails while still passing gates 1 and 10.
13. **Validity segments are exact across five shapes**, and `row_is_null` agrees
   with each. One fixture covering all of them:
   - a **nullable-but-all-valid** batch yields `ArrowNulls::AllValid` — not a
     synthesized
     all-valid bitmap, which is the allocation §4a refuses and the shape the
     Stage 0 `optional_value` column actually has;
   - a null-bearing batch yields `ArrowNulls::Physical(..)` with the correct bits;
   - a row query **crossing a batch boundary** resolves to the right segment, and
     `row_is_null` agrees with the segment at every row;
   - batch order, each segment's global `rows: Range<usize>`, and the
     `NullBuffer`'s own offset are
     preserved, so a **sliced** array reports its own validity rather than its
     parent's;
   - an array whose **physical `nulls()` is absent or insufficient** — a
     `NullArray`, or a dictionary whose *values* contain nulls — yields
     `ArrowNulls::Logical(..)`, `row_is_null` reports the logical nulls
     correctly, and the projection cache is untouched. Without this shape the
     owned `Logical` branch is dead code while every other assertion passes,
     since an implementation that only ever called `Array::nulls()` satisfies the
     four above.
14. **Decoding is confined to the artifact.** `polars::io::ipc` appears **only**
   in the artifact's projection path — not in `adapters/toron` and not in
   `adapters/bigquery`, both of which decode to Arrow via `arrow-ipc`. Per §1b
   the projection decodes the retained IPC once and caches, so two decode
   *kinds* exist by design; what the gate forbids is a second site performing
   either.
15. **All four carriers hold the artifact.** `ExecutionResult::Rows`,
    `RuneDispatchResult`, the registry, and `StampedResult` each hold
    `Arc<ResultArtifact>` — asserted as four carrier **types**, not as one
    allocation traversing all four, which §5 shows no artifact does — and a Toron result's `Known` nullability is readable at
    `StampedResult` — proving nothing round-tripped through `DataFrame` and back
    in between, which is the failure §5 exists to prevent.
16. **Kernels are untouched.** Every `prepare_*` signature still takes
    `&DataFrame`, and the `plot/` suite passes without kernel edits — the
    evidence that this is a substrate cut.

## Stop condition

1. **A consumer needs `Known` to function.** §3 forbids this record from granting
   it. If a consumer cannot work without a refusal on `Unknown`, that is its
   record to write, and it must reckon with PRODUCT-0082 §2's accepted row
   semantics rather than route around them.
2. **A non-Arrow producer's nullability turns out to be free.** If SQLite,
   Postgres, MySQL, or Parquet metadata makes `Known` available with no design
   decision, §2's `Unknown` understates what that adapter has, and upgrading it
   is a small amendment rather than a reason to widen this record mid-flight.
3. **The projection is needed eagerly.** §4's laziness assumes consumers pull the
   frame. A path that needs it materialized before the artifact is stored would
   make the projection a second eager representation, which is the duplication
   this record exists to remove.
4. **Retained IPC bytes prove material.** §1b holds the encoded streams because
   no Arrow-to-Polars bridge is proven. If that retention is measurably costly, a
   direct bridge must be **proven** before it is required — the record does not
   assume one exists, and swapping to an unproven one is not a fix.
5. **A contract needs dtype from the truth boundary.** §1c deliberately carries
   only names and nullability. A consumer needing producer-neutral types must
   choose that vocabulary on its own evidence, not inherit one chosen here.
6. **The first end-to-end Toron workflow runs without requesting the
   projection.** That is the signal the bridge has done its job, and the removal
   record is drafted **immediately** — deleting the cached projection and the
   retained IPC streams together. Named as a trigger because a compatibility
   bridge left unscheduled during dogfooding becomes architecture by default.
7. **BigQuery's Stage 0 becomes runnable.** §2 pins `Unknown` because no live
   BigQuery was available to probe, not because the metadata is known to be
   absent. A live asymmetric probe — two columns differing only in declared
   nullability, both containing no nulls — either upgrades the mapping to
   `Known` or records that BigQuery's `is_nullable` is uniformly permissive and
   therefore worthless. Until one of those runs, `Unknown` is the honest answer
   rather than a settled one.

## Supersession

Supersedes nothing. Discharges PRODUCT-0082 §10's cited trigger and forecloses
its stop condition 4. Leaves PRODUCT-0082 §2's row semantics in force, per §3.

## Rejected

**Arrow-native for every producer.** Attractive because one backing is simpler
than an enum, and it would make the type honestly Arrow-native. Rejected because
four producers have no Arrow, so they would have to synthesize schemas —
manufacturing exactly the confident-looking metadata this record exists to stop
manufacturing.

**Toron-only artifact; the others keep `DataFrame`.** Attractive as the smallest
cut, and Toron is the only producer with a proven distinction. Rejected because it
leaves two truth boundaries and an artifact meaning "Arrow when we happen to have
it", which is too weak to hang a schema contract on and would need re-opening the
first time a Postgres contract wanted nullability.

**`Option<bool>` instead of a tri-state enum.** Attractive as idiomatic and
smaller. Rejected because `None` reads as "not nullable" at a glance in exactly
the code that must not confuse the two, and the whole record is about a boundary
that could not distinguish absence of information from information.

**Refuse on `Unknown`.** Attractive because it would let PRODUCT-0082 recover its
original contract immediately. Rejected because it overturns an accepted decision
from a record that never argued the point — availability and admission are
separate, and a substrate record that silently changes what a mark accepts is
doing surface work without a surface record.

**Remove Polars.** Attractive as the clean endpoint. Rejected as out of scope: 23
`plot/` consumers and the table path take `&DataFrame`, and migrating them is
per-consumer surface work. Polars stays as a computation adapter regardless.

## Consequences

Polariton gains one truth boundary, and a property Toron reported is answerable
where a contract needs it. The lesson generalizes past this column: any
**nullability** fact the wire carries can now reach a consumer — dtype
deliberately excluded by §1c — and `Unknown` gives a producer a way to be honest
about what it does not know.

**The Polars projection is a bridge with a destination, not a permanent public
workflow.** Toron is expected to subsume Polars as the computation authority, at
which point Arrow replaces `DataFrame` as Polariton's result representation
outright. An earlier draft called the migration off `DataFrame` "23 independent
surface decisions"; that both understates its coherence and overstates the work.
Most of it is **one substrate migration** — teach resolution and the
prepared-data builders to consume a narrow Arrow-backed interface — after which
the cached projection and the retained IPC streams are deleted **together**,
since §1b's retention exists only to feed the projection. Mark semantics need not
change.

The pipeline that ends this is `Rune/SQL → Toron computation → Arrow result →
Polariton resolution and preparation → renderer`, with Polars surviving only
where a non-Toron adapter or an unmigrated consumer still demands it.

PRODUCT-0082 §2 may now reclaim its original nullable-required contract, and §3
deliberately does not do that for it. Whether an all-non-null outlier column
should be admitted is a box-plot question with an accepted answer, and it should
change only where that answer was argued.
