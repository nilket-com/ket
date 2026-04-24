# ADR 035: Vectorized WPBC Executor

## Status
Accepted

## Context
Wendelon Pushdown Bytecode (WPBC) is the storage-side compute contract accepted by ADR 005. Today the interpreter in `src/schema/wpbc.rs` compiles bytecode once, then evaluates each Arrow `RecordBatch` row by row. That implementation is correct and fuzzed, but it contradicts the actual shape of the storage engine: Wendelon stores and returns columnar Arrow batches.

The row loop creates three avoidable costs on every query:

- per-row dynamic dispatch and downcast work
- per-row stack allocation and scalar enum traffic
- missed use of Arrow's vectorized compute kernels

The larger product direction makes this more important, not less. Toron is the distributed compute tier, but Wendelon must still be aggressive about storage-local filtering and projection. Rune is the preferred user surface, SQL is the compatibility surface, and both lower through Toron's logical/physical planning into the same constrained storage pushdown target (see ADR 036).

WPBC therefore needs to become a vectorized physical expression target, not a row interpreter that happens to sit near Arrow.

This ADR is scoped to local `RecordBatch` execution only. L2/serverless pushdown is a different compute-placement problem with key-custody, audit, provider, deployment, and cost questions, tracked separately.

## Decision
Wendelon replaces WPBC's default execution model with a vectorized Arrow-batch interpreter.

1. **The bytecode ISA remains stable for this workstream.**
   Existing opcodes keep their meaning. The implementation changes from scalar row evaluation to batch evaluation, but valid programs continue to produce the same filter and projection results. WPBC remains V1; no new opcodes, widened immediates, or new Arrow type families land in this workstream.

2. **The runtime stack holds arrays and scalars, not row values.**
   The vectorized interpreter evaluates a `Program` once per `RecordBatch`. Stack entries are typed vector values:
   - `ArrayRef` for columns and computed arrays
   - scalar literals represented by a small WPBC-local scalar enum
   - `BooleanArray` masks for predicates
   - `Identity` and `Locality` context values kept as distinct newtype families

3. **Arrow compute kernels are the preferred execution primitive.**
   Comparisons, boolean operations, null checks, and filtering dispatch to `arrow::compute` kernels where available. Custom loops are allowed only for gaps Arrow does not cover, and those loops must be isolated behind small helper functions.

4. **The scalar interpreter becomes a permanent test oracle, not a production fallback.**
   The existing row interpreter moves behind a test-only `execute_program_scalar_reference` path and remains there indefinitely. Differential parity is enforced via two fuzz layers: byte-mutation fuzz (exercises parser and validator) and generator-based valid-program fuzz (exercises the kernel paths). Both must pass scalar-vs-vector byte-identical output on every CI run. This is cheap insurance against Arrow kernel semantic drift.

5. **WPBC remains deterministic, bounded, and non-general-purpose.**
   Vectorization does not turn Wendelon into a general compute engine. WPBC still rejects unbounded control flow, arbitrary I/O, arbitrary user code, and storage-unsafe operators. `FUEL_PER_ROW` is retired from the production vectorized path; the operative bounds are `MAX_BYTECODE_BYTES`, `STACK_DEPTH_LIMIT`, compile-time validation, query deadlines, and Data Ring materialization limits.

6. **Projection remains terminal and cheap.**
   `PROJECT` stays terminal. Filtering produces a Boolean mask and applies `arrow::compute::filter_record_batch`; projection then calls `RecordBatch::project`. Projection-only programs avoid unnecessary mask allocation.

7. **Toron compiles pushdown from `toron-ir`, not from SQL or Rune syntax.**
   Rune and SQL are authoring surfaces. The stable pushdown decision is made on typed Toron expression nodes. Wendelon receives validated WPBC bytes; new opcode families later require explicit version/capability metadata per ADR 036.

8. **Trap behavior stays observable-compatible with scalar WPBC.**
   Scalar WPBC traps a bad operation per row and drops that row. Vector WPBC may detect the same mismatch once per batch. Observable behavior remains an empty-row batch with `PROJECT` applied, not a hard query error, for runtime type mismatches that were valid bytecode but invalid against the batch.

9. **Current cross-type comparison semantics are compatibility requirements.**
   The scalar interpreter currently supports `Int`/`Float` comparison by `f64` conversion and `Timestamp(ms)`/`Int` comparison by integer millisecond value. Vectorization preserves those semantics explicitly. No general implicit casting is introduced; any semantic change is deferred to a separate ADR.

10. **Batch-size admission stays upstream.**
    The vectorized executor does not own a hard rows-per-batch or bytes-per-batch cap. Query-path materialization limits and deadlines remain the admission boundary. If operators need WPBC-specific caps later, that deserves its own ADR.

11. **Structured metrics are part of the hot-path rewrite.**
    Vectorized WPBC emits the following metric surface per program execution:

    - counters: `wpbc_rows_scanned`, `wpbc_rows_returned`, `wpbc_kernel_error_total`, `wpbc_intermediate_arrays`
    - timer: `wpbc_exec_micros`
    - derived: `wpbc_mask_selectivity` (`rows_returned / rows_scanned`)
    - common label: `runtime={vectorized,scalar_reference}` — applied where emission is shared with the scalar-reference oracle in tests. Production telemetry only ever carries `runtime=vectorized` because the scalar path is `#[cfg(test)]` only.

    These are observability hooks, not admission hooks.

12. **Projection-only programs are a first-class fast path.**
    Empty-op programs with a terminal `PROJECT` use direct `RecordBatch::project` and allocate no Boolean mask. An empty program with no projection returns the original batch unchanged.

13. **The WPBC spec tracks the executor change.**
    `plans/schema/compute_ast.md` is updated in the implementation PR to describe batch/vector execution while preserving the V1 ISA. The intro and compute-boundary prose are also modernized to reference Toron/`toron-ir` (per ADR 036) rather than prior external-compute framings.

## Rationale
The storage nodes already have Arrow batches in hand. Evaluating bytecode row by row turns a columnar data path back into a miniature row engine at the exact place where Wendelon most needs memory-bandwidth behavior.

Keeping the ISA stable makes this an implementation acceleration rather than a wire-format redesign. WPBC is already present in query frames, tests, and fuzzing; the correct first step is semantic parity with much better execution mechanics.

Arrow kernels are the right default because they preserve type correctness, null semantics, and SIMD-friendly execution without Wendelon owning a large handwritten expression engine. Wendelon's advantage should come from placing compute beside storage and avoiding network bytes, not from maintaining a competing kernel library.

Using `toron-ir` as the source of pushdown keeps the Rune-first product direction clean. Rune can feel native and programmable without making Wendelon understand Rune. SQL can remain supported without making SQL the architecture.

Keeping the scalar interpreter as a test oracle is worth the small maintenance cost. Arrow is a dependency surface. A future Arrow upgrade that changes null handling, string comparison, timestamp coercion, or NaN behavior is caught by differential tests before it reaches users.

Separating L2/serverless pushdown protects the implementation schedule. Local vectorized execution is bounded and measurable; provider-adjacent execution has separate trust and cost boundaries.

Keeping admission upstream preserves layering. WPBC's job is to evaluate a validated physical expression over a batch it is handed; query planning and data-server materialization decide how large that batch is allowed to be.

## Consequences (The "Negatives")
Vectorized execution makes WPBC internals more complex. The current scalar `Value` model is easy to inspect in a debugger; vector stack entries need stricter type handling, clearer error paths, and more parity tests.

Some current semantics are awkward in a vector model. Typed identity/locality opcodes read a single `ExecutionContext` and therefore produce all-true or all-false masks for a batch. That is acceptable for current range-query usage, but future per-row identity/locality filtering would require new typed column opcodes rather than reusing generic `PUSH_COL`.

Arrow kernel behavior must be treated as part of the compatibility surface. Null propagation, string comparison, timestamp comparison, NaN handling, and type coercion need explicit tests because a dependency upgrade can subtly change behavior.

Vectorization changes safety accounting. The old per-row fuel model is not the right runtime bound for batch execution. Compile-time bytecode length, stack depth, and opcode count bounds remain, plus batch-size/materialization limits in the query path.

The first vectorized implementation may allocate intermediate arrays for each expression. That is acceptable for the first correctness milestone; mask fusion and temporary-array reuse are follow-on work, not blockers.

Runtime mismatches lose row-local diagnostics. The scalar engine could conceptually identify a specific failing row; the vector engine identifies the failing opcode/type pair. Error context names the opcode and involved Arrow types even when the observable result is an empty batch.

Metrics add hot-path work. The trade is accepted because this is a major executor rewrite; without basic visibility, correctness and performance regressions become harder to diagnose once the code is under Toron-driven query load.

General implicit casting remains rejected. Only the cross-type comparisons already implemented by the scalar interpreter are preserved. Any broader cast/coercion behavior belongs in a future ISA/version decision, not in vectorization.

## Rejected Alternatives
- **JIT predicate execution.** Rejected for V1. Arrow kernels give most of the benefit without adding a compiler, toolchain, security, and reproducibility burden to the storage binary.
- **Partial vectorization with production scalar fallback.** Rejected. Keeping a row-loop fallback in production would hide unsupported vector paths and split the correctness surface. The scalar interpreter stays test-only.
- **Bundling L2/serverless pushdown.** Rejected for this ADR. That is a compute-placement and trust-boundary problem, not an executor-internals problem.
- **Deleting the scalar interpreter after parity.** Rejected. It is the cheapest long-term guard against Arrow semantic drift.

## Open Questions
1. **Kernel coverage gaps.** Which existing opcodes cannot be expressed cleanly through Arrow kernels in the pinned Arrow version, and which require custom helpers? Resolved during Phase 0 of the implementation spike.
2. **Future aggregate protocol shape.** The default position (ADR 036 decision 10) is that partial aggregates should be a sibling protocol, not WPBC V2. The final form is decided when the first partial-aggregate workstream opens.

## References
- ADR 005: Decoupling Compute via Pushdown Bytecode (WPBC)
- ADR 029: Identity vs. Locality (the newtype-units discipline preserved here)
- ADR 034: Toron-Wendelon Integration Contract
- ADR 036: WPBC as the Toron-to-Wendelon Pushdown Contract
- `plans/schema/compute_ast.md` — WPBC V1 spec
- `src/schema/wpbc.rs` — the file this ADR rewrites
