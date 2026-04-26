# Brainstorm 005: Vectorized Stack Machine for PBC

## The Premise
The current implementation of Pushdown Bytecode (PBC) in `src/schema/pbc.rs` evaluates its stack machine using a scalar, row-by-row loop (`for row in 0..n_rows`). For every single row, it downcasts Arrow arrays, pushes scalar `Value` enum variants onto a stack, and evaluates them one by one.

While this makes sense for parsing raw bytes out of a Raft WAL, the Data Ring already holds data natively in columnar Apache Arrow `RecordBatch`es. By iterating row-by-row, we destroy branch prediction, pay the cost of dynamic dispatch repeatedly, and completely fail to utilize modern CPU SIMD (Single Instruction, Multiple Data) capabilities. We are leaving massive performance on the table.

## The Conceptual Shift: A Vectorized Stack
Standard stack machines (like the JVM or Wasm) operate on scalar values. To fix PBC's performance, we need to transition to a **Vectorized Stack Machine**.

Instead of the evaluation stack holding a single row's values (e.g., `Value::Int(18)`), the stack will hold entire columns: `Arc<dyn Array>` or Arrow `Scalar` representations.

### How it Works

1.  **Data Push (`PUSH_COL`):** 
    Instead of extracting `array.value(row)` 1,000 times, the opcode simply takes an `Arc` clone of the entire column from the `RecordBatch` and pushes it onto the stack. *Cost: O(1).*
2.  **Literal Push (`PUSH_INT`):** 
    Pushes a `Scalar` wrapper onto the stack, logically representing a column where every row contains that integer.
3.  **Comparisons (`GT`, `EQ`):** 
    Pops two items off the stack (e.g., the `age` array and the `18` scalar). Instead of a loop, it calls Arrow's highly optimized, SIMD-accelerated C++-like kernel: `arrow::compute::gt(&age_array, &scalar_array)`. This executes using AVX-512/NEON instructions and returns a single `BooleanArray` representing the results for *all rows at once*. This array is pushed back onto the stack.
4.  **Logical Ops (`AND`, `OR`):** 
    Pops two `BooleanArray`s off the stack, calls `arrow::compute::and(&bool_arr1, &bool_arr2)`, and pushes the resulting `BooleanArray` back on.

### Terminal State
At the end of the bytecode execution, the stack will contain exactly one item: a single `BooleanArray`. This is the final filter mask. We pass it directly into `arrow::compute::filter_record_batch()` and return the result.

## The Wins

1.  **No `for` loops in Rust:** The Rust code only loops over the bytecode instructions *once per batch*, not once per row.
2.  **O(1) Type Dispatch:** We match on `DataType` and downcast exactly once per column per batch, rather than `N` times per row.
3.  **Hardware Acceleration for Free:** By feeding whole arrays to `arrow::compute` kernels, Wendelon's storage nodes inherit heavily optimized SIMD filtering out of the box, allowing the Data Ring to scan gigabytes per second per core.

## Conclusion
PBC doesn't need to become a complex distributed execution engine like Spark. By simply upgrading its internal evaluation model from a Scalar Stack to a Vectorized Stack, it can become the ultimate, SIMD-accelerated "bouncer" for the Data Ring, dropping rows at memory-bandwidth speeds before they ever touch the network.

---

*The serverless/L2 pushdown ideas that were originally part of this brainstorm moved to brainstorm 010 as a separate workstream. The executor change itself was accepted as ADR 035 (Vectorized PBC Executor).*
