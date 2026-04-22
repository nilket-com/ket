# Brainstorm 005: Vectorized Stack Machine for WPBC

## The Premise
The current implementation of Wendelon Pushdown Bytecode (WPBC) in `src/schema/wpbc.rs` evaluates its stack machine using a scalar, row-by-row loop (`for row in 0..n_rows`). For every single row, it downcasts Arrow arrays, pushes scalar `Value` enum variants onto a stack, and evaluates them one by one.

While this makes sense for parsing raw bytes out of a Raft WAL, the Data Ring already holds data natively in columnar Apache Arrow `RecordBatch`es. By iterating row-by-row, we destroy branch prediction, pay the cost of dynamic dispatch repeatedly, and completely fail to utilize modern CPU SIMD (Single Instruction, Multiple Data) capabilities. We are leaving massive performance on the table.

## The Conceptual Shift: A Vectorized Stack
Standard stack machines (like the JVM or Wasm) operate on scalar values. To fix WPBC's performance, we need to transition to a **Vectorized Stack Machine**. 

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
WPBC doesn't need to become a complex distributed execution engine like Spark. By simply upgrading its internal evaluation model from a Scalar Stack to a Vectorized Stack, it can become the ultimate, SIMD-accelerated "bouncer" for the Data Ring, dropping rows at memory-bandwidth speeds before they ever touch the network.

---

## Part 2: The L2 Core Strategy (Serverless Pushdown)

If WPBC is a clean, serialized AST of vectorized operations, it doesn't just run on Wendelon's L1 NVMe nodes. It is the foundational mechanism for querying the **L2 Cold Storage tier (S3)**.

When Toron queries years of historical data, downloading terabytes of Arrow IPC files from S3 to a Wendelon node just to filter out 99% of the rows is a massive waste of network egress and CPU time. Wendelon's explicit architecture is to push the compute to the storage.

### 1. S3 Select Translation (The Simple Filter)
For basic predicates (e.g., `age > 18` or `tenant_id = 5`), Wendelon transpiles the WPBC bytecode on the fly into an **S3 Select** SQL query. AWS S3 executes the filter directly on their proprietary storage arrays and only returns the matching bytes over the network to the Wendelon node. This is a day-one architectural requirement, not an afterthought.

### 2. S3 Object Lambda / Serverless Functions (The Complex Filter)
For more complex pushdowns—like Vector Distance metrics, JSON parsing, or custom UDFs—S3 Select isn't expressive enough.

This is where the defining Toron analogy comes into play: **Rune:Toron :: Scala:Spark.** Rune (`rune-rs`) is the primary language target for data engineering, and SQL is merely the secondary compatibility surface. Toron compiles Rune scripts into our strict, vectorized WPBC AST. 

Wendelon's deployment model includes deploying a pre-compiled native Rust runtime as an AWS Lambda function (via S3 Object Lambda) or a Cloudflare Worker in the same region as the L2 bucket. 

When Toron executes a complex Rune aggregate script, Wendelon routes the compiled WPBC bytecode to the Lambda/Worker. The serverless function fetches the S3 object over the cloud provider's internal multi-terabit backbone, runs the vectorized operations against the Arrow columns, and streams *only the heavily reduced result set* back to the Wendelon node. We bypass the Wasm 4GB memory limit trap entirely because the workers execute native Rust interpreting WPBC on raw Arrow memory boundaries.

### The Impact
By serializing compute intent (WPBC) instead of just pulling bytes, Wendelon turns dumb S3 storage into a massive, infinitely scalable, serverless compute grid. It gives a single developer running Wendelon on a $5 VPS the ability to execute petabyte-scale queries by hijacking the cloud provider's internal infrastructure to do the heavy lifting, completely neutralizing Databricks' primary architectural advantage.