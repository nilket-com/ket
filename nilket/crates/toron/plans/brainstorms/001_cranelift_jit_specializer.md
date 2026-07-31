# Brainstorm 001: Pure Cranelift JIT as a First-Class Query Specializer

**Status:** Speculative / Non-Contractual
**Relationship to V1:** This brainstorm represents a potential long-term optimization path. It is **not** part of the Toron V1 "Integrity Floor" defined in ADR 002. The V1 architecture relies on a vectorized interpreter and Arrow-rs; the Polars and JIT-compilation strategies described here are reserved for future architectural deliberation.

## The Premise
Bridging a dynamic scripting language like Rune to a distributed Rust dataframe ecosystem often forces a harsh tradeoff between flexibility and bare-metal performance. However, by treating Cranelift as a "Just-In-Time Specializer," we can achieve zero-cost abstractions on the hot path. We don't compile the entire script; instead, the scripting language handles the "cold" orchestration, while we JIT-compile the "hot kernels" (predicates, maps, and complex mathematical UDFs).

By leveraging the pure `cranelift-jit` library over WebAssembly sandboxes, Toron can compile dynamic query plans into native machine code that operates directly on Wendelon's distributed memory.

## 1. The "Intermediate" Representation (The Bridge)
We cannot compile Rune bytecode directly into Cranelift efficiently due to dynamic typing. Instead, we introduce a shared Expression AST as the contract between the script and the JIT.

*   **Mechanism:** When a user writes a predicate like `df.filter(a + b > 10)`, Rune does not execute the math. It constructs a static tree representation, such as `Op::Greater(Op::Add(Col("a"), Col("b")), Lit(10))`. This tree is then passed directly to the Cranelift module.
*   **The Advantage:** This entirely decouples the slow, dynamic VM from the execution phase. The JIT compiler receives a perfectly typed, deterministic blueprint of the logic, allowing it to apply aggressive optimizations before a single row of data is processed.

## 2. ABI and Memory Mapping (The "Secret Sauce")
To achieve native speeds, the JIT-compiled machine code must "see" the Arrow/Polars memory exactly as it sits in RAM, avoiding any serialization overhead.

*   **Mechanism:** Because Polars uses Arrow RecordBatches, the data resides in contiguous buffers. We define the JIT function signature in Cranelift using `I64` parameters to represent raw pointers and lengths. The resulting machine code looks like this to the CPU:

```rust
// What the JIT function looks like to the CPU
fn jit_kernel(len: usize, col_a: *const u64, col_b: *const u64, out: *mut u8) {
    for i in 0..len {
        let a = unsafe { *col_a.add(i) };
        let b = unsafe { *col_b.add(i) };
        unsafe { *out.add(i) = (a + b > 10) as u8 };
    }
}
```

*   **The Advantage:** By passing raw pointers into the JIT, we bypass the 4GiB WASM boundary and sandbox constraints entirely. The compiled code lives in the exact same address space as Wendelon’s storage layer, iterating over memory-mapped arrays at maximum CPU cache efficiency.

## 3. The "Trampoline" to Polars
We need a seamless way for the Rust host (Toron) to jump into the dynamic memory address where Cranelift just wrote the machine code, and integrate it with our data frames.

*   **Mechanism:** We walk the Expression AST to emit Cranelift IR, call `module.finalize_definitions()`, and extract the memory address using `module.get_finalized_function()`. We then use `mem::transmute` to cast that raw `*const u8` into a callable Rust function pointer. Finally, we extract the underlying buffer from a Polars Series, pass its `.as_ptr()` to our function, and wrap the result back into a `ChunkedArray`.
*   **The Advantage:** This allows us to hook deeply into Polars using native `ChunkedArray::apply` or `map_batches` paradigms without rewriting the underlying dataframe library. We cross the FFI boundary exactly once per batch, completely eliminating per-row VM overhead.

## 4. The "Fear-Based" Strategy: Scalar UDFs First
Building a complete vectorizing JIT from scratch is a massive undertaking. We can de-risk the architecture by starting small.

*   **Mechanism:** Before attempting to JIT an entire dataframe loop, we start by JIT-compiling a single Scalar UDF (e.g., a function that takes two `u64` values and returns one `u64`).
*   **The Advantage:** This validates the end-to-end pipeline—from Rune AST generation to Cranelift IR lowering to `mem::transmute` execution. Once we can dynamically call an `add(a, b)` function, upgrading to a full Arrow Buffer loop simply requires adding pointer arithmetic offsets to our existing IR generator.

## Conclusion
By embedding `cranelift-jit` directly into Toron, we create an architecture where high-level "Vibe Coding" in Rune orchestrates bare-metal execution. Because Pure Cranelift operates without WASM sandboxing, our JIT functions can accept raw DMA pointers or `io_uring` buffers directly. This guarantees zero-copy, native-speed execution for arbitrary user-defined computations across the Wendelon ring.
