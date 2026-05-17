# Brainstorm 004: Memory Arenas and Custom Allocators for Query Planning

**Status:** Speculative / Non-Contractual
**Relationship to V1:** This brainstorm explores memory allocation strategies for query plans and execution structures. While early ADRs deferred these decisions (often citing "allocator questions that need their own evidence"), this document serves as a repository for ideas regarding custom allocators to reduce memory fragmentation, overhead, and allocator churn during complex query planning and execution.

## The Premise

In a vectorized execution engine like Toron (which heavily utilizes Arrow-rs morsels/record batches), the execution phase primarily involves coarse-grained allocations—allocating large arrays of columnar data at once. Because of this, per-row fine-grained object churn is largely eliminated during execution.

Instead, the **Query Planning Pipeline** (parsing ASTs, binder resolution, logical planning, optimization rules, and physical plan generation) becomes the primary hotspot for massive numbers of small, short-lived objects. Relying purely on the global allocator (like `jemalloc` or system malloc) for each plan node via `Box` or `Rc`/`Arc` leads to significant allocation overhead and cache locality issues during these phases.

By introducing memory arenas or region-based memory management for the planning pipeline, we can:
1. Drastically reduce allocation time (bump allocation is merely pointer incrementation).
2. Improve data locality, reducing cache misses during plan traversal and optimization passes.
3. Eliminate the need to drop individual nodes, freeing entire plan sub-trees in a single operation (`O(1)` deallocation).

## Potential Approaches

### 1. External Crates (`bumpalo` or `typed-arena`)

The Rust ecosystem has established crates for arena allocation:

*   **`bumpalo`**: A fast bump allocation arena for Rust. It allows allocating objects of different types into a single arena and drops the entire arena at once. It’s widely used in compilers and parsers (e.g., in the Rust compiler and JS engines).
    *   *Pros:* Highly optimized, supports different types, integrates well with `std` collections via `bumpalo::collections`.
    *   *Cons:* Can make borrowing and lifetimes more complex within the AST if not designed carefully. Requires `#![feature(allocator_api)]` for some seamless std integrations, or using their custom collections.

*   **`typed-arena`**: An arena that allocates objects of a single, specific type.
    *   *Pros:* Extremely simple to use, safe, and efficient for homogeneous graph structures (e.g., if all AST nodes are the same enum size).
    *   *Cons:* If the AST has vastly different node sizes or we need many different types, we might need multiple typed arenas, which increases complexity.

### 2. Cranelift's BForest Inspiration and the SOA Connection

The Cranelift compiler uses a highly specialized data structure called `BForest` (from `cranelift-bforest`) which is a B-tree-like structure optimized for allocating many small trees using a common memory pool.

*   **Concept:** Instead of general-purpose allocation, we model a `QueryPlanPool` or `AstPool`. We allocate nodes continuously and represent tree/graph edges as indices (e.g., `NodeId(u32)`) rather than raw pointers or references.
*   **The SOA Connection:** This index-based approach is eerily similar to the **Structure of Arrays (SOA)** pattern. By flattening trees into contiguous vectors and navigating via indices, we achieve the same cache-friendly, pointer-chasing-free memory layout that makes vectorized execution fast, but applied to the query planner's graph structures.
*   **The Advantage:** Indices remove the lifetime contagion (`'a`) associated with standard arena references, making the planner structures easily movable, serializable, and mutable without fighting the borrow checker. We pass the `QueryPlanPool` context whenever we need to resolve a node.

### 3. Roll Our Own Allocator / Pool

Depending on our exact access patterns, a domain-specific pool might be optimal.

*   *Structure:* A simple `Vec<PlanNode>` where nodes are allocated by appending to the vector and referenced by index.
*   *Recycling:* We could maintain a free-list inside the vector if we need to remove and re-add nodes dynamically during query optimization rules (similar to an Entity-Component-System (ECS) generation arena like `slab` or `generational-arena`).

### 4. Expression Representation: Logical AST vs Physical Expression Program

The current Toron expression surface is a boxed recursive AST:

```rust
pub enum Expr {
    Column(ColumnRef),
    Literal(ScalarValue),
    Cast(Box<Expr>, DataType),
    TryCast(Box<Expr>, DataType),
    Add(Box<Expr>, Box<Expr>),
    // ...
}
```

This shape is a good **logical contract** for SQL lowering, planner
guards, source-read architecture tests, serialization, and small
tree rewrites. It is not the shape we should assume for future
hot-path expression execution. Recursive `Box<Expr>` nodes imply
heap allocation, pointer chasing, and branch-heavy tree walking.

A future SoA/SIMD-friendly design should keep the current `Expr`
as the logical representation and lower it after binding into a
physical expression program. Two plausible layers:

```rust
pub type ExprId = u32;

pub enum PhysicalExprNode {
    Column { ordinal: u32 },
    Literal { slot: u32 },
    Cast { input: ExprId, target: DataType },
    TryCast { input: ExprId, target: DataType },
    Binary { op: BinaryOp, left: ExprId, right: ExprId },
    Unary { op: UnaryOp, input: ExprId },
}

pub struct PhysicalExprArena {
    nodes: Vec<PhysicalExprNode>,
    output: ExprId,
    literals: Vec<ScalarValue>,
}
```

For execution, the arena can be lowered again into a flat typed
instruction tape:

```rust
pub enum TypedOp {
    LoadColumn { ordinal: u32, ty: DataType },
    LoadLiteral { slot: u32, ty: DataType },
    AddInt32,
    AddInt64,
    AddFloat32,
    AddFloat64,
    CastFloat64ToInt32,
    TryCastFloat64ToInt32,
}
```

The important performance boundary is not merely "tree vs tape."
SIMD friendliness comes from **typed vector kernels over Arrow
arrays**: contiguous primitive buffers plus validity bitmaps. By
the time execution runs, the binder should have resolved data
types so the physical program dispatches to tight typed kernels
instead of a generic `Op::Add` that re-checks types row by row.

Recommended future split:

1. **Frontend / logical layer:** keep `Expr` boxed and readable.
2. **Middle-end / optimizer layer:** optionally lower to
   `PhysicalExprArena` or another index-based arena for cache-
   friendly rewrites without lifetime contagion.
3. **Backend / execution layer:** lower to a typed expression tape
   or compact physical expression program over Arrow arrays.

This also gives future JIT work a clean seam: a
`PhysicalExprProgram` can be interpreted first, then compiled later
without changing the SQL planner's logical `Expr` contract.

## Early Questions to Investigate

1. **Planner Mutability:** How often do we mutate or rewrite the query plan? If it’s read-only after creation, bump allocation is perfect. If optimization passes heavily rewrite and delete nodes, an index-based pool with a free-list (`slab` or generational indices) might be better to prevent memory bloat.
2. **Lifetime Contagion:** Are we willing to thread an `'arena` lifetime through the entire Binder, Planner, and Execution phases? If not, index-based pools (`u32` indices) are the mandatory path forward.
3. **Execution State:** Can we use the same arena strategy for execution-time structures (e.g., hash tables, grouping states) to mitigate the allocator churn noted in ADR 005 (Hash Join Build Path Efficiency)?
4. **Logical/Physical Split:** Where should `Expr` lower into a
   physical expression arena or tape? Candidate seam: after binding,
   once column ordinals and data types are explicit.
5. **Typed Tape vs Arena Interpreter:** Is the first physical
   executor a flat typed tape, an arena interpreter, or both
   (arena for rewrites, tape for execution)?
6. **Arrow/SIMD Contract:** Which kernels must stay hand-written
   typed loops over Arrow arrays, and which can be delegated to
   Arrow compute kernels without obscuring Toron's null/error
   semantics?
7. **Plan Digest Stability:** If logical `Expr` remains the digest
   contract and physical programs are derived artifacts, how do we
   keep plan digests byte-identical while adding physical lowering?

## Conclusion

Before locking in the global allocator for all logical and physical plan structures, we should benchmark a prototype AST/Plan built on `bumpalo` and another using an index-based `Vec` pool (like `cranelift-bforest` or `slab`). The index-based approach seems very promising to avoid lifetime complexity while achieving cache locality.

For expressions specifically, do not prematurely replace the
logical boxed `Expr` just to chase SIMD. Keep `Expr` as the planner
contract, and introduce a separate physical expression program when
execution evidence justifies it. That future ADR should measure
arena/tape interpretation against the current recursive evaluator
and should pin which Arrow-array kernels are expected to
auto-vectorize or use explicit SIMD.
