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

## Early Questions to Investigate

1. **Planner Mutability:** How often do we mutate or rewrite the query plan? If it’s read-only after creation, bump allocation is perfect. If optimization passes heavily rewrite and delete nodes, an index-based pool with a free-list (`slab` or generational indices) might be better to prevent memory bloat.
2. **Lifetime Contagion:** Are we willing to thread an `'arena` lifetime through the entire Binder, Planner, and Execution phases? If not, index-based pools (`u32` indices) are the mandatory path forward.
3. **Execution State:** Can we use the same arena strategy for execution-time structures (e.g., hash tables, grouping states) to mitigate the allocator churn noted in ADR 005 (Hash Join Build Path Efficiency)?

## Conclusion

Before locking in the global allocator for all logical and physical plan structures, we should benchmark a prototype AST/Plan built on `bumpalo` and another using an index-based `Vec` pool (like `cranelift-bforest` or `slab`). The index-based approach seems very promising to avoid lifetime complexity while achieving cache locality.