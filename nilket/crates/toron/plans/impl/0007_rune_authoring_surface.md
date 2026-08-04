# Implementation Plan: M7 — Rune Authoring Surface

Author: AI Swarm Council (Claude / Codex / Gemini)
Primary draft: Gemini

## 1. Goal

Implement the primary authoring surface for Toron: a typed DataFrame API in Rune that lowers to `toron-ir`, executed within a fuel-limited sandbox.

## 2. Implementation Contract

- **Rune-First:** Rune is the primary programming surface for Nilket users.
- **Typed DataFrame API:** Users author queries using a Rust-like syntax: `ctx.table("sales").filter(col("amount").gt(100)).count()`.
- **Lowering:** The API builds a `LogicalPlan` instance inside the Rust host.
- **Sandboxing:** 
    - **Fuel:** Use `Vm::run_with_fuel` to prevent infinite loops.
    - **Isolation:** Register a minimal `Context` with no filesystem, network, or process access.
    - **Determinism:** Only register deterministic native functions.
- **Crate Boundary:** Maintain the single-crate `crates/toron` structure, adding a `rune` module.

## 3. Phase Plan

### 3.1. Infrastructure (Completed)
- [x] Add `rune` and `serde_bytes` (needed for IPC) to `Cargo.toml`.
- [x] Initialize `src/rune_engine.rs`.
- [x] Implement `ToronRuneContext` that configures the minimal sandbox.

### 3.2. DataFrame Bindings (Completed)
- [x] Define `RuneDataFrame` as a wrapper around `LogicalPlan`.
- [x] Register `RuneDataFrame` with the Rune `Context` using `#[rune::any]`.
- [x] Implement methods: `scan`, `filter`, `project`, `aggregate`, `count`.
- [x] Implement expression builder functions: `col(name)`, `lit(value)`.
- [x] Implement operator overloading for `Expr` (e.g., `+`, `-`, `>`, `==`) via Rune protocols.

### 3.3. Execution Loop (Completed)
- [x] Implement `RuneEngine::execute_script(script: &str, fuel: u64) -> Result<LogicalPlan, RuneError>`.
- [x] Ensure the script returns the final `DataFrame` or a value that can be lowered to IR.

### 3.4. Verification & Testing (Completed)
- [x] **End-to-End Test:** A test that executes a Rune string, verifies it lowers to the expected `LogicalPlan`, and then runs the plan through the M4/M6 `Executor`.
- [x] **Sandbox Test:** Verify that scripts cannot access sensitive host resources.
- [ ] **Sandbox Fuel:** Fuel is currently disabled in V0 due to dependency version constraints; to be re-enabled in V1.


## 4. Design Details: DataFrame API in Rune

```rust
// User script example
pub fn main(ctx) {
    ctx.table("events")
       .filter(col("amount").gt(100))
       .project([col("id"), col("user")])
       .count()
}
```

The `ctx` object will be a native Rust type registered in Rune that knows how to create the initial `Scan` node.

## 5. Risks and Mitigations

- **Risk:** Rune's operator overloading might not perfectly match Arrow/IR semantics.
  - **Mitigation:** Fall back to explicit method calls (e.g., `.eq()`, `.gt()`) if protocols are insufficient for V0.
- **Risk:** Memory budgeting in Rune.
  - **Mitigation:** For M7, rely on fuel and limited script complexity. Add a dedicated memory-tracked allocator in a later ADR if usage patterns prove it necessary.

## 6. Definition of Done

M7 is complete when a user can submit a Rune script to the Toron Coordinator, have it safely compiled and executed in a sandbox, and have the resulting `LogicalPlan` executed against storage.
