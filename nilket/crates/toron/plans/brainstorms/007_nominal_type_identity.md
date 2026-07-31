# Brainstorm 007: Nominal Type Identity Substrate

**Status:** Brainstorm / Strategic Lead
**Category:** Toron Substrate / Type System
**Lead:** Gemini

**Outcome note:** ADR 087 promoted a narrower first cut from this brainstorm:
`LogicalTypeId` as a sidecar over Arrow `DataType`, plus Rune
`.as_logical_type::<PersonId>()` marker dispatch. The full `DataType` replacement
and string-based Rune surface below remain brainstorm context, not the promoted
ADR 087 plan.

## The Problem: Structural "Oops" Joins

In standard SQL (and current Toron), types are **Structural**. If two columns are physically `u64`, they are "compatible." This allows nonsensical operations that pass the binder but fail the business logic:

```sql
-- Physically legal, logically wrong
SELECT * FROM products p
JOIN customers c ON p.id = c.id;
```

Current "refinement types" (like in PGSQL) are typically implemented as `CHECK` constraints run at row-time. They are "ghetto" because they don't provide **static protection** against relationship errors.

## The Goal: Data Integrity as a Compiler

We want to elevate the **Newtype Pattern** from programming languages into the Database substrate. A column should not just be an `Int64`; it should be a `PersonId` that happens to be stored as an `Int64`.

### The "Substrate Leap"

We move from a flat `DataType` to a dual-layered **Nominal Identity**:

1.  **Physical Layout:** `Int32`, `Int64`, `Utf8`, etc. (The "How").
2.  **Logical Identity:** `PersonId`, `ProductId`, `None` (The "Who").

### The Stack-Local Identity (Technical Hinge)

To ensure O(1) safety checks without heap overhead, the Identity must be stack-local. We have two primary candidates:

*   **Option A (The Index):** `logical_id: u32`. A vtable-style index into a **Type Registry**. 0 is "naked."
*   **Option B (The Tag):** `logical_tag: [u8; 8]`. A fixed-width inline string (e.g., `"PERS_ID"`). Fast comparison, no registry lookup for basic join protection.

**The Index (`u32`) is the stronger long-term lead** because it provides a handle for **Refinement Contracts** (e.g., `val > 0`) which can be used by the binder for static plan elision.

### The Semantic Classification

To provide clarity for the council, we classify these concepts into distinct "integrity levels" for the Nilket substrate:

1.  **Nominal Types (Identity Protection):** Types that are distinct based on their **identity**, even if their physical structure is identical. This is the "Newtype" pattern. (e.g., `PersonId != ProductId`). **Goal: ADR 087.**
2.  **Logical Types (Abstraction Layer):** The category name for semantic meanings mapped onto physical storage. In Nilket, "Logical Type" is the *mechanism* (the sidecar metadata) used to implement Nominal Typing.
3.  **Refinement Types (Contract Layer):** A base type plus a **predicate** that must hold for all values (e.g., `Int64 where val > 0`). This unblocks **Static Plan Elision** in the binder. **Goal: Post-ADR 087.**
4.  **Dependent Types (Hard Mode):** A type whose definition **depends on a value** (e.g., a column whose schema changes based on another column in the same row). This is the far-horizon goal for value-driven integrity.

## The New Type Substrate (`DataType`)

```rust
pub struct DataType {
    pub physical: PhysicalType, // 1 byte enum
    pub logical_id: u32,        // 4 byte handle
}
```

This 5-byte struct remains register-friendly and copyable.

## Enforcement: The Nominal Join Gate

The **Binder** changes its definition of equality. An expression `A = B` (and thus a Join condition) is only valid if:
`A.type.physical == B.type.physical` **AND** `A.type.logical_id == B.type.logical_id`.

*   **Result:** `Person.id (PersonId) = Task.person_id (PersonId)` -> **VALID**.
*   **Result:** `Person.id (PersonId) = Product.id (ProductId)` -> **BINDER ERROR**.

### The Escape Hatch (Explicit Intent)
To join across identities, the user must author an explicit cast:
`ON p.id::Naked = c.id::Naked` or `ON p.id = c.id::PersonId`.
This makes the "logical bypass" visible and deliberate in the source code.

## Elevating to Refinement Types

With the `logical_id` handle, the binder can query a **Refinement Registry**:

```rust
// logical_id 42 -> PersonId (Int64, "val > 0")
let person_id_contract = registry.get_contract(42);
```

During binding, Toron uses these contracts for **Static Proofs**:
*   Query: `WHERE person_id = -5`
*   Binder: `-5` violates `val > 0`.
*   Optimization: Statically return `EmptyRelation`. Zero storage I/O.

## Pilot Implementation (ADR 087 Candidate)

We can pilot this using the Rune composition layer:

1.  **Substrate:** Update `DataType` in `ir.rs`.
2.  **Authoring:** Add `expr.as_logical_type("PersonId")` to Rune.
3.  **Proof:** A Rune script where joining `product_id` to `customer_id` fails to bind, proving **Nominal Protection**.

## Strategic Impact

This turns Nilket from a "bucket of bits" into a "statically typed relation engine." It prevents a whole class of data-modeling bugs at the plan-authoring stage, long before a single row is read from Wendelon.
