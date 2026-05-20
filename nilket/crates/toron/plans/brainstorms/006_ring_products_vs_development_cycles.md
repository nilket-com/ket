# Brainstorm 006: Ring Products vs. Development Cycles

**Status:** Adopted — promoted to a standing convention at
`../conventions/ring-product-placement.md`. This file remains the
origin/rationale record; the convention doc is authoritative for
the rule as applied.
**Context:** Navigating the expansion of the "Operator Plane" and the symmetric parity between Toron (compute) and Wendelon (storage).
**Adoption note:** Validated against the full ADR 040-onward arc
(consistent with every prior filing) and triggered by a concrete
misfile — `nilket wendelon inspect` was drafted as Toron ADR 049
and relocated to Wendelon ADR 071 per The Placement Rule below.

## The Framework

A 2x2 matrix for categorizing design cycles based on their product scope (Interior/Exterior) and technical nature (Substrate/Composition).

| | **Substrate Cycle** (The "How") | **Composition Cycle** (The "What") |
| :--- | :--- | :--- |
| **Interior Product** (Shared Infrastructure) | **Extraction / Refactor.** <br> *Example:* `tls-pki`, `observability-http`. <br> *Goal:* Pay down debt, consolidate shared logic. | **Coordination / Glue.** <br> *Example:* `nilket` unified entrypoint. <br> *Goal:* Unify the product surface, hide complexity. |
| **Exterior Product** (Ring-Specific Role) | **Engine / Protocol.** <br> *Example:* `toron-ir`, `wendelon-lsm`. <br> <br> *Goal:* Build ring "truth", scale, and capability. | **Feature / Surface.** <br> *Example:* `inspect` CLI, `sql` runner. <br> *Goal:* Deliver parity, user-facing verbs. |

## Key Heuristics

### 1. The Placement Rule
- **Interior Products** (Substrate or Composition) should live in the shared coordination hub (`crates/toron/plans/adr/` acting as the workspace aggregator).
- **Exterior Products** should "Return Home" to their specific ring's design sequence. For example, storage-specific features like `nilket wendelon inspect` live in `crates/wendelon/plans/adr/071_...`.

### 2. Risk Profiles
- **Substrate Cycles** carry **Integration Risk**. Success is measured by byte-identical behavior preservation and architecture guard stability.
- **Composition Cycles** carry **Parity Risk**. Success is measured by how accurately they mirror a user-facing contract across rings.

### 3. The Leap Rule
Genuinely new features (like Multi-tenant routing) are often **Substrate Leaps** disguised as Composition tasks. They require a design of the "Interior How" (isolation logic) before the "Exterior What" (user flags) can be wired.

## Implications for the Swarm Council

- **Topic Selection:** Use the matrix to identify if a candidate is a "clean-up" (Interior Substrate), a "parity lift" (Exterior Composition), or a "capability expansion" (Exterior Substrate).
- **Documentation Hygiene:** Prevents Toron's ADR sequence from becoming a catch-all for storage-ring features, preserving the architectural separation of concerns.
