# Wendelon Philosophy: The Principle of Unification

## The Unified Ring

Wendelon rejects the traditional bifurcated architecture of "Control Planes" and "Data Planes." Such divisions introduce artificial boundaries, complex failure modes, and cognitive overhead.

In this project, there is only the **Unified Ring**. Every node is a peer on this ring, and every record exists at a specific coordinate within it.

### The Identity vs. Locality Invariant

To maintain a single ring while supporting data-warehouse scales, we distinguish between a record's **Identity** and its **Locality**:

1. **Identity (High Entropy):** The "True Name" of a record, represented by a 256-bit hash (BLAKE3). At Wendelon's target scale, collision risk is negligible enough that the storage engine can treat the full Identity as the authoritative record coordinate.
2. **Locality (64-bit Projection):** The "Address" of a record on the Ring. This is a 64-bit projection obtained via `Locality::from(identity)` and used for routing and placement.

**The Unification Rule:** A routing collision (two distinct Identities projecting to the same 64-bit Locality) is harmless. It simply means two records share a physical home on the ring. The system logic remains unified; only the precision of the view changes depending on whether we are **routing** (Locality) or **retrieving** (Identity).

## Linguistic Forbidden Zones

The words **"Control Plane"** and **"Data Plane"** are strictly forbidden in this codebase.

- Using these terms suggests a mental model of separation that we have intentionally discarded.
- This is enforced by architectural tests (see `tests/architecture_test.rs`).
- Violation of this nomenclature is considered an architectural regression.

## Implications for implementation

- **No Split State:** Metadata and payload data are both managed by the ring, even if they follow different durability paths (e.g., the "Stellarator" bifurcated write model).
- **Single Topology:** There is one membership set and one epoch-transition mechanism that governs the entire node.
- **Unified Scaling:** Scaling the ring scales both the coordination capacity and the storage capacity simultaneously.
