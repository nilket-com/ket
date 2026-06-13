# Convention — Module decomposition (move-only)

How to break a multi-thousand-line module into a directory of cohesive
submodules **without changing behavior**. Codifies the ADR 176 (Toron binder)
and ADR 077 (Wendelon data server) carves so the next one is mechanical.

## The discipline: move-only

Every edit in a decomposition cut is one of:

- a code **move** (a method, type, or test block relocated verbatim),
- an **import-path** fix (`use super::*;`, a parent `use child::{…}`),
- a **visibility** change (`pub(super)` on a moved item, field, or method;
  `pub use` to re-export a type referenced outside the module), or
- a **guard retarget** (a source-scan that read the flat file now reads the
  concatenation helper).

It is **never** a logic change. The proof of neutrality is the full behavior
suite passing with no assertion/behavior rewrites, plus the structural guards.

## Layout (Rust 2018, mod.rs-free)

`mod.rs` files are manifests-only in this workspace (an architecture guard
enforces it). So the module root stays a **sibling file**, not `mod.rs`:

```
network/data_server.rs          # root: struct + construction + dispatch + shared helpers
network/data_server/
    counter.rs                  # a cohesive domain family
    table_query.rs
    write_replication.rs
    descriptor_migration.rs
    streaming.rs
    replica_routing.rs
    tests.rs                    # the extracted #[cfg(test)] block
```

The public path (`wendelon::network::data_server::…`) is preserved.

## Visibility — the one rule that bites

Inherent-method/type privacy is "visible in the defining module **and its
descendants**." This asymmetry drives the whole mechanical loop:

- **Child → parent (free):** a handler moved into `counter.rs` can still call a
  *private* helper defined in the root impl — children see parent-module
  privates. This is why thin handlers extract cleanly while their shared helpers
  stay in the root untouched.
- **Parent → child (needs `pub(super)`):** once a handler/type/field moves into a
  child module, the root (and sibling modules) can no longer see it. Mark moved
  handlers, the types they own, **and any field the parent constructs or reads**,
  `pub(super)`. Add a parent `use child::{…}` for types the parent names.
- **External refs (needs `pub use`):** a type referenced outside the module
  (e.g. `ReplicaRouter`) is re-exported from the root with `pub use child::Type;`
  so its external path is unchanged.

## The extraction loop (per family)

1. Move the family (handlers + the types they own + their private helpers) into
   `family.rs`, headed `use super::*;`.
2. Declare `mod family;` in the root; add `use family::{…}` / `pub use …` for
   names the root or the world references.
3. `pub(super)` the moved items, their fields, and the inherent-impl methods the
   parent still calls.
4. **Retarget guards** that region-extract a handler by "next handler" end
   markers — a marker that crossed a module boundary must point at a handler
   still in the same file (ADR 077 retargeted the counter-region end marker).
5. `cargo check --all-targets` (benches/examples construct the struct too), then
   the full architecture binary.

## Guards a decomposition ADR lands

- `*_source_concatenates_submodules` — a `family_source_files()` helper returns
  the root + every **production** submodule (tests excluded), and every prior
  source-scan guard reads that concatenation, not the flat file. Pins the file
  manifest so adding a module is deliberate.
- `*_semantic_modules_exist` — each named module present and declared.
- `*_dispatch_and_struct_stay_in_root` — struct def + dispatch in the root;
  carved handler *definitions* are not (call sites legitimately remain).
- `*_root_is_bounded` — a line cap on the root, set from the post-carve size +
  modest headroom. **Ratchet it DOWN** as later cuts relocate domain helpers; it
  exists to forbid regrowth toward the monolith.
- `*_tests_extracted` — the `#[cfg(test)]` block lives in `tests.rs`.
- `*_no_wire_or_runtime_surface_change` — dirty-tree scan: while the cut is in
  flight, no production source outside the module directory is dirty. A
  development-time tripwire (clean after commit, like ADR 076).

## What this carve does NOT do (the honest boundary)

A handler-family carve relocates the **handler entry points, their owned types,
and the test block**. The thick **shared/domain helper methods** (audit
emission, encoding, construction, and domain helpers called across families)
stay on the root impl — moving genuinely-shared helpers into one domain module
is not move-only and creates a cross-module visibility web. So the root after a
first carve is smaller but not minimal (ADR 077: 30,602 → ~15,000). Driving the
root toward a small dispatch-and-struct core is a **follow-on**: triage each
remaining method as domain-private (relocate) vs cross-cutting (keep/extract to a
shared module), and ratchet `*_root_is_bounded` down as you go. Name this
follow-on in the ADR rather than pretending the first carve reaches the floor.
