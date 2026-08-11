# PRODUCT-0076 implementation evidence: executable presentation values

Implemented 2026-08-11 against
`plans/product/adr/0076_executable_presentation_values_with_static_view_topology.md`.

## Outcome

Polariton now executes presentation values in a bounded private Rune VM. The
outer `.views([...])` array, its order, and literal names remain a static shell;
the expression in each slot may use ordinary bindings, arrays, arithmetic,
conditionals, and helpers. Relation source remains Rune text dispatched to
Toron. Presentation-only closures do not cross that boundary.

`bar(...).stacked().percent()` and every other accepted modifier are registered
associated functions over owned typed values. The old figure, mark, table,
layer, bounds, tick, skin, palette, legend, and facet AST semantic recognisers
were deleted in the same cut. `rune_view_compiler` owns only static topology,
occurrence dependencies, source partition, origin injection, and generated
source maps; `rune_presentation` is the one presentation semantic authority.

Presentation work runs in `spawn_blocking` under 256 MiB compile/runtime
allocation limits and a 50,000,000-instruction budget. It participates in the
existing per-buffer revision scheduler before any Toron dispatch. Restore still
publishes unresolved durable placeholders without compiling or executing the
buffer.

## Acceptance gates

| gate | evidence | result |
|---|---|---|
| 1. Computed `yb` | `product_0076_test21_computed_bounds_compile_as_twelve_static_views` compiles the repository-owned `uat/test21.rn`, observes twelve static identities and one relation execution, and proves `y_bounds` is absent from Toron source. The banked Stage-0 test retains the old specific refusal | pass |
| 2. Complete helper surface | `product_0076_helpers_cover_the_complete_presentation_vocabulary` drives every figure furniture member, table setting, four mark constructors, layer members, and every accepted modifier through two helper calls, then compares all decoded intents with the inline control. The lower-level typed-value test covers computed arrays | pass |
| 3. Real modifier dispatch | The typed module registers raw associated functions with `build_associated::<RuneMark>()`; duplicate, order, applicability, family, and normalization behavior remain in the inherited 0064/0072 corpus. `product_0076_presentation_has_one_typed_semantic_authority` rejects retired semantic recognisers and modifier-name interpretation in the compiler | pass |
| 4. Helper attribution | `product_0076_helper_failures_keep_the_helper_body_origin` calls invalid bounds through three helper levels and pins the exact original `y_bounds(9, 2)` slice | pass |
| 5. Later/conflicting attribution | `product_0076_duplicate_and_applicability_fail_at_injected_origins` pins the later title and offending cross-register modifier. The complete inherited furniture/register refusal matrix remains green through the VM | pass |
| 6. Slot/compiler/source-map attribution | `product_0076_allocation_budget_and_wrong_slot_are_bounded_and_attributed` pins a wrong runtime value to its whole slot; `product_0076_compile_diagnostics_map_copied_text_to_the_exact_argument` pins copied helper text; `product_0076_source_map_distinguishes_copied_and_synthetic_text` distinguishes copied, synthetic, overlapping, and fallback mappings | pass |
| 7. Resolution phase remains downstream | The unchanged 0055/0058 GUI and envelope matrices prove executed strings reach field, dtype, and pair resolution with existing spans. The whole-struct inline/bound equality gate still proves binding/origin identity never enters authored or resolved specs | pass |
| 8. Lowering refuses before Toron | Compiler/VM failures return before a `TransformedRuneView` can enter the relation scheduler. GUI refusal gates pin relation `submitted == 0`; the established schema refusals still dispatch their required relation | pass |
| 9. Four partition classes | `product_0076_partition_separates_vocabularies_and_keeps_literal_dual_use`, `...computed_dual_use_and_mixed_helpers...`, and `...unclaimed_effects...` prove disjoint vocabulary, literal-only sharing, pre-execution mixed refusal, and relation-only retention of unclaimed effects | pass |
| 10. Occurrence shadowing | `product_0076_partition_tracks_shadowed_occurrences_by_position` uses three definitions of one name and proves each destination gets its resolved occurrence, while the unclaimed earlier occurrence remains only relation-side | pass |
| 11. VM context independence | `product_0076_context_divergence_executes_locally_but_never_dual_replays` executes `std::char::from_i64` in the presentation context and refuses its dual-use closure before replay. The mixed `col`/`y_bounds` fixture names the indivisible class | pass |
| 12. Static topology | `product_0076_topology_remains_literal_and_names_remain_static` refuses a helper-produced list, conditional list, loop-produced list, bound name, and conditional name, while the computed-slot control preserves static named and anonymous order | pass |
| 13. One evaluator for every spelling | The complete pre-record compatibility corpus—legacy roots, relation sugar, canonical/named views, figures, layers, tables, and modified marks—passes through `compile`. The architecture gate proves no production per-element AST parser remains | pass |
| 14. Resource bounds | Tiny instruction and allocation limits stop an infinite loop and compiler construction without using production-sized resources. The same tests pin all three production constants | pass |
| 15. Revision scheduling | `product_0076_presentation_scheduler_coalesces_before_invocation` proves the older request never invokes Rune; `product_0076_in_flight_presentation_completion_cannot_publish_after_supersession` proves an already-running older result cannot dispatch. Both leave the newest revision current and no stuck entry | pass |
| 16. Durable identities and bytes | Existing 0061/0062/0070 persistence gates pass unchanged. The static shell remains the census authority; no persistence file, owner schema, allocator, panel/group mapping, or colour-lineage code changed | pass |
| 17. Restore does not execute | Existing saved-process and restart gates pass unchanged: named and anonymous placeholders restore unresolved and resolve in place only after an explicit rerun | pass |
| 18. Full verification | Polariton behavior and architecture suites, all targets/features, workspace suite, PRODUCT-0057 Vulkan evidence, persistence/process, export, formatting, and diff checks are listed below | pass |

## Implementation findings

### Fixed-arity native registration would have bypassed typed refusals

The first native module used fixed-signature functions. Rune rejected a wrong
argument count before the typed constructor ran, so the error lost the authored
origin and inherited diagnostic wording. Constructors and modifiers now use
raw variadic registrations: the hidden origin is decoded first and the typed
value owns arity, type, duplicate, and applicability refusals.

### One failed modifier could be overwritten by a later modifier

Modifier chains initially continued mutating a failed `RuneMark`; a later
modifier could replace the first failure and its origin. The carrier now keeps
the first failure while still accepting any chain order on a valid mark.

### Helpers require per-slot lexical environments

Hoisting one global presentation closure changed most-recent-preceding
shadowing when two slots resolved the same name at different source positions.
The generated program now gives every static slot its own lexical block and
copies only that slot's position-resolved dependency closure.

### Presentation-only literals were still leaking to Toron

The initial partition treated every statically decoded literal as shared. A
three-occurrence shadowing gate showed that a presentation-only `revenue`
binding remained in relation source. `Neutral` now means a literal occurrence
actually reached by both closures; a presentation-only literal is removed from
Toron, while an unclaimed legacy statement remains relation-only as promised.

### Unclaimed calls needed classification before compatibility retention

Retaining every unclaimed statement would send `layers(...)` or another local
presentation call to Toron. Deleting every unclaimed statement would erase
possible relation-side effects. The compiler first claims any occurrence which
reaches presentation vocabulary; only genuinely unclaimed occurrences take
the compatibility relation path.

### Linker diagnostics expose real spans

Rune linker failures are not ordinary compile errors, but `MissingFunction`
does carry spans. Mapping that span rather than the generated-program fallback
keeps undefined helpers attributed to the authored identifier.

### The inherited architecture suite encoded the retired implementation

Thirteen accepted architecture tests searched for `parse_figure`, modifier
string matches, or the old literal resolver. The behavior they protected still
stood, but their instrument licensed the parser PRODUCT-0076 deletes. Each was
redirected to the typed associated-function, figure-decoder, or static-neutral
authority that now owns the same boundary. PRODUCT-0003 now names both
`rune_relation_envelope` and `rune_view_compiler` as the split transport
boundary while continuing to forbid Toron plans elsewhere.

### The original showcase gate depended on an unversioned sibling directory

The first implementation compiled `../../../../uat/test21.rn`, which resolved
to `/home/me/work/uat/test21.rn` outside this repository. It proved the local
exhibit but made a fresh clone and CI depend on an untracked file. The showcase
and its idempotent four-quarter seed now live together under repository-owned
`uat/`; the compiler gate points there, and an architecture guard rejects a
return to the sibling path.

The production-window exhibit rendered all twelve views with the shared
computed bounds. The expanded seed preserved unfaceted totals while giving the
four facet quarters distinct silhouettes. Contact also exposed a separate
pre-existing facet-gutter defect: adjacent percent tick labels overlap at an
inner column boundary. That finding is not attributed to PRODUCT-0076.

## Verification

```text
cargo test -p polariton --bin polariton
  pass (565 passed; 5 pre-existing live-Toron tests ignored)
cargo test -p polariton --test architecture_test
  pass (51 passed; 0 ignored)
cargo test -p polariton product_0076
  pass (23 unit + 2 architecture; 0 ignored)
cargo check -p polariton --all-targets --all-features
  pass, no warnings
cargo check -p polariton --no-default-features --all-targets
  pass (five pre-existing feature-substrate warnings across bin and tests)
cargo test -p polariton --all-targets --all-features -- --test-threads=1
  pass (behavior, architecture, trybuild, discovery/process, and Vulkan targets)
cargo test -p polariton --test product_0057_render_evidence \
  --features product-0057-render-evidence -- --test-threads=1
  pass (Intel UHD 770, Vulkan; all backend and strata probes)
cargo test -p polariton --bin polariton adapters::toron::tests:: \
  -- --ignored --test-threads=1
  pass (5/5 after seeding the named test2 dogfood fixture through nilket 0544)
cargo test --workspace --all-targets --all-features --quiet \
  -- --test-threads=1
  pass
cargo fmt --all -- --check
  pass
git diff --check
  pass
```

The five live-Toron tests remain annotated `#[ignore]` because the ordinary
suite cannot create or assume their external endpoint and dogfood relations.
They were also run explicitly in this implementation and all five passed. No
new ignored test or tolerated failure was added.

The real repository-owned `uat/test21.rn` source is compiled directly by gate
1, including its computed shared `yb`. The post-build production-window capture
rendered all twelve panels and remains contact evidence rather than a hidden
automated claim.
