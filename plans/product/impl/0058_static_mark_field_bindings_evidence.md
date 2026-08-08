# PRODUCT-0058 implementation evidence: static mark-field bindings

Status: accepted 2026-08-08.

Plan: `f65f3a66`
(`plans/product/adr/0058_static_mark_field_bindings.md`). Implementation ships
as `impl: adr 0058 static mark-field bindings` together with this evidence.

## Result

Every established bar, line, and scatter field position now consumes
PRODUCT-0040's static string resolver. Legacy `plot.<mark>` calls, relation
sugar, canonical and named views, figure bodies, modified standalone marks,
and accepted line/scatter layer fields all lower bound names to the exact owned
strings used by inline literals. No binding identifier enters
`DisposableRunePlotSink`, `LayerSpec`, `AuthoredPlotSpec`, `ChannelBinding`, or
`ResolvedChannel`.

The phase boundary is observed in production completion. Bindings resolving to
`value`, `category`, and `series` become those exact authored and resolved
channels before PRODUCT-0055 selects `HorizontalBar`. A two-quantitative bound
pair reaches the existing complete-sink refusal naming concrete `revenue` and
`profit` fields and their semantic types, while the attributed source span
remains `bar(x_field, y_field)`.

`/home/me/work/uat/test16.rn` now binds `region`, `revenue`, and `product` once
and reuses the names in both bar figures. Its modifier values remain inline;
register-argument bindings are still the separate unrequested coverage item.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Inline/bound equality across every standalone position | `product_0058_bound_fields_equal_inline_in_every_standalone_position` crosses bar/line/scatter, two/three fields, legacy, sugar, canonical, named, figure, and each mark register; every field ordinal is bound | pass |
| 2. Layer fields without surface widening | `product_0058_bound_layer_fields_preserve_the_closed_layer_surface` proves equal line/scatter `LayerSpec`s and retains third-field, bar-layer, and bound numeric modifier refusals | pass |
| 3. Static strings precede bar orientation | `product_0058_bound_fields_resolve_before_bar_pair_and_channel_resolution` observes concrete authored/resolved fields and canonical `HorizontalBar`, equal to the inline control | pass |
| 4. Pair and field attribution | The same completion gate requires resolved `revenue`/`profit` semantic facts at the complete mark span; envelope refusal cases separately pin exact argument spans | pass |
| 5. Third-field channels and colors | `product_0058_bound_third_fields_share_inline_color_resolution_for_every_mark` proves exact resolved-spec, domain, and categorical-color equality for bar, line, and scatter | pass |
| 6. Register composition | Gate 1 covers all four accepted inline mark members; gate 2 proves a bound numeric modifier remains refused | pass |
| 7. PRODUCT-0040 lookup/refusal rules | `product_0058_lookup_retention_and_argument_refusals_follow_product_0040` covers aliases, preceding shadow selection, constructor collision, cycle, unbound/computed/arithmetic/numeric refusal, and argument spans | pass |
| 8. Binding retention and lifecycle | The lookup test observes the original `let` statements in transformed Rune; equality gates prove source factoring does not enter intent or identity | pass |
| 9. `test16.rn` factoring and zero pixels | The fixture contains one category/value/series binding set reused by both marks; completed bound and inline specs are exact equals and no render baseline changed | pass |
| 10. Inline compatibility and full suite | Existing inline tests pass; exactly two recognition/attribution premises moved to the bound-field rule and no SVG/draw/image premise changed | pass |
| 11. Documentation closure | README, corpus v0.58, language-as-built, deferred ledger/triage, ADR status, and this evidence close DB-03 while retaining register bindings | pass |

## Stage 0 finding and bounded premise updates

The arithmetic refusal gate exposed a PRODUCT-0040 decoder defect before the
surface widened: `decode_string_literal` stripped the first and last quote from
the complete source text, so `"x" + "z"` masqueraded as one string with the
interior text `x" + "z`. The repair rejects unescaped interior quotes. It
implements PRODUCT-0040's already accepted refusal of concatenation; it adds no
evaluation and changes no valid literal.

The first full Polariton run identified exactly two expected premise updates:

- a correctly shaped legacy `plot.scatter(relation, x, "y")` is no longer
  retained as ordinary malformed Rune; PRODUCT-0058 recognizes the established
  field slot and refuses the unbound `x` at its argument span;
- the layered `scatter(x, "y")` refusal moves from the complete layer call to
  the exact `x` argument.

Wrong arity remains ordinary in an unproven legacy root, and every existing
layer arity/mark/register boundary remains closed. No output premise or
baseline changed.

## Verification

- `cargo fmt --all -- --check` — passed.
- `cargo test -p polariton product_0058` — 5 passed.
- `cargo test -p polariton --no-fail-fast` — 478 passed, 5 live tests ignored;
  41 architecture tests and the trybuild pass/fail pair passed.
- `cargo test --workspace --all-targets --no-fail-fast` passed with no failures.
- `git diff --check` — passed.
