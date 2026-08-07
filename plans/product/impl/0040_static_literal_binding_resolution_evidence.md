# PRODUCT-0040 implementation evidence: static literal binding resolution

Status: accepted 2026-08-06.

Plan: `cd01c633` (`plans/product/adr/0040_static_literal_binding_resolution.md`).
Implementation: `20aac53e` (`feat: resolve static literal figure bindings`).

## Result

Figure presentation arguments now accept top-level names bound, directly or
through aliases, to source string or numeric literals. Resolution happens in
the relation envelope before dispatch; it performs no Rune evaluation and keeps
the original bindings in the program. The resolved value is passed through the
same validation as an inline literal, so positivity, ordering, applicability,
capacity, and malformed-value rules remain unchanged. Computed expressions,
cycles, wrong-kind bindings, and unbound names refuse at the authored argument
span with an attributed diagnostic. Mark arguments remain outside this cut.

## Acceptance gate ledger

| gate | evidence | result |
|---|---|---|
| 1. Shared headings in `test13.rn` | External UAT fixture shape; envelope accepts both figures' shared title/subtitle bindings | pass |
| 2. Every figure element accepts the correct bound kind | `product_0040_static_literal_bindings_feed_every_figure_element` covers title, subtitle, skin, palette, bounds, tick step, and minor ticks | pass |
| 3. Unary-negated numeric binding | Same envelope test resolves `let floor = -1` through `y_bounds` | pass |
| 4. Alias chains and cycles | Same test covers a chained title alias; resolver uses visited-name tracking for cycles | pass |
| 5. Most-recent preceding binding | Resolver selects `statement_start < use_start`, matching relation-origin lookup | pass |
| 6. Attributed refusal cases | Envelope diagnostics cover wrong kind and computed values; architecture coverage pins binding attribution and use-site spans | pass |
| 7. Existing value/order validation | Bound `-1` tick step retains PRODUCT-0038 positivity diagnostic; equal bound retains PRODUCT-0036 inversion diagnostic | pass |
| 8. Shadow-name constructor | `let title = "x"; figure([title(title)])` resolves by positional figure-element dispatch | pass |
| 9. Bindings retained | Resolver consumes initializer text without peeling `let` statements; architecture test asserts no peeling/evaluation path | pass |
| 10. Inline compatibility | Full Polariton envelope suite passes without changing inline literal behavior | pass |

## Verification

- `cargo fmt --all -- --check` — passed.
- `cargo test -q -p polariton --features toron` — 374 passed, 5 ignored.
- `cargo test -q -p polariton --test architecture_test` — 38 passed.
- `cargo test -q -p polariton product_0040` — 1 passed.
- `cargo test -q -p libaryon` — 1,437 passed, 22 ignored; auxiliary suites passed.
- `cargo check -q -p polariton --no-default-features` — passed (existing
  feature-gated unused warnings only).
- `cargo build -q -p polariton` — passed.
- `git diff --check` — passed.
