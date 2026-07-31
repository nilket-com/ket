# Capability vs feature naming

In Toron code — types, fields, methods, identifiers — use
**capability** for a declared, gated, or negotiated ability.
Reserve **feature** for casual prose: marketing copy, blog posts,
informal documentation.

## Rule

- Code: `CapabilityCategory`, `CapabilityDenied`,
  `classify_denied_capability`, and any future declared-ability
  surface.
- Casual prose: "Toron's runtime-filter feature lets the planner
  probe rows before they hit the wire" — fine in a README intro.
- Don't mix within a single file or section. Pick the register
  and stay there.

## Why

"Capability" is the precise term for a declared, gated ability.
"Feature" is a fuzzier marketing word that mixes
"thing-the-product-does" with "ability-the-system-grants." When
code uses both interchangeably, the resulting type names blur the
distinction the spec text relies on.

Toron's load-bearing capability surface today is the Rune sandbox
(ADR 007): the sandbox is constructed as an **allow-list**, and
each grantable ability is a `CapabilityCategory` —
`Filesystem`, `Process`, `Network`, `Environment`, `Time`,
`Random`, `Threading`, `Unknown`. A denied ability surfaces as
`RuneSandboxViolation::CapabilityDenied { capability }`. Calling
any of that "feature" vocabulary would erase the precise
allow-list/grant semantics ADR 007 pinned.

Inconsistent vocabulary compounds: every new field, method, or
doc section that picks the wrong word adds another correction the
next collaborator has to make.

## How to apply

- New types and fields: prefer "capability" if the thing being
  named is a declared, gated, or negotiated ability. If it is a
  user-visible product surface ("the sort feature"), "feature" is
  fine in the doc — but the underlying type still uses
  "capability" if it is a gated grant.
- The Rune sandbox surface uses `CapabilityCategory`,
  `CapabilityDenied`, and `classify_denied_capability`. New code
  adding a grantable ability or classifying a denied one should
  reuse this vocabulary, not introduce a parallel "feature"
  spelling.
- If Toron later grows a node- or worker-level negotiated-ability
  surface, it inherits the same rule: declared/negotiated
  abilities are capabilities in code, whatever the marketing
  prose calls them.

## How to apply in review

When a diff introduces a type or field named `*Feature` for
something that is actually a gated or declared ability, push back
and ask for "capability." When it correctly uses "capability" for
a gated ability, note it positively — the norm is precise
vocabulary, and reinforcing it is cheaper than correcting drift
later.

## See also

- [no-project-name-in-protocols.md](no-project-name-in-protocols.md)
  — same discipline applied to a different vocabulary axis (brand
  vs. protocol).
