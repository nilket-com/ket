# ADR 553 - Typed scalar function registry

**Status:** Proposed 2026-08-15. **Implemented and measured 2026-08-17.** The
mechanism landed with `EXP` as its reference function; the following registrations
and the contract-preserving `FLOOR` migration measured it. §Forward records what
they cost.

## Context

At proposal time, Toron had no scalar-function mechanism. Every function was its
own `Expr` variant: `Floor`, `TimeBucket`, `AtTimeZone`, `Extract`, `DateTrunc`,
`Substring`. There was no `ScalarFunction { .. }`, so there was no such thing as
*registering* a function.

`FLOOR` was the proof rather than an exception. ADR 545 needed histogram
bucketing, and the answer was a sqlparser grammar form wired to a dedicated IR
variant. At proposal time there was no `ABS`, `ROUND`, `CEIL`, `MOD`, `SIGN`,
`EXP`, `POW`, `SQRT`, or `LN` anywhere in the SQL surface — not because they
were considered and declined, but because nothing had been willing to pay for
them.

**The cost is not in the evaluator.** Computing `exp` is trivial. The cost is in
every match that does *not* care which function it is: `Expr::is_pbc_pushdown_eligible`
names every function purely to answer "no", and the same shape recurs through
binding, type inference, and distributed fragmenting. Adding a variant also trips
the source-scanning architecture guards and requires an `--all-targets` pass,
because benches and examples match on the enum too. A one-line capability is a
planner-wide edit.

**This has already chosen an outcome once.** ADR 552 needed `exp`, a fifth-root,
and a way to generate evaluation rows. Three bespoke expression variants plus a
row-producing primitive, against one bespoke plan node — the node was genuinely
the smaller cut, and that record made the right call on the economics it faced.
Those economics will choose a special-purpose operator again, for the next
capability, and the one after that.

So the weakness is not a short function list. It is that **each function is an
architectural event**, and a surface priced that way stays small no matter who is
paying attention.

## Decision

### 1. One typed expression, with an enum identifier

`Expr::ScalarFunction { id, args }`, where `id` is an **enum**, not a string.

An enum keeps exhaustive matching available to the rules that genuinely care
about a specific function, while collapsing it for the rules that do not. A
string-keyed registry would trade the variant explosion for name resolution and
dynamic dispatch on the row path, which is a bad bargain in an engine whose perf
arc fights for exactly those margins.

### 2. The registry owns planner metadata, not only evaluation

Arity, argument and result types, null / non-finite / error behaviour,
determinism, pushdown eligibility, and evaluator selection are **registry data**.

This is the load-bearing half of the decision. A registry that owned evaluation
alone would leave every planner-wide match still naming functions one by one, and
function two would cost what function one did. What makes the mechanism pay is
that `is_pbc_pushdown_eligible` and its siblings become a single arm plus a
property lookup.

### 3. SQL and Rune bind to the same canonical identifier

One semantic definition, two spellings. A function that exists on one surface and
not the other is a defect rather than a feature — the divergence this arc has
already paid for elsewhere.

This does not oblige every capability to be a scalar. ADR 552's density is a
relation operator and stays Rune-only by its own reasoning; that is a decision
about what kind of thing it is, not about which surface may spell it.

### 4. Grandfathered variants are a closed, enumerated set

`TimeBucket`, `AtTimeZone`, `Extract`, `DateTrunc`, and `Substring` remain
expression variants, **by name**. `FLOOR` migrated to the registry on
2026-08-17, preserving its narrower integer-or-float domain and evaluation-time
non-finite rejection; the closed list therefore shrank from six to five. `Case`,
`IsNull`, and `IsNotNull` are control and predicate forms rather than functions
and are not registry candidates.

The list is names, never a predicate. "Temporal functions may remain variants" is
a loophole that readmits the pattern under a rule; a finite list of five either
shrinks over time or stays honestly five. Migration remains optional — the
remaining five work, and rewriting them buys nothing a user can see.

### 5. A guard that fails, or the mechanism is optional

An architecture control asserts the set of scalar-function `Expr` variants equals
the grandfathered list. A new one fails the build.

Without it, the registry is the thing people use when convenient, and the next
capability under deadline adds a variant exactly as every previous one did. A
foundation with no guard is a suggestion.

### 6. One reference function, not a shopping list

`exp` is in this record because the acceptance gates already depend on it: it is
the executable proof that registration, SQL, Rune, binding, metadata, and
evaluation share one canonical identifier. `pow`, `sqrt`, `abs`, and their
neighbours remain the following cut, and their cost is this record's success
measure rather than its content.

## Acceptance

- **Adding `exp` is a typed registration and an evaluator test** — no new `Expr`
  variant, and no edit to any planner-wide match. This is the record's point, and
  it is the control that would fail against today's engine.
- A match that previously named every function names **one arm** and consults the
  registry. `is_pbc_pushdown_eligible` is the concrete case.
- SQL `EXP(x)` and Rune `exp(x)` bind to the same identifier and produce the same
  bound plan, asserted by digest equivalence the way the grouped-quantile
  equivalence is asserted today.
- The guard refuses a seventh scalar-function variant: the control enumerates the
  variants and compares against the closed list, so adding one fails rather than
  passing unnoticed.
- Null and non-finite behaviour is read from the registry, with **no second table**
  in the executor. A duplicated behaviour table is the same two-definitions defect
  in a new place.

## Guardrails

The identifier is an enum. If it becomes a string, the mechanism has traded a
compile-time cost for a per-row one, and the trade should be re-argued rather
than discovered in a flame graph.

The grandfathered list shrinks or stays. Any change that lengthens it means the
guard was bypassed, and the bypass is the finding.

The registry prevents new variants. It does not prevent bad functions — admission
stays a review question, and a registry that makes adding a function free is not
a licence to add every function.

## Risks

The metadata set is a guess until the second and third functions land. If the
first handful all want a property the registry does not model, the shape is
wrong — which is why acceptance names `exp` specifically, so the shape is tested
on function two rather than function twenty.

Migrating grandfathered variants touches working code for no user-visible gain.
Left optional deliberately; the guard does not require it.

A mechanism that makes functions cheap invites a wider surface than anyone
decided to support. That is a governance cost this record accepts in exchange for
not accumulating special-purpose plan nodes.

## Forward

`exp`, `pow`, `sqrt`, `abs` and their neighbours are the following cut and the
measure of whether this worked.

**Measured 2026-08-17.** `SQRT`, `POW`, and `ABS` landed together and touched four
source files: the registry, the binder's signature module, the evaluator, and the
Rune surface. Nothing planner-wide changed — `sql_planner.rs`, `pbc.rs`,
`binder.rs`, `expression_walk.rs`, `outer_join_demotion.rs`, `ir.rs`, and
`ir/execution.rs` are all untouched by the three additions, and the `Expr` variant
count is unchanged at six grandfathered plus the generic one. Under the old
pattern each of the three would have been a variant and a planner-wide edit.

`SQRT` is the cheapest case and the clearest evidence: it reuses `EXP`'s type rule
verbatim, so it is a registration, an evaluator arm, and a Rune line.

**The metadata shape held, with two additions the record anticipated.** `POW` is
binary, which `ScalarFunctionArity::Exact` already expressed, and `ABS` preserves
integers and decimals rather than producing floats. Each needed a
`ScalarFunctionSignature` variant and a matching binder arm — one enum and one
`match` in a single file, which is what a type rule genuinely differing is
supposed to cost. No property had to be added to `ScalarFunctionMetadata` and no
consumer of it changed, so the Risks section's "the metadata set is a guess"
survived contact with function two.

Two decisions inside those rules are deliberately narrow and locked by tests that
fail against the alternative. `POW` refuses mixed float widths rather than
promoting, because promotion is a decision about the engine's coercion rules and
does not belong inside one function's binder arm. `ABS` reports overflow for the
most negative two's-complement value, matching the checked-arithmetic convention
the engine already uses, rather than wrapping and returning the negative input as
a magnitude.

**`LN` and `ROUND` followed, and `ROUND` is where the arity model moved.** `LN`
is the freebie case again — `EXP`'s rule, one registration, one evaluator arm.
`ROUND` has an optional digit count, which `ScalarFunctionArity::Exact` could not
express, so arity gained `Between(min, max)`. That is the first change to a
metadata *type* rather than an addition to the table, and it was confined to the
registry: no consumer of `arity` changed, because both the SQL lowering and the
binder already asked `accepts(count)` rather than reading a number.

The digit count is read per row rather than required to be a literal, so a column
of scales works. It is checked as an integer and deliberately excluded from the
output type — `ROUND(rating32, 1)` stays `Float32`, and a test fails if the count
ever widens the value.

**Rune spells the optional argument explicitly**, as `lag` already spells its
offset: SQL's `ROUND(value)` is `round(value, lit(0))`. That keeps one identifier
and one canonical spelling per function while leaving the defaulting to the SQL
surface, which is where the dialect convention lives. A digest test pins the two
forms to the same result.

**`CEIL`, `SIGN`, and `MOD` closed the named set, and two of them cost something
the mechanism could not absorb.**

`SIGN` reuses `ABS`'s rule outright, so a decimal's sign is that decimal's own
`1` rather than an integer. That is the Postgres reading and it keeps the rule
count from growing.

`CEIL` needed a **frontend grammar arm**. sqlparser gives `CEIL` and `FLOOR`
dedicated AST variants, so `CEIL(x)` never reaches the registry's function path —
`sql_planner.rs` gained one arm mapping the grammar form onto the registry
identifier. The registry cannot make a function reachable that the parser routes
elsewhere, and that limit belongs on the record: a bespoke grammar is a per-name
cost the mechanism does not remove.

**`FLOOR` then collected the back debt without moving its contract.** Before the
migration, tests characterized its direct-variant path: `Int32`, `Int64`,
`Float32`, and `Float64` are accepted and preserve type; `Decimal128` and named
logical types are rejected at bind time; NaN and infinity are rejected only at
evaluation. The migration carries that exact domain through
`UnaryIntegerOrFloatPreserving`, not the broader decimal-accepting numeric rule,
and makes `ScalarFunctionNonFiniteBehavior::Rejects` discriminate for the first
time. It remains evaluation-time behavior.

The direct `Expr::Floor` and `BoundExprNode::Floor` forms, their bespoke binder,
and their planner-wide arms are gone. SQL's dedicated sqlparser grammar form and
Rune's `floor` spelling both now lower to `ScalarFunctionId::Floor`; a digest test
asserts the shared bound plan and result. The grandfathered list shrank from six
to five, so the registry now demonstrates removal of pre-existing planner-wide
cost as well as avoidance of new cost.

`MOD` falsified an assumption this record made about naming. §3 says one semantic
definition, two spellings; the implementation had read that as *the same*
spelling, asserted as `sql_name.to_lowercase() == rune_name`. `mod` is a keyword
in Rune, so `mod(a, b)` cannot parse there at all. The Rune spelling is `modulo`,
the SQL spelling is `MOD`, the identifier is one, and the equality test became an
explicit exception list so a divergence stays visible instead of becoming a habit.
Its integer-only domain is deliberate: a floating remainder is its own admission
decision.

**`POW` was the wrong name, and finding that added the second field with a real
consumer.** ANSI spells it `POWER`; `POW` appears in no revision. It shipped here
first because the registration was written from Rust's `powf` rather than from
the standard, and nothing could catch it — the registry held one `sql_name` per
function and resolved it with a single comparison.

`sql_aliases` fixes that and is read immediately by name resolution, unlike the
still-deferred fallibility property. `POWER` is canonical, `POW` stays resolvable
so an existing query keeps working, and `CEILING` joins `CEIL` — both are standard
spellings of the ceiling function. Diagnostics name the canonical function
regardless of which spelling was typed, so an arity error on `POW(x)` reads
`POWER expects exactly two arguments`.

The two spellings of the ceiling function also travel different routes: `CEIL`
arrives through sqlparser's dedicated grammar form and `CEILING` as an ordinary
function call. A test asserts they converge on the same bound plan, because
"both resolve" is weaker than "both produce the same plan".

The Rune surface follows the canonical name — `pow` became `power` — rather than
taking a spelling exception. The exception list stays at one entry, `MOD`, where
the canonical name is unusable because it is a Rune keyword.

**The string family is where the metadata model stopped being numeric-shaped.**
`UPPER`, `LOWER`, `TRIM`, and `CHAR_LENGTH` landed together because they test one
thing as a group: until now every rule produced an output type drawn from an
input. `StringToInteger` is the first that does not — a string in, an `Int64`
out — and it needed no change to `ScalarFunctionMetadata`, only a rule.

`CHAR_LENGTH` returns `Int64` for both `Utf8` and `LargeUtf8` inputs, so the
result type does not leak how a column happens to be stored, and it counts
**characters**: `str::len` answers in bytes and agrees only for ASCII.
`OCTET_LENGTH` is the byte measure and is not admitted here. `UnaryStringPreserving`
keeps the input's offset width, so a `LargeUtf8` column is not narrowed by passing
through a function. `CHARACTER_LENGTH` is registered as an alias, which is the
alias mechanism earning its keep on a new function rather than only in retrofit.

`TRIM` needed a grammar arm like `CEIL` did, and its specification forms
(`TRIM(LEADING 'x' FROM s)`) are **rejected by name** rather than quietly treated
as plain `TRIM` — answering a different question than the one asked is worse than
refusing. The bare `TRIM(LEADING FROM s)` form does not parse in this dialect at
all, so the engine never sees it; a test pins that too, because "we reject it"
and "it never arrives" are different facts.

**Not yet paid for: a fallibility property.** `ABS` can fail at run time, and the
registry has no field saying so. Nothing consumes such a field today, so adding
one now would be speculative. It becomes real the moment a rule wants to know
whether a function may be evaluated speculatively — constant folding is the likely
first caller.

**Series and grid generation is explicitly not this record.** It produces rows, so
it is a cardinality and costing decision with its own benchmarks, not a scalar
registration.

**`stddev` and `variance` remain ADR 552's named follow-on.** A mergeable,
numerically stable variance across partitions is neither a scalar nor this
mechanism.

Had this existed, ADR 552 might have been a composition rather than a capability.
That is not a criticism of 552 — its single-place bandwidth guardrail is
independently right, and would have been worth keeping either way — but it is the
honest measure of what this mechanism buys.
