# ADR 0076: executable presentation values with static view topology

Status: proposed 2026-08-11. This record replaces Polariton's per-element
source recognisers with a bounded local Rune evaluation for presentation
values while keeping the number, order, names, and durable identities of views
source-static. It makes bindings such as `let yb = y_bounds(0, 160)` and helper
functions ordinary Rune without sending presentation vocabulary to Toron or
reopening the persistence model. Relation execution, schema resolution, and
restore-time non-execution remain unchanged.

## Context

The authored plotting surface has outgrown the mechanism that recognises it.
The smallest current failure is not a missing plot feature:

```rune
let yb = y_bounds(0, 160);

workspace
    .table("demo_sales")
    .views([
        figure([
            title("Grouped bars"),
            yb,
            bar("region", "revenue", "product"),
        ]),
    ])
```

`/home/me/work/uat/test21.rn` refuses at `yb` because `parse_figure` requires
each child to be an AST call whose callee Polariton recognises. The value is
valid Rune and has exactly the native type the figure needs; the envelope never
lets Rune produce it.

The same split is visible in mark modifiers. `.stacked()` and `.percent()` look
like method calls but `parse_modified_mark` walks their syntax, compares method
names as strings, and mutates `AuthoredPlotSpec` itself. Rune does not dispatch
either method. Every new presentation value therefore needs another syntactic
case, another literal decoder, and another manual implementation of evaluation
semantics. Factoring a value is enough to expose the boundary.

PRODUCT-0040 and PRODUCT-0058 deliberately stopped before evaluation. Their
static resolver handles literal bindings and aliases, and their concrete-value
normal form remains correct, but the coverage rule no longer matches the
language authors see. `table("demo_sales").sort(...)` already remains Rune
source and executes in Toron; presentation calls merely imitate execution in a
different parser.

Stage 0 established that Polariton is already source-in/source-out at the
relation boundary. `transform` emits a rewritten Rune program containing the
retained statements and relation expression, and Toron executes that text. A
second serialized plan format is not required to preserve pushdown. What is
missing is a local execution of the presentation half.

Stage 0 also established four constraints.

First, the current renderer and persistence model depend on static view
topology:

- `.views([...])` supplies a source-visible output count and order;
- `LocalViewIdentity::Anonymous(output_order)` uses that ordinal;
- named identities use a source-static string;
- PRODUCT-0061's coupled witness relies on append-only panel meaning; and
- PRODUCT-0063 restores unresolved panels without executing user code.

Executing a runtime-produced view list would therefore be a persistence and
identity redesign, not a larger form of the same evaluator.

Second, source partition is unavoidable. `retained_regions` currently copies
every statement not peeled as a presentation root. A presentation-only binding
such as `yb` cannot be sent to Toron, whose Rune vocabulary does not contain
`y_bounds`. A relation binding must still reach Toron. A literal may be used by
both. The partition must follow lexical binding occurrences and their
dependencies rather than copy all surviving text.

Third, exact source attribution is accepted product behaviour. Presentation
refusals use `RuneEnvelopeError::InvalidView { span, message }`; field,
modifier, duplicate, and complete-sink gates depend on different spans. An
executed `FigureElement` carries no useful origin unless Polariton deliberately
adds one.

Rune 0.14 native functions cannot discover their caller. `Module::function`
and `raw_function` pass only registered arguments. The host can map a VM
instruction pointer through `DebugInfo`, but a native constructor has no
caller context from which to obtain that pointer. The transformer must inject
the original origin at each constructor and modifier call.

Fourth, local execution can be bounded with existing substrate. Rune 0.14
provides `runtime::budget::with` for per-instruction limits and `alloc::limit`
for allocation limits. Polariton's startup VM already uses both, and ordinary
Rune buffers already execute inside `tokio::task::spawn_blocking`. Presentation
evaluation can therefore remain off the UI thread and fail finitely before any
Toron request.

The context constructors are not equivalent. `Context::with_config(false)`
does **not** disable Rune's standard modules; its boolean disables low-level
stdio while installing Rune's default module set. Toron's authoring context
instead starts empty and installs an explicit smaller list plus its relation
module. The two VMs therefore cannot treat “mentions neither product
vocabulary” as proof that a helper can compile or mean the same thing in both.

## Decision

### 1. The view set is static; each view value may execute

The outer output shell remains source-structural. `.views` accepts a literal
array with source-visible length and order:

```rune
relation.views([
    computed_figure(),
    view("detail", another_computed_figure()),
])
```

The expression in each slot executes locally and may use bindings, blocks,
conditionals, helper functions, arrays of figure elements, and ordinary Rune
arithmetic. The array that contains the slots does not execute as a runtime
collection. Its source order continues to assign anonymous ordinals.

A named wrapper keeps a literal name in the static shell while its body may
execute. A bound or computed name refuses before either VM runs. This preserves
the durable key `(DocumentId, LocalViewIdentity)` and prevents data-dependent
identity.

Legacy `plot.<mark>(relation, ...)`, relation-sugar `<relation>.<mark>(...)`,
and a terminal table remain accepted. The structural analyser normalizes each
accepted legacy root into one anonymous static slot and separates its relation
receiver from its presentation expression. It does not interpret that
expression's constructors or modifiers.

The shell may establish that a slot exists, where its relation comes from, and
whether its identity is named or anonymous. It may not decide the slot's title,
skin, channels, mark member, normalization, or other presentation meaning.

These remain refused in this record:

```rune
relation.views(build_views())
relation.views(rows.map(|row| view(row.name, make_figure(row))))
relation.views([view(compute_name(), make_figure())])
```

They are valid candidates for a future runtime-topology record only after
identity, coupled witnesses, reconciliation, and restore-time intent have new
answers.

### 2. Polariton executes a typed presentation vocabulary locally

Polariton registers owned native Rune values for its existing presentation
surface: table and figure elements, figure composition, plot bodies, layers,
mark registers, bounds, tick settings, skins, palettes, legends, facets, and
the modifiers already accepted by their records.

Constructors return typed values. Modifiers are real associated functions over
those values. For example, `bar(...)` returns the authored Bar-family value;
`.stacked()` and `.percent()` dispatch through that value's registered methods;
`figure([...])` consumes `Vec<FigureElement>` and produces one presentation
intent. Runtime type and member lookup replace callee-name and modifier-string
comparison.

The local context is `Context::with_config(false)` plus one private Polariton
presentation module. Authors therefore receive Rune 0.14's default modules
with stdio disabled, together with the typed constructors and modifiers in
this record. The context does not mirror Toron's allow profile, and Toron's
module list is not imported as a Polariton build-time contract. A Rune upgrade
that changes the default module set requires the same host-surface audit as it
does for Polariton's existing local Rune host.

The local context contains no database connection, relation data, Toron
client, editor handle, panel registry, session store, mutable `AppState`, or
Toron relation module. It cannot query a table or inspect a schema. All values
crossing back are owned authored intent plus original source origins.

The generated presentation entrypoint evaluates exactly one expression per
static slot and returns exactly one value for each. Evaluation is atomic for
the view set: if compilation, runtime execution, decoding, or validation fails,
no slot is published and no relation is dispatched.

This record does not make every arbitrary Rune value presentable. A static slot
must complete as one accepted table, plot, figure, or layered intent. A string,
number, relation handle, runtime list of views, or other value refuses at that
slot's source span.

### 3. Source is partitioned by lexical binding occurrence

The transformer builds a dependency graph over top-level statement and item
occurrences. A node is identified by source position plus its bound name, not
by name alone. Every reference resolves to the most recent preceding lexical
definition that is visible at that occurrence, preserving PRODUCT-0040 and
PRODUCT-0058 shadowing.

Each occurrence closure is classified:

| class | meaning | emitted destination |
|---|---|---|
| `RelationOnly` | reaches a relation receiver or relation expression | Toron program only |
| `PresentationOnly` | reaches a static slot's presentation expression | local program only |
| `Neutral` | proven replay-safe and needed by both halves | both programs |
| `Mixed` | one indivisible occurrence depends on both vocabularies | refusal |

Classification follows resolved occurrences transitively through aliases and
helper bodies. It does not infer from spelling alone. Shadowed definitions with
the same name are independent nodes.

In v1, the only `Neutral` closure is the existing static literal grammar from
PRODUCT-0040 and PRODUCT-0058: string and numeric literals, one accepted unary
numeric negation, and aliases composed solely from those values. That grammar
is source-known, deterministic, side-effect-free, and already proven to have
identical meaning without evaluation. Apart from the named unary-negation
form, a duplicable closure contains no call, method call, operator, macro,
indexing, field access, interpolation, collection construction, or block
evaluation. “Neutral” means **literal-valued and statically decoded**, not
“called no Polariton or Toron function.”

A computed helper may be relation-only or presentation-only. It is not replayed
in both VMs merely because its syntax appears vocabulary-free. Time, randomness,
I/O, mutable state, or future native functions could make two evaluations
diverge. A computed value used by both halves therefore refuses in v1. Widening
`Neutral` requires a separately specified purity and determinism contract.

The refusal also keeps the two module sets independent. A function call that
reaches neither product vocabulary may execute when used by only one half, if
that half provides it. If the value is needed by both halves, partition refuses
before either compilation rather than discovering later that one context lacks
the function. Polariton does not compute an intersection of installed Rune
modules and does not track Toron module drift.

A helper whose closure uses both relation and presentation vocabularies also
refuses at partition time. For example, a function that calls both `col()` and
`y_bounds()` cannot be emitted as one meaningful function into either VM. The
diagnostic names the helper and the two use closures; Polariton does not emit
two subtly different programs and hope their meanings agree.

Within each destination, retained definitions keep original source order. A
dependency cannot be reordered across a shadowing boundary to make the
partition easier.

An otherwise unclaimed top-level occurrence remains in the Toron program as a
compatibility-retained `RelationOnly` occurrence. The current transformer sends
every non-peeled region to Toron, and absence from both dependency closures does
not prove that an executable statement is free of effects. Such an occurrence
is never duplicated locally. If its own closure reaches presentation
vocabulary, it is no longer unclaimed and must classify as `PresentationOnly`
or `Mixed`; the compatibility rule cannot be used to leak `y_bounds`, `bar`, or
another local native into Toron.

### 4. Relation pushdown remains source text

For every relation root, Polariton continues to emit the Rune source Toron
already executes. The program contains the `RelationOnly` closure, any allowed
`Neutral` occurrences, and the relation expression. Presentation-only
definitions and the terminal presentation sink are absent.

Polariton does not interpret `.sort`, `.filter`, `.group`, `.limit`, `col`, or
another relation verb. It does not serialize a relation plan and does not ask
Toron to accept Polariton's presentation types. Existing relation-origin
grouping and source maps remain the transport.

The local presentation program contains the `PresentationOnly` closure, any
allowed `Neutral` occurrences, injected origin metadata, and a generated
entrypoint for the static slots. It does not execute the `.views` receiver or
construct a local relation proxy. The structural shell already associates each
slot with its relation origin; the two results join by the source-static slot
inventory.

If a sound partition later requires understanding relation semantics rather
than lexical dependencies, the escalation is an opaque relation-expression
proxy or a shared plan protocol. Neither is introduced speculatively here.

### 5. Every presentation constructor and modifier carries an original origin

Before compiling the local program, the transformer rewrites every accepted
presentation constructor and modifier call at its syntactic location to pass a
hidden origin token. Conceptually:

```rune
y_bounds(0, 160)
    -> __p0076_y_bounds(0, 160, __p0076_origin(438, 455))

bar("region", "revenue").bar_width(0.88)
    -> __p0076_bar("region", "revenue", origin_of_bar)
         .__p0076_bar_width(0.88, origin_of_modifier)
```

The concrete generated names are per-source reserved names selected after
scanning identifiers. User source cannot forge, shadow, or call the internal
constructors or manufacture an origin. A collision causes regeneration or a
structural refusal before compilation; it never makes user text privileged.

Constructor routing preserves the accepted positional namespace. The
structural pass rewrites a constructor callee to its qualified private native
function before Rune resolves ordinary value bindings, while leaving argument
references alone. Thus PRODUCT-0040's `let title = "x";
figure([title(title), ...])` and PRODUCT-0058's `let bar = "region";
bar(bar, "revenue")` remain valid: callee position selects presentation
vocabulary and argument position selects the lexical binding. The rewrite
chooses a native function; it does not validate that function's arity, value,
register, or applicability. Those semantics belong to the invoked type.

Each resulting native value is `Spanned<T>` or an equivalent wrapper carrying
the original buffer range. A helper therefore attributes where the value was
constructed in the helper body, even when called through several levels. That
is more useful than a native call-site span, which would identify only the
outer `figure([yb, ...])` slot, and Rune 0.14 does not expose that caller context
to native functions in any case.

The generated-program source map gains two segment kinds:

- **copied** segments map offsets linearly to retained original text, as the
  relation source map does today; and
- **synthetic** segments map the complete generated fragment to the original
  call or modifier range that caused it.

Compile and VM diagnostics in copied text map precisely. A diagnostic in a
generated wrapper or injected argument maps to its synthetic owner rather than
to invented source. Runtime validation normally uses the origin already
carried by the value; the source map is the fallback for compiler and VM errors
that occur before a value exists.

Failure attribution is fixed by class:

| failure | controlling original span |
|---|---|
| constructor argument or constructor-local invariant | that constructor call, or its exact offending argument when retained |
| duplicate/conflicting figure element | the later element |
| mark-family/register applicability | the offending modifier call |
| value produced in a helper | the constructor or modifier in the helper body |
| wrong value returned for a static slot | the complete slot expression |
| missing field, semantic type, bar pair, or joint schema rule | the complete plot body under its existing downstream rule |
| generated wrapper/compiler failure with no narrower owner | the static slot expression |

No diagnostic falls back to the whole buffer merely because presentation now
executes.

### 6. Lowering and resolution remain separate phases

Local evaluation replaces AST recognition only at presentation lowering. It
does not move schema-dependent decisions earlier.

The execution order is:

```text
1. parse the source and establish the static output shell
2. resolve occurrence dependencies and partition the two programs
3. compile and execute the bounded local presentation program
4. decode and validate the complete authored intents
5. dispatch each relation program to Toron
6. resolve returned schema, channels, bar orientation/pairs, domains, colours,
   guides, facets, geometry, and rendering
```

Wrong topology, an unpartitionable binding, a Rune compile/runtime failure, an
invalid constructor value, duplicate figure furniture, a forbidden modifier,
or an invalid local composition refuses before step 5 and produces zero Toron
requests.

A field name is only a string at step 4. Missing columns, semantic dtype
compatibility, categorical capacity, bar orientation, joint pair rules, and
other returned-relation facts remain step-6 refusals after exactly the relation
work they require. PRODUCT-0055's complete-body attribution and every
resolution-time diagnostic remain consumers of the executed authored intent;
they are not reimplemented in the VM.

### 7. Local execution is bounded and participates in revision scheduling

Presentation compilation and execution run in `spawn_blocking`, never on the
iced update thread. Compilation uses a named allocation cap; execution uses a
named allocation cap and Rune instruction budget. Their initial production
values are explicit and match Polariton's already-shipped startup VM limits:

```text
PRESENTATION_COMPILE_ALLOCATION_CAP = 256 MiB
PRESENTATION_RUNTIME_ALLOCATION_CAP = 256 MiB
PRESENTATION_INSTRUCTION_BUDGET     = 50,000,000
```

Changing one is a measured resource-policy change, not an incidental tuning
inside a plot record. Tests invoke the same helpers with deliberately tiny
limits rather than allocating hundreds of megabytes or waiting for the
production instruction count.

Native presentation constructors perform only bounded validation and owned
value construction. A future native function that loops, blocks, performs I/O,
or allocates outside Rune's accounted allocator is not admitted merely because
the VM has an instruction budget; Rune's own budget contract requires native
cooperation.

The pre-dispatch phase joins `RuneViewScheduler` rather than running as an
untracked task. A newer revision may supersede an older revision before its
local VM starts. If an older local VM is already executing, its fixed budget
may run to completion, but a superseded completion cannot dispatch Toron or
publish presentation. The scheduler then starts only the newest retained
revision. A bounded VM failure terminates that revision and releases the
scheduler just like a pre-dispatch envelope refusal.

This is finite supersession, not cooperative cancellation inside arbitrary
Rune bytecode. Adding long-running native presentation work would require a
separate cancellation contract.

### 8. Static identity and restore semantics do not change

The executed values are authored intent for the current successful run. They
are not persisted. PRODUCT-0062 continues to persist panel/group meaning and
colour allocation keyed by `DocumentId` and the source-static
`LocalViewIdentity`; PRODUCT-0063 continues to restore unresolved placeholders
without executing Rune.

Re-running a slot may change its title, mark, settings, or content kind while
retaining the same named identity or anonymous ordinal, just as editing the
inline expression does today. A failed run leaves the previous visible result
stale under the existing lifecycle. No owner schema or logical-layout schema
changes.

Runtime list cardinality, runtime ordering, and runtime names remain outside
the saved contract. The local VM cannot add, remove, or reorder identities
because the static shell, not its returned values, is the census authority.

### 9. The old semantic recognisers leave in the same cut

The source analyser remains responsible for parsing, lexical dependencies,
relation roots, compatibility-root normalization, the literal `.views` shell,
static names, slot spans, and output order.

It no longer owns separate semantic parsers for figure children, mark
arguments, modifier registers, layers, bounds, ticks, skins, palettes, legends,
facets, or table presentation. Those meanings have one implementation in the
local native types and their registered functions.

The existing static literal machinery may remain as the proof and decoder for
the narrow `Neutral` closure and for static outer identity. It is not a second
presentation evaluator. An inline and a bound presentation value must reach the
same VM path.

The switch is atomic across the accepted presentation corpus. No compatibility
form remains on the AST recogniser while canonical `.views` uses the VM, or
vice versa. Two semantic paths would make later surface additions dependent on
which spelling reached them.

## Implementation sequence

1. Bank the current red `yb` fixture, the complete accepted inline corpus,
   emitted Toron relation programs/source maps, exact diagnostic spans, view
   identities, and scheduler counts. The red fixture must fail specifically
   because a figure child is an identifier, not because its constructor or
   relation fails.
2. Add owned spanned presentation values and a private Rune module. Exercise
   constructors, arrays, helpers, modifier methods, conflicts, and decoding
   directly under explicit allocation and instruction limits without changing
   the production envelope.
3. Replace retained-all analysis with the occurrence dependency graph and four
   closure classes. At this boundary, sources whose retained statements all
   belong to the relation or allowed shared closure emit byte-identical Toron
   source. A presentation-only literal may disappear from that source, but the
   relation result and every mapped relation span remain identical. Add
   refusals for mixed helpers and computed dual-use values before enabling
   local execution.
4. Generate the presentation program, inject per-call origins, and extend the
   source map with synthetic owners. Bank helper-body, modifier, later-element,
   slot, compiler, and VM attribution before switching any production syntax.
5. Put bounded presentation work into the revision scheduler and prove
   coalescing, superseded in-flight completion, failure release, and zero query
   dispatch. Preserve the existing post-query completion path.
6. Switch every accepted table/plot/figure/layer/register spelling to the typed
   VM in one cut. Delete the old per-element semantic recognisers and retain
   only the static shell and relation analyser.
7. Run the complete inline/bound equivalence matrix and all downstream schema,
   colour, facet, persistence, export, and restore gates. No baseline or saved
   payload changes are expected.
8. Run `/home/me/work/uat/test21.rn` with its computed `yb`, confirm all bar and
   percent panels live, and capture the exhibit. The screenshot is contact
   evidence, not a substitute for gates 1-18.

## Acceptance gates

1. `/home/me/work/uat/test21.rn` accepts `let yb = y_bounds(0, 160)` and reuses
   it in multiple figures. Its rendered bounds equal the prior inline spelling,
   while the banked pre-implementation fixture proves the identifier form was
   genuinely refused before this record.
2. Every existing figure element, table setting, mark constructor, layer
   member, and accepted modifier can be returned by a presentation-only helper
   at least two calls deep. Computed arrays of figure elements and ordinary
   numeric/string expressions execute; inline and executed spellings decode to
   equal authored intents.
3. `.stacked()` and `.percent()` are invoked as registered Rune associated
   functions. Modifier order, duplicate refusal, argument refusal, cross-
   register refusal, family identity, normalization, and persisted colour
   lineage equal PRODUCT-0064 and PRODUCT-0072. A source/architecture gate
   proves no production parser compares those modifier names to implement
   their semantics.
4. An invalid `y_bounds` constructed inside a helper called through at least
   three levels refuses at the helper body's constructor/argument origin, not
   at the outer slot, generated wrapper, or whole buffer. The test asserts the
   exact original source slice.
5. Duplicate title, subtitle, skin, palette, legend, bounds, tick, and
   applicable register entries attribute to the later element or modifier.
   Mark-family applicability attributes to the offending modifier. At least
   one case is produced through a helper so AST position at the outer slot
   cannot satisfy the gate accidentally.
6. A slot returning the wrong runtime type attributes to its complete static
   slot expression. A compile error in copied helper text and one deliberately
   induced generated-fragment error map through copied and synthetic source-map
   segments respectively; neither exposes generated coordinates or names.
7. Bound fields produced by execution reach schema resolution as their exact
   strings. Missing-field and wrong-dtype failures retain their established
   channel attribution; an invalid two-quantitative bar pair retains PRODUCT-
   0055's complete-body span and names both resolved fields/types. No binding or
   origin token enters `AuthoredPlotSpec` or a resolved channel.
8. Each lowering-time failure class -- topology, partition, compilation,
   instruction exhaustion, allocation exhaustion, constructor validation,
   duplicate element, and modifier applicability -- dispatches zero Toron
   requests. A schema-dependent refusal dispatches exactly its required
   relation and cannot be manufactured by the local VM.
9. Table-driven partition tests cover `RelationOnly`, `PresentationOnly`,
   allowed static `Neutral`, and `Mixed`. Relation output contains no
   presentation vocabulary; local output contains no relation vocabulary;
   neutral literals/aliases have identical concrete meaning in both and contain
   no calls. A mixed helper refuses before either program executes. An
   unreferenced legacy statement remains only in the Toron program, proving
   partition does not silently delete or duplicate an effect merely because
   neither output reads its value.
10. Most-recent-preceding shadowing is tested with two and three occurrences of
    one name assigned to different closures. The emitted programs contain the
    correct occurrence by source position, not every definition sharing the
    name. Moving a use across a shadowing statement changes only the intended
    dependency.
11. A computed vocabulary-free helper used by both halves refuses as
    unsupported dual-use, while the same helper used in only one half executes.
    Static literals, negated numerics, and their alias chains remain the only
    v1 dual-use closure. An anti-vacuity fixture uses
    `std::char::from_i64`, supplied by the presentation context's default
    `char` module and absent from Toron's explicit module list: its isolated
    closure compiles locally and fails in the real Toron context, while a
    dual-use occurrence refuses during partition before either result can
    decide the outcome. A helper containing both `col()` and `y_bounds()`
    refuses as `Mixed` with both dependency sites named.
12. The literal `.views` shell accepts computed values in fixed slots but
    refuses a computed list, conditional list, loop-produced list, bound or
    computed view name, and any source whose runtime result could change count
    or order. Named and anonymous identities equal the current inline corpus.
13. Compatibility roots, relation sugar, canonical views, named views,
    figures, layers, table views, and modified standalone marks all reach the
    same typed evaluator. The complete accepted pre-record corpus lowers to
    equal authored intents, positional constructor-name collisions from
    PRODUCT-0040 and PRODUCT-0058 remain accepted, and no spelling retains a
    second AST semantic path.
14. A fixed tiny instruction budget stops an infinite presentation loop, and a
    parameterized allocation cap stops an oversized value. Production constants
    equal their documented values. Both errors release the scheduler and
    dispatch no relation.
15. With a barrier around local VM invocation, an older in-flight revision
    superseded by a newer one may finish but dispatches nothing; the newest
    revision alone reaches Toron. Without the barrier, coalescing before VM
    invocation executes only the newest revision. Failure and success each
    leave no stuck in-flight entry.
16. A successful rerun preserves `(DocumentId, LocalViewIdentity)` panel and
    group meanings plus categorical colour lineage exactly. Adding local
    helper statements cannot change the three-set census, output order, or
    allocator high-water marks. `polariton_session` bytes and schema version
    remain unchanged.
17. Reopening a saved session restores the same unresolved named and anonymous
    placeholders without compiling or executing the presentation program.
    Re-running then resolves them in place. A process fixture makes execution
    observable and proves restore itself performs none.
18. The full workspace suite, all-target/all-feature check, PRODUCT-0057 render
    evidence, saved-session process gates, export gates, and live `test21.rn`
    exhibit pass with no ignored or tolerated failure. Existing pixel and
    serialized baselines are unchanged.

## Supersession

This record supersedes PRODUCT-0016 §4, PRODUCT-0040 §9, and PRODUCT-0058 §6
only where they refuse computed **presentation values** because the envelope
does not evaluate Rune. Their resulting element semantics, literal grammar,
most-recent-preceding shadowing, concrete-value normal form, phase separation,
and diagnostic obligations remain accepted.

PRODUCT-0040 §5's retain-every-literal-binding implementation rule and PRODUCT-
0058 §5's corresponding retention premise are replaced by occurrence-based
partition. An allowed neutral literal is emitted to both programs when both
need it; a presentation-only literal need not be sent to Toron. This changes
generated relation source only where the removed statement is provably outside
the relation closure. It does not change the relation's meaning.

PRODUCT-0055's pair resolver, PRODUCT-0061's coupled witness, PRODUCT-0062's
identity and colour persistence, PRODUCT-0063's no-execution restore rule, and
every later mark/register decision stand. They are constraints and downstream
consumers, not amended behaviour.

## Consequences

Presentation code becomes Rune instead of a Rune-shaped syntax tree. Authors
can factor `y_bounds`, build element arrays, write helpers, and compute modifier
arguments without Polariton teaching the envelope each alias shape.

The flexibility stops at a deliberate boundary: what each static slot means may
execute, but which slots exist and what they are durably called remains visible
in source. The boundary preserves saved identity and makes restore honest.

Polariton now compiles two programs from one buffer. That is real machinery:
dependency analysis, source mapping, resource limits, and scheduling all become
part of the envelope contract. It is preferable to an indefinitely growing
second evaluator only because the semantic parser is removed in the same cut.

Relation pushdown remains Toron's Rune program, not a Polariton reimplementation
of `.sort` or another verb. No cross-team protocol or database change is
required.

Diagnostics preserve or improve their current precision. A helper-produced
failure identifies the construct that created the bad value rather than the
outer slot that merely carried it.

Computed dual-use helpers remain deliberately unavailable. This is the visible
cost of refusing to claim purity or deterministic replay that Rune does not
currently prove.

## Rejected

- **Teach `parse_figure` to resolve `yb`:** fixes one alias while preserving the
  manual evaluator that made the alias fragile.
- **Add computed cases to each AST recogniser:** duplicates more Rune semantics
  and guarantees future helpers fail at the next unimplemented expression.
- **Execute the complete `.views` result:** makes view count, order, and names
  runtime data and reopens durable identity, the coupled witness, reconciliation,
  and restore.
- **Execute relation verbs locally through a hand-built proxy in this cut:**
  adds a second relation vocabulary when existing source transport and lexical
  partition suffice. It remains the named escalation if Stage 1 disproves that
  premise.
- **Serialize a relation plan for Toron:** introduces a cross-team protocol and
  duplicates a Rune source transport that already works.
- **Register presentation types in Toron:** moves product UI vocabulary into
  the database solely to avoid a local VM and lets presentation work affect
  query planning.
- **Keep AST semantics for compatibility spellings:** gives one authored
  surface two evaluators and makes later marks or modifiers spelling-dependent.
- **Recover native caller spans from Rune:** Rune 0.14 does not provide caller
  context to registered functions. Host-side instruction lookup is not
  reachable from the constructor and would attribute helper calls less usefully
  even if it were.
- **Use generated-program spans as diagnostics:** exposes text the author never
  wrote and regresses accepted exact-source attribution.
- **Replay every vocabulary-free helper in both VMs:** syntax does not prove
  purity, determinism, or equal native vocabulary. v1 duplicates only the
  statically decoded literal closure.
- **Mirror Toron's modules or compute their intersection:** couples a local UI
  evaluator to a database allow profile and turns module drift into a silent
  replay defect. Literal-only duplication needs no shared module contract.
- **Persist executed presentation values:** creates a second authority beside
  the `.rn` buffer and makes migration depend on every presentation type.
- **Execute presentation during restore:** violates PRODUCT-0063's explicit
  no-code-on-open boundary and gives opening a session side effects.
- **Run local evaluation synchronously in `update`:** lets an infinite loop or
  expensive compile freeze the GUI despite Rune's available limits and the
  existing blocking-task precedent.

## Stop conditions

- If relation and presentation closures cannot be separated without Polariton
  understanding Toron relation semantics, stop and specify the opaque proxy or
  shared-plan escalation. Do not guess from callee strings or leak
  presentation vocabulary into Toron.
- If original helper-body, modifier, later-element, slot, and complete-body
  attribution cannot survive origin injection and copied/synthetic source
  mapping, stop. Coarse whole-buffer diagnostics are not an acceptable price
  for evaluation.
- If implementation needs runtime view cardinality, runtime ordering, a
  computed view name, a new `LocalViewIdentity`, changed witness contents, or a
  saved-schema version, stop and return the topology question to persistence.
- If local compilation/execution cannot be allocation-bounded,
  instruction-bounded, run off the UI thread, and prevented from dispatching a
  superseded revision, stop rather than introducing an unbounded author-code
  path.
- If any accepted spelling must retain the old per-element semantic parser
  beside the VM, stop and narrow the switch openly. Two presentation
  authorities are worse than the current single limited one.
- If the work requires a Toron change, relation-plan protocol, database schema
  inspection before dispatch, or execution during restore, stop and write the
  owning record instead of widening this one.
