# Specification: Runtime Registry Overlay

## Overview

Replace Baryon's compile-time-only command, keymap, and option registries with engine-owned,
per-session **immutable defaults plus a runtime overlay**. The overlay is language-neutral enabling
infrastructure: it must be possible for a component other than the binary to own a handler, register
and unregister its metadata at runtime, and have dispatch, help, completion, suggestions, and keymap
documentation observe the same effective registry.

This is item 5 of `plans/rune-scripting.md` §5. It does **not** embed Rune. The Rune host later plugs
an action executor into the capability established here. The registry does not know whether a dynamic
action is backed by an `Arc` callable, an index into a Rune function store, or another host-owned
representation.

The input-contract migration (`plans/input-contract.md`) is an implementation prerequisite. It moves
modal input, ex-command parsing, prompt state, and all three registries into `libaryon`; implementing
an overlay while those surfaces still execute in `baryon` would create two authorities and make
headless behavior diverge from the TUI.

## 1. Ground truth

§1.1-§1.3 preserve the pre-input-migration inventory against which builtin behavior is
characterized. §1.4 records the current ownership boundary.

### 1.1 Ex-commands are a static frontend table

The pre-Rune characterization contained 25 `CommandSpec` entries in `COMMANDS`. The Rune lifecycle
adds the deliberately characterized `:source[!]` builtin, bringing the effective count to 26. Names,
aliases, argument rules, completion kind, category, help text, and dispatch are all `&'static`; exact
handlers and fallback parsers are bare function pointers (`:43-46`, `:97-121`).

`CommandCtx` (`:13-39`) can send `EditorCommand`, set a frontend status, read the projected cursor
line / visual range, and request quit. It cannot synchronously read engine state. Exact dispatch runs
first, bare boolean option commands second, and the four fixed fallback grammars last
(`dispatch_ex_command`, `:431-476`). The fallback order is observable: line number, substitute,
sort, then uniq.

The same static slice drives argument completion (`completion_command_names`, `:484`), typo
suggestions (`:497-551`), and help (`baryon/ui/help.rs`). Adding a second dynamic lookup only to
dispatch would create commands that cannot be completed, suggested, or documented.

### 1.2 The keymap is a static graph of frontend actions

`baryon/ui/keymap.rs` contains 236 bindings grouped in `KEYMAP`. A binding is one
of five closed variants — Normal, Visual, Insert, Prompt, Confirm — whose actions are closed enums
implemented by `Frontend` (`baryon/ui/dispatch.rs`). `KeyPattern` is crossterm-shaped today.

Every resolver scans `KEYMAP` and returns `Option<&'static BindingSpec>`
(`keymap.rs:2625-2739`). `arm_target` turns selected builtin actions into private pending contexts;
`key_docs` walks the same graph to derive complete documented sequences (`:2793-2854`). Applicability
and graph assertions protect the current static table but there is no insertion, replacement,
unmapping, owner cleanup, or runtime ambiguity check.

The input-contract spec changes the underlying types: portable keys, exact modifiers, engine-owned
pending state, synthesized prefix nodes, and prefix-free registration. This specification consumes
that result; it does not duplicate the migration.

### 1.3 Options are two static frontend tables

`baryon/ui/option_registry.rs` has six `BoolOptionSpec` defaults and four `ValueOptionSpec` defaults.
Their session/persistence actions and parsers are bare function pointers. `parse_set_command`
(`:174-237`) directly loops those slices, while setting-key and value completion directly imports
them in `baryon/ui/command.rs` (`complete_setting_key`, `value_option_arg_start`). Help imports them
again in `baryon/ui/help.rs`.

`UI_COLOR_SPECS`, theme definitions, and language providers are separate engine registries. They are
not plugin-defined options and are not part of this overlay.

### 1.4 The input-contract prerequisite is complete

`EngineHandle` exposes typed `CommandSender` and `InputSender` adapters over one crate-private,
bounded `EngineRequest` stream; `InternalSender` keeps refresh/job events unforgeable. Modal state,
prompt/history/completion state, ex-command parsing, and the builtin command/keymap/option registries
now live in `libaryon`. Input-generated commands invoke the extracted dispatcher synchronously and
render once. The TUI translates crossterm input and renders `InputProjection`; it no longer resolves
mappings or commands. Path completion returns through the ordered internal stream with stale-result
rejection. Stage 0 of this plan is therefore complete, and builtin characterization is the next edge.

The migrated engine catalog contains 263 bindings across 93 groups. The 236-binding count in §1.2
is the preserved pre-migration inventory; 263 is the post-input-contract baseline that the overlay
must preserve, not drift to erase.

### 1.5 Resolution cannot lend the registry across invocation

A dynamic handler may register, replace, or remove mappings while it runs. Dispatch therefore cannot
hold `&RegistryEntry` across callback invocation: doing so either prevents self-modification through
Rust borrowing or invites unsafe interior mutability. Resolution must return an **owned invocation
snapshot** and release the registry borrow before invoking it.

## 2. Required capability and ownership

### 2.1 One per-session authority in the engine

After the input-contract move, `EditorSession` owns one `RuntimeRegistries` value containing command,
mapping, and option registries. Each registry presents one effective view:

```
immutable builtin defaults + owner-scoped runtime entries = effective registry
```

There is no process-global mutable registry. Two `EngineHost` instances do not share plugins,
configuration, mappings, or callbacks. Headless input and the TUI resolve against the same session
state.

One monotonic `RegistryRev` covers the cross-registry effective view. A successful transaction or
owner cleanup advances it once; failed/no-op transactions do not. Completion jobs and introspection
snapshots capture the revision so a result built from retired metadata is rejected rather than
installed into a newer prompt/session state.

Direct `EditorCommand` submission remains semantic input and **bypasses key mappings**, as decided by
`plans/input-contract.md` §2.1. Runtime commands and options are reached through ex dispatch or the
future direct Rune API, not by teaching embedders to forge internal input actions.

### 2.2 Runtime entries have monotonic owners

Every runtime entry belongs to a monotonic, never-reused `RegistryOwnerId`. The future host allocates
one for `init.rn`, each sourced script generation, and each plugin instance. Builtins have no runtime
owner.

Owners also have a stable precedence slot. A newly loaded logical owner is appended; replacing a
sourced generation mints a fresh `RegistryOwnerId` for stale-handle safety but inherits the retired
generation's slot. Re-sourcing therefore cannot jump ahead of an unrelated plugin and silently change
which exact key override wins.

Owner identity provides:

- infallible cleanup of every command, option, and mapping owned by a retired script/plugin;
- replacement of one source generation without confusing stale action references with a new owner;
- collision diagnostics naming both owners;
- a foundation the event bus can reuse without coupling this registry to Rune.

Removing an owner must not invoke its handlers. It removes metadata and handler associations, then
the host may release the corresponding callback storage. A stale invocation lease is rejected cleanly
by the executor rather than reaching a new callback.

### 2.3 Action storage stays opaque, but its behavior is specified

Builtin actions remain typed engine actions. A runtime entry associates its metadata with a handler
owned by the extension host through an opaque action/executor interface. Resolution obtains an owned
invocation lease before releasing the registry borrow. This plan deliberately does not choose whether
that lease is an `Arc` to a callable, a callback-store key, or another representation.

The required behavior is:

- registry resolution obtains an owned invocation lease and drops all registry borrows;
- invocation happens synchronously on the engine thread;
- unregistering during invocation affects the next resolution, not the already-resolved call;
- invoking an action whose owner has retired returns a normal error/status;
- action failures preserve the already-decided non-transactional callback semantics from
  `rune-scripting.md` §3.12;
- host calls may mutate the registry but may not re-enter the VM;
- an attempt to invoke another dynamic action while one is active returns a clean non-reentrancy
  error in this milestone. The later event-bus/VM specs may turn selected cases into deferred work,
  but the registry never recursively invokes the executor.

Tests use a fake executor and fake invocation lease. The Rune VM lifecycle spec chooses the concrete
production backing and adds its guarded-argument upgrade tripwires.

### 2.4 Registration is bounded and atomic

`RuntimeRegistries` accepts a cross-registry owner transaction: command, mapping, and option changes
are validated against one candidate effective state and either commit together or change nothing. A
transaction may nominate an older generation of the same source to retire; validation sees the
candidate *after* removing that generation and adding the replacement, so re-sourcing does not
collide with itself. The future VM lifecycle may stage host registration calls into this transaction
and publish them only after the source invocation succeeds. Ordinary live registration is a
one-transaction immediate commit.

Validation includes names, aliases, collisions, key-prefix rules, owner authority, sequence length,
completion metadata, and resource caps.

The initial caps are explicit:

| Resource | Cap |
|---|---:|
| entries in one owner transaction | 4,096 |
| runtime entries/layers in one session | 16,384 |
| owned metadata in one transaction | 1 MiB |
| owned registry metadata in one session | 8 MiB |
| command/option name or alias | 128 bytes |
| aliases on one entry | 16 |
| summary or usage | 512 bytes |
| help body | 16 KiB |
| static completion candidates on one entry | 1,024 entries / 64 KiB |
| mapping sequence | 16 portable keystrokes |
| dynamic option query display | 4 KiB |

The byte totals include every owned string and sequence in shadowed mapping layers, not just effective
entries. Replacing an owner generation is charged against the candidate state after the old generation
is removed, so a source can replace a full allocation without temporarily requiring twice the session
cap.

These are enforcement, not documentation. Rune's instruction budget does not interrupt native host
work (`plans/rune-host-access-spike.md`), so registration may not validate or allocate an unbounded
host-provided collection.

## 3. Command registry

### 3.1 Builtins are protected defaults

The existing command table becomes the immutable builtin catalog. Runtime command names and aliases
share one case-sensitive namespace with builtin names, builtin aliases, other runtime entries, and
every builtin option token that can appear as a bare command (including `no` forms). Builtins cannot
be replaced or hidden by the runtime overlay. A runtime owner may replace one of its own command
definitions explicitly; it may not silently take a name owned by another plugin.

Names and aliases use a bounded command-token grammar: an ASCII letter first, followed by ASCII
letters, digits, `_`, `-`, or `.`. Whitespace, `!`, leading digits, range punctuation, and fallback
syntax are rejected. This keeps exact runtime commands disjoint from the fixed fallback grammars.

### 3.2 Runtime commands are exact commands only

A runtime command owns:

- canonical name and aliases;
- bang acceptance;
- `None` / `Optional` / `Required` argument rule;
- usage, summary, help, and category metadata;
- bounded completion metadata;
- one opaque dynamic handler association.

Runtime commands are documented in a final **Extensions** category; plugins do not inject arbitrary
top-level help categories.

Invocation receives owned structured data: canonical command name, the spelling used, raw trimmed
arguments, and bang. Generalized Vim range parsing is not introduced here; the existing line,
substitute, sort, and uniq fallback parsers retain their current range behavior. A later API may add a
range contract without opening runtime fallback grammars.

Runtime code cannot register a fallback parser. Extending the command grammar is engine work; a
plugin-owned native parser would run unbounded work on the engine thread and make precedence depend
on plugin load order.

### 3.3 Dispatch preserves one execution path and current precedence

Effective dispatch order stays:

1. builtin and runtime exact commands (`set` is one builtin exact command and resolves the effective
   option registry);
2. the builtin bare session-toggle forms such as `wrap` / `nowrap`;
3. the fixed builtin fallback parsers in their existing order;
4. unknown-command diagnostics and suggestions.

Builtin handlers produce internal input actions / `EditorCommand`s and dispatch them synchronously,
as required by the ordered input contract. They are not re-enqueued. A runtime command invokes its
resolved dynamic action at the same seam with the live editor borrow.

### 3.4 Completion is declarative

Runtime command completion may be none, one of the engine's bounded builtin providers (path,
language, theme), or a bounded static candidate list. Arbitrary completion callbacks are deferred to
the plugin-facing background-job system: they cannot block the engine thread, and invoking Rune from
inside an already-running Rune callback would violate non-reentrancy.

## 4. Keymap registry

### 4.1 Plugins register public sequences, never pending states

Runtime mappings specify a public mode and a concrete sequence of portable keystrokes. They cannot
name `NormalPending`, `VisualPending`, `Arm*`, wildcard capture, or another internal state. `<leader>`
and string notation, if offered by the Rune API, are parsed into concrete strokes before reaching the
registry.

A runtime target is either an ex-command invocation or an opaque dynamic handler. Key targets do not feed
synthetic key events back through the resolver, so recursion and `remap` semantics are absent. A
command target resolves the command when the mapping fires; removing that command later produces a
clean error rather than retaining a dangling handler.

### 4.2 The effective map remains prefix-free

The input-contract invariant remains load-bearing: within one public mode/effective context, a
sequence is a terminal action or a prefix, never both. Registration validates the candidate trie and
reports the two conflicting sequences and owners. No timeout or deadline is introduced.

Concrete runtime strokes may refine a builtin wildcard terminal (for example a specific character
after builtin `r`); exact matching wins and removal reveals the wildcard behavior. Modifier sets are
exact under the input-contract normalization rules.

### 4.3 Overrides preserve trie structure

Runtime mappings form owner layers at **terminal sequences**:

- an owner may replace the action at an existing terminal sequence;
- several runtime owners may layer replacements at the same terminal; the highest stable owner
  precedence is effective, and owner removal reveals the previous layer/default;
- an owner may add a terminal at a genuinely vacant, prefix-free leaf;
- removing an owner removes its layers and safely reveals what was underneath.

This milestone does **not** suppress builtin terminals or convert a builtin terminal into a prefix (or
vice versa). Those tombstones create cross-owner unload dependencies: removing one owner can reveal a
default that conflicts with another owner's descendant mapping. Exact terminal overrides cover
ordinary remapping; a no-op action can disable a terminal behavior. Structural unmapping is deferred
until it has an unload-safe ownership model.

### 4.4 Mapping metadata drives effective documentation

Each runtime mapping carries a bounded summary and is documented in a final **Extensions** category.
Key help walks the effective trie: overridden builtin terminals show the effective runtime summary,
added mappings appear in their mode, and removing an owner restores the previous documentation. Help
never walks shadowed action storage.

## 5. Option registry

### 5.1 Runtime options are executable configuration, not TOML schema

Runtime options are `:set`-only, session-scoped definitions owned by a script/plugin. They do not add
keys to `settings.toml`; `init.rn` remains their persistence mechanism, preserving the settled
configuration layering in `rune-scripting.md` §2.3.

The builtin setting namespace is protected: boolean/value option names and aliases, boolean `no`
forms, and UI-color names and aliases. Runtime names share the bounded identifier grammar with
commands and may not collide with one of those builtins or another option owner. A runtime owner may
explicitly replace its own definition.

A runtime Bool definition reserves both its positive tokens and their derived `no` forms; validation
therefore rejects defining `foo` and `nofoo` as two independent options whose parse order would decide
the result.

### 5.2 Typed parsing stays in the engine

A runtime option declares Bool, Integer, or String input plus usage, summary, help, and optional
bounded static value candidates. The engine performs syntax/type parsing before invoking the action:

- Bool accepts `:set name`, `:set noname`, and the existing boolean value spellings;
- Integer requires a checked `i64` argument;
- String receives the trimmed remainder and may require non-empty input;
- `:set name?` produces a structured query invocation.

The dynamic action receives `Set(OptionValue)` or `Query`. The handler owns semantic validation and
state. A query returns a bounded display value or a normal error. Dynamic options do not gain bare
`:name` ex-command aliases; that namespace remains the command registry's.

### 5.3 UI colors and provider catalogs remain builtin

`UI_COLOR_SPECS`, themes, languages, and save-durability modes continue to use their dedicated
builtin catalogs. The effective setting-key completion and help views chain those catalogs with the
runtime option registry rather than absorbing them into a generic plugin schema.

## 6. Help, completion, suggestions, and introspection

### 6.1 One effective registry view feeds every surface

Dispatch, prompt completion, typo suggestions, `:help`, key docs, and the future Rune introspection
API consume a read-only effective-registry view. No surface imports mutable overlay storage directly,
and no dynamic-only side table is queried by dispatch alone.

Ordering is deterministic:

- builtin help retains source order;
- runtime commands/options sort by canonical name;
- key docs retain mode/category order and sort added sequences canonically within a category;
- suggestions deduplicate effective names and aliases before scoring.

CLI `baryon --help` runs before an engine session or `init.rn` exists and therefore documents builtin
defaults only. In-app `:help` uses the live effective registry and includes runtime entries.

`HelpTopic` can no longer be only an enum of `&'static` specs. Topic resolution produces an owned
metadata snapshot tagged with `RegistryRev`; an already-open help buffer remains a text snapshot,
while reopening or refreshing resolves against the current revision.

### 6.2 Introspection never exposes action capabilities

Registry snapshots expose owned metadata, owner identity, and whether an entry is builtin or runtime.
They do not expose handler associations, internal pending nodes, or mutable registry references. This
is the surface the later `baryon::*` API uses to list commands, options, and mappings safely.

## 7. Mutation and failure semantics

- Resolution returns an owned action/invocation plan before any callback runs.
- Each registration batch is mutation-free on validation failure.
- Replacing an owner's entry commits metadata and its handler association together.
- Owner cleanup validates owner identity and revision capacity before mutation; its commit is
  infallible and leaves every effective registry valid.
- A callback may modify its own registrations; the current invocation continues from its snapshot.
- Registry mutations request a render when they change projected pending-input labels, completion, or
  an open help view. Static already-rendered help buffers are snapshots and do not rewrite themselves;
  reopening/refreshing help observes the new registry.
- Dynamic action failure does not roll back unrelated editor mutations or earlier successful callback
  work, matching the accepted non-transactional host contract.

## 8. Explicit non-goals

- Embedding Rune or choosing the concrete dynamic-action backing.
- The event bus, autocommands, plugin jobs, subprocesses, or network access.
- Runtime fallback command grammars or arbitrary completion callbacks.
- General Vim range syntax for user commands.
- Keymap timeouts, recursive mappings, synthetic key replay, or exposing pending states.
- Structural unmapping/tombstoning of builtin keymap prefixes or terminals.
- Persisting plugin-defined options in `settings.toml`.
- Runtime definition of UI color keys, themes, languages, or save-durability modes.
- Process-global registries shared by multiple editor sessions.

## 9. Acceptance criteria

1. All builtin command, keymap, option, help, completion, and suggestion behavior is characterized
   before the move and remains equivalent after it.
2. Resolver APIs return owned invocation snapshots, not `&'static` or a borrow held across execution.
3. A fake non-binary owner can register a command, invoke it through ex dispatch with args/bang, see
   it in completion/help/suggestions, replace its own definition, and remove it.
4. Builtin and cross-owner command/option collisions reject the complete batch without mutation.
5. A fake owner can override an exact builtin mapping, add a prefix-free mapping, see both in key
   docs, and remove them so the prior/default behavior returns.
6. Terminal/prefix conflicts, overlong sequences, inexact modifier forms, and structural builtin
   unmaps are rejected atomically with actionable diagnostics.
7. Actual key dispatch and documented effective sequences are derived from the same trie.
8. Runtime Bool/Integer/String options parse typed values, support bounded queries, appear in `:set`
   completion/help, invoke the fake owner, and never change serialized TOML.
9. Removing an owner cleans all three registries and makes every stale action fail cleanly; monotonic
   owner ids are never recycled.
10. A handler can replace or unregister itself during invocation without a borrow failure, deadlock,
    use-after-free, or effect on the already-running call.
11. Replacing a sourced owner generation validates and commits all three registries atomically; a
    successful replacement retains its precedence slot, and a failed candidate leaves the old
    generation fully effective.
12. Registration caps are tested at every boundary, and rejection leaves registry counts, metadata,
    `RegistryRev`, and effective resolution byte-for-byte unchanged.
13. Completion/introspection results tagged with a retired `RegistryRev` are rejected cleanly.
14. TUI input and headless portable input observe the same runtime mappings; direct `EditorCommand`
    submission bypasses them.

## 10. Staging

0. ~~**Input-contract prerequisite.**~~ **DONE** — `plans/input-contract.md`: ordered request stream,
   minimal completion/history jobs, engine-owned input/prompt/ex dispatch, and the portable keymap.
   Do not create a temporary frontend overlay.
1. ~~**Characterize builtin catalogs.**~~ **DONE** —
   `crates/libaryon/src/engine/input/builtin_catalog_tests.rs` fingerprints command
   dispatch/metadata, every boolean option spelling and the value-option parser contract, all 263
   effective key bindings as complete documented sequences, every plain/Markdown help page,
   completion, suggestions, and the fixed fallback order at the engine boundary. Focused registry
   tests remain the explanatory assertions beneath that migration tripwire.
2. ~~**Owner and action boundary.**~~ **DONE** — `EditorSession` owns an independent
   `RuntimeRegistries`; monotonic `RegistryOwnerId`/stable precedence and `RegistryRev` spaces,
   complete-owner `RegistrationBatch` snapshots, and exact entry/metadata/schema caps establish the
   bounded cross-registry commit boundary. Validation is mutation-free and one successful owner
   snapshot swaps command, mapping, and option declarations together before advancing the revision
   once. `ActionLease`, owned `InvocationSnapshot`, the language-neutral executor trait, and the
   engine-thread `InvocationGate` establish stale-owner and non-reentrancy failure semantics without
   choosing a Rune callback representation. Fake-executor and boundary tests cover self-replacement,
   stale owners, nested invocation, cap/exhaustion failure, namespace collision, and all-or-nothing
   publication. Runtime declarations remain intentionally ineffective: command/option consumers move
   in Stages 3-4, while builtin-trie compatibility and exact modifier validation land with the keymap
   consumer in Stage 5.
3. ~~**Command overlay end to end.**~~ **DONE** — `input/runtime_catalog.rs` is the sole
   session-effective command adapter over the immutable builtin table and the per-session runtime
   declarations. Engine-owned prompt dispatch resolves runtime exact commands to owned
   `InvocationSnapshot`s, preserves builtin exact/session-toggle/fallback precedence, and executes
   dynamic work synchronously through the injected language-neutral executor and `InvocationGate`.
   Argument/bang validation, path/language/theme/static completion, typo suggestions, in-app topic
   resolution, the Extensions help category, and action-free introspection all consume the same
   effective metadata; CLI help remains builtin-only. Path-completion jobs capture `RegistryRev` and
   reject a result after catalog replacement. Fake-owner tests cover prompt-to-executor invocation,
   aliases, structured args/bang, usage errors, replacement/removal, completion, suggestions, help,
   and introspection. No public registration surface or concrete language executor is introduced;
   the Rune lifecycle stage supplies that backing later.
4. ~~**Option overlay end to end.**~~ **DONE** — `input/runtime_options.rs` is the sole
   session-effective option adapter over persistent builtin settings and session-scoped runtime
   declarations. Bool/Integer/String parsing, aliases and derived `no` forms, structured queries,
   static value completion, typo suggestions, in-app topic resolution, the Extensions section of
   `:help set`, and action-free introspection all consume the same owned metadata. Runtime `:set`
   resolves only to an `InvocationSnapshot` and never emits an `EditorCommand`, structurally keeping
   plugin options out of `settings.toml`; CLI help and the builtin schema remain unchanged.
   `InvocationGate::invoke_dynamic` requires a query display, rejects one from non-query actions,
   enforces the 4 KiB display cap, and projects the accepted value as a rendered status message.
   Fake-owner tests cover every type and boolean spelling, checked `i64`, string remainder handling,
   queries and exact cap boundaries, prompt completion, suggestions, help, introspection,
   replacement/removal, and the no-persistence-command invariant.
5. ~~**Keymap overlay end to end.**~~ **DONE** — `input/runtime_keymap.rs` derives one
   revision-cached, session-effective prefix-free map from the immutable builtin graph and the
   runtime owner layers. Registration validates exact portable modifiers and rejects any runtime or
   builtin terminal/prefix conversion atomically, while allowing concrete exact overrides, vacant
   leaves, shared builtin prefixes, and exact refinements of builtin wildcard terminals. Exact owner
   layers resolve by stable precedence; replacement/removal reveals the lower owner or builtin
   default. `InputRuntime` tracks runtime prefixes beside the existing modal pending state, resolves
   opaque actions to owned invocation snapshots, and resolves mapped ex-command text only when the
   terminal key arrives, so replacement/removal during a pending sequence cannot queue a stale
   target. Effective key help and action-free introspection consume the same runtime metadata,
   suppress overridden concrete builtin rows, retain wildcard fallback documentation, and append
   runtime bindings in a final Extensions category; CLI help remains builtin-only. Fake-owner tests
   cover layered exact overrides and restoration, vacant/shared/wildcard prefixes, mismatch
   cancellation, execution-time command replacement/removal, real invocation-gate execution,
   actionable builtin and cross-owner conflicts, modifier canonicalization, and identical portable
   dispatch through every public mapping mode. No public registration surface or concrete language
   executor is introduced; owner lifecycle and Rune integration remain in later stages.
6. ~~**Owner lifecycle.**~~ **DONE** — `RuntimeRegistries::remove_owner` retires the owner identity,
   command/option/mapping declarations, action associations, entry accounting, and metadata
   accounting in one revisioned commit; after owner/revision preflight, cleanup has no fallible
   mutation step. `replace_owner_generation` requires a newer monotonic owner, validates the
   candidate effective view after excluding both the retired generation and the staged replacement,
   then atomically publishes all three declaration sets under the fresh identity while inheriting
   the retired generation's stable precedence slot. Candidate validation and revision exhaustion
   fail before mutation, leaving both generations and the effective view unchanged; entry and
   metadata cap tests prove a source at the full allocation can replace itself without transient
   double charging. Owned snapshots from a retired generation remain memory-safe but the executor's
   owner-liveness check rejects them normally. Fake-host tests cover command/option/mapping cleanup,
   layered-precedence preservation, successful and colliding generation swaps, monotonic ID and
   precedence non-reuse, stale snapshots for all three action kinds, and a callback removing its own
   owner while the already-running invocation completes. No public registration API or callback
   store is introduced; the Rune lifecycle supplies the concrete owner/action store later.
7. ~~**Integration gate.**~~ **DONE** — the builtin catalog fingerprint, all 18 owner/action tests,
   and all 242 engine-input tests pass, covering exact modifiers, builtin/runtime pending-state
   interaction, help, completion, and portable dispatch. The full headless library suite passes both
   in its default parallel order and serialized (`977 passed; 2 ignored` in each), and the forced
   blocking-resolver suite passes serialized with the same totals. That backend gate exposed one
   stale test assumption rather than a resolver defect: on Linux the cleanup test equated "platform"
   with io_uring and demanded a backend-private device entry even when
   `BARYON_SVP_BACKEND=blocking` had deliberately selected the blocking backend, which owns no
   second device map. The assertion now derives its expected private-entry count from the selected
   backend and pins both shapes; registration/unregistration production code is unchanged. All 74
   TUI tests, both architecture tests, workspace Clippy with all targets and warnings denied,
   formatting, and diff checks pass. No public registration surface or Rune action executor is
   enabled by this gate.

Stages 3-5 each move one registry across **all** of its consumers. A stage may not land dispatch from
the overlay while help or completion still reads only the static defaults; the single-authority
property is the point of the refactor.
