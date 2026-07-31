# Literal and regular-expression repository search

Scope: split the repository picker into three honest modes:

- `:fz QUERY` — the shipped fuzzy scorer;
- `:grep QUERY` — case-sensitive literal line search;
- `:grep! REGEX` — case-sensitive regular-expression line search.

Remove the misleading `:rg` alias. Reuse the existing picker, corpus,
streaming, cancellation, live-buffer, navigation, and frontend machinery.
Deliverable is one `impl:` commit reviewed against this plan.

## Engine choice

Use the workspace's existing `regex-automata` dependency
(`meta` + `perf`). Compile one `meta::Regex` per query generation on the
search worker. The meta engine chooses its optimized execution strategy;
do not add a native Hyperscan/VectorScan dependency or a second scanner.

Compilation errors are ordinary picker state, not command failures or
panics. An incomplete expression while the user types shows the error and
keeps the modal editable. A later valid generation supersedes it normally.

## Search contract

Introduce an explicit search-mode value carried from command dispatch
through picker state, task input, viewport projection, and acceptance-time
revalidation. Never infer mode from the command name or query contents.

All modes remain line-oriented:

- one result per matching line;
- the first match supplies byte/display column;
- no cross-line matching, captures, replacement, lookaround, or
  backreferences;
- existing per-file/global/corpus limits remain unchanged and honest.

Literal mode uses byte substring search and is case-sensitive. Regex mode
uses Rust regex syntax and is case-sensitive unless the expression opts
into flags such as `(?i)`. Fuzzy mode retains its existing smartcase and
ranking tiers unchanged.

Result ordering remains deterministic. Fuzzy results retain their score.
Literal and regex results share an exact-match tier, preferring earlier
columns, shorter lines, shallower paths, then path/line/column order.

## Correctness boundaries

- Open files are searched from immutable snapshots of their live bytes,
  including unsaved changes.
- Query generations and cancellation apply equally to all modes.
- Accept always re-runs the selected mode against the recorded line in the
  current target bytes. If it no longer matches, reject and refresh; never
  clamp.
- Dirty cross-file refusal and Ctrl-o/Ctrl-i history remain the shared
  navigation path.
- Regex compilation happens once per worker generation and once for
  acceptance-time revalidation, never once per file or line.
- Empty queries yield no results in every mode.

## Surface

The Baryon overlay title identifies `fuzzy`, `literal`, or `regex`, and an
invalid regex is shown inside the modal without hiding prior editor state.
Polariton receives the expanded protocol value but its picker surface
remains out of scope.

Catalog/help text must make the bang semantics explicit and contain no
claim that `:grep` is fuzzy or that `:grep!` is PCRE. Regenerate the
built-in catalog snapshot after removing `:rg` and adding `:grep`.

## Tests

1. Command dispatch: `:fz`, `:grep`, and `:grep!` preserve mode/query;
   `:rg` is unknown.
2. Literal metacharacters are literal under `:grep`; the same query is
   interpreted under `:grep!`.
3. Regex syntax error streams into the active generation, stays modal,
   and disappears after a valid edit.
4. Unicode and tab-containing lines produce correct byte/display columns.
5. Supersession and cancellation cover regex compilation/search.
6. Live dirty-buffer result and acceptance re-resolution work in all
   non-fuzzy modes.
7. Existing fuzzy scorer and `render_frame` acceptance tests remain green.
8. Full libaryon, Baryon, and Polariton suites plus strict Clippy pass.

## Non-goals

- Native regex engines or architecture-specific packaging.
- Multiline regex, captures UI, replacement, quickfix export.
- Glob/file-name filtering, persisted indexes, parallel corpus shards.
- Implementing the adjacent concept-index plan.

## Review checklist

- [ ] Command vocabulary is honest: `:fz`, `:grep`, `:grep!`; no `:rg`.
- [ ] Existing `regex-automata` engine is compiled once per generation.
- [ ] Invalid patterns are visible, recoverable picker state.
- [ ] Live bytes, cancellation, caps, stale rejection, and jump history
      remain common machinery rather than mode-specific copies.
- [ ] Fuzzy behavior is unchanged.
- [ ] No native dependency or frontend-side matching.
