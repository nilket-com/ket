# Specification: workspace-level cache for the Rust semantic provider

Today `RustProvider` holds one `AnalysisState` keyed by a single `current_file_path`
(svp/rust_provider.rs:63, :113–132). Any semantic request for a different path discards
the entire `AnalysisHost` and re-runs `init_workspace`: manifest discovery,
`ProjectWorkspace::load` (cargo metadata + sysroot discovery, twice on the fallback
path), crate-graph construction, and a recursive scan reading workspace `.rs` files into
a fresh VFS. Opening N Rust files costs N full initializations; ping-ponging edits
between two open files costs one per switch. The fix: cache per **workspace**, not per
file.

## 1. Cache shape (settled)

- `RustProvider` holds `Mutex<HashMap<WorkspaceKey, WorkspaceState>>` plus an alias map
  `canonical path prefix / member manifest → WorkspaceKey`.
- `WorkspaceKey` = `ProjectWorkspace::workspace_root()`, resolved **after** loading
  Cargo metadata — not the manifest found by `ProjectManifest::discover_single`, which
  returns the nearest member `Cargo.toml` and would give one cache entry (and one full
  initialization) per workspace member. At initialization, register aliases for every
  member manifest/root the loaded workspace reports, so the first file opened in any
  other member resolves to the existing entry without a reload.
- Alias lookup order is deterministic: canonical file path → **longest** known
  member-root prefix; otherwise discover nearest manifest → alias lookup; otherwise
  cold-load. Longest-prefix wins so a parent workspace never captures a nested
  workspace's files. On rebuild or eviction of an entry, remove every alias pointing at
  it.
- `WorkspaceState` = `{ host: AnalysisHost, canonical VfsPath → FileId map,
  diagnostic_mode, manifest/lockfile fingerprint }`. The per-state
  `active_file_id`/`current_file_path` fields are deleted; the active file is resolved
  per request from the map. Request paths are canonicalized before lookup, and the map
  is keyed on canonical `VfsPath`s, never raw editor strings — otherwise relative paths
  and symlinks mint duplicate identities.
- Files with no discoverable manifest, unsaved buffers, and non-canonicalizable paths
  keep the existing `init_single_file` fallback, cached in the same map under a per-file
  key with `SyntaxOnly` diagnostics — behavior unchanged from today.
- A request for a file in a known workspace does: check fingerprint (§3), look up
  `FileId`, `change_file` with the editor text, highlight + diagnostics. No teardown.

## 2. Source-root loading: drop the per-active-file narrowing (option 1)

`roots_for_active_crates` narrows loaded roots to the active file's package plus
dependencies textually mentioned in the active source. That is incompatible with a
shared host (open a file in crate Y after initializing from crate X, and Y's sources
were never loaded). Replace it:

- **Workspace-local package roots** (`PackageRoot::is_local`): all loaded at
  initialization, unconditionally. One up-front scan instead of one per file switch —
  strictly cheaper than today's per-switch full scan by the second request.
- **Crate graph**: built for all crates the workspace reports, as today.
- **External (library) source roots**: eagerly load full sources only for
  `std`/`core`/`alloc` plus the **direct** dependencies of local crates — normal
  dependency edges, including those whose destination package exposes a proc-macro
  target (proc macros are not a separate Cargo dependency kind); dev and build
  dependencies excluded. Transitive dependency sources are not eagerly loaded.
  Measure RSS / file count / bytes loaded on this repo before considering a wider
  closure — full transitive source loading can dominate memory and startup on a large
  workspace, and widening later is a policy change inside one function.
- `roots_for_active_crates` and `source_mentions_crate` are deleted. The textual
  name-mention filter is not carried forward in any form — aliases and re-exports defeat
  it, and the direct-dependency closure replaces it with a well-defined set.

## 3. Invalidation (new contract, stated explicitly)

The old code re-ran workspace initialization on every file switch, which *incidentally*
picked up external `.rs` edits and `Cargo.toml`/lockfile changes. A persistent host does
not. This is an intentional trade, not "no regression":

- **Manifest/lockfile fingerprinting now**: `WorkspaceState` stores a content hash of
  the workspace `Cargo.toml`, member manifests, and `Cargo.lock` (content hash rather
  than mtime+size, sidestepping filesystem timestamp resolution). The fingerprint
  represents **absence** as a distinct state — a newly created `Cargo.lock` must
  invalidate. Checked before serving each request; on mismatch, drop and rebuild the
  entry (and its aliases). Cheap, and prevents analyzing against a stale crate graph
  after dependency changes.
- **External source-file changes** (a `.rs` edited outside the editor): the entry is
  stale until fingerprint-triggered or FileId-miss-triggered rebuild. Watching source
  files is deferred; an explicit reload command can force it in the meantime.

## 4. Request handling within one workspace

- `FileId` miss splits into two cases:
  - **New on-disk file under a known root** (path canonicalizes and sits under the
    workspace): rebuild that workspace entry once, then resolve. If it still doesn't
    resolve, single-file fallback. No silent empty output.
  - **Unsaved or non-canonicalizable path**: cannot join the workspace VFS through the
    path flow — single-file fallback immediately, no rebuild attempt.
- `diagnostic_mode` stays what it is today: `Full` for workspace-backed analysis,
  `SyntaxOnly` for the single-file fallback.
- Beneficial semantic change to lock in with a test: a shared host preserves unsaved
  edits to file A while analyzing file B (cross-file context from in-memory text). The
  old per-file host silently dropped that on every switch.

## 5. Explicitly deferred (out of scope)

- **Eviction**: distinct workspaces accumulate in the map for the session. Bounded by
  the number of distinct repos the user opens; fine.
- **External source-file watching** (beyond the fingerprint + rebuild paths in §3).
- **Per-buffer request coalescing/cancellation** in the semantic reactor queue:
  independent change, separate plan.

## 6. Verification

- Characterization first: capture highlight/diagnostic output for a fixture workspace
  file under the current code; assert identical output post-change (contract is
  outputs, not latency). Known intentional deltas: external-dependency highlight
  coverage may differ where the old textual-mention filter loaded a different set than
  the direct-dependency closure; capture and accept these explicitly.
- New tests:
  - two files in one workspace → second request performs no re-initialization
    (init counter or map-entry identity);
  - two files in **different members** of one Cargo workspace share one entry (the
    alias map works; no per-member initialization);
  - both get `Full` diagnostics;
  - manifest-less file still gets `SyntaxOnly`;
  - file added on disk after workspace init resolves via the one-shot rebuild path;
  - unsaved/non-canonicalizable path goes straight to single-file fallback;
  - `Cargo.toml`/`Cargo.lock` modification between requests triggers rebuild
    (fingerprint);
  - unsaved edits to file A are visible when analyzing file B (cross-file in-memory
    context preserved);
  - symlinked/relative path variants of one file resolve to a single `FileId`.
