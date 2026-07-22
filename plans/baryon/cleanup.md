# Specification: Shadowing System Cleanup & Maintenance

## Objective
Ensure the "Shadowing" system for large files does not leave orphaned `.baryon` directories or consume excessive disk space over time.

## 1. Session-End Cleanup
To prevent `.baryon` directories from becoming permanent, we should trigger a cleanup when they are no longer needed.
-   **Tab Close**: When a `DocTab` is closed, if it has an active `ShadowSession`, call `discard_shadow_session`.
-   **Editor Exit**: Ensure all active shadow sessions are discarded on a clean exit.
-   **Crash Recovery (Optional)**: If we want to support recovering large file edits after a crash, we can skip the automatic cleanup and rely on the manual `discard_shadow_session` or the Startup GC (see Section 3).

## 2. Generations Cap (Sliding Window)
Continuous editing on massive files can create hundreds of generation files.
-   **Implementation**: Introduce a `const MAX_GENERATIONS: usize = 50`.
-   **Pruning**: When `session.generations.len() > MAX_GENERATIONS`, delete the file associated with `session.generations[0]` and remove it from the vector.
-   **Alignment**: Ensure the `current_index` is adjusted correctly after pruning.

## 3. Startup Garbage Collection
Orphaned shadow directories (from crashes or force-quits) should be cleaned up.
-   **Mechanism**: A background task at startup that scans the project for `.baryon` directories.
-   **Criteria for Deletion**:
    -   The "Original File" specified in the `manifest` no longer exists.
    -   The directory is older than a certain threshold (e.g., 7 days) and is not currently open in an editor instance.

## 4. Post-Save Optimization
A "Real" save (`:w`) creates a new source of truth on disk.
-   **Strategy**: After a successful `save_document_atomic`, we can either:
    1.  Discard the entire shadow session (resetting undo history for the large file).
    2.  Keep only the most recent generation and re-base the shadow session on the newly saved file to free up space from older undo states.

## 5. Implementation Roadmap
1.  **Refactor `ShadowSession`**: Add a `prune_old_generations()` method.
2.  **Lifecycle Hooks**: Wire `discard_shadow_session` into the `Engine`'s tab-closing logic.
3.  **GC Utility**: Create a simple utility function in `src/engine/shadow.rs` for project-wide cleanup.
