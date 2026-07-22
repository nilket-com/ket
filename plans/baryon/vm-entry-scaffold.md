# VM-entry scaffold unification

## Scope

Characterize the existing VM-entry behavior in these three paths before
extracting shared scaffolding:

- `RuneHost::complete_job`
- `RuneHost::invoke_autocmd`
- `DynamicActionExecutor<EditorHost>::invoke`

The refactor must preserve the current remove/restore lifetime boundary for a
generation, callback allowance rejection, signed-delta accounting, deferred
job finalization, deferred lifecycle application, effects, and error/status
behavior.

## Characterization tests

Add focused tests that exercise each entry point with the same matrix:

1. A valid callback runs only while its generation is removed from the host,
   and the generation is restored before accounting and deferred lifecycle
   application.
2. A retired/invalid owner or callback is rejected without entering the VM or
   consuming the generation.
3. Zero allowance rejects before VM entry and restores the generation.
4. A callback that allocates and releases retained data applies the signed
   delta exactly once, including negative deltas.
5. Deferred jobs commit only after successful callback completion; callback
   failure discards reservations.
6. Deferred source lifecycle actions run after generation restoration and
   report their status in the path-specific result channel.
7. Every entry resets the host-call budget to `DEFAULT_HOST_CALL_QUOTA` and
   clears prior `HostEffects` before invoking the callback.
8. Nested entry is rejected by the invocation gate; the generation removal and
   restoration boundary must not weaken that non-reentrancy invariant.

Keep the assertions path-specific where the public result differs: subscriber
outcomes for jobs/autocmds and `ActionError`/`DynamicActionOutcome` for dynamic
actions. Once these tests pin the behavior, extract the common entry scaffold
as a separate zero-behavior-change commit.
