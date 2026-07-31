# ADR 463 implementation receipt

The coordinator query budget is config-owned. `[worker]` gains two optional knobs —
`query_memory_per_task_bytes` and `query_spill_bytes_reserved_bytes` — validated at
load (zero rejected loudly), threaded through
`ToronWorkerConfig::query_budget()`, and consumed at the daemon's coordinator
construction in place of the bare test constant. Absent knobs are byte-identical to
before; no derivation formula shipped, per the plan's fork decision.

## Shape

- Raw and public config structs widened; the mapping validates via
  `validate_optional_budget_bytes` (`ToronConfigError::Invalid`, matching the gauge
  knobs' loud-rejection contract).
- `query_budget()` starts from the explicit test constant and overrides exactly the
  present coordinates — proven by the equality assertion against a
  field-patched `test_default()` in the round-trip test.
- The example config documents both knobs (commented out) with a pointer to the 455
  frontier vocabulary for what `memory_per_task` means.
- Four listener/daemon test fixtures gained the two `None` fields (compile-forced —
  the widened struct is itself a guard against silent construction).

## Tests and gates

- `adr463_worker_query_budget_knobs_parse_override_and_reject_zero`: absent → test
  constant; present → exactly the two coordinates change; zero → config load error.
- `adr463_daemon_coordinator_budget_is_config_owned` (architecture): the daemon site
  must flow through `query_budget()` — a bare constant there would orphan the knobs
  silently.
- Full architecture_test **2,093/0** (adr176's surface allowlist extended for
  config.rs and the listener fixtures via its established mechanism); workspace
  `check --all-targets` clean; full engine suite green.
- Behavior-neutrality with absent knobs is relied upon; no perf claims.

## Forward

Evidence-priced derivation (a gauge for coordinator budget policy — what fraction of
`memory_budget_bytes` per task, argued with 455's frontier law) remains the named
successor. Operators can now express any policy explicitly in the meantime.
