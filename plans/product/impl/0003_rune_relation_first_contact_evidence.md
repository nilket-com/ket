# Rune relation first-contact evidence

Status: automated implementation evidence complete 2026-08-01; installed
local-profile and experiential acceptance remain pending. Parent:
`0003_rune_relation_first_contact.md`. This record does not claim that a result
stream, durable relation, or PlotSpec exists.

## The implemented thread

Polariton structurally removes only the terminal `table`, `plot.bar`, or
`plot.scatter` presentation call. The remaining Rune source crosses
`toron-client` as a versioned `ExecuteRune` request. Toron's existing
`RuneEngine` produces its private `LogicalPlan`; the normal coordinator and
Wendelon execution path returns Arrow IPC under the inherited 128 MiB client
ceiling. Polariton converts that bounded result through the existing
compatibility quarantine and feeds the existing table and plot surfaces.

No SQL printer, product-side relation algebra, `LogicalPlan` wire type, or
parallel ingress was added. The wire mapping from Toron's internal typed
sandbox violations to the public protocol is exhaustive and does not inspect
error prose. RuneEngine's older internal diagnostic classification remains an
implementation detail and was not widened into the protocol.

Rune programs without a terminal relation view continue through Polariton's
existing local Rune host. The new route is selected only by a structurally
recognized terminal relation view; malformed recognized view calls refuse
with Rune-view guidance rather than falling through.

## Automated evidence

- `rune_source_executes_over_the_authenticated_query_boundary_with_typed_failures`
  uses real Toron, real Wendelon, and the public client. One composed source
  exercises table lookup, filter, aggregate, sort, limit, and projection. The
  same fixture proves typed sandbox and compile failures, a compile span, and
  Rune—not SQL—audit attribution.
- `toron_sql_select_executes_over_authenticated_remote_listener` sends both SQL
  and Rune through the same mTLS listener and proves the Rune workspace uses
  the authenticated tenant.
- `each_rune_version_axis_refuses_skew_with_requested_and_supported_versions`
  exercises request, language, and sandbox skew independently.
- `collector_preserves_typed_rune_failure_and_span`,
  `rune_results_inherit_the_bounded_collector_ceiling`, and the postcard
  discriminant tests preserve client categories, the collection bound, and
  append-only wire identity.
- `terminal_table_is_substituted_and_mapped` proves arithmetic source mapping
  and terminal-view fallback. `plot_forms_retain_only_view_intent` and
  `rune_relation_results_feed_the_existing_table_and_plot_surfaces` prove that
  both geometries reuse the current presentation path.
- `scheduler_executes_in_flight_then_only_the_newest_pending_view` records the
  deterministic work census: three submissions, two executions, one request
  superseded before execution. Cancellation also clears the buffer's pending
  and in-flight identity.
- `unavailable_workspace_refuses_locally_with_attach_guidance` proves an
  adapter without the capability performs no network exchange and points at
  `nilket`.
- `product_0003_terminal_envelope_is_the_only_relation_view_transformation_site`
  carries a synthetic positive control and keeps private Toron relation/plan
  types out of Polariton.

## The seven measurements

1. **Rune spellings:** implemented spelling is exact and deliberately small:
   `workspace.table`, `project`, `filter`, `aggregate`, `sort`, `limit`, then a
   terminal positional view call. Whether it feels natural is pending lived
   use; the sink spelling remains sentenced to die with the plotting ADR.
2. **One relation to table and plot:** composition is proven automatically;
   lived acceptance is pending.
3. **Obsolete work:** counters are `submitted`, `executed`, and
   `superseded_before_execution`; the deterministic burst reads `3 / 2 / 1`.
4. **Rune-native refusals:** compile and sandbox categories cross the real
   transport, compile spans map arithmetically, and wrapper-only spans attach
   to the terminal expression. The felt loss of pre-network semantic refusal
   remains a dogfood question.
5. **Relation vs finite frame vs aggregate:** the temporary sinks currently
   consume the bounded relation result. No conclusion about the eventual
   plotting execution policy is claimed.
6. **Revision semantics:** newest-wins request revisions exist; durable plot
   reopening does not, so this question remains pending by design.
7. **Default invisibility:** the capability noun is implemented and engine
   brands do not enter Rune source. Whether `workspace.table` disappears into
   daily use remains Joel's verdict.

## Live-profile gate

`live_rune_relation_reaches_table_bar_and_scatter_surfaces` is ignored by
default because it requires the newly built protocol on the installed local
deployment and Joel's `test2` relation. It is read-only, runs the same relation
through table, bar, and scatter, and exercises the existing plot kernel. It
must run after the deliberate install/restart; this implementation did not
restart or mutate the deployment that was serving during development.

The three view rows in the acceptance corpus remain pending until that gate
and the human first-contact pass succeed. That is evidence discipline, not an
implementation omission.

## Validation snapshot

- Toron library: 1,478 passed.
- Toron SQL/Rune listener: 26 passed.
- Toron architecture: 2,098 passed.
- Toron native Rune relation integration: 106 passed.
- `toron-client`: 13 unit + 3 architecture passed.
- Polariton with Toron: 139 passed, 3 live fixtures ignored; 10 architecture
  passed.
- Polariton default: 131 + 10 architecture passed.

Focused Clippy completed with no new implementation warning. Existing debt
remains visible: two Polariton warnings and Toron's previously recorded lint
backlog.
