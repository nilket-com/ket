use super::support::*;

const BINDER_MODULES: [&str; 18] = [
	"aggregate",
	"aggregate_exchange",
	"arithmetic",
	"cast",
	"decision_log",
	"explain",
	"expression_walk",
	"generated",
	"join",
	"join_order",
	"outer_count_preaggregation",
	"outer_join_demotion",
	"pushdown",
	"runtime_evidence",
	"scalar_function",
	"scalar_reduction",
	"selectivity",
	"set_ops",
];

#[test]
pub(crate) fn adr176_binder_source_concatenates_submodules() {
	let manifest = manifest_dir();
	let expected = [
		"src/binder.rs",
		"src/binder/aggregate.rs",
		"src/binder/aggregate_exchange.rs",
		"src/binder/arithmetic.rs",
		"src/binder/cast.rs",
		"src/binder/decision_log.rs",
		"src/binder/explain.rs",
		"src/binder/expression_walk.rs",
		"src/binder/generated.rs",
		"src/binder/join.rs",
		"src/binder/join_order.rs",
		"src/binder/outer_count_preaggregation.rs",
		"src/binder/outer_join_demotion.rs",
		"src/binder/pushdown.rs",
		"src/binder/runtime_evidence.rs",
		"src/binder/scalar_function.rs",
		"src/binder/scalar_reduction.rs",
		"src/binder/selectivity.rs",
		"src/binder/set_ops.rs",
		"src/binder/tests.rs",
	];
	let actual = binder_source_files()
		.into_iter()
		.map(|path| {
			path.strip_prefix(&manifest)
				.expect("binder source file under manifest")
				.to_string_lossy()
				.replace('\\', "/")
		})
		.collect::<Vec<_>>();

	assert_eq!(actual, expected);

	let support = std::fs::read_to_string(manifest.join("tests/architecture/support.rs"))
		.expect("read architecture support");
	assert_eq!(support.matches("pub(crate) fn binder_source()").count(), 1);
	assert!(support.contains("pub(crate) fn binder_source_files()"));
	assert!(support.contains("source_files(&module_root)"));
	assert!(support.contains("files.extend(production)"));
	assert!(support.contains("files.extend(tests)"));

	let binder = binder_source();
	for marker in [
		"struct Binder",
		"pub(super) fn aggregate_schema",
		"pub(super) fn bind_dimensioned_arithmetic",
		"pub(super) fn table_name",
		"pub(super) fn choose_join_strategy",
		"pub(super) fn analyze",
		"try_outer_count_preaggregation",
		"pub(super) fn demote_null_rejected_outer_joins",
		"pub(super) fn predicate_pushdown_plan",
		"pub(super) fn predicate_includes_primary_key_equality",
		"fn binder_resolves_columns_to_ordinals",
	] {
		assert!(binder.contains(marker), "missing binder marker {marker}");
	}
}

#[test]
pub(crate) fn adr176_no_scattered_binder_file_reads() {
	let architecture_dir = manifest_dir().join("tests/architecture");
	let allowed = [
		architecture_dir.join("support.rs"),
		architecture_dir.join("binder_decomposition.rs"),
	];
	let offenders = source_lines(&architecture_dir)
		.into_iter()
		.filter(|(path, _, line)| {
			!allowed.iter().any(|allowed| allowed == path)
				&& ((line.contains("source(\"src/binder.rs\")")
					|| line.contains("join(\"src/binder.rs\")"))
					|| (line.contains("read_to_string") && line.contains("src/binder.rs")))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"architecture tests still read src/binder.rs directly:\n{}",
		format_offenders(&manifest_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr556_statement_clock_capture_is_centralized() {
	let binder = binder_root_source();
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert_eq!(
		exec.matches("SystemTime::now()").count(),
		1,
		"the system wall clock belongs to the statement-clock implementation"
	);
	assert!(exec.contains("impl StatementClock for SystemStatementClock"));
	assert!(binder.contains("StatementContext::capture_system()"));
	assert!(coordinator.contains("for_statement_with_clock(self.statement_clock.as_ref())"));
	assert!(!binder.contains("capture_wall_clock_nanos()"));
	assert!(!coordinator.contains("capture_wall_clock_nanos()"));
}

#[test]
pub(crate) fn adr176_binder_root_excludes_carved_clusters() {
	let root = binder_root_source();
	assert!(root.contains("struct Binder"));
	assert!(root.contains("fn bind_plan_with_trace"));

	for carved in [
		"bind_aggregate_call",
		"aggregate_schema",
		"bind_dimensioned_arithmetic",
		"predicate_pushdown_plan",
		"choose_join_strategy",
		"predicate_includes_primary_key_equality",
		"table_name",
	] {
		assert!(
			!root.contains(&format!("fn {carved}(")),
			"binder root still defines carved cluster function {carved}"
		);
	}
}

#[test]
pub(crate) fn adr176_test_module_is_extracted() {
	let manifest = manifest_dir();
	let root = binder_root_source();
	let tests_path = manifest.join("src/binder/tests.rs");

	assert!(tests_path.exists(), "binder/tests.rs must exist");
	assert!(root.contains("#[cfg(test)]\nmod tests;"));
	assert!(
		!root.contains("mod tests {"),
		"binder root must not keep inline test module"
	);
}

#[test]
pub(crate) fn adr176_semantic_modules_exist() {
	let manifest = manifest_dir();
	let root = binder_root_source();

	for module in BINDER_MODULES {
		let path = manifest.join(format!("src/binder/{module}.rs"));
		assert!(path.exists(), "missing binder semantic module {module}");
		assert!(
			root.contains(&format!("mod {module};")),
			"binder root does not declare module {module}"
		);
	}
}

#[test]
pub(crate) fn adr176_binder_root_is_bounded() {
	let lines = binder_root_source().lines().count();
	// The post-ADR176 target tracks legitimate IR growth; ADR 241 added the `Window` bind + prune arms,
	// ADR 246 the `WindowFunction::Value` bind arm, ADR 255 the `RecursiveCte`/`RecursiveRef` bind arms
	// plus the `bind_recursive_cte` helper and the `recursive_ctx` threading through the dispatch; ADR
	// 263 added the `ScalarValue::Decimal128` literal coercion arms + the decimal-literal helpers; ADR 270
	// wired the comma-join equi recognition pass into the bind entry (kept inline so the demote→recognize→
	// bind sequence stays legible in the root, per adr209/adr216); ADR 279 added the `DeferredExists` bind
	// arm + the `bind_deferred_exists` resolve (the correlated-EXISTS frontend→binder handoff, incl. its
	// scratch-trace scope-probe binds so discarded plans don't pollute the optimizer trace); ADR 282 added the
	// `DeferredScalarSubquery` bind arm + the `bind_deferred_scalar_subquery` resolve (the correlated-scalar
	// frontend→binder handoff, reusing the same scratch-trace + ownership-classification machinery). ADR 308
	// added the logical ON-residual normalization fallback at bind entry. ADR 323 added the semi/anti
	// match-filter binding path for non-equi correlated EXISTS residuals. ADR 325 added exact decimal-point
	// literal preservation plus the literal-only Decimal→Float compatibility rewrites for pre-existing float
	// expression contexts. ADR 409 threads scan-size metadata from binder facts into the execution scan node.
	// ADR 466 made the hash arm honor SwappedRightBuild (the placement/rewrite contract), a real
	// rewrite-pass change. ADR 472 added the unknown-size fact and its shared helper. ADR 490 adds
	// the single-node raw-memory local-hash admission branch; it is a real physical-shape decision
	// guarded by the coordinator/fragmenter distributed rails, so it remains at this root boundary.
	// The admission decision also records a per-join receipt on the optimizer trace (the bare bool
	// proved unauditable in the first live cell), which is the small remainder of this bound.
	// ADR 511 adds the filtered-build-side-swap placement knob on `PlannerConfig` — a real
	// placement-pass decision surface, the same class as ADR 472's unknown-size fact. ADR 512
	// adds the reachable-key reduction pass: its knob, the deferred-path hook, and the
	// recombine-shape re-entry in the Join arm; the pass body lives in binder/scalar_reduction.rs.
	// ADR 515 adds the membership-first commute: its knob and the Join-arm re-entry hook; the
	// commute body also lives in binder/scalar_reduction.rs.
	// ADR 517 adds the side-local filter routing knob on `PlannerConfig`; the routing itself
	// lives at the reassociation site in binder/outer_join_demotion.rs.
	// ADR 518 adds only the membership-filtered reachable-key coverage knob at the root; indexed
	// subset derivation, directional semi traversal, and deferred-shape exposure live in
	// binder/scalar_reduction.rs; the Join arm carries only the terminating re-entry hook.
	// ADR 521 adds the proven-key outer-count knob and Aggregate-arm rewrite hook; the six-gate
	// recognizer and field-contract assertion live in binder/outer_count_preaggregation.rs.
	// ADR 537 adds three lines: one `PlannerConfig` switch, its default, and one doc line. It is
	// a configuration field, not a new pass or IR node — the lineage tracer it governs lives in
	// distributed/fragment.rs. Bumped by exactly its cost, not rounded up.
	// ADR 552 adds the terminal density bind arm and its globally-gathered child exchange.
	// ADR 553 adds the ScalarFunction bind arm; function-specific type binding lives in
	// binder/scalar_function.rs.
	// ADR 556 carries the statement timestamp through the binder entry points so a
	// per-statement scalar is fixed before a plan is fragmented.
	// ADR 569 adds the virtual Range leaf: its bind arm and exact-facts
	// constructor. Bumped by its cost.
	// ADR 572 threads the materialization certificate through the CTAS bind
	// arm and requires the recomputed canonical encoding to match it byte for
	// byte; the authoring bind and the recomputation live in binder/generated.rs.
	// ADR 579 adds the row-preserving projection arm to the safe-row-limit
	// traversal, so a top-level LIMIT reaches the scan through a column list.
	assert!(
		lines <= 6_472,
		"binder root has {lines} lines; expected <= 6,472 (bounded; bump only for real new IR nodes / passes)"
	);
}

#[test]
pub(crate) fn adr176_no_runtime_or_wire_surface_change() {
	let output = std::process::Command::new("git")
		.arg("-C")
		.arg(manifest_dir())
		.args(["status", "--porcelain", "--", "."])
		.output()
		.expect("run git status");
	assert!(
		output.status.success(),
		"git status failed: {}",
		String::from_utf8_lossy(&output.stderr)
	);

	let disallowed = String::from_utf8_lossy(&output.stdout)
		.lines()
		.filter_map(|line| line.get(3..))
		.map(|path| path.split(" -> ").last().unwrap_or(path))
		// PRODUCT-0001 grafted the Toron workspace under `nilket/` without
		// rewriting its history. Git reports paths from the unified repository
		// root, while this ADR-176 allowlist intentionally retains Toron's
		// original workspace-relative vocabulary.
		.map(|path| path.strip_prefix("nilket/").unwrap_or(path))
		.filter(|path| {
			let later_adr_test_and_runtime_surfaces = [
				"crates/toron/benches/distributed_inner_equi_join.rs",
				"crates/toron/benches/storage_execution.rs",
				"crates/toron/src/aggregate_kernel.rs",
				"crates/toron/src/audit.rs",
				"crates/toron/src/bin/toron_fixture_worker.rs",
				"crates/toron/src/coordinator.rs",
				"crates/toron/src/daemon.rs",
				"crates/toron/src/distributed/fragment.rs",
				// ADR 529: behavior-neutral live-shuffle backpressure receipt consumed only by
				// the explicit execution census rail.
				"crates/toron/src/distributed/partition_store.rs",
				"crates/toron/src/distributed/scheduler.rs",
				"crates/toron/src/distributed/task.rs",
				// ADR 340: BudgetExhausted carries a producer label so scale wall-movers can be
				// verified from retained diagnostics.
				"crates/toron/src/error.rs",
				"crates/toron/src/exec.rs",
				// ADR 350: semantic executor decomposition moves join substrate out of exec.rs.
				"crates/toron/src/exec/join.rs",
				// ADR 552: relation-side Gaussian density is a terminal executor module.
				"crates/toron/src/exec/density.rs",
				// ADR 396: join build row storage and env-gated build profiling live under the
				// existing join executor substrate.
				"crates/toron/src/exec/join/build_profile.rs",
				"crates/toron/src/exec/join/build_rows.rs",
				"crates/toron/src/exec/join/columns.rs",
				// ADR 438: the query-neutral fused global-fold join/filter diagnosis ledger stays
				// inside the semantic join executor split.
				"crates/toron/src/exec/join/global_fold_profile.rs",
				// ADR 443: query-neutral grouped-chain exclusive-span diagnostics stay inside the
				// semantic join executor split without replacing ADR 429 residency evidence.
				"crates/toron/src/exec/join/grouped_chain_profile.rs",
				// ADR 441: query-neutral membership-chain phase/operation diagnostics stay inside
				// the semantic join executor split.
				"crates/toron/src/exec/join/membership_chain_profile.rs",
				// ADR 351: join spill storage submodule inside the semantic executor split.
				"crates/toron/src/exec/join/spill.rs",
				// ADR 507: the query-neutral composed-gap attribution contract stays inside the
				// semantic join executor split; it is pure reconciliation arithmetic, no wire surface.
				"crates/toron/src/exec/join/stage_attribution.rs",
				// ADR 507: per-plan-node executor attribution — a query-neutral diagnostic keyed by
				// node kind, gated on an env path, emitting no wire/cluster struct.
				"crates/toron/src/exec/node_attribution.rs",
				// ADR 430: join spill placement policy stays inside the semantic join executor split.
				"crates/toron/src/exec/join/spill_placement.rs",
				// ADR 429: query-neutral grouped-reservoir diagnostics stay inside the semantic join
				// executor split.
				"crates/toron/src/exec/join/residency_profile.rs",
				// ADR 433: projected streamed-spilled LeftOuter diagnostics stay inside the semantic
				// join executor split.
				"crates/toron/src/exec/join/throughput_profile.rs",
				// ADR 446: local grouped-chain runtime-filter bridge reuses the ordinal builder.
				"crates/toron/src/exec/runtime_filter.rs",
				// ADR 352: join streaming orchestration submodule inside the semantic executor split.
				"crates/toron/src/exec/join/streaming.rs",
				// ADR 353: aggregate executor/state submodule inside the semantic executor split.
				"crates/toron/src/exec/aggregate.rs",
				// ADR 435: generic bound grouped folds share one hash-table/residency owner.
				"crates/toron/src/exec/aggregate/group_table.rs",
				// ADR 354: expression evaluator submodule inside the semantic executor split.
				"crates/toron/src/exec/expr.rs",
				"crates/toron/src/ir/execution.rs",
				"crates/toron/src/ir.rs",
				"crates/toron/src/lib.rs",
				"crates/toron/src/network/observability.rs",
				"crates/toron/src/rune_engine.rs",
				"crates/toron/src/server/mod.rs",
				// PRODUCT-0003: the append-only ExecuteRune request and typed failure
				// variants extend the public client protocol without exposing LogicalPlan.
				"crates/toron/src/sql_wire.rs",
				"crates/toron/src/sql_planner.rs",
				// ADR 544: the terminal CLI composes one-shot requests from exact source
				// slices; tokenizer ownership remains quarantined under sql_planner/.
				"crates/toron/src/cli.rs",
				"crates/toron/src/sql_planner/script.rs",
				// impl 535: the typed fragment-liveness origin joins the existing bounded
				// query-error telemetry classifier.
				"crates/toron/src/telemetry.rs",
				"crates/toron/tests/aggregate_projection_accounting_test.rs",
				"crates/toron/tests/catalog_logical_identity_test.rs",
				"crates/toron/tests/clause_accounting_test.rs",
				"crates/toron/tests/correlated_scalar_test.rs",
				"crates/toron/tests/cross_join_test.rs",
				// ADR 409: existing integration fixtures fill the new scan estimate metadata field.
				"crates/toron/tests/ctas_test.rs",
				// Destructive OR REPLACE is refused until staged publication exists.
				// The create and drop batteries assert that refusal and re-reach the
				// shared drop helper through explicit DROP; neither alters binder
				// decomposition ownership.
				"crates/toron/tests/create_table_test.rs",
				"crates/toron/tests/drop_table_test.rs",
				"crates/toron/tests/distributed_process_test.rs",
				"crates/toron/tests/exists_decorrelation_test.rs",
				"crates/toron/tests/explain_analyze_test.rs",
				"crates/toron/tests/group_by_test.rs",
				"crates/toron/tests/hash_join_fast_path_parity_test.rs",
				"crates/toron/tests/hash_join_kinds_test.rs",
				"crates/toron/tests/insert_test.rs",
				"crates/toron/tests/join_kind_test.rs",
				"crates/toron/tests/logical_type_ddl_test.rs",
				"crates/toron/tests/observability_listener_test.rs",
				"crates/toron/tests/predicate_widening_test.rs",
				"crates/toron/tests/qualified_resolution_test.rs",
				// ADR 357: focused executor regression for the generalized reoriented inner-chain shape.
				"crates/toron/tests/reoriented_inner_chain_streaming_test.rs",
				"crates/toron/tests/rune_relation_handles_test.rs",
				"crates/toron/tests/rune_window_test.rs",
				"crates/toron/tests/runtime_filter_pushdown_test.rs",
				// ADR 367: focused executor regression for scan-to-global-aggregate streaming.
				"crates/toron/tests/scan_global_aggregate_streaming_test.rs",
				"crates/toron/tests/sort_ordering_test.rs",
				"crates/toron/tests/wendelon_storage_execution_test.rs",
				"crates/toron/tests/window_test.rs",
				// ADR 305: SUBSTRING is a bound expression node and must explicitly decline PBC.
				"crates/toron/src/pbc.rs",
				// ADR 399: executor-internal join build maps use a private keyed fast hasher.
				"crates/toron/Cargo.toml",
				// ADR 462: the silent QueryBudget default was removed; these sites
				// became explicit test_default() choices.
				"crates/toron/src/catalog.rs",
				"crates/toron/src/distributed/budget.rs",
				"crates/toron/tests/config_test.rs",
				// ADR 509: query-neutral ownership accounting for the partitioned build gate.
				"crates/toron/src/exec/join/partitioned_build_profile.rs",
				// ADR 463: the worker config owns the coordinator query budget;
				// listener fixtures construct the widened worker config.
				"crates/toron/src/config.rs",
				"crates/toron/tests/daemon_test.rs",
				"crates/toron/tests/sql_listener_test.rs",
				"crates/toron/tests/tls_listener_test.rs",
				// ADR 512: the reachable-key reduction's execution equivalence battery.
				"crates/toron/tests/scalar_reduction_test.rs",
				// ADR 541: the caller's end-to-end deadline crosses the storage boundary;
				// the scan request carries it and every construction site fills it.
				"crates/toron/src/storage.rs",
				"crates/toron/src/coordinator/delete.rs",
				// ADR 542 widens storage-write fixtures with explicit operation identity
				// and caller deadlines; no binder behaviour changes.
				"crates/toron/tests/distributed_write_context_test.rs",
				"crates/toron/tests/update_test.rs",
				// ADR 543: query-scoped shuffle leases and typed retirement widen the
				// worker runtime after the binder-only decomposition record.
				"crates/toron/src/distributed/cancellation.rs",
				"crates/toron/src/distributed/mod.rs",
				"crates/toron/src/distributed/namespace_lifecycle.rs",
				"crates/toron/src/distributed/worker_registry.rs",
				// ADR 546 carries one execution seed and a conserved quantile-state
				// allowance through distributed task context; it does not alter binder
				// decomposition ownership.
				"crates/toron/src/distributed/context.rs",
				"crates/toron/src/error/distributed.rs",
				"crates/toron/src/wire.rs",
				"crates/toron-tpch/src/lib.rs",
				// ADR 551 meters scan-stream consumption so base-relation time is
				// attributed the same whether a read is bare or fused; it owns no
				// binder surface.
				"crates/toron/src/scan_metering.rs",
				"crates/toron/src/exec/scan_open.rs",
				"crates/toron/src/exec/phase_receipt.rs",
				"crates/toron/src/exec/join/spilled_hash.rs",
				"crates/toron/src/exec/join/streamed_spill.rs",
				"crates/toron/src/phase_ledger.rs",
				// ADR 521: proof-gated outer-count structural and equivalence battery.
				"crates/toron/tests/outer_count_preaggregation_test.rs",
				// ADR 559: one temporal format compiler serves rendering and
				// parsing; it is bind-time vocabulary, not binder decomposition.
				"crates/toron/src/temporal_format.rs",
				// ADR 553: EXTRACT and DATE_TRUNC moved into the typed scalar registry,
				// including their shared identity-bearing regression fixture.
				"crates/toron/src/ir/scalar_function.rs",
				// ADR 557: Date32 arithmetic and CURRENT_DATE add the session-zone
				// date conversion and checked temporal kernels.
				"crates/toron/src/date32.rs",
				"crates/toron/src/timestamp.rs",
				// ADR 564: the explicit-cast support table is bind-time policy the
				// binder and the executor conformance tests both read; it owns no
				// binder decomposition surface.
				"crates/toron/src/cast_support.rs",
				// ADR 570: the SIMILAR TO compiler is bind-time vocabulary, the
				// temporal_format precedent.
				"crates/toron/src/similar_to.rs",
				"crates/toron/tests/non_aggregate_projection_accounting_test.rs",
				// Plan filenames are zero-padded to four digits; this README documents a
				// command whose argument is one of the renamed implementation docs.
				"crates/toron/scripts/bench/README.md",
				// ADR 577: the SQL QUIC liveness policy is transport configuration.
				// It owns no binder surface; it exists because quinn's default
				// idle timeout was silently capping statement execution.
				"crates/toron/src/quic.rs",
				// KET-0006 / ADR 572: the canonical-expression lowerer and its
				// seed witnesses are the producer-side certificate boundary; it
				// owns no binder decomposition surface.
				"crates/toron/src/canonical_expression.rs",
				// ADR 0094: materialization write chaining changes the coordinator's
				// runtime write policy; it owns no binder decomposition surface.
				"crates/toron/src/coordinator/write.rs",
				// ADR 554 Branch A follow-on: the input-processing timing bracket is
				// executor telemetry, carved out of the executor root to keep ADR 354's
				// bound spent on evaluator logic. It owns no binder decomposition surface.
				"crates/toron/src/exec/input_processing.rs",
				// ADR 554 output-materialization line: the join output gather machinery —
				// run tracking, the lazy index build, the slice fast path and its
				// equivalence check — carved out of the join root behaviour-unchanged, for
				// the same reason. It owns no binder decomposition surface.
				"crates/toron/src/exec/join/output_materialization.rs",
				// Its input-side sibling: the concatenation bracket, its per-side split
				// and the shape counters that price a multi-batch redesign. Executor
				// telemetry, no binder decomposition surface.
				"crates/toron/src/exec/join/input_materialization.rs",
				// ADR 580: the batch-local probe route. An executor path change with its
				// own receipts and equivalence diagnostic; no binder decomposition
				// surface.
				"crates/toron/src/exec/join/batch_local_probe.rs",
			];
			*path != "crates/toron/src/binder.rs"
				&& !path.starts_with("crates/toron/src/binder/")
				&& !path.starts_with("crates/toron/tests/architecture/")
				&& !path.starts_with("crates/toron/plans/")
				&& !later_adr_test_and_runtime_surfaces.contains(path)
		})
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>();

	assert!(
		disallowed.is_empty(),
		"ADR176 must not touch runtime, wire, storage, Wendelon, descriptor, optimizer-fact, or frontend surfaces: {disallowed:?}"
	);
}
