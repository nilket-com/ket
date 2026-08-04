use super::support::*;

const BINDER_MODULES: [&str; 14] = [
	"aggregate",
	"aggregate_exchange",
	"arithmetic",
	"decision_log",
	"explain",
	"join",
	"join_order",
	"outer_count_preaggregation",
	"outer_join_demotion",
	"pushdown",
	"runtime_evidence",
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
		"src/binder/decision_log.rs",
		"src/binder/explain.rs",
		"src/binder/join.rs",
		"src/binder/join_order.rs",
		"src/binder/outer_count_preaggregation.rs",
		"src/binder/outer_join_demotion.rs",
		"src/binder/pushdown.rs",
		"src/binder/runtime_evidence.rs",
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
	assert!(
		lines <= 6_275,
		"binder root has {lines} lines; expected <= 6,275 (bounded; bump only for real new IR nodes / passes)"
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
				// ADR 521: proof-gated outer-count structural and equivalence battery.
				"crates/toron/tests/outer_count_preaggregation_test.rs",
				// Plan filenames are zero-padded to four digits; this README documents a
				// command whose argument is one of the renamed implementation docs.
				"crates/toron/scripts/bench/README.md",
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
