//! ADR 536 Phase 0: fragment-time publication resolution and scheduler admission must be
//! told the same concurrent-task cap.
//!
//! A live shuffle component can only drain if every one of its tasks can be resident at
//! once. The scheduler enforces that budget (`in_flight_count < limit`); the fragmenter
//! decides publication mode. If the fragmenter cannot see the cap, it can elect a live
//! component the scheduler will never fully admit, and the producers block forever on an
//! undrained partition. This guard pins the wiring, not the policy — Phase 0 changes no
//! publication behavior.

use super::support::manifest_dir;

/// Every site that hands the admission cap to a `DistributedScheduler` must hand the same
/// expression to the `FragmentPlanConfig` fragmented for that scheduler. The two wirings
/// are written identically, so each scheduler construction accounts for exactly two
/// occurrences of the shared expression: one on the fragment config, one on the scheduler.
#[test]
fn adr536_fragment_and_scheduler_receive_the_same_admission_cap() {
	const CAP_WIRING: &str = ".with_max_in_flight_tasks(query_budget.max_in_flight_tasks)";
	const SCHEDULER_CONSTRUCTION: &str = "DistributedScheduler::new(";

	let sources = ["src/coordinator.rs", "src/lib.rs"];
	let mut violations = Vec::new();

	for relative in sources {
		let path = manifest_dir().join(relative);
		let source = std::fs::read_to_string(&path)
			.unwrap_or_else(|error| panic!("read {}: {error}", path.display()));

		// A scheduler built without a cap is unbounded and fragments nothing that needs
		// pairing; only a capped construction implies a fragmented plan that must see the
		// same cap. The builder call follows the constructor closely, so a bounded window
		// is enough to tell the two apart.
		let capped_schedulers = source
			.match_indices(SCHEDULER_CONSTRUCTION)
			.filter(|(index, _)| {
				let tail_start = index + SCHEDULER_CONSTRUCTION.len();
				let tail_end = source.len().min(tail_start + 200);
				source
					.get(tail_start..tail_end)
					.is_some_and(|tail| tail.contains(".with_max_in_flight_tasks("))
			})
			.count();
		let cap_wirings = source.matches(CAP_WIRING).count();

		if cap_wirings != capped_schedulers * 2 {
			violations.push(format!(
				"{relative}: {capped_schedulers} capped scheduler construction(s) need {} cap \
				 wiring(s) (one on the fragment config, one on the scheduler), found {cap_wirings}",
				capped_schedulers * 2
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 536: fragment-time resolution and scheduler admission must share one cap; \
		 see plans/adr/0536_admission_safe_live_publication.md\n{}",
		violations.join("\n")
	);
}

/// `None` is unbounded and is the default, so every plan fragmented without an explicit
/// cap keeps its pre-ADR-536 shape. The builder floors a supplied limit at one, matching
/// `DistributedScheduler::with_max_in_flight_tasks`, so the two can never disagree about
/// what a zero means.
#[test]
fn adr536_admission_cap_default_is_unbounded_and_floors_at_one() {
	use toron::distributed::budget::{QueryBudget, TaskBudget};
	use toron::distributed::fragment::FragmentPlanConfig;
	use toron::ir::QueryId;

	let base = FragmentPlanConfig::new(
		QueryId::new(),
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	);
	assert_eq!(
		base.max_in_flight_tasks, None,
		"default admission cap must be unbounded so existing plans keep their shape"
	);

	assert_eq!(
		base.clone()
			.with_max_in_flight_tasks(None)
			.max_in_flight_tasks,
		None,
		"an explicit None stays unbounded"
	);
	assert_eq!(
		base.clone()
			.with_max_in_flight_tasks(Some(6))
			.max_in_flight_tasks,
		Some(6),
		"a supplied cap is carried verbatim"
	);
	assert_eq!(
		base.with_max_in_flight_tasks(Some(0)).max_in_flight_tasks,
		Some(1),
		"zero floors to one, matching the scheduler builder"
	);
}

/// ADR 537: the runtime-filter probe target is proven at fragmentation and carried, never
/// rebuilt. Each property below could regress silently while every behavioral control
/// still passed.
#[test]
fn adr537_runtime_filter_target_is_carried_not_reconstructed() {
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragment source");

	// 1. The reconstruction helper stays deleted — the one-line path back to pairing a
	//    scan fingerprint with a post-projection ordinal.
	assert!(
		!scheduler.contains("fn first_scan_fingerprint"),
		"ADR 537: first_scan_fingerprint paired facts from different schemas and must not \
		 return; the probe target is carried on the producer binding instead"
	);

	// 2. Descriptor construction consumes the carried proof.
	let descriptor_fn = scheduler
		.split("fn runtime_filter_descriptor_for_probe")
		.nth(1)
		.expect("descriptor constructor");
	assert!(
		descriptor_fn.contains("runtime_filter_probe_target: Some(target)"),
		"ADR 537: the descriptor must be built from the carried target"
	);
	assert!(
		descriptor_fn.contains("target.physical_column_ordinal")
			&& descriptor_fn.contains("target.probe_descriptor_fingerprint"),
		"ADR 537: both the ordinal and the fingerprint must come from that one target"
	);

	// 3. The tracer keeps a fail-closed fallback. It is deliberately not held to the
	//    exhaustive-leaf discipline of other plan walkers: an unrecognized node must lose
	//    the optimization, never acquire an unproven filter.
	let tracer = fragment
		.split("fn trace_runtime_filter_probe_target")
		.nth(1)
		.expect("tracer body");
	assert!(
		tracer.contains("_ => None"),
		"ADR 537: the lineage tracer must fail closed on any node it does not understand"
	);
}
