use super::support::*;

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr132_dominance_comparator_in_single_helper() {
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert_eq!(
		binder.matches("fn right_build_materially_smaller(").count(),
		1
	);
	assert_eq!(
		binder
			.matches("const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR")
			.count(),
		1
	);
	assert!(choose_auto.contains("right_build_materially_smaller(left, right)"));
	assert!(choose_auto.contains("let left_viable"));
	assert!(choose_auto.contains("let right_viable"));
	assert!(choose_auto.contains("match (left_viable, right_viable)"));
	assert!(!bind_join.contains("right_build_materially_smaller"));
	assert!(!bind_join.contains("AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR"));
	assert!(!bind_join.contains("rejected_side_bytes"));
}

#[test]
pub(crate) fn adr132_dominance_is_integer_ratio_over_hard_bytes() {
	let binder = binder_source();
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");
	let helper = source_block(&binder, "fn right_build_materially_smaller");

	assert!(binder.contains("const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR: u64 = 2"));
	assert!(helper.contains("u128::from(right_bytes.get())"));
	assert!(helper.contains("u128::from(AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR)"));
	assert!(helper.contains("<= u128::from(left_bytes.get())"));
	assert!(choose_auto.contains("left_build_bytes"));
	assert!(choose_auto.contains("right_build_bytes"));
	assert!(choose_auto.contains("byte_upper_bound"));
	assert!(choose_auto.contains("filter(|build_bytes| *build_bytes < threshold)"));
	for forbidden in [
		"f32",
		"f64",
		"0.8",
		"row_upper_bound",
		"confidence",
		"selectivity",
		"saturating_mul",
	] {
		assert!(
			!helper.contains(forbidden),
			"dominance comparator contains forbidden marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr132_left_biased_hysteresis() {
	let binder = binder_source();
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(choose_auto.contains("right_build_materially_smaller(left, right)"));
	assert!(choose_auto.contains("BroadcastBuildSide::Right"));
	assert!(choose_auto.contains("JoinPhysicalPlacement::SwappedRightBuild"));
	assert!(choose_auto.contains("(Some(left), Some(right)) => Some(("));
	assert!(choose_auto.contains("BroadcastBuildSide::Left"));
	assert!(choose_auto.contains("JoinPhysicalPlacement::Natural"));
	assert!(binder.contains("both_sides_tiny_equal_keeps_left_build"));
	assert!(binder.contains("both_sides_eligible_near_tie_keeps_left_build"));
}

#[test]
pub(crate) fn adr132_broadcast_inferred_receipt_carries_rejected_side_bytes() {
	let binder = binder_source();
	let reason = source_block(&binder, "enum JoinStrategyDecisionReason");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(reason.contains("rejected_side_bytes: Option<ByteCount>"));
	assert!(choose_auto.contains("rejected_side_bytes"));
	assert!(choose_auto.contains("Some(left)"));
	assert!(choose_auto.contains("Some(right)"));
	assert!(choose_auto.contains("None"));
	assert!(binder.contains("sole_eligible_side_reports_no_rejected_bytes"));
}

#[test]
pub(crate) fn adr132_no_config_or_public_explain_surface() {
	let manifest_dir = manifest_dir();
	let binder = binder_source();
	let planner_config = source_block(&binder, "pub struct PlannerConfig");

	assert!(!planner_config.contains("dominance"));
	assert!(!planner_config.contains("rejected_side_bytes"));
	assert!(!planner_config.contains("right_build_materially_smaller"));

	let mut offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			path.file_name().and_then(|name| name.to_str()) != Some("binder.rs")
				&& (line.contains("rejected_side_bytes")
					|| line.contains("AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR")
					|| line.contains("right_build_materially_smaller"))
		})
		.collect::<Vec<_>>();
	offenders.extend(
		source_lines(&manifest_dir.join("src"))
			.into_iter()
			.filter(|(_, _, line)| {
				line.contains("EXPLAIN") && line.contains("rejected_side_bytes")
			}),
	);
	assert!(
		offenders.is_empty(),
		"ADR132 receipt/config leaked into public source surface:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr132_no_executor_fragmenter_scheduler_wire_or_wendelon_change() {
	let manifest_dir = manifest_dir();
	let markers = [
		"AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR",
		"right_build_materially_smaller",
		"rejected_side_bytes",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR132 dominance comparator leaked into execution/distributed/wire/Wendelon substrate:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}
