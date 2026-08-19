//! ADR 561 — aggregate metadata registry guards.
//!
//! The registry is the single classification seam for aggregates: admitting a
//! function is a review decision made in `src/aggregate_kernel.rs`, and these
//! guards force that review the way `scalar_functions.rs` does for scalars.

use super::support::*;

#[test]
pub(crate) fn adr561_aggregate_catalog_is_the_single_classification_seam() {
	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source");
	assert!(lib.contains("mod aggregate_kernel;"));
	assert!(!lib.contains("pub mod aggregate_kernel"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("debug_assert_eq!(spec.kind, kernel);"));
	// Stop at the enum's own closing brace: the impl block that follows lists
	// every variant again in `ALL`, which would double the count.
	let stable_kernel_enum = catalog
		.split("pub(crate) enum StableAggregateKernel")
		.nth(1)
		.and_then(|rest| rest.split("\n}").next())
		.expect("stable aggregate kernel enum");
	// ADR 561: admitting an aggregate is a review decision. The exact set below
	// must change deliberately when one is added.
	let expected_kinds = [
		"CountAll",
		"Count",
		"Sum",
		"Min",
		"Max",
		"Avg",
		"ApproxQuantile",
		"VarPop",
		"VarSamp",
		"StddevPop",
		"StddevSamp",
		"LinearRegress",
		"StringAgg",
	];
	for variant in expected_kinds {
		assert!(
			stable_kernel_enum.contains(variant),
			"stable aggregate kernel missing {variant}"
		);
	}
	assert_eq!(enum_variant_count(stable_kernel_enum), expected_kinds.len());
	// The mapping from AggregateFunction is total, so a new variant fails to
	// compile rather than failing a query.
	assert!(catalog.contains(
		"pub(crate) fn stable_kernel_for_function(function: &AggregateFunction) -> StableAggregateKernel"
	));

	let stable_rows = catalog
		.split("STABLE_AGGREGATE_KERNELS")
		.nth(1)
		.and_then(|rest| rest.split("];").next())
		.expect("stable aggregate kernel rows");
	assert_eq!(
		stable_rows.matches("AggregateKernelSpec {").count(),
		expected_kinds.len()
	);
	for kind in expected_kinds {
		assert!(
			stable_rows.contains(&format!("kind: StableAggregateKernel::{kind}")),
			"catalog missing an entry for {kind}"
		);
	}
	for marker in [
		"arity: AggregateArity::Zero",
		"arity: AggregateArity::One",
		"arity: AggregateArity::Two",
		"input_domain: AggregateInputDomain::Any",
		"input_domain: AggregateInputDomain::SumNumeric",
		"input_domain: AggregateInputDomain::AvgNumeric",
		"input_domain: AggregateInputDomain::OrderedMinMax",
		"input_domain: AggregateInputDomain::QuantileNumeric",
		"output_rule: AggregateOutputRule::Int64",
		"output_rule: AggregateOutputRule::AvgNumeric",
		"output_rule: AggregateOutputRule::SameAsInput",
		"output_rule: AggregateOutputRule::SumNumeric",
		"output_rule: AggregateOutputRule::Float64",
		"logical_type: AggregateLogicalTypeRule::Naked",
		"logical_type: AggregateLogicalTypeRule::Preserve",
		"logical_type: AggregateLogicalTypeRule::AdditiveUnit",
		"order_sensitivity: AggregateOrderSensitivity::Insensitive",
		"order_sensitivity: AggregateOrderSensitivity::InsensitiveUnlessFloat",
		"order_sensitivity: AggregateOrderSensitivity::Sensitive",
		"window_eligible: true",
		"window_eligible: false",
		"state_family: AggregateStateFamily::Count",
		"state_family: AggregateStateFamily::Spread",
		"state_family: AggregateStateFamily::StringAgg",
		"input_domain: AggregateInputDomain::Utf8String",
		"nullable_for_nonempty: false",
		"nullable_for_nonempty: true",
		"nullable_for_empty: false",
		"nullable_for_empty: true",
		"grouped_supported: true",
		"grouped_supported: false",
	] {
		assert!(stable_rows.contains(marker), "catalog missing {marker}");
	}
	assert_eq!(stable_rows.matches("sql_name: \"COUNT\"").count(), 2);
	for sql_name in [
		"COUNT",
		"SUM",
		"MIN",
		"MAX",
		"AVG",
		"APPROX_QUANTILE",
		"VAR_POP",
		"VAR_SAMP",
		"STDDEV_POP",
		"STDDEV_SAMP",
		"LINEAR_REGRESS",
		"STRING_AGG",
	] {
		assert!(
			stable_rows.contains(&format!("sql_name: \"{sql_name}\"")),
			"catalog missing SQL name {sql_name}"
		);
	}
	// ADR 560's bare-name aliases live on their entries; a new alias must land
	// beside its canonical registration.
	assert!(stable_rows.contains("sql_aliases: &[\"VARIANCE\"]"));
	assert!(stable_rows.contains("sql_aliases: &[\"STDDEV\"]"));
	// Exactly one experimental kernel, flagged rather than excluded.
	assert_eq!(stable_rows.matches("experimental: true").count(), 1);
	assert!(
		stable_rows
			.split("LinearRegress")
			.nth(1)
			.is_some_and(|entry| entry.contains("experimental: true")),
		"the experimental kernel must be flagged in its registry entry"
	);
	// Every entry's Rune spelling is registered with the engine; the field has
	// no runtime reader, so this scan is its enforcement.
	let rune_source = std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read rune engine source");
	for row in stable_rows.split("rune_name: \"").skip(1) {
		let rune_name = row.split('"').next().expect("rune name literal");
		assert!(
			rune_source.contains(&format!("m.function([\"{rune_name}\"]")),
			"rune surface missing aggregate spelling {rune_name}"
		);
	}
}

#[test]
pub(crate) fn adr028_experimental_kernels_excluded_from_grouped_signatures() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	let mapping = brace_blocks_after_marker(&catalog, "fn stable_kernel_for_function")
		.into_iter()
		.next()
		.expect("stable kernel for function")
		.1;
	// ADR 561: the mapping is total — the experimental kernel has an entry and
	// its exclusion from grouped signatures reads `grouped_supported` instead
	// of a None arm.
	assert!(
		mapping
			.contains("AggregateFunction::LinearRegress => StableAggregateKernel::LinearRegress")
	);
	assert!(
		mapping.contains(
			"AggregateFunction::ApproxQuantile(_) => StableAggregateKernel::ApproxQuantile"
		)
	);
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
		.into_iter()
		.next()
		.expect("grouped aggregate signature")
		.1;
	assert!(signature.contains("if !spec.grouped_supported"));
	assert!(signature.contains("return None"));
	let linreg_entry = catalog
		.split("kind: StableAggregateKernel::LinearRegress,")
		.nth(1)
		.and_then(|rest| rest.split("},").next())
		.expect("LinearRegress registry entry");
	assert!(linreg_entry.contains("grouped_supported: false"));
}

#[test]
pub(crate) fn adr563_refused_quantile_names_are_an_exact_set() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	let refused = brace_blocks_after_marker(&catalog, "fn refused_aggregate_sql_name")
		.into_iter()
		.next()
		.expect("refused aggregate names")
		.1;
	// ADR 563: a name leaves this set only through the ADR that decides exact
	// quantile buffering, spill, and accounting. Additions are review decisions
	// for the same reason.
	for name in ["\"MEDIAN\"", "\"PERCENTILE_CONT\"", "\"PERCENTILE_DISC\""] {
		assert!(refused.contains(name), "refusal table missing {name}");
	}
	// Count the quoted SCREAMING_CASE tokens — the name patterns — rather than
	// raw quotes, which the directive messages also contain.
	let refused_names = refused
		.split('"')
		.skip(1)
		.step_by(2)
		.filter(|content| {
			!content.is_empty()
				&& content
					.chars()
					.all(|character| character.is_ascii_uppercase() || character == '_')
		})
		.count();
	assert_eq!(
		refused_names, 3,
		"the refusal table holds exactly its three names; changing the set is a review decision"
	);
	// The directive must name the honest alternative, or it is a bare refusal.
	assert_eq!(refused.matches("APPROX_QUANTILE").count(), 2);

	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("refused_aggregate_message(&f.name)"));
}
