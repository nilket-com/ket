#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	cancellation::*, descriptor_sync::*, resource_governance::*, scheduler_dispatch::*,
	tenant_admission::*, worker_lifecycle::*,
};

#[test]
pub(crate) fn adr544_file_composes_ordered_one_statement_requests_only() {
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read CLI");
	let script = std::fs::read_to_string(manifest_dir().join("src/sql_planner/script.rs"))
		.expect("read splitter");
	let wire = std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read wire");
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");

	assert!(cli.contains("for (statement_index, statement) in statements.iter().enumerate()"));
	assert!(cli.contains("execute_with_timezone(statement, session_timezone)"));
	assert!(script.contains("Token::SemiColon"));
	assert!(script.contains("Result<Vec<&str>, SqlScriptError>"));
	for forbidden in [
		"--continue-on-error",
		"SqlFileSequence",
		"MultiStatement",
		"TransactionCommand",
		"RollbackLoop",
	] {
		assert!(!cli.contains(forbidden), "CLI gained forbidden {forbidden}");
		assert!(
			!wire.contains(forbidden),
			"wire gained forbidden {forbidden}"
		);
		assert!(!ir.contains(forbidden), "IR gained forbidden {forbidden}");
	}
}

#[test]
pub(crate) fn adr544_file_source_is_bounded_and_borrowed() {
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read CLI");
	let script = std::fs::read_to_string(manifest_dir().join("src/sql_planner/script.rs"))
		.expect("read splitter");

	assert!(cli.contains(
		"const MAX_SQL_FILE_BYTES: usize = toron_client::SQL_STATEMENT_BYTES_HARD_CEILING as usize;"
	));
	assert!(cli.contains(".saturating_add(1)"));
	assert!(cli.contains("reader\n\t\t.take(read_limit)"));
	assert!(!cli.contains("std::fs::metadata"));
	assert!(script.contains("statements.push(statement)"));
	assert!(!script.contains("statement.to_string()"));
}

#[test]
pub(crate) fn adr544_failing_source_is_confined_to_ephemeral_cli_stderr() {
	let manifest = manifest_dir();
	let cli = std::fs::read_to_string(manifest.join("src/cli.rs")).expect("read CLI");
	let marker = "--- failing SQL ---";
	assert_eq!(cli.matches(marker).count(), 1);
	let reporter = brace_blocks_after_marker(&cli, "fn print_sql_file_failure")
		.into_iter()
		.next()
		.expect("file failure reporter")
		.1;
	assert!(reporter.contains("eprintln!"));
	for persistent in ["tracing::", "audit", "telemetry", "metric"] {
		assert!(
			!reporter.contains(persistent),
			"failing SQL entered persistent surface {persistent}"
		);
	}

	let offenders = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(path, _, line)| path != &manifest.join("src/cli.rs") && line.contains(marker))
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"failing SQL marker escaped CLI:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr544_help_and_product_fixture_use_direct_file_execution() {
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read CLI");
	let evidence = std::fs::read_to_string(
		manifest_dir()
			.join("../../../plans/product/impl/0072_percent_of_total_stacking_evidence.md"),
	)
	.expect("read PRODUCT-0072 evidence");

	assert!(cli.contains("(--file|-f) <path>"));
	assert!(evidence.contains("nilket toron sql --file ../uat/demo_percent_stacks.sql"));
	assert!(!evidence.contains("$(<demo_percent_stacks.sql)"));
}
