use super::support::*;
use std::collections::BTreeSet;

fn source(path: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(path)).unwrap_or_else(|_| panic!("read {path}"))
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr149_date32_selectivity_breadth_is_typed_and_estimate_only() {
	let binder = binder_source();
	let ir = source("src/ir.rs");
	let planner = source("src/sql_planner.rs");
	let exec = exec_source();
	let rune = source("src/rune_engine.rs");
	let date32 = source("src/date32.rs");
	let write = source("src/coordinator/write.rs");
	let pbc = source("src/pbc.rs");
	let create_table_test = source("tests/create_table_test.rs");
	let insert_test = source("tests/insert_test.rs");
	let ctas_test = source("tests/ctas_test.rs");
	let logical_type_test = source("tests/logical_type_ddl_test.rs");
	let cli = source("src/cli.rs");
	let scalar = source_block(&ir, "pub enum ScalarValue");
	let insert_literal = source_block(&ir, "pub enum InsertLiteral");
	let lower_create = source_block(&planner, "fn lower_create_table_data_type");
	let lower_insert = source_block(&planner, "fn lower_insert_literal");
	let lower_expr = source_block(&planner, "fn lower_expr");
	let typed_null = source_block(&binder, "fn typed_null_for_data_type");
	let ctas_lattice = source_block(&binder, "fn is_supported_ctas_column_type");
	let coerce_insert = source_block(&binder, "fn coerce_insert_literal");
	let coerce_compare = source_block(&binder, "fn coerce_literal_value");
	let in_list_scalar = source_block(&ir, "pub fn scalar_literal_key");
	let in_list_key = source_block(&ir, "pub enum ScalarLiteralKey");
	let carrier_literal = source_block(&binder, "enum CarrierLiteral");
	let carrier_domain = source_block(&binder, "enum CarrierDomain");
	let range_scalar = source_block(&binder, "fn range_scalar_for_fact");
	let decode_range = source_block(&binder, "fn decode_range_scalar");
	let sql_keyword = source_block(&binder, "fn sql_carrier_keyword");
	let pbc_eligible = source_block(&binder, "fn is_pbc_eligible_type");
	let encode_partition = source_block(&exec, "fn encode_partition_key_value");
	let pbc_literal = source_block(&pbc, "fn emit_literal");
	let write_build_array = source_block(&write, "fn build_array");

	for required in [
		"parse_date_ymd",
		"format_date_ymd",
		"expected YYYY-MM-DD",
		"month out of range",
		"day out of range",
		"rejects_noncanonical_or_invalid_dates",
	] {
		assert!(
			date32.contains(required),
			"Date32 helper missing {required}"
		);
	}
	assert!(scalar.contains("Date32(Option<i32>)"));
	assert!(ir.contains("Self::Date32(_) => DataType::Date32"));
	assert!(ir.contains("format_date_ymd"));
	assert!(insert_literal.contains("Date32(i32)"));
	assert!(lower_create.contains("SqlDataType::Date | SqlDataType::Date32"));
	assert!(lower_create.contains("CreateTableColumnType::Physical(DataType::Date32)"));
	assert!(lower_insert.contains("SqlExpr::TypedString"));
	assert!(lower_insert.contains("InsertLiteral::Date32"));
	assert!(lower_expr.contains("ScalarValue::Date32(Some(value))"));
	assert!(planner.contains("fn is_date_sql_type"));
	assert!(typed_null.contains("DataType::Date32"));
	assert!(ctas_lattice.contains("DataType::Date32"));
	assert!(coerce_insert.contains("InsertLiteral::Date32(value)"));
	assert!(coerce_insert.contains("DataType::Date32 => Ok(ScalarValue::Date32"));
	assert!(coerce_compare.contains("(ScalarValue::Utf8(Some(value)), DataType::Date32)"));
	assert!(coerce_compare.contains("parse_date_ymd"));
	assert!(!coerce_compare.contains("DataType::Date32) => ScalarValue::Int32"));
	assert!(in_list_key.contains("Date32(i32)"));
	assert!(in_list_scalar.contains("ScalarValue::Date32(Some(value))"));
	assert!(carrier_literal.contains("Date32(i32)"));
	assert!(carrier_domain.contains("Date32(OrderedDomain<i32>)"));
	assert!(range_scalar.contains("ARROW_STATS_TAG_DATE32"));
	assert!(range_scalar.contains("ScalarValue::Date32(Some(value))"));
	assert!(range_scalar.contains("RangeScalar::Integral"));
	assert!(decode_range.contains("ARROW_STATS_TAG_DATE32"));
	assert!(decode_range.contains("decode_signed_optimizer_fact_scalar"));
	assert!(sql_keyword.contains("DataType::Date32 => Ok(\"DATE\")"));
	assert!(!pbc_eligible.contains("DataType::Date32"));
	assert!(pbc_literal.contains("ScalarValue::Date32(Some(_))"));
	assert!(pbc_literal.contains("type-preserving PBC support"));
	assert!(encode_partition.contains("DataType::Date32"));
	assert!(encode_partition.contains("downcast_ref::<Date32Array>"));
	assert!(encode_partition.contains("encoded.push(13)"));
	assert!(write_build_array.contains("DataType::Date32"));
	assert!(write_build_array.contains("Date32Builder::with_capacity"));
	assert!(rune.contains("pub fn lit_date(value: String)"));
	assert!(rune.contains("ScalarValue::Date32(Some(value))"));
	assert!(rune.contains("m.function([\"lit_date\"], lit_date).build()?;"));
	assert!(
		binder.contains(
			"date32_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate"
		)
	);
	assert!(binder.contains("date32_range_facts_keep_discrete_interpolation"));
	assert!(binder.contains("date32_in_list_ndv_estimates_distinct_non_null_literals"));
	assert!(binder.contains("date32_comparisons_coerce_bare_string_literals_only_against_date32"));
	assert!(create_table_test.contains("created_on DATE"));
	assert!(insert_test.contains("InsertLiteral::Date32"));
	assert!(ctas_test.contains("query_coordinator_executes_date32_projection_ctas"));
	assert!(logical_type_test.contains("Some(\"DATE\".to_string())"));
	assert!(cli.contains("fn date32_select_output_renders_iso_date"));
	assert!(cli.contains("format_batches_for_display"));
}

#[test]
pub(crate) fn adr150_timestamp_is_tz_anchored_and_estimate_only() {
	let binder = binder_source();
	let ir = source("src/ir.rs");
	let planner = source("src/sql_planner.rs");
	let exec = exec_source();
	let write = source("src/coordinator/write.rs");
	let pbc = source("src/pbc.rs");
	let rune = source("src/rune_engine.rs");
	let cli = source("src/cli.rs");
	let daemon = source("src/daemon.rs");
	let sql_wire = source("../toron-client/src/protocol.rs");
	let timestamp = source("src/timestamp.rs");
	let ctas_test = source("tests/ctas_test.rs");
	let rune_test = source("tests/rune_relation_handles_test.rs");
	let scalar = source_block(&ir, "pub enum ScalarValue");
	let insert_literal = source_block(&ir, "pub enum InsertLiteral");
	let lower_create = source_block(&planner, "fn lower_create_table_data_type");
	let lower_insert = source_block(&planner, "fn lower_insert_literal");
	let lower_expr = source_block(&planner, "fn lower_expr");
	let typed_null = source_block(&binder, "fn typed_null_for_data_type");
	let ctas_lattice = source_block(&binder, "fn is_supported_ctas_column_type");
	let coerce_insert = source_block(&binder, "fn coerce_insert_literal");
	let coerce_compare = source_block(&binder, "fn coerce_literal_value");
	let in_list_scalar = source_block(&ir, "pub fn scalar_literal_key");
	let in_list_key = source_block(&ir, "pub enum ScalarLiteralKey");
	let carrier_literal = source_block(&binder, "enum CarrierLiteral");
	let carrier_domain = source_block(&binder, "enum CarrierDomain");
	let range_scalar = source_block(&binder, "fn range_scalar_for_fact");
	let decode_range = source_block(&binder, "fn decode_range_scalar");
	let sql_keyword = source_block(&binder, "fn sql_carrier_keyword");
	let pbc_literal = source_block(&pbc, "fn emit_literal");
	let literal_array = source_block(&exec, "fn timestamp_literal_array");
	let encode_partition = source_block(&exec, "fn encode_partition_key_value");
	let group_array = source_block(&exec, "fn group_column(");
	let write_build_array = source_block(&write, "fn build_array");
	let write_timestamp_array = source_block(&write, "fn build_timestamp_array");
	let restamp = source_block(&daemon, "fn restamp_sql_result_field");

	for required in [
		"parse_session_timezone",
		"parse_explicit_timestamptz",
		"parse_wall_clock_timestamp",
		"parse_timestamp_text",
		"LocalResult::None",
		"LocalResult::Ambiguous",
		"wall-clock time does not exist in timezone",
		"timestamp is not exactly representable at millisecond precision",
	] {
		assert!(
			timestamp.contains(required),
			"timestamp helper missing {required}"
		);
	}
	// ADR 263 added a multi-line `Decimal128` struct variant, so rustfmt now renders the sibling
	// `Timestamp` struct variant multi-line too — assert the tz-anchored shape format-agnostically.
	assert!(
		scalar.contains("Timestamp {")
			&& scalar.contains("value: Option<i64>")
			&& scalar.contains("unit: TimeUnit"),
		"ScalarValue::Timestamp stays tz-anchored (value: Option<i64>, unit: TimeUnit): {scalar}"
	);
	assert!(ir.contains("utc_timestamp_data_type(*unit)"));
	assert!(ir.contains("format_utc_timestamp"));
	assert!(insert_literal.contains("Timestamp { value: i64, unit: TimeUnit }"));
	assert!(lower_create.contains("TimezoneInfo::WithTimeZone | TimezoneInfo::Tz"));
	assert!(lower_create.contains("utc_timestamp_data_type"));
	assert!(planner.contains("TIMESTAMP WITHOUT TIME ZONE is not supported; use TIMESTAMPTZ"));
	assert!(planner.contains("unsupported TIMESTAMPTZ precision"));
	assert!(lower_insert.contains("parse_explicit_timestamptz"));
	assert!(lower_insert.contains("InsertLiteral::Timestamp"));
	assert!(lower_expr.contains("ScalarValue::Timestamp"));
	assert!(lower_expr.contains("parse_explicit_timestamptz"));
	assert!(binder.contains("pub session_timezone: String"));
	assert!(binder.contains("session_timezone: \"UTC\".to_string()"));
	assert!(binder.contains("with_session_timezone"));
	assert!(binder.contains("resolved_session_timezone"));
	assert!(!binder.contains("iana_time_zone"));
	assert!(typed_null.contains("DataType::Timestamp(unit, Some(timezone))"));
	assert!(typed_null.contains("timezone.as_ref() == \"UTC\""));
	assert!(ctas_lattice.contains("crate::timestamp::is_supported_utc_timestamp(data_type)"));
	assert!(coerce_insert.contains("InsertLiteral::Timestamp { value, unit }"));
	assert!(coerce_insert.contains("parse_timestamp_text_for_bind"));
	assert!(
		coerce_compare.contains(
			"(ScalarValue::Utf8(Some(value)), DataType::Timestamp(unit, Some(timezone)))"
		)
	);
	assert!(coerce_compare.contains("parse_timestamp_text_for_bind"));
	assert!(coerce_compare.contains("timezone.as_ref() == \"UTC\""));
	assert!(
		coerce_compare
			.contains("TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond")
	);
	assert!(in_list_key.contains("Timestamp(TimeUnit, i64)"));
	assert!(in_list_scalar.contains("ScalarValue::Timestamp"));
	assert!(carrier_literal.contains("Timestamp(i64)"));
	assert!(carrier_domain.contains("Timestamp(OrderedDomain<i64>)"));
	assert!(range_scalar.contains("ARROW_STATS_TAG_TIMESTAMP_MILLISECOND"));
	assert!(range_scalar.contains("ARROW_STATS_TAG_TIMESTAMP_MICROSECOND"));
	assert!(range_scalar.contains("ARROW_STATS_TAG_TIMESTAMP_NANOSECOND"));
	assert!(range_scalar.contains("DataType::Timestamp(column_unit, Some(timezone))"));
	assert!(range_scalar.contains("timestamp_fact_unit(range.arrow_type_tag)"));
	assert!(range_scalar.contains("column_unit == unit"));
	assert!(!range_scalar.contains("ARROW_STATS_TAG_DATE64"));
	assert!(decode_range.contains("ARROW_STATS_TAG_TIMESTAMP_MILLISECOND"));
	assert!(decode_range.contains("decode_signed_optimizer_fact_scalar"));
	assert!(sql_keyword.contains("DataType::Timestamp(_, Some(timezone))"));
	assert!(sql_keyword.contains("Ok(\"TIMESTAMPTZ\")"));
	assert!(literal_array.contains("TimestampMillisecondArray"));
	assert!(literal_array.contains("with_timezone(\"UTC\")"));
	assert!(encode_partition.contains("encoded.push(14)"));
	assert!(encode_partition.contains("encoded.push(15)"));
	assert!(encode_partition.contains("encoded.push(16)"));
	assert!(group_array.contains("TimestampNanosecondArray"));
	assert!(group_array.contains("with_timezone(\"UTC\")"));
	assert!(write_build_array.contains("build_timestamp_array"));
	assert!(write_timestamp_array.contains("TimestampNanosecondBuilder"));
	assert!(write_timestamp_array.contains("with_timezone(\"UTC\")"));
	assert!(pbc_literal.contains("ScalarValue::Timestamp { value: Some(_), .. }"));
	assert!(pbc_literal.contains("Timestamp literals require type-preserving PBC support"));
	assert!(rune.contains("pub fn lit_timestamp(value: String)"));
	assert!(rune.contains("parse_explicit_timestamptz(&value, TimeUnit::Nanosecond)"));
	assert!(rune.contains("ScalarValue::Timestamp"));
	assert!(rune.contains("m.function([\"lit_timestamp\"], lit_timestamp).build()?;"));
	assert!(sql_wire.contains("pub fn execute_with_timezone"));
	assert!(sql_wire.contains("pub session_timezone: String"));
	assert!(cli.contains("\"--timezone\""));
	assert!(cli.contains("iana_time_zone::get_timezone"));
	assert!(daemon.contains("parse_session_timezone(&session_timezone)"));
	assert!(daemon.contains("with_session_timezone(session_timezone.clone())"));
	assert!(
		daemon.contains("execute_query_with_context_and_config(&plan, context, planner_config)")
	);
	assert!(
		restamp.contains("DataType::Timestamp(*unit, Some(session_timezone.to_string().into()))")
	);
	assert!(daemon.contains("fn restamp_sql_result_array"));
	assert!(daemon.contains("with_timezone(session_timezone.to_string())"));
	assert!(
		binder.contains("timestamp_session_zone_range_filtered_dim_infers_broadcast_by_estimate")
	);
	assert!(binder.contains("timestamp_string_range_uses_session_timezone_for_estimate"));
	assert!(binder.contains("bare_string_timestamp_interpreted_in_session_zone"));
	assert!(binder.contains("bare_string_timestamp_with_explicit_offset_normalizes_to_utc"));
	assert!(binder.contains("insert_timestamp_bare_string_with_explicit_offset_normalizes_to_utc"));
	assert!(binder.contains("timestamp_string_dst_gap_is_binding_error"));
	assert!(binder.contains("timestamp_string_dst_fold_chooses_earlier_instant"));
	assert!(binder.contains("timestamp_precision_loss_in_target_unit_is_bind_error"));
	assert!(binder.contains("timestamp_in_list_uses_existing_ndv_fact_and_writes_no_hard_bounds"));
	assert!(binder.contains("timestamp_range_fact_requires_timestamp_column_and_matching_unit"));
	assert!(
		binder
			.contains("date32_stays_civil_not_timestamp_day_boundary_and_date64_fact_remains_dark")
	);
	assert!(ctas_test.contains("query_coordinator_executes_timestamptz_projection_ctas"));
	assert!(rune_test.contains("rune_lit_timestamp_lowers_explicit_offset_to_utc_nanos"));
}

#[test]
pub(crate) fn adr151_session_zoned_temporal_operations_are_bound_and_toron_executed() {
	let binder = binder_source();
	let ir = source("src/ir.rs");
	let execution_ir = source("src/ir/execution.rs");
	let planner = source("src/sql_planner.rs");
	let exec = exec_source();
	let timestamp = source("src/timestamp.rs");
	let date32 = source("src/date32.rs");
	let daemon = source("src/daemon.rs");
	let pbc = source("src/pbc.rs");
	let rune = source("src/rune_engine.rs");
	let lower_expr = source_block(&planner, "fn lower_expr");
	let bind_expr = source_block(&binder, "fn bind_expr_into_program");
	let bind_extract = source_block(&binder, "fn bind_temporal_extract");
	let bind_trunc = source_block(&binder, "fn bind_date_trunc");
	let operand_zone = source_block(&binder, "fn temporal_operand_zone");
	let range_scalar = source_block(&binder, "fn range_scalar_for_fact");
	let restamp_field = source_block(&daemon, "fn restamp_sql_result_field");
	let restamp_array = source_block(&daemon, "fn restamp_sql_result_array");
	let bound_node = source_block(&execution_ir, "pub enum BoundExprNode");

	// ADR 553: AT TIME ZONE became a registration. Its zone is still a
	// validated constant reaching the binder, and it is still Toron-executed —
	// the invariant this guard protects — but it now lives in the registry.
	let registry = source("src/ir/scalar_function.rs");
	assert!(registry.contains("const AT_TIME_ZONE: ScalarFunctionMetadata"));
	assert!(registry.contains("TimestampWithConstantZone"));
	assert!(binder.contains("validate_temporal_zone"));

	for required in [
		"pub enum TemporalExtractField",
		"pub enum TemporalTruncField",
		"ScalarFunction {",
		"parameter: ScalarFunctionParameter",
	] {
		assert!(ir.contains(required), "IR missing {required}");
	}
	for required in [
		"ScalarFunction {",
		"parameter: ScalarFunctionParameter",
		"append_parameterized_scalar_function",
	] {
		assert!(
			execution_ir.contains(required),
			"bound IR missing {required}"
		);
	}
	assert!(bound_node.contains("parameter: ScalarFunctionParameter"));
	assert!(registry.contains("ExtractScalarFunctionParameter"));
	assert!(registry.contains("DateTruncScalarFunctionParameter"));
	assert!(lower_expr.contains("SqlExpr::Extract { field, expr, .. }"));
	assert!(lower_expr.contains("SqlExpr::AtTimeZone"));
	assert!(lower_expr.contains("function_name_eq(&f.name, \"DATE_PART\")"));
	assert!(lower_expr.contains("function_name_eq(&f.name, \"DATE_TRUNC\")"));
	assert!(planner.contains("lower_extract_datetime_field"));
	assert!(planner.contains("unsupported EXTRACT field"));
	assert!(planner.contains("unsupported date_trunc field"));
	assert!(binder.contains("ScalarFunctionSignature::TimestampWithConstantZone"));
	assert!(bind_expr.contains("parameter,\n\t\t} => bind_scalar_function"));
	assert!(operand_zone.contains("as_at_time_zone"));
	assert!(operand_zone.contains("ctx.session_timezone.to_string()"));
	assert!(!operand_zone.contains("DataType::Timestamp"));
	assert!(bind_extract.contains("field.is_date32_supported()"));
	assert!(bind_extract.contains("append_parameterized_scalar_function"));
	assert!(bind_extract.contains("ScalarFunctionParameter::Extract"));
	assert!(bind_extract.contains("AT TIME ZONE requires Timestamp"));
	assert!(bind_trunc.contains("Date32 date_trunc is deferred in ADR 151"));
	assert!(bind_trunc.contains("DataType::Timestamp(unit, Some(\"UTC\".into()))"));
	assert!(timestamp.contains("pub(crate) fn timestamp_extract"));
	assert!(timestamp.contains("pub(crate) fn timestamp_trunc"));
	assert!(timestamp.contains("timezone.from_local_datetime(&naive)"));
	assert!(timestamp.contains("LocalResult::None"));
	assert!(timestamp.contains("LocalResult::Ambiguous"));
	assert!(date32.contains("pub(crate) fn date32_extract"));
	assert!(exec.contains("fn evaluate_at_time_zone"));
	assert!(exec.contains("fn evaluate_extract"));
	assert!(exec.contains("fn evaluate_date_trunc"));
	assert!(exec.contains("ScalarFunctionEvaluator::AtTimeZone"));
	assert!(exec.contains("ScalarFunctionEvaluator::Extract"));
	assert!(exec.contains("ScalarFunctionId::DateTrunc"));
	assert!(restamp_field.contains("timezone.as_ref() == \"UTC\""));
	assert!(restamp_field.contains("other => other.clone()"));
	assert!(restamp_array.contains("timezone.as_ref() == \"UTC\""));
	assert!(pbc.contains("metadata.pbc_pushdown_eligible"));
	assert!(pbc.contains("BoundExprNode::ScalarFunction { id, .. }"));
	for (name, id) in [("EXTRACT", "Extract"), ("DATE_TRUNC", "DateTrunc")] {
		let metadata =
			brace_blocks_after_marker(&registry, &format!("const {name}: ScalarFunctionMetadata"))
				.into_iter()
				.next()
				.expect("temporal scalar metadata")
				.1;
		assert!(metadata.contains(&format!("id: ScalarFunctionId::{id}")));
		assert!(metadata.contains("pbc_pushdown_eligible: false"));
		assert!(metadata.contains("guard_filter_pushdown_safe: true"));
	}
	let transparent_ids = brace_blocks_after_marker(&registry, "const ")
		.into_iter()
		.filter_map(|(_, block)| {
			if !block.contains(": ScalarFunctionMetadata = ScalarFunctionMetadata")
				|| !block.contains("guard_filter_pushdown_safe: true")
			{
				return None;
			}
			block
				.split_once("id: ScalarFunctionId::")
				.and_then(|(_, rest)| rest.split_once(',').map(|(id, _)| id.to_string()))
		})
		.collect::<BTreeSet<_>>();
	assert_eq!(
		transparent_ids,
		BTreeSet::from([
			"AtTimeZone".to_string(),
			"DateTrunc".to_string(),
			"Extract".to_string(),
			"Substring".to_string(),
			"TimeBucket".to_string(),
		])
	);
	assert!(rune.contains("pub fn extract(field: String, expr: RuneExpr)"));
	assert!(rune.contains("pub fn date_trunc(field: String, expr: RuneExpr)"));
	assert!(rune.contains("pub fn at_time_zone(expr: RuneExpr, zone: String)"));
	assert!(rune.contains("m.function([\"date_trunc\"], date_trunc).build()?;"));
	assert!(!range_scalar.contains("Some(\"America/Chicago\""));
	assert!(binder.contains("extract_hour_from_timestamp_at_time_zone_uses_named_zone"));
	assert!(binder.contains("extract_utc_column_without_at_time_zone_uses_session_zone"));
	assert!(binder.contains("extract_at_time_zone_utc_uses_utc_not_session_zone"));
	assert!(binder.contains("date_trunc_day_returns_utc_instant_for_session_local_midnight"));
	assert!(binder.contains("date_trunc_day_at_time_zone_returns_utc_instant_for_local_midnight"));
	assert!(binder.contains("date_trunc_day_across_dst_reuses_adr150_gap_fold_policy"));
	assert!(binder.contains("date32_extract_is_civil_and_zone_free"));
	assert!(binder.contains("at_time_zone_rejects_date32_operand"));
	assert!(binder.contains("rune_date_trunc_day_groups_event_time_in_session_zone"));
	assert!(daemon.contains("sql_result_timestamp_restamp_preserves_explicit_non_utc_zone"));
	assert!(!planner.contains("SET TIME ZONE"));
	assert!(binder.contains("date_trunc is not supported for Date64"));
}

#[test]
pub(crate) fn adr152_fixed_duration_interval_delta_arithmetic_is_toron_only() {
	let binder = binder_source();
	let ir = source("src/ir.rs");
	let execution_ir = source("src/ir/execution.rs");
	let planner = source("src/sql_planner.rs");
	let exec = exec_source();
	let pbc = source("src/pbc.rs");
	let rune = source("src/rune_engine.rs");
	let lower_expr = source_block(&planner, "fn lower_expr");
	let bind_arithmetic = source_block(&binder, "fn bind_arithmetic");
	let arithmetic_output = source_block(&execution_ir, "pub fn arithmetic_output_type");
	let temporal_exec = source_block(&exec, "fn evaluate_temporal_arithmetic");
	let reject_nested = source_block(&planner, "fn reject_nested_aggregate");
	let contains_aggregate = source_block(&planner, "fn expr_contains_aggregate_call");
	let contains_subquery = source_block(&planner, "fn expr_contains_subquery");
	let contains_window = source_block(&planner, "fn expr_contains_window_function");
	let contains_column = source_block(&planner, "fn expr_contains_nonaggregate_column_reference");

	assert!(ir.contains("Interval(Option<i64>)"));
	assert!(ir.contains("pub fn interval_data_type() -> DataType"));
	assert!(ir.contains("DataType::Duration(TimeUnit::Nanosecond)"));
	assert!(ir.contains("Self::Interval(_) => interval_data_type()"));
	assert!(!source_block(&execution_ir, "pub enum BoundExprNode").contains("Interval"));
	assert!(arithmetic_output.contains("DataType::*"));
	assert!(!arithmetic_output.contains("Duration"));
	assert!(!arithmetic_output.contains("Timestamp"));
	assert!(bind_arithmetic.contains("temporal_arithmetic_output_type(&op"));
	assert!(
		bind_arithmetic
			.find("temporal_arithmetic_output_type(&op")
			.expect("temporal branch")
			< bind_arithmetic
				.find("arithmetic_output_type(&left_type, &right_type)")
				.expect("numeric branch")
	);
	assert!(binder.contains("fn fold_temporal_arithmetic"));
	assert!(binder.contains("Date32 +/- Interval is deferred"));
	assert!(binder.contains("mixed-unit Timestamp - Timestamp is deferred in ADR 152"));
	assert!(binder.contains("not exactly representable at {} precision"));
	assert!(lower_expr.contains("SqlExpr::Interval(interval) => lower_interval_literal(interval)"));
	assert!(planner.contains("fn lower_interval_literal"));
	assert!(planner.contains("DateTimeField::Day | DateTimeField::Days"));
	assert!(planner.contains("calendar INTERVAL unit"));
	// ADR 0129 moved interval arithmetic out of this planner's local helpers.
	// Pin the public lowering behavior rather than the spelling/location of the
	// conversion constant so a correct relocation does not create a false red.
	let LogicalPlan::Project { exprs, .. } = toron::sql_planner::SqlPlanner::new()
		.plan("SELECT INTERVAL '1 day' FROM sales")
		.expect("plan fixed-duration interval")
	else {
		panic!("expected project");
	};
	assert!(matches!(
		&exprs[0],
		Expr::Literal(ScalarValue::Interval(Some(value))) if *value == 86_400_000_000_000
	));
	assert!(reject_nested.contains("SqlExpr::Interval(SqlInterval { value: expr, .. })"));
	assert!(contains_aggregate.contains("SqlExpr::Interval(SqlInterval { value, .. })"));
	assert!(contains_subquery.contains("SqlExpr::Interval(SqlInterval { value, .. })"));
	assert!(contains_window.contains("SqlExpr::Interval(SqlInterval { value, .. })"));
	assert!(contains_column.contains("SqlExpr::Interval(SqlInterval { value, .. })"));
	assert!(exec.contains("DurationNanosecondArray"));
	assert!(temporal_exec.contains("Timestamp + Timestamp is not supported"));
	assert!(temporal_exec.contains("Interval - Timestamp is not supported"));
	assert!(exec.contains("fn checked_interval_nanos_to_timestamp_ticks_exec"));
	assert!(!temporal_exec.contains("arrow::compute"));
	assert!(pbc.contains("fixed-duration interval arithmetic is not PBC-eligible"));
	assert!(pbc.contains("DataType::Duration(TimeUnit::Nanosecond)"));
	for required in [
		"pub fn nanoseconds(value: i64)",
		"pub fn microseconds(value: i64)",
		"pub fn milliseconds(value: i64)",
		"pub fn seconds(value: i64)",
		"pub fn minutes(value: i64)",
		"pub fn hours(value: i64)",
		"pub fn days(value: i64)",
		"pub fn months(value: i64)",
		"pub fn years(value: i64)",
		"m.function([\"days\"], days).build()?;",
		"m.function([\"months\"], months).build()?;",
		"m.function([\"years\"], years).build()?;",
	] {
		assert!(rune.contains(required), "Rune missing {required}");
	}
	for required_test in [
		"relative_interval_filter_spends_timestamp_range_fact",
		"relative_calendar_month_filter_spends_timestamp_range_fact",
		"timestamp_plus_interval_preserves_utc_instant_type",
		"timestamp_minus_interval_preserves_utc_instant_type",
		"timestamp_minus_timestamp_returns_interval_in_shared_unit",
		"mixed_unit_timestamp_difference_rejects_in_v1",
		"interval_plus_interval_returns_interval",
		"interval_comparison_orders_fixed_durations",
		"timestamp_plus_timestamp_rejects",
		"interval_day_is_fixed_86400s_not_civil",
		"non_representable_interval_precision_rejects",
		"calendar_month_year_interval_literals_are_year_month",
		"timestamp_plus_calendar_interval_uses_session_zone_and_eom_clamp",
		"timestamp_calendar_literal_arithmetic_folds_in_bind",
		"date32_calendar_arithmetic_clamps_end_of_month",
		"calendar_interval_plus_interval_returns_year_month",
		"calendar_interval_runtime_array_arithmetic_uses_year_month",
		"calendar_interval_comparison_rejects",
		"calendar_fixed_interval_mix_rejects",
		"date32_plus_interval_rejects_in_v1",
		"rune_interval_arithmetic_matches_sql",
		"rune_calendar_interval_arithmetic_matches_sql",
	] {
		assert!(
			binder.contains(required_test),
			"missing behavior gate {required_test}"
		);
	}
}

#[test]
pub(crate) fn adr153_calendar_month_year_interval_arithmetic_is_session_framed() {
	let binder = binder_source();
	let ir = source("src/ir.rs");
	let planner = source("src/sql_planner.rs");
	let exec = exec_source();
	let timestamp = source("src/timestamp.rs");
	let date32 = source("src/date32.rs");
	let pbc = source("src/pbc.rs");
	let rune = source("src/rune_engine.rs");
	let write = source("src/coordinator/write.rs");
	let bind_arithmetic = source_block(&binder, "fn bind_arithmetic");
	let calendar_output = source_block(&binder, "fn calendar_arithmetic_output_type");
	let calendar_fold = source_block(&binder, "fn fold_calendar_arithmetic");
	let calendar_exec = source_block(&exec, "fn evaluate_calendar_arithmetic");
	let timestamp_helper = source_block(&timestamp, "pub(crate) fn add_calendar_months");
	let date32_helper = source_block(&date32, "pub(crate) fn add_calendar_months");

	assert!(ir.contains("CalendarMonthInterval(Option<i32>)"));
	assert!(ir.contains("pub fn calendar_month_interval_data_type() -> DataType"));
	assert!(ir.contains("DataType::Interval(IntervalUnit::YearMonth)"));
	assert!(planner.contains("LoweredIntervalLiteral::CalendarMonths"));
	assert!(planner.contains("ScalarValue::CalendarMonthInterval(Some(months))"));
	assert!(planner.contains("DateTimeField::Month | DateTimeField::Months"));
	assert!(planner.contains("DateTimeField::Year | DateTimeField::Years"));
	// The calendar-unit conversion now lives in toron-client. Verify the
	// planner's public result instead of pinning that implementation detail.
	for (sql, expected_months) in [
		("SELECT INTERVAL '1 month' FROM sales", 1),
		("SELECT INTERVAL '1 year' FROM sales", 12),
	] {
		let LogicalPlan::Project { exprs, .. } = toron::sql_planner::SqlPlanner::new()
			.plan(sql)
			.unwrap_or_else(|error| panic!("plan calendar interval {sql:?}: {error}"))
		else {
			panic!("expected project for {sql:?}");
		};
		assert!(matches!(
			&exprs[0],
			Expr::Literal(ScalarValue::CalendarMonthInterval(Some(value)))
				if *value == expected_months
		));
	}
	assert!(rune.contains("fn calendar_interval_literal"));
	assert!(rune.contains("ScalarValue::CalendarMonthInterval(Some(months))"));
	assert!(rune.contains("pub fn months(value: i64)"));
	assert!(rune.contains("pub fn years(value: i64)"));

	assert!(bind_arithmetic.contains("calendar_arithmetic_output_type(&op"));
	assert!(
		bind_arithmetic
			.find("calendar_arithmetic_output_type(&op")
			.expect("calendar branch")
			< bind_arithmetic
				.find("temporal_arithmetic_output_type(&op")
				.expect("fixed temporal branch")
	);
	assert!(calendar_output.contains("Timestamp + CalendarMonthInterval"));
	assert!(calendar_output.contains("CalendarMonthInterval + Timestamp"));
	assert!(calendar_output.contains("DataType::Date32"));
	assert!(calendar_output.contains("CalendarMonthInterval - Timestamp is not supported"));
	assert!(binder.contains("fn bind_calendar_arithmetic_operands"));
	assert!(binder.contains("fn bind_calendar_timestamp_operand"));
	assert!(binder.contains("crate::ir::ScalarFunctionId::AtTimeZone"));
	assert!(binder.contains("CalendarMonthInterval comparison is deferred"));
	assert!(calendar_fold.contains("add_calendar_months_for_bind"));
	assert!(calendar_fold.contains("add_date32_calendar_months_for_bind"));
	assert!(binder.contains("crate::timestamp::add_calendar_months"));
	assert!(binder.contains("crate::date32::add_calendar_months"));

	assert!(timestamp_helper.contains("Months::new"));
	assert!(timestamp_helper.contains("resolve_local_datetime"));
	assert!(timestamp.contains("fn resolve_local_datetime"));
	assert!(date32_helper.contains("civil_from_days"));
	assert!(date32_helper.contains("days_from_civil"));
	assert!(date32.contains("fn days_in_month_i64"));

	assert!(exec.contains("IntervalYearMonthArray"));
	assert!(exec.contains("IntervalYearMonthBuilder"));
	assert!(exec.contains("fn evaluate_calendar_arithmetic"));
	assert!(exec.contains("fn calendar_timestamp_context"));
	assert!(exec.contains("parse_temporal_execution_zone(zone.as_ref())"));
	assert!(calendar_exec.contains("evaluate_timestamp_calendar_interval"));
	assert!(calendar_exec.contains("evaluate_date32_calendar_interval"));
	assert!(calendar_exec.contains("evaluate_calendar_interval_interval"));
	assert!(!calendar_exec.contains("arrow::compute"));
	assert!(write.contains("IntervalYearMonthBuilder"));
	assert!(pbc.contains("calendar interval arithmetic is not PBC-eligible"));
	assert!(pbc.contains("calendar interval literals are not PBC-eligible"));
}
