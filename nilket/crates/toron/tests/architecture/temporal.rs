use super::support::*;

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
	let binder = source("src/binder.rs");
	let ir = source("src/ir.rs");
	let planner = source("src/sql_planner.rs");
	let exec = source("src/exec.rs");
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
	let in_list_scalar = source_block(&binder, "fn in_list_scalar_literal_cardinality");
	let in_list_key = source_block(&binder, "enum InListLiteralKey");
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
	let binder = source("src/binder.rs");
	let ir = source("src/ir.rs");
	let planner = source("src/sql_planner.rs");
	let exec = source("src/exec.rs");
	let write = source("src/coordinator/write.rs");
	let pbc = source("src/pbc.rs");
	let rune = source("src/rune_engine.rs");
	let cli = source("src/cli.rs");
	let daemon = source("src/daemon.rs");
	let sql_wire = source("src/sql_wire.rs");
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
	let in_list_scalar = source_block(&binder, "fn in_list_scalar_literal_cardinality");
	let in_list_key = source_block(&binder, "enum InListLiteralKey");
	let carrier_literal = source_block(&binder, "enum CarrierLiteral");
	let carrier_domain = source_block(&binder, "enum CarrierDomain");
	let range_scalar = source_block(&binder, "fn range_scalar_for_fact");
	let decode_range = source_block(&binder, "fn decode_range_scalar");
	let sql_keyword = source_block(&binder, "fn sql_carrier_keyword");
	let pbc_literal = source_block(&pbc, "fn emit_literal");
	let literal_array = source_block(&exec, "fn timestamp_literal_array");
	let encode_partition = source_block(&exec, "fn encode_partition_key_value");
	let group_array = source_block(&exec, "fn group_column");
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
	assert!(scalar.contains("Timestamp { value: Option<i64>, unit: TimeUnit }"));
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
	let binder = source("src/binder.rs");
	let ir = source("src/ir.rs");
	let execution_ir = source("src/ir/execution.rs");
	let planner = source("src/sql_planner.rs");
	let exec = source("src/exec.rs");
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

	for required in [
		"pub enum TemporalExtractField",
		"pub enum TemporalTruncField",
		"AtTimeZone(Box<Expr>, String)",
		"Extract(TemporalExtractField, Box<Expr>)",
		"DateTrunc(TemporalTruncField, Box<Expr>)",
		"Self::AtTimeZone(_, _)",
		"Self::Extract(_, _)",
		"Self::DateTrunc(_, _)",
	] {
		assert!(ir.contains(required), "IR missing {required}");
	}
	for required in [
		"AtTimeZone {",
		"Extract {",
		"zone: Option<String>",
		"DateTrunc {",
		"append_at_time_zone",
		"append_extract",
		"append_date_trunc",
	] {
		assert!(
			execution_ir.contains(required),
			"bound IR missing {required}"
		);
	}
	assert!(bound_node.contains("field: TemporalExtractField"));
	assert!(bound_node.contains("field: TemporalTruncField"));
	assert!(lower_expr.contains("SqlExpr::Extract { field, expr, .. }"));
	assert!(lower_expr.contains("SqlExpr::AtTimeZone"));
	assert!(lower_expr.contains("function_name_eq(&f.name, \"DATE_PART\")"));
	assert!(lower_expr.contains("function_name_eq(&f.name, \"DATE_TRUNC\")"));
	assert!(planner.contains("lower_extract_datetime_field"));
	assert!(planner.contains("unsupported EXTRACT field"));
	assert!(planner.contains("unsupported date_trunc field"));
	assert!(bind_expr.contains("Expr::AtTimeZone(inner, zone)"));
	assert!(bind_expr.contains("Expr::Extract(field, inner)"));
	assert!(bind_expr.contains("Expr::DateTrunc(field, inner)"));
	assert!(operand_zone.contains("Expr::AtTimeZone(timestamp, zone)"));
	assert!(operand_zone.contains("ctx.session_timezone.to_string()"));
	assert!(!operand_zone.contains("DataType::Timestamp"));
	assert!(bind_extract.contains("field.is_date32_supported()"));
	assert!(bind_extract.contains("builder.append_extract(field, None"));
	assert!(bind_extract.contains("builder.append_extract(field, Some(zone)"));
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
	assert!(exec.contains("BoundExprNode::AtTimeZone"));
	assert!(exec.contains("BoundExprNode::Extract"));
	assert!(exec.contains("BoundExprNode::DateTrunc"));
	assert!(restamp_field.contains("timezone.as_ref() == \"UTC\""));
	assert!(restamp_field.contains("other => other.clone()"));
	assert!(restamp_array.contains("timezone.as_ref() == \"UTC\""));
	assert!(pbc.contains("BoundExprNode::AtTimeZone"));
	assert!(pbc.contains("BoundExprNode::Extract"));
	assert!(pbc.contains("BoundExprNode::DateTrunc"));
	assert!(pbc.contains("session-zoned temporal operations are not PBC-eligible"));
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
