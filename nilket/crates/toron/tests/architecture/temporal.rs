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
