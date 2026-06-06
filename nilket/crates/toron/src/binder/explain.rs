use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinTraceTarget {
	pub(crate) join_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecisionReceipt {
	pub(crate) target: JoinTraceTarget,
	pub(crate) strategy: JoinStrategy,
	pub(crate) placement: JoinPhysicalPlacement,
	pub(crate) reason: JoinStrategyDecisionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanTraceTarget {
	pub(crate) scan_index: usize,
	pub(crate) table_id: Option<TableId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterEstimateSource {
	Range,
	ConvertedRange,
	NdvEquality,
	NdvInList,
	HeavyHitterTopK,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanFilterOutcome {
	FullyPushed,
	PartiallyPushed,
	ResidualOnly,
	StaticElided,
	PointRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanFilterReceipt {
	pub(crate) target: ScanTraceTarget,
	pub(crate) outcome: ScanFilterOutcome,
	pub(crate) converted_pushdown: bool,
	pub(crate) profile_hint: QueryProfileHint,
	pub(crate) estimated_rows: Option<RowCount>,
	pub(crate) spent_fact: Option<FilterEstimateSource>,
	pub(crate) spent_column: Option<ColumnOrdinal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FilterEstimateEvidence {
	pub(super) rows: RowCount,
	pub(super) source: FilterEstimateSource,
	pub(super) column: Option<ColumnOrdinal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanFilterPushdownEvidence {
	pub(super) outcome: ScanFilterOutcome,
	pub(super) converted_pushdown: bool,
	pub(super) profile_hint: QueryProfileHint,
	pub(super) table_id: Option<TableId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OptimizerTrace {
	pub(crate) join_decisions: Vec<DecisionReceipt>,
	pub(crate) scan_filter_decisions: Vec<ScanFilterReceipt>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct OptimizerExplainRenderTrace {
	pub(super) scan_filters: Vec<ScanFilterRenderReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScanFilterRenderReceipt {
	pub(super) target: ScanTraceTarget,
	pub(super) predicate_text: Option<String>,
	pub(super) pushed_predicate_text: Option<String>,
	pub(super) residual_predicate_text: Option<String>,
	pub(super) spent_column_name: Option<String>,
}

impl OptimizerExplainRenderTrace {
	pub(super) fn scan_filter(&self, target: ScanTraceTarget) -> Option<&ScanFilterRenderReceipt> {
		self.scan_filters
			.iter()
			.find(|receipt| receipt.target == target)
	}
}

pub(super) fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

pub(super) fn is_information_schema_namespace(table: &TableRef) -> bool {
	table
		.schema
		.as_deref()
		.is_some_and(|schema| schema.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAMESPACE))
}

pub(super) fn reject_information_schema_target(table: &TableRef) -> Result<(), BindingError> {
	if is_information_schema_namespace(table) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("information_schema is read-only: {}", table_name(table)),
		});
	}
	Ok(())
}

pub(super) fn information_schema_logical_types_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("logical_type_id", DataType::Int64, false),
		Field::new("name", DataType::Utf8, false),
		Field::new("physical", DataType::Utf8, false),
		Field::new("refinement", DataType::Utf8, true),
	]))
}

pub(super) fn information_schema_tables_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("table_schema", DataType::Utf8, false),
		Field::new("table_name", DataType::Utf8, false),
		Field::new("table_type", DataType::Utf8, false),
	]))
}

pub(super) fn information_schema_columns_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("table_schema", DataType::Utf8, false),
		Field::new("table_name", DataType::Utf8, false),
		Field::new("column_name", DataType::Utf8, false),
		Field::new("ordinal_position", DataType::Int64, false),
		Field::new("data_type", DataType::Utf8, false),
		Field::new("is_nullable", DataType::Utf8, false),
		Field::new("logical_type_name", DataType::Utf8, true),
	]))
}

pub(super) fn information_schema_schemata_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![Field::new(
		"schema_name",
		DataType::Utf8,
		false,
	)]))
}

pub(super) fn optimizer_explain_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("decision_kind", DataType::Utf8, false),
		Field::new("target", DataType::Utf8, false),
		Field::new("outcome", DataType::Utf8, false),
		Field::new("detail", DataType::Utf8, true),
		Field::new("predicate_text", DataType::Utf8, true),
		Field::new("pushed_predicate_text", DataType::Utf8, true),
		Field::new("residual_predicate_text", DataType::Utf8, true),
		Field::new("estimated_rows", DataType::Int64, true),
		Field::new("spent_fact", DataType::Utf8, true),
		Field::new("spent_column", DataType::Utf8, true),
		Field::new("spent_column_name", DataType::Utf8, true),
		Field::new("build_side", DataType::Utf8, true),
		Field::new("build_bytes", DataType::Int64, true),
		Field::new("threshold_bytes", DataType::Int64, true),
		Field::new("rejected_side_bytes", DataType::Int64, true),
	]))
}

pub(super) fn optimizer_explain_plan(
	trace: OptimizerTrace,
	render_trace: Option<OptimizerExplainRenderTrace>,
) -> Result<ExecutionPlan, BindingError> {
	let mut rows = trace
		.join_decisions
		.iter()
		.map(optimizer_explain_row)
		.collect::<Result<Vec<_>, _>>()?;
	let render_trace = render_trace.as_ref();
	rows.extend(
		trace
			.scan_filter_decisions
			.iter()
			.map(|receipt| {
				optimizer_explain_scan_filter_row(
					receipt,
					render_trace.and_then(|trace| trace.scan_filter(receipt.target)),
				)
			})
			.collect::<Result<Vec<_>, _>>()?,
	);
	system_table_plan(
		SystemTableKind::OptimizerExplain,
		optimizer_explain_schema(),
		rows,
		None,
	)
}

pub(super) fn optimizer_explain_row(
	receipt: &DecisionReceipt,
) -> Result<Vec<ScalarValue>, BindingError> {
	let (build_side, build_bytes, threshold_bytes, rejected_side_bytes) =
		optimizer_explain_build_columns(receipt)?;
	Ok(vec![
		ScalarValue::Utf8(Some("join".to_string())),
		ScalarValue::Utf8(Some(join_target_label(receipt.target)?)),
		ScalarValue::Utf8(Some(join_strategy_label(receipt.strategy).to_string())),
		ScalarValue::Utf8(Some(join_strategy_decision_detail_label(receipt))),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(build_side.map(str::to_string)),
		ScalarValue::Int64(build_bytes),
		ScalarValue::Int64(threshold_bytes),
		ScalarValue::Int64(rejected_side_bytes),
	])
}

pub(super) fn optimizer_explain_scan_filter_row(
	receipt: &ScanFilterReceipt,
	render_receipt: Option<&ScanFilterRenderReceipt>,
) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Utf8(Some("scan_filter".to_string())),
		ScalarValue::Utf8(Some(scan_filter_target_label(receipt.target)?)),
		ScalarValue::Utf8(Some(scan_filter_outcome_label(
			receipt.outcome,
			receipt.converted_pushdown,
		))),
		ScalarValue::Utf8(Some(profile_hint_label(receipt.profile_hint).to_string())),
		ScalarValue::Utf8(render_receipt.and_then(|receipt| receipt.predicate_text.clone())),
		ScalarValue::Utf8(render_receipt.and_then(|receipt| receipt.pushed_predicate_text.clone())),
		ScalarValue::Utf8(
			render_receipt.and_then(|receipt| receipt.residual_predicate_text.clone()),
		),
		ScalarValue::Int64(
			receipt
				.estimated_rows
				.map(|rows| row_count_to_i64(rows, "estimated_rows"))
				.transpose()?,
		),
		ScalarValue::Utf8(receipt.spent_fact.map(spent_fact_label).map(str::to_string)),
		ScalarValue::Utf8(receipt.spent_column.map(column_ordinal_label).transpose()?),
		ScalarValue::Utf8(render_receipt.and_then(|receipt| receipt.spent_column_name.clone())),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	])
}

pub(super) fn optimizer_explain_build_columns(
	receipt: &DecisionReceipt,
) -> Result<(Option<&'static str>, Option<i64>, Option<i64>, Option<i64>), BindingError> {
	match receipt.reason {
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side,
			build_bytes,
			threshold,
			rejected_side_bytes,
		} => Ok((
			Some(broadcast_build_side_label(build_side)),
			Some(byte_count_to_i64(build_bytes, "build_bytes")?),
			Some(byte_count_to_i64(threshold, "threshold_bytes")?),
			rejected_side_bytes
				.map(|bytes| byte_count_to_i64(bytes, "rejected_side_bytes"))
				.transpose()?,
		)),
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side,
			estimated_byte_size,
			threshold,
			rejected_side_bytes,
		} => Ok((
			Some(broadcast_build_side_label(build_side)),
			Some(byte_count_to_i64(estimated_byte_size, "build_bytes")?),
			Some(byte_count_to_i64(threshold, "threshold_bytes")?),
			rejected_side_bytes
				.map(|bytes| byte_count_to_i64(bytes, "rejected_side_bytes"))
				.transpose()?,
		)),
		JoinStrategyDecisionReason::PinnedByAuthor
			if receipt.strategy == JoinStrategy::Broadcast =>
		{
			Ok((
				Some(match receipt.placement {
					JoinPhysicalPlacement::Natural => "left",
					JoinPhysicalPlacement::SwappedRightBuild => "right",
				}),
				None,
				None,
				None,
			))
		}
		JoinStrategyDecisionReason::PinnedByAuthor
		| JoinStrategyDecisionReason::HashByDefault { .. } => Ok((None, None, None, None)),
	}
}

pub(super) fn join_strategy_label(strategy: JoinStrategy) -> &'static str {
	match strategy {
		JoinStrategy::HashPartitioned => "hash_partitioned",
		JoinStrategy::Broadcast => "broadcast",
	}
}

pub(super) fn join_target_label(target: JoinTraceTarget) -> Result<String, BindingError> {
	Ok(format!(
		"join[{}]",
		usize_to_i64(target.join_index, "join_index")?
	))
}

pub(super) fn join_physical_placement_label(placement: JoinPhysicalPlacement) -> &'static str {
	match placement {
		JoinPhysicalPlacement::Natural => "natural",
		JoinPhysicalPlacement::SwappedRightBuild => "swapped_right_build",
	}
}

pub(super) fn join_strategy_decision_reason_label(
	reason: &JoinStrategyDecisionReason,
) -> &'static str {
	match reason {
		JoinStrategyDecisionReason::PinnedByAuthor => "pinned_by_author",
		JoinStrategyDecisionReason::BroadcastInferred { .. } => "broadcast_inferred",
		JoinStrategyDecisionReason::BroadcastEstimated { .. } => "broadcast_estimated",
		JoinStrategyDecisionReason::HashByDefault { .. } => "hash_by_default",
	}
}

pub(super) fn join_strategy_decision_reason_detail(
	reason: &JoinStrategyDecisionReason,
) -> Option<&'static str> {
	match reason {
		JoinStrategyDecisionReason::HashByDefault { reason } => {
			Some(hash_by_default_reason_label(*reason))
		}
		JoinStrategyDecisionReason::PinnedByAuthor
		| JoinStrategyDecisionReason::BroadcastInferred { .. }
		| JoinStrategyDecisionReason::BroadcastEstimated { .. } => None,
	}
}

pub(super) fn join_strategy_decision_detail_label(receipt: &DecisionReceipt) -> String {
	let placement = join_physical_placement_label(receipt.placement);
	let reason = join_strategy_decision_reason_label(&receipt.reason);
	match join_strategy_decision_reason_detail(&receipt.reason) {
		Some(detail) => format!("{placement}:{reason}:{detail}"),
		None => format!("{placement}:{reason}"),
	}
}

pub(super) fn hash_by_default_reason_label(reason: HashByDefaultReason) -> &'static str {
	match reason {
		HashByDefaultReason::AutoBroadcastDisabled => "auto_broadcast_disabled",
		HashByDefaultReason::NonInnerJoin => "non_inner_join",
		HashByDefaultReason::LeftInputNotSingleBaseRelation => {
			"left_input_not_single_base_relation"
		}
		HashByDefaultReason::RightInputNotSingleBaseRelation => {
			"right_input_not_single_base_relation"
		}
		HashByDefaultReason::ProbeNotScanLinear => "probe_not_scan_linear",
		HashByDefaultReason::MissingBuildByteUpperBound => "missing_build_byte_upper_bound",
		HashByDefaultReason::BuildByteUpperBoundAboveThreshold => {
			"build_byte_upper_bound_above_threshold"
		}
		HashByDefaultReason::HashAfterBroadcastCap => "hash_after_broadcast_cap",
	}
}

pub(super) fn broadcast_build_side_label(side: BroadcastBuildSide) -> &'static str {
	match side {
		BroadcastBuildSide::Left => "left",
		BroadcastBuildSide::Right => "right",
	}
}

pub(super) fn scan_filter_target_label(target: ScanTraceTarget) -> Result<String, BindingError> {
	let scan = usize_to_i64(target.scan_index, "scan_index")?;
	Ok(match target.table_id {
		Some(table_id) => format!("scan[{scan}] table_id={}", table_id.0),
		None => format!("scan[{scan}]"),
	})
}

pub(super) fn scan_filter_outcome_label(
	outcome: ScanFilterOutcome,
	converted_pushdown: bool,
) -> String {
	let label = match outcome {
		ScanFilterOutcome::FullyPushed => "fully_pushed",
		ScanFilterOutcome::PartiallyPushed => "partially_pushed",
		ScanFilterOutcome::ResidualOnly => "residual_only",
		ScanFilterOutcome::StaticElided => "static_elided",
		ScanFilterOutcome::PointRead => "point_read",
	};
	if converted_pushdown {
		format!("{label}:converted")
	} else {
		label.to_string()
	}
}

pub(super) fn profile_hint_label(profile_hint: QueryProfileHint) -> &'static str {
	match profile_hint {
		QueryProfileHint::PointRead => "point_read",
		QueryProfileHint::Scan => "scan",
	}
}

pub(super) fn spent_fact_label(source: FilterEstimateSource) -> &'static str {
	match source {
		FilterEstimateSource::Range => "range",
		FilterEstimateSource::ConvertedRange => "converted_range",
		FilterEstimateSource::NdvEquality => "ndv_equality",
		FilterEstimateSource::NdvInList => "ndv_in_list",
		FilterEstimateSource::HeavyHitterTopK => "heavy_hitter_top_k",
	}
}

pub(super) fn column_ordinal_label(column: ColumnOrdinal) -> Result<String, BindingError> {
	Ok(format!("col[{}]", i64::from(column.0)))
}

pub(super) fn byte_count_to_i64(
	value: ByteCount,
	column: &'static str,
) -> Result<i64, BindingError> {
	i64::try_from(value.get()).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

pub(super) fn usize_to_i64(value: usize, column: &'static str) -> Result<i64, BindingError> {
	i64::try_from(value).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

pub(super) fn row_count_to_i64(value: RowCount, column: &'static str) -> Result<i64, BindingError> {
	i64::try_from(value.get()).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

pub(super) fn system_table_plan(
	kind: SystemTableKind,
	full_schema: SchemaRef,
	rows: Vec<Vec<ScalarValue>>,
	projection: Option<&Vec<String>>,
) -> Result<ExecutionPlan, BindingError> {
	let Some(projection) = projection else {
		return Ok(ExecutionPlan::SystemTable {
			kind,
			schema: full_schema,
			rows,
		});
	};

	let requested = projection
		.iter()
		.map(|column| resolve_column(column, &full_schema).map(|column| column.ordinal))
		.collect::<Result<Vec<_>, _>>()?;
	let schema = project_schema(&full_schema, &requested)?;
	let rows = project_scalar_rows(&rows, &requested)?;

	Ok(ExecutionPlan::SystemTable { kind, schema, rows })
}

pub(super) fn logical_type_spec_row(
	spec: LogicalTypeSpec,
) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Int64(Some(i64::from(spec.id.0))),
		ScalarValue::Utf8(Some(spec.name)),
		ScalarValue::Utf8(Some(sql_carrier_keyword(&spec.physical)?.to_string())),
		ScalarValue::Utf8(
			spec.refinement
				.as_ref()
				.map(render_refinement_check)
				.transpose()?,
		),
	])
}

pub(super) fn information_schema_table_rows(
	descriptors: &[TableSchemaDescriptor],
) -> Vec<Vec<ScalarValue>> {
	descriptors
		.iter()
		.map(|descriptor| {
			vec![
				ScalarValue::Utf8(Some(descriptor.namespace.clone())),
				ScalarValue::Utf8(Some(descriptor.table_name.clone())),
				ScalarValue::Utf8(Some("BASE TABLE".to_string())),
			]
		})
		.collect()
}

pub(super) fn information_schema_schemata_rows(
	descriptors: &[TableSchemaDescriptor],
) -> Vec<Vec<ScalarValue>> {
	descriptors
		.iter()
		.map(|descriptor| descriptor.namespace.clone())
		.collect::<BTreeSet<_>>()
		.into_iter()
		.map(|schema_name| vec![ScalarValue::Utf8(Some(schema_name))])
		.collect()
}

pub(super) fn information_schema_column_rows(
	descriptors: &[TableSchemaDescriptor],
	logical_types: &LogicalTypeRegistry,
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	let mut rows = Vec::new();
	for descriptor in descriptors {
		let schema =
			descriptor
				.arrow_schema()
				.map_err(|error| BindingError::UnsupportedExpression {
					message: format!(
						"failed to decode table descriptor schema for {}.{}: {error}",
						descriptor.namespace, descriptor.table_name
					),
				})?;
		for (index, field) in schema.fields().iter().enumerate() {
			let ordinal =
				i64::try_from(index + 1).map_err(|_| BindingError::UnsupportedExpression {
					message: "information_schema.columns ordinal overflow".to_string(),
				})?;
			rows.push(vec![
				ScalarValue::Utf8(Some(descriptor.namespace.clone())),
				ScalarValue::Utf8(Some(descriptor.table_name.clone())),
				ScalarValue::Utf8(Some(field.name().clone())),
				ScalarValue::Int64(Some(ordinal)),
				ScalarValue::Utf8(Some(sql_carrier_keyword(field.data_type())?.to_string())),
				ScalarValue::Utf8(Some(
					if field.is_nullable() { "YES" } else { "NO" }.to_string(),
				)),
				ScalarValue::Utf8(logical_type_display_name(field, logical_types)?),
			]);
		}
	}
	Ok(rows)
}

pub(super) fn logical_type_display_name(
	field: &Field,
	logical_types: &LogicalTypeRegistry,
) -> Result<Option<String>, BindingError> {
	let logical_type = decode_column_logical_type_with_resolver(
		field,
		LogicalTypeResolver::with_registry(logical_types),
	)
	.map_err(|error| BindingError::UnsupportedExpression {
		message: error.to_string(),
	})?;
	if logical_type.is_naked() {
		return Ok(None);
	}
	LogicalTypeResolver::with_registry(logical_types)
		.spec(logical_type)
		.map(|spec| Some(spec.name))
		.ok_or_else(|| BindingError::UnsupportedExpression {
			message: format!(
				"logical type metadata for column {} references unknown id {}",
				field.name(),
				logical_type.0
			),
		})
}

pub(super) fn sql_carrier_keyword(data_type: &DataType) -> Result<&'static str, BindingError> {
	match data_type {
		DataType::Int32 => Ok("INT"),
		DataType::Int64 => Ok("BIGINT"),
		DataType::Date32 => Ok("DATE"),
		DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC" => Ok("TIMESTAMPTZ"),
		DataType::Float32 => Ok("REAL"),
		DataType::Float64 => Ok("DOUBLE"),
		DataType::Utf8 => Ok("TEXT"),
		DataType::Boolean => Ok("BOOLEAN"),
		other => Err(BindingError::UnsupportedExpression {
			message: format!("unsupported SQL carrier for information_schema: {other:?}"),
		}),
	}
}

pub(super) fn render_refinement_check(
	predicate: &LogicalTypeRefinementPredicate,
) -> Result<String, BindingError> {
	Ok(format!(
		"CHECK ({})",
		render_refinement_predicate(predicate)?
	))
}

pub(super) fn render_refinement_predicate(
	predicate: &LogicalTypeRefinementPredicate,
) -> Result<String, BindingError> {
	match predicate {
		LogicalTypeRefinementPredicate::Comparison { left, op, right } => Ok(format!(
			"{} {} {}",
			render_refinement_value(left)?,
			refinement_op_sql(*op),
			render_refinement_value(right)?
		)),
		LogicalTypeRefinementPredicate::And(predicates) => predicates
			.iter()
			.map(render_refinement_predicate)
			.collect::<Result<Vec<_>, _>>()
			.map(|parts| parts.join(" AND ")),
	}
}

pub(super) fn render_refinement_value(
	value: &LogicalTypeRefinementValue,
) -> Result<String, BindingError> {
	match value {
		LogicalTypeRefinementValue::Value => Ok("VALUE".to_string()),
		LogicalTypeRefinementValue::Int64(value) => Ok(value.to_string()),
		LogicalTypeRefinementValue::Int32(value) => Ok(value.to_string()),
		LogicalTypeRefinementValue::Float64(bits) => {
			let value = f64::from_bits(*bits);
			if value.is_finite() {
				Ok(value.to_string())
			} else {
				Err(BindingError::UnsupportedExpression {
					message: "logical type FLOAT64 refinement literal is not finite".to_string(),
				})
			}
		}
		LogicalTypeRefinementValue::Float32(bits) => {
			let value = f32::from_bits(*bits);
			if value.is_finite() {
				Ok(value.to_string())
			} else {
				Err(BindingError::UnsupportedExpression {
					message: "logical type FLOAT32 refinement literal is not finite".to_string(),
				})
			}
		}
		LogicalTypeRefinementValue::Utf8(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
		LogicalTypeRefinementValue::Boolean(value) => {
			Ok(if *value { "TRUE" } else { "FALSE" }.to_string())
		}
	}
}

pub(super) fn refinement_op_sql(op: LogicalTypeRefinementComparisonOp) -> &'static str {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => "=",
		LogicalTypeRefinementComparisonOp::Ne => "!=",
		LogicalTypeRefinementComparisonOp::Lt => "<",
		LogicalTypeRefinementComparisonOp::Le => "<=",
		LogicalTypeRefinementComparisonOp::Gt => ">",
		LogicalTypeRefinementComparisonOp::Ge => ">=",
	}
}

pub(super) fn project_scalar_rows(
	rows: &[Vec<ScalarValue>],
	requested: &[ColumnOrdinal],
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	rows.iter()
		.map(|row| {
			requested
				.iter()
				.map(|column| {
					row.get(column.0 as usize).cloned().ok_or_else(|| {
						BindingError::UnsupportedExpression {
							message: format!(
								"internal: system table projection column {} out of range",
								column.0
							),
						}
					})
				})
				.collect::<Result<Vec<_>, _>>()
		})
		.collect()
}

pub(super) type OrdinalRemap = HashMap<ColumnOrdinal, ColumnOrdinal>;
