use super::*;

pub(super) fn predicate_includes_primary_key_equality(
	predicate: &BoundExprProgram,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	if primary_key_columns.len() != 1 {
		return false;
	}
	let Some(root) = predicate.single_root() else {
		return false;
	};
	predicate_node_includes_primary_key_equality(predicate, root, primary_key_columns)
}

pub(super) fn filter_includes_primary_key_row_cap(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
) -> bool {
	match input {
		ExecutionPlan::Scan {
			primary_key_columns,
			..
		} => predicate_includes_primary_key_equality(predicate, primary_key_columns),
		_ => false,
	}
}

pub(super) fn filter_equality_ndv_estimated_row_count(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
	facts: PlanOptimizerFacts,
) -> Option<RowCount> {
	if facts.origin != PlanOrigin::SingleBaseRelation {
		return None;
	}
	let base_rows = facts.row_upper_bound?;
	let root = predicate.single_root()?;
	predicate_node_equality_ndv_estimated_row_count(predicate, root, input, base_rows)
}

pub(super) fn filter_equality_ndv_estimate_evidence(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
	facts: PlanOptimizerFacts,
) -> Option<FilterEstimateEvidence> {
	if facts.origin != PlanOrigin::SingleBaseRelation {
		return None;
	}
	let base_rows = facts.row_upper_bound?;
	let root = predicate.single_root()?;
	predicate_node_equality_ndv_estimate_evidence(predicate, root, input, base_rows)
}

pub(super) fn predicate_node_equality_ndv_estimated_row_count(
	program: &BoundExprProgram,
	id: BoundExprId,
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<RowCount> {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Eq,
			left,
			right,
		}) => equality_ndv_estimated_row_count(program, *left, *right, input, base_rows)
			.or_else(|| equality_ndv_estimated_row_count(program, *right, *left, input, base_rows)),
		Some(BoundExprNode::InList {
			expr,
			list,
			negated,
		}) => {
			if *negated {
				None
			} else {
				in_list_ndv_estimated_row_count(program, *expr, list, input, base_rows)
			}
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			let left =
				predicate_node_equality_ndv_estimated_row_count(program, *left, input, base_rows);
			let right =
				predicate_node_equality_ndv_estimated_row_count(program, *right, input, base_rows);
			match (left, right) {
				(Some(left), Some(right)) => Some(std::cmp::min(left, right)),
				(Some(left), None) => Some(left),
				(None, Some(right)) => Some(right),
				(None, None) => None,
			}
		}
		_ => None,
	}
}

pub(super) fn predicate_node_equality_ndv_estimate_evidence(
	program: &BoundExprProgram,
	id: BoundExprId,
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<FilterEstimateEvidence> {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Eq,
			left,
			right,
		}) => equality_ndv_estimate_evidence(program, *left, *right, input, base_rows)
			.or_else(|| equality_ndv_estimate_evidence(program, *right, *left, input, base_rows)),
		Some(BoundExprNode::InList {
			expr,
			list,
			negated,
		}) => {
			if *negated {
				None
			} else {
				in_list_ndv_estimate_evidence(program, *expr, list, input, base_rows)
			}
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			let left =
				predicate_node_equality_ndv_estimate_evidence(program, *left, input, base_rows);
			let right =
				predicate_node_equality_ndv_estimate_evidence(program, *right, input, base_rows);
			min_filter_estimate_evidence(left, right)
		}
		_ => None,
	}
}

pub(super) fn equality_ndv_estimated_row_count(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<RowCount> {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return None;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return None;
	};
	let Some(value) = program.literals.get(*literal as usize) else {
		return None;
	};
	if !literal_is_non_null(value) {
		return None;
	}
	if let Some(fact) = column_heavy_hitter_for_input(input, column.ordinal)
		&& let Some(estimate) =
			heavy_hitter_equality_estimate(fact, value, input, column.ordinal, base_rows)
	{
		return Some(estimate);
	}
	let distinct_count = column_ndv_for_input(input, column.ordinal)?;
	Some(estimated_rows_for_ndv(
		base_rows,
		distinct_count.point_estimate(),
	))
}

pub(super) fn equality_ndv_estimate_evidence(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<FilterEstimateEvidence> {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return None;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return None;
	};
	let Some(value) = program.literals.get(*literal as usize) else {
		return None;
	};
	if !literal_is_non_null(value) {
		return None;
	}
	if let Some(fact) = column_heavy_hitter_for_input(input, column.ordinal)
		&& let Some(estimate) =
			heavy_hitter_equality_estimate(fact, value, input, column.ordinal, base_rows)
	{
		return Some(FilterEstimateEvidence {
			rows: estimate,
			source: FilterEstimateSource::HeavyHitterTopK,
			column: Some(column.ordinal),
		});
	}
	let distinct_count = column_ndv_for_input(input, column.ordinal)?;
	Some(FilterEstimateEvidence {
		rows: estimated_rows_for_ndv(base_rows, distinct_count.point_estimate()),
		source: FilterEstimateSource::NdvEquality,
		column: Some(column.ordinal),
	})
}

pub(super) fn in_list_ndv_estimated_row_count(
	program: &BoundExprProgram,
	expr: BoundExprId,
	list: &[BoundExprId],
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<RowCount> {
	let Some(BoundExprNode::Column(column)) = program.node(expr) else {
		return None;
	};
	let mut literal_keys = BTreeSet::new();
	for item in list {
		match in_list_literal_cardinality(program, *item)? {
			ScalarLiteralKeyClassification::Key(key) => {
				literal_keys.insert(key);
			}
			ScalarLiteralKeyClassification::Null => {}
			ScalarLiteralKeyClassification::Unsupported => return None,
		}
	}
	if literal_keys.is_empty() {
		return None;
	}
	if let Some(fact) = column_heavy_hitter_for_input(input, column.ordinal)
		&& let Some(estimate) =
			heavy_hitter_in_list_estimate(fact, &literal_keys, input, column.ordinal, base_rows)
	{
		return Some(estimate);
	}
	let distinct_count = column_ndv_for_input(input, column.ordinal)?;
	let rows_per_value = estimated_rows_for_ndv(base_rows, distinct_count.point_estimate());
	let distinct_literal_count = u64::try_from(literal_keys.len()).unwrap_or(u64::MAX);
	let estimated_rows = rows_per_value.get().saturating_mul(distinct_literal_count);
	Some(RowCount::new(std::cmp::min(
		base_rows.get(),
		estimated_rows,
	)))
}

pub(super) fn in_list_ndv_estimate_evidence(
	program: &BoundExprProgram,
	expr: BoundExprId,
	list: &[BoundExprId],
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<FilterEstimateEvidence> {
	let Some(BoundExprNode::Column(column)) = program.node(expr) else {
		return None;
	};
	let mut literal_keys = BTreeSet::new();
	for item in list {
		match in_list_literal_cardinality(program, *item)? {
			ScalarLiteralKeyClassification::Key(key) => {
				literal_keys.insert(key);
			}
			ScalarLiteralKeyClassification::Null => {}
			ScalarLiteralKeyClassification::Unsupported => return None,
		}
	}
	if literal_keys.is_empty() {
		return None;
	}
	if let Some(fact) = column_heavy_hitter_for_input(input, column.ordinal)
		&& let Some(estimate) =
			heavy_hitter_in_list_estimate(fact, &literal_keys, input, column.ordinal, base_rows)
	{
		return Some(FilterEstimateEvidence {
			rows: estimate,
			source: FilterEstimateSource::HeavyHitterTopK,
			column: Some(column.ordinal),
		});
	}
	let distinct_count = column_ndv_for_input(input, column.ordinal)?;
	let rows_per_value = estimated_rows_for_ndv(base_rows, distinct_count.point_estimate());
	let distinct_literal_count = u64::try_from(literal_keys.len()).unwrap_or(u64::MAX);
	let estimated_rows = rows_per_value.get().saturating_mul(distinct_literal_count);
	Some(FilterEstimateEvidence {
		rows: RowCount::new(std::cmp::min(base_rows.get(), estimated_rows)),
		source: FilterEstimateSource::NdvInList,
		column: Some(column.ordinal),
	})
}

pub(super) fn in_list_literal_cardinality(
	program: &BoundExprProgram,
	id: BoundExprId,
) -> Option<ScalarLiteralKeyClassification> {
	match program.node(id)? {
		BoundExprNode::NullLiteral => Some(ScalarLiteralKeyClassification::Null),
		BoundExprNode::Literal(literal) => {
			Some(scalar_literal_key(program.literals.get(*literal as usize)?))
		}
		_ => None,
	}
}

pub(super) fn column_ndv_for_input(
	input: &ExecutionPlan,
	requested_column: ColumnOrdinal,
) -> Option<DistinctCountFact> {
	match input {
		ExecutionPlan::Scan { column_ndv, .. } => {
			column_ndv.iter().find_map(|(column, distinct_count)| {
				(*column == requested_column).then_some(*distinct_count)
			})
		}
		ExecutionPlan::Filter { input, .. } => column_ndv_for_input(input, requested_column),
		_ => None,
	}
}

pub(super) fn clean_base_column_ndv_for_join_input(
	input: &ExecutionPlan,
	requested_output_column: ColumnOrdinal,
) -> Option<DistinctCountFact> {
	let ExecutionPlan::Scan {
		projection,
		column_ndv,
		..
	} = input
	else {
		return None;
	};

	let descriptor_column = match projection {
		Some(columns) => {
			let requested_index = usize::try_from(requested_output_column.0).ok()?;
			columns
				.get(requested_index)
				.copied()
				.unwrap_or(requested_output_column)
		}
		None => requested_output_column,
	};

	column_ndv.iter().find_map(|(column, distinct_count)| {
		(*column == descriptor_column).then_some(*distinct_count)
	})
}

pub(super) fn clean_base_column_range_for_join_input(
	input: &ExecutionPlan,
	requested_output_column: ColumnOrdinal,
) -> Option<RangeFact> {
	let ExecutionPlan::Scan {
		projection,
		column_range,
		..
	} = input
	else {
		return None;
	};

	let descriptor_column = match projection {
		Some(columns) => {
			let requested_index = usize::try_from(requested_output_column.0).ok()?;
			columns
				.get(requested_index)
				.copied()
				.unwrap_or(requested_output_column)
		}
		None => requested_output_column,
	};

	column_range
		.iter()
		.find_map(|(column, range)| (*column == descriptor_column).then(|| range.clone()))
}

pub(super) fn clean_base_column_heavy_hitter_for_join_input(
	input: &ExecutionPlan,
	requested_output_column: ColumnOrdinal,
) -> Option<&ColumnHeavyHitterFact> {
	let ExecutionPlan::Scan {
		projection,
		column_heavy_hitters,
		..
	} = input
	else {
		return None;
	};

	let descriptor_column = match projection {
		Some(columns) => {
			let requested_index = usize::try_from(requested_output_column.0).ok()?;
			columns
				.get(requested_index)
				.copied()
				.unwrap_or(requested_output_column)
		}
		None => requested_output_column,
	};

	column_heavy_hitters
		.iter()
		.find_map(|(column, fact)| (*column == descriptor_column).then_some(fact))
}

pub(super) fn clean_base_column_group_top_k_for_join_input(
	input: &ExecutionPlan,
	requested_output_columns: &[ColumnOrdinal],
) -> Option<ColumnGroupTopKFact> {
	let ExecutionPlan::Scan {
		projection,
		column_group_top_k,
		..
	} = input
	else {
		return None;
	};
	if requested_output_columns.len() < 2 {
		return None;
	}

	let mut descriptor_columns = Vec::with_capacity(requested_output_columns.len());
	for requested_output_column in requested_output_columns {
		let descriptor_column = match projection {
			Some(columns) => {
				let requested_index = usize::try_from(requested_output_column.0).ok()?;
				columns
					.get(requested_index)
					.copied()
					.unwrap_or(*requested_output_column)
			}
			None => *requested_output_column,
		};
		descriptor_columns.push(descriptor_column);
	}
	let requested_set = descriptor_columns.iter().copied().collect::<BTreeSet<_>>();
	if requested_set.len() != descriptor_columns.len() {
		return None;
	}

	for (fact_columns, fact) in column_group_top_k {
		if fact_columns.len() != descriptor_columns.len()
			|| fact_columns.iter().copied().collect::<BTreeSet<_>>() != requested_set
		{
			continue;
		}
		let mut permutation = Vec::with_capacity(descriptor_columns.len());
		for descriptor_column in &descriptor_columns {
			permutation.push(
				fact_columns
					.iter()
					.position(|column| column == descriptor_column)?,
			);
		}
		let mut normalized_entries = BTreeMap::new();
		for (tuple_key, frequency) in &fact.entries {
			if tuple_key.len() != permutation.len() || *frequency == 0 {
				return None;
			}
			let normalized_key = permutation
				.iter()
				.map(|index| tuple_key.get(*index).cloned())
				.collect::<Option<Vec<_>>>()?;
			if normalized_entries
				.insert(normalized_key, *frequency)
				.is_some()
			{
				return None;
			}
		}
		let arrow_type_tags = permutation
			.iter()
			.map(|index| fact.arrow_type_tags.get(*index).copied())
			.collect::<Option<Vec<_>>>()?;
		return Some(ColumnGroupTopKFact {
			arrow_type_tags,
			entries: normalized_entries,
		});
	}
	None
}

pub(super) fn clean_base_column_group_ndv_for_join_input(
	input: &ExecutionPlan,
	requested_output_columns: &[ColumnOrdinal],
) -> Option<DistinctCountFact> {
	let ExecutionPlan::Scan {
		projection,
		column_group_ndv,
		..
	} = input
	else {
		return None;
	};
	if requested_output_columns.len() < 2 {
		return None;
	}

	let mut descriptor_columns = Vec::with_capacity(requested_output_columns.len());
	for requested_output_column in requested_output_columns {
		let descriptor_column = match projection {
			Some(columns) => {
				let requested_index = usize::try_from(requested_output_column.0).ok()?;
				columns
					.get(requested_index)
					.copied()
					.unwrap_or(*requested_output_column)
			}
			None => *requested_output_column,
		};
		descriptor_columns.push(descriptor_column);
	}
	let requested_set = descriptor_columns.iter().copied().collect::<BTreeSet<_>>();
	if requested_set.len() != descriptor_columns.len() {
		return None;
	}

	for (fact_columns, distinct_count) in column_group_ndv {
		if fact_columns.len() != descriptor_columns.len()
			|| fact_columns.iter().copied().collect::<BTreeSet<_>>() != requested_set
			|| distinct_count.point_estimate() == 0
		{
			continue;
		}
		return Some(*distinct_count);
	}
	None
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct HotBothJoinContribution {
	pub(super) rows: u128,
	pub(super) count: u64,
	pub(super) left_rows: u128,
	pub(super) right_rows: u128,
}

pub(super) fn ndv_containment_join_rows(
	left_rows: RowCount,
	right_rows: RowCount,
	left_distinct_count: DistinctCountFact,
	right_distinct_count: DistinctCountFact,
) -> Option<RowCount> {
	let left_ndv = left_distinct_count.point_estimate();
	let right_ndv = right_distinct_count.point_estimate();
	ndv_containment_join_rows_from_points(left_rows, right_rows, left_ndv, right_ndv)
}

pub(super) fn ndv_containment_join_rows_from_points(
	left_rows: RowCount,
	right_rows: RowCount,
	left_ndv: u64,
	right_ndv: u64,
) -> Option<RowCount> {
	if left_ndv == 0 || right_ndv == 0 {
		return None;
	}

	ndv_containment_join_rows_from_denominator(
		left_rows,
		right_rows,
		u128::from(left_ndv.max(right_ndv)),
	)
}

pub(super) fn ndv_containment_join_rows_from_denominator(
	left_rows: RowCount,
	right_rows: RowCount,
	denominator: u128,
) -> Option<RowCount> {
	if denominator == 0 {
		return None;
	}

	let numerator = u128::from(left_rows.get()) * u128::from(right_rows.get());
	let estimated = if numerator == 0 {
		0
	} else {
		((numerator - 1) / denominator) + 1
	};

	Some(RowCount::new(u64::try_from(estimated).unwrap_or(u64::MAX)))
}

pub(super) fn multiply_denominator_capped_at_numerator(
	denominator: u128,
	factor: u128,
	numerator: u128,
) -> Option<u128> {
	if factor == 0 {
		return None;
	}
	if numerator > 0 && denominator >= numerator {
		return Some(numerator);
	}
	match denominator.checked_mul(factor) {
		Some(product) if numerator > 0 => Some(product.min(numerator)),
		Some(product) => Some(product),
		None if numerator > 0 => Some(numerator),
		None => Some(u128::MAX),
	}
}

pub(super) fn multi_key_join_cardinality(
	kind: JoinKind,
	left: &ScopedExecutionPlan,
	right: &ScopedExecutionPlan,
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
) -> Option<RowCount> {
	if kind != JoinKind::Inner
		|| left_keys.len() != right_keys.len()
		|| left_keys.len() < 2
		|| left.facts.origin != PlanOrigin::SingleBaseRelation
		|| right.facts.origin != PlanOrigin::SingleBaseRelation
	{
		return None;
	}

	let left_rows =
		estimate_row_source(left.facts.row_upper_bound, left.facts.estimated_row_count)?;
	let right_rows =
		estimate_row_source(right.facts.row_upper_bound, right.facts.estimated_row_count)?;
	let numerator = u128::from(left_rows.get()) * u128::from(right_rows.get());
	let mut denominator = 1_u128;
	for (left_key, right_key) in left_keys.iter().zip(right_keys) {
		let left_ndv = clean_base_column_ndv_for_join_input(&left.plan, left_key.column.ordinal)?
			.point_estimate();
		let right_ndv =
			clean_base_column_ndv_for_join_input(&right.plan, right_key.column.ordinal)?
				.point_estimate();
		denominator = multiply_denominator_capped_at_numerator(
			denominator,
			u128::from(left_ndv.max(right_ndv)),
			numerator,
		)?;
	}
	let left_key_ordinals = left_keys
		.iter()
		.map(|key| key.column.ordinal)
		.collect::<Vec<_>>();
	let right_key_ordinals = right_keys
		.iter()
		.map(|key| key.column.ordinal)
		.collect::<Vec<_>>();
	let mut effective_group_denominator = None;
	if let (Some(left_group_ndv), Some(right_group_ndv)) = (
		clean_base_column_group_ndv_for_join_input(&left.plan, &left_key_ordinals),
		clean_base_column_group_ndv_for_join_input(&right.plan, &right_key_ordinals),
	) {
		let composite_denominator = left_group_ndv
			.point_estimate()
			.max(right_group_ndv.point_estimate());
		if composite_denominator != 0 {
			denominator = denominator.min(u128::from(composite_denominator));
			effective_group_denominator = Some(denominator);
		}
	}

	let mut refined =
		ndv_containment_join_rows_from_denominator(left_rows, right_rows, denominator)?;
	for (left_key, right_key) in left_keys.iter().zip(right_keys) {
		if let (Some(left_range), Some(right_range)) = (
			clean_base_column_range_for_join_input(&left.plan, left_key.column.ordinal),
			clean_base_column_range_for_join_input(&right.plan, right_key.column.ordinal),
		) {
			refined = range_refined_containment_join_rows(
				refined,
				&left_range,
				&left_key.column.data_type,
				&right_range,
				&right_key.column.data_type,
			)
			.unwrap_or(refined);
		}
	}
	if refined.get() == 0 {
		return Some(refined);
	}
	let left_key_types = left_keys
		.iter()
		.map(|key| key.column.data_type.clone())
		.collect::<Vec<_>>();
	let right_key_types = right_keys
		.iter()
		.map(|key| key.column.data_type.clone())
		.collect::<Vec<_>>();
	let hot_tuple = match (
		clean_base_column_group_top_k_for_join_input(&left.plan, &left_key_ordinals),
		clean_base_column_group_top_k_for_join_input(&right.plan, &right_key_ordinals),
	) {
		(Some(left_fact), Some(right_fact)) => match hot_tuple_join_contribution(
			&left_fact,
			left_rows,
			&left_key_types,
			&right_fact,
			right_rows,
			&right_key_types,
		) {
			Some(hot_tuple) => hot_tuple,
			None => return Some(refined),
		},
		_ => return Some(refined),
	};
	let hot_floor = RowCount::new(
		refined
			.get()
			.max(u64::try_from(hot_tuple.rows).unwrap_or(u64::MAX)),
	);
	let Some(effective_group_denominator) = effective_group_denominator else {
		return Some(hot_floor);
	};

	let cold_left_rows = u128::from(left_rows.get()).saturating_sub(hot_tuple.left_rows);
	let cold_right_rows = u128::from(right_rows.get()).saturating_sub(hot_tuple.right_rows);
	let cold_denominator = effective_group_denominator.saturating_sub(u128::from(hot_tuple.count));
	let cold_residual = if cold_left_rows == 0 || cold_right_rows == 0 {
		0
	} else if cold_denominator == 0 {
		return Some(hot_floor);
	} else {
		let Some(mut cold_refined) = ndv_containment_join_rows_from_denominator(
			RowCount::new(u64::try_from(cold_left_rows).unwrap_or(u64::MAX)),
			RowCount::new(u64::try_from(cold_right_rows).unwrap_or(u64::MAX)),
			cold_denominator,
		) else {
			return Some(hot_floor);
		};
		for (left_key, right_key) in left_keys.iter().zip(right_keys) {
			if let (Some(left_range), Some(right_range)) = (
				clean_base_column_range_for_join_input(&left.plan, left_key.column.ordinal),
				clean_base_column_range_for_join_input(&right.plan, right_key.column.ordinal),
			) {
				cold_refined = range_refined_containment_join_rows(
					cold_refined,
					&left_range,
					&left_key.column.data_type,
					&right_range,
					&right_key.column.data_type,
				)
				.unwrap_or(cold_refined);
			}
		}
		u128::from(cold_refined.get())
	};
	let Some(candidate) = hot_tuple.rows.checked_add(cold_residual) else {
		return Some(hot_floor);
	};
	Some(RowCount::new(
		hot_floor
			.get()
			.max(u64::try_from(candidate).unwrap_or(u64::MAX)),
	))
}

pub(super) fn join_output_ndv_containment_estimate(
	kind: JoinKind,
	left: &ScopedExecutionPlan,
	right: &ScopedExecutionPlan,
	left_key: ColumnOrdinal,
	right_key: ColumnOrdinal,
	left_key_type: &DataType,
	right_key_type: &DataType,
) -> Option<RowCount> {
	if kind != JoinKind::Inner
		|| left.facts.origin != PlanOrigin::SingleBaseRelation
		|| right.facts.origin != PlanOrigin::SingleBaseRelation
	{
		return None;
	}

	let left_rows =
		estimate_row_source(left.facts.row_upper_bound, left.facts.estimated_row_count)?;
	let right_rows =
		estimate_row_source(right.facts.row_upper_bound, right.facts.estimated_row_count)?;
	let left_ndv = clean_base_column_ndv_for_join_input(&left.plan, left_key)?;
	let right_ndv = clean_base_column_ndv_for_join_input(&right.plan, right_key)?;
	let left_ndv_point = left_ndv.point_estimate();
	let right_ndv_point = right_ndv.point_estimate();

	let base = ndv_containment_join_rows(left_rows, right_rows, left_ndv, right_ndv)?;
	let left_range = clean_base_column_range_for_join_input(&left.plan, left_key);
	let right_range = clean_base_column_range_for_join_input(&right.plan, right_key);
	let range_overlap = match (&left_range, &right_range) {
		(Some(left_range), Some(right_range)) => {
			join_key_range_overlap_fraction(left_range, left_key_type, right_range, right_key_type)
		}
		_ => None,
	};
	let prior = match (&left_range, &right_range) {
		(Some(left_range), Some(right_range)) => range_refined_containment_join_rows(
			base,
			left_range,
			left_key_type,
			right_range,
			right_key_type,
		)
		.unwrap_or(base),
		_ => base,
	};
	if matches!(range_overlap, Some((0, _))) {
		return Some(prior);
	}

	let hot_both = match (
		clean_base_column_heavy_hitter_for_join_input(&left.plan, left_key),
		clean_base_column_heavy_hitter_for_join_input(&right.plan, right_key),
	) {
		(Some(left_fact), Some(right_fact)) => match hot_both_join_contribution(
			left_fact,
			left_rows,
			left_key_type,
			right_fact,
			right_rows,
			right_key_type,
		) {
			Some(hot_both) => hot_both,
			None => return Some(prior),
		},
		_ => return Some(prior),
	};
	let residual_left_rows = u128::from(left_rows.get()).saturating_sub(hot_both.left_rows);
	let residual_right_rows = u128::from(right_rows.get()).saturating_sub(hot_both.right_rows);
	let residual_left_ndv = left_ndv_point.saturating_sub(hot_both.count);
	let residual_right_ndv = right_ndv_point.saturating_sub(hot_both.count);
	let residual = if residual_left_rows == 0 || residual_right_rows == 0 {
		0
	} else if residual_left_ndv == 0 || residual_right_ndv == 0 {
		return Some(prior);
	} else {
		let cold_base = ndv_containment_join_rows_from_points(
			RowCount::new(u64::try_from(residual_left_rows).unwrap_or(u64::MAX)),
			RowCount::new(u64::try_from(residual_right_rows).unwrap_or(u64::MAX)),
			residual_left_ndv,
			residual_right_ndv,
		)?;
		let refined = match (&left_range, &right_range) {
			(Some(left_range), Some(right_range)) => range_refined_containment_join_rows(
				cold_base,
				left_range,
				left_key_type,
				right_range,
				right_key_type,
			)
			.unwrap_or(cold_base),
			_ => cold_base,
		};
		u128::from(refined.get())
	};
	let total = hot_both.rows.checked_add(residual)?;
	let candidate = RowCount::new(u64::try_from(total).unwrap_or(u64::MAX));
	Some(RowCount::new(prior.get().max(candidate.get())))
}

pub(super) fn hot_both_join_contribution(
	left: &ColumnHeavyHitterFact,
	left_rows: RowCount,
	left_key_type: &DataType,
	right: &ColumnHeavyHitterFact,
	right_rows: RowCount,
	right_key_type: &DataType,
) -> Option<HotBothJoinContribution> {
	heavy_hitter_total_frequency(left, left_rows)?;
	heavy_hitter_total_frequency(right, right_rows)?;
	if !heavy_hitter_fact_matches_join_key_type(left, left_key_type)
		|| !heavy_hitter_fact_matches_join_key_type(right, right_key_type)
	{
		return None;
	}

	let mut contribution = HotBothJoinContribution::default();
	if left.entries.len() <= right.entries.len() {
		for (key, left_frequency) in &left.entries {
			let Some(right_frequency) = right.entries.get(key) else {
				continue;
			};
			contribution.add(*left_frequency, *right_frequency)?;
		}
	} else {
		for (key, right_frequency) in &right.entries {
			let Some(left_frequency) = left.entries.get(key) else {
				continue;
			};
			contribution.add(*left_frequency, *right_frequency)?;
		}
	}
	Some(contribution)
}

pub(super) fn hot_tuple_join_contribution(
	left: &ColumnGroupTopKFact,
	left_rows: RowCount,
	left_key_types: &[DataType],
	right: &ColumnGroupTopKFact,
	right_rows: RowCount,
	right_key_types: &[DataType],
) -> Option<HotBothJoinContribution> {
	column_group_top_k_total_frequency(left, left_rows)?;
	column_group_top_k_total_frequency(right, right_rows)?;
	if !column_group_top_k_fact_matches_join_key_types(left, left_key_types)
		|| !column_group_top_k_fact_matches_join_key_types(right, right_key_types)
	{
		return None;
	}

	let mut contribution = HotBothJoinContribution::default();
	if left.entries.len() <= right.entries.len() {
		for (key, left_frequency) in &left.entries {
			let Some(right_frequency) = right.entries.get(key) else {
				continue;
			};
			contribution.add(*left_frequency, *right_frequency)?;
		}
	} else {
		for (key, right_frequency) in &right.entries {
			let Some(left_frequency) = left.entries.get(key) else {
				continue;
			};
			contribution.add(*left_frequency, *right_frequency)?;
		}
	}
	Some(contribution)
}

impl HotBothJoinContribution {
	pub(super) fn add(&mut self, left_frequency: u64, right_frequency: u64) -> Option<()> {
		let rows = u128::from(left_frequency).checked_mul(u128::from(right_frequency))?;
		self.rows = self.rows.checked_add(rows)?;
		self.count = self.count.checked_add(1)?;
		self.left_rows = self.left_rows.checked_add(u128::from(left_frequency))?;
		self.right_rows = self.right_rows.checked_add(u128::from(right_frequency))?;
		Some(())
	}
}

pub(super) fn column_group_top_k_total_frequency(
	fact: &ColumnGroupTopKFact,
	rows: RowCount,
) -> Option<u64> {
	let mut total = 0_u64;
	for frequency in fact.entries.values() {
		if *frequency == 0 {
			return None;
		}
		total = total.checked_add(*frequency)?;
	}
	(total <= rows.get()).then_some(total)
}

pub(super) fn column_group_top_k_fact_matches_join_key_types(
	fact: &ColumnGroupTopKFact,
	key_types: &[DataType],
) -> bool {
	if fact.arrow_type_tags.len() != key_types.len() {
		return false;
	}
	if !fact
		.arrow_type_tags
		.iter()
		.zip(key_types)
		.all(|(tag, key_type)| top_k_tag_matches_join_key_type(*tag, key_type))
	{
		return false;
	}
	fact.entries.keys().all(|tuple_key| {
		tuple_key.len() == key_types.len()
			&& tuple_key
				.iter()
				.zip(key_types)
				.all(|(key, key_type)| scalar_literal_key_matches_join_key_type(key, key_type))
	})
}

pub(super) fn heavy_hitter_fact_matches_join_key_type(
	fact: &ColumnHeavyHitterFact,
	key_type: &DataType,
) -> bool {
	fact.entries
		.keys()
		.all(|key| scalar_literal_key_matches_join_key_type(key, key_type))
}

pub(super) fn scalar_literal_key_matches_join_key_type(
	key: &ScalarLiteralKey,
	key_type: &DataType,
) -> bool {
	match (key, key_type) {
		(ScalarLiteralKey::Boolean(_), DataType::Boolean)
		| (ScalarLiteralKey::Int32(_), DataType::Int32)
		| (ScalarLiteralKey::Int64(_), DataType::Int64)
		| (ScalarLiteralKey::Date32(_), DataType::Date32)
		| (ScalarLiteralKey::Float32(_), DataType::Float32)
		| (ScalarLiteralKey::Float64(_), DataType::Float64)
		| (ScalarLiteralKey::Utf8(_), DataType::Utf8) => true,
		(ScalarLiteralKey::Timestamp(unit, _), DataType::Timestamp(key_unit, Some(timezone))) => {
			timezone.as_ref() == "UTC" && unit == key_unit
		}
		_ => false,
	}
}

pub(super) fn top_k_tag_matches_join_key_type(arrow_type_tag: u8, key_type: &DataType) -> bool {
	match (arrow_type_tag, key_type) {
		(ARROW_STATS_TAG_BOOLEAN, DataType::Boolean)
		| (ARROW_STATS_TAG_INT32, DataType::Int32)
		| (ARROW_STATS_TAG_INT64, DataType::Int64)
		| (ARROW_STATS_TAG_DATE32, DataType::Date32)
		| (ARROW_STATS_TAG_FLOAT32, DataType::Float32)
		| (ARROW_STATS_TAG_FLOAT64, DataType::Float64)
		| (ARROW_STATS_TAG_UTF8, DataType::Utf8) => true,
		(
			ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
			| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
			| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND,
			DataType::Timestamp(unit, Some(timezone)),
		) => timezone.as_ref() == "UTC" && timestamp_fact_unit(arrow_type_tag) == Some(*unit),
		_ => false,
	}
}

pub(super) fn range_refined_containment_join_rows(
	base: RowCount,
	left_range: &RangeFact,
	left_key_type: &DataType,
	right_range: &RangeFact,
	right_key_type: &DataType,
) -> Option<RowCount> {
	let (overlap_width, min_width) =
		join_key_range_overlap_fraction(left_range, left_key_type, right_range, right_key_type)?;
	if min_width == 0 {
		return None;
	}
	if overlap_width == 0 {
		return Some(RowCount::new(0));
	}
	let refined = u128::from(base.get()).checked_mul(overlap_width)? / min_width;
	let refined = if refined == 0 && base.get() > 0 {
		1
	} else {
		refined
	};
	Some(RowCount::new(u64::try_from(refined).unwrap_or(u64::MAX)))
}

pub(super) fn join_key_range_overlap_fraction(
	left_range: &RangeFact,
	left_key_type: &DataType,
	right_range: &RangeFact,
	right_key_type: &DataType,
) -> Option<(u128, u128)> {
	if !range_tag_matches_join_key_type(left_range.arrow_type_tag, left_key_type)
		|| !range_tag_matches_join_key_type(right_range.arrow_type_tag, right_key_type)
		|| left_range.arrow_type_tag != right_range.arrow_type_tag
	{
		return None;
	}

	let left_min = decode_range_scalar(left_range.arrow_type_tag, left_range.min.as_deref()?)?;
	let left_max = decode_range_scalar(left_range.arrow_type_tag, left_range.max.as_deref()?)?;
	let right_min = decode_range_scalar(right_range.arrow_type_tag, right_range.min.as_deref()?)?;
	let right_max = decode_range_scalar(right_range.arrow_type_tag, right_range.max.as_deref()?)?;
	let (
		RangeScalar::Integral(left_min),
		RangeScalar::Integral(left_max),
		RangeScalar::Integral(right_min),
		RangeScalar::Integral(right_max),
	) = (left_min, left_max, right_min, right_max)
	else {
		return None;
	};

	let left_width = integral_range_width(left_min, left_max)?;
	let right_width = integral_range_width(right_min, right_max)?;
	let min_width = left_width.min(right_width);
	if min_width == 0 {
		return None;
	}

	let overlap_low = left_min.max(right_min);
	let overlap_high = left_max.min(right_max);
	if overlap_high < overlap_low {
		return Some((0, 1));
	}
	let overlap_width = integral_range_width(overlap_low, overlap_high)?;
	Some((overlap_width, min_width))
}

pub(super) fn integral_range_width(low: i128, high: i128) -> Option<u128> {
	if high < low {
		return None;
	}
	u128::try_from(high.checked_sub(low)?.checked_add(1)?).ok()
}

pub(super) fn column_heavy_hitter_for_input(
	input: &ExecutionPlan,
	requested_column: ColumnOrdinal,
) -> Option<&ColumnHeavyHitterFact> {
	match input {
		ExecutionPlan::Scan {
			column_heavy_hitters,
			..
		} => column_heavy_hitters
			.iter()
			.find_map(|(column, fact)| (*column == requested_column).then_some(fact)),
		ExecutionPlan::Filter { input, .. } => {
			column_heavy_hitter_for_input(input, requested_column)
		}
		_ => None,
	}
}

pub(super) fn heavy_hitter_equality_estimate(
	fact: &ColumnHeavyHitterFact,
	value: &ScalarValue,
	input: &ExecutionPlan,
	column: ColumnOrdinal,
	base_rows: RowCount,
) -> Option<RowCount> {
	let total_frequency = heavy_hitter_total_frequency(fact, base_rows)?;
	let ScalarLiteralKeyClassification::Key(key) = scalar_literal_key(value) else {
		return None;
	};
	if let Some(frequency) = fact.entries.get(&key) {
		return Some(RowCount::new((*frequency).min(base_rows.get())));
	}
	let distinct_count = column_ndv_for_input(input, column)?;
	heavy_hitter_uniform_tail_from_total(
		fact,
		base_rows,
		distinct_count.point_estimate(),
		total_frequency,
	)
}

pub(super) fn heavy_hitter_in_list_estimate(
	fact: &ColumnHeavyHitterFact,
	literal_keys: &BTreeSet<ScalarLiteralKey>,
	input: &ExecutionPlan,
	column: ColumnOrdinal,
	base_rows: RowCount,
) -> Option<RowCount> {
	let total_frequency = heavy_hitter_total_frequency(fact, base_rows)?;
	let mut matched_frequency = 0_u64;
	let mut missed_keys = 0_u64;
	for key in literal_keys {
		if let Some(frequency) = fact.entries.get(key) {
			matched_frequency = matched_frequency.saturating_add(*frequency);
		} else {
			missed_keys = missed_keys.saturating_add(1);
		}
	}
	if missed_keys == 0 {
		return Some(RowCount::new(matched_frequency.min(base_rows.get())));
	}
	let distinct_count = column_ndv_for_input(input, column)?;
	let tail = heavy_hitter_uniform_tail_from_total(
		fact,
		base_rows,
		distinct_count.point_estimate(),
		total_frequency,
	)?;
	let remaining_rows = base_rows.get().saturating_sub(total_frequency);
	let tail_rows = missed_keys.saturating_mul(tail.get()).min(remaining_rows);
	Some(RowCount::new(
		base_rows
			.get()
			.min(matched_frequency.saturating_add(tail_rows)),
	))
}

pub(super) fn heavy_hitter_total_frequency(
	fact: &ColumnHeavyHitterFact,
	base_rows: RowCount,
) -> Option<u64> {
	let mut total = 0_u64;
	for frequency in fact.entries.values() {
		if *frequency == 0 {
			return None;
		}
		total = total.checked_add(*frequency)?;
		if total > base_rows.get() {
			return None;
		}
	}
	Some(total)
}

pub(super) fn heavy_hitter_uniform_tail_from_total(
	fact: &ColumnHeavyHitterFact,
	base_rows: RowCount,
	ndv: u64,
	total_frequency: u64,
) -> Option<RowCount> {
	let tracked_count = u64::try_from(fact.entries.len()).unwrap_or(u64::MAX);
	let tail_distinct = ndv.saturating_sub(tracked_count).max(1);
	let remaining_rows = base_rows.get().saturating_sub(total_frequency);
	let estimated =
		(u128::from(remaining_rows) + u128::from(tail_distinct) - 1) / u128::from(tail_distinct);
	Some(RowCount::new(
		u64::try_from(estimated)
			.unwrap_or(u64::MAX)
			.min(remaining_rows)
			.min(base_rows.get()),
	))
}

pub(super) fn filter_range_selectivity_estimated_row_count(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
	facts: PlanOptimizerFacts,
	logical_types: &LogicalTypeRegistry,
) -> Option<RowCount> {
	if facts.origin != PlanOrigin::SingleBaseRelation {
		return None;
	}
	let root = predicate.single_root()?;
	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);
	let mut intervals = HashMap::<ColumnOrdinal, RangeInterval>::new();

	for conjunct in conjuncts {
		let Some(atom) = range_atom(predicate, conjunct, input)
			.or_else(|| converted_linear_range_atom(predicate, conjunct, input, logical_types))
		else {
			continue;
		};
		intervals
			.entry(atom.column)
			.or_insert_with(|| RangeInterval::for_scalar(atom.literal_value))
			.apply(atom.op, atom.literal_value);
	}

	let mut estimate = None;
	for (column, interval) in intervals {
		let range = column_range_for_input(input, column)?;
		let distribution = column_range_distribution_for_input(input, column);
		let column_estimate = estimated_rows_for_range(range, distribution, interval)?;
		estimate = min_optional_row_count(estimate, Some(column_estimate));
	}
	estimate
}

pub(super) fn filter_range_selectivity_estimate_evidence(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
	facts: PlanOptimizerFacts,
	logical_types: &LogicalTypeRegistry,
) -> Option<FilterEstimateEvidence> {
	if facts.origin != PlanOrigin::SingleBaseRelation {
		return None;
	}
	let root = predicate.single_root()?;
	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);
	let mut intervals = HashMap::<ColumnOrdinal, (RangeInterval, FilterEstimateSource)>::new();

	for conjunct in conjuncts {
		let Some(atom) = range_atom(predicate, conjunct, input)
			.or_else(|| converted_linear_range_atom(predicate, conjunct, input, logical_types))
		else {
			continue;
		};
		let entry = intervals
			.entry(atom.column)
			.or_insert_with(|| (RangeInterval::for_scalar(atom.literal_value), atom.source));
		entry.0.apply(atom.op, atom.literal_value);
		if atom.source == FilterEstimateSource::ConvertedRange {
			entry.1 = FilterEstimateSource::ConvertedRange;
		}
	}

	let mut estimate = None;
	for (column, (interval, source)) in intervals {
		let range = column_range_for_input(input, column)?;
		let distribution = column_range_distribution_for_input(input, column);
		let column_estimate = estimated_rows_for_range(range, distribution, interval)?;
		estimate = min_filter_estimate_evidence(
			estimate,
			Some(FilterEstimateEvidence {
				rows: column_estimate,
				source,
				column: Some(column),
			}),
		);
	}
	estimate
}

pub(super) fn converted_linear_range_atom(
	program: &BoundExprProgram,
	id: BoundExprId,
	input: &ExecutionPlan,
	logical_types: &LogicalTypeRegistry,
) -> Option<RangeAtom> {
	let comparison = match_linear_converted_comparison(program, id, logical_types)?;
	let threshold =
		widened_converted_linear_threshold(comparison.constant, comparison.factor, &comparison.op)?;
	let range = column_range_for_input(input, comparison.column.ordinal)?;
	let literal_value = range_scalar_for_fact(
		range,
		&comparison.column.data_type,
		&ScalarValue::Float64(Some(threshold)),
	)?;
	Some(RangeAtom {
		column: comparison.column.ordinal,
		op: comparison.op,
		literal_value,
		source: FilterEstimateSource::ConvertedRange,
	})
}

pub(super) struct RangeAtom {
	column: ColumnOrdinal,
	op: BoundBinaryOp,
	literal_value: RangeScalar,
	source: FilterEstimateSource,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RangeScalar {
	Integral(i128),
	Real(f64),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FloatBound {
	value: f64,
	inclusive: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RangeInterval {
	Integral {
		lower_inclusive: Option<i128>,
		upper_inclusive: Option<i128>,
	},
	Real {
		lower: Option<FloatBound>,
		upper: Option<FloatBound>,
	},
}

impl RangeInterval {
	pub(super) fn for_scalar(value: RangeScalar) -> Self {
		match value {
			RangeScalar::Integral(_) => Self::Integral {
				lower_inclusive: None,
				upper_inclusive: None,
			},
			RangeScalar::Real(_) => Self::Real {
				lower: None,
				upper: None,
			},
		}
	}

	pub(super) fn apply(&mut self, op: BoundBinaryOp, literal_value: RangeScalar) {
		match (self, literal_value) {
			(
				RangeInterval::Integral {
					lower_inclusive,
					upper_inclusive,
				},
				RangeScalar::Integral(value),
			) => apply_integral_range_bound(op, value, lower_inclusive, upper_inclusive),
			(RangeInterval::Real { lower, upper }, RangeScalar::Real(value)) => {
				apply_real_range_bound(op, value, lower, upper)
			}
			_ => {}
		}
	}
}

pub(super) fn apply_integral_range_bound(
	op: BoundBinaryOp,
	value: i128,
	lower_inclusive: &mut Option<i128>,
	upper_inclusive: &mut Option<i128>,
) {
	match op {
		BoundBinaryOp::Gt => {
			*lower_inclusive = Some(
				lower_inclusive
					.map(|current| current.max(value + 1))
					.unwrap_or(value + 1),
			);
		}
		BoundBinaryOp::Gte => {
			*lower_inclusive = Some(
				lower_inclusive
					.map(|current| current.max(value))
					.unwrap_or(value),
			);
		}
		BoundBinaryOp::Lt => {
			*upper_inclusive = Some(
				upper_inclusive
					.map(|current| current.min(value - 1))
					.unwrap_or(value - 1),
			);
		}
		BoundBinaryOp::Lte => {
			*upper_inclusive = Some(
				upper_inclusive
					.map(|current| current.min(value))
					.unwrap_or(value),
			);
		}
		_ => {}
	}
}

pub(super) fn apply_real_range_bound(
	op: BoundBinaryOp,
	value: f64,
	lower: &mut Option<FloatBound>,
	upper: &mut Option<FloatBound>,
) {
	match op {
		BoundBinaryOp::Gt => {
			*lower = Some(tightest_real_lower(
				*lower,
				FloatBound {
					value,
					inclusive: false,
				},
			));
		}
		BoundBinaryOp::Gte => {
			*lower = Some(tightest_real_lower(
				*lower,
				FloatBound {
					value,
					inclusive: true,
				},
			));
		}
		BoundBinaryOp::Lt => {
			*upper = Some(tightest_real_upper(
				*upper,
				FloatBound {
					value,
					inclusive: false,
				},
			));
		}
		BoundBinaryOp::Lte => {
			*upper = Some(tightest_real_upper(
				*upper,
				FloatBound {
					value,
					inclusive: true,
				},
			));
		}
		_ => {}
	}
}

pub(super) fn tightest_real_lower(
	current: Option<FloatBound>,
	candidate: FloatBound,
) -> FloatBound {
	let Some(current) = current else {
		return candidate;
	};
	if candidate.value > current.value {
		candidate
	} else if candidate.value < current.value {
		current
	} else {
		FloatBound {
			value: current.value,
			inclusive: current.inclusive && candidate.inclusive,
		}
	}
}

pub(super) fn tightest_real_upper(
	current: Option<FloatBound>,
	candidate: FloatBound,
) -> FloatBound {
	let Some(current) = current else {
		return candidate;
	};
	if candidate.value < current.value {
		candidate
	} else if candidate.value > current.value {
		current
	} else {
		FloatBound {
			value: current.value,
			inclusive: current.inclusive && candidate.inclusive,
		}
	}
}

pub(super) fn range_atom(
	program: &BoundExprProgram,
	id: BoundExprId,
	input: &ExecutionPlan,
) -> Option<RangeAtom> {
	let BoundExprNode::Binary { op, left, right } = program.node(id)? else {
		return None;
	};
	if !matches!(
		op,
		BoundBinaryOp::Lt | BoundBinaryOp::Lte | BoundBinaryOp::Gt | BoundBinaryOp::Gte
	) {
		return None;
	}
	range_atom_sides(program, op.clone(), *left, *right, input)
		.or_else(|| range_atom_sides(program, flip_range_op(op.clone()), *right, *left, input))
}

pub(super) fn range_atom_sides(
	program: &BoundExprProgram,
	op: BoundBinaryOp,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	input: &ExecutionPlan,
) -> Option<RangeAtom> {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return None;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return None;
	};
	let range = column_range_for_input(input, column.ordinal)?;
	let value = program.literals.get(*literal as usize)?;
	let literal_value = range_scalar_for_fact(range, &column.data_type, value)?;
	Some(RangeAtom {
		column: column.ordinal,
		op,
		literal_value,
		source: FilterEstimateSource::Range,
	})
}

pub(super) fn flip_range_op(op: BoundBinaryOp) -> BoundBinaryOp {
	match op {
		BoundBinaryOp::Lt => BoundBinaryOp::Gt,
		BoundBinaryOp::Lte => BoundBinaryOp::Gte,
		BoundBinaryOp::Gt => BoundBinaryOp::Lt,
		BoundBinaryOp::Gte => BoundBinaryOp::Lte,
		other => other,
	}
}

pub(super) fn range_scalar_for_fact(
	range: &RangeFact,
	column_type: &DataType,
	value: &ScalarValue,
) -> Option<RangeScalar> {
	match (range.arrow_type_tag, column_type, value) {
		(ARROW_STATS_TAG_INT32, DataType::Int32, ScalarValue::Int32(Some(value))) => {
			Some(RangeScalar::Integral(i128::from(*value)))
		}
		(ARROW_STATS_TAG_INT64, DataType::Int64, ScalarValue::Int64(Some(value))) => {
			Some(RangeScalar::Integral(i128::from(*value)))
		}
		(ARROW_STATS_TAG_DATE32, DataType::Date32, ScalarValue::Date32(Some(value))) => {
			Some(RangeScalar::Integral(i128::from(*value)))
		}
		(
			ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
			| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
			| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND,
			DataType::Timestamp(column_unit, Some(timezone)),
			ScalarValue::Timestamp {
				value: Some(value),
				unit,
			},
		) if timezone.as_ref() == "UTC"
			&& timestamp_fact_unit(range.arrow_type_tag) == Some(*column_unit)
			&& column_unit == unit =>
		{
			Some(RangeScalar::Integral(i128::from(*value)))
		}
		(ARROW_STATS_TAG_FLOAT32 | ARROW_STATS_TAG_FLOAT64, _, ScalarValue::Int32(Some(value))) => {
			Some(RangeScalar::Real(f64::from(*value)))
		}
		(ARROW_STATS_TAG_FLOAT32 | ARROW_STATS_TAG_FLOAT64, _, ScalarValue::Int64(Some(value))) => {
			exact_i64_to_f64(*value).map(RangeScalar::Real)
		}
		(
			ARROW_STATS_TAG_FLOAT32 | ARROW_STATS_TAG_FLOAT64,
			_,
			ScalarValue::Float32(Some(value)),
		) => value
			.is_finite()
			.then_some(RangeScalar::Real(f64::from(*value))),
		(
			ARROW_STATS_TAG_FLOAT32 | ARROW_STATS_TAG_FLOAT64,
			_,
			ScalarValue::Float64(Some(value)),
		) => value.is_finite().then_some(RangeScalar::Real(*value)),
		_ => None,
	}
}

pub(super) fn timestamp_fact_unit(arrow_type_tag: u8) -> Option<TimeUnit> {
	match arrow_type_tag {
		ARROW_STATS_TAG_TIMESTAMP_MILLISECOND => Some(TimeUnit::Millisecond),
		ARROW_STATS_TAG_TIMESTAMP_MICROSECOND => Some(TimeUnit::Microsecond),
		ARROW_STATS_TAG_TIMESTAMP_NANOSECOND => Some(TimeUnit::Nanosecond),
		_ => None,
	}
}

pub(super) fn range_tag_matches_join_key_type(arrow_type_tag: u8, key_type: &DataType) -> bool {
	match (arrow_type_tag, key_type) {
		(ARROW_STATS_TAG_INT32, DataType::Int32)
		| (ARROW_STATS_TAG_INT64, DataType::Int64)
		| (ARROW_STATS_TAG_DATE32, DataType::Date32) => true,
		(
			ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
			| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
			| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND,
			DataType::Timestamp(unit, Some(timezone)),
		) => timezone.as_ref() == "UTC" && timestamp_fact_unit(arrow_type_tag) == Some(*unit),
		_ => false,
	}
}

pub(super) fn exact_i64_to_f64(value: i64) -> Option<f64> {
	const MAX_EXACT_F64_INTEGER: i64 = 9_007_199_254_740_992;
	(-MAX_EXACT_F64_INTEGER..=MAX_EXACT_F64_INTEGER)
		.contains(&value)
		.then_some(value as f64)
}

pub(super) fn column_range_for_input(
	input: &ExecutionPlan,
	requested_column: ColumnOrdinal,
) -> Option<&RangeFact> {
	match input {
		ExecutionPlan::Scan { column_range, .. } => column_range
			.iter()
			.find_map(|(column, range)| (*column == requested_column).then_some(range)),
		ExecutionPlan::Filter { input, .. } => column_range_for_input(input, requested_column),
		_ => None,
	}
}

pub(super) fn column_range_distribution_for_input(
	input: &ExecutionPlan,
	requested_column: ColumnOrdinal,
) -> Option<&ColumnRangeDistributionFact> {
	match input {
		ExecutionPlan::Scan {
			column_range_distributions,
			..
		} => column_range_distributions
			.iter()
			.find_map(|(column, fact)| (*column == requested_column).then_some(fact)),
		ExecutionPlan::Filter { input, .. } => {
			column_range_distribution_for_input(input, requested_column)
		}
		_ => None,
	}
}

pub(super) fn estimated_rows_for_range(
	range: &RangeFact,
	distribution: Option<&ColumnRangeDistributionFact>,
	interval: RangeInterval,
) -> Option<RowCount> {
	if range.non_null_count == 0 {
		return Some(RowCount::new(0));
	}
	let min = decode_range_scalar(range.arrow_type_tag, range.min.as_deref()?)?;
	let max = decode_range_scalar(range.arrow_type_tag, range.max.as_deref()?)?;
	match (min, max, interval) {
		(
			RangeScalar::Integral(min),
			RangeScalar::Integral(max),
			RangeInterval::Integral {
				lower_inclusive,
				upper_inclusive,
			},
		) => estimated_rows_for_integral_range(
			range.non_null_count,
			min,
			max,
			lower_inclusive,
			upper_inclusive,
			distribution,
			range.arrow_type_tag,
		),
		(RangeScalar::Real(min), RangeScalar::Real(max), RangeInterval::Real { lower, upper }) => {
			estimated_rows_for_real_range(
				range.non_null_count,
				min,
				max,
				lower,
				upper,
				distribution,
				range.arrow_type_tag,
			)
		}
		_ => None,
	}
}

pub(super) fn estimated_rows_for_integral_range(
	non_null_count: u64,
	envelope_low: i128,
	envelope_high: i128,
	lower_inclusive: Option<i128>,
	upper_inclusive: Option<i128>,
	distribution: Option<&ColumnRangeDistributionFact>,
	arrow_type_tag: u8,
) -> Option<RowCount> {
	if envelope_high < envelope_low {
		return None;
	}
	let low = lower_inclusive.unwrap_or(envelope_low).max(envelope_low);
	let high = upper_inclusive.unwrap_or(envelope_high).min(envelope_high);
	if low > high {
		return Some(RowCount::new(0));
	}
	if let Some(estimate) =
		range_distribution_integral_estimate(distribution, arrow_type_tag, low, high)
	{
		return Some(estimate);
	}
	let domain_width = u128::try_from(envelope_high - envelope_low + 1).ok()?;
	let overlap_width = u128::try_from(high - low + 1).ok()?;
	if domain_width == 0 {
		return None;
	}
	let numerator = u128::from(non_null_count).checked_mul(overlap_width)?;
	let estimate = numerator.div_ceil(domain_width);
	Some(RowCount::new(u64::try_from(estimate).unwrap_or(u64::MAX)))
}

pub(super) fn range_distribution_integral_estimate(
	distribution: Option<&ColumnRangeDistributionFact>,
	arrow_type_tag: u8,
	low: i128,
	high: i128,
) -> Option<RowCount> {
	let fact = distribution?;
	if fact.arrow_type_tag != arrow_type_tag || fact.non_null_count == 0 || fact.buckets.is_empty()
	{
		return None;
	}
	let mut total_count = 0_u64;
	let mut estimated = 0_u64;
	let mut previous_upper: Option<i128> = None;
	for bucket in &fact.buckets {
		if bucket.count == 0 {
			return None;
		}
		let RangeScalar::Integral(bucket_low) =
			decode_range_scalar(fact.arrow_type_tag, &bucket.lower)?
		else {
			return None;
		};
		let RangeScalar::Integral(bucket_high) =
			decode_range_scalar(fact.arrow_type_tag, &bucket.upper)?
		else {
			return None;
		};
		if bucket_high < bucket_low {
			return None;
		}
		if previous_upper.is_some_and(|previous| previous >= bucket_low) {
			return None;
		}
		total_count = total_count.checked_add(bucket.count)?;
		if total_count > fact.non_null_count {
			return None;
		}
		let overlap_low = low.max(bucket_low);
		let overlap_high = high.min(bucket_high);
		if overlap_low <= overlap_high {
			let bucket_estimate = if low <= bucket_low && bucket_high <= high {
				bucket.count
			} else {
				let bucket_width =
					u128::try_from(bucket_high.checked_sub(bucket_low)?.checked_add(1)?).ok()?;
				let overlap_width =
					u128::try_from(overlap_high.checked_sub(overlap_low)?.checked_add(1)?).ok()?;
				let numerator = u128::from(bucket.count).checked_mul(overlap_width)?;
				let estimate = numerator.div_ceil(bucket_width).max(1);
				u64::try_from(estimate)
					.unwrap_or(u64::MAX)
					.min(bucket.count)
			};
			estimated = estimated.checked_add(bucket_estimate)?;
		}
		previous_upper = Some(bucket_high);
	}
	Some(RowCount::new(estimated.min(fact.non_null_count)))
}

pub(super) fn estimated_rows_for_real_range(
	non_null_count: u64,
	envelope_low: f64,
	envelope_high: f64,
	lower: Option<FloatBound>,
	upper: Option<FloatBound>,
	distribution: Option<&ColumnRangeDistributionFact>,
	arrow_type_tag: u8,
) -> Option<RowCount> {
	if !envelope_low.is_finite() || !envelope_high.is_finite() || envelope_high < envelope_low {
		return None;
	}
	if envelope_high == envelope_low {
		return Some(
			if real_interval_contains_value(lower, upper, envelope_low) {
				RowCount::new(non_null_count)
			} else {
				RowCount::new(0)
			},
		);
	}
	if real_interval_is_empty(lower, upper) {
		return Some(RowCount::new(0));
	}
	let low = clip_real_lower(lower, envelope_low);
	let high = clip_real_upper(upper, envelope_high);
	if low.value > high.value {
		return Some(RowCount::new(0));
	}
	if low.value == high.value {
		return Some(if low.inclusive && high.inclusive {
			RowCount::new(1)
		} else {
			RowCount::new(0)
		});
	}
	if let Some(estimate) =
		range_distribution_real_estimate(distribution, arrow_type_tag, low.value, high.value)
	{
		return Some(estimate);
	}
	let width = envelope_high - envelope_low;
	if width <= 0.0 {
		return None;
	}
	let frac = ((high.value - low.value) / width).clamp(0.0, 1.0);
	if frac == 0.0 {
		return Some(RowCount::new(0));
	}
	let estimate = ((non_null_count as f64) * frac).ceil().max(1.0);
	let estimate = if estimate > u64::MAX as f64 {
		u64::MAX
	} else {
		estimate as u64
	};
	Some(RowCount::new(estimate))
}

pub(super) fn range_distribution_real_estimate(
	distribution: Option<&ColumnRangeDistributionFact>,
	arrow_type_tag: u8,
	low: f64,
	high: f64,
) -> Option<RowCount> {
	let fact = distribution?;
	if fact.arrow_type_tag != arrow_type_tag || fact.non_null_count == 0 || fact.buckets.is_empty()
	{
		return None;
	}
	let mut total_count = 0_u64;
	let mut estimated = 0_u64;
	let mut previous_upper: Option<f64> = None;
	for bucket in &fact.buckets {
		if bucket.count == 0 {
			return None;
		}
		let RangeScalar::Real(bucket_low) =
			decode_range_scalar(fact.arrow_type_tag, &bucket.lower)?
		else {
			return None;
		};
		let RangeScalar::Real(bucket_high) =
			decode_range_scalar(fact.arrow_type_tag, &bucket.upper)?
		else {
			return None;
		};
		if !bucket_low.is_finite() || !bucket_high.is_finite() || bucket_high <= bucket_low {
			return None;
		}
		if previous_upper.is_some_and(|previous| previous >= bucket_low) {
			return None;
		}
		total_count = total_count.checked_add(bucket.count)?;
		if total_count > fact.non_null_count {
			return None;
		}
		let overlap_low = low.max(bucket_low);
		let overlap_high = high.min(bucket_high);
		if overlap_low < overlap_high {
			let bucket_estimate = if low <= bucket_low && bucket_high <= high {
				bucket.count
			} else {
				let bucket_width = bucket_high - bucket_low;
				let overlap_width = overlap_high - overlap_low;
				if bucket_width <= 0.0 || overlap_width <= 0.0 {
					return None;
				}
				let estimate = ((bucket.count as f64) * (overlap_width / bucket_width))
					.ceil()
					.max(1.0);
				if !estimate.is_finite() {
					return None;
				}
				if estimate > u64::MAX as f64 {
					u64::MAX
				} else {
					estimate as u64
				}
				.min(bucket.count)
			};
			estimated = estimated.checked_add(bucket_estimate)?;
		}
		previous_upper = Some(bucket_high);
	}
	Some(RowCount::new(estimated.min(fact.non_null_count)))
}

pub(super) fn real_interval_contains_value(
	lower: Option<FloatBound>,
	upper: Option<FloatBound>,
	value: f64,
) -> bool {
	lower
		.map(|bound| value > bound.value || (value == bound.value && bound.inclusive))
		.unwrap_or(true)
		&& upper
			.map(|bound| value < bound.value || (value == bound.value && bound.inclusive))
			.unwrap_or(true)
}

pub(super) fn real_interval_is_empty(lower: Option<FloatBound>, upper: Option<FloatBound>) -> bool {
	match (lower, upper) {
		(Some(lower), Some(upper)) if lower.value > upper.value => true,
		(Some(lower), Some(upper)) if lower.value == upper.value => {
			!(lower.inclusive && upper.inclusive)
		}
		_ => false,
	}
}

pub(super) fn clip_real_lower(lower: Option<FloatBound>, envelope_low: f64) -> FloatBound {
	match lower {
		Some(bound) if bound.value > envelope_low => bound,
		_ => FloatBound {
			value: envelope_low,
			inclusive: true,
		},
	}
}

pub(super) fn clip_real_upper(upper: Option<FloatBound>, envelope_high: f64) -> FloatBound {
	match upper {
		Some(bound) if bound.value < envelope_high => bound,
		_ => FloatBound {
			value: envelope_high,
			inclusive: true,
		},
	}
}

pub(super) fn decode_range_scalar(arrow_type_tag: u8, bytes: &[u8]) -> Option<RangeScalar> {
	match arrow_type_tag {
		ARROW_STATS_TAG_INT32 if bytes.len() == 4 => decode_signed_optimizer_fact_scalar(bytes)
			.map(|value| RangeScalar::Integral(value.into())),
		ARROW_STATS_TAG_INT64 if bytes.len() == 8 => decode_signed_optimizer_fact_scalar(bytes)
			.map(|value| RangeScalar::Integral(value.into())),
		ARROW_STATS_TAG_DATE32 if bytes.len() == 4 => decode_signed_optimizer_fact_scalar(bytes)
			.map(|value| RangeScalar::Integral(value.into())),
		ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
		| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
		| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND
			if bytes.len() == 8 =>
		{
			decode_signed_optimizer_fact_scalar(bytes)
				.map(|value| RangeScalar::Integral(value.into()))
		}
		ARROW_STATS_TAG_FLOAT32 if bytes.len() == 4 => {
			decode_float32_optimizer_fact_scalar(bytes).map(|value| RangeScalar::Real(value.into()))
		}
		ARROW_STATS_TAG_FLOAT64 if bytes.len() == 8 => {
			decode_float64_optimizer_fact_scalar(bytes).map(RangeScalar::Real)
		}
		_ => None,
	}
}

pub(super) fn min_optional_row_count(
	left: Option<RowCount>,
	right: Option<RowCount>,
) -> Option<RowCount> {
	match (left, right) {
		(Some(left), Some(right)) => Some(std::cmp::min(left, right)),
		(Some(left), None) => Some(left),
		(None, Some(right)) => Some(right),
		(None, None) => None,
	}
}

pub(super) fn min_filter_estimate_evidence(
	left: Option<FilterEstimateEvidence>,
	right: Option<FilterEstimateEvidence>,
) -> Option<FilterEstimateEvidence> {
	match (left, right) {
		(Some(left), Some(right)) => {
			if left.rows <= right.rows {
				Some(left)
			} else {
				Some(right)
			}
		}
		(Some(left), None) => Some(left),
		(None, Some(right)) => Some(right),
		(None, None) => None,
	}
}

pub(super) fn estimated_rows_for_ndv(base_rows: RowCount, ndv: u64) -> RowCount {
	let rows = base_rows.get();
	if rows == 0 {
		return RowCount::new(0);
	}
	let divisor = u128::from(ndv.max(1));
	let estimated = (u128::from(rows) + divisor - 1) / divisor;
	RowCount::new(u64::try_from(estimated).unwrap_or(u64::MAX).max(1))
}

pub(super) fn predicate_node_includes_primary_key_equality(
	program: &BoundExprProgram,
	id: BoundExprId,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Eq,
			left,
			right,
		}) => {
			primary_key_literal_equality(program, *left, *right, primary_key_columns)
				|| primary_key_literal_equality(program, *right, *left, primary_key_columns)
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			predicate_node_includes_primary_key_equality(program, *left, primary_key_columns)
				|| predicate_node_includes_primary_key_equality(
					program,
					*right,
					primary_key_columns,
				)
		}
		_ => false,
	}
}

pub(super) fn primary_key_literal_equality(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return false;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return false;
	};
	let Some(value) = program.literals.get(*literal as usize) else {
		return false;
	};
	primary_key_columns.contains(&column.ordinal) && literal_is_non_null(value)
}

pub(super) fn literal_is_non_null(value: &ScalarValue) -> bool {
	match value {
		ScalarValue::Boolean(value) => value.is_some(),
		ScalarValue::Int32(value) => value.is_some(),
		ScalarValue::Int64(value) => value.is_some(),
		ScalarValue::Date32(value) => value.is_some(),
		ScalarValue::Timestamp { value, .. } => value.is_some(),
		ScalarValue::Interval(value) => value.is_some(),
		ScalarValue::CalendarMonthInterval(value) => value.is_some(),
		ScalarValue::Float32(value) => value.is_some(),
		ScalarValue::Float64(value) => value.is_some(),
		ScalarValue::Decimal128 { value, .. } => value.is_some(),
		ScalarValue::Utf8(value) => value.is_some(),
	}
}
