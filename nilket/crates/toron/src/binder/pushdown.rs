use super::*;

pub(super) fn compile_pbc(predicate: &BoundExprProgram) -> Option<PbcProgram> {
	PbcCompiler::new()
		.compile(Some(predicate), None)
		.ok()
		.map(PbcProgram)
}

pub(super) const BOOLEAN_FACTOR_MAX_NODES: usize = 256;
pub(super) const BOOLEAN_FACTOR_MAX_OR_BRANCHES: usize = 16;
pub(super) const BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH: usize = 16;
pub(super) const BOOLEAN_FACTOR_MAX_CANDIDATES: usize = 8;
pub(super) const STATIC_ELISION_MAX_AND_CONJUNCTS: usize = BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH;

pub(super) fn attach_pbc_program(
	mut plan: ExecutionPlan,
	program: PbcProgram,
) -> (ExecutionPlan, bool) {
	match &mut plan {
		ExecutionPlan::Scan { pbc_program, .. } => {
			*pbc_program = Some(program);
			(plan, true)
		}
		_ => (plan, false),
	}
}

pub(super) struct OptimizedJoinInputs {
	pub(super) left: ExecutionPlan,
	pub(super) right: ExecutionPlan,
	pub(super) left_keys: Vec<ColumnOrdinal>,
	pub(super) right_keys: Vec<ColumnOrdinal>,
	pub(super) match_filter: Option<BoundExprProgram>,
	pub(super) output_schema: SchemaRef,
	pub(super) output_remap: OrdinalRemap,
}

pub(super) fn optimize_join_inputs_for_required(
	kind: JoinKind,
	left: ExecutionPlan,
	right: ExecutionPlan,
	left_keys: Vec<ColumnOrdinal>,
	right_keys: Vec<ColumnOrdinal>,
	match_filter: Option<BoundExprProgram>,
	output_schema: &SchemaRef,
	required_output: Option<BTreeSet<ColumnOrdinal>>,
	logical_types: &LogicalTypeRegistry,
) -> Result<OptimizedJoinInputs, BindingError> {
	let original_left_width = left.schema().fields().len();
	let original_right_width = right.schema().fields().len();
	let requested_outputs = required_output.unwrap_or_else(|| {
		(0..output_schema.fields().len())
			.map(|idx| ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow")))
			.collect()
	});
	let requested_outputs_vec = requested_outputs.iter().copied().collect::<Vec<_>>();
	let mut left_required = BTreeSet::new();
	let mut right_required = BTreeSet::new();
	for output in &requested_outputs_vec {
		collect_join_output_column_requirement(
			kind,
			original_left_width,
			original_right_width,
			*output,
			&mut left_required,
			&mut right_required,
		)?;
	}
	left_required.extend(left_keys.iter().copied());
	right_required.extend(right_keys.iter().copied());
	if let Some(filter) = &match_filter {
		let mut filter_columns = BTreeSet::new();
		collect_program_columns(filter, &mut filter_columns);
		for column in filter_columns {
			collect_concat_column_requirement(
				original_left_width,
				original_right_width,
				column,
				&mut left_required,
				&mut right_required,
			)?;
		}
	}
	retain_row_bearing_column(&mut left_required, original_left_width);
	retain_row_bearing_column(&mut right_required, original_right_width);

	let (left, left_remap) = optimize_for_required(left, Some(left_required), logical_types)?;
	let (right, right_remap) = optimize_for_required(right, Some(right_required), logical_types)?;
	let optimized_left_width = left.schema().fields().len();
	let left_keys = rewrite_join_side_keys(left_keys, &left_remap, "left join key")?;
	let right_keys = rewrite_join_side_keys(right_keys, &right_remap, "right join key")?;
	let match_filter = match_filter
		.map(|filter| {
			let filter_remap = join_concat_remap(
				original_left_width,
				original_right_width,
				optimized_left_width,
				&left_remap,
				&right_remap,
			);
			rewrite_program_ordinals(filter, &filter_remap)
		})
		.transpose()?;
	let output_remap = join_output_remap(
		kind,
		original_left_width,
		original_right_width,
		optimized_left_width,
		&left_remap,
		&right_remap,
		&requested_outputs_vec,
	)?;
	let output_schema = join_schema(kind, &left.schema(), &right.schema());
	Ok(OptimizedJoinInputs {
		left,
		right,
		left_keys,
		right_keys,
		match_filter,
		output_schema,
		output_remap,
	})
}

fn retain_row_bearing_column(required: &mut BTreeSet<ColumnOrdinal>, width: usize) {
	if required.is_empty() && width > 0 {
		required.insert(ColumnOrdinal(0));
	}
}

pub(super) fn collect_join_output_column_requirement(
	kind: JoinKind,
	left_width: usize,
	right_width: usize,
	output: ColumnOrdinal,
	left_required: &mut BTreeSet<ColumnOrdinal>,
	right_required: &mut BTreeSet<ColumnOrdinal>,
) -> Result<(), BindingError> {
	match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => collect_concat_column_requirement(
			left_width,
			right_width,
			output,
			left_required,
			right_required,
		),
		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			validate_side_column(output, left_width, "left semi/anti join output")?;
			left_required.insert(output);
			Ok(())
		}
		JoinKind::RightSemi | JoinKind::RightAnti => {
			validate_side_column(output, right_width, "right semi/anti join output")?;
			right_required.insert(output);
			Ok(())
		}
	}
}

pub(super) fn collect_concat_column_requirement(
	left_width: usize,
	right_width: usize,
	column: ColumnOrdinal,
	left_required: &mut BTreeSet<ColumnOrdinal>,
	right_required: &mut BTreeSet<ColumnOrdinal>,
) -> Result<(), BindingError> {
	let index = column.0 as usize;
	if index < left_width {
		left_required.insert(column);
		return Ok(());
	}
	let right_index = index.saturating_sub(left_width);
	if right_index < right_width {
		right_required.insert(ColumnOrdinal(
			u32::try_from(right_index).expect("column ordinal overflow"),
		));
		return Ok(());
	}
	Err(BindingError::UnknownColumn {
		column: format!("join input ordinal {}", column.0),
	})
}

fn validate_side_column(
	column: ColumnOrdinal,
	width: usize,
	label: &str,
) -> Result<(), BindingError> {
	if (column.0 as usize) < width {
		return Ok(());
	}
	Err(BindingError::UnknownColumn {
		column: format!("{label} ordinal {}", column.0),
	})
}

pub(super) fn rewrite_join_side_keys(
	keys: Vec<ColumnOrdinal>,
	remap: &OrdinalRemap,
	label: &str,
) -> Result<Vec<ColumnOrdinal>, BindingError> {
	keys.into_iter()
		.map(|column| {
			remap
				.get(&column)
				.copied()
				.ok_or_else(|| BindingError::UnsupportedExpression {
					message: format!("missing ordinal remap for {label} {:?}", column),
				})
		})
		.collect()
}

pub(super) fn join_concat_remap(
	original_left_width: usize,
	original_right_width: usize,
	optimized_left_width: usize,
	left_remap: &OrdinalRemap,
	right_remap: &OrdinalRemap,
) -> OrdinalRemap {
	let mut remap = OrdinalRemap::new();
	for original in 0..original_left_width {
		let original = ColumnOrdinal(u32::try_from(original).expect("column ordinal overflow"));
		if let Some(mapped) = left_remap.get(&original) {
			remap.insert(original, *mapped);
		}
	}
	for original in 0..original_right_width {
		let original_column =
			ColumnOrdinal(u32::try_from(original).expect("column ordinal overflow"));
		if let Some(mapped) = right_remap.get(&original_column) {
			let original_output = ColumnOrdinal(
				u32::try_from(original_left_width + original).expect("column ordinal overflow"),
			);
			let mapped_output = ColumnOrdinal(
				u32::try_from(optimized_left_width + mapped.0 as usize)
					.expect("column ordinal overflow"),
			);
			remap.insert(original_output, mapped_output);
		}
	}
	remap
}

pub(super) fn join_output_remap(
	kind: JoinKind,
	original_left_width: usize,
	original_right_width: usize,
	optimized_left_width: usize,
	left_remap: &OrdinalRemap,
	right_remap: &OrdinalRemap,
	requested_outputs: &[ColumnOrdinal],
) -> Result<OrdinalRemap, BindingError> {
	let mut output_remap = OrdinalRemap::new();
	let concat_remap = join_concat_remap(
		original_left_width,
		original_right_width,
		optimized_left_width,
		left_remap,
		right_remap,
	);
	for output in requested_outputs {
		let mapped = match kind {
			JoinKind::Inner
			| JoinKind::Cross
			| JoinKind::LeftOuter
			| JoinKind::RightOuter
			| JoinKind::FullOuter => concat_remap.get(output).copied().ok_or_else(|| {
				BindingError::UnsupportedExpression {
					message: format!("missing ordinal remap for join output {:?}", output),
				}
			})?,
			JoinKind::LeftSemi | JoinKind::LeftAnti => {
				left_remap.get(output).copied().ok_or_else(|| {
					BindingError::UnsupportedExpression {
						message: format!(
							"missing ordinal remap for left semi/anti output {:?}",
							output
						),
					}
				})?
			}
			JoinKind::RightSemi | JoinKind::RightAnti => right_remap
				.get(output)
				.copied()
				.ok_or_else(|| BindingError::UnsupportedExpression {
					message: format!(
						"missing ordinal remap for right semi/anti output {:?}",
						output
					),
				})?,
		};
		output_remap.insert(*output, mapped);
	}
	Ok(output_remap)
}

pub(super) struct PredicatePushdownPlan {
	pub(super) pushed_predicate: Option<BoundExprProgram>,
	pub(super) residual_predicate: Option<BoundExprProgram>,
	pub(super) pbc_program: Option<PbcProgram>,
	pub(super) profile_hint: Option<QueryProfileHint>,
	pub(super) converted_pushdown: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScanFilterPredicateRender {
	pub(super) pushed_predicate_text: Option<String>,
	pub(super) residual_predicate_text: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticFilterClass {
	AlwaysPass,
	AlwaysReject,
	Unknown,
}

pub(super) fn plan_filter_with_predicate_pushdown(
	input: ExecutionPlan,
	predicate: BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> (ExecutionPlan, QueryProfileHint) {
	let (plan, profile_hint, _, _) =
		plan_filter_with_predicate_pushdown_evidence(input, predicate, logical_types, false);
	(plan, profile_hint)
}

pub(super) fn plan_filter_with_predicate_pushdown_evidence(
	mut input: ExecutionPlan,
	predicate: BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
	render_predicates: bool,
) -> (
	ExecutionPlan,
	QueryProfileHint,
	ScanFilterPushdownEvidence,
	Option<ScanFilterPredicateRender>,
) {
	let table_id = scan_table_id(&input);
	let original_predicate = predicate.clone();
	let fallback_profile = scan_profile_hint(&original_predicate, &input);
	if matches!(
		static_filter_class(&input, &original_predicate, logical_types),
		StaticFilterClass::AlwaysReject
	) {
		let profile_hint = fallback_profile.unwrap_or(QueryProfileHint::Scan);
		return (
			ExecutionPlan::EmptyRelation {
				schema: input.schema(),
			},
			profile_hint,
			ScanFilterPushdownEvidence {
				outcome: ScanFilterOutcome::StaticElided,
				converted_pushdown: false,
				profile_hint,
				table_id,
			},
			None,
		);
	}

	let pushdown = predicate_pushdown_plan(predicate, &input, logical_types);
	let predicate_render = render_predicates.then(|| ScanFilterPredicateRender {
		pushed_predicate_text: pushdown
			.pushed_predicate
			.as_ref()
			.map(BoundExprProgram::pretty),
		residual_predicate_text: pushdown
			.residual_predicate
			.as_ref()
			.map(BoundExprProgram::pretty),
	});
	let profile_hint = pushdown
		.profile_hint
		.or(fallback_profile)
		.unwrap_or(QueryProfileHint::Scan);
	apply_scan_profile_hint(&mut input, Some(profile_hint));

	if let Some(pbc_program) = pushdown.pbc_program {
		let (filter_input, attached) = attach_pbc_program(input, pbc_program);
		if attached {
			let has_residual = pushdown.residual_predicate.is_some();
			let (predicate, disposition) = match pushdown.residual_predicate {
				Some(residual_predicate) => {
					(residual_predicate, FilterDisposition::EvaluateInToron)
				}
				None => (
					pushdown
						.pushed_predicate
						.expect("PBC program requires pushed predicate"),
					FilterDisposition::FullyPushed,
				),
			};
			let outcome =
				scan_filter_outcome_from_disposition(profile_hint, disposition, has_residual);
			return (
				ExecutionPlan::Filter {
					input: Box::new(filter_input),
					predicate,
					disposition,
				},
				profile_hint,
				ScanFilterPushdownEvidence {
					outcome,
					converted_pushdown: pushdown.converted_pushdown,
					profile_hint,
					table_id,
				},
				predicate_render,
			);
		}

		let mut filter_input = filter_input;
		let fallback_profile = fallback_profile.unwrap_or(QueryProfileHint::Scan);
		apply_scan_profile_hint(&mut filter_input, Some(fallback_profile));
		let predicate_render = render_predicates.then(|| ScanFilterPredicateRender {
			pushed_predicate_text: None,
			residual_predicate_text: Some(original_predicate.pretty()),
		});
		return (
			ExecutionPlan::Filter {
				input: Box::new(filter_input),
				predicate: original_predicate,
				disposition: FilterDisposition::EvaluateInToron,
			},
			fallback_profile,
			ScanFilterPushdownEvidence {
				outcome: scan_filter_outcome_from_disposition(
					fallback_profile,
					FilterDisposition::EvaluateInToron,
					false,
				),
				converted_pushdown: false,
				profile_hint: fallback_profile,
				table_id,
			},
			predicate_render,
		);
	}

	let residual_predicate_text = render_predicates.then(|| {
		pushdown
			.residual_predicate
			.as_ref()
			.map(BoundExprProgram::pretty)
			.unwrap_or_else(|| original_predicate.pretty())
	});
	(
		ExecutionPlan::Filter {
			input: Box::new(input),
			predicate: pushdown.residual_predicate.unwrap_or(original_predicate),
			disposition: FilterDisposition::EvaluateInToron,
		},
		profile_hint,
		ScanFilterPushdownEvidence {
			outcome: scan_filter_outcome_from_disposition(
				profile_hint,
				FilterDisposition::EvaluateInToron,
				false,
			),
			converted_pushdown: false,
			profile_hint,
			table_id,
		},
		render_predicates.then(|| ScanFilterPredicateRender {
			pushed_predicate_text: None,
			residual_predicate_text,
		}),
	)
}

pub(super) fn scan_table_id(input: &ExecutionPlan) -> Option<TableId> {
	match input {
		ExecutionPlan::Scan { local_table_id, .. } => Some(*local_table_id),
		ExecutionPlan::Filter { input, .. } => scan_table_id(input),
		_ => None,
	}
}

pub(super) fn spent_column_name_for_predicate(
	predicate: &BoundExprProgram,
	column: ColumnOrdinal,
) -> Option<String> {
	let mut name: Option<String> = None;
	for node in &predicate.nodes {
		let BoundExprNode::Column(resolved) = node else {
			continue;
		};
		if resolved.ordinal != column {
			continue;
		}
		if name
			.as_deref()
			.is_some_and(|existing| existing != resolved.name.0)
		{
			return None;
		}
		name.get_or_insert_with(|| resolved.name.0.clone());
	}
	name
}

pub(super) fn scan_filter_outcome_from_disposition(
	profile_hint: QueryProfileHint,
	disposition: FilterDisposition,
	has_residual: bool,
) -> ScanFilterOutcome {
	if profile_hint == QueryProfileHint::PointRead {
		return ScanFilterOutcome::PointRead;
	}
	match (disposition, has_residual) {
		(FilterDisposition::FullyPushed, _) => ScanFilterOutcome::FullyPushed,
		(FilterDisposition::EvaluateInToron, true) => ScanFilterOutcome::PartiallyPushed,
		(FilterDisposition::EvaluateInToron, false) => ScanFilterOutcome::ResidualOnly,
	}
}

pub(super) fn static_filter_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> StaticFilterClass {
	if let Some(class) = static_filter_and_class(input, predicate, logical_types) {
		return class;
	}
	static_filter_atom_class(input, predicate, logical_types)
}

pub(super) fn static_filter_atom_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> StaticFilterClass {
	if structural_non_nullable_is_null(predicate) {
		return StaticFilterClass::AlwaysReject;
	}
	if matches!(
		refinement_static_filter_class(input, predicate, logical_types),
		StaticFilterClass::AlwaysReject
	) {
		return StaticFilterClass::AlwaysReject;
	}
	if predicate.single_root().is_some() && !program_has_column_references(predicate) {
		return closed_root_filter_class(predicate);
	}
	StaticFilterClass::Unknown
}

pub(super) fn static_filter_and_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> Option<StaticFilterClass> {
	let root = predicate.single_root()?;
	if !matches!(
		predicate.node(root),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			..
		})
	) {
		return None;
	}
	let Ok(node_count) = count_bound_subgraph_nodes(predicate, root) else {
		return Some(StaticFilterClass::Unknown);
	};
	if node_count > BOOLEAN_FACTOR_MAX_NODES {
		return Some(StaticFilterClass::Unknown);
	}

	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);
	if conjuncts.len() > STATIC_ELISION_MAX_AND_CONJUNCTS {
		return Some(StaticFilterClass::Unknown);
	}

	let mut found_reject = false;
	for conjunct in conjuncts {
		let Ok(isolated) = isolate_bound_subgraph(predicate, conjunct) else {
			return Some(StaticFilterClass::Unknown);
		};
		match static_filter_atom_class(input, &isolated, logical_types) {
			StaticFilterClass::AlwaysReject => found_reject = true,
			StaticFilterClass::AlwaysPass | StaticFilterClass::Unknown => {
				if !static_elision_error_free_conjunct(&isolated) {
					return Some(StaticFilterClass::Unknown);
				}
			}
		}
	}

	if found_reject {
		Some(StaticFilterClass::AlwaysReject)
	} else {
		Some(StaticFilterClass::Unknown)
	}
}

pub(super) fn static_elision_error_free_conjunct(predicate: &BoundExprProgram) -> bool {
	let Some(root) = predicate.single_root() else {
		return false;
	};
	if !program_has_column_references(predicate) {
		return !matches!(
			closed_root_filter_class(predicate),
			StaticFilterClass::Unknown
		);
	}
	static_elision_error_free_node(predicate, root)
}

pub(super) fn static_elision_error_free_node(
	predicate: &BoundExprProgram,
	id: BoundExprId,
) -> bool {
	match predicate.node(id) {
		Some(BoundExprNode::Column(_)) | Some(BoundExprNode::Literal(_)) => true,
		Some(BoundExprNode::Binary {
			op:
				BoundBinaryOp::Eq
				| BoundBinaryOp::Neq
				| BoundBinaryOp::Lt
				| BoundBinaryOp::Lte
				| BoundBinaryOp::Gt
				| BoundBinaryOp::Gte,
			left,
			right,
		}) => {
			static_elision_error_free_operand(predicate, *left)
				&& static_elision_error_free_operand(predicate, *right)
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			static_elision_error_free_node(predicate, *left)
				&& static_elision_error_free_node(predicate, *right)
		}
		Some(BoundExprNode::Unary {
			op: BoundUnaryOp::IsNull | BoundUnaryOp::IsNotNull,
			input,
		}) => static_elision_error_free_operand(predicate, *input),
		_ => false,
	}
}

pub(super) fn static_elision_error_free_operand(
	predicate: &BoundExprProgram,
	id: BoundExprId,
) -> bool {
	matches!(
		predicate.node(id),
		Some(BoundExprNode::Column(_)) | Some(BoundExprNode::Literal(_))
	)
}

pub(super) fn structural_non_nullable_is_null(predicate: &BoundExprProgram) -> bool {
	let Some(root) = predicate.single_root() else {
		return false;
	};
	let Some(BoundExprNode::Unary {
		op: BoundUnaryOp::IsNull,
		input,
	}) = predicate.node(root)
	else {
		return false;
	};
	let Some(BoundExprNode::Column(column)) = predicate.node(*input) else {
		return false;
	};
	!column.nullable
}

pub(super) fn program_has_column_references(predicate: &BoundExprProgram) -> bool {
	predicate
		.nodes
		.iter()
		.any(|node| matches!(node, BoundExprNode::Column(_)))
}

pub(super) fn closed_root_filter_class(predicate: &BoundExprProgram) -> StaticFilterClass {
	let schema = Arc::new(Schema::empty());
	let options = RecordBatchOptions::new().with_row_count(Some(1));
	let Ok(batch) = RecordBatch::try_new_with_options(schema, Vec::new(), &options) else {
		return StaticFilterClass::Unknown;
	};
	let Ok(outputs) = Executor::evaluate_bound_program_without_storage(predicate, &batch) else {
		return StaticFilterClass::Unknown;
	};
	let [output] = outputs.as_slice() else {
		return StaticFilterClass::Unknown;
	};
	let Some(mask) = output.as_any().downcast_ref::<BooleanArray>() else {
		return StaticFilterClass::Unknown;
	};
	if mask.len() != 1 {
		return StaticFilterClass::Unknown;
	}
	if mask.is_null(0) || !mask.value(0) {
		StaticFilterClass::AlwaysReject
	} else {
		StaticFilterClass::AlwaysPass
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RefinementCmpOp {
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,
}

impl RefinementCmpOp {
	pub(super) fn flip(self) -> Self {
		match self {
			Self::Eq => Self::Eq,
			Self::Ne => Self::Ne,
			Self::Lt => Self::Gt,
			Self::Le => Self::Ge,
			Self::Gt => Self::Lt,
			Self::Ge => Self::Le,
		}
	}
}

#[derive(Debug, Clone)]
pub(super) struct StoredRefinementAtom {
	op: RefinementCmpOp,
	literal: CarrierLiteral,
	refinement: LogicalTypeRefinementPredicate,
}

pub(super) const STORED_REFINEMENT_RECEIPT_MAX_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy)]
pub(super) struct StoredRefinementReceipt<'a> {
	field: &'a Field,
}

pub(super) fn refinement_static_filter_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> StaticFilterClass {
	let Some(atom) = stored_scan_refinement_atom(input, predicate, logical_types) else {
		return StaticFilterClass::Unknown;
	};
	let Some(refinement_domain) = domain_from_refinement(&atom.refinement) else {
		return StaticFilterClass::Unknown;
	};
	let Some(filter_domain) = domain_from_comparison(atom.op, atom.literal) else {
		return StaticFilterClass::Unknown;
	};
	let Some(intersection) = refinement_domain.intersect(&filter_domain) else {
		return StaticFilterClass::Unknown;
	};
	if intersection.is_empty() {
		StaticFilterClass::AlwaysReject
	} else {
		StaticFilterClass::Unknown
	}
}

pub(super) fn stored_scan_refinement_atom(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> Option<StoredRefinementAtom> {
	let root = predicate.single_root()?;
	let BoundExprNode::Binary { op, left, right } = predicate.node(root)? else {
		return None;
	};
	let op = filter_refinement_cmp_op(op)?;
	let (column_node, literal_node, op) = match (predicate.node(*left)?, predicate.node(*right)?) {
		(BoundExprNode::Column(_), BoundExprNode::Literal(_)) => (*left, *right, op),
		(BoundExprNode::Literal(_), BoundExprNode::Column(_)) => (*right, *left, op.flip()),
		_ => return None,
	};
	let BoundExprNode::Column(column) = predicate.node(column_node)? else {
		return None;
	};
	let receipt = stored_refinement_receipt(input, column.ordinal, 0)?;
	let field = receipt.field;
	let logical_type = predicate.node_logical_type(column_node)?;
	if logical_type.is_naked() {
		return None;
	}
	let spec = logical_types.spec(logical_type)?;
	if field.data_type() != &spec.physical || column.data_type != spec.physical {
		return None;
	}
	let refinement = spec.refinement?;
	let literal = predicate_carrier_literal(predicate, literal_node, field.data_type())?;
	Some(StoredRefinementAtom {
		op,
		literal,
		refinement,
	})
}

pub(super) fn transparent_project_child_ordinal(
	exprs: &BoundExprProgram,
	ordinal: ColumnOrdinal,
) -> Option<ColumnOrdinal> {
	let root = *exprs.roots.get(ordinal.0 as usize)?;
	match exprs.node(root)? {
		BoundExprNode::Column(column) => Some(column.ordinal),
		_ => None,
	}
}

pub(super) fn stored_refinement_receipt<'a>(
	input: &'a ExecutionPlan,
	ordinal: ColumnOrdinal,
	depth: usize,
) -> Option<StoredRefinementReceipt<'a>> {
	if depth > STORED_REFINEMENT_RECEIPT_MAX_DEPTH {
		return None;
	}
	match input {
		ExecutionPlan::Scan {
			projection: None,
			schema,
			..
		} => {
			let field = schema.fields().get(ordinal.0 as usize)?.as_ref();
			Some(StoredRefinementReceipt { field })
		}
		ExecutionPlan::Project { input, exprs, .. } => {
			let child = transparent_project_child_ordinal(exprs, ordinal)?;
			let input_schema = input.schema();
			let child_field = input_schema.fields().get(child.0 as usize)?;
			let child_logical_type = decode_column_logical_type(child_field).ok()?;
			if exprs.root_logical_type(ordinal.0 as usize)? != child_logical_type {
				return None;
			}
			stored_refinement_receipt(input, child, depth + 1)
		}
		_ => None,
	}
}

pub(super) fn filter_refinement_cmp_op(op: &BoundBinaryOp) -> Option<RefinementCmpOp> {
	match op {
		BoundBinaryOp::Eq => Some(RefinementCmpOp::Eq),
		BoundBinaryOp::Neq => Some(RefinementCmpOp::Ne),
		BoundBinaryOp::Lt => Some(RefinementCmpOp::Lt),
		BoundBinaryOp::Lte => Some(RefinementCmpOp::Le),
		BoundBinaryOp::Gt => Some(RefinementCmpOp::Gt),
		BoundBinaryOp::Gte => Some(RefinementCmpOp::Ge),
		_ => None,
	}
}

pub(super) fn predicate_carrier_literal(
	predicate: &BoundExprProgram,
	literal_node: BoundExprId,
	carrier: &DataType,
) -> Option<CarrierLiteral> {
	let BoundExprNode::Literal(literal) = predicate.node(literal_node)? else {
		return None;
	};
	let value = predicate.literals.get(*literal as usize)?;
	carrier_literal_from_scalar(value, carrier)
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum CarrierLiteral {
	Int32(i32),
	Int64(i64),
	Date32(i32),
	Timestamp(i64),
	Float32(FiniteF32),
	Float64(FiniteF64),
	Utf8(String),
	Boolean(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(super) struct FiniteF32(f32);

impl FiniteF32 {
	pub(super) fn new(value: f32) -> Option<Self> {
		value.is_finite().then_some(Self(value))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(super) struct FiniteF64(f64);

impl FiniteF64 {
	pub(super) fn new(value: f64) -> Option<Self> {
		value.is_finite().then_some(Self(value))
	}
}

pub(super) fn carrier_literal_from_scalar(
	value: &ScalarValue,
	carrier: &DataType,
) -> Option<CarrierLiteral> {
	match (carrier, value) {
		(DataType::Int32, ScalarValue::Int32(Some(value))) => Some(CarrierLiteral::Int32(*value)),
		(DataType::Int64, ScalarValue::Int64(Some(value))) => Some(CarrierLiteral::Int64(*value)),
		(DataType::Date32, ScalarValue::Date32(Some(value))) => {
			Some(CarrierLiteral::Date32(*value))
		}
		(
			DataType::Timestamp(target_unit, Some(timezone)),
			ScalarValue::Timestamp {
				value: Some(value),
				unit,
			},
		) if timezone.as_ref() == "UTC" && target_unit == unit => Some(CarrierLiteral::Timestamp(*value)),
		(DataType::Float32, ScalarValue::Float32(Some(value))) => {
			FiniteF32::new(*value).map(CarrierLiteral::Float32)
		}
		(DataType::Float64, ScalarValue::Float64(Some(value))) => {
			FiniteF64::new(*value).map(CarrierLiteral::Float64)
		}
		(DataType::Utf8, ScalarValue::Utf8(Some(value))) => {
			Some(CarrierLiteral::Utf8(value.clone()))
		}
		(DataType::Boolean, ScalarValue::Boolean(Some(value))) => {
			Some(CarrierLiteral::Boolean(*value))
		}
		_ => None,
	}
}

pub(super) fn carrier_literal_from_refinement_value(
	value: &LogicalTypeRefinementValue,
) -> Option<CarrierLiteral> {
	match value {
		LogicalTypeRefinementValue::Value => None,
		LogicalTypeRefinementValue::Int64(value) => Some(CarrierLiteral::Int64(*value)),
		LogicalTypeRefinementValue::Int32(value) => Some(CarrierLiteral::Int32(*value)),
		LogicalTypeRefinementValue::Float64(bits) => {
			FiniteF64::new(f64::from_bits(*bits)).map(CarrierLiteral::Float64)
		}
		LogicalTypeRefinementValue::Float32(bits) => {
			FiniteF32::new(f32::from_bits(*bits)).map(CarrierLiteral::Float32)
		}
		LogicalTypeRefinementValue::Utf8(value) => Some(CarrierLiteral::Utf8(value.clone())),
		LogicalTypeRefinementValue::Boolean(value) => Some(CarrierLiteral::Boolean(*value)),
	}
}

#[derive(Debug, Clone)]
pub(super) struct DomainBound<T> {
	value: T,
	inclusive: bool,
}

#[derive(Debug, Clone)]
pub(super) struct OrderedDomain<T> {
	lower: Option<DomainBound<T>>,
	upper: Option<DomainBound<T>>,
	excluded: Vec<T>,
}

impl<T> OrderedDomain<T>
where
	T: Clone + PartialEq + PartialOrd,
{
	pub(super) fn point(value: T) -> Self {
		Self {
			lower: Some(DomainBound {
				value: value.clone(),
				inclusive: true,
			}),
			upper: Some(DomainBound {
				value,
				inclusive: true,
			}),
			excluded: Vec::new(),
		}
	}

	pub(super) fn lower(value: T, inclusive: bool) -> Self {
		Self {
			lower: Some(DomainBound { value, inclusive }),
			upper: None,
			excluded: Vec::new(),
		}
	}

	pub(super) fn upper(value: T, inclusive: bool) -> Self {
		Self {
			lower: None,
			upper: Some(DomainBound { value, inclusive }),
			excluded: Vec::new(),
		}
	}

	pub(super) fn excluded(value: T) -> Self {
		Self {
			lower: None,
			upper: None,
			excluded: vec![value],
		}
	}

	pub(super) fn intersect(&self, other: &Self) -> Self {
		let lower = intersect_lower_bound(&self.lower, &other.lower);
		let upper = intersect_upper_bound(&self.upper, &other.upper);
		let mut excluded = self.excluded.clone();
		for value in &other.excluded {
			if !excluded.iter().any(|existing| existing == value) {
				excluded.push(value.clone());
			}
		}
		Self {
			lower,
			upper,
			excluded,
		}
	}

	pub(super) fn is_empty(&self) -> bool {
		let (Some(lower), Some(upper)) = (&self.lower, &self.upper) else {
			return false;
		};
		match lower.value.partial_cmp(&upper.value) {
			Some(Ordering::Greater) | None => true,
			Some(Ordering::Less) => false,
			Some(Ordering::Equal) => {
				!lower.inclusive
					|| !upper.inclusive
					|| self.excluded.iter().any(|value| value == &lower.value)
			}
		}
	}
}

pub(super) fn intersect_lower_bound<T>(
	left: &Option<DomainBound<T>>,
	right: &Option<DomainBound<T>>,
) -> Option<DomainBound<T>>
where
	T: Clone + PartialEq + PartialOrd,
{
	match (left, right) {
		(None, None) => None,
		(Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
		(Some(left), Some(right)) => match left.value.partial_cmp(&right.value) {
			Some(Ordering::Greater) => Some(left.clone()),
			Some(Ordering::Less) => Some(right.clone()),
			Some(Ordering::Equal) => Some(DomainBound {
				value: left.value.clone(),
				inclusive: left.inclusive && right.inclusive,
			}),
			None => Some(DomainBound {
				value: left.value.clone(),
				inclusive: false,
			}),
		},
	}
}

pub(super) fn intersect_upper_bound<T>(
	left: &Option<DomainBound<T>>,
	right: &Option<DomainBound<T>>,
) -> Option<DomainBound<T>>
where
	T: Clone + PartialEq + PartialOrd,
{
	match (left, right) {
		(None, None) => None,
		(Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
		(Some(left), Some(right)) => match left.value.partial_cmp(&right.value) {
			Some(Ordering::Greater) => Some(right.clone()),
			Some(Ordering::Less) => Some(left.clone()),
			Some(Ordering::Equal) => Some(DomainBound {
				value: left.value.clone(),
				inclusive: left.inclusive && right.inclusive,
			}),
			None => Some(DomainBound {
				value: left.value.clone(),
				inclusive: false,
			}),
		},
	}
}

#[derive(Debug, Clone)]
pub(super) struct Utf8Domain {
	only: Option<String>,
	excluded: Vec<String>,
}

impl Utf8Domain {
	pub(super) fn only(value: String) -> Self {
		Self {
			only: Some(value),
			excluded: Vec::new(),
		}
	}

	pub(super) fn excluding(value: String) -> Self {
		Self {
			only: None,
			excluded: vec![value],
		}
	}

	pub(super) fn intersect(&self, other: &Self) -> Option<Self> {
		let only = match (&self.only, &other.only) {
			(Some(left), Some(right)) if left == right => Some(left.clone()),
			(Some(_), Some(_)) => return None,
			(Some(value), None) | (None, Some(value)) => Some(value.clone()),
			(None, None) => None,
		};
		let mut excluded = self.excluded.clone();
		for value in &other.excluded {
			if !excluded.iter().any(|existing| existing == value) {
				excluded.push(value.clone());
			}
		}
		if let Some(value) = &only
			&& excluded.iter().any(|excluded| excluded == value)
		{
			return None;
		}
		Some(Self { only, excluded })
	}
}

#[derive(Debug, Clone)]
pub(super) struct BooleanDomain {
	allow_false: bool,
	allow_true: bool,
}

impl BooleanDomain {
	pub(super) fn only(value: bool) -> Self {
		Self {
			allow_false: !value,
			allow_true: value,
		}
	}

	pub(super) fn excluding(value: bool) -> Self {
		Self {
			allow_false: value,
			allow_true: !value,
		}
	}

	pub(super) fn intersect(&self, other: &Self) -> Self {
		Self {
			allow_false: self.allow_false && other.allow_false,
			allow_true: self.allow_true && other.allow_true,
		}
	}

	pub(super) fn is_empty(&self) -> bool {
		!self.allow_false && !self.allow_true
	}
}

#[derive(Debug, Clone)]
pub(super) enum CarrierDomain {
	Empty,
	Int32(OrderedDomain<i32>),
	Int64(OrderedDomain<i64>),
	Date32(OrderedDomain<i32>),
	Timestamp(OrderedDomain<i64>),
	Float32(OrderedDomain<FiniteF32>),
	Float64(OrderedDomain<FiniteF64>),
	Utf8(Utf8Domain),
	Boolean(BooleanDomain),
}

impl CarrierDomain {
	pub(super) fn intersect(&self, other: &Self) -> Option<Self> {
		match (self, other) {
			(Self::Empty, _) | (_, Self::Empty) => Some(Self::Empty),
			(Self::Int32(left), Self::Int32(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Int32(domain))
				}
			}
			(Self::Int64(left), Self::Int64(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Int64(domain))
				}
			}
			(Self::Date32(left), Self::Date32(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Date32(domain))
				}
			}
			(Self::Timestamp(left), Self::Timestamp(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Timestamp(domain))
				}
			}
			(Self::Float32(left), Self::Float32(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Float32(domain))
				}
			}
			(Self::Float64(left), Self::Float64(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Float64(domain))
				}
			}
			(Self::Utf8(left), Self::Utf8(right)) => {
				left.intersect(right).map(Self::Utf8).or(Some(Self::Empty))
			}
			(Self::Boolean(left), Self::Boolean(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Boolean(domain))
				}
			}
			_ => None,
		}
	}

	pub(super) fn is_empty(&self) -> bool {
		matches!(self, Self::Empty)
	}
}

pub(super) fn domain_from_refinement(
	predicate: &LogicalTypeRefinementPredicate,
) -> Option<CarrierDomain> {
	match predicate {
		LogicalTypeRefinementPredicate::Comparison { left, op, right } => {
			let (op, literal) = refinement_comparison_literal(left, *op, right)?;
			let literal = carrier_literal_from_refinement_value(literal)?;
			domain_from_comparison(op, literal)
		}
		LogicalTypeRefinementPredicate::And(predicates) => {
			let mut domain: Option<CarrierDomain> = None;
			for predicate in predicates {
				let child = domain_from_refinement(predicate)?;
				domain = Some(match domain {
					None => child,
					Some(existing) => existing.intersect(&child)?,
				});
				if matches!(domain, Some(CarrierDomain::Empty)) {
					break;
				}
			}
			domain
		}
	}
}

pub(super) fn refinement_comparison_literal<'a>(
	left: &'a LogicalTypeRefinementValue,
	op: LogicalTypeRefinementComparisonOp,
	right: &'a LogicalTypeRefinementValue,
) -> Option<(RefinementCmpOp, &'a LogicalTypeRefinementValue)> {
	let op = refinement_cmp_op(op);
	match (left, right) {
		(LogicalTypeRefinementValue::Value, literal)
			if !matches!(literal, LogicalTypeRefinementValue::Value) =>
		{
			Some((op, literal))
		}
		(literal, LogicalTypeRefinementValue::Value)
			if !matches!(literal, LogicalTypeRefinementValue::Value) =>
		{
			Some((op.flip(), literal))
		}
		_ => None,
	}
}

pub(super) fn refinement_cmp_op(op: LogicalTypeRefinementComparisonOp) -> RefinementCmpOp {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => RefinementCmpOp::Eq,
		LogicalTypeRefinementComparisonOp::Ne => RefinementCmpOp::Ne,
		LogicalTypeRefinementComparisonOp::Lt => RefinementCmpOp::Lt,
		LogicalTypeRefinementComparisonOp::Le => RefinementCmpOp::Le,
		LogicalTypeRefinementComparisonOp::Gt => RefinementCmpOp::Gt,
		LogicalTypeRefinementComparisonOp::Ge => RefinementCmpOp::Ge,
	}
}

pub(super) fn domain_from_comparison(
	op: RefinementCmpOp,
	literal: CarrierLiteral,
) -> Option<CarrierDomain> {
	match literal {
		CarrierLiteral::Int32(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Int32)
		}
		CarrierLiteral::Int64(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Int64)
		}
		CarrierLiteral::Date32(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Date32)
		}
		CarrierLiteral::Timestamp(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Timestamp)
		}
		CarrierLiteral::Float32(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Float32)
		}
		CarrierLiteral::Float64(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Float64)
		}
		CarrierLiteral::Utf8(value) => match op {
			RefinementCmpOp::Eq => Some(CarrierDomain::Utf8(Utf8Domain::only(value))),
			RefinementCmpOp::Ne => Some(CarrierDomain::Utf8(Utf8Domain::excluding(value))),
			_ => None,
		},
		CarrierLiteral::Boolean(value) => match op {
			RefinementCmpOp::Eq => Some(CarrierDomain::Boolean(BooleanDomain::only(value))),
			RefinementCmpOp::Ne => Some(CarrierDomain::Boolean(BooleanDomain::excluding(value))),
			_ => None,
		},
	}
}

pub(super) fn ordered_domain_from_comparison<T>(
	op: RefinementCmpOp,
	value: T,
) -> Option<OrderedDomain<T>>
where
	T: Clone + PartialEq + PartialOrd,
{
	match op {
		RefinementCmpOp::Eq => Some(OrderedDomain::point(value)),
		RefinementCmpOp::Ne => Some(OrderedDomain::excluded(value)),
		RefinementCmpOp::Lt => Some(OrderedDomain::upper(value, false)),
		RefinementCmpOp::Le => Some(OrderedDomain::upper(value, true)),
		RefinementCmpOp::Gt => Some(OrderedDomain::lower(value, false)),
		RefinementCmpOp::Ge => Some(OrderedDomain::lower(value, true)),
	}
}

pub(super) fn predicate_pushdown_plan(
	predicate: BoundExprProgram,
	input: &ExecutionPlan,
	logical_types: &LogicalTypeRegistry,
) -> PredicatePushdownPlan {
	let fallback_profile = scan_profile_hint(&predicate, input);
	let Some((pushed_predicate, residual_predicate)) = partition_bound_predicate(&predicate) else {
		return PredicatePushdownPlan {
			pushed_predicate: None,
			residual_predicate: Some(predicate),
			pbc_program: None,
			profile_hint: fallback_profile,
			converted_pushdown: false,
		};
	};

	let derived_predicate = converted_linear_pushdown_predicate(&predicate, logical_types);
	let converted_pushdown = derived_predicate.is_some();
	let pushed_predicate = combine_pushdown_predicates(pushed_predicate, derived_predicate);

	let Some(pushed_predicate) = pushed_predicate else {
		return PredicatePushdownPlan {
			pushed_predicate: None,
			residual_predicate: residual_predicate.or(Some(predicate)),
			pbc_program: None,
			profile_hint: fallback_profile,
			converted_pushdown: false,
		};
	};

	let Some(pbc_program) = compile_pbc(&pushed_predicate) else {
		return PredicatePushdownPlan {
			pushed_predicate: None,
			residual_predicate: Some(predicate),
			pbc_program: None,
			profile_hint: fallback_profile,
			converted_pushdown: false,
		};
	};
	let profile_hint = scan_profile_hint(&pushed_predicate, input);

	PredicatePushdownPlan {
		pushed_predicate: Some(pushed_predicate),
		residual_predicate,
		pbc_program: Some(pbc_program),
		profile_hint,
		converted_pushdown,
	}
}

pub(super) fn combine_pushdown_predicates(
	pushed: Option<BoundExprProgram>,
	derived: Option<BoundExprProgram>,
) -> Option<BoundExprProgram> {
	match (pushed, derived) {
		(Some(pushed), Some(derived)) => combine_bound_programs_as_and(vec![pushed, derived])
			.ok()
			.flatten(),
		(Some(pushed), None) => Some(pushed),
		(None, Some(derived)) => Some(derived),
		(None, None) => None,
	}
}

#[derive(Debug, Clone)]
pub(super) struct LinearConvertedComparison {
	pub(super) column: ResolvedColumn,
	pub(super) source_logical: LogicalTypeId,
	pub(super) factor: f64,
	pub(super) op: BoundBinaryOp,
	pub(super) constant: f64,
}

pub(super) fn converted_linear_pushdown_predicate(
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> Option<BoundExprProgram> {
	let root = predicate.single_root()?;
	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);
	let derived = conjuncts
		.into_iter()
		.filter_map(|conjunct| {
			match_linear_converted_comparison(predicate, conjunct, logical_types)
				.and_then(|comparison| derived_linear_converted_predicate(&comparison))
		})
		.collect::<Vec<_>>();

	combine_bound_programs_as_and(derived).ok().flatten()
}

pub(super) fn match_linear_converted_comparison(
	program: &BoundExprProgram,
	root: BoundExprId,
	logical_types: &LogicalTypeRegistry,
) -> Option<LinearConvertedComparison> {
	let BoundExprNode::Binary { op, left, right } = program.node(root)? else {
		return None;
	};
	if !matches!(
		op,
		BoundBinaryOp::Lt | BoundBinaryOp::Lte | BoundBinaryOp::Gt | BoundBinaryOp::Gte
	) {
		return None;
	}
	match_linear_converted_comparison_sides(program, op.clone(), *left, *right, logical_types)
		.or_else(|| {
			match_linear_converted_comparison_sides(
				program,
				flip_range_op(op.clone()),
				*right,
				*left,
				logical_types,
			)
		})
}

pub(super) fn match_linear_converted_comparison_sides(
	program: &BoundExprProgram,
	op: BoundBinaryOp,
	converted_side: BoundExprId,
	literal_side: BoundExprId,
	logical_types: &LogicalTypeRegistry,
) -> Option<LinearConvertedComparison> {
	let constant = finite_float64_literal(program, literal_side)?;
	let (column_id, column, factor) = match_linear_converted_mul(program, converted_side)?;
	let target_logical = program.node_logical_type(converted_side)?;
	let source_logical = program.node_logical_type(column_id)?;
	let source_spec = LogicalTypeResolver::with_registry(logical_types).spec(source_logical)?;
	let target_spec = LogicalTypeResolver::with_registry(logical_types).spec(target_logical)?;
	let source_unit = source_spec.unit.as_ref()?;
	let target_unit = target_spec.unit.as_ref()?;
	if source_spec.physical != DataType::Float64 || target_spec.physical != DataType::Float64 {
		return None;
	}
	if source_unit.quantity_kind != UnitQuantityKind::Delta
		|| target_unit.quantity_kind != UnitQuantityKind::Delta
	{
		return None;
	}
	if source_unit.dimension != target_unit.dimension {
		return None;
	}
	let transform = logical_unit_convert_transform(source_unit, target_unit);
	if !transform.factor.is_finite() || transform.factor <= 0.0 || transform.shift != 0.0 {
		return None;
	}
	if factor.to_bits() != transform.factor.to_bits() {
		return None;
	}
	Some(LinearConvertedComparison {
		column,
		source_logical,
		factor,
		op,
		constant,
	})
}

pub(super) fn match_linear_converted_mul(
	program: &BoundExprProgram,
	id: BoundExprId,
) -> Option<(BoundExprId, ResolvedColumn, f64)> {
	let BoundExprNode::Binary {
		op: BoundBinaryOp::Mul,
		left,
		right,
	} = program.node(id)?
	else {
		return None;
	};
	match_linear_converted_mul_sides(program, *left, *right)
		.or_else(|| match_linear_converted_mul_sides(program, *right, *left))
}

pub(super) fn match_linear_converted_mul_sides(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
) -> Option<(BoundExprId, ResolvedColumn, f64)> {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return None;
	};
	if !column.pbc_eligible || column.data_type != DataType::Float64 {
		return None;
	}
	let factor = finite_float64_literal(program, literal_side)?;
	if factor <= 0.0 {
		return None;
	}
	Some((column_side, column.clone(), factor))
}

pub(super) fn finite_float64_literal(program: &BoundExprProgram, id: BoundExprId) -> Option<f64> {
	let Some(BoundExprNode::Literal(literal)) = program.node(id) else {
		return None;
	};
	let Some(ScalarValue::Float64(Some(value))) = program.literals.get(*literal as usize) else {
		return None;
	};
	value.is_finite().then_some(*value)
}

pub(super) fn derived_linear_converted_predicate(
	comparison: &LinearConvertedComparison,
) -> Option<BoundExprProgram> {
	let threshold =
		widened_converted_linear_threshold(comparison.constant, comparison.factor, &comparison.op)?;

	let mut builder = BoundExprProgramBuilder::new();
	let column = builder.append_column(comparison.column.clone());
	if !builder.set_node_logical_type(column, comparison.source_logical) {
		return None;
	}
	let literal = builder.append_literal(ScalarValue::Float64(Some(threshold)));
	let root = builder.append_binary(comparison.op.clone(), column, literal, DataType::Boolean);
	builder.push_root(root);
	let program = builder.build();
	program.validate().ok()?;
	Some(program)
}

pub(super) fn widened_converted_linear_threshold(
	constant: f64,
	factor: f64,
	op: &BoundBinaryOp,
) -> Option<f64> {
	if !constant.is_finite() || !factor.is_finite() || factor <= 0.0 {
		return None;
	}
	let threshold = constant / factor;
	if !threshold.is_finite() {
		return None;
	}
	let widened = match op {
		BoundBinaryOp::Gt | BoundBinaryOp::Gte => next_float_down(threshold),
		BoundBinaryOp::Lt | BoundBinaryOp::Lte => next_float_up(threshold),
		_ => return None,
	};
	widened.is_finite().then_some(widened)
}

pub(super) fn next_float_up(value: f64) -> f64 {
	if value.is_nan() || value == f64::INFINITY {
		return value;
	}
	if value == f64::NEG_INFINITY {
		return f64::MIN;
	}
	if value == 0.0 {
		return f64::from_bits(1);
	}
	let bits = value.to_bits();
	if value > 0.0 {
		f64::from_bits(bits + 1)
	} else {
		f64::from_bits(bits - 1)
	}
}

pub(super) fn next_float_down(value: f64) -> f64 {
	if value.is_nan() || value == f64::NEG_INFINITY {
		return value;
	}
	if value == f64::INFINITY {
		return f64::MAX;
	}
	if value == 0.0 {
		return -f64::from_bits(1);
	}
	let bits = value.to_bits();
	if value > 0.0 {
		f64::from_bits(bits - 1)
	} else {
		f64::from_bits(bits + 1)
	}
}

pub(super) fn partition_bound_predicate(
	predicate: &BoundExprProgram,
) -> Option<(Option<BoundExprProgram>, Option<BoundExprProgram>)> {
	let root = predicate.single_root()?;
	match split_boolean_tree(predicate, root) {
		Ok(split) => Some((split.pushed, split.residual)),
		Err(BooleanSplitError::OverBudget) => legacy_top_level_and_partition(predicate),
		Err(BooleanSplitError::Malformed) => None,
	}
}

#[derive(Debug, Clone)]
pub(super) struct BooleanPredicateSplit {
	pub(super) pushed: Option<BoundExprProgram>,
	pub(super) residual: Option<BoundExprProgram>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BooleanSplitError {
	Malformed,
	OverBudget,
}

pub(super) fn split_boolean_tree(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<BooleanPredicateSplit, BooleanSplitError> {
	let node_count = count_bound_subgraph_nodes(source, root)?;
	if node_count > BOOLEAN_FACTOR_MAX_NODES {
		return Err(BooleanSplitError::OverBudget);
	}

	let whole = isolate_bound_subgraph(source, root)?;
	if compile_pbc(&whole).is_some() {
		return Ok(BooleanPredicateSplit {
			pushed: Some(whole),
			residual: None,
		});
	}

	match source.node(root).ok_or(BooleanSplitError::Malformed)? {
		BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		} => {
			let left = split_boolean_tree(source, *left)?;
			let right = split_boolean_tree(source, *right)?;
			Ok(BooleanPredicateSplit {
				pushed: combine_bound_programs_as_and(
					[left.pushed, right.pushed]
						.into_iter()
						.flatten()
						.collect::<Vec<_>>(),
				)?,
				residual: combine_bound_programs_as_and(
					[left.residual, right.residual]
						.into_iter()
						.flatten()
						.collect::<Vec<_>>(),
				)?,
			})
		}
		BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			..
		} => {
			if let Some(split) = try_extract_common_or_factors(source, root)? {
				return Ok(split);
			}
			Ok(BooleanPredicateSplit {
				pushed: None,
				residual: Some(isolate_bound_subgraph(source, root)?),
			})
		}
		BoundExprNode::Unary {
			op: BoundUnaryOp::Not,
			input,
		} => {
			if let Some(split) = try_extract_not_or_branch_negations(source, root, *input)? {
				return Ok(split);
			}
			Ok(BooleanPredicateSplit {
				pushed: None,
				residual: Some(isolate_bound_subgraph(source, root)?),
			})
		}
		_ => Ok(BooleanPredicateSplit {
			pushed: None,
			residual: Some(isolate_bound_subgraph(source, root)?),
		}),
	}
}

pub(super) fn legacy_top_level_and_partition(
	predicate: &BoundExprProgram,
) -> Option<(Option<BoundExprProgram>, Option<BoundExprProgram>)> {
	let root = predicate.single_root()?;
	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);

	let mut pushed = Vec::new();
	let mut residual = Vec::new();
	for conjunct in conjuncts {
		let isolated = combine_bound_conjuncts(predicate, &[conjunct])?;
		if compile_pbc(&isolated).is_some() {
			pushed.push(conjunct);
		} else {
			residual.push(conjunct);
		}
	}

	Some((
		combine_bound_conjuncts(predicate, &pushed),
		combine_bound_conjuncts(predicate, &residual),
	))
}

pub(super) fn try_extract_common_or_factors(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	let mut branches = Vec::new();
	collect_bound_or_branches(source, root, &mut branches)?;
	if branches.len() < 2 {
		return Ok(None);
	}

	let mut branch_terms = Vec::with_capacity(branches.len());
	for branch in branches {
		let mut terms = Vec::new();
		collect_top_level_bound_and(source, branch, &mut terms);
		if terms.len() > BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH {
			return Err(BooleanSplitError::OverBudget);
		}
		branch_terms.push(terms);
	}

	let Some(first_branch) = branch_terms.first() else {
		return Ok(None);
	};
	let mut necessary_candidates = Vec::new();
	for term in first_branch {
		if necessary_candidates
			.iter()
			.any(|candidate| bound_subgraphs_structurally_equal(source, *candidate, *term))
		{
			continue;
		}
		let present_in_every_branch = branch_terms.iter().skip(1).all(|branch| {
			branch
				.iter()
				.any(|candidate| bound_subgraphs_structurally_equal(source, *term, *candidate))
		});
		if !present_in_every_branch {
			continue;
		}

		let isolated = isolate_bound_subgraph(source, *term)?;
		if compile_pbc(&isolated).is_some() {
			necessary_candidates.push(*term);
			if necessary_candidates.len() > BOOLEAN_FACTOR_MAX_CANDIDATES {
				return Err(BooleanSplitError::OverBudget);
			}
		}
	}

	if necessary_candidates.is_empty() {
		return Ok(None);
	}

	let pushed_programs = necessary_candidates
		.iter()
		.copied()
		.map(|candidate| isolate_bound_subgraph(source, candidate))
		.collect::<Result<Vec<_>, _>>()?;
	let pushed =
		combine_bound_programs_as_and(pushed_programs)?.ok_or(BooleanSplitError::Malformed)?;

	let mut branch_residuals = Vec::with_capacity(branch_terms.len());
	for terms in &branch_terms {
		let mut used = vec![false; terms.len()];
		for common in &necessary_candidates {
			let Some(index) = terms.iter().enumerate().find_map(|(index, term)| {
				(!used[index] && bound_subgraphs_structurally_equal(source, *common, *term))
					.then_some(index)
			}) else {
				return original_or_residual_factoring(source, root, pushed);
			};
			used[index] = true;
		}

		let remaining = terms
			.iter()
			.enumerate()
			.filter_map(|(index, term)| (!used[index]).then_some(*term))
			.collect::<Vec<_>>();
		if remaining.is_empty() {
			return Ok(Some(BooleanPredicateSplit {
				pushed: Some(pushed),
				residual: None,
			}));
		}

		let Some(branch) = combine_bound_conjuncts(source, &remaining) else {
			return original_or_residual_factoring(source, root, pushed);
		};
		branch_residuals.push(branch);
	}

	let Ok(residual) = combine_bound_programs_as_or(branch_residuals) else {
		return original_or_residual_factoring(source, root, pushed);
	};
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual,
	}))
}

pub(super) fn try_extract_not_or_branch_negations(
	source: &BoundExprProgram,
	not_root: BoundExprId,
	or_root: BoundExprId,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	if !matches!(
		source.node(or_root),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			..
		})
	) {
		return Ok(None);
	}

	let mut branches = Vec::new();
	collect_bound_or_branches(source, or_root, &mut branches)?;

	let mut pushed_negations = Vec::new();
	let mut residual_negations = Vec::new();
	for branch in branches {
		let candidate = build_negated_subgraph(source, branch)?;
		if compile_pbc(&candidate).is_some() {
			pushed_negations.push(candidate);
			if pushed_negations.len() > BOOLEAN_FACTOR_MAX_CANDIDATES {
				return Err(BooleanSplitError::OverBudget);
			}
		} else {
			residual_negations.push(candidate);
		}
	}

	if pushed_negations.is_empty() {
		return Ok(None);
	}
	let pushed =
		combine_bound_programs_as_and(pushed_negations)?.ok_or(BooleanSplitError::Malformed)?;
	let Ok(residual) = combine_bound_programs_as_and(residual_negations) else {
		return original_not_or_residual_factoring(source, not_root, pushed);
	};
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual,
	}))
}

pub(super) fn original_or_residual_factoring(
	source: &BoundExprProgram,
	root: BoundExprId,
	pushed: BoundExprProgram,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual: Some(isolate_bound_subgraph(source, root)?),
	}))
}

pub(super) fn original_not_or_residual_factoring(
	source: &BoundExprProgram,
	root: BoundExprId,
	pushed: BoundExprProgram,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual: Some(isolate_bound_subgraph(source, root)?),
	}))
}

pub(super) fn collect_bound_or_branches(
	program: &BoundExprProgram,
	id: BoundExprId,
	branches: &mut Vec<BoundExprId>,
) -> Result<(), BooleanSplitError> {
	if branches.len() > BOOLEAN_FACTOR_MAX_OR_BRANCHES {
		return Err(BooleanSplitError::OverBudget);
	}
	match program.node(id).ok_or(BooleanSplitError::Malformed)? {
		BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			left,
			right,
		} => {
			collect_bound_or_branches(program, *left, branches)?;
			collect_bound_or_branches(program, *right, branches)?;
		}
		_ => {
			branches.push(id);
			if branches.len() > BOOLEAN_FACTOR_MAX_OR_BRANCHES {
				return Err(BooleanSplitError::OverBudget);
			}
		}
	}
	Ok(())
}

pub(super) fn collect_top_level_bound_and(
	program: &BoundExprProgram,
	id: BoundExprId,
	conjuncts: &mut Vec<BoundExprId>,
) {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			collect_top_level_bound_and(program, *left, conjuncts);
			collect_top_level_bound_and(program, *right, conjuncts);
		}
		_ => conjuncts.push(id),
	}
}

pub(super) fn isolate_bound_subgraph(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<BoundExprProgram, BooleanSplitError> {
	combine_bound_conjuncts(source, &[root]).ok_or(BooleanSplitError::Malformed)
}

pub(super) fn combine_bound_conjuncts(
	source: &BoundExprProgram,
	roots: &[BoundExprId],
) -> Option<BoundExprProgram> {
	let mut roots = roots.iter().copied();
	let first_root = roots.next()?;
	let mut builder = BoundExprProgramBuilder::new();
	let mut remap = HashMap::new();
	let mut combined = clone_bound_subgraph(source, first_root, &mut builder, &mut remap)?;

	for root in roots {
		let next = clone_bound_subgraph(source, root, &mut builder, &mut remap)?;
		combined = builder.append_binary(BoundBinaryOp::And, combined, next, DataType::Boolean);
	}

	builder.push_root(combined);
	let program = builder.build();
	program.validate().ok()?;
	Some(program)
}

pub(super) fn combine_bound_programs_as_and(
	programs: Vec<BoundExprProgram>,
) -> Result<Option<BoundExprProgram>, BooleanSplitError> {
	combine_bound_programs_as_binary(programs, BoundBinaryOp::And)
}

pub(super) fn combine_bound_programs_as_or(
	programs: Vec<BoundExprProgram>,
) -> Result<Option<BoundExprProgram>, BooleanSplitError> {
	combine_bound_programs_as_binary(programs, BoundBinaryOp::Or)
}

pub(super) fn combine_bound_programs_as_binary(
	programs: Vec<BoundExprProgram>,
	op: BoundBinaryOp,
) -> Result<Option<BoundExprProgram>, BooleanSplitError> {
	let mut programs = programs.into_iter();
	let Some(first_program) = programs.next() else {
		return Ok(None);
	};
	let mut builder = BoundExprProgramBuilder::new();
	let mut combined = clone_single_root_into_builder(&first_program, &mut builder)?;

	for program in programs {
		let next = clone_single_root_into_builder(&program, &mut builder)?;
		combined = builder.append_binary(op.clone(), combined, next, DataType::Boolean);
	}

	builder.push_root(combined);
	let program = builder.build();
	program
		.validate()
		.map_err(|_| BooleanSplitError::Malformed)?;
	Ok(Some(program))
}

pub(super) fn clone_single_root_into_builder(
	source: &BoundExprProgram,
	builder: &mut BoundExprProgramBuilder,
) -> Result<BoundExprId, BooleanSplitError> {
	let root = source.single_root().ok_or(BooleanSplitError::Malformed)?;
	let mut remap = HashMap::new();
	clone_bound_subgraph(source, root, builder, &mut remap).ok_or(BooleanSplitError::Malformed)
}

pub(super) fn build_negated_subgraph(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<BoundExprProgram, BooleanSplitError> {
	let mut builder = BoundExprProgramBuilder::new();
	let mut remap = HashMap::new();
	let input = clone_bound_subgraph(source, root, &mut builder, &mut remap)
		.ok_or(BooleanSplitError::Malformed)?;
	let negated = builder.append_unary(BoundUnaryOp::Not, input, DataType::Boolean);
	builder.push_root(negated);
	let program = builder.build();
	program
		.validate()
		.map_err(|_| BooleanSplitError::Malformed)?;
	Ok(program)
}

pub(super) fn clone_bound_subgraph(
	source: &BoundExprProgram,
	id: BoundExprId,
	builder: &mut BoundExprProgramBuilder,
	remap: &mut HashMap<BoundExprId, BoundExprId>,
) -> Option<BoundExprId> {
	if let Some(cloned) = remap.get(&id) {
		return Some(*cloned);
	}

	let output_type = source.node_type(id)?.clone();
	let cloned = match source.node(id)? {
		BoundExprNode::Column(column) => builder.append_column(column.clone()),
		BoundExprNode::Literal(literal) => {
			builder.append_literal(source.literals.get(*literal as usize)?.clone())
		}
		BoundExprNode::NullLiteral => builder.append_null(),
		BoundExprNode::Cast { input, target } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_cast(input, target.clone(), output_type)
		}
		BoundExprNode::TryCast { input, target } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_try_cast(input, target.clone(), output_type)
		}
		BoundExprNode::Binary { op, left, right } => {
			let left = clone_bound_subgraph(source, *left, builder, remap)?;
			let right = clone_bound_subgraph(source, *right, builder, remap)?;
			builder.append_binary(op.clone(), left, right, output_type)
		}
		BoundExprNode::Unary { op, input } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_unary(op.clone(), input, output_type)
		}
		BoundExprNode::InList {
			expr,
			list,
			negated,
		} => {
			let expr = clone_bound_subgraph(source, *expr, builder, remap)?;
			let list = list
				.iter()
				.map(|item| clone_bound_subgraph(source, *item, builder, remap))
				.collect::<Option<Vec<_>>>()?;
			builder.append_in_list(expr, list, *negated)
		}
		BoundExprNode::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => {
			let expr = clone_bound_subgraph(source, *expr, builder, remap)?;
			builder.append_like(expr, pattern.clone(), *negated, *case_insensitive)
		}
		BoundExprNode::Substring {
			input,
			from,
			length,
		} => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_substring(input, *from, *length)
		}
		BoundExprNode::Case {
			when_then,
			else_input,
		} => {
			let when_then = when_then
				.iter()
				.map(|(condition, result)| {
					Some((
						clone_bound_subgraph(source, *condition, builder, remap)?,
						clone_bound_subgraph(source, *result, builder, remap)?,
					))
				})
				.collect::<Option<Vec<_>>>()?;
			let else_input = clone_bound_subgraph(source, *else_input, builder, remap)?;
			builder.append_case(when_then, else_input, output_type)
		}
		BoundExprNode::ScalarFunction { id, args } => {
			let args = args
				.iter()
				.map(|arg| clone_bound_subgraph(source, *arg, builder, remap))
				.collect::<Option<Vec<_>>>()?;
			builder.append_scalar_function(*id, args, output_type)
		}
		BoundExprNode::TimeBucket { interval, input } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_time_bucket(*interval, input, output_type)
		}
		BoundExprNode::AtTimeZone { input, zone } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_at_time_zone(input, zone.clone(), output_type)
		}
		BoundExprNode::Extract { field, zone, input } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_extract(*field, zone.clone(), input, output_type)
		}
		BoundExprNode::DateTrunc { field, zone, input } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_date_trunc(*field, zone.clone(), input, output_type)
		}
	};
	if let Some(logical_type) = source.node_logical_type(id) {
		builder.set_node_logical_type(cloned, logical_type);
	}
	remap.insert(id, cloned);
	Some(cloned)
}

pub(super) fn bound_subgraphs_structurally_equal(
	source: &BoundExprProgram,
	left: BoundExprId,
	right: BoundExprId,
) -> bool {
	let mut seen = HashSet::new();
	bound_subgraphs_structurally_equal_inner(source, left, right, &mut seen)
}

pub(super) fn bound_subgraphs_structurally_equal_inner(
	source: &BoundExprProgram,
	left: BoundExprId,
	right: BoundExprId,
	seen: &mut HashSet<(BoundExprId, BoundExprId)>,
) -> bool {
	if left == right {
		return true;
	}
	if !seen.insert((left, right)) {
		return true;
	}
	if source.node_type(left) != source.node_type(right) {
		return false;
	}

	match (source.node(left), source.node(right)) {
		(Some(BoundExprNode::Column(left)), Some(BoundExprNode::Column(right))) => left == right,
		(Some(BoundExprNode::Literal(left)), Some(BoundExprNode::Literal(right))) => {
			source.literals.get(*left as usize) == source.literals.get(*right as usize)
		}
		(
			Some(BoundExprNode::Cast {
				input: left_input,
				target: left_target,
			}),
			Some(BoundExprNode::Cast {
				input: right_input,
				target: right_target,
			}),
		)
		| (
			Some(BoundExprNode::TryCast {
				input: left_input,
				target: left_target,
			}),
			Some(BoundExprNode::TryCast {
				input: right_input,
				target: right_target,
			}),
		) => {
			left_target == right_target
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::Binary {
				op: left_op,
				left: left_left,
				right: left_right,
			}),
			Some(BoundExprNode::Binary {
				op: right_op,
				left: right_left,
				right: right_right,
			}),
		) => {
			left_op == right_op
				&& bound_subgraphs_structurally_equal_inner(source, *left_left, *right_left, seen)
				&& bound_subgraphs_structurally_equal_inner(source, *left_right, *right_right, seen)
		}
		(
			Some(BoundExprNode::Unary {
				op: left_op,
				input: left_input,
			}),
			Some(BoundExprNode::Unary {
				op: right_op,
				input: right_input,
			}),
		) => {
			left_op == right_op
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::InList {
				expr: left_expr,
				list: left_list,
				negated: left_negated,
			}),
			Some(BoundExprNode::InList {
				expr: right_expr,
				list: right_list,
				negated: right_negated,
			}),
		) => {
			left_negated == right_negated
				&& left_list.len() == right_list.len()
				&& bound_subgraphs_structurally_equal_inner(source, *left_expr, *right_expr, seen)
				&& left_list
					.iter()
					.zip(right_list)
					.all(|(left_item, right_item)| {
						bound_subgraphs_structurally_equal_inner(
							source,
							*left_item,
							*right_item,
							seen,
						)
					})
		}
		(
			Some(BoundExprNode::Like {
				expr: left_expr,
				pattern: left_pattern,
				negated: left_negated,
				case_insensitive: left_case_insensitive,
			}),
			Some(BoundExprNode::Like {
				expr: right_expr,
				pattern: right_pattern,
				negated: right_negated,
				case_insensitive: right_case_insensitive,
			}),
		) => {
			left_pattern == right_pattern
				&& left_negated == right_negated
				&& left_case_insensitive == right_case_insensitive
				&& bound_subgraphs_structurally_equal_inner(source, *left_expr, *right_expr, seen)
		}
		(
			Some(BoundExprNode::Substring {
				input: left_input,
				from: left_from,
				length: left_length,
			}),
			Some(BoundExprNode::Substring {
				input: right_input,
				from: right_from,
				length: right_length,
			}),
		) => {
			left_from == right_from
				&& left_length == right_length
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::Case {
				when_then: left_when_then,
				else_input: left_else,
			}),
			Some(BoundExprNode::Case {
				when_then: right_when_then,
				else_input: right_else,
			}),
		) => {
			left_when_then.len() == right_when_then.len()
				&& left_when_then.iter().zip(right_when_then.iter()).all(
					|((left_condition, left_result), (right_condition, right_result))| {
						bound_subgraphs_structurally_equal_inner(
							source,
							*left_condition,
							*right_condition,
							seen,
						) && bound_subgraphs_structurally_equal_inner(
							source,
							*left_result,
							*right_result,
							seen,
						)
					},
				) && bound_subgraphs_structurally_equal_inner(source, *left_else, *right_else, seen)
		}
		(
			Some(BoundExprNode::TimeBucket {
				interval: left_interval,
				input: left_input,
			}),
			Some(BoundExprNode::TimeBucket {
				interval: right_interval,
				input: right_input,
			}),
		) => {
			left_interval == right_interval
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::AtTimeZone {
				input: left_input,
				zone: left_zone,
			}),
			Some(BoundExprNode::AtTimeZone {
				input: right_input,
				zone: right_zone,
			}),
		) => {
			left_zone == right_zone
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::Extract {
				field: left_field,
				zone: left_zone,
				input: left_input,
			}),
			Some(BoundExprNode::Extract {
				field: right_field,
				zone: right_zone,
				input: right_input,
			}),
		) => {
			left_field == right_field
				&& left_zone == right_zone
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::DateTrunc {
				field: left_field,
				zone: left_zone,
				input: left_input,
			}),
			Some(BoundExprNode::DateTrunc {
				field: right_field,
				zone: right_zone,
				input: right_input,
			}),
		) => {
			left_field == right_field
				&& left_zone == right_zone
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		_ => false,
	}
}

pub(super) fn count_bound_subgraph_nodes(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<usize, BooleanSplitError> {
	let mut seen = HashSet::new();
	count_bound_subgraph_nodes_inner(source, root, &mut seen)
}

pub(super) fn count_bound_subgraph_nodes_inner(
	source: &BoundExprProgram,
	id: BoundExprId,
	seen: &mut HashSet<BoundExprId>,
) -> Result<usize, BooleanSplitError> {
	if !seen.insert(id) {
		return Ok(0);
	}
	let node = source.node(id).ok_or(BooleanSplitError::Malformed)?;
	let mut count = 1;
	for child in bound_node_children(node) {
		count += count_bound_subgraph_nodes_inner(source, child, seen)?;
		if count > BOOLEAN_FACTOR_MAX_NODES {
			return Ok(count);
		}
	}
	Ok(count)
}

pub(super) fn bound_node_children(node: &BoundExprNode) -> Vec<BoundExprId> {
	match node {
		BoundExprNode::Column(_) | BoundExprNode::Literal(_) | BoundExprNode::NullLiteral => {
			Vec::new()
		}
		BoundExprNode::Cast { input, .. }
		| BoundExprNode::TryCast { input, .. }
		| BoundExprNode::Unary { input, .. }
		| BoundExprNode::Like { expr: input, .. }
		| BoundExprNode::Substring { input, .. }
		| BoundExprNode::TimeBucket { input, .. }
		| BoundExprNode::AtTimeZone { input, .. }
		| BoundExprNode::Extract { input, .. }
		| BoundExprNode::DateTrunc { input, .. } => vec![*input],
		BoundExprNode::Binary { left, right, .. } => vec![*left, *right],
		BoundExprNode::InList { expr, list, .. } => {
			let mut children = Vec::with_capacity(list.len() + 1);
			children.push(*expr);
			children.extend(list.iter().copied());
			children
		}
		BoundExprNode::Case {
			when_then,
			else_input,
		} => {
			let mut children = Vec::with_capacity(when_then.len() * 2 + 1);
			for (condition, result) in when_then {
				children.push(*condition);
				children.push(*result);
			}
			children.push(*else_input);
			children
		}
		BoundExprNode::ScalarFunction { args, .. } => args.clone(),
	}
}
