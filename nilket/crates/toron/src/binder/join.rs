use super::*;

pub(super) struct JoinStrategyDecision {
	pub(super) strategy: JoinStrategy,
	pub(super) placement: JoinPhysicalPlacement,
	pub(super) reason: JoinStrategyDecisionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinPhysicalPlacement {
	Natural,
	SwappedRightBuild,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BroadcastBuildSide {
	Left,
	Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinStrategyDecisionReason {
	PinnedByAuthor,
	BroadcastInferred {
		build_side: BroadcastBuildSide,
		build_bytes: ByteCount,
		threshold: ByteCount,
		rejected_side_bytes: Option<ByteCount>,
	},
	BroadcastEstimated {
		build_side: BroadcastBuildSide,
		estimated_byte_size: ByteCount,
		threshold: ByteCount,
		rejected_side_bytes: Option<ByteCount>,
	},
	HashByDefault {
		reason: HashByDefaultReason,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashByDefaultReason {
	AutoBroadcastDisabled,
	NonInnerJoin,
	LeftInputNotSingleBaseRelation,
	RightInputNotSingleBaseRelation,
	ProbeNotScanLinear,
	MissingBuildByteUpperBound,
	BuildByteUpperBoundAboveThreshold,
	HashAfterBroadcastCap,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct JoinInputOptimization {
	pub(super) facts: PlanOptimizerFacts,
	pub(super) probe_scan_linear: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BuildByteSource {
	Hard,
	Estimated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BuildByteCandidate {
	pub(super) bytes: ByteCount,
	pub(super) source: BuildByteSource,
}

#[derive(Debug, Clone)]
pub(super) struct RelationScope {
	schema: SchemaRef,
	entries: Vec<RelationScopeEntry>,
	next_source_id: usize,
}

#[derive(Debug, Clone)]
pub(super) struct RelationScopeEntry {
	qualifier: TableRef,
	column: String,
	ordinal: ColumnOrdinal,
	source_id: usize,
}

impl RelationScope {
	pub(super) fn empty(schema: SchemaRef) -> Self {
		Self {
			schema,
			entries: Vec::new(),
			next_source_id: 0,
		}
	}

	pub(super) fn scan(qualifier: TableRef, schema: &SchemaRef) -> Self {
		Self::single_source(qualifier, schema)
	}

	pub(super) fn alias(qualifier: TableRef, schema: &SchemaRef) -> Self {
		Self::single_source(qualifier, schema)
	}

	pub(super) fn single_source(qualifier: TableRef, schema: &SchemaRef) -> Self {
		let entries = schema
			.fields()
			.iter()
			.enumerate()
			.map(|(idx, field)| RelationScopeEntry {
				qualifier: qualifier.clone(),
				column: field.name().clone(),
				ordinal: ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow")),
				source_id: 0,
			})
			.collect();
		Self {
			schema: schema.clone(),
			entries,
			next_source_id: 1,
		}
	}

	pub(super) fn with_schema(&self, schema: SchemaRef) -> Self {
		let mut scope = self.clone();
		scope.schema = schema;
		scope
	}

	pub(super) fn project(&self, exprs: &BoundExprProgram, output_schema: &SchemaRef) -> Self {
		let mut entries = Vec::new();
		for (output_idx, root) in exprs.roots.iter().enumerate() {
			let Some(BoundExprNode::Column(column)) = exprs.node(*root) else {
				continue;
			};
			let output_field = output_schema.field(output_idx);
			let output_ordinal =
				ColumnOrdinal(u32::try_from(output_idx).expect("column ordinal overflow"));
			for entry in self
				.entries
				.iter()
				.filter(|entry| entry.ordinal == column.ordinal)
			{
				entries.push(RelationScopeEntry {
					qualifier: entry.qualifier.clone(),
					column: output_field.name().clone(),
					ordinal: output_ordinal,
					source_id: entry.source_id,
				});
			}
		}
		Self {
			schema: output_schema.clone(),
			entries,
			next_source_id: self.next_source_id,
		}
	}

	pub(super) fn aggregate(
		&self,
		group_expr: &BoundExprProgram,
		output_schema: &SchemaRef,
	) -> Self {
		let mut entries = Vec::new();
		for (output_idx, root) in group_expr.roots.iter().enumerate() {
			let Some(BoundExprNode::Column(column)) = group_expr.node(*root) else {
				continue;
			};
			let output_field = output_schema.field(output_idx);
			let output_ordinal =
				ColumnOrdinal(u32::try_from(output_idx).expect("column ordinal overflow"));
			for entry in self
				.entries
				.iter()
				.filter(|entry| entry.ordinal == column.ordinal)
			{
				entries.push(RelationScopeEntry {
					qualifier: entry.qualifier.clone(),
					column: output_field.name().clone(),
					ordinal: output_ordinal,
					source_id: entry.source_id,
				});
			}
		}
		Self {
			schema: output_schema.clone(),
			entries,
			next_source_id: self.next_source_id,
		}
	}

	pub(super) fn join(
		kind: JoinKind,
		left: &RelationScope,
		right: &RelationScope,
		output_schema: &SchemaRef,
	) -> Self {
		match kind {
			JoinKind::LeftSemi | JoinKind::LeftAnti => left.with_schema(output_schema.clone()),
			JoinKind::RightSemi | JoinKind::RightAnti => right.with_schema(output_schema.clone()),
			JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter => {
				let mut entries = left.entries.clone();
				let right_offset = ColumnOrdinal(
					u32::try_from(left.schema.fields().len()).expect("column ordinal overflow"),
				);
				entries.extend(right.entries.iter().map(|entry| RelationScopeEntry {
					qualifier: entry.qualifier.clone(),
					column: entry.column.clone(),
					ordinal: ColumnOrdinal(entry.ordinal.0 + right_offset.0),
					source_id: entry.source_id + left.next_source_id,
				}));
				Self {
					schema: output_schema.clone(),
					entries,
					next_source_id: left.next_source_id + right.next_source_id,
				}
			}
		}
	}

	pub(super) fn resolve_join_column(
		&self,
		column_ref: &ColumnRef,
		side: &'static str,
	) -> Result<Option<ResolvedLogicalColumn>, BindingError> {
		if column_ref.qualifier.is_some() {
			if !self.contains_qualifier(column_ref.qualifier.as_ref().expect("checked")) {
				return Ok(None);
			}
			return resolve_qualified_column(column_ref, self).map(Some);
		}

		match resolve_logical_column(&column_ref.name.0, &self.schema) {
			Ok(column) => Ok(Some(column)),
			Err(BindingError::UnknownColumn { .. }) => Ok(None),
			Err(error) => Err(error),
		}
		.map_err(|error| match error {
			BindingError::AmbiguousColumn { column } => BindingError::AmbiguousColumnRef {
				name: column,
				found_in: vec![side.to_string()],
			},
			other => other,
		})
	}

	pub(super) fn contains_qualifier(&self, qualifier: &TableRef) -> bool {
		self.entries
			.iter()
			.any(|entry| &entry.qualifier == qualifier)
	}

	pub(super) fn source_label_for_ordinal(&self, ordinal: ColumnOrdinal) -> Option<String> {
		let labels = self
			.entries
			.iter()
			.filter(|entry| entry.ordinal == ordinal)
			.map(|entry| relation_key_label(&entry.qualifier))
			.collect::<BTreeSet<_>>();
		if labels.len() == 1 {
			labels.into_iter().next()
		} else {
			None
		}
	}

	pub(super) fn unique_source_label(&self) -> Option<String> {
		let labels = self
			.entries
			.iter()
			.map(|entry| relation_key_label(&entry.qualifier))
			.collect::<BTreeSet<_>>();
		if labels.len() == 1 {
			labels.into_iter().next()
		} else {
			None
		}
	}
}

pub(super) fn resolve_qualified_column(
	column_ref: &ColumnRef,
	scope: &RelationScope,
) -> Result<ResolvedLogicalColumn, BindingError> {
	let Some(qualifier) = &column_ref.qualifier else {
		return resolve_logical_column(&column_ref.name.0, &scope.schema);
	};
	let qualifier_entries = scope
		.entries
		.iter()
		.filter(|entry| &entry.qualifier == qualifier)
		.collect::<Vec<_>>();
	if qualifier_entries.is_empty() {
		return Err(BindingError::UnknownTableRef {
			qualifier: table_name(qualifier),
		});
	}
	let source_count = qualifier_entries
		.iter()
		.map(|entry| entry.source_id)
		.collect::<BTreeSet<_>>()
		.len();
	if source_count > 1 {
		return Err(BindingError::AmbiguousTableRef {
			qualifier: table_name(qualifier),
			found_in: qualifier_entries
				.iter()
				.map(|entry| entry.source_id.to_string())
				.collect::<BTreeSet<_>>()
				.into_iter()
				.collect(),
		});
	}
	let column_entries = qualifier_entries
		.iter()
		.filter(|entry| entry.column == column_ref.name.0)
		.collect::<Vec<_>>();
	let [entry] = column_entries.as_slice() else {
		if column_entries.is_empty() {
			return Err(BindingError::UnknownColumnInRelation {
				qualifier: table_name(qualifier),
				column: column_ref.name.0.clone(),
				available: qualifier_entries
					.iter()
					.map(|entry| entry.column.clone())
					.collect::<BTreeSet<_>>()
					.into_iter()
					.collect(),
			});
		}
		return Err(BindingError::AmbiguousColumnRef {
			name: format!("{}.{}", table_name(qualifier), column_ref.name.0),
			found_in: column_entries
				.iter()
				.map(|entry| entry.source_id.to_string())
				.collect::<BTreeSet<_>>()
				.into_iter()
				.collect(),
		});
	};
	resolved_logical_column_at_ordinal(&scope.schema, entry.ordinal)
}

pub(super) fn bind_join_key_pair(
	left_expr: &Expr,
	right_expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Result<(ResolvedJoinKey, ResolvedJoinKey), BindingError> {
	let (left_ref, left_logical_type) = join_key_column_ref(left_expr)?;
	let (right_ref, right_logical_type) = join_key_column_ref(right_expr)?;

	match (left_expr, right_expr) {
		(Expr::Eq(_, _), _) | (_, Expr::Eq(_, _)) => {
			return Err(BindingError::JoinPredicateNotEqui);
		}
		_ => {}
	}

	match (
		resolve_join_column_across(left_ref, left_scope, right_scope)?,
		resolve_join_column_across(right_ref, left_scope, right_scope)?,
	) {
		(JoinColumnResolution::Left(left), JoinColumnResolution::Right(right)) => Ok((
			resolved_join_key(left, left_logical_type)?,
			resolved_join_key(right, right_logical_type)?,
		)),
		(JoinColumnResolution::Right(right), JoinColumnResolution::Left(left)) => Ok((
			resolved_join_key(left, right_logical_type)?,
			resolved_join_key(right, left_logical_type)?,
		)),
		(JoinColumnResolution::Left(_), JoinColumnResolution::Left(_))
		| (JoinColumnResolution::Right(_), JoinColumnResolution::Right(_)) => {
			Err(BindingError::JoinComputedKeyUnsupported)
		}
	}
}

#[derive(Debug, Clone)]
pub(super) struct ResolvedJoinKey {
	pub(super) column: ResolvedColumn,
	pub(super) logical_type: LogicalTypeId,
}

#[derive(Debug, Clone)]
pub(super) struct ResolvedLogicalColumn {
	pub(super) column: ResolvedColumn,
	pub(super) logical_type: LogicalTypeId,
}

pub(super) fn broadcast_replay_eligibility(
	intent: JoinStrategyIntent,
) -> BroadcastReplayEligibility {
	match intent {
		JoinStrategyIntent::Auto
		| JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::SqlHint,
		} => BroadcastReplayEligibility::Replayable,
		JoinStrategyIntent::Pinned { .. } => BroadcastReplayEligibility::Terminal,
	}
}

pub(super) fn choose_join_strategy(
	replay_id: JoinReplayId,
	intent: JoinStrategyIntent,
	kind: JoinKind,
	left_input: JoinInputOptimization,
	right_input: JoinInputOptimization,
	config: &PlannerConfig,
) -> JoinStrategyDecision {
	let force_hash = config.force_hash_join_ids.contains(&replay_id);
	match intent {
		JoinStrategyIntent::Auto if force_hash => {
			hash_join_decision(HashByDefaultReason::HashAfterBroadcastCap)
		}
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::SqlHint,
		} if force_hash => hash_join_decision(HashByDefaultReason::HashAfterBroadcastCap),
		JoinStrategyIntent::Pinned { strategy, .. } => JoinStrategyDecision {
			strategy,
			placement: JoinPhysicalPlacement::Natural,
			reason: JoinStrategyDecisionReason::PinnedByAuthor,
		},
		JoinStrategyIntent::Auto => {
			choose_auto_join_strategy(kind, left_input, right_input, config)
		}
	}
}

pub(super) fn choose_auto_join_strategy(
	kind: JoinKind,
	left_input: JoinInputOptimization,
	right_input: JoinInputOptimization,
	config: &PlannerConfig,
) -> JoinStrategyDecision {
	let Some(threshold) = effective_auto_broadcast_threshold(config) else {
		return hash_join_decision(HashByDefaultReason::AutoBroadcastDisabled);
	};
	if kind != JoinKind::Inner {
		return hash_join_decision(HashByDefaultReason::NonInnerJoin);
	}

	let left_build_candidate = build_byte_candidate(left_input.facts, threshold);
	let right_build_candidate = build_byte_candidate(right_input.facts, threshold);

	let left_viable = left_build_candidate.filter(|_| right_input.probe_scan_linear);
	let right_viable = right_build_candidate.filter(|_| left_input.probe_scan_linear);
	let chosen = match (left_viable, right_viable) {
		(Some(left), Some(right)) if right_build_materially_smaller(left.bytes, right.bytes) => {
			Some((
				BroadcastBuildSide::Right,
				JoinPhysicalPlacement::SwappedRightBuild,
				right,
				Some(left.bytes),
			))
		}
		(Some(left), Some(right)) => Some((
			BroadcastBuildSide::Left,
			JoinPhysicalPlacement::Natural,
			left,
			Some(right.bytes),
		)),
		(Some(left), None) => Some((
			BroadcastBuildSide::Left,
			JoinPhysicalPlacement::Natural,
			left,
			None,
		)),
		(None, Some(right)) => Some((
			BroadcastBuildSide::Right,
			JoinPhysicalPlacement::SwappedRightBuild,
			right,
			None,
		)),
		(None, None) => None,
	};
	if let Some((build_side, placement, candidate, rejected_side_bytes)) = chosen {
		let reason = match candidate.source {
			BuildByteSource::Hard => JoinStrategyDecisionReason::BroadcastInferred {
				build_side,
				build_bytes: candidate.bytes,
				threshold,
				rejected_side_bytes,
			},
			BuildByteSource::Estimated => JoinStrategyDecisionReason::BroadcastEstimated {
				build_side,
				estimated_byte_size: candidate.bytes,
				threshold,
				rejected_side_bytes,
			},
		};
		return JoinStrategyDecision {
			strategy: JoinStrategy::Broadcast,
			placement,
			reason,
		};
	}
	if left_build_candidate.is_some() || right_build_candidate.is_some() {
		return hash_join_decision(HashByDefaultReason::ProbeNotScanLinear);
	}
	if left_input.facts.origin != PlanOrigin::SingleBaseRelation {
		return hash_join_decision(HashByDefaultReason::LeftInputNotSingleBaseRelation);
	}
	if right_input.facts.origin != PlanOrigin::SingleBaseRelation {
		return hash_join_decision(HashByDefaultReason::RightInputNotSingleBaseRelation);
	}
	if !has_build_size_fact(left_input.facts) || !has_build_size_fact(right_input.facts) {
		return hash_join_decision(HashByDefaultReason::MissingBuildByteUpperBound);
	}
	hash_join_decision(HashByDefaultReason::BuildByteUpperBoundAboveThreshold)
}

pub(super) const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR: u64 = 2;

pub(super) fn build_byte_candidate(
	facts: PlanOptimizerFacts,
	threshold: ByteCount,
) -> Option<BuildByteCandidate> {
	match facts.origin {
		PlanOrigin::SingleBaseRelation => {
			if let Some(build_bytes) = facts.byte_upper_bound {
				return (build_bytes < threshold).then_some(BuildByteCandidate {
					bytes: build_bytes,
					source: BuildByteSource::Hard,
				});
			}
			facts
				.estimated_byte_size
				.filter(|estimated_byte_size| *estimated_byte_size < threshold)
				.map(|estimated_byte_size| BuildByteCandidate {
					bytes: estimated_byte_size,
					source: BuildByteSource::Estimated,
				})
		}
		PlanOrigin::Composite => facts
			.estimated_byte_size
			.filter(|estimated_byte_size| *estimated_byte_size < threshold)
			.map(|estimated_byte_size| BuildByteCandidate {
				bytes: estimated_byte_size,
				source: BuildByteSource::Estimated,
			}),
		PlanOrigin::Unknown => None,
	}
}

pub(super) fn has_build_size_fact(facts: PlanOptimizerFacts) -> bool {
	facts.byte_upper_bound.is_some() || facts.estimated_byte_size.is_some()
}

pub(super) fn right_build_materially_smaller(
	left_bytes: ByteCount,
	right_bytes: ByteCount,
) -> bool {
	u128::from(right_bytes.get()) * u128::from(AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR)
		<= u128::from(left_bytes.get())
}

pub(super) fn broadcast_probe_is_scan_linear(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Scan { row_limit, .. } => row_limit.is_none(),
		ExecutionPlan::Filter { input, .. } | ExecutionPlan::Project { input, .. } => {
			broadcast_probe_is_scan_linear(input)
		}
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			strategy,
			..
		} if *kind == JoinKind::Inner
			&& *strategy == JoinStrategy::Broadcast
			&& matches!(terminal_exchange(left), Some((1, _))) =>
		{
			broadcast_probe_is_scan_linear(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Aggregate { .. }
		| ExecutionPlan::Exchange { .. }
		| ExecutionPlan::Sort { .. }
		| ExecutionPlan::Take { .. }
		| ExecutionPlan::Join { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => false,
	}
}

pub(super) fn hash_join_decision(reason: HashByDefaultReason) -> JoinStrategyDecision {
	JoinStrategyDecision {
		strategy: JoinStrategy::HashPartitioned,
		placement: JoinPhysicalPlacement::Natural,
		reason: JoinStrategyDecisionReason::HashByDefault { reason },
	}
}

pub(super) fn effective_auto_broadcast_threshold(config: &PlannerConfig) -> Option<ByteCount> {
	let configured = config.auto_broadcast_max_build_bytes?;
	let static_cap = default_auto_broadcast_cap_ceiling();
	Some(std::cmp::min(configured, static_cap))
}

pub(super) fn default_auto_broadcast_cap_ceiling() -> ByteCount {
	ByteCount::new(QueryBudget::default().broadcast_build_bytes_per_task.get() / 4)
}

pub(super) fn restore_declared_join_order_project(
	input: ExecutionPlan,
	physical_output_schema: &SchemaRef,
	declared_output_schema: SchemaRef,
	declared_left_width: usize,
	declared_right_width: usize,
) -> Result<ExecutionPlan, BindingError> {
	let physical_width = physical_output_schema.fields().len();
	if physical_width != declared_left_width + declared_right_width
		|| declared_output_schema.fields().len() != physical_width
	{
		return Err(BindingError::UnsupportedExpression {
			message: "right-build broadcast join projection arity mismatch".to_string(),
		});
	}

	let restore_ordinals = (declared_right_width..declared_right_width + declared_left_width)
		.chain(0..declared_right_width);
	let mut builder = BoundExprProgramBuilder::new();
	for physical_ordinal in restore_ordinals {
		let field = physical_output_schema.field(physical_ordinal);
		let logical_type = decode_column_logical_type(field).map_err(|error| {
			BindingError::UnsupportedExpression {
				message: error.to_string(),
			}
		})?;
		let node = builder.append_column(ResolvedColumn {
			name: ColumnName(field.name().clone()),
			ordinal: ColumnOrdinal(
				u32::try_from(physical_ordinal).expect("column ordinal overflow"),
			),
			data_type: field.data_type().clone(),
			nullable: field.is_nullable(),
			pbc_eligible: is_pbc_eligible_type(field.data_type()),
		});
		if !builder.set_node_logical_type(node, logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "right-build broadcast join projection target is malformed".to_string(),
			});
		}
		builder.push_root(node);
	}
	let exprs = builder.build();
	exprs
		.validate()
		.map_err(|error| BindingError::UnsupportedExpression {
			message: format!("{error:?}"),
		})?;
	Ok(ExecutionPlan::Project {
		input: Box::new(input),
		exprs,
		output_schema: declared_output_schema,
	})
}

pub(super) fn exchange_for_join_keys(
	input: ExecutionPlan,
	keys: &[ResolvedJoinKey],
	partition_count: u32,
) -> Result<ExecutionPlan, BindingError> {
	Ok(ExecutionPlan::Exchange {
		input: Box::new(input),
		partition_count,
		key: join_key_program(keys)?,
	})
}

pub(super) fn strip_root_exchange(input: ExecutionPlan) -> ExecutionPlan {
	match input {
		ExecutionPlan::Exchange { input, .. } => *input,
		other => other,
	}
}

pub(super) fn join_key_program(keys: &[ResolvedJoinKey]) -> Result<BoundExprProgram, BindingError> {
	let mut builder = BoundExprProgramBuilder::new();
	for key in keys {
		let node = builder.append_column(key.column.clone());
		if !builder.set_node_logical_type(node, key.logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "logical join key target is malformed".to_string(),
			});
		}
		builder.push_root(node);
	}
	Ok(builder.build())
}

pub(super) fn join_key_column_ref(
	expr: &Expr,
) -> Result<(&ColumnRef, LogicalTypeId), BindingError> {
	match expr {
		Expr::Column(column_ref) => Ok((column_ref, LogicalTypeId::NAKED)),
		Expr::LogicalAscription { expr, logical_type } => match expr.as_ref() {
			Expr::Column(column_ref) => Ok((column_ref, *logical_type)),
			Expr::Eq(_, _) => Err(BindingError::JoinPredicateNotEqui),
			_ => Err(BindingError::JoinComputedKeyUnsupported),
		},
		Expr::SqlLogicalCast { .. } => Err(BindingError::JoinComputedKeyUnsupported),
		Expr::SqlConvert { .. } => Err(BindingError::JoinComputedKeyUnsupported),
		Expr::Eq(_, _) => Err(BindingError::JoinPredicateNotEqui),
		_ => Err(BindingError::JoinComputedKeyUnsupported),
	}
}

pub(super) fn resolved_join_key(
	column: ResolvedLogicalColumn,
	declared_logical_type: LogicalTypeId,
) -> Result<ResolvedJoinKey, BindingError> {
	validate_logical_ascription_physical(&column.column.data_type, declared_logical_type)?;
	let logical_type = reconcile_identity(
		column.logical_type,
		declared_logical_type,
		&column.column.data_type,
	)?;
	validate_logical_ascription_physical(&column.column.data_type, logical_type)?;
	Ok(ResolvedJoinKey {
		column: column.column,
		logical_type,
	})
}

pub(super) enum JoinColumnResolution {
	Left(ResolvedLogicalColumn),
	Right(ResolvedLogicalColumn),
}

pub(super) fn resolve_join_column_across(
	column_ref: &ColumnRef,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Result<JoinColumnResolution, BindingError> {
	let left = left_scope.resolve_join_column(column_ref, "left")?;
	let right = right_scope.resolve_join_column(column_ref, "right")?;
	match (left, right) {
		(Some(left), None) => Ok(JoinColumnResolution::Left(left)),
		(None, Some(right)) => Ok(JoinColumnResolution::Right(right)),
		(Some(_), Some(_)) => Err(BindingError::AmbiguousColumnRef {
			name: column_ref.name.0.clone(),
			found_in: vec!["left".to_string(), "right".to_string()],
		}),
		(None, None) => {
			if let Some(qualifier) = &column_ref.qualifier {
				Err(BindingError::UnknownTableRef {
					qualifier: table_name(qualifier),
				})
			} else {
				Err(BindingError::UnknownColumn {
					column: column_ref.name.0.clone(),
				})
			}
		}
	}
}

pub(super) fn validate_join_key_types(
	left: &ResolvedJoinKey,
	right: &ResolvedJoinKey,
) -> Result<(), BindingError> {
	if left.column.data_type != right.column.data_type {
		return Err(BindingError::JoinKeyTypeMismatch {
			left_type: format!("{:?}", left.column.data_type),
			right_type: format!("{:?}", right.column.data_type),
		});
	}
	if matches!(
		left.column.data_type,
		DataType::Float16 | DataType::Float32 | DataType::Float64
	) {
		return Err(BindingError::JoinKeyTypeUnsupported {
			type_name: format!("{:?}", left.column.data_type),
		});
	}
	nominal_compatible(
		&left.column.data_type,
		&right.column.data_type,
		left.logical_type,
		right.logical_type,
		false,
		false,
	)?;
	Ok(())
}

pub(super) fn validate_strategy_shape(
	kind: JoinKind,
	strategy: JoinStrategy,
	left: &ExecutionPlan,
	right: &ExecutionPlan,
	left_keys: &[ColumnOrdinal],
	right_keys: &[ColumnOrdinal],
) -> Result<(), BindingError> {
	match strategy {
		JoinStrategy::HashPartitioned => {
			let Some((left_count, left_exchange_keys)) = terminal_exchange(left) else {
				return Err(BindingError::JoinKeyExchangeMismatch);
			};
			let Some((right_count, right_exchange_keys)) = terminal_exchange(right) else {
				return Err(BindingError::JoinKeyExchangeMismatch);
			};
			if left_count != right_count {
				return Err(BindingError::JoinPartitionCountMismatch {
					left: left_count,
					right: right_count,
				});
			}
			if exchange_key_ordinals(left_exchange_keys) != left_keys
				|| exchange_key_ordinals(right_exchange_keys) != right_keys
			{
				return Err(BindingError::JoinKeyExchangeMismatch);
			}
		}
		JoinStrategy::Broadcast => {
			if kind != JoinKind::Inner {
				return Err(BindingError::UnsupportedExpression {
					message: "broadcast join is only supported for INNER joins".to_string(),
				});
			}
			let Some((left_count, _)) = terminal_exchange(left) else {
				return Err(BindingError::BroadcastBuildSidePlacement);
			};
			if left_count != 1 {
				return Err(BindingError::BroadcastBuildSidePlacement);
			}
			if terminal_exchange(right).is_some() {
				return Err(BindingError::BroadcastBuildSidePlacement);
			}
		}
	}
	Ok(())
}

pub(super) fn terminal_exchange(plan: &ExecutionPlan) -> Option<(u32, &BoundExprProgram)> {
	match plan {
		ExecutionPlan::Exchange {
			partition_count,
			key,
			..
		} => Some((*partition_count, key)),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => terminal_exchange(input),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::Join { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

pub(super) fn exchange_key_ordinals(key: &BoundExprProgram) -> Vec<ColumnOrdinal> {
	key.root_columns()
		.unwrap_or_default()
		.into_iter()
		.map(|column| column.ordinal)
		.collect()
}

pub fn join_schema(kind: JoinKind, left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	let fields = match kind {
		JoinKind::Inner => join_fields(left, right),
		JoinKind::LeftOuter => left_fields(left)
			.into_iter()
			.chain(nullable_fields(right))
			.collect(),
		JoinKind::RightOuter => nullable_fields(left)
			.into_iter()
			.chain(left_fields(right))
			.collect(),
		JoinKind::FullOuter => nullable_fields(left)
			.into_iter()
			.chain(nullable_fields(right))
			.collect(),
		JoinKind::LeftSemi | JoinKind::LeftAnti => left_fields(left),
		JoinKind::RightSemi | JoinKind::RightAnti => left_fields(right),
	};
	Arc::new(Schema::new(fields))
}

pub(super) fn join_fields(left: &SchemaRef, right: &SchemaRef) -> Vec<Field> {
	left_fields(left)
		.into_iter()
		.chain(left_fields(right))
		.collect()
}

pub(super) fn left_fields(schema: &SchemaRef) -> Vec<Field> {
	schema
		.fields()
		.iter()
		.map(|field| field.as_ref().clone())
		.collect()
}

pub(super) fn nullable_fields(schema: &SchemaRef) -> Vec<Field> {
	schema
		.fields()
		.iter()
		.map(|field| field.as_ref().clone().with_nullable(true))
		.collect()
}
