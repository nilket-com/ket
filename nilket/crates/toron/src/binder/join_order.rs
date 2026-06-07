use super::*;

pub(super) type JoinOrderSourceMap = BTreeMap<String, JoinOrderRelationFacts>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinOrderRelationFacts {
	pub(super) rows: Option<RowCount>,
	pub(super) column_ndv: HashMap<String, DistinctCountFact>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOrderEstimateSource {
	SingleKeyNdvContainment,
	MultiKeyNdvContainment,
	Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinOrderEdgeEvidence {
	pub(super) target: JoinTraceTarget,
	pub(super) probe: Option<String>,
	pub(super) dimension: Option<String>,
	pub(super) declared_position: usize,
	pub(super) estimated_rows: Option<RowCount>,
	pub(super) estimate_source: JoinOrderEstimateSource,
	pub(super) inner_equi: bool,
	pub(super) probe_scan_linear: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinOrderDimEvidence {
	pub(super) dimension: String,
	pub(super) declared_position: usize,
	pub(super) estimated_rows: RowCount,
	pub(super) estimate_source: JoinOrderEstimateSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderEvidence {
	pub(super) probe: String,
	pub(super) dimensions: Vec<JoinOrderDimEvidence>,
	pub(super) declared_order: Vec<String>,
	pub(super) cost_preferred_order: Vec<String>,
	pub(super) declared_cost: u128,
	pub(super) preferred_cost: u128,
	pub(super) orders_agree: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderDeclineReason {
	NonInnerEquiJoin,
	MissingRelationLabel,
	MultipleProbeRelations,
	DuplicateDimension,
	MissingEstimate,
	ProbeNotScanLinear,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderDecline {
	pub(super) reason: StarOrderDeclineReason,
	pub(super) edge_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StarOrderOutcome {
	Recognized(StarOrderEvidence),
	Declined(StarOrderDecline),
}

pub(super) fn relation_key_label(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

pub(super) fn join_order_sources_for_scan(
	qualifier: &TableRef,
	schema: &SchemaRef,
	projection: Option<&[ColumnOrdinal]>,
	facts: &TableOptimizerFacts,
) -> JoinOrderSourceMap {
	let mut column_ndv = HashMap::new();
	for (output_index, field) in schema.fields().iter().enumerate() {
		let output_ordinal =
			ColumnOrdinal(u32::try_from(output_index).expect("join order source ordinal overflow"));
		let descriptor_ordinal = projection
			.and_then(|columns| columns.get(output_index).copied())
			.unwrap_or(output_ordinal);
		if let Some(distinct_count) = facts.column_ndv.get(&descriptor_ordinal) {
			column_ndv.insert(field.name().clone(), *distinct_count);
		}
	}
	let rows = estimate_row_source(facts.row_upper_bound, None);
	BTreeMap::from([(
		relation_key_label(qualifier),
		JoinOrderRelationFacts { rows, column_ndv },
	)])
}

pub(super) fn alias_join_order_sources(
	sources: Option<JoinOrderSourceMap>,
	alias: &TableRef,
) -> Option<JoinOrderSourceMap> {
	let mut sources = sources?;
	if sources.len() != 1 {
		return None;
	}
	let (_, facts) = sources.pop_first().expect("single source");
	Some(BTreeMap::from([(relation_key_label(alias), facts)]))
}

pub(super) fn update_single_source_join_order_rows(
	sources: Option<JoinOrderSourceMap>,
	scope: &RelationScope,
	rows: Option<RowCount>,
) -> Option<JoinOrderSourceMap> {
	let mut sources = sources?;
	let label = scope.unique_source_label()?;
	if let Some(facts) = sources.get_mut(&label) {
		facts.rows = rows.or(facts.rows);
	}
	Some(sources)
}

pub(super) fn merge_join_order_sources(
	left: Option<JoinOrderSourceMap>,
	right: Option<JoinOrderSourceMap>,
) -> Option<JoinOrderSourceMap> {
	match (left, right) {
		(Some(mut left), Some(right)) => {
			for (label, facts) in right {
				if left.insert(label, facts).is_some() {
					return None;
				}
			}
			Some(left)
		}
		(Some(left), None) => Some(left),
		(None, Some(right)) => Some(right),
		(None, None) => None,
	}
}

pub(super) fn relation_label_for_join_keys(
	scope: &RelationScope,
	keys: &[ResolvedJoinKey],
) -> Option<String> {
	let mut labels = BTreeSet::new();
	for key in keys {
		labels.insert(scope.source_label_for_ordinal(key.column.ordinal)?);
	}
	if labels.len() == 1 {
		labels.into_iter().next()
	} else {
		None
	}
}

pub(super) fn join_order_estimate_source(
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
	estimated_rows: Option<RowCount>,
) -> JoinOrderEstimateSource {
	if estimated_rows.is_none() {
		return JoinOrderEstimateSource::Missing;
	}
	if left_keys.len() == 1 && right_keys.len() == 1 {
		JoinOrderEstimateSource::SingleKeyNdvContainment
	} else if left_keys.len() == right_keys.len() && left_keys.len() >= 2 {
		JoinOrderEstimateSource::MultiKeyNdvContainment
	} else {
		JoinOrderEstimateSource::Missing
	}
}

pub(super) fn join_order_edge_estimate(
	left_sources: Option<&JoinOrderSourceMap>,
	right_sources: Option<&JoinOrderSourceMap>,
	probe: Option<&str>,
	dimension: Option<&str>,
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
	actual_join_estimate: Option<RowCount>,
) -> (Option<RowCount>, JoinOrderEstimateSource) {
	if actual_join_estimate.is_some() {
		return (
			actual_join_estimate,
			join_order_estimate_source(left_keys, right_keys, actual_join_estimate),
		);
	}
	// Later left-deep star edges see a composite left input, so the normal output
	// estimate can fail open. This evidence-only fallback reuses carried base
	// row/NDV facts and never feeds PlanOptimizerFacts or strategy selection.
	let Some(estimated_rows) = estimate_join_order_edge_from_sources(
		left_sources,
		right_sources,
		probe,
		dimension,
		left_keys,
		right_keys,
	) else {
		return (None, JoinOrderEstimateSource::Missing);
	};
	(
		Some(estimated_rows),
		join_order_estimate_source(left_keys, right_keys, Some(estimated_rows)),
	)
}

fn estimate_join_order_edge_from_sources(
	left_sources: Option<&JoinOrderSourceMap>,
	right_sources: Option<&JoinOrderSourceMap>,
	probe: Option<&str>,
	dimension: Option<&str>,
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
) -> Option<RowCount> {
	if left_keys.len() != right_keys.len() || left_keys.is_empty() {
		return None;
	}
	let probe = probe?;
	let dimension = dimension?;
	let left_facts = left_sources?.get(probe)?;
	let right_facts = right_sources?.get(dimension)?;
	let left_rows = left_facts.rows?;
	let right_rows = right_facts.rows?;
	if left_keys.len() == 1 {
		let left_ndv = *left_facts.column_ndv.get(&left_keys[0].column.name.0)?;
		let right_ndv = *right_facts.column_ndv.get(&right_keys[0].column.name.0)?;
		return ndv_containment_join_rows(left_rows, right_rows, left_ndv, right_ndv);
	}

	let numerator = u128::from(left_rows.get()) * u128::from(right_rows.get());
	let mut denominator = 1_u128;
	for (left_key, right_key) in left_keys.iter().zip(right_keys) {
		let left_ndv = left_facts
			.column_ndv
			.get(&left_key.column.name.0)?
			.point_estimate();
		let right_ndv = right_facts
			.column_ndv
			.get(&right_key.column.name.0)?
			.point_estimate();
		denominator = multiply_denominator_capped_at_numerator(
			denominator,
			u128::from(left_ndv.max(right_ndv)),
			numerator,
		)?;
	}
	ndv_containment_join_rows_from_denominator(left_rows, right_rows, denominator)
}

pub(super) fn analyze(edges: &[JoinOrderEdgeEvidence]) -> Option<StarOrderOutcome> {
	if edges.len() < 2 {
		return None;
	}
	if edges.iter().any(|edge| !edge.inner_equi) {
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::NonInnerEquiJoin,
			edge_count: edges.len(),
		}));
	}
	if edges.iter().any(|edge| !edge.probe_scan_linear) {
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::ProbeNotScanLinear,
			edge_count: edges.len(),
		}));
	}

	let Some(first_probe) = edges.first().and_then(|edge| edge.probe.as_ref()) else {
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::MissingRelationLabel,
			edge_count: edges.len(),
		}));
	};
	if edges
		.iter()
		.any(|edge| edge.probe.as_deref() != Some(first_probe.as_str()))
	{
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::MultipleProbeRelations,
			edge_count: edges.len(),
		}));
	}

	let mut dimensions = Vec::with_capacity(edges.len());
	let mut seen_dimensions = BTreeSet::new();
	for edge in edges {
		let Some(dimension) = &edge.dimension else {
			return Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason: StarOrderDeclineReason::MissingRelationLabel,
				edge_count: edges.len(),
			}));
		};
		if !seen_dimensions.insert(dimension.clone()) {
			return Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason: StarOrderDeclineReason::DuplicateDimension,
				edge_count: edges.len(),
			}));
		}
		let Some(estimated_rows) = edge.estimated_rows else {
			return Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason: StarOrderDeclineReason::MissingEstimate,
				edge_count: edges.len(),
			}));
		};
		dimensions.push(JoinOrderDimEvidence {
			dimension: dimension.clone(),
			declared_position: edge.declared_position,
			estimated_rows,
			estimate_source: edge.estimate_source,
		});
	}

	let declared_order = dimensions
		.iter()
		.map(|dim| dim.dimension.clone())
		.collect::<Vec<_>>();
	let mut preferred_dims = dimensions.clone();
	preferred_dims.sort_by(|left, right| {
		left.estimated_rows
			.cmp(&right.estimated_rows)
			.then(left.declared_position.cmp(&right.declared_position))
	});
	let cost_preferred_order = preferred_dims
		.iter()
		.map(|dim| dim.dimension.clone())
		.collect::<Vec<_>>();
	let declared_cost = order_cost(&dimensions);
	let preferred_cost = order_cost(&preferred_dims);
	let orders_agree = declared_order == cost_preferred_order;

	Some(StarOrderOutcome::Recognized(StarOrderEvidence {
		probe: first_probe.clone(),
		dimensions,
		declared_order,
		cost_preferred_order,
		declared_cost,
		preferred_cost,
		orders_agree,
	}))
}

fn order_cost(dimensions: &[JoinOrderDimEvidence]) -> u128 {
	let len = dimensions.len();
	dimensions
		.iter()
		.enumerate()
		.map(|(idx, dim)| {
			let downstream_weight =
				u128::try_from(len - idx).expect("join order cost weight overflow");
			downstream_weight * u128::from(dim.estimated_rows.get())
		})
		.sum()
}
