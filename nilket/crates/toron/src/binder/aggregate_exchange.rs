use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlanPartitioningProof {
	SinglePartition,
	Hash { key_sets: Vec<Vec<ColumnOrdinal>> },
}

pub(super) fn collapse_redundant_pre_aggregate_exchange(
	input: ScopedExecutionPlan,
	group_expr: &BoundExprProgram,
) -> ScopedExecutionPlan {
	let ScopedExecutionPlan {
		plan,
		scope,
		facts,
		join_order_sources,
	} = input;
	let plan = match plan {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} if pre_aggregate_exchange_is_redundant(
			input.as_ref(),
			partition_count,
			&key,
			group_expr,
		) =>
		{
			*input
		}
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		},
		other => other,
	};
	ScopedExecutionPlan {
		plan,
		scope,
		facts,
		join_order_sources,
	}
}

fn pre_aggregate_exchange_is_redundant(
	input: &ExecutionPlan,
	_partition_count: u32,
	key: &BoundExprProgram,
	group_expr: &BoundExprProgram,
) -> bool {
	let Some(exchange_key_columns) = root_column_ordinals(key) else {
		return false;
	};
	let Some(group_columns) = root_column_ordinals(group_expr) else {
		return false;
	};
	if !exchange_key_columns
		.iter()
		.all(|column| group_columns.contains(column))
	{
		return false;
	}

	match plan_partitioning_proof(input) {
		Some(PlanPartitioningProof::SinglePartition) => true,
		Some(PlanPartitioningProof::Hash { key_sets }) => {
			!key_sets.is_empty()
				&& key_sets
					.iter()
					.all(|set| set.iter().any(|column| group_columns.contains(column)))
		}
		None => false,
	}
}

fn plan_partitioning_proof(plan: &ExecutionPlan) -> Option<PlanPartitioningProof> {
	match plan {
		ExecutionPlan::Exchange {
			partition_count,
			key,
			..
		} if *partition_count == 1 => Some(PlanPartitioningProof::SinglePartition),
		ExecutionPlan::Exchange {
			partition_count,
			key,
			..
		} => root_column_ordinals(key).and_then(|keys| {
			if keys.is_empty() {
				// Keyless exchange proves co-location only as a single-partition gather. A keyless
				// multi-partition scatter proves nothing about where equal group keys land.
				(*partition_count == 1).then_some(PlanPartitioningProof::SinglePartition)
			} else {
				Some(PlanPartitioningProof::Hash {
					key_sets: keys.into_iter().map(|key| vec![key]).collect(),
				})
			}
		}),
		ExecutionPlan::Filter { input, .. } | ExecutionPlan::Take { input, .. } => {
			plan_partitioning_proof(input)
		}
		ExecutionPlan::Project { input, exprs, .. } => {
			remap_partitioning_through_project(plan_partitioning_proof(input)?, exprs)
		}
		ExecutionPlan::Join {
			kind,
			left,
			left_keys,
			right_keys,
			strategy,
			..
		} if *kind == JoinKind::Inner
			&& *strategy == JoinStrategy::HashPartitioned
			&& !left_keys.is_empty() =>
		{
			let left_width = left.schema().fields().len();
			let key_sets = left_keys
				.iter()
				.zip(right_keys)
				.map(|(left_key, right_key)| {
					vec![
						*left_key,
						ColumnOrdinal(
							u32::try_from(left_width).expect("left width fits u32") + right_key.0,
						),
					]
				})
				.collect();
			Some(PlanPartitioningProof::Hash { key_sets })
		}
		_ => None,
	}
}

fn remap_partitioning_through_project(
	partitioning: PlanPartitioningProof,
	exprs: &BoundExprProgram,
) -> Option<PlanPartitioningProof> {
	match partitioning {
		PlanPartitioningProof::SinglePartition => Some(PlanPartitioningProof::SinglePartition),
		PlanPartitioningProof::Hash { key_sets } => {
			let projected_columns = root_column_ordinals(exprs)?;
			let mut remapped_sets = Vec::with_capacity(key_sets.len());
			for set in key_sets {
				let mut remapped = Vec::new();
				for key in set {
					if let Some(position) =
						projected_columns.iter().position(|column| *column == key)
					{
						remapped.push(ColumnOrdinal(
							u32::try_from(position).expect("projection ordinal fits u32"),
						));
					}
				}
				if remapped.is_empty() {
					return None;
				}
				remapped_sets.push(remapped);
			}
			Some(PlanPartitioningProof::Hash {
				key_sets: remapped_sets,
			})
		}
	}
}

fn root_column_ordinals(program: &BoundExprProgram) -> Option<Vec<ColumnOrdinal>> {
	program
		.root_columns()?
		.into_iter()
		.map(|column| Some(column.ordinal))
		.collect()
}
