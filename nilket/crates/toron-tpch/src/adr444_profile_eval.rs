use serde::Serialize;

pub const ADR444_CHILDREN: usize = 5;
pub const ADR444_SHARE_SPREAD_LIMIT: f64 = 0.25;
pub const ADR444_ABSOLUTE_SPREAD_LIMIT: f64 = 0.05;
pub const ADR444_MEDIAN_SEPARATION: f64 = 1.15;
pub const ADR444_MECHANISM_ENVELOPE: f64 = 0.2386;

#[derive(Debug, Clone)]
pub struct Adr444ChildSample {
	pub shares: Vec<f64>,
	pub micros: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Adr444Gate {
	G1 { selected: usize },
	G2 { selected: usize },
	G3 { selected: usize },
	G4,
}

impl Adr444Gate {
	pub fn selected(&self) -> Option<usize> {
		match self {
			Self::G1 { selected } | Self::G2 { selected } | Self::G3 { selected } => {
				Some(*selected)
			}
			Self::G4 => None,
		}
	}
}

#[derive(Debug, Clone, Serialize)]
pub struct Adr444Evaluation {
	pub gate: Adr444Gate,
	pub median_shares: Vec<f64>,
	pub share_spreads: Vec<Option<f64>>,
	pub absolute_time_spreads: Vec<Option<f64>>,
	pub envelope_passed: bool,
}

pub fn evaluate_adr444_cohort(children: &[Adr444ChildSample]) -> Result<Adr444Evaluation, String> {
	if children.len() != ADR444_CHILDREN {
		return Err(format!(
			"ADR 444 evaluator requires {ADR444_CHILDREN} children, got {}",
			children.len()
		));
	}
	let width = children
		.first()
		.map(|child| child.shares.len())
		.unwrap_or(0);
	if width < 2 {
		return Err("ADR 444 evaluator requires at least two sub-leaves".to_string());
	}
	for (child_index, child) in children.iter().enumerate() {
		if child.shares.len() != width || child.micros.len() != width {
			return Err(format!(
				"ADR 444 child {child_index} width mismatch: shares={} micros={} expected={width}",
				child.shares.len(),
				child.micros.len()
			));
		}
		for (kind, values) in [("share", &child.shares), ("micros", &child.micros)] {
			for (value_index, value) in values.iter().enumerate() {
				if !value.is_finite() || *value < 0.0 {
					return Err(format!(
						"ADR 444 child {child_index} {kind} {value_index} is not finite and nonnegative"
					));
				}
			}
		}
	}

	let per_leaf = |source: fn(&Adr444ChildSample) -> &Vec<f64>, leaf: usize| {
		children
			.iter()
			.map(|child| source(child)[leaf])
			.collect::<Vec<_>>()
	};
	let median_shares = (0..width)
		.map(|leaf| median(&per_leaf(|child| &child.shares, leaf)))
		.collect::<Vec<_>>();
	let share_spreads = (0..width)
		.map(|leaf| spread(&per_leaf(|child| &child.shares, leaf)))
		.collect::<Vec<_>>();
	let absolute_time_spreads = (0..width)
		.map(|leaf| spread(&per_leaf(|child| &child.micros, leaf)))
		.collect::<Vec<_>>();

	let unique_largest = children
		.iter()
		.map(|child| unique_largest_index(&child.shares))
		.collect::<Vec<_>>();
	let g1_selected = unique_largest
		.first()
		.copied()
		.flatten()
		.filter(|selected| {
			unique_largest
				.iter()
				.all(|candidate| candidate == &Some(*selected))
		})
		.filter(|selected| stable(share_spreads[*selected], ADR444_SHARE_SPREAD_LIMIT));

	let mut ranked = (0..width).collect::<Vec<_>>();
	ranked.sort_by(|left, right| {
		median_shares[*right]
			.total_cmp(&median_shares[*left])
			.then_with(|| left.cmp(right))
	});
	let top = ranked[0];
	let runner_up = ranked[1];
	let top_median = median_shares[top];
	let runner_median = median_shares[runner_up];
	let separated = top_median > 0.0
		&& (runner_median == 0.0 || top_median >= ADR444_MEDIAN_SEPARATION * runner_median);

	let gate = if let Some(selected) = g1_selected {
		Adr444Gate::G1 { selected }
	} else if separated && stable(share_spreads[top], ADR444_SHARE_SPREAD_LIMIT) {
		Adr444Gate::G2 { selected: top }
	} else if !separated {
		let top_stable = stable(absolute_time_spreads[top], ADR444_ABSOLUTE_SPREAD_LIMIT);
		let runner_stable = stable(
			absolute_time_spreads[runner_up],
			ADR444_ABSOLUTE_SPREAD_LIMIT,
		);
		match (top_stable, runner_stable) {
			(true, false) if stable(share_spreads[top], ADR444_SHARE_SPREAD_LIMIT) => {
				Adr444Gate::G3 { selected: top }
			}
			(false, true) if stable(share_spreads[runner_up], ADR444_SHARE_SPREAD_LIMIT) => {
				Adr444Gate::G3 {
					selected: runner_up,
				}
			}
			_ => Adr444Gate::G4,
		}
	} else {
		Adr444Gate::G4
	};
	let envelope_passed = gate
		.selected()
		.is_some_and(|selected| median_shares[selected] > ADR444_MECHANISM_ENVELOPE);

	Ok(Adr444Evaluation {
		gate,
		median_shares,
		share_spreads,
		absolute_time_spreads,
		envelope_passed,
	})
}

fn unique_largest_index(values: &[f64]) -> Option<usize> {
	let mut largest = 0usize;
	for index in 1..values.len() {
		if values[index] > values[largest] {
			largest = index;
		}
	}
	(values
		.iter()
		.enumerate()
		.filter(|(_, value)| **value == values[largest])
		.count()
		== 1)
		.then_some(largest)
}

fn median(values: &[f64]) -> f64 {
	let mut values = values.to_vec();
	values.sort_by(f64::total_cmp);
	values[values.len() / 2]
}

fn spread(values: &[f64]) -> Option<f64> {
	let median = median(values);
	if median == 0.0 {
		return values.iter().all(|value| *value == 0.0).then_some(0.0);
	}
	let minimum = values.iter().copied().min_by(f64::total_cmp).unwrap_or(0.0);
	let maximum = values.iter().copied().max_by(f64::total_cmp).unwrap_or(0.0);
	Some((maximum - minimum) / median)
}

fn stable(spread: Option<f64>, limit: f64) -> bool {
	spread.is_some_and(|spread| spread <= limit)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Adr444VarianceVerdict {
	Owned { selected: usize },
	UnderInstrumented,
	MultiSource,
}

#[derive(Debug, Clone, Serialize)]
pub struct Adr444VarianceEvaluation {
	pub verdict: Adr444VarianceVerdict,
	pub parent_delta: i128,
	pub sub_leaf_deltas: Vec<i128>,
	pub gap_delta: i128,
	pub positive_total: u128,
}

pub fn evaluate_adr444_variance(
	max_parent: u64,
	min_parent: u64,
	max_sub_leaves: &[u64],
	min_sub_leaves: &[u64],
	max_gap: u64,
	min_gap: u64,
) -> Result<Adr444VarianceEvaluation, String> {
	if max_sub_leaves.len() != min_sub_leaves.len() || max_sub_leaves.is_empty() {
		return Err("ADR 444 variance vectors must have the same nonzero width".to_string());
	}
	let parent_delta = i128::from(max_parent) - i128::from(min_parent);
	if parent_delta < 0 {
		return Err(
			"ADR 444 maximum-write parent is smaller than minimum-write parent".to_string(),
		);
	}
	let sub_leaf_deltas = max_sub_leaves
		.iter()
		.zip(min_sub_leaves)
		.map(|(maximum, minimum)| i128::from(*maximum) - i128::from(*minimum))
		.collect::<Vec<_>>();
	let gap_delta = i128::from(max_gap) - i128::from(min_gap);
	let reconciled = sub_leaf_deltas
		.iter()
		.try_fold(gap_delta, |total, delta| total.checked_add(*delta))
		.ok_or_else(|| "ADR 444 signed variance reconciliation overflow".to_string())?;
	if reconciled != parent_delta {
		return Err(format!(
			"ADR 444 variance reconciliation failed: sub-leaves+gap={reconciled} parent={parent_delta}"
		));
	}
	let positive = |delta: i128| u128::try_from(delta.max(0)).unwrap_or(0);
	let positive_total = sub_leaf_deltas
		.iter()
		.try_fold(positive(gap_delta), |total, delta| {
			total.checked_add(positive(*delta))
		})
		.ok_or_else(|| "ADR 444 positive variance total overflow".to_string())?;
	let qualifies = |delta: i128| {
		positive_total > 0
			&& positive(delta).saturating_mul(100) >= positive_total.saturating_mul(60)
	};
	let qualifying = sub_leaf_deltas
		.iter()
		.enumerate()
		.filter_map(|(index, delta)| qualifies(*delta).then_some(index))
		.collect::<Vec<_>>();
	let verdict = if qualifies(gap_delta) {
		Adr444VarianceVerdict::UnderInstrumented
	} else if qualifying.len() == 1 {
		Adr444VarianceVerdict::Owned {
			selected: qualifying[0],
		}
	} else {
		Adr444VarianceVerdict::MultiSource
	};
	Ok(Adr444VarianceEvaluation {
		verdict,
		parent_delta,
		sub_leaf_deltas,
		gap_delta,
		positive_total,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn child(shares: [f64; 3], micros: [f64; 3]) -> Adr444ChildSample {
		Adr444ChildSample {
			shares: shares.to_vec(),
			micros: micros.to_vec(),
		}
	}

	fn repeated(shares: [f64; 3], micros: [f64; 3]) -> Vec<Adr444ChildSample> {
		(0..ADR444_CHILDREN)
			.map(|_| child(shares, micros))
			.collect()
	}

	#[test]
	fn evaluator_covers_g1_g2_g3_and_g4() {
		let g1 =
			evaluate_adr444_cohort(&repeated([0.4, 0.3, 0.1], [40.0, 30.0, 10.0])).expect("g1");
		assert_eq!(g1.gate, Adr444Gate::G1 { selected: 0 });

		let mut g2 = repeated([0.4, 0.3, 0.1], [40.0, 30.0, 10.0]);
		g2[0].shares = vec![0.35, 0.36, 0.1];
		assert_eq!(
			evaluate_adr444_cohort(&g2).expect("g2").gate,
			Adr444Gate::G2 { selected: 0 }
		);

		let mut g3 = repeated([0.4, 0.38, 0.1], [40.0, 38.0, 10.0]);
		g3[0].shares = vec![0.38, 0.4, 0.1];
		g3[0].micros[0] = 60.0;
		assert_eq!(
			evaluate_adr444_cohort(&g3).expect("g3").gate,
			Adr444Gate::G3 { selected: 1 }
		);

		let mut g4 = repeated([0.4, 0.38, 0.1], [40.0, 38.0, 10.0]);
		g4[0].shares = vec![0.38, 0.4, 0.1];
		assert_eq!(
			evaluate_adr444_cohort(&g4).expect("g4").gate,
			Adr444Gate::G4
		);
	}

	#[test]
	fn evaluator_defines_ties_zeroes_boundaries_and_invalid_inputs() {
		let ties = repeated([0.4, 0.4, 0.1], [40.0, 40.0, 10.0]);
		assert_eq!(
			evaluate_adr444_cohort(&ties).expect("ties").gate,
			Adr444Gate::G4
		);
		let zeros = repeated([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]);
		assert_eq!(
			evaluate_adr444_cohort(&zeros).expect("zeros").gate,
			Adr444Gate::G4
		);
		let mut median_zero = repeated([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]);
		median_zero[0].shares[0] = 0.1;
		assert_eq!(
			evaluate_adr444_cohort(&median_zero)
				.expect("median zero")
				.share_spreads[0],
			None
		);
		let mut runner_zero = repeated([0.4, 0.0, 0.0], [40.0, 0.0, 0.0]);
		runner_zero[0].shares = vec![0.35, 0.36, 0.0];
		assert_eq!(
			evaluate_adr444_cohort(&runner_zero)
				.expect("runner zero")
				.gate,
			Adr444Gate::G2 { selected: 0 }
		);
		let mut ratio_boundary = repeated([0.46, 0.4, 0.1], [46.0, 40.0, 10.0]);
		ratio_boundary[0].shares = vec![0.44, 0.45, 0.1];
		assert_eq!(
			evaluate_adr444_cohort(&ratio_boundary)
				.expect("ratio boundary")
				.gate,
			Adr444Gate::G2 { selected: 0 }
		);
		let mut share_boundary = repeated([0.4, 0.3, 0.1], [40.0, 30.0, 10.0]);
		share_boundary[0].shares[0] = 0.5;
		assert_eq!(
			evaluate_adr444_cohort(&share_boundary)
				.expect("share boundary")
				.gate,
			Adr444Gate::G1 { selected: 0 }
		);
		let mut unstable_g1 = repeated([0.4, 0.3, 0.1], [40.0, 30.0, 10.0]);
		unstable_g1[0].shares[0] = 0.6;
		assert_eq!(
			evaluate_adr444_cohort(&unstable_g1)
				.expect("unstable g1")
				.gate,
			Adr444Gate::G4
		);
		let mut time_boundary = repeated([0.4, 0.38, 0.1], [40.0, 38.0, 10.0]);
		time_boundary[0].shares = vec![0.38, 0.4, 0.1];
		time_boundary[0].micros[0] = 42.0;
		time_boundary[0].micros[1] = 45.0;
		assert_eq!(
			evaluate_adr444_cohort(&time_boundary)
				.expect("time boundary")
				.gate,
			Adr444Gate::G3 { selected: 0 }
		);
		let mut invalid = repeated([0.4, 0.3, 0.1], [40.0, 30.0, 10.0]);
		invalid[0].shares[0] = f64::NAN;
		assert!(evaluate_adr444_cohort(&invalid).is_err());
		invalid[0].shares[0] = -1.0;
		assert!(evaluate_adr444_cohort(&invalid).is_err());
	}

	#[test]
	fn variance_reconciles_signed_deltas_and_handles_owner_gap_and_multi_source() {
		let owned =
			evaluate_adr444_variance(200, 100, &[160, 20], &[80, 10], 20, 10).expect("owned");
		assert_eq!(owned.verdict, Adr444VarianceVerdict::Owned { selected: 0 });

		let gap = evaluate_adr444_variance(200, 100, &[80, 20], &[60, 10], 100, 30).expect("gap");
		assert_eq!(gap.verdict, Adr444VarianceVerdict::UnderInstrumented);

		let multi =
			evaluate_adr444_variance(200, 100, &[120, 90], &[70, 40], 10, 10).expect("multi");
		assert_eq!(multi.verdict, Adr444VarianceVerdict::MultiSource);

		let negative = evaluate_adr444_variance(200, 100, &[180, 10], &[60, 30], 10, 10)
			.expect("negative offset");
		assert_eq!(negative.parent_delta, 100);
		assert_eq!(negative.sub_leaf_deltas, vec![120, -20]);
	}

	#[test]
	#[ignore = "reads the persisted ADR 444 five-child cohort; run explicitly for the results receipt"]
	fn persisted_cohort_uses_the_locked_evaluator() {
		let cohort = std::env::var_os("TORON_ADR444_COHORT_DIR")
			.map(std::path::PathBuf::from)
			.expect("set TORON_ADR444_COHORT_DIR to the cohort root");
		let mut names = Vec::new();
		let mut children = Vec::new();
		let mut write_parents = Vec::new();
		let mut write_gaps = Vec::new();
		let mut write_sub_leaves = Vec::new();
		for child in 1..=ADR444_CHILDREN {
			let path = cohort
				.join(format!("child{child}"))
				.join("profile/grouped_chain_0000.json");
			let value: serde_json::Value = serde_json::from_slice(
				&std::fs::read(&path)
					.unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
			)
			.unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
			assert_eq!(value["schema_version"], 2, "{}", path.display());
			let sub_leaves = value["sub_leaves"]
				.as_array()
				.unwrap_or_else(|| panic!("sub_leaves in {}", path.display()));
			let child_names = sub_leaves
				.iter()
				.map(|leaf| leaf["name"].as_str().expect("sub-leaf name").to_string())
				.collect::<Vec<_>>();
			if names.is_empty() {
				names = child_names;
			} else {
				assert_eq!(names, child_names, "sub-leaf order drift in child {child}");
			}
			children.push(Adr444ChildSample {
				shares: sub_leaves
					.iter()
					.map(|leaf| leaf["share_of_root"].as_f64().expect("share_of_root"))
					.collect(),
				micros: sub_leaves
					.iter()
					.map(|leaf| leaf["micros"].as_u64().expect("sub-leaf micros") as f64)
					.collect(),
			});
			let write = value["parent_refinements"]
				.as_array()
				.expect("parent refinements")
				.iter()
				.find(|parent| parent["parent"] == "reservoir_partition_spill_write")
				.expect("write parent refinement");
			write_parents.push(
				write["measured_micros"]
					.as_u64()
					.expect("write parent micros"),
			);
			write_gaps.push(write["gap_micros"].as_u64().expect("write gap micros"));
			write_sub_leaves.push(
				sub_leaves
					.iter()
					.filter(|leaf| leaf["parent"] == "reservoir_partition_spill_write")
					.map(|leaf| leaf["micros"].as_u64().expect("write sub-leaf micros"))
					.collect::<Vec<_>>(),
			);
		}
		let evaluation = evaluate_adr444_cohort(&children).expect("evaluate persisted cohort");
		let max_write = write_parents
			.iter()
			.enumerate()
			.max_by_key(|(_, micros)| **micros)
			.map(|(index, _)| index)
			.expect("maximum write child");
		let min_write = write_parents
			.iter()
			.enumerate()
			.min_by_key(|(_, micros)| **micros)
			.map(|(index, _)| index)
			.expect("minimum write child");
		let variance = evaluate_adr444_variance(
			write_parents[max_write],
			write_parents[min_write],
			&write_sub_leaves[max_write],
			&write_sub_leaves[min_write],
			write_gaps[max_write],
			write_gaps[min_write],
		)
		.expect("evaluate persisted write variance");
		println!(
			"{}",
			serde_json::to_string_pretty(&serde_json::json!({
				"sub_leaf_names": names,
				"cohort": evaluation,
				"max_write_child": max_write + 1,
				"min_write_child": min_write + 1,
				"write_variance": variance,
			}))
			.expect("serialize evaluator receipt")
		);
	}
}
