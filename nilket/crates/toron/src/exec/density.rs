use super::*;

use crate::ir::{
	ScalarLiteralKey, ScalarLiteralKeyClassification, ScalarValue, scalar_literal_key,
};
use arrow::array::Array;
use std::collections::BTreeMap;
use wendelon::schema::optimizer_facts::{
	ARROW_STATS_TAG_FLOAT64, decode_float64_optimizer_fact_scalar,
	encode_float64_optimizer_fact_scalar,
};
use wendelon::storage::sketch::KllSketch;

const IQR_GAUSSIAN_SCALE: f64 = 1.349;
const P90_P10_GAUSSIAN_SCALE: f64 = 2.563;
const SILVERMAN_FACTOR: f64 = 0.9;
const INV_SQRT_TWO_PI: f64 = 0.398_942_280_401_432_7;
const QUANTILE_BYTES_PER_GROUP: u64 = 5_601 * 8 + 512;

#[derive(Debug)]
struct DensityGroup {
	label: ScalarValue,
	values: Vec<f64>,
	non_finite: bool,
}

impl Executor {
	pub(in crate::exec) fn execute_kernel_density(
		&self,
		input_batches: &[RecordBatch],
		group_program: &BoundExprProgram,
		value_program: &BoundExprProgram,
		points: usize,
		output_schema: &SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut groups = BTreeMap::<Option<ScalarLiteralKey>, DensityGroup>::new();
		let group_type =
			group_program
				.root_type(0)
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "KERNEL_DENSITY group expression has no output".to_string(),
				})?;
		let value_type =
			value_program
				.root_type(0)
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "KERNEL_DENSITY value expression has no output".to_string(),
				})?;

		for batch in input_batches {
			let group_array = self
				.evaluate_bound_program(group_program, batch)?
				.into_iter()
				.next()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "KERNEL_DENSITY group expression produced no array".to_string(),
				})?;
			let value_array = self
				.evaluate_bound_program(value_program, batch)?
				.into_iter()
				.next()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "KERNEL_DENSITY value expression produced no array".to_string(),
				})?;
			for row in 0..batch.num_rows() {
				let label = super::scalar_value_from_array(&group_array, row, group_type, "group")?;
				let key = match scalar_literal_key(&label) {
					ScalarLiteralKeyClassification::Key(key) => Some(key),
					ScalarLiteralKeyClassification::Null => None,
					ScalarLiteralKeyClassification::Unsupported => {
						return Err(ExecutionError::InvalidExpression {
							message: "KERNEL_DENSITY group value cannot be used as a key"
								.to_string(),
						});
					}
				};
				let entry = groups.entry(key).or_insert_with(|| DensityGroup {
					label: label.clone(),
					values: Vec::new(),
					non_finite: false,
				});
				if value_array.is_null(row) {
					continue;
				}
				let value = numeric_value_from_array(&value_array, row, value_type)?;
				if value.is_finite() {
					entry.values.push(value);
				} else {
					entry.non_finite = true;
				}
			}
		}

		let (mut extent_min, mut extent_max) = (f64::INFINITY, f64::NEG_INFINITY);
		for group in groups.values() {
			for &value in &group.values {
				extent_min = extent_min.min(value);
				extent_max = extent_max.max(value);
			}
		}
		if !extent_min.is_finite() || !extent_max.is_finite() || extent_min == extent_max {
			return Ok(vec![super::materialize_scalar_rows(output_schema, &[])?]);
		}
		let quantile_groups = groups
			.values()
			.filter(|group| group.values.len() >= 2)
			.count();
		if quantile_groups > self.context.quantile_state_allowance {
			return Err(super::aggregate::quantile_state_cap_error(quantile_groups));
		}
		let declared_bytes = (quantile_groups as u64).saturating_mul(QUANTILE_BYTES_PER_GROUP);
		if declared_bytes > self.context.task_budget.memory.get() {
			return Err(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "KERNEL_DENSITY declared accumulator footprint",
				requested: ByteCount::new(declared_bytes),
				available: self.context.task_budget.memory,
				detail: crate::error::BudgetExhaustedDetail::None,
			});
		}
		let quantile_reservation = super::aggregate::reserve_quantile_states(
			self.context.quantile_states_admitted.clone(),
			self.context.quantile_state_allowance,
			quantile_groups,
		)?;

		let step = (extent_max - extent_min) / (points - 1) as f64;
		let seed = self.context.quantile_execution_seed;
		let mut rows = Vec::new();
		for (key, group) in groups {
			if group.values.len() < 2 {
				continue;
			}
			let bandwidth = if group.non_finite {
				f64::NAN
			} else {
				let identity = *blake3::hash(&postcard::to_stdvec(&key).map_err(|error| {
					ExecutionError::Kernel {
						message: format!("KERNEL_DENSITY group identity failed: {error}"),
					}
				})?)
				.as_bytes();
				let (first_quartile, third_quartile, p10, p90) =
					approximate_spans(&group.values, seed, identity)?;
				let spread = third_quartile - first_quartile;
				let spread = if spread > 0.0 {
					spread / IQR_GAUSSIAN_SCALE
				} else {
					(p90 - p10) / P90_P10_GAUSSIAN_SCALE
				};
				if spread <= 0.0 {
					continue;
				}
				SILVERMAN_FACTOR * spread * (group.values.len() as f64).powf(-0.2)
			};

			for point in 0..points {
				let evaluation_value = extent_min + point as f64 * step;
				let density = if bandwidth.is_nan() {
					f64::NAN
				} else {
					let normalization = INV_SQRT_TWO_PI / (group.values.len() as f64 * bandwidth);
					group
						.values
						.iter()
						.map(|value| {
							let z = (evaluation_value - value) / bandwidth;
							normalization * (-0.5 * z * z).exp()
						})
						.sum()
				};
				rows.push(vec![
					group.label.clone(),
					ScalarValue::Float64(Some(evaluation_value)),
					ScalarValue::Float64(Some(density)),
				]);
			}
		}

		quantile_reservation.commit();
		Ok(vec![super::materialize_scalar_rows(output_schema, &rows)?])
	}
}

fn numeric_value_from_array(
	array: &ArrayRef,
	row: usize,
	data_type: &DataType,
) -> Result<f64, ExecutionError> {
	match data_type {
		DataType::Int32 => Ok(array
			.as_any()
			.downcast_ref::<Int32Array>()
			.ok_or_else(|| type_mismatch(data_type))?
			.value(row) as f64),
		DataType::Int64 => Ok(array
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| type_mismatch(data_type))?
			.value(row) as f64),
		DataType::Float32 => Ok(f64::from(
			array
				.as_any()
				.downcast_ref::<Float32Array>()
				.ok_or_else(|| type_mismatch(data_type))?
				.value(row),
		)),
		DataType::Float64 => Ok(array
			.as_any()
			.downcast_ref::<Float64Array>()
			.ok_or_else(|| type_mismatch(data_type))?
			.value(row)),
		_ => Err(type_mismatch(data_type)),
	}
}

fn type_mismatch(data_type: &DataType) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!("KERNEL_DENSITY value array does not match {data_type:?}"),
	}
}

fn approximate_spans(
	values: &[f64],
	seed: [u8; 32],
	identity: [u8; 32],
) -> Result<(f64, f64, f64, f64), ExecutionError> {
	let mut sketch = KllSketch::new_with_randomness(
		ARROW_STATS_TAG_FLOAT64,
		super::aggregate::QUANTILE_K,
		seed,
		identity,
	)
	.map_err(|error| ExecutionError::Kernel {
		message: format!("KERNEL_DENSITY quantile initialization failed: {error}"),
	})?;
	for &value in values {
		let encoded = encode_float64_optimizer_fact_scalar(value).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("KERNEL_DENSITY quantile encoding failed: {error}"),
			}
		})?;
		sketch.insert(encoded);
	}
	let query = |q| {
		let encoded = sketch
			.query_quantile(q)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("KERNEL_DENSITY quantile query failed: {error}"),
			})?
			.ok_or_else(|| ExecutionError::Kernel {
				message: "KERNEL_DENSITY quantile query returned no value".to_string(),
			})?;
		decode_float64_optimizer_fact_scalar(&encoded).ok_or_else(|| ExecutionError::Kernel {
			message: "KERNEL_DENSITY quantile decode failed".to_string(),
		})
	};
	Ok((query(0.25)?, query(0.75)?, query(0.10)?, query(0.90)?))
}
