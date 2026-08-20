use super::*;
use ahash::RandomState;

mod group_table;

use self::group_table::BoundGroupedAggregateEntry;
pub(in crate::exec) use self::group_table::BoundGroupedAggregateGroups;

const EMPTY_PRIMITIVE_INT64_BUCKET: u32 = u32::MAX;
const PRIMITIVE_INT64_INITIAL_CAPACITY: usize = 1024;
const PRIMITIVE_INT64_PRODUCER: &str = "primitive Int64 grouped aggregate resident state";
const BORROW_SINGLE_UTF8_GROUP_KEY_ENV: &str = "TORON_TPCH_BORROW_SINGLE_UTF8_GROUP_KEY";
const PRIMITIVE_INT64_COUNT_ENV: &str = "TORON_EXEC_PRIMITIVE_INT64_COUNT";
const GROUPED_FOLD_RECEIPT_PATH_ENV: &str = "TORON_GROUPED_FOLD_RECEIPT_PATH";
pub(in crate::exec) const MAX_QUANTILE_STATES_PER_EXECUTION: usize = 1_024;
pub(in crate::exec) const QUANTILE_K: wendelon::schema::KllK = wendelon::schema::KllK::new(800);

#[derive(Default)]
struct GroupedFoldReceipt {
	source_batches: u64,
	fold_batches: u64,
	fold_rows: u64,
	fold_micros: u64,
	// ADR 500 residual decomposition (all batch-level, gated on the receipt being Some):
	// the per-batch scan-stream pull (decode + async framework — subtract the parquet decode
	// receipt to isolate framework), the per-batch projection, and the one-time finalize. These
	// bracket exactly the loop body OUTSIDE fold_micros, so they reconcile the residual.
	stream_pull_micros: u64,
	projection_micros: u64,
	finalize_micros: u64,
}

const LOCAL_GROUPED_EXCHANGE_RECEIPT_PATH_ENV: &str = "TORON_LOCAL_GROUPED_EXCHANGE_RECEIPT_PATH";
const LOCAL_GROUPED_EXCHANGE_BYPASS_ENV: &str = "TORON_LOCAL_GROUPED_EXCHANGE_BYPASS";

const FOLD_GROWTH_CENSUS_ENV: &str = "TORON_FOLD_GROWTH_CENSUS";

/// Opt-in, and taken with the heavier counter profiler off: the census times every growth event
/// inside the hottest loop in the engine, so a run that is not asking for it should not pay for it.
fn fold_growth_census_enabled() -> bool {
	matches!(
		std::env::var(FOLD_GROWTH_CENSUS_ENV).ok().as_deref(),
		Some("1")
	)
}

fn local_grouped_exchange_bypass_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

fn local_grouped_exchange_bypass_enabled() -> bool {
	local_grouped_exchange_bypass_enabled_from_env(
		std::env::var(LOCAL_GROUPED_EXCHANGE_BYPASS_ENV)
			.ok()
			.as_deref(),
	)
}

fn persist_local_grouped_exchange_receipt(
	path: std::path::PathBuf,
	receipt: serde_json::Value,
) -> Result<(), ExecutionError> {
	std::fs::write(
		path,
		serde_json::to_vec(&receipt).map_err(|error| ExecutionError::Kernel {
			message: error.to_string(),
		})?,
	)
	.map_err(|error| ExecutionError::Kernel {
		message: format!("local grouped exchange receipt write failed: {error}"),
	})
}

#[derive(Default)]
pub(in crate::exec) struct LocalGroupedExchangeReceipt {
	pub source_batches: u64,
	pub source_rows: u64,
	pub partition_batches: u64,
	pub partition_rows: u64,
	pub key_eval_micros: u64,
	pub routing_index_micros: u64,
	pub take_micros: u64,
	pub index_bytes: u64,
	pub take_columns: u64,
	pub take_bytes: u64,
}

impl Executor {
	pub(in crate::exec) fn repartition_exchange_batch_profiled(
		&self,
		batch: &RecordBatch,
		partition_count: u32,
		key: &BoundExprProgram,
		mut receipt: Option<&mut LocalGroupedExchangeReceipt>,
	) -> Result<Vec<(usize, RecordBatch)>, ExecutionError> {
		let rows = batch.num_rows();
		if rows == 0 {
			return Ok(Vec::new());
		}
		if let Some(r) = receipt.as_deref_mut() {
			r.source_batches = r.source_batches.saturating_add(1);
			r.source_rows = r
				.source_rows
				.saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
		}
		let started = receipt.as_ref().map(|_| Instant::now());
		let keys = self.evaluate_bound_program(key, batch)?;
		if let (Some(r), Some(t)) = (receipt.as_deref_mut(), started) {
			r.key_eval_micros = r
				.key_eval_micros
				.saturating_add(u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX));
		}
		let started = receipt.as_ref().map(|_| Instant::now());
		let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
		let mut scratch = Vec::new();
		for row in 0..rows {
			let bucket = partition_bucket_with_scratch(&keys, row, partition_count, &mut scratch)?;
			builders[bucket].push(row as u32);
		}
		if let (Some(r), Some(t)) = (receipt.as_deref_mut(), started) {
			r.routing_index_micros = r
				.routing_index_micros
				.saturating_add(u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX));
			r.index_bytes = r
				.index_bytes
				.saturating_add(builders.iter().fold(0u64, |n, v| {
					n.saturating_add(
						u64::try_from(v.capacity().saturating_mul(std::mem::size_of::<u32>()))
							.unwrap_or(u64::MAX),
					)
				}));
		}
		let mut output = Vec::new();
		for (partition, indices) in builders.into_iter().enumerate() {
			if indices.is_empty() {
				continue;
			}
			let routed_rows = indices.len();
			let indices = arrow::array::UInt32Array::from(indices);
			let mut columns = Vec::new();
			for column in batch.columns() {
				let started = receipt.as_ref().map(|_| Instant::now());
				let taken = arrow::compute::take(column, &indices, None)?;
				if let (Some(r), Some(t)) = (receipt.as_deref_mut(), started) {
					r.take_micros = r
						.take_micros
						.saturating_add(u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX));
					r.take_columns = r.take_columns.saturating_add(1);
					r.take_bytes = r.take_bytes.saturating_add(
						u64::try_from(taken.get_buffer_memory_size()).unwrap_or(u64::MAX),
					);
				}
				columns.push(taken);
			}
			// A count-only input (all columns pruned) still carries rows; arrow requires an
			// explicit row count to build the column-less routed batch.
			let options =
				arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(routed_rows));
			let partitioned = RecordBatch::try_new_with_options(batch.schema(), columns, &options)?;
			if let Some(r) = receipt.as_deref_mut() {
				r.partition_batches = r.partition_batches.saturating_add(1);
				r.partition_rows = r
					.partition_rows
					.saturating_add(u64::try_from(partitioned.num_rows()).unwrap_or(u64::MAX));
			}
			output.push((partition, partitioned));
		}
		Ok(output)
	}
}

impl GroupedFoldReceipt {
	fn record_source_batch(&mut self) {
		self.source_batches = self.source_batches.saturating_add(1);
	}

	fn record_fold_batch(&mut self, rows: usize, elapsed: std::time::Duration) {
		self.fold_batches = self.fold_batches.saturating_add(1);
		self.fold_rows = self
			.fold_rows
			.saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
		self.fold_micros = self
			.fold_micros
			.saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
	}

	fn record_stream_pull(&mut self, elapsed: std::time::Duration) {
		self.stream_pull_micros = self
			.stream_pull_micros
			.saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
	}

	fn record_projection(&mut self, elapsed: std::time::Duration) {
		self.projection_micros = self
			.projection_micros
			.saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
	}

	fn record_finalize(&mut self, elapsed: std::time::Duration) {
		self.finalize_micros = self
			.finalize_micros
			.saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
	}
}

fn borrow_single_utf8_group_key_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

fn borrow_single_utf8_group_key_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| {
		borrow_single_utf8_group_key_enabled_from_env(
			std::env::var(BORROW_SINGLE_UTF8_GROUP_KEY_ENV)
				.ok()
				.as_deref(),
		)
	})
}

fn primitive_int64_count_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

fn primitive_int64_count_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| {
		primitive_int64_count_enabled_from_env(
			std::env::var(PRIMITIVE_INT64_COUNT_ENV).ok().as_deref(),
		)
	})
}

#[derive(Clone, Copy)]
struct StreamingScanGlobalInput<'a> {
	scan: &'a ExecutionPlan,
	scan_filter_predicate: Option<&'a BoundExprProgram>,
	project: Option<(&'a BoundExprProgram, &'a SchemaRef)>,
	project_filter_predicate: Option<&'a BoundExprProgram>,
}

impl StreamingScanGlobalInput<'_> {
	/// Whether `apply_streaming_scan_input_ops` has anything to do. ADR 508: a shape with no ops
	/// performs no projection, so the projection phase does not exist for it and must not be listed
	/// as a required stage — absence is not coverage.
	fn has_ops(&self) -> bool {
		self.scan_filter_predicate.is_some()
			|| self.project.is_some()
			|| self.project_filter_predicate.is_some()
	}

	fn output_schema(&self) -> SchemaRef {
		self.project
			.map(|(_, schema)| schema.clone())
			.unwrap_or_else(|| self.scan.schema())
	}
}

#[derive(Clone, Copy)]
struct StreamingScanGroupedInput<'a> {
	scan_input: StreamingScanGlobalInput<'a>,
	exchange: Option<(u32, &'a BoundExprProgram)>,
}

#[derive(Debug, Clone, Copy)]
enum PrimitiveInt64SumInput {
	Int64,
	Decimal128,
}

#[derive(Debug, Clone, Copy)]
struct PrimitiveInt64SumSpec {
	group_column: ColumnOrdinal,
	value_column: ColumnOrdinal,
	input: PrimitiveInt64SumInput,
}

#[derive(Debug, Clone, Copy)]
struct PrimitiveInt64CountSpec {
	group_column: ColumnOrdinal,
}

struct PrimitiveInt64Groups {
	buckets: Vec<u32>,
	keys: Vec<i64>,
	totals: Vec<i128>,
	entry_limit: usize,
	hasher: RandomState,
	input_rows: u64,
	peak_bytes: u64,
	#[cfg(test)]
	growth_count: u64,
}

impl PrimitiveInt64Groups {
	fn new() -> Self {
		Self {
			buckets: Vec::new(),
			keys: Vec::new(),
			totals: Vec::new(),
			entry_limit: 0,
			hasher: RandomState::new(),
			input_rows: 0,
			peak_bytes: 0,
			#[cfg(test)]
			growth_count: 0,
		}
	}

	fn insert(&mut self, key: i64, value: i128, cap: u64) -> Result<(), ExecutionError> {
		self.input_rows = self.input_rows.saturating_add(1);
		if let Some(entry) = self.lookup_entry(key)? {
			self.totals[entry] =
				self.totals[entry]
					.checked_add(value)
					.ok_or_else(|| ExecutionError::Kernel {
						message: "primitive Int64 grouped accumulator overflow".to_string(),
					})?;
			return Ok(());
		}
		if self.keys.len() == self.entry_limit {
			self.grow(cap)?;
		}
		let entry = u32::try_from(self.keys.len()).map_err(|_| ExecutionError::Kernel {
			message: "primitive Int64 grouped aggregate exceeds u32 entry range".to_string(),
		})?;
		if entry == EMPTY_PRIMITIVE_INT64_BUCKET {
			return Err(ExecutionError::Kernel {
				message: "primitive Int64 grouped aggregate exhausted the u32 entry domain"
					.to_string(),
			});
		}
		let bucket = Self::find_bucket_in(&self.hasher, &self.buckets, &self.keys, key)?;
		let key_capacity = self.keys.capacity();
		let total_capacity = self.totals.capacity();
		self.keys.push(key);
		self.totals.push(value);
		debug_assert_eq!(self.keys.capacity(), key_capacity);
		debug_assert_eq!(self.totals.capacity(), total_capacity);
		self.buckets[bucket] = entry;
		Ok(())
	}

	fn grow(&mut self, cap: u64) -> Result<(), ExecutionError> {
		let target = if self.entry_limit == 0 {
			PRIMITIVE_INT64_INITIAL_CAPACITY
		} else {
			self.entry_limit
				.checked_mul(2)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "primitive Int64 grouped aggregate capacity overflow".to_string(),
				})?
		};
		if target >= EMPTY_PRIMITIVE_INT64_BUCKET as usize {
			return Err(ExecutionError::Kernel {
				message: "primitive Int64 grouped aggregate exceeds u32 entry range".to_string(),
			});
		}
		let bucket_count = target
			.checked_mul(2)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "primitive Int64 grouped aggregate bucket capacity overflow".to_string(),
			})?;

		let old_bytes = self.accounted_bytes();
		let requested_key_bytes = primitive_capacity_bytes::<i64>(target);
		self.guard_peak(old_bytes.saturating_add(requested_key_bytes), cap)?;
		let mut keys = Vec::new();
		keys.try_reserve_exact(target)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("primitive Int64 grouped key allocation failed: {error}"),
			})?;
		let key_bytes = primitive_capacity_bytes::<i64>(keys.capacity());
		self.guard_peak(old_bytes.saturating_add(key_bytes), cap)?;
		keys.extend_from_slice(&self.keys);

		let requested_total_bytes = primitive_capacity_bytes::<i128>(target);
		self.guard_peak(
			old_bytes
				.saturating_add(key_bytes)
				.saturating_add(requested_total_bytes),
			cap,
		)?;
		let mut totals = Vec::new();
		totals
			.try_reserve_exact(target)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("primitive Int64 grouped state allocation failed: {error}"),
			})?;
		let total_bytes = primitive_capacity_bytes::<i128>(totals.capacity());
		self.guard_peak(
			old_bytes
				.saturating_add(key_bytes)
				.saturating_add(total_bytes),
			cap,
		)?;
		totals.extend_from_slice(&self.totals);

		let requested_bucket_bytes = primitive_capacity_bytes::<u32>(bucket_count);
		self.guard_peak(
			old_bytes
				.saturating_add(key_bytes)
				.saturating_add(total_bytes)
				.saturating_add(requested_bucket_bytes),
			cap,
		)?;
		let mut buckets = Vec::new();
		buckets
			.try_reserve_exact(bucket_count)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("primitive Int64 grouped bucket allocation failed: {error}"),
			})?;
		let bucket_bytes = primitive_capacity_bytes::<u32>(buckets.capacity());
		let peak = old_bytes
			.saturating_add(key_bytes)
			.saturating_add(total_bytes)
			.saturating_add(bucket_bytes);
		self.guard_peak(peak, cap)?;
		buckets.resize(bucket_count, EMPTY_PRIMITIVE_INT64_BUCKET);
		for (entry, key) in keys.iter().copied().enumerate() {
			let bucket = Self::find_bucket_in(&self.hasher, &buckets, &keys, key)?;
			buckets[bucket] = u32::try_from(entry).map_err(|_| ExecutionError::Kernel {
				message: "primitive Int64 grouped aggregate exceeds u32 entry range".to_string(),
			})?;
		}

		self.peak_bytes = self.peak_bytes.max(peak);
		self.keys = keys;
		self.totals = totals;
		self.buckets = buckets;
		self.entry_limit = target;
		#[cfg(test)]
		{
			self.growth_count = self.growth_count.saturating_add(1);
		}
		Ok(())
	}

	fn lookup_entry(&self, key: i64) -> Result<Option<usize>, ExecutionError> {
		if self.buckets.is_empty() {
			return Ok(None);
		}
		let bucket = Self::find_bucket_in(&self.hasher, &self.buckets, &self.keys, key)?;
		let entry = self.buckets[bucket];
		Ok((entry != EMPTY_PRIMITIVE_INT64_BUCKET).then_some(entry as usize))
	}

	fn find_bucket_in(
		hasher: &RandomState,
		buckets: &[u32],
		keys: &[i64],
		key: i64,
	) -> Result<usize, ExecutionError> {
		if buckets.is_empty() || !buckets.len().is_power_of_two() {
			return Err(ExecutionError::Kernel {
				message: "primitive Int64 grouped aggregate has invalid bucket capacity"
					.to_string(),
			});
		}
		let mut bucket = (hasher.hash_one(key) as usize) & (buckets.len() - 1);
		for _ in 0..buckets.len() {
			let entry = buckets[bucket];
			if entry == EMPTY_PRIMITIVE_INT64_BUCKET
				|| keys
					.get(entry as usize)
					.is_some_and(|candidate| *candidate == key)
			{
				return Ok(bucket);
			}
			bucket = (bucket + 1) & (buckets.len() - 1);
		}
		Err(ExecutionError::Kernel {
			message: "primitive Int64 grouped aggregate has no free bucket".to_string(),
		})
	}

	fn guard_peak(&self, requested: u64, cap: u64) -> Result<(), ExecutionError> {
		if requested > cap {
			return Err(super::join::budget_exhausted(
				BudgetDimension::Memory,
				PRIMITIVE_INT64_PRODUCER,
				requested,
				cap,
			));
		}
		Ok(())
	}

	fn accounted_bytes(&self) -> u64 {
		primitive_capacity_bytes::<u32>(self.buckets.capacity())
			.saturating_add(primitive_capacity_bytes::<i64>(self.keys.capacity()))
			.saturating_add(primitive_capacity_bytes::<i128>(self.totals.capacity()))
	}

	fn finish(
		self,
		output_schema: &SchemaRef,
		memory_cap: u64,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let retained_cap = super::join::join_spill_retained_footprint_memory_cap(memory_cap);
		self.finish_with_retained_cap(output_schema, memory_cap, retained_cap)
	}

	fn finish_with_retained_cap(
		self,
		output_schema: &SchemaRef,
		memory_cap: u64,
		retained_cap: u64,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let output_cap = super::join::join_output_materialization_memory_cap(memory_cap);
		let output_bytes = primitive_capacity_bytes::<i64>(self.keys.len()).saturating_add(
			match output_schema.field(1).data_type() {
				DataType::Int64 => primitive_capacity_bytes::<i64>(self.totals.len()),
				DataType::Decimal128(_, _) => primitive_capacity_bytes::<i128>(self.totals.len()),
				_ => u64::MAX,
			},
		);
		if output_bytes > output_cap {
			return Err(super::join::budget_exhausted(
				BudgetDimension::Memory,
				"primitive Int64 grouped aggregate output",
				output_bytes,
				output_cap,
			));
		}
		if matches!(output_schema.field(1).data_type(), DataType::Int64) {
			let conversion_peak = self
				.accounted_bytes()
				.saturating_add(primitive_capacity_bytes::<i64>(self.totals.len()));
			self.guard_peak(conversion_peak, retained_cap)?;
		}
		let PrimitiveInt64Groups {
			buckets,
			keys,
			totals,
			..
		} = self;
		drop(buckets);
		let group_column = Arc::new(Int64Array::from(keys)) as ArrayRef;
		let sum_column = match output_schema.field(1).data_type() {
			DataType::Int64 => {
				let values = totals
					.into_iter()
					.map(|value| {
						i64::try_from(value).map_err(|_| ExecutionError::Kernel {
							message: "integer SUM result does not fit Int64".to_string(),
						})
					})
					.collect::<Result<Vec<_>, _>>()?;
				Arc::new(Int64Array::from(values)) as ArrayRef
			}
			DataType::Decimal128(precision, scale) => Arc::new(
				Decimal128Array::from(totals)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef,
			data_type => {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"primitive Int64 grouped aggregate cannot produce {data_type:?}"
					),
				});
			}
		};
		Ok(vec![RecordBatch::try_new(
			output_schema.clone(),
			vec![group_column, sum_column],
		)?])
	}
}

fn primitive_capacity_bytes<T>(capacity: usize) -> u64 {
	u64::try_from(capacity.saturating_mul(std::mem::size_of::<T>())).unwrap_or(u64::MAX)
}

impl Executor {
	/// Time one batch's aggregate accumulation and add it to the task phase
	/// counter (ADR 554 Branch A).
	///
	/// One helper for all four fused sites so the boundary cannot drift between
	/// them. The contract is semantic: callers open it at the **first
	/// aggregate-accumulation instruction after input processing** — which is not
	/// always the line following `apply_streaming_scan_input_ops` — and close it
	/// at the end of the loop body. Scan acquisition is already metered, input
	/// processing is not accumulation, and post-loop finalization is excluded by
	/// decision.
	fn accumulate_aggregate_batch<T>(
		&self,
		body: impl FnOnce() -> Result<T, ExecutionError>,
	) -> Result<T, ExecutionError> {
		let started = Instant::now();
		let outcome = body();
		let elapsed = super::join::duration_micros(started);
		// Charged even when the body failed: the time was spent inside the task
		// wall either way, and dropping it on the error path would silently move
		// it back into the residual this counter exists to reduce.
		super::join::record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.aggregate_accumulation_micros = task
				.phases
				.aggregate_accumulation_micros
				.saturating_add(elapsed);
		});
		outcome
	}

	pub(in crate::exec) async fn try_streaming_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_scan_grouped_aggregate(
				input,
				group_expr,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("scan_grouped");
			return Ok(Some(output));
		}
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_left_outer_grouped_aggregate(
				input,
				group_expr,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("left_outer_grouped");
			return Ok(Some(output));
		}
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_projected_left_semi_grouped_aggregate(
				input,
				group_expr,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("projected_left_semi_grouped");
			return Ok(Some(output));
		}
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = Box::pin(self.try_streaming_inner_prefix_semi_anti_grouped_aggregate(
			input,
			group_expr,
			aggr_expr,
			output_schema,
			morsel,
		))
		.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("inner_prefix_semi_anti_grouped");
			return Ok(Some(output));
		}
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_reoriented_inner_chain_grouped_aggregate(
				input,
				group_expr,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("reoriented_inner_chain_grouped");
			return Ok(Some(output));
		}
		Ok(None)
	}

	pub(in crate::exec) async fn try_streaming_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_scan_global_aggregate(input, aggr_expr, output_schema, morsel)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("scan_global");
			return Ok(Some(output));
		}
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_correlated_scalar_left_outer_global_aggregate(
				input,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("correlated_scalar_left_outer_global");
			return Ok(Some(output));
		}
		let handler_span =
			crate::exec::node_attribution::open_handler_span(&self.context.task_telemetry);
		if let Some(output) = self
			.try_streaming_collapsing_global_aggregate(input, aggr_expr, output_schema, morsel)
			.await?
		{
			if let Some(handler_span) = handler_span {
				crate::exec::node_attribution::close_handler_span(
					handler_span,
					&self.context.task_telemetry,
				);
			}
			crate::exec::node_attribution::record_route("collapsing_global");
			return Ok(Some(output));
		}
		Ok(None)
	}

	pub(in crate::exec) async fn try_streaming_scan_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let Some(streaming_input) = decompose_scan_global_aggregate_input(input) else {
			return Ok(None);
		};
		let req = self
			.bound_scan_request(streaming_input.scan, morsel)
			.expect("streaming scan global aggregate input should build a ScanRequest");
		let row_limit = req.row_limit;
		let mut stream = self.open_scan(req).await?;
		let mut rows = 0usize;
		let mut state = init_global_aggregate_state_for_executor(&self.context, aggr_expr)?;

		while row_limit.map(|limit| rows < limit).unwrap_or(true) {
			let Some(batch) = stream.next().await else {
				break;
			};
			let batch = batch?;
			let mut batch = if let Some(limit) = row_limit {
				let remaining = limit - rows;
				if batch.num_rows() > remaining {
					batch.slice(0, remaining)
				} else {
					batch
				}
			} else {
				batch
			};
			rows = rows.saturating_add(batch.num_rows());

			batch = self.apply_streaming_scan_input_ops(&streaming_input, batch)?;

			self.accumulate_aggregate_batch(|| {
				self.fold_global_aggregate_state(&mut state, aggr_expr, &batch)
			})?;
		}

		finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some)
	}

	pub(in crate::exec) async fn try_streaming_scan_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let Some(streaming_input) =
			decompose_scan_grouped_aggregate_input(input, group_expr, aggr_expr)?
		else {
			return Ok(None);
		};
		if let Some(spec) =
			primitive_int64_sum_spec(&streaming_input, group_expr, aggr_expr, output_schema)
		{
			return self
				.execute_primitive_int64_sum_grouped_scan(
					&streaming_input,
					spec,
					output_schema,
					morsel,
				)
				.await
				.map(Some);
		}
		if let Some(spec) =
			primitive_int64_count_spec(&streaming_input, group_expr, aggr_expr, output_schema)
		{
			let output = self
				.execute_primitive_int64_count_grouped_scan(
					&streaming_input,
					spec,
					output_schema,
					morsel,
				)
				.await?;
			if let Some(path) = std::env::var_os(LOCAL_GROUPED_EXCHANGE_RECEIPT_PATH_ENV) {
				persist_local_grouped_exchange_receipt(
					std::path::PathBuf::from(path),
					serde_json::json!({
						"route": "primitive_int64_count",
						"admission": "primitive_int64_count_admitted",
						"exchange_bypassed": true,
						"exchange_micros": 0,
					}),
				)?;
			}
			return Ok(Some(output));
		}
		// ADR 508 step 2: the generic-loop lifecycle. Declared only here — *after* both primitive
		// admissions declined — because the primitive paths above have their own lifecycles and no
		// stage instrumentation. A cell that takes one will report missing attribution until it gets
		// a vocabulary of its own, which is the honest state rather than a borrowed one.
		//
		// The required set is applicability-driven, not a fixed list: a shape with no scan-input ops
		// never performs projection, and letting a phase that does not exist satisfy a required name
		// would turn absence into false coverage. The exchange variant is likewise distinct — with the
		// ADR 499 bypass off, repartition is real work that must be a named stage rather than residual.
		let projection_applies = streaming_input.scan_input.has_ops();
		let exchange_active =
			streaming_input.exchange.is_some() && !local_grouped_exchange_bypass_enabled();
		let (vocabulary_name, required_stages): (&'static str, &'static [&'static str]) =
			match (projection_applies, exchange_active) {
				(true, false) => (
					"scan_grouped_generic",
					&["stream_pull", "projection", "fold", "finalize"],
				),
				(false, false) => (
					"scan_grouped_generic_no_projection",
					&["stream_pull", "fold", "finalize"],
				),
				(true, true) => (
					"scan_grouped_generic_exchange",
					&[
						"stream_pull",
						"projection",
						"repartition",
						"fold",
						"finalize",
					],
				),
				(false, true) => (
					"scan_grouped_generic_exchange_no_projection",
					&["stream_pull", "repartition", "fold", "finalize"],
				),
			};
		// Predeclared from ADR 508 step-1 grounding with the pre-existing fold receipt: the largest
		// unexplained remainder observed was 202 ms (per-batch loop overhead, scan setup, receipt
		// write), so ~2.5x that. It is deliberately NOT derived from this instrument's own output.
		const SCAN_GROUPED_RESIDUAL_BOUNDARY_MICROS: u64 = 500_000;
		let vocabulary = node_attribution::declare_stage_vocabulary(
			vocabulary_name,
			node_attribution::SpillCounterKind::Flow,
			required_stages,
			SCAN_GROUPED_RESIDUAL_BOUNDARY_MICROS,
		);
		let telemetry = self.context.task_telemetry.clone();
		let req = self
			.bound_scan_request(streaming_input.scan_input.scan, morsel)
			.expect("streaming scan grouped aggregate input should build a ScanRequest");
		let row_limit = req.row_limit;
		let mut stream = self.open_scan(req).await?;
		let mut rows = 0usize;
		let mut groups = BoundGroupedAggregateGroups::new();
		// ADR 508 step 2: the sampled component estimator that splits the exact `row_lookup_update`
		// stage into key preparation, lookup/insertion and state update. Off unless asked for, and
		// keyed to this invocation so the sample set is reproducible for this plan.
		if fold_growth_census_enabled() && vocabulary.is_some() {
			groups.arm_growth_census();
		}
		let mut group_types = Vec::new();
		let mut folded_input = false;
		let receipt_path =
			std::env::var_os(GROUPED_FOLD_RECEIPT_PATH_ENV).map(std::path::PathBuf::from);
		let mut fold_receipt = receipt_path.as_ref().map(|_| GroupedFoldReceipt::default());
		let exchange_receipt_path =
			std::env::var_os(LOCAL_GROUPED_EXCHANGE_RECEIPT_PATH_ENV).map(std::path::PathBuf::from);
		// ADR 499: this executor owns one shared group table and serially folds every
		// input batch. The SQL planner's exchange therefore supplies no local parallelism;
		// retain it only for the explicit control arm or for distributed execution outside
		// this local streaming route.
		let bypass_exchange =
			streaming_input.exchange.is_some() && local_grouped_exchange_bypass_enabled();
		let fold_streaming_input = StreamingScanGroupedInput {
			scan_input: streaming_input.scan_input,
			exchange: if bypass_exchange {
				None
			} else {
				streaming_input.exchange
			},
		};
		let mut exchange_receipt = exchange_receipt_path
			.as_ref()
			.map(|_| LocalGroupedExchangeReceipt::default());

		while row_limit.map(|limit| rows < limit).unwrap_or(true) {
			// stream_pull brackets the awaited scan pull: it includes the parquet decode measured
			// separately by the scan ledger, so async framework = stream_pull − decode.
			let pull_started = fold_receipt.as_ref().map(|_| Instant::now());
			let pull_stage = vocabulary
				.as_ref()
				.and_then(|v| node_attribution::open_stage(v, "stream_pull", &telemetry));
			let next = stream.next().await;
			if let Some(stage) = pull_stage {
				node_attribution::close_stage(stage, &telemetry);
			}
			let Some(batch) = next else {
				break;
			};
			let batch = batch?;
			if let (Some(receipt), Some(started)) = (fold_receipt.as_mut(), pull_started) {
				receipt.record_stream_pull(started.elapsed());
			}
			let batch = if let Some(limit) = row_limit {
				let remaining = limit - rows;
				if batch.num_rows() > remaining {
					batch.slice(0, remaining)
				} else {
					batch
				}
			} else {
				batch
			};
			rows = rows.saturating_add(batch.num_rows());

			let projection_started = fold_receipt.as_ref().map(|_| Instant::now());
			let projection_stage = (projection_applies)
				.then(|| {
					vocabulary
						.as_ref()
						.and_then(|v| node_attribution::open_stage(v, "projection", &telemetry))
				})
				.flatten();
			let batch = self.apply_streaming_scan_input_ops(&streaming_input.scan_input, batch)?;
			if let Some(stage) = projection_stage {
				node_attribution::close_stage(stage, &telemetry);
			}
			if let Some(receipt) = fold_receipt.as_mut() {
				if let Some(started) = projection_started {
					receipt.record_projection(started.elapsed());
				}
				receipt.record_source_batch();
			}
			// Scope opens at the fold, not at the input-ops return: the two statements
			// above close the projection span and its receipt, which belong to input
			// ops. The nested `fold` stage span inside is a batch-local diagnostic and
			// deliberately overlaps this counter; only this counter feeds `accounted`.
			self.accumulate_aggregate_batch(|| {
				let fold_stage = vocabulary
					.as_ref()
					.and_then(|v| node_attribution::open_stage(v, "fold", &telemetry));
				let fold_result = self.fold_streaming_grouped_scan_batch(
					&fold_streaming_input,
					batch,
					group_expr,
					aggr_expr,
					&mut groups,
					&mut group_types,
					&mut folded_input,
					fold_receipt.as_mut(),
					exchange_receipt.as_mut(),
				);
				if let Some(stage) = fold_stage {
					node_attribution::close_stage(stage, &telemetry);
				}
				fold_result?;
				Ok(())
			})?;
		}

		if !folded_input {
			let batch = RecordBatch::new_empty(streaming_input.scan_input.output_schema());
			self.fold_bound_grouped_aggregate_batch(
				&mut groups,
				&mut group_types,
				group_expr,
				aggr_expr,
				&batch,
			)?;
		}

		let finalize_started = fold_receipt.as_ref().map(|_| Instant::now());
		let finalize_stage = vocabulary
			.as_ref()
			.and_then(|v| node_attribution::open_stage(v, "finalize", &telemetry));
		let output = self.finalize_bound_grouped_aggregate(
			&mut groups,
			&group_types,
			aggr_expr,
			output_schema,
		);
		if let Some(stage) = finalize_stage {
			node_attribution::close_stage(stage, &telemetry);
		}
		if let Some(handle) = vocabulary.as_ref()
			&& let Some(parent_micros) = node_attribution::accumulated_stage_micros(
				handle,
				"row_lookup_update",
				Some("fold"),
			) && let Some(census) = groups.growth_census_json(parent_micros)
		{
			node_attribution::record_growth_census(handle, census);
		}
		let output = output?;
		if let (Some(receipt), Some(started)) = (fold_receipt.as_mut(), finalize_started) {
			receipt.record_finalize(started.elapsed());
		}
		if let (Some(path), Some(receipt)) = (receipt_path, fold_receipt) {
			let receipt = serde_json::json!({
				"source_batches": receipt.source_batches,
				"fold_batches": receipt.fold_batches,
				"fold_rows": receipt.fold_rows,
				"fold_micros": receipt.fold_micros,
				"stream_pull_micros": receipt.stream_pull_micros,
				"projection_micros": receipt.projection_micros,
				"finalize_micros": receipt.finalize_micros,
			});
			std::fs::write(
				path,
				serde_json::to_vec(&receipt).map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
			)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("grouped fold receipt write failed: {error}"),
			})?;
		}
		if let (Some(path), Some(receipt)) = (exchange_receipt_path, exchange_receipt) {
			persist_local_grouped_exchange_receipt(
				path,
				serde_json::json!({
					"route": "generic_local_serial_shared_table",
				"admission": if bypass_exchange { "generic_local_serial_shared_table_bypassed" } else { "generic_local_exchange_bypass_disabled" },
				"exchange_bypassed": bypass_exchange,
					"source_batches": receipt.source_batches,
					"source_rows": receipt.source_rows,
					"partition_batches": receipt.partition_batches,
					"partition_rows": receipt.partition_rows,
					"key_eval_micros": receipt.key_eval_micros,
					"routing_index_micros": receipt.routing_index_micros,
					"take_micros": receipt.take_micros,
					"index_bytes": receipt.index_bytes,
					"take_columns": receipt.take_columns,
					"take_bytes": receipt.take_bytes,
				}),
			)?;
		}
		Ok(Some(output))
	}

	async fn execute_primitive_int64_sum_grouped_scan(
		&self,
		streaming_input: &StreamingScanGroupedInput<'_>,
		spec: PrimitiveInt64SumSpec,
		output_schema: &SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let req = self
			.bound_scan_request(streaming_input.scan_input.scan, morsel)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "primitive Int64 grouped SUM lost its scan request".to_string(),
			})?;
		let row_limit = req.row_limit;
		let mut stream = self.open_scan(req).await?;
		let cap = super::join::join_spill_retained_footprint_memory_cap(
			self.context.task_budget.memory.get(),
		);
		let started = Instant::now();
		let mut rows = 0usize;
		let mut groups = PrimitiveInt64Groups::new();
		while row_limit.map(|limit| rows < limit).unwrap_or(true) {
			let Some(batch) = stream.next().await else {
				break;
			};
			let batch = batch?;
			let batch = if let Some(limit) = row_limit {
				let remaining = limit - rows;
				if batch.num_rows() > remaining {
					batch.slice(0, remaining)
				} else {
					batch
				}
			} else {
				batch
			};
			rows = rows.saturating_add(batch.num_rows());
			let batch = self.apply_streaming_scan_input_ops(&streaming_input.scan_input, batch)?;
			self.accumulate_aggregate_batch(|| {
				let keys = batch
					.column(spec.group_column.0 as usize)
					.as_any()
					.downcast_ref::<Int64Array>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "primitive grouped SUM key must be Int64".to_string(),
					})?;
				if keys.null_count() != 0 {
					return Err(ExecutionError::InvalidExpression {
						message: "primitive grouped SUM received a null key".to_string(),
					});
				}
				match spec.input {
					PrimitiveInt64SumInput::Int64 => {
						let values = batch
							.column(spec.value_column.0 as usize)
							.as_any()
							.downcast_ref::<Int64Array>()
							.ok_or_else(|| ExecutionError::InvalidExpression {
								message: "primitive grouped SUM input must be Int64".to_string(),
							})?;
						if values.null_count() != 0 {
							return Err(ExecutionError::InvalidExpression {
								message: "primitive grouped SUM received a null input".to_string(),
							});
						}
						for row in 0..batch.num_rows() {
							groups.insert(keys.value(row), i128::from(values.value(row)), cap)?;
						}
					}
					PrimitiveInt64SumInput::Decimal128 => {
						let values = batch
							.column(spec.value_column.0 as usize)
							.as_any()
							.downcast_ref::<Decimal128Array>()
							.ok_or_else(|| ExecutionError::InvalidExpression {
								message: "primitive grouped SUM input must be Decimal128"
									.to_string(),
							})?;
						if values.null_count() != 0 {
							return Err(ExecutionError::InvalidExpression {
								message: "primitive grouped SUM received a null input".to_string(),
							});
						}
						for row in 0..batch.num_rows() {
							groups.insert(keys.value(row), values.value(row), cap)?;
						}
					}
				}
				Ok(())
			})?;
		}
		record_primitive_int64_sum_profile(&groups, cap, started.elapsed());
		groups.finish(output_schema, self.context.task_budget.memory.get())
	}

	async fn execute_primitive_int64_count_grouped_scan(
		&self,
		streaming_input: &StreamingScanGroupedInput<'_>,
		spec: PrimitiveInt64CountSpec,
		output_schema: &SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let req = self
			.bound_scan_request(streaming_input.scan_input.scan, morsel)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "primitive Int64 grouped COUNT lost its scan request".to_string(),
			})?;
		let row_limit = req.row_limit;
		let mut stream = self.open_scan(req).await?;
		let cap = self.context.task_budget.memory.get();
		let started = Instant::now();
		let mut rows = 0usize;
		let mut groups = PrimitiveInt64Groups::new();
		while row_limit.map(|limit| rows < limit).unwrap_or(true) {
			let Some(batch) = stream.next().await else {
				break;
			};
			let batch = batch?;
			let batch = if let Some(limit) = row_limit {
				let remaining = limit - rows;
				if batch.num_rows() > remaining {
					batch.slice(0, remaining)
				} else {
					batch
				}
			} else {
				batch
			};
			rows = rows.saturating_add(batch.num_rows());
			let batch = self.apply_streaming_scan_input_ops(&streaming_input.scan_input, batch)?;
			self.accumulate_aggregate_batch(|| {
				let keys = batch
					.column(spec.group_column.0 as usize)
					.as_any()
					.downcast_ref::<Int64Array>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "primitive grouped COUNT key must be Int64".to_string(),
					})?;
				if keys.null_count() != 0 {
					return Err(ExecutionError::InvalidExpression {
						message: "primitive grouped COUNT received a null key".to_string(),
					});
				}
				for row in 0..batch.num_rows() {
					groups.insert(keys.value(row), 1, cap)?;
				}
				Ok(())
			})?;
		}
		record_primitive_int64_sum_profile(&groups, cap, started.elapsed());
		groups.finish_with_retained_cap(output_schema, self.context.task_budget.memory.get(), cap)
	}

	fn apply_streaming_scan_input_ops(
		&self,
		streaming_input: &StreamingScanGlobalInput<'_>,
		mut batch: RecordBatch,
	) -> Result<RecordBatch, ExecutionError> {
		if let Some(predicate) = streaming_input.scan_filter_predicate {
			batch = self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
		}
		if let Some((exprs, schema)) = streaming_input.project {
			let cols = self.evaluate_bound_program(exprs, &batch)?;
			batch = RecordBatch::try_new(schema.clone(), cols)?;
		}
		if let Some(predicate) = streaming_input.project_filter_predicate {
			batch = self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
		}
		Ok(batch)
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_streaming_grouped_scan_batch(
		&self,
		streaming_input: &StreamingScanGroupedInput<'_>,
		batch: RecordBatch,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
		mut receipt: Option<&mut GroupedFoldReceipt>,
		mut exchange_receipt: Option<&mut LocalGroupedExchangeReceipt>,
	) -> Result<(), ExecutionError> {
		let mut fold_batch = |batch: RecordBatch| {
			let rows = batch.num_rows();
			let started = receipt.as_ref().map(|_| Instant::now());
			self.fold_bound_grouped_aggregate_batch(
				groups,
				group_types,
				group_expr,
				aggr_expr,
				&batch,
			)?;
			if let (Some(receipt), Some(started)) = (receipt.as_deref_mut(), started) {
				receipt.record_fold_batch(rows, started.elapsed());
			}
			Ok::<(), ExecutionError>(())
		};
		if batch.num_rows() == 0 {
			if !*folded_input {
				fold_batch(batch)?;
				*folded_input = true;
			}
			return Ok(());
		}
		if let Some((partition_count, exchange_key)) = streaming_input.exchange {
			for (_partition, partitioned) in self.repartition_exchange_batch_profiled(
				&batch,
				partition_count,
				exchange_key,
				exchange_receipt.as_deref_mut(),
			)? {
				fold_batch(partitioned)?;
				*folded_input = true;
			}
		} else {
			fold_batch(batch)?;
			*folded_input = true;
		}
		Ok(())
	}

	pub(in crate::exec) fn bound_global_aggregate(
		&self,
		input_batches: &[RecordBatch],
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut state = init_global_aggregate_state_for_executor(&self.context, aggr_expr)?;
		for batch in input_batches {
			self.fold_global_aggregate_state(&mut state, aggr_expr, batch)?;
		}
		finalize_global_aggregate_state(&state, aggr_expr, output_schema)
	}

	pub(in crate::exec) fn fold_global_aggregate_state(
		&self,
		state: &mut [AggState],
		aggr_expr: &[BoundAggregateCall],
		batch: &RecordBatch,
	) -> Result<(), ExecutionError> {
		for (state, call) in state.iter_mut().zip(aggr_expr.iter()) {
			match call.function {
				AggregateFunction::CountAll => {
					let AggState::Count(total) = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "COUNT ALL reached non-COUNT accumulator".to_string(),
						});
					};
					*total = total
						.checked_add(i64::try_from(batch.num_rows()).map_err(|_| {
							ExecutionError::Kernel {
								message: "COUNT ALL row count exceeds Int64".to_string(),
							}
						})?)
						.ok_or_else(|| ExecutionError::Kernel {
							message: "COUNT ALL overflow".to_string(),
						})?;
				}
				AggregateFunction::Count => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "COUNT arg")?;
					let AggState::Count(total) = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "COUNT reached non-COUNT accumulator".to_string(),
						});
					};
					for row in 0..batch.num_rows() {
						if !arr.is_null(row) {
							*total =
								total.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
									message: "COUNT overflow".to_string(),
								})?;
						}
					}
				}
				AggregateFunction::ScalarSubqueryValue => {
					let arr =
						self.evaluate_bound_single_root(&call.args, batch, "scalar subquery")?;
					let AggState::ScalarSubqueryValue { rows, value } = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "scalar subquery reached the wrong accumulator".to_string(),
						});
					};
					for row in 0..batch.num_rows() {
						*rows += 1;
						if *rows > 1 {
							return Err(ExecutionError::Kernel {
								message: "scalar subquery produced more than one row".to_string(),
							});
						}
						*value = Some(super::scalar_value_from_array(
							&arr,
							row,
							arr.data_type(),
							"scalar subquery value",
						)?);
					}
				}
				AggregateFunction::Sum => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "SUM arg")?;
					// ADR 475: resolve the array type once, not once per row.
					let reader = numeric_array_reader(&arr)?;
					for row in 0..batch.num_rows() {
						if let Some(value) = reader.value(row) {
							add_sum_value(state, value)?;
						}
					}
				}
				AggregateFunction::Avg => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "AVG arg")?;
					let reader = numeric_array_reader(&arr)?;
					for row in 0..batch.num_rows() {
						if let Some(value) = reader.value(row) {
							add_avg_value(state, value)?;
						}
					}
				}
				AggregateFunction::VarPop
				| AggregateFunction::VarSamp
				| AggregateFunction::StddevPop
				| AggregateFunction::StddevSamp => {
					let arr =
						self.evaluate_bound_single_root(&call.args, batch, "spread aggregate arg")?;
					let reader = numeric_array_reader(&arr)?;
					let AggState::Spread { count, mean, m2 } = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "spread aggregate reached non-spread accumulator".to_string(),
						});
					};
					for row in 0..batch.num_rows() {
						if let Some(value) = reader.value(row) {
							spread_accumulate(
								count,
								mean,
								m2,
								spread_input_value(value, arr.data_type()),
							);
						}
					}
				}
				AggregateFunction::Min => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "MIN arg")?;
					for row in 0..batch.num_rows() {
						update_min_state(state, &arr, row)?;
					}
				}
				AggregateFunction::Max => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "MAX arg")?;
					for row in 0..batch.num_rows() {
						update_max_state(state, &arr, row)?;
					}
				}
				AggregateFunction::ApproxQuantile(_) => {
					let arr =
						self.evaluate_bound_single_root(&call.args, batch, "APPROX_QUANTILE arg")?;
					for row in 0..batch.num_rows() {
						update_quantile_state(state, &arr, row)?;
					}
				}
				AggregateFunction::StringAgg(_) => {
					// The program's roots are [value, key…]; one evaluation
					// feeds both channels.
					let roots = self.evaluate_bound_program(&call.args, batch)?;
					let AggState::StringAgg(state) = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "STRING_AGG reached a non-string-agg accumulator".to_string(),
						});
					};
					let (value_array, key_arrays) =
						roots
							.split_first()
							.ok_or_else(|| ExecutionError::InvalidExpression {
								message: "STRING_AGG requires a value argument".to_string(),
							})?;
					for row in 0..batch.num_rows() {
						string_agg_accumulate_row(state, value_array, key_arrays, row)?;
					}
				}
				AggregateFunction::LinearRegress => {
					return Err(ExecutionError::NotImplemented {
						feature: "bound experimental aggregate execution",
					});
				}
			}
		}
		Ok(())
	}

	pub(in crate::exec) fn fold_global_decimal_sum_state_profiled(
		&self,
		state: &mut [AggState],
		aggr_expr: &[BoundAggregateCall],
		batch: &RecordBatch,
		profile: &super::join::GlobalFoldJoinFilterProfile,
	) -> Result<(), ExecutionError> {
		let [state] = state else {
			return Err(ExecutionError::InvalidExpression {
				message: "profiled global Decimal SUM expected one accumulator".to_string(),
			});
		};
		let [call] = aggr_expr else {
			return Err(ExecutionError::InvalidExpression {
				message: "profiled global Decimal SUM expected one aggregate call".to_string(),
			});
		};
		if call.function != AggregateFunction::Sum {
			return Err(ExecutionError::InvalidExpression {
				message: "profiled global Decimal SUM reached a non-SUM call".to_string(),
			});
		}

		let expression_timer =
			profile.begin_leaf(super::join::GlobalFoldProfileLeaf::DecimalExpr)?;
		let evaluated = self.evaluate_bound_single_root(&call.args, batch, "SUM arg");
		expression_timer.finish()?;
		let arr = evaluated?;
		profile.record_sum_argument_rows(batch.num_rows())?;

		let fold_timer = profile.begin_leaf(super::join::GlobalFoldProfileLeaf::NumericFold)?;
		let folded = (|| {
			let reader = numeric_array_reader(&arr)?;
			for row in 0..batch.num_rows() {
				if let Some(value) = reader.value(row) {
					add_sum_value(state, value)?;
				}
			}
			Ok::<(), ExecutionError>(())
		})();
		fold_timer.finish()?;
		folded?;
		profile.record_numeric_fold_rows(batch.num_rows())
	}
}

pub(in crate::exec) fn global_fold_join_filter_profile_eligible(
	join_filter_predicate: Option<&BoundExprProgram>,
	project: Option<(&BoundExprProgram, &SchemaRef)>,
	project_filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
) -> bool {
	let Some(_) = join_filter_predicate else {
		return false;
	};
	if project.is_some() || project_filter_predicate.is_some() {
		return false;
	}
	let [call] = aggr_expr else {
		return false;
	};
	call.function == AggregateFunction::Sum
		&& matches!(call.args.root_type(0), Some(DataType::Decimal128(_, _)))
}

fn primitive_int64_sum_spec(
	input: &StreamingScanGroupedInput<'_>,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	output_schema: &SchemaRef,
) -> Option<PrimitiveInt64SumSpec> {
	if input.scan_input.scan_filter_predicate.is_some()
		|| input.scan_input.project_filter_predicate.is_some()
	{
		return None;
	}
	let (_, exchange_key) = input.exchange?;
	if exchange_key != group_expr || aggr_expr.len() != 1 {
		return None;
	}
	let call = &aggr_expr[0];
	if call.function != AggregateFunction::Sum {
		return None;
	}
	let group = group_expr.single_column_root()?;
	let value = call.args.single_column_root()?;
	let input_schema = input.scan_input.output_schema();
	let group_field = input_schema.fields().get(group.ordinal.0 as usize)?;
	let value_field = input_schema.fields().get(value.ordinal.0 as usize)?;
	if group.data_type != DataType::Int64
		|| group.nullable
		|| group_field.data_type() != &DataType::Int64
		|| group_field.is_nullable()
		|| group.data_type != *group_field.data_type()
		|| value.nullable
		|| value.data_type != *value_field.data_type()
		|| value.nullable != value_field.is_nullable()
		|| output_schema.fields().len() != 2
		|| output_schema.field(0).data_type() != &DataType::Int64
		|| output_schema.field(0).is_nullable()
	{
		return None;
	}
	let input = match (&value.data_type, output_schema.field(1).data_type()) {
		(DataType::Int64, DataType::Int64) => PrimitiveInt64SumInput::Int64,
		(DataType::Decimal128(_, input_scale), DataType::Decimal128(_, output_scale))
			if input_scale == output_scale =>
		{
			PrimitiveInt64SumInput::Decimal128
		}
		_ => return None,
	};
	Some(PrimitiveInt64SumSpec {
		group_column: group.ordinal,
		value_column: value.ordinal,
		input,
	})
}

fn primitive_int64_count_spec(
	input: &StreamingScanGroupedInput<'_>,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	output_schema: &SchemaRef,
) -> Option<PrimitiveInt64CountSpec> {
	if !primitive_int64_count_enabled()
		|| input.scan_input.scan_filter_predicate.is_some()
		|| input.scan_input.project.is_some()
		|| input.scan_input.project_filter_predicate.is_some()
	{
		return None;
	}
	let (_, exchange_key) = input.exchange?;
	if exchange_key != group_expr {
		return None;
	}
	let group = group_expr.single_column_root()?;
	let input_schema = input.scan_input.output_schema();
	let group_field = input_schema.fields().get(group.ordinal.0 as usize)?;
	let [call] = aggr_expr else {
		return None;
	};
	let count_rows = matches!(call.function, AggregateFunction::CountAll)
		|| (matches!(call.function, AggregateFunction::Count)
			&& matches!(
				call.args
					.single_root()
					.and_then(|root| call.args.node(root)),
				Some(BoundExprNode::Literal(_))
			) && call.args.root_nullable(0) == Some(false));
	if !count_rows
		|| group.data_type != DataType::Int64
		|| group.nullable
		|| group_field.data_type() != &DataType::Int64
		|| group_field.is_nullable()
		|| output_schema.fields().len() != 2
		|| output_schema.field(0).data_type() != &DataType::Int64
		|| output_schema.field(0).is_nullable()
		|| output_schema.field(1).data_type() != &DataType::Int64
		|| output_schema.field(1).is_nullable()
	{
		return None;
	}
	Some(PrimitiveInt64CountSpec {
		group_column: group.ordinal,
	})
}

fn record_primitive_int64_sum_profile(
	groups: &PrimitiveInt64Groups,
	cap: u64,
	elapsed: std::time::Duration,
) {
	let Some(path) = std::env::var_os("TORON_PRIMITIVE_GROUPED_SUM_PROFILE_PATH") else {
		return;
	};
	let path = std::path::PathBuf::from(path);
	let temporary = path.with_extension("tmp");
	let elapsed_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
	let contents = format!(
		"input_rows={}\ngroups={}\nentry_capacity={}\nbucket_capacity={}\naccounted_bytes={}\npeak_bytes={}\ncap={}\nelapsed_micros={}\n",
		groups.input_rows,
		groups.keys.len(),
		groups.keys.capacity(),
		groups.buckets.capacity(),
		groups.accounted_bytes(),
		groups.peak_bytes,
		cap,
		elapsed_micros,
	);
	if std::fs::write(&temporary, contents).is_ok() {
		let _ = std::fs::rename(temporary, path);
	}
}

/// Whether this plan fuses its base-relation read into a global aggregate.
///
/// ADR 551: the route a control asserts on directly, rather than inferring it
/// from SQL text or output shape.
#[cfg(test)]
pub(in crate::exec) fn fuses_scan_into_global_aggregate(plan: &ExecutionPlan) -> bool {
	// The decomposer answers a question about an aggregate's *input* — a bare
	// `Scan` is a fusable input, not a fused plan — so the aggregate itself has to
	// be matched first.
	let ExecutionPlan::Aggregate {
		input, group_expr, ..
	} = plan
	else {
		return false;
	};
	group_expr.root_count() == 0 && decompose_scan_global_aggregate_input(input.as_ref()).is_some()
}

fn decompose_scan_global_aggregate_input(
	input: &ExecutionPlan,
) -> Option<StreamingScanGlobalInput<'_>> {
	match input {
		ExecutionPlan::Scan { .. } => Some(StreamingScanGlobalInput {
			scan: input,
			scan_filter_predicate: None,
			project: None,
			project_filter_predicate: None,
		}),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => match input.as_ref() {
			ExecutionPlan::Scan { .. } => Some(StreamingScanGlobalInput {
				scan: input.as_ref(),
				scan_filter_predicate: streaming_filter_predicate(predicate, *disposition),
				project: None,
				project_filter_predicate: None,
			}),
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				let (scan, scan_filter_predicate) =
					decompose_scan_or_filtered_scan(input.as_ref())?;
				Some(StreamingScanGlobalInput {
					scan,
					scan_filter_predicate,
					project: Some((exprs, output_schema)),
					project_filter_predicate: streaming_filter_predicate(predicate, *disposition),
				})
			}
			_ => None,
		},
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => {
			let (scan, scan_filter_predicate) = decompose_scan_or_filtered_scan(input.as_ref())?;
			Some(StreamingScanGlobalInput {
				scan,
				scan_filter_predicate,
				project: Some((exprs, output_schema)),
				project_filter_predicate: None,
			})
		}
		_ => None,
	}
}

fn decompose_scan_grouped_aggregate_input<'a>(
	input: &'a ExecutionPlan,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
) -> Result<Option<StreamingScanGroupedInput<'a>>, ExecutionError> {
	match input {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			if *partition_count == 0 {
				return Err(ExecutionError::InvalidExpression {
					message: "exchange partition_count must be greater than zero".to_string(),
				});
			}
			if key != group_expr && !super::join::grouped_aggregate_is_order_insensitive(aggr_expr)
			{
				return Ok(None);
			}
			let Some(scan_input) = decompose_scan_global_aggregate_input(input.as_ref()) else {
				return Ok(None);
			};
			Ok(Some(StreamingScanGroupedInput {
				scan_input,
				exchange: Some((*partition_count, key)),
			}))
		}
		_ => Ok(
			decompose_scan_global_aggregate_input(input).map(|scan_input| {
				StreamingScanGroupedInput {
					scan_input,
					exchange: None,
				}
			}),
		),
	}
}

fn decompose_scan_or_filtered_scan(
	input: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Option<&BoundExprProgram>)> {
	match input {
		ExecutionPlan::Scan { .. } => Some((input, None)),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => match input.as_ref() {
			ExecutionPlan::Scan { .. } => Some((
				input.as_ref(),
				streaming_filter_predicate(predicate, *disposition),
			)),
			_ => None,
		},
		_ => None,
	}
}

fn streaming_filter_predicate(
	predicate: &BoundExprProgram,
	disposition: FilterDisposition,
) -> Option<&BoundExprProgram> {
	match disposition {
		FilterDisposition::FullyPushed => None,
		FilterDisposition::EvaluateInToron => Some(predicate),
	}
}

pub(in crate::exec) fn init_global_aggregate_state(
	context: &ExecutionContext,
	aggr_expr: &[BoundAggregateCall],
) -> Result<Vec<AggState>, ExecutionError> {
	init_global_aggregate_state_for_executor(context, aggr_expr)
}

fn init_global_aggregate_state_for_executor(
	context: &ExecutionContext,
	aggr_expr: &[BoundAggregateCall],
) -> Result<Vec<AggState>, ExecutionError> {
	let (populations, declared_bytes) = quantile_populations_and_declared_bytes(aggr_expr)?;
	if populations > context.quantile_state_allowance {
		return Err(quantile_state_cap_error(populations));
	}
	if declared_bytes > context.task_budget.memory.get() {
		return Err(ExecutionError::BudgetExhausted {
			dimension: BudgetDimension::Memory,
			producer: "APPROX_QUANTILE declared accumulator footprint",
			requested: crate::distributed::budget::ByteCount::new(declared_bytes),
			available: context.task_budget.memory,
			detail: crate::error::BudgetExhaustedDetail::None,
		});
	}
	let reservation = reserve_quantile_states(
		context.quantile_states_admitted.clone(),
		context.quantile_state_allowance,
		populations,
	)?;
	let states = init_bound_states_with_seed(aggr_expr, context.quantile_execution_seed, &[])?;
	reservation.commit();
	Ok(states)
}

pub(in crate::exec) fn finalize_global_aggregate_state(
	state: &[AggState],
	aggr_expr: &[BoundAggregateCall],
	output_schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let mut results_cols = Vec::new();
	for (state, call) in state.iter().zip(aggr_expr.iter()) {
		match call.function {
			AggregateFunction::CountAll => {
				let AggState::Count(total) = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "COUNT ALL reached non-COUNT accumulator".to_string(),
					});
				};
				results_cols.push(Arc::new(Int64Array::from(vec![*total])) as ArrayRef);
			}
			AggregateFunction::Count => {
				let AggState::Count(total) = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "COUNT reached non-COUNT accumulator".to_string(),
					});
				};
				results_cols.push(Arc::new(Int64Array::from(vec![*total])) as ArrayRef);
			}
			AggregateFunction::Sum => {
				let output_type = output_schema.field(results_cols.len()).data_type().clone();
				results_cols.push(sum_state_array(&state, &output_type)?);
			}
			AggregateFunction::Avg => {
				results_cols.push(avg_state_array(
					&state,
					output_schema.field(results_cols.len()).data_type(),
				)?);
			}
			AggregateFunction::ScalarSubqueryValue => {
				let AggState::ScalarSubqueryValue { rows, value } = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "scalar subquery reached the wrong accumulator".to_string(),
					});
				};
				let output_type = output_schema.field(results_cols.len()).data_type().clone();
				results_cols.push(match value {
					Some(value) if *rows == 1 => super::expr::literal_array(value, 1)?,
					_ => arrow::array::new_null_array(&output_type, 1),
				});
			}
			AggregateFunction::VarPop
			| AggregateFunction::VarSamp
			| AggregateFunction::StddevPop
			| AggregateFunction::StddevSamp => {
				let AggState::Spread { count, m2, .. } = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "spread aggregate reached non-spread accumulator".to_string(),
					});
				};
				results_cols.push(Arc::new(Float64Array::from(vec![spread_finalize(
					&call.function,
					*count,
					*m2,
				)])) as ArrayRef);
			}
			AggregateFunction::Min => {
				results_cols.push(min_max_state_array(
					&state,
					output_schema.field(results_cols.len()).data_type(),
				)?);
			}
			AggregateFunction::Max => {
				results_cols.push(min_max_state_array(
					&state,
					output_schema.field(results_cols.len()).data_type(),
				)?);
			}
			AggregateFunction::ApproxQuantile(_) => {
				results_cols.push(Arc::new(Float64Array::from(vec![finalize_quantile_state(
					state,
				)?])) as ArrayRef);
			}
			AggregateFunction::StringAgg(ref call_payload) => {
				let AggState::StringAgg(state) = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "STRING_AGG reached a non-string-agg accumulator".to_string(),
					});
				};
				let output_type = output_schema.field(results_cols.len()).data_type().clone();
				results_cols.push(string_agg_column(
					vec![string_agg_finalize(state, call_payload.as_ref())],
					&output_type,
				)?);
			}
			AggregateFunction::LinearRegress => {
				return Err(ExecutionError::NotImplemented {
					feature: "bound experimental aggregate execution",
				});
			}
		}
	}

	Ok(vec![RecordBatch::try_new(
		output_schema.clone(),
		results_cols,
	)?])
}

impl Executor {
	#[cfg(test)]
	pub(in crate::exec) fn cast_to_f64(
		&self,
		arr: &ArrayRef,
	) -> Result<Float64Array, ExecutionError> {
		if let Some(f64_arr) = arr.as_any().downcast_ref::<Float64Array>() {
			return Ok(f64_arr.clone());
		}
		if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
			let values: Vec<Option<f64>> = int_arr.iter().map(|v| v.map(|v| v as f64)).collect();
			return Ok(Float64Array::from(values));
		}
		Err(ExecutionError::InvalidExpression {
			message: format!("cannot cast to f64: {:?}", arr.data_type()),
		})
	}

	#[cfg(test)]
	pub(in crate::exec) fn grouped_aggregate(
		&self,
		input_batches: &[RecordBatch],
		group_expr: &[Expr],
		aggr_expr: &[(AggregateFunction, Vec<Expr>)],
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut groups = BoundGroupedAggregateGroups::new();
		let mut group_types = Vec::new();
		let mut aggregate_output_types = vec![None; aggr_expr.len()];

		for batch in input_batches {
			let group_arrays = group_expr
				.iter()
				.map(|expr| self.evaluate_expr(expr, batch))
				.collect::<Result<Vec<_>, _>>()?;
			if group_types.is_empty() {
				group_types = group_arrays
					.iter()
					.map(|array| array.data_type().clone())
					.collect();
				groups.configure_key_storage(&group_types);
			}

			let aggr_arrays = aggr_expr
				.iter()
				.map(|(function, args)| match function {
					AggregateFunction::CountAll => Ok(None),
					AggregateFunction::Count
					| AggregateFunction::Sum
					| AggregateFunction::Min
					| AggregateFunction::Max
					| AggregateFunction::Avg => Ok(Some(self.evaluate_expr(&args[0], batch)?)),
					// ADR 560: the spread family binds through the bound path only.
					AggregateFunction::VarPop
					| AggregateFunction::VarSamp
					| AggregateFunction::StddevPop
					| AggregateFunction::StddevSamp
					| AggregateFunction::LinearRegress
					| AggregateFunction::StringAgg(_)
					| AggregateFunction::ScalarSubqueryValue
					| AggregateFunction::ApproxQuantile(_) => Err(ExecutionError::NotImplemented {
						feature: "grouped experimental aggregate",
					}),
				})
				.collect::<Result<Vec<_>, _>>()?;
			for (agg_idx, ((function, _), input)) in
				aggr_expr.iter().zip(aggr_arrays.iter()).enumerate()
			{
				if aggregate_output_types[agg_idx].is_none()
					&& let Some(array) = input
				{
					aggregate_output_types[agg_idx] =
						Some(legacy_aggregate_output_type(function, array.data_type()));
				}
			}

			// ADR 475: resolve each numeric input's concrete type once per batch.
			// Non-numeric inputs (Count/Min/Max over other types) stay `None` and
			// keep the original per-row path, error text included.
			let readers = aggr_arrays
				.iter()
				.map(|input| {
					input
						.as_ref()
						.and_then(|array| numeric_array_reader(array).ok())
				})
				.collect::<Vec<_>>();
			// ADR 476: one reusable key buffer for the whole batch — a probe that
			// hits an existing group now allocates nothing.
			// ADR 477: one reader per group column per batch.
			let group_readers = group_arrays
				.iter()
				.map(group_array_reader)
				.collect::<Result<Vec<_>, _>>()?;
			let borrow_single_utf8_group_key = borrow_single_utf8_group_key_enabled();
			let mut key = Vec::with_capacity(group_arrays.len());
			for row in 0..batch.num_rows() {
				let states = if borrow_single_utf8_group_key
					&& let [GroupArrayReader::Utf8(array)] = group_readers.as_slice()
				{
					groups.entry_or_init_single_utf8(
						(!array.is_null(row)).then(|| array.value(row)),
						aggr_expr.len(),
						|| init_states(aggr_expr, &aggr_arrays),
					)?
				} else {
					key.clear();
					for reader in group_readers.iter() {
						key.push(reader.value(row));
					}
					groups.entry_or_init(&key, aggr_expr.len(), || {
						init_states(aggr_expr, &aggr_arrays)
					})?
				};
				for ((state, input), reader) in states
					.iter_mut()
					.zip(aggr_arrays.iter())
					.zip(readers.iter())
				{
					match input {
						None => {
							if let AggState::Count(total) = state {
								*total += 1;
							}
						}
						Some(array) => match state {
							AggState::Count(total) => {
								if !array.is_null(row) {
									*total += 1;
								}
							}
							AggState::ScalarSubqueryValue { .. } => {
								return Err(ExecutionError::NotImplemented {
									feature: "unbound scalar-subquery aggregate",
								});
							}
							AggState::SumInt { .. } | AggState::SumFloat { .. } => {
								let value = match reader {
									Some(reader) => reader.value(row),
									None => numeric_value(array, row)?,
								};
								if let Some(value) = value {
									add_sum_value(state, value)?;
								}
							}
							AggState::AvgInt { .. }
							| AggState::AvgFloat { .. }
							| AggState::AvgDecimal { .. } => {
								let value = match reader {
									Some(reader) => reader.value(row),
									None => numeric_value(array, row)?,
								};
								if let Some(value) = value {
									add_avg_value(state, value)?;
								}
							}
							AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. }
							| AggState::MinDecimal128 { .. } => {
								update_min_state(state, array, row)?;
							}
							AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::MaxDecimal128 { .. } => {
								update_max_state(state, array, row)?;
							}
							AggState::Quantile(_) => {
								update_quantile_state(state, array, row)?;
							}
							AggState::Spread { .. } => {
								return Err(ExecutionError::NotImplemented {
									feature: "unbound grouped spread aggregate",
								});
							}
							AggState::StringAgg(_) => {
								return Err(ExecutionError::NotImplemented {
									feature: "unbound grouped string aggregate",
								});
							}
						},
					}
				}
			}
		}

		let entries = groups.finalize_sorted()?;
		let keys = entries
			.iter()
			.map(|entry| entry.key.to_vec())
			.collect::<Vec<_>>();
		let mut columns = Vec::new();
		let mut fields = Vec::new();

		for (idx, data_type) in group_types.iter().enumerate() {
			columns.push(group_column(&keys, idx, data_type)?);
			fields.push(arrow::datatypes::Field::new(
				format!("group_{idx}"),
				data_type.clone(),
				true,
			));
		}

		for (agg_idx, (function, _)) in aggr_expr.iter().enumerate() {
			match function {
				AggregateFunction::ScalarSubqueryValue => {
					return Err(ExecutionError::NotImplemented {
						feature: "unbound scalar-subquery aggregate",
					});
				}
				AggregateFunction::CountAll | AggregateFunction::Count => {
					let values = entries
						.iter()
						.map(|entry| match entry.states[agg_idx] {
							AggState::Count(value) => value,
							AggState::SumInt { .. }
							| AggState::SumFloat { .. }
							| AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. }
							| AggState::MinDecimal128 { .. }
							| AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::MaxDecimal128 { .. }
							| AggState::AvgInt { .. }
							| AggState::AvgFloat { .. }
							| AggState::AvgDecimal { .. }
							| AggState::Quantile(_)
							| AggState::Spread { .. }
							| AggState::StringAgg(_)
							| AggState::ScalarSubqueryValue { .. } => 0,
						})
						.collect::<Vec<_>>();
					columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
				}
				AggregateFunction::Sum => {
					let output_type = aggregate_output_types[agg_idx]
						.clone()
						.unwrap_or(DataType::Int64);
					columns.push(sum_column_from_groups(&entries, agg_idx, &output_type)?);
				}
				AggregateFunction::Min | AggregateFunction::Max => {
					let output_type = aggregate_output_types[agg_idx]
						.clone()
						.unwrap_or(DataType::Int64);
					columns.push(min_max_column_from_groups(&entries, agg_idx, &output_type)?);
				}
				AggregateFunction::Avg => {
					let output_type = aggregate_output_types[agg_idx]
						.clone()
						.unwrap_or(DataType::Float64);
					columns.push(avg_column_from_groups(&entries, agg_idx, &output_type)?);
				}
				AggregateFunction::VarPop
				| AggregateFunction::VarSamp
				| AggregateFunction::StddevPop
				| AggregateFunction::StddevSamp
				| AggregateFunction::LinearRegress
				| AggregateFunction::StringAgg(_)
				| AggregateFunction::ApproxQuantile(_) => {
					return Err(ExecutionError::NotImplemented {
						feature: "grouped experimental aggregate",
					});
				}
			}
			let field_name = aggregate_state_output_name(function);
			let field_type = aggregate_output_types[agg_idx]
				.clone()
				.unwrap_or(match function {
					AggregateFunction::Avg => arrow::datatypes::DataType::Float64,
					_ => arrow::datatypes::DataType::Int64,
				});
			fields.push(arrow::datatypes::Field::new(
				field_name,
				field_type,
				!matches!(
					function,
					AggregateFunction::CountAll | AggregateFunction::Count
				),
			));
		}

		let schema = Arc::new(arrow::datatypes::Schema::new(fields));
		Ok(vec![RecordBatch::try_new(schema, columns)?])
	}

	pub(in crate::exec) fn bound_grouped_aggregate(
		&self,
		input_batches: &[RecordBatch],
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut groups = BoundGroupedAggregateGroups::new();
		let mut group_types = Vec::new();

		for batch in input_batches {
			self.fold_bound_grouped_aggregate_batch(
				&mut groups,
				&mut group_types,
				group_expr,
				aggr_expr,
				batch,
			)?;
		}

		self.finalize_bound_grouped_aggregate(&mut groups, &group_types, aggr_expr, output_schema)
	}

	pub(in crate::exec) fn fold_bound_grouped_aggregate_batch(
		&self,
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		batch: &RecordBatch,
	) -> Result<(), ExecutionError> {
		let (quantile_populations, quantile_bytes) =
			quantile_populations_and_declared_bytes(aggr_expr)?;
		groups.configure_quantile_admission(
			quantile_populations,
			quantile_bytes,
			self.context.quantile_state_allowance,
			self.context.quantile_states_admitted.clone(),
		);
		if quantile_populations != 0 {
			// ADR 0546 adds a byte refusal for quantile populations without silently
			// changing the long-standing generic grouped-fold budget contract for
			// every non-quantile aggregate.
			groups.set_growth_budget_bytes(Some(self.context.task_budget.memory.get()));
		}
		let fold_started = groups.profile_enabled().then(Instant::now);
		// ADR 508 step 2 fold refinement: three batch-level sub-stages inside the `fold` phase, each
		// reconciling to `fold` rather than to the handler. No per-row locks — probe and growth
		// evidence comes from the existing grouped-table profile, which already counts them.
		let refine = node_attribution::current_handle_requiring("fold");
		let telemetry = self.context.task_telemetry.clone();
		let group_expr_stage = refine.as_ref().and_then(|h| {
			node_attribution::open_substage(h, "fold", "group_expr_eval", &telemetry)
		});
		let group_expression_started = groups.profile_enabled().then(Instant::now);
		let group_arrays = self.evaluate_bound_program(group_expr, batch)?;
		if let Some(stage) = group_expr_stage {
			node_attribution::close_stage(stage, &telemetry);
		}
		if let Some(started) = group_expression_started {
			groups.record_group_expression_evaluation(
				batch.num_rows(),
				batch.num_rows().saturating_mul(group_arrays.len()),
				started.elapsed(),
			);
		}
		if group_types.is_empty() {
			*group_types = group_arrays
				.iter()
				.map(|array| array.data_type().clone())
				.collect();
			groups.configure_key_storage(group_types);
		}

		let arg_stage = refine.as_ref().and_then(|h| {
			node_attribution::open_substage(h, "fold", "arg_eval_and_readers", &telemetry)
		});
		// ADR 562: STRING_AGG's ordering-key arrays ride a per-call side channel
		// so the single-array fast path the other kernels share stays untouched.
		let mut string_agg_keys: Vec<Option<Vec<ArrayRef>>> = vec![None; aggr_expr.len()];
		let aggr_arrays = aggr_expr
			.iter()
			.enumerate()
			.map(|(agg_idx, call)| match call.function {
				AggregateFunction::CountAll => Ok(None),
				AggregateFunction::Count
				| AggregateFunction::Sum
				| AggregateFunction::Min
				| AggregateFunction::Max
				| AggregateFunction::Avg
				| AggregateFunction::ScalarSubqueryValue
				| AggregateFunction::VarPop
				| AggregateFunction::VarSamp
				| AggregateFunction::StddevPop
				| AggregateFunction::StddevSamp
				| AggregateFunction::ApproxQuantile(_) => Ok(Some(self.evaluate_bound_single_root(
					&call.args,
					batch,
					"aggregate arg",
				)?)),
				AggregateFunction::StringAgg(_) => {
					let mut roots = self.evaluate_bound_program(&call.args, batch)?;
					if roots.is_empty() {
						return Err(ExecutionError::InvalidExpression {
							message: "STRING_AGG requires a value argument".to_string(),
						});
					}
					let value = roots.remove(0);
					string_agg_keys[agg_idx] = Some(roots);
					Ok(Some(value))
				}
				AggregateFunction::LinearRegress => Err(ExecutionError::NotImplemented {
					feature: "bound grouped experimental aggregate",
				}),
			})
			.collect::<Result<Vec<_>, _>>()?;

		// ADR 475: one type resolution per input array, not per row.
		let readers = aggr_arrays
			.iter()
			.map(|input| {
				input
					.as_ref()
					.and_then(|array| numeric_array_reader(array).ok())
			})
			.collect::<Vec<_>>();
		let group_readers = group_arrays
			.iter()
			.map(group_array_reader)
			.collect::<Result<Vec<_>, _>>()?;
		let borrow_single_utf8_group_key = borrow_single_utf8_group_key_enabled();
		let quantile_seed = self.context.quantile_execution_seed;
		if let Some(stage) = arg_stage {
			node_attribution::close_stage(stage, &telemetry);
		}
		// One bracket around the whole row loop: per-row stage tokens would cost a lock per row and
		// swamp the very arithmetic they measure.
		let row_stage = refine.as_ref().and_then(|h| {
			node_attribution::open_substage(h, "fold", "row_lookup_update", &telemetry)
		});
		let mut key = Vec::with_capacity(group_arrays.len());
		for row in 0..batch.num_rows() {
			let states = if borrow_single_utf8_group_key
				&& let [GroupArrayReader::Utf8(array)] = group_readers.as_slice()
			{
				groups.entry_or_init_single_utf8(
					(!array.is_null(row)).then(|| array.value(row)),
					aggr_expr.len(),
					|| {
						let identity = group_identity_bytes(&[match array.is_null(row) {
							true => GroupValue::Null,
							false => GroupValue::Utf8(array.value(row).to_owned()),
						}]);
						init_bound_states_with_seed(aggr_expr, quantile_seed, &identity)
					},
				)?
			} else {
				key.clear();
				for reader in group_readers.iter() {
					key.push(reader.value(row));
				}
				groups.entry_or_init(&key, aggr_expr.len(), || {
					let identity = group_identity_bytes(&key);
					init_bound_states_with_seed(aggr_expr, quantile_seed, &identity)
				})?
			};
			for (agg_idx, ((state, input), reader)) in states
				.iter_mut()
				.zip(aggr_arrays.iter())
				.zip(readers.iter())
				.enumerate()
			{
				match input {
					None => {
						if let AggState::Count(total) = state {
							*total += 1;
						}
					}
					Some(array) => match state {
						AggState::StringAgg(string_state) => {
							let keys = string_agg_keys[agg_idx].as_deref().unwrap_or(&[]);
							string_agg_accumulate_row(string_state, array, keys, row)?;
						}
						AggState::ScalarSubqueryValue { rows, value } => {
							*rows += 1;
							if *rows > 1 {
								return Err(ExecutionError::Kernel {
									message: "scalar subquery produced more than one row"
										.to_string(),
								});
							}
							*value = Some(super::scalar_value_from_array(
								array,
								row,
								array.data_type(),
								"scalar subquery value",
							)?);
						}
						AggState::Count(total) => {
							if !array.is_null(row) {
								*total += 1;
							}
						}
						AggState::SumInt { .. } | AggState::SumFloat { .. } => {
							let value = match reader {
								Some(reader) => reader.value(row),
								None => numeric_value(array, row)?,
							};
							if let Some(value) = value {
								add_sum_value(state, value)?;
							}
						}
						AggState::Spread { count, mean, m2 } => {
							let value = match reader {
								Some(reader) => reader.value(row),
								None => numeric_value(array, row)?,
							};
							if let Some(value) = value {
								spread_accumulate(
									count,
									mean,
									m2,
									spread_input_value(value, array.data_type()),
								);
							}
						}
						AggState::AvgInt { .. }
						| AggState::AvgFloat { .. }
						| AggState::AvgDecimal { .. } => {
							let value = match reader {
								Some(reader) => reader.value(row),
								None => numeric_value(array, row)?,
							};
							if let Some(value) = value {
								add_avg_value(state, value)?;
							}
						}
						AggState::MinInt32 { .. }
						| AggState::MinInt64 { .. }
						| AggState::MinFloat32 { .. }
						| AggState::MinFloat64 { .. }
						| AggState::MinDecimal128 { .. } => {
							update_min_state(state, array, row)?;
						}
						AggState::MaxInt32 { .. }
						| AggState::MaxInt64 { .. }
						| AggState::MaxFloat32 { .. }
						| AggState::MaxFloat64 { .. }
						| AggState::MaxDecimal128 { .. } => {
							update_max_state(state, array, row)?;
						}
						AggState::Quantile(_) => {
							update_quantile_state(state, array, row)?;
						}
					},
				}
			}
		}

		if let Some(stage) = row_stage {
			node_attribution::close_stage(stage, &telemetry);
		}
		if let Some(started) = fold_started {
			groups.record_fold_batch(batch.num_rows(), aggr_arrays.len(), started.elapsed());
		}

		Ok(())
	}

	pub(in crate::exec) fn finalize_bound_grouped_aggregate(
		&self,
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &[DataType],
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let finalize_started = groups.profile_enabled().then(Instant::now);
		let output = {
			let entries = groups.finalize_sorted()?;
			let keys = entries
				.iter()
				.map(|entry| entry.key.to_vec())
				.collect::<Vec<_>>();
			let mut columns = Vec::new();
			for (idx, data_type) in group_types.iter().enumerate() {
				columns.push(group_column(&keys, idx, data_type)?);
			}

			for (agg_idx, call) in aggr_expr.iter().enumerate() {
				match call.function {
					AggregateFunction::CountAll | AggregateFunction::Count => {
						let values = entries
							.iter()
							.map(|entry| match entry.states[agg_idx] {
								AggState::Count(value) => value,
								AggState::SumInt { .. }
								| AggState::SumFloat { .. }
								| AggState::MinInt32 { .. }
								| AggState::MinInt64 { .. }
								| AggState::MinFloat32 { .. }
								| AggState::MinFloat64 { .. }
								| AggState::MinDecimal128 { .. }
								| AggState::MaxInt32 { .. }
								| AggState::MaxInt64 { .. }
								| AggState::MaxFloat32 { .. }
								| AggState::MaxFloat64 { .. }
								| AggState::MaxDecimal128 { .. }
								| AggState::AvgInt { .. }
								| AggState::AvgFloat { .. }
								| AggState::AvgDecimal { .. }
								| AggState::Quantile(_)
								| AggState::Spread { .. }
								| AggState::StringAgg(_)
								| AggState::ScalarSubqueryValue { .. } => 0,
							})
							.collect::<Vec<_>>();
						columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
					}
					AggregateFunction::Sum => {
						let output_type = output_schema.field(columns.len()).data_type().clone();
						columns.push(sum_column_from_groups(&entries, agg_idx, &output_type)?);
					}
					AggregateFunction::Min | AggregateFunction::Max => {
						let output_type = output_schema.field(columns.len()).data_type().clone();
						columns.push(min_max_column_from_groups(&entries, agg_idx, &output_type)?);
					}
					AggregateFunction::Avg => {
						let output_type = output_schema.field(columns.len()).data_type().clone();
						columns.push(avg_column_from_groups(&entries, agg_idx, &output_type)?);
					}
					AggregateFunction::ApproxQuantile(_) => {
						let values = entries
							.iter()
							.map(|entry| finalize_quantile_state(&entry.states[agg_idx]))
							.collect::<Result<Vec<_>, _>>()?;
						columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
					}
					AggregateFunction::VarPop
					| AggregateFunction::VarSamp
					| AggregateFunction::StddevPop
					| AggregateFunction::StddevSamp => {
						let values = entries
							.iter()
							.map(|entry| match entry.states[agg_idx] {
								AggState::Spread { count, m2, .. } => {
									Ok(spread_finalize(&call.function, count, m2))
								}
								_ => Err(ExecutionError::InvalidExpression {
									message: "spread aggregate reached non-spread accumulator"
										.to_string(),
								}),
							})
							.collect::<Result<Vec<_>, ExecutionError>>()?;
						columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
					}
					AggregateFunction::StringAgg(ref payload) => {
						let output_type = output_schema.field(columns.len()).data_type().clone();
						let values = entries
							.iter()
							.map(|entry| match &entry.states[agg_idx] {
								AggState::StringAgg(state) => {
									Ok(string_agg_finalize(state, payload.as_ref()))
								}
								_ => Err(ExecutionError::InvalidExpression {
									message: "STRING_AGG reached a non-string-agg accumulator"
										.to_string(),
								}),
							})
							.collect::<Result<Vec<_>, ExecutionError>>()?;
						columns.push(string_agg_column(values, &output_type)?);
					}
					AggregateFunction::ScalarSubqueryValue => {
						let output_type = output_schema.field(columns.len()).data_type().clone();
						let arrays = entries
							.iter()
							.map(|entry| match &entry.states[agg_idx] {
								AggState::ScalarSubqueryValue {
									rows,
									value: Some(value),
								} if *rows == 1 => super::expr::literal_array(value, 1),
								AggState::ScalarSubqueryValue { .. } => {
									Ok(arrow::array::new_null_array(&output_type, 1))
								}
								_ => Err(ExecutionError::InvalidExpression {
									message: "scalar subquery reached the wrong accumulator"
										.to_string(),
								}),
							})
							.collect::<Result<Vec<_>, ExecutionError>>()?;
						if arrays.is_empty() {
							columns.push(arrow::array::new_empty_array(&output_type));
						} else {
							let refs: Vec<&dyn arrow::array::Array> =
								arrays.iter().map(|array| array.as_ref()).collect();
							columns.push(arrow::compute::concat(&refs)?);
						}
					}
					AggregateFunction::LinearRegress => {
						return Err(ExecutionError::NotImplemented {
							feature: "bound grouped experimental aggregate",
						});
					}
				}
			}

			RecordBatch::try_new(output_schema.clone(), columns)?
		};
		if let Some(started) = finalize_started {
			groups.record_finalize_and_persist(started.elapsed())?;
		}
		Ok(vec![output])
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::exec) enum GroupValue {
	Null,
	Boolean(bool),
	Int32(i32),
	Int64(i64),
	Float32(u32),
	Float64(u64),
	Date32(i32),
	Timestamp(TimeUnit, i64),
	Utf8(String),
	// ADR 275: a Decimal128 group key. The raw i128 is exact (no float-bit canonicalization), so equal Decimal
	// values group together and reconstruct losslessly.
	Decimal128(i128),
}

#[derive(Debug, Clone, Copy)]
pub(in crate::exec) enum QuantileInputType {
	Int32,
	Int64,
	Float32,
	Float64,
}

impl QuantileInputType {
	fn from_data_type(data_type: &DataType) -> Result<Self, ExecutionError> {
		match data_type {
			DataType::Int32 => Ok(Self::Int32),
			DataType::Int64 => Ok(Self::Int64),
			DataType::Float32 => Ok(Self::Float32),
			DataType::Float64 => Ok(Self::Float64),
			other => Err(ExecutionError::InvalidExpression {
				message: format!("APPROX_QUANTILE does not support input type {other:?}"),
			}),
		}
	}

	fn type_tag(self) -> u8 {
		use wendelon::schema::optimizer_facts::{
			ARROW_STATS_TAG_FLOAT32, ARROW_STATS_TAG_FLOAT64, ARROW_STATS_TAG_INT32,
			ARROW_STATS_TAG_INT64,
		};
		match self {
			Self::Int32 => ARROW_STATS_TAG_INT32,
			Self::Int64 => ARROW_STATS_TAG_INT64,
			Self::Float32 => ARROW_STATS_TAG_FLOAT32,
			Self::Float64 => ARROW_STATS_TAG_FLOAT64,
		}
	}
}

#[derive(Debug, Clone)]
pub(in crate::exec) struct QuantileState {
	sketch: Arc<Mutex<KllSketch>>,
	q: f64,
	input_type: QuantileInputType,
	owner: bool,
}

/// ADR 561: the window reject arms read the same registry property the planner
/// gates on, rather than keeping their own list. Reaching this with a
/// window-eligible function means the registry was flipped without executor
/// support — a defect this assert names instead of silently miscomputing.
pub(in crate::exec) fn non_window_aggregate_error(function: &AggregateFunction) -> ExecutionError {
	debug_assert!(
		!crate::aggregate_kernel::stable_spec_for_function(function).window_eligible,
		"window-eligible aggregate reached the executor without window support"
	);
	ExecutionError::NotImplemented {
		feature: "window experimental aggregate execution",
	}
}

/// ADR 560: one Welford step, used by every path that accumulates a spread.
///
/// The naive `sum(x²) − n·mean²` form is accurate on small synthetic data and
/// catastrophically wrong on large-magnitude, low-variance columns, which is
/// what this engine actually serves. Keeping the update in one function means
/// the global and grouped paths cannot diverge into different algorithms.
pub(in crate::exec) fn spread_accumulate(
	count: &mut u64,
	mean: &mut f64,
	m2: &mut f64,
	value: f64,
) {
	*count += 1;
	let delta = value - *mean;
	*mean += delta / (*count as f64);
	*m2 += delta * (value - *mean);
}

/// Reads the accumulator for one spelling. `None` is the NULL answer: the
/// sample forms have no meaning below two observations, and neither form has
/// one below one.
pub(in crate::exec) fn spread_finalize(
	function: &AggregateFunction,
	count: u64,
	m2: f64,
) -> Option<f64> {
	match function {
		AggregateFunction::VarPop => (count >= 1).then(|| m2 / count as f64),
		AggregateFunction::StddevPop => (count >= 1).then(|| (m2 / count as f64).sqrt()),
		AggregateFunction::VarSamp => (count >= 2).then(|| m2 / (count - 1) as f64),
		AggregateFunction::StddevSamp => (count >= 2).then(|| (m2 / (count - 1) as f64).sqrt()),
		_ => None,
	}
}

#[derive(Debug, Clone)]
pub(in crate::exec) enum AggState {
	Count(i64),
	/// ADR 560: Welford's running moments, shared by all four spread spellings.
	/// They differ only in how the finalizer reads `count` and `m2`, so one
	/// accumulator serves them and cannot drift between them.
	Spread {
		count: u64,
		mean: f64,
		m2: f64,
	},
	/// ADR 562: buffered ordered concatenation; boxed so the common numeric
	/// states stay small.
	StringAgg(Box<StringAggState>),
	/// ADR 571: the at-most-one accumulator. `rows` distinguishes an empty
	/// group from one row whose value is NULL; a second row raises.
	ScalarSubqueryValue {
		rows: u64,
		value: Option<ScalarValue>,
	},
	SumInt {
		total: i128,
		seen: bool,
	},
	SumFloat {
		total: f64,
		seen: bool,
	},
	MinInt32 {
		value: Option<i32>,
	},
	MinInt64 {
		value: Option<i64>,
	},
	MinFloat32 {
		value: Option<f32>,
	},
	MinFloat64 {
		value: Option<f64>,
	},
	MinDecimal128 {
		value: Option<i128>,
	},
	MaxInt32 {
		value: Option<i32>,
	},
	MaxInt64 {
		value: Option<i64>,
	},
	MaxFloat32 {
		value: Option<f32>,
	},
	MaxFloat64 {
		value: Option<f64>,
	},
	MaxDecimal128 {
		value: Option<i128>,
	},
	AvgInt {
		total: i128,
		count: u64,
	},
	AvgFloat {
		total: f64,
		count: u64,
	},
	// ADR 268: AVG(Decimal) accumulates the raw unscaled i128 (like SumInt) and carries the input scale so
	// the finalize can rescale to the widened output scale before dividing.
	AvgDecimal {
		total: i128,
		count: u64,
		input_scale: i8,
	},
	Quantile(Box<QuantileState>),
}

#[derive(Debug, Clone, Copy)]
pub(in crate::exec) enum NumericValue {
	Int(i128),
	Float(f64),
}

/// ADR 560: narrows an aggregate input to the `Float64` the spread accumulator
/// works in.
///
/// A `Decimal128` reader yields its **unscaled** integer, so the scale has to be
/// applied here or a variance comes out wrong by `10^(2·scale)`. This is the only
/// conversion the spread family uses, so the global and grouped paths cannot
/// disagree about it.
pub(in crate::exec) fn spread_input_value(value: NumericValue, input_type: &DataType) -> f64 {
	let raw = match value {
		NumericValue::Int(value) => value as f64,
		NumericValue::Float(value) => value,
	};
	match input_type {
		DataType::Decimal128(_, scale) => raw / 10f64.powi(i32::from(*scale)),
		_ => raw,
	}
}

#[cfg(test)]
fn init_states(
	aggr_expr: &[(AggregateFunction, Vec<Expr>)],
	aggr_arrays: &[Option<ArrayRef>],
) -> Result<Vec<AggState>, ExecutionError> {
	aggr_expr
		.iter()
		.zip(aggr_arrays.iter())
		.map(|((function, _), array)| match function {
			AggregateFunction::ScalarSubqueryValue => Err(ExecutionError::NotImplemented {
				feature: "unbound scalar-subquery aggregate",
			}),
			AggregateFunction::CountAll | AggregateFunction::Count => Ok(AggState::Count(0)),
			AggregateFunction::Sum => Ok(array
				.as_ref()
				.map(|array| sum_state_for_data_type(array.data_type()))
				.unwrap_or(AggState::SumInt {
					total: 0,
					seen: false,
				})),
			AggregateFunction::Min => array
				.as_ref()
				.map(|array| min_state_for_data_type(array.data_type()))
				.transpose()
				.map(|state| state.unwrap_or(AggState::MinInt64 { value: None })),
			AggregateFunction::Max => array
				.as_ref()
				.map(|array| max_state_for_data_type(array.data_type()))
				.transpose()
				.map(|state| state.unwrap_or(AggState::MaxInt64 { value: None })),
			AggregateFunction::Avg => array
				.as_ref()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "AVG requires one argument".to_string(),
				})
				.and_then(|array| avg_state_for_data_type(array.data_type())),
			AggregateFunction::VarPop
			| AggregateFunction::VarSamp
			| AggregateFunction::StddevPop
			| AggregateFunction::StddevSamp
			| AggregateFunction::LinearRegress
			| AggregateFunction::StringAgg(_)
			| AggregateFunction::ApproxQuantile(_) => Ok(AggState::SumInt {
				total: 0,
				seen: false,
			}),
		})
		.collect()
}

#[cfg(test)]
fn legacy_aggregate_output_type(function: &AggregateFunction, input_type: &DataType) -> DataType {
	match function {
		AggregateFunction::CountAll | AggregateFunction::Count => DataType::Int64,
		AggregateFunction::Sum => match input_type {
			DataType::Float32 | DataType::Float64 => DataType::Float64,
			_ => DataType::Int64,
		},
		AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::ScalarSubqueryValue => input_type.clone(),
		// ADR 268: delegate to the kernel so AVG(Decimal) yields the widened Decimal type, not Float64.
		AggregateFunction::Avg => {
			aggregate_kernel::stable_output_data_type(function, Some(input_type))
				.unwrap_or(DataType::Float64)
		}
		AggregateFunction::VarPop
		| AggregateFunction::VarSamp
		| AggregateFunction::StddevPop
		| AggregateFunction::StddevSamp
		| AggregateFunction::LinearRegress
		| AggregateFunction::ApproxQuantile(_) => DataType::Float64,
		// Unreachable here: the unbound path rejects STRING_AGG at evaluation.
		AggregateFunction::StringAgg(_) => input_type.clone(),
	}
}

fn init_bound_states_with_seed(
	aggr_expr: &[BoundAggregateCall],
	seed: [u8; 32],
	group_identity: &[u8],
) -> Result<Vec<AggState>, ExecutionError> {
	let mut shared = HashMap::<Vec<u8>, Arc<Mutex<KllSketch>>>::new();
	let mut states = Vec::with_capacity(aggr_expr.len());
	for call in aggr_expr {
		if let AggregateFunction::ApproxQuantile(q) = call.function {
			let input_type = QuantileInputType::from_data_type(first_arg_type(call)?)?;
			let expression =
				postcard::to_stdvec(&call.args).map_err(|error| ExecutionError::Kernel {
					message: format!("APPROX_QUANTILE expression identity failed: {error}"),
				})?;
			let mut population_key =
				Vec::with_capacity(expression.len() + group_identity.len() + 8);
			population_key.extend_from_slice(&(expression.len() as u64).to_be_bytes());
			population_key.extend_from_slice(&expression);
			population_key.extend_from_slice(group_identity);
			let existing = shared.get(&population_key).cloned();
			let owner = existing.is_none();
			let sketch = match existing {
				Some(sketch) => sketch,
				None => {
					if shared.len() == MAX_QUANTILE_STATES_PER_EXECUTION {
						return Err(quantile_state_cap_error(shared.len() + 1));
					}
					let identity = *blake3::keyed_hash(&seed, &population_key).as_bytes();
					let sketch = Arc::new(Mutex::new(
						KllSketch::new_with_randomness(
							input_type.type_tag(),
							QUANTILE_K,
							seed,
							identity,
						)
						.map_err(|error| ExecutionError::Kernel {
							message: error.to_string(),
						})?,
					));
					shared.insert(population_key, sketch.clone());
					sketch
				}
			};
			states.push(AggState::Quantile(Box::new(QuantileState {
				sketch,
				q,
				input_type,
				owner,
			})));
		} else {
			states.push(init_bound_state_for_call(call)?);
		}
	}
	Ok(states)
}

fn group_identity_bytes(key: &[GroupValue]) -> Vec<u8> {
	let mut bytes = Vec::new();
	for value in key {
		match value {
			GroupValue::Null => bytes.push(0),
			GroupValue::Boolean(value) => bytes.extend_from_slice(&[1, u8::from(*value)]),
			GroupValue::Int32(value) => {
				bytes.push(2);
				bytes.extend_from_slice(&value.to_be_bytes());
			}
			GroupValue::Int64(value) => {
				bytes.push(3);
				bytes.extend_from_slice(&value.to_be_bytes());
			}
			GroupValue::Float32(value) => {
				bytes.push(4);
				bytes.extend_from_slice(&value.to_be_bytes());
			}
			GroupValue::Float64(value) => {
				bytes.push(5);
				bytes.extend_from_slice(&value.to_be_bytes());
			}
			GroupValue::Date32(value) => {
				bytes.push(6);
				bytes.extend_from_slice(&value.to_be_bytes());
			}
			GroupValue::Timestamp(unit, value) => {
				bytes.push(match unit {
					TimeUnit::Second => 7,
					TimeUnit::Millisecond => 8,
					TimeUnit::Microsecond => 9,
					TimeUnit::Nanosecond => 10,
				});
				bytes.extend_from_slice(&value.to_be_bytes());
			}
			GroupValue::Utf8(value) => {
				bytes.push(11);
				bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
				bytes.extend_from_slice(value.as_bytes());
			}
			GroupValue::Decimal128(value) => {
				bytes.push(12);
				bytes.extend_from_slice(&value.to_be_bytes());
			}
		}
	}
	bytes
}

fn init_bound_state_for_call(call: &BoundAggregateCall) -> Result<AggState, ExecutionError> {
	// ADR 561: the accumulator family is registry metadata; the per-type
	// constructor each family applies stays the executor's own.
	match aggregate_kernel::stable_spec_for_function(&call.function).state_family {
		aggregate_kernel::AggregateStateFamily::Count => Ok(AggState::Count(0)),
		aggregate_kernel::AggregateStateFamily::Spread => Ok(AggState::Spread {
			count: 0,
			mean: 0.0,
			m2: 0.0,
		}),
		aggregate_kernel::AggregateStateFamily::StringAgg => {
			Ok(AggState::StringAgg(Box::default()))
		}
		aggregate_kernel::AggregateStateFamily::ScalarSubqueryValue => {
			Ok(AggState::ScalarSubqueryValue {
				rows: 0,
				value: None,
			})
		}
		aggregate_kernel::AggregateStateFamily::Sum => Ok(call
			.args
			.root_type(0)
			.map(sum_state_for_data_type)
			.unwrap_or(AggState::SumInt {
				total: 0,
				seen: false,
			})),
		aggregate_kernel::AggregateStateFamily::Min => call
			.args
			.root_type(0)
			.map(min_state_for_data_type)
			.transpose()
			.map(|state| state.unwrap_or(AggState::MinInt64 { value: None })),
		aggregate_kernel::AggregateStateFamily::Max => call
			.args
			.root_type(0)
			.map(max_state_for_data_type)
			.transpose()
			.map(|state| state.unwrap_or(AggState::MaxInt64 { value: None })),
		aggregate_kernel::AggregateStateFamily::Avg => {
			avg_state_for_data_type(first_arg_type(call)?)
		}
		// The experimental path owns its execution; this placeholder is never
		// accumulated into because the grouped path rejects downstream.
		aggregate_kernel::AggregateStateFamily::Experimental => Ok(AggState::SumInt {
			total: 0,
			seen: false,
		}),
		aggregate_kernel::AggregateStateFamily::Quantile => {
			unreachable!("quantile state is shared by program")
		}
	}
}

pub(in crate::exec) fn quantile_state_cap_error(requested: usize) -> ExecutionError {
	ExecutionError::BudgetExhausted {
		dimension: BudgetDimension::Memory,
		producer: "MAX_QUANTILE_STATES_PER_EXECUTION=1024",
		requested: crate::distributed::budget::ByteCount::new(requested as u64),
		available: crate::distributed::budget::ByteCount::new(
			MAX_QUANTILE_STATES_PER_EXECUTION as u64,
		),
		detail: crate::error::BudgetExhaustedDetail::None,
	}
}

pub(in crate::exec) struct QuantileStateReservation {
	admitted: Arc<std::sync::atomic::AtomicUsize>,
	count: usize,
	committed: bool,
}

impl QuantileStateReservation {
	pub(in crate::exec) fn commit(mut self) {
		self.committed = true;
	}
}

impl Drop for QuantileStateReservation {
	fn drop(&mut self) {
		if !self.committed {
			self.admitted
				.fetch_sub(self.count, std::sync::atomic::Ordering::AcqRel);
		}
	}
}

pub(in crate::exec) fn reserve_quantile_states(
	admitted: Arc<std::sync::atomic::AtomicUsize>,
	limit: usize,
	count: usize,
) -> Result<QuantileStateReservation, ExecutionError> {
	loop {
		let current = admitted.load(std::sync::atomic::Ordering::Acquire);
		let requested = current.saturating_add(count);
		if requested > limit {
			return Err(quantile_state_cap_error(requested));
		}
		if admitted
			.compare_exchange_weak(
				current,
				requested,
				std::sync::atomic::Ordering::AcqRel,
				std::sync::atomic::Ordering::Acquire,
			)
			.is_ok()
		{
			return Ok(QuantileStateReservation {
				admitted,
				count,
				committed: false,
			});
		}
	}
}

pub(in crate::exec) fn quantile_populations_and_declared_bytes(
	aggr_expr: &[BoundAggregateCall],
) -> Result<(usize, u64), ExecutionError> {
	let mut programs = Vec::<Vec<u8>>::new();
	let mut bytes = 0_u64;
	for call in aggr_expr {
		if !matches!(call.function, AggregateFunction::ApproxQuantile(_)) {
			continue;
		}
		let program =
			postcard::to_allocvec(&call.args).map_err(|error| ExecutionError::Kernel {
				message: format!("APPROX_QUANTILE expression identity failed: {error}"),
			})?;
		if programs.iter().any(|existing| existing == &program) {
			continue;
		}
		let width = match first_arg_type(call)? {
			DataType::Int32 | DataType::Float32 => 4_u64,
			DataType::Int64 | DataType::Float64 => 8_u64,
			other => {
				return Err(ExecutionError::InvalidExpression {
					message: format!("APPROX_QUANTILE does not support input type {other:?}"),
				});
			}
		};
		// ADR 0083: 5,601 retained slots at epsilon=1%, plus a conservative fixed
		// allowance for level vectors, randomness, counters, and sampler metadata.
		bytes = bytes.saturating_add(5_601_u64.saturating_mul(width).saturating_add(512));
		programs.push(program);
	}
	Ok((programs.len(), bytes))
}

fn update_quantile_state(
	state: &mut AggState,
	array: &ArrayRef,
	row: usize,
) -> Result<(), ExecutionError> {
	let AggState::Quantile(quantile) = state else {
		return Err(ExecutionError::InvalidExpression {
			message: "APPROX_QUANTILE value reached non-quantile accumulator".to_string(),
		});
	};
	if !quantile.owner || array.is_null(row) {
		return Ok(());
	}
	use wendelon::schema::optimizer_facts::{
		encode_float32_optimizer_fact_scalar, encode_float64_optimizer_fact_scalar,
		encode_signed_optimizer_fact_scalar,
	};
	let encoded = match quantile.input_type {
		QuantileInputType::Int32 => encode_signed_optimizer_fact_scalar(
			i64::from(
				array
					.as_any()
					.downcast_ref::<Int32Array>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "APPROX_QUANTILE Int32 array type mismatch".to_string(),
					})?
					.value(row),
			),
			4,
		),
		QuantileInputType::Int64 => encode_signed_optimizer_fact_scalar(
			array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "APPROX_QUANTILE Int64 array type mismatch".to_string(),
				})?
				.value(row),
			8,
		),
		QuantileInputType::Float32 => encode_float32_optimizer_fact_scalar(
			array
				.as_any()
				.downcast_ref::<Float32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "APPROX_QUANTILE Float32 array type mismatch".to_string(),
				})?
				.value(row),
		),
		QuantileInputType::Float64 => encode_float64_optimizer_fact_scalar(
			array
				.as_any()
				.downcast_ref::<Float64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "APPROX_QUANTILE Float64 array type mismatch".to_string(),
				})?
				.value(row),
		),
	}
	.map_err(|error| ExecutionError::Kernel {
		message: format!("APPROX_QUANTILE observation refused: {error}"),
	})?;
	quantile
		.sketch
		.lock()
		.map_err(|_| ExecutionError::Kernel {
			message: "APPROX_QUANTILE state lock poisoned".to_string(),
		})?
		.insert(encoded);
	Ok(())
}

fn finalize_quantile_state(state: &AggState) -> Result<Option<f64>, ExecutionError> {
	let AggState::Quantile(quantile) = state else {
		return Err(ExecutionError::InvalidExpression {
			message: "APPROX_QUANTILE finalize over non-quantile state".to_string(),
		});
	};
	let encoded = quantile
		.sketch
		.lock()
		.map_err(|_| ExecutionError::Kernel {
			message: "APPROX_QUANTILE state lock poisoned".to_string(),
		})?
		.query_quantile(quantile.q)
		.map_err(|error| ExecutionError::Kernel {
			message: error.to_string(),
		})?;
	let Some(encoded) = encoded else {
		return Ok(None);
	};
	use wendelon::schema::optimizer_facts::{
		decode_float32_optimizer_fact_scalar, decode_float64_optimizer_fact_scalar,
		decode_signed_optimizer_fact_scalar,
	};
	let value = match quantile.input_type {
		QuantileInputType::Int32 | QuantileInputType::Int64 => {
			decode_signed_optimizer_fact_scalar(&encoded).map(|value| value as f64)
		}
		QuantileInputType::Float32 => decode_float32_optimizer_fact_scalar(&encoded).map(f64::from),
		QuantileInputType::Float64 => decode_float64_optimizer_fact_scalar(&encoded),
	};
	value.map(Some).ok_or_else(|| ExecutionError::Kernel {
		message: "APPROX_QUANTILE result decode failed".to_string(),
	})
}

fn first_arg_type(call: &BoundAggregateCall) -> Result<&DataType, ExecutionError> {
	call.args
		.root_type(0)
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{:?} requires one argument", call.function),
		})
}

/// ADR 477: a group column's concrete type resolved **once per batch**, so key
/// extraction stops paying a twelve-branch `as_any().downcast_ref()` chain per row
/// (ADR 476 isolated that chain as grouping's remaining per-row cost). This is the
/// **single canonical** row → `GroupValue` conversion — the variant set, canonicalisation,
/// null contract, and error text all live here, and the test-only [`group_value`]
/// delegates to it so the two cannot drift. `GroupValue` ownership is deliberately
/// unchanged: a Utf8 key still owns its `String` (ADR 477 scope note).
pub(in crate::exec) enum GroupArrayReader<'a> {
	Boolean(&'a BooleanArray),
	Int32(&'a Int32Array),
	Int64(&'a Int64Array),
	Float32(&'a Float32Array),
	Float64(&'a Float64Array),
	Date32(&'a Date32Array),
	TimestampMillisecond(&'a TimestampMillisecondArray),
	TimestampMicrosecond(&'a TimestampMicrosecondArray),
	TimestampNanosecond(&'a TimestampNanosecondArray),
	Utf8(&'a StringArray),
	Decimal128(&'a Decimal128Array),
}

impl GroupArrayReader<'_> {
	pub(in crate::exec) fn value(&self, row: usize) -> GroupValue {
		let null = match self {
			Self::Boolean(array) => array.is_null(row),
			Self::Int32(array) => array.is_null(row),
			Self::Int64(array) => array.is_null(row),
			Self::Float32(array) => array.is_null(row),
			Self::Float64(array) => array.is_null(row),
			Self::Date32(array) => array.is_null(row),
			Self::TimestampMillisecond(array) => array.is_null(row),
			Self::TimestampMicrosecond(array) => array.is_null(row),
			Self::TimestampNanosecond(array) => array.is_null(row),
			Self::Utf8(array) => array.is_null(row),
			Self::Decimal128(array) => array.is_null(row),
		};
		if null {
			return GroupValue::Null;
		}
		match self {
			Self::Boolean(array) => GroupValue::Boolean(array.value(row)),
			Self::Int32(array) => GroupValue::Int32(array.value(row)),
			Self::Int64(array) => GroupValue::Int64(array.value(row)),
			Self::Float32(array) => GroupValue::Float32(canonical_group_f32_bits(array.value(row))),
			Self::Float64(array) => GroupValue::Float64(canonical_group_f64_bits(array.value(row))),
			Self::Date32(array) => GroupValue::Date32(array.value(row)),
			Self::TimestampMillisecond(array) => {
				GroupValue::Timestamp(TimeUnit::Millisecond, array.value(row))
			}
			Self::TimestampMicrosecond(array) => {
				GroupValue::Timestamp(TimeUnit::Microsecond, array.value(row))
			}
			Self::TimestampNanosecond(array) => {
				GroupValue::Timestamp(TimeUnit::Nanosecond, array.value(row))
			}
			Self::Utf8(array) => GroupValue::Utf8(array.value(row).to_string()),
			Self::Decimal128(array) => GroupValue::Decimal128(array.value(row)),
		}
	}
}

pub(in crate::exec) fn group_array_reader(
	array: &ArrayRef,
) -> Result<GroupArrayReader<'_>, ExecutionError> {
	let any = array.as_any();
	if let Some(array) = any.downcast_ref::<BooleanArray>() {
		return Ok(GroupArrayReader::Boolean(array));
	}
	if let Some(array) = any.downcast_ref::<Int32Array>() {
		return Ok(GroupArrayReader::Int32(array));
	}
	if let Some(array) = any.downcast_ref::<Int64Array>() {
		return Ok(GroupArrayReader::Int64(array));
	}
	if let Some(array) = any.downcast_ref::<Float32Array>() {
		return Ok(GroupArrayReader::Float32(array));
	}
	if let Some(array) = any.downcast_ref::<Float64Array>() {
		return Ok(GroupArrayReader::Float64(array));
	}
	if let Some(array) = any.downcast_ref::<Date32Array>() {
		return Ok(GroupArrayReader::Date32(array));
	}
	if let Some(array) = any.downcast_ref::<TimestampMillisecondArray>() {
		return Ok(GroupArrayReader::TimestampMillisecond(array));
	}
	if let Some(array) = any.downcast_ref::<TimestampMicrosecondArray>() {
		return Ok(GroupArrayReader::TimestampMicrosecond(array));
	}
	if let Some(array) = any.downcast_ref::<TimestampNanosecondArray>() {
		return Ok(GroupArrayReader::TimestampNanosecond(array));
	}
	if let Some(array) = any.downcast_ref::<StringArray>() {
		return Ok(GroupArrayReader::Utf8(array));
	}
	if let Some(array) = any.downcast_ref::<Decimal128Array>() {
		return Ok(GroupArrayReader::Decimal128(array));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("unsupported GROUP BY type: {:?}", array.data_type()),
	})
}

/// Test-only reference extractor. Production key extraction resolves the column type
/// once per batch through [`GroupArrayReader`] (ADR 477); this delegates to that same
/// canonical conversion so the two paths cannot drift, and it is gated to test builds
/// because no production caller remains after ADR 499's exchange bypass.
#[cfg(test)]
pub(in crate::exec) fn group_value(
	array: &ArrayRef,
	row: usize,
) -> Result<GroupValue, ExecutionError> {
	Ok(group_array_reader(array)?.value(row))
}

fn canonical_group_f32_bits(value: f32) -> u32 {
	if value == 0.0 {
		0.0f32.to_bits()
	} else if value.is_nan() {
		f32::NAN.to_bits()
	} else {
		value.to_bits()
	}
}

fn canonical_group_f64_bits(value: f64) -> u64 {
	if value == 0.0 {
		0.0f64.to_bits()
	} else if value.is_nan() {
		f64::NAN.to_bits()
	} else {
		value.to_bits()
	}
}

pub(in crate::exec) fn sum_state_for_data_type(data_type: &DataType) -> AggState {
	match data_type {
		DataType::Float32 | DataType::Float64 => AggState::SumFloat {
			total: 0.0,
			seen: false,
		},
		_ => AggState::SumInt {
			total: 0,
			seen: false,
		},
	}
}

pub(in crate::exec) fn min_state_for_data_type(
	data_type: &DataType,
) -> Result<AggState, ExecutionError> {
	match data_type {
		DataType::Int32 => Ok(AggState::MinInt32 { value: None }),
		DataType::Int64 => Ok(AggState::MinInt64 { value: None }),
		DataType::Float32 => Ok(AggState::MinFloat32 { value: None }),
		DataType::Float64 => Ok(AggState::MinFloat64 { value: None }),
		DataType::Decimal128(_, _) => Ok(AggState::MinDecimal128 { value: None }),
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("MIN does not support input type {:?}", data_type),
		}),
	}
}

pub(in crate::exec) fn max_state_for_data_type(
	data_type: &DataType,
) -> Result<AggState, ExecutionError> {
	match data_type {
		DataType::Int32 => Ok(AggState::MaxInt32 { value: None }),
		DataType::Int64 => Ok(AggState::MaxInt64 { value: None }),
		DataType::Float32 => Ok(AggState::MaxFloat32 { value: None }),
		DataType::Float64 => Ok(AggState::MaxFloat64 { value: None }),
		DataType::Decimal128(_, _) => Ok(AggState::MaxDecimal128 { value: None }),
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("MAX does not support input type {:?}", data_type),
		}),
	}
}

pub(in crate::exec) fn avg_state_for_data_type(
	data_type: &DataType,
) -> Result<AggState, ExecutionError> {
	match data_type {
		DataType::Float32 | DataType::Float64 => Ok(AggState::AvgFloat {
			total: 0.0,
			count: 0,
		}),
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64 => Ok(AggState::AvgInt { total: 0, count: 0 }),
		// ADR 268: AVG(Decimal) — raw i128 accumulator carrying the input scale for the rescale-and-divide
		// finalize. Validate through the same kernel rule used by bind/type derivation so an invalid
		// high-scale Decimal cannot form a Decimal state and later fall back to Float64 materialization.
		DataType::Decimal128(_, scale)
			if aggregate_kernel::stable_output_data_type(
				&AggregateFunction::Avg,
				Some(data_type),
			)
			.is_some() =>
		{
			Ok(AggState::AvgDecimal {
				total: 0,
				count: 0,
				input_scale: *scale,
			})
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("AVG does not support input type {:?}", data_type),
		}),
	}
}

pub(in crate::exec) fn add_sum_value(
	state: &mut AggState,
	value: NumericValue,
) -> Result<(), ExecutionError> {
	match (state, value) {
		(AggState::SumInt { total, seen }, NumericValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "integer SUM overflow".to_string(),
				})?;
			*seen = true;
			Ok(())
		}
		(AggState::SumFloat { total, seen }, NumericValue::Float(value)) => {
			*total += value;
			*seen = true;
			Ok(())
		}
		(AggState::SumFloat { total, seen }, NumericValue::Int(value)) => {
			*total += value as f64;
			*seen = true;
			Ok(())
		}
		(AggState::SumInt { .. }, NumericValue::Float(_)) => {
			Err(ExecutionError::InvalidExpression {
				message: "floating SUM value reached integer accumulator".to_string(),
			})
		}
		(AggState::Count(_), _)
		| (AggState::MinInt32 { .. }, _)
		| (AggState::MinInt64 { .. }, _)
		| (AggState::MinFloat32 { .. }, _)
		| (AggState::MinFloat64 { .. }, _)
		| (AggState::MinDecimal128 { .. }, _)
		| (AggState::MaxInt32 { .. }, _)
		| (AggState::MaxInt64 { .. }, _)
		| (AggState::MaxFloat32 { .. }, _)
		| (AggState::MaxFloat64 { .. }, _)
		| (AggState::MaxDecimal128 { .. }, _)
		| (AggState::AvgInt { .. }, _)
		| (AggState::AvgFloat { .. }, _)
		| (AggState::AvgDecimal { .. }, _)
		| (AggState::Spread { .. }, _)
		| (AggState::StringAgg(_), _)
		| (AggState::ScalarSubqueryValue { .. }, _)
		| (AggState::Quantile(_), _) => Err(ExecutionError::InvalidExpression {
			message: "SUM value reached non-SUM accumulator".to_string(),
		}),
	}
}

pub(in crate::exec) fn add_avg_value(
	state: &mut AggState,
	value: NumericValue,
) -> Result<(), ExecutionError> {
	match (state, value) {
		(AggState::AvgInt { total, count }, NumericValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "integer AVG overflow".to_string(),
				})?;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgFloat { total, count }, NumericValue::Float(value)) => {
			*total += value;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgFloat { total, count }, NumericValue::Int(value)) => {
			*total += value as f64;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgInt { .. }, NumericValue::Float(_)) => {
			Err(ExecutionError::InvalidExpression {
				message: "floating AVG value reached integer accumulator".to_string(),
			})
		}
		// ADR 268: a Decimal value enters the i128 accumulator as its raw unscaled integer.
		(AggState::AvgDecimal { total, count, .. }, NumericValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "decimal AVG overflow".to_string(),
				})?;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgDecimal { .. }, NumericValue::Float(_)) => {
			Err(ExecutionError::InvalidExpression {
				message: "floating AVG value reached decimal accumulator".to_string(),
			})
		}
		(AggState::Count(_), _)
		| (AggState::SumInt { .. }, _)
		| (AggState::SumFloat { .. }, _)
		| (AggState::MinInt32 { .. }, _)
		| (AggState::MinInt64 { .. }, _)
		| (AggState::MinFloat32 { .. }, _)
		| (AggState::MinFloat64 { .. }, _)
		| (AggState::MinDecimal128 { .. }, _)
		| (AggState::MaxInt32 { .. }, _)
		| (AggState::MaxInt64 { .. }, _)
		| (AggState::MaxFloat32 { .. }, _)
		| (AggState::MaxFloat64 { .. }, _)
		| (AggState::MaxDecimal128 { .. }, _)
		| (AggState::Spread { .. }, _)
		| (AggState::StringAgg(_), _)
		| (AggState::ScalarSubqueryValue { .. }, _)
		| (AggState::Quantile(_), _) => Err(ExecutionError::InvalidExpression {
			message: "AVG value reached non-AVG accumulator".to_string(),
		}),
	}
}

// ADR 022 pins sticky-NaN and IEEE totalOrder signed-zero semantics
// for Float MIN/MAX instead of Rust's one-sided f32/f64 min/max helpers.
#[inline]
fn f32_min_sticky_total(current: f32, candidate: f32) -> f32 {
	if current.is_nan() || candidate.is_nan() {
		return f32::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Greater => candidate,
		_ => current,
	}
}

#[inline]
fn f32_max_sticky_total(current: f32, candidate: f32) -> f32 {
	if current.is_nan() || candidate.is_nan() {
		return f32::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Less => candidate,
		_ => current,
	}
}

#[inline]
fn f64_min_sticky_total(current: f64, candidate: f64) -> f64 {
	if current.is_nan() || candidate.is_nan() {
		return f64::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Greater => candidate,
		_ => current,
	}
}

#[inline]
fn f64_max_sticky_total(current: f64, candidate: f64) -> f64 {
	if current.is_nan() || candidate.is_nan() {
		return f64::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Less => candidate,
		_ => current,
	}
}

pub(in crate::exec) fn update_min_state(
	state: &mut AggState,
	array: &ArrayRef,
	row: usize,
) -> Result<(), ExecutionError> {
	if array.is_null(row) {
		return Ok(());
	}
	match state {
		AggState::MinInt32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Int32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.min(candidate)));
			Ok(())
		}
		AggState::MinInt64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Int64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.min(candidate)));
			Ok(())
		}
		AggState::MinFloat32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Float32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f32_min_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MinFloat64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Float64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f64_min_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MinDecimal128 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Decimal128Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Decimal128) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.min(candidate)));
			Ok(())
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: "MIN value reached non-MIN accumulator".to_string(),
		}),
	}
}

pub(in crate::exec) fn update_max_state(
	state: &mut AggState,
	array: &ArrayRef,
	row: usize,
) -> Result<(), ExecutionError> {
	if array.is_null(row) {
		return Ok(());
	}
	match state {
		AggState::MaxInt32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Int32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.max(candidate)));
			Ok(())
		}
		AggState::MaxInt64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Int64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.max(candidate)));
			Ok(())
		}
		AggState::MaxFloat32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Float32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f32_max_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MaxFloat64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Float64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f64_max_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MaxDecimal128 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Decimal128Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Decimal128) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.max(candidate)));
			Ok(())
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: "MAX value reached non-MAX accumulator".to_string(),
		}),
	}
}

pub(in crate::exec) fn min_max_state_array(
	state: &AggState,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match (state, output_type) {
		(AggState::MinInt32 { value }, DataType::Int32)
		| (AggState::MaxInt32 { value }, DataType::Int32) => {
			Ok(Arc::new(Int32Array::from(vec![*value])) as ArrayRef)
		}
		(AggState::MinInt64 { value }, DataType::Int64)
		| (AggState::MaxInt64 { value }, DataType::Int64) => {
			Ok(Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
		}
		(AggState::MinFloat32 { value }, DataType::Float32)
		| (AggState::MaxFloat32 { value }, DataType::Float32) => {
			Ok(Arc::new(Float32Array::from(vec![*value])) as ArrayRef)
		}
		(AggState::MinFloat64 { value }, DataType::Float64)
		| (AggState::MaxFloat64 { value }, DataType::Float64) => {
			Ok(Arc::new(Float64Array::from(vec![*value])) as ArrayRef)
		}
		(
			AggState::MinDecimal128 { value } | AggState::MaxDecimal128 { value },
			DataType::Decimal128(precision, scale),
		) => Ok(Arc::new(
			Decimal128Array::from(vec![*value])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		) as ArrayRef),
		(other, data_type) => Err(ExecutionError::InvalidExpression {
			message: format!("cannot materialize {other:?} as MIN/MAX output {data_type:?}"),
		}),
	}
}

pub(in crate::exec) fn sum_state_array(
	state: &AggState,
	data_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match (state, data_type) {
		(AggState::SumInt { total, seen }, DataType::Int64) => {
			let value = if *seen {
				Some(i64::try_from(*total).map_err(|_| ExecutionError::Kernel {
					message: "integer SUM result does not fit Int64".to_string(),
				})?)
			} else {
				None
			};
			Ok(Arc::new(Int64Array::from(vec![value])) as ArrayRef)
		}
		(AggState::SumFloat { total, seen }, DataType::Float64) => {
			Ok(Arc::new(Float64Array::from(vec![seen.then_some(*total)])) as ArrayRef)
		}
		(AggState::SumInt { total, seen }, DataType::Float64) => {
			Ok(Arc::new(Float64Array::from(vec![seen.then_some(*total as f64)])) as ArrayRef)
		}
		// ADR 263: SUM(Decimal128) finalizes the i128 accumulator back to the column's (precision,
		// scale) (scalar global aggregate, no GROUP BY).
		(AggState::SumInt { total, seen }, DataType::Decimal128(precision, scale)) => Ok(Arc::new(
			Decimal128Array::from(vec![seen.then_some(*total)])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		)
			as ArrayRef),
		(other, data_type) => Err(ExecutionError::InvalidExpression {
			message: format!("cannot materialize {other:?} as SUM output {data_type:?}"),
		}),
	}
}

fn sum_column_from_groups(
	groups: &[BoundGroupedAggregateEntry<'_>],
	agg_idx: usize,
	data_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match data_type {
		DataType::Int64 => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::SumInt { total, seen } => {
						if seen {
							Ok::<Option<i64>, ExecutionError>(Some(i64::try_from(total).map_err(
								|_| ExecutionError::Kernel {
									message: "integer SUM result does not fit Int64".to_string(),
								},
							)?))
						} else {
							Ok(None)
						}
					}
					AggState::SumFloat { .. }
					| AggState::Count(_)
					| AggState::MinInt32 { .. }
					| AggState::MinInt64 { .. }
					| AggState::MinFloat32 { .. }
					| AggState::MinFloat64 { .. }
					| AggState::MinDecimal128 { .. }
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::MaxDecimal128 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. }
					| AggState::AvgDecimal { .. }
					| AggState::Quantile(_)
					| AggState::Spread { .. }
					| AggState::StringAgg(_)
					| AggState::ScalarSubqueryValue { .. } => Ok(None),
				})
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
		}
		DataType::Float64 => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::SumFloat { total, seen } => seen.then_some(total),
					AggState::SumInt { total, seen } => seen.then_some(total as f64),
					AggState::Count(_)
					| AggState::MinInt32 { .. }
					| AggState::MinInt64 { .. }
					| AggState::MinFloat32 { .. }
					| AggState::MinFloat64 { .. }
					| AggState::MinDecimal128 { .. }
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::MaxDecimal128 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. }
					| AggState::AvgDecimal { .. }
					| AggState::Quantile(_)
					| AggState::Spread { .. }
					| AggState::StringAgg(_)
					| AggState::ScalarSubqueryValue { .. } => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
		// ADR 263: grouped SUM(Decimal128) mirrors the global finalize — the per-group i128 `SumInt`
		// accumulator carries the raw running sum at the column's shared scale. Keeps bind and execute
		// in lockstep (the SUM-Decimal domain accepts Decimal128 at bind, so execute must too).
		DataType::Decimal128(precision, scale) => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::SumInt { total, seen } => seen.then_some(total),
					AggState::SumFloat { .. }
					| AggState::Count(_)
					| AggState::MinInt32 { .. }
					| AggState::MinInt64 { .. }
					| AggState::MinFloat32 { .. }
					| AggState::MinFloat64 { .. }
					| AggState::MinDecimal128 { .. }
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::MaxDecimal128 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. }
					| AggState::AvgDecimal { .. }
					| AggState::Quantile(_)
					| AggState::Spread { .. }
					| AggState::StringAgg(_)
					| AggState::ScalarSubqueryValue { .. } => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported SUM output type: {data_type:?}"),
		}),
	}
}

fn min_max_column_from_groups(
	groups: &[BoundGroupedAggregateEntry<'_>],
	agg_idx: usize,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Int32 => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::MinInt32 { value } | AggState::MaxInt32 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
		}
		DataType::Int64 => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::MinInt64 { value } | AggState::MaxInt64 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
		}
		DataType::Float32 => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::MinFloat32 { value } | AggState::MaxFloat32 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
		}
		DataType::Float64 => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::MinFloat64 { value } | AggState::MaxFloat64 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
		DataType::Decimal128(precision, scale) => {
			let values = groups
				.iter()
				.map(|entry| match entry.states[agg_idx] {
					AggState::MinDecimal128 { value } | AggState::MaxDecimal128 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported MIN/MAX output type: {output_type:?}"),
		}),
	}
}

fn avg_finalize(state: &AggState) -> Result<Option<f64>, ExecutionError> {
	match state {
		AggState::AvgInt { total, count } => {
			Ok((*count != 0).then_some(*total as f64 / *count as f64))
		}
		AggState::AvgFloat { total, count } => Ok((*count != 0).then_some(*total / *count as f64)),
		_ => Err(ExecutionError::InvalidExpression {
			message: "AVG finalize over non-AVG state".to_string(),
		}),
	}
}

// ADR 268: finalize AVG(Decimal). Rescale the i128 sum from the input scale to the widened output scale,
// then divide by the count rounding **half away from zero**, all in checked integer arithmetic. Returns the
// raw unscaled i128 (the caller stamps `(out_precision, out_scale)` onto the Decimal128 array).
pub(in crate::exec) fn avg_decimal_finalize(
	state: &AggState,
	out_precision: u8,
	out_scale: i8,
) -> Result<Option<i128>, ExecutionError> {
	let AggState::AvgDecimal {
		total,
		count,
		input_scale,
	} = state
	else {
		return Err(ExecutionError::InvalidExpression {
			message: "decimal AVG finalize over non-decimal-AVG state".to_string(),
		});
	};
	if *count == 0 {
		return Ok(None);
	}
	let scale_delta = i32::from(out_scale) - i32::from(*input_scale);
	if scale_delta < 0 {
		return Err(ExecutionError::Kernel {
			message: "AVG decimal output scale is below input scale".to_string(),
		});
	}
	let scale_overflow = || ExecutionError::Kernel {
		message: "AVG decimal scale overflow".to_string(),
	};
	let multiplier = 10_i128
		.checked_pow(scale_delta as u32)
		.ok_or_else(scale_overflow)?;
	let scaled = total.checked_mul(multiplier).ok_or_else(scale_overflow)?;
	let count = i128::from(*count);
	let mut quotient = scaled / count;
	let remainder = scaled % count;
	// Half away from zero: round up when twice the magnitude of the remainder reaches the divisor.
	if remainder.unsigned_abs() * 2 >= count.unsigned_abs() {
		quotient = quotient
			.checked_add(scaled.signum())
			.ok_or_else(|| ExecutionError::Kernel {
				message: "AVG decimal rounding overflow".to_string(),
			})?;
	}
	if !decimal_fits_precision(quotient, out_precision) {
		return Err(ExecutionError::Kernel {
			message: "AVG decimal result does not fit output precision".to_string(),
		});
	}
	Ok(Some(quotient))
}

pub(in crate::exec) fn avg_state_array(
	state: &AggState,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Decimal128(precision, scale) => {
			let value = avg_decimal_finalize(state, *precision, *scale)?;
			Ok(Arc::new(
				Decimal128Array::from(vec![value])
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => Ok(Arc::new(Float64Array::from(vec![avg_finalize(state)?])) as ArrayRef),
	}
}

fn avg_column_from_groups(
	groups: &[BoundGroupedAggregateEntry<'_>],
	agg_idx: usize,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Decimal128(precision, scale) => {
			let values = groups
				.iter()
				.map(|entry| avg_decimal_finalize(&entry.states[agg_idx], *precision, *scale))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => {
			let values = groups
				.iter()
				.map(|entry| avg_finalize(&entry.states[agg_idx]))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
	}
}

#[cfg(test)]
pub(in crate::exec) fn aggregate_state_keyword(function: &AggregateFunction) -> &'static str {
	aggregate_kernel::stable_spec_for_function(function).sql_name
}

#[cfg(test)]
pub(in crate::exec) fn aggregate_state_output_name(function: &AggregateFunction) -> &'static str {
	aggregate_kernel::stable_spec_for_function(function).output_name
}

/// ADR 475: the array's concrete type resolved **once**, so accumulation loops stop
/// paying an eleven-branch `as_any().downcast_ref()` chain per row (ADR 474 named
/// that chain as the owner of the m02 gap). The type set, conversions, and error
/// text mirror [`numeric_value`] exactly — the two resolve through this one place
/// so they cannot drift.
pub(in crate::exec) enum NumericArrayReader<'a> {
	Int8(&'a Int8Array),
	Int16(&'a Int16Array),
	Int32(&'a Int32Array),
	Int64(&'a Int64Array),
	UInt8(&'a UInt8Array),
	UInt16(&'a UInt16Array),
	UInt32(&'a UInt32Array),
	UInt64(&'a UInt64Array),
	Float32(&'a Float32Array),
	Float64(&'a Float64Array),
	/// ADR 263: raw (unscaled) integer into the accumulator; SUM finalize
	/// re-applies the column's (precision, scale).
	Decimal128(&'a Decimal128Array),
}

impl NumericArrayReader<'_> {
	/// The value at `row`, or `None` when the slot is null — the same
	/// null contract [`numeric_value`] has always had.
	pub(in crate::exec) fn value(&self, row: usize) -> Option<NumericValue> {
		let null = match self {
			Self::Int8(array) => array.is_null(row),
			Self::Int16(array) => array.is_null(row),
			Self::Int32(array) => array.is_null(row),
			Self::Int64(array) => array.is_null(row),
			Self::UInt8(array) => array.is_null(row),
			Self::UInt16(array) => array.is_null(row),
			Self::UInt32(array) => array.is_null(row),
			Self::UInt64(array) => array.is_null(row),
			Self::Float32(array) => array.is_null(row),
			Self::Float64(array) => array.is_null(row),
			Self::Decimal128(array) => array.is_null(row),
		};
		if null {
			return None;
		}
		Some(match self {
			Self::Int8(array) => NumericValue::Int(array.value(row) as i128),
			Self::Int16(array) => NumericValue::Int(array.value(row) as i128),
			Self::Int32(array) => NumericValue::Int(array.value(row) as i128),
			Self::Int64(array) => NumericValue::Int(array.value(row) as i128),
			Self::UInt8(array) => NumericValue::Int(array.value(row) as i128),
			Self::UInt16(array) => NumericValue::Int(array.value(row) as i128),
			Self::UInt32(array) => NumericValue::Int(array.value(row) as i128),
			Self::UInt64(array) => NumericValue::Int(array.value(row) as i128),
			Self::Float32(array) => NumericValue::Float(array.value(row) as f64),
			Self::Float64(array) => NumericValue::Float(array.value(row)),
			Self::Decimal128(array) => NumericValue::Int(array.value(row)),
		})
	}
}

pub(in crate::exec) fn numeric_array_reader(
	array: &ArrayRef,
) -> Result<NumericArrayReader<'_>, ExecutionError> {
	let any = array.as_any();
	if let Some(array) = any.downcast_ref::<Int8Array>() {
		return Ok(NumericArrayReader::Int8(array));
	}
	if let Some(array) = any.downcast_ref::<Int16Array>() {
		return Ok(NumericArrayReader::Int16(array));
	}
	if let Some(array) = any.downcast_ref::<Int32Array>() {
		return Ok(NumericArrayReader::Int32(array));
	}
	if let Some(array) = any.downcast_ref::<Int64Array>() {
		return Ok(NumericArrayReader::Int64(array));
	}
	if let Some(array) = any.downcast_ref::<UInt8Array>() {
		return Ok(NumericArrayReader::UInt8(array));
	}
	if let Some(array) = any.downcast_ref::<UInt16Array>() {
		return Ok(NumericArrayReader::UInt16(array));
	}
	if let Some(array) = any.downcast_ref::<UInt32Array>() {
		return Ok(NumericArrayReader::UInt32(array));
	}
	if let Some(array) = any.downcast_ref::<UInt64Array>() {
		return Ok(NumericArrayReader::UInt64(array));
	}
	if let Some(array) = any.downcast_ref::<Float32Array>() {
		return Ok(NumericArrayReader::Float32(array));
	}
	if let Some(array) = any.downcast_ref::<Float64Array>() {
		return Ok(NumericArrayReader::Float64(array));
	}
	if let Some(array) = any.downcast_ref::<Decimal128Array>() {
		return Ok(NumericArrayReader::Decimal128(array));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"aggregate expects numeric input, got {:?}",
			array.data_type()
		),
	})
}

pub(in crate::exec) fn numeric_value(
	array: &ArrayRef,
	row: usize,
) -> Result<Option<NumericValue>, ExecutionError> {
	if array.is_null(row) {
		return Ok(None);
	}
	if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
		return Ok(Some(NumericValue::Float(array.value(row) as f64)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
		return Ok(Some(NumericValue::Float(array.value(row))));
	}
	// ADR 263: a Decimal128 value enters the i128 accumulator as its raw (unscaled) integer; the
	// SUM finalize re-applies the column's (precision, scale). (SUM over Decimal.)
	if let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() {
		return Ok(Some(NumericValue::Int(array.value(row))));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"aggregate expects numeric input, got {:?}",
			array.data_type()
		),
	})
}

pub(in crate::exec) fn group_column(
	keys: &[Vec<GroupValue>],
	idx: usize,
	data_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	match data_type {
		arrow::datatypes::DataType::Boolean => Ok(Arc::new(BooleanArray::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Boolean(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Int32 => Ok(Arc::new(Int32Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Int32(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Int64 => Ok(Arc::new(Int64Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Int64(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Float32 => Ok(Arc::new(Float32Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Float32(value) => Ok(Some(f32::from_bits(*value))),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Float64 => Ok(Arc::new(Float64Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Float64(value) => Ok(Some(f64::from_bits(*value))),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Date32 => Ok(Arc::new(Date32Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Date32(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Ok(Arc::new(
				TimestampMillisecondArray::from(
					keys.iter()
						.map(|key| match &key[idx] {
							GroupValue::Null => Ok(None),
							GroupValue::Timestamp(TimeUnit::Millisecond, value) => Ok(Some(*value)),
							other => Err(group_type_error(data_type, other)),
						})
						.collect::<Result<Vec<_>, _>>()?,
				)
				.with_timezone("UTC"),
			))
		}
		arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Ok(Arc::new(
				TimestampMicrosecondArray::from(
					keys.iter()
						.map(|key| match &key[idx] {
							GroupValue::Null => Ok(None),
							GroupValue::Timestamp(TimeUnit::Microsecond, value) => Ok(Some(*value)),
							other => Err(group_type_error(data_type, other)),
						})
						.collect::<Result<Vec<_>, _>>()?,
				)
				.with_timezone("UTC"),
			))
		}
		arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Ok(Arc::new(
				TimestampNanosecondArray::from(
					keys.iter()
						.map(|key| match &key[idx] {
							GroupValue::Null => Ok(None),
							GroupValue::Timestamp(TimeUnit::Nanosecond, value) => Ok(Some(*value)),
							other => Err(group_type_error(data_type, other)),
						})
						.collect::<Result<Vec<_>, _>>()?,
				)
				.with_timezone("UTC"),
			))
		}
		arrow::datatypes::DataType::Utf8 => Ok(Arc::new(StringArray::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Utf8(value) => Ok(Some(value.as_str())),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		// ADR 275: rebuild the Decimal128 group key, carrying the exact (precision, scale) from the column type.
		arrow::datatypes::DataType::Decimal128(precision, scale) => {
			let values = keys
				.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Decimal128(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::InvalidExpression {
						message: format!("decimal128 group column: {error}"),
					})?,
			))
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported GROUP BY type: {data_type:?}"),
		}),
	}
}

fn group_type_error(data_type: &arrow::datatypes::DataType, value: &GroupValue) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!("group key value {value:?} does not match type {data_type:?}"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
	use crate::ir::{BoundExprProgramBuilder, ColumnName, ResolvedColumn};
	use crate::storage::InMemoryStorageScan;
	use arrow::datatypes::Field;

	fn column_program(
		name: &str,
		ordinal: ColumnOrdinal,
		data_type: DataType,
		nullable: bool,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let column = builder.append_column(ResolvedColumn {
			name: ColumnName(name.to_string()),
			ordinal,
			data_type,
			nullable,
			pbc_eligible: false,
		});
		builder.push_root(column);
		builder.build()
	}

	#[test]
	fn profiled_decimal_sum_preserves_unprofiled_output() {
		let input_schema = Arc::new(Schema::new(vec![Field::new(
			"amount",
			DataType::Decimal128(15, 2),
			false,
		)]));
		let amounts = Decimal128Array::from(vec![Some(125i128), Some(250), Some(-25)])
			.with_precision_and_scale(15, 2)
			.expect("decimal input");
		let batch = RecordBatch::try_new(input_schema, vec![Arc::new(amounts)])
			.expect("profile parity input");
		let calls = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: column_program(
				"amount",
				ColumnOrdinal(0),
				DataType::Decimal128(15, 2),
				false,
			),
		}];
		let output_schema = Arc::new(Schema::new(vec![Field::new(
			"sum_amount",
			DataType::Decimal128(25, 2),
			true,
		)]));
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		}));
		let context = ExecutionContext::local_test();
		let mut unprofiled =
			init_global_aggregate_state(&context, &calls).expect("unprofiled state");
		executor
			.fold_global_aggregate_state(&mut unprofiled, &calls, &batch)
			.expect("unprofiled fold");

		let predicate = column_program("flag", ColumnOrdinal(0), DataType::Boolean, false);
		let profile = super::join::GlobalFoldJoinFilterProfile::at_test_directory(
			std::env::temp_dir().join(format!("toron_global_fold_parity_{}", std::process::id())),
			&predicate,
		)
		.expect("test profile");
		let root = profile.begin_root().expect("begin profile root");
		let mut profiled = init_global_aggregate_state(&context, &calls).expect("profiled state");
		executor
			.fold_global_decimal_sum_state_profiled(&mut profiled, &calls, &batch, &profile)
			.expect("profiled fold");
		profile.finish_root(root).expect("finish profile root");

		assert_eq!(
			finalize_global_aggregate_state(&unprofiled, &calls, &output_schema)
				.expect("unprofiled output"),
			finalize_global_aggregate_state(&profiled, &calls, &output_schema)
				.expect("profiled output")
		);
	}

	#[test]
	fn generic_grouped_hash_finalize_is_order_sensitive_and_keeps_mixed_states_paired() {
		let input_schema = Arc::new(Schema::new(vec![
			Field::new("group_int", DataType::Int64, true),
			Field::new("group_text", DataType::Utf8, false),
			Field::new("value", DataType::Int64, true),
		]));
		let input = RecordBatch::try_new(
			input_schema,
			vec![
				Arc::new(Int64Array::from(vec![
					Some(2),
					Some(1),
					Some(2),
					None,
					Some(1),
					None,
				])),
				Arc::new(StringArray::from(vec!["b", "a", "b", "z", "a", "z"])),
				Arc::new(Int64Array::from(vec![
					Some(10),
					Some(20),
					None,
					Some(5),
					Some(30),
					Some(7),
				])),
			],
		)
		.expect("mixed grouped input");
		let mut group_builder = BoundExprProgramBuilder::new();
		let group_int = group_builder.append_column(ResolvedColumn {
			name: ColumnName("group_int".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: true,
			pbc_eligible: false,
		});
		let group_text = group_builder.append_column(ResolvedColumn {
			name: ColumnName("group_text".to_string()),
			ordinal: ColumnOrdinal(1),
			data_type: DataType::Utf8,
			nullable: false,
			pbc_eligible: false,
		});
		group_builder.push_root(group_int);
		group_builder.push_root(group_text);
		let group_expr = group_builder.build();
		let value = || column_program("value", ColumnOrdinal(2), DataType::Int64, true);
		let aggr_expr = vec![
			BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgramBuilder::new().build(),
			},
			BoundAggregateCall {
				function: AggregateFunction::Count,
				args: value(),
			},
			BoundAggregateCall {
				function: AggregateFunction::Sum,
				args: value(),
			},
			BoundAggregateCall {
				function: AggregateFunction::Min,
				args: value(),
			},
			BoundAggregateCall {
				function: AggregateFunction::Max,
				args: value(),
			},
			BoundAggregateCall {
				function: AggregateFunction::Avg,
				args: value(),
			},
		];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("group_int", DataType::Int64, true),
			Field::new("group_text", DataType::Utf8, false),
			Field::new("count_all", DataType::Int64, false),
			Field::new("count_value", DataType::Int64, false),
			Field::new("sum_value", DataType::Int64, true),
			Field::new("min_value", DataType::Int64, true),
			Field::new("max_value", DataType::Int64, true),
			Field::new("avg_value", DataType::Float64, true),
		]));
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		}));
		let output = executor
			.bound_grouped_aggregate(&[input], &group_expr, &aggr_expr, &output_schema)
			.expect("generic grouped output");
		let expected = RecordBatch::try_new(
			output_schema,
			vec![
				Arc::new(Int64Array::from(vec![None, Some(1), Some(2)])),
				Arc::new(StringArray::from(vec!["z", "a", "b"])),
				Arc::new(Int64Array::from(vec![2, 2, 2])),
				Arc::new(Int64Array::from(vec![2, 2, 1])),
				Arc::new(Int64Array::from(vec![12, 50, 10])),
				Arc::new(Int64Array::from(vec![5, 20, 10])),
				Arc::new(Int64Array::from(vec![7, 30, 10])),
				Arc::new(Float64Array::from(vec![6.0, 25.0, 10.0])),
			],
		)
		.expect("expected grouped output");
		assert_eq!(output, vec![expected]);
	}

	#[test]
	fn adr546_one_grouped_sketch_serves_distinct_quartile_parameters() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("group", DataType::Utf8, false),
			Field::new("value", DataType::Int64, false),
		]));
		let input = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(StringArray::from(vec!["g"; 9])),
				Arc::new(Int64Array::from((-4_i64..=4).collect::<Vec<_>>())),
			],
		)
		.expect("quantile input");
		let group_expr = column_program("group", ColumnOrdinal(0), DataType::Utf8, false);
		let value = || column_program("value", ColumnOrdinal(1), DataType::Int64, false);
		let calls = [0.25, 0.5, 0.75]
			.into_iter()
			.map(|q| BoundAggregateCall {
				function: AggregateFunction::ApproxQuantile(q),
				args: value(),
			})
			.collect::<Vec<_>>();

		let mut states = init_bound_states_with_seed(&calls, [7; 32], b"group-g")
			.expect("shared quantile states");
		let sketches = states
			.iter()
			.map(|state| match state {
				AggState::Quantile(quantile) => quantile.sketch.clone(),
				other => panic!("unexpected state {other:?}"),
			})
			.collect::<Vec<_>>();
		assert!(Arc::ptr_eq(&sketches[0], &sketches[1]));
		assert!(Arc::ptr_eq(&sketches[1], &sketches[2]));
		let skewed = Arc::new(Int64Array::from(
			std::iter::repeat_n(0_i64, 1_600)
				.chain(std::iter::repeat_n(1_i64, 800))
				.chain(2_i64..402)
				.collect::<Vec<_>>(),
		)) as ArrayRef;
		for row in 0..skewed.len() {
			for state in &mut states {
				update_quantile_state(state, &skewed, row).expect("fold skewed quantile");
			}
		}
		let quartiles = states
			.iter()
			.map(finalize_quantile_state)
			.collect::<Result<Vec<_>, _>>()
			.expect("finalize shared skewed sketch")
			.into_iter()
			.map(|value| value.expect("nonempty skewed quantile"))
			.collect::<Vec<_>>();
		assert!(quartiles.windows(2).all(|pair| pair[0] <= pair[1]));
		assert!(
			sketches[0]
				.lock()
				.expect("shared quantile lock")
				.retained_item_count()
				< skewed.len(),
			"the shared-state ordering control must force compaction"
		);

		let output_schema = Arc::new(Schema::new(vec![
			Field::new("group", DataType::Utf8, false),
			Field::new("lower_quartile", DataType::Float64, true),
			Field::new("median", DataType::Float64, true),
			Field::new("upper_quartile", DataType::Float64, true),
		]));
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		}));
		let output = executor
			.bound_grouped_aggregate(&[input], &group_expr, &calls, &output_schema)
			.expect("grouped quantiles");
		let batch = &output[0];
		let observed = (1..=3)
			.map(|column| {
				batch
					.column(column)
					.as_any()
					.downcast_ref::<Float64Array>()
					.expect("quantile column")
					.value(0)
			})
			.collect::<Vec<_>>();
		assert_eq!(observed, vec![-2.0, 0.0, 2.0]);
	}

	#[test]
	fn adr546_global_initialization_consumes_the_execution_seed_and_admission() {
		let call = BoundAggregateCall {
			function: AggregateFunction::ApproxQuantile(0.5),
			args: column_program("value", ColumnOrdinal(0), DataType::Int64, false),
		};
		let seed = [0x54; 32];
		let context = ExecutionContext::local_test().with_quantile_execution(seed, 1);
		let states = init_global_aggregate_state(&context, &[call]).expect("global quantile state");
		let AggState::Quantile(quantile) = &states[0] else {
			panic!("global quantile must use the canonical sketch state");
		};
		let wendelon::schema::aggregate::AggregateState::Kll {
			seed: state_seed, ..
		} = quantile
			.sketch
			.lock()
			.expect("quantile state lock")
			.clone()
			.into_state()
		else {
			panic!("quantile sketch must serialize as KLL state");
		};
		assert_eq!(state_seed, seed);
		assert_eq!(
			context
				.quantile_states_admitted
				.load(std::sync::atomic::Ordering::Acquire),
			1,
			"global initialization must consume this terminal task's allowance"
		);
	}

	#[test]
	fn adr546_state_count_cap_refuses_independently_of_the_byte_budget() {
		let admitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		reserve_quantile_states(
			admitted.clone(),
			MAX_QUANTILE_STATES_PER_EXECUTION,
			MAX_QUANTILE_STATES_PER_EXECUTION,
		)
		.expect("the declared confidence population fits exactly")
		.commit();

		let error =
			match reserve_quantile_states(admitted.clone(), MAX_QUANTILE_STATES_PER_EXECUTION, 1) {
				Ok(_) => panic!("state 1025 must refuse even if bytes remain"),
				Err(error) => error,
			};
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "MAX_QUANTILE_STATES_PER_EXECUTION=1024",
				requested,
				available,
				..
			} if requested.get() == 1_025 && available.get() == 1_024
		));
		assert_eq!(
			admitted.load(std::sync::atomic::Ordering::Acquire),
			1_024,
			"a refused reservation must not mutate admission state"
		);
	}

	#[test]
	fn adr546_grouped_memory_guard_charges_the_declared_sketch_before_construction() {
		const DECLARED_INT64_SKETCH_BYTES: u64 = 5_601 * 8 + 512;
		let admission = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let mut groups = BoundGroupedAggregateGroups::with_test_profile_lookup(4, |_| None);
		groups.configure_quantile_admission(
			1,
			DECLARED_INT64_SKETCH_BYTES,
			MAX_QUANTILE_STATES_PER_EXECUTION,
			admission.clone(),
		);
		groups.set_growth_budget_bytes(Some(u64::MAX));
		groups
			.entry_or_init(&[GroupValue::Int64(1)], 1, || Ok(vec![AggState::Count(0)]))
			.expect("first group");

		let before = groups.current_resident_bytes();
		groups
			.set_growth_budget_bytes(Some(before.saturating_add(DECLARED_INT64_SKETCH_BYTES - 1)));
		let constructor_called = std::cell::Cell::new(false);
		let error = groups
			.entry_or_init(&[GroupValue::Int64(2)], 1, || {
				constructor_called.set(true);
				Ok(vec![AggState::Count(0)])
			})
			.expect_err("the second declared sketch must exceed the small task budget");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "generic bound grouped-fold hash table",
				..
			}
		));
		assert!(
			!constructor_called.get(),
			"refusal must precede state construction"
		);
		assert_eq!(admission.load(std::sync::atomic::Ordering::Acquire), 1);
	}

	#[test]
	fn primitive_int64_sum_groups_are_compact_guarded_and_exact() {
		let mut groups = PrimitiveInt64Groups::new();
		let cap = 1024 * 1024;
		for key in 0..1_025i64 {
			groups.insert(key, i128::from(key), cap).expect("insert");
		}
		groups.insert(10, 5, cap).expect("duplicate sum");
		groups.insert(i64::MIN, 7, cap).expect("full-domain key");
		assert_eq!(groups.entry_limit, 2_048);
		assert_eq!(groups.keys.len(), 1_026);
		assert_eq!(groups.buckets.len(), 4_096);
		assert_eq!(groups.accounted_bytes(), 65_536);
		assert_eq!(groups.peak_bytes, 98_304);
		assert_eq!(groups.growth_count, 2);
		let ten = groups.lookup_entry(10).expect("lookup").expect("key 10");
		assert_eq!(groups.totals[ten], 15);
		assert!(
			groups
				.lookup_entry(99_999)
				.expect("missing lookup")
				.is_none()
		);
	}

	#[test]
	fn primitive_int64_sum_growth_guard_is_preallocation() {
		let mut groups = PrimitiveInt64Groups::new();
		let initial_bytes = primitive_capacity_bytes::<i64>(1024)
			.saturating_add(primitive_capacity_bytes::<i128>(1024))
			.saturating_add(primitive_capacity_bytes::<u32>(2048));
		let error = groups
			.insert(1, 2, initial_bytes - 1)
			.expect_err("guard must reject the complete allocation");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: PRIMITIVE_INT64_PRODUCER,
				..
			}
		));
		assert!(groups.keys.is_empty());
		assert!(groups.totals.is_empty());
		assert!(groups.buckets.is_empty());
	}

	#[test]
	fn primitive_int64_decimal_sum_matches_generic_grouped_fold() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("quantity", DataType::Decimal128(15, 2), false),
		]));
		let quantities = Decimal128Array::from(vec![100_i128, 200, 350, -50])
			.with_precision_and_scale(15, 2)
			.expect("decimal quantities");
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2, 1, 2, i64::MIN])),
				Arc::new(quantities),
			],
		)
		.expect("input batch");
		let group_expr = column_program("order_key", ColumnOrdinal(0), DataType::Int64, false);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: column_program(
				"quantity",
				ColumnOrdinal(1),
				DataType::Decimal128(15, 2),
				false,
			),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("sum_quantity", DataType::Decimal128(25, 2), true),
		]));
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		}));
		let generic = executor
			.bound_grouped_aggregate(&[batch.clone()], &group_expr, &aggr_expr, &output_schema)
			.expect("generic grouped fold");
		let mut primitive = PrimitiveInt64Groups::new();
		let keys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let values = batch
			.column(1)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.unwrap();
		for row in 0..batch.num_rows() {
			primitive
				.insert(keys.value(row), values.value(row), 1024 * 1024)
				.expect("primitive insert");
		}
		let primitive = primitive
			.finish(&output_schema, 64 * 1024 * 1024)
			.expect("primitive finish");
		assert_eq!(
			canonical_arrow_ipc_digest(&primitive, SortPolicy::FullRowLex)
				.expect("primitive digest"),
			canonical_arrow_ipc_digest(&generic, SortPolicy::FullRowLex).expect("generic digest")
		);
	}

	#[test]
	fn adr495_primitive_int64_count_matches_generic_grouped_fold() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"order_key",
			DataType::Int64,
			false,
		)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![2, 1, 2, i64::MIN]))],
		)
		.expect("input batch");
		let group_expr = column_program("order_key", ColumnOrdinal(0), DataType::Int64, false);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::CountAll,
			args: BoundExprProgram::empty(),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("row_count", DataType::Int64, false),
		]));
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		}));
		let generic = executor
			.bound_grouped_aggregate(&[batch.clone()], &group_expr, &aggr_expr, &output_schema)
			.expect("generic grouped count");
		let keys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 keys");
		let mut primitive = PrimitiveInt64Groups::new();
		for key in keys.values() {
			primitive
				.insert(*key, 1, 1024 * 1024)
				.expect("primitive count insert");
		}
		let primitive = primitive
			.finish(&output_schema, 64 * 1024 * 1024)
			.expect("primitive count finish");
		assert_eq!(
			canonical_arrow_ipc_digest(&primitive, SortPolicy::FullRowLex)
				.expect("primitive digest"),
			canonical_arrow_ipc_digest(&generic, SortPolicy::FullRowLex).expect("generic digest")
		);
	}

	#[test]
	fn primitive_int64_sum_int64_finalize_checks_output_range() {
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("key", DataType::Int64, false),
			Field::new("sum", DataType::Int64, true),
		]));
		let mut valid = PrimitiveInt64Groups::new();
		valid.insert(1, 10, 1024 * 1024).expect("first value");
		valid.insert(1, -3, 1024 * 1024).expect("second value");
		let output = valid
			.finish(&output_schema, 64 * 1024 * 1024)
			.expect("valid Int64 output");
		let sums = output[0]
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 sum");
		assert_eq!(sums.value(0), 7);

		let mut overflow = PrimitiveInt64Groups::new();
		overflow
			.insert(1, i128::from(i64::MAX) + 1, 1024 * 1024)
			.expect("i128 accumulator accepts value");
		assert!(overflow.finish(&output_schema, 64 * 1024 * 1024).is_err());
	}

	#[test]
	fn primitive_int64_sum_selection_is_structural() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("quantity", DataType::Decimal128(15, 2), false),
		]));
		let scan = ExecutionPlan::EmptyRelation {
			schema: schema.clone(),
		};
		let group_expr = column_program("order_key", ColumnOrdinal(0), DataType::Int64, false);
		let args = column_program(
			"quantity",
			ColumnOrdinal(1),
			DataType::Decimal128(15, 2),
			false,
		);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args,
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("sum_quantity", DataType::Decimal128(25, 2), true),
		]));
		let input = StreamingScanGroupedInput {
			scan_input: StreamingScanGlobalInput {
				scan: &scan,
				scan_filter_predicate: None,
				project: None,
				project_filter_predicate: None,
			},
			exchange: Some((4, &group_expr)),
		};
		assert!(
			primitive_int64_sum_spec(&input, &group_expr, &aggr_expr, &output_schema).is_some()
		);
		let nullable_group = column_program("order_key", ColumnOrdinal(0), DataType::Int64, true);
		let nullable_input = StreamingScanGroupedInput {
			scan_input: input.scan_input,
			exchange: Some((4, &nullable_group)),
		};
		assert!(
			primitive_int64_sum_spec(&nullable_input, &nullable_group, &aggr_expr, &output_schema)
				.is_none()
		);
		let mut count = aggr_expr.clone();
		count[0].function = AggregateFunction::Count;
		assert!(primitive_int64_sum_spec(&input, &group_expr, &count, &output_schema).is_none());
		let nullable_args = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: column_program(
				"quantity",
				ColumnOrdinal(1),
				DataType::Decimal128(15, 2),
				true,
			),
		}];
		assert!(
			primitive_int64_sum_spec(&input, &group_expr, &nullable_args, &output_schema).is_none()
		);
		let filtered_input = StreamingScanGroupedInput {
			scan_input: StreamingScanGlobalInput {
				scan_filter_predicate: Some(&group_expr),
				..input.scan_input
			},
			exchange: input.exchange,
		};
		assert!(
			primitive_int64_sum_spec(&filtered_input, &group_expr, &aggr_expr, &output_schema)
				.is_none()
		);
		let no_exchange = StreamingScanGroupedInput {
			scan_input: input.scan_input,
			exchange: None,
		};
		assert!(
			primitive_int64_sum_spec(&no_exchange, &group_expr, &aggr_expr, &output_schema)
				.is_none()
		);
	}

	#[test]
	fn adr495_primitive_int64_count_selection_is_structural_and_budgeted_by_default() {
		assert!(primitive_int64_count_enabled_from_env(None));
		assert!(primitive_int64_count_enabled_from_env(Some("1")));
		assert!(!primitive_int64_count_enabled_from_env(Some("0")));

		let schema = Arc::new(Schema::new(vec![Field::new(
			"order_key",
			DataType::Int64,
			false,
		)]));
		let scan = ExecutionPlan::EmptyRelation {
			schema: schema.clone(),
		};
		let group_expr = column_program("order_key", ColumnOrdinal(0), DataType::Int64, false);
		let mut literal = BoundExprProgramBuilder::new();
		let one = literal.append_literal(ScalarValue::Int64(Some(1)));
		literal.push_root(one);
		let count = vec![BoundAggregateCall {
			function: AggregateFunction::Count,
			args: literal.build(),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("row_count", DataType::Int64, false),
		]));
		let input = StreamingScanGroupedInput {
			scan_input: StreamingScanGlobalInput {
				scan: &scan,
				scan_filter_predicate: None,
				project: None,
				project_filter_predicate: None,
			},
			exchange: Some((4, &group_expr)),
		};
		assert!(primitive_int64_count_spec(&input, &group_expr, &count, &output_schema).is_some());

		let count_all = vec![BoundAggregateCall {
			function: AggregateFunction::CountAll,
			args: BoundExprProgram::empty(),
		}];
		assert!(
			primitive_int64_count_spec(&input, &group_expr, &count_all, &output_schema).is_some()
		);

		let column_count = vec![BoundAggregateCall {
			function: AggregateFunction::Count,
			args: group_expr.clone(),
		}];
		assert!(
			primitive_int64_count_spec(&input, &group_expr, &column_count, &output_schema)
				.is_none()
		);

		// The compact fold assumes the exchange co-locates every group's rows on one node. An
		// exchange that repartitions on a different key would split a group across partitions and
		// undercount; reject it exactly as the SUM path does.
		let other_key = column_program("other_key", ColumnOrdinal(1), DataType::Int64, false);
		let mismatched_exchange = StreamingScanGroupedInput {
			scan_input: input.scan_input,
			exchange: Some((4, &other_key)),
		};
		assert!(
			primitive_int64_count_spec(&mismatched_exchange, &group_expr, &count, &output_schema)
				.is_none()
		);

		let no_exchange = StreamingScanGroupedInput {
			scan_input: input.scan_input,
			exchange: None,
		};
		assert!(
			primitive_int64_count_spec(&no_exchange, &group_expr, &count, &output_schema).is_none()
		);
	}

	// ADR 268: the decimal AVG finalize rescales to the widened output scale and divides half away from
	// zero, in pure integer arithmetic. Pins the rounding sign (positive AND negative ties) and the
	// non-divisible case the discriminating execute test can't reach from a 2-row fixture.
	#[test]
	fn avg_decimal_finalize_rounds_half_away_from_zero() {
		let avg = |total: i128, count: u64, input_scale: i8, p: u8, s: i8| {
			avg_decimal_finalize(
				&AggState::AvgDecimal {
					total,
					count,
					input_scale,
				},
				p,
				s,
			)
			.expect("finalize ok")
		};
		// Non-divisible: AVG(0.01, 0.02, 0.02) at scale 2 -> sum raw 5, count 3, widen to scale 6:
		// 5*10^4/3 = 50000/3 = 16666.67 -> 16667 (0.016667).
		assert_eq!(avg(5, 3, 2, 19, 6), Some(16667));
		// Exact: AVG(0.06, 0.05) -> sum 11, count 2 -> 110000/2 = 55000 (0.055000), no rounding.
		assert_eq!(avg(11, 2, 2, 19, 6), Some(55000));
		// Positive tie: 5/2 = 2.5 -> 3 (away from zero).
		assert_eq!(avg(5, 2, 0, 19, 0), Some(3));
		// Negative tie: -5/2 = -2.5 -> -3 (away from zero, not -2).
		assert_eq!(avg(-5, 2, 0, 19, 0), Some(-3));
		// Empty group -> NULL.
		assert_eq!(avg(0, 0, 2, 19, 6), None);
	}

	#[test]
	fn avg_decimal_state_rejects_unrepresentable_widening() {
		assert!(
			matches!(
				avg_state_for_data_type(&DataType::Decimal128(15, 2)),
				Ok(AggState::AvgDecimal { .. })
			),
			"ordinary Decimal128 AVG state is allowed"
		);
		assert!(
			avg_state_for_data_type(&DataType::Decimal128(38, 35)).is_err(),
			"scale 35 would widen to scale 39, which cannot fit Decimal128"
		);
	}
	// ADR 275: group_value reads a Decimal128 key as the raw i128 (NULL handled by the leading null-check).
	#[test]
	fn adr477_reader_matches_group_value_for_every_supported_type() {
		use arrow::array::{
			BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
			TimestampMillisecondArray,
		};
		// Parity across the whole variant set, nulls included: the reader and the
		// per-row path must resolve identically or grouping silently diverges.
		let arrays: Vec<ArrayRef> = vec![
			Arc::new(BooleanArray::from(vec![Some(true), None])),
			Arc::new(Int64Array::from(vec![Some(7), None])),
			Arc::new(Float64Array::from(vec![Some(-0.0), None])),
			Arc::new(Date32Array::from(vec![Some(19_000), None])),
			Arc::new(TimestampMillisecondArray::from(vec![Some(1_700_000), None])),
			Arc::new(StringArray::from(vec![Some("R"), None])),
			Arc::new(
				Decimal128Array::from(vec![Some(12_345i128), None])
					.with_precision_and_scale(15, 2)
					.expect("decimal array"),
			),
		];
		for array in arrays {
			let reader = group_array_reader(&array).expect("reader");
			for row in 0..array.len() {
				assert_eq!(
					reader.value(row),
					group_value(&array, row).expect("group value"),
					"reader/group_value parity at row {row}"
				);
			}
		}
		// Unsupported types keep the identical error text.
		let unsupported: ArrayRef = Arc::new(arrow::array::UInt8Array::from(vec![1u8]));
		let reader_err = match group_array_reader(&unsupported) {
			Err(error) => error.to_string(),
			Ok(_) => panic!("unsupported GROUP BY type must not resolve"),
		};
		let value_err = group_value(&unsupported, 0).unwrap_err().to_string();
		assert_eq!(reader_err, value_err);
	}

	#[test]
	fn group_value_reads_decimal128_raw_i128() {
		let array: ArrayRef = Arc::new(
			Decimal128Array::from(vec![Some(12_345_i128), None, Some(-67_800_i128)])
				.with_precision_and_scale(15, 2)
				.expect("decimal column"),
		);
		assert_eq!(
			group_value(&array, 0).expect("group value"),
			GroupValue::Decimal128(12_345)
		);
		assert_eq!(
			group_value(&array, 1).expect("group value"),
			GroupValue::Null
		);
		assert_eq!(
			group_value(&array, 2).expect("group value"),
			GroupValue::Decimal128(-67_800)
		);
	}

	// ADR 275: group_column rebuilds the Decimal128 group key with the EXACT (precision, scale) from the column
	// type and the exact raw i128 values (incl. NULL). A digest-only integration check can't pin the core
	// (p,s) round-trip.
	#[test]
	fn group_column_rebuilds_decimal128_with_exact_precision_scale_and_values() {
		let keys = vec![
			vec![GroupValue::Decimal128(12_345)],
			vec![GroupValue::Decimal128(-67_800)],
			vec![GroupValue::Null],
		];
		let column = group_column(&keys, 0, &arrow::datatypes::DataType::Decimal128(15, 2))
			.expect("group column");
		assert_eq!(
			column.data_type(),
			&arrow::datatypes::DataType::Decimal128(15, 2)
		);
		let decimals = column
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("decimal128 column");
		assert_eq!(decimals.value(0), 12_345);
		assert_eq!(decimals.value(1), -67_800);
		assert!(decimals.is_null(2));
	}
}
#[test]
fn adr494_borrow_single_utf8_group_key_ablation_defaults_on() {
	assert!(borrow_single_utf8_group_key_enabled_from_env(None));
	assert!(borrow_single_utf8_group_key_enabled_from_env(Some("1")));
	assert!(!borrow_single_utf8_group_key_enabled_from_env(Some("0")));
}

#[test]
fn adr499_local_grouped_exchange_bypass_defaults_on_with_faithful_control() {
	assert!(local_grouped_exchange_bypass_enabled_from_env(None));
	assert!(local_grouped_exchange_bypass_enabled_from_env(Some("1")));
	assert!(!local_grouped_exchange_bypass_enabled_from_env(Some("0")));
}

#[cfg(test)]
mod adr560_spread_tests {
	use super::*;

	fn accumulate(values: &[f64]) -> (u64, f64) {
		let (mut count, mut mean, mut m2) = (0u64, 0.0f64, 0.0f64);
		for value in values {
			spread_accumulate(&mut count, &mut mean, &mut m2, *value);
		}
		(count, m2)
	}

	/// The naive form is what this decision exists to avoid, so the control
	/// computes it too and asserts it visibly fails on the same data. A test
	/// that only checked Welford would pass under either algorithm.
	#[test]
	fn welford_is_exact_where_the_naive_sum_of_squares_collapses() {
		let base = 1.0e9_f64;
		let values = [base + 4.0, base + 7.0, base + 13.0, base + 16.0];
		let (count, m2) = accumulate(&values);
		let welford = spread_finalize(&AggregateFunction::VarPop, count, m2).expect("var_pop");
		assert!(
			(welford - 22.5).abs() < 1e-9,
			"Welford must be exact, got {welford}"
		);

		let n = values.len() as f64;
		let sum: f64 = values.iter().sum();
		let sum_squares: f64 = values.iter().map(|value| value * value).sum();
		let naive = sum_squares / n - (sum / n) * (sum / n);
		assert!(
			(naive - 22.5).abs() > 1.0,
			"the naive form must visibly fail here, or this control proves nothing; got {naive}"
		);
	}

	#[test]
	fn sample_forms_are_null_below_two_observations_and_population_is_zero_at_one() {
		let (count, m2) = accumulate(&[5.0]);
		assert_eq!(
			spread_finalize(&AggregateFunction::VarSamp, count, m2),
			None,
			"one observation carries no sample spread"
		);
		assert_eq!(
			spread_finalize(&AggregateFunction::StddevSamp, count, m2),
			None
		);
		assert_eq!(
			spread_finalize(&AggregateFunction::VarPop, count, m2),
			Some(0.0),
			"a single observation is a population with no spread"
		);
		assert_eq!(
			spread_finalize(&AggregateFunction::StddevPop, count, m2),
			Some(0.0)
		);

		let (empty_count, empty_m2) = accumulate(&[]);
		for function in [
			AggregateFunction::VarPop,
			AggregateFunction::VarSamp,
			AggregateFunction::StddevPop,
			AggregateFunction::StddevSamp,
		] {
			assert_eq!(
				spread_finalize(&function, empty_count, empty_m2),
				None,
				"an empty group has no spread of any kind"
			);
		}
	}

	#[test]
	fn standard_deviation_is_the_square_root_of_its_variance() {
		let (count, m2) = accumulate(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
		let var_pop = spread_finalize(&AggregateFunction::VarPop, count, m2).expect("var_pop");
		let stddev_pop =
			spread_finalize(&AggregateFunction::StddevPop, count, m2).expect("stddev_pop");
		assert!((stddev_pop - var_pop.sqrt()).abs() < 1e-12);
		assert!((var_pop - 4.0).abs() < 1e-12, "got {var_pop}");

		let var_samp = spread_finalize(&AggregateFunction::VarSamp, count, m2).expect("var_samp");
		let stddev_samp =
			spread_finalize(&AggregateFunction::StddevSamp, count, m2).expect("stddev_samp");
		assert!((stddev_samp - var_samp.sqrt()).abs() < 1e-12);
		assert!(
			var_samp > var_pop,
			"the sample form divides by a smaller denominator"
		);
	}

	#[test]
	fn a_non_finite_input_propagates_rather_than_erroring_or_being_skipped() {
		let (count, m2) = accumulate(&[1.0, f64::NAN, 3.0]);
		assert_eq!(count, 3, "a NaN is an observation, unlike a NULL");
		let value = spread_finalize(&AggregateFunction::VarPop, count, m2).expect("var_pop");
		assert!(value.is_nan(), "a non-finite input propagates, got {value}");
	}
}

/// ADR 561: the accumulator family is registry metadata; this init applies the
/// per-type constructor the executor owns for each family. Lives beside those
/// constructors rather than in the exec root.
pub(in crate::exec) fn fresh_window_agg_state(
	function: &AggregateFunction,
	arg_type: Option<&DataType>,
) -> Result<Option<AggState>, ExecutionError> {
	let require_arg = || ExecutionError::InvalidExpression {
		message: "window aggregate requires an argument type".to_string(),
	};
	Ok(
		match aggregate_kernel::stable_spec_for_function(function).state_family {
			aggregate_kernel::AggregateStateFamily::Count => None,
			aggregate_kernel::AggregateStateFamily::Sum => {
				Some(sum_state_for_data_type(arg_type.ok_or_else(require_arg)?))
			}
			aggregate_kernel::AggregateStateFamily::Avg => {
				Some(avg_state_for_data_type(arg_type.ok_or_else(require_arg)?)?)
			}
			aggregate_kernel::AggregateStateFamily::Min => {
				Some(min_state_for_data_type(arg_type.ok_or_else(require_arg)?)?)
			}
			aggregate_kernel::AggregateStateFamily::Max => {
				Some(max_state_for_data_type(arg_type.ok_or_else(require_arg)?)?)
			}
			// Non-window families carry no window accumulator; the planner never
			// admits them and the reject arms assert that agreement.
			aggregate_kernel::AggregateStateFamily::Quantile
			| aggregate_kernel::AggregateStateFamily::Spread
			| aggregate_kernel::AggregateStateFamily::StringAgg
			| aggregate_kernel::AggregateStateFamily::ScalarSubqueryValue
			| aggregate_kernel::AggregateStateFamily::Experimental => None,
		},
	)
}

/// ADR 562: one ordering key value, captured per row for the string-agg buffer.
///
/// This exists because `GroupValue` orders floats by canonical bits — right for
/// grouping equality, wrong for sorting. `Null` is greatest so an ascending
/// sort places it last; a descending key reverses the comparison and places it
/// first, which is the Postgres default placement this cut adopts.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec) enum SortKeyValue {
	Int(i128),
	Float(f64),
	Text(String),
	Null,
}

impl SortKeyValue {
	fn rank(&self) -> u8 {
		match self {
			Self::Int(_) => 0,
			Self::Float(_) => 1,
			Self::Text(_) => 2,
			Self::Null => 3,
		}
	}

	fn compare(&self, other: &Self) -> std::cmp::Ordering {
		match (self, other) {
			(Self::Int(left), Self::Int(right)) => left.cmp(right),
			(Self::Float(left), Self::Float(right)) => left.total_cmp(right),
			(Self::Text(left), Self::Text(right)) => left.cmp(right),
			// A key position is one bound expression, so cross-variant
			// comparison only happens against Null; rank order keeps it total.
			_ => self.rank().cmp(&other.rank()),
		}
	}
}

/// ADR 562: the buffered accumulator behind `AggregateStateFamily::StringAgg`.
/// Every (keys, value) pair is held until finalization — the unbounded buffer
/// the ADR names, gated on state accounting before adversarial use.
#[derive(Debug, Clone, Default)]
pub(in crate::exec) struct StringAggState {
	rows: Vec<(Vec<SortKeyValue>, String)>,
}

fn string_agg_key_value(array: &ArrayRef, row: usize) -> Result<SortKeyValue, ExecutionError> {
	if array.is_null(row) {
		return Ok(SortKeyValue::Null);
	}
	let any = array.as_any();
	if let Some(array) = any.downcast_ref::<Int32Array>() {
		return Ok(SortKeyValue::Int(i128::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<Int64Array>() {
		return Ok(SortKeyValue::Int(i128::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<Date32Array>() {
		return Ok(SortKeyValue::Int(i128::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<TimestampMillisecondArray>() {
		return Ok(SortKeyValue::Int(i128::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<TimestampMicrosecondArray>() {
		return Ok(SortKeyValue::Int(i128::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<TimestampNanosecondArray>() {
		return Ok(SortKeyValue::Int(i128::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<Float32Array>() {
		return Ok(SortKeyValue::Float(f64::from(array.value(row))));
	}
	if let Some(array) = any.downcast_ref::<Float64Array>() {
		return Ok(SortKeyValue::Float(array.value(row)));
	}
	if let Some(array) = any.downcast_ref::<StringArray>() {
		return Ok(SortKeyValue::Text(array.value(row).to_owned()));
	}
	if let Some(array) = any.downcast_ref::<LargeStringArray>() {
		return Ok(SortKeyValue::Text(array.value(row).to_owned()));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"STRING_AGG ordering key type {:?} is not orderable here",
			array.data_type()
		),
	})
}

fn string_agg_value(array: &ArrayRef, row: usize) -> Result<Option<String>, ExecutionError> {
	if array.is_null(row) {
		return Ok(None);
	}
	let any = array.as_any();
	if let Some(array) = any.downcast_ref::<StringArray>() {
		return Ok(Some(array.value(row).to_owned()));
	}
	if let Some(array) = any.downcast_ref::<LargeStringArray>() {
		return Ok(Some(array.value(row).to_owned()));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"STRING_AGG expects a string value, got {:?}",
			array.data_type()
		),
	})
}

/// Accumulates one row: a NULL value contributes neither element nor
/// separator; NULL keys are captured and placed by the sort.
pub(in crate::exec) fn string_agg_accumulate_row(
	state: &mut StringAggState,
	value_array: &ArrayRef,
	key_arrays: &[ArrayRef],
	row: usize,
) -> Result<(), ExecutionError> {
	let Some(value) = string_agg_value(value_array, row)? else {
		return Ok(());
	};
	let mut keys = Vec::with_capacity(key_arrays.len());
	for array in key_arrays {
		keys.push(string_agg_key_value(array, row)?);
	}
	state.rows.push((keys, value));
	Ok(())
}

/// Sorts by the keys with per-key directions and joins. `None` is the NULL
/// answer for an empty or all-NULL group. The sort is stable, so tied keys
/// keep arrival order — arrival is what the `Sensitive` classification admits.
pub(in crate::exec) fn string_agg_finalize(
	state: &StringAggState,
	call: &crate::ir::StringAggCall,
) -> Option<String> {
	if state.rows.is_empty() {
		return None;
	}
	let mut ordered: Vec<&(Vec<SortKeyValue>, String)> = state.rows.iter().collect();
	ordered.sort_by(|left, right| {
		for (index, (left_key, right_key)) in left.0.iter().zip(right.0.iter()).enumerate() {
			let mut ordering = left_key.compare(right_key);
			if call.descending.get(index).copied().unwrap_or(false) {
				ordering = ordering.reverse();
			}
			if ordering != std::cmp::Ordering::Equal {
				return ordering;
			}
		}
		std::cmp::Ordering::Equal
	});
	Some(
		ordered
			.iter()
			.map(|(_, value)| value.as_str())
			.collect::<Vec<_>>()
			.join(&call.separator),
	)
}

/// Builds the output column at the schema's width, so a `LargeUtf8` value does
/// not silently narrow.
pub(in crate::exec) fn string_agg_column(
	values: Vec<Option<String>>,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Utf8 => Ok(Arc::new(values.into_iter().collect::<StringArray>()) as ArrayRef),
		DataType::LargeUtf8 => {
			Ok(Arc::new(values.into_iter().collect::<LargeStringArray>()) as ArrayRef)
		}
		other => Err(ExecutionError::InvalidExpression {
			message: format!("STRING_AGG output must be a string type, got {other:?}"),
		}),
	}
}

#[cfg(test)]
mod adr562_string_agg_tests {
	use super::*;

	/// A negative float must order numerically. `GroupValue` orders floats by
	/// canonical bits — where every negative's bits exceed every positive's —
	/// so this control fails if the key representation ever routes through it.
	#[test]
	fn float_keys_order_numerically_not_by_bits() {
		assert_eq!(
			SortKeyValue::Float(-2.0).compare(&SortKeyValue::Float(1.0)),
			std::cmp::Ordering::Less
		);
		assert_eq!(
			SortKeyValue::Float(-30.5).compare(&SortKeyValue::Float(-10.5)),
			std::cmp::Ordering::Less
		);
		assert_eq!(
			SortKeyValue::Float(f64::NEG_INFINITY).compare(&SortKeyValue::Float(0.0)),
			std::cmp::Ordering::Less
		);
		// NULL is greatest, so ascending places it last; a descending key
		// reverses and places it first.
		assert_eq!(
			SortKeyValue::Float(f64::INFINITY).compare(&SortKeyValue::Null),
			std::cmp::Ordering::Less
		);
	}

	#[test]
	fn large_utf8_values_keep_their_width_and_accumulate() {
		let values = Arc::new(LargeStringArray::from(vec![
			Some("beta"),
			Some("alpha"),
			None,
		])) as ArrayRef;
		let keys = Arc::new(Int64Array::from(vec![Some(2_i64), Some(1), Some(0)])) as ArrayRef;
		let mut state = StringAggState::default();
		for row in 0..3 {
			string_agg_accumulate_row(&mut state, &values, std::slice::from_ref(&keys), row)
				.expect("accumulate LargeUtf8 row");
		}
		let call = crate::ir::StringAggCall {
			separator: "-".into(),
			descending: vec![false],
		};
		assert_eq!(
			string_agg_finalize(&state, &call).as_deref(),
			Some("alpha-beta"),
			"the NULL value row contributes neither element nor separator"
		);
		let column = string_agg_column(
			vec![string_agg_finalize(&state, &call)],
			&DataType::LargeUtf8,
		)
		.expect("LargeUtf8 output column");
		assert_eq!(
			column.data_type(),
			&DataType::LargeUtf8,
			"a LargeUtf8 value must not narrow to Utf8"
		);
	}
}
