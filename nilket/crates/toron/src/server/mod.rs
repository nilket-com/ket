pub mod memory;
pub mod spill;
pub mod transport;

use self::memory::MemoryManager;
use self::transport::ToronTransport;
use crate::binder::PlannerConfig;
use crate::catalog::Catalog;
use crate::distributed::budget::{BudgetDimension, ByteCount};
use crate::distributed::identity::{FrameByteLen, PartitionId, StreamSeq, TaskIdentity, WorkerId};
use crate::distributed::partition_store::{
	InMemoryPartitionStore, PartitionPayload, PartitionStore, PartitionStoreError,
};
use crate::distributed::scheduler::{DispatchOutcome, TaskDispatcher};
use crate::distributed::task::{
	ArrowIpcDigest, PartitionAddress, PartitionBackend, PartitionManifest, ProducerPartitionRef,
	TaskCompletionOutput, TaskInputBinding, TaskOutputBinding, TaskSpec, TaskTelemetry,
	TaskTelemetryLabels, shuffle_input_table_id,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::error::{
	PartitionMissingReason, QueryError, RuntimeError, StorageError, TaskFailureKind, WireError,
};
use crate::exec::{ExecutionContext, Executor};
use crate::ir::{ExecutionPlan, JoinStrategy, MorselId, TableId};
use crate::storage::{ScanRequest, ScanStream, StorageScan};
use crate::wire::{
	ArrowIpcBytes, CancelTaskPayload, EnvelopeIdentity, MessageKind, PartitionDataPayload,
	PartitionMissingPayload, ProtocolVersion, PullPartitionPayload, StartTaskPayload,
	TaskBatchPayload, TaskCancelledPayload, TaskCompletePayload, TaskFailedPayload,
	TaskStartedPayload, WireEnvelope, decode_payload, encode_payload,
};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wendelon::schema::RuntimeFilterDescriptor;

pub struct ToronWorker {
	storage: Arc<dyn StorageScan>,
	memory: Arc<MemoryManager>,
	partition_store: Arc<dyn PartitionStore>,
	worker_id: WorkerId,
	endpoint: WorkerEndpoint,
}

struct TaskExecutionReport {
	results: Vec<RecordBatch>,
	telemetry: TaskTelemetry,
}

#[derive(Default)]
struct ShuffleInputTelemetry {
	pulled_shuffle_bytes: u64,
	left_shuffle_bytes: u64,
	right_shuffle_bytes: u64,
}

impl ToronWorker {
	pub fn new(
		storage: Arc<dyn StorageScan>,
		memory: Arc<MemoryManager>,
		catalog: Arc<dyn Catalog>,
	) -> Self {
		Self::with_planner_config(storage, memory, catalog, PlannerConfig::default())
	}

	pub fn with_planner_config(
		storage: Arc<dyn StorageScan>,
		memory: Arc<MemoryManager>,
		_catalog: Arc<dyn Catalog>,
		_planner_config: PlannerConfig,
	) -> Self {
		Self::with_partition_store(storage, memory, Arc::new(InMemoryPartitionStore::new()))
	}

	pub fn with_partition_store(
		storage: Arc<dyn StorageScan>,
		memory: Arc<MemoryManager>,
		partition_store: Arc<dyn PartitionStore>,
	) -> Self {
		Self::with_partition_store_and_endpoint(
			storage,
			memory,
			partition_store,
			WorkerId::new(),
			WorkerEndpoint::new("local"),
		)
	}

	pub fn with_partition_store_and_endpoint(
		storage: Arc<dyn StorageScan>,
		memory: Arc<MemoryManager>,
		partition_store: Arc<dyn PartitionStore>,
		worker_id: WorkerId,
		endpoint: WorkerEndpoint,
	) -> Self {
		Self {
			storage,
			memory,
			partition_store,
			worker_id,
			endpoint,
		}
	}

	pub async fn execute_task_local(
		&self,
		spec: &TaskSpec,
	) -> Result<Vec<RecordBatch>, QueryError> {
		self.execute_task_local_with_telemetry(spec)
			.await
			.map(|report| report.results)
	}

	async fn execute_task_local_with_telemetry(
		&self,
		spec: &TaskSpec,
	) -> Result<TaskExecutionReport, QueryError> {
		let task_started = Instant::now();
		let telemetry = Arc::new(Mutex::new(task_telemetry_for_spec(spec)));
		validate_broadcast_caps(spec)?;
		let morsel = scan_share_morsel(spec);
		let shuffle_started = Instant::now();
		let (overrides, shuffle_telemetry) = self.shuffle_input_overrides(spec).await?;
		record_worker_telemetry(&telemetry, |task| {
			task.phases.shuffle_pull_decode_micros = duration_micros(shuffle_started);
			task.counters.pulled_shuffle_bytes = shuffle_telemetry.pulled_shuffle_bytes;
			task.counters.left_shuffle_bytes = shuffle_telemetry.left_shuffle_bytes;
			task.counters.right_shuffle_bytes = shuffle_telemetry.right_shuffle_bytes;
		});
		let mut task_context = ExecutionContext::new(
			spec.context.session_id,
			spec.identity.query_id,
			spec.context.tenant_id,
			spec.context.read_bound,
		)
		.with_task_budget(spec.budget)
		.with_task_telemetry(telemetry.clone())
		.with_runtime_filter(scan_runtime_filter(spec));
		task_context.trace_context = spec.context.trace_context;
		let results = if overrides.is_empty() {
			let executor = Executor::with_context(self.storage.clone(), task_context);
			executor.execute_bound(&spec.plan, morsel).await?
		} else {
			let executor = Executor::with_context(
				Arc::new(TaskInputStorageScan {
					base: self.storage.clone(),
					legacy_single_override: legacy_single_shuffle_override(&spec.plan, &overrides),
					overrides,
				}),
				task_context,
			);
			executor.execute_bound(&spec.plan, morsel).await?
		};
		record_worker_telemetry(&telemetry, |task| {
			task.counters.emitted_rows =
				results.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
			task.counters.emitted_batches = results.len() as u64;
			task.phases.task_wall_micros = duration_micros(task_started);
			recompute_framework_overhead(task);
		});
		Ok(TaskExecutionReport {
			results,
			telemetry: snapshot_worker_telemetry(&telemetry),
		})
	}

	async fn pull_shuffle_inputs(
		&self,
		partitions: &[ProducerPartitionRef],
	) -> Result<(Vec<RecordBatch>, u64), QueryError> {
		let coordinator = ToronCoordinator::new(self.memory.clone());
		let mut batches = Vec::new();
		let mut pulled_bytes = 0u64;

		for partition in partitions {
			let stream = tokio::net::TcpStream::connect(partition.endpoint.as_str()).await?;
			let payload = coordinator
				.pull_partition_from_worker(stream, partition.address, partition.address.attempt_id)
				.await?;
			pulled_bytes = pulled_bytes.saturating_add(payload.payload.as_slice().len() as u64);
			batches.extend(decode_record_batches(payload.payload.as_slice())?);
		}

		Ok((batches, pulled_bytes))
	}

	async fn shuffle_input_overrides(
		&self,
		spec: &TaskSpec,
	) -> Result<(HashMap<TableId, ScanOverride>, ShuffleInputTelemetry), QueryError> {
		let mut overrides: HashMap<TableId, ScanOverride> = HashMap::new();
		let mut telemetry = ShuffleInputTelemetry::default();

		for (input_index, input) in spec.inputs.iter().enumerate() {
			let TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_partitions,
				..
			} = input
			else {
				continue;
			};
			let table_id = shuffle_input_table_id(*exchange_id);
			let (batches, pulled_bytes) = self.pull_shuffle_inputs(producer_partitions).await?;
			telemetry.pulled_shuffle_bytes =
				telemetry.pulled_shuffle_bytes.saturating_add(pulled_bytes);
			match shuffle_input_side_for_telemetry(spec, input_index) {
				ShuffleInputSide::Left => {
					telemetry.left_shuffle_bytes =
						telemetry.left_shuffle_bytes.saturating_add(pulled_bytes);
				}
				ShuffleInputSide::Right => {
					telemetry.right_shuffle_bytes =
						telemetry.right_shuffle_bytes.saturating_add(pulled_bytes);
				}
				ShuffleInputSide::Unknown => {}
			}
			let schema = batches
				.first()
				.map(RecordBatch::schema)
				.or_else(|| scan_schema_for_table_id(&spec.plan, table_id))
				.unwrap_or_else(|| spec.plan.schema());
			if let Some(override_entry) = overrides.get_mut(&table_id) {
				override_entry.batches.extend(batches);
			} else {
				overrides.insert(table_id, ScanOverride { schema, batches });
			}
		}

		Ok((overrides, telemetry))
	}

	fn publish_task_output(
		&self,
		identity: TaskIdentity,
		spec: &TaskSpec,
		results: &[RecordBatch],
	) -> Result<TaskCompletionOutput, QueryError> {
		let TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key: _,
		} = &spec.output
		else {
			return Ok(TaskCompletionOutput::FinalResult {
				output_digest: None,
			});
		};

		let partition_count_usize =
			usize::try_from(partition_count.get()).expect("partition count fits usize");
		if results.len() > partition_count_usize {
			return Err(QueryError::Internal {
				message: format!(
					"shuffle task produced {} partitions for declared count {}",
					results.len(),
					partition_count.get()
				),
			});
		}

		let mut payloads = Vec::with_capacity(results.len());
		for (idx, batch) in results.iter().enumerate() {
			let partition_id =
				PartitionId::new(u32::try_from(idx).expect("partition index fits partition id"));
			let address = PartitionAddress {
				query_id: identity.query_id,
				exchange_id: *exchange_id,
				producer_stage_id: identity.stage_id,
				producer_task_id: identity.task_id,
				attempt_id: identity.attempt_id,
				partition_id,
			};
			let bytes = encode_record_batch(batch)?;
			let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
			payloads.push(PartitionPayload::new(
				address,
				bytes,
				crate::distributed::task::RowCount::new(
					u64::try_from(batch.num_rows()).expect("row count fits u64"),
				),
				digest,
			));
		}

		let manifest = PartitionManifest {
			query_id: identity.query_id,
			stage_id: identity.stage_id,
			task_id: identity.task_id,
			attempt_id: identity.attempt_id,
			exchange_id: *exchange_id,
			producer_worker_id: self.worker_id,
			producer_endpoint: self.endpoint.clone(),
			partitions: payloads.iter().map(PartitionPayload::entry).collect(),
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let manifest = self
			.partition_store
			.publish(manifest, payloads)
			.map_err(partition_store_publish_error)?;
		Ok(TaskCompletionOutput::ShuffleProducer { manifest })
	}

	pub async fn handle_connection<S>(&self, mut stream: S) -> Result<(), QueryError>
	where
		S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		loop {
			let envelope = match read_envelope(&mut stream, self.memory.as_ref()).await {
				Ok(envelope) => envelope,
				Err(QueryError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
					break;
				}
				Err(error) => return Err(error),
			};
			envelope
				.validate_runtime(ProtocolVersion::CURRENT, FrameByteLen::from_u32(u32::MAX))?;

			match envelope.kind {
				MessageKind::StartTask => {
					let payload: StartTaskPayload = decode_payload(&envelope.payload)?;
					let identity = payload.spec.identity;
					let task_started = Instant::now();
					if envelope.identity != EnvelopeIdentity::task(identity) {
						return Err(QueryError::UnexpectedMessage {
							expected: "StartTask route matching TaskSpec identity",
						});
					}

					send_payload(
						&mut stream,
						MessageKind::TaskStarted,
						EnvelopeIdentity::task(identity),
						None,
						&TaskStartedPayload { identity },
					)
					.await?;
					let task_setup_micros = duration_micros(task_started);

					match self.execute_task_local_with_telemetry(&payload.spec).await {
						Ok(mut report) => {
							for (idx, batch) in report.results.iter().enumerate() {
								let stream_seq = StreamSeq::new(
									u64::try_from(idx).expect("record batch index fits u64"),
								);
								let batch_encode_started = Instant::now();
								let encoded_batch = encode_record_batch(&batch)?;
								report.telemetry.phases.batch_encode_micros = report
									.telemetry
									.phases
									.batch_encode_micros
									.saturating_add(duration_micros(batch_encode_started));
								let batch_send_started = Instant::now();
								send_payload(
									&mut stream,
									MessageKind::TaskBatch,
									EnvelopeIdentity::task(identity),
									Some(stream_seq),
									&TaskBatchPayload {
										identity,
										stream_seq,
										payload: ArrowIpcBytes::new(encoded_batch),
									},
								)
								.await?;
								report.telemetry.phases.batch_send_micros = report
									.telemetry
									.phases
									.batch_send_micros
									.saturating_add(duration_micros(batch_send_started));
							}

							let publish_started = Instant::now();
							match self.publish_task_output(identity, &payload.spec, &report.results)
							{
								Ok(output) => {
									report.telemetry.phases.output_publish_micros =
										duration_micros(publish_started);
									report.telemetry.counters.published_shuffle_bytes =
										published_shuffle_bytes(&output);
									report.telemetry.phases.task_setup_micros = task_setup_micros;
									report.telemetry.phases.task_wall_micros =
										duration_micros(task_started);
									recompute_framework_overhead(&mut report.telemetry);
									send_payload(
										&mut stream,
										MessageKind::TaskComplete,
										EnvelopeIdentity::task(identity),
										None,
										&TaskCompletePayload {
											identity,
											output,
											telemetry: Some(report.telemetry),
										},
									)
									.await?;
								}
								Err(error) => {
									send_task_failed(&mut stream, identity, error).await?;
								}
							}
						}
						Err(error) => {
							send_task_failed(&mut stream, identity, error).await?;
						}
					}
				}
				MessageKind::CancelTask => {
					let identity = task_envelope_identity(envelope.identity)?;
					send_payload(
						&mut stream,
						MessageKind::TaskCancelled,
						EnvelopeIdentity::task(identity),
						None,
						&TaskCancelledPayload { identity },
					)
					.await?;
				}
				MessageKind::PullPartition => {
					let payload: PullPartitionPayload = decode_payload(&envelope.payload)?;
					if envelope.identity != EnvelopeIdentity::partition(payload.address) {
						return Err(QueryError::UnexpectedMessage {
							expected: "PullPartition envelope identity matching address",
						});
					}

					match self
						.partition_store
						.pull(payload.address, payload.expected_attempt)
					{
						Ok(partition) => {
							send_payload(
								&mut stream,
								MessageKind::PartitionData,
								EnvelopeIdentity::partition(partition.address),
								Some(StreamSeq::new(0)),
								&PartitionDataPayload {
									address: partition.address,
									stream_seq: StreamSeq::new(0),
									payload: ArrowIpcBytes::new(partition.bytes),
								},
							)
							.await?;
						}
						Err(reason) => {
							send_payload(
								&mut stream,
								MessageKind::PartitionMissing,
								EnvelopeIdentity::partition(payload.address),
								None,
								&PartitionMissingPayload {
									address: payload.address,
									reason,
								},
							)
							.await?;
						}
					}
				}
				_ => {
					return Err(QueryError::UnexpectedMessage {
						expected: "StartTask, CancelTask, or PullPartition",
					});
				}
			}
		}

		Ok(())
	}
}

fn scan_share_morsel(spec: &TaskSpec) -> Option<(MorselId, u32)> {
	spec.inputs.iter().find_map(|input| match input {
		TaskInputBinding::Scan {
			share: Some(share), ..
		} => Some((MorselId(share.worker_index), share.worker_count)),
		TaskInputBinding::Scan { share: None, .. } | TaskInputBinding::ShuffleConsumer { .. } => {
			None
		}
	})
}

fn scan_runtime_filter(spec: &TaskSpec) -> Option<RuntimeFilterDescriptor> {
	spec.inputs.iter().find_map(|input| match input {
		TaskInputBinding::Scan {
			runtime_filter: Some(runtime_filter),
			..
		} => Some(runtime_filter.clone()),
		TaskInputBinding::Scan {
			runtime_filter: None,
			..
		}
		| TaskInputBinding::ShuffleConsumer { .. } => None,
	})
}

fn duration_micros(started: Instant) -> u64 {
	u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn record_worker_telemetry(
	telemetry: &Arc<Mutex<TaskTelemetry>>,
	update: impl FnOnce(&mut TaskTelemetry),
) {
	let lock_started = Instant::now();
	if let Ok(mut telemetry) = telemetry.lock() {
		telemetry.phases.telemetry_lock_micros = telemetry
			.phases
			.telemetry_lock_micros
			.saturating_add(duration_micros(lock_started));
		update(&mut telemetry);
	}
}

fn snapshot_worker_telemetry(telemetry: &Arc<Mutex<TaskTelemetry>>) -> TaskTelemetry {
	telemetry
		.lock()
		.map(|telemetry| telemetry.clone())
		.unwrap_or_default()
}

fn recompute_framework_overhead(telemetry: &mut TaskTelemetry) {
	let accounted = telemetry
		.phases
		.shuffle_pull_decode_micros
		.saturating_add(telemetry.phases.build_micros)
		.saturating_add(telemetry.phases.probe_micros)
		.saturating_add(telemetry.phases.spill_micros)
		.saturating_add(telemetry.phases.output_publish_micros)
		.saturating_add(telemetry.phases.task_setup_micros)
		.saturating_add(telemetry.phases.batch_encode_micros)
		.saturating_add(telemetry.phases.batch_send_micros)
		.saturating_add(telemetry.phases.telemetry_lock_micros);
	telemetry.phases.framework_overhead_micros =
		telemetry.phases.task_wall_micros.saturating_sub(accounted);
}

fn published_shuffle_bytes(output: &TaskCompletionOutput) -> u64 {
	match output {
		TaskCompletionOutput::FinalResult { .. } => 0,
		TaskCompletionOutput::ShuffleProducer { manifest } => manifest
			.partitions
			.iter()
			.map(|entry| entry.size_bytes.get())
			.sum(),
	}
}

fn task_telemetry_for_spec(spec: &TaskSpec) -> TaskTelemetry {
	TaskTelemetry {
		labels: TaskTelemetryLabels {
			strategy: join_strategy_label(&spec.plan).map(str::to_string),
			stage_role: stage_role_label(spec).to_string(),
			operator_kind: operator_kind_label(&spec.plan).to_string(),
			worker_count: worker_count_label(spec),
		},
		..TaskTelemetry::default()
	}
}

fn join_strategy_label(plan: &ExecutionPlan) -> Option<&'static str> {
	match plan {
		ExecutionPlan::Join { strategy, .. } => Some(match strategy {
			JoinStrategy::HashPartitioned => "hash_partitioned",
			JoinStrategy::Broadcast => "broadcast",
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => join_strategy_label(input),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

fn contains_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => contains_join(input),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => false,
	}
}

fn operator_kind_label(plan: &ExecutionPlan) -> &'static str {
	if contains_join(plan) {
		"join"
	} else {
		match plan {
			ExecutionPlan::Exchange { .. } => "exchange",
			ExecutionPlan::Filter { .. } => "filter",
			ExecutionPlan::Project { .. } => "project",
			ExecutionPlan::Aggregate { .. } => "aggregate",
			ExecutionPlan::Sort { .. } => "sort",
			ExecutionPlan::Scan { .. } => "scan",
			ExecutionPlan::Join { .. } => "join",
			ExecutionPlan::CreateTable { .. } => "catalog",
			ExecutionPlan::CreateTableAsSelect { .. } => "create_table_as_select",
			ExecutionPlan::DropTable { .. } => "drop_table",
			ExecutionPlan::Delete { .. } => "delete",
			ExecutionPlan::Insert { .. } => "insert",
			ExecutionPlan::InsertSelect { .. } => "insert_select",
		}
	}
}

fn stage_role_label(spec: &TaskSpec) -> &'static str {
	if contains_join(&spec.plan) {
		return match &spec.output {
			TaskOutputBinding::FinalResult => "join_final",
			TaskOutputBinding::ShuffleProducer { .. } => "join_publish",
		};
	}
	if matches!(spec.output, TaskOutputBinding::ShuffleProducer { .. }) {
		"source_publish"
	} else if spec
		.inputs
		.iter()
		.any(|input| matches!(input, TaskInputBinding::ShuffleConsumer { .. }))
	{
		"shuffle_consumer"
	} else {
		"scan"
	}
}

fn worker_count_label(spec: &TaskSpec) -> Option<u32> {
	spec.inputs
		.iter()
		.find_map(|input| match input {
			TaskInputBinding::Scan {
				share: Some(share), ..
			} => Some(share.worker_count),
			TaskInputBinding::Scan { share: None, .. }
			| TaskInputBinding::ShuffleConsumer { .. } => None,
		})
		.or_else(|| match &spec.output {
			TaskOutputBinding::ShuffleProducer {
				partition_count, ..
			} => Some(partition_count.get()),
			TaskOutputBinding::FinalResult => None,
		})
		.or_else(|| {
			spec.inputs.iter().find_map(|input| match input {
				TaskInputBinding::ShuffleConsumer {
					producer_partitions,
					..
				} => u32::try_from(producer_partitions.len()).ok(),
				TaskInputBinding::Scan { .. } => None,
			})
		})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShuffleInputSide {
	Left,
	Right,
	Unknown,
}

fn shuffle_input_side_for_telemetry(spec: &TaskSpec, input_index: usize) -> ShuffleInputSide {
	if matches!(join_strategy_label(&spec.plan), Some("broadcast")) {
		return ShuffleInputSide::Left;
	}
	let shuffle_index = spec
		.inputs
		.iter()
		.take(input_index + 1)
		.filter(|input| matches!(input, TaskInputBinding::ShuffleConsumer { .. }))
		.count();
	match shuffle_index {
		1 => ShuffleInputSide::Left,
		2 => ShuffleInputSide::Right,
		_ => ShuffleInputSide::Unknown,
	}
}

fn validate_broadcast_caps(spec: &TaskSpec) -> Result<(), QueryError> {
	if !contains_broadcast_join(&spec.plan) {
		return Ok(());
	}

	let build_bytes = spec
		.inputs
		.iter()
		.filter_map(|input| match input {
			TaskInputBinding::ShuffleConsumer {
				producer_partitions,
				..
			} => Some(producer_partitions),
			TaskInputBinding::Scan { .. } => None,
		})
		.flatten()
		.fold(0u64, |total, partition| {
			total.saturating_add(partition.size_bytes.get())
		});
	let worker_count = spec
		.inputs
		.iter()
		.find_map(|input| match input {
			TaskInputBinding::Scan {
				share: Some(share), ..
			} => Some(share.worker_count),
			TaskInputBinding::Scan { share: None, .. }
			| TaskInputBinding::ShuffleConsumer { .. } => None,
		})
		.unwrap_or(1);

	if build_bytes > spec.budget.broadcast_build_bytes.get() {
		return Err(broadcast_cap_exhausted(
			build_bytes,
			spec.budget.broadcast_build_bytes.get(),
		));
	}

	let replicated = build_bytes.saturating_mul(u64::from(worker_count));
	if replicated > spec.budget.broadcast_replicated_bytes.get() {
		return Err(broadcast_cap_exhausted(
			replicated,
			spec.budget.broadcast_replicated_bytes.get(),
		));
	}

	Ok(())
}

fn broadcast_cap_exhausted(requested: u64, available: u64) -> QueryError {
	QueryError::Runtime(RuntimeError::TaskFailure {
		kind: TaskFailureKind::BudgetExhausted {
			dimension: BudgetDimension::BroadcastReplication,
			requested: ByteCount::new(requested),
			available: ByteCount::new(available),
		},
	})
}

fn contains_broadcast_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} => {
			*strategy == JoinStrategy::Broadcast
				|| contains_broadcast_join(left)
				|| contains_broadcast_join(right)
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => contains_broadcast_join(input),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{QueryBudget, TaskBudget};
	use crate::distributed::identity::{AttemptId, ExchangeId, QueryId, StageId, TaskId};
	use crate::distributed::task::{
		ArrowIpcDigest, PartitionAddress, PartitionCount, ProducerPartitionRef,
	};
	use crate::ir::{BoundExprProgram, ColumnOrdinal, JoinKind, QueryProfileHint};
	use crate::storage::InMemoryStorageScan;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, RuntimeFilterBloom,
		RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};

	#[test]
	fn scan_runtime_filter_reads_scan_binding_descriptor() {
		let plan = scan(TableId(1), 1);
		let ExecutionPlan::Scan { fingerprint, .. } = &plan else {
			panic!("scan plan");
		};
		let runtime_filter = RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![0],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 7,
				bit_count: 8,
				hash_count: 1,
				byte_budget: 1,
				target_fp_rate_bp: 100,
				payload: vec![0b0101_0101],
			}),
			probe_descriptor_fingerprint: *fingerprint,
		};
		let spec = TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(1),
				StageId::new(1),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			plan,
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: Some(runtime_filter.clone()),
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};

		assert_eq!(scan_runtime_filter(&spec), Some(runtime_filter));
	}

	#[test]
	fn rune_sandbox_dispatch_failure_is_non_retryable_task_failed() {
		let error = QueryError::Frontend(crate::error::FrontendError::RuneSandbox {
			kind: crate::rune_engine::RuneSandboxViolation::FuelExhausted {
				fuel_consumed: 10,
				fuel_budget: 10,
			},
			message: "fuel exhausted".to_string(),
		});

		let kind = dispatch_failure_kind(error);

		assert!(matches!(
			kind,
			TaskFailureKind::TaskFailed { retryable: false, ref reason }
				if reason == "RuneSandbox::FuelExhausted"
		));
	}

	#[test]
	fn broadcast_cap_validation_charges_replicated_build_bytes() {
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(64);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(10);
		let spec = TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(1),
				StageId::new(1),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			plan: ExecutionPlan::Join {
				left: Box::new(scan(TableId(1), 1)),
				right: Box::new(scan(TableId(2), 2)),
				kind: JoinKind::Inner,
				left_keys: vec![ColumnOrdinal(0)],
				right_keys: vec![ColumnOrdinal(0)],
				strategy: JoinStrategy::Broadcast,
				output_schema: Arc::new(Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("id", DataType::Int64, false),
				])),
			},
			inputs: vec![
				TaskInputBinding::Scan {
					share: Some(crate::distributed::task::ScanShare {
						worker_index: 0,
						worker_count: 2,
					}),
					runtime_filter: None,
				},
				TaskInputBinding::ShuffleConsumer {
					exchange_id: ExchangeId::new(0),
					producer_stage_id: StageId::new(0),
					partition_id: PartitionId::new(0),
					producer_partitions: vec![ProducerPartitionRef {
						address: PartitionAddress {
							query_id: QueryId::from_u128(1),
							exchange_id: ExchangeId::new(0),
							producer_stage_id: StageId::new(0),
							producer_task_id: TaskId::new(0),
							attempt_id: AttemptId::first(),
							partition_id: PartitionId::new(0),
						},
						worker_id: WorkerId::from_u128(1),
						endpoint: WorkerEndpoint::new("127.0.0.1:0"),
						size_bytes: ByteCount::new(8),
						digest: ArrowIpcDigest::new([0; 32]),
					}],
				},
			],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(query_budget),
			trace: None,
		};

		let err = validate_broadcast_caps(&spec).expect_err("replicated cap must fail");

		assert!(matches!(
			err,
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::BroadcastReplication,
					requested,
					available,
				}
			}) if requested == ByteCount::new(16) && available == ByteCount::new(10)
		));
	}

	#[tokio::test]
	async fn worker_sends_task_failed_when_shuffle_publish_budget_exhausted() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
		)
		.expect("batch");
		let mut budget = QueryBudget::test_default();
		budget.shuffle_bytes_published_per_task = ByteCount::new(1);
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let bind_listener = tokio::net::TcpListener::bind;
		let listener = bind_listener("127.0.0.1:0").await.expect("bind listener");
		let endpoint =
			WorkerEndpoint::new(listener.local_addr().expect("listener addr").to_string());
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::with_budget(budget)),
			WorkerId::from_u128(7),
			endpoint.clone(),
		));
		let spec = TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(7),
				StageId::new(0),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id: ExchangeId::new(1),
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.expect("accept worker connection");
			worker.handle_connection(server_stream).await
		});
		let client = tokio::net::TcpStream::connect(endpoint.as_str())
			.await
			.expect("connect worker");

		let err = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(client, spec)
			.await
			.expect_err("publish budget exhaustion must surface as TaskFailed");

		assert!(matches!(
			err,
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::ShuffleBytes,
					..
				}
			})
		));
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	fn scan(table_id: TableId, object_byte: u8) -> ExecutionPlan {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([object_byte; 16]),
			1,
			"default",
			"broadcast_cap_test",
			schema.as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor");
		ExecutionPlan::Scan {
			legacy_table_id: table_id,
			table: descriptor.table_object_id,
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
		}
	}
}

#[derive(Clone)]
struct ScanOverride {
	schema: SchemaRef,
	batches: Vec<RecordBatch>,
}

struct TaskInputStorageScan {
	base: Arc<dyn StorageScan>,
	overrides: HashMap<TableId, ScanOverride>,
	legacy_single_override: Option<ScanOverride>,
}

#[async_trait::async_trait]
impl StorageScan for TaskInputStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		if let Some(override_entry) = self.overrides.get(&request.table_id) {
			return Ok(ScanStream::from_batches(
				override_entry.schema.clone(),
				request.session_id,
				request.query_id,
				override_entry.batches.clone(),
			));
		}
		if let Some(override_entry) = &self.legacy_single_override {
			return Ok(ScanStream::from_batches(
				override_entry.schema.clone(),
				request.session_id,
				request.query_id,
				override_entry.batches.clone(),
			));
		}

		self.base.scan(request).await
	}
}

fn legacy_single_shuffle_override(
	plan: &ExecutionPlan,
	overrides: &HashMap<TableId, ScanOverride>,
) -> Option<ScanOverride> {
	if overrides.len() != 1 {
		return None;
	}
	if overrides
		.keys()
		.any(|table_id| scan_schema_for_table_id(plan, *table_id).is_some())
	{
		return None;
	}
	overrides.values().next().cloned()
}

fn scan_schema_for_table_id(plan: &ExecutionPlan, table_id: TableId) -> Option<SchemaRef> {
	match plan {
		ExecutionPlan::Scan {
			legacy_table_id,
			schema,
			..
		} if *legacy_table_id == table_id => Some(schema.clone()),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => scan_schema_for_table_id(input, table_id),
		ExecutionPlan::Join { left, right, .. } => scan_schema_for_table_id(left, table_id)
			.or_else(|| scan_schema_for_table_id(right, table_id)),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

pub struct ToronCoordinator {
	memory: Arc<MemoryManager>,
}

#[derive(Debug)]
pub struct RemoteTaskCompletion {
	pub batches: Vec<RecordBatch>,
	pub output: TaskCompletionOutput,
	pub telemetry: TaskTelemetry,
}

pub struct NetworkTaskDispatcher<Transport> {
	coordinator: ToronCoordinator,
	transport: Transport,
}

impl<Transport> NetworkTaskDispatcher<Transport> {
	pub fn new(memory: Arc<MemoryManager>, transport: Transport) -> Self {
		Self {
			coordinator: ToronCoordinator::new(memory),
			transport,
		}
	}
}

#[async_trait::async_trait]
impl<Transport> TaskDispatcher for NetworkTaskDispatcher<Transport>
where
	Transport: ToronTransport + Send + Sync,
{
	async fn dispatch(
		&mut self,
		worker: &crate::distributed::worker_registry::WorkerEntry,
		spec: &TaskSpec,
	) -> DispatchOutcome {
		let stream = match self.transport.connect(worker.endpoint.as_str()).await {
			Ok(stream) => stream,
			Err(error) => return DispatchOutcome::Failed(dispatch_failure_kind(error)),
		};

		match self
			.coordinator
			.execute_task_on_worker_with_telemetry(stream, spec.clone())
			.await
		{
			Ok(completion) => DispatchOutcome::CompletedWithTelemetry {
				output: completion.output,
				batches: completion.batches,
				telemetry: completion.telemetry,
			},
			Err(error) => DispatchOutcome::Failed(dispatch_failure_kind(error)),
		}
	}
}

impl ToronCoordinator {
	pub fn new(memory: Arc<MemoryManager>) -> Self {
		Self { memory }
	}

	pub async fn execute_task_on_worker<Stream>(
		&self,
		stream: Stream,
		spec: TaskSpec,
	) -> Result<Vec<RecordBatch>, QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		self.execute_task_on_worker_with_completion(stream, spec)
			.await
			.map(|(results, _)| results)
	}

	pub async fn execute_task_on_worker_with_telemetry<Stream>(
		&self,
		mut stream: Stream,
		spec: TaskSpec,
	) -> Result<RemoteTaskCompletion, QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		let identity = spec.identity;
		send_payload(
			&mut stream,
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			&StartTaskPayload { spec },
		)
		.await?;

		let mut results = Vec::new();
		let mut batch_decode_micros = 0u64;
		let (output, telemetry) = loop {
			let response = read_envelope(&mut stream, self.memory.as_ref()).await?;
			response
				.validate_runtime(ProtocolVersion::CURRENT, FrameByteLen::from_u32(u32::MAX))?;
			match response.kind {
				MessageKind::TaskStarted => {
					let payload: TaskStartedPayload = decode_payload(&response.payload)?;
					if payload.identity != identity {
						return Err(QueryError::UnexpectedMessage {
							expected: "TaskStarted for requested task",
						});
					}
				}
				MessageKind::TaskBatch => {
					let payload: TaskBatchPayload = decode_payload(&response.payload)?;
					if payload.identity != identity {
						return Err(QueryError::UnexpectedMessage {
							expected: "TaskBatch for requested task",
						});
					}
					let batch_decode_started = Instant::now();
					let mut reader = StreamReader::try_new(payload.payload.as_slice(), None)?;
					while let Some(batch) = reader.next().transpose()? {
						results.push(batch);
					}
					batch_decode_micros =
						batch_decode_micros.saturating_add(duration_micros(batch_decode_started));
				}
				MessageKind::TaskComplete => {
					let payload: TaskCompletePayload = decode_payload(&response.payload)?;
					if payload.identity != identity {
						return Err(QueryError::UnexpectedMessage {
							expected: "TaskComplete for requested task",
						});
					}
					let mut telemetry = payload.telemetry.unwrap_or_default();
					telemetry.phases.batch_decode_micros = telemetry
						.phases
						.batch_decode_micros
						.saturating_add(batch_decode_micros);
					break (payload.output, telemetry);
				}
				MessageKind::TaskFailed => {
					let payload: TaskFailedPayload = decode_payload(&response.payload)?;
					return Err(QueryError::Runtime(RuntimeError::TaskFailure {
						kind: payload.kind,
					}));
				}
				_ => {
					return Err(QueryError::UnexpectedMessage {
						expected: "TaskStarted, TaskBatch, TaskComplete, or TaskFailed",
					});
				}
			}
		};

		Ok(RemoteTaskCompletion {
			batches: results,
			output,
			telemetry,
		})
	}

	pub async fn execute_task_on_worker_with_completion<Stream>(
		&self,
		stream: Stream,
		spec: TaskSpec,
	) -> Result<(Vec<RecordBatch>, TaskCompletionOutput), QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		let completion = self
			.execute_task_on_worker_with_telemetry(stream, spec)
			.await?;
		Ok((completion.batches, completion.output))
	}

	pub async fn pull_partition_from_worker<Stream>(
		&self,
		mut stream: Stream,
		address: crate::distributed::task::PartitionAddress,
		expected_attempt: crate::distributed::identity::AttemptId,
	) -> Result<PartitionDataPayload, QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		send_payload(
			&mut stream,
			MessageKind::PullPartition,
			EnvelopeIdentity::partition(address),
			None,
			&PullPartitionPayload {
				address,
				expected_attempt,
			},
		)
		.await?;

		let response = read_envelope(&mut stream, self.memory.as_ref()).await?;
		response.validate_runtime(ProtocolVersion::CURRENT, FrameByteLen::from_u32(u32::MAX))?;
		match response.kind {
			MessageKind::PartitionData => {
				let payload: PartitionDataPayload = decode_payload(&response.payload)?;
				if payload.address != address {
					return Err(QueryError::UnexpectedMessage {
						expected: "PartitionData for requested address",
					});
				}
				Ok(payload)
			}
			MessageKind::PartitionMissing => {
				let payload: PartitionMissingPayload = decode_payload(&response.payload)?;
				Err(partition_missing_error(payload.reason))
			}
			_ => Err(QueryError::UnexpectedMessage {
				expected: "PartitionData or PartitionMissing",
			}),
		}
	}

	pub async fn cancel_task_on_worker<Stream>(
		&self,
		mut stream: Stream,
		identity: TaskIdentity,
	) -> Result<(), QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		send_payload(
			&mut stream,
			MessageKind::CancelTask,
			EnvelopeIdentity::task(identity),
			None,
			&CancelTaskPayload { identity },
		)
		.await?;

		let response = read_envelope(&mut stream, self.memory.as_ref()).await?;
		response.validate_runtime(ProtocolVersion::CURRENT, FrameByteLen::from_u32(u32::MAX))?;
		if response.kind != MessageKind::TaskCancelled {
			return Err(QueryError::UnexpectedMessage {
				expected: "TaskCancelled",
			});
		}

		let payload: TaskCancelledPayload = decode_payload(&response.payload)?;
		if payload.identity != identity {
			return Err(QueryError::UnexpectedMessage {
				expected: "TaskCancelled for requested task",
			});
		}

		Ok(())
	}
}

async fn read_envelope<Stream>(
	stream: &mut Stream,
	memory: &MemoryManager,
) -> Result<WireEnvelope, QueryError>
where
	Stream: tokio::io::AsyncRead + Unpin,
{
	let mut len_buf = [0u8; 4];
	stream.read_exact(&mut len_buf).await?;
	let len = usize::try_from(u32::from_le_bytes(len_buf)).expect("u32 frame length fits usize");

	let mut msg_buf = memory.acquire_at_least(len);
	msg_buf.as_mut_vec().resize(len, 0);
	stream.read_exact(msg_buf.as_mut_vec()).await?;

	Ok(WireEnvelope::decode(&msg_buf)?)
}

async fn send_payload<Stream, Payload>(
	stream: &mut Stream,
	kind: MessageKind,
	identity: EnvelopeIdentity,
	stream_seq: Option<StreamSeq>,
	payload: &Payload,
) -> Result<(), QueryError>
where
	Stream: tokio::io::AsyncWrite + Unpin,
	Payload: serde::Serialize,
{
	let envelope =
		WireEnvelope::try_new(kind, identity, stream_seq, None, encode_payload(payload))?;
	send_envelope(stream, &envelope).await
}

async fn send_envelope<Stream>(
	stream: &mut Stream,
	envelope: &WireEnvelope,
) -> Result<(), QueryError>
where
	Stream: tokio::io::AsyncWrite + Unpin,
{
	let payload = envelope.encode();
	let len = u32::try_from(payload.len())
		.map_err(|_| WireError::FrameTooLarge {
			payload_len: payload.len(),
		})?
		.to_le_bytes();
	stream.write_all(&len).await?;
	stream.write_all(&payload).await?;
	Ok(())
}

async fn send_task_failed<Stream>(
	stream: &mut Stream,
	identity: TaskIdentity,
	error: QueryError,
) -> Result<(), QueryError>
where
	Stream: tokio::io::AsyncWrite + Unpin,
{
	let kind = match &error {
		QueryError::Runtime(RuntimeError::TaskFailure { kind }) if kind.worker_emittable() => {
			kind.clone()
		}
		QueryError::Frontend(crate::error::FrontendError::RuneSandbox { kind, .. }) => {
			rune_sandbox_task_failure_kind(kind)
		}
		QueryError::Execution(crate::error::ExecutionError::BudgetExhausted {
			dimension,
			requested,
			available,
		}) => TaskFailureKind::BudgetExhausted {
			dimension: *dimension,
			requested: *requested,
			available: *available,
		},
		_ => TaskFailureKind::Internal {
			reason: error.to_string(),
		},
	};
	send_payload(
		stream,
		MessageKind::TaskFailed,
		EnvelopeIdentity::task(identity),
		None,
		&TaskFailedPayload::new_worker_emitted(identity, kind).expect("kind is worker-emittable"),
	)
	.await
}

fn task_envelope_identity(identity: EnvelopeIdentity) -> Result<TaskIdentity, QueryError> {
	match identity {
		EnvelopeIdentity::Task(identity) => Ok(identity),
		_ => Err(QueryError::UnexpectedMessage {
			expected: "task envelope identity",
		}),
	}
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, QueryError> {
	let mut buffer = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(buffer)
}

fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, QueryError> {
	let mut reader = StreamReader::try_new(bytes, None)?;
	let mut batches = Vec::new();
	while let Some(batch) = reader.next().transpose()? {
		batches.push(batch);
	}
	Ok(batches)
}

fn partition_missing_error(reason: PartitionMissingReason) -> QueryError {
	QueryError::Runtime(RuntimeError::PartitionStoreMismatch {
		message: format!("{reason:?}"),
	})
}

fn dispatch_failure_kind(error: QueryError) -> TaskFailureKind {
	let reason = error.to_string();
	match error {
		QueryError::Runtime(RuntimeError::TaskFailure { kind }) => kind,
		QueryError::Execution(crate::error::ExecutionError::BudgetExhausted {
			dimension,
			requested,
			available,
		}) => TaskFailureKind::BudgetExhausted {
			dimension,
			requested,
			available,
		},
		QueryError::Runtime(RuntimeError::ProtocolMismatch { .. }) => {
			TaskFailureKind::ProtocolMismatch
		}
		QueryError::Frontend(crate::error::FrontendError::RuneSandbox { kind, .. }) => {
			rune_sandbox_task_failure_kind(&kind)
		}
		QueryError::Wire(WireError::FrameTooLarge { payload_len }) => {
			TaskFailureKind::BudgetExhausted {
				dimension: BudgetDimension::FrameSize,
				requested: ByteCount::new(u64::try_from(payload_len).unwrap_or(u64::MAX)),
				available: ByteCount::ZERO,
			}
		}
		QueryError::Io(_) => TaskFailureKind::TaskFailed {
			reason,
			retryable: true,
		},
		_ => TaskFailureKind::Internal { reason },
	}
}

fn rune_sandbox_task_failure_kind(
	kind: &crate::rune_engine::RuneSandboxViolation,
) -> TaskFailureKind {
	TaskFailureKind::TaskFailed {
		reason: format!("RuneSandbox::{}", kind.stable_label()),
		retryable: false,
	}
}

fn partition_store_publish_error(error: PartitionStoreError) -> QueryError {
	match error {
		PartitionStoreError::BudgetExhausted {
			dimension,
			requested,
			available,
		} => QueryError::Runtime(RuntimeError::TaskFailure {
			kind: TaskFailureKind::BudgetExhausted {
				dimension,
				requested,
				available,
			},
		}),
		PartitionStoreError::CancelledQuery { .. } => {
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::Cancelled,
			})
		}
		other => QueryError::Internal {
			message: other.to_string(),
		},
	}
}
