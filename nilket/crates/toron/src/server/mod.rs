pub mod memory;
pub mod spill;
pub mod transport;

use self::memory::MemoryManager;
use self::transport::ToronTransport;
use crate::audit::{
	ToronAuditEmitter, ToronAuditEvent, WorkerTaskAuditFields, worker_task_error_class,
};
use crate::binder::PlannerConfig;
use crate::catalog::Catalog;
use crate::distributed::arrow_ipc::{decode_record_batches, encode_record_batch};
use crate::distributed::budget::{BudgetDimension, ByteCount};
use crate::distributed::cancellation::{
	ActiveQueryHandle, ActiveTaskRegistry, DownstreamCancellationContext,
	DownstreamCancellationMetrics, TaskCancellationSignal,
};
use crate::distributed::identity::{FrameByteLen, PartitionId, StreamSeq, TaskIdentity, WorkerId};
use crate::distributed::partition_store::{
	InMemoryPartitionStore, PartitionPayload, PartitionStore, PartitionStoreError,
};
use crate::distributed::scheduler::{DispatchOutcome, TaskDispatcher};
use crate::distributed::task::{
	ArrowIpcDigest, LiveProducerPartitionRef, PartitionAddress, PartitionBackend, PartitionCount,
	PartitionEntry, PartitionManifest, RowCount, ShufflePartitionRef, TaskCompletionOutput,
	TaskInputBinding, TaskOutputBinding, TaskSpec, TaskTelemetry, TaskTelemetryLabels,
	is_streamable_exchange_producer, is_streamable_final_result, is_streamable_shuffle_consumer,
	shuffle_input_table_id,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::error::{
	ExecutionError, PartitionMissingReason, QueryError, RuntimeError, StorageError,
	TaskFailureKind, WireError,
};
use crate::exec::{ExecutionContext, Executor, runtime_filter::RuntimeFilterFragmentBuilder};
use crate::ir::{
	BoundExprProgram, ExecutionPlan, FilterDisposition, JoinStrategy, MorselId, TableId,
};
use crate::storage::{ScanRequest, ScanStream, StorageScan};
use crate::wire::{
	ArrowIpcBytes, CancelTaskPayload, EnvelopeIdentity, MessageKind, PartitionDataPayload,
	PartitionMissingPayload, ProtocolVersion, PullPartitionPayload, StartTaskPayload,
	TaskBatchPayload, TaskCancelledPayload, TaskCompletePayload, TaskFailedPayload,
	TaskStartedPayload, WireEnvelope, decode_payload, encode_payload,
};
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::Instrument;
use wendelon::schema::RuntimeFilterDescriptor;

#[cfg(test)]
use crate::distributed::task::JoinRuntimeObservationMode;

pub struct ToronWorker {
	storage: Arc<dyn StorageScan>,
	memory: Arc<MemoryManager>,
	partition_store: Arc<dyn PartitionStore>,
	active_tasks: ActiveTaskRegistry,
	worker_id: WorkerId,
	endpoint: WorkerEndpoint,
	audit: ToronAuditEmitter,
}

struct TaskExecutionReport {
	results: Vec<RecordBatch>,
	output: Option<TaskCompletionOutput>,
	telemetry: TaskTelemetry,
}

struct WorkerTaskAuditRecorder<'a> {
	audit: &'a ToronAuditEmitter,
	fields: WorkerTaskAuditFields,
	task_started: Instant,
	terminal_recorded: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerConnectionPhase {
	AwaitingFirstEnvelope,
	ActiveExchange,
	BetweenCompletedExchanges,
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
			active_tasks: ActiveTaskRegistry::new(),
			worker_id,
			endpoint,
			audit: ToronAuditEmitter::disabled(),
		}
	}

	pub fn with_downstream_cancellation_metrics(
		mut self,
		metrics: Arc<DownstreamCancellationMetrics>,
	) -> Self {
		self.active_tasks = ActiveTaskRegistry::with_downstream_cancellation_metrics(Some(metrics));
		self
	}

	pub fn with_audit_emitter(mut self, audit: ToronAuditEmitter) -> Self {
		self.audit = audit;
		self
	}

	pub async fn execute_task_local(
		&self,
		spec: &TaskSpec,
	) -> Result<Vec<RecordBatch>, QueryError> {
		self.execute_task_local_with_telemetry(spec, None)
			.await
			.map(|report| report.results)
	}

	async fn execute_task_local_with_telemetry(
		&self,
		spec: &TaskSpec,
		downstream_cancellation: Option<DownstreamCancellationContext>,
	) -> Result<TaskExecutionReport, QueryError> {
		self.execute_task_local_with_telemetry_and_stream_sink(spec, downstream_cancellation, None)
			.await
	}

	async fn execute_task_local_with_telemetry_and_stream_sink(
		&self,
		spec: &TaskSpec,
		downstream_cancellation: Option<DownstreamCancellationContext>,
		mut final_result_sink: Option<&mut dyn StreamableBatchSink>,
	) -> Result<TaskExecutionReport, QueryError> {
		let task_span = crate::telemetry::task_span(spec);
		let report = async {
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
			.with_join_runtime_observation_mode(spec.join_runtime_observation_mode.clone())
			.with_runtime_filter(scan_runtime_filter(spec))
			.with_downstream_cancellation(downstream_cancellation);
			task_context.trace_context = if crate::telemetry::enabled() {
				crate::telemetry::task_trace_context(spec)
			} else {
				spec.context.trace_context
			};
			let mut streamed_rows = 0u64;
			let mut streamed_batches = 0u64;
			let mut streamed_batch_encode_micros = 0u64;
			let mut streamed_batch_send_micros = 0u64;
			let (results, output) =
				if is_streamable_final_result(spec) && final_result_sink.is_some() {
					let sink = final_result_sink
						.as_deref_mut()
						.expect("checked streamable final-result sink");
					if self.has_live_shuffle_input(spec) && is_streamable_shuffle_consumer(spec) {
						let base: Arc<dyn StorageScan> = if overrides.is_empty() {
							self.storage.clone()
						} else {
							Arc::new(TaskInputStorageScan {
								base: self.storage.clone(),
								legacy_single_override: legacy_single_shuffle_override(
									&spec.plan, &overrides,
								),
								overrides,
							})
						};
						let storage = Arc::new(LiveInputStorageScan {
							base,
							partition_store: self.partition_store.clone(),
							live_overrides: live_scan_overrides(spec),
						});
						let executor = Executor::with_context(storage, task_context);
						drive_streamable_plan(&spec.plan, &executor, morsel, sink).await?;
					} else if overrides.is_empty() {
						let executor = Executor::with_context(self.storage.clone(), task_context);
						drive_streamable_plan(&spec.plan, &executor, morsel, sink).await?;
					} else {
						let executor = Executor::with_context(
							Arc::new(TaskInputStorageScan {
								base: self.storage.clone(),
								legacy_single_override: legacy_single_shuffle_override(
									&spec.plan, &overrides,
								),
								overrides,
							}),
							task_context,
						);
						drive_streamable_plan(&spec.plan, &executor, morsel, sink).await?;
					}
					streamed_rows = sink.emitted_rows();
					streamed_batches = sink.emitted_batches();
					streamed_batch_encode_micros = sink.batch_encode_micros();
					streamed_batch_send_micros = sink.batch_send_micros();
					(
						Vec::new(),
						Some(TaskCompletionOutput::FinalResult {
							output_digest: None,
						}),
					)
				} else if is_streamable_exchange_producer(spec) {
					self.execute_live_shuffle_producer(spec, task_context, morsel)
						.await?
				} else if self.has_live_shuffle_input(spec) {
					if is_streamable_shuffle_consumer(spec) {
						self.execute_live_shuffle_consumer(spec, task_context, morsel)
							.await?
					} else {
						let base: Arc<dyn StorageScan> = if overrides.is_empty() {
							self.storage.clone()
						} else {
							Arc::new(TaskInputStorageScan {
								base: self.storage.clone(),
								legacy_single_override: legacy_single_shuffle_override(
									&spec.plan, &overrides,
								),
								overrides,
							})
						};
						let storage = Arc::new(LiveInputStorageScan {
							base,
							partition_store: self.partition_store.clone(),
							live_overrides: live_scan_overrides(spec),
						});
						let executor = Executor::with_context(storage, task_context);
						(executor.execute_bound(&spec.plan, morsel).await?, None)
					}
				} else if overrides.is_empty() {
					let executor = Executor::with_context(self.storage.clone(), task_context);
					(executor.execute_bound(&spec.plan, morsel).await?, None)
				} else {
					let executor = Executor::with_context(
						Arc::new(TaskInputStorageScan {
							base: self.storage.clone(),
							legacy_single_override: legacy_single_shuffle_override(
								&spec.plan, &overrides,
							),
							overrides,
						}),
						task_context,
					);
					(executor.execute_bound(&spec.plan, morsel).await?, None)
				};
			record_worker_telemetry(&telemetry, |task| {
				let (result_rows, result_batches) = match &output {
					Some(TaskCompletionOutput::ShuffleProducer { manifest })
						if is_streamable_exchange_producer(spec) && results.is_empty() =>
					{
						(
							manifest
								.partitions
								.iter()
								.map(|partition| partition.row_count.get())
								.sum::<u64>(),
							u64::try_from(manifest.partitions.len())
								.expect("manifest partition count fits u64"),
						)
					}
					_ => (
						results.iter().map(RecordBatch::num_rows).sum::<usize>() as u64,
						results.len() as u64,
					),
				};
				task.counters.emitted_rows = result_rows.saturating_add(streamed_rows);
				task.counters.emitted_batches = result_batches.saturating_add(streamed_batches);
				task.phases.batch_encode_micros = task
					.phases
					.batch_encode_micros
					.saturating_add(streamed_batch_encode_micros);
				task.phases.batch_send_micros = task
					.phases
					.batch_send_micros
					.saturating_add(streamed_batch_send_micros);
				task.phases.task_wall_micros = duration_micros(task_started);
				recompute_framework_overhead(task);
			});
			Ok(TaskExecutionReport {
				results,
				output,
				telemetry: snapshot_worker_telemetry(&telemetry),
			})
		}
		.instrument(task_span.clone())
		.await;
		match &report {
			Ok(report) => crate::telemetry::record_task_telemetry(&task_span, &report.telemetry),
			Err(error) => crate::telemetry::record_query_error(&task_span, error),
		}
		report
	}

	async fn pull_shuffle_inputs(
		&self,
		partitions: &[ShufflePartitionRef],
	) -> Result<(Vec<RecordBatch>, u64), QueryError> {
		let coordinator = ToronCoordinator::new(self.memory.clone());
		let mut batches = Vec::new();
		let mut pulled_bytes = 0u64;

		for partition in partitions {
			let Some(partition) = partition.as_sealed() else {
				return Err(QueryError::Internal {
					message: "internal: live shuffle partition reached sealed pull path"
						.to_string(),
				});
			};
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
			if producer_partitions
				.iter()
				.any(|partition| matches!(partition, ShufflePartitionRef::Live(_)))
			{
				continue;
			}
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

	fn has_live_shuffle_input(&self, spec: &TaskSpec) -> bool {
		spec.inputs.iter().any(|input| {
			matches!(
				input,
				TaskInputBinding::ShuffleConsumer {
					producer_partitions,
					..
				} if producer_partitions
					.iter()
					.any(|partition| matches!(partition, ShufflePartitionRef::Live(_)))
			)
		})
	}

	async fn execute_live_shuffle_producer(
		&self,
		spec: &TaskSpec,
		task_context: ExecutionContext,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(Vec<RecordBatch>, Option<TaskCompletionOutput>), QueryError> {
		let TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			..
		} = &spec.output
		else {
			return Err(QueryError::Internal {
				message: "internal: live producer without shuffle output".to_string(),
			});
		};
		let ExecutionPlan::Exchange { input, .. } = &spec.plan else {
			return Err(QueryError::Internal {
				message: "internal: live producer without terminal exchange".to_string(),
			});
		};
		let partition_count_usize =
			usize::try_from(partition_count.get()).expect("partition count fits usize");
		let schema = input.schema();
		let addresses = (0..partition_count.get())
			.map(|partition| PartitionAddress {
				query_id: spec.identity.query_id,
				exchange_id: *exchange_id,
				producer_stage_id: spec.identity.stage_id,
				producer_task_id: spec.identity.task_id,
				attempt_id: spec.identity.attempt_id,
				partition_id: PartitionId::new(partition),
			})
			.collect::<Vec<_>>();
		for address in &addresses {
			self.partition_store
				.open_live_with_downstream_cancellation(
					*address,
					schema.clone(),
					task_context.downstream_cancellation.clone(),
				)
				.map_err(partition_store_publish_error)?;
		}

		let executor = Executor::with_context(self.storage.clone(), task_context);
		let partition_store = self.partition_store.clone();
		let mut append_sink = LiveAppendStreamableBatchSink {
			executor: &executor,
			partition_store,
			addresses: &addresses,
			partition_count: *partition_count,
			partition_key,
			runtime_filter_builder: RuntimeFilterFragmentBuilder::new(partition_key),
			partition_row_counts: vec![0; partition_count_usize],
			partition_chunk_counts: vec![0; partition_count_usize],
			emitted_rows: 0,
			emitted_batches: 0,
		};
		drive_streamable_plan(input, &executor, morsel, &mut append_sink).await?;
		let runtime_filter_fragment = append_sink
			.runtime_filter_builder
			.take()
			.map(RuntimeFilterFragmentBuilder::finish)
			.transpose()?
			.flatten();

		let manifest = PartitionManifest {
			query_id: spec.identity.query_id,
			stage_id: spec.identity.stage_id,
			task_id: spec.identity.task_id,
			attempt_id: spec.identity.attempt_id,
			exchange_id: *exchange_id,
			producer_worker_id: self.worker_id,
			producer_endpoint: self.endpoint.clone(),
			partitions: (0..partition_count.get())
				.map(|partition| PartitionEntry {
					partition_id: PartitionId::new(partition),
					size_bytes: ByteCount::ZERO,
					row_count: RowCount::new(0),
					digest: ArrowIpcDigest::new([0; 32]),
				})
				.collect(),
			runtime_filter_fragment,
			backend: PartitionBackend::InMemory,
		};
		let manifest = self
			.partition_store
			.seal_live(manifest)
			.map_err(partition_store_publish_error)?;
		Ok((
			Vec::new(),
			Some(TaskCompletionOutput::ShuffleProducer { manifest }),
		))
	}

	async fn execute_live_shuffle_consumer(
		&self,
		spec: &TaskSpec,
		task_context: ExecutionContext,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(Vec<RecordBatch>, Option<TaskCompletionOutput>), QueryError> {
		let storage = Arc::new(LiveInputStorageScan {
			base: self.storage.clone(),
			partition_store: self.partition_store.clone(),
			live_overrides: live_scan_overrides(spec),
		});
		let executor = Executor::with_context(storage, task_context);
		let mut results = Vec::new();
		let mut collect = |batch: RecordBatch| -> Result<bool, QueryError> {
			results.push(batch);
			Ok(true)
		};
		let mut collect_sink = SyncStreamableBatchSink::new(&mut collect);
		drive_streamable_plan(&spec.plan, &executor, morsel, &mut collect_sink).await?;
		Ok((results, None))
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
			..
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
		S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
	{
		let mut phase = WorkerConnectionPhase::AwaitingFirstEnvelope;
		'connection: loop {
			let envelope =
				match read_worker_envelope(&mut stream, self.memory.as_ref(), phase).await? {
					Some(envelope) => envelope,
					None => break,
				};
			phase = WorkerConnectionPhase::ActiveExchange;
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
					let active_task = self.active_tasks.register(identity);
					let audit_fields = self.worker_task_audit_fields(&payload.spec);
					let mut audit_recorder =
						WorkerTaskAuditRecorder::new(&self.audit, audit_fields, task_started);
					if active_task.cancelled_before_start {
						self.partition_store.cancel_query(identity.query_id);
						let error = match audit_recorder.record_cancelled().await {
							Ok(()) => cancelled_task_error(),
							Err(error) => {
								tracing::warn!(error = %error, "required worker cancellation audit unavailable");
								audit_unavailable_task_error()
							}
						};
						send_task_failed(&mut stream, &mut phase, identity, error).await?;
						continue 'connection;
					}

					if let Err(error) = audit_recorder.record_started().await {
						tracing::warn!(error = %error, "required worker start audit unavailable");
						send_task_failed(
							&mut stream,
							&mut phase,
							identity,
							audit_unavailable_task_error(),
						)
						.await?;
						continue 'connection;
					}
					if let Err(error) = send_payload(
						&mut stream,
						MessageKind::TaskStarted,
						EnvelopeIdentity::task(identity),
						None,
						&TaskStartedPayload { identity },
					)
					.await
					{
						let _ = audit_recorder.record_failed(&error).await;
						return Err(error);
					}
					let task_setup_micros = duration_micros(task_started);

					let execution_result = if is_streamable_final_result(&payload.spec) {
						let mut streaming_sink = WorkerTaskBatchSink::new(
							&mut stream,
							identity,
							active_task.signal.clone(),
						);
						let execution = self.execute_task_local_with_telemetry_and_stream_sink(
							&payload.spec,
							Some(active_task.downstream.context()),
							Some(&mut streaming_sink),
						);
						tokio::pin!(execution);
						tokio::select! {
							result = &mut execution => result,
							_ = active_task.signal.cancelled() => Err(cancelled_task_error()),
						}
					} else {
						let execution = self.execute_task_local_with_telemetry(
							&payload.spec,
							Some(active_task.downstream.context()),
						);
						tokio::pin!(execution);
						tokio::select! {
							result = &mut execution => result,
							_ = active_task.signal.cancelled() => Err(cancelled_task_error()),
						}
					};

					match execution_result {
						Ok(mut report) => {
							for (idx, batch) in report.results.iter().enumerate() {
								if active_task.signal.is_cancelled() {
									self.partition_store.cancel_query(identity.query_id);
									let error = match audit_recorder.record_cancelled().await {
										Ok(()) => cancelled_task_error(),
										Err(error) => {
											tracing::warn!(error = %error, "required worker cancellation audit unavailable");
											audit_unavailable_task_error()
										}
									};
									send_task_failed(&mut stream, &mut phase, identity, error)
										.await?;
									continue 'connection;
								}
								let stream_seq = StreamSeq::new(
									u64::try_from(idx).expect("record batch index fits u64"),
								);
								let batch_encode_started = Instant::now();
								let encoded_batch = match encode_record_batch(batch) {
									Ok(encoded_batch) => encoded_batch,
									Err(error) => {
										let query_error: QueryError = error.into();
										let error = match audit_recorder
											.record_failed(&query_error)
											.await
										{
											Ok(()) => query_error,
											Err(audit_error) => {
												tracing::warn!(error = %audit_error, "required worker failure audit unavailable");
												audit_unavailable_task_error()
											}
										};
										send_task_failed(&mut stream, &mut phase, identity, error)
											.await?;
										continue 'connection;
									}
								};
								report.telemetry.phases.batch_encode_micros = report
									.telemetry
									.phases
									.batch_encode_micros
									.saturating_add(duration_micros(batch_encode_started));
								let batch_send_started = Instant::now();
								if let Err(error) = send_payload(
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
								.await
								{
									let _ = audit_recorder.record_failed(&error).await;
									return Err(error);
								}
								report.telemetry.phases.batch_send_micros = report
									.telemetry
									.phases
									.batch_send_micros
									.saturating_add(duration_micros(batch_send_started));
							}
							let publish_started = Instant::now();
							if active_task.signal.is_cancelled() {
								self.partition_store.cancel_query(identity.query_id);
								let error = match audit_recorder.record_cancelled().await {
									Ok(()) => cancelled_task_error(),
									Err(error) => {
										tracing::warn!(error = %error, "required worker cancellation audit unavailable");
										audit_unavailable_task_error()
									}
								};
								send_task_failed(&mut stream, &mut phase, identity, error).await?;
								continue 'connection;
							}
							let published_output = if let Some(output) = report.output.take() {
								Ok(output)
							} else {
								self.publish_task_output(identity, &payload.spec, &report.results)
							};
							match published_output {
								Ok(output) => {
									if active_task.signal.is_cancelled() {
										self.partition_store.cancel_query(identity.query_id);
										let error = match audit_recorder.record_cancelled().await {
											Ok(()) => cancelled_task_error(),
											Err(error) => {
												tracing::warn!(error = %error, "required worker cancellation audit unavailable");
												audit_unavailable_task_error()
											}
										};
										send_task_failed(&mut stream, &mut phase, identity, error)
											.await?;
										continue 'connection;
									}
									report.telemetry.phases.output_publish_micros =
										duration_micros(publish_started);
									report.telemetry.counters.published_shuffle_bytes =
										published_shuffle_bytes(&output);
									report.telemetry.phases.task_setup_micros = task_setup_micros;
									report.telemetry.phases.task_wall_micros =
										duration_micros(task_started);
									recompute_framework_overhead(&mut report.telemetry);
									let emitted_rows = report.telemetry.counters.emitted_rows;
									if let Err(error) =
										audit_recorder.record_completed(emitted_rows).await
									{
										tracing::warn!(error = %error, "required worker completion audit unavailable");
										send_task_failed(
											&mut stream,
											&mut phase,
											identity,
											audit_unavailable_task_error(),
										)
										.await?;
										continue 'connection;
									}
									if let Err(error) = send_terminal_payload(
										&mut stream,
										&mut phase,
										MessageKind::TaskComplete,
										EnvelopeIdentity::task(identity),
										None,
										&TaskCompletePayload {
											identity,
											output,
											telemetry: Some(report.telemetry),
										},
									)
									.await
									{
										let _ = audit_recorder.record_failed(&error).await;
										return Err(error);
									}
								}
								Err(error) => {
									let error = match audit_recorder.record_failed(&error).await {
										Ok(()) => error,
										Err(audit_error) => {
											tracing::warn!(error = %audit_error, "required worker failure audit unavailable");
											audit_unavailable_task_error()
										}
									};
									send_task_failed(&mut stream, &mut phase, identity, error)
										.await?;
								}
							}
							if phase != WorkerConnectionPhase::BetweenCompletedExchanges {
								return Err(QueryError::Internal {
									message: "worker request ended without a terminal response"
										.to_string(),
								});
							}
						}
						Err(error) => {
							if matches!(
								error,
								QueryError::Runtime(RuntimeError::TaskFailure {
									kind: TaskFailureKind::Cancelled
								})
							) {
								self.partition_store.cancel_query(identity.query_id);
							}
							if matches!(
								error,
								QueryError::Runtime(RuntimeError::TaskFailure {
									kind: TaskFailureKind::Cancelled
								})
							) {
								let error = match audit_recorder.record_cancelled().await {
									Ok(()) => error,
									Err(audit_error) => {
										tracing::warn!(error = %audit_error, "required worker cancellation audit unavailable");
										audit_unavailable_task_error()
									}
								};
								send_task_failed(&mut stream, &mut phase, identity, error).await?;
								continue 'connection;
							} else {
								let error = match audit_recorder.record_failed(&error).await {
									Ok(()) => error,
									Err(audit_error) => {
										tracing::warn!(error = %audit_error, "required worker failure audit unavailable");
										audit_unavailable_task_error()
									}
								};
								send_task_failed(&mut stream, &mut phase, identity, error).await?;
								continue 'connection;
							}
						}
					}
					drop(active_task.guard);
				}
				MessageKind::CancelTask => {
					let identity = task_envelope_identity(envelope.identity)?;
					let payload: CancelTaskPayload = decode_payload(&envelope.payload)?;
					if payload.identity != identity {
						return Err(QueryError::UnexpectedMessage {
							expected: "CancelTask for requested task",
						});
					}
					self.active_tasks.cancel(identity);
					self.partition_store.cancel_query(identity.query_id);
					send_terminal_payload(
						&mut stream,
						&mut phase,
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
							send_terminal_payload(
								&mut stream,
								&mut phase,
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
							send_terminal_payload(
								&mut stream,
								&mut phase,
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

	fn worker_task_audit_fields(&self, spec: &TaskSpec) -> WorkerTaskAuditFields {
		WorkerTaskAuditFields {
			tenant_id: spec.context.tenant_id,
			query_id: spec.identity.query_id,
			stage_id: spec.identity.stage_id,
			task_id: spec.identity.task_id,
			attempt_id: spec.identity.attempt_id,
			worker_id: self.worker_id,
			stage_role: stage_role_label(spec),
			operator_kind: operator_kind_label(&spec.plan),
		}
	}
}

impl<'a> WorkerTaskAuditRecorder<'a> {
	fn new(
		audit: &'a ToronAuditEmitter,
		fields: WorkerTaskAuditFields,
		task_started: Instant,
	) -> Self {
		Self {
			audit,
			fields,
			task_started,
			terminal_recorded: false,
		}
	}

	async fn record_started(&self) -> Result<(), crate::audit::AuditUnavailable> {
		self.audit
			.record_required(ToronAuditEvent::worker_task_started(self.fields.clone()))
			.await
	}

	async fn record_completed(
		&mut self,
		row_count: u64,
	) -> Result<(), crate::audit::AuditUnavailable> {
		if self.terminal_recorded {
			return Ok(());
		}
		self.terminal_recorded = true;
		self.audit
			.record_required(
				ToronAuditEvent::worker_task_completed(self.fields.clone())
					.with_duration(self.task_started.elapsed())
					.with_row_count(row_count),
			)
			.await
	}

	async fn record_failed(
		&mut self,
		error: &QueryError,
	) -> Result<(), crate::audit::AuditUnavailable> {
		if self.terminal_recorded {
			return Ok(());
		}
		self.terminal_recorded = true;
		self.audit
			.record_required(
				ToronAuditEvent::worker_task_failed(
					self.fields.clone(),
					worker_task_error_class(error),
				)
				.with_duration(self.task_started.elapsed()),
			)
			.await
	}

	async fn record_cancelled(&mut self) -> Result<(), crate::audit::AuditUnavailable> {
		if self.terminal_recorded {
			return Ok(());
		}
		self.terminal_recorded = true;
		self.audit
			.record_required(
				ToronAuditEvent::worker_task_cancelled(self.fields.clone())
					.with_duration(self.task_started.elapsed()),
			)
			.await
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
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => join_strategy_label(input),
		ExecutionPlan::Union { left, right, .. } => {
			join_strategy_label(left).or_else(|| join_strategy_label(right))
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE is one self-contained local node — opaque to these plan walkers.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
	}
}

fn contains_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => contains_join(input),
		ExecutionPlan::Union { left, right, .. } => contains_join(left) || contains_join(right),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE is one self-contained local node — opaque to these plan walkers.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => false,
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
			ExecutionPlan::Union { .. } => "union",
			ExecutionPlan::Repeat { .. } => "repeat",
			ExecutionPlan::Aggregate { .. } => "aggregate",
			ExecutionPlan::Sort { .. } => "sort",
			ExecutionPlan::Window { .. } => "window",
			ExecutionPlan::Take { .. } => "take",
			ExecutionPlan::EmptyRelation { .. } => "empty_relation",
			ExecutionPlan::SystemTable { .. } => "system_table",
			ExecutionPlan::Scan { .. } => "scan",
			ExecutionPlan::Join { .. } => "join",
			ExecutionPlan::CreateTable { .. } => "catalog",
			ExecutionPlan::CreateLogicalType { .. } => "catalog",
			ExecutionPlan::CreateTableAsSelect { .. } => "create_table_as_select",
			ExecutionPlan::DropTable { .. } => "drop_table",
			ExecutionPlan::Delete { .. } => "delete",
			ExecutionPlan::Update { .. } => "update",
			ExecutionPlan::Insert { .. } => "insert",
			ExecutionPlan::InsertSelect { .. } => "insert_select",
			// ADR 255: a recursive CTE's body is opaque; it is labelled as one node.
			ExecutionPlan::RecursiveCte { .. } => "recursive_cte",
			ExecutionPlan::RecursiveRef { .. } => "recursive_ref",
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
		.try_fold(0u64, |total, partition| {
			let Some(partition) = partition.as_sealed() else {
				return Err(QueryError::Internal {
					message: "internal: live shuffle partition reached broadcast cap validator"
						.to_string(),
				});
			};
			Ok(total.saturating_add(partition.size_bytes.get()))
		})?;
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
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => contains_broadcast_join(input),
		ExecutionPlan::Union { left, right, .. } => {
			contains_broadcast_join(left) || contains_broadcast_join(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE is one self-contained local node — opaque to these plan walkers.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::audit::{
		ToronAuditDropReason, ToronAuditEventClass, ToronAuditMetrics, ToronAuditOperation,
	};
	use crate::distributed::budget::{QueryBudget, TaskBudget};
	use crate::distributed::cancellation::DownstreamAttemptRegistry;
	use crate::distributed::identity::{AttemptId, ExchangeId, QueryId, StageId, TaskId};
	use crate::distributed::task::{
		ArrowIpcDigest, PartitionAddress, PartitionCount, ProducerPartitionRef,
	};
	use crate::ir::{
		BoundExprProgram, BroadcastReplayEligibility, ColumnOrdinal, JoinKind, JoinReplayId,
		QueryProfileHint,
	};
	use crate::quic::QuicEndpoint;
	use crate::server::transport::QuicTransport;
	use crate::storage::InMemoryStorageScan;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::time::Duration;
	use tokio::io::duplex as test_duplex;
	use tokio::sync::{Notify, mpsc};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterBloom,
		RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind, SchemaEvolutionPolicy,
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
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
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
	fn descriptor_evolved_dispatch_failure_carries_stale_table_id() {
		let kind = dispatch_failure_kind(QueryError::Execution(ExecutionError::Storage(
			StorageError::DescriptorEvolved {
				table_id: TableId(42),
			},
		)));

		assert!(matches!(
			kind,
			TaskFailureKind::DescriptorEvolved {
				table_id: TableId(42),
				expected: None,
				current: None
			}
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
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Join {
				replay_id: JoinReplayId::SYNTHETIC,
				replay_eligibility: BroadcastReplayEligibility::Terminal,
				left: Box::new(scan(TableId(1), 1)),
				right: Box::new(scan(TableId(2), 2)),
				kind: JoinKind::Inner,
				left_keys: vec![ColumnOrdinal(0)],
				right_keys: vec![ColumnOrdinal(0)],
				match_filter: None,
				null_safe_keys: false,
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
					producer_partitions: vec![ShufflePartitionRef::Sealed(ProducerPartitionRef {
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
					})],
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
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id: ExchangeId::new(1),
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
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

	#[tokio::test]
	async fn worker_task_success_emits_started_and_completed_audit() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch =
			RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
				.expect("batch");
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let (tx, mut rx) = mpsc::channel(8);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(InMemoryStorageScan { table_data: batch }),
				memory.clone(),
				Arc::new(InMemoryPartitionStore::new()),
				WorkerId::from_u128(7),
				WorkerEndpoint::new("127.0.0.1:0"),
			)
			.with_audit_emitter(ToronAuditEmitter::Enabled {
				tx,
				metrics,
				fail_closed: false,
				ack_timeout: Duration::from_millis(500),
			}),
		);
		let identity = TaskIdentity::new(
			QueryId::from_u128(78),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		let (rows, output) = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(client, spec)
			.await
			.expect("task succeeds");

		assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
		assert!(matches!(output, TaskCompletionOutput::FinalResult { .. }));
		let started = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("started audit timeout")
			.expect("started audit");
		let completed = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("completed audit timeout")
			.expect("completed audit");
		assert_eq!(started.operation(), ToronAuditOperation::WorkerTaskStarted);
		assert_eq!(started.event_class(), ToronAuditEventClass::TaskLifecycle);
		assert_eq!(
			completed.operation(),
			ToronAuditOperation::WorkerTaskCompleted
		);
		assert_eq!(completed.event_class(), ToronAuditEventClass::TaskLifecycle);

		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn worker_task_failure_emits_failed_with_bounded_error_class() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
		)
		.expect("batch");
		let mut budget = QueryBudget::test_default();
		budget.shuffle_bytes_published_per_task = ByteCount::new(1);
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let (tx, mut rx) = mpsc::channel(8);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(InMemoryStorageScan { table_data: batch }),
				memory.clone(),
				Arc::new(InMemoryPartitionStore::with_budget(budget)),
				WorkerId::from_u128(7),
				WorkerEndpoint::new("127.0.0.1:0"),
			)
			.with_audit_emitter(ToronAuditEmitter::Enabled {
				tx,
				metrics,
				fail_closed: false,
				ack_timeout: Duration::from_millis(500),
			}),
		);
		let identity = TaskIdentity::new(
			QueryId::from_u128(80),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id: ExchangeId::new(1),
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		let err = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(client, spec)
			.await
			.expect_err("publish budget exhaustion must fail task");

		assert!(matches!(
			err,
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::ShuffleBytes,
					..
				}
			})
		));
		let started = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("started audit timeout")
			.expect("started audit");
		let failed = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("failed audit timeout")
			.expect("failed audit");
		assert_eq!(started.operation(), ToronAuditOperation::WorkerTaskStarted);
		assert_eq!(failed.operation(), ToronAuditOperation::WorkerTaskFailed);
		assert_eq!(failed.error_class_for_test(), Some("budget_exhausted"));
		assert!(rx.try_recv().is_err());

		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn worker_audit_queue_full_records_drop_without_failing_task() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
			.expect("batch");
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let (tx, _rx) = mpsc::channel(1);
		tx.try_send(crate::audit::ToronAuditCommand::fire_and_forget(
			ToronAuditEvent::lifecycle_startup(),
		))
		.expect("fill audit queue");
		let metrics = Arc::new(ToronAuditMetrics::default());
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(InMemoryStorageScan { table_data: batch }),
				memory.clone(),
				Arc::new(InMemoryPartitionStore::new()),
				WorkerId::from_u128(7),
				WorkerEndpoint::new("127.0.0.1:0"),
			)
			.with_audit_emitter(ToronAuditEmitter::Enabled {
				tx,
				metrics: Arc::clone(&metrics),
				fail_closed: false,
				ack_timeout: Duration::from_millis(500),
			}),
		);
		let identity = TaskIdentity::new(
			QueryId::from_u128(79),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		let (rows, _output) = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(client, spec)
			.await
			.expect("audit queue full must not fail task");

		assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
		assert!(
			metrics.dropped_count(
				ToronAuditEventClass::TaskLifecycle,
				ToronAuditDropReason::QueueFull
			) >= 1
		);
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn worker_started_audit_failure_prevents_task_execution() {
		let scan_started = Arc::new(AtomicBool::new(false));
		let scan_notify = Arc::new(Notify::new());
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let (tx, _rx) = mpsc::channel(1);
		tx.try_send(crate::audit::ToronAuditCommand::fire_and_forget(
			ToronAuditEvent::lifecycle_startup(),
		))
		.expect("fill audit queue");
		let metrics = Arc::new(ToronAuditMetrics::default());
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(BlockingStorageScan {
					started: Arc::clone(&scan_started),
					notify: Arc::clone(&scan_notify),
				}),
				memory.clone(),
				Arc::new(InMemoryPartitionStore::new()),
				WorkerId::from_u128(7),
				WorkerEndpoint::new("127.0.0.1:0"),
			)
			.with_audit_emitter(ToronAuditEmitter::Enabled {
				tx,
				metrics,
				fail_closed: true,
				ack_timeout: Duration::from_millis(50),
			}),
		);
		let identity = TaskIdentity::new(
			QueryId::from_u128(83),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (mut client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		send_payload(
			&mut client,
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			&StartTaskPayload { spec },
		)
		.await
		.expect("send start task");
		let failed = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read audit unavailable failure");
		assert_eq!(failed.kind, MessageKind::TaskFailed);
		let payload: TaskFailedPayload =
			decode_payload(&failed.payload).expect("decode task failed");
		assert!(matches!(
			payload.kind,
			TaskFailureKind::TaskFailed { ref reason, retryable }
				if reason == "audit_unavailable" && retryable
		));
		assert!(
			!scan_started.load(Ordering::SeqCst),
			"task executed despite missing required started audit"
		);

		drop(client);
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn worker_audit_disabled_is_noop() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
			.expect("batch");
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(7),
			WorkerEndpoint::new("127.0.0.1:0"),
		));
		assert!(matches!(&worker.audit, ToronAuditEmitter::Disabled));
		let identity = TaskIdentity::new(
			QueryId::from_u128(81),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		let (rows, _output) = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(client, spec)
			.await
			.expect("disabled audit is a no-op");

		assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn cancel_task_before_registration_tombstones_and_prevents_execution() {
		assert_cancel_before_start_emits_cancelled_without_started().await;
	}

	#[tokio::test]
	async fn worker_task_cancelled_before_start_emits_cancelled_without_started() {
		assert_cancel_before_start_emits_cancelled_without_started().await;
	}

	async fn assert_cancel_before_start_emits_cancelled_without_started() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch =
			RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
				.expect("batch");
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let (tx, mut rx) = mpsc::channel(8);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(InMemoryStorageScan { table_data: batch }),
				memory.clone(),
				Arc::new(InMemoryPartitionStore::new()),
				WorkerId::from_u128(7),
				WorkerEndpoint::new("127.0.0.1:0"),
			)
			.with_audit_emitter(ToronAuditEmitter::Enabled {
				tx,
				metrics,
				fail_closed: false,
				ack_timeout: Duration::from_millis(500),
			}),
		);
		let identity = TaskIdentity::new(
			QueryId::from_u128(77),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (mut client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		send_payload(
			&mut client,
			MessageKind::CancelTask,
			EnvelopeIdentity::task(identity),
			None,
			&CancelTaskPayload { identity },
		)
		.await
		.expect("send cancel task");
		let cancelled = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read cancellation ack");
		assert_eq!(cancelled.kind, MessageKind::TaskCancelled);

		send_payload(
			&mut client,
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			&StartTaskPayload { spec },
		)
		.await
		.expect("send start task");
		let failed = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read pre-cancel failure");
		assert_eq!(failed.kind, MessageKind::TaskFailed);
		let payload: TaskFailedPayload =
			decode_payload(&failed.payload).expect("decode task failed");
		assert!(matches!(payload.kind, TaskFailureKind::Cancelled));
		let cancelled_audit = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("cancelled audit timeout")
			.expect("cancelled audit");
		assert_eq!(
			cancelled_audit.operation(),
			ToronAuditOperation::WorkerTaskCancelled
		);
		assert!(rx.try_recv().is_err());

		drop(client);
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn worker_task_cancelled_during_execution_emits_single_cancelled_terminal_event() {
		let scan_started = Arc::new(AtomicBool::new(false));
		let scan_notify = Arc::new(Notify::new());
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let (tx, mut rx) = mpsc::channel(8);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(BlockingStorageScan {
					started: Arc::clone(&scan_started),
					notify: Arc::clone(&scan_notify),
				}),
				memory.clone(),
				Arc::new(InMemoryPartitionStore::new()),
				WorkerId::from_u128(7),
				WorkerEndpoint::new("127.0.0.1:0"),
			)
			.with_audit_emitter(ToronAuditEmitter::Enabled {
				tx,
				metrics,
				fail_closed: false,
				ack_timeout: Duration::from_millis(500),
			}),
		);
		let identity = TaskIdentity::new(
			QueryId::from_u128(82),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (mut task_client, task_server) = test_duplex(1024 * 1024);
		let (cancel_client, cancel_server) = test_duplex(1024 * 1024);
		let task_worker = Arc::clone(&worker);
		let task_handle =
			tokio::spawn(async move { task_worker.handle_connection(task_server).await });
		let cancel_worker = Arc::clone(&worker);
		let cancel_handle =
			tokio::spawn(async move { cancel_worker.handle_connection(cancel_server).await });

		send_payload(
			&mut task_client,
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			&StartTaskPayload { spec },
		)
		.await
		.expect("send start task");
		let started = read_envelope(&mut task_client, memory.as_ref())
			.await
			.expect("read task started");
		assert_eq!(started.kind, MessageKind::TaskStarted);
		tokio::time::timeout(Duration::from_secs(1), async {
			loop {
				if scan_started.load(Ordering::SeqCst) {
					break;
				}
				scan_notify.notified().await;
			}
		})
		.await
		.expect("storage scan started");

		ToronCoordinator::new(memory.clone())
			.cancel_task_on_worker(cancel_client, identity)
			.await
			.expect("cancel task");
		let failed = read_envelope(&mut task_client, memory.as_ref())
			.await
			.expect("read cancelled task failure");
		assert_eq!(failed.kind, MessageKind::TaskFailed);
		let payload: TaskFailedPayload =
			decode_payload(&failed.payload).expect("decode task failed");
		assert!(matches!(payload.kind, TaskFailureKind::Cancelled));

		let started_audit = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("started audit timeout")
			.expect("started audit");
		let cancelled_audit = tokio::time::timeout(Duration::from_secs(1), rx.recv())
			.await
			.expect("cancelled audit timeout")
			.expect("cancelled audit");
		assert_eq!(
			started_audit.operation(),
			ToronAuditOperation::WorkerTaskStarted
		);
		assert_eq!(
			cancelled_audit.operation(),
			ToronAuditOperation::WorkerTaskCancelled
		);
		assert!(rx.try_recv().is_err());

		drop(task_client);
		task_handle
			.await
			.expect("task worker")
			.expect("task worker ok");
		cancel_handle
			.await
			.expect("cancel worker")
			.expect("cancel worker ok");
	}

	struct BlockingStorageScan {
		started: Arc<AtomicBool>,
		notify: Arc<Notify>,
	}

	struct CancelledStorageScan;

	#[async_trait::async_trait]
	impl StorageScan for CancelledStorageScan {
		async fn scan(&self, _request: ScanRequest) -> Result<ScanStream, StorageError> {
			Err(StorageError::Cancelled)
		}
	}

	fn final_result_task_spec(seed: u128) -> TaskSpec {
		TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(seed),
				StageId::new(0),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		}
	}

	async fn spawn_one_quic_worker(
		endpoint: QuicEndpoint,
		worker: Arc<ToronWorker>,
	) -> Result<(), QueryError> {
		let connection = endpoint.accept_connection().await?;
		let stream = QuicEndpoint::accept_stream_on(&connection).await?;
		worker.handle_connection(stream).await
	}

	struct ShutdownFails;

	impl tokio::io::AsyncWrite for ShutdownFails {
		fn poll_write(
			self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
			buf: &[u8],
		) -> std::task::Poll<std::io::Result<usize>> {
			std::task::Poll::Ready(Ok(buf.len()))
		}

		fn poll_flush(
			self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			std::task::Poll::Ready(Ok(()))
		}

		fn poll_shutdown(
			self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			std::task::Poll::Ready(Err(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"injected shutdown failure",
			)))
		}
	}

	#[tokio::test]
	async fn terminal_application_error_outranks_shutdown_failure() {
		let application_error = QueryError::Runtime(RuntimeError::TaskFailure {
			kind: TaskFailureKind::Cancelled,
		});
		let observed =
			finish_terminal_exchange::<_, ()>(&mut ShutdownFails, Err(application_error))
				.await
				.expect_err("application failure remains primary");
		assert!(matches!(
			observed,
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::Cancelled
			})
		));

		let close_error = finish_terminal_exchange(&mut ShutdownFails, Ok(()))
			.await
			.expect_err("successful exchange reports close failure");
		assert!(matches!(
			close_error,
			QueryError::Io(ref error) if error.kind() == std::io::ErrorKind::BrokenPipe
		));
	}

	#[tokio::test]
	async fn quic_worker_task_completion_closes_cleanly() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch =
			RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
				.expect("batch");
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			Arc::clone(&memory),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(533),
			WorkerEndpoint::new("127.0.0.1:0"),
		));
		let endpoint =
			QuicEndpoint::bind_loopback("127.0.0.1:0".parse().unwrap()).expect("bind QUIC worker");
		let address = endpoint.local_addr().expect("worker address");
		let server_endpoint = endpoint.clone();
		let worker_handle =
			tokio::spawn(spawn_one_quic_worker(server_endpoint, Arc::clone(&worker)));

		let transport = QuicTransport::new().expect("QUIC transport");
		let stream = transport
			.connect(address.to_string().as_str())
			.await
			.expect("connect worker");
		let completion = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(stream, final_result_task_spec(533))
			.await
			.expect("task completion");
		assert_eq!(
			completion
				.0
				.iter()
				.map(RecordBatch::num_rows)
				.sum::<usize>(),
			3
		);
		tokio::time::timeout(Duration::from_secs(2), worker_handle)
			.await
			.expect("worker teardown timeout")
			.expect("worker join")
			.expect("worker clean teardown");
		endpoint.close();
	}

	#[tokio::test]
	async fn quic_worker_task_failure_closes_cleanly() {
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(CancelledStorageScan),
			Arc::clone(&memory),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(538),
			WorkerEndpoint::new("127.0.0.1:0"),
		));
		let endpoint =
			QuicEndpoint::bind_loopback("127.0.0.1:0".parse().unwrap()).expect("bind QUIC worker");
		let address = endpoint.local_addr().expect("worker address");
		let server_endpoint = endpoint.clone();
		let worker_handle =
			tokio::spawn(spawn_one_quic_worker(server_endpoint, Arc::clone(&worker)));
		let transport = QuicTransport::new().expect("QUIC transport");
		let stream = transport
			.connect(address.to_string().as_str())
			.await
			.expect("connect worker");
		let error = ToronCoordinator::new(memory)
			.execute_task_on_worker_with_completion(stream, final_result_task_spec(538))
			.await
			.expect_err("cancelled storage produces TaskFailed");
		assert!(matches!(
			error,
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::Cancelled
			})
		));
		tokio::time::timeout(Duration::from_secs(2), worker_handle)
			.await
			.expect("worker teardown timeout")
			.expect("worker join")
			.expect("worker clean teardown");
		endpoint.close();
	}

	#[tokio::test]
	async fn quic_worker_only_accepts_connection_loss_between_completed_exchanges() {
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan {
				table_data: empty_batch,
			}),
			Arc::clone(&memory),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(534),
			WorkerEndpoint::new("127.0.0.1:0"),
		));
		let endpoint = QuicEndpoint::bind_loopback("127.0.0.1:0".parse().unwrap())
			.expect("bind completed-exchange worker");
		let address = endpoint.local_addr().expect("worker address");
		let server_endpoint = endpoint.clone();
		let worker_handle =
			tokio::spawn(spawn_one_quic_worker(server_endpoint, Arc::clone(&worker)));
		let client_endpoint = QuicEndpoint::client_loopback().expect("QUIC client");
		let mut stream = client_endpoint
			.connect_stream(address)
			.await
			.expect("connect worker");
		let identity = final_result_task_spec(534).identity;
		send_payload(
			&mut stream,
			MessageKind::CancelTask,
			EnvelopeIdentity::task(identity),
			None,
			&CancelTaskPayload { identity },
		)
		.await
		.expect("send cancellation");
		let response = read_envelope(&mut stream, memory.as_ref())
			.await
			.expect("read terminal cancellation");
		assert_eq!(response.kind, MessageKind::TaskCancelled);
		client_endpoint.close();
		drop(stream);
		tokio::time::timeout(Duration::from_secs(2), worker_handle)
			.await
			.expect("completed-exchange teardown timeout")
			.expect("completed-exchange worker join")
			.expect("completed exchange closes cleanly");
		endpoint.close();

		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan {
				table_data: empty_batch,
			}),
			Arc::clone(&memory),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(535),
			WorkerEndpoint::new("127.0.0.1:0"),
		));
		let endpoint = QuicEndpoint::bind_loopback("127.0.0.1:0".parse().unwrap())
			.expect("bind active-exchange worker");
		let address = endpoint.local_addr().expect("worker address");
		let server_endpoint = endpoint.clone();
		let worker_handle = tokio::spawn(spawn_one_quic_worker(server_endpoint, worker));
		let client_endpoint = QuicEndpoint::client_loopback().expect("QUIC client");
		let mut stream = client_endpoint
			.connect_stream(address)
			.await
			.expect("connect active worker");
		let spec = final_result_task_spec(535);
		let identity = spec.identity;
		let envelope = WireEnvelope::try_new(
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			None,
			encode_payload(&StartTaskPayload { spec }),
		)
		.expect("encode active task");
		let encoded = envelope.encode();
		let frame_len = u32::try_from(encoded.len())
			.expect("frame length")
			.to_le_bytes();
		stream
			.write_all(&frame_len)
			.await
			.expect("write active frame length");
		stream
			.write_all(&encoded[..encoded.len() - 1])
			.await
			.expect("write partial active frame");
		client_endpoint.close();
		drop(stream);
		let error = tokio::time::timeout(Duration::from_secs(2), worker_handle)
			.await
			.expect("active-exchange teardown timeout")
			.expect("active-exchange worker join")
			.expect_err("active exchange loss remains an error");
		assert!(matches!(error, QueryError::Io(_)));
		endpoint.close();
	}

	#[tokio::test]
	async fn worker_keeps_multi_envelope_stream_open_until_client_shutdown() {
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan {
				table_data: empty_batch,
			}),
			Arc::clone(&memory),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(536),
			WorkerEndpoint::new("local"),
		));
		let (mut client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		for seed in [536_u128, 537] {
			let identity = final_result_task_spec(seed).identity;
			send_payload(
				&mut client,
				MessageKind::CancelTask,
				EnvelopeIdentity::task(identity),
				None,
				&CancelTaskPayload { identity },
			)
			.await
			.expect("send cancellation");
			let response = read_envelope(&mut client, memory.as_ref())
				.await
				.expect("read cancellation response");
			assert_eq!(response.kind, MessageKind::TaskCancelled);
		}
		client.shutdown().await.expect("close reusable stream");
		worker_handle
			.await
			.expect("worker join")
			.expect("worker clean teardown");
	}

	#[async_trait::async_trait]
	impl StorageScan for BlockingStorageScan {
		async fn scan(&self, _request: ScanRequest) -> Result<ScanStream, StorageError> {
			self.started.store(true, Ordering::SeqCst);
			self.notify.notify_waiters();
			std::future::pending::<Result<ScanStream, StorageError>>().await
		}
	}

	struct TwoBatchBlockingStorageScan {
		schema: SchemaRef,
		first: RecordBatch,
		second: RecordBatch,
		release_tail: Arc<Notify>,
	}

	#[async_trait::async_trait]
	impl StorageScan for TwoBatchBlockingStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			let first = self.first.clone();
			let second = self.second.clone();
			let release_tail = self.release_tail.clone();
			let stream = futures::stream::unfold(0_u8, move |state| {
				let first = first.clone();
				let second = second.clone();
				let release_tail = release_tail.clone();
				async move {
					match state {
						0 => Some((Ok(first), 1)),
						1 => {
							release_tail.notified().await;
							Some((Ok(second), 2))
						}
						_ => None,
					}
				}
			})
			.boxed();
			Ok(ScanStream::new(
				self.schema.clone(),
				request.session_id,
				request.query_id,
				stream,
			))
		}
	}

	struct CountingBatchStorageScan {
		schema: SchemaRef,
		batches: Vec<RecordBatch>,
		yielded_batches: Arc<AtomicUsize>,
	}

	#[async_trait::async_trait]
	impl StorageScan for CountingBatchStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			let batches = self.batches.clone();
			let yielded_batches = self.yielded_batches.clone();
			let stream = futures::stream::unfold(0_usize, move |index| {
				let batches = batches.clone();
				let yielded_batches = yielded_batches.clone();
				async move {
					let batch = batches.get(index)?.clone();
					yielded_batches.fetch_add(1, Ordering::SeqCst);
					Some((Ok(batch), index + 1))
				}
			})
			.boxed();
			Ok(ScanStream::new(
				self.schema.clone(),
				request.session_id,
				request.query_id,
				stream,
			))
		}
	}

	#[tokio::test]
	async fn worker_streamable_final_result_sends_taskbatch_before_taskcomplete() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let first = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("first batch");
		let second = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![2_i64]))],
		)
		.expect("second batch");
		let release_tail = Arc::new(Notify::new());
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(TwoBatchBlockingStorageScan {
				schema,
				first,
				second,
				release_tail: release_tail.clone(),
			}),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(120),
			WorkerEndpoint::new("local"),
		));
		let identity = TaskIdentity::new(
			QueryId::from_u128(120),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (mut client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		send_payload(
			&mut client,
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			&StartTaskPayload { spec },
		)
		.await
		.expect("send start task");
		let started = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read task started");
		assert_eq!(started.kind, MessageKind::TaskStarted);
		let first_batch = tokio::time::timeout(
			Duration::from_millis(250),
			read_envelope(&mut client, memory.as_ref()),
		)
		.await
		.expect("streamable final result must emit first TaskBatch before tail release")
		.expect("read first task batch");
		assert_eq!(first_batch.kind, MessageKind::TaskBatch);
		let no_complete = tokio::time::timeout(
			Duration::from_millis(50),
			read_envelope(&mut client, memory.as_ref()),
		)
		.await;
		assert!(
			no_complete.is_err(),
			"TaskComplete must not arrive while the scan tail is still blocked"
		);

		release_tail.notify_waiters();
		let second_batch = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read second task batch");
		assert_eq!(second_batch.kind, MessageKind::TaskBatch);
		let complete = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read task complete");
		assert_eq!(complete.kind, MessageKind::TaskComplete);

		drop(client);
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn network_dispatcher_forwards_final_taskbatch_to_cursor_without_buffering() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let first = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("first batch");
		let second = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![2_i64]))],
		)
		.expect("second batch");
		let release_tail = Arc::new(Notify::new());
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(TwoBatchBlockingStorageScan {
				schema,
				first,
				second,
				release_tail: release_tail.clone(),
			}),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(121),
			WorkerEndpoint::new("local"),
		));
		let identity = TaskIdentity::new(
			QueryId::from_u128(121),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: scan(TableId(1), 1),
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });
		let (tx, mut rx) = mpsc::channel(16);
		let completion = {
			let memory = memory.clone();
			tokio::spawn(async move {
				ToronCoordinator::new(memory)
					.execute_task_on_worker_with_telemetry_and_batch_sender(
						client,
						spec,
						Some(tx),
						None,
					)
					.await
			})
		};

		let first_forwarded = tokio::time::timeout(Duration::from_millis(250), rx.recv())
			.await
			.expect("first final-result batch must reach cursor before task completion")
			.expect("cursor batch")
			.expect("cursor batch ok");
		assert_eq!(first_forwarded.num_rows(), 1);
		assert!(
			tokio::time::timeout(Duration::from_millis(50), rx.recv())
				.await
				.is_err(),
			"second batch must wait behind the blocked scan tail"
		);

		release_tail.notify_waiters();
		let second_forwarded = rx
			.recv()
			.await
			.expect("second cursor batch")
			.expect("second cursor batch ok");
		assert_eq!(second_forwarded.num_rows(), 1);
		let completion = completion
			.await
			.expect("client task")
			.expect("task completion");
		assert!(
			completion.batches.is_empty(),
			"forwarded final-result batches must not also be buffered locally"
		);

		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	struct CancellationAwareTwoBatchStorageScan {
		schema: SchemaRef,
		first: RecordBatch,
		second: RecordBatch,
		second_emitted: Arc<AtomicBool>,
	}

	#[async_trait::async_trait]
	impl StorageScan for CancellationAwareTwoBatchStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			let first = self.first.clone();
			let second = self.second.clone();
			let second_emitted = self.second_emitted.clone();
			let downstream_cancellation = request
				.downstream_cancellation
				.clone()
				.expect("live producer scan should carry downstream cancellation");
			let stream = futures::stream::unfold(0_u8, move |state| {
				let first = first.clone();
				let second = second.clone();
				let second_emitted = second_emitted.clone();
				let downstream_cancellation = downstream_cancellation.clone();
				async move {
					match state {
						0 => Some((Ok(first), 1)),
						1 => {
							downstream_cancellation.cancelled().await;
							Some((Err(StorageError::Cancelled), 2))
						}
						_ => {
							second_emitted.store(true, Ordering::SeqCst);
							Some((Ok(second), 3))
						}
					}
				}
			})
			.boxed();
			Ok(ScanStream::new(
				self.schema.clone(),
				request.session_id,
				request.query_id,
				stream,
			))
		}
	}

	#[tokio::test]
	async fn streaming_exchange_producer_appends_before_execute_bound_would_return() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let first = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
		)
		.expect("first batch");
		let second = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
		)
		.expect("second batch");
		let release_tail = Arc::new(Notify::new());
		let store = Arc::new(InMemoryPartitionStore::new());
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(TwoBatchBlockingStorageScan {
				schema,
				first,
				second,
				release_tail: release_tail.clone(),
			}),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			store.clone(),
			WorkerId::from_u128(17),
			WorkerEndpoint::new("local"),
		));
		let exchange_id = ExchangeId::new(119);
		let identity = TaskIdentity::new(
			QueryId::from_u128(119),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Exchange {
				input: Box::new(scan(TableId(1), 1)),
				partition_count: 1,
				key: BoundExprProgram::empty(),
			},
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let address = PartitionAddress {
			query_id: identity.query_id,
			exchange_id,
			producer_stage_id: identity.stage_id,
			producer_task_id: identity.task_id,
			attempt_id: identity.attempt_id,
			partition_id: PartitionId::new(0),
		};
		let mut live = store
			.pull_live(address, AttemptId::first())
			.expect("live pull");
		let task = tokio::spawn(async move {
			worker
				.execute_task_local_with_telemetry(&spec, None)
				.await
				.expect("live producer")
		});

		let payload = tokio::time::timeout(Duration::from_secs(1), live.next())
			.await
			.expect("first live payload before scan tail release")
			.expect("payload")
			.expect("payload ok");
		assert_eq!(
			decode_record_batches(&payload.bytes).unwrap()[0].num_rows(),
			2
		);
		release_tail.notify_waiters();
		let report = task.await.expect("producer task");
		let manifest = match report.output {
			Some(TaskCompletionOutput::ShuffleProducer { manifest }) => manifest,
			other => panic!("expected shuffle output, got {other:?}"),
		};
		assert!(report.results.is_empty());
		assert_eq!(
			manifest
				.partitions
				.iter()
				.map(|partition| partition.row_count.get())
				.sum::<u64>(),
			4
		);
		assert_eq!(report.telemetry.counters.emitted_rows, 4);
		assert_eq!(report.telemetry.counters.emitted_batches, 1);
	}

	async fn run_single_partition_live_shuffle_producer(rows: Vec<i64>) -> TaskExecutionReport {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows))])
			.expect("live producer batch");
		let worker = ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(1260),
			WorkerEndpoint::new("local"),
		);
		let spec = TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(1260),
				StageId::new(0),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Exchange {
				input: Box::new(scan(TableId(1), 1)),
				partition_count: 1,
				key: BoundExprProgram::empty(),
			},
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id: ExchangeId::new(1260),
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};

		worker
			.execute_task_local_with_telemetry(&spec, None)
			.await
			.expect("live shuffle producer")
	}

	#[tokio::test]
	async fn live_shuffle_producer_returns_no_result_batches() {
		let report = run_single_partition_live_shuffle_producer(vec![1, 2, 3]).await;
		assert!(report.results.is_empty());
		assert!(matches!(
			report.output,
			Some(TaskCompletionOutput::ShuffleProducer { .. })
		));
	}

	#[tokio::test]
	async fn live_shuffle_producer_telemetry_uses_manifest_rows() {
		let report = run_single_partition_live_shuffle_producer(vec![1, 2, 3, 4]).await;
		assert!(report.results.is_empty());
		assert_eq!(report.telemetry.counters.emitted_rows, 4);
		assert_eq!(report.telemetry.counters.emitted_batches, 1);
	}

	#[tokio::test]
	async fn live_shuffle_worker_sends_no_task_batch_for_shuffle_producer() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
		)
		.expect("batch");
		let memory = Arc::new(MemoryManager::new(8, 1024 * 1024));
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			WorkerId::from_u128(126),
			WorkerEndpoint::new("local"),
		));
		let identity = TaskIdentity::new(
			QueryId::from_u128(126),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let exchange_id = ExchangeId::new(126);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Exchange {
				input: Box::new(scan(TableId(1), 1)),
				partition_count: 1,
				key: BoundExprProgram::empty(),
			},
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let (mut client, server) = test_duplex(1024 * 1024);
		let worker_handle = tokio::spawn(async move { worker.handle_connection(server).await });

		send_payload(
			&mut client,
			MessageKind::StartTask,
			EnvelopeIdentity::task(identity),
			None,
			&StartTaskPayload { spec },
		)
		.await
		.expect("send start task");
		let started = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read task started");
		assert_eq!(started.kind, MessageKind::TaskStarted);
		let complete = read_envelope(&mut client, memory.as_ref())
			.await
			.expect("read task complete");
		assert_eq!(complete.kind, MessageKind::TaskComplete);
		let payload: TaskCompletePayload = decode_payload(&complete.payload).expect("complete");
		let TaskCompletionOutput::ShuffleProducer { manifest } = payload.output else {
			panic!("expected shuffle output");
		};
		assert_eq!(manifest.partitions.len(), 1);
		assert_eq!(manifest.partitions[0].row_count, RowCount::new(3));
		assert_eq!(
			payload.telemetry.expect("telemetry").counters.emitted_rows,
			3
		);

		drop(client);
		worker_handle
			.await
			.expect("worker task")
			.expect("worker ok");
	}

	#[tokio::test]
	async fn scan_pull_pauses_when_live_append_awaits() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let backlog_depth = crate::distributed::partition_store::LIVE_BACKLOG_DEPTH;
		let batches = (0..(backlog_depth + 2))
			.map(|value| {
				RecordBatch::try_new(
					schema.clone(),
					vec![Arc::new(Int64Array::from(vec![value as i64]))],
				)
				.expect("scan batch")
			})
			.collect::<Vec<_>>();
		let yielded_batches = Arc::new(AtomicUsize::new(0));
		let store = Arc::new(InMemoryPartitionStore::new());
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(CountingBatchStorageScan {
				schema: schema.clone(),
				batches,
				yielded_batches: yielded_batches.clone(),
			}),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			store.clone(),
			WorkerId::from_u128(124),
			WorkerEndpoint::new("local"),
		));
		let exchange_id = ExchangeId::new(124);
		let identity = TaskIdentity::new(
			QueryId::from_u128(124),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let spec = TaskSpec {
			identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Exchange {
				input: Box::new(scan(TableId(1), 1)),
				partition_count: 1,
				key: BoundExprProgram::empty(),
			},
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let address = PartitionAddress {
			query_id: identity.query_id,
			exchange_id,
			producer_stage_id: identity.stage_id,
			producer_task_id: identity.task_id,
			attempt_id: identity.attempt_id,
			partition_id: PartitionId::new(0),
		};
		let mut live = store
			.pull_live(address, AttemptId::first())
			.expect("live pull");
		let producer =
			tokio::spawn(
				async move { worker.execute_task_local_with_telemetry(&spec, None).await },
			);

		let blocked_at = backlog_depth + 1;
		tokio::time::timeout(Duration::from_secs(1), async {
			while yielded_batches.load(Ordering::SeqCst) < blocked_at {
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("producer should pull until the first over-depth append blocks");
		assert_eq!(yielded_batches.load(Ordering::SeqCst), blocked_at);
		assert!(
			tokio::time::timeout(Duration::from_millis(50), async {
				while yielded_batches.load(Ordering::SeqCst) == blocked_at {
					tokio::task::yield_now().await;
				}
			})
			.await
			.is_err(),
			"upstream scan must stop pulling while append_live awaits capacity"
		);

		let first = live
			.next()
			.await
			.expect("first live chunk")
			.expect("first live chunk ok");
		assert_eq!(
			decode_record_batches(&first.bytes).unwrap()[0].num_rows(),
			1
		);
		tokio::time::timeout(Duration::from_secs(1), async {
			while yielded_batches.load(Ordering::SeqCst) <= blocked_at {
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("producer should resume scanning after the consumer drains one chunk");

		store.cancel_live(address, PartitionMissingReason::WorkerLost);
		let _ = tokio::time::timeout(Duration::from_secs(1), producer)
			.await
			.expect("producer should wake after live cancellation")
			.expect("producer task join");
	}

	#[tokio::test]
	async fn live_consumer_does_not_materialize_all_shuffle_input_before_execution() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))])
			.expect("batch");
		let store = Arc::new(InMemoryPartitionStore::new());
		let exchange_id = ExchangeId::new(120);
		let address = PartitionAddress {
			query_id: QueryId::from_u128(120),
			exchange_id,
			producer_stage_id: StageId::new(0),
			producer_task_id: TaskId::new(0),
			attempt_id: AttemptId::first(),
			partition_id: PartitionId::new(0),
		};
		store.open_live(address, batch.schema()).expect("open live");
		let bytes = encode_record_batch(&batch).expect("encode");
		store
			.append_live(PartitionPayload::new(
				address,
				bytes.clone(),
				RowCount::new(2),
				ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes)),
			))
			.await
			.expect("append live");
		let worker = ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan {
				table_data: RecordBatch::new_empty(batch.schema()),
			}),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			store,
			WorkerId::from_u128(18),
			WorkerEndpoint::new("local"),
		);
		let scan = scan(
			crate::distributed::task::shuffle_input_table_id(exchange_id),
			2,
		);
		let spec = TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(120),
				StageId::new(1),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: Some(PartitionId::new(0)),
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Take {
				input: Box::new(scan),
				limit: 1,
				output_schema: batch.schema(),
			},
			inputs: vec![TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id: StageId::new(0),
				partition_id: PartitionId::new(0),
				producer_partitions: vec![ShufflePartitionRef::Live(LiveProducerPartitionRef {
					address,
					worker_id: WorkerId::from_u128(17),
					endpoint: WorkerEndpoint::new("local"),
				})],
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};

		let report = tokio::time::timeout(
			Duration::from_secs(1),
			worker.execute_task_local_with_telemetry(&spec, None),
		)
		.await
		.expect("consumer should not wait for live seal")
		.expect("live consumer");

		assert_eq!(report.results.len(), 1);
		assert_eq!(report.results[0].num_rows(), 1);
	}

	#[tokio::test]
	async fn multi_upstream_streamable_both_worker_uses_live_input_storage_scan() {
		let rows = execute_live_hash_join_kind(JoinKind::Inner).await;

		assert_eq!(rows, 1);
	}

	#[tokio::test]
	async fn multi_upstream_streamable_both_all_hashpartitioned_kinds_execute_correctly() {
		for kind in [
			JoinKind::Inner,
			JoinKind::LeftOuter,
			JoinKind::RightOuter,
			JoinKind::FullOuter,
			JoinKind::LeftSemi,
			JoinKind::RightSemi,
			JoinKind::LeftAnti,
			JoinKind::RightAnti,
		] {
			let rows = execute_live_hash_join_kind(kind).await;

			assert_eq!(rows, expected_live_hash_join_rows(kind), "{kind:?}");
		}
	}

	#[tokio::test]
	async fn execution_plan_join_still_rejected_by_drive_streamable_plan() {
		let left = scan(TableId(1), 41);
		let right = scan(TableId(2), 42);
		let plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left.clone()),
			right: Box::new(right.clone()),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: crate::binder::join_schema(
				JoinKind::Inner,
				&left.schema(),
				&right.schema(),
			),
		};
		let storage = Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(left.schema()),
		});
		let executor = Executor::with_context(storage, ExecutionContext::local_test());
		let mut collect = |_batch: RecordBatch| -> Result<bool, QueryError> { Ok(true) };
		let mut sink = SyncStreamableBatchSink::new(&mut collect);

		let err = drive_streamable_plan(&plan, &executor, None, &mut sink)
			.await
			.expect_err("join must stay rejected by streamable output path");

		assert!(matches!(
			err,
			QueryError::Internal { message } if message == "internal: non-streamable plan reached live stream path"
		));
	}

	async fn execute_live_hash_join_kind(kind: JoinKind) -> usize {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let left_batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
		)
		.expect("left batch");
		let right_batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![2_i64, 3]))],
		)
		.expect("right batch");
		let store = Arc::new(InMemoryPartitionStore::new());
		let query_id = QueryId::from_u128(123_000 + kind as u128);
		let left_exchange_id = ExchangeId::new(1239);
		let right_exchange_id = ExchangeId::new(1240);
		let left_address = PartitionAddress {
			query_id,
			exchange_id: left_exchange_id,
			producer_stage_id: StageId::new(0),
			producer_task_id: TaskId::new(0),
			attempt_id: AttemptId::first(),
			partition_id: PartitionId::new(0),
		};
		let right_address = PartitionAddress {
			query_id,
			exchange_id: right_exchange_id,
			producer_stage_id: StageId::new(1),
			producer_task_id: TaskId::new(0),
			attempt_id: AttemptId::first(),
			partition_id: PartitionId::new(0),
		};
		let endpoint = WorkerEndpoint::new("local");
		let worker_id = WorkerId::from_u128(23);
		seed_live_partition(
			store.as_ref(),
			left_address,
			&left_batch,
			worker_id,
			endpoint.clone(),
		)
		.await;
		seed_live_partition(
			store.as_ref(),
			right_address,
			&right_batch,
			worker_id,
			endpoint.clone(),
		)
		.await;
		let worker = ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan {
				table_data: RecordBatch::new_empty(schema.clone()),
			}),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			store,
			worker_id,
			endpoint.clone(),
		);
		let left = scan(shuffle_input_table_id(left_exchange_id), 43);
		let right = scan(shuffle_input_table_id(right_exchange_id), 44);
		let spec = TaskSpec {
			identity: TaskIdentity::new(
				query_id,
				StageId::new(2),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: Some(PartitionId::new(0)),
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Join {
				replay_id: JoinReplayId::SYNTHETIC,
				replay_eligibility: BroadcastReplayEligibility::Terminal,
				left: Box::new(left.clone()),
				right: Box::new(right.clone()),
				kind,
				left_keys: vec![ColumnOrdinal(0)],
				right_keys: vec![ColumnOrdinal(0)],
				match_filter: None,
				null_safe_keys: false,
				strategy: JoinStrategy::HashPartitioned,
				output_schema: crate::binder::join_schema(kind, &left.schema(), &right.schema()),
			},
			inputs: vec![
				TaskInputBinding::ShuffleConsumer {
					exchange_id: left_exchange_id,
					producer_stage_id: StageId::new(0),
					partition_id: PartitionId::new(0),
					producer_partitions: vec![ShufflePartitionRef::Live(
						LiveProducerPartitionRef {
							address: left_address,
							worker_id,
							endpoint: endpoint.clone(),
						},
					)],
				},
				TaskInputBinding::ShuffleConsumer {
					exchange_id: right_exchange_id,
					producer_stage_id: StageId::new(1),
					partition_id: PartitionId::new(0),
					producer_partitions: vec![ShufflePartitionRef::Live(
						LiveProducerPartitionRef {
							address: right_address,
							worker_id,
							endpoint,
						},
					)],
				},
			],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};

		let report = tokio::time::timeout(
			Duration::from_secs(1),
			worker.execute_task_local_with_telemetry(&spec, None),
		)
		.await
		.expect("live hash join should finish from sealed live chunks")
		.expect("live hash join");

		report.results.iter().map(RecordBatch::num_rows).sum()
	}

	async fn seed_live_partition(
		store: &InMemoryPartitionStore,
		address: PartitionAddress,
		batch: &RecordBatch,
		worker_id: WorkerId,
		endpoint: WorkerEndpoint,
	) {
		store.open_live(address, batch.schema()).expect("open live");
		let bytes = encode_record_batch(batch).expect("encode live batch");
		let payload = PartitionPayload::new(
			address,
			bytes.clone(),
			RowCount::new(u64::try_from(batch.num_rows()).expect("row count fits u64")),
			ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes)),
		);
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		store
			.seal_live(PartitionManifest {
				query_id: address.query_id,
				stage_id: address.producer_stage_id,
				task_id: address.producer_task_id,
				attempt_id: address.attempt_id,
				exchange_id: address.exchange_id,
				producer_worker_id: worker_id,
				producer_endpoint: endpoint,
				partitions: vec![payload.entry()],
				runtime_filter_fragment: None,
				backend: PartitionBackend::InMemory,
			})
			.expect("seal live");
	}

	fn expected_live_hash_join_rows(kind: JoinKind) -> usize {
		match kind {
			JoinKind::Inner => 1,
			JoinKind::LeftOuter => 2,
			JoinKind::RightOuter => 2,
			JoinKind::FullOuter => 3,
			JoinKind::LeftSemi => 1,
			JoinKind::RightSemi => 1,
			JoinKind::LeftAnti => 1,
			JoinKind::RightAnti => 1,
			JoinKind::Cross => {
				unreachable!(
					"CROSS is broadcast-only; not exercised by the live hash-join-kinds test"
				)
			}
		}
	}

	#[tokio::test]
	async fn downstream_take_cancels_live_shuffle_and_stops_upstream_scan() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let first = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
		)
		.expect("first batch");
		let second = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
		)
		.expect("second batch");
		let second_emitted = Arc::new(AtomicBool::new(false));
		let store = Arc::new(InMemoryPartitionStore::new());
		let producer_worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(CancellationAwareTwoBatchStorageScan {
				schema: schema.clone(),
				first,
				second,
				second_emitted: second_emitted.clone(),
			}),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			store.clone(),
			WorkerId::from_u128(19),
			WorkerEndpoint::new("local"),
		));
		let consumer_worker = ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan {
				table_data: RecordBatch::new_empty(schema.clone()),
			}),
			Arc::new(MemoryManager::new(8, 1024 * 1024)),
			store,
			WorkerId::from_u128(20),
			WorkerEndpoint::new("local"),
		);
		let query_id = QueryId::from_u128(119_001);
		let exchange_id = ExchangeId::new(121);
		let producer_identity = TaskIdentity::new(
			query_id,
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let producer_spec = TaskSpec {
			identity: producer_identity,
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Exchange {
				input: Box::new(scan(TableId(1), 3)),
				partition_count: 1,
				key: BoundExprProgram::empty(),
			},
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
				publish_mode: crate::distributed::task::ShufflePublishMode::Live,
			},
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};
		let address = PartitionAddress {
			query_id,
			exchange_id,
			producer_stage_id: producer_identity.stage_id,
			producer_task_id: producer_identity.task_id,
			attempt_id: producer_identity.attempt_id,
			partition_id: PartitionId::new(0),
		};
		let downstream = DownstreamAttemptRegistry::with_default_limits(None);
		let producer = {
			let producer_worker = producer_worker.clone();
			let producer_spec = producer_spec.clone();
			let downstream_cancellation = downstream.context();
			tokio::spawn(async move {
				producer_worker
					.execute_task_local_with_telemetry(
						&producer_spec,
						Some(downstream_cancellation),
					)
					.await
			})
		};
		let scan = scan(shuffle_input_table_id(exchange_id), 4);
		let consumer_spec = TaskSpec {
			identity: TaskIdentity::new(
				query_id,
				StageId::new(1),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: Some(PartitionId::new(0)),
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Take {
				input: Box::new(scan),
				limit: 1,
				output_schema: schema,
			},
			inputs: vec![TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id: StageId::new(0),
				partition_id: PartitionId::new(0),
				producer_partitions: vec![ShufflePartitionRef::Live(LiveProducerPartitionRef {
					address,
					worker_id: WorkerId::from_u128(19),
					endpoint: WorkerEndpoint::new("local"),
				})],
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		};

		let consumer_report = tokio::time::timeout(
			Duration::from_secs(1),
			consumer_worker.execute_task_local_with_telemetry(&consumer_spec, None),
		)
		.await
		.expect("consumer should finish from the first live chunk")
		.expect("consumer report");
		assert_eq!(consumer_report.results.len(), 1);
		assert_eq!(consumer_report.results[0].num_rows(), 1);

		let producer_error = tokio::time::timeout(Duration::from_secs(1), producer)
			.await
			.expect("producer should observe downstream cancellation")
			.expect("producer task join")
			.err()
			.expect("producer scan should be cancelled");
		match producer_error {
			QueryError::Storage(StorageError::Cancelled)
			| QueryError::Execution(ExecutionError::Storage(StorageError::Cancelled)) => {}
			other => panic!("unexpected producer error: {other:?}"),
		}
		assert!(
			!second_emitted.load(Ordering::SeqCst),
			"producer must not emit the second scan batch after downstream take finishes"
		);
	}

	fn scan(table_id: TableId, object_byte: u8) -> ExecutionPlan {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([object_byte; 16]),
			1,
			"default",
			"broadcast_cap_test",
			schema.as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor");
		ExecutionPlan::Scan {
			local_table_id: table_id,
			table: descriptor.table_object_id,
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			row_limit: None,
			profile_hint: QueryProfileHint::Scan,
			estimated_byte_size: None,
			primary_key_columns: vec![ColumnOrdinal(0)],
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}
}

type StreamableBatchFuture<'a> =
	std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, QueryError>> + Send + 'a>>;

trait StreamableBatchSink: Send {
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a>;

	fn emitted_rows(&self) -> u64 {
		0
	}

	fn emitted_batches(&self) -> u64 {
		0
	}

	fn batch_encode_micros(&self) -> u64 {
		0
	}

	fn batch_send_micros(&self) -> u64 {
		0
	}
}

struct LiveAppendStreamableBatchSink<'a> {
	executor: &'a Executor,
	partition_store: Arc<dyn PartitionStore>,
	addresses: &'a [PartitionAddress],
	partition_count: PartitionCount,
	partition_key: &'a BoundExprProgram,
	runtime_filter_builder: Option<RuntimeFilterFragmentBuilder>,
	partition_row_counts: Vec<u64>,
	partition_chunk_counts: Vec<u64>,
	emitted_rows: u64,
	emitted_batches: u64,
}

impl StreamableBatchSink for LiveAppendStreamableBatchSink<'_> {
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a> {
		Box::pin(async move {
			let partitioned_batches = partition_record_batch(
				self.executor,
				&batch,
				self.partition_count.get(),
				self.partition_key,
			)?;
			if let Some(builder) = &mut self.runtime_filter_builder {
				builder.push_batch(&batch)?;
			}
			for (partition, partition_batch) in partitioned_batches.into_iter().enumerate() {
				let Some(partition_batch) = partition_batch else {
					continue;
				};
				let row_count =
					u64::try_from(partition_batch.num_rows()).expect("row count fits u64");
				let bytes = encode_record_batch(&partition_batch)?;
				let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
				let payload = PartitionPayload::new(
					self.addresses[partition],
					bytes,
					RowCount::new(row_count),
					digest,
				);
				self.partition_store
					.append_live(payload)
					.await
					.map_err(partition_store_publish_error)?;
				self.partition_row_counts[partition] =
					self.partition_row_counts[partition].saturating_add(row_count);
				self.partition_chunk_counts[partition] =
					self.partition_chunk_counts[partition].saturating_add(1);
				self.emitted_rows = self.emitted_rows.saturating_add(row_count);
				self.emitted_batches = self.emitted_batches.saturating_add(1);
			}
			Ok(true)
		})
	}
}

struct SyncStreamableBatchSink<F> {
	on_batch: F,
}

impl<F> SyncStreamableBatchSink<F> {
	fn new(on_batch: F) -> Self {
		Self { on_batch }
	}
}

impl<F> StreamableBatchSink for SyncStreamableBatchSink<F>
where
	F: FnMut(RecordBatch) -> Result<bool, QueryError> + Send,
{
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a> {
		let result = (self.on_batch)(batch);
		Box::pin(async move { result })
	}
}

struct FilterStreamableBatchSink<'a, 'b> {
	executor: &'a Executor,
	predicate: &'a BoundExprProgram,
	disposition: &'a FilterDisposition,
	downstream: &'b mut dyn StreamableBatchSink,
}

impl StreamableBatchSink for FilterStreamableBatchSink<'_, '_> {
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a> {
		Box::pin(async move {
			if *self.disposition == FilterDisposition::FullyPushed {
				return self.downstream.emit(batch).await;
			}
			for filtered in self.executor.filter_batches_with_bound_predicate(
				self.predicate,
				vec![batch],
				"filter predicate",
			)? {
				if filtered.num_rows() > 0 && !self.downstream.emit(filtered).await? {
					return Ok(false);
				}
			}
			Ok(true)
		})
	}
}

struct ProjectStreamableBatchSink<'a, 'b> {
	executor: &'a Executor,
	exprs: &'a BoundExprProgram,
	output_schema: &'a SchemaRef,
	downstream: &'b mut dyn StreamableBatchSink,
}

impl StreamableBatchSink for ProjectStreamableBatchSink<'_, '_> {
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a> {
		Box::pin(async move {
			let cols = self.executor.evaluate_bound_program(self.exprs, &batch)?;
			self.downstream
				.emit(RecordBatch::try_new(self.output_schema.clone(), cols)?)
				.await
		})
	}
}

struct TakeStreamableBatchSink<'a> {
	remaining: usize,
	downstream: &'a mut dyn StreamableBatchSink,
}

impl StreamableBatchSink for TakeStreamableBatchSink<'_> {
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a> {
		Box::pin(async move {
			if self.remaining == 0 {
				return Ok(false);
			}
			let batch = if batch.num_rows() > self.remaining {
				batch.slice(0, self.remaining)
			} else {
				batch
			};
			self.remaining = self.remaining.saturating_sub(batch.num_rows());
			if batch.num_rows() > 0 && !self.downstream.emit(batch).await? {
				return Ok(false);
			}
			Ok(self.remaining > 0)
		})
	}
}

struct WorkerTaskBatchSink<'a, S> {
	stream: &'a mut S,
	identity: TaskIdentity,
	signal: TaskCancellationSignal,
	next_seq: u64,
	emitted_rows: u64,
	emitted_batches: u64,
	batch_encode_micros: u64,
	batch_send_micros: u64,
}

impl<'a, S> WorkerTaskBatchSink<'a, S> {
	fn new(stream: &'a mut S, identity: TaskIdentity, signal: TaskCancellationSignal) -> Self {
		Self {
			stream,
			identity,
			signal,
			next_seq: 0,
			emitted_rows: 0,
			emitted_batches: 0,
			batch_encode_micros: 0,
			batch_send_micros: 0,
		}
	}
}

impl<S> StreamableBatchSink for WorkerTaskBatchSink<'_, S>
where
	S: tokio::io::AsyncWrite + Unpin + Send,
{
	fn emit<'a>(&'a mut self, batch: RecordBatch) -> StreamableBatchFuture<'a> {
		Box::pin(async move {
			if self.signal.is_cancelled() {
				return Err(cancelled_task_error());
			}
			let stream_seq = StreamSeq::new(self.next_seq);
			self.next_seq = self.next_seq.saturating_add(1);
			let batch_encode_started = Instant::now();
			let encoded_batch = encode_record_batch(&batch)?;
			self.batch_encode_micros = self
				.batch_encode_micros
				.saturating_add(duration_micros(batch_encode_started));
			let batch_send_started = Instant::now();
			send_payload(
				self.stream,
				MessageKind::TaskBatch,
				EnvelopeIdentity::task(self.identity),
				Some(stream_seq),
				&TaskBatchPayload {
					identity: self.identity,
					stream_seq,
					payload: ArrowIpcBytes::new(encoded_batch),
				},
			)
			.await?;
			self.batch_send_micros = self
				.batch_send_micros
				.saturating_add(duration_micros(batch_send_started));
			self.emitted_rows = self.emitted_rows.saturating_add(
				u64::try_from(batch.num_rows()).expect("record batch row count fits u64"),
			);
			self.emitted_batches = self.emitted_batches.saturating_add(1);
			Ok(true)
		})
	}

	fn emitted_rows(&self) -> u64 {
		self.emitted_rows
	}

	fn emitted_batches(&self) -> u64 {
		self.emitted_batches
	}

	fn batch_encode_micros(&self) -> u64 {
		self.batch_encode_micros
	}

	fn batch_send_micros(&self) -> u64 {
		self.batch_send_micros
	}
}

fn drive_streamable_plan<'a>(
	plan: &'a ExecutionPlan,
	executor: &'a Executor,
	morsel: Option<(MorselId, u32)>,
	on_batch: &'a mut dyn StreamableBatchSink,
) -> StreamableBatchFuture<'a> {
	Box::pin(async move {
		match plan {
			ExecutionPlan::Scan {
				local_table_id,
				table,
				fingerprint,
				projection,
				pbc_program,
				row_limit,
				row_identity,
				profile_hint,
				..
			} => {
				if *row_limit == Some(0) {
					return Ok(true);
				}
				let context = executor.context();
				let req = ScanRequest {
					session_id: context.session_id,
					query_id: context.query_id,
					tenant_id: context.tenant_id,
					table_id: *local_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					read_bound: context.read_bound,
					projected_columns: projection.clone(),
					pbc_predicate: pbc_program.as_ref().map(|program| program.0.clone()),
					runtime_filter: context.runtime_filter.clone(),
					row_identity: *row_identity,
					profile_hint: Some(*profile_hint),
					trace_context: context.trace_context,
					morsel,
					row_limit: *row_limit,
					downstream_cancellation: context.downstream_cancellation.clone(),
				};
				let stream = executor.storage_arc().scan(req).await?;
				let mut stream = stream.into_batches();
				let mut rows = 0usize;
				while let Some(batch) = stream.next().await {
					let mut batch = batch?;
					if let Some(limit) = row_limit {
						if rows >= *limit {
							break;
						}
						if batch.num_rows() > limit.saturating_sub(rows) {
							batch = batch.slice(0, limit.saturating_sub(rows));
						}
						rows = rows.saturating_add(batch.num_rows());
					}
					if batch.num_rows() == 0 {
						continue;
					}
					if !on_batch.emit(batch).await? {
						return Ok(false);
					}
				}
				Ok(true)
			}
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				let mut filter_batch = FilterStreamableBatchSink {
					executor,
					predicate,
					disposition,
					downstream: on_batch,
				};
				drive_streamable_plan(input, executor, morsel, &mut filter_batch).await
			}
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				let mut project_batch = ProjectStreamableBatchSink {
					executor,
					exprs,
					output_schema,
					downstream: on_batch,
				};
				drive_streamable_plan(input, executor, morsel, &mut project_batch).await
			}
			ExecutionPlan::Take {
				input,
				limit,
				output_schema,
			} => {
				if *limit == 0 {
					let empty = RecordBatch::new_empty(output_schema.clone());
					let _ = on_batch.emit(empty).await?;
					return Ok(false);
				}
				let mut take_batch = TakeStreamableBatchSink {
					remaining: *limit,
					downstream: on_batch,
				};
				drive_streamable_plan(input, executor, morsel, &mut take_batch).await
			}
			ExecutionPlan::Exchange { .. }
			| ExecutionPlan::Union { .. }
			| ExecutionPlan::Sort { .. }
			| ExecutionPlan::Window { .. }
			| ExecutionPlan::Aggregate { .. }
			| ExecutionPlan::Join { .. }
			| ExecutionPlan::Repeat { .. }
			| ExecutionPlan::EmptyRelation { .. }
			| ExecutionPlan::SystemTable { .. }
			| ExecutionPlan::CreateTable { .. }
			| ExecutionPlan::CreateLogicalType { .. }
			| ExecutionPlan::CreateTableAsSelect { .. }
			| ExecutionPlan::DropTable { .. }
			| ExecutionPlan::Delete { .. }
			| ExecutionPlan::Update { .. }
			| ExecutionPlan::Insert { .. }
			| ExecutionPlan::InsertSelect { .. }
			// ADR 255: a recursive CTE is a materializing fixpoint, not streamable — it runs through
			// the buffered executor path (`execute_recursive_cte`), never this live-stream driver.
			| ExecutionPlan::RecursiveCte { .. }
			| ExecutionPlan::RecursiveRef { .. } => Err(QueryError::Internal {
				message: "internal: non-streamable plan reached live stream path".to_string(),
			}),
		}
	})
}

fn partition_record_batch(
	executor: &Executor,
	batch: &RecordBatch,
	partition_count: u32,
	key: &BoundExprProgram,
) -> Result<Vec<Option<RecordBatch>>, QueryError> {
	let key_arrays = executor.evaluate_bound_program(key, batch)?;
	let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
	for row in 0..batch.num_rows() {
		let bucket = crate::exec::partition_bucket(&key_arrays, row, partition_count)?;
		builders[bucket].push(row as u32);
	}
	builders
		.into_iter()
		.map(|indices| {
			if indices.is_empty() {
				return Ok(None);
			}
			let indices_arr = arrow::array::UInt32Array::from(indices);
			let cols = batch
				.columns()
				.iter()
				.map(|col| arrow::compute::take(col, &indices_arr, None))
				.collect::<Result<Vec<ArrayRef>, _>>()?;
			Ok(Some(RecordBatch::try_new(batch.schema(), cols)?))
		})
		.collect()
}

#[derive(Clone)]
struct ScanOverride {
	schema: SchemaRef,
	batches: Vec<RecordBatch>,
}

#[derive(Clone)]
struct LiveScanOverride {
	schema: SchemaRef,
	partitions: Vec<LiveProducerPartitionRef>,
}

struct TaskInputStorageScan {
	base: Arc<dyn StorageScan>,
	overrides: HashMap<TableId, ScanOverride>,
	legacy_single_override: Option<ScanOverride>,
}

struct LiveInputStorageScan {
	base: Arc<dyn StorageScan>,
	partition_store: Arc<dyn PartitionStore>,
	live_overrides: HashMap<TableId, LiveScanOverride>,
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

#[async_trait::async_trait]
impl StorageScan for LiveInputStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		if let Some(override_entry) = self.live_overrides.get(&request.table_id) {
			let schema = override_entry.schema.clone();
			let session_id = request.session_id;
			let query_id = request.query_id;
			let partitions = override_entry.partitions.clone();
			let partition_store = self.partition_store.clone();
			let stream = futures::stream::unfold(
				LiveScanState {
					partition_store,
					partitions,
					next_partition: 0,
					current: None,
					pending: VecDeque::new(),
				},
				|mut state| async move {
					loop {
						if let Some(batch) = state.pending.pop_front() {
							return Some((Ok(batch), state));
						}
						if state.current.is_none() {
							let partition = state.partitions.get(state.next_partition)?.clone();
							state.next_partition += 1;
							match state
								.partition_store
								.pull_live(partition.address, partition.address.attempt_id)
							{
								Ok(stream) => state.current = Some(stream),
								Err(reason) => {
									return Some((
										Err(storage_error_from_partition_missing(reason)),
										state,
									));
								}
							}
						}
						let current = state.current.as_mut().expect("current live stream");
						match current.next().await {
							Some(Ok(payload)) => match decode_record_batches(&payload.bytes) {
								Ok(batches) => state.pending.extend(batches),
								Err(error) => {
									return Some((
										Err(storage_error_from_query_error(error.into())),
										state,
									));
								}
							},
							Some(Err(reason)) => {
								return Some((
									Err(storage_error_from_partition_missing(reason)),
									state,
								));
							}
							None => state.current = None,
						}
					}
				},
			)
			.boxed();
			return Ok(ScanStream::new(schema, session_id, query_id, stream));
		}
		self.base.scan(request).await
	}
}

struct LiveScanState {
	partition_store: Arc<dyn PartitionStore>,
	partitions: Vec<LiveProducerPartitionRef>,
	next_partition: usize,
	current: Option<crate::distributed::partition_store::LivePartitionStream>,
	pending: VecDeque<RecordBatch>,
}

fn live_scan_overrides(spec: &TaskSpec) -> HashMap<TableId, LiveScanOverride> {
	let mut overrides = HashMap::new();
	for input in &spec.inputs {
		let TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_partitions,
			..
		} = input
		else {
			continue;
		};
		let partitions = producer_partitions
			.iter()
			.filter_map(|partition| match partition {
				ShufflePartitionRef::Live(live) => Some(live.clone()),
				ShufflePartitionRef::Sealed(_) => None,
			})
			.collect::<Vec<_>>();
		if partitions.is_empty() {
			continue;
		}
		let table_id = shuffle_input_table_id(*exchange_id);
		let schema =
			scan_schema_for_table_id(&spec.plan, table_id).unwrap_or_else(|| spec.plan.schema());
		overrides.insert(table_id, LiveScanOverride { schema, partitions });
	}
	overrides
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
			local_table_id,
			schema,
			..
		} if *local_table_id == table_id => Some(schema.clone()),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => scan_schema_for_table_id(input, table_id),
		ExecutionPlan::Join { left, right, .. } => scan_schema_for_table_id(left, table_id)
			.or_else(|| scan_schema_for_table_id(right, table_id)),
		ExecutionPlan::Union { left, right, .. } => scan_schema_for_table_id(left, table_id)
			.or_else(|| scan_schema_for_table_id(right, table_id)),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE is one self-contained local node — opaque to these plan walkers.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
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
	active_query: Option<ActiveQueryHandle>,
	final_result_sender: Option<mpsc::Sender<Result<RecordBatch, QueryError>>>,
	final_result_batch_sent: Option<Arc<AtomicBool>>,
}

impl<Transport> NetworkTaskDispatcher<Transport> {
	pub fn new(memory: Arc<MemoryManager>, transport: Transport) -> Self {
		Self {
			coordinator: ToronCoordinator::new(memory),
			transport,
			active_query: None,
			final_result_sender: None,
			final_result_batch_sent: None,
		}
	}

	pub fn with_active_query(mut self, active_query: Option<ActiveQueryHandle>) -> Self {
		self.active_query = active_query;
		self
	}

	pub fn with_final_result_sender(
		mut self,
		sender: Option<mpsc::Sender<Result<RecordBatch, QueryError>>>,
		batch_sent: Option<Arc<AtomicBool>>,
	) -> Self {
		self.final_result_sender = sender;
		self.final_result_batch_sent = batch_sent;
		self
	}
}

#[async_trait::async_trait]
impl<Transport> TaskDispatcher for NetworkTaskDispatcher<Transport>
where
	Transport: ToronTransport + Send + Sync,
{
	async fn dispatch(
		&self,
		worker: &crate::distributed::worker_registry::WorkerEntry,
		spec: &TaskSpec,
	) -> DispatchOutcome {
		if self
			.active_query
			.as_ref()
			.is_some_and(ActiveQueryHandle::is_cancelled)
		{
			return DispatchOutcome::Failed(TaskFailureKind::Cancelled);
		}
		let stream = match self.transport.connect_worker(worker).await {
			Ok(stream) => stream,
			Err(error) => return DispatchOutcome::Failed(dispatch_failure_kind(error)),
		};
		if let Some(active_query) = &self.active_query {
			active_query.record_remote_task(worker.clone(), spec.identity);
			if active_query.is_cancelled() {
				return DispatchOutcome::Failed(TaskFailureKind::Cancelled);
			}
		}

		match self
			.coordinator
			.execute_task_on_worker_with_telemetry_and_batch_sender(
				stream,
				spec.clone(),
				self.final_result_sender.clone(),
				self.final_result_batch_sent.clone(),
			)
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
		stream: Stream,
		spec: TaskSpec,
	) -> Result<RemoteTaskCompletion, QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		self.execute_task_on_worker_with_telemetry_and_batch_sender(stream, spec, None, None)
			.await
	}

	pub async fn execute_task_on_worker_with_telemetry_and_batch_sender<Stream>(
		&self,
		mut stream: Stream,
		spec: TaskSpec,
		final_result_sender: Option<mpsc::Sender<Result<RecordBatch, QueryError>>>,
		final_result_batch_sent: Option<Arc<AtomicBool>>,
	) -> Result<RemoteTaskCompletion, QueryError>
	where
		Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		let identity = spec.identity;
		let forward_final_batches =
			matches!(spec.output, TaskOutputBinding::FinalResult) && final_result_sender.is_some();
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
		let terminal_result = loop {
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
					for batch in decode_record_batches(payload.payload.as_slice())? {
						if forward_final_batches {
							if let Some(batch_sent) = &final_result_batch_sent {
								batch_sent.store(true, Ordering::SeqCst);
							}
							let sender = final_result_sender
								.as_ref()
								.expect("checked final result sender");
							if sender.send(Ok(batch)).await.is_err() {
								return Err(cancelled_task_error());
							}
						} else {
							results.push(batch);
						}
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
					break Ok((payload.output, telemetry));
				}
				MessageKind::TaskFailed => {
					let payload: TaskFailedPayload = decode_payload(&response.payload)?;
					break Err(QueryError::Runtime(RuntimeError::TaskFailure {
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
		let (output, telemetry) = finish_terminal_exchange(&mut stream, terminal_result).await?;

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
		let terminal_result = match response.kind {
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
			_ => {
				return Err(QueryError::UnexpectedMessage {
					expected: "PartitionData or PartitionMissing",
				});
			}
		};
		finish_terminal_exchange(&mut stream, terminal_result).await
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

		finish_terminal_exchange(&mut stream, Ok(())).await
	}
}

async fn finish_terminal_exchange<Stream, Output>(
	stream: &mut Stream,
	terminal_result: Result<Output, QueryError>,
) -> Result<Output, QueryError>
where
	Stream: tokio::io::AsyncWrite + Unpin,
{
	match terminal_result {
		Ok(output) => {
			stream.shutdown().await?;
			Ok(output)
		}
		Err(error) => {
			// The peer's terminal application error is the request result. A
			// secondary close failure cannot replace it with transport noise.
			let _ = stream.shutdown().await;
			Err(error)
		}
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

async fn read_worker_envelope<Stream>(
	stream: &mut Stream,
	memory: &MemoryManager,
	phase: WorkerConnectionPhase,
) -> Result<Option<WireEnvelope>, QueryError>
where
	Stream: tokio::io::AsyncRead + Unpin,
{
	let mut len_buf = [0u8; 4];
	match stream.read(&mut len_buf[..1]).await {
		Ok(0) => return Ok(None),
		Ok(_) => {}
		Err(error)
			if phase == WorkerConnectionPhase::BetweenCompletedExchanges
				&& error.kind() == std::io::ErrorKind::NotConnected =>
		{
			// Quinn maps a connection disappearing after its final response to
			// NotConnected. At this exact protocol boundary no work is active;
			// the same error before a terminal response remains a failure.
			return Ok(None);
		}
		Err(error) => return Err(error.into()),
	}
	stream.read_exact(&mut len_buf[1..]).await?;
	let len = usize::try_from(u32::from_le_bytes(len_buf)).expect("u32 frame length fits usize");

	let mut msg_buf = memory.acquire_at_least(len);
	msg_buf.as_mut_vec().resize(len, 0);
	stream.read_exact(msg_buf.as_mut_vec()).await?;

	Ok(Some(WireEnvelope::decode(&msg_buf)?))
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
	phase: &mut WorkerConnectionPhase,
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
			producer: _,
			requested,
			available,
			..
		}) => TaskFailureKind::BudgetExhausted {
			dimension: *dimension,
			requested: *requested,
			available: *available,
		},
		QueryError::Execution(ExecutionError::Storage(StorageError::DescriptorEvolved {
			table_id,
		}))
		| QueryError::Storage(StorageError::DescriptorEvolved { table_id }) => {
			TaskFailureKind::DescriptorEvolved {
				table_id: *table_id,
				expected: None,
				current: None,
			}
		}
		QueryError::Execution(ExecutionError::Storage(StorageError::Cancelled))
		| QueryError::Storage(StorageError::Cancelled) => TaskFailureKind::Cancelled,
		_ => TaskFailureKind::Internal {
			reason: error.to_string(),
		},
	};
	send_terminal_payload(
		stream,
		phase,
		MessageKind::TaskFailed,
		EnvelopeIdentity::task(identity),
		None,
		&TaskFailedPayload::new_worker_emitted(identity, kind).expect("kind is worker-emittable"),
	)
	.await
}

async fn send_terminal_payload<Stream, Payload>(
	stream: &mut Stream,
	phase: &mut WorkerConnectionPhase,
	kind: MessageKind,
	identity: EnvelopeIdentity,
	stream_seq: Option<StreamSeq>,
	payload: &Payload,
) -> Result<(), QueryError>
where
	Stream: tokio::io::AsyncWrite + Unpin,
	Payload: serde::Serialize,
{
	send_payload(stream, kind, identity, stream_seq, payload).await?;
	*phase = WorkerConnectionPhase::BetweenCompletedExchanges;
	Ok(())
}

fn cancelled_task_error() -> QueryError {
	QueryError::Runtime(RuntimeError::TaskFailure {
		kind: TaskFailureKind::Cancelled,
	})
}

fn audit_unavailable_task_error() -> QueryError {
	QueryError::Runtime(RuntimeError::TaskFailure {
		kind: TaskFailureKind::TaskFailed {
			reason: "audit_unavailable".to_string(),
			retryable: true,
		},
	})
}

fn task_envelope_identity(identity: EnvelopeIdentity) -> Result<TaskIdentity, QueryError> {
	match identity {
		EnvelopeIdentity::Task(identity) => Ok(identity),
		_ => Err(QueryError::UnexpectedMessage {
			expected: "task envelope identity",
		}),
	}
}

fn partition_missing_error(reason: PartitionMissingReason) -> QueryError {
	QueryError::Runtime(RuntimeError::PartitionStoreMismatch {
		message: format!("{reason:?}"),
	})
}

fn storage_error_from_partition_missing(reason: PartitionMissingReason) -> StorageError {
	match reason {
		PartitionMissingReason::CancelledTaskCleanup => StorageError::Cancelled,
		other => StorageError::Internal {
			message: format!("live partition unavailable: {other:?}"),
		},
	}
}

fn storage_error_from_query_error(error: QueryError) -> StorageError {
	StorageError::Internal {
		message: error.to_string(),
	}
}

fn dispatch_failure_kind(error: QueryError) -> TaskFailureKind {
	let reason = error.to_string();
	match error {
		QueryError::Runtime(RuntimeError::TaskFailure { kind }) => kind,
		QueryError::Execution(crate::error::ExecutionError::BudgetExhausted {
			dimension,
			producer: _,
			requested,
			available,
			..
		}) => TaskFailureKind::BudgetExhausted {
			dimension,
			requested,
			available,
		},
		QueryError::Execution(ExecutionError::Storage(StorageError::DescriptorEvolved {
			table_id,
		}))
		| QueryError::Storage(StorageError::DescriptorEvolved { table_id }) => {
			TaskFailureKind::DescriptorEvolved {
				table_id,
				expected: None,
				current: None,
			}
		}
		QueryError::Execution(ExecutionError::Storage(StorageError::Cancelled))
		| QueryError::Storage(StorageError::Cancelled) => TaskFailureKind::Cancelled,
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
