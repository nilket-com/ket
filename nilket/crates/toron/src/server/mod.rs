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
use crate::distributed::budget::{BudgetDimension, ByteCount};
use crate::distributed::cancellation::{
	ActiveQueryHandle, ActiveTaskRegistry, DownstreamCancellationContext,
	DownstreamCancellationMetrics,
};
use crate::distributed::identity::{FrameByteLen, PartitionId, StreamSeq, TaskIdentity, WorkerId};
use crate::distributed::partition_store::{
	InMemoryPartitionStore, PartitionPayload, PartitionStore, PartitionStoreError,
};
use crate::distributed::scheduler::{DispatchOutcome, TaskDispatcher};
use crate::distributed::task::{
	ArrowIpcDigest, LiveProducerPartitionRef, PartitionAddress, PartitionBackend,
	PartitionManifest, RowCount, ShufflePartitionRef, TaskCompletionOutput, TaskInputBinding,
	TaskOutputBinding, TaskSpec, TaskTelemetry, TaskTelemetryLabels,
	is_streamable_exchange_producer, is_streamable_shuffle_consumer, shuffle_input_table_id,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::error::{
	ExecutionError, PartitionMissingReason, QueryError, RuntimeError, StorageError,
	TaskFailureKind, WireError,
};
use crate::exec::{ExecutionContext, Executor};
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
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::Instrument;
use wendelon::schema::RuntimeFilterDescriptor;

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
			.with_runtime_filter(scan_runtime_filter(spec))
			.with_downstream_cancellation(downstream_cancellation);
			task_context.trace_context = if crate::telemetry::enabled() {
				crate::telemetry::task_trace_context(spec)
			} else {
				spec.context.trace_context
			};
			let (results, output) = if is_streamable_exchange_producer(spec) {
				self.execute_live_shuffle_producer(spec, task_context, morsel)
					.await?
			} else if self.has_live_shuffle_input(spec) && is_streamable_shuffle_consumer(spec) {
				self.execute_live_shuffle_consumer(spec, task_context, morsel)
					.await?
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
				task.counters.emitted_rows =
					results.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
				task.counters.emitted_batches = results.len() as u64;
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
					task_context.downstream_cancellation.clone(),
				)
				.map_err(partition_store_publish_error)?;
		}

		let executor = Executor::with_context(self.storage.clone(), task_context);
		let partition_store = self.partition_store.clone();
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();
		let mut append_batch = |batch: RecordBatch| -> Result<bool, QueryError> {
			for (partition, partition_batch) in
				partition_record_batch(&executor, &batch, partition_count.get(), partition_key)?
					.into_iter()
					.enumerate()
			{
				let Some(partition_batch) = partition_batch else {
					continue;
				};
				let bytes = encode_record_batch(&partition_batch)?;
				let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
				let payload = PartitionPayload::new(
					addresses[partition],
					bytes,
					RowCount::new(
						u64::try_from(partition_batch.num_rows()).expect("row count fits u64"),
					),
					digest,
				);
				partition_store
					.append_live(payload)
					.map_err(partition_store_publish_error)?;
				partition_batches[partition].push(partition_batch);
			}
			Ok(true)
		};
		drive_streamable_plan(input, &executor, morsel, &mut append_batch).await?;

		let results = partition_batches
			.into_iter()
			.map(|batches| {
				if batches.is_empty() {
					Ok(RecordBatch::new_empty(schema.clone()))
				} else {
					Ok(arrow::compute::concat_batches(&schema, &batches)?)
				}
			})
			.collect::<Result<Vec<_>, QueryError>>()?;
		let output = self.publish_task_output(spec.identity, spec, &results)?;
		if let TaskCompletionOutput::ShuffleProducer { manifest } = &output {
			self.partition_store
				.seal_live(manifest.clone())
				.map_err(partition_store_publish_error)?;
		}
		Ok((results, Some(output)))
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
		drive_streamable_plan(&spec.plan, &executor, morsel, &mut collect).await?;
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
		'connection: loop {
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
						send_task_failed(&mut stream, identity, error).await?;
						continue 'connection;
					}

					if let Err(error) = audit_recorder.record_started().await {
						tracing::warn!(error = %error, "required worker start audit unavailable");
						send_task_failed(&mut stream, identity, audit_unavailable_task_error())
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

					let execution = self.execute_task_local_with_telemetry(
						&payload.spec,
						Some(active_task.downstream.context()),
					);
					tokio::pin!(execution);
					let execution_result = tokio::select! {
						result = &mut execution => result,
						_ = active_task.signal.cancelled() => Err(cancelled_task_error()),
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
									send_task_failed(&mut stream, identity, error).await?;
									continue 'connection;
								}
								let stream_seq = StreamSeq::new(
									u64::try_from(idx).expect("record batch index fits u64"),
								);
								let batch_encode_started = Instant::now();
								let encoded_batch = match encode_record_batch(batch) {
									Ok(encoded_batch) => encoded_batch,
									Err(error) => {
										let error = match audit_recorder.record_failed(&error).await
										{
											Ok(()) => error,
											Err(audit_error) => {
												tracing::warn!(error = %audit_error, "required worker failure audit unavailable");
												audit_unavailable_task_error()
											}
										};
										send_task_failed(&mut stream, identity, error).await?;
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
							if active_task.signal.is_cancelled() {
								continue 'connection;
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
								send_task_failed(&mut stream, identity, error).await?;
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
										send_task_failed(&mut stream, identity, error).await?;
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
											identity,
											audit_unavailable_task_error(),
										)
										.await?;
										continue 'connection;
									}
									if let Err(error) = send_payload(
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
									send_task_failed(&mut stream, identity, error).await?;
								}
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
								send_task_failed(&mut stream, identity, error).await?;
								continue 'connection;
							} else {
								let error = match audit_recorder.record_failed(&error).await {
									Ok(()) => error,
									Err(audit_error) => {
										tracing::warn!(error = %audit_error, "required worker failure audit unavailable");
										audit_unavailable_task_error()
									}
								};
								send_task_failed(&mut stream, identity, error).await?;
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
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => join_strategy_label(input),
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
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => contains_join(input),
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
		| ExecutionPlan::Take { input, .. } => contains_broadcast_join(input),
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
		| ExecutionPlan::InsertSelect { .. } => false,
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
	use crate::ir::{BoundExprProgram, ColumnOrdinal, JoinKind, QueryProfileHint};
	use crate::storage::InMemoryStorageScan;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::time::Duration;
	use tokio::io::duplex as test_duplex;
	use tokio::sync::{Notify, mpsc};
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
		assert!(matches!(
			report.output,
			Some(TaskCompletionOutput::ShuffleProducer { .. })
		));
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
		store.open_live(address).expect("open live");
		let bytes = encode_record_batch(&batch).expect("encode");
		store
			.append_live(PartitionPayload::new(
				address,
				bytes.clone(),
				RowCount::new(2),
				ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes)),
			))
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
			row_limit: None,
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
		}
	}
}

type StreamableBatchFuture<'a> =
	std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, QueryError>> + Send + 'a>>;

fn drive_streamable_plan<'a>(
	plan: &'a ExecutionPlan,
	executor: &'a Executor,
	morsel: Option<(MorselId, u32)>,
	on_batch: &'a mut (dyn FnMut(RecordBatch) -> Result<bool, QueryError> + Send + 'a),
) -> StreamableBatchFuture<'a> {
	Box::pin(async move {
		match plan {
			ExecutionPlan::Scan {
				legacy_table_id,
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
					table_id: *legacy_table_id,
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
					if !on_batch(batch)? {
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
				let mut filter_batch = |batch: RecordBatch| -> Result<bool, QueryError> {
					if *disposition == FilterDisposition::FullyPushed {
						return on_batch(batch);
					}
					for filtered in executor.filter_batches_with_bound_predicate(
						predicate,
						vec![batch],
						"filter predicate",
					)? {
						if filtered.num_rows() > 0 && !on_batch(filtered)? {
							return Ok(false);
						}
					}
					Ok(true)
				};
				drive_streamable_plan(input, executor, morsel, &mut filter_batch).await
			}
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				let mut project_batch = |batch: RecordBatch| -> Result<bool, QueryError> {
					let cols = executor.evaluate_bound_program(exprs, &batch)?;
					on_batch(RecordBatch::try_new(output_schema.clone(), cols)?)
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
					let _ = on_batch(empty)?;
					return Ok(false);
				}
				let mut remaining = *limit;
				let mut take_batch = |batch: RecordBatch| -> Result<bool, QueryError> {
					if remaining == 0 {
						return Ok(false);
					}
					let batch = if batch.num_rows() > remaining {
						batch.slice(0, remaining)
					} else {
						batch
					};
					remaining = remaining.saturating_sub(batch.num_rows());
					if batch.num_rows() > 0 && !on_batch(batch)? {
						return Ok(false);
					}
					Ok(remaining > 0)
				};
				drive_streamable_plan(input, executor, morsel, &mut take_batch).await
			}
			ExecutionPlan::Exchange { .. }
			| ExecutionPlan::Sort { .. }
			| ExecutionPlan::Aggregate { .. }
			| ExecutionPlan::Join { .. }
			| ExecutionPlan::EmptyRelation { .. }
			| ExecutionPlan::SystemTable { .. }
			| ExecutionPlan::CreateTable { .. }
			| ExecutionPlan::CreateLogicalType { .. }
			| ExecutionPlan::CreateTableAsSelect { .. }
			| ExecutionPlan::DropTable { .. }
			| ExecutionPlan::Delete { .. }
			| ExecutionPlan::Update { .. }
			| ExecutionPlan::Insert { .. }
			| ExecutionPlan::InsertSelect { .. } => Err(QueryError::Internal {
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
										Err(storage_error_from_query_error(error)),
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
			legacy_table_id,
			schema,
			..
		} if *legacy_table_id == table_id => Some(schema.clone()),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => scan_schema_for_table_id(input, table_id),
		ExecutionPlan::Join { left, right, .. } => scan_schema_for_table_id(left, table_id)
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
	active_query: Option<ActiveQueryHandle>,
}

impl<Transport> NetworkTaskDispatcher<Transport> {
	pub fn new(memory: Arc<MemoryManager>, transport: Transport) -> Self {
		Self {
			coordinator: ToronCoordinator::new(memory),
			transport,
			active_query: None,
		}
	}

	pub fn with_active_query(mut self, active_query: Option<ActiveQueryHandle>) -> Self {
		self.active_query = active_query;
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
	send_payload(
		stream,
		MessageKind::TaskFailed,
		EnvelopeIdentity::task(identity),
		None,
		&TaskFailedPayload::new_worker_emitted(identity, kind).expect("kind is worker-emittable"),
	)
	.await
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
			requested,
			available,
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
