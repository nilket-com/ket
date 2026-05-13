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
	TaskCompletionOutput, TaskInputBinding, TaskOutputBinding, TaskSpec,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::error::{
	PartitionMissingReason, QueryError, RuntimeError, StorageError, TaskFailureKind, WireError,
};
use crate::exec::Executor;
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
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct ToronWorker {
	executor: Executor,
	memory: Arc<MemoryManager>,
	partition_store: Arc<dyn PartitionStore>,
	worker_id: WorkerId,
	endpoint: WorkerEndpoint,
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
			executor: Executor::new(storage),
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
		let shuffle_inputs = shuffle_input_partitions(spec);
		if !shuffle_inputs.is_empty() {
			let input_batches = self.pull_shuffle_inputs(&shuffle_inputs).await?;
			let schema = input_batches
				.first()
				.map(RecordBatch::schema)
				.unwrap_or_else(|| spec.plan.schema());
			let executor = Executor::new(Arc::new(ShuffleInputStorageScan {
				schema,
				batches: input_batches,
			}));
			return Ok(executor.execute_bound(&spec.plan, None).await?);
		}

		Ok(self.executor.execute_bound(&spec.plan, None).await?)
	}

	async fn pull_shuffle_inputs(
		&self,
		partitions: &[ProducerPartitionRef],
	) -> Result<Vec<RecordBatch>, QueryError> {
		let coordinator = ToronCoordinator::new(self.memory.clone());
		let mut batches = Vec::new();

		for partition in partitions {
			let stream = tokio::net::TcpStream::connect(partition.endpoint.as_str()).await?;
			let payload = coordinator
				.pull_partition_from_worker(stream, partition.address, partition.address.attempt_id)
				.await?;
			batches.extend(decode_record_batches(payload.payload.as_slice())?);
		}

		Ok(batches)
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

					match self.execute_task_local(&payload.spec).await {
						Ok(results) => {
							for (idx, batch) in results.iter().enumerate() {
								let stream_seq = StreamSeq::new(
									u64::try_from(idx).expect("record batch index fits u64"),
								);
								let encoded_batch = encode_record_batch(&batch)?;
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
							}

							let output =
								self.publish_task_output(identity, &payload.spec, &results)?;
							send_payload(
								&mut stream,
								MessageKind::TaskComplete,
								EnvelopeIdentity::task(identity),
								None,
								&TaskCompletePayload { identity, output },
							)
							.await?;
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

fn shuffle_input_partitions(spec: &TaskSpec) -> Vec<ProducerPartitionRef> {
	spec.inputs
		.iter()
		.flat_map(|input| match input {
			TaskInputBinding::Scan => Vec::new(),
			TaskInputBinding::ShuffleConsumer {
				producer_partitions,
				..
			} => producer_partitions.clone(),
		})
		.collect()
}

struct ShuffleInputStorageScan {
	schema: SchemaRef,
	batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for ShuffleInputStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		Ok(ScanStream::from_batches(
			self.schema.clone(),
			request.session_id,
			request.query_id,
			self.batches.clone(),
		))
	}
}

pub struct ToronCoordinator {
	memory: Arc<MemoryManager>,
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
			.execute_task_on_worker_with_completion(stream, spec.clone())
			.await
		{
			Ok((batches, output)) => DispatchOutcome::CompletedWithBatches { output, batches },
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

	pub async fn execute_task_on_worker_with_completion<Stream>(
		&self,
		mut stream: Stream,
		spec: TaskSpec,
	) -> Result<(Vec<RecordBatch>, TaskCompletionOutput), QueryError>
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
		let completion = loop {
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
					let mut reader = StreamReader::try_new(payload.payload.as_slice(), None)?;
					while let Some(batch) = reader.next().transpose()? {
						results.push(batch);
					}
				}
				MessageKind::TaskComplete => {
					let payload: TaskCompletePayload = decode_payload(&response.payload)?;
					if payload.identity != identity {
						return Err(QueryError::UnexpectedMessage {
							expected: "TaskComplete for requested task",
						});
					}
					break payload.output;
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

		Ok((results, completion))
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
	send_payload(
		stream,
		MessageKind::TaskFailed,
		EnvelopeIdentity::task(identity),
		None,
		&TaskFailedPayload::new_worker_emitted(
			identity,
			TaskFailureKind::Internal {
				reason: error.to_string(),
			},
		)
		.expect("Internal is worker-emittable"),
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
		QueryError::Runtime(RuntimeError::ProtocolMismatch { .. }) => {
			TaskFailureKind::ProtocolMismatch
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
