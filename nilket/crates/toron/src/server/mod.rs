pub mod memory;
pub mod spill;
pub mod transport;

use self::memory::MemoryManager;
use crate::binder::{PlannerConfig, bind};
use crate::catalog::Catalog;
use crate::error::QueryError;
use crate::exec::Executor;
use crate::storage::StorageScan;
use crate::wire::ToronMessage;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct ToronWorker {
	executor: Executor,
	catalog: Arc<dyn Catalog>,
	planner_config: PlannerConfig,
	memory: Arc<MemoryManager>,
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
		catalog: Arc<dyn Catalog>,
		planner_config: PlannerConfig,
	) -> Self {
		Self {
			executor: Executor::new(storage),
			catalog,
			planner_config,
			memory,
		}
	}

	pub async fn execute_morsel_local(
		&self,
		plan: &crate::ir::LogicalPlan,
		morsel: Option<(crate::ir::MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let bound = bind(plan, self.catalog.as_ref(), &self.planner_config).await?;
		Ok(self.executor.execute_bound(&bound, morsel).await?)
	}

	pub async fn handle_connection<S>(&self, mut stream: S) -> Result<(), QueryError>
	where
		S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		loop {
			// 1. Read message length
			let mut len_buf = [0u8; 4];
			if let Err(e) = stream.read_exact(&mut len_buf).await {
				if e.kind() == std::io::ErrorKind::UnexpectedEof {
					break; // Connection closed
				}
				return Err(QueryError::Io(e));
			}
			let len = u32::from_le_bytes(len_buf) as usize;

			let mut msg_buf = self.memory.acquire_at_least(len);
			msg_buf.as_mut_vec().resize(len, 0);
			stream.read_exact(msg_buf.as_mut_vec()).await?;

			let msg = ToronMessage::decode(&msg_buf)?;

			match msg {
				ToronMessage::ExecuteMorsel { plan, morsel } => {
					// 2. Execute
					let bound = bind(&plan, self.catalog.as_ref(), &self.planner_config).await?;
					let results = self.executor.execute_bound(&bound, morsel).await?;

					// 3. Stream Arrow batches back
					for batch in results {
						let mut buffer = self.memory.acquire();
						{
							let mut writer =
								StreamWriter::try_new(buffer.as_mut_vec(), &batch.schema())?;
							writer.write(&batch)?;
							writer.finish()?;
						}

						let response = ToronMessage::ArrowBatch {
							payload: buffer.as_vec().clone(),
						};
						// Note: postcard currently forces a copy of the payload Vec<u8> during serialization.
						// True zero-copy requires a custom RDMA-aware codec.
						self.send_message(&mut stream, response).await?;
					}

					// 4. Signal completion
					self.send_message(&mut stream, ToronMessage::MorselComplete)
						.await?;
				}
				ToronMessage::ShuffleData {
					stage_id: _,
					partition_id: _,
					payload,
				} => {
					if payload.len() > 1024 * 1024 {
						tokio::time::sleep(std::time::Duration::from_millis(10)).await;
					}

					let mut reader = arrow::ipc::reader::StreamReader::try_new(&payload[..], None)?;
					while let Some(batch) = reader.next().transpose()? {
						drop(batch);
					}
				}
				ToronMessage::MorselComplete => break,
				_ => {
					return Err(QueryError::UnexpectedMessage {
						expected: "ExecuteMorsel, ShuffleData, or MorselComplete",
					});
				}
			}
		}

		Ok(())
	}

	async fn send_message<S>(&self, stream: &mut S, msg: ToronMessage) -> Result<(), QueryError>
	where
		S: tokio::io::AsyncWrite + Unpin,
	{
		let payload = msg.encode();
		let len = (payload.len() as u32).to_le_bytes();
		stream.write_all(&len).await?;
		stream.write_all(&payload).await?;
		Ok(())
	}

	pub async fn send_shuffle_data<S>(
		&self,
		mut stream: S,
		stage_id: crate::ir::StageId,
		partition_id: u32,
		batch: &RecordBatch,
	) -> Result<(), QueryError>
	where
		S: tokio::io::AsyncWrite + Unpin,
	{
		let mut buffer = self.memory.acquire();
		{
			let mut writer = StreamWriter::try_new(buffer.as_mut_vec(), &batch.schema())?;
			writer.write(batch)?;
			writer.finish()?;
		}

		let msg = ToronMessage::ShuffleData {
			stage_id,
			partition_id,
			payload: buffer.as_vec().clone(),
		};

		self.send_message(&mut stream, msg).await
	}
}

pub struct ToronCoordinator {
	memory: Arc<MemoryManager>,
}

impl ToronCoordinator {
	pub fn new(memory: Arc<MemoryManager>) -> Self {
		Self { memory }
	}

	pub async fn execute_morsel_on_worker<S>(
		&self,
		mut stream: S,
		plan: crate::ir::LogicalPlan,
		morsel: Option<(crate::ir::MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, QueryError>
	where
		S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
	{
		// 1. Send ExecuteMorsel
		let msg = ToronMessage::ExecuteMorsel { plan, morsel };
		let payload = msg.encode();
		let len = (payload.len() as u32).to_le_bytes();
		stream.write_all(&len).await?;
		stream.write_all(&payload).await?;

		// 2. Read results stream
		let mut results = Vec::new();
		loop {
			let mut len_buf = [0u8; 4];
			stream.read_exact(&mut len_buf).await?;
			let len = u32::from_le_bytes(len_buf) as usize;

			let mut msg_buf = self.memory.acquire_at_least(len);
			msg_buf.as_mut_vec().resize(len, 0);
			stream.read_exact(msg_buf.as_mut_vec()).await?;

			let response = ToronMessage::decode(&msg_buf)?;
			match response {
				ToronMessage::ArrowBatch { payload } => {
					let mut reader = arrow::ipc::reader::StreamReader::try_new(&payload[..], None)?;
					while let Some(batch) = reader.next().transpose()? {
						results.push(batch);
					}
				}
				ToronMessage::MorselComplete => break,
				ToronMessage::Error(e) => return Err(QueryError::Remote { message: e }),
				_ => {
					return Err(QueryError::UnexpectedMessage {
						expected: "ArrowBatch, MorselComplete, or Error",
					});
				}
			}
		}

		Ok(results)
	}
}
