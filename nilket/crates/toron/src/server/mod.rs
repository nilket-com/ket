pub mod memory;
pub mod transport;
pub mod spill;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::exec::Executor;
use crate::storage::StorageScan;
use crate::wire::ToronMessage;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use self::memory::MemoryManager;

pub struct ToronWorker {
    executor: Executor,
    memory: Arc<MemoryManager>,
}

impl ToronWorker {
    pub fn new(storage: Arc<dyn StorageScan>, memory: Arc<MemoryManager>) -> Self {
        Self {
            executor: Executor::new(storage),
            memory,
        }
    }

    pub async fn execute_morsel_local(
        &self, 
        plan: &crate::ir::LogicalPlan, 
        morsel: Option<(crate::ir::MorselId, u32)>
    ) -> Result<Vec<RecordBatch>, String> {
        self.executor.execute(plan, morsel).await
            .map_err(|e| format!("Execution error: {:?}", e))
    }

    pub async fn handle_connection<S>(&self, mut stream: S) -> Result<(), String>
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
                return Err(e.to_string());
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            
            let mut msg_buf = self.memory.acquire_at_least(len);
            msg_buf.as_mut_vec().resize(len, 0);
            stream.read_exact(msg_buf.as_mut_vec()).await.map_err(|e| e.to_string())?;
            
            let msg = ToronMessage::decode(&msg_buf)?;
            
            match msg {
                ToronMessage::ExecuteMorsel { plan, morsel } => {
                    // 2. Execute
                    let results = self.executor.execute(&plan, morsel).await
                        .map_err(|e| format!("Execution error: {:?}", e))?;
                        
                    // 3. Stream Arrow batches back
                    for batch in results {
                        let mut buffer = self.memory.acquire();
                        {
                            let mut writer = StreamWriter::try_new(buffer.as_mut_vec(), &batch.schema())
                                .map_err(|e| e.to_string())?;
                            writer.write(&batch).map_err(|e: arrow::error::ArrowError| e.to_string())?;
                            writer.finish().map_err(|e: arrow::error::ArrowError| e.to_string())?;
                        }
                        
                        let response = ToronMessage::ArrowBatch { payload: buffer.as_vec().clone() };
                        // Note: postcard currently forces a copy of the payload Vec<u8> during serialization.
                        // True zero-copy requires a custom RDMA-aware codec.
                        self.send_message(&mut stream, response).await?;
                    }
                    
                    // 4. Signal completion
                    self.send_message(&mut stream, ToronMessage::MorselComplete).await?;
                }
                ToronMessage::ShuffleData { stage_id: _, partition_id: _, payload } => {
                    // M6: Receive shuffle data from peer worker
                    // Mock receive side backpressure: if payload is too big, simulate a delay
                    if payload.len() > 1024 * 1024 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    
                    // Reconstruct batch (just verifying it works for M6)
                    let mut reader = arrow::ipc::reader::StreamReader::try_new(&payload[..], None)
                        .map_err(|e| e.to_string())?;
                    while let Some(batch) = reader.next().transpose().map_err(|e| e.to_string())? {
                        // In a real impl, we'd buffer this for the local reducer stage
                        drop(batch);
                    }
                }
                ToronMessage::MorselComplete => break,
                _ => return Err("Unexpected message".to_string()),
            }
        }
        
        Ok(())
    }

    async fn send_message<S>(&self, stream: &mut S, msg: ToronMessage) -> Result<(), String>
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        let payload = msg.encode();
        let len = (payload.len() as u32).to_le_bytes();
        stream.write_all(&len).await.map_err(|e| e.to_string())?;
        stream.write_all(&payload).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn send_shuffle_data<S>(
        &self, 
        mut stream: S, 
        stage_id: crate::ir::StageId, 
        partition_id: u32, 
        batch: &RecordBatch
    ) -> Result<(), String>
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        let mut buffer = self.memory.acquire();
        {
            let mut writer = StreamWriter::try_new(buffer.as_mut_vec(), &batch.schema())
                .map_err(|e| e.to_string())?;
            writer.write(batch).map_err(|e: arrow::error::ArrowError| e.to_string())?;
            writer.finish().map_err(|e: arrow::error::ArrowError| e.to_string())?;
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
        morsel: Option<(crate::ir::MorselId, u32)>
    ) -> Result<Vec<RecordBatch>, String>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // 1. Send ExecuteMorsel
        let msg = ToronMessage::ExecuteMorsel { plan, morsel };
        let payload = msg.encode();
        let len = (payload.len() as u32).to_le_bytes();
        stream.write_all(&len).await.map_err(|e| e.to_string())?;
        stream.write_all(&payload).await.map_err(|e| e.to_string())?;
        
        // 2. Read results stream
        let mut results = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = stream.read_exact(&mut len_buf).await {
                return Err(e.to_string());
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            
            let mut msg_buf = self.memory.acquire_at_least(len);
            msg_buf.as_mut_vec().resize(len, 0);
            stream.read_exact(msg_buf.as_mut_vec()).await.map_err(|e| e.to_string())?;
            
            let response = ToronMessage::decode(&msg_buf)?;
            match response {
                ToronMessage::ArrowBatch { payload } => {
                    let mut reader = arrow::ipc::reader::StreamReader::try_new(&payload[..], None)
                        .map_err(|e| e.to_string())?;
                    while let Some(batch) = reader.next().transpose().map_err(|e| e.to_string())? {
                        results.push(batch);
                    }
                }
                ToronMessage::MorselComplete => break,
                ToronMessage::Error(e) => return Err(e),
                _ => return Err("Unexpected message from worker".to_string()),
            }
        }
        
        Ok(results)
    }
}
