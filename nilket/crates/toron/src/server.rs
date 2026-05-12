use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::exec::Executor;
use crate::storage::StorageScan;
use crate::wire::ToronMessage;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

pub struct ToronWorker {
    executor: Executor,
}

impl ToronWorker {
    pub fn new(storage: Arc<dyn StorageScan>) -> Self {
        Self {
            executor: Executor::new(storage),
        }
    }

    pub async fn handle_connection<S>(&self, mut stream: S) -> Result<(), String>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // 1. Read ExecuteMorsel request
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| e.to_string())?;
        let len = u32::from_le_bytes(len_buf) as usize;
        
        let mut msg_buf = vec![0u8; len];
        stream.read_exact(&mut msg_buf).await.map_err(|e| e.to_string())?;
        
        let msg = ToronMessage::decode(&msg_buf)?;
        
        if let ToronMessage::ExecuteMorsel { plan, morsel } = msg {
            // 2. Execute
            let results = self.executor.execute(&plan, morsel).await
                .map_err(|e| format!("Execution error: {:?}", e))?;
                
            // 3. Stream Arrow batches back
            for batch in results {
                let mut buffer = Vec::new();
                {
                    let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
                        .map_err(|e| e.to_string())?;
                    writer.write(&batch).map_err(|e: arrow::error::ArrowError| e.to_string())?;
                    writer.finish().map_err(|e: arrow::error::ArrowError| e.to_string())?;
                }
                
                let response = ToronMessage::ArrowBatch { payload: buffer };
                self.send_message(&mut stream, response).await?;
            }
            
            // 4. Signal completion
            self.send_message(&mut stream, ToronMessage::MorselComplete).await?;
        } else {
            return Err("Expected ExecuteMorsel message".to_string());
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
}

pub struct ToronCoordinator {
    // In M5, simple coordinator that dials workers
}

impl ToronCoordinator {
    pub fn new() -> Self {
        Self {}
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
            stream.read_exact(&mut len_buf).await.map_err(|e| e.to_string())?;
            let len = u32::from_le_bytes(len_buf) as usize;
            
            let mut msg_buf = vec![0u8; len];
            stream.read_exact(&mut msg_buf).await.map_err(|e| e.to_string())?;
            
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
