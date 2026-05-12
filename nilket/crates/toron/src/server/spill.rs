use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use arrow::ipc::reader::StreamReader;
use wendelon::storage::file_ops::AsyncFileOps;
use aws_lc_rs::aead::{AES_256_GCM_SIV, Aad, Nonce, UnboundKey, LessSafeKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpillId(pub u64);

pub struct SpillManager {
    file_ops: Arc<dyn AsyncFileOps>,
    spill_dir: PathBuf,
    key: LessSafeKey,
    next_id: AtomicU64,
    /// Metadata for each spilled batch to allow decryption
    metadata: Mutex<std::collections::HashMap<SpillId, SpillMetadata>>,
}

struct SpillMetadata {
    nonce: [u8; 12],
}

impl SpillManager {
    pub fn new(file_ops: Arc<dyn AsyncFileOps>, spill_dir: PathBuf) -> Result<Self, String> {
        let mut key_bytes = [0u8; 32];
        aws_lc_rs::rand::fill(&mut key_bytes).map_err(|e| e.to_string())?;
        
        let unbound_key = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|e| e.to_string())?;
        let key = LessSafeKey::new(unbound_key);

        Ok(Self {
            file_ops,
            spill_dir,
            key,
            next_id: AtomicU64::new(1),
            metadata: Mutex::new(std::collections::HashMap::new()),
        })
    }

    pub async fn spill_batch(&self, batch: &RecordBatch) -> Result<SpillId, String> {
        let id = SpillId(self.next_id.fetch_add(1, Ordering::SeqCst));
        
        // 1. Serialize to Arrow IPC
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
                .map_err(|e| e.to_string())?;
            writer.write(batch).map_err(|e: arrow::error::ArrowError| e.to_string())?;
            writer.finish().map_err(|e: arrow::error::ArrowError| e.to_string())?;
        }

        // 2. Encrypt
        let mut nonce_bytes = [0u8; 12];
        aws_lc_rs::rand::fill(&mut nonce_bytes).map_err(|e| e.to_string())?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        // AES-GCM-SIV is in-place. we use seal_in_place_append_tag
        let mut in_out = buffer;
        self.key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
            .map_err(|e| format!("Encryption failed: {:?}", e))?;

        // 3. Write to disk
        let path = self.spill_dir.join(format!("spill_{}.bin", id.0));
        self.file_ops.write_all(&path, &in_out).await
            .map_err(|e| format!("Disk write failed: {:?}", e))?;

        // 4. Store metadata
        self.metadata.lock().unwrap().insert(id, SpillMetadata { nonce: nonce_bytes });

        Ok(id)
    }

    pub async fn read_batch(&self, id: SpillId) -> Result<RecordBatch, String> {
        let metadata = self.metadata.lock().unwrap().get(&id)
            .ok_or_else(|| format!("SpillId {} not found", id.0))?
            .nonce;

        // 1. Read from disk
        let path = self.spill_dir.join(format!("spill_{}.bin", id.0));
        let mut ciphertext = self.file_ops.read_all(&path).await
            .map_err(|e| format!("Disk read failed: {:?}", e))?;

        // 2. Decrypt
        let nonce = Nonce::assume_unique_for_key(metadata);
        
        let plaintext = self.key.open_in_place(nonce, Aad::empty(), &mut ciphertext)
            .map_err(|e| format!("Decryption failed: {:?}", e))?;

        // 3. Deserialize Arrow IPC
        let mut reader = StreamReader::try_new(&*plaintext, None)
            .map_err(|e: arrow::error::ArrowError| e.to_string())?;
        
        let batch = reader.next()
            .ok_or_else(|| "No batch found in spill file".to_string())?
            .map_err(|e: arrow::error::ArrowError| e.to_string())?;

        Ok(batch)
    }

    pub async fn cleanup(&self, id: SpillId) -> std::io::Result<()> {
        let path = self.spill_dir.join(format!("spill_{}.bin", id.0));
        self.file_ops.unlink(&path).await?;
        self.metadata.lock().unwrap().remove(&id);
        Ok(())
    }
}
