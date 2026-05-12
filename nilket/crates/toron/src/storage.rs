use crate::ir::TableId;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TenantId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HlcBound(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryProfileHint {
    PointRead,
    Scan,
}

pub struct ScanRequest {
    pub tenant_id: TenantId,
    pub table_id: TableId,
    pub read_bound: HlcBound,
    pub projected_columns: Option<Vec<String>>,
    pub pbc_predicate: Option<Vec<u8>>, // Mocked PBC program payload
    pub profile_hint: Option<QueryProfileHint>,
}

pub struct ScanStream {
    pub batches: Vec<RecordBatch>, // Mocked as full buffered Vec for M1
}

#[derive(Debug)]
pub enum StorageError {
    TableNotFound,
    Internal(String),
}

pub trait StorageScan {
    fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError>;
}

// In-memory mock for M1 execution
pub struct InMemoryStorageScan {
    pub table_data: RecordBatch,
}

impl StorageScan for InMemoryStorageScan {
    fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
        // M1 simplified scan logic: return the full table, 
        // rely on Executor to do filtering/projection if PBC is not pushed down.
        // We will pass the full batch back as a stream.
        let mut batch = self.table_data.clone();
        
        // Naive projection pushdown simulation for M1:
        if let Some(cols) = request.projected_columns {
            let schema = batch.schema();
            let mut indices = vec![];
            for col in cols {
                if let Ok(idx) = schema.index_of(&col) {
                    indices.push(idx);
                }
            }
            batch = batch.project(&indices).map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(ScanStream { batches: vec![batch] })
    }
}
