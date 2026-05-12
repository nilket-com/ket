use crate::ir::TableId;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

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
    pub batches: Vec<RecordBatch>, // Mocked as full buffered Vec for M1/M3
}

#[derive(Debug)]
pub enum StorageError {
    TableNotFound,
    Internal(String),
}

#[async_trait::async_trait]
pub trait StorageScan: Send + Sync {
    async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError>;
}

// In-memory mock for M1 execution
pub struct InMemoryStorageScan {
    pub table_data: RecordBatch,
}

#[async_trait::async_trait]
impl StorageScan for InMemoryStorageScan {
    async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
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

// Wendelon Client Adapter for M3
pub struct WendelonStorageScan {
    client: Arc<wendelon_client::Client>,
    descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptorV1>,
}

impl WendelonStorageScan {
    pub fn new(
        client: Arc<wendelon_client::Client>, 
        descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptorV1>
    ) -> Self {
        Self { client, descriptors }
    }
}

#[async_trait::async_trait]
impl StorageScan for WendelonStorageScan {
    async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
        let descriptor = self.descriptors.get(&request.table_id)
            .ok_or(StorageError::TableNotFound)?;
        
        // Use an empty PBC program (no filtering/projection) if none was pushed down
        let pbc = request.pbc_predicate.unwrap_or_default();
        
        let mut options = wendelon_client::TableQueryOptions::default();
        if request.read_bound.0 > 0 {
            options.as_of_hlc = Some(wendelon::schema::HybridTimestamp {
                physical_ms: request.read_bound.0,
                logical: 0,
                node_id: 0,
            });
        }

        let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);

        let query_result = self.client.table_query(tenant_id, descriptor, pbc, options).await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(ScanStream { batches: query_result.batches })
    }
}
