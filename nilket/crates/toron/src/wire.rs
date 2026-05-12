use serde::{Deserialize, Serialize};
use crate::ir::{LogicalPlan, MorselId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToronMessage {
    /// Dispatched from Coordinator to Worker to execute a specific morsel.
    ExecuteMorsel {
        plan: LogicalPlan,
        morsel: Option<(MorselId, u32)>,
    },
    /// Streamed from Worker to Coordinator: an Arrow batch result.
    ArrowBatch {
        // Arrow IPC bytes
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    /// P2P worker exchange: sends a partitioned batch to another worker.
    ShuffleData {
        stage_id: crate::ir::StageId,
        partition_id: u32,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    /// Sent from Worker to Coordinator: the morsel execution is complete.
    MorselComplete,
    /// Error notification.
    Error(String),
}

impl ToronMessage {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("failed to encode ToronMessage")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        postcard::from_bytes(bytes).map_err(|e| e.to_string())
    }
}
