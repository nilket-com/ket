use crate::schema::{FinalizationMarker, LeaseDescriptor, NodeId};
use serde::{Deserialize, Serialize};

/// The unified Node ID type used across Wendelon.
/// OpenRaft requires the `NodeId` type to implement various standard traits,
/// so we wrap our `bytemuck` struct in a newtype or implement them directly.
impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// The state machine application data (mutations).
/// In HashRaft, the actual data (Arrow batches) never enters the Raft log (ADR 003).
/// The Raft log only contains control-ring metadata and Data Ring finalization markers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonRequest {
    /// A Data Ring quorum has been achieved; advance the E_safe watermark.
    Finalize(FinalizationMarker),
    /// Grant a Per-Range Lease to a Data Node.
    AcquireLease(LeaseDescriptor),
    /// A namespace creation or schema registry update.
    # [serde(with = "serde_bytes")]
    SchemaUpdate(Vec<u8>), 
}

/// The response returned to the client after a Raft command is committed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonResponse {
    /// The command was successfully committed and applied.
    Success,
    /// The lease was denied because another node currently holds a valid lease.
    LeaseDenied,
}

// The Wendelon implementation of the `openraft::RaftTypeConfig` trait.
// This trait wires our custom types into the OpenRaft engine.
openraft::declare_raft_types!(
    /// The configuration type for the Wendelon Control Ring.
    pub WendelonTypeConfig:
        D = WendelonRequest,
        R = WendelonResponse,
        NodeId = u32,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<WendelonTypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// The concrete Wendelon Raft alias.
pub type WendelonRaft = openraft::Raft<WendelonTypeConfig>;
