//! Server-side decode table for internal Raft RPCs (ADR 003).
//!
//! The mirror image of [`crate::consensus::raft_network`]: where the
//! client side serialises a `*_REQ` into an HRFT frame and awaits a
//! `*_RESP`, this file decodes that request on the accepting node,
//! drives the local [`openraft::Raft`] handle, and encodes the reply.
//!
//! Plugged into [`crate::network::router::MsgRouter`] via
//! `MsgRouterBuilder::with_raft_internal`. The router already scopes
//! `msg_type` to `0x20..=0x2F` before dispatching here, so we only
//! handle the three request variants plus a catch-all.
//!
//! ## Response envelope
//!
//! The wire body is `Result<Resp, RaftError<NodeId, …>>`. Logical
//! peer errors (e.g., term mismatches, snapshot progress faults)
//! round-trip faithfully to the client as `RPCError::RemoteError`
//! rather than being flattened into a transport-level
//! `Unreachable`. Only serialisation / framing failures close the
//! stream; everything else is a successful response carrying an
//! `Err` inside.

use crate::consensus::monitor::NodeTracker;
use crate::consensus::{WendelonRaft, WendelonRequest, WendelonTypeConfig};
use crate::network::core::{HrftFrame, HRFT_MAGIC};
use crate::network::pool::BufPool;
use crate::network::router::RaftInternalServer;
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId};
use openraft::EntryPayload;
use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use std::io::{Error, ErrorKind};
use std::sync::Arc;

/// Server-side adapter that turns Raft internal HRFT frames into
/// calls on a local [`WendelonRaft`] handle.
///
/// The `pool` is shared with the rest of the HRFT endpoint: every
/// response body is postcard-encoded into a `PooledBuf` drawn from
/// it and returned once the outer `handle_stream` finishes writing
/// the frame.
pub struct WendelonRaftInternalServer {
    raft: WendelonRaft,
    pool: Arc<BufPool>,
    /// Optional liveness/drift tracker. When attached, every
    /// inbound APPEND_ENTRIES_REQ feeds the leader's HLC (pulled
    /// from any `Finalize` entries it carries) into the tracker —
    /// so the drift monitor and eviction watchdog pick up
    /// Raft-layer signal in addition to the data-plane.
    tracker: Option<Arc<NodeTracker>>,
}

impl WendelonRaftInternalServer {
    pub fn new(raft: WendelonRaft, pool: Arc<BufPool>) -> Self {
        Self {
            raft,
            pool,
            tracker: None,
        }
    }

    /// Builder-style tracker attachment. See [`Self::tracker`] on
    /// the equivalent method in `WendelonDataServer` — same story:
    /// production bootstrap wires it, tests skip it.
    pub fn with_tracker(mut self, tracker: Arc<NodeTracker>) -> Self {
        self.tracker = Some(tracker);
        self
    }
}

#[async_trait]
impl RaftInternalServer for WendelonRaftInternalServer {
    async fn handle_raft_frame(&self, frame: HrftFrame) -> std::io::Result<Option<HrftFrame>> {
        let msg_type = MsgType(frame.header.msg_type);
        let (resp_type, body) = match msg_type {
            MsgType::APPEND_ENTRIES_REQ => {
                let req: AppendEntriesRequest<WendelonTypeConfig> =
                    wire::decode(&frame.meta)?;
                // Harvest peer HLCs from any `Finalize` entries
                // before the request moves into openraft. Every
                // FinalizationMarker carries the leader's commit
                // HLC — exactly what the drift monitor wants.
                if let Some(tracker) = &self.tracker {
                    for entry in &req.entries {
                        if let EntryPayload::Normal(
                            WendelonRequest::Finalize(marker),
                        ) = &entry.payload
                        {
                            let peer = NodeId {
                                id: marker.commit_hlc.node_id,
                                _reserved: 0,
                            };
                            tracker.record_hlc(peer, marker.commit_hlc);
                        }
                    }
                }
                let envelope: Result<AppendEntriesResponse<u32>, RaftError<u32>> =
                    self.raft.append_entries(req).await;
                (
                    MsgType::APPEND_ENTRIES_RESP,
                    wire::encode_into(&self.pool, &envelope)?,
                )
            }
            MsgType::VOTE_REQ => {
                let req: VoteRequest<u32> = wire::decode(&frame.meta)?;
                let envelope: Result<VoteResponse<u32>, RaftError<u32>> =
                    self.raft.vote(req).await;
                (
                    MsgType::VOTE_RESP,
                    wire::encode_into(&self.pool, &envelope)?,
                )
            }
            MsgType::INSTALL_SNAPSHOT_REQ => {
                let req: InstallSnapshotRequest<WendelonTypeConfig> =
                    wire::decode(&frame.meta)?;
                let envelope: Result<
                    InstallSnapshotResponse<u32>,
                    RaftError<u32, InstallSnapshotError>,
                > = self.raft.install_snapshot(req).await;
                (
                    MsgType::INSTALL_SNAPSHOT_RESP,
                    wire::encode_into(&self.pool, &envelope)?,
                )
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "unexpected Raft internal msg_type=0x{:02x}",
                        msg_type.0
                    ),
                ));
            }
        };

        let header = FrameHeader {
            magic: HRFT_MAGIC,
            version: 1,
            msg_type: resp_type.0,
            flags: 0,
            stream_id: frame.header.stream_id,
            meta_len: body.len() as u32,
            arrow_len: 0,
            trailing_len: 0,
        };

        Ok(Some(HrftFrame {
            header,
            meta: body,
            arrow_payload: None,
            trailing: None,
        }))
    }
}
