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
use crate::consensus::retention::{RetentionGuard, RetentionViolation};
use crate::consensus::{WendelonRaft, WendelonRequest, WendelonTypeConfig};
use crate::network::core::{HRFT_MAGIC, HrftFrame};
use crate::network::pool::BufPool;
use crate::network::router::RaftInternalServer;
use crate::network::server::RequestContext;
use crate::network::tls::ActorRole;
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId};
use async_trait::async_trait;
use openraft::EntryPayload;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
	AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
	VoteRequest, VoteResponse,
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
	/// Raft-layer signal in addition to the data-ring.
	tracker: Option<Arc<NodeTracker>>,
	replacement_replay: Option<(
		Arc<crate::storage::manager::SlabManager>,
		crate::consensus::raft_storage::WendelonStateMachine,
		crate::consensus::raft_storage::WendelonLogStore,
	)>,
	/// The node's Control retention guard. Under full retention an inbound
	/// snapshot is refused here, before decode and before openraft sees it.
	retention: Arc<RetentionGuard>,
	/// A frame from a peer on another decision authority is refused before decode.
	decision_mode: crate::consensus::decision_mode::DecisionMode,
}

impl WendelonRaftInternalServer {
	pub fn new(raft: WendelonRaft, pool: Arc<BufPool>, retention: Arc<RetentionGuard>) -> Self {
		Self {
			raft,
			pool,
			tracker: None,
			replacement_replay: None,
			retention,
			decision_mode: Default::default(),
		}
	}

	pub(crate) fn with_decision_mode(
		mut self,
		decision_mode: crate::consensus::decision_mode::DecisionMode,
	) -> Self {
		self.decision_mode = decision_mode;
		self
	}

	/// Builder-style tracker attachment. See [`Self::tracker`] on
	/// the equivalent method in `WendelonDataServer` — same story:
	/// production bootstrap wires it, tests skip it.
	pub(crate) fn with_replacement_replay(
		mut self,
		slab: Arc<crate::storage::manager::SlabManager>,
		state: crate::consensus::raft_storage::WendelonStateMachine,
		log: crate::consensus::raft_storage::WendelonLogStore,
	) -> Self {
		self.replacement_replay = Some((slab, state, log));
		self
	}

	async fn confirm_replacement_replay(&self, confirmed: Option<u64>, leader: u32) {
		let Some((slab, state, log)) = &self.replacement_replay else {
			return;
		};
		let Some(confirmed) = confirmed else {
			return;
		};
		if !slab.is_awaiting_cluster() || self.raft.current_leader().await != Some(leader) {
			return;
		}
		if !log.replacement_replay_base_verified().await {
			return;
		}
		let state = state.state_snapshot().await;
		let local = self.raft.metrics().borrow().id;
		// This accepted leader confirmation must follow the replacement's join.
		if !state.last_membership.nodes().any(|(id, _)| *id == local) {
			return;
		}
		let Some(applied) = state.last_applied_log_id.map(|id| id.index) else {
			return;
		};
		if applied < confirmed {
			return;
		}
		if state
			.last_membership
			.log_id()
			.as_ref()
			.is_none_or(|joined| confirmed < joined.index)
		{
			return;
		}
		// ADR 0697: compare only this store's own committed pointers. A view
		// that is not authoritative for this store leaves replay fenced.
		let Some(pointers) = slab.own_tier_view(state.tiered_slabs.values(), &state.incarnations)
		else {
			tracing::warn!(
				applied,
				confirmed,
				"replacement replay comparison remains fenced: control view is not authoritative for this store"
			);
			return;
		};
		if let Err(error) = slab
			.finish_replacement_log_replay(pointers, applied, confirmed)
			.await
		{
			tracing::warn!(%error, applied, confirmed, "replacement replay comparison remains fenced");
		}
	}

	pub fn with_tracker(mut self, tracker: Arc<NodeTracker>) -> Self {
		self.tracker = Some(tracker);
		self
	}
}

/// ADR 0103 (wendelon): a Raft request is admitted only from an unscoped cluster node, and
/// only when the request names that node as its candidate or leader (`claimed`). Called
/// after decoding and before any effect of the request.
fn admit_raft_sender(
	context: &RequestContext,
	kind: &'static str,
	claimed: u32,
) -> std::io::Result<()> {
	let actor = &context.actor;
	let authenticated = match (actor.role, &actor.tenant_id, actor.node_id, context.remote) {
		(ActorRole::DataNode, None, Some(node), Some(remote)) if node == remote => node.id,
		_ => {
			tracing::warn!(
				kind,
				claimed,
				role = %actor.role,
				tenant_scoped = actor.tenant_id.is_some(),
				"Raft request refused: the caller is not an unscoped cluster node"
			);
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!("{kind} refused: the caller is not an unscoped cluster node"),
			));
		}
	};
	if claimed != authenticated {
		tracing::warn!(
			kind,
			claimed,
			authenticated,
			"Raft request refused: it names another node as its sender"
		);
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			format!("{kind} from node {authenticated} names node {claimed} as its sender"),
		));
	}
	Ok(())
}

#[async_trait]
impl RaftInternalServer for WendelonRaftInternalServer {
	async fn handle_raft_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.decision_mode.check_raft_frame(frame.header.version)?;
		let msg_type = MsgType(frame.header.msg_type);
		if msg_type == MsgType::INSTALL_SNAPSHOT_REQ
			&& self.retention.refuses(RetentionViolation::SnapshotRequest)
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				RetentionViolation::SnapshotRequest,
			));
		}
		let (resp_type, body) = match msg_type {
			MsgType::APPEND_ENTRIES_REQ => {
				let req: AppendEntriesRequest<WendelonTypeConfig> = wire::decode(&frame.meta)?;
				admit_raft_sender(&context, "AppendEntries", req.vote.leader_id.node_id)?;
				// Harvest peer HLCs from any `Finalize` entries
				// before the request moves into openraft. Every
				// FinalizationMarker carries the leader's commit
				// HLC — exactly what the drift monitor wants.
				if let Some(tracker) = &self.tracker {
					for entry in &req.entries {
						if let EntryPayload::Normal(WendelonRequest::Finalize(finalization)) =
							&entry.payload
						{
							let peer = NodeId {
								id: finalization.marker.commit_hlc.node_id,
								_reserved: 0,
							};
							tracker.record_hlc(peer, finalization.marker.commit_hlc);
						}
					}
				}
				let confirmed = req.leader_commit.map(|id| id.index);
				let leader = req.vote.leader_id.node_id;
				let envelope: Result<AppendEntriesResponse<u32>, RaftError<u32>> =
					self.raft.append_entries(req).await;
				if matches!(&envelope, Ok(AppendEntriesResponse::Success)) {
					self.confirm_replacement_replay(confirmed, leader).await;
				}
				(
					MsgType::APPEND_ENTRIES_RESP,
					wire::encode_into(&self.pool, &envelope)?,
				)
			}
			MsgType::VOTE_REQ => {
				let req: VoteRequest<u32> = wire::decode(&frame.meta)?;
				admit_raft_sender(&context, "Vote", req.vote.leader_id.node_id)?;
				let envelope: Result<VoteResponse<u32>, RaftError<u32>> = self.raft.vote(req).await;
				(
					MsgType::VOTE_RESP,
					wire::encode_into(&self.pool, &envelope)?,
				)
			}
			MsgType::INSTALL_SNAPSHOT_REQ => {
				let req: InstallSnapshotRequest<WendelonTypeConfig> = wire::decode(&frame.meta)?;
				admit_raft_sender(&context, "InstallSnapshot", req.vote.leader_id.node_id)?;
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
					format!("unexpected Raft internal msg_type=0x{:02x}", msg_type.0),
				));
			}
		};

		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: self.decision_mode.raft_frame_version(),
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

#[cfg(test)]
mod admission_tests {
	use super::*;
	use crate::network::tls::ActorIdentity;

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	/// The handler's own check, without the router: the context's remote, the actor's
	/// node, its tenant scope and the claimed sender must all agree.
	#[test]
	fn the_handler_admits_only_an_unscoped_node_naming_itself() {
		let context = |remote: Option<u32>, actor: ActorIdentity| RequestContext {
			remote: remote.map(node),
			actor,
		};
		assert!(
			admit_raft_sender(
				&context(Some(2), ActorIdentity::data_node(node(2))),
				"Vote",
				2
			)
			.is_ok()
		);
		// The claimed sender is another node.
		assert!(
			admit_raft_sender(
				&context(Some(2), ActorIdentity::data_node(node(2))),
				"Vote",
				3
			)
			.is_err()
		);
		// The actor and the remote disagree (the router normally refuses this first).
		assert!(
			admit_raft_sender(
				&context(Some(2), ActorIdentity::data_node(node(3))),
				"Vote",
				3
			)
			.is_err()
		);
		assert!(
			admit_raft_sender(&context(None, ActorIdentity::data_node(node(2))), "Vote", 2)
				.is_err()
		);
		// A tenant-scoped data node naming itself.
		let scoped = ActorIdentity::new(
			ActorRole::DataNode,
			Some(node(2)),
			Some("1".to_owned()),
			vec!["cluster".to_owned()],
			b"scoped".to_vec(),
		)
		.unwrap();
		assert!(admit_raft_sender(&context(Some(2), scoped), "AppendEntries", 2).is_err());
		// Not a data node at all.
		let operator = ActorIdentity::new(
			ActorRole::Operator,
			None,
			None,
			Vec::new(),
			b"operator".to_vec(),
		)
		.unwrap();
		assert!(admit_raft_sender(&context(Some(2), operator), "InstallSnapshot", 2).is_err());
	}
}
