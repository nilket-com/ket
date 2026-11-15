//! ADR 0103 (wendelon), DC-243 step 1: who may offer a Raft request to a node.
//!
//! Each request goes through the node's real published service (router ingress, then the Raft
//! handler) over the in-process simulated transport. The caller identity is the RequestContext
//! the simulated transport hands the service (`SimulatedTransportProvider::with_caller`), NOT a
//! TLS exchange: this tests the admission logic after transport verification, not the TLS
//! layer. Every request carries term 0, which the receiver refuses on the term alone, so no
//! admitted request changes Raft state. The oracle is only whether the request reached the
//! Raft handler (a Raft response frame came back) or was refused before dispatch (the stream
//! ends with no response). The pre-fix RED of the first two cases is recorded in
//! reviews/member-replacement-20261115/DC243-RED-AND-FIX-DESIGN-12.md.

use super::*;
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::tls::{ActorIdentity, ActorRole};
use crate::network::transport::TransportProvider;
use crate::schema::{FrameHeader, MsgType};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

const TIER: TransportTier = TransportTier::QuicIoUring;

/// The three Raft request kinds, each naming `sender` as its candidate or leader, at term 0.
fn requests(sender: u32) -> Vec<(MsgType, Vec<u8>)> {
	let vote = openraft::Vote::new(0, sender);
	let append: AppendEntriesRequest<crate::consensus::WendelonTypeConfig> = AppendEntriesRequest {
		vote,
		prev_log_id: None,
		entries: Vec::new(),
		leader_commit: None,
	};
	let snapshot: InstallSnapshotRequest<crate::consensus::WendelonTypeConfig> =
		InstallSnapshotRequest {
			vote,
			meta: openraft::SnapshotMeta {
				last_log_id: None,
				last_membership: openraft::StoredMembership::default(),
				snapshot_id: "dc243".to_owned(),
			},
			offset: 0,
			data: Vec::new(),
			done: false,
		};
	vec![
		(
			MsgType::VOTE_REQ,
			crate::network::wire::encode(&VoteRequest::new(vote, None)).unwrap(),
		),
		(
			MsgType::APPEND_ENTRIES_REQ,
			crate::network::wire::encode(&append).unwrap(),
		),
		(
			MsgType::INSTALL_SNAPSHOT_REQ,
			crate::network::wire::encode(&snapshot).unwrap(),
		),
	]
}

/// One Raft request from `local` presenting `caller`, to `target`: the response's msg_type,
/// or None when the stream ended without one (refused before dispatch).
async fn offer(
	cluster: &Cluster,
	local: u32,
	caller: Option<ActorIdentity>,
	target: u32,
	msg_type: MsgType,
	meta: &[u8],
) -> Option<u8> {
	let mut provider = SimulatedTransportProvider::new(
		crate::schema::NodeId {
			id: local,
			_reserved: 0,
		},
		cluster.network.clone(),
		crate::network::pool::BufPool::new(64, 1024 * 1024),
		TIER,
	);
	if let Some(caller) = caller {
		provider = provider.with_caller(caller);
	}
	let pool = crate::network::pool::BufPool::new(64, 1024 * 1024);
	let connection = provider
		.connect(
			crate::schema::NodeId {
				id: target,
				_reserved: 0,
			},
			&address(target),
		)
		.await
		.unwrap();
	let mut stream = connection.open_bi().await.unwrap();
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: crate::consensus::decision_mode::DecisionMode::ParentDecisions
			.raft_frame_version(),
		msg_type: msg_type.0,
		flags: 0,
		stream_id: 1,
		meta_len: meta.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, meta, None, None)
		.await
		.unwrap();
	stream.finish().await.unwrap();
	match tokio::time::timeout(
		std::time::Duration::from_secs(10),
		read_frame(&mut *stream, &pool),
	)
	.await
	{
		Ok(Ok(frame)) => Some(frame.header.msg_type),
		Ok(Err(_)) => None,
		Err(_) => panic!("no answer and no close within 10 s"),
	}
}

fn storage_actor(node: u32) -> ActorIdentity {
	ActorIdentity::new(
		ActorRole::DataNode,
		Some(crate::schema::NodeId {
			id: node,
			_reserved: 0,
		}),
		Some("1".to_owned()),
		vec!["cluster".to_owned()],
		b"dc243-storage".to_vec(),
	)
	.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn only_a_cluster_node_naming_itself_reaches_the_raft_handler() {
	let (cluster, leader, _descriptor) = ready().await;
	let target = *NODES.iter().find(|id| **id != leader).unwrap();
	let other = *NODES.iter().find(|id| **id != target).unwrap();
	let liar = *NODES
		.iter()
		.find(|id| **id != target && **id != other)
		.unwrap();
	let mut admitted = Vec::new();
	for (msg_type, meta) in requests(other) {
		// A tenant-scoped storage identity naming a member as the sender.
		let answer = offer(
			&cluster,
			99,
			Some(storage_actor(99)),
			target,
			msg_type,
			&meta,
		)
		.await;
		eprintln!("DC243 storage-99-as-{other} {msg_type:?} -> {answer:?}");
		if answer.is_some() {
			admitted.push(format!("tenant storage 99 naming {other}: {msg_type:?}"));
		}
		// A tenant-scoped identity whose authenticated id IS the claimed sender: only the
		// tenant rule refuses it.
		let answer = offer(
			&cluster,
			other,
			Some(storage_actor(other)),
			target,
			msg_type,
			&meta,
		)
		.await;
		eprintln!("DC243 storage-{other}-as-itself {msg_type:?} -> {answer:?}");
		if answer.is_some() {
			admitted.push(format!("tenant-scoped {other} naming itself: {msg_type:?}"));
		}
		// A genuine cluster node naming ANOTHER node as the sender.
		let answer = offer(&cluster, liar, None, target, msg_type, &meta).await;
		eprintln!("DC243 node-{liar}-as-{other} {msg_type:?} -> {answer:?}");
		if answer.is_some() {
			admitted.push(format!("node {liar} naming node {other}: {msg_type:?}"));
		}
	}
	// Control: a cluster node naming itself reaches the handler (a protocol refusal of
	// term 0 is an answer, not an authorization failure).
	for (msg_type, meta) in requests(other) {
		let answer = offer(&cluster, other, None, target, msg_type, &meta).await;
		eprintln!("DC243 node-{other}-as-itself {msg_type:?} -> {answer:?}");
		assert!(
			answer.is_some(),
			"control: node {other} naming itself got no Raft answer for {msg_type:?}"
		);
	}
	assert!(
		admitted.is_empty(),
		"admitted before dispatch: {admitted:#?}"
	);
}

/// A refused AppendEntries has no effect before its refusal: the receiver's drift tracker
/// records nothing from a Finalize entry it carries. The same request from the node it names
/// is harvested, which shows the observation would have seen it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_refused_append_entries_leaves_no_hlc_trace() {
	let (cluster, leader, _descriptor) = ready().await;
	let target = *NODES.iter().find(|id| **id != leader).unwrap();
	let other = *NODES.iter().find(|id| **id != target).unwrap();
	let liar = *NODES
		.iter()
		.find(|id| **id != target && **id != other)
		.unwrap();
	// A peer id no node uses, so only this entry could record it.
	const MARKED: u32 = 77;
	let mut marker: crate::schema::FinalizationMarker = bytemuck::Zeroable::zeroed();
	marker.commit_hlc = HybridTimestamp {
		physical_ms: 4242,
		logical: 0,
		node_id: MARKED,
	};
	let entry = openraft::Entry::<crate::consensus::WendelonTypeConfig> {
		log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(0, other), 1),
		payload: openraft::EntryPayload::Normal(crate::consensus::WendelonRequest::Finalize(
			crate::consensus::TenantFinalization {
				marker,
				tenant_id: TenantId::ZERO,
				key_identity: bytemuck::Zeroable::zeroed(),
				charged_bytes: 0,
				record_count: 1,
			},
		)),
	};
	let append: AppendEntriesRequest<crate::consensus::WendelonTypeConfig> = AppendEntriesRequest {
		vote: openraft::Vote::new(0, other),
		prev_log_id: None,
		entries: vec![entry],
		leader_commit: None,
	};
	let meta = crate::network::wire::encode(&append).unwrap();
	let marked = crate::schema::NodeId {
		id: MARKED,
		_reserved: 0,
	};
	let tracker = cluster.node(target).tracker.clone();
	assert!(tracker.last_seen(marked).is_none());
	let refused = offer(
		&cluster,
		liar,
		None,
		target,
		MsgType::APPEND_ENTRIES_REQ,
		&meta,
	)
	.await;
	assert_eq!(refused, None, "node {liar} naming {other} is refused");
	assert!(
		tracker.last_seen(marked).is_none(),
		"the refused request was harvested"
	);
	let admitted = offer(
		&cluster,
		other,
		None,
		target,
		MsgType::APPEND_ENTRIES_REQ,
		&meta,
	)
	.await;
	assert!(admitted.is_some(), "node {other} naming itself is answered");
	assert_eq!(
		tracker
			.last_seen(marked)
			.map(|seen| seen.remote_physical_ms),
		Some(4242),
		"the admitted request's entry is harvested"
	);
}
