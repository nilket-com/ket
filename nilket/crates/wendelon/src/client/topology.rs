//! Topology snapshot sourcing for the Smart Client SDK.
//!
//! The client keeps a local view of the Unified Hash Ring (ADR 002)
//! so it can dial the right replicas for a given `key_hash` without
//! bouncing every request through a proxy node. This module owns
//! the two halves of that picture:
//!
//! 1. [`TopologySource`] — the trait the client asks for a fresh
//!    [`MembershipEpoch`]. Abstracting the fetch lets tests inject
//!    a static table while production code goes over the real
//!    Control Ring RPC.
//! 2. [`ControlRingTopologySource`] — the production impl. Opens
//!    a bidi stream on a bootstrap peer via the shared
//!    [`TransportProviderFactory`], sends an empty `STATUS_REQ`
//!    frame, and decodes the postcard-encoded
//!    [`ClusterStatusWire`] reply. Any richer trailing control-ring
//!    payload (such as lease metadata) is ignored here so topology
//!    refresh stays compatible with the stable status meta path.
//!
//! ## Why a dedicated wire struct
//!
//! [`crate::network::traits::ClusterStatusResponseMeta`] is an
//! in-process placeholder whose wire layout is explicitly not yet
//! frozen. Rather than promote it to a [`bytemuck::Pod`] prematurely,
//! we ship a postcard-encoded projection here — the client pays one
//! varint decode per topology refresh (which happens on the order of
//! once per epoch, not per request) and the server-side RPC surface
//! gets to evolve freely behind it.
//!
//! ## Bootstrap problem
//!
//! The very first topology fetch has to dial *somebody*, so the
//! client is constructed with a `MembershipEpoch::empty()` plus a
//! handful of bootstrap peers already wired into the shared
//! watch channel. The first successful refresh replaces the seed
//! epoch with the authoritative snapshot, and every subsequent
//! refresh just updates it in place.

use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::transport::{MembershipEpoch, NodeRoleSet, PeerCapabilities, TransportTierSet};
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

// ============================================================
//  WIRE SHAPE
// ============================================================

/// Client-visible projection of the cluster status RPC reply.
///
/// The shape intentionally mirrors
/// [`crate::network::transport::MembershipEpoch`] plus the two
/// cluster-global knobs that matter for client-side routing
/// decisions (current leader, replication factor). The server side
/// is expected to build this from its own `ring(E)` projection when
/// the Control Ring status RPC lands; until then tests feed the
/// shape directly through a [`StaticTopologySource`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStatusWire {
	/// Monotonic Raft epoch the snapshot was taken from.
	pub epoch: u64,
	/// Current leader id, or `None` if the cluster is mid-election.
	/// Clients preferring the leader (linearizable reads, writes)
	/// should target this node when it is populated.
	pub leader: Option<u32>,
	/// Cluster-wide replication factor. V1 routes assume a static
	/// RF; advancing past that lives with the Unified Hash Ring
	/// projection in a later slice.
	pub replication_factor: u32,
	/// Full peer list: id, reachable address, advertised transport
	/// tier bitmask. Fed straight into a fresh [`MembershipEpoch`]
	/// by [`TopologySource::fetch`] consumers.
	pub peers: Vec<ClusterPeerWire>,
}

/// Single-peer record inside a [`ClusterStatusWire`]. Kept flat so
/// a deserializer pass does not need to allocate a nested map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterPeerWire {
	pub node_id: u32,
	pub address: String,
	/// Raw [`TransportTierSet`] bitmask. The client reconstructs
	/// the typed form via `TransportTierSet::from_u8` at consume
	/// time so upgrades to the tier enum do not churn this struct.
	pub tier_bits: u8,
	/// Raw [`NodeRoleSet`] bitmask (ADR 002). Absent on the wire —
	/// and defaulted to 0 on deserialize — for backwards
	/// compatibility with older control-ring endpoints that have
	/// not been upgraded yet; such peers are effectively invisible
	/// to the Unified Hash Ring until they re-publish with roles.
	#[serde(default)]
	pub role_bits: u8,
}

impl ClusterStatusWire {
	/// Project into the form the transport factory consumes. The
	/// `replication_factor` and `leader` are not part of
	/// [`MembershipEpoch`]; callers pick those off separately and
	/// feed them to [`crate::client::core::WendelonClient`] so the
	/// router and redirect logic stay in sync.
	pub fn to_membership(&self) -> MembershipEpoch {
		let mut peers = HashMap::with_capacity(self.peers.len());
		for p in &self.peers {
			peers.insert(
				NodeId {
					id: p.node_id,
					_reserved: 0,
				},
				PeerCapabilities {
					address: p.address.clone(),
					tiers: TransportTierSet::from_u8(p.tier_bits),
					roles: NodeRoleSet::from_u8(p.role_bits),
				},
			);
		}
		MembershipEpoch {
			epoch: self.epoch,
			peers,
		}
	}
}

// ============================================================
//  SOURCE TRAIT
// ============================================================

/// Pluggable provider of fresh cluster topology snapshots.
///
/// The trait is deliberately narrow — one async call returning a
/// whole-snapshot `ClusterStatusWire` — so the client can swap a
/// production Control Ring source for an in-process static mock
/// without either side knowing about the other.
#[async_trait]
pub trait TopologySource: Send + Sync {
	async fn fetch(&self) -> std::io::Result<ClusterStatusWire>;

	/// Fetch a snapshot, preferring the supplied peers when the
	/// source implementation can use them. The default keeps the
	/// existing pull model and ignores the hints.
	async fn fetch_from_candidates(
		&self,
		_candidates: &[NodeId],
	) -> std::io::Result<ClusterStatusWire> {
		self.fetch().await
	}
}

// ============================================================
//  STATIC SOURCE (BOOTSTRAP / TEST)
// ============================================================

/// Hand-rolled topology source backed by an in-memory snapshot.
///
/// Used in two places:
/// 1. Tests inject a known topology without spinning a server.
/// 2. Production clients can seed a freshly-constructed
///    [`WendelonClient`][crate::client::core::WendelonClient] with a
///    well-known bootstrap snapshot before the first real refresh.
pub struct StaticTopologySource {
	snapshot: std::sync::Mutex<ClusterStatusWire>,
}

impl StaticTopologySource {
	pub fn new(initial: ClusterStatusWire) -> Self {
		Self {
			snapshot: std::sync::Mutex::new(initial),
		}
	}

	/// Swap in a fresh snapshot. Tests use this to simulate a
	/// topology shift between two `fetch` calls.
	pub fn set(&self, snapshot: ClusterStatusWire) {
		*self.snapshot.lock().unwrap() = snapshot;
	}
}

#[async_trait]
impl TopologySource for StaticTopologySource {
	async fn fetch(&self) -> std::io::Result<ClusterStatusWire> {
		Ok(self.snapshot.lock().unwrap().clone())
	}
}

// ============================================================
//  CONTROL RING SOURCE (PRODUCTION)
// ============================================================

/// Fetches topology by dialing a bootstrap peer and running a
/// Control Ring `STATUS_REQ` RPC over HRFT.
///
/// The implementation keeps a list of bootstrap node ids and dials
/// them in order until one answers — a failed dial simply falls
/// through to the next. Connection caching lives in the shared
/// [`TransportProviderFactory`], so retries on a hot cluster never
/// incur a second handshake.
pub struct ControlRingTopologySource {
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	/// Ordered list of peers the client can fall back to for
	/// bootstrap. Typically a small set of well-known admin nodes;
	/// mutating the list across a running client is not expected.
	bootstrap: Vec<NodeId>,
}

impl ControlRingTopologySource {
	pub fn new(
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		bootstrap: Vec<NodeId>,
	) -> Self {
		Self {
			factory,
			pool,
			bootstrap,
		}
	}

	async fn fetch_from(&self, target: NodeId) -> std::io::Result<ClusterStatusWire> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STATUS_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &[], None, None).await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

		let resp = read_frame(&mut *stream, &self.pool).await?;
		if resp.header.msg_type != MsgType::STATUS_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected STATUS_RESP, got msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		if resp.arrow_payload.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"STATUS_RESP must not carry an Arrow payload",
			));
		}
		wire::decode::<ClusterStatusWire>(&resp.meta)
	}

	fn candidate_order(&self, preferred: &[NodeId]) -> Vec<NodeId> {
		let mut ordered = Vec::with_capacity(preferred.len() + self.bootstrap.len());
		for node in preferred.iter().chain(self.bootstrap.iter()) {
			if !ordered
				.iter()
				.any(|existing: &NodeId| existing.id == node.id)
			{
				ordered.push(*node);
			}
		}
		ordered
	}
}

#[async_trait]
impl TopologySource for ControlRingTopologySource {
	async fn fetch(&self) -> std::io::Result<ClusterStatusWire> {
		self.fetch_from_candidates(&[]).await
	}

	async fn fetch_from_candidates(
		&self,
		candidates: &[NodeId],
	) -> std::io::Result<ClusterStatusWire> {
		let mut last_err: Option<Error> = None;
		for peer in self.candidate_order(candidates) {
			match self.fetch_from(peer).await {
				Ok(snapshot) => return Ok(snapshot),
				Err(e) => last_err = Some(e),
			}
		}
		Err(last_err.unwrap_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				"no bootstrap peers configured for topology refresh",
			)
		}))
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::transport::TransportTier;

	fn sample_snapshot() -> ClusterStatusWire {
		ClusterStatusWire {
			epoch: 42,
			leader: Some(1),
			replication_factor: 3,
			peers: vec![
				ClusterPeerWire {
					node_id: 1,
					address: "127.0.0.1:9001".into(),
					tier_bits: TransportTierSet::empty()
						.with(TransportTier::QuicIoUring)
						.as_u8(),
					role_bits: NodeRoleSet::empty()
						.with(crate::network::transport::NodeRole::Data)
						.as_u8(),
				},
				ClusterPeerWire {
					node_id: 2,
					address: "127.0.0.1:9002".into(),
					tier_bits: TransportTierSet::empty()
						.with(TransportTier::QuicIoUring)
						.with(TransportTier::TcpTls)
						.as_u8(),
					role_bits: NodeRoleSet::empty()
						.with(crate::network::transport::NodeRole::Data)
						.as_u8(),
				},
			],
		}
	}

	#[test]
	fn to_membership_populates_factory_inputs() {
		let snap = sample_snapshot();
		let mem = snap.to_membership();
		assert_eq!(mem.epoch, 42);
		assert_eq!(mem.peers.len(), 2);
		let p1 = mem
			.peers
			.get(&NodeId {
				id: 1,
				_reserved: 0,
			})
			.unwrap();
		assert_eq!(p1.address, "127.0.0.1:9001");
		assert!(p1.tiers.contains(TransportTier::QuicIoUring));
		assert!(!p1.tiers.contains(TransportTier::TcpTls));
		let p2 = mem
			.peers
			.get(&NodeId {
				id: 2,
				_reserved: 0,
			})
			.unwrap();
		assert!(p2.tiers.contains(TransportTier::TcpTls));
	}

	#[tokio::test]
	async fn static_topology_source_returns_set_snapshot() {
		let src = StaticTopologySource::new(sample_snapshot());
		let first = src.fetch().await.unwrap();
		assert_eq!(first.epoch, 42);

		let mut newer = sample_snapshot();
		newer.epoch = 43;
		newer.leader = Some(2);
		src.set(newer);
		let second = src.fetch().await.unwrap();
		assert_eq!(second.epoch, 43);
		assert_eq!(second.leader, Some(2));
	}

	#[test]
	fn wire_roundtrips_through_postcard() {
		let snap = sample_snapshot();
		let bytes = postcard::to_allocvec(&snap).unwrap();
		let decoded: ClusterStatusWire = postcard::from_bytes(&bytes).unwrap();
		assert_eq!(decoded, snap);
	}
}
