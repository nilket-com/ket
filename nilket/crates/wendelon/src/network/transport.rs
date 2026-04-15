//! Transport abstraction for the Auto-Adaptive Multi-Transport
//! architecture (ADR 011).
//!
//! Wendelon runs on fabrics that range from a developer laptop
//! (TCP + TLS) to a datacenter RoCEv2 deployment (RDMA kernel
//! bypass). Every pair of nodes negotiates the highest-priority
//! transport they both support at connection time, and the rest of
//! the system — `DataRing`, `ControlRing`, `WendelonRaftNetwork` —
//! talks to whatever wire was chosen through the traits below.
//!
//! ## What lives here
//!
//! * [`TransportTier`] — the finite ranked enum of transports we
//!   will ever negotiate over, with the numeric value doubling as
//!   the bit position inside a [`TransportTierSet`].
//! * [`TransportTierSet`] — compact `u8` bitmask of a node's
//!   supported tiers. Rides in the Raft `ring(E)` snapshot so the
//!   resolver can answer "what does node N speak?" locally.
//! * [`TransportStream`] — the bidi byte stream that
//!   [`read_frame`][crate::network::core::read_frame] /
//!   [`write_frame`][crate::network::core::write_frame] work over.
//!   Deliberately keeps `AsyncRead + AsyncWrite + Unpin + Send` as
//!   supertraits so the existing codecs stay transport-agnostic —
//!   callers pass `&mut *stream` to them without adapter shims.
//! * [`TransportConnection`] — a per-peer session. `open_bi` /
//!   `accept_bi` return `Box<dyn TransportStream>`; the heap alloc
//!   per stream is the acknowledged cost of dyn-dispatched
//!   polymorphism and is deliberately preferred over pushing a
//!   generic parameter through every caller.
//! * [`CapabilityResolver`] — the Slice-3 hook that lets the
//!   factory look up a target node's advertised tiers before
//!   dialing. Kept async so future impls can await a
//!   membership-epoch watch instead of returning stale data.
//! * [`PoolRegistrar`] — the RDMA hook. Tier 0 needs the NIC to pin
//!   every [`PooledBuf`][crate::network::pool::PooledBuf] the data
//!   ring will DMA into, and it stores the returned LKey on the
//!   buffer itself.

use crate::network::pool::PooledBuf;
use crate::schema::NodeId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

// ============================================================
//  TIERS
// ============================================================

/// Ranked set of transport implementations Wendelon will negotiate
/// over. Lower numeric discriminants are higher priority: a node
/// that advertises both Tier 0 and Tier 1 will be dialed over
/// Tier 0 if the local side also supports it.
///
/// The discriminant doubles as the bit position inside
/// [`TransportTierSet`] (`1u8 << tier as u8`), so adding a new
/// tier is "pick the next free bit and update [`Self::ALL`]."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TransportTier {
	/// Tier 0 — RDMA over Converged Ethernet v2. Zero-copy,
	/// kernel-bypass, requires a RoCE-capable NIC on both ends.
	RdmaRoceV2 = 0,
	/// Tier 1 — QUIC on top of `io_uring` + GSO. The universal
	/// baseline: always available on hosts that can run Wendelon
	/// at all, and the channel that carries the capability
	/// handshake even when an upgrade to Tier 0 is about to happen.
	QuicIoUring = 1,
	/// Tier 2 — TCP + TLS 1.3. Compatibility fallback for
	/// environments where QUIC is blocked (middleboxes, some
	/// corporate egress).
	TcpTls = 2,
}

impl TransportTier {
	/// All tiers in priority order, highest first. Iteration order
	/// matters — [`TransportTierSet::highest`] walks this to find
	/// the best tier present in a bitmask.
	pub const ALL: [TransportTier; 3] = [
		TransportTier::RdmaRoceV2,
		TransportTier::QuicIoUring,
		TransportTier::TcpTls,
	];

	#[inline]
	const fn bit(self) -> u8 {
		1u8 << (self as u8)
	}
}

/// Compact bitmask of supported tiers. One byte per node, cheap to
/// include in the `ring(E)` Raft snapshot.
///
/// The `best_common` method implements the negotiation rule from
/// ADR 011: pick the highest-priority tier that appears on both
/// sides of a would-be connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransportTierSet(u8);

impl TransportTierSet {
	pub const fn empty() -> Self {
		Self(0)
	}

	/// Bitmask-literal constructor for test fixtures and the Raft
	/// snapshot decoder; round-trips with [`Self::as_u8`].
	pub const fn from_u8(bits: u8) -> Self {
		Self(bits)
	}

	pub const fn as_u8(self) -> u8 {
		self.0
	}

	pub const fn is_empty(self) -> bool {
		self.0 == 0
	}

	pub fn with(mut self, tier: TransportTier) -> Self {
		self.0 |= tier.bit();
		self
	}

	pub fn contains(self, tier: TransportTier) -> bool {
		self.0 & tier.bit() != 0
	}

	/// Highest-priority tier present in the set, if any.
	pub fn highest(self) -> Option<TransportTier> {
		TransportTier::ALL.into_iter().find(|t| self.contains(*t))
	}

	/// Best tier supported by both sides — the negotiated choice
	/// for a new connection.
	pub fn best_common(self, other: TransportTierSet) -> Option<TransportTier> {
		TransportTierSet(self.0 & other.0).highest()
	}
}

// ============================================================
//  STREAM / CONNECTION
// ============================================================

/// Transport-agnostic bidirectional byte stream.
///
/// The supertraits (`AsyncRead + AsyncWrite + Unpin + Send`) are
/// what lets existing code — especially
/// [`read_frame`][crate::network::core::read_frame] and
/// [`write_frame`][crate::network::core::write_frame] — operate on
/// `&mut dyn TransportStream` without knowing whether the bytes
/// are flowing over QUIC, RDMA send queues, or a raw TLS socket.
#[async_trait]
pub trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send {
	/// Unique id within the owning connection. For QUIC this wraps
	/// the peer-observable `quinn::StreamId`; RDMA providers will
	/// synthesize a monotonic counter per queue pair.
	fn stream_id(&self) -> u64;

	/// Cleanly close the write side of the stream. Reads may
	/// continue until the peer returns FIN on its direction.
	async fn finish(&mut self) -> std::io::Result<()>;
}

/// One transport session between two nodes. Opaque to the higher
/// layers — they ask for a stream, not a socket.
///
/// `open_bi` / `accept_bi` return `Box<dyn TransportStream>` so the
/// concrete quinn / ibverbs / tokio-rustls types never leak above
/// this module. The heap allocation per stream is an intentional
/// trade against pushing an associated type through every call site.
#[async_trait]
pub trait TransportConnection: Send + Sync {
	/// Open a new bidi stream toward the remote peer.
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>>;

	/// Accept the next bidi stream the peer has opened.
	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>>;

	/// Peer identity. Used as the cache key in the factory and as
	/// the attribution target for `RPCError::RemoteError`.
	fn remote_id(&self) -> NodeId;

	/// Which tier this connection is actually running over. A node
	/// whose advertised `TransportTierSet` contains Tier 0 may
	/// still end up with a Tier 1 connection if negotiation fell
	/// back — the caller reads the real answer from here.
	fn transport_type(&self) -> TransportTier;

	/// Whether the session has terminated (locally or by the peer).
	/// Cached connections that report `true` should be dropped and
	/// re-dialed on the next RPC.
	fn is_closed(&self) -> bool;
}

// ============================================================
//  PROVIDER (DIAL SIDE)
// ============================================================

/// Dials a peer over a specific transport tier.
///
/// `connect` returns a fresh, uncached connection — connection
/// caching is the caller's job today (`WendelonRaftNetwork` keeps
/// one per peer). Slice 3 will introduce a higher-level factory
/// that owns a per-`NodeId` connection cache and a
/// [`CapabilityResolver`] so callers can stop passing the address
/// in by hand.
#[async_trait]
pub trait TransportProvider: Send + Sync {
	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>>;
}

// ============================================================
//  NODE ROLES
// ============================================================

/// Cluster role a node advertises. Distinct from the transport
/// tier — the tier says *how* to talk to a peer, the role says
/// *what* the peer is willing to do once the stream is open.
///
/// The discriminant doubles as the bit position inside
/// [`NodeRoleSet`] (`1u8 << role as u8`), so adding a new role
/// is "pick the next free bit and update [`Self::ALL`]."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeRole {
	/// Accepts Data Ring traffic — writes, reads, and pushdown
	/// queries land here. The Unified Hash Ring walks this role
	/// when resolving replica sets.
	Data = 0,
	/// Raft voting member. Only these nodes accept RPCs from the
	/// consensus layer; clients never route by this role.
	Control = 1,
	/// Terminates L2 traffic (object-store shuffling, cold-tier
	/// backfill). Reserved so the bit position stays stable even
	/// before the L2 router lands.
	L2 = 2,
}

impl NodeRole {
	pub const ALL: [NodeRole; 3] = [NodeRole::Data, NodeRole::Control, NodeRole::L2];

	#[inline]
	const fn bit(self) -> u8 {
		1u8 << (self as u8)
	}
}

/// Compact bitmask of advertised roles. One byte per node — lives
/// next to [`TransportTierSet`] on [`PeerCapabilities`] and is
/// projected out of the Raft `ring(E)` snapshot together with
/// the tier bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeRoleSet(u8);

impl NodeRoleSet {
	pub const fn empty() -> Self {
		Self(0)
	}

	pub const fn from_u8(bits: u8) -> Self {
		Self(bits)
	}

	pub const fn as_u8(self) -> u8 {
		self.0
	}

	pub const fn is_empty(self) -> bool {
		self.0 == 0
	}

	pub fn with(mut self, role: NodeRole) -> Self {
		self.0 |= role.bit();
		self
	}

	pub fn contains(self, role: NodeRole) -> bool {
		self.0 & role.bit() != 0
	}
}

// ============================================================
//  MEMBERSHIP SNAPSHOT
// ============================================================

/// Everything the dial side needs to know about a peer: *where*
/// to reach it and *what* transport tiers it advertises. Sourced
/// from the Raft `ring(E)` snapshot, projected by the consensus
/// layer into the in-memory form the factory consumes.
///
/// Kept alongside [`TransportTierSet`] rather than in
/// `crate::schema` because the address is wire-erased at this
/// layer — the factory hands it to whichever
/// [`TransportProvider`] won negotiation, and each provider
/// parses the string according to its own rules (a socket addr
/// for QUIC/TCP; a GID + QPN pair for RDMA).
#[derive(Debug, Clone)]
pub struct PeerCapabilities {
	pub address: String,
	pub tiers: TransportTierSet,
	/// Roles the peer accepts traffic for. Routing layers filter
	/// the Unified Hash Ring (ADR 002) by the role they care
	/// about — Data for replica placement, Control for Raft RPCs.
	/// Defaults to an empty set so a peer that elects not to
	/// serve any role is addressable but unreachable for routing.
	pub roles: NodeRoleSet,
}

/// Projection of Raft `ring(E)` into the shape the transport
/// factory consumes. Published by the consensus layer through a
/// [`tokio::sync::watch`] channel every time membership changes —
/// the factory's invalidation task walks the cache on each
/// transition and drops connections to peers that have left the
/// ring or dropped support for the tier we had negotiated.
///
/// `epoch` is the monotonic Raft epoch the snapshot was derived
/// from; kept on the struct for tracing / audit, not read for
/// routing decisions.
#[derive(Debug, Clone)]
pub struct MembershipEpoch {
	pub epoch: u64,
	pub peers: HashMap<NodeId, PeerCapabilities>,
}

impl MembershipEpoch {
	/// Empty seed epoch — the value the consensus layer should
	/// publish into the watch channel before wiring anything into
	/// it, so the factory's initial `borrow()` is always valid.
	pub fn empty() -> Self {
		Self {
			epoch: 0,
			peers: HashMap::new(),
		}
	}
}

// ============================================================
//  CAPABILITY RESOLUTION
// ============================================================

/// Looks up the current dial-time picture of a remote node —
/// address, advertised tiers, the lot.
///
/// The trait is async so implementations can await a freshly
/// applied membership epoch instead of serving stale data right
/// after a reconfiguration. [`EpochCapabilityResolver`] is the
/// watch-backed reference implementation; tests can swap in a
/// simpler in-memory resolver.
#[async_trait]
pub trait CapabilityResolver: Send + Sync {
	/// Full lookup: address + tiers. The factory calls this before
	/// every fresh dial.
	async fn resolve(&self, node: NodeId) -> std::io::Result<PeerCapabilities>;

	/// Convenience accessor for callers that only care about
	/// advertised tiers (e.g. capability-reporting endpoints).
	/// Defaults to projecting out of [`Self::resolve`].
	async fn tiers(&self, node: NodeId) -> std::io::Result<TransportTierSet> {
		Ok(self.resolve(node).await?.tiers)
	}
}

/// [`CapabilityResolver`] backed by a `watch::Receiver` on the
/// consensus-layer [`MembershipEpoch`] publication.
///
/// Reads are O(1) and non-blocking: the resolver holds onto the
/// most recent `Arc<MembershipEpoch>` via `borrow()`, bumps the
/// Arc's refcount, and answers out of that snapshot. The factory's
/// separate invalidation task (see
/// [`crate::network::factory::TransportProviderFactory`]) is what
/// *reacts* to changes; this resolver simply reads the latest
/// value on demand.
pub struct EpochCapabilityResolver {
	rx: watch::Receiver<Arc<MembershipEpoch>>,
}

impl EpochCapabilityResolver {
	pub fn new(rx: watch::Receiver<Arc<MembershipEpoch>>) -> Self {
		Self { rx }
	}
}

#[async_trait]
impl CapabilityResolver for EpochCapabilityResolver {
	async fn resolve(&self, node: NodeId) -> std::io::Result<PeerCapabilities> {
		// `borrow()` hands back a borrow of the Arc; cloning the
		// inner `PeerCapabilities` is cheaper than holding the
		// borrow across the caller's next await.
		let epoch = self.rx.borrow().clone();
		epoch.peers.get(&node).cloned().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"peer {node:?} not present in membership epoch {}",
					epoch.epoch
				),
			)
		})
	}
}

// ============================================================
//  POOL REGISTRATION (RDMA hook)
// ============================================================

/// Pins a [`PooledBuf`][crate::network::pool::PooledBuf]'s backing
/// allocation with the NIC and records the returned LKey on the
/// buffer itself.
///
/// QUIC providers do not need this; their concrete `PoolRegistrar`
/// is a no-op that always returns `Ok(None)`. The RDMA provider
/// (forthcoming `src/network/rdma.rs`) installs a real registrar
/// whose `register` call issues `ibv_reg_mr` against a protection
/// domain and stores the resulting local key.
///
/// ## Lifetime contract
///
/// Registration is tied to the buffer's current heap allocation.
/// If the pool subsequently releases and re-acquires the Vec
/// without reallocating (the common case under `max_cap_bytes`),
/// the LKey remains valid; if the Vec reallocates, the RDMA path
/// must re-register before the next work request.
pub trait PoolRegistrar: Send + Sync {
	/// Pin `buf`'s memory with the fabric and stash the returned
	/// LKey on the buffer. Returns the key for the caller's
	/// convenience, or `None` if this transport does not require
	/// registration.
	fn register(&self, buf: &mut PooledBuf) -> std::io::Result<Option<NonZeroU32>>;
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tier_bits_are_disjoint() {
		let set = TransportTierSet::empty()
			.with(TransportTier::RdmaRoceV2)
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls);
		assert_eq!(set.as_u8(), 0b0000_0111);
	}

	#[test]
	fn highest_prefers_lowest_discriminant() {
		let set = TransportTierSet::empty()
			.with(TransportTier::TcpTls)
			.with(TransportTier::QuicIoUring);
		assert_eq!(set.highest(), Some(TransportTier::QuicIoUring));
	}

	#[test]
	fn best_common_intersects_and_ranks() {
		let ours = TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls);
		let theirs = TransportTierSet::empty()
			.with(TransportTier::RdmaRoceV2)
			.with(TransportTier::TcpTls);
		assert_eq!(ours.best_common(theirs), Some(TransportTier::TcpTls));
	}

	#[test]
	fn best_common_empty_intersection_is_none() {
		let ours = TransportTierSet::empty().with(TransportTier::RdmaRoceV2);
		let theirs = TransportTierSet::empty().with(TransportTier::TcpTls);
		assert_eq!(ours.best_common(theirs), None);
	}

	#[test]
	fn contains_only_the_tiers_added() {
		let set = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		assert!(set.contains(TransportTier::QuicIoUring));
		assert!(!set.contains(TransportTier::RdmaRoceV2));
		assert!(!set.contains(TransportTier::TcpTls));
	}

	#[test]
	fn from_u8_round_trips() {
		let set = TransportTierSet::empty()
			.with(TransportTier::RdmaRoceV2)
			.with(TransportTier::TcpTls);
		assert_eq!(TransportTierSet::from_u8(set.as_u8()), set);
	}
}
