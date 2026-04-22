//! Per-`NodeId` connection factory and transport-selection front
//! door for ADR 011's QUIC-first support strategy.
//!
//! Slice 2 left connection caching inside `WendelonRaftNetwork` —
//! one Raft peer, one cached session, redial on
//! [`TransportConnection::is_closed`]. That works for Raft's
//! small, stable peer set but it does not scale to the
//! client-initiated Data-Ring streams that will share the same
//! fabric: every subsystem can't keep its own per-peer cache.
//!
//! This module centralises that job.
//!
//! QUIC is the supported transport today. The factory still owns
//! provider dispatch and capability-aware selection so the codebase
//! keeps a clean seam for optional compatibility paths or future
//! premium-hardware transports, but that seam is not itself a
//! promise that Wendelon already ships an adaptive transport stack.
//!
//! ## Responsibilities
//!
//! 1. **Tier negotiation** — combine the local node's
//!    [`TransportTierSet`] with the target peer's advertised tiers
//!    (via [`CapabilityResolver`]) and pick the highest-priority
//!    intersection with [`TransportTierSet::best_common`].
//! 2. **Provider dispatch** — look up the concrete
//!    [`TransportProvider`] for the negotiated tier and hand it
//!    the peer address drawn from the same resolver lookup.
//! 3. **Per-peer connection cache** — one
//!    `Arc<dyn TransportConnection>` per `NodeId`, reused across
//!    every caller (Raft, Data Ring, etc.) until the session
//!    reports `is_closed()` or membership invalidation drops it.
//! 4. **Membership invalidation** — a background task subscribes
//!    to the `watch::Receiver<Arc<MembershipEpoch>>` and, on every
//!    transition, walks the cache and evicts sessions whose peer
//!    left the ring or stopped advertising the negotiated tier.
//!
//! ## Race handling
//!
//! The cache uses a `std::sync::Mutex<HashMap>` that is **never**
//! held across `await` — lookups, inserts, and retain walks are
//! all synchronous. A missed-cache dial issues its
//! `provider.connect()` call without the lock held, then
//! re-acquires the lock for a double-check insert so concurrent
//! racers collapse onto a single winning connection (the loser's
//! freshly-dialed session drops when its `Arc` goes out of scope —
//! unused but harmless, and race-induced duplicate dials are rare
//! in practice because steady-state traffic is cache-hits).
//!
//! ## Invalidation task lifetime
//!
//! The spawned task holds a `Weak<TransportProviderFactory>`. When
//! the factory's last `Arc` goes away the upgrade fails and the
//! task returns. Dropping the factory is therefore clean — no
//! channel close, no poisoning, no orphaned task — and tests can
//! construct and discard factories freely.

use crate::network::transport::{
	CapabilityResolver, MembershipEpoch, TransportConnection, TransportProvider, TransportTier,
	TransportTierSet,
};
use crate::schema::NodeId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::watch;

// ============================================================
//  FACTORY
// ============================================================

/// Single process-wide entry point for obtaining a connection to
/// any peer in the cluster.
///
/// Construct via [`TransportProviderFactory::new`]; always keep
/// the returned `Arc<Self>` alive for as long as any subsystem
/// intends to call [`Self::get_connection`].
pub struct TransportProviderFactory {
	/// Tiers this node supports locally — the bitmask published
	/// into our own `ring(E)` entry at boot. Combined with the
	/// peer's advertised set to pick the negotiated tier.
	local_tiers: TransportTierSet,

	/// One provider per tier we can actually dial over.
	///
	/// A tier present in `local_tiers` but missing here is a
	/// configuration error — the factory will fail any dial that
	/// negotiates to a missing provider with
	/// `ErrorKind::Unsupported`.
	providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,

	/// Peer capability + address lookup. The same resolver is
	/// shared with anything else that wants to know who's in the
	/// ring.
	resolver: Arc<dyn CapabilityResolver>,
	/// Latest published membership snapshot. This is a temporary
	/// convenience seam for callers that already depend on the
	/// factory and need a read-only view of membership without
	/// threading a second watch channel through their constructors.
	/// If another subsystem grows the same need, promote this to a
	/// dedicated membership-view service instead of expanding the
	/// factory's responsibilities further.
	membership_rx: watch::Receiver<Arc<MembershipEpoch>>,

	/// Per-`NodeId` live-connection cache.
	///
	/// Sync `Mutex` is deliberate: we never hold the lock across
	/// an `await`, so there is no value in paying for a tokio
	/// mutex. The invalidation task and `get_connection` both
	/// grab and drop the lock over a single synchronous block.
	connections: Mutex<HashMap<NodeId, Arc<dyn TransportConnection>>>,
	policy: TransportSelectionPolicy,
	health: Mutex<HashMap<NodeId, HashMap<TransportTier, TierHealth>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportSelectionPolicy {
	pub degrade_after_failures: usize,
	pub cooldown: Duration,
}

impl Default for TransportSelectionPolicy {
	fn default() -> Self {
		Self {
			degrade_after_failures: 1,
			cooldown: Duration::from_secs(30),
		}
	}
}

#[derive(Debug, Clone, Copy, Default)]
struct TierHealth {
	consecutive_failures: usize,
	degraded_until: Option<Instant>,
}

impl TransportProviderFactory {
	/// Build a factory and spawn its invalidation task.
	///
	/// Must be called from inside a tokio runtime — the spawn
	/// happens eagerly so the caller cannot forget to wire
	/// invalidation up after the fact.
	pub fn new(
		local_tiers: TransportTierSet,
		providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,
		resolver: Arc<dyn CapabilityResolver>,
		epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
	) -> Arc<Self> {
		Self::new_with_policy(
			local_tiers,
			providers,
			resolver,
			epoch_rx,
			TransportSelectionPolicy::default(),
		)
	}

	pub fn new_with_policy(
		local_tiers: TransportTierSet,
		providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,
		resolver: Arc<dyn CapabilityResolver>,
		epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
		policy: TransportSelectionPolicy,
	) -> Arc<Self> {
		let factory = Arc::new(Self {
			local_tiers,
			providers,
			resolver,
			membership_rx: epoch_rx.clone(),
			connections: Mutex::new(HashMap::new()),
			policy,
			health: Mutex::new(HashMap::new()),
		});
		let weak = Arc::downgrade(&factory);
		tokio::spawn(invalidation_loop(weak, epoch_rx));
		factory
	}

	/// Return a live connection to `target`, reusing the cached
	/// session if one exists and has not closed, otherwise
	/// negotiating a tier and dialing through the appropriate
	/// [`TransportProvider`].
	pub async fn get_connection(
		&self,
		target: NodeId,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		let peer = self.resolver.resolve(target).await?;
		self.connect_with_tiers(target, &peer.address, peer.tiers)
			.await
	}

	/// Like [`Self::get_connection`], but for call sites that already
	/// know the peer address and need tier fallback without waiting on
	/// membership resolution first (bootstrap joins, openraft dialers).
	pub async fn get_connection_to(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		self.connect_with_tiers(target, address, self.local_tiers)
			.await
	}

	/// Synchronous cache probe. Returns the cached connection
	/// only if it is still open; otherwise evicts the stale entry
	/// and returns `None` so the caller re-dials.
	fn lookup_fresh(&self, target: NodeId) -> Option<Arc<dyn TransportConnection>> {
		let mut cache = self.connections.lock().unwrap();
		match cache.get(&target) {
			Some(conn) if !conn.is_closed() => Some(conn.clone()),
			Some(_) => {
				cache.remove(&target);
				None
			}
			None => None,
		}
	}

	/// Diagnostic: how many cached sessions the factory holds.
	pub fn cached_connection_count(&self) -> usize {
		self.connections.lock().unwrap().len()
	}

	/// Read-only view of the latest membership snapshot the factory
	/// is routing against. Convenience accessor, not a stable
	/// cross-subsystem contract.
	pub fn membership_snapshot(&self) -> Arc<MembershipEpoch> {
		self.membership_rx.borrow().clone()
	}

	fn candidate_tiers(
		&self,
		target: NodeId,
		peer_tiers: TransportTierSet,
	) -> Option<Vec<TransportTier>> {
		let mut healthy = Vec::new();
		let mut degraded = Vec::new();
		let now = Instant::now();
		let mut health = self.health.lock().unwrap();
		let per_peer = health.entry(target).or_default();
		for tier in TransportTier::ALL.iter().copied() {
			if !self.local_tiers.contains(tier) || !peer_tiers.contains(tier) {
				continue;
			}
			let is_degraded = match per_peer.get(&tier).and_then(|h| h.degraded_until) {
				Some(until) if until > now => true,
				Some(_) => {
					per_peer.remove(&tier);
					false
				}
				None => false,
			};
			if is_degraded {
				degraded.push(tier);
			} else {
				healthy.push(tier);
			}
		}
		if healthy.is_empty() && degraded.is_empty() {
			None
		} else {
			healthy.extend(degraded);
			Some(healthy)
		}
	}

	fn record_connect_success(&self, target: NodeId, tier: TransportTier) {
		let mut health = self.health.lock().unwrap();
		if let Some(per_peer) = health.get_mut(&target) {
			per_peer.remove(&tier);
			if per_peer.is_empty() {
				health.remove(&target);
			}
		}
	}

	fn record_connect_failure(&self, target: NodeId, tier: TransportTier) {
		let mut health = self.health.lock().unwrap();
		let per_peer = health.entry(target).or_default();
		let entry = per_peer.entry(tier).or_default();
		entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
		if entry.consecutive_failures >= self.policy.degrade_after_failures {
			entry.consecutive_failures = 0;
			entry.degraded_until = Some(Instant::now() + self.policy.cooldown);
		}
	}

	async fn connect_with_tiers(
		&self,
		target: NodeId,
		address: &str,
		peer_tiers: TransportTierSet,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		if let Some(conn) = self.lookup_fresh(target) {
			return Ok(conn);
		}

		let candidate_tiers = self.candidate_tiers(target, peer_tiers).ok_or_else(|| {
			Error::new(
				ErrorKind::Unsupported,
				format!(
					"no common transport tier with peer {target:?} \
                     (ours: 0x{:02x}, theirs: 0x{:02x})",
					self.local_tiers.as_u8(),
					peer_tiers.as_u8(),
				),
			)
		})?;
		let mut last_err: Option<Error> = None;
		let mut fresh: Option<Arc<dyn TransportConnection>> = None;
		for tier in candidate_tiers {
			let Some(provider) = self.providers.get(&tier) else {
				last_err = Some(Error::new(
					ErrorKind::Unsupported,
					format!("no provider registered for negotiated tier {tier:?}"),
				));
				self.record_connect_failure(target, tier);
				continue;
			};
			match provider.connect(target, address).await {
				Ok(conn) => {
					self.record_connect_success(target, tier);
					fresh = Some(conn);
					break;
				}
				Err(err) => {
					self.record_connect_failure(target, tier);
					last_err = Some(Error::new(
						err.kind(),
						format!("connect via {tier:?} to {address} failed: {err}"),
					));
				}
			}
		}
		let fresh = fresh.ok_or_else(|| {
			last_err.unwrap_or_else(|| {
				Error::new(
					ErrorKind::ConnectionRefused,
					format!("failed to connect to peer {target:?}"),
				)
			})
		})?;

		// Double-check insert: another task may have completed a
		// dial to the same peer while we were awaiting. Prefer the
		// earlier entry so both callers share a session.
		let mut cache = self.connections.lock().unwrap();
		if let Some(existing) = cache.get(&target) {
			if !existing.is_closed() {
				return Ok(existing.clone());
			}
		}
		cache.insert(target, fresh.clone());
		Ok(fresh)
	}
}

#[async_trait]
impl TransportProvider for TransportProviderFactory {
	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		self.get_connection_to(target, address).await
	}
}

// ============================================================
//  INVALIDATION
// ============================================================

/// Background task driven by the membership watch.
///
/// Every `changed()` event walks the cache and drops entries for
/// peers that have either left the ring or revoked support for
/// the tier our cached session is running on. Any survivors
/// remain valid — we do *not* rebuild connections eagerly here;
/// the next `get_connection` call will re-dial if needed.
async fn invalidation_loop(
	factory: Weak<TransportProviderFactory>,
	mut rx: watch::Receiver<Arc<MembershipEpoch>>,
) {
	while rx.changed().await.is_ok() {
		let Some(factory) = factory.upgrade() else {
			return;
		};
		let snapshot = rx.borrow_and_update().clone();
		factory.apply_epoch(&snapshot);
	}
}

impl TransportProviderFactory {
	/// Apply a newly published membership snapshot to the cache.
	/// Exposed on `Self` rather than buried inside
	/// [`invalidation_loop`] so tests can exercise the eviction
	/// logic without spinning a task.
	pub(crate) fn apply_epoch(&self, snapshot: &MembershipEpoch) {
		let mut cache = self.connections.lock().unwrap();
		cache.retain(|node_id, conn| match snapshot.peers.get(node_id) {
			// Peer still present and still advertising the tier
			// our session is running on.
			Some(peer) if peer.tiers.contains(conn.transport_type()) => !conn.is_closed(),
			// Peer missing, or present but no longer speaks our
			// tier. Drop the session so the next dial renegotiates.
			_ => false,
		});
		let mut health = self.health.lock().unwrap();
		health.retain(|node_id, _| snapshot.peers.contains_key(node_id));
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::tls::ActorIdentity;
	use crate::network::transport::{EpochCapabilityResolver, PeerCapabilities, TransportStream};
	use async_trait::async_trait;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::time::Duration;

	// ----- Mocks -----

	struct MockConnection {
		remote: NodeId,
		tier: TransportTier,
		closed: AtomicBool,
		actor: ActorIdentity,
	}

	impl MockConnection {
		fn new(remote: NodeId, tier: TransportTier) -> Arc<Self> {
			Arc::new(Self {
				remote,
				tier,
				closed: AtomicBool::new(false),
				actor: ActorIdentity::data_node(remote),
			})
		}
	}

	#[async_trait]
	impl TransportConnection for MockConnection {
		async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			unimplemented!("mock does not open streams")
		}
		async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			unimplemented!("mock does not accept streams")
		}
		fn remote_id(&self) -> NodeId {
			self.remote
		}
		fn actor_identity(&self) -> &ActorIdentity {
			&self.actor
		}
		fn transport_type(&self) -> TransportTier {
			self.tier
		}
		fn is_closed(&self) -> bool {
			self.closed.load(Ordering::SeqCst)
		}
	}

	struct MockProvider {
		tier: TransportTier,
		dial_count: AtomicUsize,
		failures_remaining: AtomicUsize,
		/// Every successfully-dialed connection, in dial order.
		/// Tests reach into this to flip `is_closed` on a specific
		/// session without downcasting the `Arc<dyn ...>` the
		/// factory hands back.
		dialed: Mutex<Vec<Arc<MockConnection>>>,
	}

	impl MockProvider {
		fn new(tier: TransportTier) -> Arc<Self> {
			Arc::new(Self {
				tier,
				dial_count: AtomicUsize::new(0),
				failures_remaining: AtomicUsize::new(0),
				dialed: Mutex::new(Vec::new()),
			})
		}

		fn last_dialed(&self) -> Arc<MockConnection> {
			self.dialed.lock().unwrap().last().unwrap().clone()
		}

		fn fail_times(&self, count: usize) {
			self.failures_remaining.store(count, Ordering::SeqCst);
		}
	}

	#[async_trait]
	impl TransportProvider for MockProvider {
		async fn connect(
			&self,
			target: NodeId,
			_address: &str,
		) -> std::io::Result<Arc<dyn TransportConnection>> {
			self.dial_count.fetch_add(1, Ordering::SeqCst);
			let remaining = self.failures_remaining.load(Ordering::SeqCst);
			if remaining > 0 {
				self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
				return Err(Error::new(
					ErrorKind::ConnectionRefused,
					format!("forced {:?} connect failure", self.tier),
				));
			}
			let conn = MockConnection::new(target, self.tier);
			self.dialed.lock().unwrap().push(conn.clone());
			Ok(conn)
		}
	}

	// ----- Fixtures -----

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn peer(addr: &str, tiers: TransportTierSet) -> PeerCapabilities {
		PeerCapabilities {
			address: addr.into(),
			tiers,
			roles: crate::network::transport::NodeRoleSet::empty()
				.with(crate::network::transport::NodeRole::Data),
		}
	}

	fn make_factory(
		local: TransportTierSet,
		provider_tier: TransportTier,
		epoch: MembershipEpoch,
	) -> (
		Arc<TransportProviderFactory>,
		Arc<MockProvider>,
		watch::Sender<Arc<MembershipEpoch>>,
	) {
		let (tx, rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn CapabilityResolver> =
			Arc::new(EpochCapabilityResolver::new(rx.clone()));
		let provider = MockProvider::new(provider_tier);
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(provider_tier, provider.clone());
		let factory = TransportProviderFactory::new(local, providers, resolver, rx);
		(factory, provider, tx)
	}

	// ----- Tests -----

	#[tokio::test]
	async fn caches_connection_on_second_call() {
		let mut peers = HashMap::new();
		peers.insert(
			node(7),
			peer(
				"127.0.0.1:9001",
				TransportTierSet::empty().with(TransportTier::QuicIoUring),
			),
		);
		let epoch = MembershipEpoch { epoch: 1, peers };

		let local = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let (factory, provider, _tx) = make_factory(local, TransportTier::QuicIoUring, epoch);

		let a = factory.get_connection(node(7)).await.unwrap();
		let b = factory.get_connection(node(7)).await.unwrap();
		assert_eq!(provider.dial_count.load(Ordering::SeqCst), 1);
		assert!(Arc::ptr_eq(&a, &b));
	}

	#[tokio::test]
	async fn redials_when_cached_connection_closed() {
		let mut peers = HashMap::new();
		peers.insert(
			node(7),
			peer(
				"127.0.0.1:9001",
				TransportTierSet::empty().with(TransportTier::QuicIoUring),
			),
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let local = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let (factory, provider, _tx) = make_factory(local, TransportTier::QuicIoUring, epoch);

		let first = factory.get_connection(node(7)).await.unwrap();
		// Flip the mock's closed flag through the provider's own
		// ledger — no need to downcast the Arc<dyn ...> the factory
		// returned.
		provider.last_dialed().closed.store(true, Ordering::SeqCst);

		let second = factory.get_connection(node(7)).await.unwrap();
		assert_eq!(provider.dial_count.load(Ordering::SeqCst), 2);
		assert!(!Arc::ptr_eq(&first, &second));
	}

	#[tokio::test]
	async fn negotiates_best_common_tier() {
		// We speak QUIC + TCP. Peer speaks only TCP. Expect a TCP
		// dial.
		let mut peers = HashMap::new();
		peers.insert(
			node(2),
			peer(
				"127.0.0.1:9002",
				TransportTierSet::empty().with(TransportTier::TcpTls),
			),
		);
		let epoch = MembershipEpoch { epoch: 1, peers };

		let local = TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls);

		let (tx, rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn CapabilityResolver> =
			Arc::new(EpochCapabilityResolver::new(rx.clone()));
		let quic = MockProvider::new(TransportTier::QuicIoUring);
		let tcp = MockProvider::new(TransportTier::TcpTls);
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, quic.clone());
		providers.insert(TransportTier::TcpTls, tcp.clone());
		let factory = TransportProviderFactory::new(local, providers, resolver, rx);

		let conn = factory.get_connection(node(2)).await.unwrap();
		assert_eq!(conn.transport_type(), TransportTier::TcpTls);
		assert_eq!(tcp.dial_count.load(Ordering::SeqCst), 1);
		assert_eq!(quic.dial_count.load(Ordering::SeqCst), 0);
		drop(tx);
	}

	#[tokio::test]
	async fn missing_peer_returns_not_found() {
		let epoch = MembershipEpoch::empty();
		let local = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let (factory, _provider, _tx) = make_factory(local, TransportTier::QuicIoUring, epoch);

		let err = match factory.get_connection(node(99)).await {
			Ok(_) => panic!("expected error for unknown peer"),
			Err(e) => e,
		};
		assert_eq!(err.kind(), ErrorKind::NotFound);
	}

	#[tokio::test]
	async fn no_common_tier_returns_unsupported() {
		// We speak QUIC. Peer speaks only RDMA. No intersection.
		let mut peers = HashMap::new();
		peers.insert(
			node(3),
			peer(
				"rdma://fe80::1/1",
				TransportTierSet::empty().with(TransportTier::RdmaRoceV2),
			),
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let local = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let (factory, _provider, _tx) = make_factory(local, TransportTier::QuicIoUring, epoch);

		let err = match factory.get_connection(node(3)).await {
			Ok(_) => panic!("expected error for tier mismatch"),
			Err(e) => e,
		};
		assert_eq!(err.kind(), ErrorKind::Unsupported);
	}

	#[tokio::test]
	async fn falls_back_to_lower_tier_after_connect_failure() {
		let mut peers = HashMap::new();
		peers.insert(
			node(2),
			peer(
				"127.0.0.1:9002",
				TransportTierSet::empty()
					.with(TransportTier::QuicIoUring)
					.with(TransportTier::TcpTls),
			),
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let local = TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls);
		let (tx, rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn CapabilityResolver> =
			Arc::new(EpochCapabilityResolver::new(rx.clone()));
		let quic = MockProvider::new(TransportTier::QuicIoUring);
		quic.fail_times(1);
		let tcp = MockProvider::new(TransportTier::TcpTls);
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, quic.clone());
		providers.insert(TransportTier::TcpTls, tcp.clone());
		let factory = TransportProviderFactory::new(local, providers, resolver, rx);

		let conn = factory.get_connection(node(2)).await.unwrap();
		assert_eq!(conn.transport_type(), TransportTier::TcpTls);
		assert_eq!(quic.dial_count.load(Ordering::SeqCst), 1);
		assert_eq!(tcp.dial_count.load(Ordering::SeqCst), 1);
		drop(tx);
	}

	#[tokio::test]
	async fn degraded_tier_is_skipped_until_cooldown_expires() {
		let mut peers = HashMap::new();
		peers.insert(
			node(2),
			peer(
				"127.0.0.1:9002",
				TransportTierSet::empty()
					.with(TransportTier::QuicIoUring)
					.with(TransportTier::TcpTls),
			),
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let local = TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls);
		let (tx, rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn CapabilityResolver> =
			Arc::new(EpochCapabilityResolver::new(rx.clone()));
		let quic = MockProvider::new(TransportTier::QuicIoUring);
		quic.fail_times(1);
		let tcp = MockProvider::new(TransportTier::TcpTls);
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, quic.clone());
		providers.insert(TransportTier::TcpTls, tcp.clone());
		let factory = TransportProviderFactory::new_with_policy(
			local,
			providers,
			resolver,
			rx,
			TransportSelectionPolicy {
				degrade_after_failures: 1,
				cooldown: Duration::from_secs(300),
			},
		);

		let first = factory.get_connection(node(2)).await.unwrap();
		assert_eq!(first.transport_type(), TransportTier::TcpTls);
		tcp.last_dialed().closed.store(true, Ordering::SeqCst);

		let second = factory.get_connection(node(2)).await.unwrap();
		assert_eq!(second.transport_type(), TransportTier::TcpTls);
		assert_eq!(quic.dial_count.load(Ordering::SeqCst), 1);
		assert_eq!(tcp.dial_count.load(Ordering::SeqCst), 2);
		drop(tx);
	}

	#[tokio::test]
	async fn apply_epoch_evicts_departed_peer() {
		let mut peers = HashMap::new();
		peers.insert(
			node(7),
			peer(
				"127.0.0.1:9001",
				TransportTierSet::empty().with(TransportTier::QuicIoUring),
			),
		);
		let epoch = MembershipEpoch {
			epoch: 1,
			peers: peers.clone(),
		};
		let local = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let (factory, _provider, _tx) = make_factory(local, TransportTier::QuicIoUring, epoch);

		factory.get_connection(node(7)).await.unwrap();
		assert_eq!(factory.cached_connection_count(), 1);

		// Node 7 has left the ring.
		let new_epoch = MembershipEpoch {
			epoch: 2,
			peers: HashMap::new(),
		};
		factory.apply_epoch(&new_epoch);
		assert_eq!(factory.cached_connection_count(), 0);
	}

	#[tokio::test]
	async fn apply_epoch_evicts_when_tier_revoked() {
		let mut peers = HashMap::new();
		peers.insert(
			node(7),
			peer(
				"127.0.0.1:9001",
				TransportTierSet::empty().with(TransportTier::QuicIoUring),
			),
		);
		let epoch = MembershipEpoch {
			epoch: 1,
			peers: peers.clone(),
		};
		let local = TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls);
		let (factory, _provider, _tx) = make_factory(local, TransportTier::QuicIoUring, epoch);

		factory.get_connection(node(7)).await.unwrap();

		// Peer swaps from QUIC → TCP. Our cached QUIC session is
		// no longer valid; it must be evicted.
		let mut downgraded = HashMap::new();
		downgraded.insert(
			node(7),
			peer(
				"127.0.0.1:9001",
				TransportTierSet::empty().with(TransportTier::TcpTls),
			),
		);
		let new_epoch = MembershipEpoch {
			epoch: 2,
			peers: downgraded,
		};
		factory.apply_epoch(&new_epoch);
		assert_eq!(factory.cached_connection_count(), 0);
	}
}
