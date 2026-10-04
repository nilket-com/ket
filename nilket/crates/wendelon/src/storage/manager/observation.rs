//! Counted storage-publication ownership, adjacent to the delegating entry point.
use super::*;
use crate::storage::finality_observation::{WorkKind, WorkScope, with_work};
use std::future::Future;

impl SlabManager {
	pub(crate) fn finality_observation_registry(
		&self,
	) -> Arc<crate::storage::finality_observation::FinalityObservationRegistry> {
		self.finality_observation.clone()
	}

	/// Append a record and force the destination slab's durability
	/// barrier before returning. The manager index is published only
	/// after the coalesced `fdatasync` succeeds, so callers that
	/// require durable visibility do not leak
	/// append-complete-but-unflushed writes through the in-memory
	/// read path.
	pub async fn append_durable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		self.append_durable_with_locality(
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			hlc,
			frame,
		)
		.await
	}

	// Normal function: registration occurs at the caller's existing entry point.
	pub(super) fn observe_publication<F: Future>(
		&self,
		tenant: TenantId,
		future: F,
	) -> impl Future<Output = F::Output> + use<F> {
		let token = self
			.finality_observation
			.begin(WorkScope::Tenant(tenant), WorkKind::StoragePublication);
		with_work(token, future)
	}
}
