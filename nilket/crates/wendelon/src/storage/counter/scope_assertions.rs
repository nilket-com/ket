//! Identical fixed semantic assertions shared with the external unmodified-f2 adapter.
//! Adapters translate APIs only; neither adapter decides expected results.
#[derive(Clone, Copy)]
pub(crate) struct Scope {
	pub(crate) tenant: u64,
	pub(crate) origin: u32,
}
pub(crate) trait ScopeAdapter {
	async fn tentative(&self, scope: Scope, op: u64, key: &[u8], value: u64, stamp: u64);
	async fn commit(&self, scope: Scope, op: u64, key: &[u8]);
	async fn rollback(&self, scope: Scope, op: u64, key: &[u8]);
	async fn read(&self, scope: Scope, key: &[u8]) -> Option<u64>;
}
async fn independent(adapter: &impl ScopeAdapter, a: Scope, b: Scope) {
	let ka = b"scope-a";
	let kb = b"scope-b";
	adapter.tentative(a, 1, ka, 11, 1).await;
	adapter.tentative(b, 1, kb, 22, 1).await;
	assert_eq!(
		adapter.read(a, ka).await,
		Some(11),
		"A tentative remains visible"
	);
	assert_eq!(
		adapter.read(b, kb).await,
		Some(22),
		"B independent tentative must append despite colliding op1"
	);
	adapter.commit(a, 1, ka).await;
	assert_eq!(
		adapter.read(b, kb).await,
		Some(22),
		"A commit leaves B intact"
	);
	adapter.commit(b, 1, kb).await;
	adapter.tentative(a, 2, ka, 33, 2).await;
	adapter.tentative(b, 2, kb, 44, 2).await;
	adapter.rollback(a, 2, ka).await;
	assert_eq!(
		adapter.read(a, ka).await,
		Some(11),
		"A rollback restores its committed value"
	);
	assert_eq!(
		adapter.read(b, kb).await,
		Some(44),
		"A rollback cannot unindex B"
	);
	adapter.commit(b, 2, kb).await;
	assert_eq!(adapter.read(a, ka).await, Some(11));
	assert_eq!(adapter.read(b, kb).await, Some(44));
}
pub(crate) async fn two_tenants(adapter: &impl ScopeAdapter) {
	independent(
		adapter,
		Scope {
			tenant: 7,
			origin: 1,
		},
		Scope {
			tenant: 8,
			origin: 1,
		},
	)
	.await;
}
pub(crate) async fn two_origins(adapter: &impl ScopeAdapter) {
	independent(
		adapter,
		Scope {
			tenant: 7,
			origin: 1,
		},
		Scope {
			tenant: 7,
			origin: 2,
		},
	)
	.await;
}
