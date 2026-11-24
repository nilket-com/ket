//! Wendelon ADR 0129: a tenant applied on a member after that member's last tenant policy
//! refresh is admitted from the member's applied state, inside the refresh interval.
//! - The order is staged and witnessed, not timed: the periodic refresh is stopped, the cache
//!   is refreshed, the tenant is created, its creation is read back as applied on this member,
//!   and only then is the request issued, with the cache still fresh and still without it.
//! - Reads and writes share the lookup: the point-read handler and a table write are both
//!   admitted, and the write's local charge lands on the admitted entry.
//! - A tenant this member has not applied is still refused, keeps nothing in the cache and
//!   causes no refresh. Suspended still reads and refuses writes as suspended.
//! - A refresh from an earlier applied state does not replace a later entry or its charge.
//! - Two requests around one miss, in both orders: a miss that lands after a charged write keeps
//!   the charge (the quota it filled refuses the next write), and a miss that reads an applied
//!   state already holding that write counts it once (a write the quota has room for is
//!   admitted), all before any refresh.
use crate::consensus::WendelonRequest;
use crate::consensus::core::TenantLifecycleState;
use crate::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use crate::network::data_server::recover_rwlock_read;
use crate::network::data_server::recover_rwlock_write;
use crate::network::data_server::tests::{
	append_only_table_descriptor, bootstrap_tempdir, shutdown_lease_node, simple_read_meta,
	single_key_batch, typed_table_batch, typed_table_meta,
};
use crate::network::traits::DataRing;
use crate::node::{NodeBootstrapConfig, WendelonNode};
use crate::schema::{
	Consistency as SchemaConsistency, Identity, ReadStatus, TableSchemaDescriptor, TenantId,
	WriteStatus, encode_table_schema_descriptor_v1,
};
use std::time::{Duration, Instant};

/// A single-node store whose periodic tenant policy refresh is stopped: the cache then
/// refreshes only when a request finds it older than the interval.
async fn node(id: u32, tag: &str) -> (crate::test_support::TestDir, WendelonNode) {
	let dir = bootstrap_tempdir(tag);
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		id,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	while node.raft.current_leader().await != Some(node.local_node.id) {
		assert!(
			tokio::time::Instant::now() < deadline,
			"leader wait expired"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	node.handles.tenant_policy_reconciler.abort();
	(dir, node)
}

async fn create_table(
	node: &WendelonNode,
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptor,
) {
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new([tenant_id.get() as u8; 16]),
		mutation: SchemaMutationKind::Create,
		payload: encode_table_schema_descriptor_v1(descriptor).unwrap(),
	};
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			encode_schema_update_envelope_v1(&envelope).unwrap(),
		))
		.await
		.unwrap();
}

/// The cache's last refresh and whether it holds `tenant`.
fn cache(node: &WendelonNode, tenant: TenantId) -> (Instant, bool) {
	let guard = recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache");
	(
		guard.last_refresh.expect("refreshed"),
		guard.policies.contains_key(&tenant),
	)
}

async fn applied(node: &WendelonNode, tenant: TenantId) -> Option<TenantLifecycleState> {
	node.state_machine
		.read_state(move |state| state.tenants.get(&tenant).map(|record| record.state))
		.await
}

/// Stages the order under test and returns the tenant and the refresh it was staged against:
/// the cache is refreshed; then a new tenant (with its table when `descriptor` is given) is
/// created; then its creation is witnessed applied on this member; and the cache is still
/// that refresh, without the tenant, with more than half its interval left. A slow machine
/// retries with a new tenant rather than weakening the premise.
async fn staged(
	node: &WendelonNode,
	first: u64,
	descriptor: Option<&TableSchemaDescriptor>,
	then: impl AsyncFn(&WendelonNode, TenantId),
) -> (TenantId, Instant) {
	let interval = node.data_server.tenant_cache_refresh_interval;
	for attempt in 0..40 {
		let tenant = TenantId::new(first + attempt);
		node.data_server.sync_tenant_policy_cache_once().await;
		node.create_tenant(tenant, u64::MAX, 1).await.unwrap();
		if let Some(descriptor) = descriptor {
			create_table(node, tenant, descriptor).await;
		}
		then(node, tenant).await;
		assert!(
			applied(node, tenant).await.is_some(),
			"witness: applied here"
		);
		let (refreshed, cached) = cache(node, tenant);
		assert!(
			!cached,
			"premise: nothing refreshed the cache after the creation"
		);
		if refreshed.elapsed() < interval / 2 {
			return (tenant, refreshed);
		}
	}
	panic!("the premise was never staged inside the interval");
}

fn read_meta(tenant: TenantId) -> crate::schema::ReadRequestMeta {
	let mut meta = simple_read_meta(1);
	meta.tenant_id = tenant;
	meta.key_identity = Identity::blake3_tenant(tenant, b"k");
	meta.consistency = SchemaConsistency::SERIALIZABLE;
	meta
}

#[tokio::test]
async fn a_read_for_a_tenant_applied_after_the_last_refresh_is_admitted_inside_the_interval() {
	let (_dir, node) = node(49, "tenant-policy-miss-read").await;
	let interval = node.data_server.tenant_cache_refresh_interval;
	let (tenant, refreshed) = staged(&node, 200, None, async |_, _| {}).await;
	assert_eq!(
		applied(&node, tenant).await,
		Some(TenantLifecycleState::Active)
	);
	let (response, _) = node
		.data_server
		.read(read_meta(tenant), single_key_batch())
		.await
		.unwrap();
	let (after, cached) = cache(&node, tenant);
	eprintln!(
		"ADR0129 read: {tenant:?} applied Active, cache refreshed {:?} before the read (interval {interval:?}) without it -> {:?}; refreshed again {}, entry now cached {cached}",
		refreshed.elapsed(),
		response.status,
		after != refreshed
	);
	assert_eq!(after, refreshed, "premise: the answer came with no refresh");
	assert_ne!(
		response.status,
		ReadStatus::TENANT_UNAVAILABLE,
		"a tenant this member has applied is not unavailable on it"
	);
	assert!(cached, "the admitted entry answers later calls");
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn a_write_for_such_a_tenant_is_admitted_and_its_charge_lands_on_the_entry() {
	let (_dir, node) = node(50, "tenant-policy-miss-write").await;
	let descriptor = append_only_table_descriptor();
	let (tenant, refreshed) = staged(&node, 300, Some(&descriptor), async |_, _| {}).await;
	let mut meta = typed_table_meta(&descriptor, 1);
	meta.tenant_id = tenant;
	meta.parent_write_id.sequence_no = 129_001;
	let (response, _) = node
		.data_server
		.table_write(meta, typed_table_batch(vec![7]))
		.await
		.unwrap();
	let entry = recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache")
		.policies
		.get(&tenant)
		.copied();
	let applied_used = node
		.state_machine
		.read_state(move |state| state.tenants.get(&tenant).map(|record| record.used_bytes))
		.await;
	eprintln!(
		"ADR0129 write: {tenant:?} -> {:?}; entry {entry:?}; applied used_bytes {applied_used:?}; cache refreshed again {}",
		response.write.status,
		cache(&node, tenant).0 != refreshed
	);
	assert_eq!(
		response.write.status,
		WriteStatus::OK,
		"a tenant this member has applied takes its write"
	);
	let entry = entry.expect("the admitted entry");
	if cache(&node, tenant).0 == refreshed {
		// No refresh replaced the entry: it carries this write's anticipated bytes and size.
		assert!(
			entry.used_bytes > 0 && entry.mean_write_size_bytes > 1,
			"the committed write's charge lands on the admitted entry: {entry:?}"
		);
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn a_tenant_this_member_has_not_applied_is_refused_and_leaves_nothing() {
	let (_dir, node) = node(51, "tenant-policy-miss-unknown").await;
	let mut witnessed = 0;
	for round in 0..20u64 {
		node.data_server.sync_tenant_policy_cache_once().await;
		let before = {
			let guard =
				recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache");
			(guard.last_refresh, guard.policies.len())
		};
		for unknown in 0..50u64 {
			let tenant = TenantId::new(900_000 + round * 100 + unknown);
			assert_eq!(applied(&node, tenant).await, None);
			let (response, _) = node
				.data_server
				.read(read_meta(tenant), single_key_batch())
				.await
				.unwrap();
			assert_eq!(response.status, ReadStatus::TENANT_UNAVAILABLE);
		}
		let after = {
			let guard =
				recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache");
			assert!(
				guard.policies.keys().all(|tenant| tenant.get() < 900_000),
				"no entry is kept for a tenant that does not exist"
			);
			(guard.last_refresh, guard.policies.len())
		};
		assert_eq!(after.1, before.1, "the cache holds what it held");
		if after.0 == before.0 {
			// Fifty misses for fifty distinct unknown ids, and the cache was not rebuilt.
			witnessed += 1;
		}
	}
	eprintln!(
		"ADR0129 unknown ids: 20 rounds of 50 distinct ids, {witnessed} rounds with no refresh at all"
	);
	assert!(
		witnessed > 0,
		"a stream of unknown ids does not rebuild the cache"
	);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn a_suspended_tenant_admitted_on_a_miss_reads_and_refuses_writes_as_suspended() {
	let (_dir, node) = node(52, "tenant-policy-miss-suspended").await;
	let descriptor = append_only_table_descriptor();
	let (tenant, refreshed) = staged(&node, 400, Some(&descriptor), async |node, tenant| {
		node.suspend_tenant(tenant).await.unwrap();
	})
	.await;
	assert_eq!(
		applied(&node, tenant).await,
		Some(TenantLifecycleState::Suspended)
	);
	let (read, _) = node
		.data_server
		.read(read_meta(tenant), single_key_batch())
		.await
		.unwrap();
	let mut meta = typed_table_meta(&descriptor, 1);
	meta.tenant_id = tenant;
	meta.parent_write_id.sequence_no = 129_002;
	let (write, _) = node
		.data_server
		.table_write(meta, typed_table_batch(vec![8]))
		.await
		.unwrap();
	eprintln!(
		"ADR0129 suspended: read {:?}, write {:?}; cache refreshed again {}",
		read.status,
		write.write.status,
		cache(&node, tenant).0 != refreshed
	);
	assert_ne!(read.status, ReadStatus::TENANT_UNAVAILABLE);
	assert_eq!(write.write.status, WriteStatus::TENANT_SUSPENDED);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn a_refresh_from_an_earlier_applied_state_keeps_a_later_entry_and_its_charge() {
	let (_dir, node) = node(53, "tenant-policy-miss-refresh").await;
	let (tenant, _) = staged(&node, 500, None, async |_, _| {}).await;
	node.data_server
		.read(read_meta(tenant), single_key_batch())
		.await
		.unwrap();
	// The entry as a miss admits it, then marked as read at a later applied index than any
	// refresh's snapshot can have, with a local charge on it.
	let admitted = {
		let mut guard =
			recover_rwlock_write(&node.data_server.tenant_policy_cache, "tenant_policy_cache");
		let entry = guard.policies.get_mut(&tenant).expect("admitted");
		let admitted = *entry;
		entry.applied_index = Some(u64::MAX);
		entry.used_bytes = 4_096;
		entry.mean_write_size_bytes = 512;
		admitted
	};
	assert!(
		admitted.applied_index.is_some(),
		"an entry records its index"
	);
	node.data_server.sync_tenant_policy_cache_once().await;
	let kept = *recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache")
		.policies
		.get(&tenant)
		.unwrap();
	assert_eq!(
		(
			kept.applied_index,
			kept.used_bytes,
			kept.mean_write_size_bytes
		),
		(Some(u64::MAX), 4_096, 512),
		"an earlier snapshot does not replace the later entry"
	);
	// The same entry at the refresh's own index or earlier is the refresh's to replace, with
	// its mean write size carried as before.
	recover_rwlock_write(&node.data_server.tenant_policy_cache, "tenant_policy_cache")
		.policies
		.get_mut(&tenant)
		.unwrap()
		.applied_index = admitted.applied_index;
	node.data_server.sync_tenant_policy_cache_once().await;
	let replaced =
		*recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache")
			.policies
			.get(&tenant)
			.unwrap();
	eprintln!("ADR0129 refresh: admitted {admitted:?} kept {kept:?} replaced {replaced:?}");
	assert_eq!(
		(replaced.used_bytes, replaced.mean_write_size_bytes),
		(0, 512),
		"the applied state's bytes, the entry's mean write size"
	);
	assert!(replaced.applied_index >= admitted.applied_index);
	shutdown_lease_node(node).await;
}

/// One staging of two requests around a miss for one tenant, on a member whose cache was
/// refreshed before the tenant existed. `quota` is the tenant's; `first_held` is the request
/// that misses first and is held: `Held::Write` holds a write and then a read behind an
/// unrelated apply, and resumes the write first; `Held::Read` holds a read, then lets a write
/// run to its commit before the read resumes. Returns `None` when a refresh intervened (a slow
/// machine), so the caller restages; otherwise the entry after the write, the entry after the
/// read, the read's status and the tenant with a write meta to issue more writes from.
#[derive(Clone, Copy, PartialEq)]
enum Held {
	Write,
	Read,
}

async fn two_requests_around_a_miss(
	node: &WendelonNode,
	descriptor: &TableSchemaDescriptor,
	attempt: u64,
	quota: u64,
	first_held: Held,
) -> Option<(
	super::super::tenant_policy::CachedTenantPolicy,
	super::super::tenant_policy::CachedTenantPolicy,
	ReadStatus,
	TenantId,
	crate::schema::TableWriteRequestMeta,
	Instant,
)> {
	use crate::network::data_server::tenant_policy::miss_pause;
	let entry_of = |tenant: TenantId| {
		recover_rwlock_read(&node.data_server.tenant_policy_cache, "tenant_policy_cache")
			.policies
			.get(&tenant)
			.copied()
	};
	let (tenant, other) = (TenantId::new(600 + attempt), TenantId::new(700 + attempt));
	node.data_server.sync_tenant_policy_cache_once().await;
	node.create_tenant(tenant, quota, 1).await.unwrap();
	create_table(node, tenant, descriptor).await;
	assert!(applied(node, tenant).await.is_some(), "witness: applied");
	let (refreshed, cached) = cache(node, tenant);
	assert!(!cached, "premise: the cache has not seen the tenant");

	let mut arrived = miss_pause::arm(node.local_node.id, tenant);
	let mut meta = typed_table_meta(descriptor, 1);
	meta.tenant_id = tenant;
	meta.parent_write_id.sequence_no = 129_100 + attempt;
	let write = |server: std::sync::Arc<crate::network::data_server::WendelonDataServer>| {
		tokio::spawn(async move { server.table_write(meta, typed_table_batch(vec![1])).await })
	};
	let read = |server: std::sync::Arc<crate::network::data_server::WendelonDataServer>| {
		tokio::spawn(async move { server.read(read_meta(tenant), single_key_batch()).await })
	};
	let (a, resume_write, b, resume_read) = match first_held {
		Held::Write => {
			let a = write(node.data_server.clone());
			let resume_write = arrived.recv().await.expect("the write missed and is held");
			// An unrelated apply moves the member's applied index on.
			node.create_tenant(other, u64::MAX, 1).await.unwrap();
			let b = read(node.data_server.clone());
			let resume_read = arrived.recv().await.expect("the read missed and is held");
			(a, resume_write, b, resume_read)
		}
		Held::Read => {
			let b = read(node.data_server.clone());
			let resume_read = arrived.recv().await.expect("the read missed and is held");
			let a = write(node.data_server.clone());
			let resume_write = arrived.recv().await.expect("the write missed and is held");
			(a, resume_write, b, resume_read)
		}
	};
	miss_pause::disarm();
	assert_eq!(
		entry_of(tenant),
		None,
		"premise: neither has placed an entry"
	);

	resume_write.send(()).unwrap();
	let (written, _) = a.await.unwrap().unwrap();
	assert_eq!(written.write.status, WriteStatus::OK, "the held write");
	let charged = entry_of(tenant).expect("the write's entry");
	let applied_used = node
		.state_machine
		.read_state(move |state| state.tenants.get(&tenant).map(|record| record.used_bytes))
		.await
		.unwrap();
	assert!(
		charged.used_bytes > 0 && applied_used == charged.used_bytes,
		"witness: the write is charged in the cache and applied: {charged:?}, applied {applied_used}"
	);

	resume_read.send(()).unwrap();
	let (response, _) = b.await.unwrap().unwrap();
	let after = entry_of(tenant).expect("the entry after the read");
	if cache(node, tenant).0 != refreshed {
		return None;
	}
	Some((charged, after, response.status, tenant, meta, refreshed))
}

/// Both requests miss before either reads the applied state: a write, then (after an unrelated
/// apply) a read. The write resumes, is admitted, commits and is charged; the read resumes and
/// meets the write's entry. The charge must survive it: the bytes and the mean write size are
/// the write's, and the quota the write filled refuses the next write, before any refresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_miss_that_lands_after_a_charged_write_keeps_the_charge() {
	let (_dir, node) = node(54, "tenant-policy-miss-charge").await;
	let descriptor = append_only_table_descriptor();
	for attempt in 0..40u64 {
		// A quota one write fills.
		let Some((charged, after, read, tenant, meta, refreshed)) =
			two_requests_around_a_miss(&node, &descriptor, attempt, 2_000, Held::Write).await
		else {
			continue;
		};
		let mut again = meta;
		again.parent_write_id.sequence_no = 129_200 + attempt;
		let (second, _) = node
			.data_server
			.table_write(again, typed_table_batch(vec![2]))
			.await
			.unwrap();
		if cache(&node, tenant).0 != refreshed {
			continue;
		}
		eprintln!(
			"ADR0129 charge kept: the write's entry {charged:?}; after the later miss {after:?}; read {read:?}; the next write {:?}; no refresh throughout",
			second.write.status
		);
		assert_ne!(read, ReadStatus::TENANT_UNAVAILABLE);
		assert_eq!(
			(after.used_bytes, after.mean_write_size_bytes),
			(charged.used_bytes, charged.mean_write_size_bytes),
			"the later miss keeps the charge and the mean write size"
		);
		assert_eq!(
			second.write.status,
			WriteStatus::QUOTA_EXCEEDED,
			"the quota the write filled refuses the next write"
		);
		shutdown_lease_node(node).await;
		return;
	}
	panic!("the order was never staged without a refresh");
}

/// The complementary order: a read misses and is held before it reads the applied state; a
/// write then misses, is admitted, commits and is charged, in the cache and in the applied
/// state; the read resumes and reads an applied state that already includes that write. The
/// charge must be counted once: with a quota that has room for a second write, the second
/// write is admitted, and the third, which the quota has no room for, is refused, all before
/// any refresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_miss_that_reads_an_already_applied_write_counts_its_charge_once() {
	let (_dir, node) = node(55, "tenant-policy-miss-once").await;
	let descriptor = append_only_table_descriptor();
	for attempt in 0..40u64 {
		// A quota with room for the first write and a second, and none for a third.
		let Some((charged, after, read, tenant, meta, refreshed)) =
			two_requests_around_a_miss(&node, &descriptor, attempt, 4_000, Held::Read).await
		else {
			continue;
		};
		let mut statuses = Vec::new();
		for extra in 1..=2u64 {
			let mut more = meta;
			more.parent_write_id.sequence_no = 129_300 + attempt * 10 + extra;
			let (response, _) = node
				.data_server
				.table_write(more, typed_table_batch(vec![extra as i64 + 1]))
				.await
				.unwrap();
			statuses.push(response.write.status);
		}
		if cache(&node, tenant).0 != refreshed {
			continue;
		}
		eprintln!(
			"ADR0129 charge once: the write's entry {charged:?}; after the miss that read it applied {after:?}; read {read:?}; the next two writes {statuses:?}; no refresh throughout"
		);
		assert_ne!(read, ReadStatus::TENANT_UNAVAILABLE);
		assert_eq!(
			after.used_bytes, charged.used_bytes,
			"the write's bytes are counted once"
		);
		assert_eq!(
			statuses,
			vec![WriteStatus::OK, WriteStatus::QUOTA_EXCEEDED],
			"a write the quota has room for is admitted; the one it has no room for is refused"
		);
		shutdown_lease_node(node).await;
		return;
	}
	panic!("the order was never staged without a refresh");
}
