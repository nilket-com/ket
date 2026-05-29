use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arrow::datatypes::SchemaRef;
use aws_lc_rs::aead::{AES_256_GCM_SIV, Aad, LessSafeKey, Nonce, UnboundKey};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};

use crate::distributed::arrow_ipc::{ArrowIpcError, reassemble_partition_chunks};
use crate::distributed::budget::{
	BudgetDimension, BudgetRejection, BudgetReservation, BudgetTracker, ByteCount, QueryBudget,
};
use crate::distributed::cancellation::{
	DownstreamCancellationContext, DownstreamCancellationTrigger,
};
use crate::distributed::identity::{AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId};
use crate::distributed::task::{
	ArrowIpcDigest, PartitionAddress, PartitionBackend, PartitionEntry, PartitionManifest, RowCount,
};
use crate::error::PartitionMissingReason;
use crate::server::spill::SpillId;

pub(crate) const LIVE_BACKLOG_DEPTH: usize = 4;

#[async_trait::async_trait]
pub trait PartitionStore: Send + Sync {
	fn publish(
		&self,
		manifest: PartitionManifest,
		payloads: Vec<PartitionPayload>,
	) -> Result<PartitionManifest, PartitionStoreError>;
	fn manifest(&self, key: ManifestKey) -> Option<PartitionManifest>;
	fn pull(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<PartitionPayload, PartitionMissingReason>;
	fn open_live(
		&self,
		address: PartitionAddress,
		schema: SchemaRef,
	) -> Result<(), PartitionStoreError> {
		let _ = (address, schema);
		Err(PartitionStoreError::LiveUnsupported)
	}
	fn open_live_with_downstream_cancellation(
		&self,
		address: PartitionAddress,
		schema: SchemaRef,
		downstream_cancellation: Option<DownstreamCancellationContext>,
	) -> Result<(), PartitionStoreError> {
		let _ = downstream_cancellation;
		self.open_live(address, schema)
	}
	async fn append_live(&self, payload: PartitionPayload) -> Result<(), PartitionStoreError> {
		let _ = payload;
		Err(PartitionStoreError::LiveUnsupported)
	}
	fn seal_live(
		&self,
		manifest: PartitionManifest,
	) -> Result<PartitionManifest, PartitionStoreError> {
		let _ = manifest;
		Err(PartitionStoreError::LiveUnsupported)
	}
	fn cancel_live(&self, address: PartitionAddress, reason: PartitionMissingReason) {
		let _ = (address, reason);
	}
	fn pull_live(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<LivePartitionStream, PartitionMissingReason> {
		let _ = (address, expected_attempt);
		Err(PartitionMissingReason::StoreEvicted)
	}
	fn remove_query(&self, query_id: QueryId);
	fn cancel_query(&self, query_id: QueryId);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ManifestKey {
	pub query_id: QueryId,
	pub exchange_id: ExchangeId,
	pub producer_stage_id: StageId,
	pub producer_task_id: TaskId,
	pub attempt_id: AttemptId,
}

impl ManifestKey {
	pub const fn new(
		query_id: QueryId,
		exchange_id: ExchangeId,
		producer_stage_id: StageId,
		producer_task_id: TaskId,
		attempt_id: AttemptId,
	) -> Self {
		Self {
			query_id,
			exchange_id,
			producer_stage_id,
			producer_task_id,
			attempt_id,
		}
	}

	pub const fn from_address(address: PartitionAddress) -> Self {
		Self {
			query_id: address.query_id,
			exchange_id: address.exchange_id,
			producer_stage_id: address.producer_stage_id,
			producer_task_id: address.producer_task_id,
			attempt_id: address.attempt_id,
		}
	}

	pub const fn from_manifest(manifest: &PartitionManifest) -> Self {
		Self {
			query_id: manifest.query_id,
			exchange_id: manifest.exchange_id,
			producer_stage_id: manifest.stage_id,
			producer_task_id: manifest.task_id,
			attempt_id: manifest.attempt_id,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPayload {
	pub address: PartitionAddress,
	pub bytes: Vec<u8>,
	pub row_count: RowCount,
	pub digest: ArrowIpcDigest,
}

impl PartitionPayload {
	pub fn new(
		address: PartitionAddress,
		bytes: Vec<u8>,
		row_count: RowCount,
		digest: ArrowIpcDigest,
	) -> Self {
		Self {
			address,
			bytes,
			row_count,
			digest,
		}
	}

	pub fn size_bytes(&self) -> ByteCount {
		ByteCount::from_usize(self.bytes.len())
	}

	pub fn entry(&self) -> PartitionEntry {
		PartitionEntry {
			partition_id: self.address.partition_id,
			size_bytes: self.size_bytes(),
			row_count: self.row_count,
			digest: self.digest,
		}
	}
}

pub struct LivePartitionStream {
	chunks: BoxStream<'static, Result<PartitionPayload, PartitionMissingReason>>,
	reader_guard: Option<LivePartitionReaderGuard>,
}

struct LivePartitionReaderGuard {
	inner: Arc<RwLock<PartitionStoreInner>>,
	address: PartitionAddress,
}

impl LivePartitionStream {
	fn without_reader_guard(
		chunks: BoxStream<'static, Result<PartitionPayload, PartitionMissingReason>>,
	) -> Self {
		Self {
			chunks,
			reader_guard: None,
		}
	}

	fn with_reader_guard(
		chunks: BoxStream<'static, Result<PartitionPayload, PartitionMissingReason>>,
		inner: Arc<RwLock<PartitionStoreInner>>,
		address: PartitionAddress,
	) -> Self {
		Self {
			chunks,
			reader_guard: Some(LivePartitionReaderGuard { inner, address }),
		}
	}

	pub async fn next(&mut self) -> Option<Result<PartitionPayload, PartitionMissingReason>> {
		let next = self.chunks.next().await;
		if next.is_none() || matches!(next, Some(Err(_))) {
			if let Some(guard) = self.reader_guard.take() {
				guard.finish(false);
			}
		}
		next
	}
}

impl LivePartitionReaderGuard {
	fn finish(self, cancel_if_unsealed: bool) {
		let downstream_cancellation = {
			let mut inner = self.inner.write().expect("partition store poisoned");
			let Some(live) = inner.live_partitions.get_mut(&self.address) else {
				return;
			};
			debug_assert!(live.active_readers > 0);
			live.active_readers = live.active_readers.saturating_sub(1);
			if cancel_if_unsealed && !live.sealed && live.cancelled.is_none() {
				live.cancelled = Some(PartitionMissingReason::CancelledTaskCleanup);
				live.notify.notify_waiters();
				live.pacing_semaphore.close();
				live.downstream_cancellation.clone()
			} else {
				release_live_chunks_if_idle(live);
				None
			}
		};
		if let Some(downstream_cancellation) = downstream_cancellation {
			downstream_cancellation
				.cancel_registered_attempts(DownstreamCancellationTrigger::WorkerCancelTask);
		}
	}
}

impl Drop for LivePartitionStream {
	fn drop(&mut self) {
		if let Some(guard) = self.reader_guard.take() {
			guard.finish(true);
		}
	}
}

#[derive(Debug, Error)]
pub enum PartitionStoreError {
	#[error("query has been cancelled: {query_id:?}")]
	CancelledQuery { query_id: QueryId },
	#[error("partition payload address does not belong to manifest")]
	AddressManifestMismatch,
	#[error("partition payload is absent from manifest: {partition_id:?}")]
	PartitionAbsentFromManifest { partition_id: PartitionId },
	#[error("duplicate partition address: {address:?}")]
	DuplicatePartition { address: PartitionAddress },
	#[error(
		"partition store budget exhausted for {dimension:?}: requested {requested:?}, available {available:?}"
	)]
	BudgetExhausted {
		dimension: BudgetDimension,
		requested: ByteCount,
		available: ByteCount,
	},
	#[error("partition store IO error: {0}")]
	Io(#[from] std::io::Error),
	#[error("partition store random generation failed during {operation}: {message}")]
	Random {
		operation: &'static str,
		message: String,
	},
	#[error("partition store encryption failed during {operation}: {message}")]
	Crypto {
		operation: &'static str,
		message: String,
	},
	#[error("partition store Arrow IPC error during {operation}: {message}")]
	ArrowIpc {
		operation: &'static str,
		message: String,
	},
	#[error("live partition lifecycle is unsupported by this partition store")]
	LiveUnsupported,
}

impl From<BudgetRejection> for PartitionStoreError {
	fn from(rejection: BudgetRejection) -> Self {
		Self::BudgetExhausted {
			dimension: rejection.dimension,
			requested: rejection.requested,
			available: rejection.available,
		}
	}
}

impl From<ArrowIpcError> for PartitionStoreError {
	fn from(error: ArrowIpcError) -> Self {
		Self::ArrowIpc {
			operation: "live-seal-reassembly",
			message: error.to_string(),
		}
	}
}

#[derive(Debug)]
pub struct InMemoryPartitionStore {
	inner: Arc<RwLock<PartitionStoreInner>>,
	budget: BudgetTracker,
}

#[derive(Debug, Default)]
struct PartitionStoreInner {
	manifests: HashMap<ManifestKey, PartitionManifest>,
	partitions: HashMap<PartitionAddress, PartitionPayload>,
	reservations: HashMap<PartitionAddress, BudgetReservation>,
	live_partitions: HashMap<PartitionAddress, LivePartition>,
	cancelled_queries: HashSet<QueryId>,
}

#[derive(Debug)]
struct LivePartition {
	opened: bool,
	schema: Option<SchemaRef>,
	chunks: Vec<PartitionPayload>,
	reservations: Vec<BudgetReservation>,
	sealed: bool,
	cancelled: Option<PartitionMissingReason>,
	downstream_cancellation: Option<DownstreamCancellationContext>,
	notify: Arc<Notify>,
	pacing_semaphore: Arc<Semaphore>,
	active_readers: usize,
	farthest_released_index: usize,
}

impl LivePartition {
	fn placeholder() -> Self {
		Self {
			opened: false,
			schema: None,
			chunks: Vec::new(),
			reservations: Vec::new(),
			sealed: false,
			cancelled: None,
			downstream_cancellation: None,
			notify: Arc::new(Notify::new()),
			pacing_semaphore: Arc::new(Semaphore::new(LIVE_BACKLOG_DEPTH)),
			active_readers: 0,
			farthest_released_index: 0,
		}
	}
}

fn release_live_chunks_if_idle(live: &mut LivePartition) {
	if live.sealed && live.active_readers == 0 {
		live.chunks.clear();
		live.reservations.clear();
	}
}

impl InMemoryPartitionStore {
	pub fn new() -> Self {
		Self::with_budget(QueryBudget::test_default())
	}

	pub fn with_budget(budget: QueryBudget) -> Self {
		Self {
			inner: Arc::new(RwLock::new(PartitionStoreInner::default())),
			budget: BudgetTracker::new(budget),
		}
	}

	pub fn budget_used(&self, dimension: BudgetDimension) -> ByteCount {
		self.budget.used(dimension)
	}
}

impl Default for InMemoryPartitionStore {
	fn default() -> Self {
		Self::new()
	}
}

#[async_trait::async_trait]
impl PartitionStore for InMemoryPartitionStore {
	fn publish(
		&self,
		manifest: PartitionManifest,
		payloads: Vec<PartitionPayload>,
	) -> Result<PartitionManifest, PartitionStoreError> {
		let key = ManifestKey::from_manifest(&manifest);
		validate_manifest_payloads(&manifest, &payloads)?;

		let mut inner = self.inner.write().expect("partition store poisoned");
		if inner.cancelled_queries.contains(&manifest.query_id) {
			return Err(PartitionStoreError::CancelledQuery {
				query_id: manifest.query_id,
			});
		}
		for payload in &payloads {
			if inner.partitions.contains_key(&payload.address) {
				return Err(PartitionStoreError::DuplicatePartition {
					address: payload.address,
				});
			}
		}
		let mut reservations = Vec::with_capacity(payloads.len());
		for payload in &payloads {
			reservations.push((
				payload.address,
				self.budget
					.reserve(BudgetDimension::ShuffleBytes, payload.size_bytes())?,
			));
		}
		for payload in payloads {
			inner.partitions.insert(payload.address, payload);
		}
		for (address, reservation) in reservations {
			inner.reservations.insert(address, reservation);
		}
		inner.manifests.insert(key, manifest.clone());
		Ok(manifest)
	}

	fn manifest(&self, key: ManifestKey) -> Option<PartitionManifest> {
		self.inner
			.read()
			.expect("partition store poisoned")
			.manifests
			.get(&key)
			.cloned()
	}

	fn pull(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<PartitionPayload, PartitionMissingReason> {
		if address.attempt_id != expected_attempt {
			return Err(PartitionMissingReason::AttemptStale {
				expected_attempt,
				observed_attempt: address.attempt_id,
			});
		}

		let inner = self.inner.read().expect("partition store poisoned");
		if inner.cancelled_queries.contains(&address.query_id) {
			return Err(PartitionMissingReason::CancelledTaskCleanup);
		}
		if let Some(payload) = inner.partitions.get(&address) {
			return Ok(payload.clone());
		}

		if let Some(observed_attempt) = inner
			.partitions
			.keys()
			.find(|candidate| same_logical_partition(**candidate, address))
			.map(|candidate| candidate.attempt_id)
		{
			return Err(PartitionMissingReason::AttemptStale {
				expected_attempt,
				observed_attempt,
			});
		}

		Err(PartitionMissingReason::StoreEvicted)
	}

	fn open_live(
		&self,
		address: PartitionAddress,
		schema: SchemaRef,
	) -> Result<(), PartitionStoreError> {
		self.open_live_with_downstream_cancellation(address, schema, None)
	}

	fn open_live_with_downstream_cancellation(
		&self,
		address: PartitionAddress,
		schema: SchemaRef,
		downstream_cancellation: Option<DownstreamCancellationContext>,
	) -> Result<(), PartitionStoreError> {
		let mut inner = self.inner.write().expect("partition store poisoned");
		if inner.cancelled_queries.contains(&address.query_id) {
			return Err(PartitionStoreError::CancelledQuery {
				query_id: address.query_id,
			});
		}
		if inner.partitions.contains_key(&address) {
			return Err(PartitionStoreError::DuplicatePartition { address });
		}
		let live = inner
			.live_partitions
			.entry(address)
			.or_insert_with(LivePartition::placeholder);
		live.opened = true;
		live.schema = Some(schema);
		if downstream_cancellation.is_some() {
			live.downstream_cancellation = downstream_cancellation;
		}
		live.notify.notify_waiters();
		Ok(())
	}

	async fn append_live(&self, payload: PartitionPayload) -> Result<(), PartitionStoreError> {
		let semaphore = {
			let inner = self.inner.read().expect("partition store poisoned");
			if inner.cancelled_queries.contains(&payload.address.query_id) {
				return Err(PartitionStoreError::CancelledQuery {
					query_id: payload.address.query_id,
				});
			}
			match inner.live_partitions.get(&payload.address) {
				Some(live) if live.cancelled.is_some() || live.sealed => {
					return Err(PartitionStoreError::CancelledQuery {
						query_id: payload.address.query_id,
					});
				}
				Some(live) => live.pacing_semaphore.clone(),
				None => {
					drop(inner);
					let mut inner = self.inner.write().expect("partition store poisoned");
					if inner.cancelled_queries.contains(&payload.address.query_id) {
						return Err(PartitionStoreError::CancelledQuery {
							query_id: payload.address.query_id,
						});
					}
					let live = inner
						.live_partitions
						.entry(payload.address)
						.or_insert_with(LivePartition::placeholder);
					if live.cancelled.is_some() || live.sealed {
						return Err(PartitionStoreError::CancelledQuery {
							query_id: payload.address.query_id,
						});
					}
					live.pacing_semaphore.clone()
				}
			}
		};

		let permit =
			semaphore
				.acquire_owned()
				.await
				.map_err(|_| PartitionStoreError::CancelledQuery {
					query_id: payload.address.query_id,
				})?;
		let reservation = self
			.budget
			.reserve(BudgetDimension::ShuffleBytes, payload.size_bytes())?;

		let mut inner = self.inner.write().expect("partition store poisoned");
		if inner.cancelled_queries.contains(&payload.address.query_id) {
			return Err(PartitionStoreError::CancelledQuery {
				query_id: payload.address.query_id,
			});
		}
		let live = inner
			.live_partitions
			.entry(payload.address)
			.or_insert_with(LivePartition::placeholder);
		if live.cancelled.is_some() || live.sealed {
			return Err(PartitionStoreError::CancelledQuery {
				query_id: payload.address.query_id,
			});
		}
		live.opened = true;
		live.chunks.push(payload);
		live.reservations.push(reservation);
		live.notify.notify_waiters();
		permit.forget();
		Ok(())
	}

	fn seal_live(
		&self,
		mut manifest: PartitionManifest,
	) -> Result<PartitionManifest, PartitionStoreError> {
		enum SealInput {
			Published(PartitionAddress, PartitionPayload),
			Chunks {
				address: PartitionAddress,
				schema: Option<SchemaRef>,
				chunks: Vec<PartitionPayload>,
			},
		}

		let key = ManifestKey::from_manifest(&manifest);

		let inputs = {
			let inner = self.inner.read().expect("partition store poisoned");
			if inner.cancelled_queries.contains(&manifest.query_id) {
				return Err(PartitionStoreError::CancelledQuery {
					query_id: manifest.query_id,
				});
			}

			let mut inputs = Vec::new();
			for entry in &manifest.partitions {
				let partition_id = entry.partition_id;
				let address = PartitionAddress {
					query_id: manifest.query_id,
					exchange_id: manifest.exchange_id,
					producer_stage_id: manifest.stage_id,
					producer_task_id: manifest.task_id,
					attempt_id: manifest.attempt_id,
					partition_id,
				};
				let published_payload = inner.partitions.get(&address).cloned();
				let Some(live) = inner.live_partitions.get(&address) else {
					continue;
				};
				if let Some(payload) = published_payload {
					inputs.push(SealInput::Published(address, payload));
					continue;
				}
				inputs.push(SealInput::Chunks {
					address,
					schema: live.schema.clone(),
					chunks: live.chunks.clone(),
				});
			}
			inputs
		};

		let mut sealed_payloads = Vec::new();
		for input in inputs {
			match input {
				SealInput::Published(address, payload) => {
					sealed_payloads.push((address, payload, None));
				}
				SealInput::Chunks {
					address,
					schema,
					chunks,
				} => {
					let (bytes, row_count) = reassemble_partition_chunks(schema, &chunks)?;
					let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
					let payload = PartitionPayload::new(address, bytes, row_count, digest);
					let reservation = self
						.budget
						.reserve(BudgetDimension::ShuffleBytes, payload.size_bytes())?;
					sealed_payloads.push((address, payload, Some(reservation)));
				}
			}
		}

		let mut inner = self.inner.write().expect("partition store poisoned");
		if inner.cancelled_queries.contains(&manifest.query_id) {
			return Err(PartitionStoreError::CancelledQuery {
				query_id: manifest.query_id,
			});
		}
		for (address, _, reservation) in &sealed_payloads {
			if reservation.is_some() && inner.partitions.contains_key(address) {
				return Err(PartitionStoreError::DuplicatePartition { address: *address });
			}
		}

		let mut entries = Vec::new();
		let mut sealed_notifies = Vec::new();
		for (address, payload, reservation) in sealed_payloads {
			let entry = payload.entry();
			if !inner.live_partitions.contains_key(&address) {
				continue;
			}
			entries.push(entry);
			if let Some(reservation) = reservation {
				inner.partitions.insert(address, payload);
				inner.reservations.insert(address, reservation);
			}
			let notify = {
				let live = inner
					.live_partitions
					.get_mut(&address)
					.expect("live partition checked above");
				live.sealed = true;
				live.pacing_semaphore.close();
				release_live_chunks_if_idle(live);
				live.notify.clone()
			};
			sealed_notifies.push(notify);
		}
		for notify in sealed_notifies {
			notify.notify_waiters();
		}
		if !entries.is_empty() {
			manifest.partitions = entries;
		}
		inner.manifests.insert(key, manifest.clone());
		Ok(manifest)
	}

	fn cancel_live(&self, address: PartitionAddress, reason: PartitionMissingReason) {
		let mut inner = self.inner.write().expect("partition store poisoned");
		let live = inner
			.live_partitions
			.entry(address)
			.or_insert_with(LivePartition::placeholder);
		live.cancelled = Some(reason);
		live.notify.notify_waiters();
		live.pacing_semaphore.close();
	}

	fn pull_live(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<LivePartitionStream, PartitionMissingReason> {
		if address.attempt_id != expected_attempt {
			return Err(PartitionMissingReason::AttemptStale {
				expected_attempt,
				observed_attempt: address.attempt_id,
			});
		}
		let inner = self.inner.clone();
		let fallback = {
			let mut guard = inner.write().expect("partition store poisoned");
			if guard.cancelled_queries.contains(&address.query_id) {
				return Err(PartitionMissingReason::CancelledTaskCleanup);
			}
			let use_fallback = {
				let live = guard
					.live_partitions
					.entry(address)
					.or_insert_with(LivePartition::placeholder);
				if live.cancelled.is_none() && live.sealed && live.chunks.is_empty() {
					true
				} else {
					live.active_readers += 1;
					false
				}
			};
			if use_fallback {
				Some(
					guard
						.partitions
						.get(&address)
						.cloned()
						.ok_or(PartitionMissingReason::StoreEvicted)?,
				)
			} else {
				None
			}
		};
		if let Some(payload) = fallback {
			return Ok(LivePartitionStream::without_reader_guard(
				stream::once(async move { Ok(payload) }).boxed(),
			));
		}
		Ok(LivePartitionStream::with_reader_guard(
			stream::unfold(
				(inner.clone(), address, 0usize),
				|(inner, address, index)| async move {
					loop {
						let notify = {
							let guard = inner.read().expect("partition store poisoned");
							if guard.cancelled_queries.contains(&address.query_id) {
								return Some((
									Err(PartitionMissingReason::CancelledTaskCleanup),
									(inner.clone(), address, index),
								));
							}
							let live = guard.live_partitions.get(&address)?;
							if let Some(reason) = &live.cancelled {
								return Some((
									Err(reason.clone()),
									(inner.clone(), address, index),
								));
							}
							if live.chunks.get(index).is_some() {
								drop(guard);
								let mut guard = inner.write().expect("partition store poisoned");
								let live = guard.live_partitions.get_mut(&address)?;
								if let Some(reason) = &live.cancelled {
									return Some((
										Err(reason.clone()),
										(inner.clone(), address, index),
									));
								}
								let chunk = live.chunks.get(index).cloned()?;
								if index == live.farthest_released_index {
									live.farthest_released_index += 1;
									live.pacing_semaphore.add_permits(1);
								}
								return Some((Ok(chunk), (inner.clone(), address, index + 1)));
							}
							if live.sealed && live.chunks.is_empty() {
								if index == 0 {
									let payload = guard
										.partitions
										.get(&address)
										.cloned()
										.ok_or(PartitionMissingReason::StoreEvicted);
									return Some((payload, (inner.clone(), address, 1)));
								}
								return None;
							}
							if live.sealed {
								return None;
							}
							live.notify.clone()
						};
						notify.notified().await;
					}
				},
			)
			.boxed(),
			inner,
			address,
		))
	}

	fn remove_query(&self, query_id: QueryId) {
		let mut inner = self.inner.write().expect("partition store poisoned");
		for live in inner
			.live_partitions
			.iter()
			.filter(|(address, _)| address.query_id == query_id)
			.map(|(_, live)| live)
		{
			live.pacing_semaphore.close();
		}
		inner.manifests.retain(|key, _| key.query_id != query_id);
		inner
			.partitions
			.retain(|address, _| address.query_id != query_id);
		inner
			.reservations
			.retain(|address, _| address.query_id != query_id);
		inner
			.live_partitions
			.retain(|address, _| address.query_id != query_id);
	}

	fn cancel_query(&self, query_id: QueryId) {
		let mut inner = self.inner.write().expect("partition store poisoned");
		inner.cancelled_queries.insert(query_id);
		inner.manifests.retain(|key, _| key.query_id != query_id);
		inner
			.partitions
			.retain(|address, _| address.query_id != query_id);
		inner
			.reservations
			.retain(|address, _| address.query_id != query_id);
		for live in inner
			.live_partitions
			.values_mut()
			.filter(|live| !live.sealed)
		{
			live.cancelled = Some(PartitionMissingReason::CancelledTaskCleanup);
			live.notify.notify_waiters();
			live.pacing_semaphore.close();
		}
	}
}

pub struct EncryptedSpillPartitionStore {
	inner: RwLock<EncryptedSpillInner>,
	budget: BudgetTracker,
	spill_dir: PathBuf,
	key: LessSafeKey,
	next_spill_id: AtomicU64,
}

#[derive(Debug, Default)]
struct EncryptedSpillInner {
	manifests: HashMap<ManifestKey, PartitionManifest>,
	partitions: HashMap<PartitionAddress, SpilledPartition>,
	reservations: HashMap<PartitionAddress, BudgetReservation>,
	cancelled_queries: HashSet<QueryId>,
}

#[derive(Debug, Clone)]
struct SpilledPartition {
	path: PathBuf,
	nonce: [u8; 12],
	row_count: RowCount,
	digest: ArrowIpcDigest,
}

impl EncryptedSpillPartitionStore {
	pub fn new(spill_dir: PathBuf) -> Result<Self, PartitionStoreError> {
		Self::with_budget(spill_dir, QueryBudget::test_default())
	}

	pub fn with_budget(
		spill_dir: PathBuf,
		budget: QueryBudget,
	) -> Result<Self, PartitionStoreError> {
		std::fs::create_dir_all(&spill_dir)?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| PartitionStoreError::Random {
			operation: "partition-spill-key-generation",
			message: format!("{error:?}"),
		})?;
		let unbound_key = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			PartitionStoreError::Crypto {
				operation: "partition-spill-key-construction",
				message: format!("{error:?}"),
			}
		})?;

		Ok(Self {
			inner: RwLock::new(EncryptedSpillInner::default()),
			budget: BudgetTracker::new(budget),
			spill_dir,
			key: LessSafeKey::new(unbound_key),
			next_spill_id: AtomicU64::new(1),
		})
	}

	pub fn budget_used(&self, dimension: BudgetDimension) -> ByteCount {
		self.budget.used(dimension)
	}

	fn next_spill_id(&self) -> SpillId {
		SpillId::new(self.next_spill_id.fetch_add(1, Ordering::SeqCst))
	}

	fn spill_path(&self, spill_id: SpillId, partition_id: PartitionId) -> PathBuf {
		self.spill_dir.join(format!(
			"partition_{}_{}.bin",
			spill_id.get(),
			partition_id.get()
		))
	}

	fn encrypt_payload(
		&self,
		payload: &PartitionPayload,
	) -> Result<(Vec<u8>, [u8; 12]), PartitionStoreError> {
		let mut nonce_bytes = [0u8; 12];
		aws_lc_rs::rand::fill(&mut nonce_bytes).map_err(|error| PartitionStoreError::Random {
			operation: "partition-spill-nonce-generation",
			message: format!("{error:?}"),
		})?;
		let nonce = Nonce::assume_unique_for_key(nonce_bytes);
		let mut ciphertext = payload.bytes.clone();
		self.key
			.seal_in_place_append_tag(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| PartitionStoreError::Crypto {
				operation: "partition-spill-encrypt",
				message: format!("{error:?}"),
			})?;
		Ok((ciphertext, nonce_bytes))
	}

	fn decrypt_payload(
		&self,
		ciphertext: &mut [u8],
		nonce_bytes: [u8; 12],
	) -> Result<Vec<u8>, PartitionMissingReason> {
		let nonce = Nonce::assume_unique_for_key(nonce_bytes);
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), ciphertext)
			.map_err(|_| PartitionMissingReason::StoreEvicted)?;
		Ok(plaintext.to_vec())
	}
}

#[async_trait::async_trait]
impl PartitionStore for EncryptedSpillPartitionStore {
	fn publish(
		&self,
		mut manifest: PartitionManifest,
		payloads: Vec<PartitionPayload>,
	) -> Result<PartitionManifest, PartitionStoreError> {
		let key = ManifestKey::from_manifest(&manifest);
		validate_manifest_payloads(&manifest, &payloads)?;

		let mut inner = self.inner.write().expect("partition store poisoned");
		if inner.cancelled_queries.contains(&manifest.query_id) {
			return Err(PartitionStoreError::CancelledQuery {
				query_id: manifest.query_id,
			});
		}
		for payload in &payloads {
			if inner.partitions.contains_key(&payload.address) {
				return Err(PartitionStoreError::DuplicatePartition {
					address: payload.address,
				});
			}
		}

		let spill_id = self.next_spill_id();
		let mut written = Vec::new();
		let mut spilled = Vec::with_capacity(payloads.len());
		let mut reservations = Vec::with_capacity(payloads.len());
		for payload in &payloads {
			let (ciphertext, nonce) = self.encrypt_payload(payload)?;
			let reserved = ByteCount::from_usize(ciphertext.len());
			reservations.push((
				payload.address,
				self.budget.reserve(BudgetDimension::SpillBytes, reserved)?,
			));
			let path = self.spill_path(spill_id, payload.address.partition_id);
			if let Err(error) = std::fs::write(&path, &ciphertext) {
				for path in &written {
					let _ = std::fs::remove_file(path);
				}
				return Err(PartitionStoreError::Io(error));
			}
			written.push(path.clone());
			spilled.push((
				payload.address,
				SpilledPartition {
					path,
					nonce,
					row_count: payload.row_count,
					digest: payload.digest,
				},
			));
		}

		manifest.backend = PartitionBackend::EncryptedSpill { spill_id };
		for (address, partition) in spilled {
			inner.partitions.insert(address, partition);
		}
		for (address, reservation) in reservations {
			inner.reservations.insert(address, reservation);
		}
		inner.manifests.insert(key, manifest.clone());
		Ok(manifest)
	}

	fn manifest(&self, key: ManifestKey) -> Option<PartitionManifest> {
		self.inner
			.read()
			.expect("partition store poisoned")
			.manifests
			.get(&key)
			.cloned()
	}

	fn pull(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<PartitionPayload, PartitionMissingReason> {
		if address.attempt_id != expected_attempt {
			return Err(PartitionMissingReason::AttemptStale {
				expected_attempt,
				observed_attempt: address.attempt_id,
			});
		}

		let partition = {
			let inner = self.inner.read().expect("partition store poisoned");
			if inner.cancelled_queries.contains(&address.query_id) {
				return Err(PartitionMissingReason::CancelledTaskCleanup);
			}
			if let Some(partition) = inner.partitions.get(&address) {
				partition.clone()
			} else if let Some(observed_attempt) = inner
				.partitions
				.keys()
				.find(|candidate| same_logical_partition(**candidate, address))
				.map(|candidate| candidate.attempt_id)
			{
				return Err(PartitionMissingReason::AttemptStale {
					expected_attempt,
					observed_attempt,
				});
			} else {
				return Err(PartitionMissingReason::StoreEvicted);
			}
		};

		let mut ciphertext =
			std::fs::read(&partition.path).map_err(|_| PartitionMissingReason::StoreEvicted)?;
		let bytes = self.decrypt_payload(&mut ciphertext, partition.nonce)?;
		Ok(PartitionPayload::new(
			address,
			bytes,
			partition.row_count,
			partition.digest,
		))
	}

	fn remove_query(&self, query_id: QueryId) {
		let mut inner = self.inner.write().expect("partition store poisoned");
		let removed = remove_spilled_query(&mut inner, query_id);
		for path in removed {
			let _ = std::fs::remove_file(path);
		}
	}

	fn cancel_query(&self, query_id: QueryId) {
		let mut inner = self.inner.write().expect("partition store poisoned");
		inner.cancelled_queries.insert(query_id);
		let removed = remove_spilled_query(&mut inner, query_id);
		for path in removed {
			let _ = std::fs::remove_file(path);
		}
	}
}

pub struct HybridPartitionStore {
	in_memory: InMemoryPartitionStore,
	encrypted_spill: EncryptedSpillPartitionStore,
	spill_threshold: ByteCount,
}

impl HybridPartitionStore {
	pub fn new(
		spill_dir: PathBuf,
		spill_threshold: ByteCount,
	) -> Result<Self, PartitionStoreError> {
		Self::with_budget(spill_dir, spill_threshold, QueryBudget::test_default())
	}

	pub fn with_budget(
		spill_dir: PathBuf,
		spill_threshold: ByteCount,
		budget: QueryBudget,
	) -> Result<Self, PartitionStoreError> {
		Ok(Self {
			in_memory: InMemoryPartitionStore::with_budget(budget),
			encrypted_spill: EncryptedSpillPartitionStore::with_budget(spill_dir, budget)?,
			spill_threshold,
		})
	}

	pub fn budget_used(&self, dimension: BudgetDimension) -> ByteCount {
		ByteCount::new(
			self.in_memory.budget_used(dimension).get()
				+ self.encrypted_spill.budget_used(dimension).get(),
		)
	}
}

#[async_trait::async_trait]
impl PartitionStore for HybridPartitionStore {
	fn publish(
		&self,
		manifest: PartitionManifest,
		payloads: Vec<PartitionPayload>,
	) -> Result<PartitionManifest, PartitionStoreError> {
		let total_bytes = ByteCount::new(
			payloads
				.iter()
				.map(|payload| payload.size_bytes().get())
				.sum(),
		);
		if total_bytes > self.spill_threshold {
			self.encrypted_spill.publish(manifest, payloads)
		} else {
			self.in_memory.publish(manifest, payloads)
		}
	}

	fn manifest(&self, key: ManifestKey) -> Option<PartitionManifest> {
		self.in_memory
			.manifest(key)
			.or_else(|| self.encrypted_spill.manifest(key))
	}

	fn pull(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<PartitionPayload, PartitionMissingReason> {
		match self.in_memory.pull(address, expected_attempt) {
			Ok(payload) => Ok(payload),
			Err(PartitionMissingReason::StoreEvicted) => {
				self.encrypted_spill.pull(address, expected_attempt)
			}
			Err(reason) => Err(reason),
		}
	}

	fn open_live(
		&self,
		address: PartitionAddress,
		schema: SchemaRef,
	) -> Result<(), PartitionStoreError> {
		self.in_memory.open_live(address, schema)
	}

	fn open_live_with_downstream_cancellation(
		&self,
		address: PartitionAddress,
		schema: SchemaRef,
		downstream_cancellation: Option<DownstreamCancellationContext>,
	) -> Result<(), PartitionStoreError> {
		self.in_memory.open_live_with_downstream_cancellation(
			address,
			schema,
			downstream_cancellation,
		)
	}

	async fn append_live(&self, payload: PartitionPayload) -> Result<(), PartitionStoreError> {
		self.in_memory.append_live(payload).await
	}

	fn seal_live(
		&self,
		manifest: PartitionManifest,
	) -> Result<PartitionManifest, PartitionStoreError> {
		self.in_memory.seal_live(manifest)
	}

	fn cancel_live(&self, address: PartitionAddress, reason: PartitionMissingReason) {
		self.in_memory.cancel_live(address, reason);
	}

	fn pull_live(
		&self,
		address: PartitionAddress,
		expected_attempt: AttemptId,
	) -> Result<LivePartitionStream, PartitionMissingReason> {
		self.in_memory.pull_live(address, expected_attempt)
	}

	fn remove_query(&self, query_id: QueryId) {
		self.in_memory.remove_query(query_id);
		self.encrypted_spill.remove_query(query_id);
	}

	fn cancel_query(&self, query_id: QueryId) {
		self.in_memory.cancel_query(query_id);
		self.encrypted_spill.cancel_query(query_id);
	}
}

fn validate_manifest_payloads(
	manifest: &PartitionManifest,
	payloads: &[PartitionPayload],
) -> Result<(), PartitionStoreError> {
	let key = ManifestKey::from_manifest(manifest);
	let mut addresses = HashSet::new();
	for payload in payloads {
		if ManifestKey::from_address(payload.address) != key {
			return Err(PartitionStoreError::AddressManifestMismatch);
		}
		if !addresses.insert(payload.address) {
			return Err(PartitionStoreError::DuplicatePartition {
				address: payload.address,
			});
		}
		let entry = payload.entry();
		if !manifest.partitions.contains(&entry) {
			return Err(PartitionStoreError::PartitionAbsentFromManifest {
				partition_id: payload.address.partition_id,
			});
		}
	}
	Ok(())
}

fn remove_spilled_query(inner: &mut EncryptedSpillInner, query_id: QueryId) -> Vec<PathBuf> {
	inner.manifests.retain(|key, _| key.query_id != query_id);
	let mut removed = Vec::new();
	inner.partitions.retain(|address, partition| {
		if address.query_id == query_id {
			removed.push(partition.path.clone());
			false
		} else {
			true
		}
	});
	inner
		.reservations
		.retain(|address, _| address.query_id != query_id);
	removed
}

fn same_logical_partition(candidate: PartitionAddress, requested: PartitionAddress) -> bool {
	candidate.query_id == requested.query_id
		&& candidate.exchange_id == requested.exchange_id
		&& candidate.producer_stage_id == requested.producer_stage_id
		&& candidate.producer_task_id == requested.producer_task_id
		&& candidate.partition_id == requested.partition_id
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::arrow_ipc::{decode_record_batches, encode_record_batch};
	use crate::distributed::identity::WorkerId;
	use crate::distributed::task::PartitionBackend;
	use crate::distributed::worker_registry::WorkerEndpoint;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;

	fn address(attempt_id: AttemptId) -> PartitionAddress {
		PartitionAddress {
			query_id: QueryId::from_u128(1),
			exchange_id: ExchangeId::new(2),
			producer_stage_id: StageId::new(3),
			producer_task_id: TaskId::new(4),
			attempt_id,
			partition_id: PartitionId::new(5),
		}
	}

	fn payload(address: PartitionAddress) -> PartitionPayload {
		payload_with_bytes(address, vec![1, 2, 3])
	}

	fn test_schema() -> SchemaRef {
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
	}

	fn payload_with_bytes(address: PartitionAddress, bytes: Vec<u8>) -> PartitionPayload {
		let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
		PartitionPayload::new(address, bytes, RowCount::new(1), digest)
	}

	fn ipc_batch(values: &[i64]) -> RecordBatch {
		RecordBatch::try_new(
			test_schema(),
			vec![Arc::new(Int64Array::from(values.to_vec()))],
		)
		.expect("ipc test batch")
	}

	fn ipc_payload(address: PartitionAddress, values: &[i64]) -> PartitionPayload {
		let batch = ipc_batch(values);
		let bytes = encode_record_batch(&batch).expect("encode ipc payload");
		let digest = ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes));
		PartitionPayload::new(
			address,
			bytes,
			RowCount::new(u64::try_from(values.len()).expect("row count fits u64")),
			digest,
		)
	}

	fn decoded_i64_values(bytes: &[u8]) -> Vec<i64> {
		let batches = decode_record_batches(bytes).expect("decode ipc payload");
		let mut values = Vec::new();
		for batch in batches {
			let array = batch
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("int64 column");
			for row in 0..array.len() {
				values.push(array.value(row));
			}
		}
		values
	}

	fn payloads_size(payloads: &[PartitionPayload]) -> ByteCount {
		payloads.iter().fold(ByteCount::ZERO, |total, payload| {
			total.saturating_add(payload.size_bytes())
		})
	}

	fn live_chunk_state(
		store: &InMemoryPartitionStore,
		address: PartitionAddress,
	) -> (usize, usize, usize) {
		let inner = store.inner.read().expect("partition store poisoned");
		let live = inner.live_partitions.get(&address).expect("live partition");
		(
			live.chunks.len(),
			live.reservations.len(),
			live.farthest_released_index,
		)
	}

	async fn expect_live_payload(
		stream: &mut LivePartitionStream,
		label: &'static str,
	) -> PartitionPayload {
		tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
			.await
			.expect(label)
			.expect("live stream yielded payload")
			.expect("live payload ok")
	}

	async fn expect_live_eof(stream: &mut LivePartitionStream, label: &'static str) {
		assert!(
			tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
				.await
				.expect(label)
				.is_none()
		);
	}

	fn manifest(payload: &PartitionPayload) -> PartitionManifest {
		manifest_for_entry(payload.address, payload.entry())
	}

	fn manifest_for_address(address: PartitionAddress) -> PartitionManifest {
		manifest_for_entry(
			address,
			PartitionEntry {
				partition_id: address.partition_id,
				size_bytes: ByteCount::ZERO,
				row_count: RowCount::new(0),
				digest: ArrowIpcDigest::new([0; 32]),
			},
		)
	}

	fn manifest_for_entry(address: PartitionAddress, entry: PartitionEntry) -> PartitionManifest {
		PartitionManifest {
			query_id: address.query_id,
			stage_id: address.producer_stage_id,
			task_id: address.producer_task_id,
			attempt_id: address.attempt_id,
			exchange_id: address.exchange_id,
			producer_worker_id: WorkerId::from_u128(6),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9000"),
			partitions: vec![entry],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		}
	}

	fn temp_spill_dir() -> PathBuf {
		std::env::temp_dir().join(format!("toron_partition_spill_{}", rand::random::<u64>()))
	}

	#[test]
	fn in_memory_store_publishes_manifest_and_pulls_partition() {
		let store = InMemoryPartitionStore::new();
		let payload = payload(address(AttemptId::first()));
		let manifest = manifest(&payload);
		let key = ManifestKey::from_manifest(&manifest);

		store
			.publish(manifest.clone(), vec![payload.clone()])
			.expect("publish");

		assert_eq!(store.manifest(key), Some(manifest));
		assert_eq!(store.pull(payload.address, AttemptId::first()), Ok(payload));
	}

	#[tokio::test]
	async fn live_pull_before_open_waits_for_open_append_or_cancel() {
		let store = Arc::new(InMemoryPartitionStore::new());
		let address = address(AttemptId::first());
		let expected = payload(address);
		let reader = store.clone();
		let read = tokio::spawn(async move {
			let mut stream = reader
				.pull_live(address, AttemptId::first())
				.expect("live pull");
			stream.next().await.expect("chunk").expect("live chunk")
		});

		tokio::task::yield_now().await;
		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(expected.clone())
			.await
			.expect("append live");

		let observed = tokio::time::timeout(std::time::Duration::from_secs(1), read)
			.await
			.expect("live pull should wake")
			.expect("reader task");
		assert_eq!(observed, expected);
	}

	#[tokio::test]
	async fn live_cancel_wakes_blocked_reader_without_partial_success() {
		let store = Arc::new(InMemoryPartitionStore::new());
		let address = address(AttemptId::first());
		let reader = store.clone();
		let read = tokio::spawn(async move {
			let mut stream = reader
				.pull_live(address, AttemptId::first())
				.expect("live pull");
			stream.next().await.expect("terminal error")
		});

		tokio::task::yield_now().await;
		store.cancel_live(address, PartitionMissingReason::WorkerLost);

		let observed = tokio::time::timeout(std::time::Duration::from_secs(1), read)
			.await
			.expect("live cancel should wake")
			.expect("reader task");
		assert_eq!(observed, Err(PartitionMissingReason::WorkerLost));
	}

	#[tokio::test]
	async fn live_partition_failure_fails_query_without_partial_success() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let chunk = payload(address);

		store.open_live(address, test_schema()).expect("open live");
		store.append_live(chunk).await.expect("append live");
		store.cancel_live(address, PartitionMissingReason::WorkerLost);

		let mut stream = store
			.pull_live(address, AttemptId::first())
			.expect("live pull");
		let observed = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
			.await
			.expect("live failure should wake")
			.expect("terminal failure");
		assert_eq!(observed, Err(PartitionMissingReason::WorkerLost));
	}

	#[tokio::test]
	async fn abnormal_drop_of_unsealed_reader_still_cancels() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		store.open_live(address, test_schema()).expect("open live");

		let stream = store
			.pull_live(address, AttemptId::first())
			.expect("live pull");
		drop(stream);

		let mut observed = store
			.pull_live(address, AttemptId::first())
			.expect("second live pull");
		let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), observed.next())
			.await
			.expect("drop cancellation should wake")
			.expect("terminal cancellation");
		assert_eq!(terminal, Err(PartitionMissingReason::CancelledTaskCleanup));
	}

	#[tokio::test]
	async fn live_producer_blocks_when_backlog_full_until_consumer_pulls() {
		let store = Arc::new(InMemoryPartitionStore::new());
		let address = address(AttemptId::first());
		store.open_live(address, test_schema()).expect("open live");
		for value in 0..LIVE_BACKLOG_DEPTH {
			store
				.append_live(payload_with_bytes(address, vec![value as u8]))
				.await
				.expect("append within backlog depth");
		}

		let writer = store.clone();
		let mut blocked_append = tokio::spawn(async move {
			writer
				.append_live(payload_with_bytes(address, vec![99]))
				.await
		});
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(50), &mut blocked_append)
				.await
				.is_err(),
			"append beyond live backlog depth should wait for a reader"
		);

		let mut stream = store
			.pull_live(address, AttemptId::first())
			.expect("live pull");
		let first = stream
			.next()
			.await
			.expect("first payload")
			.expect("first payload ok");
		assert_eq!(first.bytes, vec![0]);

		tokio::time::timeout(std::time::Duration::from_secs(1), blocked_append)
			.await
			.expect("blocked append should resume after one pull")
			.expect("append task")
			.expect("append after pull");
	}

	#[tokio::test]
	async fn pull_releases_one_backlog_slot_once() {
		let store = Arc::new(InMemoryPartitionStore::new());
		let address = address(AttemptId::first());
		store.open_live(address, test_schema()).expect("open live");
		for value in 0..LIVE_BACKLOG_DEPTH {
			store
				.append_live(payload_with_bytes(address, vec![value as u8]))
				.await
				.expect("append initial backlog");
		}

		let mut first_reader = store
			.pull_live(address, AttemptId::first())
			.expect("first live pull");
		for value in 0..LIVE_BACKLOG_DEPTH {
			let chunk = first_reader
				.next()
				.await
				.expect("initial chunk")
				.expect("initial chunk ok");
			assert_eq!(chunk.bytes, vec![value as u8]);
		}

		for value in LIVE_BACKLOG_DEPTH..(LIVE_BACKLOG_DEPTH * 2) {
			store
				.append_live(payload_with_bytes(address, vec![value as u8]))
				.await
				.expect("append after released slots");
		}

		let writer = store.clone();
		let mut over_depth_append = tokio::spawn(async move {
			writer
				.append_live(payload_with_bytes(address, vec![200]))
				.await
		});
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(50), &mut over_depth_append)
				.await
				.is_err(),
			"over-depth append should wait before replay"
		);

		let mut replay = store
			.pull_live(address, AttemptId::first())
			.expect("replay live pull");
		for value in 0..LIVE_BACKLOG_DEPTH {
			let chunk = replay
				.next()
				.await
				.expect("replayed chunk")
				.expect("replayed chunk ok");
			assert_eq!(chunk.bytes, vec![value as u8]);
		}
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(50), &mut over_depth_append)
				.await
				.is_err(),
			"retry replay must not release already-released slots"
		);

		let next = first_reader
			.next()
			.await
			.expect("next first-reader chunk")
			.expect("next first-reader chunk ok");
		assert_eq!(next.bytes, vec![LIVE_BACKLOG_DEPTH as u8]);
		tokio::time::timeout(std::time::Duration::from_secs(1), over_depth_append)
			.await
			.expect("over-depth append should resume after first read of next chunk")
			.expect("append task")
			.expect("over-depth append");
	}

	#[tokio::test]
	async fn cancel_live_wakes_blocked_appender_with_cancellation() {
		let store = Arc::new(InMemoryPartitionStore::new());
		let address = address(AttemptId::first());
		store.open_live(address, test_schema()).expect("open live");
		for value in 0..LIVE_BACKLOG_DEPTH {
			store
				.append_live(payload_with_bytes(address, vec![value as u8]))
				.await
				.expect("fill backlog");
		}

		let writer = store.clone();
		let mut blocked_append = tokio::spawn(async move {
			writer
				.append_live(payload_with_bytes(address, vec![99]))
				.await
		});
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(50), &mut blocked_append)
				.await
				.is_err(),
			"append should be pending before cancellation"
		);

		store.cancel_live(address, PartitionMissingReason::WorkerLost);
		let observed = tokio::time::timeout(std::time::Duration::from_secs(1), blocked_append)
			.await
			.expect("blocked append should wake on cancel")
			.expect("append task");
		assert!(matches!(
			observed,
			Err(PartitionStoreError::CancelledQuery { query_id })
				if query_id == address.query_id
		));
	}

	#[tokio::test]
	async fn live_seal_reassembles_multi_chunk_arrow_ipc() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let chunk_rows: [&[i64]; 3] = [&[1, 2], &[3, 4, 5], &[6]];
		let payloads = chunk_rows
			.iter()
			.map(|values| ipc_payload(address, values))
			.collect::<Vec<_>>();
		let first_payload = payloads[0].clone();
		store.open_live(address, test_schema()).expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append chunk");
		}
		let mut stream = store
			.pull_live(address, AttemptId::first())
			.expect("live pull");
		assert_eq!(
			stream
				.next()
				.await
				.expect("first chunk")
				.expect("first chunk ok")
				.bytes,
			payloads[0].bytes
		);

		let sealed = store
			.seal_live(manifest(&first_payload))
			.expect("seal live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("sealed pull");
		assert_eq!(decoded_i64_values(&pulled.bytes), vec![1, 2, 3, 4, 5, 6]);
		assert_eq!(pulled.row_count, RowCount::new(6));
		assert_eq!(
			pulled.digest,
			ArrowIpcDigest::new(wendelon::hash::blake3::digest(&pulled.bytes))
		);
		assert_eq!(sealed.partitions[0], pulled.entry());
	}

	#[tokio::test]
	async fn live_seal_empty_partition_emits_schema_bearing_ipc() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let schema = test_schema();
		store.open_live(address, schema.clone()).expect("open live");

		let sealed = store
			.seal_live(manifest_for_address(address))
			.expect("seal empty live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("sealed pull");
		let batches = decode_record_batches(&pulled.bytes).expect("decode empty payload");

		assert_eq!(pulled.row_count, RowCount::new(0));
		assert_eq!(batches.len(), 1);
		assert_eq!(batches[0].num_rows(), 0);
		assert_eq!(batches[0].schema().as_ref(), schema.as_ref());
		assert_eq!(sealed.partitions[0], pulled.entry());
	}

	#[tokio::test]
	async fn late_sealed_pull_after_streaming_succeeds() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let payloads = [[7], [8], [9]]
			.iter()
			.map(|values| ipc_payload(address, values))
			.collect::<Vec<_>>();
		let first_payload = payloads[0].clone();
		store.open_live(address, test_schema()).expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append chunk");
		}
		{
			let mut stream = store
				.pull_live(address, AttemptId::first())
				.expect("live pull");
			for payload in &payloads {
				assert_eq!(
					stream
						.next()
						.await
						.expect("streamed chunk")
						.expect("streamed chunk ok")
						.bytes,
					payload.bytes
				);
			}
		}

		store
			.seal_live(manifest(&first_payload))
			.expect("seal live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("late sealed pull");
		assert_eq!(decoded_i64_values(&pulled.bytes), vec![7, 8, 9]);
		assert_eq!(pulled.row_count, RowCount::new(3));
	}

	#[tokio::test]
	async fn retry_pull_live_restarts_from_index_zero() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let payloads = [[10], [11], [12]]
			.iter()
			.map(|values| ipc_payload(address, values))
			.collect::<Vec<_>>();
		let first_payload = payloads[0].clone();
		store.open_live(address, test_schema()).expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append chunk");
		}

		let mut first_attempt = store
			.pull_live(address, AttemptId::first())
			.expect("first live pull");
		assert_eq!(
			first_attempt
				.next()
				.await
				.expect("first attempt chunk")
				.expect("first attempt chunk ok")
				.bytes,
			payloads[0].bytes
		);

		let mut retry = store
			.pull_live(address, AttemptId::first())
			.expect("retry live pull");
		for payload in &payloads {
			assert_eq!(
				retry
					.next()
					.await
					.expect("retry chunk")
					.expect("retry chunk ok")
					.bytes,
				payload.bytes
			);
		}
		store
			.seal_live(manifest(&first_payload))
			.expect("seal live");
	}

	#[tokio::test]
	async fn shuffle_byte_budget_still_enforced_at_live_append() {
		let store = Arc::new(InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::from_usize(LIVE_BACKLOG_DEPTH),
			..QueryBudget::test_default()
		}));
		let address = address(AttemptId::first());
		store.open_live(address, test_schema()).expect("open live");

		let rejected = store
			.append_live(payload_with_bytes(
				address,
				vec![42; LIVE_BACKLOG_DEPTH + 1],
			))
			.await;
		assert!(matches!(
			rejected,
			Err(PartitionStoreError::BudgetExhausted {
				dimension: BudgetDimension::ShuffleBytes,
				requested,
				available,
			}) if requested == ByteCount::from_usize(LIVE_BACKLOG_DEPTH + 1)
				&& available == ByteCount::from_usize(LIVE_BACKLOG_DEPTH)
		));
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);

		for value in 0..LIVE_BACKLOG_DEPTH {
			tokio::time::timeout(
				std::time::Duration::from_secs(1),
				store.append_live(payload_with_bytes(address, vec![value as u8])),
			)
			.await
			.expect("permit from failed budget reservation should be returned")
			.expect("append after failed reservation");
		}
		let writer = store.clone();
		let mut blocked_append = tokio::spawn(async move {
			writer
				.append_live(payload_with_bytes(address, vec![99]))
				.await
		});
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(50), &mut blocked_append)
				.await
				.is_err(),
			"backlog should be exactly full after depth successful appends"
		);
		store.cancel_live(address, PartitionMissingReason::WorkerLost);
		let _ = tokio::time::timeout(std::time::Duration::from_secs(1), blocked_append)
			.await
			.expect("blocked append wakes during cleanup")
			.expect("append task");
	}

	#[tokio::test]
	async fn seal_live_reserves_materialized_sealed_payload() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(1_000_000),
			..QueryBudget::test_default()
		});
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[1, 2, 3]);

		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			payload.size_bytes()
		);

		store.seal_live(manifest(&payload)).expect("seal live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("sealed pull");
		{
			let inner = store.inner.read().expect("partition store poisoned");
			assert!(inner.partitions.contains_key(&address));
			assert!(inner.reservations.contains_key(&address));
			let live = inner.live_partitions.get(&address).expect("live partition");
			assert!(live.reservations.is_empty());
		}
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			pulled.size_bytes()
		);

		store.remove_query(address.query_id);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
	}

	#[tokio::test]
	async fn release_on_seal_when_no_readers() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(1_000_000),
			..QueryBudget::test_default()
		});
		let address = address(AttemptId::first());
		let payloads = [[1, 2], [3, 4]]
			.iter()
			.map(|values| ipc_payload(address, values))
			.collect::<Vec<_>>();
		store.open_live(address, test_schema()).expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append live");
		}
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			payloads_size(&payloads)
		);

		store.seal_live(manifest(&payloads[0])).expect("seal live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("sealed pull");

		assert_eq!(live_chunk_state(&store, address), (0, 0, 0));
		assert_eq!(decoded_i64_values(&pulled.bytes), vec![1, 2, 3, 4]);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			pulled.size_bytes()
		);
	}

	#[tokio::test]
	async fn release_on_last_reader_end_after_seal() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(1_000_000),
			..QueryBudget::test_default()
		});
		let live_address = address(AttemptId::first());
		let payloads = [[10], [11]]
			.iter()
			.map(|values| ipc_payload(live_address, values))
			.collect::<Vec<_>>();
		store
			.open_live(live_address, test_schema())
			.expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append live");
		}
		let mut stream = store
			.pull_live(live_address, AttemptId::first())
			.expect("live pull");

		store.seal_live(manifest(&payloads[0])).expect("seal live");
		assert_eq!(live_chunk_state(&store, live_address).0, payloads.len());
		for payload in &payloads {
			assert_eq!(
				expect_live_payload(&mut stream, "live chunk should arrive")
					.await
					.bytes,
				payload.bytes
			);
		}
		expect_live_eof(&mut stream, "sealed stream should end").await;
		assert_eq!(
			live_chunk_state(&store, live_address),
			(0, 0, payloads.len())
		);

		let early_address = address(AttemptId::new(2));
		let early_payloads = [[20], [21]]
			.iter()
			.map(|values| ipc_payload(early_address, values))
			.collect::<Vec<_>>();
		store
			.open_live(early_address, test_schema())
			.expect("open second live");
		for payload in &early_payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append second live");
		}
		let stream = store
			.pull_live(early_address, AttemptId::new(2))
			.expect("second live pull");
		store
			.seal_live(manifest(&early_payloads[0]))
			.expect("seal second live");
		assert_eq!(
			live_chunk_state(&store, early_address).0,
			early_payloads.len()
		);
		drop(stream);
		assert_eq!(live_chunk_state(&store, early_address), (0, 0, 0));
	}

	#[tokio::test]
	async fn seal_waits_for_all_active_live_readers() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[1, 2, 3]);
		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");

		let first = store
			.pull_live(address, AttemptId::first())
			.expect("first live pull");
		let second = store
			.pull_live(address, AttemptId::first())
			.expect("second live pull");
		store.seal_live(manifest(&payload)).expect("seal live");
		assert_eq!(live_chunk_state(&store, address).0, 1);

		drop(first);
		assert_eq!(live_chunk_state(&store, address).0, 1);
		drop(second);
		assert_eq!(live_chunk_state(&store, address), (0, 0, 0));
	}

	#[tokio::test]
	async fn pull_live_after_release_serves_sealed_payload() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let chunk_rows: [&[i64]; 2] = [&[30], &[31, 32]];
		let payloads = chunk_rows
			.into_iter()
			.map(|values| ipc_payload(address, values))
			.collect::<Vec<_>>();
		store.open_live(address, test_schema()).expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append live");
		}
		store.seal_live(manifest(&payloads[0])).expect("seal live");
		assert_eq!(live_chunk_state(&store, address).0, 0);

		let mut stream = store
			.pull_live(address, AttemptId::first())
			.expect("fallback live pull");
		let fallback = expect_live_payload(&mut stream, "fallback payload should arrive").await;
		assert_eq!(decoded_i64_values(&fallback.bytes), vec![30, 31, 32]);
		expect_live_eof(&mut stream, "fallback stream should yield once").await;
	}

	#[tokio::test]
	async fn retry_from_zero_after_release_is_correct() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let payloads = [[40], [41], [42]]
			.iter()
			.map(|values| ipc_payload(address, values))
			.collect::<Vec<_>>();
		store.open_live(address, test_schema()).expect("open live");
		for payload in &payloads {
			store
				.append_live(payload.clone())
				.await
				.expect("append live");
		}
		store.seal_live(manifest(&payloads[0])).expect("seal live");
		assert_eq!(live_chunk_state(&store, address).0, 0);

		for label in ["first retry", "second retry"] {
			let mut stream = store
				.pull_live(address, AttemptId::first())
				.expect("retry live pull");
			let payload = expect_live_payload(&mut stream, label).await;
			assert_eq!(decoded_i64_values(&payload.bytes), vec![40, 41, 42]);
			expect_live_eof(&mut stream, "retry fallback should end").await;
		}
	}

	#[tokio::test]
	async fn released_chunks_free_append_reservation() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(1_000_000),
			..QueryBudget::test_default()
		});
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[50, 51]);
		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			payload.size_bytes()
		);

		store.seal_live(manifest(&payload)).expect("seal live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("sealed pull");
		assert_eq!(live_chunk_state(&store, address), (0, 0, 0));
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			pulled.size_bytes()
		);

		store.remove_query(address.query_id);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
	}

	#[tokio::test]
	async fn fallback_stream_does_not_add_pacing_permits() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[60, 61]);
		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		store.seal_live(manifest(&payload)).expect("seal live");
		assert_eq!(live_chunk_state(&store, address), (0, 0, 0));

		let mut stream = store
			.pull_live(address, AttemptId::first())
			.expect("fallback live pull");
		let payload = expect_live_payload(&mut stream, "fallback payload").await;
		assert_eq!(decoded_i64_values(&payload.bytes), vec![60, 61]);
		expect_live_eof(&mut stream, "fallback stream should end").await;
		assert_eq!(live_chunk_state(&store, address), (0, 0, 0));
	}

	#[tokio::test]
	async fn double_release_is_noop() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(1_000_000),
			..QueryBudget::test_default()
		});
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[70]);
		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		store.seal_live(manifest(&payload)).expect("seal live");
		let pulled = store
			.pull(address, AttemptId::first())
			.expect("sealed pull");
		{
			let mut inner = store.inner.write().expect("partition store poisoned");
			let live = inner
				.live_partitions
				.get_mut(&address)
				.expect("live partition");
			release_live_chunks_if_idle(live);
			release_live_chunks_if_idle(live);
		}
		assert_eq!(live_chunk_state(&store, address), (0, 0, 0));
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			pulled.size_bytes()
		);
	}

	#[tokio::test]
	async fn fallback_yields_once_not_for_index_gt_zero() {
		let store = InMemoryPartitionStore::new();
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[80, 81]);
		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		store.seal_live(manifest(&payload)).expect("seal live");

		let mut stream = store
			.pull_live(address, AttemptId::first())
			.expect("fallback live pull");
		let fallback = expect_live_payload(&mut stream, "fallback payload").await;
		assert_eq!(decoded_i64_values(&fallback.bytes), vec![80, 81]);
		expect_live_eof(&mut stream, "fallback emits once").await;

		{
			let mut inner = store.inner.write().expect("partition store poisoned");
			inner.partitions.remove(&address);
			inner.reservations.remove(&address);
		}
		assert_eq!(
			store.pull_live(address, AttemptId::first()).err(),
			Some(PartitionMissingReason::StoreEvicted)
		);
	}

	#[tokio::test]
	async fn seal_live_over_shuffle_budget_is_rejected_without_partial_publish() {
		let address = address(AttemptId::first());
		let payload = ipc_payload(address, &[1, 2, 3]);
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: payload.size_bytes(),
			..QueryBudget::test_default()
		});
		let key = ManifestKey::from_address(address);

		store.open_live(address, test_schema()).expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		let rejected = store.seal_live(manifest(&payload));

		assert!(matches!(
			rejected,
			Err(PartitionStoreError::BudgetExhausted {
				dimension: BudgetDimension::ShuffleBytes,
				..
			})
		));
		assert_eq!(store.manifest(key), None);
		assert_eq!(
			store.pull(address, AttemptId::first()),
			Err(PartitionMissingReason::StoreEvicted)
		);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			payload.size_bytes()
		);
		store.remove_query(address.query_id);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
	}

	#[tokio::test]
	async fn live_seal_surfaces_manifest_and_sealed_pull() {
		let store = InMemoryPartitionStore::new();
		let payload = ipc_payload(address(AttemptId::first()), &[1, 2, 3]);
		let manifest = manifest(&payload);
		let key = ManifestKey::from_manifest(&manifest);

		store
			.open_live(payload.address, test_schema())
			.expect("open live");
		store
			.append_live(payload.clone())
			.await
			.expect("append live");
		let sealed = store.seal_live(manifest).expect("seal live");

		assert_eq!(store.manifest(key), Some(sealed.clone()));
		let pulled = store
			.pull(payload.address, AttemptId::first())
			.expect("sealed pull");
		assert_eq!(pulled.bytes, payload.bytes);
		assert_eq!(pulled.row_count, payload.row_count);
		assert_eq!(pulled.digest, sealed.partitions[0].digest);
	}

	#[tokio::test]
	async fn live_seal_preserves_pre_published_sealed_payload() {
		let store = InMemoryPartitionStore::new();
		let live_payload = payload(address(AttemptId::first()));
		let sealed_payload = PartitionPayload::new(
			live_payload.address,
			vec![8, 7, 6, 5],
			RowCount::new(9),
			ArrowIpcDigest::new([4; 32]),
		);
		let manifest = manifest(&sealed_payload);
		let key = ManifestKey::from_manifest(&manifest);

		store
			.open_live(live_payload.address, test_schema())
			.expect("open live");
		store.append_live(live_payload).await.expect("append live");
		store
			.publish(manifest.clone(), vec![sealed_payload.clone()])
			.expect("publish sealed payload");
		let sealed = store.seal_live(manifest).expect("seal live");

		assert_eq!(store.manifest(key), Some(sealed.clone()));
		assert_eq!(sealed.partitions[0].digest, sealed_payload.digest);
		assert_eq!(sealed.partitions[0].row_count, sealed_payload.row_count);
		let pulled = store
			.pull(sealed_payload.address, AttemptId::first())
			.expect("sealed pull");
		assert_eq!(pulled.bytes, sealed_payload.bytes);
		assert_eq!(pulled.row_count, sealed_payload.row_count);
		assert_eq!(pulled.digest, sealed_payload.digest);
	}

	#[test]
	fn encrypted_spill_store_publishes_manifest_and_pulls_partition() {
		let spill_dir = temp_spill_dir();
		let store = EncryptedSpillPartitionStore::new(spill_dir.clone()).expect("spill store");
		let payload = payload(address(AttemptId::first()));
		let manifest = manifest(&payload);
		let key = ManifestKey::from_manifest(&manifest);

		let accepted = store
			.publish(manifest, vec![payload.clone()])
			.expect("publish");

		assert!(matches!(
			accepted.backend,
			PartitionBackend::EncryptedSpill { .. }
		));
		assert_eq!(store.manifest(key), Some(accepted));
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Ok(payload.clone())
		);
		assert!(store.budget_used(BudgetDimension::SpillBytes) > ByteCount::ZERO);

		let files = std::fs::read_dir(&spill_dir)
			.expect("read spill dir")
			.collect::<Result<Vec<_>, _>>()
			.expect("spill entries");
		assert_eq!(files.len(), 1);
		let ciphertext = std::fs::read(files[0].path()).expect("read ciphertext");
		assert_ne!(ciphertext, payload.bytes);

		store.remove_query(payload.address.query_id);
		assert_eq!(
			store.budget_used(BudgetDimension::SpillBytes),
			ByteCount::ZERO
		);
		assert!(
			std::fs::read_dir(&spill_dir)
				.expect("read spill dir")
				.next()
				.is_none()
		);
		std::fs::remove_dir_all(&spill_dir).expect("remove spill dir");
	}

	#[test]
	fn encrypted_spill_store_rejects_late_publish_after_cancel() {
		let spill_dir = temp_spill_dir();
		let store = EncryptedSpillPartitionStore::new(spill_dir.clone()).expect("spill store");
		let payload = payload(address(AttemptId::first()));

		store.cancel_query(payload.address.query_id);
		let result = store.publish(manifest(&payload), vec![payload.clone()]);

		assert!(matches!(
			result,
			Err(PartitionStoreError::CancelledQuery { query_id })
				if query_id == payload.address.query_id
		));
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Err(PartitionMissingReason::CancelledTaskCleanup)
		);
		std::fs::remove_dir_all(&spill_dir).expect("remove spill dir");
	}

	#[test]
	fn hybrid_store_keeps_small_partitions_in_memory() {
		let spill_dir = temp_spill_dir();
		let store =
			HybridPartitionStore::new(spill_dir.clone(), ByteCount::new(64)).expect("hybrid store");
		let payload = payload(address(AttemptId::first()));
		let manifest = manifest(&payload);

		let accepted = store
			.publish(manifest, vec![payload.clone()])
			.expect("publish");

		assert_eq!(accepted.backend, PartitionBackend::InMemory);
		assert_eq!(store.pull(payload.address, AttemptId::first()), Ok(payload));
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::new(3)
		);
		assert_eq!(
			store.budget_used(BudgetDimension::SpillBytes),
			ByteCount::ZERO
		);
		std::fs::remove_dir_all(&spill_dir).expect("remove spill dir");
	}

	#[test]
	fn hybrid_store_promotes_large_partitions_to_encrypted_spill() {
		let spill_dir = temp_spill_dir();
		let store =
			HybridPartitionStore::new(spill_dir.clone(), ByteCount::new(2)).expect("hybrid store");
		let payload = payload(address(AttemptId::first()));
		let manifest = manifest(&payload);

		let accepted = store
			.publish(manifest, vec![payload.clone()])
			.expect("publish");

		assert!(matches!(
			accepted.backend,
			PartitionBackend::EncryptedSpill { .. }
		));
		assert_eq!(store.pull(payload.address, AttemptId::first()), Ok(payload));
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
		assert!(store.budget_used(BudgetDimension::SpillBytes) > ByteCount::ZERO);
		std::fs::remove_dir_all(&spill_dir).expect("remove spill dir");
	}

	#[test]
	fn stale_attempt_is_partition_missing_not_task_failure() {
		let store = InMemoryPartitionStore::new();
		let payload = payload(address(AttemptId::new(2)));
		store
			.publish(manifest(&payload), vec![payload.clone()])
			.expect("publish");

		let stale_address = PartitionAddress {
			attempt_id: AttemptId::first(),
			..payload.address
		};

		assert_eq!(
			store.pull(stale_address, AttemptId::first()),
			Err(PartitionMissingReason::AttemptStale {
				expected_attempt: AttemptId::first(),
				observed_attempt: AttemptId::new(2),
			})
		);
	}

	#[test]
	fn duplicate_partition_publish_is_atomic() {
		let store = InMemoryPartitionStore::new();
		let payload = payload(address(AttemptId::first()));
		let key = ManifestKey::from_address(payload.address);

		let result = store.publish(manifest(&payload), vec![payload.clone(), payload.clone()]);

		assert!(matches!(
			result,
			Err(PartitionStoreError::DuplicatePartition { .. })
		));
		assert_eq!(store.manifest(key), None);
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Err(PartitionMissingReason::StoreEvicted)
		);
	}

	#[test]
	fn publish_reserves_and_releases_shuffle_budget() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(3),
			..QueryBudget::test_default()
		});
		let payload = payload(address(AttemptId::first()));

		store
			.publish(manifest(&payload), vec![payload.clone()])
			.expect("publish within budget");
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::new(3)
		);

		store.remove_query(payload.address.query_id);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
	}

	#[test]
	fn publish_over_shuffle_budget_is_rejected_atomically() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(2),
			..QueryBudget::test_default()
		});
		let payload = payload(address(AttemptId::first()));
		let key = ManifestKey::from_address(payload.address);

		let result = store.publish(manifest(&payload), vec![payload.clone()]);

		assert!(matches!(
			result,
			Err(PartitionStoreError::BudgetExhausted {
				dimension: BudgetDimension::ShuffleBytes,
				requested,
				available,
			}) if requested == ByteCount::new(3) && available == ByteCount::new(2)
		));
		assert_eq!(store.manifest(key), None);
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Err(PartitionMissingReason::StoreEvicted)
		);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
	}

	#[test]
	fn query_cleanup_removes_manifest_and_payloads() {
		let store = InMemoryPartitionStore::new();
		let payload = payload(address(AttemptId::first()));
		let key = ManifestKey::from_address(payload.address);
		store
			.publish(manifest(&payload), vec![payload.clone()])
			.expect("publish");

		store.remove_query(payload.address.query_id);

		assert_eq!(store.manifest(key), None);
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Err(PartitionMissingReason::StoreEvicted)
		);
	}

	#[test]
	fn cancellation_cleanup_releases_budget_and_marks_partitions_cancelled() {
		let store = InMemoryPartitionStore::with_budget(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(3),
			..QueryBudget::test_default()
		});
		let payload = payload(address(AttemptId::first()));
		let key = ManifestKey::from_address(payload.address);
		store
			.publish(manifest(&payload), vec![payload.clone()])
			.expect("publish");

		store.cancel_query(payload.address.query_id);

		assert_eq!(store.manifest(key), None);
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Err(PartitionMissingReason::CancelledTaskCleanup)
		);
		assert_eq!(
			store.budget_used(BudgetDimension::ShuffleBytes),
			ByteCount::ZERO
		);
	}

	#[test]
	fn late_publish_after_cancellation_is_rejected() {
		let store = InMemoryPartitionStore::new();
		let payload = payload(address(AttemptId::first()));

		store.cancel_query(payload.address.query_id);
		let result = store.publish(manifest(&payload), vec![payload.clone()]);

		assert!(matches!(
			result,
			Err(PartitionStoreError::CancelledQuery { query_id })
				if query_id == payload.address.query_id
		));
		assert_eq!(
			store.pull(payload.address, AttemptId::first()),
			Err(PartitionMissingReason::CancelledTaskCleanup)
		);
	}
}
