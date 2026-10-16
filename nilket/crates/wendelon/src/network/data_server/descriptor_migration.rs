use super::*;

/// Transient dedup for concurrent migration pulls (Plan 019 goal #5).
///
/// When a hot read races the background sweeper on the same
/// `(migration_id, key_identity)`, both paths might otherwise issue
/// a `MIGRATE_REQ` and hand the same record to
/// [`SlabManager::append_migration`] twice. `append_migration`
/// is already idempotent — that is the correctness boundary —
/// but serializing pulls here avoids the duplicate RPC round-trip
/// and the duplicate `(key_identity, hlc)` scan that would follow.
///
/// The registry hands out one `Arc<tokio::sync::Mutex<()>>` per
/// inflight key. Callers take the lock, re-check the local index,
/// and only issue a pull when the local index is still a miss.
/// The lock survives past the caller's guard so a subsequent racer
/// can synchronise on the same key without a secondary refresh.
#[derive(Default)]
pub struct MigrationRegistry {
	inflight: DashMap<(u64, TenantId, Identity), Arc<tokio::sync::Mutex<()>>>,
}

impl MigrationRegistry {
	pub fn new() -> Self {
		Self {
			inflight: DashMap::new(),
		}
	}

	/// Obtain the per-key gate. The returned `Arc` is cheap to
	/// clone and safe to hold across `.await`. Every caller for a
	/// given `(migration_id, key_identity)` observes the same lock, so
	/// at most one pull is in flight per key.
	pub fn gate(
		&self,
		migration_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> Arc<tokio::sync::Mutex<()>> {
		self.inflight
			.entry((migration_id, tenant_id, key_identity))
			.or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
			.clone()
	}

	/// Drop the registry slot for `(migration_id, key_identity)`. Called
	/// by `try_migrate_point_read` once the pull has run to
	/// completion so the map's DRAM footprint caps at
	/// `O(concurrent_requests)` rather than growing with every key
	/// ever touched by a migration. Safe because
	/// [`SlabManager::append_migration`] is the real correctness
	/// boundary — a racer that arrives after removal simply mints
	/// a fresh mutex, re-checks the local index (now a hit), and
	/// skips the pull.
	pub fn remove(&self, migration_id: u64, tenant_id: TenantId, key_identity: Identity) {
		self.inflight
			.remove(&(migration_id, tenant_id, key_identity));
	}
}

impl WendelonDataServer {
	pub(super) async fn refuse_counter_migration_rows(
		&self,
		locations: &[(
			TenantId,
			Identity,
			Locality,
			crate::storage::core::RecordLocation,
		)],
	) -> std::io::Result<()> {
		let mut checked_slabs = std::collections::HashSet::new();
		for (_, _, _, location) in locations {
			if checked_slabs.insert(location.slab_id) {
				self.slab.refuse_counter_migration(location.slab_id).await?;
			}
		}
		Ok(())
	}

	/// Handle an inbound MIGRATE_REQ frame (Plan 019).
	///
	/// Administrative peer-to-peer RPC that sources retained
	/// versions from a prior physical holder so the new logical
	/// owner can fill a lazy-migration miss. Bypasses leases —
	/// migration is an internal data-movement protocol — but still
	/// validates against the Raft Migration Ledger so a node that
	/// believes it owns a range cannot bully another replica into
	/// handing over bytes.
	///
	/// Validation order (each failure maps to a distinct status):
	///   1. Ledger lookup by `migration_id` — `STALE_LEDGER` if
	///      absent or `to_epoch` / `migration_hlc_floor` diverge.
	///   2. `logical_owner == requester` — `NOT_AUTHORIZED` if not.
	///   3. Ledger state already `Completed` by a different holder
	///      — `REDIRECT` with `source_hint = completed_by` so the
	///      caller can short-circuit the chain.
	///
	/// On `OK`, the response body is an Arrow IPC stream over
	/// [`migration_record_schema`] with one row per retained
	/// version. Point lookups return every version of the single
	/// key whose hash sits at `range_start`; range pages walk the
	/// inclusive `[range_start, range_end]` window honouring
	/// `max_records` / `max_bytes`, and set `next_cursor` for
	/// pagination.
	pub(super) async fn handle_migrate(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		metrics().data_ring_migrate_requests_total.inc();
		if frame.meta.len() < std::mem::size_of::<MigrateRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"MIGRATE_REQ meta too small",
			));
		}
		let meta: MigrateRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<MigrateRequestMeta>()]);

		let sm = self.state_machine.state_snapshot().await;
		let (resp_meta, arrow_bytes) = match sm.migrations.get(&meta.migration_id) {
			None => (
				migrate_error_meta(&meta, MigrateStatus::STALE_LEDGER, ZERO_NODE),
				None,
			),
			Some(entry)
				if entry.to_epoch != meta.expected_to_epoch
					|| entry.migration_hlc_floor != meta.migration_hlc_floor
					|| entry.range_start != meta.range_start
					|| entry.range_end != meta.range_end =>
			{
				(
					migrate_error_meta(&meta, MigrateStatus::STALE_LEDGER, ZERO_NODE),
					None,
				)
			}
			Some(entry) if entry.logical_owner.id != meta.requester.id => (
				migrate_error_meta(&meta, MigrateStatus::NOT_AUTHORIZED, ZERO_NODE),
				None,
			),
			Some(entry)
				if entry.state == MigrationState::Completed
					&& entry
						.completed_by
						.map(|c| c.id != self.local_node.id)
						.unwrap_or(false) =>
			{
				let hint = entry.completed_by.unwrap_or(ZERO_NODE);
				(
					migrate_error_meta(&meta, MigrateStatus::REDIRECT, hint),
					None,
				)
			}
			Some(entry) => self.serve_migrate_ok(&meta, entry).await?,
		};

		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp_meta));
		let arrow_payload = arrow_bytes.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			buf
		});
		let arrow_len = resp_meta.bytes_returned;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::MIGRATE_RESP.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: body.len() as u32,
			arrow_len,
			trailing_len: 0,
		};
		Ok(HrftFrame {
			header,
			meta: body,
			arrow_payload,
			trailing: None,
		})
	}

	pub(super) async fn handle_pending_witness_pull(
		&self,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<PendingWitnessPullRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"PENDING_WITNESS_REQ meta too small",
			));
		}
		let req: PendingWitnessPullRequestMeta = *bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<PendingWitnessPullRequestMeta>()],
		);
		let (resp_meta, payload) = if self.degraded_reason().is_some() {
			(
				PendingWitnessPullResponseMeta {
					status: PendingWitnessPullStatus::INTERNAL_ERROR,
					_pad1: [0; 7],
					witness_count: 0,
				},
				None,
			)
		} else {
			let records = self.pending_finalizations.load_all().await?;
			let payload = if records.is_empty() {
				None
			} else {
				Some(encode_pending_witness_records(&records)?)
			};
			tracing::debug!(
				requester = ?req.requester,
				request_epoch = req.request_epoch,
				witness_count = records.len(),
				"serving orphan-witness pull"
			);
			(
				PendingWitnessPullResponseMeta {
					status: PendingWitnessPullStatus::OK,
					_pad1: [0; 7],
					witness_count: records.len() as u64,
				},
				payload,
			)
		};

		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp_meta));
		let arrow_payload = payload.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::PENDING_WITNESS_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: body.len() as u32,
				arrow_len: arrow_payload
					.as_ref()
					.map(|buf| buf.len() as u32)
					.unwrap_or(0),
				trailing_len: 0,
			},
			meta: body,
			arrow_payload,
			trailing: None,
		})
	}
}
