use super::*;

/// ADR 075 cut-8: the network side of [`CounterReplicaTransport`] — drives the
/// counter replicate/rollback wire to a peer for a leader's remote participant.
/// Holds only the transport factory + buffer pool (the send functions are static).
pub(super) struct CounterReplicaNetworkTransport {
	pub(super) factory: Arc<TransportProviderFactory>,
	pub(super) pool: Arc<BufPool>,
}

impl crate::storage::counter::CounterReplicaTransport for CounterReplicaNetworkTransport {
	fn replicate(
		&self,
		peer: NodeId,
		tenant_id: TenantId,
		key_identity: Identity,
		op_id: u64,
		value: u64,
		commit_hlc: HybridTimestamp,
		epoch: u64,
		phase: CounterReplicaPhase,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ()>> + Send + '_>> {
		let factory = self.factory.clone();
		let pool = self.pool.clone();
		Box::pin(async move {
			let meta = CounterReplicateRequestMeta {
				commit_hlc,
				op_id,
				value,
				epoch,
				tenant_id,
				key_identity,
				phase,
				_pad: [0; 7],
			};
			match WendelonDataServer::replicate_counter_to_remote(factory, pool, peer, meta).await {
				Ok(ack) if ack.status == CounterReplicaStatus::OK => Ok(()),
				_ => Err(()),
			}
		})
	}
	fn rollback(
		&self,
		peer: NodeId,
		tenant_id: TenantId,
		key_identity: Identity,
		op_id: u64,
		epoch: u64,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
		let factory = self.factory.clone();
		let pool = self.pool.clone();
		Box::pin(async move {
			let meta = CounterRollbackRequestMeta {
				key_identity,
				op_id,
				epoch,
				tenant_id,
			};
			let _ = WendelonDataServer::rollback_counter_to_remote(factory, pool, peer, meta).await;
		})
	}
}

#[derive(Clone)]
pub(super) struct CounterParticipantFactory {
	pub(super) local_node: NodeId,
	pub(super) tenant_id: TenantId,
	pub(super) epoch: u64,
	pub(super) local: Arc<crate::storage::counter::CounterReplicaService>,
	pub(super) transport: Arc<dyn crate::storage::counter::CounterReplicaTransport>,
}

impl CounterParticipantFactory {
	pub(super) fn participant_for(
		&self,
		peer: NodeId,
	) -> crate::storage::counter::CounterParticipantKind {
		use crate::storage::counter::{
			CounterParticipantKind, LocalCounterParticipant, RemoteCounterParticipant,
		};
		if peer.id == self.local_node.id {
			CounterParticipantKind::Local(LocalCounterParticipant::new(
				self.local.clone(),
				self.tenant_id,
				self.local_node,
			))
		} else {
			CounterParticipantKind::Remote(RemoteCounterParticipant::new(
				self.transport.clone(),
				peer,
				self.tenant_id,
				self.epoch,
			))
		}
	}
}

impl WendelonDataServer {
	/// A counter leader whose dedup store latched fail-closed (an applied-log
	/// barrier failed, or a failed prepare could not withdraw its witness) has
	/// durable state this process can no longer describe: fence local writes.
	pub(super) async fn fence_unsettled_counter(
		&self,
		leader: &crate::storage::counter::CounterLeader<
			crate::storage::counter::CounterParticipantKind,
		>,
		tenant_id: TenantId,
	) {
		if leader.durability_unsettled() {
			self.trip_local_persistence_distrust(format!(
				"counter dedup store for tenant {} failed closed after a persistence failure",
				tenant_id.get()
			))
			.await;
		}
	}

	/// ADR 075 `COUNTER_REPLICATE_REQ` handler. Thin: decode the fixed meta, call
	/// `CounterReplicaService`, encode the fixed ack. The counter value rides in
	/// the meta (no arrow payload). NO leader orchestration, NO
	/// pending-finalization witness — a counter tentative does not ride ordinary
	/// replication semantics.
	pub(super) async fn handle_counter_replicate(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let meta: CounterReplicateRequestMeta = *bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<CounterReplicateRequestMeta>()],
		);
		let origin = match context.require_authenticated_node("counter replication") {
			Ok(origin) if origin._reserved == 0 => origin,
			_ => {
				return Ok(self.counter_ack_frame(
					frame.header.stream_id,
					MsgType::COUNTER_REPLICATE_RESP.0,
					CounterReplicaStatus::NOT_AUTHORIZED,
					meta.op_id,
				));
			}
		};
		if meta.phase != CounterReplicaPhase::TENTATIVE && meta.phase != CounterReplicaPhase::COMMIT
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"COUNTER_REPLICATE_REQ unknown phase",
			));
		}
		let status = if self.degraded_reason().is_some() {
			CounterReplicaStatus::INTERNAL_ERROR
		} else {
			let result = if meta.phase == CounterReplicaPhase::TENTATIVE {
				self.counter_replica
					.replicate_tentative(
						origin,
						meta.op_id,
						meta.tenant_id,
						meta.key_identity,
						meta.value,
						meta.commit_hlc,
					)
					.await
			} else {
				self.counter_replica
					.commit(origin, meta.op_id, meta.tenant_id, meta.key_identity)
					.await
			};
			match result {
				Ok(()) => CounterReplicaStatus::OK,
				Err(_) => CounterReplicaStatus::INTERNAL_ERROR,
			}
		};
		Ok(self.counter_ack_frame(
			frame.header.stream_id,
			MsgType::COUNTER_REPLICATE_RESP.0,
			status,
			meta.op_id,
		))
	}

	/// ADR 075 `COUNTER_ROLLBACK_REQ` handler. Removes only the matching
	/// tentative op (idempotent) and acks.
	pub(super) async fn handle_counter_rollback(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let meta: CounterRollbackRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CounterRollbackRequestMeta>()]);
		let origin = match context.require_authenticated_node("counter rollback") {
			Ok(origin) if origin._reserved == 0 => origin,
			_ => {
				return Ok(self.counter_ack_frame(
					frame.header.stream_id,
					MsgType::COUNTER_ROLLBACK_RESP.0,
					CounterReplicaStatus::NOT_AUTHORIZED,
					meta.op_id,
				));
			}
		};
		let status = match self
			.counter_replica
			.rollback(origin, meta.op_id, meta.tenant_id, meta.key_identity)
			.await
		{
			Ok(()) => CounterReplicaStatus::OK,
			Err(_) => CounterReplicaStatus::INTERNAL_ERROR,
		};
		Ok(self.counter_ack_frame(
			frame.header.stream_id,
			MsgType::COUNTER_ROLLBACK_RESP.0,
			status,
			meta.op_id,
		))
	}

	/// ADR 075 cut-4: the public `COUNTER_ADD_REQ` handler. Extracts + structurally
	/// verifies the counter key (D7), replays a duplicate `ClientWriteId` from the
	/// server-side D4 cache, else drives the per-tenant `CounterLeader` and caches
	/// the accepted outcome. ADR 076 derives the participant set from the key's
	/// current replica set and reaches majority quorum under replication.
	pub(super) async fn handle_counter_add(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		use crate::storage::counter::{
			CounterKey, CounterLeaderError, CounterParticipantPlan, counter_identity,
		};
		if frame.meta.len() < std::mem::size_of::<CounterAddRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"COUNTER_ADD_REQ meta too small",
			));
		}
		let meta: CounterAddRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CounterAddRequestMeta>()]);
		let stream_id = frame.header.stream_id;

		// A degraded node fails closed — no mutation.
		if self.degraded_reason().is_some() {
			return Ok(self.build_counter_add_response_frame(
				stream_id,
				self.counter_add_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					0,
					0,
					Self::zero_counter_hlc(),
				),
			));
		}

		// Extract the counter key from the Arrow payload and structurally verify its
		// domain-separated identity (D7). The server never trusts the wire identity.
		let payload_bytes = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"COUNTER_ADD_REQ missing Arrow payload",
			)
		})?;
		let batch = decode_batch(payload_bytes)?;
		let key_bytes = extract_first_binary(&batch, "key")?;
		if key_bytes.len() as u32 != meta.key_len {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"COUNTER_ADD_REQ key_len mismatch",
			));
		}
		let key = CounterKey(key_bytes.to_vec());
		if counter_identity(&key) != meta.key_identity {
			// D7 structural: the derived counter-domain identity must match the wire.
			return Ok(self.build_counter_add_response_frame(
				stream_id,
				self.counter_add_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					0,
					0,
					Self::zero_counter_hlc(),
				),
			));
		}

		let replicas = self
			.router
			.replicas_for_locality(Locality::from(meta.key_identity));
		let Some(owner) = replicas.first().copied() else {
			return Ok(self.build_counter_add_response_frame(
				stream_id,
				self.counter_add_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					0,
					0,
					Self::zero_counter_hlc(),
				),
			));
		};
		if owner.id != self.local_node.id {
			return Ok(self.build_counter_add_response_frame(
				stream_id,
				self.counter_add_response_with_hint(
					&meta,
					WriteStatus::NOT_LEADER,
					0,
					0,
					Self::zero_counter_hlc(),
					owner,
				),
			));
		}
		let participant_factory =
			self.counter_participant_factory(meta.tenant_id, meta.expected_epoch);
		let quorum = (replicas.len() / 2) + 1;
		let plan = CounterParticipantPlan::new(
			replicas
				.iter()
				.map(|peer| (*peer, participant_factory.participant_for(*peer)))
				.collect(),
			quorum,
		);

		// D4 public idempotency lives in the MUTATION layer: the leader keys its
		// durable dedup record by the canonical `ClientWriteId` bytes (never a
		// hashed/narrowed handle), serializes concurrent duplicates on its
		// per-key lock, and persists the record with the finalization witness —
		// so a duplicate replays the original previous/current without
		// re-applying whether it arrives concurrently, later in-process, or
		// after a node restart (`counter_leader_for` recovers the record).
		let leader = self.counter_leader_for(meta.tenant_id).await?;
		let write_key = meta.write_id.canonical_idempotency_bytes();
		// The leader runs the add on its own task and polls the distrust fence
		// there, so a handler dropped mid-add can neither cut the add short nor
		// skip a latch the add set.
		let fence = {
			let server = self.clone();
			let leader = leader.clone();
			let tenant_id = meta.tenant_id;
			move || async move { server.fence_unsettled_counter(&leader, tenant_id).await }
		};
		let added = leader
			.fetch_add_fenced(plan, write_key, &key, meta.delta, fence)
			.await;
		let resp = match added {
			Ok(result) => {
				let outcome = result.outcome();
				let status = if result.is_replay() {
					WriteStatus::DUPLICATE
				} else {
					WriteStatus::OK
				};
				self.counter_add_response(
					&meta,
					status,
					outcome.previous,
					outcome.current,
					Self::zero_counter_hlc(),
				)
			}
			// All rejects fail closed — the leader mutated nothing (D5/D6). A write-key
			// conflict carries zero values, never the recorded add's outcome.
			Err(err) => {
				let status = match err {
					CounterLeaderError::NotLeader => WriteStatus::NOT_LEADER,
					CounterLeaderError::ZeroDelta => WriteStatus::COUNTER_ZERO_DELTA,
					CounterLeaderError::Overflow => WriteStatus::COUNTER_OVERFLOW,
					CounterLeaderError::WriteKeyConflict => WriteStatus::WRITE_KEY_CONFLICT,
					CounterLeaderError::QuorumFailed
					| CounterLeaderError::Internal
					| CounterLeaderError::Visibility(_) => WriteStatus::INTERNAL_ERROR,
				};
				self.counter_add_response(&meta, status, 0, 0, Self::zero_counter_hlc())
			}
		};
		Ok(self.build_counter_add_response_frame(stream_id, resp))
	}

	/// ADR 075 cut-5b: the public `COUNTER_READ_REQ` handler — a point read over the
	/// counter-aware reader (counters store framed `u64`s, not Arrow value batches,
	/// so the blob read path cannot serve them). An absent counter returns
	/// `KEY_NOT_FOUND` (D6 — absence is absence, never a fabricated 0).
	pub(super) async fn handle_counter_read(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<CounterReadRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"COUNTER_READ_REQ meta too small",
			));
		}
		let meta: CounterReadRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CounterReadRequestMeta>()]);
		let stream_id = frame.header.stream_id;
		let current_epoch = self.admission_snapshot().current_epoch;
		if meta.expected_epoch < current_epoch {
			return Ok(self.build_counter_read_response_frame(
				stream_id,
				self.counter_read_response(ReadStatus::EPOCH_STALE, 0, current_epoch),
			));
		}
		if let Err(status) = self.enforce_read_policy(meta.tenant_id).await {
			return Ok(self.build_counter_read_response_frame(
				stream_id,
				self.counter_read_response(status, 0, current_epoch),
			));
		}
		// Ensure the tenant's counter leader has RECOVERED before the first read
		// after a restart: a crash between the durable witness and the commit
		// marker leaves the committed-by-contract value invisible until the
		// reconciler re-drives the marker — which leader recovery runs.
		let leader = self.counter_leader_for(meta.tenant_id).await?;
		let owner = self
			.router
			.replicas_for_locality(Locality::from(meta.key_identity))
			.first()
			.copied()
			.ok_or_else(|| Error::other("counter owner unavailable"))?;
		let value = if owner.id == self.local_node.id {
			leader
				.read_identity(meta.key_identity)
				.await
				.map_err(|error| match error {
					crate::storage::counter::CounterLeaderError::Visibility(error) => error.io(),
					_ => Error::other("counter read failed"),
				})?
		} else {
			self.counter_replica
				.read_committed(meta.tenant_id, meta.key_identity)
				.await?
		};
		let resp = match value {
			Some(v) => self.counter_read_response(ReadStatus::OK, v, current_epoch),
			None => self.counter_read_response(ReadStatus::KEY_NOT_FOUND, 0, current_epoch),
		};
		Ok(self.build_counter_read_response_frame(stream_id, resp))
	}
}
