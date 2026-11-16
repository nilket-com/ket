//! ADR 0701: a ParentDecisions holder renews its leases in bounded batches:
//! one `AcquireLeases` entry per chunk of at most `MAX_LEASES_PER_BATCH`
//! ranges, proposed by the leader for itself or requested by a follower.

use super::*;
use crate::schema::{
	AcquireLeasesRequestHeader, AcquireLeasesResponseHeader, LeaseRangeWire, LeaseResultWire,
	MAX_LEASES_PER_BATCH, decode_lease_batch,
};

/// ADR 0729: a range this node asks a lease for, and the generation of the
/// lineage it holds there (`None`: its own primary range, with no lineage).
pub(crate) type LeaseTarget = (u64, u64, Option<u64>);

impl WendelonDataServer {
	/// ADR 0729: what this node asks leases for, from applied state: every
	/// range whose lineage it holds, named by generation, and each of its
	/// `primary` ranges that has no lineage. A primary range another node holds
	/// is not asked for.
	pub(super) async fn decision_lease_targets(
		&self,
		primary: Vec<(u64, u64)>,
	) -> Vec<LeaseTarget> {
		let local = self.local_node.id;
		self.state_machine
			.read_state(move |state| {
				let Some(decisions) = state.decisions.as_ref() else {
					return Vec::new();
				};
				let mut targets: Vec<LeaseTarget> = decisions
					.lineages()
					.iter()
					.filter(|(_, lineage)| lineage.holder.id == local)
					.map(|(start, lineage)| (*start, lineage.range_end, Some(lineage.generation)))
					.collect();
				targets.extend(
					primary
						.into_iter()
						.filter(|(start, _)| !decisions.lineages().contains_key(start))
						.map(|(start, end)| (start, end, None)),
				);
				targets.sort_unstable();
				targets
			})
			.await
	}

	/// ADR 0729: one bounded pass of this store's takeover proposals, staggered
	/// by a third of its lease TTL per place in a range's replica order.
	pub(crate) async fn takeover_step(
		&self,
		maintenance: &crate::network::decision_proposals::MaintenanceProposals<'_>,
		lease_ttl: std::time::Duration,
	) -> usize {
		let Some(signer) = self.decisions.signer.as_ref() else {
			return 0;
		};
		#[cfg(test)]
		if self
			.decisions
			.pause_takeover
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return 0;
		}
		let stagger_ms = (lease_ttl.as_millis() as u64 / 3).max(1);
		maintenance
			.take_over_lapsed_once(&signer.key, signer.signer, stagger_ms, MAX_LEASES_PER_BATCH)
			.await
	}

	/// ADR 0729: the replicated stamp a leader puts on a lease batch it builds:
	/// the decision clock, through its gate.
	async fn lease_batch_stamp(&self) -> std::io::Result<u64> {
		let proposals = self
			.decisions
			.proposals
			.as_ref()
			.ok_or_else(|| Error::other("no decision proposals to stamp a lease batch"))?;
		proposals
			.decision_clock_now()
			.await
			.map_err(|refusal| Error::other(format!("lease batch stamp refused: {refusal:?}")))
	}

	/// One pass's renewals, a chunk at a time. Each chunk is stamped from the
	/// clock when it is built, never from a reading reused across a wait. Only a
	/// fully validated response updates the cache.
	///
	/// ADR 0734: the chunks are cut from this pass's walk, which begins where the
	/// last pass left off; each chunk's wait runs under `pass`, and one that is
	/// not answered inside the budget ends the pass with the chunks after it left
	/// for the next. Leases cached from earlier chunks stay.
	pub(super) async fn renew_leases_batched(
		&self,
		ranges: &[LeaseTarget],
		epoch: u64,
		clock: &dyn PhysicalClock,
		ttl_ms: u32,
		current_leader: Option<u32>,
		pass: &lease_pass::RenewalPass,
	) -> usize {
		let Some(leader) = current_leader else {
			return 0;
		};
		#[cfg(test)]
		if self
			.decisions
			.pause_lease_refresh
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return 0;
		}
		let walk = lease_pass::walk_from(ranges, |target| target.0, self.lease_renewal_cursor());
		let starts: Vec<u64> = walk.iter().map(|target| target.0).collect();
		let mut granted = 0usize;
		let mut attempted = 0usize;
		for chunk in walk.chunks(MAX_LEASES_PER_BATCH) {
			if pass.expired() {
				self.end_renewal_pass_at(&starts, lease_pass::Stopped::Before(attempted));
				self.renewal_pass_expired(
					pass,
					epoch,
					"before the next batch",
					walk.len() - attempted,
				);
				return granted;
			}
			let unit = attempted;
			attempted += chunk.len();
			let requested_hlc = HybridTimestamp {
				physical_ms: clock.now_ms(),
				logical: 0,
				node_id: self.local_node.id,
			};
			// The clock is read after the answer and before anything is cached: an
			// answer that completes after the deadline is not accepted.
			let answered = if leader == self.local_node.id {
				pass.bounded(self.propose_own_leases(chunk, epoch, requested_hlc, ttl_ms))
					.await
			} else {
				pass.bounded(self.request_remote_leases(
					NodeId {
						id: leader,
						_reserved: 0,
					},
					chunk,
					epoch,
					requested_hlc,
					ttl_ms,
				))
				.await
			};
			let Some(outcome) = answered else {
				self.end_renewal_pass_at(
					&starts,
					lease_pass::Stopped::Unanswered {
						unit,
						next: attempted,
					},
				);
				let waited_on = if leader == self.local_node.id {
					"its own commit of a batch of grants".to_owned()
				} else {
					format!("the control leader, node {leader}")
				};
				self.renewal_pass_expired(pass, epoch, &waited_on, walk.len() - attempted);
				return granted;
			};
			match outcome {
				Ok(leases) => {
					for lease in leases {
						self.cache_granted_lease(lease);
						granted += 1;
						metrics().data_ring_lease_grants_total.inc();
					}
				}
				Err(error) => {
					metrics().data_ring_lease_errors_total.inc();
					tracing::warn!(
						epoch,
						ranges = chunk.len(),
						%error,
						"lease manager: batched renewal failed; will retry next tick"
					);
				}
			}
		}
		self.set_lease_renewal_cursor(None);
		granted
	}

	/// Test seam: this node's renewal targets from the applied state.
	#[cfg(test)]
	pub(crate) async fn decision_lease_targets_for_test(&self) -> Vec<LeaseTarget> {
		self.decision_lease_targets(Vec::new()).await
	}

	/// Test seam: the batched renewals of one pass, under a budget the test supplies.
	#[cfg(test)]
	pub(crate) async fn renew_leases_batched_for_test(
		&self,
		targets: &[LeaseTarget],
		epoch: u64,
		clock: &dyn PhysicalClock,
		ttl_ms: u32,
		current_leader: Option<u32>,
		pass: &lease_pass::RenewalPass,
	) -> usize {
		self.renew_leases_batched(targets, epoch, clock, ttl_ms, current_leader, pass)
			.await
	}

	/// The leader's own chunk: signed and proposed as one entry.
	async fn propose_own_leases(
		&self,
		chunk: &[LeaseTarget],
		epoch: u64,
		requested_hlc: HybridTimestamp,
		ttl_ms: u32,
	) -> std::io::Result<Vec<LeaseDescriptor>> {
		let expiry_hlc = HybridTimestamp {
			physical_ms: requested_hlc.physical_ms.saturating_add(ttl_ms as u64),
			..requested_hlc
		};
		let leases: Vec<LeaseDescriptor> = chunk
			.iter()
			.map(|(range_start, range_end, _)| {
				self.make_signed_lease(
					epoch,
					self.local_node,
					*range_start,
					*range_end,
					requested_hlc,
					expiry_hlc,
				)
			})
			.collect();
		let stamp_ms = self.lease_batch_stamp().await?;
		// ADR 0108 (wendelon): the holder's own store incarnation, carried.
		let holder_incarnation = self.slab.owner_ref()?.incarnation();
		let grants = leases
			.iter()
			.zip(chunk)
			.map(
				|(lease, (_, _, expected_generation))| crate::consensus::LeaseBatchGrant {
					lease: *lease,
					expected_generation: *expected_generation,
					holder_incarnation,
				},
			)
			.collect();
		let batch = crate::consensus::LeaseBatch::new(stamp_ms, grants)
			.ok_or_else(|| Error::other("lease batch over its bound"))?;
		let response = self
			.raft
			.client_write(WendelonRequest::AcquireLeases(batch))
			.await
			.map_err(|error| Error::other(format!("AcquireLeases commit failed: {error}")))?;
		#[cfg(test)]
		{
			let gate = self.decisions.hold_lease_answer.lock().unwrap().clone();
			if let Some((applied, release)) = gate {
				applied.notify_one();
				release.notified().await;
			}
		}
		let WendelonResponse::LeaseResults(results) = response.data else {
			return Err(Error::other(format!(
				"AcquireLeases was not applied: {:?}",
				response.data
			)));
		};
		let results = results.results();
		if results.len() != leases.len()
			|| results
				.iter()
				.zip(&leases)
				.any(|(result, lease)| result.range_start != lease.range_start)
		{
			return Err(Error::other(
				"AcquireLeases results do not match its leases",
			));
		}
		Ok(results
			.iter()
			.zip(leases)
			.filter(|(result, _)| result.granted)
			.map(|(_, lease)| lease)
			.collect())
	}

	/// A follower's chunk: one LEASES_REQ to the leader. The response is
	/// validated whole before anything is returned for caching; a truncated or
	/// malformed reply returns nothing, and is not evidence that the omitted
	/// leases did not apply.
	pub(super) async fn request_remote_leases(
		&self,
		leader: NodeId,
		chunk: &[LeaseTarget],
		epoch: u64,
		requested_hlc: HybridTimestamp,
		ttl_ms: u32,
	) -> std::io::Result<Vec<LeaseDescriptor>> {
		let header = AcquireLeasesRequestHeader {
			epoch,
			holder: self.local_node,
			requested_hlc,
			ttl_ms,
			count: chunk.len() as u32,
			holder_incarnation: self.slab.owner_ref()?.incarnation().get().to_le_bytes(),
		};
		let entries: Vec<LeaseRangeWire> = chunk
			.iter()
			.map(|(range_start, range_end, expected)| LeaseRangeWire {
				range_start: *range_start,
				range_end: *range_end,
				expected_generation: expected.unwrap_or(0),
				has_generation: u64::from(expected.is_some()),
			})
			.collect();
		let mut meta = bytemuck::bytes_of(&header).to_vec();
		meta.extend_from_slice(bytemuck::cast_slice(&entries));
		let conn = self.factory.get_connection(leader).await?;
		let mut stream = conn.open_bi().await?;
		// ADR 0729: the batch's body names lineage generations, so it travels in
		// the mode's envelope, which a peer of the earlier body refuses.
		let envelope = self.decision_mode.raft_frame_version();
		let frame_header = FrameHeader {
			magic: HRFT_MAGIC,
			version: envelope,
			msg_type: MsgType::LEASES_REQ.0,
			flags: 0,
			stream_id: stream.stream_id() as u32,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &frame_header, &meta, None, None).await?;
		stream.finish().await?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		// Before the response is decoded or anything is cached.
		self.decision_mode.check_raft_frame(frame.header.version)?;
		if frame.header.msg_type != MsgType::LEASES_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected LEASES_RESP, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		self.accept_leases_response(&frame.meta, leader, chunk, epoch)
	}

	/// The granted leases of a whole, valid LEASES_RESP for `chunk`, or an
	/// error: a refusal of the whole request returns no lease, and any
	/// mismatch, truncation or unverifiable lease refuses the whole response.
	pub(super) fn accept_leases_response(
		&self,
		meta: &[u8],
		leader: NodeId,
		chunk: &[LeaseTarget],
		epoch: u64,
	) -> std::io::Result<Vec<LeaseDescriptor>> {
		let invalid = |why: &str| Error::new(ErrorKind::InvalidData, format!("LEASES_RESP {why}"));
		let header_len = std::mem::size_of::<AcquireLeasesResponseHeader>();
		if meta.len() == header_len {
			let header: AcquireLeasesResponseHeader = bytemuck::pod_read_unaligned(meta);
			// The whole request was refused, with one of the statuses the leader
			// emits for that: nothing to cache. Anything else is malformed.
			let whole_refusal = [
				LeaseStatus::DENIED_EPOCH_STALE,
				LeaseStatus::DENIED_NOT_LEADER,
				LeaseStatus::DENIED_INVALID_TARGET,
				LeaseStatus::INTERNAL_ERROR,
			];
			if header.count != 0 || header._pad != [0; 3] || !whole_refusal.contains(&header.status)
			{
				return Err(invalid("is a malformed refusal"));
			}
			return Ok(Vec::new());
		}
		let (header, results): (AcquireLeasesResponseHeader, Vec<LeaseResultWire>) =
			decode_lease_batch(meta, |header: &AcquireLeasesResponseHeader| header.count)?;
		if header.status != LeaseStatus::GRANTED
			|| header._pad != [0; 3]
			|| results.len() != chunk.len()
		{
			return Err(invalid("does not answer every requested range"));
		}
		let mut granted = Vec::new();
		for (result, (range_start, range_end, _)) in results.iter().zip(chunk) {
			if result.range_start != *range_start || result._pad != [0; 7] {
				return Err(invalid("answers another range"));
			}
			// Each element is granted or denied, nothing else; a denial carries
			// no lease. Any other status refuses the whole response, so a valid
			// granted prefix is never cached from a malformed one.
			if result.status == LeaseStatus::DENIED_OVERLAP {
				if result.lease != bytemuck::Zeroable::zeroed() {
					return Err(invalid("carries a lease with a denial"));
				}
				continue;
			}
			if result.status != LeaseStatus::GRANTED {
				return Err(invalid("carries an unknown status"));
			}
			let lease = result.lease;
			if lease.range_start != *range_start
				|| lease.range_end != *range_end
				|| lease.holder != self.local_node
				|| lease.epoch != epoch
				|| lease.issuer != leader
				|| self.lease_authority.verify_lease(&lease).is_err()
			{
				return Err(invalid("carries a lease that is not the requested one"));
			}
			granted.push(lease);
		}
		Ok(granted)
	}
}
