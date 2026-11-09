//! ADR 0701: a ParentDecisions holder renews its leases in bounded batches:
//! one `AcquireLeases` entry per chunk of at most `MAX_LEASES_PER_BATCH`
//! ranges, proposed by the leader for itself or requested by a follower.

use super::*;
use crate::schema::{
	AcquireLeasesRequestHeader, AcquireLeasesResponseHeader, LeaseRangeWire, LeaseResultWire,
	MAX_LEASES_PER_BATCH, decode_lease_batch,
};

impl WendelonDataServer {
	/// One pass's renewals, a chunk at a time. Each chunk is stamped from the
	/// clock when it is built, never from a reading reused across a wait. Only a
	/// fully validated response updates the cache.
	pub(super) async fn renew_leases_batched(
		&self,
		ranges: &[(u64, u64)],
		epoch: u64,
		clock: &dyn PhysicalClock,
		ttl_ms: u32,
		current_leader: Option<u32>,
	) -> usize {
		let Some(leader) = current_leader else {
			return 0;
		};
		let mut granted = 0usize;
		for chunk in ranges.chunks(MAX_LEASES_PER_BATCH) {
			let requested_hlc = HybridTimestamp {
				physical_ms: clock.now_ms(),
				logical: 0,
				node_id: self.local_node.id,
			};
			let outcome = if leader == self.local_node.id {
				self.propose_own_leases(chunk, epoch, requested_hlc, ttl_ms)
					.await
			} else {
				self.request_remote_leases(
					NodeId {
						id: leader,
						_reserved: 0,
					},
					chunk,
					epoch,
					requested_hlc,
					ttl_ms,
				)
				.await
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
		granted
	}

	/// The leader's own chunk: signed and proposed as one entry.
	async fn propose_own_leases(
		&self,
		chunk: &[(u64, u64)],
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
			.map(|(range_start, range_end)| {
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
		let batch = crate::consensus::LeaseBatch::new(leases.clone())
			.ok_or_else(|| Error::other("lease batch over its bound"))?;
		let response = self
			.raft
			.client_write(WendelonRequest::AcquireLeases(batch))
			.await
			.map_err(|error| Error::other(format!("AcquireLeases commit failed: {error}")))?;
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
		chunk: &[(u64, u64)],
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
		};
		let entries: Vec<LeaseRangeWire> = chunk
			.iter()
			.map(|(range_start, range_end)| LeaseRangeWire {
				range_start: *range_start,
				range_end: *range_end,
			})
			.collect();
		let mut meta = bytemuck::bytes_of(&header).to_vec();
		meta.extend_from_slice(bytemuck::cast_slice(&entries));
		let conn = self.factory.get_connection(leader).await?;
		let mut stream = conn.open_bi().await?;
		let frame_header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
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
		chunk: &[(u64, u64)],
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
		for (result, (range_start, range_end)) in results.iter().zip(chunk) {
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
