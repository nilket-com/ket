//! Server-side Control Ring surface.
//!
//! This module hangs off the existing HRFT listener and
//! [`MsgRouter`](crate::network::router::MsgRouter) demux rather than
//! introducing a separate control-plane bind. It assumes callers are
//! already cluster-internal and mTLS-authenticated at the transport
//! layer; the handler here focuses on routing, wire compatibility, and
//! leader-aware Control Ring behavior.

use crate::client::topology::{ClusterPeerWire, ClusterStatusWire};
use crate::consensus::{WendelonRaft, WendelonRequest, WendelonResponse};
use crate::network::core::{HrftFrame, HRFT_MAGIC};
use crate::network::pool::BufPool;
use crate::network::router::ControlRingServer;
use crate::network::traits::{
	TransferLeadershipRequestMeta, TransferLeadershipResponseMeta,
};
use crate::network::wire;
use crate::schema::{
	epoch_entry_var_schema, AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, FrameHeader,
	HybridTimestamp, LeaseDescriptor, LeaseStatus, MsgType, NodeId,
};
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use tokio::sync::watch;

pub const CONTROL_REASON_GRANTED: u8 = 0;
pub const CONTROL_REASON_NOT_LEADER: u8 = 1;
pub const CONTROL_REASON_EPOCH_STALE: u8 = 2;
pub const CONTROL_REASON_INVALID_TARGET: u8 = 3;
pub const CONTROL_REASON_UNSUPPORTED: u8 = 4;
pub const CONTROL_REASON_INVALID_CONFIG: u8 = 5;
pub const CONTROL_REASON_INTERNAL_ERROR: u8 = 6;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceEpochResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
}

pub struct WendelonControlServer {
	raft: WendelonRaft,
	local_node: NodeId,
	epoch_rx: watch::Receiver<Arc<crate::network::transport::MembershipEpoch>>,
	pool: Arc<BufPool>,
	replication_factor: u32,
	lease_ttl_cap_ms: u32,
}

impl WendelonControlServer {
	pub fn new(
		raft: WendelonRaft,
		local_node: NodeId,
		epoch_rx: watch::Receiver<Arc<crate::network::transport::MembershipEpoch>>,
		pool: Arc<BufPool>,
		replication_factor: u32,
		lease_ttl_cap_ms: u32,
	) -> Self {
		Self {
			raft,
			local_node,
			epoch_rx,
			pool,
			replication_factor,
			lease_ttl_cap_ms,
		}
	}

	async fn handle_status(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		let snapshot = self.cluster_status_wire().await;
		let meta = wire::encode_into(&self.pool, &snapshot)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::STATUS_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	async fn handle_lease(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<AcquireLeaseRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LEASE_REQ meta too small",
			));
		}
		let req: AcquireLeaseRequestMeta = *bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<AcquireLeaseRequestMeta>()],
		);

		if req.epoch != self.current_epoch() {
			return self.lease_response(&frame, LeaseStatus::DENIED_EPOCH_STALE, None);
		}
		if !self.is_leader().await {
			return self.lease_response(&frame, LeaseStatus::DENIED_NOT_LEADER, None);
		}

		let ttl_ms = req.ttl_ms.clamp(1, self.lease_ttl_cap_ms);
		let lease = LeaseDescriptor {
			epoch: req.epoch,
			holder: req.holder,
			range_start: req.range_start,
			range_end: req.range_end,
			lease_start: req.requested_hlc,
			lease_expiry: HybridTimestamp {
				physical_ms: req.requested_hlc.physical_ms.saturating_add(ttl_ms as u64),
				logical: req.requested_hlc.logical,
				node_id: req.requested_hlc.node_id,
			},
			min_commit_index: 0,
			signature: [0; 64],
			_pad: [0; 8],
		};

		let status = match self
			.raft
			.client_write(WendelonRequest::AcquireLease(lease))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					return self.lease_response(&frame, LeaseStatus::GRANTED, Some(&lease));
				}
				WendelonResponse::LeaseDenied => LeaseStatus::DENIED_OVERLAP,
			},
			Err(_) if !self.is_leader().await => LeaseStatus::DENIED_NOT_LEADER,
			Err(_) => LeaseStatus::INTERNAL_ERROR,
		};
		self.lease_response(&frame, status, None)
	}

	async fn handle_transfer(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		let req: TransferLeadershipRequestMeta = wire::decode(&frame.meta)?;
		let resp = if !self.is_leader().await {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_NOT_LEADER,
			}
		} else if req.source_epoch != self.current_epoch() {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_EPOCH_STALE,
			}
		} else if req.from_leader.id != self.local_node.id || req.to_leader.id == self.local_node.id {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_INVALID_TARGET,
			}
		} else {
			// The routed server surface exists now; the actual transfer-token
			// workflow lands in later workstream slices. Reject explicitly
			// instead of letting the RPC fall into the router void.
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_UNSUPPORTED,
			}
		};

		let meta = wire::encode_into(&self.pool, &resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TRANSFER_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	async fn handle_advance_epoch(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		let leader_hint = self.leader_hint().await;
		let resp = if !self.is_leader().await {
			AdvanceEpochResponseWire {
				accepted: false,
				reason_code: CONTROL_REASON_NOT_LEADER,
				leader_hint,
			}
		} else {
			match frame.arrow_payload.as_ref() {
				None => AdvanceEpochResponseWire {
					accepted: false,
					reason_code: CONTROL_REASON_INVALID_CONFIG,
					leader_hint: None,
				},
				Some(bytes) => {
					let batch = decode_epoch_batch(bytes)?;
					if batch.schema().as_ref() != &epoch_entry_var_schema() {
						AdvanceEpochResponseWire {
							accepted: false,
							reason_code: CONTROL_REASON_INVALID_CONFIG,
							leader_hint: None,
						}
					} else {
						// Membership advancement still uses a watch-only shortcut
						// today; later workstream slices replace this explicit
						// rejection with a committed epoch transition path.
						AdvanceEpochResponseWire {
							accepted: false,
							reason_code: CONTROL_REASON_UNSUPPORTED,
							leader_hint: None,
						}
					}
				}
			}
		};

		let meta = wire::encode_into(&self.pool, &resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::EPOCH_ADV_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	async fn cluster_status_wire(&self) -> ClusterStatusWire {
		let membership = self.epoch_rx.borrow().clone();
		let mut peers: Vec<ClusterPeerWire> = membership
			.peers
			.iter()
			.map(|(node_id, caps)| ClusterPeerWire {
				node_id: node_id.id,
				address: caps.address.clone(),
				tier_bits: caps.tiers.as_u8(),
				role_bits: caps.roles.as_u8(),
			})
			.collect();
		peers.sort_by_key(|peer| peer.node_id);

		ClusterStatusWire {
			epoch: membership.epoch,
			leader: self.raft.current_leader().await,
			replication_factor: self.replication_factor,
			peers,
		}
	}

	fn current_epoch(&self) -> u64 {
		self.epoch_rx.borrow().epoch
	}

	async fn leader_hint(&self) -> Option<u32> {
		self.raft.current_leader().await
	}

	async fn is_leader(&self) -> bool {
		self.raft.current_leader().await == Some(self.local_node.id)
	}

	fn lease_response(
		&self,
		frame: &HrftFrame,
		status: LeaseStatus,
		lease: Option<&LeaseDescriptor>,
	) -> std::io::Result<HrftFrame> {
		let meta = AcquireLeaseResponseMeta {
			status,
			_pad: [0; 7],
		};
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = lease.map(|lease| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(lease));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);

		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::LEASE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		})
	}
}

#[async_trait]
impl ControlRingServer for WendelonControlServer {
	async fn handle_control_frame(&self, frame: HrftFrame) -> std::io::Result<Option<HrftFrame>> {
		let msg_type = MsgType(frame.header.msg_type);
		let resp = match msg_type {
			MsgType::STATUS_REQ => self.handle_status(frame).await?,
			MsgType::LEASE_REQ => self.handle_lease(frame).await?,
			MsgType::TRANSFER_REQ => self.handle_transfer(frame).await?,
			MsgType::EPOCH_ADV_REQ => self.handle_advance_epoch(frame).await?,
			_ => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("unsupported Control Ring msg_type=0x{:02x}", msg_type.0),
				))
			}
		};
		Ok(Some(resp))
	}
}

fn decode_epoch_batch(bytes: &[u8]) -> std::io::Result<arrow::record_batch::RecordBatch> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("epoch batch decode: {e}")))?;
	let batch = reader
		.next()
		.transpose()
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("epoch batch read: {e}")))?
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "epoch batch stream is empty"))?;
	Ok(batch)
}
