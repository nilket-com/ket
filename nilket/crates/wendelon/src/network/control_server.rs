//! Server-side Control Ring surface.
//!
//! This module hangs off the existing HRFT listener and
//! [`MsgRouter`](crate::network::router::MsgRouter) demux rather than
//! introducing a separate control-ring bind. It assumes callers are
//! already cluster-internal and mTLS-authenticated at the transport
//! layer; the handler here focuses on routing, wire compatibility, and
//! leader-aware Control Ring behavior.

use crate::client::topology::{ClusterPeerWire, ClusterStatusWire};
use crate::consensus::{
	AdvanceEpochRecord, EpochConfig, EpochPeer, MigrationLedgerEntry, MigrationState,
	SeedMigration, WendelonRaft, WendelonRequest, WendelonResponse,
};
use crate::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::ring::RingState;
use crate::network::router::ControlRingServer;
use crate::network::traits::{TransferLeadershipRequestMeta, TransferLeadershipResponseMeta};
use crate::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportTier, TransportTierSet,
};
use crate::network::wire;
use crate::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, FrameHeader, HybridTimestamp,
	LeaseDescriptor, LeaseStatus, MsgType, NodeId, TransferToken, epoch_entry_var_schema,
};
use arrow::array::{Array, ListArray, StringArray, UInt32Array};
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

pub const CONTROL_REASON_GRANTED: u8 = 0;
pub const CONTROL_REASON_NOT_LEADER: u8 = 1;
pub const CONTROL_REASON_EPOCH_STALE: u8 = 2;
pub const CONTROL_REASON_INVALID_TARGET: u8 = 3;
pub const CONTROL_REASON_UNSUPPORTED: u8 = 4;
pub const CONTROL_REASON_INVALID_CONFIG: u8 = 5;
pub const CONTROL_REASON_INTERNAL_ERROR: u8 = 6;
pub const CONTROL_REASON_INVALID_TOKEN: u8 = 7;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceEpochResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
}

pub struct WendelonControlServer {
	raft: WendelonRaft,
	state_machine: crate::consensus::raft_storage::WendelonStateMachine,
	local_node: NodeId,
	epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	replication_factor: u32,
	lease_ttl_cap_ms: u32,
	reconcile_lock: Mutex<()>,
}

impl WendelonControlServer {
	pub fn new(
		raft: WendelonRaft,
		state_machine: crate::consensus::raft_storage::WendelonStateMachine,
		local_node: NodeId,
		epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
		epoch_rx: watch::Receiver<Arc<crate::network::transport::MembershipEpoch>>,
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		replication_factor: u32,
		lease_ttl_cap_ms: u32,
	) -> Self {
		Self {
			raft,
			state_machine,
			local_node,
			epoch_tx,
			epoch_rx,
			factory,
			pool,
			replication_factor,
			lease_ttl_cap_ms,
			reconcile_lock: Mutex::new(()),
		}
	}

	pub async fn advance_membership_epoch(&self, next: MembershipEpoch) -> std::io::Result<()> {
		let _guard = self.reconcile_lock.lock().await;
		if !self.is_leader().await {
			return Err(not_leader_error(self.leader_hint().await));
		}

		self.reconcile_pending_epoch_locked().await?;

		let visible = self.epoch_rx.borrow().clone();
		if next.epoch == visible.epoch && membership_eq(&visible, &next) {
			return Ok(());
		}
		if next.epoch != visible.epoch + 1 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"epoch advance must be sequential: visible={}, requested={}",
					visible.epoch, next.epoch
				),
			));
		}

		self.commit_epoch_transition_locked(&visible, &next).await?;
		self.reconcile_pending_epoch_locked().await
	}

	pub fn spawn_epoch_reconciler(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
		tokio::spawn(async move {
			let mut ticker = tokio::time::interval(Duration::from_millis(50));
			loop {
				ticker.tick().await;
				if !self.is_leader().await {
					continue;
				}
				let _ = self.reconcile_pending_epoch().await;
			}
		})
	}

	pub async fn reconcile_pending_epoch(&self) -> std::io::Result<()> {
		let _guard = self.reconcile_lock.lock().await;
		self.reconcile_pending_epoch_locked().await
	}

	async fn handle_status(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if !frame.meta.is_empty() || frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"STATUS_REQ must not carry meta, arrow, or trailing payloads",
			));
		}
		let snapshot = self.cluster_status_wire().await;
		let meta = wire::encode_into(&self.pool, &snapshot)?;
		let leases = self.cluster_status_leases().await;
		let trailing = if leases.is_empty() {
			None
		} else {
			Some(wire::encode_into(&self.pool, &leases)?)
		};
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::STATUS_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0),
			},
			meta,
			arrow_payload: None,
			trailing,
		})
	}

	async fn handle_lease(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<AcquireLeaseRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LEASE_REQ meta too small",
			));
		}
		let req: AcquireLeaseRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<AcquireLeaseRequestMeta>()]);

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
		if frame.arrow_payload.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TRANSFER_REQ must not carry an Arrow payload",
			));
		}
		let req: TransferLeadershipRequestMeta = wire::decode(&frame.meta)?;
		let token = match self.decode_transfer_token(&frame) {
			Ok(token) => token,
			Err(_) => {
				return self.transfer_response(
					&frame,
					TransferLeadershipResponseMeta {
						accepted: false,
						reason_code: CONTROL_REASON_INVALID_TOKEN,
					},
					None,
				);
			}
		};
		if self.validate_transfer_request(&req, &token).is_err() {
			return self.transfer_response(
				&frame,
				TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INVALID_TOKEN,
				},
				None,
			);
		}

		let resp = if req.source_epoch != self.current_epoch() {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_EPOCH_STALE,
			}
		} else if req.to_leader == self.local_node {
			match self.accept_transfer_target(&token).await {
				Ok(()) => TransferLeadershipResponseMeta {
					accepted: true,
					reason_code: CONTROL_REASON_GRANTED,
				},
				Err(err) if err.kind() == ErrorKind::NotConnected => {
					TransferLeadershipResponseMeta {
						accepted: false,
						reason_code: CONTROL_REASON_NOT_LEADER,
					}
				}
				Err(_) => TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INTERNAL_ERROR,
				},
			}
		} else if !self.is_leader().await {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_NOT_LEADER,
			}
		} else if req.from_leader != self.local_node
			|| req.to_leader == self.local_node
			|| !self.transfer_target_known(&req.to_leader)
		{
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_INVALID_TARGET,
			}
		} else {
			match self.forward_transfer_request(&frame, &req, &token).await {
				Ok(resp) => resp,
				Err(err) if err.kind() == ErrorKind::NotConnected => {
					TransferLeadershipResponseMeta {
						accepted: false,
						reason_code: CONTROL_REASON_NOT_LEADER,
					}
				}
				Err(err) if err.kind() == ErrorKind::NotFound => TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INVALID_TARGET,
				},
				Err(_) => TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INTERNAL_ERROR,
				},
			}
		};
		let accepted = resp.accepted;
		self.transfer_response(&frame, resp, accepted.then_some(&token))
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
						let next_epoch = self.current_epoch().saturating_add(1);
						let visible = self.epoch_rx.borrow().clone();
						match membership_from_epoch_batch(&batch, next_epoch, &visible) {
							Ok(next) => match self.advance_membership_epoch(next).await {
								Ok(()) => AdvanceEpochResponseWire {
									accepted: true,
									reason_code: CONTROL_REASON_GRANTED,
									leader_hint: None,
								},
								Err(err) if err.kind() == ErrorKind::InvalidInput => {
									AdvanceEpochResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_INVALID_CONFIG,
										leader_hint: None,
									}
								}
								Err(err) if err.kind() == ErrorKind::NotConnected => {
									AdvanceEpochResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_NOT_LEADER,
										leader_hint: self.leader_hint().await,
									}
								}
								Err(_) => AdvanceEpochResponseWire {
									accepted: false,
									reason_code: CONTROL_REASON_INTERNAL_ERROR,
									leader_hint: None,
								},
							},
							Err(_) => AdvanceEpochResponseWire {
								accepted: false,
								reason_code: CONTROL_REASON_INVALID_CONFIG,
								leader_hint: None,
							},
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

	async fn commit_epoch_transition_locked(
		&self,
		from: &MembershipEpoch,
		to: &MembershipEpoch,
	) -> std::io::Result<()> {
		let from_cfg = epoch_config_from_membership(from);
		let to_cfg = epoch_config_from_membership(to);
		let snapshot = self.state_machine.state_snapshot().await;
		if let Some(existing) = snapshot.epoch_history.get(&to.epoch) {
			if existing != &to_cfg {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("epoch {} already committed with different config", to.epoch),
				));
			}
			return Ok(());
		}
		drop(snapshot);
		match self
			.raft
			.client_write(WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
				from: from_cfg,
				to: to_cfg,
			}))
			.await
		{
			Ok(_) => {}
			Err(_err) if !self.is_leader().await => {
				return Err(not_leader_error(self.leader_hint().await));
			}
			Err(err) => {
				return Err(Error::new(
					ErrorKind::Other,
					format!("advance_epoch commit failed: {err}"),
				));
			}
		}
		Ok(())
	}

	async fn reconcile_pending_epoch_locked(&self) -> std::io::Result<()> {
		let snapshot = self.state_machine.state_snapshot().await;
		let Some((&latest_epoch, latest_cfg)) = snapshot.epoch_history.iter().next_back() else {
			return Ok(());
		};
		let visible = self.epoch_rx.borrow().clone();
		if latest_epoch <= visible.epoch {
			return Ok(());
		}

		let previous_cfg = snapshot
			.epoch_history
			.get(&visible.epoch)
			.cloned()
			.unwrap_or_else(|| epoch_config_from_membership(&visible));
		let target = membership_from_epoch_config(latest_cfg);
		let expected = build_seed_batch(
			&membership_from_epoch_config(&previous_cfg),
			&target,
			self.replication_factor as usize,
			&snapshot,
		)?;
		let missing = missing_seed_batch(&expected, &snapshot.migrations)?;
		drop(snapshot);

		for seed in missing {
			if !self.is_leader().await {
				return Err(not_leader_error(self.leader_hint().await));
			}
			match self
				.raft
				.client_write(WendelonRequest::SeedMigration(seed))
				.await
			{
				Ok(_) => {}
				Err(_err) if !self.is_leader().await => {
					return Err(not_leader_error(self.leader_hint().await));
				}
				Err(err) => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("seed migration commit failed: {err}"),
					));
				}
			}
		}

		self.epoch_tx
			.send(Arc::new(target))
			.map_err(|_| Error::new(ErrorKind::BrokenPipe, "epoch watch receiver closed"))?;
		Ok(())
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

	async fn cluster_status_leases(&self) -> Vec<LeaseDescriptor> {
		let mut leases: Vec<LeaseDescriptor> = self
			.state_machine
			.state_snapshot()
			.await
			.leases
			.values()
			.copied()
			.collect();
		leases.sort_by_key(|lease| (lease.range_start, lease.range_end, lease.holder.id));
		leases
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

	fn transfer_response(
		&self,
		frame: &HrftFrame,
		resp: TransferLeadershipResponseMeta,
		token: Option<&TransferToken>,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, &resp)?;
		let trailing = token.map(|token| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(token));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TRANSFER_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta,
			arrow_payload: None,
			trailing,
		})
	}

	fn decode_transfer_token(&self, frame: &HrftFrame) -> std::io::Result<TransferToken> {
		let bytes = frame.trailing.as_deref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TRANSFER_REQ missing TransferToken trailing payload",
			)
		})?;
		if bytes.len() != std::mem::size_of::<TransferToken>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TRANSFER_REQ TransferToken size mismatch: got {}, expected {}",
					bytes.len(),
					std::mem::size_of::<TransferToken>()
				),
			));
		}
		Ok(*bytemuck::from_bytes(bytes))
	}

	fn validate_transfer_request(
		&self,
		req: &TransferLeadershipRequestMeta,
		token: &TransferToken,
	) -> std::io::Result<()> {
		if req.source_epoch == 0
			|| req.from_leader.id == 0
			|| req.to_leader.id == 0
			|| req.from_leader == req.to_leader
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"invalid transfer request identity fields",
			));
		}
		if token.source_epoch != req.source_epoch
			|| token.from_leader != req.from_leader
			|| token.to_leader != req.to_leader
			|| token.target_epoch != token.source_epoch.saturating_add(1)
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"transfer token does not match request metadata",
			));
		}
		Ok(())
	}

	fn transfer_target_known(&self, target: &NodeId) -> bool {
		self.epoch_rx
			.borrow()
			.peers
			.get(target)
			.map(|caps| caps.roles.contains(NodeRole::Control))
			.unwrap_or(false)
	}

	async fn forward_transfer_request(
		&self,
		frame: &HrftFrame,
		req: &TransferLeadershipRequestMeta,
		token: &TransferToken,
	) -> std::io::Result<TransferLeadershipResponseMeta> {
		let conn = self.factory.get_connection(req.to_leader).await?;
		let mut stream = conn.open_bi().await?;
		let meta = wire::encode(req)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TRANSFER_REQ.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: std::mem::size_of::<TransferToken>() as u32,
		};
		write_frame(
			&mut *stream,
			&header,
			&meta,
			None,
			Some(bytemuck::bytes_of(token)),
		)
		.await?;
		stream.finish().await?;
		let resp = read_frame(&mut *stream, &self.pool).await?;
		if resp.header.msg_type != MsgType::TRANSFER_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"transfer target returned wrong msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		wire::decode(&resp.meta)
	}

	async fn accept_transfer_target(&self, token: &TransferToken) -> std::io::Result<()> {
		self.raft
			.trigger()
			.elect()
			.await
			.map_err(|err| Error::new(ErrorKind::Other, format!("trigger_elect failed: {err}")))?;
		let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
		loop {
			if self.raft.current_leader().await == Some(self.local_node.id) {
				return Ok(());
			}
			if tokio::time::Instant::now() >= deadline {
				return Err(Error::new(
					ErrorKind::TimedOut,
					format!(
						"target node {} did not become leader for transfer from {} in epoch {}",
						self.local_node.id, token.from_leader.id, token.source_epoch
					),
				));
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
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
				));
			}
		};
		Ok(Some(resp))
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeedPlanSegment {
	start: u64,
	end: u64,
	logical_owner: NodeId,
	source_replicas: Vec<NodeId>,
	upstream_migration_id: Option<u64>,
	migration_hlc_floor: HybridTimestamp,
}

#[derive(Default)]
struct PeerOverrides {
	tier_bits: Option<u8>,
	role_bits: Option<u8>,
}

pub(crate) fn select_bootstrap_epoch(
	snapshot: &crate::consensus::raft_storage::SmState,
	fallback: &MembershipEpoch,
	rf: usize,
) -> std::io::Result<MembershipEpoch> {
	let Some((&latest_epoch, latest_cfg)) = snapshot.epoch_history.iter().next_back() else {
		return Ok(fallback.clone());
	};
	let latest = membership_from_epoch_config(latest_cfg);
	let Some((_, previous_cfg)) = snapshot.epoch_history.range(..latest_epoch).next_back() else {
		return Ok(latest);
	};
	let previous = membership_from_epoch_config(previous_cfg);
	let expected = build_seed_batch(&previous, &latest, rf, snapshot)?;
	if missing_seed_batch(&expected, &snapshot.migrations)?.is_empty() {
		Ok(latest)
	} else {
		Ok(previous)
	}
}

pub(crate) fn epoch_config_from_membership(epoch: &MembershipEpoch) -> EpochConfig {
	let mut peers = BTreeMap::new();
	for (node_id, caps) in &epoch.peers {
		peers.insert(
			node_id.id,
			EpochPeer {
				node_id: *node_id,
				address: caps.address.clone(),
				tier_bits: caps.tiers.as_u8(),
				role_bits: caps.roles.as_u8(),
			},
		);
	}
	EpochConfig {
		epoch: epoch.epoch,
		peers,
	}
}

pub(crate) fn membership_from_epoch_config(config: &EpochConfig) -> MembershipEpoch {
	let mut peers = HashMap::with_capacity(config.peers.len());
	for peer in config.peers.values() {
		peers.insert(
			peer.node_id,
			PeerCapabilities {
				address: peer.address.clone(),
				tiers: TransportTierSet::from_u8(peer.tier_bits),
				roles: NodeRoleSet::from_u8(peer.role_bits),
			},
		);
	}
	MembershipEpoch {
		epoch: config.epoch,
		peers,
	}
}

pub(crate) fn build_seed_batch(
	from: &MembershipEpoch,
	to: &MembershipEpoch,
	rf: usize,
	snapshot: &crate::consensus::raft_storage::SmState,
) -> std::io::Result<Vec<SeedMigration>> {
	let old_ring = RingState::build(from, rf);
	let new_ring = RingState::build(to, rf);
	let old_segments = old_ring.replica_segments();
	let new_segments = new_ring.replica_segments();
	if old_segments.is_empty() || new_segments.is_empty() {
		return Ok(Vec::new());
	}

	let mut planned: Vec<SeedPlanSegment> = Vec::new();
	let mut i = 0usize;
	let mut j = 0usize;
	while i < old_segments.len() && j < new_segments.len() {
		let old_seg = &old_segments[i];
		let new_seg = &new_segments[j];
		let start = old_seg.start.max(new_seg.start);
		let end = old_seg.end.min(new_seg.end);
		if start <= end {
			let old_primary = old_seg.replicas.first().copied();
			let new_primary = new_seg.replicas.first().copied();
			if let (Some(old_owner), Some(new_owner)) = (old_primary, new_primary) {
				if old_owner.id != new_owner.id {
					let (source_replicas, upstream_migration_id, migration_hlc_floor) =
						if let Some(upstream) =
							find_upstream_migration(snapshot, start, end, old_owner, to.epoch)
						{
							(
								upstream.source_replicas.clone(),
								Some(upstream.migration_id),
								upstream.migration_hlc_floor,
							)
						} else {
							(
								old_seg.replicas.clone(),
								None,
								HybridTimestamp {
									physical_ms: 0,
									logical: 0,
									node_id: 0,
								},
							)
						};
					push_or_merge_seed_segment(
						&mut planned,
						SeedPlanSegment {
							start,
							end,
							logical_owner: new_owner,
							source_replicas,
							upstream_migration_id,
							migration_hlc_floor,
						},
					);
				}
			}
		}
		if old_seg.end == end {
			i += 1;
		}
		if new_seg.end == end {
			j += 1;
		}
	}

	let mut out = Vec::with_capacity(planned.len());
	for segment in planned {
		out.push(SeedMigration {
			entry: MigrationLedgerEntry {
				migration_id: deterministic_migration_id(to.epoch, segment.start, segment.end),
				range_start: segment.start,
				range_end: segment.end,
				from_epoch: from.epoch,
				to_epoch: to.epoch,
				logical_owner: segment.logical_owner,
				upstream_migration_id: segment.upstream_migration_id,
				source_replicas: segment.source_replicas,
				migration_hlc_floor: segment.migration_hlc_floor,
				state: MigrationState::Pending,
				completed_by: None,
				completed_at_hlc: None,
			},
		});
	}
	Ok(out)
}

fn missing_seed_batch(
	expected: &[SeedMigration],
	existing: &HashMap<u64, MigrationLedgerEntry>,
) -> std::io::Result<Vec<SeedMigration>> {
	let mut missing = Vec::new();
	for seed in expected {
		match existing.get(&seed.entry.migration_id) {
			Some(entry) if entry == &seed.entry => {}
			Some(_) => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"migration_id {} already exists with different contents",
						seed.entry.migration_id
					),
				));
			}
			None => missing.push(seed.clone()),
		}
	}
	Ok(missing)
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

fn membership_from_epoch_batch(
	batch: &arrow::record_batch::RecordBatch,
	epoch: u64,
	current: &MembershipEpoch,
) -> std::io::Result<MembershipEpoch> {
	if batch.num_rows() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"epoch batch must contain exactly one row, got {}",
				batch.num_rows()
			),
		));
	}

	let voter_ids = required_list_u32(batch, "voter_ids")?;
	let voter_addresses = required_list_string(batch, "voter_addresses")?;
	let learner_ids = optional_list_u32(batch, "learner_ids")?;
	let learner_addresses = optional_list_string(batch, "learner_addresses")?;
	if voter_ids.len() != voter_addresses.len() || learner_ids.len() != learner_addresses.len() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"epoch batch id/address list lengths do not match",
		));
	}

	let mut overrides = HashMap::<u32, PeerOverrides>::new();
	for (key, value) in optional_list_string(batch, "meta_keys")?
		.into_iter()
		.zip(optional_list_string(batch, "meta_values")?.into_iter())
	{
		if let Some(node_id) = key
			.strip_prefix("node.")
			.and_then(|rest| rest.strip_suffix(".tier_bits"))
		{
			let bits = value.parse::<u8>().map_err(|e| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("invalid tier_bits override {value:?}: {e}"),
				)
			})?;
			overrides
				.entry(node_id.parse().map_err(|e| {
					Error::new(
						ErrorKind::InvalidInput,
						format!("invalid node id in meta key {key:?}: {e}"),
					)
				})?)
				.or_default()
				.tier_bits = Some(bits);
		} else if let Some(node_id) = key
			.strip_prefix("node.")
			.and_then(|rest| rest.strip_suffix(".role_bits"))
		{
			let bits = value.parse::<u8>().map_err(|e| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("invalid role_bits override {value:?}: {e}"),
				)
			})?;
			overrides
				.entry(node_id.parse().map_err(|e| {
					Error::new(
						ErrorKind::InvalidInput,
						format!("invalid node id in meta key {key:?}: {e}"),
					)
				})?)
				.or_default()
				.role_bits = Some(bits);
		}
	}

	let mut peers = HashMap::new();
	for (id, address) in voter_ids.into_iter().zip(voter_addresses.into_iter()) {
		insert_epoch_peer(&mut peers, current, &overrides, id, address)?;
	}
	for (id, address) in learner_ids.into_iter().zip(learner_addresses.into_iter()) {
		insert_epoch_peer(&mut peers, current, &overrides, id, address)?;
	}
	if peers.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"epoch batch defines no peers",
		));
	}

	Ok(MembershipEpoch { epoch, peers })
}

fn insert_epoch_peer(
	peers: &mut HashMap<NodeId, PeerCapabilities>,
	current: &MembershipEpoch,
	overrides: &HashMap<u32, PeerOverrides>,
	id: u32,
	address: String,
) -> std::io::Result<()> {
	let node_id = NodeId { id, _reserved: 0 };
	if peers.contains_key(&node_id) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("duplicate node {id} in epoch batch"),
		));
	}
	let mut caps = current
		.peers
		.get(&node_id)
		.cloned()
		.unwrap_or(PeerCapabilities {
			address: address.clone(),
			tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			roles: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control),
		});
	caps.address = address;
	if let Some(override_bits) = overrides.get(&id).and_then(|o| o.tier_bits) {
		caps.tiers = TransportTierSet::from_u8(override_bits);
	}
	if let Some(override_bits) = overrides.get(&id).and_then(|o| o.role_bits) {
		caps.roles = NodeRoleSet::from_u8(override_bits);
	}
	peers.insert(node_id, caps);
	Ok(())
}

fn required_list_u32(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<u32>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_u32_values(batch.column(idx).as_ref(), name, false)
}

fn optional_list_u32(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<u32>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_u32_values(batch.column(idx).as_ref(), name, true)
}

fn required_list_string(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<String>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_string_values(batch.column(idx).as_ref(), name, false)
}

fn optional_list_string(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<String>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_string_values(batch.column(idx).as_ref(), name, true)
}

fn list_u32_values(array: &dyn Array, name: &str, nullable: bool) -> std::io::Result<Vec<u32>> {
	let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} is not a ListArray"),
		)
	})?;
	if list.len() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} must contain exactly one row"),
		));
	}
	if nullable && list.is_null(0) {
		return Ok(Vec::new());
	}
	let values = list.value(0);
	let ints = values
		.as_any()
		.downcast_ref::<UInt32Array>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("epoch batch column {name:?} does not contain UInt32 values"),
			)
		})?;
	Ok((0..ints.len()).map(|idx| ints.value(idx)).collect())
}

fn list_string_values(
	array: &dyn Array,
	name: &str,
	nullable: bool,
) -> std::io::Result<Vec<String>> {
	let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} is not a ListArray"),
		)
	})?;
	if list.len() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} must contain exactly one row"),
		));
	}
	if nullable && list.is_null(0) {
		return Ok(Vec::new());
	}
	let values = list.value(0);
	let strings = values
		.as_any()
		.downcast_ref::<StringArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("epoch batch column {name:?} does not contain Utf8 values"),
			)
		})?;
	Ok((0..strings.len())
		.map(|idx| strings.value(idx).to_string())
		.collect())
}

fn find_upstream_migration(
	snapshot: &crate::consensus::raft_storage::SmState,
	range_start: u64,
	range_end: u64,
	old_primary: NodeId,
	to_epoch: u64,
) -> Option<MigrationLedgerEntry> {
	snapshot
		.migrations
		.values()
		.filter(|entry| {
			entry.state != MigrationState::Completed
				&& entry.to_epoch < to_epoch
				&& entry.logical_owner.id == old_primary.id
				&& entry.range_start <= range_end
				&& range_start <= entry.range_end
		})
		.cloned()
		.max_by_key(|entry| (entry.to_epoch, entry.migration_id))
}

fn push_or_merge_seed_segment(out: &mut Vec<SeedPlanSegment>, segment: SeedPlanSegment) {
	match out.last_mut() {
		Some(last)
			if last.end != u64::MAX
				&& last.end.saturating_add(1) == segment.start
				&& last.logical_owner == segment.logical_owner
				&& last.source_replicas == segment.source_replicas
				&& last.upstream_migration_id == segment.upstream_migration_id
				&& last.migration_hlc_floor == segment.migration_hlc_floor =>
		{
			last.end = segment.end;
		}
		_ => out.push(segment),
	}
}

fn deterministic_migration_id(to_epoch: u64, range_start: u64, range_end: u64) -> u64 {
	let mut input = [0u8; 24];
	input[..8].copy_from_slice(&to_epoch.to_le_bytes());
	input[8..16].copy_from_slice(&range_start.to_le_bytes());
	input[16..24].copy_from_slice(&range_end.to_le_bytes());
	let hash = blake3::hash(&input);
	u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("8 bytes"))
}

fn membership_eq(left: &MembershipEpoch, right: &MembershipEpoch) -> bool {
	epoch_config_from_membership(left) == epoch_config_from_membership(right)
}

fn not_leader_error(leader_hint: Option<u32>) -> Error {
	Error::new(
		ErrorKind::NotConnected,
		format!("not leader (leader_hint={leader_hint:?})"),
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::transport::{TransportTier, TransportTierSet};

	fn n(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn hlc(physical_ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: 7,
		}
	}

	fn membership(epoch: u64, node_ids: &[u32]) -> MembershipEpoch {
		let mut peers = HashMap::new();
		for id in node_ids {
			peers.insert(
				n(*id),
				PeerCapabilities {
					address: format!("127.0.0.1:{id}"),
					tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
					roles: NodeRoleSet::empty()
						.with(NodeRole::Data)
						.with(NodeRole::Control),
				},
			);
		}
		MembershipEpoch { epoch, peers }
	}

	#[test]
	fn build_seed_batch_reuses_upstream_lineage_for_chained_ranges() {
		let epoch1 = membership(1, &[1]);
		let epoch2 = membership(2, &[1, 2]);
		let epoch3 = membership(3, &[1, 3]);

		let mut snapshot = crate::consensus::raft_storage::SmState::default();
		let mut first_batch = build_seed_batch(&epoch1, &epoch2, 1, &snapshot).unwrap();
		assert!(!first_batch.is_empty());
		for seed in &mut first_batch {
			seed.entry.migration_hlc_floor = hlc(42);
			snapshot
				.migrations
				.insert(seed.entry.migration_id, seed.entry.clone());
		}

		let second_batch = build_seed_batch(&epoch2, &epoch3, 1, &snapshot).unwrap();
		assert!(!second_batch.is_empty());

		let chained: Vec<&SeedMigration> = second_batch
			.iter()
			.filter(|seed| seed.entry.upstream_migration_id.is_some())
			.collect();
		assert!(
			!chained.is_empty(),
			"expected at least one range to carry forward upstream lineage"
		);

		for seed in chained {
			let upstream_id = seed.entry.upstream_migration_id.unwrap();
			let upstream = snapshot.migrations.get(&upstream_id).unwrap();
			assert_eq!(upstream.logical_owner.id, 2);
			assert_ne!(seed.entry.logical_owner.id, upstream.logical_owner.id);
			assert_eq!(seed.entry.to_epoch, 3);
			assert_eq!(seed.entry.source_replicas, upstream.source_replicas);
			assert_eq!(seed.entry.migration_hlc_floor, upstream.migration_hlc_floor);
		}
	}
}
