use crate::metrics::{
	CdcMetricSource, CdcSubscriptionMetricResult, NetworkCompressionPath, global as metrics,
};
use crate::network::core::{HRFT_MAGIC, write_frame};
use crate::network::ipc::wire_encode_batch_for_path;
use crate::schema::{
	AtomicCdcCursor, CdcCheckpointMeta, CdcCursor, CdcFrameMeta, CdcFrameSource, CdcRejectCode,
	CdcRejectMeta, CdcSubscribeBound, CdcSubscriptionId, FRAME_FLAG_STREAM_END, FrameHeader,
	MsgType, TableObjectId, TenantId,
};
use crate::storage::CdcLiveEntry;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

pub const CDC_SUBSCRIPTION_BUFFER: usize = 128;

#[derive(Debug)]
pub struct SubscriptionHandle {
	pub subscription_id: CdcSubscriptionId,
	pub cursor_high_water: AtomicCdcCursor,
	pub bound: CdcSubscribeBound,
	pub logical_schema_fingerprint: crate::schema::LogicalSchemaFingerprint,
	pub send: mpsc::Sender<Arc<CdcLiveEntry>>,
}

impl SubscriptionHandle {
	pub fn new(
		subscription_id: CdcSubscriptionId,
		start_cursor: CdcCursor,
		bound: CdcSubscribeBound,
		logical_schema_fingerprint: crate::schema::LogicalSchemaFingerprint,
		send: mpsc::Sender<Arc<CdcLiveEntry>>,
	) -> Self {
		Self {
			subscription_id,
			cursor_high_water: AtomicCdcCursor::new(start_cursor),
			bound,
			logical_schema_fingerprint,
			send,
		}
	}
}

#[derive(Default)]
pub struct CdcSubscriptionRegistry {
	inner: DashMap<(TenantId, TableObjectId), Vec<Arc<SubscriptionHandle>>>,
	next_subscription_id: AtomicU64,
}

impl CdcSubscriptionRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn register(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		start_cursor: CdcCursor,
		bound: CdcSubscribeBound,
		logical_schema_fingerprint: crate::schema::LogicalSchemaFingerprint,
	) -> (Arc<SubscriptionHandle>, mpsc::Receiver<Arc<CdcLiveEntry>>) {
		let id = CdcSubscriptionId::new(
			u128::from(self.next_subscription_id.fetch_add(1, Ordering::Relaxed)) + 1,
		);
		let (send, recv) = mpsc::channel(CDC_SUBSCRIPTION_BUFFER);
		let handle = Arc::new(SubscriptionHandle::new(
			id,
			start_cursor,
			bound,
			logical_schema_fingerprint,
			send,
		));
		self.inner
			.entry((tenant_id, table_object_id))
			.or_default()
			.push(handle.clone());
		metrics().observe_cdc_subscription(CdcSubscriptionMetricResult::Opened);
		(handle, recv)
	}

	pub fn unregister(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		subscription_id: CdcSubscriptionId,
		result: CdcSubscriptionMetricResult,
	) -> bool {
		let removed = self.remove(tenant_id, table_object_id, subscription_id);
		if removed {
			metrics().observe_cdc_subscription(result);
		}
		removed
	}

	pub fn remove(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		subscription_id: CdcSubscriptionId,
	) -> bool {
		let Some(mut handles) = self.inner.get_mut(&(tenant_id, table_object_id)) else {
			return false;
		};
		let before = handles.len();
		handles.retain(|handle| handle.subscription_id != subscription_id);
		handles.len() != before
	}

	pub fn active_count(&self, tenant_id: TenantId, table_object_id: TableObjectId) -> usize {
		self.inner
			.get(&(tenant_id, table_object_id))
			.map(|handles| handles.len())
			.unwrap_or(0)
	}

	pub fn fanout_finalized_entry(&self, entry: Arc<CdcLiveEntry>) -> CdcFanoutOutcome {
		if !entry.is_finalized() {
			return CdcFanoutOutcome::default();
		}
		let key = (entry.tenant_id, entry.table_object_id);
		let Some(mut handles) = self.inner.get_mut(&key) else {
			return CdcFanoutOutcome::default();
		};

		let mut outcome = CdcFanoutOutcome::default();
		handles.retain(|handle| {
			if entry.cursor <= handle.cursor_high_water.load() {
				return true;
			}
			match handle.send.try_send(entry.clone()) {
				Ok(()) => {
					outcome.sent += 1;
					true
				}
				Err(mpsc::error::TrySendError::Full(_)) => {
					outcome.full += 1;
					true
				}
				Err(mpsc::error::TrySendError::Closed(_)) => {
					outcome.closed += 1;
					metrics().observe_cdc_subscription(CdcSubscriptionMetricResult::Cancelled);
					false
				}
			}
		});
		outcome
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CdcFanoutOutcome {
	pub sent: usize,
	pub full: usize,
	pub closed: usize,
}

pub async fn cdc_emit_frame<W: AsyncWrite + Unpin + ?Sized>(
	writer: &mut W,
	stream_id: u32,
	meta: &CdcFrameMeta,
	batch: &RecordBatch,
	flags: u16,
) -> std::io::Result<()> {
	let arrow = wire_encode_batch_for_path(batch, NetworkCompressionPath::CdcEvent)?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::CDC_FRAME.0,
		flags,
		stream_id,
		meta_len: std::mem::size_of::<CdcFrameMeta>() as u32,
		arrow_len: arrow.len() as u32,
		trailing_len: 0,
	};
	write_frame(
		writer,
		&header,
		bytemuck::bytes_of(meta),
		Some(&arrow),
		None,
	)
	.await?;
	let source = cdc_metric_source(meta.source().map_err(cdc_decode_error)?);
	metrics().observe_cdc_event_emitted(source, meta.event_count.get(), arrow.len() as u64);
	Ok(())
}

pub async fn cdc_emit_empty_frame<W: AsyncWrite + Unpin + ?Sized>(
	writer: &mut W,
	stream_id: u32,
	meta: &CdcFrameMeta,
	stream_end: bool,
) -> std::io::Result<()> {
	let flags = if stream_end { FRAME_FLAG_STREAM_END } else { 0 };
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::CDC_FRAME.0,
		flags,
		stream_id,
		meta_len: std::mem::size_of::<CdcFrameMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(writer, &header, bytemuck::bytes_of(meta), None, None).await
}

pub async fn cdc_emit_checkpoint<W: AsyncWrite + Unpin + ?Sized>(
	writer: &mut W,
	stream_id: u32,
	meta: &CdcCheckpointMeta,
	source: CdcFrameSource,
) -> std::io::Result<()> {
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::CDC_CHECKPOINT.0,
		flags: 0,
		stream_id,
		meta_len: std::mem::size_of::<CdcCheckpointMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(writer, &header, bytemuck::bytes_of(meta), None, None).await?;
	metrics().observe_cdc_checkpoint_emitted(cdc_metric_source(source));
	Ok(())
}

pub async fn cdc_emit_reject<W: AsyncWrite + Unpin + ?Sized>(
	writer: &mut W,
	stream_id: u32,
	meta: &CdcRejectMeta,
) -> std::io::Result<()> {
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::CDC_REJECT.0,
		flags: FRAME_FLAG_STREAM_END,
		stream_id,
		meta_len: std::mem::size_of::<CdcRejectMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(writer, &header, bytemuck::bytes_of(meta), None, None).await?;
	let code = meta.code().map_err(cdc_decode_error)?;
	metrics().observe_cdc_reject(code);
	metrics().observe_cdc_subscription(CdcSubscriptionMetricResult::Rejected);
	Ok(())
}

fn cdc_metric_source(source: CdcFrameSource) -> CdcMetricSource {
	match source {
		CdcFrameSource::Ring => CdcMetricSource::Ring,
		CdcFrameSource::Catchup => CdcMetricSource::Catchup,
	}
}

fn cdc_decode_error(err: crate::schema::CdcDecodeError) -> Error {
	Error::new(ErrorKind::InvalidData, err)
}

pub fn cdc_reject_meta(subscription_id: CdcSubscriptionId, code: CdcRejectCode) -> CdcRejectMeta {
	CdcRejectMeta::new(subscription_id, code)
}
