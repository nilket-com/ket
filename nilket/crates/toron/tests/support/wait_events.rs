//! Test-only passive wait events (ADR 626 Stage A). Shared by the fixture worker binary (through
//! `#[path]`) and the process tests. Everything here observes and forwards: no byte is changed, no
//! bound is added, and every transport or stream call returns exactly what the inner one did.
//!
//! One JSON object per line: `event`, `role`, `at_us` (the instant the observer captured, on this
//! process's steady clock since the log opened — every interval field is an exact difference of
//! two `at_us` values), `emitted_us` (when the line was formatted, same clock, at or after `at_us`)
//! and `unix_ms` (provenance only). Frame boundaries follow the wire framing (a `u32` little-endian
//! length, then that many envelope bytes) in both directions; each frame's envelope prefix
//! (protocol version, kind, identity) is decoded from its first bytes with the library's own types.
#![allow(dead_code)]

use std::io::Write as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use toron::distributed::identity::WorkerId;
use toron::distributed::worker_registry::{WorkerEndpoint, WorkerEntry};
use toron::error::QueryError;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::wire::{EnvelopeIdentity, MessageKind, ProtocolVersion};

tokio::task_local! {
	/// The dispatch a transport call belongs to, when an observed dispatcher set it.
	pub static DISPATCH_TASK: String;
	/// The served stream whose handling made a transport call, when the worker set it.
	pub static SERVED_STREAM: u64;
}

/// A read gap at or above this is counted separately (it is never a bound).
const LONG_GAP_US: u64 = 1_000_000;
/// Bytes of each frame kept for decoding its envelope prefix.
const PREFIX_BYTES: usize = 512;
/// ADR 628: frames up to this size are kept whole, so a `TaskComplete` can be decoded passively.
const WHOLE_FRAME_BYTES: u64 = 256 * 1024;

pub struct EventLog {
	file: Mutex<std::fs::File>,
	opened: Instant,
	next_stream: AtomicU64,
	/// ADR 627: in-process observers of every event after it is written (the probe times its
	/// signal from them). Passive: a closed observer is dropped, and nothing waits on one.
	observers: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>>,
	/// ADR 627: set once any write fails; the log is then incomplete instrumentation.
	failed: std::sync::atomic::AtomicBool,
}

impl EventLog {
	/// Opens a fresh log; an existing file refuses.
	pub fn create(path: &std::path::Path) -> std::io::Result<Arc<Self>> {
		let file = std::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(path)?;
		Ok(Arc::new(Self {
			file: Mutex::new(file),
			opened: Instant::now(),
			next_stream: AtomicU64::new(1),
			observers: Mutex::new(Vec::new()),
			failed: std::sync::atomic::AtomicBool::new(false),
		}))
	}

	pub fn now_us(&self) -> u64 {
		u64::try_from(self.opened.elapsed().as_micros()).unwrap_or(u64::MAX)
	}

	pub fn next_stream_id(&self) -> u64 {
		self.next_stream.fetch_add(1, Ordering::Relaxed)
	}

	/// Appends one event captured at `at_us`. A failed write is reported on stderr with the fixed
	/// marker `wait event log write failed` (the readers treat it as incomplete instrumentation) and
	/// otherwise ignored: the observed operation behaves exactly as it would unobserved.
	pub fn emit_at(&self, at_us: u64, event: &str, role: &str, mut fields: serde_json::Value) {
		let unix_ms = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|elapsed| elapsed.as_millis() as u64)
			.unwrap_or(0);
		if !fields.is_object() {
			fields = serde_json::json!({});
		}
		fields["event"] = event.into();
		fields["role"] = role.into();
		fields["at_us"] = at_us.into();
		fields["emitted_us"] = self.now_us().into();
		fields["unix_ms"] = unix_ms.into();
		let mut line = fields.to_string();
		line.push('\n');
		let written = self
			.file
			.lock()
			.map_err(|_| std::io::Error::other("event log lock poisoned"))
			.and_then(|mut file| file.write_all(line.as_bytes()));
		if let Err(error) = written {
			eprintln!("wait event log write failed: {error}");
			// An event that was not persisted is never delivered to an observer: nothing may act on
			// an event the record does not contain.
			self.failed.store(true, Ordering::SeqCst);
			return;
		}
		if let Ok(mut observers) = self.observers.lock() {
			observers.retain(|observer| observer.send(fields.clone()).is_ok());
		}
	}

	/// ADR 627: whether any write to this log has failed.
	pub fn failed(&self) -> bool {
		self.failed.load(Ordering::SeqCst)
	}

	/// ADR 627: every event emitted from now on, after its line is written.
	pub fn subscribe(&self) -> tokio::sync::mpsc::UnboundedReceiver<serde_json::Value> {
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		self.observers
			.lock()
			.expect("event log observers poisoned")
			.push(sender);
		receiver
	}

	pub fn emit(&self, event: &str, role: &str, fields: serde_json::Value) {
		self.emit_at(self.now_us(), event, role, fields);
	}
}

fn dispatch_task() -> Option<String> {
	DISPATCH_TASK.try_with(|task| task.clone()).ok()
}

fn served_stream() -> Option<u64> {
	SERVED_STREAM.try_with(|stream| *stream).ok()
}

fn worker_text(worker_id: &WorkerId) -> String {
	worker_id.get().as_u128().to_string()
}

/// The envelope's leading fields, in wire order; postcard reads only what these need.
#[derive(serde::Deserialize)]
struct EnvelopePrefix {
	protocol_version: ProtocolVersion,
	kind: MessageKind,
	identity: EnvelopeIdentity,
}

fn identity_json(identity: &EnvelopeIdentity) -> serde_json::Value {
	match identity {
		EnvelopeIdentity::Task(task) => serde_json::json!({ "task": {
			"query": task.query_id.get().to_string(),
			"stage": task.stage_id.get(),
			"task": task.task_id.get(),
			"attempt": task.attempt_id.get(),
		}}),
		EnvelopeIdentity::Partition(address) => serde_json::json!({ "partition": {
			"query": address.query_id.get().to_string(),
			"exchange": address.exchange_id.get(),
			"producer_stage": address.producer_stage_id.get(),
			"producer_task": address.producer_task_id.get(),
			"attempt": address.attempt_id.get(),
			"partition": address.partition_id.get(),
		}}),
		EnvelopeIdentity::Worker { worker_id, .. } => {
			serde_json::json!({ "worker": worker_text(worker_id) })
		}
		EnvelopeIdentity::Query(query) => serde_json::json!({ "query": query.get().to_string() }),
	}
}

fn address_json(address: &toron::distributed::task::PartitionAddress) -> serde_json::Value {
	serde_json::json!({
		"query": address.query_id.get().to_string(),
		"exchange": address.exchange_id.get(),
		"stage": address.producer_stage_id.get(),
		"task": address.producer_task_id.get(),
		"attempt": address.attempt_id.get(),
		"partition": address.partition_id.get(),
	})
}

/// ADR 628: the typed content of a whole frame (up to `WHOLE_FRAME_BYTES`), decoded passively:
/// a sealed producer's `TaskComplete` manifest, a `PullPartition` request, or its `PartitionMissing`
/// (typed reason) / `PartitionData` reply. `None` for any other kind, a truncated or larger frame, or
/// a payload that does not decode.
fn whole_frame_event(whole: &[u8], len: u64) -> Option<(&'static str, serde_json::Value)> {
	if len > WHOLE_FRAME_BYTES || whole.len() as u64 != len {
		return None;
	}
	let envelope = toron::wire::WireEnvelope::decode(whole).ok()?;
	match envelope.kind {
		MessageKind::TaskComplete => {
			task_complete_manifest(&envelope.payload).map(|m| ("task_complete_manifest", m))
		}
		MessageKind::PullPartition => {
			let p: toron::wire::PullPartitionPayload =
				toron::wire::decode_payload(&envelope.payload).ok()?;
			Some((
				"pull_partition_request",
				serde_json::json!({
					"address": address_json(&p.address),
					"expected_attempt": p.expected_attempt.get(),
				}),
			))
		}
		MessageKind::PartitionMissing => {
			let p: toron::wire::PartitionMissingPayload =
				toron::wire::decode_payload(&envelope.payload).ok()?;
			Some((
				"partition_missing_reply",
				serde_json::json!({
					"address": address_json(&p.address),
					"reason": format!("{:?}", p.reason),
				}),
			))
		}
		MessageKind::PartitionData => {
			let p: toron::wire::PartitionDataPayload =
				toron::wire::decode_payload(&envelope.payload).ok()?;
			Some((
				"partition_data_reply",
				serde_json::json!({
					"address": address_json(&p.address),
					"bytes": p.payload.0.len(),
				}),
			))
		}
		_ => None,
	}
}

/// The producer manifest a `TaskComplete` payload carries, or `None` (not a sealed producer).
fn task_complete_manifest(payload: &[u8]) -> Option<serde_json::Value> {
	let payload: toron::wire::TaskCompletePayload = toron::wire::decode_payload(payload).ok()?;
	let toron::distributed::task::TaskCompletionOutput::ShuffleProducer { manifest } =
		payload.output
	else {
		return None;
	};
	Some(serde_json::json!({
		"task": {
			"query": manifest.query_id.get().to_string(),
			"stage": manifest.stage_id.get(),
			"task": manifest.task_id.get(),
			"attempt": manifest.attempt_id.get(),
		},
		"exchange": manifest.exchange_id.get(),
		"producer_worker": manifest.producer_worker_id.get().as_u128().to_string(),
		"producer_endpoint": manifest.producer_endpoint.as_str(),
		"partitions": manifest.partitions.iter().map(|entry| serde_json::json!({
			"partition": entry.partition_id.get(),
			"rows": entry.row_count.get(),
			"bytes": entry.size_bytes.get(),
			"digest": format!("{:?}", entry.digest),
		})).collect::<Vec<_>>(),
	}))
}

/// The decoded prefix as event fields, or `undecoded` with the reason.
fn prefix_json(prefix: &[u8]) -> serde_json::Value {
	match postcard::take_from_bytes::<EnvelopePrefix>(prefix) {
		Ok((envelope, _)) => serde_json::json!({
			"protocol_version": envelope.protocol_version.get(),
			"kind": format!("{:?}", envelope.kind),
			"identity": identity_json(&envelope.identity),
		}),
		Err(error) => serde_json::json!({ "undecoded": error.to_string() }),
	}
}

/// A transport that records each dial (start, resolved or failed, elapsed) and observes the
/// stream it returns. `role` names the side: `pull` (a consumer's sealed pull) or `dispatch`.
/// The recorded interval covers connection establishment and stream open together.
pub struct ObservedTransport {
	pub inner: Arc<dyn ToronTransport>,
	pub log: Arc<EventLog>,
	pub role: &'static str,
}

impl ObservedTransport {
	async fn observe<F>(
		&self,
		op: &str,
		target: serde_json::Value,
		dial: F,
	) -> Result<Box<dyn ToronStream>, QueryError>
	where
		F: std::future::Future<Output = Result<Box<dyn ToronStream>, QueryError>>,
	{
		let stream_id = self.log.next_stream_id();
		let mut fields = serde_json::json!({ "stream": stream_id, "op": op, "target": target });
		if let Some(task) = dispatch_task() {
			fields["task"] = task.into();
		}
		if let Some(served) = served_stream() {
			fields["served_stream"] = served.into();
		}
		let started = self.log.now_us();
		self.log
			.emit_at(started, "dial_start", self.role, fields.clone());
		// ADR 627: a dial dropped before it resolves (an outer timeout, a cancelled caller) records
		// `dial_abandoned`; without this, the abandonment would leave no event at all.
		struct Abandoned<'a> {
			log: &'a EventLog,
			role: &'static str,
			fields: serde_json::Value,
			started: u64,
			armed: bool,
		}
		impl Drop for Abandoned<'_> {
			fn drop(&mut self) {
				if self.armed {
					let ended = self.log.now_us();
					let mut fields = self.fields.clone();
					fields["elapsed_us"] = ended.saturating_sub(self.started).into();
					self.log.emit_at(ended, "dial_abandoned", self.role, fields);
				}
			}
		}
		let mut guard = Abandoned {
			log: &self.log,
			role: self.role,
			fields: fields.clone(),
			started,
			armed: true,
		};
		let outcome = dial.await;
		guard.armed = false;
		let ended = self.log.now_us();
		fields["elapsed_us"] = ended.saturating_sub(started).into();
		match outcome {
			Ok(stream) => {
				self.log.emit_at(ended, "dial_resolved", self.role, fields);
				Ok(Box::new(ObservedStream::new(
					stream,
					self.log.clone(),
					self.role,
					stream_id,
				)))
			}
			Err(error) => {
				fields["error"] = error.to_string().into();
				self.log.emit_at(ended, "dial_failed", self.role, fields);
				Err(error)
			}
		}
	}
}

#[async_trait::async_trait]
impl ToronTransport for ObservedTransport {
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		self.observe(
			"connect",
			serde_json::json!({ "endpoint": address }),
			self.inner.connect(address),
		)
		.await
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.observe(
			"connect_peer",
			serde_json::json!({ "worker": worker_text(worker_id), "endpoint": endpoint.as_str() }),
			self.inner.connect_peer(worker_id, endpoint),
		)
		.await
	}

	async fn connect_worker(
		&self,
		worker: &WorkerEntry,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.observe(
			"connect_worker",
			serde_json::json!({ "worker": worker_text(&worker.worker_id), "endpoint": worker.endpoint.as_str() }),
			self.inner.connect_worker(worker),
		)
		.await
	}
}

/// One direction's framing: header bytes, the current frame's length and progress, and the first
/// bytes of its envelope.
#[derive(Default)]
struct Framing {
	frame: u64,
	header: Vec<u8>,
	payload_len: Option<u64>,
	payload_done: u64,
	prefix: Vec<u8>,
	prefix_emitted: bool,
	/// ADR 628: the whole payload of a small frame (up to `WHOLE_FRAME_BYTES`).
	whole: Vec<u8>,
	started_us: u64,
	header_done_us: u64,
	transfers: u64,
	last_us: Option<u64>,
	max_gap_us: u64,
	long_gaps: u64,
}

impl Framing {
	fn mid_frame(&self) -> bool {
		!self.header.is_empty() || self.payload_len.is_some()
	}
}

pub struct ObservedStream<S> {
	inner: S,
	log: Arc<EventLog>,
	role: &'static str,
	stream: u64,
	extra: serde_json::Value,
	first_read_us: Option<u64>,
	first_write_us: Option<u64>,
	bytes_read: u64,
	bytes_written: u64,
	reads: Framing,
	writes: Framing,
	ended: bool,
}

impl<S> ObservedStream<S> {
	pub fn new(inner: S, log: Arc<EventLog>, role: &'static str, stream: u64) -> Self {
		Self::with_fields(inner, log, role, stream, serde_json::json!({}))
	}

	/// `extra` is carried on every event of this stream (for example the remote address).
	pub fn with_fields(
		inner: S,
		log: Arc<EventLog>,
		role: &'static str,
		stream: u64,
		extra: serde_json::Value,
	) -> Self {
		Self {
			inner,
			log,
			role,
			stream,
			extra,
			first_read_us: None,
			first_write_us: None,
			bytes_read: 0,
			bytes_written: 0,
			reads: Framing::default(),
			writes: Framing::default(),
			ended: false,
		}
	}

	/// An accepted (served) stream, numbered from the log's own counter.
	pub fn served(inner: S, log: Arc<EventLog>, extra: serde_json::Value) -> Self {
		let stream = log.next_stream_id();
		let observed = Self::with_fields(inner, log, "served", stream, extra);
		observed.emit(
			observed.log.now_us(),
			"stream_accepted",
			serde_json::json!({}),
		);
		observed
	}

	pub fn stream_id(&self) -> u64 {
		self.stream
	}

	fn emit(&self, at_us: u64, event: &str, extra: serde_json::Value) {
		let mut fields = self.extra.clone();
		if !fields.is_object() {
			fields = serde_json::json!({});
		}
		fields["stream"] = self.stream.into();
		if let serde_json::Value::Object(map) = extra {
			for (key, value) in map {
				fields[key] = value;
			}
		}
		self.log.emit_at(at_us, event, self.role, fields);
	}

	/// Advances one direction's framing over `bytes` observed at `now`.
	fn observe_bytes(&mut self, read: bool, bytes: &[u8], now: u64) {
		let direction = if read { "read" } else { "written" };
		let mut events = Vec::new();
		{
			let framing = if read {
				&mut self.reads
			} else {
				&mut self.writes
			};
			if let Some(last) = framing.last_us {
				if framing.mid_frame() {
					let gap = now.saturating_sub(last);
					framing.max_gap_us = framing.max_gap_us.max(gap);
					if gap >= LONG_GAP_US {
						framing.long_gaps += 1;
					}
				}
			}
			framing.last_us = Some(now);
			framing.transfers += 1;
			let mut rest = bytes;
			while !rest.is_empty() {
				match framing.payload_len {
					None => {
						if framing.header.is_empty() {
							framing.started_us = now;
							framing.max_gap_us = 0;
							framing.long_gaps = 0;
							framing.transfers = 1;
						}
						let take = (4 - framing.header.len()).min(rest.len());
						framing.header.extend_from_slice(&rest[..take]);
						rest = &rest[take..];
						if framing.header.len() == 4 {
							let len = u64::from(u32::from_le_bytes([
								framing.header[0],
								framing.header[1],
								framing.header[2],
								framing.header[3],
							]));
							framing.header.clear();
							framing.payload_len = Some(len);
							framing.payload_done = 0;
							framing.prefix.clear();
							framing.prefix_emitted = false;
							framing.whole.clear();
							framing.header_done_us = now;
							events.push((
								format!("frame_header_{direction}"),
								serde_json::json!({ "frame": framing.frame, "payload_len": len,
									"header_us": now.saturating_sub(framing.started_us) }),
							));
						}
					}
					Some(len) => {
						let take = ((len - framing.payload_done).min(rest.len() as u64)) as usize;
						let keep = PREFIX_BYTES.saturating_sub(framing.prefix.len()).min(take);
						framing.prefix.extend_from_slice(&rest[..keep]);
						if len <= WHOLE_FRAME_BYTES {
							framing.whole.extend_from_slice(&rest[..take]);
						}
						framing.payload_done += take as u64;
						rest = &rest[take..];
						if !framing.prefix_emitted
							&& (framing.prefix.len() >= PREFIX_BYTES || framing.payload_done == len)
						{
							framing.prefix_emitted = true;
							let mut fields = prefix_json(&framing.prefix);
							fields["frame"] = framing.frame.into();
							events.push((format!("frame_envelope_{direction}"), fields));
						}
					}
				}
				if let Some(len) = framing.payload_len {
					if framing.payload_done == len {
						if !framing.prefix_emitted {
							framing.prefix_emitted = true;
							let mut fields = prefix_json(&framing.prefix);
							fields["frame"] = framing.frame.into();
							events.push((format!("frame_envelope_{direction}"), fields));
						}
						// ADR 628: typed content decoded passively from a whole frame at the
						// production observation boundary (a sealed producer's manifest; a
						// partition pull and its typed reply).
						if let Some((name, mut fields)) = whole_frame_event(&framing.whole, len) {
							fields["frame"] = framing.frame.into();
							events.push((format!("{name}_{direction}"), fields));
						}
						events.push((
							format!("frame_payload_{direction}"),
							serde_json::json!({
								"frame": framing.frame,
								"payload_len": len,
								"payload_us": now.saturating_sub(framing.header_done_us),
								"transfers": framing.transfers,
								"max_gap_us": framing.max_gap_us,
								"long_gaps": framing.long_gaps,
							}),
						));
						framing.frame += 1;
						framing.payload_len = None;
						framing.payload_done = 0;
					}
				}
			}
		}
		for (event, fields) in events {
			self.emit(now, &event, fields);
		}
	}

	fn end(&mut self, reason: &str, error: Option<String>) {
		if self.ended {
			return;
		}
		self.ended = true;
		let mut extra = serde_json::json!({
			"reason": reason,
			"bytes_read": self.bytes_read,
			"bytes_written": self.bytes_written,
			"frames_read": self.reads.frame,
			"frames_written": self.writes.frame,
			"partial_read_frame": self.reads.mid_frame(),
			"partial_written_frame": self.writes.mid_frame(),
		});
		if let Some(error) = error {
			extra["error"] = error.into();
		}
		self.emit(self.log.now_us(), "stream_end", extra);
	}
}

impl<S> Drop for ObservedStream<S> {
	fn drop(&mut self) {
		self.end("dropped", None);
	}
}

impl<S: AsyncRead + Unpin> AsyncRead for ObservedStream<S> {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		let capacity = buf.remaining();
		let before = buf.filled().len();
		let polled = Pin::new(&mut this.inner).poll_read(cx, buf);
		match &polled {
			Poll::Ready(Ok(())) => {
				let after = buf.filled().len();
				if after > before {
					let now = this.log.now_us();
					if this.first_read_us.is_none() {
						this.first_read_us = Some(now);
						this.emit(now, "first_byte_read", serde_json::json!({}));
					}
					this.bytes_read += (after - before) as u64;
					this.observe_bytes(true, &buf.filled()[before..after], now);
				} else if capacity > 0 {
					// Only a read with room that returns nothing is end of stream.
					this.end("eof", None);
				}
			}
			Poll::Ready(Err(error)) => this.end("read_error", Some(error.to_string())),
			Poll::Pending => {}
		}
		polled
	}
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ObservedStream<S> {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let this = self.get_mut();
		let polled = Pin::new(&mut this.inner).poll_write(cx, buf);
		match &polled {
			Poll::Ready(Ok(written)) if *written > 0 => {
				let now = this.log.now_us();
				if this.first_write_us.is_none() {
					this.first_write_us = Some(now);
					this.emit(now, "first_byte_written", serde_json::json!({}));
				}
				this.bytes_written += *written as u64;
				this.observe_bytes(false, &buf[..*written], now);
			}
			Poll::Ready(Err(error)) => this.end("write_error", Some(error.to_string())),
			_ => {}
		}
		polled
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.get_mut().inner).poll_flush(cx)
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
	}
}

#[cfg(test)]
mod observer_tests {
	use super::*;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};

	/// A scripted inner stream: each read returns the next chunk (or error); writes are captured.
	struct Script {
		chunks: std::collections::VecDeque<Result<Vec<u8>, std::io::ErrorKind>>,
		written: Vec<u8>,
		write_error: Option<std::io::ErrorKind>,
	}

	impl AsyncRead for Script {
		fn poll_read(
			self: Pin<&mut Self>,
			_cx: &mut Context<'_>,
			buf: &mut ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			let this = self.get_mut();
			match this.chunks.pop_front() {
				Some(Ok(chunk)) => {
					let take = chunk.len().min(buf.remaining());
					buf.put_slice(&chunk[..take]);
					if take < chunk.len() {
						this.chunks.push_front(Ok(chunk[take..].to_vec()));
					}
					Poll::Ready(Ok(()))
				}
				Some(Err(kind)) => Poll::Ready(Err(std::io::Error::new(kind, "scripted"))),
				None => Poll::Ready(Ok(())),
			}
		}
	}

	impl AsyncWrite for Script {
		fn poll_write(
			self: Pin<&mut Self>,
			_cx: &mut Context<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			let this = self.get_mut();
			if let Some(kind) = this.write_error {
				return Poll::Ready(Err(std::io::Error::new(kind, "scripted write")));
			}
			this.written.extend_from_slice(buf);
			Poll::Ready(Ok(buf.len()))
		}
		fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}
		fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}
	}

	fn script(chunks: Vec<Result<Vec<u8>, std::io::ErrorKind>>) -> Script {
		Script {
			chunks: chunks.into(),
			written: Vec::new(),
			write_error: None,
		}
	}

	fn fresh_log() -> (Arc<EventLog>, std::path::PathBuf) {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		let path = std::env::temp_dir().join(format!(
			"adr626-observer-{}-{}.jsonl",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed)
		));
		(EventLog::create(&path).expect("fresh log"), path)
	}

	fn read_events(path: &std::path::Path) -> Vec<serde_json::Value> {
		std::fs::read_to_string(path)
			.expect("read log")
			.lines()
			.map(|line| serde_json::from_str(line).expect("json line"))
			.collect()
	}

	fn named<'a>(events: &'a [serde_json::Value], name: &str) -> Vec<&'a serde_json::Value> {
		events
			.iter()
			.filter(|event| event["event"] == name)
			.collect()
	}

	/// A real envelope frame (length prefix plus postcard envelope) for a partition address.
	fn frame(kind: MessageKind, payload_len: usize) -> Vec<u8> {
		let address = toron::distributed::task::PartitionAddress {
			query_id: toron::distributed::identity::QueryId::from_u128(7),
			exchange_id: toron::distributed::identity::ExchangeId::new(1),
			producer_stage_id: toron::distributed::identity::StageId::new(2),
			producer_task_id: toron::distributed::identity::TaskId::new(3),
			attempt_id: toron::distributed::identity::AttemptId::first(),
			partition_id: toron::distributed::identity::PartitionId::new(4),
		};
		let envelope = toron::wire::WireEnvelope::try_new(
			kind,
			EnvelopeIdentity::Partition(address),
			None,
			None,
			vec![0xAB; payload_len],
		)
		.expect("envelope")
		.encode();
		let mut out = (envelope.len() as u32).to_le_bytes().to_vec();
		out.extend_from_slice(&envelope);
		out
	}

	/// A transport whose dials never resolve.
	struct NeverResolves;

	#[async_trait::async_trait]
	impl ToronTransport for NeverResolves {
		async fn connect(&self, _: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			futures::future::pending().await
		}
		async fn connect_peer(
			&self,
			_: &WorkerId,
			_: &WorkerEndpoint,
		) -> Result<Box<dyn ToronStream>, QueryError> {
			futures::future::pending().await
		}
		async fn connect_worker(
			&self,
			_: &WorkerEntry,
		) -> Result<Box<dyn ToronStream>, QueryError> {
			futures::future::pending().await
		}
	}

	/// ADR 627: a dial dropped by an outer timeout records `dial_abandoned` (with its elapsed time),
	/// exactly once, and nothing claims it resolved or failed.
	#[tokio::test]
	async fn a_dial_dropped_before_it_resolves_is_recorded_as_abandoned() {
		let (log, path) = fresh_log();
		let transport = ObservedTransport {
			inner: Arc::new(NeverResolves),
			log: log.clone(),
			role: "dispatch",
		};
		let dial = transport.connect("127.0.0.1:9");
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(50), dial)
				.await
				.is_err()
		);
		let events = read_events(&path);
		assert_eq!(named(&events, "dial_start").len(), 1);
		let abandoned = named(&events, "dial_abandoned");
		assert_eq!(abandoned.len(), 1, "{events:?}");
		assert!(abandoned[0]["elapsed_us"].as_u64().expect("elapsed") >= 50_000);
		assert!(
			named(&events, "dial_resolved").is_empty() && named(&events, "dial_failed").is_empty()
		);
	}

	#[tokio::test]
	async fn fragmented_header_and_payload_yield_one_frame_with_its_envelope() {
		let (log, path) = fresh_log();
		let bytes = frame(MessageKind::PartitionData, 3000);
		let chunks = vec![
			Ok(bytes[..1].to_vec()),
			Ok(bytes[1..3].to_vec()),
			Ok(bytes[3..10].to_vec()),
			Ok(bytes[10..].to_vec()),
		];
		let mut stream = ObservedStream::new(script(chunks), log, "pull", 1);
		let mut got = Vec::new();
		stream.read_to_end(&mut got).await.expect("read");
		assert_eq!(got, bytes, "bytes are forwarded unchanged");
		drop(stream);
		let events = read_events(&path);
		let header = named(&events, "frame_header_read");
		let payload = named(&events, "frame_payload_read");
		assert_eq!(header.len(), 1);
		assert_eq!(payload.len(), 1);
		assert_eq!(header[0]["payload_len"], (bytes.len() - 4) as u64);
		assert_eq!(payload[0]["payload_len"], header[0]["payload_len"]);
		let envelope = named(&events, "frame_envelope_read");
		assert_eq!(envelope[0]["kind"], "PartitionData");
		assert_eq!(envelope[0]["identity"]["partition"]["producer_task"], 3);
		assert_eq!(named(&events, "stream_end")[0]["reason"], "eof");
		assert_eq!(named(&events, "stream_end")[0]["partial_read_frame"], false);
	}

	#[tokio::test]
	async fn several_frames_in_one_read_are_each_reported() {
		let (log, path) = fresh_log();
		let mut bytes = frame(MessageKind::PartitionMissing, 0);
		bytes.extend(frame(MessageKind::PartitionData, 10));
		bytes.extend(frame(MessageKind::PartitionData, 20));
		let mut stream = ObservedStream::new(script(vec![Ok(bytes.clone())]), log, "pull", 1);
		let mut got = Vec::new();
		stream.read_to_end(&mut got).await.expect("read");
		assert_eq!(got, bytes);
		drop(stream);
		let events = read_events(&path);
		let frames = named(&events, "frame_payload_read")
			.iter()
			.map(|event| event["frame"].as_u64().expect("frame"))
			.collect::<Vec<_>>();
		assert_eq!(frames, vec![0, 1, 2]);
		let kinds = named(&events, "frame_envelope_read")
			.iter()
			.map(|event| event["kind"].as_str().expect("kind").to_string())
			.collect::<Vec<_>>();
		assert_eq!(
			kinds,
			vec!["PartitionMissing", "PartitionData", "PartitionData"]
		);
	}

	#[tokio::test]
	async fn a_read_error_is_forwarded_and_marks_the_partial_frame() {
		let (log, path) = fresh_log();
		let bytes = frame(MessageKind::PartitionData, 100);
		let chunks = vec![
			Ok(bytes[..20].to_vec()),
			Err(std::io::ErrorKind::ConnectionReset),
		];
		let mut stream = ObservedStream::new(script(chunks), log, "pull", 1);
		let mut got = vec![0u8; bytes.len()];
		let error = stream
			.read_exact(&mut got)
			.await
			.expect_err("the inner error is forwarded");
		assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
		drop(stream);
		let end = named(&read_events(&path), "stream_end")[0].clone();
		assert_eq!(end["reason"], "read_error");
		assert_eq!(end["partial_read_frame"], true);
	}

	#[tokio::test]
	async fn a_dropped_stream_ends_once_and_a_zero_capacity_read_is_not_eof() {
		let (log, path) = fresh_log();
		let mut stream = ObservedStream::new(script(vec![Ok(vec![1, 2])]), log, "pull", 1);
		let mut empty = [0u8; 0];
		let read = stream.read(&mut empty).await.expect("zero-capacity read");
		assert_eq!(read, 0);
		drop(stream);
		let ends = named(&read_events(&path), "stream_end")
			.into_iter()
			.cloned()
			.collect::<Vec<_>>();
		assert_eq!(ends.len(), 1);
		assert_eq!(ends[0]["reason"], "dropped");
	}

	#[tokio::test]
	async fn written_frames_are_decoded_and_write_errors_forwarded() {
		let (log, path) = fresh_log();
		let request = frame(MessageKind::PullPartition, 8);
		let mut stream = ObservedStream::new(script(Vec::new()), log.clone(), "pull", 1);
		stream.write_all(&request[..3]).await.expect("write");
		stream.write_all(&request[3..]).await.expect("write");
		assert_eq!(
			stream.inner.written, request,
			"written bytes forwarded unchanged"
		);
		drop(stream);
		let events = read_events(&path);
		assert_eq!(
			named(&events, "frame_envelope_written")[0]["kind"],
			"PullPartition"
		);
		assert_eq!(named(&events, "frame_payload_written").len(), 1);

		let (log, path) = fresh_log();
		let mut failing = script(Vec::new());
		failing.write_error = Some(std::io::ErrorKind::BrokenPipe);
		let mut stream = ObservedStream::new(failing, log, "pull", 2);
		let error = stream.write_all(&request).await.expect_err("forwarded");
		assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
		drop(stream);
		assert_eq!(
			named(&read_events(&path), "stream_end")[0]["reason"],
			"write_error"
		);
	}

	#[tokio::test]
	async fn interval_fields_are_exact_differences_of_captured_instants() {
		let (log, path) = fresh_log();
		let bytes = frame(MessageKind::PartitionData, 50_000);
		let chunks = bytes.chunks(4096).map(|chunk| Ok(chunk.to_vec())).collect();
		let mut stream = ObservedStream::new(script(chunks), log, "pull", 1);
		let mut got = Vec::new();
		stream.read_to_end(&mut got).await.expect("read");
		drop(stream);
		let events = read_events(&path);
		let header = named(&events, "frame_header_read")[0];
		let payload = named(&events, "frame_payload_read")[0];
		assert_eq!(
			payload["payload_us"].as_u64(),
			Some(payload["at_us"].as_u64().unwrap() - header["at_us"].as_u64().unwrap())
		);
		for event in &events {
			assert!(event["emitted_us"].as_u64() >= event["at_us"].as_u64());
		}
	}
}
