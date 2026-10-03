//! ADR 628 Stage A: phased, synthetic fault fixtures for the coordinator's traffic to one target
//! worker. Test-only and passive apart from the declared delay. Each fixture states exactly what it
//! delays:
//! - `connect_delay`: the caller's dial is held *before* the inner transport dials. No network bytes
//!   move during the delay; this is a synthetic caller delay, not network handshake latency (the
//!   relay controls that separately).
//! - `write_delay`: the stream's first write is held *before* its bytes reach the inner (observed)
//!   stream, so the bytes leave the coordinator later.
//! - `read_delay`: the stream's first read is delivered to the caller late. The inner (observed)
//!   stream records the frame when it arrives; only its delivery to the caller is delayed.
//! - `close_delay`: the stream's shutdown is held *before* the inner shutdown, i.e. before the
//!   terminal exchange's bytes leave.
//!
//! Every applied delay is recorded in the probe log (`fault_phase`) with the target, the phase, the
//! stream ordinal and its start and end on the log's clock.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use toron::distributed::identity::WorkerId;
use toron::distributed::query_reliance::ExchangeTag;
use toron::distributed::worker_registry::{WorkerEndpoint, WorkerEntry};
use toron::error::QueryError;
use toron::server::transport::{ToronStream, ToronTransport};

use crate::wait_events::EventLog;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseDelays {
	pub connect: Duration,
	pub write: Duration,
	pub read: Duration,
	pub close: Duration,
}

impl PhaseDelays {
	pub fn is_none(&self) -> bool {
		*self == Self::default()
	}

	pub fn evidence(&self) -> serde_json::Value {
		serde_json::json!({
			"connect_ms": self.connect.as_millis() as u64,
			"write_ms": self.write.as_millis() as u64,
			"read_ms": self.read.as_millis() as u64,
			"close_ms": self.close.as_millis() as u64,
		})
	}
}

/// Wraps the coordinator's transport; only `connect_worker` dials to `target` are affected, and
/// every one of them is (dispatch issuance as well as renewal and retirement).
pub struct PhaseDelayTransport {
	pub inner: Arc<dyn ToronTransport>,
	pub target: WorkerId,
	pub delays: PhaseDelays,
	pub log: Arc<EventLog>,
	pub ordinal: AtomicU64,
}

impl PhaseDelayTransport {
	pub fn new(
		inner: Arc<dyn ToronTransport>,
		target: WorkerId,
		delays: PhaseDelays,
		log: Arc<EventLog>,
	) -> Self {
		Self {
			inner,
			target,
			delays,
			log,
			ordinal: AtomicU64::new(0),
		}
	}
}

fn worker_text(worker: &WorkerId) -> String {
	worker.get().as_u128().to_string()
}

fn record(log: &EventLog, target: &str, phase: &str, ordinal: u64, started: u64) {
	log.emit_at(
		log.now_us(),
		"fault_phase",
		"dispatch",
		serde_json::json!({ "target": target, "phase": phase, "ordinal": ordinal, "started_us": started }),
	);
}

#[async_trait::async_trait]
impl ToronTransport for PhaseDelayTransport {
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		self.inner.connect(address).await
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.inner.connect_peer(worker_id, endpoint).await
	}

	async fn connect_worker(
		&self,
		worker: &WorkerEntry,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.dial(worker, None).await
	}

	async fn connect_worker_tagged(
		&self,
		worker: &WorkerEntry,
		tag: &ExchangeTag,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.dial(worker, Some(tag)).await
	}
}

/// The inner dial, forwarding an exchange tag when there is one.
async fn inner_dial(
	inner: &Arc<dyn ToronTransport>,
	worker: &WorkerEntry,
	tag: Option<&ExchangeTag>,
) -> Result<Box<dyn ToronStream>, QueryError> {
	match tag {
		Some(tag) => inner.connect_worker_tagged(worker, tag).await,
		None => inner.connect_worker(worker).await,
	}
}

impl PhaseDelayTransport {
	async fn dial(
		&self,
		worker: &WorkerEntry,
		tag: Option<&ExchangeTag>,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		if worker.worker_id != self.target || self.delays.is_none() {
			return inner_dial(&self.inner, worker, tag).await;
		}
		let ordinal = self.ordinal.fetch_add(1, Ordering::SeqCst);
		let target = worker_text(&self.target);
		if !self.delays.connect.is_zero() {
			// A dial dropped during the delay (an outer timeout) records the phase as cancelled.
			struct Pending<'a> {
				log: &'a EventLog,
				target: &'a str,
				ordinal: u64,
				started: u64,
				armed: bool,
			}
			impl Drop for Pending<'_> {
				fn drop(&mut self) {
					if self.armed {
						self.log.emit_at(
							self.log.now_us(),
							"fault_phase_cancelled",
							"dispatch",
							serde_json::json!({ "target": self.target, "phase": "connect", "ordinal": self.ordinal, "started_us": self.started }),
						);
					}
				}
			}
			let started = self.log.now_us();
			let mut pending = Pending {
				log: &self.log,
				target: &target,
				ordinal,
				started,
				armed: true,
			};
			tokio::time::sleep(self.delays.connect).await;
			pending.armed = false;
			record(&self.log, &target, "connect", ordinal, started);
		}
		let stream = inner_dial(&self.inner, worker, tag).await?;
		Ok(Box::new(PhaseDelayStream::wrap(
			stream,
			self.delays,
			self.log.clone(),
			target,
			ordinal,
		)))
	}
}

/// A one-shot delay: armed until its sleep has elapsed once.
struct Gate {
	delay: Duration,
	state: GateState,
}

enum GateState {
	Armed,
	Sleeping(Pin<Box<tokio::time::Sleep>>, u64),
	Open,
}

impl Gate {
	fn new(delay: Duration) -> Self {
		Self {
			delay,
			state: if delay.is_zero() {
				GateState::Open
			} else {
				GateState::Armed
			},
		}
	}

	/// Ready once the delay has elapsed; records the phase when it opens.
	fn poll_open(
		&mut self,
		cx: &mut Context<'_>,
		log: &EventLog,
		target: &str,
		phase: &str,
		ordinal: u64,
	) -> Poll<()> {
		loop {
			match &mut self.state {
				GateState::Open => return Poll::Ready(()),
				GateState::Armed => {
					self.state =
						GateState::Sleeping(Box::pin(tokio::time::sleep(self.delay)), log.now_us());
				}
				GateState::Sleeping(sleep, started) => {
					if sleep.as_mut().poll(cx).is_pending() {
						return Poll::Pending;
					}
					record(log, target, phase, ordinal, *started);
					self.state = GateState::Open;
				}
			}
		}
	}
}

pub struct PhaseDelayStream {
	inner: Box<dyn ToronStream>,
	delays: PhaseDelays,
	log: Arc<EventLog>,
	target: String,
	ordinal: u64,
	write: Gate,
	read: Gate,
	close: Gate,
	stash: Vec<u8>,
}

impl PhaseDelayStream {
	pub fn wrap(
		inner: Box<dyn ToronStream>,
		delays: PhaseDelays,
		log: Arc<EventLog>,
		target: String,
		ordinal: u64,
	) -> Self {
		Self {
			inner,
			delays,
			log,
			target,
			ordinal,
			write: Gate::new(delays.write),
			read: Gate::new(delays.read),
			close: Gate::new(delays.close),
			stash: Vec::new(),
		}
	}
}

/// A stream dropped while one of its gates is sleeping records that phase as cancelled; bytes still
/// withheld in the stash are recorded as discarded (never delivered).
impl Drop for PhaseDelayStream {
	fn drop(&mut self) {
		if !self.stash.is_empty() {
			self.log.emit_at(
				self.log.now_us(),
				"fault_read_discarded",
				"dispatch",
				serde_json::json!({ "target": self.target, "ordinal": self.ordinal, "bytes": self.stash.len() }),
			);
		}
		for (gate, phase) in [
			(&self.write, "write"),
			(&self.read, "read"),
			(&self.close, "close"),
		] {
			if let GateState::Sleeping(_, started) = &gate.state {
				self.log.emit_at(
					self.log.now_us(),
					"fault_phase_cancelled",
					"dispatch",
					serde_json::json!({ "target": self.target, "phase": phase, "ordinal": self.ordinal, "started_us": started }),
				);
			}
		}
	}
}

impl AsyncRead for PhaseDelayStream {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		// While the read gate is closed, the inner stream is still read (and observed) into a stash;
		// only delivery to the caller waits. Once open, the stash drains before the inner stream.
		if !matches!(this.read.state, GateState::Open) {
			if matches!(this.read.state, GateState::Armed) {
				let mut scratch = [0u8; 8192];
				let mut scratch_buf = ReadBuf::new(&mut scratch);
				match Pin::new(&mut this.inner).poll_read(cx, &mut scratch_buf) {
					Poll::Ready(Ok(())) if scratch_buf.filled().is_empty() => {
						return Poll::Ready(Ok(()));
					}
					Poll::Ready(Ok(())) => {
						// Arrived at the observed inner stream, withheld from the caller.
						this.stash.extend_from_slice(scratch_buf.filled());
						this.log.emit_at(
							this.log.now_us(),
							"fault_read_stashed",
							"dispatch",
							serde_json::json!({ "target": this.target, "ordinal": this.ordinal, "bytes": scratch_buf.filled().len() }),
						);
					}
					other => return other,
				}
			}
			if this
				.read
				.poll_open(cx, &this.log, &this.target, "read", this.ordinal)
				.is_pending()
			{
				return Poll::Pending;
			}
		}
		if !this.stash.is_empty() {
			let n = this.stash.len().min(buf.remaining());
			buf.put_slice(&this.stash[..n]);
			this.stash.drain(..n);
			// Delivered to the caller. Whether the caller then validated it and completed its
			// operation within its own bound is not observed here.
			this.log.emit_at(
				this.log.now_us(),
				"fault_read_delivered",
				"dispatch",
				serde_json::json!({ "target": this.target, "ordinal": this.ordinal, "bytes": n }),
			);
			return Poll::Ready(Ok(()));
		}
		Pin::new(&mut this.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for PhaseDelayStream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		data: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let this = self.get_mut();
		if this
			.write
			.poll_open(cx, &this.log, &this.target, "write", this.ordinal)
			.is_pending()
		{
			return Poll::Pending;
		}
		Pin::new(&mut this.inner).poll_write(cx, data)
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.get_mut().inner).poll_flush(cx)
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		if this
			.close
			.poll_open(cx, &this.log, &this.target, "close", this.ordinal)
			.is_pending()
		{
			return Poll::Pending;
		}
		Pin::new(&mut this.inner).poll_shutdown(cx)
	}
}

#[cfg(test)]
mod gate_tests {
	use super::*;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};

	fn log() -> (Arc<EventLog>, std::path::PathBuf) {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		let path = std::env::temp_dir().join(format!(
			"adr628-gates-{}-{}.jsonl",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed)
		));
		(EventLog::create(&path).expect("fresh log"), path)
	}

	fn events(path: &std::path::Path) -> Vec<serde_json::Value> {
		std::fs::read_to_string(path)
			.expect("read log")
			.lines()
			.map(|line| serde_json::from_str(line).expect("json"))
			.collect()
	}

	fn delays(write: u64, read: u64, close: u64) -> PhaseDelays {
		PhaseDelays {
			connect: Duration::ZERO,
			write: Duration::from_millis(write),
			read: Duration::from_millis(read),
			close: Duration::from_millis(close),
		}
	}

	/// Bytes pass through unchanged in both directions, in partial reads and writes, and each phase
	/// is delayed exactly once per stream, released at its delay.
	#[tokio::test(start_paused = true)]
	async fn a_gated_stream_delays_each_phase_once_and_passes_bytes_unchanged() {
		let (log, path) = log();
		let (client, mut peer) = tokio::io::duplex(64);
		let mut stream = PhaseDelayStream::wrap(
			Box::new(client),
			delays(30, 40, 50),
			log.clone(),
			"t".into(),
			0,
		);
		let payload: Vec<u8> = (0..200u32).map(|i| (i % 251) as u8).collect();
		let started = tokio::time::Instant::now();
		let echo = tokio::spawn(async move {
			let mut got = vec![0u8; 200];
			peer.read_exact(&mut got).await.expect("peer read");
			peer.write_all(&got).await.expect("peer echo");
			got
		});
		// Partial writes: the 64-byte duplex forces several polls; only the first waits.
		stream.write_all(&payload).await.expect("write");
		assert_eq!(started.elapsed(), Duration::from_millis(30));
		let mut back = vec![0u8; 200];
		let mut filled = 0;
		while filled < back.len() {
			let end = filled + 7.min(back.len() - filled);
			let n = stream.read(&mut back[filled..end]).await.expect("read");
			assert!(n > 0);
			filled += n;
		}
		assert_eq!(back, payload);
		assert_eq!(echo.await.expect("echo"), payload);
		assert_eq!(started.elapsed(), Duration::from_millis(30 + 40));
		stream.shutdown().await.expect("shutdown");
		assert_eq!(started.elapsed(), Duration::from_millis(30 + 40 + 50));
		// A second write after the gate opened is not delayed again.
		let before = tokio::time::Instant::now();
		let _ = stream.write(&[1]).await;
		assert_eq!(before.elapsed(), Duration::ZERO);
		let phases = events(&path)
			.into_iter()
			.filter(|e| e["event"] == "fault_phase")
			.map(|e| e["phase"].as_str().expect("phase").to_string())
			.collect::<Vec<_>>();
		assert_eq!(phases, ["write", "read", "close"]);
	}

	/// A stream dropped while a gate sleeps records the phase as cancelled, not released.
	#[tokio::test(start_paused = true)]
	async fn a_stream_dropped_mid_delay_records_the_phase_as_cancelled() {
		let (log, path) = log();
		let (client, _peer) = tokio::io::duplex(64);
		let mut stream = PhaseDelayStream::wrap(
			Box::new(client),
			delays(1_000, 0, 0),
			log.clone(),
			"t".into(),
			3,
		);
		let attempt =
			tokio::time::timeout(Duration::from_millis(10), stream.write_all(&[1, 2, 3])).await;
		assert!(attempt.is_err(), "the write is still gated");
		drop(stream);
		let recorded = events(&path);
		assert!(recorded.iter().all(|e| e["event"] != "fault_phase"));
		let cancelled = recorded
			.iter()
			.filter(|e| e["event"] == "fault_phase_cancelled")
			.collect::<Vec<_>>();
		assert_eq!(cancelled.len(), 1);
		assert_eq!(
			(
				cancelled[0]["phase"].as_str(),
				cancelled[0]["ordinal"].as_u64()
			),
			(Some("write"), Some(3))
		);
	}

	/// The read gate stashes what arrived early and delivers it, in order, once released; stashed,
	/// delivered and discarded byte counts reconcile (no loss, no double delivery).
	#[tokio::test(start_paused = true)]
	async fn early_bytes_are_stashed_and_delivered_after_the_read_gate() {
		let (log, path) = log();
		let (client, mut peer) = tokio::io::duplex(1024);
		let mut stream =
			PhaseDelayStream::wrap(Box::new(client), delays(0, 100, 0), log, "t".into(), 0);
		peer.write_all(b"hello world").await.expect("peer write");
		let started = tokio::time::Instant::now();
		let mut got = [0u8; 11];
		stream.read_exact(&mut got).await.expect("read");
		assert_eq!(&got, b"hello world");
		assert_eq!(started.elapsed(), Duration::from_millis(100));
		let sum = |name: &str| {
			events(&path)
				.iter()
				.filter(|e| e["event"] == name)
				.map(|e| e["bytes"].as_u64().expect("bytes"))
				.sum::<u64>()
		};
		assert_eq!(sum("fault_read_stashed"), 11);
		assert_eq!(sum("fault_read_delivered"), 11);
		drop(stream);
		assert_eq!(sum("fault_read_discarded"), 0);
	}

	/// Cancelled while bytes are withheld: they are recorded as discarded, never as delivered.
	#[tokio::test(start_paused = true)]
	async fn bytes_withheld_at_cancellation_are_discarded_not_delivered() {
		let (log, path) = log();
		let (client, mut peer) = tokio::io::duplex(1024);
		let mut stream =
			PhaseDelayStream::wrap(Box::new(client), delays(0, 1_000, 0), log, "t".into(), 0);
		peer.write_all(b"ack").await.expect("peer write");
		let mut got = [0u8; 3];
		assert!(
			tokio::time::timeout(Duration::from_millis(10), stream.read_exact(&mut got))
				.await
				.is_err()
		);
		drop(stream);
		let recorded = events(&path);
		let sum = |name: &str| {
			recorded
				.iter()
				.filter(|e| e["event"] == name)
				.map(|e| e["bytes"].as_u64().expect("bytes"))
				.sum::<u64>()
		};
		assert_eq!(
			(
				sum("fault_read_stashed"),
				sum("fault_read_delivered"),
				sum("fault_read_discarded")
			),
			(3, 0, 3)
		);
		assert!(
			recorded
				.iter()
				.any(|e| e["event"] == "fault_phase_cancelled" && e["phase"] == "read")
		);
	}
}
