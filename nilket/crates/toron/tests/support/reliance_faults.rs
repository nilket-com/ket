//! ADR 628 B1.C instrument: renewal-path faults and the reliance observer adapter. Test-only.
//!
//! `RenewalFaultTransport` is installed through the coordinator's renewal seam
//! (`with_renewal_transport`), so it sees ONLY renewal dials: dispatch (with its inline leases),
//! retirement, cancellation and peer pulls keep the dispatch transport. It is renewal-path isolation,
//! never a network partition. Faults fire on the target worker's renewal attempts selected by
//! `Firing`, by the ledger's attempt ordinal from the dial's exchange tag. Every application is
//! recorded in the probe log (`fault_renewal`), with what was realized.
//!
//! - `delays`: the Stage A phase delays (connect, write, read, close) on that attempt's stream;
//! - `withhold_reply`: the stream reaches the worker unchanged, and every byte the worker sends back
//!   is withheld from the coordinator and discarded (counted). The worker may have applied the lease
//!   AND written its reply: this is a coordinator-side lost ACK, not an unwritten worker reply;
//! - `refuse`: the dial fails with a typed TLS refusal (alert 42), as the QUIC transport reports one;
//! - `unreachable`: the dial never completes.

#![allow(dead_code)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use toron::distributed::identity::WorkerId;
use toron::distributed::query_reliance::{
	ExchangeId, ExchangeTag, RelianceEvent, RelianceObserver,
};
use toron::distributed::worker_registry::{WorkerEndpoint, WorkerEntry};
use toron::error::QueryError;
use toron::server::transport::{ToronStream, ToronTransport};

use crate::lease_faults::{PhaseDelayTransport, PhaseDelays};
use crate::wait_events::EventLog;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenewalFault {
	Delays(PhaseDelays),
	WithholdReply,
	Refuse,
	Unreachable,
}

impl RenewalFault {
	pub fn evidence(&self) -> serde_json::Value {
		match self {
			Self::Delays(delays) => {
				serde_json::json!({ "kind": "delays", "delays": delays.evidence() })
			}
			Self::WithholdReply => serde_json::json!({ "kind": "withhold_reply" }),
			Self::Refuse => serde_json::json!({ "kind": "refuse", "alert": 42 }),
			Self::Unreachable => serde_json::json!({ "kind": "unreachable" }),
		}
	}
}

/// Which of the target's renewal attempts (ledger ordinals from 1) the fault applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Firing {
	Every,
	From(u64),
	Only(u64),
}

impl Firing {
	fn fires(self, attempt: u64) -> bool {
		match self {
			Self::Every => true,
			Self::From(first) => attempt >= first,
			Self::Only(only) => attempt == only,
		}
	}

	pub fn evidence(&self) -> serde_json::Value {
		match self {
			Self::Every => serde_json::json!({ "kind": "every" }),
			Self::From(first) => serde_json::json!({ "kind": "from", "attempt": first }),
			Self::Only(only) => serde_json::json!({ "kind": "only", "attempt": only }),
		}
	}
}

pub struct RenewalFaultTransport {
	pub inner: Arc<dyn ToronTransport>,
	pub target: WorkerId,
	pub fault: RenewalFault,
	pub firing: Firing,
	pub log: Arc<EventLog>,
	/// Renewal dials this transport saw (any worker), and those with a fault applied.
	pub dials: AtomicU64,
	pub applied: AtomicU64,
}

impl RenewalFaultTransport {
	pub fn new(
		inner: Arc<dyn ToronTransport>,
		target: WorkerId,
		fault: RenewalFault,
		firing: Firing,
		log: Arc<EventLog>,
	) -> Self {
		Self {
			inner,
			target,
			fault,
			firing,
			log,
			dials: AtomicU64::new(0),
			applied: AtomicU64::new(0),
		}
	}
}

fn worker_text(worker: &WorkerId) -> String {
	worker.get().as_u128().to_string()
}

#[async_trait::async_trait]
impl ToronTransport for RenewalFaultTransport {
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		panic!("the renewal transport dials workers only, not {address}");
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		_endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		panic!("the renewal transport never makes peer dials ({worker_id:?})");
	}

	async fn connect_worker(
		&self,
		worker: &WorkerEntry,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		panic!(
			"an untagged dial to {:?} reached the renewal transport",
			worker.worker_id
		);
	}

	async fn connect_worker_tagged(
		&self,
		worker: &WorkerEntry,
		tag: &ExchangeTag,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.dials.fetch_add(1, Ordering::SeqCst);
		let ExchangeId::Renewal { attempt } = tag.exchange else {
			panic!("a dispatch dial reached the renewal transport: {tag:?}");
		};
		if worker.worker_id != self.target || !self.firing.fires(attempt) {
			return self.inner.connect_worker_tagged(worker, tag).await;
		}
		self.applied.fetch_add(1, Ordering::SeqCst);
		self.log.emit(
			"fault_renewal",
			"coordinator",
			serde_json::json!({
				"target": worker_text(&self.target), "attempt": attempt, "tag": tag.evidence(),
				"fault": self.fault.evidence(), "firing": self.firing.evidence(),
			}),
		);
		match self.fault {
			RenewalFault::Delays(delays) => {
				PhaseDelayTransport::new(
					Arc::clone(&self.inner),
					self.target,
					delays,
					Arc::clone(&self.log),
				)
				.connect_worker_tagged(worker, tag)
				.await
			}
			RenewalFault::WithholdReply => {
				let stream = self.inner.connect_worker_tagged(worker, tag).await?;
				Ok(Box::new(WithholdStream {
					inner: stream,
					log: Arc::clone(&self.log),
					target: worker_text(&self.target),
					attempt,
					withheld: 0,
					reported: AtomicBool::new(false),
				}))
			}
			RenewalFault::Refuse => Err(QueryError::Io(std::io::Error::new(
				std::io::ErrorKind::ConnectionRefused,
				toron::error::EstablishmentFailure::Tls {
					alert: 42,
					reason: "ADR 628 B1.C renewal refusal fixture".to_string(),
				},
			))),
			RenewalFault::Unreachable => std::future::pending().await,
		}
	}
}

/// Delivers nothing the worker sends: every byte read from the inner stream is counted and discarded.
/// Writes and the close pass through unchanged.
struct WithholdStream {
	inner: Box<dyn ToronStream>,
	log: Arc<EventLog>,
	target: String,
	attempt: u64,
	withheld: u64,
	reported: AtomicBool,
}

impl WithholdStream {
	fn report(&self, end: &str) {
		if !self.reported.swap(true, Ordering::SeqCst) {
			self.log.emit(
				"fault_reply_withheld",
				"coordinator",
				serde_json::json!({
					"target": self.target, "attempt": self.attempt, "withheld_bytes": self.withheld, "end": end,
				}),
			);
		}
	}
}

impl AsyncRead for WithholdStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		_buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let mut scratch = [0u8; 4096];
		loop {
			let mut discard = ReadBuf::new(&mut scratch);
			match Pin::new(&mut self.inner).poll_read(cx, &mut discard) {
				Poll::Ready(Ok(())) if discard.filled().is_empty() => {
					// The worker closed its side: report what was withheld; deliver nothing, ever.
					self.report("worker_eof");
					return Poll::Pending;
				}
				Poll::Ready(Ok(())) => {
					self.withheld += discard.filled().len() as u64;
				}
				Poll::Ready(Err(error)) => {
					self.report("read_error");
					return Poll::Ready(Err(error));
				}
				Poll::Pending => return Poll::Pending,
			}
		}
	}
}

impl AsyncWrite for WithholdStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

impl Drop for WithholdStream {
	fn drop(&mut self) {
		self.report("dropped");
	}
}

/// Writes every reliance event to the probe log as `reliance`, with the ledger's `seq`, the query and
/// the event's own fields. Observational: it never decides anything. A write failure marks the log
/// failed (the readers then refuse).
pub struct EventLogRelianceObserver {
	pub log: Arc<EventLog>,
}

impl RelianceObserver for EventLogRelianceObserver {
	fn record(&self, event: &RelianceEvent) {
		let mut fields = event.fields.clone();
		fields["seq"] = event.seq.into();
		fields["query"] = event.query.map(|q| q.get().to_string()).into();
		fields["kind"] = event.kind.into();
		self.log.emit("reliance", "coordinator", fields);
	}
}
