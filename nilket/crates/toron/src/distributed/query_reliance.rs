//! ADR 628 B1.A/B1.C: the coordinator's reliance on each worker's query lease.
//!
//! One ledger per query holds, for every worker the query has dispatched to, whether the
//! coordinator may currently rely on that worker's lease. From the coordinator-clock send marker
//! `t_send` of a request whose exchange completed `Applied`, the worker's lease cannot expire
//! before `B_w = t_send + D − M`, where `D = floor(L·(1 − ρ_c)/(1 + ρ_w))`. That claim is
//! CONDITIONAL on the declared clock-rate assumption (`ρ`), on the namespace surviving, and on the
//! worker not restarting. Every action that depends on a worker samples the clock under the ledger
//! lock and refuses at or after its bound (equality counts as lapsed).
//!
//! The coordinator clock is `CLOCK_BOOTTIME`: steady (never decreasing), including suspended time. It is
//! frequency-adjusted like `CLOCK_MONOTONIC`, so it does not remove the `ρ` assumption. Timers
//! may not fire during suspend, so every resumed action re-checks its gate.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::distributed::identity::{QueryId, WorkerId};
use crate::distributed::namespace_lifecycle::LeaseIgnored;
use crate::distributed::worker_registry::WorkerEntry;

/// Nanoseconds on the coordinator's boot clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BootInstant(u128);

impl BootInstant {
	pub const fn from_nanos(nanos: u128) -> Self {
		Self(nanos)
	}

	pub const fn as_nanos(self) -> u128 {
		self.0
	}

	pub fn checked_add(self, span: Duration) -> Option<Self> {
		self.0.checked_add(span.as_nanos()).map(Self)
	}

	/// `self − earlier`, or zero when `earlier` is not before `self`.
	pub fn saturating_since(self, earlier: Self) -> Duration {
		duration_from_nanos(self.0.saturating_sub(earlier.0))
	}
}

fn duration_from_nanos(nanos: u128) -> Duration {
	let secs = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
	Duration::new(secs, (nanos % 1_000_000_000) as u32)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockFault {
	/// `clock_gettime` failed; the value is its errno.
	ReadFailed(i32),
	/// The clock returned an invalid `timespec`.
	Invalid,
	/// A sample was earlier than one already observed by this ledger.
	Regressed,
}

impl fmt::Display for ClockFault {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::ReadFailed(errno) => write!(f, "boot clock read failed (errno {errno})"),
			Self::Invalid => write!(f, "boot clock returned an invalid value"),
			Self::Regressed => write!(f, "boot clock regressed"),
		}
	}
}

/// The coordinator's reliance clock. Production uses [`BootClock`]; tests inject a clock.
pub trait RelianceClock: Send + Sync + fmt::Debug {
	fn now(&self) -> Result<BootInstant, ClockFault>;
}

/// `CLOCK_BOOTTIME` through `clock_gettime`. There is deliberately no fallback to `Instant`.
#[derive(Debug, Default, Clone, Copy)]
pub struct BootClock;

impl RelianceClock for BootClock {
	fn now(&self) -> Result<BootInstant, ClockFault> {
		let mut ts = libc::timespec {
			tv_sec: 0,
			tv_nsec: 0,
		};
		// SAFETY: a valid, initialized out-parameter for clock_gettime.
		let rc = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
		if rc != 0 {
			return Err(ClockFault::ReadFailed(
				std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
			));
		}
		let secs = u128::try_from(ts.tv_sec).map_err(|_| ClockFault::Invalid)?;
		let nanos = u128::try_from(ts.tv_nsec).map_err(|_| ClockFault::Invalid)?;
		if nanos >= 1_000_000_000 {
			return Err(ClockFault::Invalid);
		}
		secs.checked_mul(1_000_000_000)
			.and_then(|s| s.checked_add(nanos))
			.map(BootInstant)
			.ok_or(ClockFault::Invalid)
	}
}

/// A clock set by hand, for deterministic tests (including suspend jumps and faults).
#[derive(Debug)]
pub struct SettableClock {
	state: Mutex<Result<BootInstant, ClockFault>>,
}

impl SettableClock {
	pub fn at(nanos: u128) -> Self {
		Self {
			state: Mutex::new(Ok(BootInstant(nanos))),
		}
	}

	pub fn set(&self, nanos: u128) {
		*self.state.lock().expect("settable clock poisoned") = Ok(BootInstant(nanos));
	}

	pub fn advance(&self, by: Duration) {
		let mut state = self.state.lock().expect("settable clock poisoned");
		if let Ok(now) = *state {
			*state = Ok(BootInstant(now.0 + by.as_nanos()));
		}
	}

	pub fn fail(&self, fault: ClockFault) {
		*self.state.lock().expect("settable clock poisoned") = Err(fault);
	}
}

impl RelianceClock for SettableClock {
	fn now(&self) -> Result<BootInstant, ClockFault> {
		*self.state.lock().expect("settable clock poisoned")
	}
}

/// A reliance clock that follows tokio's clock from a fixed origin. Production uses
/// [`BootClock`]. This is for simulated time (paused-clock tests), where it moves in step with
/// tokio's timers.
#[derive(Debug)]
pub struct TokioRelianceClock {
	origin: tokio::time::Instant,
	offset_nanos: u128,
}

impl TokioRelianceClock {
	pub fn new() -> Self {
		Self {
			origin: tokio::time::Instant::now(),
			// Starts well above zero so arithmetic near the origin is unremarkable.
			offset_nanos: 1_000_000_000_000,
		}
	}
}

impl Default for TokioRelianceClock {
	fn default() -> Self {
		Self::new()
	}
}

impl RelianceClock for TokioRelianceClock {
	fn now(&self) -> Result<BootInstant, ClockFault> {
		Ok(BootInstant(
			self.offset_nanos
				+ tokio::time::Instant::now()
					.duration_since(self.origin)
					.as_nanos(),
		))
	}
}

/// Declared assumptions and provisional operational budgets (ADR 628 B1.C). The budgets are
/// operational choices from B1.B's distributions, not tail bounds; safety comes only from the bound
/// and the gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelianceParams {
	/// The declared lease term `L` the worker applies.
	pub term: Duration,
	/// Worker clock rate assumption, parts per million.
	pub rho_worker_ppm: u32,
	/// Coordinator clock rate assumption, parts per million.
	pub rho_coordinator_ppm: u32,
	/// Margin `M`, in coordinator-clock units.
	pub margin: Duration,
	pub connect_budget: Duration,
	/// From the send marker to a validated reply.
	pub ack_budget: Duration,
	/// A whole exchange: connect through close (renewal) or through the validated handshake reply
	/// (dispatch). Never task execution.
	pub whole_budget: Duration,
	pub cadence: Duration,
	pub backoff_initial: Duration,
	pub backoff_max: Duration,
}

pub const PPM: u128 = 1_000_000;

impl RelianceParams {
	/// The declared production values for a given lease term.
	pub const fn production(term: Duration) -> Self {
		Self {
			term,
			rho_worker_ppm: 500,
			rho_coordinator_ppm: 500,
			margin: Duration::from_secs(1),
			connect_budget: Duration::from_millis(2000),
			ack_budget: Duration::from_millis(2000),
			whole_budget: Duration::from_millis(3000),
			cadence: Duration::from_secs(5),
			backoff_initial: Duration::from_millis(250),
			backoff_max: Duration::from_millis(2000),
		}
	}

	/// `D − M` in whole nanoseconds of the coordinator clock, where
	/// `D = floor(L_ns · (10^6 − ρ_c) / (10^6 + ρ_w))`. The computation uses checked `u128`
	/// arithmetic. It fails on overflow, on `ρ ≥ 10^6`, or when `D − M` is not positive.
	pub fn reliance_span(&self) -> Result<Duration, RelianceCause> {
		let rho_c = u128::from(self.rho_coordinator_ppm);
		let rho_w = u128::from(self.rho_worker_ppm);
		if rho_c >= PPM || rho_w >= PPM || self.term.is_zero() {
			return Err(RelianceCause::Arithmetic);
		}
		let d = self
			.term
			.as_nanos()
			.checked_mul(PPM - rho_c)
			.and_then(|n| n.checked_div(PPM + rho_w))
			.ok_or(RelianceCause::Arithmetic)?;
		let span = d
			.checked_sub(self.margin.as_nanos())
			.filter(|span| *span > 0)
			.ok_or(RelianceCause::Arithmetic)?;
		Ok(duration_from_nanos(span))
	}
}

/// How a query ended without a reliance failure. Sealed into the ledger at the moment it happens,
/// so nothing after it can authorize a new action, refresh a bound or commit success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryEnd {
	/// The query's own work failed. The original outward error is kept by the caller; this is its
	/// description.
	Error(String),
	/// The query was cancelled (client, SQL cancel, timeout or disconnect).
	Cancelled,
	/// The query's owner was dropped before an outcome.
	Dropped,
	/// The query was marked terminal before an outcome.
	Retired,
	/// The result stream ended while the query was still open.
	DriverLost,
}

impl fmt::Display for QueryEnd {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Error(m) => write!(f, "query failed: {m}"),
			Self::Cancelled => write!(f, "query cancelled"),
			Self::Dropped => write!(f, "query owner dropped"),
			Self::Retired => write!(f, "query retired before an outcome"),
			Self::DriverLost => write!(f, "result stream ended before the query committed"),
		}
	}
}

/// Why a worker can no longer be relied on, or why the query can no longer rely on anything. The
/// typed cause is always retained; [`Refusal::task_failure_kind`] gives the outward kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelianceCause {
	/// The bound passed with no usable refresh.
	Lapsed { bound: BootInstant },
	/// A validated `Ignored` reply (B0: terminal at the reply).
	Ignored(LeaseIgnored),
	/// A malformed or uncorrelated reply: wrong version, kind, query, term or frame.
	Protocol(String),
	/// Authentication or TLS refusal.
	Auth(String),
	/// The coordinator clock could not be read or regressed.
	Clock(ClockFault),
	/// The bound could not be computed (overflow, `D − M ≤ 0`, an unrepresentable instant).
	Arithmetic,
	/// The first handshake to this worker failed (transport, budget, or a reply completed past its
	/// budget) while it was still `Issuing`. There is no prior reliance to keep, and the query fails.
	IssuanceFailed(String),
	/// A detected restart, retirement or invalidation.
	Invalidated(String),
	/// The query ended for a reason other than reliance.
	QueryEnded(QueryEnd),
}

impl fmt::Display for RelianceCause {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Lapsed { bound } => write!(f, "lease reliance lapsed at boot {}ns", bound.0),
			Self::Ignored(reason) => write!(f, "lease ignored by worker: {reason:?}"),
			Self::Protocol(m) => write!(f, "lease protocol violation: {m}"),
			Self::Auth(m) => write!(f, "lease exchange refused: {m}"),
			Self::Clock(fault) => write!(f, "{fault}"),
			Self::Arithmetic => write!(f, "lease reliance bound not computable"),
			Self::IssuanceFailed(m) => write!(f, "first lease issuance failed: {m}"),
			Self::Invalidated(m) => write!(f, "worker namespace invalidated: {m}"),
			Self::QueryEnded(end) => write!(f, "{end}"),
		}
	}
}

/// The reliance state of one (query, worker).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerReliance {
	Unissued,
	/// A first issuance is in flight. There is no bound yet.
	Issuing,
	/// Reliance holds until `bound`. `marker` is the send marker that supports this (max) bound.
	Reliable {
		bound: BootInstant,
		marker: BootInstant,
	},
	Lapsed {
		bound: BootInstant,
	},
	Terminal(RelianceCause),
}

/// The query's outcome. `Committed` and `Failed` are mutually exclusive and irreversible. A failure
/// names its worker when it came from one; a clock, arithmetic or query-level end names none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOutcome {
	Open,
	Committed,
	Failed {
		worker: Option<WorkerId>,
		cause: RelianceCause,
	},
}

/// Which kind of exchange produced a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangePath {
	/// An inline `QueryLease` on a task stream (every dispatch sends one).
	Dispatch,
	/// A periodic renewal exchange (whole exchange, close included).
	Renewal,
}

/// One lease exchange, identified by the ledger: a renewal attempt (per-worker ordinal from 1) or one
/// dispatch's inline lease (its ticket, per-query ordinal from 1). The ledger assigns both, so they
/// name actual requests; a transport that records dials receives the same identity through
/// [`ExchangeTag`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeId {
	Renewal { attempt: u64 },
	Dispatch { ticket: u64 },
}

impl ExchangeId {
	pub fn path(self) -> ExchangePath {
		match self {
			Self::Renewal { .. } => ExchangePath::Renewal,
			Self::Dispatch { .. } => ExchangePath::Dispatch,
		}
	}

	pub fn evidence(self) -> serde_json::Value {
		match self {
			Self::Renewal { attempt } => {
				serde_json::json!({ "kind": "renewal", "attempt": attempt })
			}
			Self::Dispatch { ticket } => {
				serde_json::json!({ "kind": "dispatch", "ticket": ticket })
			}
		}
	}
}

/// The identity of one exchange's dial, passed to the transport (`connect_worker_tagged`). Production
/// transports ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExchangeTag {
	pub query: Option<QueryId>,
	pub worker: WorkerId,
	pub exchange: ExchangeId,
}

impl ExchangeTag {
	pub fn evidence(&self) -> serde_json::Value {
		serde_json::json!({
			"query": self.query.map(|q| q.get().to_string()),
			"worker": worker_text(self.worker),
			"exchange": self.exchange.evidence(),
		})
	}
}

/// A budget phase of one exchange. Connect runs from the exact connect start; ACK from the send
/// marker, capped by whole; whole from the exchange start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
	Connect,
	Ack,
	Whole,
}

impl Phase {
	fn label(self) -> &'static str {
		match self {
			Self::Connect => "connect",
			Self::Ack => "ack",
			Self::Whole => "whole",
		}
	}
}

/// Where a use gate runs, and what it authorizes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseSite {
	/// `start_task`, `publish`, `accept_before_close` or `accept_after_close`.
	pub site: &'static str,
	pub ticket: Option<u64>,
	pub task: Option<String>,
	/// The per-task ordinal of a published batch.
	pub batch: Option<u64>,
}

impl UseSite {
	pub fn new(site: &'static str) -> Self {
		Self {
			site,
			ticket: None,
			task: None,
			batch: None,
		}
	}
}

/// The start of one renewal attempt, as admitted by the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenewalAttemptStart {
	pub attempt: u64,
	pub started: BootInstant,
	pub connect_by: BootInstant,
	pub whole_by: BootInstant,
}

/// One recorded reliance event. `seq` is assigned under the ledger lock, in transition order, from 1
/// with no gaps. Delivery happens after the lock is released, so a reader orders by `seq`, never by
/// arrival. Time fields are the boot-clock samples the decision itself used.
#[derive(Debug, Clone, PartialEq)]
pub struct RelianceEvent {
	pub seq: u64,
	pub query: Option<QueryId>,
	pub kind: &'static str,
	pub fields: serde_json::Value,
}

/// Records reliance events for an instrument. Observational: it records and never decides lease
/// policy. It runs on the caller's task outside the ledger lock, so its own cost and any panic are the
/// instrument's (a panic propagates to the caller and fails the instrumented run).
pub trait RelianceObserver: Send + Sync {
	fn record(&self, event: &RelianceEvent);
}

/// A refusal at a gate or at enrollment: the action must not proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
	pub worker: Option<WorkerId>,
	pub cause: RelianceCause,
}

impl Refusal {
	/// The failure table's outward kind: a clock or arithmetic fault is local (`Internal`); a query
	/// that ended otherwise was cancelled from the dispatcher's view (its own error is reported by
	/// its owner); every worker cause is `WorkerLost { w }`.
	pub fn task_failure_kind(&self) -> crate::error::TaskFailureKind {
		use crate::error::TaskFailureKind;
		match (&self.cause, self.worker) {
			(RelianceCause::Clock(_) | RelianceCause::Arithmetic, _)
			| (RelianceCause::QueryEnded(QueryEnd::DriverLost), _) => TaskFailureKind::Internal {
				reason: format!("query lease reliance unavailable: {}", self.cause),
			},
			(RelianceCause::QueryEnded(_), _) => TaskFailureKind::Cancelled,
			(_, Some(worker_id)) => TaskFailureKind::WorkerLost { worker_id },
			(_, None) => TaskFailureKind::Internal {
				reason: format!("query lease reliance unavailable: {}", self.cause),
			},
		}
	}

	/// The outward query error for this refusal. A local fault is the existing `Internal` kind.
	pub fn query_error(&self) -> crate::error::QueryError {
		use crate::error::{QueryError, RuntimeError, TaskFailureKind};
		match self.task_failure_kind() {
			TaskFailureKind::Internal { reason } => QueryError::Internal { message: reason },
			kind => QueryError::Runtime(RuntimeError::TaskFailure { kind }),
		}
	}
}

/// A validated `Applied` checked against its exchange's budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
	/// Processed within its budget. The installed (max) bound, which a late or older reply leaves
	/// unchanged.
	Bound(BootInstant),
	/// Completed after its budget: it refreshes nothing (recorded as an overrun). The existing bound
	/// is kept and the caller's retry policy applies within it.
	Late,
}

/// What a renewal loop should do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewalPlan {
	/// Stop: the worker is no longer `Reliable`, renewal is closed, or the query is sealed.
	Stop,
	/// Sleep until this instant (the next due renewal, or the bound when nothing fits), unless the
	/// ledger changes first.
	SleepUntil(BootInstant),
	/// Start one attempt now. It is admitted because `now + whole_budget < bound`.
	Attempt,
}

fn ns(instant: BootInstant) -> serde_json::Value {
	serde_json::Value::from(u64::try_from(instant.as_nanos()).unwrap_or(u64::MAX))
}

fn opt_ns(instant: Option<BootInstant>) -> serde_json::Value {
	instant.map_or(serde_json::Value::Null, ns)
}

fn worker_text(worker: WorkerId) -> String {
	worker.get().as_u128().to_string()
}

fn cause_text(cause: &RelianceCause) -> String {
	cause.to_string()
}

fn state_text(state: Option<&WorkerReliance>) -> &'static str {
	match state {
		None => "absent",
		Some(WorkerReliance::Unissued) => "unissued",
		Some(WorkerReliance::Issuing) => "issuing",
		Some(WorkerReliance::Reliable { .. }) => "reliable",
		Some(WorkerReliance::Lapsed { .. }) => "lapsed",
		Some(WorkerReliance::Terminal(_)) => "terminal",
	}
}

fn bound_of(state: Option<&WorkerReliance>) -> Option<BootInstant> {
	match state {
		Some(WorkerReliance::Reliable { bound, .. } | WorkerReliance::Lapsed { bound }) => {
			Some(*bound)
		}
		_ => None,
	}
}

fn outcome_text(outcome: &QueryOutcome) -> &'static str {
	match outcome {
		QueryOutcome::Open => "open",
		QueryOutcome::Committed => "committed",
		QueryOutcome::Failed { .. } => "failed",
	}
}

#[derive(Debug)]
struct Entry {
	worker: WorkerEntry,
	state: WorkerReliance,
	/// First issuances to this worker currently in flight (dispatch handshakes not yet resolved).
	issuing_in_flight: u32,
	/// Whether a renewal loop has been requested for this worker.
	loop_requested: bool,
	/// Renewal attempts admitted for this worker (the last attempt ordinal).
	attempts: u64,
}

#[derive(Debug)]
struct Ledger {
	workers: HashMap<WorkerId, Entry>,
	outcome: QueryOutcome,
	last_sample: Option<BootInstant>,
	/// Exchanges that finished after their admitted budget (recorded, never a guarantee).
	budget_overruns: u64,
	enroller: Option<tokio::sync::mpsc::UnboundedSender<WorkerEntry>>,
	/// Set when the renewal owner is dropped or shut down: no renewal exchange is admitted after it.
	renewal_closed: bool,
	/// A reliance failure that moved the query from `Open` to `Failed`, not yet reported to the
	/// failure hook. It is reported after the lock is released.
	pending_failure: Option<Option<WorkerId>>,
	/// Dispatch tickets issued (the last ticket ordinal).
	tickets: u64,
	/// Whether an observer is installed; events are built only then.
	observing: bool,
	/// The last event sequence number assigned.
	seq: u64,
	/// Events recorded under the lock, delivered after it is released.
	events: Vec<RelianceEvent>,
}

/// Called once, outside the ledger lock, when the query fails on reliance (a lapse, a fatal outcome,
/// or a local clock or arithmetic fault, which names no worker). The coordinator uses it to stop
/// every dispatcher.
pub type FailureHook = Arc<dyn Fn(Option<WorkerId>) + Send + Sync>;

/// Reports a pending failure and delivers recorded events after the ledger guard in the same scope has
/// been released. Declare it BEFORE taking the ledger lock: locals drop in reverse order.
struct FireOnDrop<'a>(&'a QueryReliance);

impl Drop for FireOnDrop<'_> {
	fn drop(&mut self) {
		self.0.deliver_events();
		self.0.fire_pending();
	}
}

/// The per-query reliance ledger.
pub struct QueryReliance {
	params: RelianceParams,
	/// `D − M`, computed once. `Err` makes every reliance impossible (fatal local).
	span: Result<Duration, RelianceCause>,
	clock: Arc<dyn RelianceClock>,
	ledger: Mutex<Ledger>,
	changed: Notify,
	failure_hook: Mutex<Option<FailureHook>>,
	query: std::sync::OnceLock<QueryId>,
	observer: Mutex<Option<Arc<dyn RelianceObserver>>>,
}

impl fmt::Debug for QueryReliance {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("QueryReliance")
			.field("params", &self.params)
			.field("span", &self.span)
			.field("clock", &self.clock)
			.field("ledger", &self.ledger)
			.field("query", &self.query)
			.finish_non_exhaustive()
	}
}

impl QueryReliance {
	pub fn new(params: RelianceParams, clock: Arc<dyn RelianceClock>) -> Self {
		Self {
			span: params.reliance_span(),
			params,
			clock,
			ledger: Mutex::new(Ledger {
				workers: HashMap::new(),
				outcome: QueryOutcome::Open,
				last_sample: None,
				budget_overruns: 0,
				enroller: None,
				renewal_closed: false,
				pending_failure: None,
				tickets: 0,
				observing: false,
				seq: 0,
				events: Vec::new(),
			}),
			changed: Notify::new(),
			failure_hook: Mutex::new(None),
			query: std::sync::OnceLock::new(),
			observer: Mutex::new(None),
		}
	}

	/// The query this ledger belongs to (for tags and events).
	pub fn set_query(&self, query: QueryId) {
		let _ = self.query.set(query);
	}

	pub fn query(&self) -> Option<QueryId> {
		self.query.get().copied()
	}

	/// Installs an instrument observer. Events are recorded from now on, starting at sequence 1.
	pub fn set_observer(&self, observer: Arc<dyn RelianceObserver>) {
		*self.observer.lock().expect("reliance observer poisoned") = Some(observer);
		self.lock().observing = true;
	}

	/// The tag for one exchange's dial.
	pub fn tag(&self, worker: WorkerId, exchange: ExchangeId) -> ExchangeTag {
		ExchangeTag {
			query: self.query(),
			worker,
			exchange,
		}
	}

	pub fn set_failure_hook(&self, hook: FailureHook) {
		*self.failure_hook.lock().expect("failure hook poisoned") = Some(hook);
	}

	fn fire_pending(&self) {
		let pending = self.lock().pending_failure.take();
		if let Some(worker) = pending {
			let hook = self
				.failure_hook
				.lock()
				.expect("failure hook poisoned")
				.clone();
			if let Some(hook) = hook {
				hook(worker);
			}
		}
	}

	/// Records one event under the lock (only while observing).
	fn note(
		&self,
		ledger: &mut Ledger,
		kind: &'static str,
		fields: impl FnOnce() -> serde_json::Value,
	) {
		if !ledger.observing {
			return;
		}
		ledger.seq += 1;
		let seq = ledger.seq;
		ledger.events.push(RelianceEvent {
			seq,
			query: self.query(),
			kind,
			fields: fields(),
		});
	}

	/// Delivers recorded events outside the lock. Concurrent deliveries may interleave; `seq` is the
	/// order.
	fn deliver_events(&self) {
		let events = {
			let mut ledger = self.lock();
			if ledger.events.is_empty() {
				return;
			}
			std::mem::take(&mut ledger.events)
		};
		let observer = self
			.observer
			.lock()
			.expect("reliance observer poisoned")
			.clone();
		if let Some(observer) = observer {
			for event in &events {
				observer.record(event);
			}
		}
	}

	pub fn params(&self) -> &RelianceParams {
		&self.params
	}

	/// Where a worker's renewal loop is requested the first time it becomes `Reliable`.
	pub fn set_enroller(&self, enroller: tokio::sync::mpsc::UnboundedSender<WorkerEntry>) {
		self.lock().enroller = Some(enroller);
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, Ledger> {
		self.ledger.lock().expect("query reliance ledger poisoned")
	}

	/// Samples the clock with the ledger lock held, refusing any sample below one already observed.
	fn sample(&self, ledger: &mut Ledger) -> Result<BootInstant, ClockFault> {
		let now = self.clock.now()?;
		if ledger.last_sample.is_some_and(|last| now < last) {
			return Err(ClockFault::Regressed);
		}
		ledger.last_sample = Some(now);
		Ok(now)
	}

	/// Samples under the lock; a fault fails the query (fail closed) and is returned as a refusal.
	fn sample_or_fail(&self, ledger: &mut Ledger) -> Result<BootInstant, Refusal> {
		self.sample(ledger)
			.map_err(|fault| self.fail_locked(ledger, None, RelianceCause::Clock(fault)))
	}

	pub fn outcome(&self) -> QueryOutcome {
		self.lock().outcome.clone()
	}

	pub fn state(&self, worker: WorkerId) -> Option<WorkerReliance> {
		self.lock().workers.get(&worker).map(|e| e.state.clone())
	}

	pub fn budget_overruns(&self) -> u64 {
		self.lock().budget_overruns
	}

	/// Whether `now ≤ deadline` for one exchange's budget phase, sampled under the lock. A completion
	/// after its deadline is recorded as a budget overrun. A clock fault fails the query.
	pub fn within(
		&self,
		worker: WorkerId,
		exchange: ExchangeId,
		phase: Phase,
		deadline: BootInstant,
	) -> Result<bool, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let sampled = self.sample_or_fail(&mut ledger);
		let within = sampled.as_ref().ok().map(|now| *now <= deadline);
		if within == Some(false) {
			ledger.budget_overruns += 1;
		}
		let at = sampled.as_ref().ok().copied();
		let cause = sampled.as_ref().err().map(|r| cause_text(&r.cause));
		self.note(&mut ledger, "phase", || {
			serde_json::json!({
				"worker": worker_text(worker), "exchange": exchange.evidence(), "phase": phase.label(),
				"deadline_ns": ns(deadline), "at_ns": opt_ns(at), "within": within, "cause": cause,
			})
		});
		sampled.map(|now| now <= deadline)
	}

	/// Resolves when the ledger changes (a refresh, a state transition, or a seal).
	pub async fn changed(&self) {
		self.changed.notified().await;
	}

	/// Arms a change notification before the caller re-reads the ledger, so no change between the
	/// read and the wait is missed.
	pub fn changed_notified(&self) -> tokio::sync::futures::Notified<'_> {
		self.changed.notified()
	}

	/// Records a fatal, lapse or local-fault outcome. The query fails unless it is already sealed.
	/// Must be called with the lock held. Returns the refusal.
	fn fail_locked(
		&self,
		ledger: &mut Ledger,
		worker: Option<WorkerId>,
		cause: RelianceCause,
	) -> Refusal {
		if ledger.outcome == QueryOutcome::Committed {
			// Context only: a committed result is never reconsidered.
			return Refusal { worker, cause };
		}
		if let Some(id) = worker
			&& let Some(entry) = ledger.workers.get_mut(&id)
		{
			entry.state = match &cause {
				RelianceCause::Lapsed { bound } => WorkerReliance::Lapsed { bound: *bound },
				other => WorkerReliance::Terminal(other.clone()),
			};
			if !matches!(cause, RelianceCause::Lapsed { .. }) {
				let text = cause_text(&cause);
				self.note(
					ledger,
					"terminal",
					|| serde_json::json!({ "worker": worker_text(id), "cause": text }),
				);
			}
		}
		if ledger.outcome == QueryOutcome::Open {
			ledger.outcome = QueryOutcome::Failed {
				worker,
				cause: cause.clone(),
			};
			ledger.pending_failure = Some(worker);
			let text = cause_text(&cause);
			self.note(
				ledger,
				"query_failed",
				|| serde_json::json!({ "worker": worker.map(worker_text), "cause": text }),
			);
		}
		self.changed.notify_waiters();
		Refusal { worker, cause }
	}

	/// Lapses every `Reliable` worker whose bound has passed at `now`, recording where the lapse was
	/// observed. Returns the first refusal.
	fn enforce_bounds_locked(
		&self,
		ledger: &mut Ledger,
		now: BootInstant,
		site: &'static str,
	) -> Option<Refusal> {
		let mut expired: Vec<(WorkerId, BootInstant)> = ledger
			.workers
			.iter()
			.filter_map(|(id, e)| match e.state {
				WorkerReliance::Reliable { bound, .. } if now >= bound => Some((*id, bound)),
				_ => None,
			})
			.collect();
		expired.sort_by_key(|(id, _)| id.get().as_u128());
		let mut first = None;
		for (id, bound) in expired {
			self.note(ledger, "lapsed", || {
				serde_json::json!({
					"worker": worker_text(id), "bound_ns": ns(bound), "observed_ns": ns(now), "site": site,
				})
			});
			let refusal = self.fail_locked(ledger, Some(id), RelianceCause::Lapsed { bound });
			first.get_or_insert(refusal);
		}
		first
	}

	/// The refusal implied by a sealed or failed query, if any. Checked BEFORE any clock sample, so
	/// a committed result is never reconsidered because of a later clock fault.
	fn sealed_refusal(ledger: &Ledger, worker: Option<WorkerId>) -> Option<Refusal> {
		match &ledger.outcome {
			QueryOutcome::Open => None,
			QueryOutcome::Committed => Some(Refusal {
				worker,
				cause: RelianceCause::Invalidated("query already committed".to_string()),
			}),
			QueryOutcome::Failed { worker, cause } => Some(Refusal {
				worker: *worker,
				cause: cause.clone(),
			}),
		}
	}

	/// The worker's own state as a refusal, if it cannot be relied on (`Lapsed`, `Terminal`).
	fn worker_refusal(ledger: &Ledger, worker: WorkerId) -> Option<Refusal> {
		match ledger.workers.get(&worker).map(|e| &e.state) {
			Some(WorkerReliance::Lapsed { bound }) => Some(Refusal {
				worker: Some(worker),
				cause: RelianceCause::Lapsed { bound: *bound },
			}),
			Some(WorkerReliance::Terminal(cause)) => Some(Refusal {
				worker: Some(worker),
				cause: cause.clone(),
			}),
			_ => None,
		}
	}

	/// The DIAL gate. Before a dispatch dials `worker`: refuses if the query is sealed, samples the
	/// clock and lapses every expired bound (so an already expired `Reliable` worker is refused
	/// before any dial), and refuses if this worker is `Lapsed`/`Terminal`. Then registers it
	/// (`Unissued → Issuing`; a first issuance has no old bound to check), counts one in-flight
	/// issuance and issues the dispatch's ticket. This is the guard that prevents any fresh issuance
	/// after `Terminal` (including a scheduler retry of a failed first issuance). It never waits on
	/// other workers.
	pub fn begin_dispatch(
		self: &Arc<Self>,
		worker: &WorkerEntry,
	) -> Result<DispatchTicket, Refusal> {
		let _fire = FireOnDrop(self);
		let id = worker.worker_id;
		let mut ledger = self.lock();
		let result = self.admit_dispatch_locked(&mut ledger, worker);
		let state = ledger.workers.get(&id).map(|e| e.state.clone());
		let (ticket, at, cause) = match &result {
			Ok((ticket, at)) => (Some(*ticket), Some(*at), None),
			Err(refusal) => (None, None, Some(cause_text(&refusal.cause))),
		};
		self.note(&mut ledger, "dispatch_begin", || {
			serde_json::json!({
				"worker": worker_text(id), "ticket": ticket, "at_ns": opt_ns(at),
				"state": state_text(state.as_ref()), "bound_ns": opt_ns(bound_of(state.as_ref())),
				"result": if cause.is_none() { "admitted" } else { "refused" }, "cause": cause,
			})
		});
		drop(ledger);
		result.map(|(ticket, started)| DispatchTicket {
			worker: id,
			ticket,
			started,
			reliance: Arc::clone(self),
		})
	}

	fn admit_dispatch_locked(
		&self,
		ledger: &mut Ledger,
		worker: &WorkerEntry,
	) -> Result<(u64, BootInstant), Refusal> {
		let id = worker.worker_id;
		if let Some(refusal) = Self::sealed_refusal(ledger, Some(id)) {
			return Err(refusal);
		}
		let now = self.sample_or_fail(ledger)?;
		if let Some(refusal) = self.enforce_bounds_locked(ledger, now, "dial") {
			return Err(refusal);
		}
		if let Some(refusal) = Self::worker_refusal(ledger, id) {
			return Err(refusal);
		}
		let entry = ledger.workers.entry(id).or_insert_with(|| Entry {
			worker: worker.clone(),
			state: WorkerReliance::Unissued,
			issuing_in_flight: 0,
			loop_requested: false,
			attempts: 0,
		});
		if entry.state == WorkerReliance::Unissued {
			entry.state = WorkerReliance::Issuing;
		}
		entry.issuing_in_flight += 1;
		ledger.tickets += 1;
		Ok((ledger.tickets, now))
	}

	/// The dispatch's connect start: samples under the lock and returns the connect deadline, capped by
	/// the whole budget. A clock fault fails the query.
	pub fn begin_connect(
		&self,
		worker: WorkerId,
		exchange: ExchangeId,
		whole_by: BootInstant,
	) -> Result<BootInstant, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let sampled = self.sample_or_fail(&mut ledger);
		let connect_by = sampled.as_ref().ok().map(|now| {
			now.checked_add(self.params.connect_budget)
				.map_or(whole_by, |by| by.min(whole_by))
		});
		let at = sampled.as_ref().ok().copied();
		self.note(&mut ledger, "connect_begin", || {
			serde_json::json!({
				"worker": worker_text(worker), "exchange": exchange.evidence(), "at_ns": opt_ns(at),
				"connect_by_ns": opt_ns(connect_by), "whole_by_ns": ns(whole_by),
			})
		});
		sampled.map(|_| connect_by.expect("sampled"))
	}

	/// Admits one renewal attempt for `worker`: refuses if the query is sealed or renewal is closed,
	/// samples the exact attempt (and connect) start, and assigns the attempt's ordinal and its
	/// connect and whole deadlines.
	pub fn begin_renewal_attempt(&self, worker: WorkerId) -> Result<RenewalAttemptStart, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let result = self.admit_renewal_locked(&mut ledger, worker);
		let bound = bound_of(ledger.workers.get(&worker).map(|e| &e.state));
		let (start, cause) = match &result {
			Ok(start) => (Some(*start), None),
			Err(refusal) => (None, Some(cause_text(&refusal.cause))),
		};
		self.note(&mut ledger, "renewal_attempt_begin", || {
			serde_json::json!({
				"worker": worker_text(worker), "attempt": start.map(|s| s.attempt),
				"at_ns": opt_ns(start.map(|s| s.started)), "connect_by_ns": opt_ns(start.map(|s| s.connect_by)),
				"whole_by_ns": opt_ns(start.map(|s| s.whole_by)), "bound_ns": opt_ns(bound),
				"term_ms": u64::try_from(self.params.term.as_millis()).unwrap_or(u64::MAX),
				"result": if cause.is_none() { "admitted" } else { "refused" }, "cause": cause,
			})
		});
		result
	}

	fn admit_renewal_locked(
		&self,
		ledger: &mut Ledger,
		worker: WorkerId,
	) -> Result<RenewalAttemptStart, Refusal> {
		if let Some(refusal) = Self::sealed_refusal(ledger, Some(worker)) {
			return Err(refusal);
		}
		if ledger.renewal_closed {
			return Err(Refusal {
				worker: Some(worker),
				cause: RelianceCause::QueryEnded(QueryEnd::Dropped),
			});
		}
		let started = self.sample_or_fail(ledger)?;
		let (Some(whole_by), Some(connect_by)) = (
			started.checked_add(self.params.whole_budget),
			started.checked_add(self.params.connect_budget),
		) else {
			return Err(self.fail_locked(ledger, None, RelianceCause::Arithmetic));
		};
		let Some(entry) = ledger.workers.get_mut(&worker) else {
			return Err(Refusal {
				worker: Some(worker),
				cause: RelianceCause::Protocol("renewal attempt for an unknown worker".to_string()),
			});
		};
		entry.attempts += 1;
		Ok(RenewalAttemptStart {
			attempt: entry.attempts,
			started,
			connect_by: connect_by.min(whole_by),
			whole_by,
		})
	}

	/// Records how one renewal attempt ended: `applied`, `late`, `transient`, `fatal`, `refused`, or
	/// `aborted` (its future was dropped, for example by renewal cleanup).
	pub fn end_renewal_attempt(
		&self,
		worker: WorkerId,
		attempt: u64,
		outcome: &'static str,
		reason: Option<String>,
	) {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		self.note(&mut ledger, "renewal_attempt_end", || {
			serde_json::json!({
				"worker": worker_text(worker), "attempt": attempt, "outcome": outcome, "reason": reason,
			})
		});
	}

	/// The LEASE-WRITE gate and send marker, taken immediately before a `QueryLease` is written.
	/// Refuses if the query is sealed, if `exchange` is a renewal and renewal is closed, if any bound
	/// has passed (lapsing it), or if `worker` is not `Issuing` (dispatch only: no old bound) or
	/// `Reliable` with `now < bound`.
	pub fn send_marker(
		&self,
		worker: WorkerId,
		exchange: ExchangeId,
	) -> Result<BootInstant, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let result = self.lease_write_locked(&mut ledger, worker, exchange);
		let state = ledger.workers.get(&worker).map(|e| e.state.clone());
		let (marker, cause) = match &result {
			Ok(marker) => (Some(*marker), None),
			Err(refusal) => (None, Some(cause_text(&refusal.cause))),
		};
		self.note(&mut ledger, "lease_write", || {
			serde_json::json!({
				"worker": worker_text(worker), "exchange": exchange.evidence(), "marker_ns": opt_ns(marker),
				"state": state_text(state.as_ref()), "bound_ns": opt_ns(bound_of(state.as_ref())),
				"result": if cause.is_none() { "admitted" } else { "refused" }, "cause": cause,
			})
		});
		result
	}

	fn lease_write_locked(
		&self,
		ledger: &mut Ledger,
		worker: WorkerId,
		exchange: ExchangeId,
	) -> Result<BootInstant, Refusal> {
		if let Some(refusal) = Self::sealed_refusal(ledger, Some(worker)) {
			return Err(refusal);
		}
		let path = exchange.path();
		if path == ExchangePath::Renewal && ledger.renewal_closed {
			return Err(Refusal {
				worker: Some(worker),
				cause: RelianceCause::QueryEnded(QueryEnd::Dropped),
			});
		}
		let now = self.sample_or_fail(ledger)?;
		if let Some(refusal) = self.enforce_bounds_locked(ledger, now, "lease_write") {
			return Err(refusal);
		}
		if let Some(refusal) = Self::worker_refusal(ledger, worker) {
			return Err(refusal);
		}
		match (ledger.workers.get(&worker).map(|e| &e.state), path) {
			(Some(WorkerReliance::Reliable { .. }), _)
			| (Some(WorkerReliance::Issuing), ExchangePath::Dispatch) => Ok(now),
			_ => Err(Refusal {
				worker: Some(worker),
				cause: RelianceCause::Protocol(format!("{path:?} lease write with no issuance")),
			}),
		}
	}

	/// A validated, correlated `Applied` (renewal: the whole exchange, close included; dispatch: the
	/// validated handshake reply), with its own send marker, completed by `deadline` (the exchange's
	/// budget). Applies the refresh rule under the lock:
	/// - a sealed query refuses BEFORE sampling (after a commit the reply is context only);
	/// - a completion after `deadline` is late: it refreshes nothing. For an `Issuing` worker that is
	///   a failed first issuance; for a `Reliable` one the bound is kept;
	/// - `Issuing` has no old bound. The candidate must be in the future, otherwise the first
	///   issuance fails;
	/// - `Reliable` requires `now < old` (equality is lapsed). The candidate is installed only if
	///   it is in the future, as `max(old, candidate)`, so an older reply never moves it back;
	/// - nothing revives `Lapsed` or `Terminal`.
	pub fn on_applied(
		&self,
		worker: WorkerId,
		marker: BootInstant,
		exchange: ExchangeId,
		deadline: BootInstant,
	) -> Result<Applied, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let old = ledger.workers.get(&worker).map(|e| e.state.clone());
		let mut at = None;
		let mut candidate_seen = None;
		let result = self.apply_locked(
			&mut ledger,
			worker,
			marker,
			exchange,
			deadline,
			&mut at,
			&mut candidate_seen,
		);
		let old_bound = match old.as_ref() {
			Some(WorkerReliance::Reliable { bound, .. }) => Some(*bound),
			_ => None,
		};
		let (label, new_bound, cause) = match &result {
			Ok(Applied::Late) => ("late", None, None),
			Ok(Applied::Bound(bound)) if Some(*bound) == old_bound => ("kept", Some(*bound), None),
			Ok(Applied::Bound(bound)) => ("installed", Some(*bound), None),
			Err(refusal) => ("refused", None, Some(cause_text(&refusal.cause))),
		};
		let slack = match (old_bound, at) {
			(Some(old_bound), Some(at)) if label == "installed" => Some(
				u64::try_from(old_bound.as_nanos().saturating_sub(at.as_nanos()))
					.unwrap_or(u64::MAX),
			),
			_ => None,
		};
		self.note(&mut ledger, "applied", || {
			serde_json::json!({
				"worker": worker_text(worker), "exchange": exchange.evidence(), "marker_ns": ns(marker),
				"deadline_ns": ns(deadline), "at_ns": opt_ns(at), "state": state_text(old.as_ref()),
				"old_bound_ns": opt_ns(old_bound), "candidate_ns": opt_ns(candidate_seen),
				"new_bound_ns": opt_ns(new_bound), "slack_ns": slack,
				"term_ms": u64::try_from(self.params.term.as_millis()).unwrap_or(u64::MAX),
				"result": label, "cause": cause,
			})
		});
		result
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_locked(
		&self,
		ledger: &mut Ledger,
		worker: WorkerId,
		marker: BootInstant,
		exchange: ExchangeId,
		deadline: BootInstant,
		at: &mut Option<BootInstant>,
		candidate_seen: &mut Option<BootInstant>,
	) -> Result<Applied, Refusal> {
		if let Some(refusal) = Self::sealed_refusal(ledger, Some(worker)) {
			return Err(refusal);
		}
		let now = self.sample_or_fail(ledger)?;
		*at = Some(now);
		let span = match self.span.clone() {
			Ok(span) => span,
			Err(cause) => return Err(self.fail_locked(ledger, None, cause)),
		};
		let Some(candidate) = marker.checked_add(span) else {
			return Err(self.fail_locked(ledger, None, RelianceCause::Arithmetic));
		};
		*candidate_seen = Some(candidate);
		// The overrun itself is recorded once, at the exchange's exit (`within`).
		let late = now > deadline;
		let state = ledger.workers.get(&worker).map(|e| e.state.clone());
		let installed = match state {
			Some(WorkerReliance::Issuing) => {
				if late || now >= candidate {
					return Err(self.fail_locked(
						ledger,
						Some(worker),
						RelianceCause::IssuanceFailed(if late {
							"first Applied completed past its budget".to_string()
						} else {
							"first Applied processed at or after its own bound".to_string()
						}),
					));
				}
				candidate
			}
			Some(WorkerReliance::Reliable { bound, .. }) => {
				if now >= bound {
					self.note(ledger, "lapsed", || {
						serde_json::json!({
							"worker": worker_text(worker), "bound_ns": ns(bound), "observed_ns": ns(now),
							"site": "applied",
						})
					});
					return Err(self.fail_locked(
						ledger,
						Some(worker),
						RelianceCause::Lapsed { bound },
					));
				}
				if late {
					return Ok(Applied::Late);
				}
				if now >= candidate || candidate <= bound {
					// Late against its own candidate, or older than the installed bound: context
					// only, no move backwards.
					return Ok(Applied::Bound(bound));
				}
				candidate
			}
			Some(WorkerReliance::Lapsed { .. } | WorkerReliance::Terminal(_)) => {
				return Err(Self::worker_refusal(ledger, worker).expect("not relied on"));
			}
			Some(WorkerReliance::Unissued) | None => {
				return Err(Refusal {
					worker: Some(worker),
					cause: RelianceCause::Protocol(format!(
						"{:?} Applied for a worker with no issuance in flight",
						exchange.path()
					)),
				});
			}
		};
		let entry = ledger.workers.get_mut(&worker).expect("checked entry");
		entry.state = WorkerReliance::Reliable {
			bound: installed,
			marker,
		};
		let enroll = !entry.loop_requested;
		entry.loop_requested = true;
		let worker_entry = entry.worker.clone();
		if enroll && let Some(enroller) = &ledger.enroller {
			let _ = enroller.send(worker_entry);
		}
		self.changed.notify_waiters();
		Ok(Applied::Bound(installed))
	}

	/// A dispatch handshake failed (transport or budget) before a validated reply. While the worker
	/// is STILL `Issuing` this is a failed first issuance: the worker is `Terminal` and the query
	/// fails. If another handshake already made it `Reliable`, the existing bound is kept and the
	/// task-dispatch error policy applies (no refresh from this attempt).
	pub fn on_handshake_failed(
		&self,
		worker: WorkerId,
		exchange: ExchangeId,
		reason: &str,
	) -> Option<Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let state = ledger.workers.get(&worker).map(|e| e.state.clone());
		let result = match state {
			Some(WorkerReliance::Issuing) if ledger.outcome == QueryOutcome::Open => {
				Some(self.fail_locked(
					&mut ledger,
					Some(worker),
					RelianceCause::IssuanceFailed(reason.to_string()),
				))
			}
			_ => None,
		};
		let effect = if result.is_some() {
			"issuance_failed"
		} else {
			"no_refresh"
		};
		self.note(&mut ledger, "handshake_failed", || {
			serde_json::json!({
				"worker": worker_text(worker), "exchange": exchange.evidence(), "reason": reason,
				"state": state_text(state.as_ref()), "effect": effect,
			})
		});
		result
	}

	/// A fatal outcome (a validated `Ignored`, a protocol violation, an auth refusal, a detected
	/// invalidation). The worker is `Terminal` and the query fails, unless it is already sealed (then
	/// the outcome is context only).
	pub fn on_fatal(&self, worker: WorkerId, cause: RelianceCause) -> Refusal {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let text = cause_text(&cause);
		let prior = Self::sealed_refusal(&ledger, Some(worker));
		let effect = if prior.is_some() {
			"context_only"
		} else {
			"terminal"
		};
		let refusal = match prior {
			Some(refusal) => refusal,
			None => self.fail_locked(&mut ledger, Some(worker), cause),
		};
		self.note(
			&mut ledger,
			"fatal",
			|| serde_json::json!({ "worker": worker_text(worker), "cause": text, "effect": effect }),
		);
		refusal
	}

	/// The use gate before an action that relies on `worker` (sending `StartTask`, publishing a
	/// final batch it produced, accepting its completion). Refuses a sealed query before sampling,
	/// then samples the clock under the lock, lapses every expired bound, and requires `worker` to be
	/// `Reliable` with `now < bound`. Every authorization, granted or refused, is recorded.
	pub fn gate(&self, worker: WorkerId, site: UseSite) -> Result<(), Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let mut at = None;
		let result = self.use_locked(&mut ledger, worker, site.site, &mut at);
		let state = ledger.workers.get(&worker).map(|e| e.state.clone());
		let cause = result.as_ref().err().map(|r| cause_text(&r.cause));
		self.note(&mut ledger, "use", || {
			serde_json::json!({
				"worker": worker_text(worker), "site": site.site, "ticket": site.ticket, "task": site.task,
				"batch": site.batch, "at_ns": opt_ns(at), "state": state_text(state.as_ref()),
				"bound_ns": opt_ns(bound_of(state.as_ref())),
				"result": if cause.is_none() { "authorized" } else { "refused" }, "cause": cause,
			})
		});
		result
	}

	fn use_locked(
		&self,
		ledger: &mut Ledger,
		worker: WorkerId,
		site: &'static str,
		at: &mut Option<BootInstant>,
	) -> Result<(), Refusal> {
		if let Some(refusal) = Self::sealed_refusal(ledger, Some(worker)) {
			return Err(refusal);
		}
		let now = self.sample_or_fail(ledger)?;
		*at = Some(now);
		if let Some(refusal) = self.enforce_bounds_locked(ledger, now, site) {
			return Err(refusal);
		}
		if let Some(refusal) = Self::worker_refusal(ledger, worker) {
			return Err(refusal);
		}
		match ledger.workers.get(&worker).map(|e| &e.state) {
			Some(WorkerReliance::Reliable { .. }) => Ok(()),
			_ => Err(Refusal {
				worker: Some(worker),
				cause: RelianceCause::Protocol("gate before any Applied".to_string()),
			}),
		}
	}

	/// The delivery gate for one buffered result batch (`batch` is its delivery ordinal), at the moment
	/// it is handed to the client. `Committed`: the committed local state decides (deliver). `Failed`:
	/// suppress. `Open`: sample under the lock and require every relied-on bound to hold (expired ones
	/// lapse and fail the query). A query that has relied on no remote worker delivers without reading
	/// the clock.
	pub fn delivery_gate(&self, batch: u64) -> Result<(), Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let before = outcome_text(&ledger.outcome);
		let mut at = None;
		let result = self.deliver_locked(&mut ledger, &mut at);
		let cause = result.as_ref().err().map(|r| cause_text(&r.cause));
		self.note(&mut ledger, "delivery", || {
			serde_json::json!({
				"batch": batch, "outcome_before": before, "at_ns": opt_ns(at),
				"result": if cause.is_none() { "delivered" } else { "suppressed" }, "cause": cause,
			})
		});
		result
	}

	fn deliver_locked(
		&self,
		ledger: &mut Ledger,
		at: &mut Option<BootInstant>,
	) -> Result<(), Refusal> {
		match &ledger.outcome {
			QueryOutcome::Committed => return Ok(()),
			QueryOutcome::Failed { .. } => {
				return Err(Self::sealed_refusal(ledger, None).expect("failed"));
			}
			QueryOutcome::Open => {}
		}
		if Self::relies_on_none(ledger) {
			return Ok(());
		}
		let now = self.sample_or_fail(ledger)?;
		*at = Some(now);
		match self.enforce_bounds_locked(ledger, now, "deliver") {
			Some(refusal) => Err(refusal),
			None => Ok(()),
		}
	}

	fn relies_on_none(ledger: &Ledger) -> bool {
		ledger
			.workers
			.values()
			.all(|e| e.state == WorkerReliance::Unissued)
	}

	/// The atomic successful-completion gate. It commits only if the query is open and every
	/// required worker (every worker that reached `Reliable`, plus any still `Issuing` with an
	/// exchange in flight) is `Reliable` with `now < bound`. After a commit, buffered local results
	/// are delivered against this committed state, never against a retired remote namespace.
	/// Commit and failure (including cancellation and ordinary query failure) are linearized under
	/// one lock, so exactly one wins. A repeated commit is idempotent and samples nothing.
	pub fn commit_success(&self) -> Result<(), Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let before = outcome_text(&ledger.outcome);
		let mut at = None;
		let result = self.commit_locked(&mut ledger, &mut at);
		let cause = result.as_ref().err().map(|r| cause_text(&r.cause));
		self.note(&mut ledger, "commit", || {
			serde_json::json!({
				"outcome_before": before, "at_ns": opt_ns(at),
				"result": if cause.is_none() { "committed" } else { "refused" }, "cause": cause,
			})
		});
		result
	}

	fn commit_locked(
		&self,
		ledger: &mut Ledger,
		at: &mut Option<BootInstant>,
	) -> Result<(), Refusal> {
		match &ledger.outcome {
			QueryOutcome::Committed => return Ok(()),
			QueryOutcome::Failed { .. } => {
				return Err(Self::sealed_refusal(ledger, None).expect("failed"));
			}
			QueryOutcome::Open => {}
		}
		// A query that never relied on a remote worker has nothing to validate.
		if Self::relies_on_none(ledger) {
			ledger.outcome = QueryOutcome::Committed;
			self.changed.notify_waiters();
			return Ok(());
		}
		let now = self.sample_or_fail(ledger)?;
		*at = Some(now);
		if let Some(refusal) = self.enforce_bounds_locked(ledger, now, "commit") {
			return Err(refusal);
		}
		let stuck: Option<WorkerId> = ledger.workers.iter().find_map(|(id, e)| match e.state {
			WorkerReliance::Issuing if e.issuing_in_flight > 0 => Some(*id),
			_ => None,
		});
		if let Some(worker) = stuck {
			return Err(self.fail_locked(
				ledger,
				Some(worker),
				RelianceCause::IssuanceFailed(
					"success committed with an issuance still in flight".to_string(),
				),
			));
		}
		ledger.outcome = QueryOutcome::Committed;
		self.changed.notify_waiters();
		Ok(())
	}

	/// Seals a query that ended other than by reliance (its own error, a cancellation, a dropped
	/// owner). Irreversible: nothing after it authorizes, refreshes or commits. It never overrides a
	/// commit or an earlier failure, and samples nothing. Returns whether this call sealed it.
	pub fn seal_ended(&self, end: QueryEnd) -> bool {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let sealed = ledger.outcome == QueryOutcome::Open;
		if sealed {
			ledger.outcome = QueryOutcome::Failed {
				worker: None,
				cause: RelianceCause::QueryEnded(end.clone()),
			};
			self.changed.notify_waiters();
		}
		let text = end.to_string();
		self.note(
			&mut ledger,
			"query_ended",
			|| serde_json::json!({ "end": text, "sealed": sealed }),
		);
		sealed
	}

	/// Closes renewal admission synchronously: no renewal attempt or lease write is admitted after
	/// this returns, and every renewal plan is `Stop`.
	pub fn close_renewal(&self) {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		if !ledger.renewal_closed {
			ledger.renewal_closed = true;
			// Recorded once: a repeated close (shutdown, then the owner's drop) changes nothing.
			self.note(&mut ledger, "renewal_closed", || serde_json::json!({}));
		}
		self.changed.notify_waiters();
	}

	/// Records the renewal supervisor's cleanup result: `verified`, `supervisor_lost` or
	/// `supervisor_panicked`.
	pub fn record_cleanup(&self, result: &'static str) {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		self.note(
			&mut ledger,
			"renewal_cleanup",
			|| serde_json::json!({ "result": result }),
		);
	}

	/// The renewal loop's next step for `worker`, computed under the lock:
	/// - renewal closed, the query sealed, or `worker` not `Reliable`: `Stop`;
	/// - before the next due renewal (the supporting marker + cadence): sleep until then, or until
	///   the bound if that is earlier;
	/// - due, and `now + whole_budget < bound`: `Attempt`;
	/// - due, but no attempt fits: PAUSE until the bound, which is lapsed on waking unless an inline
	///   dispatch `Applied` has extended it.
	///
	/// `not_before` is a retry backoff deadline (the earliest next attempt), if any. A clock fault
	/// fails the query and stops.
	pub fn renewal_plan(&self, worker: WorkerId, not_before: Option<BootInstant>) -> RenewalPlan {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let mut at = None;
		let plan = self.plan_locked(&mut ledger, worker, not_before, &mut at);
		let (label, until) = match plan {
			RenewalPlan::Stop => ("stop", None),
			RenewalPlan::SleepUntil(instant) => ("sleep", Some(instant)),
			RenewalPlan::Attempt => ("attempt", None),
		};
		self.note(&mut ledger, "plan", || {
			serde_json::json!({
				"worker": worker_text(worker), "at_ns": opt_ns(at), "not_before_ns": opt_ns(not_before),
				"result": label, "until_ns": opt_ns(until),
			})
		});
		plan
	}

	fn plan_locked(
		&self,
		ledger: &mut Ledger,
		worker: WorkerId,
		not_before: Option<BootInstant>,
		at: &mut Option<BootInstant>,
	) -> RenewalPlan {
		if ledger.renewal_closed || ledger.outcome != QueryOutcome::Open {
			return RenewalPlan::Stop;
		}
		let Ok(now) = self.sample_or_fail(ledger) else {
			return RenewalPlan::Stop;
		};
		*at = Some(now);
		if self.enforce_bounds_locked(ledger, now, "plan").is_some() {
			return RenewalPlan::Stop;
		}
		let Some(WorkerReliance::Reliable { bound, marker }) =
			ledger.workers.get(&worker).map(|e| e.state.clone())
		else {
			return RenewalPlan::Stop;
		};
		let Some(due) = marker.checked_add(self.params.cadence) else {
			return RenewalPlan::SleepUntil(bound);
		};
		let earliest = not_before.map_or(due, |nb| nb.max(due));
		if now < earliest {
			return RenewalPlan::SleepUntil(earliest.min(bound));
		}
		match now.checked_add(self.params.whole_budget) {
			Some(finish) if finish < bound => RenewalPlan::Attempt,
			_ => RenewalPlan::SleepUntil(bound),
		}
	}

	/// The backoff deadline `delay` from now, after `attempt` failed. A clock fault fails the query; an
	/// unrepresentable instant is an arithmetic fault and fails it too.
	pub fn backoff_until(
		&self,
		worker: WorkerId,
		attempt: u64,
		delay: Duration,
	) -> Result<BootInstant, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let sampled = self.sample_or_fail(&mut ledger);
		let result = match sampled {
			Ok(now) => now
				.checked_add(delay)
				.ok_or_else(|| self.fail_locked(&mut ledger, None, RelianceCause::Arithmetic)),
			Err(refusal) => Err(refusal),
		};
		let not_before = result.as_ref().ok().copied();
		self.note(&mut ledger, "backoff", || {
			serde_json::json!({
				"worker": worker_text(worker), "attempt": attempt,
				"delay_ms": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
				"not_before_ns": opt_ns(not_before),
			})
		});
		result
	}

	/// The tokio-clock duration until a boot instant (zero once it has passed), sampled under the
	/// lock. Timers are advisory: whatever they wake for is re-checked under the lock. A clock fault
	/// fails the query; there is no zero-duration fallback.
	pub fn until(&self, instant: BootInstant) -> Result<Duration, Refusal> {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let now = self.sample_or_fail(&mut ledger)?;
		Ok(instant.saturating_since(now))
	}

	fn release_issuance(&self, worker: WorkerId, ticket: u64) {
		let _fire = FireOnDrop(self);
		let mut ledger = self.lock();
		let mut reverted = false;
		if let Some(entry) = ledger.workers.get_mut(&worker) {
			entry.issuing_in_flight = entry.issuing_in_flight.saturating_sub(1);
			// An abandoned first issuance (the dispatch was dropped before any reply) relied on
			// nothing: no StartTask was authorized, so the worker is not required at commit.
			if entry.issuing_in_flight == 0 && entry.state == WorkerReliance::Issuing {
				entry.state = WorkerReliance::Unissued;
				reverted = true;
			}
		}
		self.note(
			&mut ledger,
			"dispatch_release",
			|| serde_json::json!({ "worker": worker_text(worker), "ticket": ticket, "reverted": reverted }),
		);
		self.changed.notify_waiters();
	}
}

/// One dispatch's issuance slot. It is released when the dispatch ends, including when the dispatch
/// future is dropped (cancelled). An abandoned first issuance relied on nothing.
#[derive(Debug)]
pub struct DispatchTicket {
	worker: WorkerId,
	/// The ledger's per-query ordinal for this dispatch's inline lease.
	ticket: u64,
	/// The dial gate's sample: the start of this dispatch's whole-handshake budget.
	started: BootInstant,
	reliance: Arc<QueryReliance>,
}

impl DispatchTicket {
	pub fn worker(&self) -> WorkerId {
		self.worker
	}

	pub fn ticket(&self) -> u64 {
		self.ticket
	}

	pub fn exchange(&self) -> ExchangeId {
		ExchangeId::Dispatch {
			ticket: self.ticket,
		}
	}

	pub fn started(&self) -> BootInstant {
		self.started
	}

	pub fn reliance(&self) -> &Arc<QueryReliance> {
		&self.reliance
	}
}

impl Drop for DispatchTicket {
	fn drop(&mut self) {
		self.reliance.release_issuance(self.worker, self.ticket);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::ByteCount;
	use crate::distributed::worker_registry::{
		ActiveTaskCount, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
		WorkerLease, WorkerResourceState, WorkerStatus,
	};

	const SPAN_NS: u128 = 33_966_016_491; // floor(35_001e6 · 999_500 / 1_000_500) − 1e9
	const SEC: u128 = 1_000_000_000;

	fn entry(n: u128) -> WorkerEntry {
		WorkerEntry {
			worker_id: WorkerId::from_u128(n),
			endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(1),
				epoch: crate::distributed::identity::LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(1024),
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks: ActiveTaskCount::default(),
			},
			status: WorkerStatus::Live,
		}
	}

	fn ledger(at: u128) -> (Arc<QueryReliance>, Arc<SettableClock>) {
		let clock = Arc::new(SettableClock::at(at));
		let params = RelianceParams::production(Duration::from_millis(35_001));
		(Arc::new(QueryReliance::new(params, clock.clone())), clock)
	}

	/// `on_applied` within its budget (no deadline pressure), as the installed bound.
	fn applied(
		r: &QueryReliance,
		w: WorkerId,
		marker: BootInstant,
		exchange: ExchangeId,
	) -> Result<BootInstant, Refusal> {
		match r.on_applied(w, marker, exchange, BootInstant(u128::MAX / 2))? {
			Applied::Bound(bound) => Ok(bound),
			Applied::Late => panic!("no budget pressure here"),
		}
	}

	/// Issues `w` with a send marker at the current clock and an `Applied` processed at once.
	fn issue(r: &Arc<QueryReliance>, w: &WorkerEntry) -> (DispatchTicket, BootInstant) {
		let ticket = r.begin_dispatch(w).expect("dispatch");
		let marker = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.expect("marker");
		let bound =
			applied(&r, w.worker_id, marker, ExchangeId::Dispatch { ticket: 1 }).expect("applied");
		(ticket, bound)
	}

	#[test]
	fn the_reliance_span_is_checked_integer_nanoseconds() {
		let p = RelianceParams::production(Duration::from_millis(35_001));
		assert_eq!(p.reliance_span().unwrap().as_nanos(), SPAN_NS);
		let mut bad = p;
		bad.rho_worker_ppm = 1_000_000;
		assert_eq!(bad.reliance_span(), Err(RelianceCause::Arithmetic));
		let mut short = p;
		short.term = Duration::from_millis(900); // D < M
		assert_eq!(short.reliance_span(), Err(RelianceCause::Arithmetic));
		let mut equal = p;
		equal.margin = Duration::from_nanos(34_966_016_491); // D − M == 0 is refused too
		assert_eq!(equal.reliance_span(), Err(RelianceCause::Arithmetic));
	}

	#[test]
	fn first_issuance_gates_dependent_use_until_applied() {
		let (r, _) = ledger(10 * SEC);
		let w = entry(1);
		let _ticket = r.begin_dispatch(&w).unwrap();
		assert_eq!(r.state(w.worker_id), Some(WorkerReliance::Issuing));
		assert!(
			r.gate(w.worker_id, UseSite::new("start_task")).is_err(),
			"no StartTask before a validated Applied"
		);
		let marker = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		let bound = applied(&r, w.worker_id, marker, ExchangeId::Dispatch { ticket: 1 }).unwrap();
		assert_eq!(bound.as_nanos(), 10 * SEC + SPAN_NS);
		assert!(r.gate(w.worker_id, UseSite::new("start_task")).is_ok());
	}

	#[test]
	fn equality_at_the_old_boundary_is_lapsed() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		clock.set(bound.as_nanos() - 1);
		assert!(r.gate(w.worker_id, UseSite::new("start_task")).is_ok());
		clock.set(bound.as_nanos());
		let refusal = r.gate(w.worker_id, UseSite::new("start_task")).unwrap_err();
		assert_eq!(refusal.cause, RelianceCause::Lapsed { bound });
		assert!(matches!(r.outcome(), QueryOutcome::Failed { .. }));
		// A refresh processed at the old boundary does not restore reliance.
		assert!(
			applied(
				&r,
				w.worker_id,
				BootInstant(bound.as_nanos() - SEC),
				ExchangeId::Renewal { attempt: 1 }
			)
			.is_err()
		);
		assert_eq!(r.state(w.worker_id), Some(WorkerReliance::Lapsed { bound }));
	}

	#[test]
	fn a_late_or_older_reply_never_moves_the_bound_back() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, first) = issue(&r, &w);
		clock.set(5 * SEC);
		let newer = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		let extended = applied(&r, w.worker_id, newer, ExchangeId::Renewal { attempt: 1 }).unwrap();
		assert_eq!(extended.as_nanos(), 5 * SEC + SPAN_NS);
		// An out-of-order older reply (marker 1 s) arrives late: context only.
		clock.set(6 * SEC);
		let kept = applied(
			&r,
			w.worker_id,
			BootInstant(SEC),
			ExchangeId::Renewal { attempt: 1 },
		)
		.unwrap();
		assert_eq!(kept, extended);
		assert!(kept > first);
		// A reply whose own candidate has already elapsed (a late ACK) refreshes nothing.
		clock.set(5 * SEC + SPAN_NS - 1);
		let late = applied(
			&r,
			w.worker_id,
			BootInstant(0),
			ExchangeId::Renewal { attempt: 1 },
		)
		.unwrap();
		assert_eq!(late, extended);
	}

	#[test]
	fn a_first_applied_processed_after_its_own_bound_fails_issuance() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let _t = r.begin_dispatch(&w).unwrap();
		let marker = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		clock.set(SPAN_NS); // processed exactly at the candidate
		let refusal =
			applied(&r, w.worker_id, marker, ExchangeId::Dispatch { ticket: 1 }).unwrap_err();
		assert!(matches!(refusal.cause, RelianceCause::IssuanceFailed(_)));
		assert!(matches!(r.outcome(), QueryOutcome::Failed { .. }));
	}

	#[test]
	fn a_failed_first_issuance_is_terminal_and_never_reissued() {
		let (r, _) = ledger(0);
		let w = entry(1);
		let ticket = r.begin_dispatch(&w).unwrap();
		let refusal = r
			.on_handshake_failed(
				w.worker_id,
				ExchangeId::Dispatch { ticket: 1 },
				"connect timed out",
			)
			.unwrap();
		assert!(matches!(refusal.cause, RelianceCause::IssuanceFailed(_)));
		drop(ticket);
		assert!(matches!(
			r.state(w.worker_id),
			Some(WorkerReliance::Terminal(_))
		));
		// The scheduler's retry of that dispatch cannot become a fresh issuance.
		assert!(r.begin_dispatch(&w).is_err());
	}

	#[test]
	fn handshake_failure_after_another_handshake_succeeded_keeps_the_bound() {
		let (r, _) = ledger(0);
		let w = entry(1);
		let t1 = r.begin_dispatch(&w).unwrap();
		let t2 = r.begin_dispatch(&w).unwrap(); // coalesced Issuing: two first dispatches
		let m1 = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		let bound = applied(&r, w.worker_id, m1, ExchangeId::Dispatch { ticket: 1 }).unwrap();
		assert_eq!(
			r.on_handshake_failed(
				w.worker_id,
				ExchangeId::Dispatch { ticket: 1 },
				"read failed"
			),
			None
		);
		assert_eq!(
			r.state(w.worker_id),
			Some(WorkerReliance::Reliable { bound, marker: m1 })
		);
		assert_eq!(r.outcome(), QueryOutcome::Open);
		drop((t1, t2));
		// And in the other order, the failure wins first: the worker is Terminal.
		let (r2, _) = ledger(0);
		let _a = r2.begin_dispatch(&w).unwrap();
		let _b = r2.begin_dispatch(&w).unwrap();
		let m = r2
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		assert!(
			r2.on_handshake_failed(
				w.worker_id,
				ExchangeId::Dispatch { ticket: 1 },
				"read failed"
			)
			.is_some()
		);
		assert!(applied(&r2, w.worker_id, m, ExchangeId::Dispatch { ticket: 1 }).is_err());
	}

	#[test]
	fn a_new_worker_enrolls_without_waiting_on_others() {
		let (r, _) = ledger(0);
		let (a, b) = (entry(1), entry(2));
		let _ta = r.begin_dispatch(&a).unwrap(); // a still Issuing
		let tb = r.begin_dispatch(&b).unwrap();
		let m = r
			.send_marker(b.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		applied(&r, b.worker_id, m, ExchangeId::Dispatch { ticket: 1 }).unwrap();
		assert!(
			r.gate(b.worker_id, UseSite::new("start_task")).is_ok(),
			"b's StartTask gate does not wait for a"
		);
		drop(tb);
	}

	#[test]
	fn an_abandoned_issuance_is_not_required_at_commit() {
		let (r, _) = ledger(0);
		let (a, b) = (entry(1), entry(2));
		let (_ta, _) = issue(&r, &a);
		let tb = r.begin_dispatch(&b).unwrap();
		drop(tb); // the dispatch future was dropped before any reply
		assert_eq!(r.state(b.worker_id), Some(WorkerReliance::Unissued));
		assert!(r.commit_success().is_ok());
	}

	#[test]
	fn commit_refuses_an_issuance_still_in_flight() {
		let (r, _) = ledger(0);
		let (a, b) = (entry(1), entry(2));
		let (_ta, _) = issue(&r, &a);
		let _tb = r.begin_dispatch(&b).unwrap();
		assert!(r.commit_success().is_err());
	}

	#[test]
	fn commit_and_lapse_are_linearized_and_irreversible() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		clock.set(bound.as_nanos() - 1);
		assert!(r.commit_success().is_ok());
		assert_eq!(r.outcome(), QueryOutcome::Committed);
		// After the commit, late callbacks are context only: nothing turns success into failure.
		clock.set(bound.as_nanos() + SEC);
		r.on_fatal(w.worker_id, RelianceCause::Ignored(LeaseIgnored::Expired));
		r.seal_ended(QueryEnd::Error("late".into()));
		assert!(
			applied(
				&r,
				w.worker_id,
				BootInstant(bound.as_nanos()),
				ExchangeId::Renewal { attempt: 1 }
			)
			.is_err()
		);
		assert_eq!(r.outcome(), QueryOutcome::Committed);
		// The other order: a lapse before the commit wins, and a late Ok cannot commit.
		let (r2, clock2) = ledger(0);
		let (_t2, bound2) = issue(&r2, &w);
		clock2.set(bound2.as_nanos());
		assert!(r2.commit_success().is_err());
		assert!(r2.commit_success().is_err());
		assert!(matches!(r2.outcome(), QueryOutcome::Failed { .. }));
	}

	#[test]
	fn a_fatal_reply_is_terminal_and_fails_the_query() {
		let (r, _) = ledger(0);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		r.on_fatal(w.worker_id, RelianceCause::Ignored(LeaseIgnored::Expired));
		assert_eq!(
			r.state(w.worker_id),
			Some(WorkerReliance::Terminal(RelianceCause::Ignored(
				LeaseIgnored::Expired
			)))
		);
		assert!(r.gate(w.worker_id, UseSite::new("start_task")).is_err());
		assert!(r.begin_dispatch(&w).is_err());
	}

	#[test]
	fn clock_faults_are_fatal_for_reliance() {
		let (r, clock) = ledger(10 * SEC);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		clock.set(9 * SEC); // regression below an observed sample
		let refusal = r.gate(w.worker_id, UseSite::new("start_task")).unwrap_err();
		assert_eq!(refusal.cause, RelianceCause::Clock(ClockFault::Regressed));
		let (r2, clock2) = ledger(0);
		let (_t2, _) = issue(&r2, &w);
		clock2.fail(ClockFault::ReadFailed(22));
		assert_eq!(
			r2.gate(w.worker_id, UseSite::new("start_task"))
				.unwrap_err()
				.cause,
			RelianceCause::Clock(ClockFault::ReadFailed(22))
		);
	}

	#[test]
	fn a_suspend_jump_past_the_bound_is_caught_at_the_next_gate() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		clock.advance(Duration::from_secs(120)); // the coordinator was suspended
		assert!(matches!(
			r.gate(w.worker_id, UseSite::new("start_task"))
				.unwrap_err()
				.cause,
			RelianceCause::Lapsed { .. }
		));
	}

	#[test]
	fn renewal_plan_follows_the_supporting_marker_and_pauses_when_nothing_fits() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		assert_eq!(
			r.renewal_plan(w.worker_id, None),
			RenewalPlan::SleepUntil(BootInstant(5 * SEC))
		);
		clock.set(5 * SEC);
		assert_eq!(r.renewal_plan(w.worker_id, None), RenewalPlan::Attempt);
		// A backoff deadline defers the attempt.
		assert_eq!(
			r.renewal_plan(w.worker_id, Some(BootInstant(5 * SEC + SEC / 4))),
			RenewalPlan::SleepUntil(BootInstant(5 * SEC + SEC / 4))
		);
		// No attempt fits before the bound: pause until the bound (not exit).
		clock.set(bound.as_nanos() - 3 * SEC);
		assert_eq!(
			r.renewal_plan(w.worker_id, None),
			RenewalPlan::SleepUntil(bound)
		);
		// An inline dispatch Applied extends the bound while paused; the plan resumes from its marker.
		let marker = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		let newer = applied(&r, w.worker_id, marker, ExchangeId::Dispatch { ticket: 1 }).unwrap();
		assert!(newer > bound);
		assert_eq!(
			r.renewal_plan(w.worker_id, None),
			RenewalPlan::SleepUntil(marker.checked_add(Duration::from_secs(5)).unwrap())
		);
		// A stale timer at the old bound cannot lapse the newer bound.
		clock.set(bound.as_nanos());
		assert!(r.gate(w.worker_id, UseSite::new("start_task")).is_ok());
		assert_ne!(r.renewal_plan(w.worker_id, None), RenewalPlan::Stop);
	}

	#[test]
	fn the_renewal_loop_is_requested_once_on_first_reliance() {
		let (r, _) = ledger(0);
		let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
		r.set_enroller(tx);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		let m = r
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		applied(&r, w.worker_id, m, ExchangeId::Renewal { attempt: 1 }).unwrap();
		assert_eq!(rx.try_recv().unwrap().worker_id, w.worker_id);
		assert!(rx.try_recv().is_err());
	}

	#[test]
	fn a_query_with_no_remote_reliance_commits() {
		let (r, clock) = ledger(0);
		clock.fail(ClockFault::Invalid);
		assert!(r.commit_success().is_ok());
	}

	#[test]
	fn one_shot_clock_faults_in_scheduling_fail_closed() {
		for scheduling in ["after", "until"] {
			let (r, clock) = ledger(0);
			let w = entry(1);
			let (_t, bound) = issue(&r, &w);
			clock.fail(ClockFault::ReadFailed(4));
			let refusal = match scheduling {
				"after" => r
					.backoff_until(w.worker_id, 1, Duration::from_millis(250))
					.unwrap_err(),
				_ => r.until(bound).unwrap_err(),
			};
			assert_eq!(
				refusal.cause,
				RelianceCause::Clock(ClockFault::ReadFailed(4))
			);
			// The fault does not disappear when the clock recovers: the query is sealed failed.
			clock.set(SEC);
			assert!(matches!(
				r.outcome(),
				QueryOutcome::Failed {
					worker: None,
					cause: RelianceCause::Clock(_)
				}
			));
			assert!(
				r.gate(w.worker_id, UseSite::new("start_task")).is_err(),
				"{scheduling}"
			);
			assert_eq!(r.renewal_plan(w.worker_id, None), RenewalPlan::Stop);
			assert!(r.commit_success().is_err(), "{scheduling}");
		}
		// A regression is a fault too, in both.
		let (r, clock) = ledger(10 * SEC);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		clock.set(9 * SEC);
		assert_eq!(
			r.until(bound).unwrap_err().cause,
			RelianceCause::Clock(ClockFault::Regressed)
		);
	}

	#[test]
	fn a_committed_result_is_never_reconsidered_after_a_clock_fault() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		assert!(r.commit_success().is_ok());
		clock.fail(ClockFault::Invalid);
		assert!(r.commit_success().is_ok(), "idempotent, samples nothing");
		assert!(r.delivery_gate(1).is_ok());
		let late = r.on_applied(
			w.worker_id,
			bound,
			ExchangeId::Renewal { attempt: 1 },
			bound,
		);
		assert!(matches!(
			late,
			Err(Refusal {
				cause: RelianceCause::Invalidated(_),
				..
			})
		));
		assert!(r.gate(w.worker_id, UseSite::new("start_task")).is_err());
		assert_eq!(r.outcome(), QueryOutcome::Committed);
	}

	#[test]
	fn an_ordinary_end_seals_the_ledger_and_cancellation_beats_a_late_success() {
		for end in [
			QueryEnd::Cancelled,
			QueryEnd::Error("scan failed".into()),
			QueryEnd::Dropped,
		] {
			let (r, _) = ledger(0);
			let w = entry(1);
			let (_t, _) = issue(&r, &w);
			let m = r
				.send_marker(w.worker_id, ExchangeId::Renewal { attempt: 1 })
				.unwrap();
			assert!(r.seal_ended(end.clone()));
			assert!(!r.seal_ended(QueryEnd::Retired), "the first end is kept");
			let failed = QueryOutcome::Failed {
				worker: None,
				cause: RelianceCause::QueryEnded(end.clone()),
			};
			assert_eq!(r.outcome(), failed);
			// Nothing after it authorizes, refreshes or commits.
			assert!(applied(&r, w.worker_id, m, ExchangeId::Renewal { attempt: 1 }).is_err());
			assert!(r.gate(w.worker_id, UseSite::new("start_task")).is_err());
			assert!(r.begin_dispatch(&w).is_err());
			assert!(
				r.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
					.is_err()
			);
			assert!(r.delivery_gate(1).is_err());
			assert!(
				r.commit_success().is_err(),
				"{end:?}: success cannot win later"
			);
			assert_eq!(r.outcome(), failed);
		}
		// The other order: a commit first, then a cancellation is context only.
		let (r, _) = ledger(0);
		let (_t, _) = issue(&r, &entry(1));
		assert!(r.commit_success().is_ok());
		assert!(!r.seal_ended(QueryEnd::Cancelled));
		assert_eq!(r.outcome(), QueryOutcome::Committed);
	}

	#[test]
	fn an_expired_worker_is_refused_at_the_dial_and_lease_write_gates() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (t, bound) = issue(&r, &w);
		drop(t);
		clock.set(bound.as_nanos());
		// No dial for a worker whose bound has passed, even though nothing observed the lapse yet.
		let refusal = r.begin_dispatch(&w).unwrap_err();
		assert_eq!(refusal.cause, RelianceCause::Lapsed { bound });
		assert_eq!(r.state(w.worker_id), Some(WorkerReliance::Lapsed { bound }));
		// And no lease write after it, on either path.
		let (r2, clock2) = ledger(0);
		let (_t2, bound2) = issue(&r2, &w);
		clock2.set(bound2.as_nanos());
		for path in [
			ExchangeId::Renewal { attempt: 1 },
			ExchangeId::Dispatch { ticket: 1 },
		] {
			assert!(r2.send_marker(w.worker_id, path).is_err(), "{path:?}");
		}
		// A first issuance has no old bound: its lease write is admitted.
		let (r3, _) = ledger(0);
		let _t3 = r3.begin_dispatch(&w).unwrap();
		assert!(
			r3.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
				.is_ok()
		);
		assert!(
			r3.send_marker(w.worker_id, ExchangeId::Renewal { attempt: 1 })
				.is_err()
		);
	}

	#[test]
	fn closing_renewal_refuses_the_next_renewal_write_synchronously() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		clock.set(5 * SEC);
		assert_eq!(r.renewal_plan(w.worker_id, None), RenewalPlan::Attempt);
		r.close_renewal();
		assert_eq!(r.renewal_plan(w.worker_id, None), RenewalPlan::Stop);
		assert!(
			r.send_marker(w.worker_id, ExchangeId::Renewal { attempt: 1 })
				.is_err()
		);
		// Dispatch is not renewal: its gates are unaffected.
		assert!(
			r.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
				.is_ok()
		);
	}

	#[test]
	fn an_applied_completed_past_its_budget_refreshes_nothing() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		clock.set(5 * SEC);
		let marker = r
			.send_marker(w.worker_id, ExchangeId::Renewal { attempt: 1 })
			.unwrap();
		let deadline = BootInstant(5 * SEC + 3 * SEC);
		clock.set(5 * SEC + 3 * SEC + 1); // starved: completed 1 ns after its whole budget
		assert_eq!(
			r.on_applied(
				w.worker_id,
				marker,
				ExchangeId::Renewal { attempt: 1 },
				deadline
			),
			Ok(Applied::Late)
		);
		assert_eq!(
			r.state(w.worker_id),
			Some(WorkerReliance::Reliable {
				bound,
				marker: BootInstant(0)
			}),
			"the old bound is kept, not refreshed"
		);
		assert_eq!(r.outcome(), QueryOutcome::Open);
		// A first issuance completed past its budget is a failed first issuance.
		let (r2, clock2) = ledger(0);
		let _t2 = r2.begin_dispatch(&w).unwrap();
		let m = r2
			.send_marker(w.worker_id, ExchangeId::Dispatch { ticket: 1 })
			.unwrap();
		clock2.set(3 * SEC + 1);
		let refusal = r2
			.on_applied(
				w.worker_id,
				m,
				ExchangeId::Dispatch { ticket: 1 },
				BootInstant(3 * SEC),
			)
			.unwrap_err();
		assert!(matches!(refusal.cause, RelianceCause::IssuanceFailed(_)));
		assert!(matches!(r2.outcome(), QueryOutcome::Failed { .. }));
	}

	#[test]
	fn within_records_an_overrun_and_fails_closed_on_a_clock_fault() {
		let (r, clock) = ledger(0);
		assert_eq!(
			r.within(
				WorkerId::from_u128(1),
				ExchangeId::Dispatch { ticket: 1 },
				Phase::Whole,
				BootInstant(0)
			),
			Ok(true)
		);
		clock.set(1);
		assert_eq!(
			r.within(
				WorkerId::from_u128(1),
				ExchangeId::Dispatch { ticket: 1 },
				Phase::Whole,
				BootInstant(0)
			),
			Ok(false)
		);
		assert_eq!(r.budget_overruns(), 1);
		clock.fail(ClockFault::ReadFailed(9));
		assert!(
			r.within(
				WorkerId::from_u128(1),
				ExchangeId::Dispatch { ticket: 1 },
				Phase::Whole,
				BootInstant(SEC)
			)
			.is_err()
		);
		assert!(matches!(
			r.outcome(),
			QueryOutcome::Failed { worker: None, .. }
		));
	}

	#[test]
	fn delivery_is_gated_on_every_bound_while_open() {
		let (r, clock) = ledger(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		clock.set(bound.as_nanos() - 1);
		assert!(r.delivery_gate(1).is_ok());
		// Nothing else ran: no renewal, no commit, no gate. The delivery itself observes the lapse.
		clock.set(bound.as_nanos());
		let refusal = r.delivery_gate(1).unwrap_err();
		assert_eq!(refusal.cause, RelianceCause::Lapsed { bound });
		assert_eq!(refusal.worker, Some(w.worker_id));
		assert!(r.commit_success().is_err());
		// A clock fault at delivery fails closed.
		let (r2, clock2) = ledger(0);
		let (_t2, _) = issue(&r2, &w);
		clock2.fail(ClockFault::ReadFailed(3));
		assert_eq!(
			r2.delivery_gate(1).unwrap_err().cause,
			RelianceCause::Clock(ClockFault::ReadFailed(3))
		);
		assert!(r2.commit_success().is_err());
	}

	#[test]
	fn refusals_map_to_the_failure_tables_outward_kinds() {
		use crate::error::TaskFailureKind;
		let w = WorkerId::from_u128(3);
		let kind = |worker, cause| Refusal { worker, cause }.task_failure_kind();
		assert_eq!(
			kind(
				Some(w),
				RelianceCause::Lapsed {
					bound: BootInstant(1)
				}
			),
			TaskFailureKind::WorkerLost { worker_id: w }
		);
		assert_eq!(
			kind(Some(w), RelianceCause::Auth("tls".into())),
			TaskFailureKind::WorkerLost { worker_id: w }
		);
		for local in [
			RelianceCause::Clock(ClockFault::Regressed),
			RelianceCause::Arithmetic,
		] {
			// Local faults are never WorkerLost, with or without a worker in context.
			assert!(matches!(
				kind(Some(w), local.clone()),
				TaskFailureKind::Internal { .. }
			));
			assert!(matches!(
				kind(None, local),
				TaskFailureKind::Internal { .. }
			));
		}
		assert_eq!(
			kind(None, RelianceCause::QueryEnded(QueryEnd::Cancelled)),
			TaskFailureKind::Cancelled
		);
		assert!(matches!(
			kind(None, RelianceCause::QueryEnded(QueryEnd::DriverLost)),
			TaskFailureKind::Internal { .. }
		));
	}

	/// Collects every recorded event, in delivery order.
	#[derive(Default)]
	struct Collect(Mutex<Vec<RelianceEvent>>);

	impl RelianceObserver for Collect {
		fn record(&self, event: &RelianceEvent) {
			self.0.lock().unwrap().push(event.clone());
		}
	}

	fn observed(at: u128) -> (Arc<QueryReliance>, Arc<SettableClock>, Arc<Collect>) {
		let (r, clock) = ledger(at);
		let collect = Arc::new(Collect::default());
		r.set_query(QueryId::from_u128(7));
		r.set_observer(collect.clone());
		(r, clock, collect)
	}

	fn kinds(collect: &Collect) -> Vec<(u64, &'static str)> {
		let mut events = collect.0.lock().unwrap().clone();
		events.sort_by_key(|e| e.seq);
		events.iter().map(|e| (e.seq, e.kind)).collect()
	}

	#[test]
	fn the_observer_records_every_decision_in_transition_order() {
		let (r, clock, collect) = observed(0);
		let w = entry(1);
		let ticket = r.begin_dispatch(&w).unwrap();
		let marker = r.send_marker(w.worker_id, ticket.exchange()).unwrap();
		assert!(
			r.within(
				w.worker_id,
				ticket.exchange(),
				Phase::Ack,
				BootInstant(2 * SEC)
			)
			.unwrap()
		);
		let bound = match r
			.on_applied(w.worker_id, marker, ticket.exchange(), BootInstant(2 * SEC))
			.unwrap()
		{
			Applied::Bound(bound) => bound,
			Applied::Late => panic!("on time"),
		};
		let mut site = UseSite::new("start_task");
		site.ticket = Some(ticket.ticket());
		r.gate(w.worker_id, site).unwrap();
		drop(ticket);
		clock.set(5 * SEC);
		assert_eq!(r.renewal_plan(w.worker_id, None), RenewalPlan::Attempt);
		let start = r.begin_renewal_attempt(w.worker_id).unwrap();
		assert_eq!(start.attempt, 1);
		assert_eq!(start.connect_by, BootInstant(5 * SEC + 2 * SEC));
		assert_eq!(start.whole_by, BootInstant(5 * SEC + 3 * SEC));
		let renewal = ExchangeId::Renewal { attempt: 1 };
		let m2 = r.send_marker(w.worker_id, renewal).unwrap();
		let refreshed = r
			.on_applied(w.worker_id, m2, renewal, start.whole_by)
			.unwrap();
		assert_eq!(refreshed, Applied::Bound(BootInstant(5 * SEC + SPAN_NS)));
		r.end_renewal_attempt(w.worker_id, 1, "applied", None);
		clock.set(5 * SEC + SPAN_NS);
		assert!(r.delivery_gate(1).is_err());
		assert!(r.commit_success().is_err());
		assert_eq!(
			kinds(&collect).iter().map(|(_, k)| *k).collect::<Vec<_>>(),
			vec![
				"dispatch_begin",
				"lease_write",
				"phase",
				"applied",
				"use",
				"dispatch_release",
				"plan",
				"renewal_attempt_begin",
				"lease_write",
				"applied",
				"renewal_attempt_end",
				"lapsed",
				"query_failed",
				"delivery",
				"commit",
			]
		);
		// Sequence numbers are contiguous from 1.
		let seqs: Vec<u64> = kinds(&collect).iter().map(|(seq, _)| *seq).collect();
		assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
		let events = collect.0.lock().unwrap().clone();
		let find = |kind: &str, n: usize| {
			events
				.iter()
				.filter(|e| e.kind == kind)
				.nth(n)
				.cloned()
				.unwrap()
		};
		// The first issuance installs its candidate; the renewal refresh records its slack.
		assert_eq!(find("applied", 0).fields["result"], "installed");
		assert_eq!(
			find("applied", 0).fields["old_bound_ns"],
			serde_json::Value::Null
		);
		let refresh = find("applied", 1);
		assert_eq!(refresh.fields["result"], "installed");
		assert_eq!(refresh.fields["exchange"]["attempt"], 1);
		assert_eq!(
			refresh.fields["slack_ns"],
			serde_json::Value::from((bound.as_nanos() - 5 * SEC) as u64)
		);
		assert_eq!(find("use", 0).fields["result"], "authorized");
		assert_eq!(find("lapsed", 0).fields["site"], "deliver");
		assert_eq!(find("delivery", 0).fields["result"], "suppressed");
		assert!(
			events
				.iter()
				.all(|e| e.query == Some(QueryId::from_u128(7)))
		);
	}

	#[test]
	fn a_kept_bound_is_not_recorded_as_a_refresh_and_post_commit_delivery_is_legal() {
		let (r, clock, collect) = observed(0);
		let w = entry(1);
		let (_t, bound) = issue(&r, &w);
		clock.set(SEC);
		// An older on-time reply keeps the bound: "kept", not "installed".
		assert_eq!(
			r.on_applied(
				w.worker_id,
				BootInstant(0),
				ExchangeId::Renewal { attempt: 1 },
				BootInstant(2 * SEC)
			),
			Ok(Applied::Bound(bound))
		);
		assert!(r.commit_success().is_ok());
		clock.set(bound.as_nanos() + 60 * SEC);
		assert!(r.delivery_gate(1).is_ok());
		let events = collect.0.lock().unwrap().clone();
		let kept = events
			.iter()
			.filter(|e| e.kind == "applied")
			.last()
			.unwrap();
		assert_eq!(kept.fields["result"], "kept");
		let delivered = events.iter().find(|e| e.kind == "delivery").unwrap();
		assert_eq!(delivered.fields["outcome_before"], "committed");
		assert_eq!(delivered.fields["result"], "delivered");
	}

	#[test]
	fn concurrent_decisions_get_unique_contiguous_sequence_numbers() {
		let (r, _, collect) = observed(0);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		let threads: Vec<_> = (0..8)
			.map(|_| {
				let r = Arc::clone(&r);
				std::thread::spawn(move || {
					for _ in 0..50 {
						r.gate(WorkerId::from_u128(1), UseSite::new("publish"))
							.unwrap();
					}
				})
			})
			.collect();
		for thread in threads {
			thread.join().unwrap();
		}
		let seqs: Vec<u64> = kinds(&collect).iter().map(|(seq, _)| *seq).collect();
		assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
		assert_eq!(
			collect
				.0
				.lock()
				.unwrap()
				.iter()
				.filter(|e| e.kind == "use")
				.count(),
			400
		);
	}

	#[test]
	fn no_observer_records_nothing() {
		let (r, _) = ledger(0);
		let w = entry(1);
		let (_t, _) = issue(&r, &w);
		assert!(r.lock().events.is_empty());
		assert_eq!(r.lock().seq, 0);
	}
}
