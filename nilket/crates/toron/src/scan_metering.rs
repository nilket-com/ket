//! Metered consumption of scan streams (ADR 551).
//!
//! Base-relation reads happen at thirty-one call sites, only one of which was a
//! bare `ExecutionPlan::Scan` node. Instrumenting that one node made `scan` mean
//! "reads that were not fused", which is why ADR 550 withdrew ADR 538's component
//! fork. Attribution therefore hangs off stream consumption, which every route
//! must go through, rather than off a node kind.
//!
//! Two invariants carry that:
//!
//! - **The interval is acquisition only.** It opens on the first poll after a
//!   yield and closes the instant a batch is yielded, so consumer work — fused
//!   predicate evaluation, aggregate folding, join build and probe — falls
//!   outside it. Time parked waiting on storage stays inside, because the
//!   consumer is blocked on the read and nothing else is using that wall time.
//! - **Classification follows `ScanOrigin`,** carried by the stream itself. The
//!   storage decorator cannot be the classifier: both wrappers delegate to base
//!   storage when they miss.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use futures::Stream;
use futures::stream::BoxStream;

use crate::error::StorageError;
use crate::storage::{ScanOrigin, ScanStream};

/// A source of nondecreasing microsecond readings.
///
/// Nondecreasing, not increasing: two readings taken close together may be
/// **equal**, so a zero-width acquisition interval is ordinary and is not
/// evidence of a defect. Only a reading that goes *backwards* is one, and that is
/// recorded rather than clamped.
///
/// Injected so the ADR 551 controls can assert exact equalities instead of
/// tolerances around a wall-clock reading.
///
/// (The conventional one-word adjective for this property is avoided throughout:
/// ADR 043's forbidden-symbol guard substring-matches a transport crate name that
/// happens to sit inside it.)
pub trait Clock: Send + Sync + std::fmt::Debug {
	fn now_micros(&self) -> u64;
}

/// The production clock, reading elapsed time from a fixed base instant.
#[derive(Debug)]
pub struct SystemClock {
	base: Instant,
}

impl SystemClock {
	pub fn new() -> Self {
		Self {
			base: Instant::now(),
		}
	}
}

impl Default for SystemClock {
	fn default() -> Self {
		Self::new()
	}
}

impl Clock for SystemClock {
	fn now_micros(&self) -> u64 {
		u64::try_from(self.base.elapsed().as_micros()).unwrap_or(u64::MAX)
	}
}

/// A clock advanced by hand, for controls that assert exact interval arithmetic.
#[derive(Debug, Default)]
pub struct ManualClock {
	micros: AtomicU64,
}

impl ManualClock {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn advance(&self, micros: u64) {
		self.micros.fetch_add(micros, Ordering::SeqCst);
	}
}

impl Clock for ManualClock {
	fn now_micros(&self) -> u64 {
		self.micros.load(Ordering::SeqCst)
	}
}

/// Acquisition time accumulated per stream origin.
///
/// **This is an acquisition ledger, not the task phase ledger.** It totals time by
/// origin and can do nothing else: it cannot express nesting, cannot subtract a
/// scan interval from an enclosing operator bracket, and therefore cannot enforce
/// that task phases are exclusive. The task ledger that owns
/// `inclusive operator = exclusive operator + nested scan` must derive exclusive
/// operator time **structurally**, from nesting it records itself, and is a later
/// increment. Reading these totals as if they were exclusive phases would repeat
/// the ADR 538 error in a new place.
///
/// Origins are kept apart rather than summed because ADR 551 routes them to
/// different components — with one asymmetry that matters:
///
/// - `BaseRelation` acquisition is `scan`.
/// - `MaterializedExchangeInput` acquisition is `shuffle`.
/// - `LiveExchangeInput` acquisition is **neither, and not both**. A live poll
///   contains a pull/wait and a decode, which ADR 538 assigns to `shuffle` and
///   `transport` respectively. One outer interval cannot be split after the fact,
///   so the two must be measured separately inside `LiveInputStorageScan`. The
///   total here is retained as a **diagnostic bound only** and must never be fed
///   into a component.
#[derive(Debug, Default)]
pub struct ScanAcquisitionLedger {
	base_relation_micros: AtomicU64,
	materialized_exchange_micros: AtomicU64,
	live_exchange_setup_micros: AtomicU64,
	live_exchange_diagnostic_micros: AtomicU64,
	live_pull_wait_micros: AtomicU64,
	live_decode_micros: AtomicU64,
	clock_regressions: AtomicU64,
}

impl ScanAcquisitionLedger {
	pub fn new() -> Self {
		Self::default()
	}

	/// Add one measured interval to the counter it belongs to.
	fn record_slot(&self, slot: AcquisitionSlot, micros: u64) {
		let counter = match slot {
			AcquisitionSlot::BaseRelation => &self.base_relation_micros,
			AcquisitionSlot::MaterializedExchange => &self.materialized_exchange_micros,
			AcquisitionSlot::LiveSetup => &self.live_exchange_setup_micros,
			AcquisitionSlot::LiveDiagnostic => &self.live_exchange_diagnostic_micros,
			AcquisitionSlot::LivePullWait => &self.live_pull_wait_micros,
			AcquisitionSlot::LiveDecode => &self.live_decode_micros,
		};
		counter.fetch_add(micros, Ordering::SeqCst);
	}

	/// A reading that went backwards. Counted rather than clamped away: a
	/// saturating subtraction is what hid ADR 538's phase defect for a full arc.
	fn record_clock_regression(&self) {
		self.clock_regressions.fetch_add(1, Ordering::SeqCst);
	}

	pub fn base_relation_micros(&self) -> u64 {
		self.base_relation_micros.load(Ordering::SeqCst)
	}

	pub fn materialized_exchange_micros(&self) -> u64 {
		self.materialized_exchange_micros.load(Ordering::SeqCst)
	}

	/// Diagnostic bound on live-exchange acquisition.
	///
	/// **Not a component input.** It contains pull/wait and decode fused into one
	/// interval; `shuffle` and `transport` take their figures from measurements
	/// made inside `LiveInputStorageScan`, not from this total.
	pub fn live_exchange_diagnostic_micros(&self) -> u64 {
		self.live_exchange_diagnostic_micros.load(Ordering::SeqCst)
	}

	/// Readings that went backwards.
	///
	/// A nonzero count **vetoes the receipt**: attribution derived from a clock
	/// that regressed is corrupted evidence, and counting it without invalidating
	/// the run would let that evidence through. The veto lands with the receipt
	/// wiring; the count exists so it can be enforced rather than inferred.
	pub fn clock_regressions(&self) -> u64 {
		self.clock_regressions.load(Ordering::SeqCst)
	}

	/// Live-exchange stream setup, which is `shuffle`.
	///
	/// Distinct from the diagnostic poll total: setup happens before any row
	/// arrives, so it carries no decode and can enter a component cleanly.
	pub fn live_exchange_setup_micros(&self) -> u64 {
		self.live_exchange_setup_micros.load(Ordering::SeqCst)
	}

	/// Waiting on a live producer partition, which is `shuffle`.
	///
	/// Measured inside `LiveInputStorageScan`, where pull and decode are still
	/// separable. By the time a live poll returns, the two are fused into one
	/// interval that belongs to neither component.
	pub fn live_pull_wait_micros(&self) -> u64 {
		self.live_pull_wait_micros.load(Ordering::SeqCst)
	}

	/// Decoding a live producer payload, which is `transport`.
	pub fn live_decode_micros(&self) -> u64 {
		self.live_decode_micros.load(Ordering::SeqCst)
	}
}

/// Which half of a live poll an interval covers.
#[derive(Clone, Copy, Debug)]
enum LiveSegment {
	/// Waiting on the producer partition — `shuffle`.
	PullWait,
	/// Decoding the payload it delivered — `transport`.
	Decode,
}

/// The clock and ledger a task meters its scans against.
///
/// Cloned into every execution path so that opening and consuming a scan is one
/// transition with one owner, rather than a timer duplicated at thirty-one call
/// sites.
#[derive(Clone, Debug)]
pub struct ScanMeter {
	enabled: Option<EnabledScanMeter>,
}

#[derive(Clone, Debug)]
struct EnabledScanMeter {
	clock: Arc<dyn Clock>,
	ledger: Arc<ScanAcquisitionLedger>,
	/// The task's phase ledger, sharing this meter's clock.
	///
	/// ADR 551: each interval is written to **both** ledgers at the moment it is
	/// measured. The phase ledger must never reconstruct an interval by
	/// differencing cumulative counters — a difference is not an interval, it is
	/// two readings with everything that happened between them folded in, which is
	/// how a nested bracket would absorb work that was never inside it.
	phases: Arc<crate::phase_ledger::TaskPhaseLedger>,
}

impl EnabledScanMeter {
	/// Record one measured interval into both ledgers.
	fn record(
		&self,
		acquisition: AcquisitionSlot,
		kind: crate::phase_ledger::AcquisitionKind,
		micros: u64,
	) {
		self.ledger.record_slot(acquisition, micros);
		self.phases.record_acquisition(kind, micros);
	}

	/// A clock reading that went backwards, vetoing **both** receipts.
	///
	/// The two ledgers share one clock, so a regression corrupts both. Recording
	/// it on the acquisition ledger alone would let the phase receipt seal clean
	/// over the same broken timeline — published in one place and withheld in
	/// another, which is worse than either.
	fn record_clock_regression(&self) {
		self.ledger.record_clock_regression();
		self.phases.record_clock_regression();
	}
}

/// Which counter on the acquisition ledger an interval belongs to.
///
/// Distinct from `phase_ledger::AcquisitionKind` because the two ledgers do not
/// partition the same way: live setup and live pull/wait are both `Shuffle` to the
/// identity, but stay separate counters in the receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcquisitionSlot {
	BaseRelation,
	MaterializedExchange,
	LiveSetup,
	LiveDiagnostic,
	LivePullWait,
	LiveDecode,
}

impl ScanMeter {
	/// Meter against a task's clock, acquisition ledger and phase ledger.
	///
	/// All three belong to the same receipt-collecting task, and the clock passed
	/// here **must** be the one the phase ledger was built with: the ledger
	/// subtracts acquisition intervals from operator brackets, and two clocks
	/// would make that subtraction underflow and withhold every receipt.
	pub fn new(
		clock: Arc<dyn Clock>,
		ledger: Arc<ScanAcquisitionLedger>,
		phases: Arc<crate::phase_ledger::TaskPhaseLedger>,
	) -> Self {
		Self {
			enabled: Some(EnabledScanMeter {
				clock,
				ledger,
				phases,
			}),
		}
	}

	/// A meter that measures nothing, for tasks collecting no receipt.
	///
	/// Attribution exists to produce evidence. A task that will emit none has no
	/// use for a clock read and an atomic update per poll, and every ordinary
	/// query would otherwise pay for telemetry nobody reads. Disabled is the
	/// default; metering is switched on where a receipt is actually collected.
	pub fn disabled() -> Self {
		Self { enabled: None }
	}

	/// A meter for one receipt-collecting task: one clock, shared by a fresh
	/// acquisition ledger and a fresh phase ledger.
	pub fn system() -> Self {
		let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());
		Self::new(
			clock.clone(),
			Arc::new(ScanAcquisitionLedger::new()),
			Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock)),
		)
	}

	/// The task's phase ledger, when collecting. `None` while disabled.
	/// The task phase ledger, for opening operator brackets.
	///
	/// Publication does not go through here — `seal_receipt` is the only path
	/// that publishes, and it reconciles first.
	pub fn phases(&self) -> Option<&Arc<crate::phase_ledger::TaskPhaseLedger>> {
		self.enabled.as_ref().map(|meter| &meter.phases)
	}

	/// Publish this task's receipt — the one seal path.
	///
	/// Reconciles the two ledgers before publishing. Every interval is written to
	/// both at the moment it is measured, so they must agree exactly:
	///
	/// - phase base scan  = acquisition base relation
	/// - phase shuffle    = materialized exchange + live setup + live pull/wait
	/// - phase transport  = live decode
	/// - phase diagnostic = live diagnostic
	///
	/// A divergence means one ledger missed a write or classified an interval
	/// differently, and after that neither figure is evidence. It withholds.
	///
	/// A disabled meter has no receipt to publish and reports `AlreadySealed`,
	/// which is the honest answer: nothing was ever collected.
	pub fn seal_receipt(&self) -> crate::phase_ledger::SealOutcome {
		use crate::phase_ledger::SealOutcome;

		let Some(meter) = self.enabled.as_ref() else {
			return SealOutcome::AlreadySealed;
		};

		let ledger = &meter.ledger;
		let shuffle = ledger
			.materialized_exchange_micros()
			.checked_add(ledger.live_exchange_setup_micros())
			.and_then(|sum| sum.checked_add(ledger.live_pull_wait_micros()));

		let outcome = meter.phases.seal();
		let SealOutcome::Sealed(totals) = outcome else {
			return outcome;
		};

		let agrees = shuffle == Some(totals.shuffle_micros)
			&& totals.base_scan_micros == ledger.base_relation_micros()
			&& totals.transport_micros == ledger.live_decode_micros()
			&& totals.live_diagnostic_micros == ledger.live_exchange_diagnostic_micros();

		if agrees {
			SealOutcome::Sealed(totals)
		} else {
			meter.phases.record_reconciliation_mismatch();
			SealOutcome::Withheld(meter.phases.faults())
		}
	}

	pub fn is_enabled(&self) -> bool {
		self.enabled.is_some()
	}

	/// The ledger, when this meter is collecting. `None` while disabled.
	pub fn ledger(&self) -> Option<&Arc<ScanAcquisitionLedger>> {
		self.enabled.as_ref().map(|meter| &meter.ledger)
	}

	/// Open an interval, or `None` while disabled — no clock read either way.
	pub fn interval_opened(&self) -> Option<u64> {
		self.enabled.as_ref().map(|meter| meter.clock.now_micros())
	}

	/// Elapsed micros since an interval opened, or `None` while disabled.
	pub fn elapsed_since(&self, opened: Option<u64>) -> Option<u64> {
		let (Some(meter), Some(opened)) = (self.enabled.as_ref(), opened) else {
			return None;
		};
		meter.clock.now_micros().checked_sub(opened)
	}

	/// Close a live pull/wait interval, which is `shuffle`.
	///
	/// Called from inside `LiveInputStorageScan`, the one place where a live
	/// poll's pull/wait and decode are still separable. Once the poll returns they
	/// are a single interval belonging to neither component.
	pub fn close_live_pull_wait(&self, opened: Option<u64>) {
		self.close_live(opened, LiveSegment::PullWait);
	}

	/// Close a live decode interval, which is `transport`.
	pub fn close_live_decode(&self, opened: Option<u64>) {
		self.close_live(opened, LiveSegment::Decode);
	}

	fn close_live(&self, opened: Option<u64>, segment: LiveSegment) {
		let (Some(meter), Some(opened)) = (self.enabled.as_ref(), opened) else {
			return;
		};
		let closed = meter.clock.now_micros();
		let Some(elapsed) = closed.checked_sub(opened) else {
			meter.record_clock_regression();
			return;
		};
		// One measured interval, written to both ledgers at the moment it is
		// measured. Pull/wait is `shuffle`; decode is `transport`.
		let (slot, kind) = match segment {
			LiveSegment::PullWait => (
				AcquisitionSlot::LivePullWait,
				crate::phase_ledger::AcquisitionKind::Shuffle,
			),
			LiveSegment::Decode => (
				AcquisitionSlot::LiveDecode,
				crate::phase_ledger::AcquisitionKind::Transport,
			),
		};
		meter.record(slot, kind, elapsed);
	}

	/// Open a scan and hand back a metered stream — the one transition.
	///
	/// The opening interval is attributed **after** the stream returns, because
	/// only then is the origin known and only then is it decidable which component
	/// the time belongs to.
	///
	/// **A failed open records nothing.** It invalidates the timed child, which
	/// produces no eligible component receipt, so charging a partial interval to a
	/// component would attribute time to a task whose result is discarded.
	pub async fn open<F>(&self, open: F) -> Result<MeteredScanStream, StorageError>
	where
		F: std::future::Future<Output = Result<ScanStream, StorageError>>,
	{
		let Some(meter) = self.enabled.as_ref() else {
			// Disabled: no clock read, no atomic update, no ledger.
			return Ok(MeteredScanStream::unmetered(open.await?));
		};
		let opened = meter.clock.now_micros();
		let stream = open.await?;
		let closed = meter.clock.now_micros();
		match closed.checked_sub(opened) {
			// Stream setup: live input carries no decode yet, so it is `shuffle`.
			Some(elapsed) => {
				let (slot, kind) = match stream.origin() {
					ScanOrigin::BaseRelation => (
						AcquisitionSlot::BaseRelation,
						crate::phase_ledger::AcquisitionKind::BaseScan,
					),
					ScanOrigin::MaterializedExchangeInput => (
						AcquisitionSlot::MaterializedExchange,
						crate::phase_ledger::AcquisitionKind::Shuffle,
					),
					ScanOrigin::LiveExchangeInput => (
						AcquisitionSlot::LiveSetup,
						crate::phase_ledger::AcquisitionKind::Shuffle,
					),
				};
				meter.record(slot, kind, elapsed);
			}
			None => meter.record_clock_regression(),
		}
		Ok(MeteredScanStream::new(stream, self))
	}
}

/// A scan stream that accumulates its own acquisition time, classified by origin.
pub struct MeteredScanStream {
	schema: arrow::datatypes::SchemaRef,
	origin: ScanOrigin,
	inner: BoxStream<'static, Result<RecordBatch, StorageError>>,
	meter: Option<EnabledScanMeter>,
	acquisition_opened_micros: Option<u64>,
}

impl MeteredScanStream {
	/// Meter a scan stream, taking the origin from the stream itself.
	/// Meter a scan stream against a task's meter.
	///
	/// Takes the whole meter rather than its parts, so a stream cannot be built
	/// with a clock from one task and a ledger from another.
	pub fn new(stream: ScanStream, meter: &ScanMeter) -> Self {
		let schema = stream.schema.clone();
		let (origin, inner) = stream.into_metered_parts();
		Self {
			schema,
			origin,
			inner,
			meter: meter.enabled.clone(),
			acquisition_opened_micros: None,
		}
	}

	/// A pass-through for tasks collecting no receipt: same transition and same
	/// origin, but no clock read and no atomic update per poll.
	pub(crate) fn unmetered(stream: ScanStream) -> Self {
		let schema = stream.schema.clone();
		let (origin, inner) = stream.into_metered_parts();
		Self {
			schema,
			origin,
			inner,
			meter: None,
			acquisition_opened_micros: None,
		}
	}

	/// The origin is readable after consumption has begun, so provenance cannot
	/// be erased partway through a read.
	pub fn schema(&self) -> &arrow::datatypes::SchemaRef {
		&self.schema
	}

	pub fn origin(&self) -> ScanOrigin {
		self.origin
	}
}

impl Stream for MeteredScanStream {
	type Item = Result<RecordBatch, StorageError>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();
		let Some(meter) = this.meter.as_ref() else {
			// Disabled: the transition is still the only way in, but it costs
			// nothing beyond the inner poll.
			return this.inner.as_mut().poll_next(cx);
		};
		if this.acquisition_opened_micros.is_none() {
			this.acquisition_opened_micros = Some(meter.clock.now_micros());
		}
		match this.inner.as_mut().poll_next(cx) {
			// The read is still in flight; the interval stays open across parks.
			Poll::Pending => Poll::Pending,
			ready => {
				let opened = this
					.acquisition_opened_micros
					.take()
					.expect("acquisition interval is opened before the stream is polled");
				let closed = meter.clock.now_micros();
				match closed.checked_sub(opened) {
					// A live poll fuses pull/wait with decode, so it cannot enter a
					// component: it is diagnostic on both ledgers.
					Some(elapsed) => {
						let (slot, kind) = match this.origin {
							ScanOrigin::BaseRelation => (
								AcquisitionSlot::BaseRelation,
								crate::phase_ledger::AcquisitionKind::BaseScan,
							),
							ScanOrigin::MaterializedExchangeInput => (
								AcquisitionSlot::MaterializedExchange,
								crate::phase_ledger::AcquisitionKind::Shuffle,
							),
							ScanOrigin::LiveExchangeInput => (
								AcquisitionSlot::LiveDiagnostic,
								crate::phase_ledger::AcquisitionKind::LiveDiagnostic,
							),
						};
						meter.record(slot, kind, elapsed);
					}
					None => meter.record_clock_regression(),
				}
				ready
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use futures::StreamExt;

	use super::*;

	const POLL_COST: u64 = 7;
	const SCHEMA_FIELD: &str = "value";

	fn batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new(
			SCHEMA_FIELD,
			DataType::Int64,
			false,
		)]));
		RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
			.expect("single column batch builds")
	}

	/// A stream that charges the clock `POLL_COST` inside each poll, so the
	/// acquisition interval has a known exact width.
	fn charging_stream(
		clock: Arc<ManualClock>,
		batches: usize,
	) -> BoxStream<'static, Result<RecordBatch, StorageError>> {
		futures::stream::unfold(0usize, move |emitted| {
			let clock = clock.clone();
			async move {
				clock.advance(POLL_COST);
				(emitted < batches).then(|| (Ok(batch()), emitted + 1))
			}
		})
		.boxed()
	}

	fn metered(
		clock: Arc<ManualClock>,
		ledger: Arc<ScanAcquisitionLedger>,
		origin: ScanOrigin,
		batches: usize,
	) -> MeteredScanStream {
		let stream = ScanStream::new(
			batch().schema(),
			crate::ir::SessionId(1),
			crate::ir::QueryId::new(),
			origin,
			charging_stream(clock.clone(), batches),
		);
		let phases = Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock.clone()));
		MeteredScanStream::new(stream, &ScanMeter::new(clock, ledger, phases))
	}

	async fn drain_charging_consumer(
		mut stream: MeteredScanStream,
		clock: Arc<ManualClock>,
		consumer_cost: u64,
	) -> usize {
		let mut seen = 0usize;
		while let Some(item) = stream.next().await {
			item.expect("charging stream yields batches");
			seen += 1;
			// Consumer work happens strictly between yields.
			clock.advance(consumer_cost);
		}
		seen
	}

	/// ADR 551 Decision 1: consumer work between yielded batches is outside the
	/// scan scope. Scaling the consumer's per-batch cost must not move `scan`.
	///
	/// This is the control that discriminates the accepted definition from a
	/// "stream materialization" scope, which would absorb the consumer cost.
	#[tokio::test]
	async fn consumer_work_between_batches_is_outside_the_scan_scope() {
		let mut totals = Vec::new();
		for consumer_cost in [0_u64, 1_000] {
			let clock = Arc::new(ManualClock::new());
			let ledger = Arc::new(ScanAcquisitionLedger::new());
			let stream = metered(clock.clone(), ledger.clone(), ScanOrigin::BaseRelation, 4);
			let seen = drain_charging_consumer(stream, clock, consumer_cost).await;
			assert_eq!(seen, 4, "every batch is delivered");
			totals.push(ledger.base_relation_micros());
		}

		// Four yielded batches plus the terminating poll, each charged once.
		assert_eq!(totals[0], 5 * POLL_COST);
		assert_eq!(
			totals[0], totals[1],
			"scan is unchanged when per-batch consumer work grows"
		);
	}

	/// ADR 551 Decision 2: origin is carried by the stream and survives
	/// consumption, so provenance cannot be erased partway through a read.
	#[tokio::test]
	async fn origin_survives_consumption_and_selects_the_counter() {
		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		let mut stream = metered(
			clock.clone(),
			ledger.clone(),
			ScanOrigin::MaterializedExchangeInput,
			2,
		);

		assert_eq!(stream.origin(), ScanOrigin::MaterializedExchangeInput);
		while let Some(item) = stream.next().await {
			item.expect("charging stream yields batches");
			assert_eq!(
				stream.origin(),
				ScanOrigin::MaterializedExchangeInput,
				"origin is still readable mid-consumption"
			);
		}

		assert_eq!(ledger.materialized_exchange_micros(), 3 * POLL_COST);
		assert_eq!(
			ledger.base_relation_micros(),
			0,
			"exchange input never counts as a base-relation read"
		);
		assert_eq!(ledger.clock_regressions(), 0);
	}

	/// The clock is nondecreasing, not increasing: consecutive readings may be
	/// equal, so a zero-width acquisition interval is ordinary. It must record a
	/// zero total and must not be mistaken for a regression.
	#[tokio::test]
	async fn equal_clock_readings_are_ordinary_not_a_regression() {
		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		// A stream that never advances the clock: every reading is equal.
		let stream = ScanStream::new(
			batch().schema(),
			crate::ir::SessionId(1),
			crate::ir::QueryId::new(),
			ScanOrigin::BaseRelation,
			futures::stream::iter(vec![Ok(batch()), Ok(batch())]).boxed(),
		);
		let phases = Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock.clone()));
		let meter = ScanMeter::new(clock.clone(), ledger.clone(), phases);
		let mut stream = MeteredScanStream::new(stream, &meter);

		let mut seen = 0usize;
		while let Some(item) = stream.next().await {
			item.expect("stream yields batches");
			seen += 1;
		}

		assert_eq!(seen, 2);
		assert_eq!(
			ledger.base_relation_micros(),
			0,
			"zero-width intervals total zero"
		);
		assert_eq!(
			ledger.clock_regressions(),
			0,
			"equal readings are not a regression"
		);
	}

	fn scan_stream(origin: ScanOrigin, batches: usize) -> ScanStream {
		ScanStream::new(
			batch().schema(),
			crate::ir::SessionId(1),
			crate::ir::QueryId::new(),
			origin,
			futures::stream::iter((0..batches).map(|_| Ok(batch()))).boxed(),
		)
	}

	const OPEN_COST: u64 = 11;

	/// ADR 551: the opening interval is attributed only once the origin is known,
	/// and each origin lands in its own component counter — base relation to
	/// `scan`, materialized exchange to `shuffle`, live setup to `shuffle`.
	#[tokio::test]
	async fn the_opening_interval_is_attributed_by_returned_origin() {
		for (origin, expect) in [
			(ScanOrigin::BaseRelation, "base"),
			(ScanOrigin::MaterializedExchangeInput, "materialized"),
			(ScanOrigin::LiveExchangeInput, "live_setup"),
		] {
			let clock = Arc::new(ManualClock::new());
			let ledger = Arc::new(ScanAcquisitionLedger::new());
			let meter = ScanMeter::new(
				clock.clone(),
				ledger.clone(),
				Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock.clone())),
			);

			let opening = {
				let clock = clock.clone();
				async move {
					clock.advance(OPEN_COST);
					Ok(scan_stream(origin, 0))
				}
			};
			meter.open(opening).await.expect("open succeeds");

			let (base, materialized, live_setup) = (
				ledger.base_relation_micros(),
				ledger.materialized_exchange_micros(),
				ledger.live_exchange_setup_micros(),
			);
			match expect {
				"base" => assert_eq!((base, materialized, live_setup), (OPEN_COST, 0, 0)),
				"materialized" => assert_eq!((base, materialized, live_setup), (0, OPEN_COST, 0)),
				_ => assert_eq!((base, materialized, live_setup), (0, 0, OPEN_COST)),
			}
			assert_eq!(
				ledger.live_exchange_diagnostic_micros(),
				0,
				"stream setup is not a live poll"
			);
		}
	}

	/// ADR 551: a failed open invalidates the timed child, which yields no
	/// eligible component receipt. Charging its partial interval to a component
	/// would attribute time to a task whose result is thrown away.
	#[tokio::test]
	async fn a_failed_open_records_no_component_time() {
		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		let meter = ScanMeter::new(
			clock.clone(),
			ledger.clone(),
			Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock.clone())),
		);

		let opening = {
			let clock = clock.clone();
			async move {
				clock.advance(OPEN_COST);
				Err(StorageError::TableNotFound {
					table: crate::ir::TableId(4),
				})
			}
		};
		let opened = meter.open(opening).await;

		assert!(opened.is_err(), "the open fails");
		assert_eq!(ledger.base_relation_micros(), 0);
		assert_eq!(ledger.materialized_exchange_micros(), 0);
		assert_eq!(ledger.live_exchange_setup_micros(), 0);
		assert_eq!(ledger.live_exchange_diagnostic_micros(), 0);
		assert_eq!(ledger.clock_regressions(), 0);
	}

	/// ADR 551: a live poll's pull/wait is `shuffle` and its decode is
	/// `transport`. They are measured separately inside `LiveInputStorageScan`,
	/// because once the poll returns they are one interval belonging to neither.
	#[tokio::test]
	async fn live_pull_wait_and_decode_land_in_separate_components() {
		const PULL: u64 = 13;
		const DECODE: u64 = 5;

		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		let meter = ScanMeter::new(
			clock.clone(),
			ledger.clone(),
			Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock.clone())),
		);

		let opened = meter.interval_opened();
		clock.advance(PULL);
		meter.close_live_pull_wait(opened);

		let opened = meter.interval_opened();
		clock.advance(DECODE);
		meter.close_live_decode(opened);

		assert_eq!(ledger.live_pull_wait_micros(), PULL, "pull/wait is shuffle");
		assert_eq!(ledger.live_decode_micros(), DECODE, "decode is transport");
		assert_eq!(
			ledger.live_exchange_diagnostic_micros(),
			0,
			"the fused diagnostic total is not fed by the split"
		);
		assert_eq!(ledger.base_relation_micros(), 0, "no live time is scan");
		assert_eq!(ledger.clock_regressions(), 0);
	}

	/// A disabled meter takes no clock reading, so the split costs nothing on a
	/// task that will emit no receipt.
	#[tokio::test]
	async fn a_disabled_meter_records_no_live_split() {
		let meter = ScanMeter::disabled();
		assert!(!meter.is_enabled());
		assert!(
			meter.interval_opened().is_none(),
			"a disabled meter does not read the clock"
		);
		meter.close_live_pull_wait(None);
		meter.close_live_decode(None);
		assert!(meter.ledger().is_none(), "there is no ledger to read");
	}

	/// A clock that goes backwards, to prove a regression is caught rather than
	/// assumed impossible.
	#[derive(Debug)]
	struct RegressingClock {
		readings: std::sync::Mutex<Vec<u64>>,
	}

	impl Clock for RegressingClock {
		fn now_micros(&self) -> u64 {
			let mut readings = self.readings.lock().expect("test clock");
			if readings.is_empty() {
				0
			} else {
				readings.remove(0)
			}
		}
	}

	/// The two ledgers share one clock, so a regression corrupts both. Recording
	/// it on the acquisition ledger alone would let the phase receipt seal clean
	/// over the same broken timeline.
	#[tokio::test]
	async fn a_clock_regression_vetoes_both_ledgers() {
		use crate::phase_ledger::{SealOutcome, TaskPhaseLedger};

		// Open reads 100, then 40: the interval closes before it opened.
		let clock: Arc<dyn Clock> = Arc::new(RegressingClock {
			readings: std::sync::Mutex::new(vec![100, 40]),
		});
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		let phases = Arc::new(TaskPhaseLedger::new(clock.clone()));
		let meter = ScanMeter::new(clock, ledger.clone(), phases.clone());

		meter
			.open(async { Ok(scan_stream(ScanOrigin::BaseRelation, 0)) })
			.await
			.expect("open succeeds");

		assert_eq!(
			ledger.clock_regressions(),
			1,
			"the acquisition ledger sees it"
		);
		assert_eq!(
			phases.faults().clock_regressions,
			1,
			"and so does the phase ledger, over the same clock"
		);
		assert!(matches!(
			meter.seal_receipt(),
			SealOutcome::Withheld(faults) if faults.clock_regressions == 1
		));
	}

	/// The one seal path reconciles both ledgers before publishing.
	#[tokio::test]
	async fn seal_receipt_reconciles_the_two_ledgers() {
		use crate::phase_ledger::SealOutcome;
		use futures::StreamExt;

		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		let phases = Arc::new(crate::phase_ledger::TaskPhaseLedger::new(clock.clone()));
		let meter = ScanMeter::new(clock.clone(), ledger.clone(), phases);

		let mut stream = metered_with(&meter, clock.clone(), ScanOrigin::BaseRelation, 3);
		while let Some(item) = stream.next().await {
			item.expect("stream yields batches");
		}

		match meter.seal_receipt() {
			SealOutcome::Sealed(totals) => {
				assert_eq!(totals.base_scan_micros, ledger.base_relation_micros());
				assert_eq!(totals.base_scan_micros, 4 * POLL_COST);
			}
			other => panic!("agreeing ledgers publish, got {other:?}"),
		}
	}

	/// If the ledgers ever disagree about the same intervals, neither figure is
	/// evidence and the receipt is withheld.
	#[tokio::test]
	async fn diverging_ledgers_withhold_the_receipt() {
		use crate::phase_ledger::{AcquisitionKind, SealOutcome, TaskPhaseLedger};

		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(ScanAcquisitionLedger::new());
		let phases = Arc::new(TaskPhaseLedger::new(clock.clone()));
		let meter = ScanMeter::new(clock, ledger, phases.clone());

		// One ledger sees an interval the other never did.
		phases.record_acquisition(AcquisitionKind::BaseScan, 25);

		assert!(matches!(
			meter.seal_receipt(),
			SealOutcome::Withheld(faults) if faults.reconciliation_mismatch == 1
		));
	}

	fn metered_with(
		meter: &ScanMeter,
		clock: Arc<ManualClock>,
		origin: ScanOrigin,
		batches: usize,
	) -> MeteredScanStream {
		let stream = ScanStream::new(
			batch().schema(),
			crate::ir::SessionId(1),
			crate::ir::QueryId::new(),
			origin,
			charging_stream(clock, batches),
		);
		MeteredScanStream::new(stream, meter)
	}
}
