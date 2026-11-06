//! `s7-measure drive`: the load and phase driver, a separate process so the
//! measured RSS is the node child's alone. It connects with the real
//! `wendelon_client::Client` over QUIC, creates one AppendOnly MultiRow table
//! shaped like toron-tpch's dev `lineitem` (explicitly inheriting the
//! cluster's 15 s retention), and ingests `generate_dev_lineitem(sf)` in its
//! generated order as fixed-size batches on a fixed schedule:
//!
//! - slot k of a phase is admitted at `anchor + k * batch_rows / rate`;
//!   at most `max_in_flight` batches are outstanding;
//! - a pause (the warm-up drain and reset) never accrues credit: the measured
//!   phase starts a new anchor when it resumes;
//! - an admission later than one interval re-anchors the schedule instead of
//!   bursting to catch up (recorded as a re-anchor and skipped slots);
//! - input is never repeated: running out of data is recorded, and before the
//!   final acknowledgement it invalidates the block.
//!
//! Phases: warm-up (it lasts its declared duration), drain (stop admitting,
//! await every acknowledgement), SIGUSR1 reset with a correlated, bounded
//! acknowledgement, the measured interval from one anchor shared with the
//! final request, SIGUSR2 final capture while the schedule is still
//! admitting, then stop, join, and an optional drained diagnostic.
//!
//! Every wait is bounded. Admission waits for an in-flight permit only until
//! the phase deadline or the stop signal; the write tasks are owned in a
//! `JoinSet`, each admitted batch has a placeholder record until its task
//! returns, and a drain that times out aborts the rest and reports them as
//! incomplete: dropped work is never counted as acknowledged.

use crate::common::{
	ARM, BlockPaths, ControlRequest, NodeReady, hex, mono_ns, pid_start_time, read_json,
	write_json_atomic,
};
use crate::manifest::Manifest;
use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use wendelon::network::control_server::TenantAdminRequestWire;
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PolicyGeneration, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId, TableRetentionPolicy,
	TableSchemaDescriptor, TenantId,
};
use wendelon_client::{
	BootstrapPeer, Client, ClientConfig, SchemaIntentId, TableSchemaUpdateRequest, TlsPaths,
	WriteOptions,
};

/// One admitted batch, as written to `driver-batches.jsonl`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchRecord {
	pub index: u64,
	pub phase: String,
	pub slot: u64,
	pub rows: u64,
	pub t_sched_ns: u64,
	pub t_admit_ns: u64,
	pub t_ack_ns: u64,
	pub ok: bool,
	pub duplicate: bool,
	pub error: Option<String>,
	pub in_flight_at_admit: u64,
	/// False when the write never returned before the drain deadline: the
	/// batch is reported, never counted as acknowledged.
	pub completed: bool,
}

/// The dev `lineitem` descriptor of toron-tpch (`dev_lineitem_descriptor`),
/// rebuilt here over the generated batch's schema.
fn lineitem_descriptor(
	schema: &arrow::datatypes::Schema,
) -> std::io::Result<TableSchemaDescriptor> {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"lineitem",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
}

struct HashWriter(sha2::Sha256);
impl Write for HashWriter {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		sha2::Digest::update(&mut self.0, buf);
		Ok(buf.len())
	}
	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

/// The dataset in admission order: compact (copied, not sliced) batches, and
/// the SHA-256 of their Arrow IPC stream, which is identical across arms iff
/// the schema, rows and order are.
fn build_dataset(m: &Manifest) -> std::io::Result<(Vec<RecordBatch>, Value)> {
	let t = std::time::Instant::now();
	let full = toron_tpch::generate_dev_lineitem(m.scale_factor);
	let rows = full.num_rows();
	let schema = full.schema();
	let mut batches = Vec::with_capacity(rows.div_ceil(m.batch_rows));
	let mut hasher = arrow::ipc::writer::StreamWriter::try_new(
		HashWriter(<sha2::Sha256 as sha2::Digest>::new()),
		&schema,
	)
	.map_err(std::io::Error::other)?;
	let mut offset = 0;
	while offset < rows {
		let len = m.batch_rows.min(rows - offset);
		let indices = UInt32Array::from_iter_values(0..len as u32);
		let batch = arrow::compute::take_record_batch(&full.slice(offset, len), &indices)
			.map_err(std::io::Error::other)?;
		hasher.write(&batch).map_err(std::io::Error::other)?;
		batches.push(batch);
		offset += len;
	}
	hasher.finish().map_err(std::io::Error::other)?;
	let digest = sha2::Digest::finalize(hasher.into_inner().map_err(std::io::Error::other)?.0);
	let schema_text = format!("{schema:?}");
	let info = json!({
		"generator": "toron_tpch::generate_dev_lineitem",
		"scale_factor": m.scale_factor,
		"rows_total": rows,
		"batch_rows": m.batch_rows,
		"batches_total": batches.len(),
		"last_batch_rows": batches.last().map_or(0, RecordBatch::num_rows),
		"ipc_stream_sha256": hex(&digest),
		"schema_sha256": crate::common::sha256_hex(schema_text.as_bytes()),
		"generate_and_hash_ms": t.elapsed().as_millis() as u64,
	});
	Ok((batches, info))
}

fn signal_node(ready: &NodeReady, sig: libc::c_int) -> Result<(), String> {
	match pid_start_time(ready.pid) {
		Ok(start) if start == ready.pid_start_time => {}
		Ok(start) => return Err(format!("node pid {} restarted (start {start})", ready.pid)),
		Err(e) => return Err(format!("node pid {} lost: {e}", ready.pid)),
	}
	// SAFETY: plain kill(2) on a PID whose incarnation was just verified.
	let rc = unsafe { libc::kill(ready.pid as libc::pid_t, sig) };
	if rc != 0 {
		return Err(format!("kill: {}", std::io::Error::last_os_error()));
	}
	Ok(())
}

/// One table write; returns whether the receipt was a duplicate.
pub type WriteFuture = Pin<Box<dyn Future<Output = Result<bool, String>> + Send>>;

/// The write path, a seam so the bounds can be tested with a fake writer.
pub trait Writer: Send + Sync {
	fn write(&self, batch: RecordBatch) -> WriteFuture;
}

struct ClientWriter {
	client: Arc<Client>,
	tenant: TenantId,
	descriptor: Arc<TableSchemaDescriptor>,
}

impl Writer for ClientWriter {
	fn write(&self, batch: RecordBatch) -> WriteFuture {
		let (client, tenant, descriptor) =
			(self.client.clone(), self.tenant, self.descriptor.clone());
		Box::pin(async move {
			client
				.table_write(tenant, &descriptor, batch, WriteOptions::default())
				.await
				.map(|receipt| receipt.duplicate)
				.map_err(|e| e.to_string())
		})
	}
}

/// The node control path (correlated signal requests), a seam for tests.
#[async_trait::async_trait]
pub trait Control: Send + Sync {
	async fn request(&self, seq: u64, kind: &str) -> Result<Value, String>;
}

struct SignalControl {
	block: BlockPaths,
	ready: NodeReady,
	timeout: Duration,
}

#[async_trait::async_trait]
impl Control for SignalControl {
	/// Write the correlated request, signal, and await its acknowledgement
	/// within the timeout. No retry: a missing or erroneous ack ends the block.
	async fn request(&self, seq: u64, kind: &str) -> Result<Value, String> {
		let sig = if kind == "reset" {
			libc::SIGUSR1
		} else {
			libc::SIGUSR2
		};
		let req = ControlRequest {
			seq,
			kind: kind.into(),
			pid: self.ready.pid,
			pid_start_time: self.ready.pid_start_time,
		};
		write_json_atomic(&self.block.control_request(), &req).map_err(|e| e.to_string())?;
		let t_request = mono_ns();
		signal_node(&self.ready, sig)?;
		let deadline = Instant::now() + self.timeout;
		let path = self.block.control_ack(seq);
		loop {
			if let Ok(ack) = read_json::<Value>(&path) {
				let t_seen = mono_ns();
				if ack["kind"] == "control_error" {
					return Err(format!("node refused {kind} seq {seq}: {}", ack["reason"]));
				}
				let ack_kind = if kind == "reset" { "reset_ack" } else { kind };
				if ack["kind"].as_str() != Some(ack_kind) || ack["seq"].as_u64() != Some(seq) {
					return Err(format!("uncorrelated ack for {kind} seq {seq}: {ack}"));
				}
				return Ok(json!({
					"seq": seq,
					"kind": kind,
					"t_request_ns": t_request,
					"t_ack_seen_ns": t_seen,
					"ack": ack,
				}));
			}
			if Instant::now() >= deadline {
				return Err(format!(
					"no ack for {kind} seq {seq} within {:?}",
					self.timeout
				));
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}
}

/// The schedule and bounds of the phase engine.
#[derive(Clone, Debug)]
pub struct Params {
	pub interval: Duration,
	pub max_in_flight: usize,
	pub warmup: Duration,
	pub measured: Duration,
	pub drain_timeout: Duration,
	pub diagnostic_quiesce: Option<Duration>,
}

impl Params {
	pub fn from_manifest(m: &Manifest) -> Self {
		Self {
			interval: Duration::from_nanos(m.batch_interval_ns()),
			max_in_flight: m.max_in_flight,
			warmup: Duration::from_secs(m.warmup_secs),
			measured: Duration::from_secs(m.measured_secs),
			drain_timeout: Duration::from_secs(m.drain_timeout_secs),
			diagnostic_quiesce: (m.diagnostic_quiesce_secs > 0)
				.then(|| Duration::from_secs(m.diagnostic_quiesce_secs)),
		}
	}
}

enum Admit {
	Admitted,
	Exhausted,
	Stopped,
}

/// Resolves once `stop` is true (or its sender is gone).
async fn stopped(stop: &mut watch::Receiver<bool>) {
	let _ = stop.wait_for(|s| *s).await;
}

pub struct Ingest {
	writer: Arc<dyn Writer>,
	batches: Arc<Vec<RecordBatch>>,
	cursor: usize,
	interval: Duration,
	sem: Arc<Semaphore>,
	tasks: JoinSet<BatchRecord>,
	/// Admitted batches whose task has not returned: reported as incomplete
	/// unless their own record replaces them.
	pending: BTreeMap<u64, BatchRecord>,
	done: Vec<BatchRecord>,
	in_flight: Arc<AtomicU64>,
	exhausted_at_ns: Option<u64>,
	reanchors: u64,
	slots_skipped: u64,
	max_admission_lag_ns: u64,
	admitted: BTreeMap<String, u64>,
}

impl Ingest {
	pub fn new(writer: Arc<dyn Writer>, batches: Vec<RecordBatch>, p: &Params) -> Self {
		Self {
			writer,
			batches: Arc::new(batches),
			cursor: 0,
			interval: p.interval,
			sem: Arc::new(Semaphore::new(p.max_in_flight)),
			tasks: JoinSet::new(),
			pending: BTreeMap::new(),
			done: Vec::new(),
			in_flight: Arc::new(AtomicU64::new(0)),
			exhausted_at_ns: None,
			reanchors: 0,
			slots_skipped: 0,
			max_admission_lag_ns: 0,
			admitted: BTreeMap::new(),
		}
	}

	fn complete(&mut self, joined: Result<BatchRecord, tokio::task::JoinError>) {
		// A cancelled or panicked task keeps its placeholder: incomplete.
		if let Ok(rec) = joined {
			self.pending.remove(&rec.index);
			self.done.push(rec);
		}
	}

	fn reap(&mut self) {
		while let Some(joined) = self.tasks.try_join_next() {
			self.complete(joined);
		}
	}

	/// Admit slot `slot` of `phase`, scheduled at `sched`, waiting for an
	/// in-flight permit no later than `deadline` and never past `stop`.
	async fn admit(
		&mut self,
		phase: &str,
		slot: u64,
		sched: Instant,
		sched_ns: u64,
		stop: &mut watch::Receiver<bool>,
		deadline: Option<Instant>,
	) -> Admit {
		self.reap();
		if self.cursor >= self.batches.len() {
			self.exhausted_at_ns.get_or_insert_with(mono_ns);
			return Admit::Exhausted;
		}
		let acquire = self.sem.clone().acquire_owned();
		let permit = tokio::select! {
			biased;
			_ = stopped(stop) => return Admit::Stopped,
			_ = async {
				match deadline {
					Some(d) => tokio::time::sleep_until(d).await,
					None => std::future::pending().await,
				}
			} => return Admit::Stopped,
			permit = acquire => permit.expect("semaphore never closed"),
		};
		let lag = Instant::now().saturating_duration_since(sched);
		self.max_admission_lag_ns = self.max_admission_lag_ns.max(lag.as_nanos() as u64);
		let index = self.cursor as u64;
		let batch = self.batches[self.cursor].clone();
		self.cursor += 1;
		*self.admitted.entry(phase.into()).or_default() += 1;
		let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
		let placeholder = BatchRecord {
			index,
			phase: phase.to_string(),
			slot,
			rows: batch.num_rows() as u64,
			t_sched_ns: sched_ns,
			t_admit_ns: mono_ns(),
			t_ack_ns: 0,
			ok: false,
			duplicate: false,
			error: Some("incomplete: not acknowledged before the drain deadline".into()),
			in_flight_at_admit: in_flight,
			completed: false,
		};
		self.pending.insert(index, placeholder.clone());
		let (writer, gauge) = (self.writer.clone(), self.in_flight.clone());
		self.tasks.spawn(async move {
			let result = writer.write(batch).await;
			let t_ack = mono_ns();
			gauge.fetch_sub(1, Ordering::SeqCst);
			drop(permit);
			BatchRecord {
				t_ack_ns: t_ack,
				ok: result.is_ok(),
				duplicate: result.as_ref().is_ok_and(|d| *d),
				error: result.err(),
				completed: true,
				..placeholder
			}
		});
		Admit::Admitted
	}

	/// Await every outstanding batch until `timeout`; on expiry the rest stay
	/// pending (reported incomplete by `abandon`).
	async fn drain(&mut self, timeout: Duration) -> Result<(), String> {
		let deadline = Instant::now() + timeout;
		while !self.tasks.is_empty() {
			match tokio::time::timeout_at(deadline, self.tasks.join_next()).await {
				Ok(Some(joined)) => self.complete(joined),
				Ok(None) => break,
				Err(_) => {
					return Err(format!(
						"{} batches still outstanding after {timeout:?}",
						self.tasks.len()
					));
				}
			}
		}
		Ok(())
	}

	/// Abort whatever is still outstanding and report it as incomplete.
	/// Returns how many batches never completed.
	fn abandon(&mut self) -> usize {
		self.reap();
		self.tasks.abort_all();
		let incomplete = std::mem::take(&mut self.pending);
		let n = incomplete.len();
		self.done.extend(incomplete.into_values());
		self.done.sort_by_key(|r| r.index);
		n
	}

	/// Run the fixed schedule of `phase` from `anchor` until `stop`, or, with
	/// an `end`, until `end` has passed (a phase with an end always lasts it).
	async fn pace(
		&mut self,
		phase: &str,
		anchor: Instant,
		anchor_mono: u64,
		end: Option<Instant>,
		stop: &mut watch::Receiver<bool>,
	) -> u64 {
		let start = anchor;
		let mut anchor = anchor;
		let mut slot = 0u64;
		loop {
			if *stop.borrow() {
				break;
			}
			let sched = anchor + self.interval * slot as u32;
			if let Some(end) = end
				&& sched >= end
			{
				break;
			}
			tokio::select! {
				biased;
				_ = stopped(stop) => break,
				_ = tokio::time::sleep_until(sched) => {}
			}
			let sched_ns = anchor_mono + sched.saturating_duration_since(start).as_nanos() as u64;
			match self.admit(phase, slot, sched, sched_ns, stop, end).await {
				Admit::Stopped => break,
				Admit::Exhausted => {
					// Keep the schedule observable but admit nothing more.
					slot += 1;
					continue;
				}
				Admit::Admitted => {}
			}
			let lag = Instant::now().saturating_duration_since(sched);
			if lag > self.interval {
				let skipped = (lag.as_nanos() / self.interval.as_nanos()) as u64;
				self.reanchors += 1;
				self.slots_skipped += skipped;
				slot += skipped;
				anchor = Instant::now() - self.interval * slot as u32;
			}
			slot += 1;
		}
		if let Some(end) = end {
			tokio::select! {
				biased;
				_ = stopped(stop) => {}
				_ = tokio::time::sleep_until(end) => {}
			}
		}
		mono_ns()
	}
}

/// The phases of one block. Every step is bounded; on any error the caller
/// abandons what is still outstanding.
pub async fn run_phases(
	ingest: &mut Ingest,
	control: Arc<dyn Control>,
	p: &Params,
	phases: &mut Value,
) -> Result<(), String> {
	// Warm-up: its declared duration, then bound outstanding work.
	let (_keep, mut never) = watch::channel(false);
	// Stamp before anchoring, so a declared span measured from the stamp is
	// never longer than the span actually waited.
	let w_mono = mono_ns();
	let w_anchor = Instant::now();
	let w_end = ingest
		.pace(
			"warmup",
			w_anchor,
			w_mono,
			Some(w_anchor + p.warmup),
			&mut never,
		)
		.await;
	let drain = ingest.drain(p.drain_timeout).await;
	phases["warmup"] = json!({
		"t_start_ns": w_mono,
		"t_admission_stopped_ns": w_end,
		"t_drained_ns": mono_ns(),
		"declared_ns": p.warmup.as_nanos() as u64,
		"drain_error": drain.clone().err(),
	});
	drain.map_err(|e| format!("warm-up drain: {e}"))?;

	let reset = control.request(1, "reset").await;
	phases["reset"] = reset.clone().unwrap_or_else(|e| json!({"error": e}));
	reset?;

	// Measured: one anchor shared by the schedule and the final request.
	let (stop_tx, mut stop) = watch::channel(false);
	let m_mono = mono_ns();
	let m_anchor = Instant::now();
	let m_end = m_anchor + p.measured;
	phases["measured"] = json!({
		"t_start_ns": m_mono,
		"t_deadline_ns": m_mono + p.measured.as_nanos() as u64,
	});
	let final_task = {
		let (control, in_flight) = (control.clone(), ingest.in_flight.clone());
		tokio::spawn(async move {
			tokio::time::sleep_until(m_end).await;
			let at_request = in_flight.load(Ordering::SeqCst);
			let result = control.request(2, "final").await;
			let at_ack = in_flight.load(Ordering::SeqCst);
			let _ = stop_tx.send(true);
			result.map(|mut v| {
				v["in_flight_at_request"] = json!(at_request);
				v["in_flight_at_ack"] = json!(at_ack);
				v
			})
		})
	};
	let ms_end = ingest
		.pace("measured", m_anchor, m_mono, None, &mut stop)
		.await;
	let final_result = final_task
		.await
		.map_err(|e| format!("final task: {e}"))
		.and_then(|r| r);
	phases["measured"]["t_admission_stopped_ns"] = json!(ms_end);
	phases["final"] = final_result.clone().unwrap_or_else(|e| json!({"error": e}));
	let join = ingest.drain(p.drain_timeout).await;
	phases["join"] =
		json!({"t_joined_ns": mono_ns(), "ok": join.is_ok(), "error": join.clone().err()});
	final_result?;
	join.map_err(|e| format!("join: {e}"))?;

	if let Some(quiesce) = p.diagnostic_quiesce {
		tokio::time::sleep(quiesce).await;
		let d = control.request(3, "diagnostic").await;
		phases["diagnostic"] = d.clone().unwrap_or_else(|e| json!({"error": e}));
		d?;
	}
	Ok(())
}

pub async fn run(manifest_path: &Path, block_dir: &Path, node_pid: u32) -> std::io::Result<()> {
	let (m, manifest_bytes) = Manifest::load(manifest_path)?;
	let _telemetry = wendelon::metrics::telemetry::install_global(None, 0)?;
	let block = BlockPaths::new(block_dir);
	let block_id = block.block_id()?;
	let mut record = json!({
		"arm": ARM,
		"label": m.label,
		"block_id": block_id,
		"manifest_sha256": crate::common::sha256_hex(&manifest_bytes),
		"driver_deadline_secs": m.driver_deadline_secs(),
		"aborted": null,
	});
	// Backstop: every step is bounded on its own; this bounds their sum.
	let deadline = Duration::from_secs(m.driver_deadline_secs());
	let result =
		match tokio::time::timeout(deadline, drive(&m, &block, node_pid, &mut record)).await {
			Ok(result) => result,
			Err(_) => Err(format!("driver deadline {deadline:?} expired")),
		};
	if let Err(reason) = &result {
		record["aborted"] = json!(reason);
	}
	write_json_atomic(&block.driver_record(), &record)?;
	result.map_err(std::io::Error::other)
}

async fn drive(
	m: &Manifest,
	block: &BlockPaths,
	node_pid: u32,
	record: &mut Value,
) -> Result<(), String> {
	// The node's published identity, verified against the PID we were given.
	let deadline = Instant::now() + Duration::from_secs(m.node_ready_timeout_secs);
	let ready: NodeReady = loop {
		if let Ok(ready) = read_json::<NodeReady>(&block.node_ready()) {
			break ready;
		}
		if Instant::now() >= deadline {
			return Err("node never published node-ready.json".into());
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	};
	if ready.pid != node_pid {
		return Err(format!(
			"node-ready pid {} != --node-pid {node_pid}",
			ready.pid
		));
	}
	if ready.arm != ARM {
		return Err(format!("node arm {} != driver arm {ARM}", ready.arm));
	}
	record["node"] = json!(ready);

	let dataset_bound = Duration::from_secs(m.dataset_allowance_secs);
	let m2 = m.clone();
	let (batches, dataset) = tokio::time::timeout(
		dataset_bound,
		tokio::task::spawn_blocking(move || build_dataset(&m2)),
	)
	.await
	.map_err(|_| format!("dataset not built within {dataset_bound:?}"))?
	.map_err(|e| e.to_string())?
	.map_err(|e| e.to_string())?;
	let interval_ns = m.batch_interval_ns();
	let warm = m.batches_in(m.warmup_secs);
	let measured = m.batches_in(m.measured_secs);
	let needed = m.most_batches_admitted();
	// Only full batches count: a short last batch is not one more slot of the declared rate.
	let full_batches = batches
		.iter()
		.filter(|batch| batch.num_rows() as u64 == m.batch_rows as u64)
		.count();
	record["dataset"] = dataset;
	record["schedule"] = json!({
		"batch_interval_ns": interval_ns,
		"warmup_batches_planned": warm,
		"measured_batches_planned": measured,
		"batches_needed": needed,
		"batches_available": full_batches,
		"rows_needed_at_full_batches": needed * m.batch_rows as u64,
		"fits": needed as usize <= full_batches,
		"policy": "fixed interval; fresh anchor after each pause; re-anchor (no burst) after a late admission; never repeat input",
	});
	if needed as usize > full_batches {
		return Err(format!(
			"dataset has {full_batches} full batches; the schedule needs {needed}"
		));
	}

	let setup_bound = Duration::from_secs(m.setup_timeout_secs);
	let schema = batches[0].schema();
	let (client, tenant, descriptor) =
		tokio::time::timeout(setup_bound, setup(m, &ready, schema.as_ref(), record))
			.await
			.map_err(|_| format!("client setup not done within {setup_bound:?}"))??;

	let params = Params::from_manifest(m);
	let writer: Arc<dyn Writer> = Arc::new(ClientWriter {
		client,
		tenant,
		descriptor,
	});
	let control: Arc<dyn Control> = Arc::new(SignalControl {
		block: block.clone(),
		ready: ready.clone(),
		timeout: Duration::from_secs(m.ack_timeout_secs),
	});
	let mut ingest = Ingest::new(writer, batches, &params);
	let mut phases = json!({});
	let mut outcome = run_phases(&mut ingest, control, &params, &mut phases).await;
	let incomplete = ingest.abandon();
	if incomplete > 0 && outcome.is_ok() {
		outcome = Err(format!("{incomplete} batches never completed"));
	}
	write_batches(block, &ingest, record, phases, incomplete)?;
	outcome
}

async fn setup(
	m: &Manifest,
	ready: &NodeReady,
	schema: &arrow::datatypes::Schema,
	record: &mut Value,
) -> Result<(Arc<Client>, TenantId, Arc<TableSchemaDescriptor>), String> {
	// Two real SDK clients over QUIC: the operator administers the tenant,
	// table and retention; the data_node writer carries the table writes.
	let connect = |cert: std::path::PathBuf, key: std::path::PathBuf| {
		Client::connect_quic(ClientConfig::new(
			"127.0.0.1:0".parse().unwrap(),
			vec![BootstrapPeer {
				node_id: ready.node_id,
				address: ready.address.clone(),
			}],
			TlsPaths {
				cert_path: cert,
				key_path: key,
				ca_path: ready.ca_cert.clone(),
			},
		))
	};
	let admin = connect(ready.admin_cert.clone(), ready.admin_key.clone())
		.await
		.map_err(|e| format!("connect (admin): {e}"))?;
	let client = Arc::new(
		connect(ready.writer_cert.clone(), ready.writer_key.clone())
			.await
			.map_err(|e| format!("connect (writer): {e}"))?,
	);
	let tenant = TenantId::new(m.tenant_id);
	let descriptor = Arc::new(lineitem_descriptor(schema).map_err(|e| e.to_string())?);
	let created = admin
		.inner()
		.tenant_admin(&TenantAdminRequestWire::Create {
			target_tenant_id: tenant,
			quota_bytes: u64::MAX,
			replication_factor: 1,
		})
		.await
		.map_err(|e| format!("tenant create: {e}"))?;
	let table = admin
		.submit_table_schema_update(TableSchemaUpdateRequest::create(
			tenant,
			(*descriptor).clone(),
			SchemaIntentId::new([0x57; 16]),
		))
		.await
		.map_err(|e| format!("table create: {e}"))?;
	let retention = admin
		.inner()
		.tenant_admin(&TenantAdminRequestWire::SetTableRetentionPolicy {
			target_tenant_id: tenant,
			table_object_id: descriptor.table_object_id,
			expected_policy_generation: PolicyGeneration::ZERO,
			new_policy: TableRetentionPolicy::InheritCluster,
		})
		.await
		.map_err(|e| format!("table retention InheritCluster: {e}"))?;
	// Let the data plane's tenant cache (100 ms refresh) see the new tenant
	// before the first write; identical in both arms.
	let settle = Duration::from_secs(1);
	tokio::time::sleep(settle).await;
	record["setup"] = json!({
		"settle_ms": settle.as_millis() as u64,
		"tenant_create": format!("{created:?}"),
		"table_create": format!("{table:?}"),
		"table_retention": format!("{retention:?}"),
		"table_retention_policy": "InheritCluster",
		"descriptor": format!("{descriptor:?}"),
	});

	Ok((client, tenant, descriptor))
}

fn write_batches(
	block: &BlockPaths,
	ingest: &Ingest,
	record: &mut Value,
	phases: Value,
	incomplete: usize,
) -> Result<(), String> {
	let mut lines = String::new();
	let mut errors: BTreeMap<String, u64> = BTreeMap::new();
	let (mut acked_rows, mut acked_batches) = (0u64, 0u64);
	for rec in &ingest.done {
		lines.push_str(&serde_json::to_string(rec).map_err(|e| e.to_string())?);
		lines.push('\n');
		if rec.ok && rec.completed {
			acked_rows += rec.rows;
			acked_batches += 1;
		} else {
			let kind = rec.error.clone().unwrap_or_default();
			*errors.entry(kind).or_default() += 1;
		}
	}
	crate::common::write_atomic(&block.driver_batches(), lines.as_bytes())
		.map_err(|e| e.to_string())?;
	record["phases"] = phases;
	record["totals"] = json!({
		"admitted": ingest.admitted,
		"batch_records": ingest.done.len(),
		"incomplete_batches": incomplete,
		"acked_batches": acked_batches,
		"acked_rows": acked_rows,
		"errors": errors.values().sum::<u64>(),
		"error_kinds": errors,
		"duplicates": ingest.done.iter().filter(|r| r.duplicate).count(),
		"max_in_flight_observed": ingest.done.iter().map(|r| r.in_flight_at_admit).max(),
		"max_admission_lag_ns": ingest.max_admission_lag_ns,
		"reanchors": ingest.reanchors,
		"slots_skipped": ingest.slots_skipped,
		"dataset_exhausted_at_ns": ingest.exhausted_at_ns,
	});
	Ok(())
}

#[cfg(test)]
mod tests;
