//! The frozen campaign manifest (`scripts/s7-measure/manifest.json`). Every
//! workload, pacing, observation and validity parameter is read from it; none
//! is defaulted, so an omitted key is a refusal rather than a silent value.

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
	pub format_version: u32,
	/// `CAMPAIGN` for evidence; anything else (e.g. `SMOKE`) is carried into
	/// every record and verdict so it can never be mistaken for evidence.
	pub label: String,
	// Workload.
	pub scale_factor: f64,
	pub target_rows_per_sec: u64,
	pub batch_rows: usize,
	pub max_in_flight: usize,
	pub warmup_secs: u64,
	pub measured_secs: u64,
	pub tenant_id: u64,
	// Node configuration.
	pub node_id: u32,
	pub slab_max_bytes: u64,
	pub sweeper_scan_interval_ms: u64,
	pub sweeper_min_slab_bytes: u64,
	pub cluster_retention_window_secs: u64,
	// Observation.
	pub tick_secs: u64,
	/// A tick captured later than its schedule by more than this is INVALID.
	pub tick_late_tolerance_ms: u64,
	/// Missing tick indices tolerated (0: none).
	pub tick_missing_tolerance: u64,
	/// Bound on each reset/final/diagnostic acknowledgement; a later or
	/// missing acknowledgement invalidates the block (no retry).
	pub ack_timeout_secs: u64,
	pub drain_timeout_secs: u64,
	pub node_ready_timeout_secs: u64,
	/// 0 disables the drained diagnostic capture.
	pub diagnostic_quiesce_secs: u64,
	/// Bound on the whole client setup (connects, tenant, table, retention).
	pub setup_timeout_secs: u64,
	/// Bound on generating and hashing the dataset in the driver.
	pub dataset_allowance_secs: u64,
	/// Bound on the node's orderly shutdown after SIGTERM (run.sh); beyond it
	/// the node is killed and the block is INVALID.
	pub node_shutdown_timeout_secs: u64,
	/// Allowance on the declared phase timing: the warm-up may last at most
	/// this much longer than `warmup_secs`, and the final request may follow
	/// the measured deadline by at most this much.
	pub phase_timing_allowance_ms: u64,
	// Predicates.
	/// Sustained-rate rule: every window of this length inside the measured
	/// interval must acknowledge at least `rate_floor_fraction` of target.
	pub rate_window_secs: u64,
	pub rate_window_step_secs: u64,
	pub rate_floor_fraction: f64,
	pub rss_window_secs: u64,
	/// Minimum ticks in each RSS window; fewer is an incomplete window (INVALID).
	pub rss_window_min_samples: usize,
	pub rss_ratio_limit: f64,
	/// The exact lineage bound: p99 <= this many entries.
	pub lineage_quarter_cap: u64,
}

impl Manifest {
	pub fn load(path: &Path) -> std::io::Result<(Self, Vec<u8>)> {
		let bytes = std::fs::read(path)?;
		let manifest: Self = serde_json::from_slice(&bytes)
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
		manifest.validate()?;
		Ok((manifest, bytes))
	}

	pub fn validate(&self) -> std::io::Result<()> {
		let bad = |what: &str| {
			Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				format!("manifest: {what}"),
			))
		};
		if self.format_version != 1 {
			return bad("format_version must be 1");
		}
		if self.target_rows_per_sec == 0 || self.batch_rows == 0 || self.max_in_flight == 0 {
			return bad("rate, batch and in-flight must be positive");
		}
		if self.tick_secs == 0 || self.rate_window_secs == 0 || self.rate_window_step_secs == 0 {
			return bad("tick and rate windows must be positive");
		}
		if !(self.rate_floor_fraction > 0.0 && self.rate_floor_fraction <= 1.0) {
			return bad("rate_floor_fraction must be in (0, 1]");
		}
		if self.cluster_retention_window_secs == 0 {
			return bad("cluster_retention_window_secs must be positive");
		}
		Ok(())
	}

	/// The fixed admission interval: one batch per `batch_rows / rate` seconds.
	pub fn batch_interval_ns(&self) -> u64 {
		(self.batch_rows as u128 * 1_000_000_000u128 / self.target_rows_per_sec as u128) as u64
	}

	/// The driver's own end-to-end deadline: every bounded step summed, plus
	/// one minute. run.sh enforces it again from outside.
	pub fn driver_deadline_secs(&self) -> u64 {
		self.node_ready_timeout_secs
			+ self.dataset_allowance_secs
			+ self.setup_timeout_secs
			+ self.warmup_secs
			+ self.measured_secs
			+ 2 * self.drain_timeout_secs
			+ 3 * self.ack_timeout_secs
			+ self.diagnostic_quiesce_secs
			+ 60
	}

	/// The most batches a block can admit. Admission runs until the final request is answered, so
	/// the measured phase may overrun by up to its timing allowance and acknowledgement bound.
	pub fn most_batches_admitted(&self) -> u64 {
		let overrun_ms = self.phase_timing_allowance_ms + self.ack_timeout_secs * 1_000;
		self.batches_in(self.warmup_secs)
			+ self.batches_in(self.measured_secs + overrun_ms.div_ceil(1_000))
	}

	/// Batches admitted in a phase of `secs`: slots k with k * interval < secs.
	pub fn batches_in(&self, secs: u64) -> u64 {
		let span = secs as u128 * 1_000_000_000u128;
		let interval = self.batch_interval_ns() as u128;
		span.div_ceil(interval) as u64
	}
}
