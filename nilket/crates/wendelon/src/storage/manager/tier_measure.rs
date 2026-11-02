//! The §7 measurement instrument (ADR 0696 fast follow, plan r3 F).
//! Test-support only: it is never built into production.
//!
//! - **Lineage histogram.** Each live published tier layout is one sample:
//!   its redirects plus superseded entries. Layouts are counted at
//!   construction and uncounted on drop, so a read at block end describes
//!   the live published lineages, unweighted (an untiered slab, with no
//!   layout and no history, is not a sample: leaving it out can only raise
//!   p99). Fixed memory: a zero bin, 63 log-spaced bins (`bin_upper_edge`,
//!   2^18 = 262,144 = cap/4 an exact edge at k = 54, 2^21 at k = 63), and an
//!   overflow bin. p99 is certified by an exact count of lineages above
//!   262,144, never by a bin, so a bin straddling that bound can never pass;
//!   the bins are for reporting.
//! - **Block headroom.** The budget's peak of truly held bytes and its
//!   minimum headroom since the last block reset (`TierMemoryBudget`).
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bins: [0] zero, [1..=63] log-spaced, [64] overflow.
pub(super) const LINEAGE_BINS: usize = 65;

/// The quarter-cap bound the measurement certifies p99 against.
pub(super) const QUARTER_CAP: u64 = 262_144;

/// The upper edge (inclusive) of log-spaced bin `k` (1..=63):
/// `ceil(2^(k/3))`, raised to one above the previous edge where rounding
/// would repeat it (only at small k), so edges strictly increase. Exact powers
/// of two at multiples of 3, including 2^18 at k = 54 and 2^21 at k = 63.
pub(super) fn bin_upper_edge(k: usize) -> u64 {
	let mut edge = 0u64;
	for i in 1..=k {
		let ideal = 2f64.powf(i as f64 / 3.0).ceil() as u64;
		let exact = if i % 3 == 0 { 1u64 << (i / 3) } else { ideal };
		edge = exact.max(edge + 1);
	}
	edge
}

fn bin_of(entries: u64) -> usize {
	if entries == 0 {
		return 0;
	}
	(1..LINEAGE_BINS - 1)
		.find(|k| entries <= bin_upper_edge(*k))
		.unwrap_or(LINEAGE_BINS - 1)
}

/// The live lineages, by history size.
#[derive(Debug)]
pub(super) struct LineageHistogram {
	bins: [AtomicU64; LINEAGE_BINS],
	over_quarter_cap: AtomicU64,
}

impl Default for LineageHistogram {
	fn default() -> Self {
		Self {
			bins: std::array::from_fn(|_| AtomicU64::new(0)),
			over_quarter_cap: AtomicU64::new(0),
		}
	}
}

impl LineageHistogram {
	fn add(&self, entries: u64) {
		self.bins[bin_of(entries)].fetch_add(1, Ordering::AcqRel);
		if entries > QUARTER_CAP {
			self.over_quarter_cap.fetch_add(1, Ordering::AcqRel);
		}
	}

	fn remove(&self, entries: u64) {
		self.bins[bin_of(entries)].fetch_sub(1, Ordering::AcqRel);
		if entries > QUARTER_CAP {
			self.over_quarter_cap.fetch_sub(1, Ordering::AcqRel);
		}
	}

	/// The per-bin counts and the exact count above `QUARTER_CAP`.
	pub(super) fn snapshot(&self) -> ([u64; LINEAGE_BINS], u64) {
		let mut bins = [0; LINEAGE_BINS];
		for (bin, count) in bins.iter_mut().zip(&self.bins) {
			*bin = count.load(Ordering::Acquire);
		}
		(bins, self.over_quarter_cap.load(Ordering::Acquire))
	}

	/// Whether p99 over the live lineages is at most `QUARTER_CAP`: at most
	/// 1% of them are strictly above it, by the exact count. The harness
	/// applies the same rule to the exported count.
	#[cfg(test)]
	pub(super) fn p99_within_quarter_cap(&self) -> bool {
		let (bins, over) = self.snapshot();
		let total: u64 = bins.iter().sum();
		// More than 1% strictly above the bound fails, whatever the bins say.
		over * 100 <= total
	}
}

/// One live layout's sample, uncounted when the layout drops.
#[derive(Debug)]
pub(super) struct LineageSample {
	histogram: Arc<LineageHistogram>,
	entries: u64,
}

impl LineageSample {
	pub(super) fn new(histogram: &Arc<LineageHistogram>, entries: u64) -> Self {
		histogram.add(entries);
		Self {
			histogram: histogram.clone(),
			entries,
		}
	}
}

impl Drop for LineageSample {
	fn drop(&mut self) {
		self.histogram.remove(self.entries);
	}
}

/// The Prometheus text for one manager's instrument (test-support builds
/// only): the lineage histogram as cumulative buckets, the exact count
/// above cap/4, and the block's budget peak and minimum headroom.
pub(super) fn render(
	out: &mut String,
	histogram: &LineageHistogram,
	peak: u64,
	min_headroom: u64,
	limit: u64,
) {
	use std::fmt::Write;
	let (bins, over) = histogram.snapshot();
	let _ = writeln!(out, "# TYPE wendelon_test_tier_lineage_entries histogram");
	let mut cumulative = 0;
	for (k, count) in bins.iter().enumerate() {
		cumulative += count;
		let le = match k {
			0 => "0".to_string(),
			k if k == LINEAGE_BINS - 1 => "+Inf".to_string(),
			k => bin_upper_edge(k).to_string(),
		};
		let _ = writeln!(
			out,
			"wendelon_test_tier_lineage_entries_bucket{{le=\"{le}\"}} {cumulative}"
		);
	}
	let _ = writeln!(out, "wendelon_test_tier_lineage_entries_count {cumulative}");
	let _ = writeln!(
		out,
		"# TYPE wendelon_test_tier_lineages_over_quarter_cap gauge"
	);
	let _ = writeln!(out, "wendelon_test_tier_lineages_over_quarter_cap {over}");
	let _ = writeln!(
		out,
		"# TYPE wendelon_test_tier_budget_block_peak_bytes gauge"
	);
	let _ = writeln!(out, "wendelon_test_tier_budget_block_peak_bytes {peak}");
	let _ = writeln!(
		out,
		"# TYPE wendelon_test_tier_budget_block_min_headroom_bytes gauge"
	);
	let _ = writeln!(
		out,
		"wendelon_test_tier_budget_block_min_headroom_bytes {min_headroom}"
	);
	let _ = writeln!(out, "# TYPE wendelon_test_tier_budget_limit_bytes gauge");
	let _ = writeln!(out, "wendelon_test_tier_budget_limit_bytes {limit}");
}

impl super::SlabManager {
	/// Start a §8 measurement block on this manager: its budget peak and
	/// minimum headroom restart from now (test-support builds only).
	pub fn reset_tier_measurement_block(&self) {
		self.tier_budget.reset_measurement_block();
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bin_edges_are_monotone_and_land_exactly_on_cap_quarter() {
		let edges = (1..LINEAGE_BINS - 1)
			.map(bin_upper_edge)
			.collect::<Vec<_>>();
		assert!(edges.windows(2).all(|pair| pair[0] < pair[1]));
		assert_eq!(
			edges[..3],
			[2, 3, 4],
			"small edges are raised to stay distinct"
		);
		assert_eq!(bin_upper_edge(54), QUARTER_CAP);
		assert_eq!(bin_upper_edge(63), 1 << 21);
		for k in 2..LINEAGE_BINS - 1 {
			let ratio = bin_upper_edge(k) as f64 / bin_upper_edge(k - 1) as f64;
			assert!(ratio <= 2f64.powf(1.0 / 3.0) + 0.5, "k={k}: {ratio}");
		}
	}

	#[test]
	fn samples_land_in_their_bins_and_leave_on_drop() {
		let histogram = Arc::new(LineageHistogram::default());
		let samples = [0, 1, 2, QUARTER_CAP, QUARTER_CAP + 1, 1 << 22]
			.map(|entries| LineageSample::new(&histogram, entries));
		let (bins, over) = histogram.snapshot();
		assert_eq!(bins[0], 1, "zero bin");
		assert_eq!(bins[bin_of(1)], 2, "1 and 2 share the first bin (edge 2)");
		assert_eq!(bins[bin_of(QUARTER_CAP)], 1);
		assert_eq!(
			bin_of(QUARTER_CAP),
			54,
			"the bound is the edge of its own bin"
		);
		assert_eq!(bin_of(QUARTER_CAP + 1), 55, "one above lands above it");
		assert_eq!(bins[LINEAGE_BINS - 1], 1, "overflow");
		assert_eq!(over, 2, "exact count above cap/4");
		assert!(
			!histogram.p99_within_quarter_cap(),
			"2 of 6 above the bound"
		);
		drop(samples);
		let (bins, over) = histogram.snapshot();
		assert_eq!(bins.iter().sum::<u64>(), 0);
		assert_eq!(over, 0);
		assert!(histogram.p99_within_quarter_cap());
	}

	#[test]
	fn the_render_is_cumulative_and_ends_at_the_count() {
		let histogram = Arc::new(LineageHistogram::default());
		let _samples = [0, 3, 3, 1 << 20].map(|entries| LineageSample::new(&histogram, entries));
		let mut out = String::new();
		render(&mut out, &histogram, 10, 20, 30);
		assert!(out.contains("wendelon_test_tier_lineage_entries_bucket{le=\"0\"} 1\n"));
		assert!(out.contains("wendelon_test_tier_lineage_entries_bucket{le=\"+Inf\"} 4\n"));
		assert!(out.contains("wendelon_test_tier_lineage_entries_count 4\n"));
		assert!(out.contains("wendelon_test_tier_lineages_over_quarter_cap 1\n"));
		assert!(out.contains("wendelon_test_tier_budget_block_min_headroom_bytes 20\n"));
	}
}
