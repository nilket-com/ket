//! The §7 measurement instrument (ADR 0696 fast follow, plan r3 F).
//! Test-support only: it is never built into production.
//!
//! - **Lineage census.** One sample per tiered slab whose layout is
//!   published: its redirects plus superseded entries, maintained at exactly
//!   the transitions that change the published generation, so prepared,
//!   failed, cancelled and superseded generations are never samples. A
//!   tiered slab still at generation 0 has no history and no layout; it is
//!   not a sample (omitting zeros can only raise p99, never dilute a fail).
//!   Fixed memory: a zero bin, 63 log-spaced bins (`bin_upper_edge`;
//!   2^18 = 262,144 = cap/4 is the exact edge of bin 54, 2^21 of bin 63), and
//!   an overflow bin, all under one mutex. p99 passes iff at most 1% of the
//!   population is strictly above 262,144, counted from the bins above bin
//!   54, so the bound is exact.
//! - **Block headroom.** The budget's peak of truly held bytes and its
//!   minimum headroom since the last block reset (`TierMemoryBudget`).

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

/// The current lineages, by history size: one sample per tiered slab with a
/// published layout, maintained only where the published generation of a
/// slab changes (`ManagerState::install_tier_layout` and
/// `remove_tier_layout`). One mutex covers every bin, and a replacement
/// (the old size out, the new size in) is one critical section, so a
/// snapshot is always one coherent population. The count above cap/4 is
/// derived from the bins, never kept apart from them.
#[derive(Debug)]
pub(super) struct LineageCensus {
	bins: std::sync::Mutex<[u64; LINEAGE_BINS]>,
	/// Test seam: hold the next replacement inside its critical section,
	/// between taking out the old size and putting in the new one.
	#[cfg(test)]
	hold: std::sync::Mutex<
		Option<(
			std::sync::mpsc::SyncSender<()>,
			std::sync::mpsc::Receiver<()>,
		)>,
	>,
}

impl Default for LineageCensus {
	fn default() -> Self {
		Self {
			bins: std::sync::Mutex::new([0; LINEAGE_BINS]),
			#[cfg(test)]
			hold: std::sync::Mutex::default(),
		}
	}
}

impl LineageCensus {
	fn lock(&self) -> std::sync::MutexGuard<'_, [u64; LINEAGE_BINS]> {
		self.bins
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// A slab's published lineage changed from `old` to `new` entries
	/// (`None`: no published layout).
	pub(super) fn replace(&self, old: Option<u64>, new: Option<u64>) {
		#[cfg(test)]
		let hold = self.hold.lock().unwrap().take();
		let mut bins = self.lock();
		if let Some(old) = old {
			bins[bin_of(old)] -= 1;
		}
		#[cfg(test)]
		if let Some((entered, release)) = hold
			&& entered.send(()).is_ok()
		{
			let _ = release.recv_timeout(std::time::Duration::from_secs(30));
		}
		if let Some(new) = new {
			bins[bin_of(new)] += 1;
		}
	}

	/// One coherent population.
	pub(super) fn snapshot(&self) -> [u64; LINEAGE_BINS] {
		*self.lock()
	}

	#[cfg(test)]
	pub(super) fn hold_next_replacement_for_test(
		&self,
		entered: std::sync::mpsc::SyncSender<()>,
		release: std::sync::mpsc::Receiver<()>,
	) {
		*self.hold.lock().unwrap() = Some((entered, release));
	}
}

/// The lineages strictly above `QUARTER_CAP`: every bin above the one whose
/// upper edge is `QUARTER_CAP`.
pub(super) fn over_quarter_cap(bins: &[u64; LINEAGE_BINS]) -> u64 {
	bins[bin_of(QUARTER_CAP) + 1..].iter().sum()
}

/// Whether p99 over a population is at most `QUARTER_CAP`: at most 1% of
/// it is strictly above. The harness applies the same rule to the export.
#[cfg(test)]
pub(super) fn p99_within_quarter_cap(bins: &[u64; LINEAGE_BINS]) -> bool {
	let total: u64 = bins.iter().sum();
	over_quarter_cap(bins) * 100 <= total
}

/// The Prometheus text for one manager's instrument (test-support builds
/// only): the lineage histogram as cumulative buckets, the exact count
/// above cap/4, and the block's budget peak and minimum headroom.
pub(super) fn render(
	out: &mut String,
	bins: &[u64; LINEAGE_BINS],
	peak: u64,
	min_headroom: u64,
	limit: u64,
) {
	use std::fmt::Write;
	let over = over_quarter_cap(bins);
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
	fn replacements_move_one_sample_and_the_over_count_is_exact() {
		let census = LineageCensus::default();
		for entries in [0, 1, 2, QUARTER_CAP, QUARTER_CAP + 1, 1 << 22] {
			census.replace(None, Some(entries));
		}
		let bins = census.snapshot();
		assert_eq!(bins[0], 1, "zero bin");
		assert_eq!(bins[bin_of(1)], 2, "1 and 2 share the first bin (edge 2)");
		assert_eq!(
			bin_of(QUARTER_CAP),
			54,
			"the bound is the edge of its own bin"
		);
		assert_eq!(bin_of(QUARTER_CAP + 1), 55, "one above lands above it");
		assert_eq!(bins[LINEAGE_BINS - 1], 1, "overflow");
		assert_eq!(over_quarter_cap(&bins), 2, "exactly the two above cap/4");
		assert!(!p99_within_quarter_cap(&bins), "2 of 6 above the bound");
		census.replace(Some(QUARTER_CAP + 1), Some(3));
		census.replace(Some(1 << 22), None);
		let bins = census.snapshot();
		assert_eq!(bins.iter().sum::<u64>(), 5);
		assert_eq!(over_quarter_cap(&bins), 0);
		assert!(p99_within_quarter_cap(&bins));
	}

	/// A scrape never sees a replacement half done. 101 lineages, 2 above
	/// the bound, fail; one of the over-bound lineages grows, still over,
	/// so the population after also fails. Half done (old size out, new
	/// size not yet in), it would read 100 lineages with 1 over, which
	/// passes. The scrape, made during the held replacement, waits for it
	/// and sees the complete population.
	#[test]
	fn a_scrape_never_sees_a_replacement_half_done() {
		use std::sync::mpsc::{channel, sync_channel};
		use std::time::Duration;
		let census = std::sync::Arc::new(LineageCensus::default());
		for _ in 0..99 {
			census.replace(None, Some(10));
		}
		census.replace(None, Some(QUARTER_CAP + 1));
		census.replace(None, Some(QUARTER_CAP + 2));
		assert!(
			!p99_within_quarter_cap(&census.snapshot()),
			"premise: before fails"
		);
		let (entered_tx, entered) = sync_channel(1);
		let (release, release_rx) = sync_channel(1);
		census.hold_next_replacement_for_test(entered_tx, release_rx);
		let replacing = {
			let census = census.clone();
			std::thread::spawn(move || {
				census.replace(Some(QUARTER_CAP + 2), Some(QUARTER_CAP + 50));
			})
		};
		entered.recv_timeout(Duration::from_secs(30)).unwrap();
		let (scraped_tx, scraped) = channel();
		let scraper = {
			let census = census.clone();
			std::thread::spawn(move || {
				let _ = scraped_tx.send(census.snapshot());
			})
		};
		let during = scraped.recv_timeout(Duration::from_millis(300));
		release.send(()).unwrap();
		replacing.join().unwrap();
		scraper.join().unwrap();
		let seen = during
			.or_else(|_| scraped.recv_timeout(Duration::from_secs(30)))
			.unwrap();
		assert_eq!(seen.iter().sum::<u64>(), 101, "never the half-done 100");
		assert_eq!(over_quarter_cap(&seen), 2);
		assert!(
			!p99_within_quarter_cap(&seen),
			"the scrape never certifies a pass"
		);
	}

	#[test]
	fn the_render_is_cumulative_and_ends_at_the_count() {
		let census = LineageCensus::default();
		for entries in [0, 3, 3, 1 << 20] {
			census.replace(None, Some(entries));
		}
		let mut out = String::new();
		render(&mut out, &census.snapshot(), 10, 20, 30);
		assert!(out.contains("wendelon_test_tier_lineage_entries_bucket{le=\"0\"} 1\n"));
		assert!(out.contains("wendelon_test_tier_lineage_entries_bucket{le=\"+Inf\"} 4\n"));
		assert!(out.contains("wendelon_test_tier_lineage_entries_count 4\n"));
		assert!(out.contains("wendelon_test_tier_lineages_over_quarter_cap 1\n"));
		assert!(out.contains("wendelon_test_tier_budget_block_min_headroom_bytes 20\n"));
	}
}
