//! ADR 551: base-relation attribution must be structural, not enumerated.
//!
//! ADR 550 withdrew ADR 538's component fork because `scan` was written at one
//! bare-node site while thirty other read routes went unattributed. The fix is
//! not a longer list of instrumented call sites — a list falls behind the engine
//! the first time someone adds a route. It is that there be exactly one door out
//! of a `ScanStream`, with the meter behind it.
//!
//! These guards prove the door is single and that nothing walks around it.

use super::support::manifest_dir;

fn toron_source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative))
		.unwrap_or_else(|error| panic!("{relative} is readable: {error}"))
}

fn toron_sources() -> Vec<(String, String)> {
	fn walk(directory: &std::path::Path, found: &mut Vec<(String, String)>) {
		let entries = std::fs::read_dir(directory).expect("source directory is readable");
		for entry in entries {
			let path = entry.expect("directory entry is readable").path();
			if path.is_dir() {
				walk(&path, found);
			} else if path.extension().is_some_and(|extension| extension == "rs") {
				let contents = std::fs::read_to_string(&path).expect("source file is readable");
				found.push((path.display().to_string(), contents));
			}
		}
	}

	let mut found = Vec::new();
	walk(&manifest_dir().join("src"), &mut found);
	found
}

/// The raw extractor exists exactly once, and only the metered stream calls it.
///
/// `into_batches` previously handed a bare `BoxStream` to thirty-three call
/// sites, which is how provenance was erased and how thirty read routes escaped
/// attribution. Its replacement is `into_metered_parts`, and if a second caller
/// appears the single-door property is gone.
#[test]
fn the_metered_transition_is_the_only_raw_stream_extractor() {
	let storage = toron_source("src/storage.rs");
	assert!(
		storage.contains("fn into_metered_parts("),
		"the single consuming transition still exists"
	);
	assert!(
		!storage.contains("fn into_batches("),
		"into_batches is removed, not merely unused: it was the bypass"
	);

	let callers = toron_sources()
		.into_iter()
		.filter(|(path, contents)| {
			contents.contains(".into_metered_parts()") && !path.ends_with("src/storage.rs")
		})
		.map(|(path, _)| path)
		.collect::<Vec<_>>();

	assert_eq!(
		callers.len(),
		1,
		"exactly one module consumes the transition, found: {callers:?}"
	);
	assert!(
		callers[0].ends_with("src/scan_metering.rs"),
		"the sole consumer is the metered stream, found: {callers:?}"
	);
}

/// No execution path opens storage directly; every read goes through the meter.
///
/// `Executor::open_scan` is the one place that calls `StorageScan::scan` on the
/// execution path, so the opening interval is attributed once, by origin, rather
/// than at each of the routes that acquire rows.
#[test]
fn execution_paths_acquire_rows_only_through_the_metered_open() {
	let direct = toron_sources()
		.into_iter()
		.filter(|(path, _)| {
			path.contains("/src/exec.rs")
				|| path.contains("/src/exec/")
				|| path.contains("/src/server/")
		})
		.filter(|(_, contents)| {
			contents.contains(".storage.scan(") || contents.contains("storage_arc().scan(")
		})
		.map(|(path, _)| path)
		.collect::<Vec<_>>();

	let allowed_to_open = direct.iter().all(|path| {
		// `exec/scan_open.rs` holds the metered open; `server/mod.rs` holds the
		// storage decorators, which delegate rather than acquire.
		path.ends_with("src/exec/scan_open.rs") || path.ends_with("src/server/mod.rs")
	});

	assert!(
		allowed_to_open,
		"only the executor's metered open and the storage decorators may call scan directly: {direct:?}"
	);

	// The fused paths must hold no direct opens at all: those are the routes that
	// were invisible to `scan_micros`.
	let fused = toron_source("src/exec/join/streaming.rs");
	assert!(
		!fused.contains(".storage.scan("),
		"fused join paths acquire rows through Executor::open_scan"
	);
	let aggregates = toron_source("src/exec/aggregate.rs");
	assert!(
		!aggregates.contains(".storage.scan("),
		"fused aggregate paths acquire rows through Executor::open_scan"
	);
}

/// A task that collects no receipt meters nothing.
///
/// Attribution exists to produce evidence; a query that emits none should not pay
/// a clock read and an atomic update per poll.
#[test]
fn the_meter_has_a_disabled_pass_through() {
	let metering = toron_source("src/scan_metering.rs");
	assert!(
		metering.contains("pub fn disabled()"),
		"a disabled meter exists"
	);
	assert!(
		metering.contains("fn unmetered("),
		"the disabled path still goes through the single transition"
	);
}

/// Publication goes through reconciliation, structurally.
///
/// `TaskPhaseLedger::seal` publishes phase totals without checking them against
/// the acquisition ledger. ADR 551 makes `ScanMeter::seal_receipt` the only
/// production path, because it reconciles first; a receipt published around that
/// check is a receipt whose two ledgers were never compared.
#[test]
fn the_reconciling_seal_is_the_only_production_publication_path() {
	let phase_ledger = toron_source("src/phase_ledger.rs");
	assert!(
		phase_ledger.contains("pub(crate) fn seal(&self)"),
		"the unreconciled seal is crate-visible, not public API"
	);

	let metering = toron_source("src/scan_metering.rs");
	assert!(
		metering.contains("pub fn seal_receipt(&self)"),
		"the reconciling seal is the published one"
	);

	// Only the meter may call the unreconciled seal. Test modules inside the two
	// metering files are exempt; production code elsewhere is not.
	let callers = toron_sources()
		.into_iter()
		.filter(|(path, contents)| {
			contents.contains(".seal()")
				&& !path.ends_with("src/scan_metering.rs")
				&& !path.ends_with("src/phase_ledger.rs")
		})
		.map(|(path, _)| path)
		.collect::<Vec<_>>();

	assert!(
		callers.is_empty(),
		"only ScanMeter::seal_receipt may seal the phase ledger, found: {callers:?}"
	);

	let production_publishers = toron_sources()
		.into_iter()
		.filter(|(path, contents)| {
			contents.contains(".seal_receipt()") && !path.ends_with("src/scan_metering.rs")
		})
		.map(|(path, _)| path)
		.collect::<Vec<_>>();
	assert_eq!(
		production_publishers.len(),
		1,
		"one executor-root publisher seals each task exactly once: {production_publishers:?}"
	);
	assert!(production_publishers[0].ends_with("src/exec/phase_receipt.rs"));
}

/// The metered ledger is the sole authority for scan/build/probe/spill.
///
/// The 63 stable site markers remain beside their frozen measurement boundaries,
/// but the old duration assignments must not survive the flip: leaving them in
/// place would make correctness depend on the final overwrite winning.
#[test]
fn legacy_phase_writers_are_removed_after_the_authority_flip() {
	let mut assignments = Vec::new();
	for (path, contents) in toron_sources() {
		for (line_number, line) in contents.lines().enumerate() {
			if ["scan", "build", "probe", "spill"]
				.iter()
				.any(|phase| line.contains(&format!("task.phases.{phase}_micros =")))
			{
				assignments.push((path.clone(), line_number + 1, line.trim().to_string()));
			}
		}
	}

	assert_eq!(
		assignments.len(),
		4,
		"only the executor-root ledger publisher assigns the four phases: {assignments:?}"
	);
	assert!(
		assignments
			.iter()
			.all(|(path, _, _)| path.ends_with("src/exec/phase_receipt.rs")),
		"legacy node/join writers survived: {assignments:?}"
	);
}

/// Every wrapped envelope carries a source marker matching its manifest entry.
///
/// ADR 551: counting `finish_at_measurement` calls is not a measure of coverage.
/// Pilot 1 routes nine exits through one textual `discard_before_measurement`,
/// and two envelopes in this file have byte-identical source. Only stable IDs
/// distinguish them, and only set equality — not matching totals — catches a
/// duplicated marker alongside a missing one.
#[test]
fn wrapped_envelopes_carry_markers_matching_the_manifest() {
	let receipt = std::fs::read_to_string(
		manifest_dir()
			.join("plans/impl/0551_fusion_invariant_base_relation_attribution_evidence.md"),
	)
	.expect("ADR 551 evidence receipt is readable");

	// Exact grammar, so an incidental prose mention cannot stand in for a missing
	// authoritative entry. A line carrying the token but not matching its form is
	// a failure, never something to skip past.
	let mut manifest_verified = Vec::new();
	for line in receipt.lines() {
		if !line.contains("ADR0551-ID:") {
			continue;
		}
		let id = line
			.trim()
			.strip_prefix("- ADR0551-ID: `")
			.and_then(|rest| rest.strip_suffix('`'))
			.unwrap_or_else(|| {
				panic!("malformed manifest ID line, expected \"- ADR0551-ID: `<id>`\": {line}")
			});
		manifest_verified.push(id.to_string());
	}

	let mut source_markers = Vec::new();
	for (path, contents) in toron_sources() {
		for line in contents.lines() {
			if !line.contains("ADR0551-SITE:") {
				continue;
			}
			let id = line
				.trim()
				.strip_prefix("// ADR0551-SITE: ")
				.unwrap_or_else(|| {
					panic!(
						"malformed site marker in {path}, expected \"// ADR0551-SITE: <id>\": {line}"
					)
				});
			source_markers.push(id.to_string());
		}
	}

	manifest_verified.sort();
	source_markers.sort();

	for (label, ids) in [
		("source marker", &source_markers),
		("manifest ID", &manifest_verified),
	] {
		let mut deduped = ids.clone();
		deduped.dedup();
		assert_eq!(deduped.len(), ids.len(), "duplicate {label}s: {ids:?}");
	}

	assert_eq!(
		source_markers, manifest_verified,
		"source markers must equal the manifest's verified set, exactly"
	);
}
