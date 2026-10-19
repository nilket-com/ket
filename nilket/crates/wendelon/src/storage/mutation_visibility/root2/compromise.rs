//! Root2 compromise audit (ADR 0101 C-S6): a deterministic, offline, read-only fold over a node's
//! retained Control log, reporting every Root2 entry of one root with what suspected signer
//! identities did in it.
//!
//! Dormant like the rest of Root2: production apply still answers `Root2Dormant`, and the audit
//! reads raw carriers from the log, never a production state cache, the volatile log cache or a
//! mutation journal. Order: trusted manifest, read-only scan, coverage, binding, the shared fold
//! (the production reducer and verifiers through the dormant replay dispatcher, from a provisioned
//! baseline, with no recovery mark), suspect resolution, output. Containment is forward-only, so
//! the audit is how pre-detection entries are found; it undoes nothing.
//!
//! A complete fold can also yield the context export (C-S6b): the bound context, the manifest and
//! every successor's committed rotation, for the offline custody tool to re-verify.
mod attribution;
mod binding;
mod export;
mod report;
mod trace;

pub(in crate::storage::mutation_visibility::root2) use trace::Trace2;

use crate::consensus::raft_storage::scan_control_log_read_only;
use crate::storage::mutation_visibility::root2::replay::ReplayError2;
use crate::storage::mutation_visibility::root2::types::Log2;
use report::{Coverage2, Identity2, raft_log, render_complete, render_partial};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::Ordering;

/// Typed outcomes; the exit code is the status, never the text.
const COMPLETE: u8 = 0;
const INVALID: u8 = 1;
const INPUT: u8 = 2;
const PARTIAL: u8 = 3;
const UNRESOLVED: u8 = 4;

/// One audit run over `data_dir` for the root `manifest` installs. `suspects` are `(role, epoch)`
/// with role codes 1 admin, 2 governance, 3 recovery; `key`, when given instead, is resolved to
/// exactly one identity in the replayed history. `export`, the operator-asserted node id, asks for
/// the context export; only then may the suspect set be empty. Returns stdout, the export bytes
/// (only with exit 0), a stderr diagnostic and the exit code: 0 complete, 1 invalid history or
/// binding, 2 I/O or trust input, 3 partial coverage, 4 unresolved or ambiguous suspect. No
/// `complete` record and no export is ever produced with 1, 2, 3 or 4.
pub(crate) async fn compromise_audit2(
	data_dir: &Path,
	manifest: &[u8],
	suspects: &[(u8, u64)],
	key: Option<[u8; 32]>,
	suspect_only: bool,
	export: Option<u32>,
) -> (String, Option<Vec<u8>>, Option<String>, u8) {
	let fail = |code, message: String| (String::new(), None, Some(message), code);
	let unnamed = key.is_none() && suspects.is_empty();
	if key.is_some() && !suspects.is_empty() || unnamed && export.is_none() {
		return fail(
			INPUT,
			"give either suspect identities or one suspect key".into(),
		);
	}
	if unnamed && suspect_only {
		return fail(INPUT, "--suspect-only needs a suspect".into());
	}
	if suspects.iter().any(|(role, _)| !(1..=3).contains(role)) {
		return fail(
			INPUT,
			"a suspect role is not admin, governance or recovery".into(),
		);
	}
	let manifest = match binding::trusted_manifest2(manifest) {
		Ok(manifest) => manifest,
		Err(reason) => return fail(INPUT, format!("trusted manifest refused: {reason}")),
	};
	let scan = match scan_control_log_read_only(data_dir).await {
		Ok(scan) => scan,
		Err(e) if e.kind() == ErrorKind::InvalidData => {
			return fail(INVALID, format!("invalid Control log history: {e}"));
		}
		Err(e) => return fail(INPUT, format!("Control log read failed: {e}")),
	};
	// Examined: the validated retained entries at or below the local stop. Everything stored past
	// it (all of it, with no stop) is counted and never audited.
	let stop = scan.committed;
	let (examined, past): (Vec<_>, usize) = match stop {
		Some(s) => (
			scan.entries
				.range(..=s.index)
				.map(|(_, e)| raft_log(e.log_id))
				.collect(),
			scan.entries
				.range((Bound::Excluded(s.index), Bound::Unbounded))
				.count(),
		),
		None => (Vec::new(), scan.entries.len()),
	};
	let coverage = Coverage2 {
		snapshot_boundary: scan.snapshot_boundary.map(raft_log),
		first_examined: examined.first().copied(),
		last_examined: examined.last().copied(),
		stop: stop.map(raft_log),
		uncommitted_past_stop: past as u64,
		torn_tail_bytes: scan.torn_tail_bytes,
	};
	// Complete only when every entry from genesis through the stop was read; the scanner already
	// refused gaps, so a later first entry is a compacted (snapshot-explained) prefix.
	let Some(stop) = stop
		.filter(|s| scan.entries.keys().next() == Some(&0) && scan.entries.contains_key(&s.index))
	else {
		return (render_partial(&coverage), None, None, PARTIAL);
	};
	let (pin, deployment) = match binding::bind2(&manifest, &scan.entries, stop.index) {
		Ok(bound) => bound,
		Err(e) => {
			return fail(
				INVALID,
				format!("trusted manifest is not bound to this log's install entry: {e:?}"),
			);
		}
	};
	let mut entries = scan.entries;
	entries.retain(|index, _| *index <= stop.index);
	let rotations = export.map(|_| export::rotations2(&entries));
	let ids: BTreeMap<u64, Log2> = entries
		.iter()
		.map(|(index, e)| (*index, raft_log(e.log_id)))
		.collect();
	let trace = Trace2::new();
	let attempted = trace.attempted();
	// The entry the dispatcher had taken up when the fold refused, named in the diagnostic.
	let at = || {
		attempted
			.load(Ordering::Relaxed)
			.checked_sub(1)
			.and_then(|index| ids.get(&index))
			.map_or("no entry".to_string(), |l| {
				format!(
					"log {{term {}, node {}, index {}}}",
					l.term, l.node, l.index
				)
			})
	};
	let folded = crate::storage::mutation_visibility::root2::replay::audit::fold2(
		entries, stop, deployment, &pin, trace,
	)
	.await;
	let traced = match folded {
		Ok(trace) => match trace.finish() {
			Ok(traced) => traced,
			Err(fault) => return fail(INVALID, format!("audit fold refused at {}: {fault}", at())),
		},
		Err(e @ ReplayError2::Fault(_)) => {
			return fail(
				INVALID,
				format!("audit fold refused the entry at {}: {e:?}", at()),
			);
		}
		Err(e) => return fail(INVALID, format!("audit fold refused the history: {e:?}")),
	};
	let mut identities: Vec<Identity2> = match key {
		Some(key) => match resolve_key2(&traced.history, &key) {
			Ok(one) => vec![one],
			Err(reason) => return fail(UNRESOLVED, reason.into()),
		},
		None => suspects.to_vec(),
	};
	identities.sort_unstable();
	identities.dedup();
	let exported = match (export, &rotations) {
		(Some(node), Some(rotations)) => {
			match export::export2(&traced, manifest.bytes(), node, raft_log(stop), rotations) {
				Ok(bytes) => Some(bytes),
				Err(reason) => return fail(INVALID, format!("context export refused: {reason}")),
			}
		}
		_ => None,
	};
	(
		render_complete(&traced, &identities, suspect_only, &coverage),
		exported,
		None,
		COMPLETE,
	)
}
/// A suspect key names exactly one `(role, epoch)` of this root's replayed history, or nothing is
/// audited: a key absent from the root, or one the history holds twice, is unresolved.
fn resolve_key2(history: &trace::History2, key: &[u8; 32]) -> Result<Identity2, &'static str> {
	match history.find(key)[..] {
		[one] => Ok(one),
		[] => Err("--key names no identity in this root"),
		_ => Err("--key resolves to more than one identity"),
	}
}

#[cfg(test)]
#[path = "tests/compromise/resolution.rs"]
mod tests;
