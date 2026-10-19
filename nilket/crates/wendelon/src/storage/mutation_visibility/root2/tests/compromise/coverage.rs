//! Coverage, outcomes and the read-only path: A4, A5, A6, A7, A11, A12, and the binding refusals.
use super::support::*;
use super::wire;
use crate::consensus::raft_storage::read_only_fixture::{
	record_len, write_segment, write_snapshot,
};
use crate::storage::mutation_visibility::root2::command::Command2;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

fn history() -> Root {
	let mut r = Root::new();
	applied(r.governance_use(r.current(GOVERNANCE))); // 3
	r.rotate(GOVERNANCE, 41); // 4
	r.statement_use(11); // 5
	applied(r.governance_use(r.current(GOVERNANCE))); // 6
	r
}
fn index(v: &serde_json::Value) -> Option<u64> {
	v["index"].as_u64()
}

#[test]
fn a4_unresolved_key_exits_4_without_a_complete_record() {
	let r = history();
	let dirs = r.write(None);
	let absent = dirs.key_file(&key(77));
	let out = audit(&dirs, &["--key".into(), absent.display().to_string()]);
	check_outcome(&out);
	assert_eq!(out.code, 4);
	assert!(
		out.stdout.is_empty(),
		"no rows and no record for an unresolved suspect"
	);
	assert!(out.stderr.contains("no identity"));
	// Control: the governance successor's key resolves to (governance, 1).
	let held = dirs.key_file(&key(41));
	let out = audit(&dirs, &["--key".into(), held.display().to_string()]);
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert_eq!(out.indices("suspect_use"), [6]);
	assert_eq!(
		index(&out.containment("governance", 1)["activated_at"]),
		Some(4)
	);
}

#[test]
fn a5_compacted_install_entry_is_partial_with_the_boundary_and_no_rows() {
	let r = history();
	let entries = &r.f.entries;
	let boundary = entries[3].log_id;
	let dirs = Dirs::new(r.manifest());
	write_snapshot(&dirs.data, boundary);
	write_segment(
		&dirs.data,
		4,
		&entries[4..],
		Some(entries.last().unwrap().log_id),
	);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 3, "{}", out.stderr);
	assert!(
		out.rows.is_empty(),
		"no rows for a range that was not folded"
	);
	let c = out.coverage.as_ref().unwrap();
	assert_eq!(c["status"], "partial");
	assert_eq!(index(&c["snapshot_boundary"]), Some(3));
	assert_eq!(index(&c["first_examined"]), Some(4));
	assert_eq!(index(&c["last_examined"]), Some(6));
	assert_eq!(index(&c["stop_position"]), Some(6));
	assert!(c["containment"].as_array().unwrap().is_empty());
	// Control: the same history fully retained is complete.
	let full = audit(&r.write(None), &suspects(&[(ADMIN, 0)]));
	assert_eq!(full.code, 0, "{}", full.stderr);
	assert_eq!(index(&full.coverage.unwrap()["first_examined"]), Some(0));
}

#[test]
fn a6_two_runs_are_byte_identical() {
	let r = history();
	let dirs = r.write(None);
	for extra in [
		suspects(&[(ADMIN, 0), (GOVERNANCE, 0)]),
		[suspects(&[(GOVERNANCE, 1)]), vec!["--suspect-only".into()]].concat(),
	] {
		let a = audit(&dirs, &extra);
		let b = audit(&dirs, &extra);
		assert_eq!(a.code, 0, "{}", a.stderr);
		assert_eq!(a.stdout, b.stdout);
		assert!(!a.stdout.is_empty());
	}
	// The suspect set's spelling order does not change the output.
	let ab = audit(&dirs, &suspects(&[(ADMIN, 0), (GOVERNANCE, 0)]));
	let ba = audit(&dirs, &suspects(&[(GOVERNANCE, 0), (ADMIN, 0), (ADMIN, 0)]));
	assert_eq!(ab.stdout, ba.stdout);
}

/// Every file under `dir`: its bytes and modification time.
fn listing(dir: &Path) -> BTreeMap<PathBuf, (Vec<u8>, SystemTime)> {
	let mut out = BTreeMap::new();
	let mut stack = vec![dir.to_path_buf()];
	while let Some(d) = stack.pop() {
		for e in std::fs::read_dir(&d).unwrap() {
			let p = e.unwrap().path();
			let meta = std::fs::symlink_metadata(&p).unwrap();
			if meta.is_dir() {
				stack.push(p.clone());
			}
			let bytes = if meta.is_file() {
				std::fs::read(&p).unwrap()
			} else {
				Vec::new()
			};
			out.insert(p, (bytes, meta.modified().unwrap()));
		}
	}
	out
}
fn tear(path: &Path, r: &Root) -> u64 {
	use std::io::Write;
	// The first 100 bytes of one more real record: a torn final write.
	let page = {
		let scratch = Dirs::new(r.manifest());
		let seg = write_segment(&scratch.data, 0, &r.f.entries[..1], None);
		std::fs::read(seg).unwrap()
	};
	let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
	f.write_all(&page[..100]).unwrap();
	100
}

#[test]
fn a7_the_data_directory_is_untouched_including_over_a_torn_tail() {
	let r = history();
	let dirs = r.write(None);
	let segment = std::fs::read_dir(&dirs.data)
		.unwrap()
		.next()
		.unwrap()
		.unwrap()
		.path();
	for torn in [false, true] {
		if torn {
			tear(&segment, &r);
		}
		let before = listing(&dirs.data);
		let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
		assert_eq!(out.code, 0, "{}", out.stderr);
		let unresolved = audit(
			&dirs,
			&["--key".into(), dirs.key_file(&key(1)).display().to_string()],
		);
		assert_eq!(unresolved.code, 4);
		assert_eq!(listing(&dirs.data), before, "torn={torn}");
	}
	// No lock, sentinel or repair artifact appeared: only the one segment.
	assert_eq!(std::fs::read_dir(&dirs.data).unwrap().count(), 1);
}

#[test]
fn a11_corruption_in_range_is_invalid_and_a_torn_tail_past_the_stop_is_reported() {
	let r = history();
	let dirs = r.write(None);
	let segment = std::fs::read_dir(&dirs.data)
		.unwrap()
		.next()
		.unwrap()
		.unwrap()
		.path();
	// Control: intact.
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0);
	assert_eq!(out.coverage.as_ref().unwrap()["torn_tail_past_stop"], 0);
	// A torn tail past the stop: complete, with its length reported.
	let torn = tear(&segment, &r);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert_eq!(out.coverage.as_ref().unwrap()["torn_tail_past_stop"], torn);
	// One flipped body byte in record 3, inside the examined range: invalid history.
	let offset: usize = r.f.entries[..3].iter().map(record_len).sum();
	let mut bytes = std::fs::read(&segment).unwrap();
	bytes[offset + 40] ^= 0xff;
	std::fs::write(&segment, bytes).unwrap();
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 1);
	assert!(out.stdout.is_empty());
	assert!(out.stderr.contains("invalid Control log history"));
}

#[test]
fn a12_uncommitted_entries_past_the_stop_are_counted_and_never_rows() {
	let mut r = history();
	let stop = r.next() - 1; // 6
	r.statement_use(11); // 7, a suspect use past the stop
	applied(r.governance_use(r.current(GOVERNANCE))); // 8
	let dirs = r.write(Some(stop));
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let c = out.coverage.as_ref().unwrap();
	assert_eq!(c["uncommitted_past_stop"], 2);
	assert_eq!(index(&c["stop_position"]), Some(stop));
	assert_eq!(index(&c["last_examined"]), Some(stop));
	assert_eq!(c["stop_scope"], "local_committed_not_quorum_current");
	assert!(
		out.rows
			.iter()
			.all(|row| row["log_id"]["index"].as_u64() <= Some(stop))
	);
	assert_eq!(out.indices("suspect_use"), [5]);
	// Control: committing through the end audits the same suspect use at 7.
	let all = audit(&r.write(None), &suspects(&[(ADMIN, 0)]));
	assert_eq!(all.indices("suspect_use"), [5, 7]);
	assert_eq!(all.coverage.unwrap()["uncommitted_past_stop"], 0);
}

#[test]
fn binding_refusals_are_invalid_and_inputs_are_typed() {
	let r = history();
	let dirs = r.write(None);
	// Another valid profile-3 manifest (a different recovery key): not the one this log installed.
	let foreign = dirs.inputs.join("other.bin");
	let alternate = {
		use crate::storage::mutation_visibility::root2::admin::manifest::{
			AdminManifestThree3, GovernanceRole2, RecoveryRole3,
		};
		let mut base = wire::Fixture::new("b").any.admission().clone();
		base.admin_key = key(11);
		AdminManifestThree3::new(
			base,
			GovernanceRole2::new([0xd2; 16], key(12)).unwrap(),
			RecoveryRole3::new([0xd3; 16], key(14)).unwrap(),
		)
		.unwrap()
		.encode()
		.unwrap()
		.to_vec()
	};
	std::fs::write(&foreign, &alternate).unwrap();
	let out = audit_with(&dirs.data, &foreign, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(
		out.code, 1,
		"a decodable manifest the log never installed is a binding refusal"
	);
	assert!(out.stdout.is_empty());
	assert!(out.stderr.contains("Pin"), "{}", out.stderr);
	// No install entry before the stop: the pre-root prefix only.
	let pre = Dirs::new(r.manifest());
	write_segment(&pre.data, 0, &r.f.entries[..2], Some(r.f.entries[1].log_id));
	let out = audit(&pre, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 1);
	assert!(out.stderr.contains("InstallMissing"), "{}", out.stderr);
	// No local committed position: nothing is auditable as committed.
	let open = Dirs::new(r.manifest());
	write_segment(&open.data, 0, &r.f.entries, None);
	let out = audit(&open, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 3);
	let c = out.coverage.unwrap();
	assert!(c["stop_position"].is_null());
	assert_eq!(c["uncommitted_past_stop"], r.f.entries.len());
	// An unreadable data directory is an I/O error.
	let out = audit_with(
		&dirs.inputs.join("absent"),
		&dirs.manifest,
		&suspects(&[(ADMIN, 0)]),
	);
	check_outcome(&out);
	assert_eq!(out.code, 2);
	assert!(out.stdout.is_empty());
}

#[test]
fn a_committed_entry_the_verifier_refuses_stops_the_fold_and_is_named() {
	let mut r = history();
	// Control: the intact history folds.
	let out = audit(&r.write(None), &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	// A committed RotateAdmin carrier with no sidecar: a structural refusal, not a denial.
	let at = r.next();
	r.push_unverified(&Command2::RotateAdmin(key(46)), None);
	let out = audit(&r.write(None), &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 1);
	assert!(
		out.stdout.is_empty(),
		"no rows and no record for a refused fold"
	);
	assert!(out.stderr.contains("audit fold refused"), "{}", out.stderr);
	assert!(
		out.stderr
			.contains(&format!("log {{term 1, node 1, index {at}}}")),
		"{}",
		out.stderr
	);
}
