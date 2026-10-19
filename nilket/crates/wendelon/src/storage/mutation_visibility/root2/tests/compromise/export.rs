//! C-S6b context export, file and CLI: T-X1 (complete only, atomic), T-X4 (deterministic), T-X5
//! (read-only), T-PATH (never inside the data directory) and T-CLI (the empty suspect set).
use super::export_chain::exported;
use super::reader::{parse, verify};
use super::support::*;
use crate::cli::root2_audit::fail_publish_at;
use crate::consensus::raft_storage::read_only_fixture::{record_len, write_segment};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn history() -> Root {
	let mut r = Root::new();
	r.rotate(ADMIN, 21); // 3
	applied(r.governance_use(r.current(GOVERNANCE))); // 4
	r
}
/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
	let mut out: Vec<_> = std::fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().file_name().into_string().unwrap())
		.collect();
	out.sort();
	out
}
/// A run that must leave nothing at `out`, no temp file beside it, and no stdout.
fn left_nothing(run: &Out, out: &Path, code: u8) {
	assert_eq!(run.code, code, "{}", run.stderr);
	assert!(
		std::fs::symlink_metadata(out).is_err(),
		"no file at the output"
	);
	let parent = out.parent().unwrap();
	assert!(
		names(parent)
			.iter()
			.all(|n| !n.starts_with(".wrctxexp.tmp.")),
		"no temp file left"
	);
	assert!(!run.stderr.is_empty());
}

#[test]
fn t_x1_only_a_complete_fold_writes_the_file() {
	let r = history();
	let dirs = r.write(None);
	let out = dirs.inputs.join("context.wrctxexp");
	let run = audit(
		&dirs,
		&[suspects(&[(ADMIN, 0)]), export_args(&out, 7)].concat(),
	);
	check_outcome(&run);
	assert_eq!(run.code, 0, "{}", run.stderr);
	let bytes = std::fs::read(&out).unwrap();
	assert!(verify(&bytes, r.manifest()).is_ok());
	use std::os::unix::fs::MetadataExt;
	assert_eq!(std::fs::metadata(&out).unwrap().mode() & 0o777, 0o600);
	assert_eq!(names(&dirs.inputs), ["context.wrctxexp", "manifest.bin"]);
	// Exit 1: a flipped body byte inside the examined range.
	let corrupt = r.write(None);
	let segment = std::fs::read_dir(&corrupt.data)
		.unwrap()
		.next()
		.unwrap()
		.unwrap()
		.path();
	let offset: usize = r.f.entries[..3].iter().map(record_len).sum();
	let mut b = std::fs::read(&segment).unwrap();
	b[offset + 40] ^= 0xff;
	std::fs::write(&segment, b).unwrap();
	let out = corrupt.inputs.join("context.wrctxexp");
	let run = audit(
		&corrupt,
		&[suspects(&[(ADMIN, 0)]), export_args(&out, 7)].concat(),
	);
	left_nothing(&run, &out, 1);
	assert!(run.stdout.is_empty());
	// Exit 2: an unreadable trusted manifest (after the path check passed).
	let out = dirs.inputs.join("second.wrctxexp");
	let run = audit_with(
		&dirs.data,
		&dirs.inputs.join("absent"),
		&[suspects(&[(ADMIN, 0)]), export_args(&out, 7)].concat(),
	);
	left_nothing(&run, &out, 2);
	// Exit 3: no local committed position, so partial coverage; its record is still printed.
	let open = Dirs::new(r.manifest());
	write_segment(&open.data, 0, &r.f.entries, None);
	let out = open.inputs.join("context.wrctxexp");
	let run = audit(
		&open,
		&[suspects(&[(ADMIN, 0)]), export_args(&out, 7)].concat(),
	);
	assert_eq!(run.coverage.as_ref().unwrap()["status"], "partial");
	assert_eq!(run.code, 3);
	assert!(std::fs::symlink_metadata(&out).is_err());
	// Exit 4: an unresolved suspect key.
	let absent = dirs.key_file(&key(77));
	let run = audit(
		&dirs,
		&[
			vec!["--key".into(), absent.display().to_string()],
			export_args(&out, 7),
		]
		.concat(),
	);
	left_nothing(&run, &out, 4);
	assert!(run.stdout.is_empty());
}

#[test]
fn t_x1_a_write_failure_leaves_no_file_no_temp_and_no_stdout() {
	let r = history();
	let dirs = r.write(None);
	let out = dirs.inputs.join("context.wrctxexp");
	let args = [suspects(&[(ADMIN, 0)]), export_args(&out, 7)].concat();
	for step in ["write", "sync", "rename", "directory"] {
		fail_publish_at(Some(step));
		let run = audit(&dirs, &args);
		fail_publish_at(None);
		left_nothing(&run, &out, 2);
		assert!(run.stdout.is_empty(), "{step}: no rows before the export");
		assert!(run.stderr.contains(step), "{}", run.stderr);
	}
	// An unwritable output directory.
	let locked = dirs.inputs.join("locked");
	std::fs::create_dir(&locked).unwrap();
	std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
	let inside = locked.join("context.wrctxexp");
	let run = audit(
		&dirs,
		&[suspects(&[(ADMIN, 0)]), export_args(&inside, 7)].concat(),
	);
	std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
	left_nothing(&run, &inside, 2);
	assert!(run.stdout.is_empty());
	// A stdout that fails after the publish: exit 2 withdraws the export, but only the file this
	// run published. `swap` first replaces it (unlink and recreate, the old one held open so its
	// inode is not reused), which must survive and be named.
	struct Broken {
		swap: Option<std::path::PathBuf>,
		held: Option<std::fs::File>,
	}
	impl std::io::Write for Broken {
		fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
			if let Some(path) = self.swap.take() {
				self.held = Some(std::fs::File::open(&path).unwrap());
				std::fs::remove_file(&path).unwrap();
				std::fs::write(&path, b"someone else's file").unwrap();
			}
			Err(std::io::Error::other("stdout closed"))
		}
		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}
	let full = [
		vec![
			dirs.data.display().to_string(),
			"--trusted-manifest".into(),
			dirs.manifest.display().to_string(),
		],
		args.clone(),
	]
	.concat();
	let mut err = Vec::new();
	let mut broken = Broken {
		swap: None,
		held: None,
	};
	assert_eq!(
		crate::cli::root2_audit::execute(&full, &mut broken, &mut err),
		2
	);
	let err = String::from_utf8(err).unwrap();
	assert!(err.contains("stdout write failed"), "{err}");
	assert!(std::fs::symlink_metadata(&out).is_err(), "withdrawn");
	assert!(
		names(&dirs.inputs)
			.iter()
			.all(|n| !n.starts_with(".wrctxexp.tmp."))
	);
	// The swapped case: the replacement is left in place and named; still exit 2.
	let mut err = Vec::new();
	let mut broken = Broken {
		swap: Some(out.clone()),
		held: None,
	};
	assert_eq!(
		crate::cli::root2_audit::execute(&full, &mut broken, &mut err),
		2
	);
	assert!(broken.held.is_some(), "the swap ran after the publish");
	let err = String::from_utf8(err).unwrap();
	assert!(err.contains("stdout write failed"), "{err}");
	assert!(
		err.contains(&format!(
			"{} is no longer the published export",
			out.display()
		)),
		"{err}"
	);
	assert_eq!(std::fs::read(&out).unwrap(), b"someone else's file");
	std::fs::remove_file(&out).unwrap();
	// Control: the same arguments with no injected failure publish.
	let run = audit(&dirs, &args);
	assert_eq!(run.code, 0, "{}", run.stderr);
	assert!(!run.stdout.is_empty());
	assert!(std::fs::read(&out).is_ok());
}

#[test]
fn t_x4_two_runs_are_byte_identical() {
	let r = history();
	let a = exported(&r, 7);
	let b = exported(&r, 7);
	assert_eq!(a, b);
	// The node id label changes exactly its four bytes (and the digest).
	let c = exported(&r, 8);
	assert_ne!(a, c);
	assert_eq!(a[..441], c[..441]);
	assert_eq!(c[441..445], 8u32.to_le_bytes());
	assert_eq!(a[445..a.len() - 32], c[445..c.len() - 32]);
	// One more committed entry changes the stop.
	let mut more = history();
	applied(more.governance_use(more.current(GOVERNANCE)));
	let d = exported(&more, 7);
	assert_ne!(a, d);
	assert_eq!(
		parse(&d).unwrap().stop.index,
		parse(&a).unwrap().stop.index + 1
	);
}

#[test]
fn t_x5_the_data_directory_is_untouched_and_unlocked() {
	let r = history();
	let dirs = r.write(None);
	let before = listing(&dirs.data);
	let out = dirs.inputs.join("context.wrctxexp");
	let run = audit(
		&dirs,
		&[suspects(&[(ADMIN, 0)]), export_args(&out, 7)].concat(),
	);
	assert_eq!(run.code, 0, "{}", run.stderr);
	assert_eq!(listing(&dirs.data), before);
	// No lock, sentinel or temp file: only the one segment.
	assert_eq!(std::fs::read_dir(&dirs.data).unwrap().count(), 1);
	// Control: the harness sees a deliberate write into the data directory.
	std::fs::write(dirs.data.join("planted"), b"x").unwrap();
	assert_ne!(listing(&dirs.data), before);
}

#[test]
fn t_path_the_output_never_lands_inside_the_data_directory() {
	use std::os::unix::fs::symlink;
	let r = history();
	let dirs = r.write(None);
	let args = |out: &Path| [suspects(&[(ADMIN, 0)]), export_args(out, 7)].concat();
	let before = listing(&dirs.data);
	let sub = dirs.data.join("sub");
	std::fs::create_dir(&sub).unwrap();
	let before_sub = listing(&dirs.data);
	symlink(&dirs.data, dirs.inputs.join("alias")).unwrap();
	symlink(&sub, dirs.inputs.join("alias-sub")).unwrap();
	symlink(dirs.inputs.join("missing"), dirs.inputs.join("dangling")).unwrap();
	let existing = dirs.inputs.join("existing");
	std::fs::write(&existing, b"keep").unwrap();
	let data = dirs.data.display().to_string();
	let inputs = dirs.inputs.display().to_string();
	for bad in [
		// Inside, directly and below.
		format!("{data}/context.wrctxexp"),
		format!("{data}/sub/context.wrctxexp"),
		// A symlinked parent resolving to, or into, the data directory.
		format!("{inputs}/alias/context.wrctxexp"),
		format!("{inputs}/alias-sub/context.wrctxexp"),
		// Path aliases: `..` spellings, and the same directory reached through /proc.
		format!("{data}/../data/context.wrctxexp"),
		format!("{inputs}/../data/sub/../context.wrctxexp"),
		format!("/proc/self/root{data}/context.wrctxexp"),
		// Something already there: a file, the data directory itself, a dangling symlink.
		existing.display().to_string(),
		data.clone(),
		format!("{inputs}/dangling"),
		// Not one file-name component.
		format!("{inputs}/context.wrctxexp/"),
		format!("{inputs}/.."),
		format!("{inputs}/."),
		// The tool's own temp-file prefix.
		format!("{inputs}/.wrctxexp.tmp.1.0"),
		"/".to_string(),
	] {
		let out = Path::new(&bad);
		let run = audit(&dirs, &args(out));
		assert_eq!(run.code, 2, "{bad}: {}", run.stderr);
		assert!(run.stdout.is_empty(), "{bad}");
		assert!(
			run.stderr.contains("--export-context"),
			"{bad}: {}",
			run.stderr
		);
	}
	assert_eq!(listing(&dirs.data), before_sub, "nothing landed inside");
	assert_eq!(std::fs::read(&existing).unwrap(), b"keep");
	assert!(std::fs::symlink_metadata(dirs.inputs.join("missing")).is_err());
	std::fs::remove_dir(&sub).unwrap();
	assert_eq!(listing(&dirs.data), before);
	// Control: a plain directory outside the data directory, and a `..` spelling of it.
	let out = dirs.inputs.join("context.wrctxexp");
	let run = audit(&dirs, &args(&out));
	assert_eq!(run.code, 0, "{}", run.stderr);
	assert!(verify(&std::fs::read(&out).unwrap(), r.manifest()).is_ok());
	let sibling = Path::new(&format!("{data}/../inputs/second.wrctxexp")).to_path_buf();
	let run = audit(&dirs, &args(&sibling));
	assert_eq!(run.code, 0, "{}", run.stderr);
}

#[test]
fn t_cli_an_empty_suspect_set_needs_the_export() {
	let r = history();
	let dirs = r.write(None);
	let out = dirs.inputs.join("context.wrctxexp");
	let run = audit(&dirs, &export_args(&out, 7));
	check_outcome(&run);
	assert_eq!(run.code, 0, "{}", run.stderr);
	assert_eq!(run.rows.len(), 3, "every root entry, unmarked");
	assert!(run.rows.iter().all(|row| row["match"] == "none"));
	assert!(
		run.coverage.as_ref().unwrap()["containment"]
			.as_array()
			.unwrap()
			.is_empty()
	);
	assert!(std::fs::read(&out).is_ok());
	// Without the export, an empty suspect set is refused.
	let run = audit(&dirs, &[]);
	assert_eq!(run.code, 2);
	assert!(run.stdout.is_empty());
	// --suspect-only with no suspects is a usage error even with the export.
	let other = dirs.inputs.join("other.wrctxexp");
	let run = audit(
		&dirs,
		&[export_args(&other, 7), vec!["--suspect-only".into()]].concat(),
	);
	assert_eq!(run.code, 2);
	assert!(std::fs::symlink_metadata(&other).is_err());
	// --export-context and --node-id only together.
	let half = &export_args(&other, 7);
	for args in [&half[..2], &half[2..]] {
		let run = audit(&dirs, &[suspects(&[(ADMIN, 0)]), args.to_vec()].concat());
		assert_eq!(run.code, 2);
		assert!(std::fs::symlink_metadata(&other).is_err());
	}
}
