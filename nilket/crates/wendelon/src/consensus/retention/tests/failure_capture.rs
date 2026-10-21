//! Failure-only evidence, before the fixture's TestDir can destroy the WAL.
//! This is a checked per-file copy after fixture Raft shutdown, not a snapshot.
use crate::test_support::TestDir;
use serde_json::{Value, json};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) const ROOT_ENV: &str = "WENDELON_PD19_CAPTURE_ROOT";

pub(super) fn retain_failure(
	dir: TestDir,
	error: &io::Error,
	last: u64,
	expected_entries: &[Vec<u8>],
	root: Option<&Path>,
) -> String {
	let retained = dir.to_path_buf();
	// Preserve the original before any fallible evidence work. The campaign
	// must also exclude this path from external TMPDIR cleanup on failure.
	std::mem::forget(dir);
	let capture = root
		.ok_or_else(|| {
			io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("{ROOT_ENV} is not configured"),
			)
		})
		.and_then(|root| capture(&retained, root, error, last, expected_entries));
	format!(
		"retention reopen failed: {error:?}\noriginal fixture preserved: {}\ncapture: {capture:?}",
		retained.display()
	)
}

struct Entry {
	name: OsString,
	metadata: Value,
	regular: bool,
}
fn entries(root: &Path) -> io::Result<Vec<Entry>> {
	let mut entries = std::fs::read_dir(root)?
		.map(|entry| {
			let entry = entry?;
			let metadata = std::fs::symlink_metadata(entry.path())?;
			let kind = if metadata.is_file() {
				"regular"
			} else if metadata.file_type().is_symlink() {
				"symlink"
			} else if metadata.is_dir() {
				"directory"
			} else {
				"other"
			};
			let name = entry.file_name();
			let raw = name
				.as_bytes()
				.iter()
				.map(|byte| format!("{byte:02x}"))
				.collect::<String>();
			Ok(Entry {
				metadata: json!({"name": name.to_string_lossy(), "name_bytes_hex": raw, "kind": kind,
			"dev": metadata.dev(), "ino": metadata.ino(), "len": metadata.len(),
			"mtime": metadata.mtime(), "mtime_nsec": metadata.mtime_nsec()}),
				name,
				regular: metadata.is_file(),
			})
		})
		.collect::<io::Result<Vec<_>>>()?;
	entries.sort_by(|a, b| a.name.cmp(&b.name));
	Ok(entries)
}
fn open_regular(path: &Path) -> io::Result<File> {
	let file = OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
		.open(path)?;
	if !file.metadata()?.is_file() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"artifact is no longer regular",
		));
	}
	Ok(file)
}
fn hash(file: &mut File, limit: u64) -> io::Result<(u64, String)> {
	let mut reader = file.take(limit);
	let mut hash = blake3::Hasher::new();
	let mut bytes = [0; 65536];
	let mut total = 0;
	loop {
		let count = reader.read(&mut bytes)?;
		if count == 0 {
			break;
		}
		hash.update(&bytes[..count]);
		total += count as u64;
	}
	Ok((total, hash.finalize().to_hex().to_string()))
}
fn new_file(path: &Path) -> io::Result<File> {
	OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(path)
}
fn write_json(path: &Path, value: &Value) -> io::Result<()> {
	let mut file = new_file(path)?;
	serde_json::to_writer_pretty(&mut file, value).map_err(io::Error::other)?;
	file.write_all(b"\n")?;
	file.sync_all()
}
fn copy_one(source: &Path, destination: &Path, entry: &Entry) -> io::Result<Value> {
	let source_path = source.join(&entry.name);
	let mut input = open_regular(&source_path)?;
	let initial = input.metadata()?;
	if entry.metadata["dev"].as_u64() != Some(initial.dev())
		|| entry.metadata["ino"].as_u64() != Some(initial.ino())
	{
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"artifact identity changed after enumeration",
		));
	}
	let before = hash(&mut input, initial.len())?;
	input.seek(SeekFrom::Start(0))?;
	let destination_path = destination.join(&entry.name);
	let mut output = new_file(&destination_path)?;
	let copied = io::copy(&mut (&mut input).take(initial.len()), &mut output)?;
	output.sync_all()?;
	let copied_hash = hash(&mut open_regular(&destination_path)?, copied)?;
	let mut after_file = open_regular(&source_path)?;
	let after = after_file.metadata()?;
	let after_hash = hash(&mut after_file, after.len())?;
	let stable = initial.dev() == after.dev()
		&& initial.ino() == after.ino()
		&& initial.len() == after.len()
		&& initial.mtime() == after.mtime()
		&& initial.mtime_nsec() == after.mtime_nsec()
		&& copied == initial.len()
		&& before == copied_hash
		&& copied_hash == after_hash;
	Ok(
		json!({"name": entry.metadata["name"], "name_bytes_hex": entry.metadata["name_bytes_hex"],
		"before_len": before.0, "before_blake3": before.1, "copied_len": copied,
		"copy_blake3": copied_hash.1, "after_len": after_hash.0, "after_blake3": after_hash.1,
		"after_dev": after.dev(), "after_ino": after.ino(), "per_file_checks_passed": stable}),
	)
}

fn capture(
	source: &Path,
	root: &Path,
	error: &io::Error,
	last: u64,
	expected: &[Vec<u8>],
) -> io::Result<(PathBuf, bool)> {
	if !root.is_absolute() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"capture root must be explicit and absolute",
		));
	}
	// Reject even a not-yet-created lexical descendant before creating it.
	if root.starts_with(source) {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"capture root is inside original fixture",
		));
	}
	// The campaign creates this persistent root ahead of time. Resolve it
	// before any write so an alias into the fixture cannot create files there.
	let root = std::fs::canonicalize(root)?;
	if !root.is_dir() {
		return Err(io::Error::new(
			io::ErrorKind::NotADirectory,
			"capture root is not a directory",
		));
	}
	let source = std::fs::canonicalize(source)?;
	if root.starts_with(&source) {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"capture root resolves inside original fixture",
		));
	}
	let destination = root.join(format!(
		"retention-reopen-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::DirBuilder::new()
		.mode(0o700)
		.create(&destination)?;
	let result = (|| {
		let files = destination.join("files");
		std::fs::DirBuilder::new().mode(0o700).create(&files)?;
		let context = json!({"original_error": format!("{error:?}"), "error_kind": format!("{:?}", error.kind()),
		"source": source.to_string_lossy(), "capture": destination.to_string_lossy(), "last_pinned_index": last, "expected_entry_count": expected.len(),
		"fixture_quiescence": "Raft.shutdown awaited core+tick and original store/node clones dropped; failed store reopen precedes replacement Raft creation",
		"directory_owner": "none: original retention test seam uses blocking IO without an owner",
		"consistency": "per-file pre/copy/post checks and namespace comparison; NOT an atomic filesystem snapshot or global writer-quiescence proof",
		"pid": std::process::id(), "capture_complete": false});
		write_json(&destination.join("manifest.started.json"), &context)?;
		let before = entries(&source)?;
		let mut artifacts = Vec::new();
		let mut complete = true;
		for entry in &before {
			if !entry.regular {
				artifacts.push(
					json!({"skipped": entry.metadata, "reason": "not regular; never followed"}),
				);
				continue;
			}
			match copy_one(&source, &files, entry) {
				Ok(record) => {
					complete &= record["per_file_checks_passed"] == true;
					artifacts.push(record);
				}
				Err(error) => {
					complete = false;
					artifacts.push(
						json!({"name": entry.metadata["name"], "copy_error": format!("{error:?}")}),
					);
				}
			}
		}
		let after = entries(&source)?;
		let before_meta: Vec<_> = before.iter().map(|e| e.metadata.clone()).collect();
		let after_meta: Vec<_> = after.iter().map(|e| e.metadata.clone()).collect();
		let namespace_unchanged = before_meta == after_meta;
		complete &= namespace_unchanged;
		let mut expected_file = new_file(&destination.join("expected-entries.bin"))?;
		for entry in expected {
			expected_file.write_all(&(entry.len() as u64).to_le_bytes())?;
			expected_file.write_all(entry)?;
		}
		expected_file.sync_all()?;
		let mut manifest = context;
		manifest["artifact_presence"] = json!({
			"vote.wnd": before.iter().any(|e| e.name == "vote.wnd"),
			"snapshot.wnd": before.iter().any(|e| e.name == "snapshot.wnd"),
			"control.wal": before.iter().any(|e| e.name == "control.wal"),
			"segmented_wal_names": before.iter().filter(|e| e.name.to_string_lossy().starts_with("control.wal.")).map(|e| e.name.to_string_lossy().to_string()).collect::<Vec<_>>()
		});
		manifest["before_namespace"] = json!(before_meta);
		manifest["after_namespace"] = json!(after_meta);
		manifest["namespace_unchanged"] = json!(namespace_unchanged);
		manifest["artifacts"] = json!(artifacts);
		manifest["capture_complete"] = json!(complete);
		manifest["expected_entries_encoding"] = json!(
			"sequence of u64-LE length followed by original postcard entry bytes, from index0 through pinned last"
		);
		manifest["expected_entries_blake3"] = json!(
			hash(
				&mut open_regular(&destination.join("expected-entries.bin"))?,
				u64::MAX
			)?
			.1
		);
		write_json(&destination.join("manifest.json"), &manifest)?;
		File::open(&files)?.sync_all()?;
		File::open(&destination)?.sync_all()?;
		File::open(&root)?.sync_all()?;
		Ok((destination.clone(), complete))
	})();
	result.map_err(|error: io::Error| {
		io::Error::new(
			error.kind(),
			format!("partial capture {}: {error}", destination.display()),
		)
	})
}

#[test]
fn forced_error_preserves_source_and_captures_exact_artifacts() {
	let source = TestDir::new("retention-capture-forced-source");
	let retained = source.to_path_buf();
	let output = TestDir::new("retention-capture-forced-output");
	let original = io::Error::new(
		io::ErrorKind::InvalidData,
		"forced checksum mismatch at offset262144",
	);
	for (name, bytes) in [
		(
			"control.wal.00000000000000002785.seg",
			b"known-wal".as_slice(),
		),
		("vote.wnd", b"known-vote"),
		("snapshot.wnd", b"known-snapshot"),
	] {
		std::fs::write(source.join(name), bytes).unwrap();
	}
	std::os::unix::fs::symlink("/outside/not-followed", source.join("foreign-link")).unwrap();
	let message = retain_failure(
		source,
		&original,
		0,
		&[b"known-entry".to_vec()],
		Some(&output),
	);
	assert!(message.starts_with("retention reopen failed:"));
	assert!(message.contains("forced checksum mismatch at offset262144"));
	assert!(retained.exists());
	let destination = std::fs::read_dir(&output)
		.unwrap()
		.next()
		.unwrap()
		.unwrap()
		.path();
	let manifest: Value =
		serde_json::from_slice(&std::fs::read(destination.join("manifest.json")).unwrap()).unwrap();
	assert_eq!(manifest["capture_complete"], true);
	assert_eq!(manifest["namespace_unchanged"], true);
	assert!(
		manifest["original_error"]
			.as_str()
			.unwrap()
			.contains("forced checksum mismatch")
	);
	for name in [
		"control.wal.00000000000000002785.seg",
		"vote.wnd",
		"snapshot.wnd",
	] {
		let bytes = std::fs::read(retained.join(name)).unwrap();
		assert_eq!(
			std::fs::read(destination.join("files").join(name)).unwrap(),
			bytes
		);
		let record = manifest["artifacts"]
			.as_array()
			.unwrap()
			.iter()
			.find(|a| a["name"] == name)
			.unwrap();
		assert_eq!(
			record["before_blake3"],
			blake3::hash(&bytes).to_hex().to_string()
		);
		assert_eq!(record["copy_blake3"], record["before_blake3"]);
		assert_eq!(record["after_blake3"], record["before_blake3"]);
	}
	assert!(!destination.join("files/foreign-link").exists());
	assert_eq!(
		std::fs::read_link(retained.join("foreign-link")).unwrap(),
		Path::new("/outside/not-followed")
	);
	assert!(
		manifest["artifacts"]
			.as_array()
			.unwrap()
			.iter()
			.any(|a| a["skipped"]["kind"] == "symlink")
	);
	assert_eq!(
		std::fs::read(destination.join("expected-entries.bin")).unwrap(),
		[11u64.to_le_bytes().as_slice(), b"known-entry"].concat()
	);
	// Only this test's freshly allocated, deliberately retained source is removed.
	std::fs::remove_dir_all(retained).unwrap();
}

#[test]
fn capture_failure_or_unconfigured_root_never_discards_original_error_or_source() {
	let output = TestDir::new("retention-capture-bad-output");
	let not_directory = output.join("not-directory");
	std::fs::write(&not_directory, b"preserve").unwrap();
	for root in [None, Some(not_directory.as_path())] {
		let source = TestDir::new("retention-capture-error-source");
		let retained = source.to_path_buf();
		std::fs::write(source.join("vote.wnd"), b"original-vote").unwrap();
		let message = retain_failure(
			source,
			&io::Error::new(io::ErrorKind::InvalidData, "PRIMARY WAL ERROR"),
			0,
			&[],
			root,
		);
		assert!(message.contains("PRIMARY WAL ERROR"));
		assert!(message.contains("capture: Err("));
		assert!(message.contains(&retained.display().to_string()));
		assert_eq!(
			std::fs::read(retained.join("vote.wnd")).unwrap(),
			b"original-vote"
		);
		std::fs::remove_dir_all(retained).unwrap();
	}
	assert_eq!(std::fs::read(not_directory).unwrap(), b"preserve");
}

#[test]
fn capture_rejects_relative_or_descendant_root_without_touching_fixture() {
	for relative in [true, false] {
		let source = TestDir::new("retention-capture-rejected-root");
		let retained = source.to_path_buf();
		std::fs::write(source.join("vote.wnd"), b"preserve").unwrap();
		let root = if relative {
			PathBuf::from("relative-capture-root")
		} else {
			source.join("nested")
		};
		let message = retain_failure(
			source,
			&io::Error::new(io::ErrorKind::InvalidData, "PRIMARY REOPEN ERROR"),
			0,
			&[],
			Some(&root),
		);
		assert!(message.contains("PRIMARY REOPEN ERROR"));
		assert!(message.contains("capture: Err("));
		assert_eq!(std::fs::read_dir(&retained).unwrap().count(), 1);
		assert_eq!(
			std::fs::read(retained.join("vote.wnd")).unwrap(),
			b"preserve"
		);
		std::fs::remove_dir_all(retained).unwrap();
	}
}
