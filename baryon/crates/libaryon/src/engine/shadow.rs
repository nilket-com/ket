use super::undo::GenerationTarget;
use crate::core::DocByte;
use regex_automata::meta::Regex;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
pub(crate) const LARGE_FILE_GENERATION_THRESHOLD_BYTES: u64 = 1024;
#[cfg(not(test))]
pub(crate) const LARGE_FILE_GENERATION_THRESHOLD_BYTES: u64 = 100 * 1024 * 1024;

const SHADOW_DIR_SUFFIX: &str = ".baryon";
const MANIFEST_FILE_NAME: &str = "manifest";
const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub(crate) struct ShadowSession {
	pub(crate) dir: String,
	pub(crate) original_path: String,
	pub(crate) generations: Vec<GenerationTarget>,
	pub(crate) current_index: usize,
	pub(crate) next_generation_id: u64,
}

impl ShadowSession {
	pub(crate) fn current_generation(&self) -> Option<&GenerationTarget> {
		self.generations.get(self.current_index)
	}
}

fn escape_manifest_field(value: &str) -> String {
	let mut out = String::with_capacity(value.len());
	for ch in value.chars() {
		match ch {
			'\\' => out.push_str("\\\\"),
			'\n' => out.push_str("\\n"),
			'\t' => out.push_str("\\t"),
			_ => out.push(ch),
		}
	}
	out
}

fn unescape_manifest_field(value: &str) -> Result<String, String> {
	let mut out = String::with_capacity(value.len());
	let mut chars = value.chars();
	while let Some(ch) = chars.next() {
		if ch != '\\' {
			out.push(ch);
			continue;
		}
		match chars.next() {
			Some('n') => out.push('\n'),
			Some('t') => out.push('\t'),
			Some('\\') => out.push('\\'),
			Some(other) => {
				return Err(format!("Invalid manifest escape sequence: \\{}", other));
			}
			None => return Err("Invalid trailing escape in manifest".to_string()),
		}
	}
	Ok(out)
}

fn shadow_manifest_path(dir: &Path) -> PathBuf {
	dir.join(MANIFEST_FILE_NAME)
}

fn shadow_temp_path(target_path: &Path) -> Result<PathBuf, String> {
	let file_name = target_path
		.file_name()
		.ok_or_else(|| "Invalid shadow path".to_string())?;
	let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
	let file_name = file_name.to_string_lossy();
	let pid = std::process::id();

	for attempt in 0..1024u32 {
		let candidate = parent.join(format!(".{}.baryon.{}.{}.tmp", file_name, pid, attempt));
		if !candidate.exists() {
			return Ok(candidate);
		}
	}

	Err("Unable to allocate shadow temporary path".to_string())
}

fn write_atomic_bytes(
	target_path: &Path,
	build: impl FnOnce(&mut dyn Write) -> Result<(), String>,
) -> Result<(), String> {
	let temp_path = shadow_temp_path(target_path)?;
	let write_result = (|| -> Result<(), String> {
		let file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&temp_path)
			.map_err(|e| format!("Shadow write error: {}", e))?;
		let mut writer = BufWriter::with_capacity(1024 * 1024, file);
		build(&mut writer)?;
		writer
			.flush()
			.map_err(|e| format!("Shadow write error: {}", e))?;
		let file = writer
			.into_inner()
			.map_err(|e| format!("Shadow write error: {}", e.into_error()))?;
		file.sync_all()
			.map_err(|e| format!("Shadow write error: {}", e))?;
		fs::rename(&temp_path, target_path).map_err(|e| format!("Shadow write error: {}", e))?;
		Ok(())
	})();

	if write_result.is_err() {
		let _ = fs::remove_file(&temp_path);
	}

	write_result
}

fn serialize_manifest(session: &ShadowSession) -> String {
	let mut out = String::new();
	out.push_str(&format!("version\t{}\n", MANIFEST_VERSION));
	out.push_str(&format!(
		"original_path\t{}\n",
		escape_manifest_field(&session.original_path)
	));
	out.push_str(&format!("current_index\t{}\n", session.current_index));
	out.push_str(&format!(
		"next_generation_id\t{}\n",
		session.next_generation_id
	));
	for generation in &session.generations {
		out.push_str(&format!(
			"generation\t{}\t{}\t{}\n",
			generation.generation_id,
			generation.byte_len,
			escape_manifest_field(&generation.path),
		));
	}
	out
}

fn parse_manifest(dir: &Path, content: &str) -> Result<ShadowSession, String> {
	let mut original_path = None;
	let mut current_index = None;
	let mut next_generation_id = None;
	let mut generations = Vec::new();

	for line in content.lines() {
		let mut parts = line.split('\t');
		let Some(kind) = parts.next() else {
			continue;
		};
		match kind {
			"version" => {
				let Some(raw) = parts.next() else {
					return Err("Malformed shadow manifest version".to_string());
				};
				let version = raw
					.parse::<u32>()
					.map_err(|e| format!("Invalid shadow manifest version: {}", e))?;
				if version != MANIFEST_VERSION {
					return Err(format!("Unsupported shadow manifest version {}", version));
				}
			}
			"original_path" => {
				let Some(raw) = parts.next() else {
					return Err("Malformed shadow manifest original_path".to_string());
				};
				original_path = Some(unescape_manifest_field(raw)?);
			}
			"current_index" => {
				let Some(raw) = parts.next() else {
					return Err("Malformed shadow manifest current_index".to_string());
				};
				current_index = Some(
					raw.parse::<usize>()
						.map_err(|e| format!("Invalid shadow current_index: {}", e))?,
				);
			}
			"next_generation_id" => {
				let Some(raw) = parts.next() else {
					return Err("Malformed shadow manifest next_generation_id".to_string());
				};
				next_generation_id = Some(
					raw.parse::<u64>()
						.map_err(|e| format!("Invalid shadow next_generation_id: {}", e))?,
				);
			}
			"generation" => {
				let Some(raw_id) = parts.next() else {
					return Err("Malformed shadow manifest generation id".to_string());
				};
				let Some(raw_len) = parts.next() else {
					return Err("Malformed shadow manifest generation len".to_string());
				};
				let Some(raw_path) = parts.next() else {
					return Err("Malformed shadow manifest generation path".to_string());
				};
				generations.push(GenerationTarget {
					generation_id: raw_id
						.parse::<u64>()
						.map_err(|e| format!("Invalid shadow generation id: {}", e))?,
					byte_len: raw_len
						.parse::<u64>()
						.map_err(|e| format!("Invalid shadow generation length: {}", e))?,
					path: unescape_manifest_field(raw_path)?,
				});
			}
			_ => {}
		}
	}

	let original_path =
		original_path.ok_or_else(|| "Shadow manifest missing original_path".to_string())?;
	let current_index =
		current_index.ok_or_else(|| "Shadow manifest missing current_index".to_string())?;
	let next_generation_id = next_generation_id
		.ok_or_else(|| "Shadow manifest missing next_generation_id".to_string())?;
	if generations.is_empty() {
		return Err("Shadow manifest missing generations".to_string());
	}
	if current_index >= generations.len() {
		return Err("Shadow manifest current_index out of range".to_string());
	}

	Ok(ShadowSession {
		dir: dir.to_string_lossy().into_owned(),
		original_path,
		generations,
		current_index,
		next_generation_id,
	})
}

pub(crate) fn shadow_dir_for_path(original_path: &Path) -> Result<PathBuf, String> {
	let file_name = original_path
		.file_name()
		.ok_or_else(|| "Invalid shadow file path".to_string())?;
	let parent = original_path.parent().unwrap_or_else(|| Path::new("."));
	Ok(parent.join(format!(
		".{}{}",
		file_name.to_string_lossy(),
		SHADOW_DIR_SUFFIX
	)))
}

pub(crate) fn load_shadow_session(original_path: &Path) -> Result<Option<ShadowSession>, String> {
	let dir = shadow_dir_for_path(original_path)?;
	let manifest_path = shadow_manifest_path(&dir);
	if !manifest_path.exists() {
		return Ok(None);
	}
	let content = fs::read_to_string(&manifest_path)
		.map_err(|e| format!("Failed to read shadow manifest: {}", e))?;
	let session = parse_manifest(&dir, &content)?;
	if session.original_path != original_path.to_string_lossy() {
		return Err("Shadow manifest original path mismatch".to_string());
	}
	Ok(Some(session))
}

pub(crate) fn create_shadow_session(original_path: &Path) -> Result<ShadowSession, String> {
	let dir = shadow_dir_for_path(original_path)?;
	fs::create_dir_all(&dir).map_err(|e| format!("Failed to create shadow dir: {}", e))?;
	Ok(ShadowSession {
		dir: dir.to_string_lossy().into_owned(),
		original_path: original_path.to_string_lossy().into_owned(),
		generations: Vec::new(),
		current_index: 0,
		next_generation_id: 0,
	})
}

pub(crate) fn persist_shadow_session(session: &ShadowSession) -> Result<(), String> {
	let dir = Path::new(&session.dir);
	fs::create_dir_all(dir).map_err(|e| format!("Failed to create shadow dir: {}", e))?;
	let manifest_path = shadow_manifest_path(dir);
	let manifest = serialize_manifest(session);
	write_atomic_bytes(&manifest_path, |writer| {
		writer
			.write_all(manifest.as_bytes())
			.map_err(|e| format!("Shadow write error: {}", e))
	})
}

pub(crate) fn discard_shadow_session(session: &ShadowSession) -> Result<(), String> {
	let dir = Path::new(&session.dir);
	if !dir.exists() {
		return Ok(());
	}
	fs::remove_dir_all(dir).map_err(|e| format!("Failed to remove shadow dir: {}", e))
}

fn truncate_redo_generations(session: &mut ShadowSession) {
	if session.generations.is_empty() || session.current_index + 1 >= session.generations.len() {
		return;
	}
	for generation in &session.generations[session.current_index + 1..] {
		let _ = fs::remove_file(&generation.path);
	}
	session.generations.truncate(session.current_index + 1);
}

fn first_match_start(doc: &[u8], re: &Regex, byte_range: Option<(usize, usize)>) -> Option<usize> {
	let (start, end) = byte_range.unwrap_or((0, doc.len()));
	re.find(&doc[start..end]).map(|m| start + m.start())
}

fn next_generation_target(session: &mut ShadowSession) -> (u64, PathBuf) {
	let generation_id = session.next_generation_id;
	session.next_generation_id += 1;
	let path = Path::new(&session.dir).join(format!("gen-{:06}", generation_id));
	(generation_id, path)
}

fn write_generation_bytes(
	session: &mut ShadowSession,
	bytes: &[u8],
) -> Result<GenerationTarget, String> {
	let (generation_id, target_path) = next_generation_target(session);
	write_atomic_bytes(&target_path, |writer| {
		writer
			.write_all(bytes)
			.map_err(|e| format!("Shadow write error: {}", e))
	})?;
	Ok(GenerationTarget {
		generation_id,
		path: target_path.to_string_lossy().into_owned(),
		byte_len: bytes.len() as u64,
	})
}

fn write_substituted_generation(
	session: &mut ShadowSession,
	doc: &[u8],
	re: &Regex,
	replacement: &str,
	byte_range: Option<(usize, usize)>,
) -> Result<Option<(GenerationTarget, usize, DocByte)>, String> {
	let (start, end) = byte_range.unwrap_or((0, doc.len()));
	let slice = &doc[start..end];
	let Some(first_match) = re.find(slice) else {
		return Ok(None);
	};

	let (generation_id, target_path) = next_generation_target(session);
	let first_match_start = start + first_match.start();
	let cursor_after_byte =
		DocByte::new(first_match_start as u64).saturating_add(replacement.len() as u64);
	let mut match_count = 0usize;
	let mut removed_bytes = 0u64;
	write_atomic_bytes(&target_path, |writer| {
		writer
			.write_all(&doc[..start])
			.map_err(|e| format!("Shadow write error: {}", e))?;

		let mut previous_end = 0usize;
		for m in re.find_iter(slice) {
			writer
				.write_all(&slice[previous_end..m.start()])
				.map_err(|e| format!("Shadow write error: {}", e))?;
			writer
				.write_all(replacement.as_bytes())
				.map_err(|e| format!("Shadow write error: {}", e))?;
			previous_end = m.end();
			match_count += 1;
			removed_bytes += (m.end() - m.start()) as u64;
		}

		writer
			.write_all(&slice[previous_end..])
			.map_err(|e| format!("Shadow write error: {}", e))?;
		writer
			.write_all(&doc[end..])
			.map_err(|e| format!("Shadow write error: {}", e))
	})?;
	let byte_len = start as u64
		+ replacement.len() as u64 * match_count as u64
		+ (slice.len() as u64).saturating_sub(removed_bytes)
		+ doc[end..].len() as u64;
	Ok(Some((
		GenerationTarget {
			generation_id,
			path: target_path.to_string_lossy().into_owned(),
			byte_len,
		},
		match_count,
		cursor_after_byte,
	)))
}

pub(crate) fn checkpoint_substitute_generations(
	session: &mut ShadowSession,
	doc: &[u8],
	re: &Regex,
	replacement: &str,
	byte_range: Option<(usize, usize)>,
) -> Result<Option<(GenerationTarget, GenerationTarget, usize, DocByte)>, String> {
	if first_match_start(doc, re, byte_range).is_none() {
		return Ok(None);
	}
	truncate_redo_generations(session);

	let before = write_generation_bytes(session, doc)?;
	let Some((after, count, cursor_after_byte)) =
		write_substituted_generation(session, doc, re, replacement, byte_range)?
	else {
		return Ok(None);
	};

	session.generations.push(before.clone());
	session.generations.push(after.clone());
	session.current_index = session.generations.len().saturating_sub(1);
	persist_shadow_session(session)?;
	Ok(Some((before, after, count, cursor_after_byte)))
}

pub(crate) fn sync_shadow_to_generation(
	session: &mut ShadowSession,
	target: &GenerationTarget,
) -> Result<(), String> {
	let Some(index) = session
		.generations
		.iter()
		.position(|generation| generation.generation_id == target.generation_id)
	else {
		return Err("Shadow generation not found".to_string());
	};
	let previous_index = session.current_index;
	session.current_index = index;
	if let Err(error) = persist_shadow_session(session) {
		session.current_index = previous_index;
		return Err(error);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::{
		ShadowSession, checkpoint_substitute_generations, create_shadow_session,
		discard_shadow_session, load_shadow_session, persist_shadow_session,
		sync_shadow_to_generation,
	};
	use crate::engine::core::build_regex;
	use crate::engine::test_support::temp_test_path;

	#[test]
	fn shadow_manifest_round_trips_generations() {
		let path = temp_test_path("shadow-manifest.txt");
		let mut session = create_shadow_session(&path).expect("create shadow");
		session.generations = vec![
			crate::engine::undo::GenerationTarget {
				generation_id: 0,
				path: "/tmp/gen-0".to_string(),
				byte_len: 3,
			},
			crate::engine::undo::GenerationTarget {
				generation_id: 1,
				path: "/tmp/gen-1".to_string(),
				byte_len: 4,
			},
		];
		session.current_index = 1;
		session.next_generation_id = 2;
		persist_shadow_session(&session).expect("persist manifest");

		let recovered = load_shadow_session(&path)
			.expect("load manifest")
			.expect("shadow should exist");
		assert_eq!(recovered.original_path, session.original_path);
		assert_eq!(recovered.current_index, 1);
		assert_eq!(recovered.generations.len(), 2);
		assert_eq!(recovered.next_generation_id, 2);

		discard_shadow_session(&session).expect("discard shadow");
	}

	#[test]
	fn checkpoint_substitute_generations_writes_before_and_after() {
		let path = temp_test_path("shadow-substitute.txt");
		let mut session = create_shadow_session(&path).expect("create shadow");
		let re = build_regex("beta", false).expect("regex");
		let result = checkpoint_substitute_generations(
			&mut session,
			b"alpha beta gamma\n",
			&re,
			"delta",
			None,
		)
		.expect("checkpoint substitute")
		.expect("must replace");
		assert_eq!(result.2, 1);
		assert_eq!(session.generations.len(), 2);
		let before = std::fs::read(&result.0.path).expect("read before");
		let after = std::fs::read(&result.1.path).expect("read after");
		assert_eq!(&before, b"alpha beta gamma\n");
		assert_eq!(&after, b"alpha delta gamma\n");

		discard_shadow_session(&session).expect("discard shadow");
	}

	#[test]
	fn failed_shadow_sync_restores_current_generation() {
		let invalid_dir = temp_test_path("shadow-sync-invalid-dir");
		std::fs::write(&invalid_dir, b"not a directory").expect("create invalid shadow dir");
		let generations = vec![
			crate::engine::undo::GenerationTarget {
				generation_id: 0,
				path: "/tmp/gen-0".to_string(),
				byte_len: 3,
			},
			crate::engine::undo::GenerationTarget {
				generation_id: 1,
				path: "/tmp/gen-1".to_string(),
				byte_len: 4,
			},
		];
		let target = generations[1].clone();
		let mut session = ShadowSession {
			dir: invalid_dir.to_string_lossy().into_owned(),
			original_path: "/tmp/original".to_string(),
			generations,
			current_index: 0,
			next_generation_id: 2,
		};

		let error = sync_shadow_to_generation(&mut session, &target)
			.expect_err("persisting into a file must fail");
		assert!(error.contains("Failed to create shadow dir"));
		assert_eq!(session.current_index, 0);

		std::fs::remove_file(invalid_dir).expect("remove invalid shadow dir");
	}
}
