//! External history handoff surfaces for simulator-produced register traces.
//!
//! The Rust workspace owns artifact generation only. External linearizability
//! analyzers such as Elle consume these artifacts out-of-process.

use crate::simulator::checkers::{RegisterHistoryEntry, RegisterOperation};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

pub const REGISTER_HISTORY_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterHistoryArtifactPaths {
	pub json: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterHistoryArtifact {
	pub schema_version: u32,
	pub harness: String,
	pub model: String,
	pub scenario: String,
	pub seed: u64,
	pub requested_ops: u64,
	pub batches: Vec<RegisterHistoryBatch>,
	pub events: Vec<RegisterHistoryEvent>,
}

impl RegisterHistoryArtifact {
	pub fn new(
		scenario: impl Into<String>,
		seed: u64,
		requested_ops: u64,
		batches: Vec<RegisterHistoryBatch>,
	) -> Self {
		let events = flatten_batches(&batches);
		Self {
			schema_version: REGISTER_HISTORY_SCHEMA_VERSION,
			harness: "wendelon.simulator".to_string(),
			model: "single_register".to_string(),
			scenario: scenario.into(),
			seed,
			requested_ops,
			batches,
			events,
		}
	}

	pub fn write_to_dir(
		&self,
		dir: impl AsRef<Path>,
	) -> std::io::Result<RegisterHistoryArtifactPaths> {
		let dir = dir.as_ref();
		fs::create_dir_all(dir)?;
		let stem = format!(
			"{}-seed-{:016x}-ops-{}-register-history",
			sanitize_stem(&self.scenario),
			self.seed,
			self.requested_ops
		);
		let json = dir.join(format!("{stem}.json"));
		if json.exists() {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!(
					"register history artifact already exists: {}",
					json.display()
				),
			));
		}
		let body = serde_json::to_vec_pretty(self)
			.map_err(|err| Error::other(format!("serialize register history artifact: {err}")))?;
		fs::write(&json, body)?;
		Ok(RegisterHistoryArtifactPaths { json })
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterHistoryBatch {
	pub batch_index: u64,
	pub iteration_end: u64,
	pub operations: Vec<RegisterHistoryOperation>,
}

impl RegisterHistoryBatch {
	pub fn from_entries(
		batch_index: u64,
		iteration_end: u64,
		entries: &[RegisterHistoryEntry],
	) -> Self {
		Self {
			batch_index,
			iteration_end,
			operations: entries
				.iter()
				.map(RegisterHistoryOperation::from_entry)
				.collect(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterHistoryOperation {
	pub process: u64,
	pub key: String,
	pub started_at: u64,
	pub completed_at: u64,
	pub op: RegisterHistoryKind,
}

impl RegisterHistoryOperation {
	pub fn from_entry(entry: &RegisterHistoryEntry) -> Self {
		Self {
			process: entry.process,
			key: entry.key.clone(),
			started_at: entry.started_at,
			completed_at: entry.completed_at,
			op: match entry.op {
				RegisterOperation::Write { value } => RegisterHistoryKind::Write { value },
				RegisterOperation::Read { observed } => RegisterHistoryKind::Read { observed },
			},
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RegisterHistoryKind {
	Write { value: i64 },
	Read { observed: Option<i64> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterHistoryEvent {
	pub op_index: u64,
	pub batch_index: u64,
	pub phase: RegisterHistoryEventPhase,
	pub process: u64,
	pub function: RegisterHistoryFunction,
	pub key: String,
	pub value: Option<i64>,
	pub time: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisterHistoryEventPhase {
	Invoke,
	Ok,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisterHistoryFunction {
	Read,
	Write,
}

fn flatten_batches(batches: &[RegisterHistoryBatch]) -> Vec<RegisterHistoryEvent> {
	let mut out = Vec::new();
	let mut op_index = 0_u64;
	for batch in batches {
		for operation in &batch.operations {
			match operation.op {
				RegisterHistoryKind::Write { value } => {
					out.push(RegisterHistoryEvent {
						op_index,
						batch_index: batch.batch_index,
						phase: RegisterHistoryEventPhase::Invoke,
						process: operation.process,
						function: RegisterHistoryFunction::Write,
						key: operation.key.clone(),
						value: Some(value),
						time: operation.started_at,
					});
					out.push(RegisterHistoryEvent {
						op_index,
						batch_index: batch.batch_index,
						phase: RegisterHistoryEventPhase::Ok,
						process: operation.process,
						function: RegisterHistoryFunction::Write,
						key: operation.key.clone(),
						value: Some(value),
						time: operation.completed_at,
					});
				}
				RegisterHistoryKind::Read { observed } => {
					out.push(RegisterHistoryEvent {
						op_index,
						batch_index: batch.batch_index,
						phase: RegisterHistoryEventPhase::Invoke,
						process: operation.process,
						function: RegisterHistoryFunction::Read,
						key: operation.key.clone(),
						value: None,
						time: operation.started_at,
					});
					out.push(RegisterHistoryEvent {
						op_index,
						batch_index: batch.batch_index,
						phase: RegisterHistoryEventPhase::Ok,
						process: operation.process,
						function: RegisterHistoryFunction::Read,
						key: operation.key.clone(),
						value: observed,
						time: operation.completed_at,
					});
				}
			}
			op_index += 1;
		}
	}
	out
}

fn sanitize_stem(raw: &str) -> String {
	let mut out = String::with_capacity(raw.len());
	for ch in raw.chars() {
		if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
			out.push(ch);
		} else {
			out.push('_');
		}
	}
	if out.is_empty() {
		"history".to_string()
	} else {
		out
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::simulator::checkers::{RegisterHistoryEntry, RegisterOperation};

	fn tempdir(tag: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-register-history-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		fs::create_dir_all(&dir).unwrap();
		dir
	}

	#[test]
	fn register_history_artifact_flattens_batches_into_events() {
		let batch = RegisterHistoryBatch::from_entries(
			0,
			2,
			&[
				RegisterHistoryEntry {
					process: 1,
					key: "tenant/register/0".to_string(),
					started_at: 10,
					completed_at: 11,
					op: RegisterOperation::Write { value: 7 },
				},
				RegisterHistoryEntry {
					process: 2,
					key: "tenant/register/0".to_string(),
					started_at: 12,
					completed_at: 13,
					op: RegisterOperation::Read { observed: Some(7) },
				},
			],
		);
		let artifact = RegisterHistoryArtifact::new("coverage_ladder", 41, 2, vec![batch]);
		assert_eq!(artifact.events.len(), 4);
		assert_eq!(artifact.events[0].phase, RegisterHistoryEventPhase::Invoke);
		assert_eq!(artifact.events[0].function, RegisterHistoryFunction::Write);
		assert_eq!(artifact.events[1].phase, RegisterHistoryEventPhase::Ok);
		assert_eq!(artifact.events[2].phase, RegisterHistoryEventPhase::Invoke);
		assert_eq!(artifact.events[2].value, None);
		assert_eq!(artifact.events[3].value, Some(7));
	}

	#[test]
	fn register_history_artifact_writes_json_file() {
		let dir = tempdir("write");
		let batch = RegisterHistoryBatch::from_entries(
			0,
			1,
			&[RegisterHistoryEntry {
				process: 9,
				key: "tenant/register/1".to_string(),
				started_at: 20,
				completed_at: 21,
				op: RegisterOperation::Write { value: 11 },
			}],
		);
		let artifact = RegisterHistoryArtifact::new("coverage_ladder", 99, 1, vec![batch]);
		let paths = artifact.write_to_dir(&dir).unwrap();
		let body = fs::read(&paths.json).unwrap();
		let decoded: RegisterHistoryArtifact = serde_json::from_slice(&body).unwrap();
		assert_eq!(decoded.schema_version, REGISTER_HISTORY_SCHEMA_VERSION);
		assert_eq!(decoded.model, "single_register");
		assert_eq!(decoded.events.len(), 2);
	}

	#[test]
	fn register_history_artifact_rejects_path_collisions() {
		let dir = tempdir("collision");
		let batch = RegisterHistoryBatch::from_entries(
			0,
			1,
			&[RegisterHistoryEntry {
				process: 3,
				key: "tenant/register/2".to_string(),
				started_at: 30,
				completed_at: 31,
				op: RegisterOperation::Write { value: 13 },
			}],
		);
		let artifact = RegisterHistoryArtifact::new("coverage_ladder", 123, 1, vec![batch]);
		artifact.write_to_dir(&dir).unwrap();
		let err = artifact.write_to_dir(&dir).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::AlreadyExists);
	}
}
