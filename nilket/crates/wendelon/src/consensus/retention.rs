//! Control Ring log retention.
//!
//! `Compacting` is openraft's default policy: the Control log is snapshotted
//! and its prefix purged. `Full` disables automatic snapshots and policy purge
//! so the log is retained from genesis, refuses to start over a history that
//! is not demonstrably complete, and refuses every snapshot or purge path that
//! still reaches this process. A refusal is latched once per process and
//! reported; it never pretends the refused operation happened.
//!
//! The guarantee is local. Whether every peer runs `Full`, and whether this
//! binary understands the setting at all, are deployment checks outside this
//! module.

use openraft::{Config, SnapshotPolicy};
use serde_json::{Map, Value};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ControlRetention {
	/// openraft defaults: automatic snapshots and policy purge.
	#[default]
	Compacting,
	/// No automatic snapshot, no policy purge, and fail-closed refusal of
	/// every remaining snapshot or purge path.
	Full,
}

impl ControlRetention {
	/// Parses the `[storage] control_retention` value; absence is `Compacting`.
	pub fn parse(value: Option<&str>) -> Result<Self, RetentionConfigError> {
		match value {
			None | Some("compacting") => Ok(Self::Compacting),
			Some("full") => Ok(Self::Full),
			Some(_) => Err(RetentionConfigError::UnknownMode),
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Compacting => "compacting",
			Self::Full => "full",
		}
	}

	/// The openraft configuration for this mode and timing profile, before
	/// `validate()`. `Compacting` on the default profile is exactly the
	/// configuration used before either setting existed; `Full` changes only the
	/// snapshot policy and the in-snapshot retention bound. The timing fields
	/// come from `timing` and from nowhere else.
	pub fn openraft_config(
		self,
		cluster_name: &str,
		timing: crate::consensus::ConsensusTiming,
	) -> Config {
		let base = timing.apply(Config {
			cluster_name: cluster_name.into(),
			..Default::default()
		});
		match self {
			Self::Compacting => base,
			Self::Full => Config {
				snapshot_policy: SnapshotPolicy::Never,
				max_in_snapshot_log_to_keep: u64::MAX,
				..base
			},
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionConfigError {
	UnknownMode,
}

impl fmt::Display for RetentionConfigError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("storage.control_retention must be \"compacting\" or \"full\"")
	}
}

impl std::error::Error for RetentionConfigError {}

/// Why a `Full` store refused to open. Carried as the inner error of an
/// `InvalidData` `io::Error`; callers downcast to the variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionStartError {
	SnapshotArtifactPresent,
	SnapshotReceivePresent,
	LegacySegment,
	MissingPrefix { first: u64 },
	PurgedPrefix,
	Gap { after: u64 },
}

impl RetentionStartError {
	pub(crate) fn into_io(self) -> std::io::Error {
		std::io::Error::new(std::io::ErrorKind::InvalidData, self)
	}
}

impl fmt::Display for RetentionStartError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::SnapshotArtifactPresent => {
				f.write_str("full retention refuses a Control snapshot artifact")
			}
			Self::SnapshotReceivePresent => {
				f.write_str("full retention refuses a partially received Control snapshot")
			}
			Self::LegacySegment => {
				f.write_str("full retention refuses a legacy monolithic Control WAL")
			}
			Self::MissingPrefix { first } => {
				write!(
					f,
					"full retention requires history from index 0; it begins at {first}"
				)
			}
			Self::PurgedPrefix => f.write_str("full retention refuses a purged Control log"),
			Self::Gap { after } => write!(f, "full retention found a gap after index {after}"),
		}
	}
}

impl std::error::Error for RetentionStartError {}

/// What this node verified about its stored Control log at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryCheck {
	/// `Compacting`: nothing is checked.
	NotChecked,
	/// No Control artifact of any kind existed.
	Fresh,
	/// Artifacts exist, no entry is stored, and no evidence of a missing
	/// prefix was found. This is not proof that no entry was ever stored, and
	/// it is not authenticated genesis.
	EmptyLog,
	/// Stored entries are exactly `0..=stored_last`. `stored_last` may include
	/// an uncommitted suffix; nothing here says it is committed.
	Complete { stored_last: u64 },
}

impl HistoryCheck {
	fn to_json(self) -> Value {
		match self {
			Self::NotChecked => Value::String("not_checked".into()),
			Self::Fresh => Value::String("fresh".into()),
			Self::EmptyLog => Value::String("empty_log".into()),
			Self::Complete { stored_last } => {
				let mut body = Map::new();
				body.insert("complete_through".into(), Value::from(stored_last));
				Value::Object(body)
			}
		}
	}
}

/// A snapshot or purge path that `Full` refused. Holds no payload data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RetentionViolation {
	SnapshotRequest = 1,
	SnapshotReceive = 2,
	SnapshotInstall = 3,
	SnapshotBuild = 4,
	Purge = 5,
}

impl RetentionViolation {
	pub const ALL: [Self; 5] = [
		Self::SnapshotRequest,
		Self::SnapshotReceive,
		Self::SnapshotInstall,
		Self::SnapshotBuild,
		Self::Purge,
	];

	pub fn as_str(self) -> &'static str {
		match self {
			Self::SnapshotRequest => "snapshot_request",
			Self::SnapshotReceive => "snapshot_receive",
			Self::SnapshotInstall => "snapshot_install",
			Self::SnapshotBuild => "snapshot_build",
			Self::Purge => "purge",
		}
	}

	fn slot(self) -> usize {
		self as usize - 1
	}

	fn from_code(code: u8) -> Option<Self> {
		Self::ALL.into_iter().find(|path| *path as u8 == code)
	}
}

impl fmt::Display for RetentionViolation {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "full Control retention refused {}", self.as_str())
	}
}

impl std::error::Error for RetentionViolation {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionRefusals {
	pub snapshot_request: u64,
	pub snapshot_receive: u64,
	pub snapshot_install: u64,
	pub snapshot_build: u64,
	pub purge: u64,
}

/// Configured mode, startup history check and latched violation, kept apart:
/// none of them implies another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionStatus {
	pub configured: ControlRetention,
	pub history: HistoryCheck,
	pub violation: Option<RetentionViolation>,
	pub refusals: RetentionRefusals,
}

impl RetentionStatus {
	/// The status of a node that never configured `Full`.
	pub fn compacting() -> Self {
		Self {
			configured: ControlRetention::Compacting,
			history: HistoryCheck::NotChecked,
			violation: None,
			refusals: RetentionRefusals::default(),
		}
	}

	pub fn to_json(&self) -> Value {
		let mut refusals = Map::new();
		for (path, count) in [
			(
				RetentionViolation::SnapshotRequest,
				self.refusals.snapshot_request,
			),
			(
				RetentionViolation::SnapshotReceive,
				self.refusals.snapshot_receive,
			),
			(
				RetentionViolation::SnapshotInstall,
				self.refusals.snapshot_install,
			),
			(
				RetentionViolation::SnapshotBuild,
				self.refusals.snapshot_build,
			),
			(RetentionViolation::Purge, self.refusals.purge),
		] {
			refusals.insert(path.as_str().into(), Value::from(count));
		}
		let mut body = Map::new();
		body.insert(
			"configured".into(),
			Value::String(self.configured.as_str().into()),
		);
		body.insert("history".into(), self.history.to_json());
		body.insert(
			"violation".into(),
			self.violation
				.map(|path| Value::String(path.as_str().into()))
				.unwrap_or(Value::Null),
		);
		body.insert("refusals".into(), Value::Object(refusals));
		Value::Object(body)
	}
}

/// One per node, shared by the log store, the state machine, the snapshot
/// builder, the Raft internal server and the readiness view.
#[derive(Debug)]
pub struct RetentionGuard {
	mode: ControlRetention,
	history: OnceLock<HistoryCheck>,
	first: AtomicU8,
	refusals: [AtomicU64; 5],
}

impl RetentionGuard {
	pub fn new(mode: ControlRetention) -> Arc<Self> {
		crate::metrics::global().set_control_retention_full(mode == ControlRetention::Full);
		Arc::new(Self {
			mode,
			history: OnceLock::new(),
			first: AtomicU8::new(0),
			refusals: Default::default(),
		})
	}

	pub fn mode(&self) -> ControlRetention {
		self.mode
	}

	pub(crate) fn record_history(&self, history: HistoryCheck) {
		let _ = self.history.set(history);
	}

	/// `Full`: counts the attempt, latches it if it is the first, and returns
	/// true so the caller refuses before any effect. `Compacting`: false.
	pub(crate) fn refuses(&self, path: RetentionViolation) -> bool {
		if self.mode != ControlRetention::Full {
			return false;
		}
		self.refusals[path.slot()].fetch_add(1, Ordering::AcqRel);
		if self
			.first
			.compare_exchange(0, path as u8, Ordering::AcqRel, Ordering::Acquire)
			.is_ok()
		{
			crate::metrics::global().set_control_retention_violated(true);
		}
		crate::metrics::global().observe_control_retention_refusal(path);
		true
	}

	pub fn status(&self) -> RetentionStatus {
		let count = |path: RetentionViolation| self.refusals[path.slot()].load(Ordering::Acquire);
		RetentionStatus {
			configured: self.mode,
			history: self
				.history
				.get()
				.copied()
				.unwrap_or(HistoryCheck::NotChecked),
			violation: RetentionViolation::from_code(self.first.load(Ordering::Acquire)),
			refusals: RetentionRefusals {
				snapshot_request: count(RetentionViolation::SnapshotRequest),
				snapshot_receive: count(RetentionViolation::SnapshotReceive),
				snapshot_install: count(RetentionViolation::SnapshotInstall),
				snapshot_build: count(RetentionViolation::SnapshotBuild),
				purge: count(RetentionViolation::Purge),
			},
		}
	}
}

/// Refuses through `guard` when one is attached; no guard is `Compacting`.
pub(crate) fn refuses(guard: &Option<Arc<RetentionGuard>>, path: RetentionViolation) -> bool {
	guard.as_ref().is_some_and(|guard| guard.refuses(path))
}

/// Phase A, before WAL replay. Inputs are read-only observations.
pub(crate) fn check_before_replay(
	snapshot_present: bool,
	receive_present: bool,
	segments: impl IntoIterator<Item = (u64, bool)>,
) -> Result<(), RetentionStartError> {
	if snapshot_present {
		return Err(RetentionStartError::SnapshotArtifactPresent);
	}
	if receive_present {
		return Err(RetentionStartError::SnapshotReceivePresent);
	}
	let mut lowest: Option<u64> = None;
	for (start_index, legacy) in segments {
		if legacy {
			return Err(RetentionStartError::LegacySegment);
		}
		lowest = Some(lowest.map_or(start_index, |low| low.min(start_index)));
	}
	match lowest {
		Some(first) if first != 0 => Err(RetentionStartError::MissingPrefix { first }),
		_ => Ok(()),
	}
}

/// Phase B, after replay and the existing committed-pointer resolution.
pub(crate) fn check_after_replay(
	any_artifact: bool,
	purged: bool,
	stored: impl IntoIterator<Item = u64>,
	checkpoint_commit: u64,
) -> Result<HistoryCheck, RetentionStartError> {
	if purged {
		return Err(RetentionStartError::PurgedPrefix);
	}
	let mut last: Option<u64> = None;
	for index in stored {
		match last {
			None if index != 0 => return Err(RetentionStartError::MissingPrefix { first: index }),
			Some(prev) if index != prev + 1 => {
				return Err(RetentionStartError::Gap { after: prev });
			}
			_ => last = Some(index),
		}
	}
	match last {
		Some(stored_last) => Ok(HistoryCheck::Complete { stored_last }),
		None if checkpoint_commit > 0 => Err(RetentionStartError::MissingPrefix { first: 0 }),
		None if any_artifact => Ok(HistoryCheck::EmptyLog),
		None => Ok(HistoryCheck::Fresh),
	}
}

#[cfg(test)]
mod tests;
